/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/resource.h>

#include "gtest/gtest.h"

#include "lib/alloc/alloc_func.h"
#include "lib/alloc/achunk_mgr.h"
#include "lib/allocator/ob_lf_fifo_allocator.h"
#include "lib/allocator/ob_small_allocator.h"
#include "lib/objectpool/ob_concurrency_objpool.h"

using namespace ::oceanbase;
using namespace ::oceanbase::common;
using namespace ::oceanbase::lib;

#define CACHE_ALIGN_SIZE 64
#define CACHE_ALIGNED __attribute__((aligned(CACHE_ALIGN_SIZE)))

#define OB_LIKELY(x)       __builtin_expect(!!(x),1)
#define OB_UNLIKELY(x)     __builtin_expect(!!(x),0)

#define DISALLOW_COPY_AND_ASSIGN(TypeName)      \
  TypeName(const TypeName&);                    \
  void operator=(const TypeName&)

static const int64_t ALLOC_TIME_PER_THREAD = 500000;
static const int64_t WINDOW_SIZE = 16384 << 2;
static const int64_t ALLOC_SIZE = 64;
static const int64_t MAX_THREAD = 128;
static const int64_t QUEUE_SIZE = ALLOC_TIME_PER_THREAD;

// typedef unsigned long int aligned_queue[QUEUE_SIZE] CACHE_ALIGNED;
// aligned_queue addr_queue[MAX_THREAD] CACHE_ALIGNED;

struct aligned_int {
  int64_t v;
} CACHE_ALIGNED;
aligned_int heads[MAX_THREAD] CACHE_ALIGNED;
aligned_int tails[MAX_THREAD] CACHE_ALIGNED;

static const int STEP_SIZE = CACHE_ALIGN_SIZE / sizeof(unsigned long int) * 100;

// allocaters' wrapper classes

class BaseAllocator
{
public:
  virtual void reset() = 0;
  virtual void *alloc() = 0;
  virtual void free(void *p) = 0;
  virtual ~BaseAllocator() {};
};

class LFFIFOAllocator : public BaseAllocator
{
public:
  LFFIFOAllocator(int64_t size) : allocator_(NULL), size_(size)
  {
  }

  virtual void reset()
  {
    if (!allocator_) {
      allocator_ = new ObLfFIFOAllocator();
      allocator_->init(1024*1024, 3, 1);
    }
  }

  // drop cond checking, try to avoid overhead here.
  virtual void *alloc()
  {
    return allocator_->alloc(size_);
  }

  virtual void free(void *p)
  {
    allocator_->free(p);
  }

  virtual ~LFFIFOAllocator()
  {
    if (allocator_) {
      allocator_->destroy();
      delete allocator_;
    }
  }

private:
  ObLfFIFOAllocator *allocator_;
  int64_t size_;

  DISALLOW_COPY_AND_ASSIGN(LFFIFOAllocator);
};

class SmallAllocator : public BaseAllocator
{
public:
  SmallAllocator(int64_t size) : allocator_(NULL), size_(size)
  {
  }

  virtual void reset()
  {
    if (allocator_) {
      allocator_->destroy();
      delete allocator_;
    }

    allocator_ = new ObSmallAllocator();
    allocator_->init(size_, 3);
  }

  // drop cond checking, try to avoid overhead here.
  virtual void *alloc()
  {
    return allocator_->alloc();
  }

  virtual void free(void *p)
  {
    allocator_->free(p);
  }

  virtual ~SmallAllocator()
  {
    if (allocator_) {
      allocator_->destroy();
      delete allocator_;
    }
  }

private:
  ObSmallAllocator *allocator_;
  int64_t size_;

  DISALLOW_COPY_AND_ASSIGN(SmallAllocator);
};

template <int64_t size>
struct TestObj
{
  char data[size];
};

template <uint64_t size>
class ObjPoolAllocator : public BaseAllocator
{
public:
  ObjPoolAllocator()
  {
  }

  virtual void reset()
  {
  }

  virtual void *alloc()
  {
    return(op_alloc(TestObj<size>));
  }

  virtual void free(void *p)
  {
    op_free((TestObj<size>*)p);
  }

  virtual ~ObjPoolAllocator()
  {
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObjPoolAllocator);
};

class DirectAllocator : public BaseAllocator
{
public:
  DirectAllocator(int64_t size) : size_(size)
  {
  }

  virtual void reset()
  {
  }

  // drop cond checking, try to avoid overhead here.
  virtual void *alloc()
  {
    return ob_malloc(size_);
  }

  virtual void free(void *p)
  {
    ob_free(p);
  }

  virtual ~DirectAllocator()
  {
  }

private:
  int64_t size_;

  DISALLOW_COPY_AND_ASSIGN(DirectAllocator);
};

// input & output

union Params
{
  struct simple_param
  {
    int64_t times;
  } simple_param;

  struct window_param
  {
    int64_t times;
    int64_t window_len;
  } window_param;

  struct pairwise_param
  {
    int64_t times;
    unsigned long int *addr_queue;
  } pairwise_param;
};

struct ThreadStat
{
  int64_t time;
} CACHE_ALIGNED;

struct TestResult
{
  TestResult()
  {
    memset(this, 0, sizeof(TestResult));
  }

  struct ThreadStat stat[256] CACHE_ALIGNED;
  int64_t start_time, end_time;

  void reset()
  {
    memset(this, 0, sizeof(TestResult));
  }

  void display(int64_t n_thread)
  {
    int64_t time_sum = 0;

    for (int i = 0; i < n_thread; ++i) {
      time_sum += stat[i].time;
    }
    int64_t time_avg = time_sum / n_thread;

    char stime_buffer[80], etime_buffer[80];
    struct tm *time_info;

    start_time /= 1000000;
    end_time /= 1000000;

    time_info = localtime(&start_time);
    strftime(stime_buffer, 80, "%T", time_info);
    time_info = localtime(&end_time);
    strftime(etime_buffer, 80, "%T", time_info);

    printf("%3ld %15ld %15s %15s\n", n_thread, time_avg, stime_buffer, etime_buffer);
  }
};

// test routines

class TestEngine;

struct worker_context
{
  Params *params;
  TestEngine *engine;
  int64_t i_thread;
  struct ThreadStat *stat;
};

typedef void (*worker_ptr)(worker_context *);

struct task
{
  worker_ptr func;
  worker_context ctx;
};

class ThreadPool
{
public:
  static ThreadPool *get_instance()
  {
    if (NULL == instance_) {
      instance_ = new ThreadPool();
    }
    return instance_;
  }

  bool add_task(struct task *task)
  {
    bool ret = task_queue_.enqueue(task);
    if (ret) {
      sem_post(&sem_);
    }
    return ret;
  }

  void init(int64_t n_thread)
  {
    n_thread_ = n_thread;
    for (int64_t i = 0; i < n_thread_; ++i) {
      pthread_create(&workers_[i],
                     NULL,
                     (void* (*)(void *))worker,
                     this
                     );
    }
  }

  void shutdown()
  {
    for (int64_t i = 0; i < n_thread_; ++i) {
      pthread_cancel(workers_[i]);
    }

    for (int64_t i = 0; i < n_thread_; ++i) {
      pthread_join(workers_[i], NULL);
    }
  }

  ~ThreadPool()
  {
    sem_destroy(&sem_);
  }

private:
  ThreadPool()
  {
    sem_init(&sem_, 0, 0);
  }
  static ThreadPool *instance_;

  int64_t n_thread_;
  pthread_t workers_[MAX_THREAD];
  sem_t sem_;

  class TaskQueue
  {
  public:
    static const int64_t QUEUE_SIZE = 4096;

    TaskQueue() : head_(0), tail_(0)
    {
      if (0 != pthread_mutex_init(&lock_, NULL)) {
        assert(false);
      }
    }

    ~TaskQueue()
    {
      pthread_mutex_destroy(&lock_);
    }

    bool empty()
    {
      return head_ == tail_;
    }

    bool enqueue(struct task *task)
    {
      bool ret = true;

      pthread_mutex_lock(&lock_);
      if (tail_ - head_ >= QUEUE_SIZE) {
        ret = false;
      } else {
        queue_[(tail_++) % QUEUE_SIZE] = task;
      }
      pthread_mutex_unlock(&lock_);

      return ret;
    }

    task *dequeue()
    {
      task *ret = NULL;

      pthread_mutex_lock(&lock_);
      if (empty()) {
      } else {
        ret = queue_[(head_++) % QUEUE_SIZE];
      }
      pthread_mutex_unlock(&lock_);

      return ret;
    }
  private:
    pthread_mutex_t lock_;
    struct task *queue_[QUEUE_SIZE];
    int64_t head_, tail_;

    DISALLOW_COPY_AND_ASSIGN(TaskQueue);
  } task_queue_;

  static void worker(ThreadPool *pool)
  {
    struct task *task;

    while (true) {
      sem_wait(&pool->sem_);

      task = pool->task_queue_.dequeue();
      if (!task) continue;

      task->func(&task->ctx);
    }
  }

  DISALLOW_COPY_AND_ASSIGN(ThreadPool);
};

ThreadPool* ThreadPool::instance_ = NULL;

class TestEngine
{
  friend void simple_worker(struct worker_context *);
  friend void window_worker(struct worker_context *);
  friend void pairwise_worker(struct worker_context *);

public:

  TestEngine(BaseAllocator *allocator, int64_t n_thread, worker_ptr worker, Params params,
             bool skip_odd = false)
    : n_thread_(n_thread), allocator_(allocator), worker_(worker), params_(params),
      skip_odd_(skip_odd)
  {
  }

  int run()
  {
    ThreadPool *pool = ThreadPool::get_instance();

    for (int i = 1; i <= n_thread_; ++i) {
      if (skip_odd_ && (i & 1)) {
        continue;
      }

      // reset allocator
      allocator_->reset();

      result_.reset();

      // start working
      pthread_barrier_init(&barrier_, NULL, i + 1);
      pthread_barrier_init(&end_barrier_, NULL, i + 1);

      struct task tasks[i];
      for (int j = 0; j < i; ++j) {
        tasks[j].func = worker_;
        tasks[j].ctx.params = &params_;
        tasks[j].ctx.engine = this;
        tasks[j].ctx.i_thread = j;
        tasks[j].ctx.stat = &result_.stat[j];

        if (!pool->add_task(&tasks[j])) {
          printf("add task error.\n");
        }
      }

      pthread_barrier_wait(&barrier_);
      result_.start_time = ObTimeUtility::current_time();

      pthread_barrier_wait(&end_barrier_);
      result_.end_time = ObTimeUtility::current_time();

      pthread_barrier_destroy(&barrier_);
      pthread_barrier_destroy(&end_barrier_);

      result_.display(i);
    }

    return 0;
  }

  virtual ~TestEngine()
  {
  }

private:
  int64_t n_thread_;
  BaseAllocator *allocator_;
  TestResult result_;
  pthread_barrier_t barrier_, end_barrier_;
  worker_ptr worker_;
  Params params_;
  bool skip_odd_;

  DISALLOW_COPY_AND_ASSIGN(TestEngine);
};

void simple_worker(struct worker_context *ctx)
{
  int64_t n = ctx->params->simple_param.times;
  BaseAllocator *allocator = ctx->engine->allocator_;
  ThreadStat *stat = ctx->stat;

  pthread_barrier_wait(&ctx->engine->barrier_);

  int64_t start_time = ObTimeUtility::current_time();

  for (int i = 0; i < n; ++i) {
    void* p = allocator->alloc();
    allocator->free(p);
  }

  int64_t end_time = ObTimeUtility::current_time();
  stat->time = end_time - start_time;

  pthread_barrier_wait(&ctx->engine->end_barrier_);
}

void window_worker(struct worker_context *ctx)
{
  int64_t n = ctx->params->window_param.times;
  int64_t len = ctx->params->window_param.window_len;

  BaseAllocator *allocator = ctx->engine->allocator_;
  ThreadStat *stat = ctx->stat;

  unsigned long int window[len];
  int64_t l = 0, r = 0;

  pthread_barrier_wait(&ctx->engine->barrier_);

  int64_t start_time = ObTimeUtility::current_time();

  for (int i = 0; i < n; ++i) {
    window[r++] = (unsigned long int)allocator->alloc();
    r = r >= len ? 0 : r;

    if (OB_LIKELY(l == r)) {
      allocator->free((void *)window[l++]);
      l = l >= len ? 0 : l;
    }
  }

  while (l != r) {
    allocator->free((void *)window[l++]);
    l = l >= len ? 0 : l;
  }

  int64_t end_time = ObTimeUtility::current_time();
  stat->time = end_time - start_time;

  pthread_barrier_wait(&ctx->engine->end_barrier_);
}


void pairwise_worker(struct worker_context *ctx)
{
  int64_t n = ctx->params->pairwise_param.times;
  int64_t i_thread = ctx->i_thread;

  BaseAllocator *allocator = ctx->engine->allocator_;
  ThreadStat *stat = ctx->stat;

  int64_t shift = (i_thread & 1) ? -1 : 1;
  int64_t &head = heads[i_thread].v;
  int64_t &tail = tails[i_thread].v;
  int64_t &head_r = heads[i_thread + shift].v;
  int64_t &tail_r = tails[i_thread + shift].v;

  unsigned long int *window = &(ctx->params->pairwise_param.addr_queue[i_thread * QUEUE_SIZE]);
  unsigned long int *window_r = &(ctx->params->pairwise_param.addr_queue[(i_thread + shift) * QUEUE_SIZE]);

  tail = 0;
  head_r = 0;

  pthread_barrier_wait(&ctx->engine->barrier_);

  int64_t start_time = ObTimeUtility::current_time();

  int alloc_counter = 0;
  int free_counter = 0;
  while (alloc_counter < n) {
    int64_t t_tail = tail + STEP_SIZE >= QUEUE_SIZE ? tail + STEP_SIZE - QUEUE_SIZE : tail + STEP_SIZE;
    int64_t dis = t_tail - head;

    if (!(dis >= 0 && dis < STEP_SIZE)) {
      for (int i = 0; i < STEP_SIZE; i++) {
        int64_t index = tail + i >= QUEUE_SIZE ? tail + i - QUEUE_SIZE : tail + i;
        window[index] = (unsigned long int)allocator->alloc();
      }

      tail = t_tail;
      alloc_counter += STEP_SIZE;
    }

    if (head_r != tail_r) {
      for (int i = 0; i < STEP_SIZE; i++) {
        int64_t index = head_r + i >= QUEUE_SIZE ? head_r + i - QUEUE_SIZE : head_r + i;
        allocator->free((void *)window_r[index]);
      }

      head_r = head_r + STEP_SIZE >= QUEUE_SIZE ? head_r + STEP_SIZE - QUEUE_SIZE : head_r + STEP_SIZE;
      free_counter += STEP_SIZE;
    }
  }

  // clean up
  while (free_counter < n) {
    if (head_r == tail_r) {
      PAUSE();
      continue;
    }

    for (int i = 0; i < STEP_SIZE; i++) {
      int64_t index = head_r + i >= QUEUE_SIZE ? head_r + i - QUEUE_SIZE : head_r + i;
      allocator->free((void *)window_r[index]);
    }

    head_r = head_r + STEP_SIZE >= QUEUE_SIZE ? head_r + STEP_SIZE - QUEUE_SIZE : head_r + STEP_SIZE;
    free_counter += STEP_SIZE;
  }

  int64_t end_time = ObTimeUtility::current_time();
  stat->time = end_time - start_time;

  pthread_barrier_wait(&ctx->engine->end_barrier_);
}

// gtest cases

int get_core_num()
{
  FILE * fp;
  char res[128];
  fp = popen("/bin/cat /proc/cpuinfo | grep -c '^processor'", "r");
  fread(res, 1, sizeof(res) - 1, fp);
  fclose(fp);

  return atoi(res);
}

TEST(TestSimpleAllocate, lf_fifo)
{
  int64_t max_thread = get_cpu_num() * 2;
  Params params;
  params.simple_param.times = ALLOC_TIME_PER_THREAD;

  LFFIFOAllocator allocator(ALLOC_SIZE);
  TestEngine engine(&allocator, max_thread, &simple_worker, params);

  ASSERT_TRUE(engine.run() >= 0);
}

TEST(TestSimpleAllocate, small)
{
  int64_t max_thread = get_cpu_num() * 2;
  Params params;
  params.simple_param.times = ALLOC_TIME_PER_THREAD;

  SmallAllocator allocator(ALLOC_SIZE);
  TestEngine engine(&allocator, max_thread, &simple_worker, params);

  ASSERT_TRUE(engine.run() >= 0);
}

TEST(TestSimpleAllocate, objpool)
{
 int64_t max_thread = get_cpu_num() * 2;
 Params params;
 params.simple_param.times = ALLOC_TIME_PER_THREAD;

 ObjPoolAllocator<ALLOC_SIZE> allocator;
 TestEngine engine(&allocator, max_thread, &simple_worker, params);

 ASSERT_TRUE(engine.run() >= 0);
}

TEST(TestSimpleAllocate, ob_malloc)
{
  int64_t max_thread = get_cpu_num() * 2;
  Params params;
  params.simple_param.times = ALLOC_TIME_PER_THREAD;

  DirectAllocator allocator(ALLOC_SIZE);
  TestEngine engine(&allocator, max_thread, &simple_worker, params);

  ASSERT_TRUE(engine.run() >= 0);
}

TEST(TestWindowAllocate, lf_fifo)
{
  int64_t max_thread = get_cpu_num() * 2;
  Params params;
  params.window_param.times = ALLOC_TIME_PER_THREAD;
  params.window_param.window_len = WINDOW_SIZE;

  LFFIFOAllocator allocator(ALLOC_SIZE);
  TestEngine engine(&allocator, max_thread, &window_worker, params);

  ASSERT_TRUE(engine.run() >= 0);
}

TEST(TestWindowAllocate, small)
{
 int64_t max_thread = get_cpu_num() * 2;
 Params params;
 params.window_param.times = ALLOC_TIME_PER_THREAD;
 params.window_param.window_len = WINDOW_SIZE;

 SmallAllocator allocator(ALLOC_SIZE);
 TestEngine engine(&allocator, max_thread, &window_worker, params);

 ASSERT_TRUE(engine.run() >= 0);
}

TEST(TestWindowAllocate, objpool)
{
  int64_t max_thread = get_cpu_num() * 2;
  Params params;
  params.window_param.times = ALLOC_TIME_PER_THREAD;
  params.window_param.window_len = WINDOW_SIZE;

  ObjPoolAllocator<ALLOC_SIZE> allocator;
  TestEngine engine(&allocator, max_thread, &window_worker, params);

  ASSERT_TRUE(engine.run() >= 0);
}

TEST(TestWindowAllocate, ob_malloc)
{
  int64_t max_thread = get_cpu_num() * 2;
  Params params;
  params.window_param.times = ALLOC_TIME_PER_THREAD;
  params.window_param.window_len = WINDOW_SIZE;

  DirectAllocator allocator(ALLOC_SIZE);
  TestEngine engine(&allocator, max_thread, &window_worker, params);

  ASSERT_TRUE(engine.run() >= 0);
}

TEST(TestPairwiseAllocate, lf_fifo)
{
  int64_t max_thread = get_core_num() * 2;
  Params params;
  params.pairwise_param.times = ALLOC_TIME_PER_THREAD;
  params.pairwise_param.addr_queue = (unsigned long int *)malloc(sizeof(unsigned long int) * MAX_THREAD * QUEUE_SIZE);

  LFFIFOAllocator allocator(ALLOC_SIZE);
  TestEngine engine(&allocator, max_thread, &pairwise_worker, params, true);

  ASSERT_TRUE(engine.run() >= 0);

  free(params.pairwise_param.addr_queue);
}

TEST(TestPairwiseAllocate, objpool)
{
  int64_t max_thread = get_core_num() * 2;
  Params params;
  params.pairwise_param.times = ALLOC_TIME_PER_THREAD;
  params.pairwise_param.addr_queue = (unsigned long int *)malloc(sizeof(unsigned long int) * MAX_THREAD * QUEUE_SIZE);

  ObjPoolAllocator<ALLOC_SIZE> allocator;
  TestEngine engine(&allocator, max_thread, &pairwise_worker, params, true);

  ASSERT_TRUE(engine.run() >= 0);

  free(params.pairwise_param.addr_queue);
}

TEST(TestPairwiseAllocate, small)
{
  int64_t max_thread = get_core_num() * 2;
  Params params;
  params.pairwise_param.times = ALLOC_TIME_PER_THREAD;
  params.pairwise_param.addr_queue = (unsigned long int *)malloc(sizeof(unsigned long int) * MAX_THREAD * QUEUE_SIZE);

  SmallAllocator allocator(ALLOC_SIZE);
  TestEngine engine(&allocator, max_thread, &pairwise_worker, params, true);

  ASSERT_TRUE(engine.run() >= 0);

  free(params.pairwise_param.addr_queue);
}

TEST(TestPairwiseAllocate, ob_malloc)
{
  int64_t max_thread = get_core_num() * 2;
  Params params;
  params.pairwise_param.times = ALLOC_TIME_PER_THREAD;
  params.pairwise_param.addr_queue = (unsigned long int *)malloc(sizeof(unsigned long int) * MAX_THREAD * QUEUE_SIZE);

  DirectAllocator allocator(ALLOC_SIZE);
  TestEngine engine(&allocator, max_thread, &pairwise_worker, params, true);

  ASSERT_TRUE(engine.run() >= 0);

  free(params.pairwise_param.addr_queue);
}

int main(int argc, char *argv[])
{
  // system("rm -f test_allocator_performance.log*");
  // ObLogger &logger = ObLogger::get_logger();
  // logger.set_file_name("test_allocator_performance.log", true);
  // logger.set_log_level("info");

  testing::InitGoogleTest(&argc,argv);

  AChunkMgr::instance().set_limit(~(1L << 63));

  ThreadPool *pool = ThreadPool::get_instance();
  pool->init(get_cpu_num() * 2);
  int ret = RUN_ALL_TESTS();
  pool->shutdown();

  return ret;
}
