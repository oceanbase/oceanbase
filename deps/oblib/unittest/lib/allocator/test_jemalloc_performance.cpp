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
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "gtest/gtest.h"
//#include "jemalloc/jemalloc.h"

#define CACHE_ALIGN_SIZE 64
#define CACHE_ALIGNED __attribute__((aligned(CACHE_ALIGN_SIZE)))

#define OB_LIKELY(x)       __builtin_expect(!!(x),1)
#define OB_UNLIKELY(x)     __builtin_expect(!!(x),0)

#define __COMPILER_BARRIER() asm volatile("" ::: "memory")
#define PAUSE() ({asm("pause\n");})

#define DISALLOW_COPY_AND_ASSIGN(TypeName)      \
  TypeName(const TypeName&);                    \
  void operator=(const TypeName&)

static const int64_t ALLOC_TIME_PER_THREAD = 500000;
static const int64_t WINDOW_SIZE = 16384 << 2;
static const int64_t ALLOC_SIZE = 64;
static const int64_t MAX_THREAD = 128;
static const int64_t QUEUE_SIZE = 500000;

inline int64_t current_time()
{
  struct timeval t;
  if (gettimeofday(&t, NULL) < 0) {
  }
  return (static_cast<int64_t>(t.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(t.tv_usec));
}

// allocaters' wrapper classes

class BaseAllocator
{
public:
  virtual void reset() = 0;
  virtual void *alloc() = 0;
  virtual void free(void *p) = 0;
  virtual ~BaseAllocator() {};
};

class JeAllocator : public BaseAllocator
{
public:
  JeAllocator(int64_t size) : size_(size)
  {
  }

  virtual void reset()
  {
  }

  virtual void *alloc()
  {
    return(::malloc(size_));
  }

  virtual void free(void *p)
  {
    ::free(p);
  }

  virtual ~JeAllocator()
  {
  }

private:
  int64_t size_;

  DISALLOW_COPY_AND_ASSIGN(JeAllocator);
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

class TestEngine
{
  friend void simple_worker(struct worker_context *);
  friend void window_worker(struct worker_context *);
  friend void pairwise_worker(struct worker_context *);

public:
  typedef void (*worker_ptr)(worker_context *);

  TestEngine(BaseAllocator *allocator, int64_t n_thread, worker_ptr worker, Params params,
             bool skip_odd = false)
    : n_thread_(n_thread), allocator_(allocator), worker_(worker), params_(params),
      skip_odd_(skip_odd)
  {
  };

  int run()
  {
    for (int i = 1; i <= n_thread_; ++i) {
      if (skip_odd_ && (i & 1)) {
        continue;
      }

      // reset allocator
      allocator_->reset();

      result_.reset();

      // open threads
      pthread_barrier_init(&barrier_, NULL, i + 1);
      pthread_t workers[i];
      struct worker_context ctx[i];

      for (int j = 0; j < i; ++j) {
        ctx[j].params = &params_;
        ctx[j].engine = this;
        ctx[j].i_thread = j;
        ctx[j].stat = &result_.stat[j];
      }

      for (int j = 0; j < i; ++j) {
        pthread_create(&workers[j],
                       NULL,
                       (void* (*)(void *))worker_,
                       &ctx[j]);
      }

      pthread_barrier_wait(&barrier_);
      result_.start_time = current_time();

      for (int j = 0; j < i; ++j) {
        pthread_join(workers[j], NULL);
      }

      result_.end_time = current_time();

      pthread_barrier_destroy(&barrier_);

      result_.display(i);
    }

    return 0;
  }

  virtual ~TestEngine()
  {
  };

private:
  int64_t n_thread_;
  BaseAllocator *allocator_;
  TestResult result_;
  pthread_barrier_t barrier_;
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

  int64_t start_time = current_time();

  for (int i = 0; i < n; ++i) {
    void* p = allocator->alloc();
    allocator->free(p);
  }

  int64_t end_time = current_time();
  stat->time = end_time - start_time;
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

  int64_t start_time = current_time();

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

  int64_t end_time = current_time();
  stat->time = end_time - start_time;
}

typedef unsigned long int aligned_queue[QUEUE_SIZE] CACHE_ALIGNED;
aligned_queue addr_queue[MAX_THREAD] CACHE_ALIGNED;

struct aligned_int {
  int64_t v;
} CACHE_ALIGNED;
aligned_int heads[MAX_THREAD] CACHE_ALIGNED;
aligned_int tails[MAX_THREAD] CACHE_ALIGNED;

static const int STEP_SIZE = CACHE_ALIGN_SIZE / sizeof(unsigned long int) * 100;

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

  unsigned long int *window = addr_queue[i_thread];
  unsigned long int *window_r = addr_queue[i_thread + shift];

  tail = 0;
  head_r = 0;

  pthread_barrier_wait(&ctx->engine->barrier_);

  int64_t start_time = current_time();

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

  int64_t end_time = current_time();
  stat->time = end_time - start_time;
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

TEST(TestSimpleAllocate, jemalloc)
{
  int64_t max_thread = get_core_num() * 2;
  Params params;
  params.simple_param.times = ALLOC_TIME_PER_THREAD;

  JeAllocator allocator(ALLOC_SIZE);
  TestEngine engine(&allocator, max_thread, &simple_worker, params);

  ASSERT_TRUE(engine.run() >= 0);
}

TEST(TestWindowAllocate, jemalloc)
{
  int64_t max_thread = get_core_num() * 2;
  Params params;
  params.window_param.times = ALLOC_TIME_PER_THREAD;
  params.window_param.window_len = WINDOW_SIZE;

  JeAllocator allocator(ALLOC_SIZE);
  TestEngine engine(&allocator, max_thread, &window_worker, params);

  ASSERT_TRUE(engine.run() >= 0);
}

TEST(TestPairwiseAllocate, jemalloc)
{
  int64_t max_thread = get_core_num() * 2;
  Params params;
  params.pairwise_param.times = ALLOC_TIME_PER_THREAD;

  JeAllocator allocator(ALLOC_SIZE);
  TestEngine engine(&allocator, max_thread, &pairwise_worker, params, true);

  ASSERT_TRUE(engine.run() >= 0);
}

int main(int argc, char *argv[])
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
