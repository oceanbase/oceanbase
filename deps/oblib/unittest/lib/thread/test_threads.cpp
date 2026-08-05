/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <atomic>
#include <pthread.h>
#include <unistd.h>

#include <gtest/gtest.h>
#include "lib/thread/threads.h"

using namespace oceanbase::lib;

namespace
{

static const int64_t CONCURRENT_PARENT_THREAD_COUNT = 8;

class ConcurrentStartThreads : public Threads
{
public:
  explicit ConcurrentStartThreads(std::atomic<int64_t> &child_started_count)
      : child_started_count_(child_started_count)
  {}

  void run(int64_t idx) final
  {
    UNUSED(idx);
    child_started_count_.fetch_add(1, std::memory_order_relaxed);
  }

private:
  std::atomic<int64_t> &child_started_count_;
};

struct ConcurrentStartContext
{
  ConcurrentStartContext()
      : start_gate_(nullptr),
        parent_ready_count_(nullptr),
        child_started_count_(nullptr),
        start_ret_(oceanbase::OB_NOT_INIT)
  {}

  std::atomic<bool> *start_gate_;
  std::atomic<int64_t> *parent_ready_count_;
  std::atomic<int64_t> *child_started_count_;
  int start_ret_;
};

void *concurrent_start_thread(void *arg)
{
  ConcurrentStartContext *context = static_cast<ConcurrentStartContext *>(arg);
  if (nullptr != context) {
    context->parent_ready_count_->fetch_add(1, std::memory_order_release);
    while (!context->start_gate_->load(std::memory_order_acquire)) {
      usleep(10);
    }

    ConcurrentStartThreads threads(*context->child_started_count_);
    context->start_ret_ = threads.start();
    if (oceanbase::OB_SUCCESS == context->start_ret_) {
      threads.stop();
      threads.wait();
    }
  }
  return nullptr;
}

} // namespace

TEST(TestThreads, CanRun)
{
  int val = 0;

  val = 0;
  class Thread: public Threads {
  public:
    Thread(int &val) : val_(val) {}
    virtual void run(int64_t idx) override
    {
      val_++;
    }
    int &val_;
  } th(val);
  th.start();
  th.wait();
  ASSERT_EQ(1, val);
}

TEST(TestThreads, CanRunMulti)
{
  static std::atomic<int64_t> val;
  class Thread: public Threads
  {
  public:
    Thread(std::atomic<int64_t>&val) : val_(val) {}
    void run(int64_t idx) final
    {
      val += idx;
      while (!ATOMIC_LOAD(&has_set_stop()))
        ;
    }
    std::atomic<int64_t> &val_;
  } th(val);

  val = 0;
  th.set_thread_count(7);
  th.start();
  th.stop();
  th.wait();
  ASSERT_EQ(21, val);  // 0+1+2+3+4+5+6
}

TEST(TestThreads, ConcurrentStart)
{
  std::atomic<bool> start_gate(false);
  std::atomic<int64_t> parent_ready_count(0);
  std::atomic<int64_t> child_started_count(0);
  ConcurrentStartContext contexts[CONCURRENT_PARENT_THREAD_COUNT];
  pthread_t parent_threads[CONCURRENT_PARENT_THREAD_COUNT] = {};
  int64_t parent_created_count = 0;
  int parent_create_ret = 0;

  for (int64_t i = 0;
       i < CONCURRENT_PARENT_THREAD_COUNT && 0 == parent_create_ret;
       ++i) {
    contexts[i].start_gate_ = &start_gate;
    contexts[i].parent_ready_count_ = &parent_ready_count;
    contexts[i].child_started_count_ = &child_started_count;
    parent_create_ret = pthread_create(
        &parent_threads[i], nullptr, concurrent_start_thread, &contexts[i]);
    if (0 == parent_create_ret) {
      ++parent_created_count;
    }
  }

  while (parent_ready_count.load(std::memory_order_acquire) < parent_created_count) {
    usleep(10);
  }
  start_gate.store(true, std::memory_order_release);

  for (int64_t i = 0; i < parent_created_count; ++i) {
    EXPECT_EQ(0, pthread_join(parent_threads[i], nullptr));
    EXPECT_EQ(oceanbase::OB_SUCCESS, contexts[i].start_ret_);
  }
  EXPECT_EQ(CONCURRENT_PARENT_THREAD_COUNT, parent_created_count);
  EXPECT_EQ(CONCURRENT_PARENT_THREAD_COUNT,
            child_started_count.load(std::memory_order_relaxed));
}

TEST(TestThreads, DynamicThread)
{
  static std::atomic<int64_t> starts;
  static std::atomic<int64_t> exits;
  class: public Threads
  {
    std::atomic<int64_t> n_threads_;
  public:
    int set_thread_count(int64_t n_threads)
    {
      n_threads_ = n_threads;
      return Threads::set_thread_count(n_threads);
    }
    void run(int64_t idx) final
    {
      starts++;
      while (idx < n_threads_ && !has_set_stop())
        ;
      exits++;
    }
  } th;
  th.set_thread_count(1);
  th.start();
  while (starts != 1) { ::usleep(10); }
  th.set_thread_count(2);
  while (starts != 2) { ::usleep(10); }
  th.set_thread_count(4);
  while (starts != 4) { ::usleep(10); }
  th.set_thread_count(1);
  while (exits != 3) { ::usleep(10); }
  th.submit([]{});
  //ASSERT_EQ(1, th.get_cur_tasks());
  th.stop();
  th.wait();
  ASSERT_EQ(4, starts);
  ASSERT_EQ(4, exits);
}
extern "C" {
int ob_pthread_create(void **ptr, void *(*start_routine) (void *), void *arg);
void ob_pthread_join(void *ptr);
pthread_t ob_pthread_get_pth(void *ptr);
}

void *my_func(void *arg)
{
  pthread_t *pth = (pthread_t*)arg;
  *pth = pthread_self();
  return NULL;
}
TEST(TestThreads, ObPthread)
{
  pthread_t pth = 0;
  void *tid = NULL;
  ASSERT_EQ(0, ob_pthread_create(&tid, my_func, &pth));
  sleep(1);
  ASSERT_EQ(ob_pthread_get_pth(tid), pth);
  ob_pthread_join(tid);
}
int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
