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

#include <gtest/gtest.h>
#include "atomic"
#include "lib/thread/threads.h"

using namespace oceanbase::lib;

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
