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
#include "lib/coro/co.h"
#include "atomic"
#include "lib/coro/co_user_thread.h"

using namespace oceanbase::lib;

TEST(TestCoUserThread, CanRun)
{
  int val = 0;

  val = 0;
  CoUserThread th([&val] { val++; }, 1 << 20);
  th.start();
  th.wait();
  ASSERT_EQ(1, val);
}

TEST(TestCoKThread, CanRun)
{
  static std::atomic<int64_t> val;
  class : public CoKThread {
    void run(int64_t idx) final
    {
      val += idx;
      while (!ATOMIC_LOAD(&has_set_stop()))
        ;
    }
  } th;

  val = 0;
  th.set_thread_count(7);
  th.start();
  th.stop();
  th.wait();
  ASSERT_EQ(21, val);  // 0+1+2+3+4+5+6
}

TEST(TestCoKThread, DynamicThread)
{
  static std::atomic<int64_t> starts;
  static std::atomic<int64_t> exits;
  class : public CoXThread {
    std::atomic<int64_t> n_threads_;

  public:
    int set_thread_count(int64_t n_threads)
    {
      n_threads_ = n_threads;
      return CoXThread::set_thread_count(n_threads);
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
  while (starts != 1) {
    this_routine::usleep(10);
  }
  th.set_thread_count(2);
  while (starts != 2) {
    this_routine::usleep(10);
  }
  th.set_thread_count(4);
  while (starts != 4) {
    this_routine::usleep(10);
  }
  th.set_thread_count(1);
  while (exits != 3) {
    this_routine::usleep(10);
  }
  th.submit([] {});
  ASSERT_EQ(1, th.get_cur_tasks());
  th.stop();
  th.wait();
  ASSERT_EQ(4, starts);
  ASSERT_EQ(4, exits);
}

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
