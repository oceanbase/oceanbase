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
#include "lib/coro/testing.h"

using namespace oceanbase::common;

// consume CPU time at least one CPU slice.
static auto consume_cpu = [] {
  for (int64_t i = 0; i < 1 << 22; i++) {
    rdtscp();
  }
};

TEST(TestCoAll, WithCoCheck)
{
  static int counter = 0;
  // True if at least one routine has yield itself to others.
  static bool is_parallel_run = false;

  ////////////////////////////////////////////////////////////////////
  //
  // Check whether COCHECK would yield itself to others when use CPU
  // for long enough time.
  //
  is_parallel_run = false;
  cotesting::FlexPool(
      [] {
        const auto orig_counter = counter++;
        consume_cpu();
        CO_CHECK();
        is_parallel_run = is_parallel_run ?: counter - orig_counter > 1;
      },
      1,
      10)
      .start();
  EXPECT_TRUE(is_parallel_run);

  ////////////////////////////////////////////////////////////////////
  //
  // Check whether COCHECK would yield itself to others when use CPU
  // for *NOT* long enough time.
  //
  is_parallel_run = false;
  cotesting::FlexPool(
      [] {
        const auto orig_counter = counter++;
        CO_CHECK();
        is_parallel_run = is_parallel_run ?: counter - orig_counter > 1;
      },
      1,
      10)
      .start();
  EXPECT_FALSE(is_parallel_run);
}

TEST(TestCoAll, WithoutCoCheck)
{
  static int counter = 0;
  // True if at least one routine has yield itself to others.
  static bool is_parallel_run = false;
  cotesting::FlexPool(
      [] {
        const auto orig_counter = counter++;
        consume_cpu();
        is_parallel_run = is_parallel_run ?: counter - orig_counter > 1;
      },
      1,
      10)
      .start();
  EXPECT_FALSE(is_parallel_run);
}

TEST(TestCoAll, Statistics)
{
  cotesting::FlexPool(
      [] {
        EXPECT_EQ(1ul, this_thread::get_cs());
        this_routine::yield();
        EXPECT_EQ(2ul, this_thread::get_cs());
      },
      1,
      1)
      .start();
}

class CoThreadExit : public ThreadPool {
public:
  CoThreadExit()
  {}
  virtual ~CoThreadExit()
  {}
  void run1() final
  {}
};

TEST(TestCoAll, ExitCB)
{
  static int i = 0;
  int ret = CO_THREAD_ATEXIT([] { i++; });
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = CO_THREAD_ATEXIT([] { i++; });
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(i, 0);

  CoThreadExit cte;
  cte.start();
  cte.wait();
  ASSERT_EQ(i, 2);

  while (OB_SUCC(ret)) {
    ret = CO_THREAD_ATEXIT([] {});
  }
  ASSERT_EQ(ret, OB_SIZE_OVERFLOW);
}

int main(int argc, char* argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
