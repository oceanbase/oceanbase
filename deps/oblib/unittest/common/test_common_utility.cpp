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
#define private public
#include "common/ob_common_utility.h"
using namespace  oceanbase::common;
TEST(TestBasicTimeGuard, tl_time_guard)
{
  EXPECT_TRUE(NULL == ObBasicTimeGuard::get_tl_time_guard());
  BASIC_TIME_GUARD(tg, "ObLog");
  EXPECT_TRUE(&tg == ObBasicTimeGuard::get_tl_time_guard());
  {
    BASIC_TIME_GUARD(tg1, "ObMalloc");
    EXPECT_TRUE(&tg1 == ObBasicTimeGuard::get_tl_time_guard());
  }
  EXPECT_TRUE(&tg == ObBasicTimeGuard::get_tl_time_guard());
}

TEST(TestBasicTimeGuard, click_infos)
{
  BASIC_TIME_GUARD(tg, "ObMalloc");
  int index = 8;
  for (int i = 0; i < 16; ++i) {
    usleep(5);
    BASIC_TIME_GUARD_CLICK("alloc_chunk");
  }
  tg.click_infos_[index].cost_time_ = 1;
  EXPECT_EQ(index, tg.click_infos_[index].seq_);
  usleep(5);
  BASIC_TIME_GUARD_CLICK("target");
  EXPECT_EQ(16, tg.click_infos_[index].seq_);
}
int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
