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
#include "lib/ob_define.h"
#include <sys/time.h>
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace common
{

TEST(TestCurrentTime, common)
{
  int64_t t1 = ObTimeUtility::current_time();
  usleep(10);
  int64_t t2 = ObTimeUtility::current_time();
  usleep(10);
  int64_t t3 = ObTimeUtility::current_time();
  ASSERT_EQ(true, t1 > 0 && t2 > 0 && t3 > 0);
  ASSERT_EQ(true, t2 > t1);
  ASSERT_EQ(true, t3 > t2);
}

TEST(TestCurrentMonotonicTime, common)
{
  int64_t t1 = ObTimeUtility::current_monotonic_time();
  usleep(10);
  int64_t t2 = ObTimeUtility::current_monotonic_time();
  usleep(10);
  int64_t t3 = ObTimeUtility::current_monotonic_time();
  ASSERT_EQ(true, t1 > 0 && t2 > 0 && t3 > 0);
  ASSERT_EQ(true, t2 > t1);
  ASSERT_EQ(true, t3 > t2);
  // monotic can not go back, get and verify repeatedly.
  int64_t cur_ts = ObTimeUtility::current_monotonic_time();
  for (int i = 0; i < 10000; i++) {
    usleep(10);
    int64_t tmp_ts = ObTimeUtility::current_monotonic_time();
    ASSERT_EQ(true, tmp_ts > cur_ts);
    cur_ts = tmp_ts;
  }
}

TEST(TestCurrentTimeCoarse, common)
{
  int64_t t1 = ObTimeUtility::current_time_coarse();
  usleep(10);
  int64_t t2 = ObTimeUtility::current_time_coarse();
  usleep(10);
  int64_t t3 = ObTimeUtility::current_time_coarse();
  ASSERT_EQ(true, t1 > 0 && t2 > 0 && t3 > 0);
  ASSERT_EQ(true, t2 > t1);
  ASSERT_EQ(true, t3 > t2);
}

}//common
}//oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
