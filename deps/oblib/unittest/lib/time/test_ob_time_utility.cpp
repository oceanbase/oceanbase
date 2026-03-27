/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
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
