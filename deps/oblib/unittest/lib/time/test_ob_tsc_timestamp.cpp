/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <sys/time.h>
#include "lib/time/ob_tsc_timestamp.h"

namespace oceanbase
{
namespace common
{

inline int64_t current_time()
{
  struct timeval t;
  if (gettimeofday(&t, NULL) < 0) {
  }
  return (static_cast<int64_t>(t.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(t.tv_usec));
}

TEST(TestObTscTimestamp, common)
{
  int ret = OB_SUCCESS;
  int64_t time1 = OB_TSC_TIMESTAMP.current_time();
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t time2 = current_time();
  ASSERT_EQ(time1 / 100000, time2 / 100000);
  usleep(3000000);//3s
  time1 = OB_TSC_TIMESTAMP.current_time();
  ASSERT_EQ(OB_SUCCESS, ret);
  time2 = current_time();
  ASSERT_EQ(time1 / 100000, time2 / 100000);
}

}//common
}//oceanbase

int main(int argc, char **argv)
{
  //oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
