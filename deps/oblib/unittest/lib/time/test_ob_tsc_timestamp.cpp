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
#include "lib/ob_errno.h"
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
