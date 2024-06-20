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
#include "lib/oblog/ob_syslog_rate_limiter.h"

 //using namespace ::oblib;
using namespace oceanbase::lib;
using namespace oceanbase::common;

TEST(TestSampleRateLimiter, Basic)
{
  ObSyslogSampleRateLimiter rl(1, 3, 1000000/*1s*/);
  ASSERT_EQ(OB_SUCCESS, rl.try_acquire(1, OB_LOG_LEVEL_INFO));
  ASSERT_EQ(OB_EAGAIN, rl.try_acquire(1, OB_LOG_LEVEL_INFO));
  ASSERT_EQ(OB_EAGAIN, rl.try_acquire(1, OB_LOG_LEVEL_INFO));
  ASSERT_EQ(OB_SUCCESS, rl.try_acquire(1, OB_LOG_LEVEL_INFO));
  ASSERT_EQ(OB_EAGAIN, rl.try_acquire(1, OB_LOG_LEVEL_INFO));
  ASSERT_EQ(OB_EAGAIN, rl.try_acquire(1, OB_LOG_LEVEL_INFO));
  ASSERT_EQ(OB_SUCCESS, rl.try_acquire(1, OB_LOG_LEVEL_INFO));

  sleep(1);
  ASSERT_EQ(OB_SUCCESS, rl.try_acquire(1, OB_LOG_LEVEL_INFO));
  ASSERT_EQ(OB_EAGAIN, rl.try_acquire(2, OB_LOG_LEVEL_INFO));
  ASSERT_EQ(OB_SUCCESS, rl.try_acquire(1, OB_LOG_LEVEL_INFO));
  ASSERT_EQ(OB_EAGAIN, rl.try_acquire(2, OB_LOG_LEVEL_INFO));
  ASSERT_EQ(OB_SUCCESS, rl.try_acquire(1, OB_LOG_LEVEL_INFO));

  sleep(1);
  ASSERT_EQ(OB_SUCCESS, rl.try_acquire(1, OB_LOG_LEVEL_ERROR));
  ASSERT_EQ(OB_EAGAIN, rl.try_acquire(2, OB_LOG_LEVEL_ERROR));
  ASSERT_EQ(OB_SUCCESS, rl.try_acquire(1, OB_LOG_LEVEL_ERROR));

}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
