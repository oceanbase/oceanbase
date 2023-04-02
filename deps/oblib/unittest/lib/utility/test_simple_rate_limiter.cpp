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

TEST(TestSimpleRateLimiter, Basic)
{
  ObSimpleRateLimiter rl(3);
  ASSERT_EQ(OB_SUCCESS, rl.try_acquire());
  ASSERT_EQ(OB_SUCCESS, rl.try_acquire());
  ASSERT_EQ(OB_SUCCESS, rl.try_acquire());
  ASSERT_NE(OB_SUCCESS, rl.try_acquire());
  ASSERT_NE(OB_SUCCESS, rl.try_acquire());

  sleep(1);
  ASSERT_EQ(OB_SUCCESS, rl.try_acquire());
  ASSERT_EQ(OB_SUCCESS, rl.try_acquire());
  ASSERT_EQ(OB_SUCCESS, rl.try_acquire());
  ASSERT_NE(OB_SUCCESS, rl.try_acquire());
  ASSERT_NE(OB_SUCCESS, rl.try_acquire());

  sleep(1);
  rl.set_rate(1);
  ASSERT_EQ(OB_SUCCESS, rl.try_acquire());
  ASSERT_NE(OB_SUCCESS, rl.try_acquire());

  {
    ObSimpleRateLimiter rl(99);
    for (int i = 0; i < 25; i++) {
      ASSERT_EQ(OB_SUCCESS, rl.try_acquire(4)) << i;
    }
    ASSERT_NE(OB_SUCCESS, rl.try_acquire(4));
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
