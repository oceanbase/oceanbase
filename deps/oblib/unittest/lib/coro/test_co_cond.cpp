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
#include <iostream>
#include <stdint.h>
#include "lib/coro/co.h"
#include "lib/ob_errno.h"
#include "lib/coro/syscall/co_cond.h"
#include "lib/random/ob_random.h"
#include "testing.h"

using namespace std;
using namespace oceanbase::lib;
using namespace oceanbase::common;

static const int64_t CO_CNT = 10;
static const int64_t T_CNT = 4;
static const int64_t ROUND = 1000;

TEST(TestCoCond, signal)
{
  CoCond cond;
  int total = 0;
  int g_cid = 0;

  cotesting::FlexPool{[&] {
                        const auto tid = static_cast<uint32_t>(CO_CURRENT().get_tidx());
                        UNUSED(tid);
                        int cid = ATOMIC_AAF(&g_cid, 1);
                        for (int64_t i = 0; i < ROUND; i++) {
                          if (0 == cid % 2) {
                            // produce
                            this_routine::usleep(20);
                            ASSERT_EQ(OB_SUCCESS, cond.lock());
                            total++;
                            ASSERT_EQ(OB_SUCCESS, cond.signal());
                            ASSERT_EQ(OB_SUCCESS, cond.unlock());
                          } else {
                            // consume
                            ASSERT_EQ(OB_SUCCESS, cond.lock());
                            while (0 == total) {
                              ASSERT_EQ(OB_SUCCESS, cond.wait());
                            }
                            this_routine::usleep(10);
                            total--;
                            ASSERT_EQ(OB_SUCCESS, cond.unlock());
                          }
                        }
                      },
      T_CNT,
      CO_CNT}
      .start();
  ASSERT_EQ(0, total);
}

TEST(TestCoCond, broadcast)
{
  CoCond cond;
  int total = 0;
  int g_cid = 0;

  cotesting::FlexPool{[&] {
                        const auto tid = static_cast<uint32_t>(CO_CURRENT().get_tidx());
                        UNUSED(tid);
                        int cid = ATOMIC_AAF(&g_cid, 1);
                        for (int64_t i = 0; i < ROUND; i++) {
                          if (0 == cid % 2) {
                            // produce
                            this_routine::usleep(20);
                            ASSERT_EQ(OB_SUCCESS, cond.lock());
                            total++;
                            if (ObRandom::rand(1, 1000) < 100) {
                              ASSERT_EQ(OB_SUCCESS, cond.broadcast());
                            } else {
                              ASSERT_EQ(OB_SUCCESS, cond.signal());
                            }
                            ASSERT_EQ(OB_SUCCESS, cond.unlock());
                          } else {
                            // consume
                            ASSERT_EQ(OB_SUCCESS, cond.lock());
                            while (0 == total) {
                              ASSERT_EQ(OB_SUCCESS, cond.wait());
                            }
                            this_routine::usleep(10);
                            total--;
                            ASSERT_EQ(OB_SUCCESS, cond.unlock());
                          }
                        }
                      },
      T_CNT,
      CO_CNT}
      .start();
  ASSERT_EQ(0, total);
}

TEST(TestCoCond, timeout)
{
  TIME_LESS(2000 * 1000L, [] {
    CoCond cond;
    cotesting::FlexPool(
        [&] {
          ASSERT_EQ(OB_SUCCESS, cond.lock());
          EXPECT_EQ(OB_TIMEOUT, cond.timed_wait(1000L * 1000L));
          ASSERT_EQ(OB_SUCCESS, cond.unlock());
        },
        1,
        10)
        .start();
  });
}

int main(int argc, char* argv[])
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
