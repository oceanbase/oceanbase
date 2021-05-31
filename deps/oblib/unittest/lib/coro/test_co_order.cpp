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
#include "lib/coro/co_routine.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "coro/testing.h"

using namespace std;
using namespace oceanbase::lib;
using namespace oceanbase::common;

int step = 0;
#define CSI(v) ASSERT_EQ(v, step++)
constexpr int SWITCH_COUNT = 100000;

TEST(TestCoro, Main)
{
  cotesting::FlexPool(
      [](int, int coidx) {
        if (coidx % 2 == 1) {
          for (int i = 0; i < SWITCH_COUNT; i++) {
            CSI(i * 3);
            this_routine::yield();
            CSI(i * 3 + 2);
          }
        } else {
          for (int i = 0; i < SWITCH_COUNT; i++) {
            this_routine::yield();
            CSI(i * 3 + 1);
          }
        }
      },
      1,
      2)
      .start();
}

int main(int argc, char* argv[])
{
  OB_LOGGER.set_log_level(3);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
