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
#include "testing.h"
#include "lib/coro/syscall/co_futex.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace std;

TEST(TestCoFutex, WakeUp)
{
  CoFutex ftx;
  cotesting::FlexPool()
      // This thread create 10 routines and waiting for waking up.
      .create(
          [&] {
            cout << "waiting" << endl;
            EXPECT_EQ(OB_SUCCESS, ftx.wait(0, -1));
            cout << "wait done" << endl;
          },
          10)

      // This thread wakes other routines for 10 times.
      .create(
          [&] {
            for (int i = 0; i < 10; i++) {
              ftx.val() = 1;
              cout << "wake" << endl;
              ftx.wake(1);
              cout << "wake done" << endl;
              this_routine::usleep(1000);
            }
          },
          1)
      .start();
}

TEST(TestCoFutex, Timeout)
{
  TIME_LESS(2000 * 1000L, [] {
    CoFutex ftx;
    cotesting::FlexPool(
        [&] {
          // cout << "waiting" << endl;
          EXPECT_EQ(OB_TIMEOUT, ftx.wait(0, 1000L * 1000L));
          // cout << "wait done" << endl;
        },
        1,
        10)
        .start();
  });
}

int main(int argc, char* argv[])
{
  // disable cout output stream
  cout.rdbuf(nullptr);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
