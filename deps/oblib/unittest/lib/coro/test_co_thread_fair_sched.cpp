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
#include <sstream>
#include <string>
#include "lib/ob_errno.h"
#include "lib/coro/co.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/time/ob_time_utility.h"
#include "testing.h"

using namespace std;
using namespace oceanbase::lib;
using namespace oceanbase::common;

TEST(TestCoThreadFairSched, WakeWait)
{
  CoRoutine* waiting = nullptr;
  cotesting::FlexPool(
      [&] {
        stringstream ss;
        ss << "r" << CO_TIDX();
        auto name = ss.str();
        ss.seekp(0);
        ss << "r" << 3 - CO_TIDX();
        auto another_name = ss.str();

        CO_YIELD();
        int i = 10;
        while (i > 0) {
          if (ATOMIC_BCAS(&waiting, nullptr, &CO_CURRENT())) {
            cout << name << ", wait" << endl;
            CO_WAIT();
            cout << name << ", continue" << endl;
            i--;
          }
          auto now_waiting = waiting;
          if (now_waiting != nullptr) {
            if (ATOMIC_BCAS(&waiting, now_waiting, nullptr)) {
              cout << name << ", "
                   << "wakeup => " << another_name << endl;
              CO_WAKEUP(*now_waiting);
              i--;
            }
          }
        }
        cout << name << ", done" << endl;
      },
      2,
      1)
      .start();
}

TEST(TestCoThreadFairSched, Sleep)
{
  TIME_MORE(1000 * 1000, [] {
    TIME_LESS(2 * 1000 * 1000, [] { cotesting::FlexPool([&] { this_routine::usleep(1000 * 1000); }, 1, 10).start(); });
  });
}

TEST(TestCoThreadFairSched, WaitTimeout)
{
  TIME_LESS(2 * 1000 * 1000, [] { cotesting::FlexPool([] { CO_WAIT(1000000); }, 1, 10).start(); });
}

TEST(TestCoThreadFairSched, WaitTimeout2)
{
  // Test if timer is canceled after being waked up.
  TIME_MORE(900000, [] {
    CoRoutine* waiting = nullptr;
    cotesting::FlexPool(
        [&] {
          const auto start_ts = ObTimeUtility::current_time();
          if (CO_CURRENT().get_cidx() == 1) {
            waiting = &CO_CURRENT();
            CO_WAIT(100000);
            CO_WAIT(0);
            waiting = nullptr;
          } else {
            CO_WAKEUP(*waiting);
            while (ATOMIC_LOAD(&waiting)) {
              const auto now = ObTimeUtility::current_time();
              if (now - start_ts > 1000000) {
                // If waiter doesn't exist in one second, it will hardly
                // exist by self.
                CO_WAKEUP(*waiting);
                break;
              }
              this_routine::usleep(10000);
            }
          }
        },
        1,
        2)
        .start();
  });
}

TEST(TestCoThreadFairSched, WaitLargeTimeout)
{
  TIME_MORE(900000, [] {
    CoRoutine* waiting = nullptr;
    cotesting::FlexPool(
        [&] {
          const auto start_ts = ObTimeUtility::current_time();
          if (CO_CURRENT().get_cidx() == 1) {
            cout << "start r1" << endl;
            waiting = &CO_CURRENT();
            CO_WAIT(INT64_MAX);
            waiting = nullptr;
            cout << "end r1" << endl;
          } else {
            cout << "start r2" << endl;
            while (ATOMIC_LOAD(&waiting)) {
              const auto now = ObTimeUtility::current_time();
              if (now - start_ts > 1000000) {
                // If waiter doesn't exist in one second, it will hardly
                // exist by self.
                CO_WAKEUP(*waiting);
                break;
              }
              this_routine::usleep(10000);
            }
          }
        },
        1,
        2)
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
