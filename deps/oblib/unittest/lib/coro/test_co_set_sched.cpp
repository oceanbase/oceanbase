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

#include <iostream>
#include <gtest/gtest.h>
#include "lib/coro/co_set_sched.h"
#include "testing.h"
#include "lib/allocator/ob_malloc.h"

using namespace std;
using namespace oceanbase::lib;
using namespace oceanbase::common;

ObMalloc alloc;

function<void()> func(CoSetSched& sched)
{
  static int total_cnt = 0;
  return [&sched] {
    // cout << "W" << CO_ID() << " created" << endl;
    CO_YIELD();
    this_routine::usleep(1 * 1000L);
    if (total_cnt++ < 1000) {
      sched.create_worker(alloc, 0, func(sched));
    }
  };
}

TEST(TestCoSetSched, Basic)
{
  CoSetSched sched([&sched] {
    [&] {
      // Create 2 set with setid: 0,3
      // Fail since -1 is not a valid setid.
      ASSERT_NE(OB_SUCCESS, sched.create_set(-1));
      // Fail since 0 is a valid setid but has already existed in
      // schedule.
      ASSERT_NE(OB_SUCCESS, sched.create_set(0));
      // Success
      ASSERT_EQ(OB_SUCCESS, sched.create_set(3));

      // Scheduler wouldn't return OB_SUCCESS when creating worker
      // with setid doesn't exist.
      ASSERT_NE(OB_SUCCESS, sched.create_worker(alloc, -1, [] {}));
      ASSERT_NE(OB_SUCCESS, sched.create_worker(alloc, 1, [] {}));

      // Create two
      ASSERT_EQ(OB_SUCCESS, sched.create_worker(alloc, 0, func(sched)));
      ASSERT_EQ(OB_SUCCESS, sched.create_worker(3, [] {
        CO_YIELD();
        this_routine::usleep(10 * 1000L);
      }));
    }();
    if (HasFatalFailure()) {
      return OB_ERROR;
    } else {
      return OB_SUCCESS;
    }
  });
  sched.start();
  sched.wait();
}

TEST(TestCoSetSched, Perf)
{
  static int64_t counter = 0;
  CoSetSched sched([&sched] {
    sched.create_worker(alloc, 0, [&sched] {
      for (int i = 0; i < 10000; i++) {
        sched.create_worker(alloc, 0, [] { counter++; });
        CO_YIELD();
      }
    });
    return 0;
  });
  sched.start();
  sched.wait();
  cout << counter << endl;
}

TEST(TestCoSetSched, DISABLED_PerfThread)
{
  static int64_t counter = 0;
  pthread_t pth[100000];
  for (int i = 0; i < 10000; i++) {
    pthread_create(
        &pth[i],
        nullptr,
        [](void*) {
          counter++;
          return (void*)0;
        },
        nullptr);
  }
  for (int i = 0; i < 10000; i++) {
    pthread_join(pth[i], nullptr);
  }
  cout << counter << endl;
}

TEST(TestCoSetSched, Switch)
{
  CoSetSched sched([&sched] {
    sched.create_set(1);
    sched.create_set(2);
    sched.create_set(3);

    for (int i = 1; i <= 3; i++) {
      sched.create_worker(alloc, 0, [i] {
        CO_SETSCHED_SWITCH(i);
        cout << CO_SETSCHED_CURRENT().get_setid() << endl;
      });
    }
    return OB_SUCCESS;
  });
  sched.start();
  sched.wait();
}

int main(int argc, char* argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
