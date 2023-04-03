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
#include "lib/thread_local/ob_tsi_utils.h"

using namespace oceanbase::common;
using namespace std;

unsigned long long get_ticks(void)
{
  register uint64_t lo, hi;
  __asm__ __volatile__ (
      "rdtscp" : "=a"(lo), "=d"(hi)
  );
  return hi << 32 | lo;

}

TEST(TestItid, Basic)
{
  // This thread is allocate a itid before.
  EXPECT_EQ(1, itid_slots[0]);

  // And the maximum itid is the only ID: 0
  EXPECT_EQ(0, detect_max_itid());

  // Allow enough ID and check continuance.
  constexpr int64_t STARTER = 1;
  for (int i = 0; i < 1024*64-1; i++) {
    int64_t itid = alloc_itid();
    EXPECT_EQ(i+STARTER, itid);
    EXPECT_EQ(i+STARTER, detect_max_itid());
  }

  // Return -1 if exceed total count of itid.
  EXPECT_EQ(INVALID_ITID, alloc_itid());
  EXPECT_EQ(INVALID_ITID, alloc_itid());

  // Maximum itid is 1024*64-1
  EXPECT_EQ(1024*64-1, detect_max_itid());

  // Maximum itid changes if free the largest one.
  free_itid(1024*64-1);
  EXPECT_EQ(1024*64-2, detect_max_itid());

  // Free itids and check maximum itid is right.
  for (int i = 1024*64-2; i > 0; i--) {
    free_itid(i);
    EXPECT_EQ(i-1, detect_max_itid());
  }
}

TEST(TestItid, ThreadAllocFree)
{
  class Runnable
      : public lib::Threads {
    void run(obsys::CThread *, void *) override
    {
      EXPECT_LT(0, get_itid());
    }
  } r;
  r.set_thread_count(100);
  r.start();
  r.wait();
  EXPECT_EQ(0, get_itid());
  EXPECT_EQ(1, get_max_itid());
  r.start();
  r.wait();
  EXPECT_EQ(0, get_itid());
  EXPECT_EQ(1, get_max_itid());
}

int64_t my_get_max_itid() {
  static __thread int64_t itid = 0;
  if (itid == 0) {
    itid++;
  }
  return itid;
}

TEST(TestItid, Perf)
{
  int64_t current = 0
      , previous = 0;
  constexpr int64_t N = 1000000;
  {
    auto t1 = get_ticks();
    for (int i = 0; i < N; i++) {
      auto itid = get_max_itid();
      EXPECT_EQ(1, itid);
    }
    auto t2 = get_ticks();
    current = t2 - t1;
  }
  {
    auto t1 = get_ticks();
    for (int i = 0; i < N; i++) {
      auto itid = my_get_max_itid();
      EXPECT_EQ(1, itid);
    }
    auto t2 = get_ticks();
    previous = t2 - t1;
  }
  // Performance of current version of get_max_itid() shouldn't worse
  // more than 10% of previous version of this function which just
  // fetch a thread local variable.
  EXPECT_LE(static_cast<int64_t>((double)current * 0.9), previous)
      << "  current: " << current
      << ", previous: " << previous << endl;
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
