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

#include "lib/coro/co_local_storage.h"
#include "testing.h"
#include <gtest/gtest.h>

using namespace oceanbase::lib;
using namespace oceanbase::common;

// Allocate some bytes of local storage memory and fulfill with
// different data for each co-routine of each thread. Finally check
// if the data is same as before or not.
TEST(TestCoLocalStorage, Basic)
{
  cotesting::FlexPool(
      [](int coidx) {
        constexpr auto SMALL_SIZE = 1;
        void* ptr = CoLocalStorage::alloc(SMALL_SIZE);
        ASSERT_NE(nullptr, ptr);
        ((char*)ptr)[0] = (char)coidx;
        this_routine::usleep(1000);
        ASSERT_EQ(coidx, ((char*)ptr)[0]);

        constexpr auto LARGE_SIZE = (2 << 20) + 1024;
        ptr = CoLocalStorage::alloc(LARGE_SIZE);
        ASSERT_NE(nullptr, ptr);
        MEMSET(ptr, coidx, LARGE_SIZE);
        this_routine::usleep(1000);
        for (int i = 0; i < LARGE_SIZE; i++) {
          auto buf = (char*)ptr;
          ASSERT_EQ(coidx, buf[i]);
          if (i % (LARGE_SIZE / 200) == 0) {
            this_routine::usleep(1000);
          }
        }
      },
      4,
      10)
      .start();
}

// This test make sure routine local instance works well.
//
// 1. Instance is co-routine local, e.g. each routine can save value
//    into it and one routine's value won't affect other routine's.
// 2. Instance of which constructor/destructor should be called
//    rightly.
TEST(TestCoLocalStorage, LocalObject)
{
  static int nr_c = 0;
  static constexpr auto DEFV = -7;
  struct C {
    C() : v_(DEFV)
    {
      ATOMIC_DEC(&nr_c);
    }
    ~C()
    {
      ATOMIC_FAA(&nr_c, 2);
    }
    int v_;
  };

  cotesting::FlexPool(
      [](int coidx) {
        auto c = CoLocalStorage::get_instance<C>();
        ASSERT_EQ(DEFV, c->v_);  // Ensure constructor has been called.
        c->v_ = coidx;
        this_routine::usleep(1000);
        ASSERT_EQ(coidx, c->v_);  // Ensure routines won't disturb each other.
      },
      2,
      10)
      .start();
  ASSERT_EQ(2 * 10, nr_c);  // Ensure all destructors have been called.

  nr_c = 0;
  cotesting::FlexPool(
      [] {
        auto c = CoLocalStorage::get_instance<C[10]>();
        ASSERT_EQ(DEFV, c[0].v_);
      },
      2)
      .start();
  ASSERT_EQ(2 * 10, nr_c);  // Ensure all destructors have been called.

  cotesting::FlexPool(
      [] {
        auto c0 = CoLocalStorage::get_instance<C, 0>();
        auto c1 = CoLocalStorage::get_instance<C, 1>();
        ASSERT_EQ(DEFV, c0->v_);
        ASSERT_EQ(DEFV, c1->v_);
        ASSERT_NE(c0, c1);
      },
      1,
      10)
      .start();
}

int main(int argc, char* argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
