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
#include "lib/coro/testing.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace std;

TEST(TestCoroVar, Main)
{
  cotesting::FlexPool([](int, int coidx) {
    RLOCAL(int, v1);
    RLOCAL(int, v2);

    v1 = coidx;
    v2 = coidx * 10;

    EXPECT_EQ(coidx, v1);
    EXPECT_EQ(coidx * 10, v2);
    CO_YIELD();
    EXPECT_EQ(coidx, v1);
    EXPECT_EQ(coidx * 10, v2);
    v1 = v2 + 1;
    EXPECT_EQ(v2 + 1, v1);

    v1 = 0;
    EXPECT_TRUE(v1 > -1);
    EXPECT_TRUE(v1 >= -1);
    EXPECT_TRUE(v1 == 0);
    EXPECT_TRUE(v1 < 1);
    EXPECT_TRUE(v1 <= 1);

    EXPECT_NE(&v1, &v2);
  }, 1, 10).start();
}

TEST(TestCoVar, Assign)
{
  cotesting::FlexPool([] {
    RLOCAL(int, v1);
    RLOCAL(int, v2);
    v1 = 1;
    v2 = 2;
    v2 = v1;  // check operator= only copy value, not members.
    v1 = 0;
    EXPECT_EQ(0, v1);
    EXPECT_EQ(1, v2);
  }, 1, 1).start();
}

TEST(TestCoVar, Ptr)
{
  cotesting::FlexPool([] {
    RLOCAL(int*, p);
    int v = 10;
    p = &v;
    EXPECT_EQ(10, *p);
    EXPECT_GT(11, *p);
  }, 1).start();

  cotesting::FlexPool([] {
    struct S{int v_;} s;
    RLOCAL(S *, p);
    p = &s;

    s.v_ = 1;
    EXPECT_EQ(1, p->v_);

    p->v_ = -1;
    EXPECT_EQ(-1, s.v_);
  }, 1).start();
}

TEST(TestCoVar, CoObj)
{
  // Each routine increases its value, so the value must be 1 because
  // of default value is 0.
  {
    cotesting::FlexPool([&] {
      RLOCAL(int, p)(
          [&]{p.get()++;},
          [&]{p.get()--;}
      );
      EXPECT_EQ(1, p.get());
    }, 4, 10).start();
  }

  // Each routine increases variable v, and there are 10 routines. So
  // when they have finished, variable v should be the original value
  // plus 10.
  {
    int v = 3;
    cotesting::FlexPool([&] {
      RLOCAL(bool, p)(
          [&v]{v++;}, []{});
      p.get();
    }, 1, 10).start();
    EXPECT_EQ(13, v);
  }

  // Each routine increases variable v the p is first get, and
  // decreases variable v before it exists. So finally variable v
  // should be the original value.
  {
    int v = 7;
    cotesting::FlexPool([&] {
      RLOCAL(bool, p)(
          [&v]{v++;},
          [&v]{v--;}
      );
      p.get();
    }, 1, 10).start();
    EXPECT_EQ(7, v);
  }
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
