/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "lib/utility/ob_defer.h"

using namespace std;

using namespace oceanbase;
using namespace oceanbase::common;

TEST(ScopedLambda, Basic) {
  bool mybool = false;
  {
    auto exec = MakeScopedLambda([&]() { mybool = true; });  // NOLINT
    EXPECT_FALSE(mybool);
  }
  EXPECT_TRUE(mybool);

  mybool = false;
  {
    auto exec = MakeScopedLambda([&]() { mybool = true; });  // NOLINT
    EXPECT_FALSE(mybool);
    exec.deactivate();
  }
  EXPECT_FALSE(mybool);

  mybool = false;
  {
    auto exec = MakeScopedLambda([&]() { mybool = true; });  // NOLINT
    EXPECT_FALSE(mybool);
    exec.deactivate();
    exec.activate();
  }
  EXPECT_TRUE(mybool);

  int counter = 0;
  {
    auto exec = MakeScopedLambda([&]() { ++counter; });  // NOLINT
    EXPECT_EQ(0, counter);
    exec.run_and_expire();
    EXPECT_EQ(1, counter);
  }
  EXPECT_EQ(1, counter);  // should not have executed upon scope exit.
}

TEST(ScopedLambda, Defer) {
  bool mybool = false;
  {
    DEFER(mybool = true);
    EXPECT_FALSE(mybool);
  }
  EXPECT_TRUE(mybool);

  mybool = false;
  {
    NAMED_DEFER(exec, mybool = true);
    EXPECT_FALSE(mybool);
    exec.deactivate();
  }
  EXPECT_FALSE(mybool);

  mybool = false;
  {
    NAMED_DEFER(exec, mybool = true);
    EXPECT_FALSE(mybool);
    exec.deactivate();
    exec.activate();
  }
  EXPECT_TRUE(mybool);

  int counter = 0;
  {
    NAMED_DEFER(exec, ++counter);
    EXPECT_EQ(0, counter);
    exec.run_and_expire();
    EXPECT_EQ(1, counter);
  }
  EXPECT_EQ(1, counter);  // should not have executed upon scope exit.
}

TEST(Defer, InitializerLists) {
  struct S {
    int a;
    int b;
  };
  int x = 10;
  {
    DEFER({
      S s{10, 20};
      x = s.b;
    });
  }
  EXPECT_EQ(20, x);
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
