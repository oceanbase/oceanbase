/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "lib/lock/ob_scond.h"
#include "lib/coro/testing.h"

TEST(TestScond, Basic)
{
  TIME_LESS(200000l, [] {
    SCond cond;
    cotesting::FlexPool([&cond] {
      cond.prepare();
      cond.wait(100000l);
    }, 1, 10).start();
  });
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
