/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "deps/oblib/src/lib/stat/ob_di_cache.h"

namespace oceanbase
{
namespace common
{
TEST(ObStatArray, normal)
{
  int ret = OB_SUCCESS;
  ObWaitEventStatArray stats1;
  ObWaitEventStatArray stats2;

  stats1.get(0)->total_waits_ = 1;
  stats1.get(3)->total_waits_ = 2;
  stats2.get(4)->total_waits_ = 3;
  stats1.add(stats2);

  ASSERT_EQ(1, stats1.get(0)->total_waits_);
  ASSERT_EQ(2, stats1.get(3)->total_waits_);
  ASSERT_EQ(3, stats1.get(4)->total_waits_);
}

}
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}





