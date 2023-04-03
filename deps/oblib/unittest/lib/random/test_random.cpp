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
#include "lib/random/ob_random.h"

namespace oceanbase
{
namespace common
{

TEST(ObRandom, normal)
{
  int64_t res = 0;
  //test static methods
  res = ObRandom::rand(10, 10);
  ASSERT_EQ(10, res);
  res = ObRandom::rand(0, 100);
  ASSERT_TRUE(res <= 100 && res >= 0);
  res = ObRandom::rand(-1, -2);
  ASSERT_TRUE(res >= -2 && res <= -1);
  res = ObRandom::rand(10, 1);
  ASSERT_TRUE(res >= 1 && res <= 10);

  //test int64_t random number
  ObRandom rand1;
  res = rand1.get();
  res = rand1.get(10, 10);
  ASSERT_EQ(10, res);
  res = rand1.get(0, 100);
  ASSERT_TRUE(res <= 100 && res >= 0);
  res = rand1.get(-1, -2);
  ASSERT_TRUE(res >= -2 && res <= -1);
  res = rand1.get(10, 1);
  ASSERT_TRUE(res >= 1 && res <= 10);

  //test int32_t random number
  res = rand1.get_int32();
}

}
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}



