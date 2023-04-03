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

#include "lib/utility/utility.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/metrics/ob_accumulator.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;

TEST(ObAccumulator, full_test)
{
  ObAccumulator acc;
  ASSERT_EQ(0, acc.get_value());
  acc.add(1);
  ASSERT_EQ(0, acc.get_value());
  acc.freeze();
  ASSERT_EQ(1, acc.get_value());

  // After freeze, get_value always returns 1 until the next freeze
  acc.add(100);
  ASSERT_EQ(1, acc.get_value());
  acc.freeze();
  ASSERT_EQ(100, acc.get_value());

  // After freeze, call add again, the internal temporary value is calculated from 0
  acc.add(200);
  acc.add(300);
  acc.freeze();
  ASSERT_EQ(500, acc.get_value());
}


int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

