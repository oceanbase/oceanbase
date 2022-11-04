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
#include "lib/oblog/ob_log_module.h"
#include "lib/metrics/ob_ema_v2.h"
#include <gtest/gtest.h>
#include <cmath>
using namespace oceanbase::common;

bool eq(double a, double b)
{
  return std::abs(a-b) < OB_DOUBLE_EPSINON;
}

TEST(ObEMA, equal_test)
{
  ObEMA acc(0.8);
  ASSERT_TRUE(eq(0, acc.get_value()));
  acc.update(1);
  ASSERT_TRUE(eq(1, acc.get_value()));
  acc.update(1);
  ASSERT_TRUE(eq(1, acc.get_value()));
  acc.update(1);
  ASSERT_TRUE(eq(1, acc.get_value()));
}

TEST(ObEMA, serial_1_test)
{
  double alpha = 0.8;
  double expect = 0;
  int64_t val = 0;

  ObEMA acc(alpha);
  ASSERT_TRUE(eq(0, acc.get_value()));

  expect = 1;
  acc.update(1);
  ASSERT_TRUE(eq(1, acc.get_value()));

  val = 2;
  acc.update(val);
  expect = alpha * (double)val + (1 - alpha) * expect;
  ASSERT_TRUE(eq(expect, acc.get_value()));

  val = 0;
  acc.update(val);
  expect = alpha * (double)val + (1 - alpha) * expect;
  ASSERT_TRUE(eq(expect, acc.get_value()));

  COMMON_LOG(INFO, "acc value", "val", acc.get_value());
}

// serial: 1,2,3, alpha = 0.8, so the latest value has a big impact
TEST(ObEMA, serial_recent_first_test)
{
  double alpha = 0.8;
  double expect = 0;
  int64_t val = 0;

  ObEMA acc(alpha);
  ASSERT_TRUE(eq(0, acc.get_value()));

  // serial: 1,2,1
  expect = 1;
  acc.update(1);
  ASSERT_TRUE(eq(1, acc.get_value()));

  val = 2;
  acc.update(val);
  expect = alpha * (double)val + (1 - alpha) * expect;
  ASSERT_TRUE(eq(expect, acc.get_value()));

  val = 3;
  acc.update(val);
  expect = alpha * (double)val + (1 - alpha) * expect;
  ASSERT_TRUE(eq(expect, acc.get_value()));

  COMMON_LOG(INFO, "acc value", "val", acc.get_value());
}


// serial: 1,2,3, alpha = 0.1, so the historical value has a big impact
TEST(ObEMA, serial_old_first_test)
{
  double alpha = 0.1;
  double expect = 0;
  int64_t val = 0;

  ObEMA acc(alpha);
  ASSERT_TRUE(eq(0, acc.get_value()));

  expect = 1;
  acc.update(1);
  ASSERT_TRUE(eq(1, acc.get_value()));

  val = 2;
  acc.update(val);
  expect = alpha * (double)val + (1 - alpha) * expect;
  ASSERT_TRUE(eq(expect, acc.get_value()));

  val = 3;
  acc.update(val);
  expect = alpha * (double)val + (1 - alpha) * expect;
  ASSERT_TRUE(eq(expect, acc.get_value()));

  COMMON_LOG(INFO, "acc value", "val", acc.get_value());
}


int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

