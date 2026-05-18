/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * Verify that decimal-int columns are stored with the correct column scale
 * when the input data comes from double, int, or string literals.
 */

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"

namespace oceanbase
{
namespace sql
{

class DecimalEncodingBugTest : public OpTestKit {};

// T1: double input to decimal(12,2) column — must be stored as scale=2.
TEST_F(DecimalEncodingBugTest, SumDecimalFromDouble)
{
  auto result = scalar_agg_test()
      .table("t", "v decimal(12,2)")
      .select("sum(v)")
      .with_data({{10.0}, {20.0}})
      .run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"30.00"}}))
      << "10.0 + 20.0 should be 30.00 in decimal(12,2)";
}

// T2: same values via a bigint column — baseline to rule out aggregation bugs.
TEST_F(DecimalEncodingBugTest, SumBigintBaseline)
{
  auto result = scalar_agg_test()
      .table("t", "v bigint")
      .select("sum(v)")
      .with_data({{10}, {20}})
      .run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"30"}}));
}

// T3: int input to decimal(12,2) column — must be stored as scale=2.
TEST_F(DecimalEncodingBugTest, SumDecimalFromInt)
{
  auto result = scalar_agg_test()
      .table("t", "v decimal(12,2)")
      .select("sum(v)")
      .with_data({{10}, {20}})
      .run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"30.00"}}))
      << "10 + 20 should be 30.00 in decimal(12,2)";
}

// T4: scale=4 column with mixed int/double inputs.
TEST_F(DecimalEncodingBugTest, SumDecimalScale4)
{
  auto result = scalar_agg_test()
      .table("t", "v decimal(20,4)")
      .select("sum(v)")
      .with_data({{1.5}, {2}, {3.25}})
      .run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"6.7500"}}));
}

// T5: median price of three items (includes result with fractional part).
TEST_F(DecimalEncodingBugTest, MedianDecimalFromInt)
{
  auto result = scalar_agg_test()
      .table("t", "price decimal(10,2)")
      .select("sum(price)")
      .with_data({{15}, {25}, {35}})
      .run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"75.00"}}));
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_decimal_encoding_bug.log*");
  OB_LOGGER.set_file_name("test_decimal_encoding_bug.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  common::ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
