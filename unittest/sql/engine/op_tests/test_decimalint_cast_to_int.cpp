/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * Regression test for DecimalInt CAST to integer aliasing bug.
 * Bug: wide integer divide() wrote quotient=0 before remain=lhs, corrupting
 *      remain when quotient and lhs alias the same variable via
 *      divide(num, sf, num, remain) in scale_to_integer.
 * Fix: ob_wide_integer_impl.h - swap assignment order in early-return branches.
 */

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"

namespace oceanbase
{
namespace sql
{

class DecimalIntCastToIntTest : public OpTestKit {};

// ============================================================================
// Group 1: int128 storage (precision 20, scale 19)
// Primary bug path: scale==19 forces wide integer divide even for small values.
// ============================================================================

TEST_F(DecimalIntCastToIntTest, Int128_PositiveRoundUp)
{
  auto result = expr_unit_test()
      .columns("a decimal(20, 19)")
      .with_expr("CAST(a AS SIGNED)")
      .with_data({
        {"0.5773573592879270000"},
        {"0.5000000000000000000"},
        {"0.9999999999999999999"},
      })
            .run(engine_);
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"1"}, {"1"}}));
}

TEST_F(DecimalIntCastToIntTest, Int128_PositiveRoundDown)
{
  auto result = expr_unit_test()
      .columns("a decimal(20, 19)")
      .with_expr("CAST(a AS SIGNED)")
      .with_data({
        {"0.4999999999999999999"},
        {"0.0000000000000000001"},
        {"0.0000000000000000000"},
      })
            .run(engine_);
  EXPECT_TRUE(result.verify_ordered({{"0"}, {"0"}, {"0"}}));
}

TEST_F(DecimalIntCastToIntTest, Int128_NegativeRoundUp)
{
  auto result = expr_unit_test()
      .columns("a decimal(20, 19)")
      .with_expr("CAST(a AS SIGNED)")
      .with_data({
        {"-0.5773573592879270000"},
        {"-0.5000000000000000000"},
        {"-0.9999999999999999999"},
      })
            .run(engine_);
  EXPECT_TRUE(result.verify_ordered({{"-1"}, {"-1"}, {"-1"}}));
}

TEST_F(DecimalIntCastToIntTest, Int128_NegativeRoundDown)
{
  auto result = expr_unit_test()
      .columns("a decimal(20, 19)")
      .with_expr("CAST(a AS SIGNED)")
      .with_data({
        {"-0.4999999999999999999"},
        {"-0.0000000000000000001"},
      })
            .run(engine_);
  EXPECT_TRUE(result.verify_ordered({{"0"}, {"0"}}));
}

TEST_F(DecimalIntCastToIntTest, Int128_IntegerPartNonZero)
{
  auto result = expr_unit_test()
      .columns("a decimal(20, 19)")
      .with_expr("CAST(a AS SIGNED)")
      .with_data({
        {"1.5000000000000000000"},
        {"-1.5000000000000000000"},
        {"1.4999999999999999999"},
        {"-1.4999999999999999999"},
      })
            .run(engine_);
  EXPECT_TRUE(result.verify_ordered({{"2"}, {"-2"}, {"1"}, {"-1"}}));
}

// ============================================================================
// Group 2: int256 storage (precision 40, scale 19)
// Same divide aliasing bug path, wider storage.
// ============================================================================

TEST_F(DecimalIntCastToIntTest, Int256_RoundUpDown)
{
  auto result = expr_unit_test()
      .columns("a decimal(40, 19)")
      .with_expr("CAST(a AS SIGNED)")
      .with_data({
        {"0.5773573592879270000"},
        {"-0.5773573592879270000"},
        {"0.5000000000000000000"},
        {"0.4999999999999999999"},
      })
            .run(engine_);
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"-1"}, {"1"}, {"0"}}));
}

// ============================================================================
// Group 3: int64 storage (precision 18, scale 17) — control group
// Uses native division, not affected by wide integer aliasing bug.
// ============================================================================

TEST_F(DecimalIntCastToIntTest, Int64_ControlGroup)
{
  auto result = expr_unit_test()
      .columns("a decimal(18, 17)")
      .with_expr("CAST(a AS SIGNED)")
      .with_data({
        {"0.57735735928792700"},
        {"-0.57735735928792700"},
        {"0.50000000000000000"},
        {"0.49999999999999999"},
      })
            .run(engine_);
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"-1"}, {"1"}, {"0"}}));
}

// ============================================================================
// Group 4: Batch with mixed values — all rounding cases in one batch
// ============================================================================

TEST_F(DecimalIntCastToIntTest, Int128_BatchMixed)
{
  auto result = expr_unit_test()
      .columns("a decimal(20, 19)")
      .with_expr("CAST(a AS SIGNED)")
      .with_data({
        {"0.5773573592879270000"},
        {"-0.5773573592879270000"},
        {"0.5000000000000000000"},
        {"-0.5000000000000000000"},
        {"0.4999999999999999999"},
        {"-0.4999999999999999999"},
        {"0.9999999999999999999"},
        {"-0.9999999999999999999"},
        {"1.5000000000000000000"},
        {"-1.5000000000000000000"},
        {"0.0000000000000000000"},
      })
            .run(engine_);
  EXPECT_TRUE(result.verify_ordered({
    {"1"}, {"-1"}, {"1"}, {"-1"}, {"0"}, {"0"},
    {"1"}, {"-1"}, {"2"}, {"-2"}, {"0"}
  }));
}

// ============================================================================
// Group 5: NULL handling
// ============================================================================

TEST_F(DecimalIntCastToIntTest, Int128_NullHandling)
{
  auto result = expr_unit_test()
      .columns("a decimal(20, 19)")
      .with_expr("CAST(a AS SIGNED)")
      .with_data({
        {"0.5773573592879270000"},
        {TestValue::null()},
        {"-0.5773573592879270000"},
      })
      .run(engine_);
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"NULL"}, {"-1"}}));
}

// ============================================================================
// Group 6: Small batch size — stress batch boundary handling
// ============================================================================

TEST_F(DecimalIntCastToIntTest, Int128_SmallBatchSize)
{
  auto result = expr_unit_test()
      .columns("a decimal(20, 19)")
      .with_expr("CAST(a AS SIGNED)")
      .with_data({
        {"0.5773573592879270000"},
        {"-0.5773573592879270000"},
        {"0.5000000000000000000"},
        {"0.4999999999999999999"},
      })
      .with_batch_size(2)
            .run(engine_);
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"-1"}, {"1"}, {"0"}}));
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_decimalint_cast_to_int.log*");
  OB_LOGGER.set_file_name("test_decimalint_cast_to_int.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  common::ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
