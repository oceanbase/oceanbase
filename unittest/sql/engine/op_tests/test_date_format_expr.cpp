/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace oceanbase::sql;

class DateFormatExprTest : public OpTestKit {};


// Regression test for:
//   SELECT sum(length(date_format(CAST('2026-05-02 01:02:03' AS DATETIME),
//                                 repeat('%r', 500000))));
//
// Root cause: `int16_t len` in get_from_format_* overflows when cumulative
// write length exceeds 32767, corrupting heap memory (SIGSEGV).
// Fix: int16_t → int32_t + buf_len overflow guard in every get_from_format_*.
//
// With buf_len = format_len * 6:
//   format = 3000 × "%r" = 6000 bytes → buf_len = 36000
//   per-row write = 3000 × 11 = 33000 ≤ 36000 → fits, correct result returned.
// Simulate repeat('%r', 500000) at a scale that fits unit-test memory.
// 946722645000000 μs = 2000-01-01 10:30:45 UTC → %r = "10:30:45 AM".
// Before fix: int16_t len wrapped at row 2979 (32769 > 32767), corrupting heap.
// After fix:  result is correct, 3000 × "10:30:45 AM" = 33000 bytes.
TEST_F(DateFormatExprTest, RepeatPercentR_Overflow)
{
  std::string fmt;
  fmt.reserve(6000);
  for (int i = 0; i < 3000; i++) fmt += "%r";

  std::string expected;
  expected.reserve(33000);
  for (int i = 0; i < 3000; i++) expected += "10:30:45 AM";

  std::string expr = "date_format(a, '" + fmt + "')";
  auto result = expr_unit_test()
      .columns("a datetime")
      .with_expr(expr.c_str())
      .with_data({{int64_t(946722645000000LL)}})
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{expected}}));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
