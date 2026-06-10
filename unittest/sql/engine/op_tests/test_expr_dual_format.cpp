/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// Acceptance tests for ExprTestSpec vectorization 1.0 / 2.0 dual-format support.
//
// Group A (cg_as_vector_1_0): true vectorization 1.0 single-format correctness. The expression
//   frame is built without a vector header (vector_header_off_ == UINT32_MAX), data is filled as
//   plain datums, and evaluation hits the 1.0 path (eval_batch_func_/eval_func_).
// Group B (enable_dual_format_expr_check): the same expression is independently code-generated and
//   executed once as true 1.0 and once as 2.0; the framework compares row count + per-row results.
// Group C: regression — existing op_tests (test_case_expr, test_material_op, ...) must still pass;
//   those live in their own files, this file only adds the new coverage.
//
// Note on assertions: the result reader stringifies doubles via "%g" while TestValue::to_string()
// uses std::to_string ("1.500000"), so exact verify_ordered() is only used for int/string/NULL
// outputs. For double/decimal we rely on the dual-format check (engine-vs-engine, identical
// formatting) plus row_count, which is exactly what catches a 1.0-vs-2.0 divergence.

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace oceanbase::sql;

class ExprDualFormatTest : public OpTestKit {};

// ============================================================================
// Group A — cg_as_vector_1_0(): true vectorization 1.0 single-format correctness
// ============================================================================

// A1 PassThrough_Simple: SELECT a; result == input. White-box: output expr has no vector header.
TEST_F(ExprDualFormatTest, A1_PassThrough_Simple)
{
  ExprTestSpec spec;
  auto result = spec.columns("a int")
      .with_expr("a")
      .cg_as_vector_1_0()
      .with_data({{1}, {2}, {3}})
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1}, {2}, {3}}));

  // White-box: confirm we really ran a true 1.0 datum frame (no vector header).
  ASSERT_EQ(1u, spec.prepared_out_exprs_vec_.size());
  ASSERT_TRUE(spec.prepared_out_exprs_vec_[0] != nullptr);
  EXPECT_EQ(UINT32_MAX, spec.prepared_out_exprs_vec_[0]->vector_header_off_);
}

// A2 Arithmetic: a+b, a-b, a*b. Handwritten small data then 1000-row generator.
TEST_F(ExprDualFormatTest, A2_Arithmetic_Small)
{
  auto result = expr_unit_test()
      .columns("a int, b int")
      .with_expr("a + b, a - b, a * b")
      .cg_as_vector_1_0()
      .with_data({{3, 1}, {10, 4}, {7, 7}})
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{4, 2, 3}, {14, 6, 40}, {14, 0, 49}}));
}

TEST_F(ExprDualFormatTest, A2_Arithmetic_Generator)
{
  const int64_t N = 1000;
  auto result = expr_unit_test()
      .columns("a int, b int")
      .with_expr("a + b, a - b, a * b")
      .cg_as_vector_1_0()
      .with_data_generator(N, gen::sequential(1, 1), gen::sequential(1, 3))
      .run(engine_);

  EXPECT_EQ(N, result.row_count());
  EXPECT_TRUE(result.verify_ordered(N,
      [](int64_t i) -> TestValue { int64_t a = 1 + i, b = 1 + 3 * i; return TestValue(a + b); },
      [](int64_t i) -> TestValue { int64_t a = 1 + i, b = 1 + 3 * i; return TestValue(a - b); },
      [](int64_t i) -> TestValue { int64_t a = 1 + i, b = 1 + 3 * i; return TestValue(a * b); }));
}

// A3 Compare_Logic: a>b, a=b, (a>0 AND b<10). bool returns int 0/1.
TEST_F(ExprDualFormatTest, A3_Compare_Logic)
{
  auto result = expr_unit_test()
      .columns("a int, b int")
      .with_expr("a > b, a = b, a > 0 AND b < 10")
      .cg_as_vector_1_0()
      .with_data({{5, 3}, {2, 2}, {-1, 20}})
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
      {1, 0, 1},   // 5>3=1, 5=3=0, (5>0 AND 3<10)=1
      {0, 1, 1},   // 2>2=0, 2=2=1, (2>0 AND 2<10)=1
      {0, 0, 0}})); // -1>20=0, -1=20=0, (-1>0 ...)=0
}

// A4 CaseWhen: three ELSE shapes, 256-row sequential (hits 1.0 CASE eval).
TEST_F(ExprDualFormatTest, A4_CaseWhen)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 2 = 0 THEN 0 ELSE 1 END")
      .cg_as_vector_1_0()
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(256, result.row_count());
  EXPECT_TRUE(result.verify_ordered(256,
      [](int64_t i) -> TestValue { return TestValue(static_cast<int64_t>(i % 2 == 0 ? 0 : 1)); }));
}

TEST_F(ExprDualFormatTest, A4_CaseWhen_ElseNull)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 2 = 0 THEN a ELSE NULL END")
      .cg_as_vector_1_0()
      .with_data({{0}, {1}, {2}, {3}, {4}})
      .run(engine_);

  EXPECT_EQ(5, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{0}, {TestValue::null()}, {2}, {TestValue::null()}, {4}}));
}

// A5 Varchar_Concat: variable-length datum direct fill (ptr/len). Small exact + large no-crash.
TEST_F(ExprDualFormatTest, A5_Varchar_Concat_Small)
{
  auto result = expr_unit_test()
      .columns("s1 varchar(32), s2 varchar(32)")
      .with_expr("concat(s1, s2), substr(s1, 1, 3)")
      .cg_as_vector_1_0()
      .with_data({{"ab", "cd"}, {"hello", "world"}})
      .run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"abcd", "ab"}, {"helloworld", "hel"}}));
}

TEST_F(ExprDualFormatTest, A5_Varchar_Concat_Generator)
{
  const int64_t N = 1000;
  auto result = expr_unit_test()
      .columns("s1 varchar(32), s2 varchar(32)")
      .with_expr("concat(s1, s2)")
      .cg_as_vector_1_0()
      .with_data_generator(N, gen::random_string(8, 1), gen::random_string(8, 2))
      .run(engine_);

  // Random strings: verify only row_count + no crash (correctness of var-len datum fill is
  // additionally covered by the dual-format varcher checks in B8).
  EXPECT_EQ(N, result.row_count());
}

// A6 Null_Handling: a, a+1, a IS NULL with explicit null row.
TEST_F(ExprDualFormatTest, A6_Null_Handling)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("a, a + 1, a IS NULL")
      .cg_as_vector_1_0()
      .with_data({{1}, {TestValue::null()}, {3}})
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
      {1, 2, 0},
      {TestValue::null(), TestValue::null(), 1},
      {3, 4, 0}}));
}

// A7 Types_Coverage: fixed-length + variable-length + date, datum direct fill read-back.
// Restricted to deterministically-stringifiable types (int/bigint/varchar/date); double/decimal
// equivalence is covered by the dual-format checks in group B.
TEST_F(ExprDualFormatTest, A7_Types_Coverage)
{
  auto result = expr_unit_test()
      .columns("a int, b bigint, e varchar(16)")
      .with_expr("a, b, e")
      .cg_as_vector_1_0()
      .with_data({{1, 100, "x"}, {2, 200, "yy"}, {3, 300, "zzz"}})
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
      {1, 100, "x"},
      {2, 200, "yy"},
      {3, 300, "zzz"}}));
}

// A8 MultiBatch: a+1 over 1000 sequential rows (crosses 256-batch boundary).
TEST_F(ExprDualFormatTest, A8_MultiBatch)
{
  const int64_t N = 1000;
  ExprTestSpec spec;
  auto result = spec.columns("a int")
      .with_expr("a + 1")
      .cg_as_vector_1_0()
      .with_data_generator(N, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(N, result.row_count());
  EXPECT_TRUE(result.verify_ordered(N, [](int64_t i) -> TestValue { return TestValue(i + 1); }));

  // White-box: still a true 1.0 frame across multiple batches.
  ASSERT_EQ(1u, spec.prepared_out_exprs_vec_.size());
  ASSERT_TRUE(spec.prepared_out_exprs_vec_[0] != nullptr);
  EXPECT_EQ(UINT32_MAX, spec.prepared_out_exprs_vec_[0]->vector_header_off_);
}

// A9 Skip_BoundStart: a%8 over 256 sequential rows with the first k rows skipped.
TEST_F(ExprDualFormatTest, A9_Skip_BoundStart)
{
  int64_t k = 3;
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("a % 8")
      .cg_as_vector_1_0()
      .with_input_skips([k](int64_t, int64_t, ObBitVector *skip) {
        for (int64_t i = 0; i < k; ++i) { skip->set(i); }
      })
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  // First k rows skipped; remaining rows carry values (k..255), so output = (i+k) % 8.
  EXPECT_EQ(256 - k, result.row_count());
  EXPECT_TRUE(result.verify_ordered(256 - k,
      [k](int64_t i) -> TestValue { return TestValue(static_cast<int64_t>((i + k) % 8)); }));
}

// ============================================================================
// Group B — enable_dual_format_expr_check(): 1.0 / 2.0 consistency
// ============================================================================

// B1 PassThrough
TEST_F(ExprDualFormatTest, B1_PassThrough)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("a")
      .enable_dual_format_expr_check()
      .with_data({{1}, {2}, {3}})
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1}, {2}, {3}}));
}

// B2 Arithmetic: a+b, a*b, a%b. Handwritten + generator. (b never 0)
TEST_F(ExprDualFormatTest, B2_Arithmetic_Small)
{
  auto result = expr_unit_test()
      .columns("a int, b int")
      .with_expr("a + b, a * b, a % b")
      .enable_dual_format_expr_check()
      .with_data({{10, 3}, {7, 7}, {20, 6}})
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{13, 30, 1}, {14, 49, 0}, {26, 120, 2}}));
}

TEST_F(ExprDualFormatTest, B2_Arithmetic_Generator)
{
  const int64_t N = 1000;
  auto result = expr_unit_test()
      .columns("a int, b int")
      .with_expr("a + b, a * b, a % b")
      .enable_dual_format_expr_check()
      .with_data_generator(N, gen::sequential(1, 1), gen::sequential(1, 3))
      .run(engine_);

  EXPECT_EQ(N, result.row_count());
  EXPECT_TRUE(result.verify_ordered(N,
      [](int64_t i) -> TestValue { int64_t a = 1 + i, b = 1 + 3 * i; return TestValue(a + b); },
      [](int64_t i) -> TestValue { int64_t a = 1 + i, b = 1 + 3 * i; return TestValue(a * b); },
      [](int64_t i) -> TestValue { int64_t a = 1 + i, b = 1 + 3 * i; return TestValue(a % b); }));
}

// B3 Compare_Logic
TEST_F(ExprDualFormatTest, B3_Compare_Logic)
{
  auto result = expr_unit_test()
      .columns("a int, b int")
      .with_expr("a > b, a = b AND b > 0")
      .enable_dual_format_expr_check()
      .with_data({{5, 3}, {2, 2}, {0, 0}, {-1, 4}})
      .run(engine_);

  EXPECT_EQ(4, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, 0}, {0, 1}, {0, 0}, {0, 0}}));
}

// B4 CaseWhen: three ELSE shapes (重点: CASE 1.0/2.0 路径差异)
TEST_F(ExprDualFormatTest, B4_CaseWhen)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 3 = 0 THEN 0 WHEN a % 3 = 1 THEN 1 ELSE 2 END, "
                 "CASE WHEN a % 2 = 0 THEN a ELSE NULL END")
      .enable_dual_format_expr_check()
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(256, result.row_count());
}

// B5 Boundary_Values: int/bigint boundary values that do not overflow.
TEST_F(ExprDualFormatTest, B5_Boundary_Values)
{
  auto result = expr_unit_test()
      .columns("a int, b bigint")
      .with_expr("a + 1, b - 1")
      .enable_dual_format_expr_check()
      .with_data({
          {static_cast<int64_t>(2147483640), static_cast<int64_t>(9223372036854775800LL)},
          {static_cast<int64_t>(-2147483640LL), static_cast<int64_t>(-9223372036854775800LL)},
          {0, 0},
          {-1, -1}})
      .run(engine_);

  EXPECT_EQ(4, result.row_count());
}

// B6 Null_Edge: null + empty string
TEST_F(ExprDualFormatTest, B6_Null_Edge)
{
  auto result = expr_unit_test()
      .columns("a int, s varchar(16)")
      .with_expr("a + 1, concat(s, 'x'), a IS NULL")
      .enable_dual_format_expr_check()
      .with_data({{1, "ab"}, {TestValue::null(), ""}, {3, "z"}})
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
}

// B7 MultiColumn_Output: many output columns, generator 2000
TEST_F(ExprDualFormatTest, B7_MultiColumn_Output)
{
  const int64_t N = 2000;
  auto result = expr_unit_test()
      .columns("a int, b int")
      .with_expr("a, b, a + b, a * b, CASE WHEN a > b THEN a ELSE b END")
      .enable_dual_format_expr_check()
      .with_data_generator(N, gen::sequential(1, 2), gen::sequential(2, 1))
      .run(engine_);

  EXPECT_EQ(N, result.row_count());
}

// B8 Varchar_Formats: 2.0 side uses VEC_DISCRETE / VEC_CONTINUOUS vs 1.0 datum frame.
TEST_F(ExprDualFormatTest, B8_Varchar_Discrete)
{
  const int64_t N = 500;
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("concat(s, s)")
      .enable_dual_format_expr_check()
      .with_vector_format(VEC_DISCRETE)
      .with_data_generator(N, gen::random_string(8, 7))
      .run(engine_);

  EXPECT_EQ(N, result.row_count());
}

TEST_F(ExprDualFormatTest, B8_Varchar_Continuous)
{
  const int64_t N = 500;
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("length(s)")
      .enable_dual_format_expr_check()
      .with_vector_format(VEC_CONTINUOUS)
      .with_data_generator(N, gen::random_string(8, 11))
      .run(engine_);

  EXPECT_EQ(N, result.row_count());
}

// B9 Complex_Nested: nested CASE mixing arithmetic and length(), generator 3000
TEST_F(ExprDualFormatTest, B9_Complex_Nested)
{
  const int64_t N = 3000;
  auto result = expr_unit_test()
      .columns("a int, b int, s varchar(32)")
      .with_expr("CASE WHEN a % 3 = 0 THEN a + b WHEN a % 3 = 1 THEN length(s) ELSE -1 END")
      .enable_dual_format_expr_check()
      .with_data_generator(N, gen::sequential(0, 1), gen::sequential(5, 2), gen::random_string(8, 13))
      .run(engine_);

  EXPECT_EQ(N, result.row_count());
}

// B10 With_Filter: filter expression also goes through dual-format CG
TEST_F(ExprDualFormatTest, B10_With_Filter)
{
  const int64_t N = 2000;
  auto result = expr_unit_test()
      .columns("a int, b int")
      .with_expr("a + b")
      .enable_dual_format_expr_check()
      .where("a > 0 AND b < 100")
      .with_data_generator(N, gen::sequential(1, 1), gen::sequential(1, 1))
      .run(engine_);

  // 1.0 and 2.0 must agree on both the filtered row count and per-row values.
  EXPECT_GT(result.row_count(), 0);
}

// B11 BigData_MultiBatch: a*2 (int), b+1 (double) over 5000 rows
TEST_F(ExprDualFormatTest, B11_BigData_MultiBatch)
{
  const int64_t N = 5000;
  auto result = expr_unit_test()
      .columns("a int, b double")
      .with_expr("a * 2, b + 1")
      .enable_dual_format_expr_check()
      .with_data_generator(N, gen::sequential(1, 1), gen::random_double(0.0, 1000.0, 17))
      .run(engine_);

  EXPECT_EQ(N, result.row_count());
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  oceanbase::common::init_arches();  // enable AVX512
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
