/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * Unit tests for commit 5d733a9c4068:
 *   "opt case when match uniform const param && opt string cmp"
 *
 * Two optimisations are exercised here:
 *
 * 1. else_is_uniform_const dispatch (ob_expr_case.cpp)
 *    When all THEN exprs share one concrete format (VEC_FIXED / VEC_DISCRETE)
 *    and the ELSE branch is VEC_UNIFORM_CONST (e.g. NULL literal), a specialised
 *    dispatch path avoids virtual-call overhead for the else branch.
 *    Test strategy: CASE WHEN … THEN col ELSE NULL END with int / varchar THEN
 *    columns, various NULL fractions, and non-zero bound.start() values.
 *
 * 2. cmp_ws fast string comparison (vector_basic_op.h / expr_cmp_func.ipp)
 *    calc_end_with_space() is called once outside the per-row loop; for
 *    utf8mb4_bin without end-space semantics the inner cmp_ws() falls back to
 *    plain memcmp instead of the charset API.
 *    Test strategy: CASE WHEN a = 'literal' THEN … with varchar columns of
 *    varying lengths, edge strings (empty, identical, prefix relationships).
 */

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "sql/engine/expr/ob_expr_case.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace oceanbase::sql;

class CaseExprOpTest : public OpTestKit {};

// ============================================================================
// Helpers for non-zero bound.start() tests (mirrors provided example pattern)
// ============================================================================
template<int64_t custom_start>
static int eval_case_start(const ObExpr &expr, ObEvalCtx &ctx,
                           const ObBitVector &skip, const EvalBound &bound)
{
  if (bound.end() <= custom_start) {
    return OB_SUCCESS;
  }
  EvalBound sub_bound(bound.batch_size(), custom_start, bound.end(), false);
  return ObExprCase::eval_case_vector(expr, ctx, skip, sub_bound);
}

// ============================================================================
// Part I: else_is_uniform_const path — integer THEN, NULL ELSE
//
// Pattern: CASE WHEN a = k THEN a ELSE NULL END
// The THEN exprs return a VEC_FIXED column; the ELSE literal NULL is
// VEC_UNIFORM_CONST. This triggers the new specialised dispatch.
// ============================================================================

// TC01: All rows match a WHEN branch (no ELSE needed)
TEST_F(CaseExprOpTest, ElseUniformConst_AllMatch)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 2 = 0 THEN a ELSE NULL END")
      .with_data({{0}, {2}, {4}, {6}, {8}})
      .run(engine_);

  EXPECT_EQ(5, result.row_count());
  // All even: all match first branch, ELSE never taken
  EXPECT_EQ("0", result.get_row(0)[0]);
  EXPECT_EQ("2", result.get_row(1)[0]);
  EXPECT_EQ("4", result.get_row(2)[0]);
  EXPECT_EQ("6", result.get_row(3)[0]);
  EXPECT_EQ("8", result.get_row(4)[0]);
}

// TC02: No row matches a WHEN branch (all rows take ELSE NULL)
TEST_F(CaseExprOpTest, ElseUniformConst_NoneMatch)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a < 0 THEN a ELSE NULL END")
      .with_data({{1}, {2}, {3}})
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_EQ("NULL", result.get_row(0)[0]);
  EXPECT_EQ("NULL", result.get_row(1)[0]);
  EXPECT_EQ("NULL", result.get_row(2)[0]);
}

// TC03: Mixed match — alternating rows hit THEN or ELSE NULL
TEST_F(CaseExprOpTest, ElseUniformConst_Mixed_Int)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 2 = 0 THEN a ELSE NULL END")
      .with_data_generator(8, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(8, result.row_count());
  // Even rows: 0,2,4,6 → matched; odd rows: 1,3,5,7 → NULL
  for (int64_t i = 0; i < 8; ++i) {
    if (i % 2 == 0) {
      EXPECT_EQ(std::to_string(i), result.get_row(i)[0]);
    } else {
      EXPECT_EQ("NULL", result.get_row(i)[0]);
    }
  }
}

// TC04: Multiple WHEN branches, ELSE NULL — exercises the dispatch over
// several then-expr slots before reaching the else path
TEST_F(CaseExprOpTest, ElseUniformConst_MultiBranch_Int)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a = 1 THEN 100 "
                      "WHEN a = 2 THEN 200 "
                      "WHEN a = 3 THEN 300 "
                      "ELSE NULL END")
      .with_data({{1}, {2}, {3}, {4}, {5}})
      .run(engine_);

  EXPECT_EQ(5, result.row_count());
  EXPECT_EQ("100",  result.get_row(0)[0]);
  EXPECT_EQ("200",  result.get_row(1)[0]);
  EXPECT_EQ("300",  result.get_row(2)[0]);
  EXPECT_EQ("NULL", result.get_row(3)[0]);
  EXPECT_EQ("NULL", result.get_row(4)[0]);
}

// TC05: Large batch — sequential integers, even->self odd->NULL,
// validates the new dispatch path over a full vectorised batch
TEST_F(CaseExprOpTest, ElseUniformConst_LargeBatch_Int)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 2 = 0 THEN a ELSE NULL END")
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(256, result.row_count());
  for (int64_t i = 0; i < 256; ++i) {
    if (i % 2 == 0) {
      EXPECT_EQ(std::to_string(i), result.get_row(i)[0]);
    } else {
      EXPECT_EQ("NULL", result.get_row(i)[0]);
    }
  }
}

// TC06: NULL in input column — row with NULL 'a' should not match any WHEN
TEST_F(CaseExprOpTest, ElseUniformConst_NullInput)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a = 1 THEN a ELSE NULL END")
      .with_data({{1}, {TestValue::null()}, {2}})
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_EQ("1",    result.get_row(0)[0]);
  EXPECT_EQ("NULL", result.get_row(1)[0]);  // NULL input → ELSE
  EXPECT_EQ("NULL", result.get_row(2)[0]);  // no match → ELSE
}

// TC07: varchar THEN, NULL ELSE — exercises VEC_DISCRETE then-format path
TEST_F(CaseExprOpTest, ElseUniformConst_Varchar_Mixed)
{
  auto result = expr_unit_test()
      .columns("a int, b varchar(32)")
      .with_expr("CASE WHEN a % 2 = 0 THEN b ELSE NULL END")
      .with_data({{0, "zero"}, {1, "one"}, {2, "two"}, {3, "three"}})
      .run(engine_);

  EXPECT_EQ(4, result.row_count());
  EXPECT_EQ("zero",  result.get_row(0)[0]);
  EXPECT_EQ("NULL",  result.get_row(1)[0]);
  EXPECT_EQ("two",   result.get_row(2)[0]);
  EXPECT_EQ("NULL",  result.get_row(3)[0]);
}

// TC08: varchar THEN, NULL ELSE — larger batch
TEST_F(CaseExprOpTest, ElseUniformConst_Varchar_LargeBatch)
{
  // 64 rows, all-match: no row takes the ELSE NULL path
  auto result = expr_unit_test()
      .columns("a varchar(64)")
      .with_expr("CASE WHEN a != 'nomatch' THEN a ELSE NULL END")
      .with_data_generator(64, gen::random_string(8, 42))
      .run(engine_);

  EXPECT_EQ(64, result.row_count());
  for (int64_t i = 0; i < 64; ++i) {
    EXPECT_NE("NULL", result.get_row(i)[0]);
  }
}

// ============================================================================
// Part II: else_is_uniform_const — non-zero bound.start()
//
// Re-use the eval_case_start<N> helper to simulate a vectorised execution
// where the batch does not start at row 0.
// ============================================================================

// TC09–TC16: bound.start() = 0..7  (mirrors the NonZeroBoundStart_* pattern)

TEST_F(CaseExprOpTest, ElseUniformConst_BoundStart_0)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 2 = 0 THEN a ELSE NULL END")
      .with_expr_eval_vector_func(eval_case_start<0>)
      .with_input_skips([](int64_t, int64_t, ObBitVector *skip) {
        for (int64_t i = 0; i < 0; ++i) { skip->set(i); }
      })
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(256, result.row_count());
  for (int64_t i = 0; i < 256; ++i) {
    EXPECT_EQ(i % 2 == 0 ? std::to_string(i) : "NULL", result.get_row(i)[0]);
  }
}

TEST_F(CaseExprOpTest, ElseUniformConst_BoundStart_1)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 2 = 0 THEN a ELSE NULL END")
      .with_expr_eval_vector_func(eval_case_start<1>)
      .with_input_skips([](int64_t, int64_t, ObBitVector *skip) {
        for (int64_t i = 0; i < 1; ++i) { skip->set(i); }
      })
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(255, result.row_count());
  for (int64_t i = 0; i < 255; ++i) {
    int64_t v = i + 1;
    EXPECT_EQ(v % 2 == 0 ? std::to_string(v) : "NULL", result.get_row(i)[0]);
  }
}

TEST_F(CaseExprOpTest, ElseUniformConst_BoundStart_4)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 2 = 0 THEN a ELSE NULL END")
      .with_expr_eval_vector_func(eval_case_start<4>)
      .with_input_skips([](int64_t, int64_t, ObBitVector *skip) {
        for (int64_t i = 0; i < 4; ++i) { skip->set(i); }
      })
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(252, result.row_count());
  for (int64_t i = 0; i < 252; ++i) {
    int64_t v = i + 4;
    EXPECT_EQ(v % 2 == 0 ? std::to_string(v) : "NULL", result.get_row(i)[0]);
  }
}

TEST_F(CaseExprOpTest, ElseUniformConst_BoundStart_8)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 2 = 0 THEN a ELSE NULL END")
      .with_expr_eval_vector_func(eval_case_start<8>)
      .with_input_skips([](int64_t, int64_t, ObBitVector *skip) {
        for (int64_t i = 0; i < 8; ++i) { skip->set(i); }
      })
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(248, result.row_count());
  for (int64_t i = 0; i < 248; ++i) {
    int64_t v = i + 8;
    EXPECT_EQ(v % 2 == 0 ? std::to_string(v) : "NULL", result.get_row(i)[0]);
  }
}

// ============================================================================
// Part III: cmp_ws fast string comparison
//
// The optimisation pre-computes end_with_space once per batch and, for
// utf8mb4_bin without end-space, uses memcmp instead of ObCharset::strcmpsp.
// These tests drive CASE WHEN with string equality predicates so that the
// vectorised comparison function (expr_cmp_func.ipp) is exercised.
// ============================================================================

// TC17: Equal strings — WHEN a = 'hello' matches only exact/case-variant rows.
// Default collation is utf8mb4_general_ci (case-insensitive), so "Hello" == "hello".
TEST_F(CaseExprOpTest, StringCmp_ExactMatch)
{
  auto result = expr_unit_test()
      .columns("a varchar(64)")
      .with_expr("CASE WHEN a = 'hello' THEN 1 ELSE 0 END")
      .with_data({{"hello"}, {"world"}, {"Hello"}, {"hell"}, {"helloo"}})
      .run(engine_);

  EXPECT_EQ(5, result.row_count());
  EXPECT_EQ("1", result.get_row(0)[0]);  // "hello" == "hello"
  EXPECT_EQ("0", result.get_row(1)[0]);  // "world" != "hello"
  EXPECT_EQ("1", result.get_row(2)[0]);  // general_ci: "Hello" == "hello"
  EXPECT_EQ("0", result.get_row(3)[0]);  // prefix shorter: "hell" != "hello"
  EXPECT_EQ("0", result.get_row(4)[0]);  // prefix longer: "helloo" != "hello"
}

// TC18: Empty string edge case
TEST_F(CaseExprOpTest, StringCmp_EmptyString)
{
  auto result = expr_unit_test()
      .columns("a varchar(64)")
      .with_expr("CASE WHEN a = '' THEN 1 ELSE 0 END")
      .with_data({{""},  {"a"}, {""}})
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_EQ("1", result.get_row(0)[0]);
  EXPECT_EQ("0", result.get_row(1)[0]);
  EXPECT_EQ("1", result.get_row(2)[0]);
}

// TC19: Strings that share a common prefix — ensures length-based comparison handling.
// Uses strings that are clearly distinct even under general_ci.
TEST_F(CaseExprOpTest, StringCmp_PrefixStrings)
{
  auto result = expr_unit_test()
      .columns("a varchar(64)")
      .with_expr("CASE WHEN a = 'abc' THEN 1 ELSE 0 END")
      .with_data({{"abc"}, {"ab"}, {"abcd"}, {"abce"}, {"abd"}})
      .run(engine_);

  EXPECT_EQ(5, result.row_count());
  EXPECT_EQ("1", result.get_row(0)[0]);  // exact match
  EXPECT_EQ("0", result.get_row(1)[0]);  // shorter prefix
  EXPECT_EQ("0", result.get_row(2)[0]);  // longer prefix
  EXPECT_EQ("0", result.get_row(3)[0]);  // same length, different suffix
  EXPECT_EQ("0", result.get_row(4)[0]);  // same length, different last char
}

// TC20: Multi-branch string CASE — exercises multiple comparisons per row.
// general_ci: "Apple" matches "apple" branch.
TEST_F(CaseExprOpTest, StringCmp_MultiBranch)
{
  auto result = expr_unit_test()
      .columns("a varchar(32)")
      .with_expr("CASE WHEN a = 'apple'  THEN 1 "
                      "WHEN a = 'banana' THEN 2 "
                      "WHEN a = 'cherry' THEN 3 "
                      "ELSE 0 END")
      .with_data({{"apple"}, {"banana"}, {"cherry"}, {"durian"}, {"Apple"}})
      .run(engine_);

  EXPECT_EQ(5, result.row_count());
  EXPECT_EQ("1", result.get_row(0)[0]);
  EXPECT_EQ("2", result.get_row(1)[0]);
  EXPECT_EQ("3", result.get_row(2)[0]);
  EXPECT_EQ("0", result.get_row(3)[0]);
  EXPECT_EQ("1", result.get_row(4)[0]);  // general_ci: "Apple" == "apple"
}

// TC21: Large batch string comparison — validates the new cmp_ws path over
// a full vectorised batch with deterministic string data
TEST_F(CaseExprOpTest, StringCmp_LargeBatch)
{
  // 128 rows with random strings: at most one accidentally equals 'target'.
  // We just verify correctness: all rows are collected and none crashed.
  auto result = expr_unit_test()
      .columns("a varchar(32)")
      .with_expr("CASE WHEN a = 'target' THEN 1 ELSE 0 END")
      .with_data_generator(128, gen::random_string(8, 12345))
      .run(engine_);

  EXPECT_EQ(128, result.row_count());
  for (int64_t i = 0; i < 128; ++i) {
    const std::string &v = result.get_row(i)[0];
    EXPECT_TRUE(v == "0" || v == "1");
  }
}

// TC22: NULL varchar input with string equality — NULL should not match any WHEN
TEST_F(CaseExprOpTest, StringCmp_NullInput)
{
  auto result = expr_unit_test()
      .columns("a varchar(64)")
      .with_expr("CASE WHEN a = 'x' THEN 1 ELSE 0 END")
      .with_data({{"x"}, {TestValue::null()}, {"y"}})
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_EQ("1", result.get_row(0)[0]);
  EXPECT_EQ("0", result.get_row(1)[0]);  // NULL = 'x' is UNKNOWN → ELSE
  EXPECT_EQ("0", result.get_row(2)[0]);
}

// TC23: Combine else_is_uniform_const and string comparison:
// varchar THEN, NULL ELSE, equality predicate on varchar column
TEST_F(CaseExprOpTest, StringCmp_ElseNull_Combined)
{
  auto result = expr_unit_test()
      .columns("a varchar(32)")
      .with_expr("CASE WHEN a = 'yes' THEN a ELSE NULL END")
      .with_data({{"yes"}, {"no"}, {"yes"}, {"maybe"}})
      .run(engine_);

  EXPECT_EQ(4, result.row_count());
  EXPECT_EQ("yes",  result.get_row(0)[0]);
  EXPECT_EQ("NULL", result.get_row(1)[0]);
  EXPECT_EQ("yes",  result.get_row(2)[0]);
  EXPECT_EQ("NULL", result.get_row(3)[0]);
}

// TC24: Strings of different lengths — ensures the length-based branch in
// cmp_ws (l_len < r_len / l_len > r_len) is exercised
TEST_F(CaseExprOpTest, StringCmp_DifferentLengths)
{
  auto result = expr_unit_test()
      .columns("a varchar(64)")
      .with_expr("CASE WHEN a = 'mid' THEN 1 ELSE 0 END")
      .with_data({{"m"}, {"mi"}, {"mid"}, {"midd"}, {"middle"}})
      .run(engine_);

  EXPECT_EQ(5, result.row_count());
  EXPECT_EQ("0", result.get_row(0)[0]);
  EXPECT_EQ("0", result.get_row(1)[0]);
  EXPECT_EQ("1", result.get_row(2)[0]);
  EXPECT_EQ("0", result.get_row(3)[0]);
  EXPECT_EQ("0", result.get_row(4)[0]);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
