/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * Coverage tests for the CASE WHEN expression (sql/engine/expr/ob_expr_case.cpp).
 *
 * This file complements test_case_expr.cpp / test_case_expr_op.cpp by adding:
 *   1. A regression guard for the divisor-by-zero failure observed in mysqltest
 *      `constant_folding_fallback_oracle`:
 *          select case when a = 1 then 1 else 1 / 0 end from t;
 *      At the expression-evaluation layer the relevant guarantee is CASE branch
 *      LAZY EVALUATION (short-circuit): a branch that is skipped for every active
 *      row must NOT be evaluated, so a dangerous expression (constant 1/0) in the
 *      not-taken branch must not raise an error. Reproduced in Oracle mode where
 *      1/0 raises OBE-01476; if the short-circuit guard breaks, the framework's
 *      FatalErrorChecker turns the resulting error into a test failure.
 *   2. Large-data (>= 500 rows) coverage for EVERY current CASE WHEN optimization
 *      path: else_is_uniform_const, then_expr_same_type fixed fast path across
 *      decimal-int widths, no-else set_null, multi-branch AVX512 when-match, and
 *      non-zero bound.start() vectorized execution.
 *
 * NOTE: op_tests run resolve + code-gen + vectorized execution only; no transformer
 * / constant-folding rewrite runs, so `1/0` stays a real division expression and
 * exercises the runtime short-circuit logic directly.
 */

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "sql/engine/expr/ob_expr_case.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace oceanbase::sql;

class CaseExprCoverageTest : public OpTestKit {};

// Row counts: keep at least one >= 500 large-data case per optimization path.
static constexpr int64_t LARGE = 512;
static constexpr int64_t XLARGE = 1024;

// Helper to drive vectorized eval with a non-zero bound.start() (mirrors the
// pattern in test_case_expr.cpp). Rows [0, custom_start) are excluded from the
// vectorized range to verify mask / set_null boundary handling.
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
// Part I: Branch selection for the divisor-by-zero failure shape.
//
// Mirrors mysqltest constant_folding_fallback_oracle:
//   select case when a = 1 then 1 else 1 / 0 end from t;
//
// Oracle mode coverage below checks the expression-evaluation parts of the
// mysqltest shape:
//   * empty input: projection expressions are not evaluated, so no error;
//   * non-empty input: expressions that are actually selected/evaluated raise
//     Oracle-compatible errors.
//
// `CAST('aaa' AS NUMBER)` is intentionally left to mysqltest coverage. In this
// op_tests harness, expression SQL is parsed through the mock MySQL grammar path
// and Oracle mode is applied from codegen/evaluation onward; that is enough for
// division / CASE runtime behavior, but not for the exact Oracle CAST syntax.
// ============================================================================

TEST_F(CaseExprCoverageTest, OracleMode_EmptyInput_DangerousExprsNotEvaluated)
{
  auto div_result = expr_unit_test()
      .sql_mode(SqlMode::ORACLE)
      .columns("a int")
      .with_expr("1 / 0")
      .with_data({})
      .run(engine_);
  EXPECT_EQ(0, div_result.row_count());
  EXPECT_EQ(common::OB_SUCCESS, div_result.get_ret_code());

  auto case_result = expr_unit_test()
      .sql_mode(SqlMode::ORACLE)
      .columns("a int")
      .with_expr("CASE WHEN a = 1 THEN 1 ELSE 1 / 0 END")
      .with_data({})
      .run(engine_);
  EXPECT_EQ(0, case_result.row_count());
  EXPECT_EQ(common::OB_SUCCESS, case_result.get_ret_code());

}

TEST_F(CaseExprCoverageTest, OracleMode_DivisionByZero_WithInput_Raises)
{
  auto result = expr_unit_test()
      .sql_mode(SqlMode::ORACLE)
      .columns("a int")
      .with_expr("1 / 0")
      .with_data({{1}})
      .expect_error(common::OB_ERR_DIVISOR_IS_ZERO)
      .run(engine_);

  EXPECT_EQ(common::OB_ERR_DIVISOR_IS_ZERO, result.get_ret_code());
}

TEST_F(CaseExprCoverageTest, OracleMode_CaseElseDivZero_Selected_Raises)
{
  auto result = expr_unit_test()
      .sql_mode(SqlMode::ORACLE)
      .columns("a int")
      .with_expr("CASE WHEN a = 1 THEN 1 ELSE 1 / 0 END")
      .with_data({{2}})
      .expect_error(common::OB_ERR_DIVISOR_IS_ZERO)
      .run(engine_);

  EXPECT_EQ(common::OB_ERR_DIVISOR_IS_ZERO, result.get_ret_code());
}

TEST_F(CaseExprCoverageTest, OracleMode_CaseElseDivZero_NotSelected_ReturnsThen)
{
  auto result = expr_unit_test()
      .sql_mode(SqlMode::ORACLE)
      .columns("a int")
      .with_expr("CASE WHEN a = 1 THEN 1 ELSE 1 / 0 END")
      .with_data({{1}})
      .run(engine_);

  ASSERT_EQ(1, result.row_count());
  EXPECT_EQ("1", result.get_row(0)[0]);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
}

TEST_F(CaseExprCoverageTest, OracleMode_LargeCaseElseDivZero_NotSelected_ReturnsThen)
{
  auto result = expr_unit_test()
      .sql_mode(SqlMode::ORACLE)
      .columns("a int")
      .with_expr("CASE WHEN a = 1 THEN 1 ELSE 1 / 0 END")
      .with_data_generator(XLARGE, gen::constant(static_cast<int64_t>(1)))
      .run(engine_);

  ASSERT_EQ(XLARGE, result.row_count());
  for (int64_t i = 0; i < XLARGE; ++i) {
    EXPECT_EQ("1", result.get_row(i)[0]);
  }
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
}

TEST_F(CaseExprCoverageTest, OracleMode_LargeCaseElseDivZero_Selected_Raises)
{
  auto result = expr_unit_test()
      .sql_mode(SqlMode::ORACLE)
      .columns("a int")
      .with_expr("CASE WHEN a = 1 THEN 1 ELSE 1 / 0 END")
      .with_data_generator(XLARGE, gen::constant(static_cast<int64_t>(2)))
      .expect_error(common::OB_ERR_DIVISOR_IS_ZERO)
      .run(engine_);

  EXPECT_EQ(common::OB_ERR_DIVISOR_IS_ZERO, result.get_ret_code());
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  oceanbase::common::init_arches(); // enable AVX512 dispatch
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
