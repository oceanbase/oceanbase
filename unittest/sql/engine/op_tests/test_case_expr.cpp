/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "sql/engine/expr/ob_expr_case.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace oceanbase::sql;

class CaseExprOpTest : public OpTestKit {};

// Helpers for bound.start() != 0 tests
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
// TC1: NonZeroBoundStart_0 - bound.start() == 0
// ============================================================================
TEST_F(CaseExprOpTest, NonZeroBoundStart_0)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 8 = 0 THEN 0 WHEN a % 8 = 1 THEN 1 WHEN a % 8 = 2 THEN 2 WHEN a % 8 = 3 THEN 3 WHEN a % 8 = 4 THEN 4 WHEN a % 8 = 5 THEN 5 WHEN a % 8 = 6 THEN 6 WHEN a % 8 = 7 THEN 7 ELSE -1 END")
      .with_expr_eval_vector_func(eval_case_start<0>)
      .with_input_skips([](int64_t, int64_t, ObBitVector *skip) {
        for (int64_t i = 0; i < 0; ++i) { skip->set(i); }
      })
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(256, result.row_count());
  EXPECT_TRUE(result.verify_ordered(256, [](int64_t i) -> TestValue {
    return i % 8;
  }));
}

// ============================================================================
// TC2: NonZeroBoundStart_1 - bound.start() == 1
// ============================================================================
TEST_F(CaseExprOpTest, NonZeroBoundStart_1)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 8 = 0 THEN 0 WHEN a % 8 = 1 THEN 1 WHEN a % 8 = 2 THEN 2 WHEN a % 8 = 3 THEN 3 WHEN a % 8 = 4 THEN 4 WHEN a % 8 = 5 THEN 5 WHEN a % 8 = 6 THEN 6 WHEN a % 8 = 7 THEN 7 ELSE -1 END")
      .with_expr_eval_vector_func(eval_case_start<1>)
      .with_input_skips([](int64_t, int64_t, ObBitVector *skip) {
        for (int64_t i = 0; i < 1; ++i) { skip->set(i); }
      })
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(255, result.row_count());
  EXPECT_TRUE(result.verify_ordered(255, [](int64_t i) -> TestValue {
    return (i + 1) % 8;
  }));
}

// ============================================================================
// TC3: NonZeroBoundStart_2 - bound.start() == 2
// ============================================================================
TEST_F(CaseExprOpTest, NonZeroBoundStart_2)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 8 = 0 THEN 0 WHEN a % 8 = 1 THEN 1 WHEN a % 8 = 2 THEN 2 WHEN a % 8 = 3 THEN 3 WHEN a % 8 = 4 THEN 4 WHEN a % 8 = 5 THEN 5 WHEN a % 8 = 6 THEN 6 WHEN a % 8 = 7 THEN 7 ELSE -1 END")
      .with_expr_eval_vector_func(eval_case_start<2>)
      .with_input_skips([](int64_t, int64_t, ObBitVector *skip) {
        for (int64_t i = 0; i < 2; ++i) { skip->set(i); }
      })
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(254, result.row_count());
  EXPECT_TRUE(result.verify_ordered(254, [](int64_t i) -> TestValue {
    return (i + 2) % 8;
  }));
}

// ============================================================================
// TC4: NonZeroBoundStart_3 - bound.start() == 3
// ============================================================================
TEST_F(CaseExprOpTest, NonZeroBoundStart_3)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 8 = 0 THEN 0 WHEN a % 8 = 1 THEN 1 WHEN a % 8 = 2 THEN 2 WHEN a % 8 = 3 THEN 3 WHEN a % 8 = 4 THEN 4 WHEN a % 8 = 5 THEN 5 WHEN a % 8 = 6 THEN 6 WHEN a % 8 = 7 THEN 7 ELSE -1 END")
      .with_expr_eval_vector_func(eval_case_start<3>)
      .with_input_skips([](int64_t, int64_t, ObBitVector *skip) {
        for (int64_t i = 0; i < 3; ++i) { skip->set(i); }
      })
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(253, result.row_count());
  EXPECT_TRUE(result.verify_ordered(253, [](int64_t i) -> TestValue {
    return (i + 3) % 8;
  }));
}

// ============================================================================
// TC5: NonZeroBoundStart_4 - bound.start() == 4
// ============================================================================
TEST_F(CaseExprOpTest, NonZeroBoundStart_4)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 8 = 0 THEN 0 WHEN a % 8 = 1 THEN 1 WHEN a % 8 = 2 THEN 2 WHEN a % 8 = 3 THEN 3 WHEN a % 8 = 4 THEN 4 WHEN a % 8 = 5 THEN 5 WHEN a % 8 = 6 THEN 6 WHEN a % 8 = 7 THEN 7 ELSE -1 END")
      .with_expr_eval_vector_func(eval_case_start<4>)
      .with_input_skips([](int64_t, int64_t, ObBitVector *skip) {
        for (int64_t i = 0; i < 4; ++i) { skip->set(i); }
      })
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(252, result.row_count());
  EXPECT_TRUE(result.verify_ordered(252, [](int64_t i) -> TestValue {
    return (i + 4) % 8;
  }));
}

// ============================================================================
// TC6: NonZeroBoundStart_5 - bound.start() == 5
// ============================================================================
TEST_F(CaseExprOpTest, NonZeroBoundStart_5)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 8 = 0 THEN 0 WHEN a % 8 = 1 THEN 1 WHEN a % 8 = 2 THEN 2 WHEN a % 8 = 3 THEN 3 WHEN a % 8 = 4 THEN 4 WHEN a % 8 = 5 THEN 5 WHEN a % 8 = 6 THEN 6 WHEN a % 8 = 7 THEN 7 ELSE -1 END")
      .with_expr_eval_vector_func(eval_case_start<5>)
      .with_input_skips([](int64_t, int64_t, ObBitVector *skip) {
        for (int64_t i = 0; i < 5; ++i) { skip->set(i); }
      })
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(251, result.row_count());
  EXPECT_TRUE(result.verify_ordered(251, [](int64_t i) -> TestValue {
    return (i + 5) % 8;
  }));
}

// ============================================================================
// TC7: NonZeroBoundStart_6 - bound.start() == 6
// ============================================================================
TEST_F(CaseExprOpTest, NonZeroBoundStart_6)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 8 = 0 THEN 0 WHEN a % 8 = 1 THEN 1 WHEN a % 8 = 2 THEN 2 WHEN a % 8 = 3 THEN 3 WHEN a % 8 = 4 THEN 4 WHEN a % 8 = 5 THEN 5 WHEN a % 8 = 6 THEN 6 WHEN a % 8 = 7 THEN 7 ELSE -1 END")
      .with_expr_eval_vector_func(eval_case_start<6>)
      .with_input_skips([](int64_t, int64_t, ObBitVector *skip) {
        for (int64_t i = 0; i < 6; ++i) { skip->set(i); }
      })
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(250, result.row_count());
  EXPECT_TRUE(result.verify_ordered(250, [](int64_t i) -> TestValue {
    return (i + 6) % 8;
  }));
}

// ============================================================================
// TC8: NonZeroBoundStart_7 - bound.start() == 7
// ============================================================================
TEST_F(CaseExprOpTest, NonZeroBoundStart_7)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 8 = 0 THEN 0 WHEN a % 8 = 1 THEN 1 WHEN a % 8 = 2 THEN 2 WHEN a % 8 = 3 THEN 3 WHEN a % 8 = 4 THEN 4 WHEN a % 8 = 5 THEN 5 WHEN a % 8 = 6 THEN 6 WHEN a % 8 = 7 THEN 7 ELSE -1 END")
      .with_expr_eval_vector_func(eval_case_start<7>)
      .with_input_skips([](int64_t, int64_t, ObBitVector *skip) {
        for (int64_t i = 0; i < 7; ++i) { skip->set(i); }
      })
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(249, result.row_count());
  EXPECT_TRUE(result.verify_ordered(249, [](int64_t i) -> TestValue {
    return (i + 7) % 8;
  }));
}

// ============================================================================
// TC9: NonZeroBoundStart_8 - bound.start() == 8
// ============================================================================
TEST_F(CaseExprOpTest, NonZeroBoundStart_8)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 8 = 0 THEN 0 WHEN a % 8 = 1 THEN 1 WHEN a % 8 = 2 THEN 2 WHEN a % 8 = 3 THEN 3 WHEN a % 8 = 4 THEN 4 WHEN a % 8 = 5 THEN 5 WHEN a % 8 = 6 THEN 6 WHEN a % 8 = 7 THEN 7 ELSE -1 END")
      .with_expr_eval_vector_func(eval_case_start<8>)
      .with_input_skips([](int64_t, int64_t, ObBitVector *skip) {
        for (int64_t i = 0; i < 8; ++i) { skip->set(i); }
      })
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(248, result.row_count());
  EXPECT_TRUE(result.verify_ordered(248, [](int64_t i) -> TestValue {
    return (i + 8) % 8;
  }));
}

// ============================================================================
// Optimization path: const-null else (commit ea1372bbf)
// `CASE WHEN ... ELSE NULL END` should:
//  1. route unmatched rows to set_null (then_expr_idx = -1)
//  2. exclude const-null else from check_then_expr_same_type so the homogeneous
//     then-expressions can take the specialized set_res_vec fast path.
// ============================================================================

// TC10: All when branches unmatched -> all rows go through the const-null else set_null path
TEST_F(CaseExprOpTest, ConstNullElse_AllUnmatched)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a < 0 THEN 100 WHEN a > 1000 THEN 200 ELSE NULL END")
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(256, result.row_count());
  EXPECT_TRUE(result.verify_ordered(256, [](int64_t) -> TestValue {
    return TestValue::null();
  }));
}

// TC11: Partial match; the rest take the const-null else -> set_null path
TEST_F(CaseExprOpTest, ConstNullElse_PartialMatch)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 4 = 0 THEN 10 WHEN a % 4 = 1 THEN 11 ELSE NULL END")
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(256, result.row_count());
  EXPECT_TRUE(result.verify_ordered(256, [](int64_t i) -> TestValue {
    int64_t m = i % 4;
    if (m == 0) return TestValue(static_cast<int64_t>(10));
    if (m == 1) return TestValue(static_cast<int64_t>(11));
    return TestValue::null();
  }));
}

// TC12: Homogeneous then exprs + const-null else; then exprs read the same column,
// verifying the specialized set_res_vec fast path. All then exprs use column `a`
// (same type); ELSE NULL must not block the fast path.
TEST_F(CaseExprOpTest, ConstNullElse_HomogeneousThenSameColumn)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 3 = 0 THEN a WHEN a % 3 = 1 THEN a ELSE NULL END")
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(256, result.row_count());
  EXPECT_TRUE(result.verify_ordered(256, [](int64_t i) -> TestValue {
    int64_t m = i % 3;
    if (m == 2) return TestValue::null();
    return TestValue(i);
  }));
}

// TC13: All matched -> trailing else is never evaluated, but the optimization
// path still marks else_is_null_const.
TEST_F(CaseExprOpTest, ConstNullElse_AllMatched)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 2 = 0 THEN 0 WHEN a % 2 = 1 THEN 1 ELSE NULL END")
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(256, result.row_count());
  EXPECT_TRUE(result.verify_ordered(256, [](int64_t i) -> TestValue {
    return i % 2;
  }));
}

// TC14: bound.start() != 0 combined with const-null else, verifying mask and
// set_null boundary handling.
TEST_F(CaseExprOpTest, ConstNullElse_NonZeroBoundStart)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 4 = 0 THEN 10 WHEN a % 4 = 1 THEN 11 ELSE NULL END")
      .with_expr_eval_vector_func(eval_case_start<3>)
      .with_input_skips([](int64_t, int64_t, ObBitVector *skip) {
        for (int64_t i = 0; i < 3; ++i) { skip->set(i); }
      })
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(253, result.row_count());
  EXPECT_TRUE(result.verify_ordered(253, [](int64_t i) -> TestValue {
    int64_t a = i + 3;
    int64_t m = a % 4;
    if (m == 0) return TestValue(static_cast<int64_t>(10));
    if (m == 1) return TestValue(static_cast<int64_t>(11));
    return TestValue::null();
  }));
}

// TC15: Baseline - non-null const else (constant -1); should take the original
// else evaluation path.
TEST_F(CaseExprOpTest, NonNullConstElse_Baseline)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 4 = 0 THEN 10 WHEN a % 4 = 1 THEN 11 ELSE -1 END")
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(256, result.row_count());
  EXPECT_TRUE(result.verify_ordered(256, [](int64_t i) -> TestValue {
    int64_t m = i % 4;
    if (m == 0) return TestValue(static_cast<int64_t>(10));
    if (m == 1) return TestValue(static_cast<int64_t>(11));
    return TestValue(static_cast<int64_t>(-1));
  }));
}

// TC16: Baseline - no else clause (existing logic already takes set_null);
// used for regression comparison.
TEST_F(CaseExprOpTest, NoElse_Baseline)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("CASE WHEN a % 4 = 0 THEN 10 WHEN a % 4 = 1 THEN 11 END")
      .with_data_generator(256, gen::sequential(0, 1))
      .run(engine_);

  EXPECT_EQ(256, result.row_count());
  EXPECT_TRUE(result.verify_ordered(256, [](int64_t i) -> TestValue {
    int64_t m = i % 4;
    if (m == 0) return TestValue(static_cast<int64_t>(10));
    if (m == 1) return TestValue(static_cast<int64_t>(11));
    return TestValue::null();
  }));
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  oceanbase::common::init_arches(); // enable AVX512
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
