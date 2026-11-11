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

#include "sql/test_sql_utils.h"
#include "sql/ob_sql_init.h"
#include "sql/engine/expr/ob_expr_between.h"

class TestWindowRangeType : public test::TestSqlUtils, public ::testing::Test
{
protected:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
    ASSERT_EQ(OB_SUCCESS, session_.test_init(0, 0, 0, NULL));
    ASSERT_EQ(OB_SUCCESS, session_.load_default_sys_variable(false, true));
    raw_expr_.set_expr_type(T_OP_BTW);
  }

  void check_bounds(const int64_t param_num, const bool is_bound_left, const int16_t precision, const int16_t scale)
  {
    SCOPED_TRACE(::testing::Message() << "params=" << param_num << " left=" << is_bound_left
                                      << " precision=" << precision << " scale=" << scale);
    ObExprResType types[3];
    for (int64_t i = 0; i < param_num; ++i) {
      types[i].set_type(ObDecimalIntType);
      types[i].set_precision(i == 0 ? precision : precision + 2);
      types[i].set_scale(i == 0 ? scale : scale + 2);
    }
    ObExprBetween op(allocator_);
    op.set_raw_expr(&raw_expr_);
    ObExprTypeCtx type_ctx;
    type_ctx.set_session(&session_);
    type_ctx.set_raw_expr(&raw_expr_);
    ObExprResType result;
    bool need_no_cast = true;
    ASSERT_EQ(OB_SUCCESS, op.get_cmp_result_type3(result, need_no_cast, types, param_num, is_bound_left, type_ctx));
    ASSERT_FALSE(need_no_cast);
    EXPECT_EQ(0, types[0].get_cast_mode());
    for (int64_t i = 1; i < param_num; ++i) {
      const bool is_left = i == 1 ? is_bound_left : !is_bound_left;
      const uint64_t expected = is_left ? CM_CONST_TO_DECIMAL_INT_UP : CM_CONST_TO_DECIMAL_INT_DOWN;
      EXPECT_EQ(ObDecimalIntType, types[i].get_calc_type());
      EXPECT_EQ(precision, types[i].get_calc_accuracy().get_precision());
      EXPECT_EQ(scale, types[i].get_calc_scale());
      EXPECT_EQ(expected, types[i].get_cast_mode());
    }
  }

  ObSQLSessionInfo session_;
  ObOpRawExpr raw_expr_;
};

TEST_F(TestWindowRangeType, single_bound_rounding)
{
  const int16_t precisions[] = {9, 18, 38, 63};
  for (const int16_t precision : precisions) {
    check_bounds(2, true, precision, 0);
    check_bounds(2, false, precision, 0);
    check_bounds(2, true, precision, 2);
    check_bounds(2, false, precision, 2);
  }
}

TEST_F(TestWindowRangeType, double_bound_preserves_frame_slots)
{
  const int16_t precisions[] = {9, 18, 38, 63};
  for (const int16_t precision : precisions) {
    check_bounds(3, true, precision, 0);
    check_bounds(3, false, precision, 0);
    check_bounds(3, true, precision, 2);
    check_bounds(3, false, precision, 2);
  }
}

TEST_F(TestWindowRangeType, equal_accuracy_needs_no_cast)
{
  ObExprBetween op(allocator_);
  op.set_raw_expr(&raw_expr_);
  ObExprTypeCtx type_ctx;
  type_ctx.set_session(&session_);
  type_ctx.set_raw_expr(&raw_expr_);
  for (int64_t param_num = 2; param_num <= 3; ++param_num) {
    for (const bool is_bound_left : {false, true}) {
      ObExprResType types[3];
      for (int64_t i = 0; i < param_num; ++i) {
        types[i].set_type(ObDecimalIntType);
        types[i].set_precision(38);
        types[i].set_scale(2);
      }
      ObExprResType result;
      bool need_no_cast = false;
      ASSERT_EQ(OB_SUCCESS, op.get_cmp_result_type3(result, need_no_cast, types, param_num, is_bound_left, type_ctx));
      EXPECT_TRUE(need_no_cast);
      for (int64_t i = 0; i < param_num; ++i) { EXPECT_EQ(0, types[i].get_cast_mode()); }
    }
  }
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_window_range_type.log", true);
  OB_LOGGER.set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
