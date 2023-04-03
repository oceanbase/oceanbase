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

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#define private public
#define protected public
#include "ob_expr_test_kit.h"
#include "sql/engine/expr/ob_expr_util.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::common::number;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::observer;

class TestExpr : public ::testing::Test, public ObExprTestKit
{
  virtual void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, init());
  }

  virtual void TearDown() override
  {
    destroy();
  }
};

int eval_num_add(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObDatum *l = NULL;
  ObDatum *r = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, l))) {
    LOG_WARN("left eval failed", K(ret));
  } else if (l->null_) {
    datum.set_null();
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, r))) {
    LOG_WARN("right eval failed", K(ret));
  } else if (r->null_) {
    datum.set_null();
  } else {
    ObNumber lnum(l->num_->desc_.desc_, (uint32_t *)l->num_->digits_);
    ObNumber rnum(r->num_->desc_.desc_, (uint32_t *)r->num_->digits_);
    ObNumber add;
	ObNumStackOnceAlloc tmp_alloc;
    if (OB_FAIL(lnum.add(rnum, add, tmp_alloc))) {
      LOG_WARN("add number failed", K(ret));
    } else {
      datum.set_number(add);
    }
  }
  return ret;
}

int eval_int_less(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObDatum *l = NULL;
  ObDatum *r = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, l))) {
    LOG_WARN("left eval failed", K(ret));
  } else if (l->null_) {
    datum.set_null();
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, r))) {
    LOG_WARN("right eval failed", K(ret));
  } else if (r->null_) {
    datum.set_null();
  } else {
    datum.set_int(*l->int_ < *r->int_);
  }
  return ret;
}

TEST(Expr, test_print_func_ptr)
{
  // test print function pointer
  auto f = &eval_num_add;
  LOG_INFO("add func", K(f));
  char buf[1024];
  int64_t pos = 0;
  databuff_print_obj(buf, 1024, pos, f);
}

TEST_F(TestExpr, number_add)
{
  ObRawExpr *raw_expr = NULL;
  ObSqlExpression old_expr(allocator_);
  ObExpr *expr;
  ObNumber add_nums[2];
  ASSERT_EQ(OB_SUCCESS, add_nums[0].from("3.5", 3, allocator_));
  ASSERT_EQ(OB_SUCCESS, add_nums[1].from("2.8", 3, allocator_));
  ObObj add_args[2];
  add_args[0].set_number(add_nums[0]);
  add_args[1].set_number(add_nums[1]);
  ASSERT_EQ(OB_SUCCESS, create_expr("1 + 1", add_args, 2, raw_expr, old_expr, expr));
  expr->eval_func_ = &eval_num_add;

  ObNewRow row;
  ObObj result;

  ASSERT_EQ(OB_SUCCESS, old_expr.calc(expr_ctx_, row, result));
  LOG_INFO("old expr result", K(result));

  int64_t TEST_CNT = 1000000;
  ASSERT_EQ(OB_SUCCESS, timed_execute( "old add expr", TEST_CNT,
          [&] {
            // calc_buf_.reset_remain_one_page();
            return old_expr.calc(expr_ctx_, row, result);
          }));


  ObDatum *datum  = NULL;
  ASSERT_EQ(OB_SUCCESS, expr->eval(eval_ctx_, datum));
  LOG_INFO("new get result", K(*datum), K(DATUM2STR(*expr, *datum)));

  ASSERT_EQ(OB_SUCCESS, timed_execute(
          "new add expr", TEST_CNT,
          [&] {
            // calc_buf_.reset_remain_one_page();
            expr->get_eval_info(eval_ctx_).evaluated_ = false;
            return expr->eval(eval_ctx_, datum);
          }));
}

TEST_F(TestExpr, int_less)
{
  ObRawExpr *raw_expr = NULL;
  ObSqlExpression old_expr(allocator_);
  ObExpr *expr;
  ObObj add_args[2];
  add_args[0].set_int(512);
  add_args[1].set_int(1024);
  ASSERT_EQ(OB_SUCCESS, create_expr("1 < 1", add_args, 2, raw_expr, old_expr, expr));
  expr->eval_func_ = &eval_int_less;

  ObNewRow row;
  ObObj result;

  ASSERT_EQ(OB_SUCCESS, old_expr.calc(expr_ctx_, row, result));
  LOG_INFO("old expr result", K(result));

  int64_t TEST_CNT = 1000000;
  ASSERT_EQ(OB_SUCCESS, timed_execute( "old less expr", TEST_CNT,
          [&] {
            return old_expr.calc(expr_ctx_, row, result);
          }));


  ObDatum *datum  = NULL;
  ASSERT_EQ(OB_SUCCESS, expr->eval(eval_ctx_, datum));
  LOG_INFO("new get result", K(*datum), K(DATUM2STR(*expr, *datum)));

  ASSERT_EQ(OB_SUCCESS, timed_execute(
          "new less expr", TEST_CNT,
          [&] {
            expr->get_eval_info(eval_ctx_).evaluated_ = false;
            return expr->eval(eval_ctx_, datum);
          }));
}


// TODO bin.lb: test serialize && deserialize

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_expr.log", true);
  return RUN_ALL_TESTS();
}
