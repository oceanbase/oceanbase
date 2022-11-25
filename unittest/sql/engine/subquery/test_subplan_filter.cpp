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

#include <gtest/gtest.h>

#define private  public
#define protected  public
#include "sql/ob_sql_init.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/subquery/ob_subplan_filter.h"
#include "sql/engine/table/ob_fake_table.h"
#include "sql/engine/expr/ob_expr_subquery_ref.h"
#include "sql/engine/basic/ob_limit.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_server.h"
#include "observer/ob_server_struct.h"
#include "share/ob_tenant_mgr.h"
#include "sql/engine/test_engine_util.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::observer;
class ObSubPlanFilterTest: public ::testing::Test
{
public:
  ObSubPlanFilterTest();
  virtual ~ObSubPlanFilterTest();
  virtual void SetUp();
  virtual void TearDown();

  void add_filter_expr();
  void add_calc_expr();
  void add_limit();
  void init_plan(ObExecContext &ctx);
  void init_plan_for_calc(ObExecContext &ctx);
protected:
  ObFakeTable fake_table1_;
  ObFakeTable fake_table2_;
  ObLimit limit_;
  int32_t projector_;
  ObSubPlanFilter subplan_filter_;
  ObPhysicalPlan physical_plan_;
private:
  // disallow copy
  ObSubPlanFilterTest(const ObSubPlanFilterTest &other);
  ObSubPlanFilterTest& operator=(const ObSubPlanFilterTest &other);
private:
  // data members
};

ObSubPlanFilterTest::ObSubPlanFilterTest() : limit_(alloc_), subplan_filter_(alloc_)
{
  projector_ = 1;
}

ObSubPlanFilterTest::~ObSubPlanFilterTest()
{
}

void ObSubPlanFilterTest::SetUp()
{
}

void ObSubPlanFilterTest::TearDown()
{
}

void ObSubPlanFilterTest::add_filter_expr()
{
  ObSqlExpression *sql_expr = NULL;
  ObPostExprItem expr_item;
  //column 0
  ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, sql_expr));
  sql_expr->set_item_count(3);
  expr_item.set_column(0);
  sql_expr->add_expr_item(expr_item);
  //subquery ref op
  ObExprSubQueryRef *subquery_ref = NULL;
  //static_cast<ObExprSubQueryRef*>(ObExprOperator::alloc(T_REF_QUERY));
  ObExprOperator *op = NULL;
  physical_plan_.get_expr_op_factory().alloc(static_cast<ObExprOperatorType>(T_REF_QUERY), op);
  subquery_ref = static_cast<ObExprSubQueryRef*>(op);
  subquery_ref->set_real_param_num(0);
  subquery_ref->set_result_is_scalar(true);
  subquery_ref->set_result_type(ObInt32Type);
  subquery_ref->set_scalar_result_type(subquery_ref->get_result_type());
  const_cast<ObExprResType*>(&subquery_ref->get_result_type())->init_row_dimension(10);
  subquery_ref->set_subquery_idx(0);
  expr_item.assign(subquery_ref);
  sql_expr->add_expr_item(expr_item);
  //greater than
  ObExprOperator *greater_op = NULL;
  //ObExprOperator::alloc(T_OP_GT);
  physical_plan_.get_expr_op_factory().alloc(T_OP_GT, greater_op);
  greater_op->set_result_type(ObTinyIntType);
  greater_op->set_real_param_num(2);
  greater_op->set_row_dimension(-1);
  expr_item.assign(greater_op);
  sql_expr->add_expr_item(expr_item);
  ObExprOperator *expr_op = expr_item.get_expr_operator();
  ObExprResType type;
  type.set_type(ObIntType);
  type.set_calc_type(ObIntType);
  expr_op->set_result_type(type);
  ASSERT_EQ(OB_SUCCESS, subplan_filter_.add_filter(sql_expr));
}

void ObSubPlanFilterTest::add_calc_expr()
{
  ObColumnExpression *column_expr = NULL;
  ObPostExprItem expr_item;
  ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, column_expr));
  column_expr->set_item_count(6);
  column_expr->set_result_index(1);
  //subquery ref op
  //ObExprSubQueryRef *subquery_ref = static_cast<ObExprSubQueryRef*>(ObExprOperator::alloc(T_REF_QUERY));
  //ObExprSubQueryRef *subquery_ref = NULL;
  ObExprOperator *op = NULL;
  physical_plan_.get_expr_op_factory().alloc(static_cast<ObExprOperatorType>(T_REF_QUERY), op);
  ObExprSubQueryRef *subquery_ref = static_cast<ObExprSubQueryRef*>(op);
  subquery_ref->set_real_param_num(0);
  subquery_ref->set_result_is_scalar(false);
  subquery_ref->set_result_type(ObNullType);
  const_cast<ObExprResType*>(&subquery_ref->get_result_type())->init_row_dimension(10);
  subquery_ref->set_scalar_result_type(subquery_ref->get_result_type());
  subquery_ref->set_subquery_idx(0);
  expr_item.assign(subquery_ref);
  column_expr->add_expr_item(expr_item);
  //row(1, 2, 3)
  ObObj val;
  val.set_int(20);
  expr_item.assign(val);
  column_expr->add_expr_item(expr_item);
  val.set_int(21);
  expr_item.assign(val);
  column_expr->add_expr_item(expr_item);
  val.set_int(22);
  expr_item.assign(val);
  column_expr->add_expr_item(expr_item);
  //subquery equal
  ObSubQueryRelationalExpr *equal_op = NULL;
  //static_cast<ObSubQueryRelationalExpr*>(ObExprOperator::alloc(T_OP_SQ_EQ));
  physical_plan_.get_expr_op_factory().alloc(T_OP_SQ_EQ, op);
  equal_op = static_cast<ObSubQueryRelationalExpr*>(op);
  ObExprCalcType calc_type;
  calc_type.set_tinyint();
  ObExprResType result_type;
  ObArenaAllocator alloc;
  result_type.set_allocator(&alloc);
  result_type.init_row_dimension(10);
  result_type.get_row_calc_cmp_types().push_back(calc_type);
  result_type.get_row_calc_cmp_types().push_back(calc_type);
  result_type.get_row_calc_cmp_types().push_back(calc_type);
  equal_op->set_result_type(result_type);
  equal_op->set_result_type(ObTinyIntType);
  equal_op->set_real_param_num(4);
  equal_op->set_row_dimension(-1);
  equal_op->set_left_is_iter(true);
  equal_op->set_right_is_iter(false);
  expr_item.assign(equal_op);
  column_expr->add_expr_item(expr_item);
  ASSERT_EQ(OB_SUCCESS, subplan_filter_.add_compute(column_expr));
}

void ObSubPlanFilterTest::add_limit()
{
  limit_.set_phy_plan(&physical_plan_);
  ObColumnExpression *col_expr = NULL;
  ObPostExprItem expr_item;
  ObObj val;
  ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, col_expr));
  col_expr->set_item_count(2);
  val.set_int(1);
  expr_item.assign(val);
  col_expr->add_expr_item(expr_item);
  col_expr->set_result_index(1);
  ASSERT_EQ(OB_SUCCESS, limit_.add_compute(col_expr));
  ObSqlExpression *sql_expr = NULL;
  ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, sql_expr));
  sql_expr->set_item_count(1);
  sql_expr->add_expr_item(expr_item);
  limit_.set_limit(sql_expr, NULL, NULL);
  limit_.set_column_count(2);
  limit_.set_projector(&projector_, 1);
  limit_.set_id(3);
  limit_.set_child(0, fake_table2_);
}

void ObSubPlanFilterTest::init_plan(ObExecContext &ctx)
{
  fake_table1_.set_column_count(2);
  fake_table2_.set_column_count(1);
  subplan_filter_.set_column_count(2);

  fake_table1_.set_id(0);
  fake_table2_.set_id(1);
  subplan_filter_.set_id(2);

  fake_table1_.set_phy_plan(&physical_plan_);
  fake_table2_.set_phy_plan(&physical_plan_);
  subplan_filter_.set_phy_plan(&physical_plan_);
  ASSERT_EQ(OB_SUCCESS, subplan_filter_.create_child_array(2));
  subplan_filter_.set_child(0, fake_table1_);
  subplan_filter_.set_child(1, limit_);
  subplan_filter_.init_px_batch_rescan_flags(2);
  subplan_filter_.get_px_batch_rescan_flags().push_back(false);
  subplan_filter_.get_px_batch_rescan_flags().push_back(false);
  add_filter_expr();
  add_limit();
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(4));
  ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());
  ASSERT_FALSE(NULL == ctx.get_physical_plan_ctx());
  ASSERT_FALSE(NULL ==  ctx.get_my_session());
  ASSERT_EQ(OB_SUCCESS, ctx.get_my_session()->set_time_zone(ObString("+8:00"), true, true));
  ctx.get_physical_plan_ctx()->set_phy_plan(&physical_plan_);
}

void ObSubPlanFilterTest::init_plan_for_calc(ObExecContext &ctx)
{
  fake_table1_.reset();
  fake_table2_.reset();
  subplan_filter_.reset();

  fake_table1_.set_column_count(1);
  fake_table2_.set_column_count(3);
  subplan_filter_.set_column_count(2);

  fake_table1_.set_id(0);
  fake_table2_.set_id(1);
  subplan_filter_.set_id(2);

  fake_table1_.set_phy_plan(&physical_plan_);
  fake_table2_.set_phy_plan(&physical_plan_);
  subplan_filter_.set_phy_plan(&physical_plan_);

  ASSERT_EQ(OB_SUCCESS, subplan_filter_.create_child_array(2));
  subplan_filter_.set_child(0, fake_table1_);
  subplan_filter_.set_child(1, fake_table2_);

  subplan_filter_.init_px_batch_rescan_flags(2);
  subplan_filter_.get_px_batch_rescan_flags().push_back(false);
  subplan_filter_.get_px_batch_rescan_flags().push_back(false);
  add_calc_expr();
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(3));
  ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());
  ASSERT_FALSE(NULL == ctx.get_physical_plan_ctx());
  ASSERT_FALSE(NULL == ctx.get_my_session());
  ASSERT_EQ(OB_SUCCESS, ctx.get_my_session()->set_time_zone(ObString("+8:00"), true, true));
  ctx.get_physical_plan_ctx()->set_phy_plan(&physical_plan_);
  //add projector
  int32_t *projector = NULL;
  projector = static_cast<int32_t*>(ctx.get_allocator().alloc(sizeof(int32_t)));
  ASSERT_FALSE(NULL == projector);
  projector[0] = 1;
  subplan_filter_.set_projector(projector, 1);
}

TEST_F(ObSubPlanFilterTest, test_subplan_filter)
{
  int ret = OB_SUCCESS;
  const ObNewRow *row = NULL;
  ObExecContext ctx;
  init_plan(ctx);

  ADD_ROW(fake_table1_, COL(1), COL(1));
  ADD_ROW(fake_table1_, COL(2), COL(2));
  ADD_ROW(fake_table2_, COL(1));
  ADD_ROW(fake_table2_, COL(2));
  ASSERT_EQ(OB_SUCCESS, subplan_filter_.open(ctx));
  ASSERT_EQ(OB_SUCCESS, subplan_filter_.get_next_row(ctx, row));
  SQL_ENG_LOG(INFO, "row result", K(*row));
}

//select (select 1, 2, 3)=ROW(1, 2, 3)
TEST_F(ObSubPlanFilterTest, test_subplan_filter2)
{
  int ret = OB_SUCCESS;
  const ObNewRow *row = NULL;
  ObExecContext ctx;
  init_plan_for_calc(ctx);

  ADD_ROW(fake_table1_, COL(10));
  ADD_ROW(fake_table2_, COL(20), COL(21), COL(22));
  ASSERT_EQ(OB_SUCCESS, subplan_filter_.open(ctx));
  ASSERT_EQ(OB_SUCCESS, subplan_filter_.get_next_row(ctx, row));
  SQL_ENG_LOG(INFO, "row result", K(*row));
  ASSERT_EQ(OB_SUCCESS, subplan_filter_.close(ctx));
}

int main(int argc, char **argv)
{
  init_global_memory_pool();
  init_sql_factories();
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  OB_LOGGER.disable();
  return ret;
}
