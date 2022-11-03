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

#ifndef OCEANBASE_UNITTEST_SQL_AGGREGATE_TEST_UTILS_H_
#define OCEANBASE_UNITTEST_SQL_AGGREGATE_TEST_UTILS_H_

#ifndef private
#define private public
#endif
#ifndef protected
#define protected public
#endif

#include <gtest/gtest.h>
#include "sql/engine/table/ob_fake_table.h"
#include "sql/engine/aggregate/ob_groupby.h"
#include "sql/engine/aggregate/ob_scalar_aggregate.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/worker.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/ob_server.h"

using namespace oceanbase::share;
using namespace oceanbase::omt;
class TestAggregateFactory
{
public:
  TestAggregateFactory() {}
  ~TestAggregateFactory() {}

  static void init(ObExecContext &ctx,
                   ObGroupBy &groupby_op,
                   int64_t col_count,
                   bool is_distinct,
                   bool is_number = true,
                   ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN,
                   ObCollationType group_cs_type = CS_TYPE_UTF8MB4_BIN)
  {
    ObPhysicalPlanCtx *plan_ctx = NULL;
    groupby_op.reset();
    groupby_op.reuse();
    fake_table_.reset();
    fake_table_.reuse();
    result_table_.reset();
    result_table_.reuse();
    physical_plan_.reset();
    OBSERVER.init_schema();
    OBSERVER.init_tz_info_mgr();
    ASSERT_EQ(OB_SUCCESS, ObTenantConfigMgr::get_instance().add_tenant_config(1));
    ASSERT_EQ(OB_SUCCESS, my_session_.init_tenant(ObString::make_string("sys"), 1));
    ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
    ASSERT_EQ(OB_SUCCESS, my_session_.load_default_sys_variable(false, true));
    ASSERT_EQ(OB_SUCCESS, my_session_.load_default_configs_in_pc());

    THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 6000000000);
    fake_table_.set_column_count(col_count);
    result_table_.set_column_count(col_count + (is_number ? 5 : 3) + (is_distinct ? 1 : 0));    // sum, avg
    groupby_op.set_column_count(col_count + (is_number ? 5 : 3) + (is_distinct ? 1 : 0));

    fake_table_.set_id(0);
    result_table_.set_id(1);
    groupby_op.set_id(2);

    fake_table_.set_phy_plan(&physical_plan_);
    result_table_.set_phy_plan(&physical_plan_);
    groupby_op.set_phy_plan(&physical_plan_);
    groupby_op.init(10);
    groupby_op.set_child(0, fake_table_);

    ctx.set_my_session(&my_session_);
    ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(3));
    ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());
    plan_ctx = ctx.get_physical_plan_ctx();
    ASSERT_FALSE(NULL == plan_ctx);
    plan_ctx->set_phy_plan(&physical_plan_);
    add_aggr_column(groupby_op, is_distinct, is_number, agg_cs_type);
    groupby_op.add_group_column_idx(2, group_cs_type);
  }

  static void init(ObExecContext &ctx,
                   ObScalarAggregate &scalar_aggr_op,
                   int64_t col_count,
                   bool is_distinct,
                   bool is_number = true,
                   ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN)
  {
    ObPhysicalPlanCtx *plan_ctx = NULL;

    scalar_aggr_op.reset();
    scalar_aggr_op.reuse();
    fake_table_.reset();
    fake_table_.reuse();
    result_table_.reset();
    result_table_.reuse();
    physical_plan_.reset();
    OBSERVER.init_schema();
    OBSERVER.init_tz_info_mgr();
    ASSERT_EQ(OB_SUCCESS, ObTenantConfigMgr::get_instance().add_tenant_config(1));
    ASSERT_EQ(OB_SUCCESS, my_session_.init_tenant(ObString::make_string("sys"), 1));
    ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
    ASSERT_EQ(OB_SUCCESS, my_session_.load_default_sys_variable(false, true));
    ASSERT_EQ(OB_SUCCESS, my_session_.load_default_configs_in_pc());

    fake_table_.set_column_count(col_count);
    result_table_.set_column_count(col_count + (is_number ? 5 : 3) + (is_distinct ? 1 : 0));    // sum, avg
    scalar_aggr_op.set_column_count(col_count + (is_number ? 5 : 3) + (is_distinct ? 1 : 0));
    scalar_aggr_op.init(10);
    fake_table_.set_id(0);
    result_table_.set_id(1);
    scalar_aggr_op.set_id(2);

    fake_table_.set_phy_plan(&physical_plan_);
    result_table_.set_phy_plan(&physical_plan_);
    scalar_aggr_op.set_phy_plan(&physical_plan_);

    scalar_aggr_op.set_child(0, fake_table_);

    ctx.set_my_session(&my_session_);
    ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(3));
    ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());
    plan_ctx = ctx.get_physical_plan_ctx();
    ASSERT_FALSE(NULL == plan_ctx);
    plan_ctx->set_phy_plan(&physical_plan_);
    add_aggr_column(scalar_aggr_op, is_distinct, is_number, agg_cs_type);
  }

  static void init(ObExecContext &ctx, ObScalarAggregate &scalar_aggr_op, int64_t col_count)
  {
    ObPhysicalPlanCtx *plan_ctx = NULL;

    scalar_aggr_op.reset();
    scalar_aggr_op.reuse();
    fake_table_.reset();
    fake_table_.reuse();
    result_table_.reset();
    result_table_.reuse();
    physical_plan_.reset();
    OBSERVER.init_schema();
    OBSERVER.init_tz_info_mgr();
    ASSERT_EQ(OB_SUCCESS, ObTenantConfigMgr::get_instance().add_tenant_config(1));
    ASSERT_EQ(OB_SUCCESS, my_session_.init_tenant(ObString::make_string("sys"), 1));
    ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
    ASSERT_EQ(OB_SUCCESS, my_session_.load_default_sys_variable(false, true));
    ASSERT_EQ(OB_SUCCESS, my_session_.load_default_configs_in_pc());

    scalar_aggr_op.init(10);
    fake_table_.set_column_count(col_count);
    result_table_.set_column_count(col_count + 3);    // avg(c), avg(b), avg(a)
    scalar_aggr_op.set_column_count(col_count + 3);

    fake_table_.set_id(0);
    result_table_.set_id(1);
    scalar_aggr_op.set_id(2);

    fake_table_.set_phy_plan(&physical_plan_);
    result_table_.set_phy_plan(&physical_plan_);
    scalar_aggr_op.set_phy_plan(&physical_plan_);

    scalar_aggr_op.set_child(0, fake_table_);

    ctx.set_my_session(&my_session_);
    ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(3));
    ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());
    plan_ctx = ctx.get_physical_plan_ctx();
    ASSERT_FALSE(NULL == plan_ctx);
    plan_ctx->set_phy_plan(&physical_plan_);
    add_aggr_column(scalar_aggr_op);
  }

  static void open_operator(ObExecContext &ctx, ObGroupBy &groupby_op)
  {
    ObPhyOperator *op = static_cast<ObPhyOperator *>(&groupby_op);
    ASSERT_EQ(OB_SUCCESS, op->open(ctx));
    ASSERT_EQ(OB_SUCCESS, result_table_.open(ctx));
  }

  static void close_operator(ObExecContext &ctx, ObGroupBy &groupby_op)
  {
    ObPhyOperator *op = static_cast<ObPhyOperator *>(&groupby_op);
    ASSERT_EQ(OB_SUCCESS, op->close(ctx));
    ASSERT_EQ(OB_SUCCESS, result_table_.close(ctx));
  }

  static void open_operator(ObExecContext &ctx, ObScalarAggregate &groupby_op)
  {
    ObPhyOperator *op = static_cast<ObPhyOperator *>(&groupby_op);
    ASSERT_EQ(OB_SUCCESS, op->open(ctx));
    ASSERT_EQ(OB_SUCCESS, result_table_.open(ctx));
  }

  static void close_operator(ObExecContext &ctx, ObScalarAggregate &groupby_op)
  {
    ObPhyOperator *op = static_cast<ObPhyOperator *>(&groupby_op);
    ASSERT_EQ(OB_SUCCESS, op->close(ctx));
    ASSERT_EQ(OB_SUCCESS, result_table_.close(ctx));
  }

  static ObFakeTable &get_fake_table() { return fake_table_; }
  static ObFakeTable &get_result_table() { return result_table_; }
  static ObPhysicalPlan &get_physical_plan() { return physical_plan_; }
private:
  static void add_aggr_column(ObGroupBy &groupby_op,
                              bool is_distinct,
                              bool is_number = true,
                              ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN)
  {
    ObAggregateExpression *col_expr = NULL;
    ObPostExprItem expr_item;
    //count(aggr_col)
    ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, col_expr));
    col_expr->reset();
    col_expr->set_item_count(1);
    expr_item.set_column(1);
    col_expr->add_expr_item(expr_item);
    col_expr->set_result_index(3);
    col_expr->set_aggr_func(T_FUN_COUNT, is_distinct);
    col_expr->set_collation_type(agg_cs_type);
    col_expr->init_aggr_cs_type_count(1);
    col_expr->add_aggr_cs_type(agg_cs_type);
    col_expr->set_real_param_col_count(1);
    groupby_op.add_aggr_column(col_expr);
    //max(aggr_col)
    ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, col_expr));
    col_expr->set_item_count(1);
    expr_item.set_column(1);
    col_expr->add_expr_item(expr_item);
    col_expr->set_result_index(4);
    col_expr->set_aggr_func(T_FUN_MAX, is_distinct);
    col_expr->set_collation_type(agg_cs_type);
    col_expr->init_aggr_cs_type_count(1);
    col_expr->add_aggr_cs_type(agg_cs_type);
    col_expr->set_real_param_col_count(1);
    groupby_op.add_aggr_column(col_expr);
    //min(aggr_col)
    ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, col_expr));
    col_expr->set_item_count(1);
    expr_item.set_column(1);
    col_expr->add_expr_item(expr_item);
    col_expr->set_result_index(5);
    col_expr->set_aggr_func(T_FUN_MIN, is_distinct);
    col_expr->set_collation_type(agg_cs_type);
    col_expr->init_aggr_cs_type_count(1);
    col_expr->add_aggr_cs_type(agg_cs_type);
    col_expr->set_real_param_col_count(1);
    groupby_op.add_aggr_column(col_expr);
    //functions used only for number
    if (is_number) {
      //sum(aggr_col)
      ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, col_expr));
      col_expr->set_item_count(1);
      expr_item.set_column(1);
      col_expr->add_expr_item(expr_item);
      col_expr->set_result_index(6);
      col_expr->set_aggr_func(T_FUN_SUM, is_distinct);
      col_expr->init_aggr_cs_type_count(1);
      col_expr->add_aggr_cs_type(agg_cs_type);
      col_expr->set_real_param_col_count(1);
      groupby_op.add_aggr_column(col_expr);
      //avg(aggr_col)
      ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, col_expr));
      col_expr->set_item_count(1);
      expr_item.set_column(1);
      col_expr->add_expr_item(expr_item);
      col_expr->set_result_index(7);
      col_expr->set_aggr_func(T_FUN_AVG, is_distinct);
      col_expr->init_aggr_cs_type_count(1);
      col_expr->add_aggr_cs_type(agg_cs_type);
      col_expr->set_real_param_col_count(1);
      groupby_op.add_aggr_column(col_expr);
    }
    if (is_distinct) {
      //approx_count_distinct(aggr_col)
      ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, col_expr));
      col_expr->reset();
      col_expr->set_item_count(1);
      expr_item.set_column(1);
      col_expr->add_expr_item(expr_item);
      col_expr->set_result_index(is_number ? 8 : 6);
      col_expr->set_aggr_func(T_FUN_APPROX_COUNT_DISTINCT, false);
      col_expr->set_collation_type(agg_cs_type);
      col_expr->init_aggr_cs_type_count(1);
      col_expr->add_aggr_cs_type(agg_cs_type);
      col_expr->set_real_param_col_count(1);
      groupby_op.add_aggr_column(col_expr);
    }
  }

  static void add_aggr_column(ObScalarAggregate &scalar_aggr_op)
  {
    ObAggregateExpression *col_expr = NULL;
    ObPostExprItem expr_item;
    //avg(c)
    ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, col_expr));
    col_expr->set_item_count(1);
    expr_item.set_column(0);
    col_expr->add_expr_item(expr_item);
    col_expr->set_result_index(3);
    col_expr->set_aggr_func(T_FUN_AVG, false);
    col_expr->set_real_param_col_count(1);
    scalar_aggr_op.add_aggr_column(col_expr);
    //avg(b)
    ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, col_expr));
    col_expr->set_item_count(1);
    expr_item.set_column(1);
    col_expr->add_expr_item(expr_item);
    col_expr->set_result_index(4);
    col_expr->set_aggr_func(T_FUN_AVG, false);
    col_expr->set_real_param_col_count(1);
    scalar_aggr_op.add_aggr_column(col_expr);
    //avg(a)
    ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, col_expr));
    col_expr->set_item_count(1);
    expr_item.set_column(2);
    col_expr->add_expr_item(expr_item);
    col_expr->set_result_index(5);
    col_expr->set_aggr_func(T_FUN_AVG, false);
    col_expr->set_real_param_col_count(1);
    scalar_aggr_op.add_aggr_column(col_expr);
  }

  static void add_aggr_column(ObScalarAggregate &scalar_aggr_op,
                              bool is_distinct,
                              bool is_number = true,
                              ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN)
  {
    ObAggregateExpression *col_expr = NULL;
    ObPostExprItem expr_item;
    //count(aggr_col)
    ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, col_expr));
    col_expr->reset();
    col_expr->set_item_count(1);
    expr_item.set_column(1);
    col_expr->add_expr_item(expr_item);
    col_expr->set_result_index(3);
    col_expr->set_aggr_func(T_FUN_COUNT, is_distinct);
    col_expr->set_collation_type(agg_cs_type);
    col_expr->init_aggr_cs_type_count(1);
    col_expr->add_aggr_cs_type(agg_cs_type);
    col_expr->set_real_param_col_count(1);
    scalar_aggr_op.add_aggr_column(col_expr);
    //max(aggr_col)
    ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, col_expr));
    col_expr->set_item_count(1);
    expr_item.set_column(1);
    col_expr->add_expr_item(expr_item);
    col_expr->set_result_index(4);
    col_expr->set_aggr_func(T_FUN_MAX, is_distinct);
    col_expr->set_collation_type(agg_cs_type);
    col_expr->init_aggr_cs_type_count(1);
    col_expr->add_aggr_cs_type(agg_cs_type);
    col_expr->set_real_param_col_count(1);
    scalar_aggr_op.add_aggr_column(col_expr);
    //min(aggr_col)
    ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, col_expr));
    col_expr->set_item_count(1);
    expr_item.set_column(1);
    col_expr->add_expr_item(expr_item);
    col_expr->set_result_index(5);
    col_expr->set_aggr_func(T_FUN_MIN, is_distinct);
    col_expr->set_collation_type(agg_cs_type);
    col_expr->init_aggr_cs_type_count(1);
    col_expr->add_aggr_cs_type(agg_cs_type);
    col_expr->set_real_param_col_count(1);
    scalar_aggr_op.add_aggr_column(col_expr);
    //functions used only for number
    if (is_number) {
      //sum(aggr_col)
      ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, col_expr));
    col_expr->set_item_count(1);
      expr_item.set_column(1);
      col_expr->add_expr_item(expr_item);
      col_expr->set_result_index(6);
      col_expr->set_aggr_func(T_FUN_SUM, is_distinct);
      col_expr->init_aggr_cs_type_count(1);
      col_expr->add_aggr_cs_type(agg_cs_type);
      col_expr->set_real_param_col_count(1);
      scalar_aggr_op.add_aggr_column(col_expr);
      //avg(aggr_col)
      ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, col_expr));
    col_expr->set_item_count(1);
      expr_item.set_column(1);
      col_expr->add_expr_item(expr_item);
      col_expr->set_result_index(7);
      col_expr->set_aggr_func(T_FUN_AVG, is_distinct);
      col_expr->init_aggr_cs_type_count(1);
      col_expr->add_aggr_cs_type(agg_cs_type);
      col_expr->set_real_param_col_count(1);
      scalar_aggr_op.add_aggr_column(col_expr);
    }
    if (is_distinct) {
      //approx_count_distinct(aggr_col)
      ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, col_expr));
      col_expr->reset();
      col_expr->set_item_count(1);
      expr_item.set_column(1);
      col_expr->add_expr_item(expr_item);
      col_expr->set_result_index(is_number ? 8 : 6);
      col_expr->set_aggr_func(T_FUN_APPROX_COUNT_DISTINCT, false);
      col_expr->set_collation_type(agg_cs_type);
      col_expr->init_aggr_cs_type_count(1);
      col_expr->add_aggr_cs_type(agg_cs_type);
      col_expr->set_real_param_col_count(1);
      scalar_aggr_op.add_aggr_column(col_expr);
    }
  }
private:
  static ObPhysicalPlan physical_plan_;
  static ObFakeTable fake_table_;
  static ObFakeTable result_table_;
  static ObSQLSessionInfo my_session_;
};

ObPhysicalPlan TestAggregateFactory::physical_plan_;
ObFakeTable TestAggregateFactory::fake_table_(TestAggregateFactory::physical_plan_.get_allocator());
ObFakeTable TestAggregateFactory::result_table_(TestAggregateFactory::physical_plan_.get_allocator());
ObSQLSessionInfo TestAggregateFactory::my_session_;

#endif /* OCEANBASE_UNITTEST_SQL_AGGREGATE_TEST_UTILS_H_ */
