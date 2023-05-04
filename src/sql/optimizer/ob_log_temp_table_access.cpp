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

#define USING_LOG_PREFIX SQL_OPT

#include "sql/optimizer/ob_log_temp_table_access.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_join_order.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql::log_op_def;

ObLogTempTableAccess::ObLogTempTableAccess(ObLogPlan &plan)
  : ObLogicalOperator(plan),
    table_id_(0),
    temp_table_id_(OB_INVALID_ID),
    access_exprs_()
{
}

ObLogTempTableAccess::~ObLogTempTableAccess()
{
}

int ObLogTempTableAccess::generate_access_expr()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_stmt()->get_column_exprs(table_id_, access_exprs_))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogTempTableAccess::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_access_expr())) {
    LOG_WARN("failed to generate access expr", K(ret));
  } else if (OB_FAIL(append(all_exprs, access_exprs_))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get exprs", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObLogTempTableAccess::allocate_expr_post(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs_.count(); ++i) {
    ObRawExpr *expr = access_exprs_.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else if (OB_FAIL(mark_expr_produced(expr,
                                          branch_id_,
                                          id_,
                                          ctx))) {
      LOG_WARN("failed to mark expr as produced", K(branch_id_), K(ret));
    } else if (!is_plan_root() && OB_FAIL(add_var_to_array_no_dup(output_exprs_, expr))) {
      LOG_WARN("failed to add expr", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
      LOG_WARN("failed to allocate_expr_post", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogTempTableAccess::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  card = get_card();
  op_cost = get_op_cost();
  cost = get_cost();
  double selectivity = 1.0;
  const int64_t parallel = param.need_parallel_;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()),K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_basic_table_metas(),
                                                            get_plan()->get_selectivity_ctx(),
                                                            get_filter_exprs(),
                                                            selectivity,
                                                            get_plan()->get_predicate_selectivities()))) {
    LOG_WARN("failed to calculate selectivity", K(ret));
  } else { 
    double read_card = card;
    if (selectivity > 0 && selectivity <= 1) {
      read_card /= selectivity;
    } else {
      read_card = 0;
    }
    //bloom filter selectivity
    for (int64_t i = 0; i < param.join_filter_infos_.count(); ++i) {
      const JoinFilterInfo &info = param.join_filter_infos_.at(i);
      if (info.table_id_ == table_id_) {
        card *= info.join_filter_selectivity_;
      }
    }
    //refine row count
    if (param.need_row_count_ >= 0) {
      if (param.need_row_count_ >= card) {
        //do nothing
      } else {
        read_card = param.need_row_count_ / selectivity;
        card = param.need_row_count_;
      }
    }
    double per_dop_card = read_card / parallel;
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    cost = ObOptEstCost::cost_read_materialized(per_dop_card, opt_ctx.get_cost_model_type()) +
                ObOptEstCost::cost_quals(per_dop_card, get_filter_exprs(), opt_ctx.get_cost_model_type());
    op_cost = cost;
  }
  return ret;
}

int ObLogTempTableAccess::get_plan_item_info(PlanText &plan_text,
                                             ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    // print access
    const ObIArray<ObRawExpr*> &access = get_access_exprs();
    EXPLAIN_PRINT_EXPRS(access, type);
    END_BUF_PRINT(plan_item.access_predicates_,
                  plan_item.access_predicates_len_);
  }
  if (OB_SUCC(ret)) {
    const ObString &temp_table_name = get_table_name();
    const ObString &access_name = get_access_name();
    BEGIN_BUF_PRINT;
    if (access_name.empty()) {
      if (OB_FAIL(BUF_PRINTF("%.*s",
                             temp_table_name.length(),
                             temp_table_name.ptr()))) {
        LOG_WARN("failed to print str", K(ret));
      }
    } else {
      if (OB_FAIL(BUF_PRINTF("%.*s(%.*s)",
                             access_name.length(),
                             access_name.ptr(),
                             temp_table_name.length(),
                             temp_table_name.ptr()))) {
        LOG_WARN("failed to print str", K(ret));
      }
    }
    END_BUF_PRINT(plan_item.object_alias_,
                  plan_item.object_alias_len_);
  }
  return ret;
}

int ObLogTempTableAccess::get_temp_table_plan(ObLogicalOperator *& insert_op)
{
  int ret = OB_SUCCESS;
  insert_op = NULL;
  ObLogPlan *plan = get_plan();
  const uint64_t temp_table_id = get_temp_table_id();
  if (OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected null", K(ret));
  } else {
    ObIArray<ObSqlTempTableInfo*> &temp_tables = plan->get_optimizer_context().get_temp_table_infos();
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < temp_tables.count(); ++i) {
      if (OB_ISNULL(temp_tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected null", K(ret));
      } else if (temp_table_id != temp_tables.at(i)->temp_table_id_) {
        /* do nothing */
      } else {
        find = true;
        insert_op = temp_tables.at(i)->table_plan_;
      }
    }
    if (OB_SUCC(ret) && !find) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("failed to find table plan", K(ret));
    }
  }
  return ret;
}
