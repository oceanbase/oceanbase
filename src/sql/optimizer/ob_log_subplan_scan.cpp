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
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_join_order.h"
#include "common/ob_smart_call.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObLogSubPlanScan::generate_access_exprs()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(stmt), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); i++) {
      const ColumnItem *col_item = stmt->get_column_item(i);
      if (OB_ISNULL(col_item) || OB_ISNULL(col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(col_item), K(ret));
      } else if (col_item->table_id_ == subquery_id_ &&
                 col_item->expr_->is_explicited_reference() &&
                 !col_item->expr_->is_unpivot_mocked_column() &&
                 OB_FAIL(access_exprs_.push_back(col_item->expr_))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogSubPlanScan::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_access_exprs())) {
    LOG_WARN("failed to generate access exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs, access_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (ObLogicalOperator::get_op_exprs(all_exprs)) {
    LOG_WARN("failed to append op exprs", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObLogSubPlanScan::allocate_expr_post(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs_.count(); i++) {
    ObRawExpr *expr = NULL;
    if (OB_ISNULL(expr = access_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(mark_expr_produced(expr, branch_id_, id_, ctx))) {
      LOG_WARN("failed to mark expr as produced", K(ret));
    } else if (!is_plan_root() && OB_FAIL(output_exprs_.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/ }
  }
  // check if we can produce some more exprs, such as 1 + 'c1' after we have produced 'c1'
  if(OB_SUCC(ret)) {
    if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
      LOG_WARN("failed to allocate expr pre", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogSubPlanScan::print_my_plan_annotation(char *buf,
                                               int64_t &buf_len,
                                               int64_t &pos,
                                               ExplainType type)
{
  int ret = OB_SUCCESS;
  // print access
  if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else { /* Do nothing */ }
  const ObIArray<ObRawExpr*> &access = get_access_exprs();
  if (OB_SUCC(ret)) {
    EXPLAIN_PRINT_EXPRS(access, type);
  } else { /* Do nothing */ }
  return ret;
}

int ObLogSubPlanScan::generate_link_sql_post(GenLinkStmtPostContext &link_ctx)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = get_child(first_child);
  if (0 == dblink_id_) {
    // do nothing
  } else if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is NULL", K(ret));
  } else if (OB_FAIL(link_ctx.spell_subplan_scan(static_cast<const ObSelectStmt *>(get_stmt()),
                                                get_output_exprs(),
                                                child->get_output_exprs(),
                                                startup_exprs_,
                                                filter_exprs_,
                                                subquery_name_))) {
    LOG_WARN("dblink fail to reverse spell subplan scan", K(dblink_id_), K(ret));
  }
  return ret;
}

int ObLogSubPlanScan::re_est_cost(EstimateCostInfo &param, double &card, double &cost)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  int parallel = 1.0;
  double selectivity = 1.0;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child)) ||
      OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (OB_UNLIKELY((parallel = get_parallel()) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(parallel), K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_basic_table_metas(),
                                                            get_plan()->get_selectivity_ctx(),
                                                            get_filter_exprs(),
                                                            selectivity,
                                                            get_plan()->get_predicate_selectivities()))) {
      LOG_WARN("failed to calculate selectivity", K(ret));
  } else {
    double child_card = child->get_card();
    double child_cost = child->get_cost();
    if (param.need_row_count_ >= 0 && param.need_row_count_ < get_card() &&
        selectivity > 0) {
      param.need_row_count_ /= selectivity;
    }
    if (OB_FAIL(SMART_CALL(child->re_est_cost(param, child_card, child_cost)))) {
      LOG_WARN("failed to re est exchange cost", K(ret));
    } else {
      ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
      double op_cost = ObOptEstCost::cost_filter_rows(child_card / parallel, 
                                                      get_filter_exprs(),
                                                      opt_ctx.get_cost_model_type());
      cost = child_cost + op_cost;
      card = child_card * selectivity;
      if (param.override_) {
        set_op_cost(op_cost);
        set_cost(cost);
        set_card(card);
      }
    }
  }
  return ret;
}

int ObLogSubPlanScan::check_output_dependance(ObIArray<ObRawExpr *> &child_output,
                                              PPDeps &deps)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> exprs;
  LOG_TRACE("start to check output exprs", K(type_), K(child_output), K(deps));
  ObRawExprCheckDep dep_checker(child_output, deps, false);
  if (OB_FAIL(append(exprs, filter_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(exprs, output_exprs_))) {
    LOG_WARN("failed to append array no dup", K(ret));
  } else if (OB_FAIL(dep_checker.check(exprs))) {
    LOG_WARN("failed to check op_exprs", K(ret));
  } else {
    LOG_TRACE("succeed to check output exprs", K(exprs), K(type_), K(deps));
  }
  return ret;
}
