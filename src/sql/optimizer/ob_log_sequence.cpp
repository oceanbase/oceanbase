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
#include "ob_log_sequence.h"
#include "ob_log_operator_factory.h"
#include "ob_optimizer_util.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_join_order.h"
#include "common/ob_smart_call.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

int ObLogSequence::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(get_stmt()));
  } else if (OB_FAIL(stmt->get_sequence_exprs(all_exprs))) {
    LOG_WARN("fail get sequence exprs", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogSequence::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    if (0 == get_num_of_child()) {
      op_cost_ = ObOptEstCost::cost_sequence(0, 
                                             nextval_seq_ids_.count(),
                                             opt_ctx.get_cost_model_type());
      cost_ = op_cost_;
      card_ = 0.0;
    } else if (OB_ISNULL(child = get_child(first_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      op_cost_ = ObOptEstCost::cost_sequence(child->get_card(), 
                                             nextval_seq_ids_.count(),
                                             opt_ctx.get_cost_model_type());
      cost_ = op_cost_ + child->get_cost();
      card_ = child->get_card();
    }
  }
  return ret;
}

int ObLogSequence::est_width()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  double width = 0.0;
  if (0 == get_num_of_child()) {
    width = 0.0;
  } else if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else {
    width = child->get_width() + nextval_seq_ids_.count() * 8;
  }
  set_width(width);
  return ret;
}

int ObLogSequence::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  double child_card = 0.0;
  double child_cost = 0.0;
  ObLogicalOperator *child = get_child(ObLogicalOperator::first_child);
  if (0 == get_num_of_child()) {
    card = get_card();
    op_cost = get_op_cost();
    cost = get_cost();
  } else if (OB_ISNULL(get_plan()) || OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()), K(child));
  } else if (OB_FAIL(SMART_CALL(child->re_est_cost(param, child_card, child_cost)))) {
    LOG_WARN("failed to re est cost", K(ret));
  } else {
    op_cost = ObOptEstCost::cost_sequence(child_card,
                                          nextval_seq_ids_.count(),
                                          get_plan()->get_optimizer_context().get_cost_model_type());
    cost = child_cost + op_cost;
    card = child_card;
  }
  return ret;
}

int ObLogSequence::compute_op_parallel_and_server_info()
{
  int ret = common::OB_SUCCESS;
  if (get_num_of_child() == 0) {
    ret = set_parallel_and_server_info_for_match_all();
  } else {
    ret = ObLogicalOperator::compute_op_parallel_and_server_info();
  }
  return ret;
}

int ObLogSequence::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  int ret = OB_SUCCESS;
  is_fixed = false;
  ObSEArray<ObRawExpr*, 8> sequence_exprs;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(get_stmt()));
  } else if (OB_FAIL(get_stmt()->get_sequence_exprs(sequence_exprs))) {
    LOG_WARN("fail get sequence exprs", K(ret));
  } else {
    is_fixed = ObOptimizerUtil::find_item(sequence_exprs, expr);
  }
  return ret;
}
