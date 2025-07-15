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
#include "ob_log_rescan_limit.h"
#include "sql/optimizer/ob_join_order.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

int ObLogRescanLimit::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    double op_cost = 0.0;
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    set_op_cost(op_cost);
    set_cost(child->get_cost() + op_cost);
    set_card((rescan_limit_ + 1) * child->get_card());
  }
  return ret;
}

int ObLogRescanLimit::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  double child_card = 0.0;
  double child_cost = 0.0;
  ObLogicalOperator *child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FALSE_IT(param.need_row_count_ = -1)) {
  } else if (OB_FAIL(SMART_CALL(child->re_est_cost(param, child_card, child_cost)))) {
    LOG_WARN("failed to re est cost", K(ret));
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    op_cost = 0;
    cost = child_cost + op_cost;
    card = (rescan_limit_ + 1) * child_card;
  }
  return ret;
}

int ObLogRescanLimit::get_plan_item_info(PlanText &plan_text,
                                         ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    if (OB_FAIL(get_explain_name_internal(buf, buf_len, pos))) {
      LOG_WARN("failed to get explain name", K(ret));
    } else {
      END_BUF_PRINT(plan_item.operation_, plan_item.operation_len_);
    }
  }
  if (OB_SUCC(ret)) {
    BEGIN_BUF_PRINT;
    BUF_PRINTF("rescan_limit=%ld", rescan_limit_);
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }
  return ret;
}