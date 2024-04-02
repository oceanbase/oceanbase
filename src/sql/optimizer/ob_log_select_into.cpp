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
#include "ob_log_select_into.h"
#include "ob_log_table_scan.h"
#include "ob_optimizer_util.h"
#include "ob_opt_est_cost.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

int ObLogSelectInto::est_cost()
{
  int ret = OB_SUCCESS;
  int parallel = 0.0;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (OB_UNLIKELY((parallel = get_parallel()) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(parallel), K(ret)); 
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    set_op_cost(ObOptEstCost::cost_get_rows(child->get_card() / parallel, 
                                            opt_ctx));
    set_cost(op_cost_ + child->get_cost());
    set_card(child->get_card());
  }
  return ret;
}

int ObLogSelectInto::compute_plan_type()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (OB_FAIL(ObLogicalOperator::compute_plan_type())) {
    LOG_WARN("failed to compute plan type", K(ret));
  } else if (LOG_EXCHANGE == child->get_type()) {
    location_type_ = ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN;
  } else { /*do nothing*/ }
  return ret;
}

int ObLogSelectInto::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(all_exprs, select_exprs_))) {
    LOG_WARN("failed to push back select exprs", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogSelectInto::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_exprs_action(replacer, select_exprs_))) {
    LOG_WARN("failed to replace select exprs", K(ret));
  }
  return ret;
}