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

#include "sql/optimizer/ob_log_temp_table_insert.h"
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

ObLogTempTableInsert::ObLogTempTableInsert(ObLogPlan &plan)
    : ObLogicalOperator(plan),
      temp_table_id_(OB_INVALID_ID),
      temp_table_name_()
{
}

ObLogTempTableInsert::~ObLogTempTableInsert()
{
}

int ObLogTempTableInsert::compute_sharding_info()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(get_plan()), K(ret));
  } else if (child->is_match_all()) {
    //temp table insert is a data-shared operator, can not be match all 
    strong_sharding_ = get_plan()->get_optimizer_context().get_local_sharding();
  } else if (child->is_single()) {
    strong_sharding_ = child->get_sharding();
    inherit_sharding_index_ = ObLogicalOperator::first_child;
  } else {
    strong_sharding_ = get_plan()->get_optimizer_context().get_distributed_sharding();
  }
  return ret;
}

int ObLogTempTableInsert::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (child->is_single()) {
    if (OB_FAIL(ObLogicalOperator::compute_op_ordering())) {
      LOG_WARN("failed to compute op ordering", K(ret));
    } else { /*do nothing*/ }
  } else { /*do nothing*/ }
  return ret;
}

int ObLogTempTableInsert::est_cost()
{
  int ret = OB_SUCCESS;
  double card = 0.0;
  double op_cost = 0.0;
  double cost = 0.0;
  EstimateCostInfo param;
  param.need_parallel_ = get_parallel();
  if (OB_FAIL(do_re_est_cost(param, card, op_cost, cost))) {
    LOG_WARN("failed to do re est cost", K(ret));
  } else {
    set_op_cost(op_cost);
    set_cost(cost);
    set_card(card);
  }
  return ret;
}

int ObLogTempTableInsert::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  int64_t parallel = 0;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(param.need_parallel_ < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(param.need_parallel_), K(ret));
  } else if (OB_FAIL(child->re_est_cost(param, card, cost))) {
    LOG_WARN("failed to do re est cost", K(ret));
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    op_cost = ObOptEstCost::cost_material(card / param.need_parallel_,
                                          child->get_width(),
                                          opt_ctx.get_cost_model_type());
    cost += op_cost;
  }
  return ret;
}

int ObLogTempTableInsert::get_plan_item_info(PlanText &plan_text,
                                             ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else {
    ObString &name = get_table_name();
    BUF_PRINT_OB_STR(name.ptr(),
                     name.length(),
                     plan_item.object_alias_,
                     plan_item.object_alias_len_);
  }
  return ret;
}