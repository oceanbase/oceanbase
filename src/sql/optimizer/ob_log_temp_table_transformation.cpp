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

#include "ob_log_temp_table_transformation.h"
#include "ob_opt_est_cost.h"
#include "sql/optimizer/ob_log_plan.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

ObLogTempTableTransformation::ObLogTempTableTransformation(ObLogPlan &plan)
  : ObLogicalOperator(plan)
{
}

ObLogTempTableTransformation::~ObLogTempTableTransformation()
{
}

int ObLogTempTableTransformation::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *last_child = NULL;
  if (OB_ISNULL(last_child = get_child(get_num_of_child() - 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("last_child is null", K(ret));
  } else if (OB_FAIL(set_op_ordering(last_child->get_op_ordering()))) {
    LOG_WARN("failed to set op ordering", K(ret));
  } else {
    is_local_order_ = last_child->get_is_local_order();
  }
  return ret;
}

int ObLogTempTableTransformation::compute_fd_item_set()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *last_child = NULL;
  if (OB_ISNULL(last_child = get_child(get_num_of_child() - 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    set_fd_item_set(&last_child->get_fd_item_set());
  }
  return ret;
}

int ObLogTempTableTransformation::est_cost()
{
  int ret = OB_SUCCESS;
  int64_t parallel = 0.0;
  ObLogicalOperator *last_child = get_child(get_num_of_child() - 1);
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(last_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(last_child), K(ret));
  } else if (OB_UNLIKELY((parallel = get_parallel()) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(ret));  
  } else {
    double child_cost = 0.0;
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    double op_cost = ObOptEstCost::cost_filter_rows(last_child->get_card() / parallel, 
                                                    filter_exprs_,
                                                    opt_ctx.get_cost_model_type());
    for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
      if (OB_ISNULL(get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        child_cost += get_child(i)->get_cost();
      }
    }
    if (OB_SUCC(ret)) {
      set_op_cost(op_cost);
      set_cost(op_cost + child_cost);
      set_card(last_child->get_card());
    }
  }
  return ret;
}

int ObLogTempTableTransformation::est_width()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *last_child = get_child(get_num_of_child() - 1);
  if (OB_ISNULL(last_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(last_child), K(ret));
  } else {
    set_width(last_child->get_width());
  }
  return ret;
}

int ObLogTempTableTransformation::allocate_startup_expr_post()
{
  int ret = OB_SUCCESS;
  int64_t last_child = get_num_of_child() - 1;
  if (OB_FAIL(ObLogicalOperator::allocate_startup_expr_post(last_child))) {
    LOG_WARN("failed to allocate startup expr post", K(ret));
  }
  return ret;
}
