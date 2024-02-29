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
#include "sql/optimizer/ob_join_order.h"

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
  double op_cost = 0.0;
  double child_cost = 0.0;
  double card = 0.0;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
    if (OB_ISNULL(get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      child_cost += get_child(i)->get_cost();
      card = get_child(i)->get_card();
    }
  }
  if (OB_SUCC(ret)) {
    set_op_cost(op_cost);
    set_cost(op_cost + child_cost);
    set_card(card);
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

int ObLogTempTableTransformation::compute_op_parallel_and_server_info()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *last_child = get_child(get_num_of_child() - 1);
  if (OB_ISNULL(last_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(last_child), K(ret));
  } else if (OB_FAIL(get_server_list().assign(last_child->get_server_list()))) {
    LOG_WARN("failed to assign server list", K(ret));
  } else {
    set_parallel(last_child->get_parallel());
    set_server_cnt(last_child->get_server_cnt());
    if (is_single()) {
      set_available_parallel(last_child->get_available_parallel());
    }
  }
  return ret;
}

int ObLogTempTableTransformation::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  card = 0.0;
  op_cost = 0.0;
  cost = 0.0;
  ObLogicalOperator *child = NULL;
  EstimateCostInfo child_param;
  double child_card = 0.0;
  double child_cost = 0.0;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
    child_param.reset();
    child_param.override_ = param.override_;
    child_param.need_row_count_ = (get_num_of_child() - 1 ) == i ? param.need_row_count_ : -1;
    if (OB_ISNULL(child = get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(child->re_est_cost(child_param, card, child_cost))) {
      LOG_WARN("failed to re est child cost", K(ret));
    } else {
      cost += child_cost;
    }
  }
  return ret;
}

int ObLogTempTableTransformation::get_card_without_filter(double &card)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child_op = NULL;
  if (OB_NOT_NULL(child_op = get_child(get_num_of_child() - 1))) {
    card = child_op->get_card();
  } else {
    card = get_card();
  }
  return ret;
}