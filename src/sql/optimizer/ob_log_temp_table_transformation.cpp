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

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

ObLogTempTableTransformation::ObLogTempTableTransformation(ObLogPlan& plan) : ObLogicalOperator(plan)
{}

ObLogTempTableTransformation::~ObLogTempTableTransformation()
{}

int ObLogTempTableTransformation::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* op = NULL;
  ObLogTempTableTransformation* temp_table_op = NULL;
  out = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone ObLogAppend op", K(ret));
  } else if (OB_ISNULL(temp_table_op = static_cast<ObLogTempTableTransformation*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    out = temp_table_op;
  }
  return ret;
}

int ObLogTempTableTransformation::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  ObExchangeInfo exch_info;
  ObLogicalOperator* last_child = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(last_child = get_child(get_num_of_child() - 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(ret));
  } else if (OB_FAIL(compute_basic_sharding_info(ctx, is_basic))) {
    LOG_WARN("failed to compute basic sharding info", K(ret));
  } else if (is_basic) {
    /*do nothing*/
  } else if (OB_FAIL(sharding_info_.copy_with_part_keys(last_child->get_sharding_info()))) {
    LOG_WARN("failed to copy sharding info from children", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child() - 1; i++) {
      ObLogicalOperator* child = NULL;
      if (OB_ISNULL(child = get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(child), K(ret));
      } else if (child->get_sharding_info().is_sharding() &&
                 OB_FAIL(allocate_exchange_nodes_below(i, *ctx, exch_info))) {
        LOG_WARN("failed to allocate exchange nodes below", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogTempTableTransformation::allocate_exchange(AllocExchContext* ctx, ObExchangeInfo& exch_info)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* last_child = NULL;
  if (OB_ISNULL(last_child = get_child(get_num_of_child() - 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (OB_FAIL(last_child->allocate_exchange(ctx, exch_info))) {
    LOG_WARN("failed to allocate exchange", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogTempTableTransformation::transmit_local_ordering()
{
  int ret = OB_SUCCESS;
  reset_local_ordering();
  ObLogicalOperator* last_child = NULL;
  if (OB_ISNULL(last_child = get_child(get_num_of_child() - 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("last_child is null", K(ret));
  } else if (OB_FAIL(set_local_ordering(last_child->get_local_ordering()))) {
    LOG_WARN("failed to set local ordering", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogTempTableTransformation::transmit_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* last_child = NULL;
  if (OB_ISNULL(last_child = get_child(get_num_of_child() - 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("last_child is null", K(ret));
  } else if (OB_FAIL(set_op_ordering(last_child->get_op_ordering()))) {
    LOG_WARN("failed to set op ordering", K(ret));
  } else if (OB_FAIL(transmit_local_ordering())) {
    LOG_WARN("failed to set local ordering", K(ret));
  } else { /*do nothing.*/
  }
  return ret;
}

int ObLogTempTableTransformation::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* last_child = NULL;
  if (OB_ISNULL(last_child = get_child(get_num_of_child() - 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("last_child is null", K(ret));
  } else if (OB_FAIL(set_op_ordering(last_child->get_op_ordering()))) {
    LOG_WARN("failed to set op ordering", K(ret));
  } else { /*do nothing.*/
  }
  return ret;
}

int ObLogTempTableTransformation::compute_fd_item_set()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* last_child = NULL;
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
  ObLogicalOperator* last_child = get_child(get_num_of_child() - 1);
  if (OB_ISNULL(last_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    double child_cost = 0.0;
    double op_cost = ObOptEstCost::cost_subplan_scan(last_child->get_card(), last_child->get_width());
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
      set_width(last_child->get_width());
    }
  }
  return ret;
}
