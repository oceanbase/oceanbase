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
#include "ob_log_material.h"
#include "ob_log_operator_factory.h"
#include "sql/optimizer/ob_opt_est_cost.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

int ObLogMaterial::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  UNUSED(ctx);
  if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(sharding_info_.copy_with_part_keys(child->get_sharding_info()))) {
    LOG_WARN("failed to copy sharding info from children", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogMaterial::allocate_exchange(AllocExchContext* ctx, ObExchangeInfo& exch_info)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx), K(get_child(first_child)));
  } else if (OB_FAIL(child->allocate_exchange(ctx, exch_info))) {
    LOG_WARN("failed to allocate exchange", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogMaterial::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else {
    double op_cost = ObOptEstCost::cost_material(child->get_card(), child->get_width());
    set_cost(child->get_cost() + op_cost);
    set_op_cost(op_cost);
    set_card(child->get_card());
    set_width(child->get_width());
  }
  return ret;
}

int ObLogMaterial::re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est)
{
  int ret = OB_SUCCESS;
  UNUSED(parent);
  re_est = false;
  if (need_row_count >= get_card()) {
    /*do nothing*/
  } else {
    set_card(need_row_count);
    re_est = true;
  }
  return ret;
}
