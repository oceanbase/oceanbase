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

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql::log_op_def;

ObLogTempTableInsert::ObLogTempTableInsert(ObLogPlan& plan)
    : ObLogicalOperator(plan), ref_table_id_(OB_INVALID_ID), temp_table_name_()
{}

ObLogTempTableInsert::~ObLogTempTableInsert()
{}

int ObLogTempTableInsert::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (child->get_sharding_info().is_distributed()) {
    sharding_info_.set_location_type(child->get_sharding_info().get_location_type());
  } else {
    ret = sharding_info_.copy_with_part_keys(child->get_sharding_info());
  }
  return ret;
}

int ObLogTempTableInsert::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    double op_cost = ObOptEstCost::cost_material(child->get_card(), child->get_width());
    set_op_cost(op_cost);
    set_cost(child->get_cost() + op_cost);
    set_card(child->get_card());
    set_width(child->get_width());
  }
  return ret;
}
