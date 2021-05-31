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
#include "ob_log_delete.h"
#include "ob_log_plan.h"
#include "sql/optimizer/ob_optimizer_context.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

int ObLogDelete::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("allocate expr pre for delete", K(is_pdml()));
  if (OB_FAIL(add_table_columns_to_ctx(ctx))) {
    LOG_WARN("failed to add table columns to ctx", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
    LOG_WARN("failed to allocate expr pre", K(ret));
  } else if (is_pdml()) {
    if (OB_FAIL(alloc_partition_id_expr(ctx))) {
      LOG_WARN("failed alloc pseudo partition_id column for delete", K(ret));
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogDelete::allocate_expr_post(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  if (is_pdml() && is_index_maintenance()) {
    // handle shadow pk column
    if (OB_FAIL(alloc_shadow_pk_column_for_pdml(ctx))) {
      LOG_WARN("failed alloc generated column for pdml index maintain", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
      LOG_WARN("failed to allocate expr post for delete", K(ret));
    }
  }
  return ret;
}

const char* ObLogDelete::get_name() const
{
  const char* name = NULL;
  int ret = OB_SUCCESS;
  if (is_multi_part_dml()) {
    name = "MULTI PARTITION DELETE";
  } else if (is_pdml() && is_index_maintenance()) {
    name = "INDEX DELETE";
  } else {
    name = ObLogDelUpd::get_name();
  }
  return name;
}

int ObLogDelete::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(child));
  } else {
    set_op_cost(child->get_card() * static_cast<double>(DELETE_ONE_ROW_COST));
    set_cost(child->get_cost() + get_op_cost());
    set_card(child->get_card());
  }
  return ret;
}
