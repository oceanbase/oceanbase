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
 * This file contains implementation support for the log json table abstraction.
 */

#define USING_LOG_PREFIX SQL_OPT

#include "sql/optimizer/ob_log_json_table.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_log_plan.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{


int ObLogJsonTable::generate_access_exprs()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      const ColumnItem *col_item = stmt->get_column_item(i);
      if (OB_ISNULL(col_item) || OB_ISNULL(col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (col_item->table_id_ == table_id_ &&
                 col_item->expr_->is_explicited_reference() &&
                 OB_FAIL(access_exprs_.push_back(col_item->expr_))) {
        LOG_WARN("failed to push back column expr", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogJsonTable::allocate_expr_post(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs_.count(); ++i) {
    ObRawExpr *value_col = access_exprs_.at(i);
    if (OB_FAIL(mark_expr_produced(value_col, branch_id_, id_, ctx))) {
      LOG_WARN("makr expr produced failed", K(ret));
    } else if (!is_plan_root() && OB_FAIL(add_var_to_array_no_dup(output_exprs_, value_col))) {
      LOG_WARN("add expr no duplicate key failed", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
      LOG_WARN("failed to allocate expr post", K(ret));
    }
  }
  return ret;
}

int ObLogJsonTable::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = get_stmt();

  if (OB_FAIL(generate_access_exprs())) {
    LOG_WARN("failed to generate access exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs, access_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (NULL != value_expr_ && OB_FAIL(all_exprs.push_back(value_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      const ColumnItem *col_item = stmt->get_column_item(i);
      if (col_item->table_id_ == table_id_) {
        if (OB_NOT_NULL(col_item->default_value_expr_)
            && OB_FAIL(all_exprs.push_back(col_item->default_value_expr_))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_NOT_NULL(col_item->default_empty_expr_)
                   && OB_FAIL(all_exprs.push_back(col_item->default_empty_expr_))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs", K(ret));
  }

  return ret;
}

int ObLogJsonTable::get_plan_item_info(PlanText &plan_text,
                                       ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else if (OB_ISNULL(get_value_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null value expr", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    const ObRawExpr* value = get_value_expr();
    EXPLAIN_PRINT_EXPR(value, type);
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }
  if (OB_SUCC(ret)) {
    const ObString &name = get_table_name();
    BUF_PRINT_OB_STR(name.ptr(),
                     name.length(),
                     plan_item.object_alias_,
                     plan_item.object_alias_len_);
    BUF_PRINT_STR("JSON_TABLE",
                  plan_item.object_type_,
                  plan_item.object_type_len_);
  }
  return ret;
}

uint64_t ObLogJsonTable::hash(uint64_t seed) const
{
  seed = do_hash(table_name_, seed);
  seed = ObLogicalOperator::hash(seed);
  return seed;
}

} // namespace sql
}// namespace oceanbase
