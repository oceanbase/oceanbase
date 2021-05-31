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

#define USING_LOG_PREFIX SQL_REWRITE
#include "sql/rewrite/ob_transform_project_pruning.h"

#include "lib/allocator/ob_allocator.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "common/ob_smart_call.h"

namespace oceanbase {
using namespace common;
namespace sql {

int ObTransformProjectPruning::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  UNUSED(parent_stmts);
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (stmt->is_insert_stmt()) {
    /*do nothing*/
  } else {
    // traverse table items(all table items are in from items)
    ObIArray<TableItem*>& table_items = stmt->get_table_items();
    TableItem* table_item = NULL;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < table_items.count(); ++idx) {
      bool is_valid = false;
      if (OB_ISNULL(table_item = table_items.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Table item is NULL in table items", K(ret));
      } else if (table_item->is_generated_table()) {
        ObSelectStmt* ref_query = NULL;
        if (OB_ISNULL(ref_query = table_item->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Ref query of generate_table is NULL", K(ret));
        } else if (OB_FAIL(check_transform_validity(*ref_query, is_valid))) {
          LOG_WARN("failed to check transform valid", K(ret));
        } else if (!is_valid) {
          // do nothing
        } else if (OB_FAIL(project_pruning(table_item->table_id_, *ref_query, *stmt, is_happened))) {
          LOG_WARN("Failed to project pruning generated table", K(ret));
        } else {
          trans_happened |= is_happened;
        }
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformProjectPruning::is_const_expr(ObRawExpr* expr, bool& is_const)
{
  int ret = OB_SUCCESS;
  is_const = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (expr->is_const_expr()) {
    is_const = true;
  } else if (expr->is_set_op_expr()) {
    is_const = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_const && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_const_expr(expr->get_param_expr(i), is_const)))) {
        LOG_WARN("failed to check is const expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformProjectPruning::check_transform_validity(const ObSelectStmt& stmt, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  bool is_const = false;
  if (stmt.has_distinct() || stmt.is_scala_group_by() || stmt.is_recursive_union() ||
      (stmt.is_set_stmt() && stmt.is_set_distinct()) || stmt.is_hierarchical_query() || stmt.is_contains_assignment()) {
    // do nothing
  } else if (stmt.get_select_item_size() == 1 && OB_FAIL(is_const_expr(stmt.get_select_item(0).expr_, is_const))) {
    LOG_WARN("failed to check is const expr", K(ret));
  } else if (is_const) {
    // do nothing, only with a dummy output
  } else if (stmt.is_set_stmt()) {
    is_valid = true;
    const ObIArray<ObSelectStmt*>& child_stmts = stmt.get_set_query();
    ObRawExpr* order_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < child_stmts.count(); ++i) {
      if (OB_ISNULL(child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child query is null", K(ret));
      } else if (OB_FAIL(SMART_CALL(check_transform_validity(*child_stmts.at(i), is_valid)))) {
        LOG_WARN("failed to check transform validity", K(ret));
      }
    }
  } else {
    is_valid = true;
  }
  return ret;
}

int ObTransformProjectPruning::project_pruning(
    const uint64_t table_id, ObSelectStmt& child_stmt, ObDMLStmt& upper_stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> removed_idx;
  trans_happened = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt.get_select_item_size(); i++) {
    bool need_remove = false;
    if (OB_ISNULL(upper_stmt.get_column_item_by_id(table_id, i + OB_APP_MIN_COLUMN_ID)) &&
        OB_FAIL(check_need_remove(&child_stmt, i, need_remove))) {
      LOG_WARN("fail to check column in set ordrt by", K(ret));
    } else if (need_remove) {
      ret = removed_idx.add_member(i);
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret) && !removed_idx.is_empty()) {
    if (OB_FAIL(ObTransformUtils::remove_select_items(ctx_, table_id, child_stmt, upper_stmt, removed_idx))) {
      LOG_WARN("failed to remove select items", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

// cheeck whether a select expr is used by a set stmt's order by clause
int ObTransformProjectPruning::check_need_remove(ObSelectStmt* stmt, const int64_t idx, bool& need_remove)
{
  int ret = OB_SUCCESS;
  need_remove = true;
  ObRawExpr* expr = NULL;
  if (OB_ISNULL(stmt) || idx < 0 || idx >= stmt->get_select_item_size() ||
      OB_ISNULL(expr = stmt->get_select_item(idx).expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt", K(ret));
  } else if (!stmt->is_set_stmt()) {
    /*do nothing*/
  } else if (OB_ISNULL(expr = ObTransformUtils::get_expr_in_cast(expr)) || OB_UNLIKELY(!expr->is_set_op_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected set expr", K(ret), K(expr));
  } else {
    const int64_t child_idx = static_cast<ObSetOpRawExpr*>(expr)->get_idx();
    ObIArray<ObSelectStmt*>& child_stmts = stmt->get_set_query();
    ObRawExpr* order_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && need_remove && i < child_stmts.count(); ++i) {
      ret = SMART_CALL(check_need_remove(child_stmts.at(i), child_idx, need_remove));
    }
    for (int64_t i = 0; OB_SUCC(ret) && need_remove && i < stmt->get_order_item_size(); ++i) {
      if (OB_ISNULL(order_expr = stmt->get_order_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (order_expr == expr) {
        need_remove = false;
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
