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

#include "common/ob_common_utility.h"
#include "common/ob_smart_call.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_transform_set_op.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "sql/rewrite/ob_transform_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace sql {

int ObTransformSetOp::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool first_happened = false;
  bool second_happened = false;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (stmt->is_select_stmt() && static_cast<ObSelectStmt*>(stmt)->has_set_op()) {
    if (OB_FAIL(remove_order_by_for_set(stmt, first_happened))) {
      LOG_WARN("failed to remove order by  from union", K(ret));
    } else if (OB_FAIL(add_limit_order_distinct_for_union(parent_stmts, stmt, second_happened))) {
      LOG_WARN("failed to add limit for union", K(ret));
    } else {
      trans_happened |= first_happened | second_happened;
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObTransformSetOp::remove_order_by_for_set(ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* left_stmt = NULL;
  ObSelectStmt* right_stmt = NULL;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer passed to transform", K(stmt), K(ret));
  } else if (stmt->is_select_stmt() && static_cast<ObSelectStmt*>(stmt)->has_set_op()) {
    ObIArray<ObSelectStmt*>& child_stmts = static_cast<ObSelectStmt*>(stmt)->get_set_query();
    ObSelectStmt* child_stmt = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      if (OB_ISNULL(child_stmt = child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt is NULL", K(stmt), K(child_stmt), K(ret));
      } else if (NULL == child_stmt->get_limit_expr() && NULL == child_stmt->get_limit_percent_expr() &&
                 !child_stmt->is_contains_assignment() && child_stmt->has_order_by()) {
        child_stmt->get_order_items().reset();
        trans_happened = true;
      }
    }
  }
  return ret;
}

int ObTransformSetOp::add_distinct(ObSelectStmt* stmt, ObSelectStmt* upper_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(upper_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer passed to add_distinct", K(stmt), K(upper_stmt));
  } else if (upper_stmt->is_set_distinct()) {
    if (stmt->has_set_op()) {
      stmt->assign_set_distinct();
    } else {
      stmt->assign_distinct();
    }
  }
  return ret;
}

int ObTransformSetOp::add_limit(ObSelectStmt* stmt, ObSelectStmt* upper_stmt)
{
  int ret = OB_SUCCESS;
  ObRawExpr* limit_count_expr = NULL;
  ObRawExpr* limit_offset_expr = NULL;
  ObRawExpr* limit_count_offset_expr = NULL;
  ObRawExpr* nonnegative_upper_limit = NULL;
  ObRawExpr* nonnegative_upper_offset = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("class data member is not inited", KP_(ctx));
  } else if (OB_ISNULL(stmt) || OB_ISNULL(upper_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer passed to add_distinct", K(stmt), K(upper_stmt));
  } else if (OB_ISNULL(upper_stmt->get_limit_expr()) || stmt->has_limit()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt should have limit", K(ret));
  } else if (NULL == upper_stmt->get_offset_expr()) {
    limit_count_offset_expr = upper_stmt->get_limit_expr();
  } else if (OB_FAIL(ObTransformUtils::make_pushdown_limit_count(*ctx_->expr_factory_,
                 *ctx_->session_info_,
                 upper_stmt->get_limit_expr(),
                 upper_stmt->get_offset_expr(),
                 limit_count_offset_expr))) {
    LOG_WARN("make pushdown limit expr failed", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObRawExprUtils::copy_expr(
            *ctx_->expr_factory_, limit_count_offset_expr, limit_count_expr, COPY_REF_DEFAULT))) {
      LOG_WARN("fail to copy limit count expr", K(ret));
    } else if (OB_ISNULL(limit_count_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left_limit_count_expr is NULL", K(ret));
    } else {
      stmt->set_limit_offset(limit_count_expr, limit_offset_expr);
      stmt->set_fetch_with_ties(upper_stmt->is_fetch_with_ties());
    }
  }
  return ret;
}

int ObTransformSetOp::add_order_by(ObSelectStmt* stmt, ObSelectStmt* upper_stmt)
{
  int ret = OB_SUCCESS;
  // add order by
  if (OB_ISNULL(stmt) || OB_ISNULL(upper_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer passed to add_distinct", K(stmt), K(upper_stmt));
  } else if (stmt->get_order_item_size() > 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt should not have order by", K(ret), K(stmt->get_order_item_size()));
  } else if (upper_stmt->get_order_item_size() > 0) {
    OrderItem order_item;
    for (int64_t i = 0; OB_SUCC(ret) && i < upper_stmt->get_order_item_size(); ++i) {
      order_item.expr_ = upper_stmt->get_order_item(i).expr_;
      order_item.order_type_ = upper_stmt->get_order_item(i).order_type_;
      if (OB_FAIL(recursive_adjust_order_by(stmt, upper_stmt->get_order_item(i).expr_, order_item.expr_))) {
        LOG_WARN("fail to adjust order by for stmt", K(ret));
      } else if (OB_FAIL(stmt->add_order_item(order_item))) {
        LOG_WARN("fail to add order_item", K(ret));
      }
    }  // for
  }
  return ret;
}

int ObTransformSetOp::check_can_push(ObSelectStmt* stmt, ObSelectStmt* upper_stmt, bool& can_push)
{
  int ret = OB_SUCCESS;
  can_push = true;
  if (OB_ISNULL(stmt) || OB_ISNULL(upper_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer passed to add_distinct", K(ret), K(stmt), K(upper_stmt));
  } else if (!upper_stmt->has_limit()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("upper_stmt should have limit ", K(ret), K(upper_stmt->has_limit()));
  } else if (stmt->has_limit() || stmt->is_contains_assignment()) {
    can_push = false;
  } else if ((0 < upper_stmt->get_order_item_size() && 0 < stmt->get_order_item_size()) ||
             NULL == upper_stmt->get_limit_expr() || NULL != upper_stmt->get_limit_percent_expr() ||
             upper_stmt->is_fetch_with_ties()) {
    can_push = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && can_push && i < upper_stmt->get_order_item_size(); ++i) {
      ObRawExpr* order_expr = upper_stmt->get_order_item(i).expr_;
      if (OB_ISNULL(order_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(order_expr));
      } else if (order_expr->has_flag(CNT_SUB_QUERY)) {
        can_push = false;
      } else if (!order_expr->has_flag(CNT_SET_OP)) {
        can_push = false;
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformSetOp::check_can_pre_push(ObSelectStmt* stmt, ObSelectStmt* upper_stmt, bool& can_push)
{
  int ret = OB_SUCCESS;
  can_push = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(upper_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer passed to cehck_can_pre_push", K(ret), K(stmt), K(upper_stmt));
  } else if (!stmt->has_set_op() || ObSelectStmt::UNION != stmt->get_set_op() || upper_stmt->has_set_op()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid stmt", K(ret), K(stmt), K(upper_stmt));
  } else if (stmt->has_limit() || stmt->get_current_level() != upper_stmt->get_current_level() ||
             !upper_stmt->has_limit() || 0 == upper_stmt->get_order_item_size()) {
    can_push = false;
  } else if (0 < upper_stmt->get_condition_size() || upper_stmt->has_distinct() || upper_stmt->has_group_by() ||
             !upper_stmt->is_single_table_stmt() || upper_stmt->has_having() || upper_stmt->get_aggr_item_size() != 0 ||
             upper_stmt->has_window_function() || upper_stmt->is_hierarchical_query()) {
    can_push = false;
  } else if (OB_ISNULL(upper_stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(upper_stmt->get_table_item(0)));
  } else {
    can_push = true;
    const uint64_t table_id = upper_stmt->get_table_item(0)->table_id_;
    for (int64_t i = 0; OB_SUCC(ret) && can_push && i < upper_stmt->get_order_item_size(); ++i) {
      ObRawExpr* order_expr = upper_stmt->get_order_item(i).expr_;
      int64_t pos = OB_INVALID_ID;
      if (OB_ISNULL(order_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(order_expr));
      } else if (!order_expr->is_column_ref_expr() ||
                 table_id != static_cast<ObColumnRefRawExpr*>(order_expr)->get_table_id()) {
        can_push = false;
      } else if (FALSE_IT(pos = static_cast<ObColumnRefRawExpr*>(order_expr)->get_column_id() - OB_APP_MIN_COLUMN_ID)) {
        /*do nothing*/
      } else if (OB_UNLIKELY(pos < 0 || pos >= stmt->get_select_item_size())) {
        can_push = false;
        LOG_DEBUG("column not in stmt");
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformSetOp::add_limit_order_for_union(
    const common::ObIArray<ObParentDMLStmt>& parent_stmts, ObSelectStmt*& stmt)
{
  int ret = OB_SUCCESS;
  bool can_push = false;
  ObDMLStmt* parent_stmt = parent_stmts.empty() ? NULL : parent_stmts.at(parent_stmts.count() - 1).stmt_;
  if (OB_ISNULL(stmt) || !stmt->has_set_op() || ObSelectStmt::UNION != stmt->get_set_op()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, stmt is not union set", K(stmt), K(ret));
  } else if (OB_NOT_NULL(parent_stmt) && parent_stmt->is_select_stmt() &&
             !static_cast<ObSelectStmt*>(parent_stmt)->has_set_op()) {
    ObSelectStmt* upper_stmt = static_cast<ObSelectStmt*>(parent_stmt);
    if (OB_FAIL(check_can_pre_push(stmt, upper_stmt, can_push))) {
      LOG_WARN("failed to check can pre push", K(ret));
    } else if (can_push) {
      ObSEArray<ObRawExpr*, 16> old_order_exprs;
      ObSEArray<ObRawExpr*, 16> new_order_exprs;
      stmt->get_order_items().reset();
      if (OB_FAIL(append(stmt->get_order_items(), upper_stmt->get_order_items()))) {
        LOG_WARN("failed to append order item", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < upper_stmt->get_order_item_size(); ++i) {
          if (OB_FAIL(old_order_exprs.push_back(upper_stmt->get_order_items().at(i).expr_))) {
            LOG_WARN("failed to append order item", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(old_order_exprs, *stmt, new_order_exprs))) {
            LOG_WARN("failed to convert columnexpr to select expr", K(ret));
          } else if (OB_FAIL(ObTransformUtils::replace_expr_for_order_item(
                         old_order_exprs, new_order_exprs, stmt->get_order_items()))) {
            LOG_WARN("failed to replace expr for order item", K(ret));
          } else {
            upper_stmt->get_order_items().reset();
            stmt->set_limit_offset(upper_stmt->get_limit_expr(), upper_stmt->get_offset_expr());
            stmt->set_limit_percent_expr(upper_stmt->get_limit_percent_expr());
            stmt->set_fetch_with_ties(upper_stmt->is_fetch_with_ties());
            stmt->set_has_fetch(upper_stmt->has_fetch());
            upper_stmt->set_limit_offset(NULL, NULL);
            upper_stmt->set_limit_percent_expr(NULL);
            upper_stmt->set_fetch_with_ties(false);
            upper_stmt->set_has_fetch(false);
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformSetOp::add_limit_order_distinct_for_union(
    const common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_calc = false;
  ObSelectStmt* select_stmt = NULL;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer passed to transform", K(stmt), K(ret));
  } else if (stmt->is_select_stmt() && static_cast<ObSelectStmt*>(stmt)->has_set_op() &&
             ObSelectStmt::UNION == static_cast<ObSelectStmt*>(stmt)->get_set_op()) {
    select_stmt = static_cast<ObSelectStmt*>(stmt);
    if (OB_FAIL(is_calc_found_rows_for_union(parent_stmts, stmt, is_calc))) {
      LOG_WARN("fail to judge calc found rows for union", K(ret));
    } else if (is_calc) {
      /*do nothing*/
    } else if (!select_stmt->has_limit() && OB_FAIL(add_limit_order_for_union(parent_stmts, select_stmt))) {
      LOG_WARN("failed to get limit, order from parent", K(ret));
    } else if (select_stmt->has_limit()) {
      bool can_push = false;
      ObSelectStmt* child_stmt = NULL;
      ObIArray<ObSelectStmt*>& child_stmts = select_stmt->get_set_query();
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
        if (OB_ISNULL(child_stmt = child_stmts.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (OB_FAIL(check_can_push(child_stmt, select_stmt, can_push))) {
          LOG_WARN("Failed to check left stmt", K(ret));
        } else if (!can_push) {
          /*do nothing*/
        } else if (OB_FAIL(add_distinct(child_stmt, select_stmt))) {
          LOG_WARN("Failed to add distinct to left stmt", K(ret));
        } else if (OB_FAIL(add_limit(child_stmt, select_stmt))) {
          LOG_WARN("Failed to add limit to left stmt", K(ret));
        } else if (OB_FAIL(add_order_by(child_stmt, select_stmt))) {
          LOG_WARN("Failed to add order by to left stmt", K(ret));
        } else {
          trans_happened = true;
        }
      }
    }
  }
  return ret;
}

int ObTransformSetOp::is_calc_found_rows_for_union(
    const common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& is_calc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, stmt is NULL", K(stmt), K(ret));
  } else {
    ObSelectStmt* top_union_stmt = NULL;
    if (stmt->is_select_stmt() && stmt->is_set_stmt() &&
        ObSelectStmt::UNION == static_cast<ObSelectStmt*>(stmt)->get_set_op()) {
      top_union_stmt = static_cast<ObSelectStmt*>(stmt);
    } else { /* do nothing*/
    }
    for (int64_t i = parent_stmts.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      ObDMLStmt* stmt = NULL;
      if (OB_ISNULL(stmt = parent_stmts.at(i).stmt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt is null", K(ret));
      } else if (stmt->is_select_stmt() && stmt->is_set_stmt() &&
                 ObSelectStmt::UNION == static_cast<ObSelectStmt*>(stmt)->get_set_op()) {
        top_union_stmt = static_cast<ObSelectStmt*>(stmt);
      } else {
        break;
      }
    }
    if (OB_SUCC(ret) && NULL != top_union_stmt) {
      is_calc = top_union_stmt->is_calc_found_rows() ? true : false;
    }
  }
  return ret;
}

int ObTransformSetOp::recursive_adjust_order_by(ObSelectStmt* stmt, ObRawExpr* cur_expr, ObRawExpr*& find_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr* set_expr = NULL;
  int64_t idx = -1;
  if (OB_ISNULL(stmt) || OB_ISNULL(cur_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, expr is NULL", K(ret));
  } else if (OB_ISNULL(set_expr = ObTransformUtils::get_expr_in_cast(cur_expr)) ||
             OB_UNLIKELY(!set_expr->is_set_op_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected set expr", K(ret), K(set_expr));
  } else if (OB_FALSE_IT(idx = static_cast<ObSetOpRawExpr*>(set_expr)->get_idx())) {
  } else if (idx < 0 || idx >= stmt->get_select_item_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("union expr param count is wrong", K(ret), K(idx), K(stmt->get_select_item_size()));
  } else {
    find_expr = stmt->get_select_item(idx).expr_;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
