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
#include "sql/rewrite/ob_transform_simplify_limit.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

using namespace oceanbase::sql;

int ObTransformSimplifyLimit::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                 ObDMLStmt *&stmt,
                                                 bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  UNUSED(parent_stmts);
  if (OB_FAIL(add_limit_to_semi_right_table(stmt, is_happened))) {
    LOG_WARN("failed to add limit to semi right table", K(ret));
  } else {
    trans_happened = is_happened;
    OPT_TRACE("add limit to semi right table:", is_happened);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(pushdown_limit_offset(stmt, is_happened))) {
      LOG_WARN("failed to push down limit offset", K(ret));
    } else {
      trans_happened = (trans_happened || is_happened);
      OPT_TRACE("push down limit offset:", is_happened);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(pushdown_limit_order_for_union(stmt, is_happened))) {
      LOG_WARN("failed to push down limit order for union", K(ret));
    } else {
      trans_happened = (trans_happened || is_happened);
      OPT_TRACE("push down limit order for union:", is_happened);
    }
  }

  if (OB_SUCC(ret) && trans_happened) {
    if (OB_FAIL(add_transform_hint(*stmt))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }
  return ret;
}

// if no use semi right table in semi conditions, add limit 1 to semi right table
int ObTransformSimplifyLimit::add_limit_to_semi_right_table(ObDMLStmt *stmt,
                                                            bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else {
    bool need_add = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_semi_info_size(); ++i) {
      if (OB_FAIL(check_need_add_limit_to_semi_right_table(stmt, stmt->get_semi_infos().at(i),
                                                           need_add))) {
        LOG_WARN("failed to check ", K(ret), K(stmt));
      } else if (!need_add) {
        /* do nothing */
      } else if (OB_FAIL(ObTransformUtils::add_limit_to_semi_right_table(stmt, ctx_,
                                                                         stmt->get_semi_infos().at(i)))) {
        LOG_WARN("failed to add limit to semi right table", K(ret), K(*stmt));
      } else {
        trans_happened = true;
      }
    }
  }
  return ret;
}

int ObTransformSimplifyLimit::check_need_add_limit_to_semi_right_table(ObDMLStmt *stmt,
                                                                       SemiInfo *semi_info,
                                                                       bool &need_add)
{
  int ret = OB_SUCCESS;
  need_add = true;
  TableItem *right_table = NULL;
  ObSelectStmt *ref_query = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(semi_info) ||
      OB_ISNULL(right_table = stmt->get_table_item_by_id(semi_info->right_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(semi_info));
  } else if (!right_table->is_generated_table()) {
    need_add = !right_table->is_link_type();
  } else if (OB_ISNULL(ref_query = right_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ref_query));
  } else if (NULL != ref_query->get_limit_expr() ||
             NULL != ref_query->get_limit_percent_expr()) {
    need_add = false;
  }
  if (OB_SUCC(ret)) {
    ObRawExpr *expr = NULL;
    int64_t right_idx = stmt->get_table_bit_index(semi_info->right_table_id_);
    for (int64_t i = 0; OB_SUCC(ret) && need_add && i < semi_info->semi_conditions_.count(); ++i) {
      if (OB_ISNULL(expr = semi_info->semi_conditions_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret), K(expr));
      } else if (expr->get_relation_ids().has_member(right_idx)) {
        need_add = false;
      }
    }
  }
  return ret;
}

/**
 * view merge 支持 view_stmt select 包含子查询的合并后需要移除此改写
 *
 * @brief ObTransformSimplifyLimit::pushdown_limit_offset
 * 下推 limit offset, 修改 upper stmt 中 rownum
 * 为了使 view select 中 subquery1 减少无效计算
 * select rownum rn, v.* from (select subquery1 from t) v offset 10 rows;
 * => select rownum + 10 rn, v.* from (select subquery1 from t offset 10 rows) v;
 */
int ObTransformSimplifyLimit::pushdown_limit_offset(ObDMLStmt *stmt,
                                                    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSelectStmt *sel_stmt = NULL;
  ObSelectStmt *view_stmt = NULL;
  bool is_valid = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(ctx_));
  } else if (!is_oracle_mode() || !stmt->is_select_stmt()) {
    /*do nothing*/
  } else if (FALSE_IT(sel_stmt = static_cast<ObSelectStmt *>(stmt))) {
  } else if (OB_FAIL(check_pushdown_limit_offset_validity(sel_stmt, view_stmt, is_valid))) {
    LOG_WARN("failed to check pushdown limit offset validity", K(ret));
  } else if (!is_valid) {
    /*do nothing*/
  } else if (OB_FAIL(do_pushdown_limit_offset(sel_stmt, view_stmt))) {
    LOG_WARN("failed to do pushdown limit offset", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformSimplifyLimit::check_pushdown_limit_offset_validity(ObSelectStmt *upper_stmt,
                                                                   ObSelectStmt *&view_stmt,
                                                                   bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  view_stmt = NULL;
  TableItem *table = NULL;
  if (OB_ISNULL(upper_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret));
  } else if (!upper_stmt->is_single_table_stmt()
             || OB_ISNULL(table = upper_stmt->get_table_item(0))
             || !table->is_generated_table()
             || OB_ISNULL(view_stmt = table->ref_query_)
             || view_stmt->is_hierarchical_query()) {
    is_valid = false;
  } else if (!upper_stmt->has_limit()
             || NULL == upper_stmt->get_offset_expr()
             || NULL != upper_stmt->get_limit_percent_expr()
             || NULL != view_stmt->get_limit_percent_expr()
             || upper_stmt->is_fetch_with_ties()
             || view_stmt->is_fetch_with_ties()) {
    is_valid = false;
  } else if (0 != upper_stmt->get_condition_size()
             || upper_stmt->has_group_by()
             || upper_stmt->has_rollup()
             || upper_stmt->has_window_function()
             || upper_stmt->has_distinct()
             || upper_stmt->has_sequence()
             || upper_stmt->has_order_by()) {
    is_valid = false;
  } else {
    is_valid = true;
  }
  return ret;
}

int ObTransformSimplifyLimit::do_pushdown_limit_offset(ObSelectStmt *upper_stmt,
                                                       ObSelectStmt *view_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(upper_stmt) || OB_ISNULL(view_stmt) || OB_ISNULL(ctx_)
      || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    ObRawExpr *view_limit = view_stmt->get_limit_expr();
    ObRawExpr *upper_limit = upper_stmt->get_limit_expr();
    ObRawExpr *view_offset = view_stmt->get_offset_expr();
    ObRawExpr *upper_offset = upper_stmt->get_offset_expr();
    ObRawExpr *limit_expr = NULL;
    ObRawExpr *offset_expr = NULL;
    if (OB_FAIL(ObTransformUtils::merge_limit_offset(ctx_, view_limit, upper_limit,
                                                     view_offset, upper_offset,
                                                     limit_expr, offset_expr))) {
      LOG_WARN("failed to merge limit offset", K(ret));
    } else {
      ObRawExpr *rownum_expr = NULL;
      ObOpRawExpr *add_expr = NULL;
      ObSEArray<ObRawExpr*, 1> old_expr;
      ObSEArray<ObRawExpr*, 1> new_expr;
      upper_stmt->set_limit_offset(upper_limit, NULL);
      upper_stmt->set_fetch_with_ties(false);
      upper_stmt->set_limit_percent_expr(NULL);
      upper_stmt->set_has_fetch(false);
      view_stmt->set_limit_offset(limit_expr, offset_expr);
      if (OB_FAIL(upper_stmt->get_rownum_expr(rownum_expr))) {
        LOG_WARN("failed to get rownum expr", K(ret));
      } else if (NULL == rownum_expr || NULL == upper_offset) {
        /*do nothing*/
      } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_ADD, add_expr))) {
        LOG_WARN("create add op expr failed", K(ret));
      } else if (OB_ISNULL(add_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("add expr is null");
      } else if (OB_FAIL(old_expr.push_back(rownum_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(new_expr.push_back(add_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(add_expr->set_param_exprs(rownum_expr, upper_offset))) {
        LOG_WARN("set param exprs failed", K(ret));
      } else if (OB_FAIL(add_expr->formalize(ctx_->session_info_))) {
        LOG_WARN("formalize add operator failed", K(ret));
      } else if (OB_FAIL(upper_stmt->replace_relation_exprs(old_expr, new_expr))) {
        LOG_WARN("failed to replace expr", K(ret));
      }
    }
  }
  return ret;
}

/**
 * try push down limit and order by from non set stmt to union set stmt.
 */
int ObTransformSimplifyLimit::pushdown_limit_order_for_union(ObDMLStmt *stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool can_push = false;
  ObSelectStmt* set_stmt = nullptr;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else {
    ObSelectStmt* sel_stmt = static_cast<ObSelectStmt*>(stmt);
    if (OB_FAIL(check_can_pushdown_limit_order(*sel_stmt, set_stmt, can_push))) {
      LOG_WARN("failed to check can pre push", K(ret));
    } else if(!can_push) {
      // do nothing
    } else if (OB_FAIL(do_pushdown_limit_order_for_union(*sel_stmt, set_stmt))) {
      LOG_WARN("failed to pushdown limit order for union", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformSimplifyLimit::check_can_pushdown_limit_order(ObSelectStmt& upper_stmt,
                                                             ObSelectStmt*& view_stmt,
                                                             bool& can_push)
{
  int ret = OB_SUCCESS;
  can_push = false;
  TableItem* table = nullptr;
  if (!upper_stmt.is_single_table_stmt() ||
      OB_ISNULL(table = upper_stmt.get_table_item(0)) ||
      !table->is_generated_table() ||
      OB_ISNULL(view_stmt = table->ref_query_) ||
      view_stmt->is_hierarchical_query() || 
      !view_stmt->is_set_stmt() ||
      ObSelectStmt::UNION != view_stmt->get_set_op()) {
    can_push = false;
  } else if (view_stmt->has_limit() ||
             NULL == upper_stmt.get_limit_expr() ||
             NULL != upper_stmt.get_limit_percent_expr() ||
             upper_stmt.has_fetch()) {
    can_push = false;
  } else if (0 < upper_stmt.get_condition_size() ||
             upper_stmt.has_group_by() ||
             upper_stmt.has_rollup() ||
             upper_stmt.has_window_function() ||
             upper_stmt.has_distinct() ||
             upper_stmt.has_sequence()) {
    can_push = false;
  } else {
    // only push down generated table column in order by
    can_push = true;
    const uint64_t table_id = table->table_id_;
    ObRawExpr *order_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && can_push && i < upper_stmt.get_order_item_size(); ++i) {
      if (OB_ISNULL(order_expr = upper_stmt.get_order_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(order_expr));
      } else if (!order_expr->is_column_ref_expr() ||
                table_id != static_cast<ObColumnRefRawExpr*>(order_expr)->get_table_id()) {
        can_push = false;
      }
    }
  }
  return ret;
}

int ObTransformSimplifyLimit::do_pushdown_limit_order_for_union(ObSelectStmt& upper_stmt,
                                                                ObSelectStmt* view_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(view_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(view_stmt));
  } else {
    view_stmt->set_limit_offset(upper_stmt.get_limit_expr(), upper_stmt.get_offset_expr());
    upper_stmt.set_limit_offset(NULL, NULL);
    if (upper_stmt.get_order_items().empty()) {
      // do nothing
    } else if (OB_FAIL(view_stmt->get_order_items().assign(upper_stmt.get_order_items()))) {
      LOG_WARN("failed to assign order items", K(ret));
    } else {
      ObRawExpr* expr = NULL;
      int64_t pos = OB_INVALID_INDEX;
      for (int64_t i = 0; OB_SUCC(ret) && i < upper_stmt.get_order_item_size(); ++i) {
        if (OB_ISNULL(expr = upper_stmt.get_order_item(i).expr_) ||
            OB_UNLIKELY(!expr->is_column_ref_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected expr", K(ret), KPC(expr));
        } else if (FALSE_IT(pos = static_cast<ObColumnRefRawExpr*>(expr)->get_column_id()
                                  - OB_APP_MIN_COLUMN_ID)) {
          /*do nothing*/
        } else if (OB_UNLIKELY(pos < 0 || pos >= view_stmt->get_select_item_size())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid array pos", K(pos), K(view_stmt->get_select_item_size()), K(ret));
        } else {
          view_stmt->get_order_item(i).expr_ = view_stmt->get_select_item(pos).expr_;
        }
      }
      if (OB_SUCC(ret)) {
        upper_stmt.get_order_items().reset();
      }
    }
  }
  return ret;
}