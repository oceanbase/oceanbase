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
#include "sql/rewrite/ob_transform_simplify_orderby.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
using namespace oceanbase::sql;

/*@brief, remove_stmt_order_by消除stmt中可以消除的order by, eg:
* select * from t1 where c1 in (select c1 from t2 order by c2);
* ==>
* select * from t1 where c1 in (select c1 from t2);
*
* select distinct c1 from (select c1 from t1 order by c2);
* ==>
* select distinct c1 from (select c1 from t1);
*
* select  * from (select * from t1 order by c1) order by c2;
* ==>
* select  * from (select * from t1) order by c2;
*
* select c1 from (select c1 from t1 order by c2) group by c1;
* ==>
* select c1 from (select c1 from t1) group by c1;
*
* select c1 from t1 where c2 in (select c2 from t2 order by c1);
* ==>
* select c1 from t1 where c2 in (select c2 from t2);
*/
int ObTransformSimplifyOrderby::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                   ObDMLStmt *&stmt,
                                                   bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool remove_duplicates = false;
  bool subquery_happened = false;
  bool view_happened = false;
  bool set_happened = false;
  bool force_serial_set_order = false;
  trans_happened = false;
  UNUSED(parent_stmts);
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret), K(stmt), K(ctx_), K(ctx_->session_info_));
  } else if (OB_FAIL(ctx_->session_info_->is_serial_set_order_forced(force_serial_set_order, lib::is_oracle_mode()))) {
    LOG_WARN("fail to get force_serial_set_order value", K(ret));
  } else if (OB_FAIL(remove_order_by_for_subquery(stmt, subquery_happened))) {
    LOG_WARN("remove order by for subquery failed");
  } else if (OB_FAIL(remove_order_by_for_view_stmt(stmt, view_happened, force_serial_set_order))) {
    LOG_WARN("remove order by for subquery failed");
  } else if (OB_FAIL(remove_order_by_for_set_stmt(stmt, set_happened, force_serial_set_order))) {
    LOG_WARN("failed to remove order by for set stmt", K(ret));
  } else if (OB_FAIL(remove_order_by_duplicates(stmt, remove_duplicates))) {
    LOG_WARN("failed to remove order by duplicates", K(ret));
  } else {
    trans_happened = (subquery_happened || view_happened || remove_duplicates || set_happened);
    OPT_TRACE("remove order by for subquery:", subquery_happened);
    OPT_TRACE("remove order by for view:", view_happened);
    OPT_TRACE("remove order by duplicates:", remove_duplicates);
    OPT_TRACE("remove order by for set stmt:", set_happened);
  }
  if (OB_SUCC(ret) && trans_happened) {
    if (OB_FAIL(add_transform_hint(*stmt))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }
  return ret;
}

// for select/update/delete/insert
// 消除subquery中的order by子句
int ObTransformSimplifyOrderby::remove_order_by_for_subquery(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null");
  } else {
    ObIArray<ObQueryRefRawExpr*> &subquery_exprs = stmt->get_subquery_exprs();
    ObSelectStmt *subquery = NULL;
    bool happened = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs.count(); ++i) {
      ObQueryRefRawExpr *query_ref = subquery_exprs.at(i);
      if (OB_ISNULL(query_ref) || OB_ISNULL(subquery = query_ref->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subquery reference is invalid", K(ret), K(query_ref));
      } else if (!subquery->has_limit() &&
                 !subquery->is_contains_assignment() &&
                 !subquery->is_hierarchical_query() &&
                 subquery->has_order_by() &&
                 OB_FAIL(do_remove_stmt_order_by(subquery, happened))) {
        LOG_WARN("do remove stmt order by failed", K(ret));
      } else {
        trans_happened |= happened;
      }
    }
  }
  return ret;
}

int ObTransformSimplifyOrderby::remove_order_by_for_view_stmt(ObDMLStmt *stmt, bool &trans_happened, bool &force_serial_set_order)
{
  int ret = OB_SUCCESS;
  bool can_remove = false;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  //select stmt不能有window function,eg:
  // SELECT last_value(c1) OVER (PARTITION BY c2) FROM (SELECT * FROM t1 ORDER BY c1, c2)s
  // ORDER BY c1, c2;
  } else if (!stmt->is_sel_del_upd()) {
    /*do nothing*/
  } else if (stmt->is_select_stmt() && static_cast<ObSelectStmt*>(stmt)->has_window_function()) {
    /*do nothing*/
  } else if (stmt->is_select_stmt() && is_oracle_mode()) {
    common::ObIArray<ObAggFunRawExpr*> &aggr_items = static_cast<ObSelectStmt*>(stmt)->get_aggr_items();
    can_remove = true;
    for (int64_t i = 0; OB_SUCC(ret) && can_remove && i < aggr_items.count(); ++i) {
      if (OB_ISNULL(aggr_items.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(aggr_items));
      } else if (T_FUN_WM_CONCAT == aggr_items.at(i)->get_expr_type()
                || T_FUN_GROUP_CONCAT == aggr_items.at(i)->get_expr_type()) {
        can_remove = false;
      }
    }
  } else {
    can_remove = true;
  }
  if (OB_SUCC(ret) && can_remove) {
    ObSelectStmt *view_stmt = NULL;
    ObSelectStmt *select_stmt = NULL;
    bool happened = false;
    ObIArray<TableItem *> &table_items = stmt->get_table_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
      const TableItem * table_item = table_items.at(i);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("joined table is null", K(ret));
      } else if (!table_item->is_generated_table() &&
                 !table_item->is_lateral_table()) {
        /*do nothing*/
      } else if (OB_ISNULL(view_stmt = table_item->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("view stmt is null", K(ret));
      } else if (view_stmt->is_contains_assignment() ||
                 view_stmt->is_hierarchical_query() ||
                 (force_serial_set_order && view_stmt->is_set_stmt())) {
        // do nothing
      } else if (1 == table_items.count()) {
        if (stmt->is_select_stmt()) {
          bool has_rownum = false;
          select_stmt = static_cast<ObSelectStmt*>(stmt);
          if (OB_FAIL(select_stmt->has_rownum(has_rownum))) {
            LOG_WARN("failed to has rownum", K(ret));
          } else if (has_rownum) {
            /*do nothing*/
          } else if ((select_stmt->has_group_by() || select_stmt->has_distinct() ||
                     select_stmt->has_order_by()) && !view_stmt->has_limit() &&
                     view_stmt->has_order_by() &&
                     OB_FAIL(do_remove_stmt_order_by(view_stmt, happened))) {
            LOG_WARN("do remove stmt order by failed", K(ret));
          } else {
            trans_happened |= happened;
          }
        }
      } else if (!view_stmt->has_limit() && view_stmt->has_order_by() &&
                 OB_FAIL(do_remove_stmt_order_by(view_stmt, happened))) {
        LOG_WARN("do remove stmt order by failed", K(ret));
      } else {
        trans_happened |= happened;
      }
    }
  }
  return ret;
}

int ObTransformSimplifyOrderby::do_remove_stmt_order_by(ObSelectStmt *select_stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSEArray<OrderItem, 4> new_order_items;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(select_stmt), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_order_item_size(); ++i) {
    const OrderItem &order_item = select_stmt->get_order_item(i);
    if (OB_ISNULL(order_item.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("order expr is null");
    } else if (order_item.expr_->has_flag(CNT_SUB_QUERY)) {
      // in case the subquery returns more than one rows,
      // we should keep the subquery which may returns error
      if (OB_FAIL(new_order_items.push_back(order_item))) {
        LOG_WARN("store new order item failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && select_stmt->get_order_item_size() != new_order_items.count()) {
    trans_happened = true;
    if (OB_FAIL(select_stmt->get_order_items().assign(new_order_items))) {
      LOG_WARN("assign new order items failed", K(ret));
    }
  }
  return ret;
}

int ObTransformSimplifyOrderby::remove_order_by_duplicates(ObDMLStmt *stmt,
                                                           bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer passed to transform", K(ret));
  } else if (!stmt->is_sel_del_upd() ||
             (stmt->is_select_stmt() && (static_cast<ObSelectStmt*>(stmt)->has_rollup()))) {
    //do nothing
  } else {
    ObIArray<OrderItem> &order_items = stmt->get_order_items();
    ObIArray<ObRawExpr*> &cond_exprs = stmt->get_condition_exprs();
    if (order_items.count() > 1) {
      ObArray<OrderItem> new_order_items;
      int64_t N = order_items.count();
      int64_t M = cond_exprs.count();
      bool is_exist = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        OrderItem &cur_item = order_items.at(i);
        is_exist = false;
        if (OB_FAIL(exist_item_by_expr(cur_item.expr_, new_order_items, is_exist))) {
          LOG_WARN("fail to adjust exist order item", K(ret), K(cur_item), K(new_order_items), K(is_exist));
        } else if (!is_exist) {
          for (int64_t j = 0; OB_SUCC(ret) && !is_exist && j < M; ++j) {
            ObRawExpr *op_expr = cond_exprs.at(j);
            if (OB_ISNULL(op_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("null pointer in condtion exprs", K(ret));
            } else if (T_OP_EQ == op_expr->get_expr_type()) {
              if (op_expr->get_param_count() != 2) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("eq expr should have two param", K(ret));
              } else {
                ObRawExpr *left_param = op_expr->get_param_expr(0);
                ObRawExpr *right_param = op_expr->get_param_expr(1);
                if (OB_ISNULL(left_param) || OB_ISNULL(right_param)) {
                 ret = OB_ERR_UNEXPECTED;
                 LOG_WARN("null pointer in condtion exprs", K(ret));
                } else {
                  bool is_consistent = false;
                  if (right_param == cur_item.expr_) {
                    //因为判断expr的类型的序是否可传递是有方向的，所以要情况调用，这里待确定的表达式位于等号的右端
                    if (OB_FAIL(ObRawExprUtils::expr_is_order_consistent(left_param, right_param, is_consistent))) {
                      LOG_WARN("check expr is order consistent failed", K(ret), K(*left_param), K(*right_param));
                    } else if (is_consistent) {
                      if (OB_FAIL(exist_item_by_expr(left_param, new_order_items, is_exist))) {
                        LOG_WARN("fail to find expr in items", K(ret));
                      }
                    }
                  } else if (left_param == cur_item.expr_) {
                    //因为判断expr的类型的序是否可传递是有方向的，所以要情况调用，这里待确定的表达式位于等号的左端
                    if (OB_FAIL(ObRawExprUtils::expr_is_order_consistent(right_param, left_param, is_consistent))) {
                      LOG_WARN("check expr is order consistent failed", K(ret), K(*left_param), K(*right_param));
                    } else if (is_consistent) {
                      if (OB_FAIL(exist_item_by_expr(right_param, new_order_items, is_exist))) {
                        LOG_WARN("fail to find expr in items", K(ret));
                      }
                    }
                  } else {/* do nothing */}
                }
              }
            } else {/* do nothing */}
          }
        } else {/*do nothing*/}
        if (OB_SUCC(ret) && !is_exist) {
          if (OB_FAIL(new_order_items.push_back(cur_item))) {
            LOG_WARN("failed to push back order item", K(ret), K(cur_item));
          }
        }
      }// for
      if (OB_SUCC(ret)) {
        if (N != new_order_items.count()) {
          trans_happened = true;
        }
        if (OB_FAIL(order_items.assign(new_order_items))) {
          LOG_WARN("failed to reset order items", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformSimplifyOrderby::exist_item_by_expr(ObRawExpr *expr, ObIArray<OrderItem> &order_items, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  int64_t N = order_items.count();
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current order item expr is NULL", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < N && !is_exist; ++i) {
      OrderItem& this_item = order_items.at(i);
      if (OB_ISNULL(this_item.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("order item expr is NULL", K(ret));
      } else if (this_item.expr_ == expr) {
        is_exist = true;
      } else if (this_item.expr_->same_as(*expr)) {
        is_exist = true;
      }
    }
  }
  return ret;
}

int ObTransformSimplifyOrderby::remove_order_by_for_set_stmt(ObDMLStmt *&stmt, bool &trans_happened, bool &force_serial_set_order)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret), K(stmt), K(ctx_), K(ctx_->session_info_));
  } else if (!(stmt->is_select_stmt() && static_cast<ObSelectStmt*>(stmt)->is_set_stmt())) {
    //not set stmt, do nothing
  } else if (force_serial_set_order &&
             ObSelectStmt::UNION == static_cast<ObSelectStmt*>(stmt)->get_set_op() &&
             !static_cast<ObSelectStmt*>(stmt)->is_set_distinct()) {
    //under oracle mode and force_serial_set_order, union-all do not remove order items in its child stmt
  } else {
    ObIArray<ObSelectStmt*> &child_stmts = static_cast<ObSelectStmt*>(stmt)->get_set_query();
    ObSelectStmt *child_stmt = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      bool happened = false;
      if (OB_ISNULL(child_stmt = child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt is NULL", K(stmt), K(child_stmt), K(ret));
      } else if (child_stmt->is_contains_assignment() ||
                 child_stmt->is_hierarchical_query()) {
        // do nothing
      } else if (!child_stmt->has_limit() &&
                 child_stmt->has_order_by() &&
                 OB_FAIL(do_remove_stmt_order_by(child_stmt, happened))) {
        LOG_WARN("do remove stmt order by failed", K(ret));
      } else {
        trans_happened |= happened;
      }
    }
  }
  return ret;
}
