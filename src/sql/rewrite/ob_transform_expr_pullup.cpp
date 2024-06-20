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
#include "ob_transform_expr_pullup.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "common/ob_smart_call.h"

namespace oceanbase {
namespace sql {
using namespace common;

/**
 * @brief Pullup select exprs from the inline view to current stmt
 */
int ObTransformExprPullup::transform_one_stmt(ObIArray<ObParentDMLStmt> &parent_stmts,
                                              ObDMLStmt *&stmt,
                                              bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  ObSEArray<ObSelectStmt *, 4> transformed_views;
  bool is_valid = false;

  if (OB_FAIL(check_stmt_validity(stmt, is_valid))) {
    LOG_WARN("fail check stmt validity", K(ret));
  } else if (is_valid) {
    ObSelectStmt &select_stmt = static_cast<ObSelectStmt &>(*stmt);
    bool stmt_may_reduce_row_count = false;
    //for exprs in the first calcing scope which may reduce result row count of the stmt
    ObExprNodeMap parent_reject_expr_map;
    ObExprNodeMap parent_reject_subquery_map;

    if (OB_FAIL(is_stmt_may_reduce_row_count(select_stmt, stmt_may_reduce_row_count))) {
      LOG_WARN("fail to check if stmt may reduce row count", K(ret));
    } else if (OB_FAIL(build_parent_reject_exprs_map(select_stmt,
                                                     parent_reject_expr_map,
                                                     parent_reject_subquery_map))) {
      LOG_WARN("fail to build parent expr map", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_item_size(); ++i) {
      FromItem &from_item = stmt->get_from_item(i);
      TableItem *table_item = NULL;
      if (from_item.is_joined_) {
        table_item = stmt->get_joined_table(from_item.table_id_);
      } else {
        table_item = stmt->get_table_item_by_id(from_item.table_id_);
      }
      if (OB_FAIL(transform_view_recursively(table_item,
                                             select_stmt,
                                             parent_reject_expr_map,
                                             parent_reject_subquery_map,
                                             transformed_views,
                                             stmt_may_reduce_row_count,
                                             trans_happened))) {
        LOG_WARN("fail to transform view recursively", K(ret));
      }
    } //end for

    if (OB_SUCC(ret) && trans_happened) {
      if (OB_FAIL(add_transform_hint(select_stmt, &transformed_views))) {
        LOG_WARN("failed to add transform hint", K(ret));
      }
    }

  }

  return ret;
}

int ObTransformExprPullup::transform_one_stmt_with_outline(
    ObIArray<ObParentDMLStmt> &parent_stmts,
    ObDMLStmt *&stmt,
    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  ObSEArray<ObSelectStmt *, 4> transformed_views;
  bool is_valid = false;

  if (OB_FAIL(check_stmt_validity(stmt, is_valid))) {
    LOG_WARN("fail check stmt validity", K(ret));
  } else if (is_valid) {
    ObSelectStmt &select_stmt = static_cast<ObSelectStmt &>(*stmt);
    bool stmt_may_reduce_row_count = false;
    bool transform_happend_for_current_outline = false;
    //for exprs in the first calcing scope which may reduce result row count of the stmt
    ObExprNodeMap parent_reject_expr_map;
    ObExprNodeMap parent_reject_subquery_map;

    if (OB_FAIL(is_stmt_may_reduce_row_count(select_stmt, stmt_may_reduce_row_count))) {
      LOG_WARN("fail to check if stmt may reduce row count", K(ret));
    } else if (OB_FAIL(build_parent_reject_exprs_map(select_stmt,
                                                     parent_reject_expr_map,
                                                     parent_reject_subquery_map))) {
      LOG_WARN("fail to build parent expr map", K(ret));
    }

    do {
      transform_happend_for_current_outline = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_item_size(); ++i) {
        FromItem &from_item = stmt->get_from_item(i);
        TableItem *table_item = NULL;
        if (from_item.is_joined_) {
          table_item = stmt->get_joined_table(from_item.table_id_);
        } else {
          table_item = stmt->get_table_item_by_id(from_item.table_id_);
        }
        if (OB_FAIL(transform_view_recursively(table_item,
                                               select_stmt,
                                               parent_reject_expr_map,
                                               parent_reject_subquery_map,
                                               transformed_views,
                                               stmt_may_reduce_row_count,
                                               transform_happend_for_current_outline))) {
          LOG_WARN("fail to transform view recursively", K(ret));
        }
      } //end for
      if (transform_happend_for_current_outline) {
        trans_happened = true;
        ++ctx_->trans_list_loc_;
      }
    } while (OB_SUCC(ret) && transform_happend_for_current_outline);

    if (OB_SUCC(ret) && trans_happened) {
      if (OB_FAIL(add_transform_hint(select_stmt, &transformed_views))) {
        LOG_WARN("failed to add transform hint", K(ret));
      }
    }

  }
  return ret;
}

int ObTransformExprPullup::need_transform(const ObIArray<ObParentDMLStmt> &parent_stmts,
                                          const int64_t current_level,
                                          const ObDMLStmt &stmt,
                                          bool &need_trans)
{
  UNUSED(current_level);
  UNUSED(parent_stmts);
  int ret = OB_SUCCESS;
  bool is_valid = false;
  need_trans = false;

  if (OB_FAIL(check_stmt_validity(&stmt, is_valid))) {
    LOG_WARN("fail check stmt validity", K(ret));
  } else if (is_valid) {
    const ObSelectStmt &select_stmt = static_cast<const ObSelectStmt &>(stmt);
    const ObQueryHint *query_hint = NULL;
    if (OB_ISNULL(ctx_) || OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), KP(ctx_), KP(query_hint));
    } else if (!query_hint->has_outline_data()) {
      need_trans = true;
    } else {
      const ObHint *outline_hint = NULL;
      //acting according to outline
      if (OB_NOT_NULL(outline_hint = query_hint->get_outline_trans_hint(ctx_->trans_list_loc_))) {
        if (outline_hint->get_hint_type() != get_hint_type()) {
          need_trans = false;
          OPT_TRACE("outline reject transform");
        } else {
          for (int64_t i = 0; !need_trans && OB_SUCC(ret) && i < stmt.get_table_size(); ++i) {
            const TableItem *table = NULL;
            if (OB_ISNULL(table = stmt.get_table_item(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("table item is null", K(ret));
            } else if (!table->is_generated_table() || OB_ISNULL(table->ref_query_)) {
              //continue
            } else {
              need_trans = query_hint->is_valid_outline_transform(
                ctx_->trans_list_loc_, get_hint(table->ref_query_->get_stmt_hint()));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObTransformExprPullup::transform_view_recursively(TableItem *table_item,
                                                      ObSelectStmt &stmt,
                                                      ObExprNodeMap &parent_reject_expr_map,
                                                      ObExprNodeMap &parent_reject_subquery_map,
                                                      ObIArray<ObSelectStmt *> &transformed_views,
                                                      bool stmt_may_reduce_row_count,
                                                      bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;

  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (!table_item->is_joined_table()) {
    if (OB_FAIL(pullup_expr_from_view(table_item, stmt, parent_reject_expr_map,
                                      parent_reject_subquery_map, transformed_views,
                                      stmt_may_reduce_row_count, trans_happened))) {
      LOG_WARN("fail to pullup epr from view", K(ret));
    }
  } else {
    JoinedTable *joined_table = dynamic_cast<JoinedTable*>(table_item);
    if (OB_ISNULL(joined_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table item is null", K(ret));
    } else if (IS_OUTER_JOIN(joined_table->joined_type_)) {
      // the expr result of outer joined tables may be determined by the logic of outer join
      // (fill null or fill the origin value).
      // Do not rewrite for the non-determined side.
      if (joined_table->is_left_join()) {
        if (OB_FAIL(transform_view_recursively(joined_table->left_table_, stmt,
                                               parent_reject_expr_map,
                                               parent_reject_subquery_map,
                                               transformed_views,
                                               stmt_may_reduce_row_count, trans_happened))) {
          LOG_WARN("fail to pullup epr from view", K(ret));
        }
      } else if (joined_table->is_right_join()) {
        if (OB_FAIL(transform_view_recursively(joined_table->right_table_, stmt,
                                               parent_reject_expr_map,
                                               parent_reject_subquery_map,
                                               transformed_views,
                                               stmt_may_reduce_row_count, trans_happened))) {
          LOG_WARN("fail to pullup epr from view", K(ret));
        }
      }
    } else {
      if (OB_FAIL(transform_view_recursively(joined_table->left_table_, stmt,
                                             parent_reject_expr_map,
                                             parent_reject_subquery_map,
                                             transformed_views,
                                             stmt_may_reduce_row_count, trans_happened))) {
        LOG_WARN("fail to pullup epr from view", K(ret));
      } else if (OB_FAIL(transform_view_recursively(joined_table->right_table_, stmt,
                                                    parent_reject_expr_map,
                                                    parent_reject_subquery_map,
                                                    transformed_views,
                                                    stmt_may_reduce_row_count, trans_happened))) {
        LOG_WARN("fail to pullup epr from view", K(ret));
      }
    }
  }
  return ret;
}

int ObExprNodeMap::add_expr_map(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (expr->is_const_expr()) {
    // do nothing
  } else {
    ExprCounter counter;
    uint64_t hash_v = get_hash_value(expr);
    ret = expr_map_.get_refactored(hash_v, counter);
    if (OB_SUCCESS == ret || OB_HASH_NOT_EXIST == ret) {
      is_exist = (OB_SUCCESS == ret);
      counter.count_ = is_exist ? counter.count_ + 1 : 1;
      if (OB_FAIL(expr_map_.set_refactored(hash_v, counter, 1))) {
        LOG_WARN("fail to set hash map", K(ret));
      }
    } else {
      LOG_WARN("get hash map failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && !is_exist) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(add_expr_map(expr->get_param_expr(i))))) {
        LOG_WARN("fail to preorder search expr", K(ret));
      }
    }
  }

  return ret;
}

int ObExprNodeMap::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_map_.create(256, ObModIds::OB_HASH_BUCKET, ObModIds::OB_HASH_NODE))) {
    LOG_WARN("fail to init hash map", K(ret));
  }
  return ret;
}

int ObExprNodeMap::is_exist(ObRawExpr *expr, bool &is_exist)
{
  int ret = OB_SUCCESS;
  int64_t ref_count = 0;
  ret = get_ref_count(expr, ref_count);
  is_exist = (ref_count > 0);
  return ret;
}

int ObExprNodeMap::get_ref_count(ObRawExpr *expr, int64_t &ref_count)
{
  int ret = OB_SUCCESS;
  ExprCounter counter;
  ref_count = 0;
  ret = expr_map_.get_refactored(get_hash_value(expr), counter);

  if (OB_SUCCESS == ret) {
    ref_count = counter.count_;
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  } else {
    LOG_WARN("fail to get hash map", K(ret));
  }
  return ret;
}

/*
 * To judge if the pullup expr can make less calculation,
 * we take clear look at parent exprs at each scope.
 * The operations in parent stmt that may reduce the result row count are:
 * filter operation by where conds, group by operation, distinct operation
 * and limit operation.
 * If there is an operation happened and the pulluped expr is included
 * by the operation (E.g. pulluped expr used in parent where cond)
 * the calculation can not avoid.
 * To test expr can realy help reduce calcuation, we build a reject map.
 */
int ObTransformExprPullup::build_parent_reject_exprs_map(ObSelectStmt &parent,
                                                         ObExprNodeMap &expr_reject_map,
                                                         ObExprNodeMap &subquery_reject_map)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 16> exprs_need_check;
  ObSEArray<ObRawExpr *, 16> exprs_need_check_subquery;
  ObIArray<ObRawExpr*> &group_exprs = parent.get_group_exprs();
  ObIArray<ObQueryRefRawExpr*> &subquery_exprs = parent.get_subquery_exprs();
  int64_t the_first_scope_to_search = T_NONE_SCOPE;

  if (OB_FAIL(expr_reject_map.init()) || OB_FAIL(subquery_reject_map.init())) {
    LOG_WARN("fail to init expr node map", K(ret));
  }
  
  //check ref column in subqueries
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs.count(); ++i) {
    if(OB_FAIL(ObRawExprUtils::extract_column_exprs(subquery_exprs.at(i),
                                                    exprs_need_check_subquery))) {
      LOG_WARN("extract column exprs failed", K(ret));
    }
  }

  //check where cond
  if (OB_SUCC(ret) && T_NONE_SCOPE == the_first_scope_to_search) {
    if (OB_FAIL(parent.get_where_scope_conditions(exprs_need_check, true))) {
      LOG_WARN("fail to get where conds", K(ret));
    } else if (OB_FAIL(append(exprs_need_check_subquery, exprs_need_check))) {
      LOG_WARN("fail to append array", K(ret));
    } else if (exprs_need_check.count() + parent.get_condition_size() > 1) {
      //if there are more than 1 filter, pullup expr maybe good.
      exprs_need_check.reuse();
      the_first_scope_to_search = T_WHERE_SCOPE;
    } else if (exprs_need_check.empty() && parent.get_condition_exprs().empty()) {
      /* do nothing */
    } else if (OB_FAIL(append(exprs_need_check, parent.get_condition_exprs()))) {
      LOG_WARN("fail to append array", K(ret));
    } else {
      the_first_scope_to_search = T_WHERE_SCOPE;
    }
  }

  //check group by and aggr function
  if (OB_SUCC(ret) && T_NONE_SCOPE == the_first_scope_to_search) {
    if (OB_FAIL(append(exprs_need_check, parent.get_group_exprs()))) {
      LOG_WARN("fail to append array", K(ret));
    } else if (OB_FAIL(append(exprs_need_check, parent.get_aggr_items()))) {
      LOG_WARN("fail to append array", K(ret));
    } else if (exprs_need_check.count() > 0) {
      the_first_scope_to_search = T_GROUP_SCOPE;
    }
  }

  //check distinct
  if (OB_SUCC(ret) && T_NONE_SCOPE == the_first_scope_to_search) {
    if (parent.has_distinct()) {
      if (OB_FAIL(parent.get_select_exprs(exprs_need_check))) {
        LOG_WARN("fail to get select exprs", K(ret));
      }
      the_first_scope_to_search = T_FIELD_LIST_SCOPE;
    }
  }

  //check limit
  if (OB_SUCC(ret) && T_NONE_SCOPE == the_first_scope_to_search) {
    if (parent.has_limit()) {
      if (OB_FAIL(parent.get_order_exprs(exprs_need_check))) {
        LOG_WARN("fail to get order exprs", K(ret));
      }
      the_first_scope_to_search = T_ORDER_SCOPE;
    }
  }

  //build map
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs_need_check.count(); ++i) {
    if (OB_FAIL(expr_reject_map.add_expr_map(exprs_need_check.at(i)))) {
      LOG_WARN("fail to add where cond to expr map", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs_need_check_subquery.count(); ++i) {
    if (OB_FAIL(subquery_reject_map.add_expr_map(exprs_need_check_subquery.at(i)))) {
      LOG_WARN("fail to add where cond to expr map", K(ret));
    }
  }

  LOG_DEBUG("check parent first scope to search", K(the_first_scope_to_search));
  
  return ret;
}

/**
 * @brief Exprs in select list may not able to pullup because of its childrens.
 *        Split the expr into 2 parts:
 *          Part1: sub-trees of the expr which can not pullup
 *          Part2: the rest of the expr can pullup
 *        Part1 would be left in child stmt and projected as params of Part2 which
 *        would pullup to parent stmt.
 *        This function do the Part1 job.
 */
int ObTransformExprPullup::extract_params(ObRawExpr *expr,
                                          ObExprNodeMap &child_reject_map,
                                          ObIArray<ObRawExpr *> &param_exprs)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(child_reject_map.is_exist(expr, is_exist))) {
    LOG_WARN("fail to check shared expr", K(ret));
  } else if (is_exist) {
    //exist can not pullup, will be project by child stmt
    if (!ObRawExprUtils::find_expr(param_exprs, expr)) {
      if (OB_FAIL(param_exprs.push_back(expr))) {
        LOG_WARN("fail to push back expr", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(extract_params(expr->get_param_expr(i), child_reject_map, param_exprs)))) {
        LOG_WARN("fail to preorder search expr", K(ret));
      }
    }
  }
  return ret;
}

bool ObTransformExprPullup::expr_need_pullup(ObRawExpr *expr)
{
  return OB_NOT_NULL(expr)
      && !expr->is_const_expr();
}

bool ObTransformExprPullup::expr_can_pullup(ObRawExpr *expr)
{
  return OB_NOT_NULL(expr)
      && !(T_OP_ASSIGN == expr->get_expr_type()
           || expr->has_flag(IS_ROWNUM)
           || expr->is_pseudo_column_expr()
           || expr->is_column_ref_expr()
           || expr->is_aggr_expr()
           || expr->is_win_func_expr());
}

bool ObTransformExprPullup::is_view_acceptable_for_rewrite(TableItem &view)
{
  return view.is_generated_table()
      && OB_NOT_NULL(view.ref_query_)
      && !view.ref_query_->is_hierarchical_query()
      && !view.ref_query_->has_select_into()
      && !view.ref_query_->is_set_stmt()
      && !view.ref_query_->is_distinct()
      && !view.ref_query_->is_contains_assignment();
}

int ObTransformExprPullup::check_stmt_validity(const ObDMLStmt *stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;

  is_valid = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->is_select_stmt()) {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(stmt);
    if (!select_stmt->is_set_stmt()
        && !select_stmt->is_hierarchical_query()
        && !select_stmt->has_rollup()) {
      is_valid = true;
    }
  }
  return ret;
}

int ObTransformExprPullup::search_expr_cannot_pullup(ObRawExpr *expr,
                                                     ObExprNodeMap &expr_map,
                                                     ObIArray<ObRawExpr *> &expr_cannot_pullup)
{
  int ret = OB_SUCCESS;
  int64_t ref_count = 0;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (!expr_can_pullup(expr)) {
    if (OB_FAIL(expr_cannot_pullup.push_back(expr))) {
      LOG_WARN("fail to push back expr", K(ret));
    }
  } else if (OB_FAIL(expr_map.get_ref_count(expr, ref_count))) {
    LOG_WARN("failed to get expr ref count", K(ret));
  } else if (ref_count > 1)  {
    if (OB_FAIL(expr_cannot_pullup.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(search_expr_cannot_pullup(expr->get_param_expr(i),
                                                       expr_map,
                                                       expr_cannot_pullup)))) {
        LOG_WARN("fail to preorder search expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformExprPullup::rewrite_decision_by_hint(ObSelectStmt &parent,
                                                    ObSelectStmt &child,
                                                    bool stmt_may_reduce_row_count,
                                                    bool &go_rewrite)
{
  int ret = OB_SUCCESS;
  const ObQueryHint *query_hint = child.get_stmt_hint().query_hint_;
  const ObHint *pullup_hint = get_hint(child.get_stmt_hint());
  enum REASON {
    CTRL_INVALID,
    CTRL_BY_PULLUP_HINT,
    CTRL_BY_NO_REWRITE_HINT,
    CTRL_BY_SELF_DECISION
  };
  REASON reason = CTRL_INVALID;

  if (OB_NOT_NULL(query_hint) && query_hint->has_outline_data()) {
    if (query_hint->is_valid_outline_transform(ctx_->trans_list_loc_, pullup_hint)) {
      go_rewrite = pullup_hint->is_enable_hint();
      reason = CTRL_BY_PULLUP_HINT;
      if (!go_rewrite && OB_FAIL(ctx_->add_used_trans_hint(pullup_hint))) {
        LOG_WARN("failed to add used trans hint", K(ret));
      }
    } else {
      go_rewrite = false;
      OPT_TRACE("outline reject transform");
    }
  } else {
    const ObHint *child_no_rewrite = child.get_stmt_hint().get_no_rewrite_hint();
    const ObHint *parent_no_rewrite = parent.get_stmt_hint().get_no_rewrite_hint();

    if (OB_NOT_NULL(pullup_hint)) {
      go_rewrite = pullup_hint->is_enable_hint();
      if (!go_rewrite && OB_FAIL(ctx_->add_used_trans_hint(pullup_hint))) {
        LOG_WARN("failed to add used trans hint", K(ret));
      } else if (!go_rewrite) {
        OPT_TRACE("hint reject transform");
      }
      reason = CTRL_BY_PULLUP_HINT;
    } else if (OB_NOT_NULL(child_no_rewrite) || OB_NOT_NULL(parent_no_rewrite)) {
      go_rewrite = false;
      if (OB_FAIL(ctx_->add_used_trans_hint(child_no_rewrite))) {
        LOG_WARN("failed to add used trans hint", K(ret));
      } else if (OB_FAIL(ctx_->add_used_trans_hint(parent_no_rewrite))) {
        LOG_WARN("failed to add used trans hint", K(ret));
      }
      reason = CTRL_BY_NO_REWRITE_HINT;
      OPT_TRACE("parent or child`s no rewrite hint reject transform");
    } else {
      go_rewrite = stmt_may_reduce_row_count;
      reason = CTRL_BY_SELF_DECISION;
      OPT_TRACE("stmt may reduce row count:", stmt_may_reduce_row_count);
    }
  }
  LOG_DEBUG("check hint rewrite control", K(go_rewrite), K(stmt_may_reduce_row_count), K(reason));
  return ret;
}

int ObTransformExprPullup::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  UNUSED(stmt);
  ObIArray<ObSelectStmt*> *merged_stmts = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(trans_params)
      || OB_ISNULL(merged_stmts = static_cast<ObIArray<ObSelectStmt*>*>(trans_params))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(ctx_), KP(trans_params), KP(merged_stmts));
  } else {
    ObHint *hint = NULL;
    ObDMLStmt *child_stmt = NULL;
    ObString child_qb_name;
    for (int64_t i = 0; OB_SUCC(ret) && i < merged_stmts->count(); ++i) {
      if (OB_ISNULL(child_stmt = merged_stmts->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(child_stmt));
      } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_PULLUP_EXPR, hint))) {
        LOG_WARN("failed to create hint", K(ret));
      } else if (OB_FAIL(child_stmt->get_qb_name(child_qb_name))) {
        LOG_WARN("failed to get qb name", K(ret), K(child_stmt->get_stmt_id()));
      } else if (OB_FAIL(ctx_->add_src_hash_val(child_qb_name))) {
        LOG_WARN("failed to add src hash val", K(ret));
      } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
        LOG_WARN("failed to push back hint", K(ret));
      } else {
        hint->set_qb_name(child_qb_name);
      }
    }
  }
  return ret;
}

int ObTransformExprPullup::pullup_expr_from_view(TableItem *view,
                                                 ObSelectStmt &select_stmt,
                                                 ObExprNodeMap &parent_reject_expr_map,
                                                 ObExprNodeMap &parent_reject_subquery_map,
                                                 ObIArray<ObSelectStmt *> &transformed_stmts,
                                                 bool stmt_may_reduce_row_count,
                                                 bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool try_rewrite = false;
  if (OB_ISNULL(view)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("view is null", K(ret));
  } else if (!is_view_acceptable_for_rewrite(*view)) {
    //skip everying
  } else if (OB_FAIL(rewrite_decision_by_hint(select_stmt, *(view->ref_query_),
                                              stmt_may_reduce_row_count, try_rewrite))) {
    LOG_WARN("fail to check is allowed by hint", K(ret));
  } else if (try_rewrite) {
    bool cur_trans_happened = false;
    ObSelectStmt &child = static_cast<ObSelectStmt &>(*(view->ref_query_));
    ObSelectStmt &parent = select_stmt;
    ObSEArray<ObRawExpr *, 16> select_exprs;
    ObSqlBitSet<> select_expr_idxs;
    ObExprNodeMap child_reject_map;
    ObExprNodeMap child_select_map;
    ObSEArray<ObRawExpr *, 16> expr_params;
    ObSEArray<ObRawExpr*, 4> old_child_project_columns;
    ObSEArray<ObRawExpr*, 4> select_exprs_can_pullup;
    ObSEArray<ObRawExpr*, 4> new_child_project_columns;

    if (OB_FAIL(child.get_select_exprs(select_exprs))) {
      LOG_WARN("fail to get select exprs", K(ret));
    }

    //search for exprs cannot pullup
    if (OB_SUCC(ret)) {
      ObSEArray<ObRawExpr *, 16> expr_cannot_pullup;
      ObStmtExprGetter visitor;
      visitor.remove_scope(SCOPE_SELECT);
      if (OB_FAIL(child_reject_map.init())) {
        LOG_WARN("fail to init shared exprs", K(ret));
      } else if (OB_FAIL(child_select_map.init())) {
        LOG_WARN("failed to init select map", K(ret));
      } else if (OB_FAIL(child.get_relation_exprs(expr_cannot_pullup, visitor))) {
        LOG_WARN("fail to get relation exprs", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
        if (OB_FAIL(child_select_map.add_expr_map(select_exprs.at(i)))) {
          LOG_WARN("failed to add expr map", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
        if (OB_FAIL(search_expr_cannot_pullup(select_exprs.at(i),
                                              child_select_map,
                                              expr_cannot_pullup))) {
          LOG_WARN("fail to get relation exprs", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < expr_cannot_pullup.count(); ++i) {
        if (OB_FAIL(child_reject_map.add_expr_map(expr_cannot_pullup.at(i)))) {
          LOG_WARN("fail to get relation exprs", K(ret));
        }
      }
    }

    //collect exprs can pullup, the corresponding view columns, and the params of exprs
    for (int64_t i = 0; OB_SUCC(ret) && i < child.get_select_items().count(); i++) {
      SelectItem &select_item = child.get_select_items().at(i);
      ObRawExpr *view_project_column_expr = NULL;
      if (OB_ISNULL(view_project_column_expr =
                      parent.get_column_expr_by_id(view->table_id_, i + OB_APP_MIN_COLUMN_ID))) {
        //view_project_column_expr is null which means parent maynot use this expr
        //do not pullup expr
      } else {
        // child_reject_map elimiates the expr that cannot pullup to parent
        // parent_reject_map elimiates the expr that cannot help reduce calcuation when pullup to parent
        bool is_child_reject = false;
        bool is_parent_reject = false;
        bool is_parent_reject_subquery = false;
        if (OB_ISNULL(select_item.expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(select_item.expr_));
        } else if (OB_FAIL(child_reject_map.is_exist(select_item.expr_, is_child_reject))) {
          LOG_WARN("fail to check expr exist", K(ret));
        } else if (OB_FAIL(parent_reject_expr_map.is_exist(view_project_column_expr, is_parent_reject))) {
          LOG_WARN("fail to check expr exist", K(ret));
        } else if (select_item.expr_->has_flag(CNT_SUB_QUERY)
                   && OB_FAIL(parent_reject_subquery_map.is_exist(view_project_column_expr, is_parent_reject_subquery))) {
          LOG_WARN("fail to check expr exist", K(ret));
        } else if (is_child_reject || is_parent_reject || is_parent_reject_subquery
                   || !expr_need_pullup(select_item.expr_)) {
          LOG_DEBUG("can not pullup expr", K(is_child_reject), K(is_parent_reject),
                                           K(is_parent_reject_subquery), KPC(select_item.expr_));
        } else {
          ObSEArray<ObRawExpr *, 16> new_expr_params;

          if (OB_FAIL(extract_params(select_item.expr_, child_reject_map, new_expr_params))) {
            LOG_WARN("fail to add shared params", K(ret));
          } else if (OB_FAIL(append(expr_params, new_expr_params))) {
            LOG_WARN("fail to append expr params", K(ret));
          } else if (OB_FAIL(select_exprs_can_pullup.push_back(select_item.expr_))) {
            LOG_WARN("fail to push back expr", K(ret));
          } else if (OB_FAIL(select_expr_idxs.add_member(i))) {
            LOG_WARN("fail to push back expr", K(ret));
          } else if (OB_FAIL(old_child_project_columns.push_back(view_project_column_expr))) {
            LOG_WARN("fail to push back arr", K(ret));
          }
          LOG_DEBUG("expr need pullup", KPC(select_item.expr_), K(new_expr_params));
        }
      }
    }

    if (OB_SUCC(ret) && select_exprs_can_pullup.count() > 0) {
      cur_trans_happened = true;
      //remove exprs from child
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObTransformUtils::remove_select_items(ctx_, view->table_id_,
                                                          child,
                                                          parent,
                                                          select_expr_idxs))) {
          LOG_WARN("fail to remove select items", K(ret));
        }
      }

      //project expr params in child by adding new select items
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_,
                                                              *view,
                                                              &parent,
                                                              expr_params,
                                                              new_child_project_columns))) {
          LOG_WARN("failed to create select item", K(ret));
        }
      }

      //adjust exprs params to new project columns
      // and replace old exprs in parent
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObTransformUtils::replace_exprs(expr_params,
                                                    new_child_project_columns,
                                                    select_exprs_can_pullup))) {
          LOG_WARN("fail to replace exprs", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs_can_pullup.count(); i++) {
            if (OB_FAIL(select_exprs_can_pullup.at(i)->formalize(ctx_->session_info_))) {
              LOG_WARN("fail to formalize expr", K(ret));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(parent.replace_relation_exprs(old_child_project_columns,
                                                  select_exprs_can_pullup))) {
          LOG_WARN("fail to replace inner stmt expr", K(ret));
        } else if (OB_FAIL(parent.formalize_stmt(ctx_->session_info_))) {
          LOG_WARN("fail to formalize stmt", K(ret));
        } else if (OB_FAIL(child.formalize_stmt(ctx_->session_info_))) {
          LOG_WARN("fail to formalize stmt", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && cur_trans_happened) {
      if (OB_FAIL(transformed_stmts.push_back(&child))) {
        LOG_WARN("fail to push array", K(ret));
      }
    }
    if (OB_SUCC(ret) && cur_trans_happened) {
      const ObHint *pullup_hint = get_hint(child.get_stmt_hint());
      if (OB_NOT_NULL(pullup_hint)) {
        if (OB_FAIL(ctx_->add_used_trans_hint(pullup_hint))) {
          LOG_WARN("failed to add used trans hint", K(ret));
        }
      }
    }
    trans_happened |= cur_trans_happened;
  }

  return ret;
}

int ObTransformExprPullup::is_stmt_may_reduce_row_count(const ObSelectStmt &stmt, bool &is_true)
{
  int ret = OB_SUCCESS;
  is_true = false;
  ObSEArray<ObRawExpr*, 8> where_cond_exprs;
  if (stmt.get_table_size() != 1) {
    //TODO need cost-based check
    is_true = false;
  } else if (OB_FAIL(stmt.get_where_scope_conditions(where_cond_exprs))) {
    LOG_WARN("fail to get where scope expr", K(ret));
  } else if (stmt.get_group_expr_size() > 0 || where_cond_exprs.count() > 0 || stmt.has_limit()) {
    is_true = true;
    LOG_DEBUG("check is stmt may reduce row count", K(stmt.get_group_expr_size()), K(where_cond_exprs.count()), K(stmt.has_limit()));
  }
  return ret;
}

}
}
