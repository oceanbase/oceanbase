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
#include "sql/rewrite/ob_transform_simplify_set.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "sql/rewrite/ob_transform_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace sql
{

int ObTransformSimplifySet::SimplifySetHelper::assign(const SimplifySetHelper &other) {
  int ret = OB_SUCCESS;
  const_constraint_exprs_.reset();
  precalc_constraint_exprs_.reset();
  if (OB_FAIL(const_constraint_exprs_.assign(other.const_constraint_exprs_))) {
    LOG_WARN("fail to assign const constraints", K(ret));
  } else if (OB_FAIL(precalc_constraint_exprs_.assign(other.precalc_constraint_exprs_))) {
    LOG_WARN("fail to assign precal constraints", K(ret));
  }
  return ret;
}

int ObTransformSimplifySet::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                               ObDMLStmt *&stmt,
                                               bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (!(stmt->is_select_stmt() && static_cast<ObSelectStmt*>(stmt)->is_set_stmt())) {
    // do nothing
  } else {
    ObSelectStmt * sel_stmt = static_cast<ObSelectStmt*>(stmt);
    if (OB_FAIL(pruning_set_query(parent_stmts, sel_stmt, is_happened))) {
      LOG_WARN("failed to pruning set query", K(ret));
    } else {
      stmt = sel_stmt;
      trans_happened |= is_happened;
      OPT_TRACE("remove pruning set query:", is_happened);
      LOG_TRACE("succeed to pruning set query.", K(is_happened));
    }
    
    if (OB_SUCC(ret)) {
      if (stmt->is_set_stmt() &&
          OB_FAIL(add_limit_order_distinct_for_union(parent_stmts, stmt, is_happened))) {
        LOG_WARN("failed to add limit for union", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("add limit order distinct for union:", is_happened);
        LOG_TRACE("succeed to add limit order distinct for union.", K(is_happened));
      } 
    }
  }
  if (OB_SUCC(ret)) {
    if (!trans_happened) {
      // do nothing
    } else if (OB_FAIL(stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(add_transform_hint(*stmt, NULL))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }
  return ret;
}

// 从upper_stmt下推distinct到stmt
int ObTransformSimplifySet::add_distinct(ObSelectStmt *stmt, ObSelectStmt *upper_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(upper_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer passed to add_distinct", K(stmt), K(upper_stmt));
  } else if (upper_stmt->is_set_distinct()) {
    if (stmt->is_set_stmt()) {
      stmt->assign_set_distinct();
    } else {
      stmt->assign_distinct();
    }
  }
  return ret;
}

// 从upper_stmt下推limit到stmt
// 上层有不带offset的limit，直接下压到两支
// 上层有带offset的limit，将limit+offset之后下压
int ObTransformSimplifySet::add_limit(ObSelectStmt *stmt, ObSelectStmt *upper_stmt)
{
  int ret = OB_SUCCESS;
  ObRawExpr *limit_count_expr = NULL;
  ObRawExpr *limit_offset_expr = NULL;
  ObRawExpr *limit_count_offset_expr = NULL;
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
    ObRawExprCopier copier(*ctx_->expr_factory_);
    if (OB_FAIL(copier.copy(limit_count_offset_expr, limit_count_expr))) {
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

// 从upper_stmt下推order by到stmt
int ObTransformSimplifySet::add_order_by(ObSelectStmt *stmt, ObSelectStmt *upper_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> set_exprs;

  //add order by
  if (OB_ISNULL(stmt) || OB_ISNULL(upper_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer passed to add_distinct", K(stmt), K(upper_stmt));
  } else if (stmt->get_order_item_size() > 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt should not have order by", K(ret), K(stmt->get_order_item_size()));
  } else if (OB_FAIL(upper_stmt->get_pure_set_exprs(set_exprs))) {
    LOG_WARN("failed to get expr in cast", K(ret));
  } else {
    ObRawExprCopier copier(*ctx_->expr_factory_);
    for (int64_t i = 0; OB_SUCC(ret) && i < set_exprs.count(); ++i) {
      if (OB_ISNULL(set_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the set expr is invalid", K(ret), K(set_exprs.at(i)));
      } else if (set_exprs.at(i)->is_set_op_expr()) {
        int64_t idx = static_cast<ObSetOpRawExpr*>(set_exprs.at(i))->get_idx();
        if (OB_UNLIKELY(idx < 0 || idx >= stmt->get_select_item_size())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid select item index", K(ret), K(idx), K(stmt->get_select_item_size()));
        } else if (OB_FAIL(copier.add_replaced_expr(set_exprs.at(i),
                                                    stmt->get_select_item(idx).expr_))) {
          LOG_WARN("failed to add replace expr pair", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < upper_stmt->get_order_item_size(); ++i) {
      OrderItem order_item = upper_stmt->get_order_item(i);
      if (OB_FAIL(copier.copy(order_item.expr_, order_item.expr_))) {
        LOG_WARN("failed to copy expr", K(ret));
      } else if (OB_FAIL(stmt->add_order_item(order_item))) {
        LOG_WARN("failed to add order item", K(ret));
      }
    }
  }
  return ret;
}

//
// 判断upper_stmt能否下推distinct/order by/limit操作到stmt // 当上层有不带offset的limit且下层没有limit时可以下推limit，distinct(如果上层有的话)，
// 如果上层还有order by，
// 下推order by(下层order by肯定被消除了remove_order_by_for_set)
// 如果上层有order by 且 order by 跟的一个子查询，则不能下压:select * from t1 union select * from t2 order by (select min(c1) from t3) limit 1;
//
int ObTransformSimplifySet::check_can_push(ObSelectStmt *stmt, ObSelectStmt *upper_stmt, bool &can_push)
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
    // 下层有limit
    can_push = false;
  } else if ((0 < upper_stmt->get_order_item_size() && 0 < stmt->get_order_item_size())
             || NULL == upper_stmt->get_limit_expr()
             || NULL != upper_stmt->get_limit_percent_expr()
             || upper_stmt->is_fetch_with_ties()) {
    // 上层有order by且下层有也不能下推
    // 仅有 offset 不进行下推
    // limit percent不能下推, 因为可能生成topn导致结果错误
    can_push = false;
  } else {
    //上层有order by subquery 不能下推
    for (int64_t i = 0; OB_SUCC(ret) && can_push && i < upper_stmt->get_order_item_size(); ++i) {
      ObRawExpr *order_expr = upper_stmt->get_order_item(i).expr_;
      if (OB_ISNULL(order_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(order_expr));
      } else if (order_expr->has_flag(CNT_SUB_QUERY)) {
        can_push = false;
      } else if (!order_expr->has_flag(CNT_SET_OP)) {
        can_push = false;
      } else {/*do nothing*/}
    }
  }
  return ret;
}

// 对Union的stmt执行以下改写：
// 下推ORDER BY/LIMIT/DISTINCT
//
int ObTransformSimplifySet::add_limit_order_distinct_for_union(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                         ObDMLStmt *&stmt,
                                                         bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_calc = false;
  ObSelectStmt* select_stmt = NULL;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer passed to transform", K(stmt), K(ret));
  } else if (stmt->is_select_stmt()
             && static_cast<ObSelectStmt*>(stmt)->is_set_stmt()
             && ObSelectStmt::UNION == static_cast<ObSelectStmt*>(stmt)->get_set_op()) {
    select_stmt = static_cast<ObSelectStmt*>(stmt);
    if (OB_FAIL(is_calc_found_rows_for_union(parent_stmts, stmt, is_calc))) {
      LOG_WARN("fail to judge calc found rows for union", K(ret));
    } else if (is_calc) {
        /*do nothing*/
    } else if(select_stmt->has_limit()){
      bool can_push = false;
      ObSelectStmt* child_stmt = NULL;
      ObIArray<ObSelectStmt*> &child_stmts = select_stmt->get_set_query();
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

int ObTransformSimplifySet::is_calc_found_rows_for_union(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                   ObDMLStmt *&stmt,
                                                   bool &is_calc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, stmt is NULL", K(stmt), K(ret));
  } else {
    ObSelectStmt *top_union_stmt = NULL;
    if (stmt->is_select_stmt() && stmt->is_set_stmt() &&
        ObSelectStmt::UNION == static_cast<ObSelectStmt*>(stmt)->get_set_op()) {
      top_union_stmt = static_cast<ObSelectStmt*>(stmt);
    } else { /* do nothing*/ }
    for (int64_t i = parent_stmts.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      ObDMLStmt *stmt = NULL;
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

// check wether the exprs is always false.
int ObTransformSimplifySet::check_exprs_constant_false(common::ObIArray<ObRawExpr*> &exprs,
                                                       bool &constant_false,
                                                       int64_t stmt_idx,
                                                       SimplifySetHelper &helper)
{
  int ret = OB_SUCCESS;
  bool is_valid_type = true;
  constant_false |= false;
  if (OB_FAIL(ObTransformUtils::check_integer_result_type(exprs, is_valid_type))) {
    LOG_WARN("check valid type fail", K(ret));
  } else if (!is_valid_type) {
    LOG_TRACE("expr list is not valid for removing dummy exprs", K(is_valid_type));
  } else {
    ObSEArray<int64_t, 2> true_exprs;
    ObSEArray<int64_t, 2> false_exprs;
    if (OB_FAIL(ObTransformUtils::extract_const_bool_expr_info(ctx_,
                                                               exprs,
                                                               true_exprs,
                                                               false_exprs))) {
      LOG_WARN("fail to extract exprs info", K(ret));
    } else if (false_exprs.count() > 0) {
      /* do the check.
       * N: exprs.count(), M:false_exprs.count(), K:true_exprs.count();
       * M > 0; 
       */
      constant_false = true;

      // build precalc_constraints exprs.
      ObSEArray<ObRawExpr*, 4> ob_params;
      ObRawExpr *op_expr = NULL;
      if (OB_FAIL(ObTransformUtils::extract_target_exprs_by_idx(exprs, false_exprs, ob_params))) {
        LOG_WARN("fail to push back params expr", K(ret));
      } else {
        ObRawExpr *tmp_expr = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && i < ob_params.count(); i++) {
          if (OB_ISNULL(tmp_expr = ob_params.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get null pointer", K(ret));
          } else if (OB_FAIL(helper.precalc_constraint_exprs_.push_back(
                                    std::pair<ObRawExpr*, int64_t>(tmp_expr, stmt_idx)))) {
            LOG_WARN("fail to push back constraint exprs", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

/*
 * BACKGROUND:
 * There are three kind of limit.
 *  1. mysql's limit. (only positive number allowed)
 *  2. oracle's fetch. (fetch can't coexist with set query, 
 *                      oracle's set query only support simple select)
 *  3. limit that is converted from rownum. (could be complex expr)
 * 
 * PRINCIPLES: 
 * There are three situations that the stmt returns empty:
 *  1. limit_count_expr <= 0;
 *  2. limit_percent_expr <= 0;
 *  3. limit_offset_expr > max row count. (not support).
 * When we detect the first two ruls, we consider the stmt is removable. 
 */
int ObTransformSimplifySet::check_limit_zero_in_stmt(ObRawExpr *limit_expr,
                                                     ObRawExpr *offset_expr,
                                                     ObRawExpr *percent_expr,
                                                     bool &need_remove,
                                                     int64_t stmt_idx, 
                                                     SimplifySetHelper &helper)
{
  UNUSED(offset_expr);
  int ret = OB_SUCCESS;
  ObObj result;
  need_remove = false;
  bool is_valid = false;
  if (OB_NOT_NULL(limit_expr)) {
    if (OB_FAIL(ObTransformUtils::calc_const_expr_result(limit_expr, ctx_, result, is_valid))) {
      LOG_WARN("fail to calc const expr", K(ret));
    }
  } else if (OB_NOT_NULL(percent_expr)) {
    if (OB_FAIL(ObTransformUtils::calc_const_expr_result(percent_expr, ctx_, result, is_valid))) {
      LOG_WARN("fail to calc const expr", K(ret));
    }
  } else {}
  
  //check need_remove
  if (OB_SUCC(ret) && is_valid) {
    if (result.is_int()) {
      need_remove = (result.get_int() <= 0);
    } else if (result.is_double()) {
      need_remove = (result.get_double() <= 0);
    } else if (result.is_float()) {
      need_remove = (result.get_float() <= 0);
    } else if (result.is_number()) {
      need_remove = (result.is_zero_number() || result.is_negative_number());
    }
  }

  // only add constraint exprs here. 
  // the real constraints should be added after all transformation have done.
  if (OB_SUCC(ret) && need_remove) {
    if (OB_NOT_NULL(limit_expr)) {
      if (OB_FAIL(helper.const_constraint_exprs_.push_back(
                      std::pair<ObRawExpr *, int64_t>(limit_expr, stmt_idx)))) {
        LOG_WARN("fail to push back const constraints", K(ret));
      }
    } else if (OB_NOT_NULL(percent_expr)) {
      if (OB_FAIL(helper.const_constraint_exprs_.push_back(
                      std::pair<ObRawExpr *, int64_t>(percent_expr, stmt_idx)))) {
        LOG_WARN("fail to push back const constraints", K(ret));
      }
    }
  }
  return ret;
}

/* Why we should add constraints?
 * ---- const constraint (for limit expr) ----
 * since 
 * Q1: "select * from t1 where rownum < ? UNION select 1 from dual" 
 * will be converted to 
 * Q2: "select * from t1 limit ? UNION select 1 from dual"
 * However, (?==0) and (?>0) should hit different plan. 
 * When ?==0, query "select * from t1 limit ?" in Q2 would be pruned.
 * 
 * ---- precalc constraints (for constant false expr) ----
 * Q1: "select * from t1 where ? union select * from t2  where ?"
 * will be convert to  (if the second ? is false)
 * Q2: "select * from t1 where ?" 
 * if the second ? is true, the stmt shouldn't get the same plan as Q2.
 * 
 * @param: 
 *    need_remove: wether the stmt need to be removed
 *    stmt_idx: the index of the stmt in the parent_stmt, used to mark the constraints in helper.
 *    helper: store the exprs that is used to build constraints.
 */
int ObTransformSimplifySet::check_set_stmt_removable(ObSelectStmt *stmt,
                                                     bool &need_remove,
                                                     int64_t stmt_idx,
                                                     SimplifySetHelper &helper)
{
  int ret = OB_SUCCESS;
  int has_scalar_group_by = false;
  need_remove = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null pointer", K(ret));
  } else if (!stmt->is_scala_group_by() &&
             OB_FAIL(check_exprs_constant_false(stmt->get_condition_exprs(),
                                                need_remove,
                                                stmt_idx,
                                                helper))) {
    // since select count(*) from dual where 1=0, will still output a row, we should check
    // scalar group by here.
    LOG_WARN("fail to check exprs constant false", K(ret), K(stmt->get_condition_exprs()));
  } else if (!need_remove && OB_FAIL(check_exprs_constant_false(stmt->get_having_exprs(),
                                                                need_remove,
                                                                stmt_idx,
                                                                helper))) {
    LOG_WARN("fail to check exprs constant false", K(ret), K(stmt->get_having_exprs()));
  } else if (!need_remove && OB_FAIL(check_limit_zero_in_stmt(stmt->get_limit_expr(),
                                                              stmt->get_offset_expr(),
                                                              stmt->get_limit_percent_expr(),
                                                              need_remove,
                                                              stmt_idx,
                                                              helper))) {
    LOG_WARN("fail to check limit", K(ret));
  }
  return ret;
}

int ObTransformSimplifySet::pruning_set_query(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                              ObSelectStmt *&select_stmt,
                                              bool &trans_happened)
{
  int ret = OB_SUCCESS;
  SimplifySetHelper helper;
  trans_happened = false;
  bool first_can_remove = true;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null pointer", K(ret));
  } else if (select_stmt->get_set_op() == ObSelectStmt::UNION ||
              select_stmt->get_set_op() == ObSelectStmt::EXCEPT ||
              select_stmt->get_set_op() == ObSelectStmt::INTERSECT) {
    // add removable child_stmt in remove_list.
    ObSEArray<int64_t, 4> remove_list;
    for (int64_t i = 0 ; OB_SUCC(ret) && i < select_stmt->get_set_query().count(); i++) {
      ObSelectStmt* child_stmt = select_stmt->get_set_query(i);
      bool need_remove = false;
      if (OB_FAIL(check_set_stmt_removable(child_stmt, need_remove, i, helper))) {
        LOG_WARN("fail to check set stmt removeable", K(ret));
      } else if (need_remove && OB_FAIL(remove_list.push_back(i))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret) && remove_list.count() > 0 &&
        remove_list.at(0) == 0) {
      if (OB_FAIL(check_first_stmt_removable(parent_stmts,
                                             select_stmt,
                                             remove_list,
                                             first_can_remove))) {
        LOG_WARN("failed to check first stmt removable", K(ret));
      } else if(!first_can_remove &&
                OB_FAIL(remove_list.remove(0))) {
        LOG_WARN("fail to remove item", K(ret));
      }
    }
    // do the remove.
    if (OB_SUCC(ret) && remove_list.count() > 0) {
      ObSEArray<int64_t, 1> constraints_idxs;
      if (OB_FAIL(remove_set_query_in_stmt(select_stmt, remove_list,
                                           constraints_idxs, trans_happened))) {
        LOG_WARN("fail to remove dummy set query", K(ret));
      } else if (trans_happened && OB_FAIL(add_constraints_by_idx(constraints_idxs, helper))) {
        LOG_WARN("fail to add constraints by idx", K(ret));
      }
    }
  }

  return ret;
}

int ObTransformSimplifySet::remove_set_query_in_stmt(ObSelectStmt *&select_stmt,
                                                     ObIArray<int64_t> &remove_list,
                                                     common::ObIArray<int64_t> &constraints_idxs,
                                                     bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(ret));
  } else if (OB_UNLIKELY(remove_list.count() <= 0 || select_stmt->get_set_query().count() <= 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("incorrent remove_list coutn or set queries number", K(ret));
  } else {
    trans_happened = true;
    ObSelectStmt *child_stmt = NULL;
    // index of the stmt, that should add constraints.

    switch (select_stmt->get_set_op()) {
      case ObSelectStmt::UNION: {
        if (select_stmt->is_recursive_union() && select_stmt->get_set_query().count() == 2) {
          // recursive cte, the fake table branch is always at left.
          if (remove_list.count() == 1 && remove_list.at(0) == 0) {
            // if the branches should be pruned is left branch, do nothing.
            trans_happened = false;
          } else {
            // if both branches should be removed or the right branch should be removed.
            // keep the left branches.
            child_stmt = select_stmt->get_set_query(0);
            if (OB_FAIL(replace_set_stmt_with_child_stmt(select_stmt, child_stmt))) {
              LOG_WARN("fail to replace stmt with child stmt", K(ret));
            } else if (OB_FAIL(constraints_idxs.push_back(1))) {
              // mark that need to add the second branches's constraints.
              LOG_WARN("fail to add constraints", K(ret));
            }
          }
        } else if (remove_list.count() >= select_stmt->get_set_query().count() - 1) {
          // keep only one stmt. not a set query any more.
          bool found = false;
          for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_set_query().count(); i++) {
            if (!found && (!has_exist_in_array(remove_list, i) || 
                (i == select_stmt->get_set_query().count() - 1))) {
              // if all child queries are removable, keep the last stmt.
              found = true;
              child_stmt = select_stmt->get_set_query(i);
            } else if (OB_FAIL(constraints_idxs.push_back(i))){
              // the i-th branch need to be removed, add the constraints
              LOG_WARN("fail to push back constraints", K(ret));
            }
          }
          if (OB_SUCC(ret) && found) {
            if (OB_FAIL(replace_set_stmt_with_child_stmt(select_stmt, child_stmt))) {
              LOG_WARN("fail to replace stmt with child stmt", K(ret));
            }
          }
        } else {
          for (int64_t i = remove_list.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
            if (OB_LIKELY(remove_list.at(i) >= 0 && remove_list.at(i) < select_stmt->get_set_query().count())) {
              if (OB_FAIL(select_stmt->get_set_query().remove(remove_list.at(i)))) {
                LOG_WARN("fail to remove set query", K(ret));
              } else if (OB_FAIL(constraints_idxs.push_back(remove_list.at(i)))) {
                LOG_WARN("fail to push back constraints", K(ret));
              }
            }
          }
        }
        break;
      }
      case ObSelectStmt::INTERSECT: {
        if (OB_UNLIKELY(select_stmt->get_set_query().count() != 2)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("branches num of intersect should be 2", K(ret));
        } else if (remove_list.count() == 2 || (remove_list.count() == 1 &&
                                                remove_list.at(0) == 1)) {
         
          child_stmt = select_stmt->get_set_query(1);
          if (OB_FAIL(constraints_idxs.push_back(0))) {
            LOG_WARN("fail to push back constraints", K(ret));
          }
        } else if (remove_list.count() == 1 && remove_list.at(0) == 0) {
          // remove second branch.
          child_stmt = select_stmt->get_set_query(0);
          if (OB_FAIL(constraints_idxs.push_back(1))) {
            LOG_WARN("fail to push back constraints", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(replace_set_stmt_with_child_stmt(select_stmt, child_stmt))) {
            LOG_WARN("fail to replace stmt with child stmt", K(ret));
          }
        }
        break;
      } 
      case ObSelectStmt::EXCEPT: {
        if (OB_UNLIKELY(select_stmt->get_set_query().count() != 2)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("branches num of except should be 2", K(ret));
        } else if (remove_list.count() == 2 || (remove_list.count() == 1 &&
                                                remove_list.at(0) == 0)) {
          // keep the first branch.
          child_stmt = select_stmt->get_set_query(0);
          if (OB_FAIL(constraints_idxs.push_back(0))) {
            LOG_WARN("fail to push back constraints", K(ret));
          }
        } else if (remove_list.count() == 1 && remove_list.at(0) == 1) {
          // keep the first branch. Since the stmt is not empty, we should keep the order by.
          child_stmt = select_stmt->get_set_query(0);
          if (OB_FAIL(constraints_idxs.push_back(1))) {
            LOG_WARN("fail to push back constraints", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(replace_set_stmt_with_child_stmt(select_stmt, child_stmt))) {
            LOG_WARN("fail to replace stmt with child stmt", K(ret));
          }
        }
        break;
      }
      default:
        break;
    }

  }
  return ret;
}

int ObTransformSimplifySet::add_constraints_by_idx(common::ObIArray<int64_t> &constraints_idx,
                                                   SimplifySetHelper &helper)
{
  int ret = OB_SUCCESS;
  
  for (int64_t i = 0; OB_SUCC(ret) && i < constraints_idx.count(); i++) {
    int64_t branch_idx = constraints_idx.at(i);
    // precal constraints;
    ObSEArray<ObRawExpr *, 4> ob_params;
    ObRawExpr *op_expr = NULL;
    bool search_end = false;
    for (int64_t j = 0; OB_SUCC(ret) && !search_end && j < helper.precalc_constraint_exprs_.count(); j++) {
      int64_t expr_branch_idx = helper.precalc_constraint_exprs_.at(j).second; 
      if (expr_branch_idx == branch_idx) {
        if (OB_FAIL(ob_params.push_back(helper.precalc_constraint_exprs_.at(j).first))) {
          LOG_WARN("fail to push back expr", K(ret));
        }
      } else if (expr_branch_idx > branch_idx) {
        // since constraint exprs is sorted, we don't need to go through the whole array.
        search_end = true;
      }
    }
    if (ob_params.count() <= 0) {
    } else if (OB_FAIL(ObRawExprUtils::build_and_expr(*ctx_->expr_factory_,
                                                      ob_params,
                                                      op_expr))) {
      LOG_WARN("fail to build constaint expr", K(ret));
    } else if (OB_ISNULL(op_expr)) {
      LOG_WARN("get null pointer", K(ret));
    } else if (OB_FAIL(op_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("fail to formalize expr", K(ret));
    } else if (OB_FAIL(ctx_->expr_constraints_.push_back(
                ObExprConstraint(op_expr, PreCalcExprExpectResult::PRE_CALC_RESULT_FALSE)))) {
      LOG_WARN("fail to push back constraints", K(ret));
    }

    // const constraints.
    // only one expr for each branches.
    if (OB_SUCC(ret)) {
      bool found = false;
      ObRawExpr *tmp_expr = NULL;
      for (int64_t j = 0; !found && j < helper.const_constraint_exprs_.count(); j++) {
        int64_t expr_branch_idx = helper.const_constraint_exprs_.at(j).second; 
        if (expr_branch_idx == branch_idx) {
          tmp_expr = helper.const_constraint_exprs_.at(j).first;
          if (OB_FAIL(ObTransformUtils::add_const_param_constraints(tmp_expr, ctx_))) {
            LOG_WARN("fail to add const constraints", K(ret));
          }
          found = true;
        }
      }
    }
  }

  return ret;
}

// overwrite order by
// overwrite distinct -- for UNION/EXCEPT/INTERSECT, stmt should keep distinct attribute.
// overwrite limit.
int ObTransformSimplifySet::replace_set_stmt_with_child_stmt(ObSelectStmt *&parent_stmt,
                                                             ObSelectStmt *child_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(child_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null pointer", K(ret));
  } else {
    /* 1. for limit clause
      * if both parent_stmt and child_stmt has limit, we should create a view to hold parent's limit.
      *
      * 2. for add_order_by, child stmt shouldn't have order_by_items or limit items.
      * if we meet conflict order by items. We can't directly reset child's order by.
      *  e.g., select * from t1 order by c1 limit 2 UNION ALL select * from t2 where 1=2 order by c2;
      * if we overwrite the order by, the result is not correct.
      *
      * 3. for distinct.
      * if child stmt has limit, we can't add distinct to child stmt. Since limit should be done before distinct.
      *
      * 4. select into is not allowed in subquery, we add the code here to handle conflict select into.
      * just in case of some transformer rule generating conflict select into.
    */
    ObSelectStmt *view_stmt = NULL;
    if (OB_FAIL(ObTransformUtils::create_stmt_with_generated_table(ctx_, child_stmt, view_stmt))) {
      LOG_WARN("fail to create view with generated table", K(ret));
    } else if (OB_ISNULL(view_stmt)) {
      LOG_WARN("view table is null", K(ret));
    } else if (OB_FAIL(add_order_by(view_stmt, parent_stmt))) {
      // set child's order by to view.
      LOG_WARN("fail to assign parents's order items to child", K(ret));
    } else {
      // set child's limit to view. and reset child's order by items.
      view_stmt->set_fetch_info(parent_stmt->get_offset_expr(),
                                parent_stmt->get_limit_expr(),
                                parent_stmt->get_limit_percent_expr());
      if (parent_stmt->is_set_distinct()) {
        view_stmt->assign_distinct();
      }
      if (parent_stmt->has_select_into()) {
        view_stmt->set_select_into(parent_stmt->get_select_into());
      }
      // if the result type of the view stmt's projection expr does not match that of the parent set stmt
      // cast expr need to be added to view's select items
      if (OB_SUCC(ret) && OB_UNLIKELY(view_stmt->get_select_item_size() != parent_stmt->get_select_item_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select item size missmatch", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < view_stmt->get_select_item_size(); i++) {
          ObRawExpr *expr1 = parent_stmt->get_select_item(i).expr_;
          ObRawExpr *expr2 = view_stmt->get_select_item(i).expr_;
          if (OB_ISNULL(expr1) || OB_ISNULL(expr2)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null pointer", K(ret), KP(expr1), KP(expr2));
          } else {
            if (OB_FAIL(ObTransformUtils::add_cast_for_replace_if_need(*(ctx_->expr_factory_),
                                                               expr1,
                                                               expr2,
                                                               ctx_->session_info_))) {
              LOG_WARN("fail to add cast expr for replace", K(ret));
            } else {
              view_stmt->get_select_item(i).expr_ = expr2;
            }
          }
        }
      }
      parent_stmt = view_stmt;
    }
  }
  
  return ret;
}

/*
  consider following sql:
    UPDATE test1 full join  test2 ON test1.h3=null set test1.h2='null' where test2.h2='akeyashi';
  The full join will be expanded into a set of left join and anti join. If the left join is eliminated at this time,
  the null column in the anti join will be filled in the dml table info of the upper update statement,
  which may cause an exception.
*/
int ObTransformSimplifySet::check_first_stmt_removable(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                       ObSelectStmt *&stmt,
                                                       ObIArray<int64_t> &remove_list,
                                                       bool &can_remove)
{
  int ret = OB_SUCCESS;
  can_remove = true;
  ObDMLStmt* parent_stmt = NULL;
  TableItem* table_item = NULL;
  bool is_dml_table = false;
  if (parent_stmts.empty()) {
    // do nothing
  } else if (OB_ISNULL(parent_stmt = parent_stmts.at(parent_stmts.count() - 1).stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!ObStmt::is_dml_write_stmt(parent_stmt->get_stmt_type())) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::get_generated_table_item(*parent_stmt, stmt, table_item))) {
    LOG_WARN("failed to get table_item", K(ret));
  } else if (OB_ISNULL(table_item)) {
    // do nothing
  } else if (OB_FAIL(static_cast<ObDelUpdStmt *>(parent_stmt)->has_dml_table_info(table_item->table_id_,
                                                                                  is_dml_table))) {
    LOG_WARN("failed to check is dml table", K(ret));
  } else if (is_dml_table) {
    can_remove = false;
  }
  return ret;
}

}// namespace sql
} // namespace oceanbase