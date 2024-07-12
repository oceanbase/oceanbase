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
#include "sql/rewrite/ob_transform_simplify_subquery.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "common/ob_smart_call.h"

using namespace oceanbase::sql;

int ObTransformSimplifySubquery::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                    ObDMLStmt *&stmt,
                                                    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  UNUSED(parent_stmts);
  if (OB_FAIL(push_down_outer_join_condition(stmt, is_happened))) {
    LOG_WARN("failed to push down outer join condition", K(is_happened));
  } else {
    trans_happened |= is_happened;
    OPT_TRACE("push down outer join condition:", is_happened);
    LOG_TRACE("succeed to push down outer join condition", K(is_happened));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(transform_subquery_as_expr(stmt, is_happened))) {
      LOG_WARN("failed to transform subquery to expr", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("transform subquery to expr:", is_happened);
      LOG_TRACE("succeed to transform subquery to expr", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(remove_redundant_select(stmt, is_happened))) {
      LOG_WARN("failed to remove simple select", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("remove simple select:", is_happened);
      LOG_TRACE("succeed to remove simple select", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(transform_not_expr(stmt, is_happened))) {
      LOG_WARN("failed to transform not expr", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("transform not expr:", is_happened);
      LOG_TRACE("succeed to transform not expr", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_limit_for_exists_subquery(stmt, is_happened))) {
      LOG_WARN("failed to add limit for exists subquery", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("add limit for exists subquery:", is_happened);
      LOG_TRACE("succeed to add limit for exists subquery", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(transform_any_all(stmt, is_happened))) {
      LOG_WARN("failed to transform_any_all", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("simply any/all subquery:", is_happened);
      LOG_TRACE("succeed to transform_any_all", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(transform_exists_query(stmt, is_happened))) {
      LOG_WARN("failed to transform_exists_query", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("simply exists subquery:", is_happened);
      LOG_TRACE("succeed to transform_exists_query", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(transform_any_all_as_exists(stmt, is_happened))) {
      LOG_WARN("failed to transform_any_all_as_exists", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("transform any/all as exists/not exists:", is_happened);
      LOG_TRACE("succeed to transform_any_all_as_exists", K(is_happened));
    }
  }
  if (OB_SUCC(ret) && trans_happened) {
    if (OB_FAIL(stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(stmt->formalize_query_ref_exprs())) {
      LOG_WARN("failed to formalize query ref exprs");
    } else if (OB_FAIL(add_transform_hint(*stmt))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::transform_subquery_as_expr(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  ObSEArray<ObRawExprPointer, 16> relation_expr_pointers;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_expr_pointers))) {
      LOG_WARN("failed to get_relation_exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_expr_pointers.count(); ++i) {
    ObRawExpr *expr = NULL;
    if (OB_FAIL(relation_expr_pointers.at(i).get(expr))) {
      LOG_WARN("failed to get relation expr", K(ret));
    } else if (OB_FAIL(try_trans_subquery_in_expr(stmt, expr, is_happened))) {
      LOG_WARN("failed to transform expr", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::try_trans_subquery_in_expr(ObDMLStmt *stmt,
                                                    ObRawExpr *&expr,
                                                    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  bool is_stack_overflow = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr) || OB_ISNULL(ctx_)
      || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(stmt), K(expr), K(ctx_));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type()) ||
             T_OP_EXISTS == expr->get_expr_type() ||
             T_OP_NOT_EXISTS == expr->get_expr_type() ||
             expr->is_alias_ref_expr()) {
    // 如果 expr 的param 必须是 subquery，那么不去改写它包含的子查询
    //do nothing
  } else if (expr->is_query_ref_expr() &&
             !static_cast<ObQueryRefRawExpr *>(expr)->is_multiset()) {
    //如果是 query ref expr，那么尝试改写
    if (OB_FAIL(do_trans_subquery_as_expr(stmt, expr, is_happened))) {
      LOG_WARN("failed to do_trans_subquery_as_expr", K(ret));
    } else if (is_happened) {
      trans_happened = true;
    }
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    //如果是 non-terminal expr，那么继续遍历孩子节点
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(try_trans_subquery_in_expr(stmt, expr->get_param_expr(i),
                                                        is_happened)))) {
        LOG_WARN("failed to trans param expr", K(ret));
      } else if (is_happened) {
        trans_happened = true;
      }
    }
    if (OB_SUCC(ret) && trans_happened) {
      if (OB_FAIL(expr->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::do_trans_subquery_as_expr(ObDMLStmt *stmt,
                                                           ObRawExpr *&expr,
                                                           bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(ret), K(stmt), K(expr));
  } else if (expr->is_query_ref_expr()) {
    ObQueryRefRawExpr *query_ref = static_cast<ObQueryRefRawExpr *>(expr);
    ObSelectStmt *sub_stmt = query_ref->get_ref_stmt();
    ObRawExpr *sub_expr = NULL;
    if (sub_stmt->get_stmt_hint().has_disable_hint(T_UNNEST)
        || sub_stmt->get_stmt_hint().enable_no_rewrite()) {
      // do nothing
    } else if (query_ref->is_cursor()) {
      /*do nothing*/
    } else if (OB_FAIL(is_subquery_to_expr_valid(sub_stmt, is_valid))) {
      LOG_WARN("fail to check subquery", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_UNLIKELY(1 != sub_stmt->get_select_item_size())
               || OB_ISNULL(sub_expr = sub_stmt->get_select_item(0).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sub stmt has invalid select item", K(ret),
               K(sub_stmt->get_select_item_size()), K(sub_expr));
    } else if (sub_expr->has_flag(CNT_ROWNUM)) {// 当 select expr 包含 rownum 时不能进行转换
      is_valid = false;
    } else if (OB_FAIL(ObTransformUtils::decorrelate(sub_expr, query_ref->get_exec_params()))) {
      LOG_WARN("failed to decorrleation expr", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(
                         stmt->get_subquery_exprs(),
                         query_ref))) {
      LOG_WARN("failed to remove child stmt", K(ret));
    } else if (OB_FAIL(append(stmt->get_subquery_exprs(), sub_stmt->get_subquery_exprs()))) {
      LOG_WARN("failed to append stmt subquery", K(ret));
    } else if (OB_FAIL(sub_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else {
      ObSEArray<ObRawExpr *, 1> old_expr;
      ObSEArray<ObRawExpr *, 1> new_expr;
      if (OB_FAIL(old_expr.push_back(expr)) || OB_FAIL(new_expr.push_back(sub_expr))) {
        LOG_WARN("push expr into array failed", K(ret));
      } else if (OB_FAIL(stmt->replace_relation_exprs(old_expr, new_expr))) {
        LOG_WARN("stmt replace inner expr failed", K(ret));
      } else {
        trans_happened = true;
      }
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::is_subquery_to_expr_valid(const ObSelectStmt *stmt,
                                                           bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (0 == stmt->get_from_item_size()
             && 1 == stmt->get_select_item_size()
             && !stmt->is_contains_assignment()
             // && !stmt->has_subquery()
             && 0 == stmt->get_aggr_item_size()
             && 0 == stmt->get_window_func_count()
             && 0 == stmt->get_condition_size()
             && 0 == stmt->get_having_expr_size()
             && !stmt->has_limit()
             && !stmt->is_hierarchical_query()
             && !stmt->is_set_stmt()
             && !stmt->has_sequence()) {
    is_valid = true;
  }
  if (OB_SUCC(ret) && is_valid) {
    if (OB_ISNULL(stmt->get_select_item(0).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt is null", K(ret), K(stmt));
    } else if (stmt->get_select_item(0).expr_->is_const_expr()) {
      // do nothing
    } else if (stmt->get_select_item(0).expr_->has_flag(CNT_PL_UDF)) {
      is_valid = false;
    }
  }
  return ret;
}

/**
 * @brief
 * transform not col op subquery to col !op subquery, eg:
 *    not c1 not in --> c1 = any
 *    not c1 in     --> c1 != all
 *    not c1 op any --> c1 !op all, where op \in {<, >, <=, >=}
 *    not c1 op all --> c1 !op any, where op \in {<, >, <=, >=}
 *    not c1 = any  --> c1 != all
 *    not c1 != all --> c1 = any
 * @param stmt
 * @param trans_happened
 * @return int
 */
int ObTransformSimplifySubquery::transform_not_expr(ObDMLStmt *stmt,
                                                    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  ObSEArray<ObRawExprPointer, 16> relation_expr_pointers;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_expr_pointers))) {
      LOG_WARN("failed to get_relation_exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_expr_pointers.count(); ++i) {
    ObRawExpr *expr = NULL;
    if (OB_FAIL(relation_expr_pointers.at(i).get(expr))) {
      LOG_WARN("failed to get relation expr", K(ret));
    } else if (OB_FAIL(do_transform_not_expr(expr, is_happened))) {
      LOG_WARN("failed to transform expr", K(ret));
    } else if (!is_happened) {
      // do nothing
    } else if (OB_FAIL(relation_expr_pointers.at(i).set(expr))) {
      LOG_WARN("failed to set expr", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::do_transform_not_expr(ObRawExpr *&expr, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObRawExpr *param = NULL;
  ObItemType expr_type = T_INVALID;
  trans_happened = false;
  if (OB_ISNULL(expr) ||
      OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(*param));
  } else if (expr->get_expr_type() != T_OP_NOT ||
             !expr->has_flag(CNT_SUB_QUERY)) {
    // do nothing
  } else if (OB_ISNULL(param = expr->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(*param));
  } else if (OB_FALSE_IT(expr_type = param->get_expr_type())) {
  } else if (expr_type == T_OP_SQ_NSEQ) {
    // same to T_OP_SQ_EQ as long as the expr is not null
    // do nothing current now
  } else if (param->has_flag(IS_WITH_ALL) || param->has_flag(IS_WITH_ANY)) {
    ObRawExpr *new_param = NULL;
    ObOpRawExpr *new_op_expr = NULL;
    ObSubQueryKey key_flag = param->has_flag(IS_WITH_ALL) ?
                              T_WITH_ANY : T_WITH_ALL;
    ObItemType new_type = ObTransformUtils::get_opposite_sq_cmp_type(expr_type);
    // 1. not col = all subquery can not be transformed
    // 2. not col != any subquery can not be transformed
    // 3. item type with T_INVALID can not be transformed
    bool is_not_valid = (expr_type == T_OP_SQ_EQ &&
                          param->has_flag(IS_WITH_ALL)) ||
                        (expr_type == T_OP_SQ_NE &&
                          param->has_flag(IS_WITH_ANY)) ||
                        (new_type == T_INVALID);
    if (is_not_valid) {
    } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(
                                            ObRawExpr::EXPR_OPERATOR,
                                            new_type,
                                            new_param))) {
      LOG_WARN("failed to create raw expr", K(ret));
    } else if (OB_FALSE_IT(new_op_expr = static_cast<ObOpRawExpr *>(new_param))) {
    } else if (OB_FALSE_IT(new_op_expr->set_subquery_key(key_flag))) {
    } else if (OB_FAIL(append(new_op_expr->get_param_exprs(),
                        static_cast<ObOpRawExpr *>(param)->get_param_exprs()))) {
      LOG_WARN("failed to append param exprs", K(ret));
    } else if (OB_FAIL(new_op_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else {
      expr = new_op_expr;
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::remove_redundant_select(ObDMLStmt *&stmt,
                                                         bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSelectStmt *new_stmt = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt *sel_stmt = static_cast<ObSelectStmt *>(stmt);
    if (OB_FAIL(try_remove_redundant_select(*sel_stmt, new_stmt))) {
      LOG_WARN("failed to check can remove simple select", K(ret));
    } else if (NULL != new_stmt) {
      stmt = new_stmt;
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::try_remove_redundant_select(ObSelectStmt &stmt,
                                                             ObSelectStmt *&new_stmt)
{
  int ret = OB_SUCCESS;
  new_stmt = NULL;
  ObRawExpr *sel_expr = NULL;
  ObQueryRefRawExpr *query_expr = NULL;
  ObSelectStmt *subquery = NULL;
  bool is_valid = false;
  if (!stmt.is_set_stmt() &&
      !stmt.is_hierarchical_query() &&
      1 == stmt.get_select_item_size() &&
      0 == stmt.get_from_item_size() &&
      0 == stmt.get_condition_size() &&
      0 == stmt.get_aggr_item_size() &&
      0 == stmt.get_having_expr_size() &&
      0 == stmt.get_window_func_count() &&
      !stmt.has_limit()) {
    if (OB_ISNULL(sel_expr = stmt.get_select_item(0).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret));
    } else if (!sel_expr->is_query_ref_expr()) {
      // do nothing
    } else if (FALSE_IT(query_expr = static_cast<ObQueryRefRawExpr *>(sel_expr))) {
      // never reach
    } else if (OB_ISNULL(subquery = query_expr->get_ref_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null stmt", K(ret));
    } else if (OB_FAIL(check_subquery_valid(*subquery, is_valid))) {
      LOG_WARN("failed to check subquery valid", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else {
      new_stmt = subquery;
    }
  }
  return ret;
}
/**
 * @brief check_subquery_valid
 * check subquery return equal one row, if empty do nothing
 * has limit 可能使结果为空不做改写;
 * select ... where rownum >2; rownum不包含1必空，包含判断较难，暂不处理
 * subquery should in format of:
 * 1. select ... from dual;  no where condition
 * 2. select aggr() ...;  <- no group by, no having
 */
int ObTransformSimplifySubquery::check_subquery_valid(ObSelectStmt &stmt,
                                                      bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObRawExpr *sel_expr = NULL;
  if (stmt.is_set_stmt() ||
      stmt.is_hierarchical_query()) {
    // do nothing
  } else {
    if (OB_UNLIKELY(1 != stmt.get_select_item_size()) ||
      OB_ISNULL(sel_expr = stmt.get_select_item(0).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected subquery", K(ret), K(stmt.get_select_item_size()), K(sel_expr));
    } else if (OB_NOT_NULL(stmt.get_limit_expr())) {
      // do nothing
    } else if (0 == stmt.get_from_item_size() &&
               0 == stmt.get_condition_size()) {
      is_valid = true;
    } else if (0 == stmt.get_group_expr_size() &&
               0 == stmt.get_rollup_expr_size() &&
               0 == stmt.get_having_expr_size() &&
               sel_expr->has_flag(CNT_AGG)) {
      is_valid = true;
    }
  }
  return ret;
}



//对于left join, 将仅含 right table column 且包含 subquery 的 on condition 下压到 right table
int ObTransformSimplifySubquery::push_down_outer_join_condition(ObDMLStmt *stmt,
                                                                bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret), K(stmt));
  } else {
    ObIArray<JoinedTable *> &join_tables = stmt->get_joined_tables();
    for (int64_t i = 0; OB_SUCC(ret) && i < join_tables.count(); ++i) {
      bool is_happened = false;
      if (OB_FAIL(push_down_outer_join_condition(stmt, join_tables.at(i), is_happened))) {
        LOG_WARN("failed to push down outer join condition", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::push_down_outer_join_condition(ObDMLStmt *stmt,
                                                        TableItem *join_table,
                                                        bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool left_happend = false;
  bool right_happend = false;
  JoinedTable *cur_joined = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(join_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret));
  } else if (!join_table->is_joined_table()) {
    /*do nothing*/
  } else if (FALSE_IT(cur_joined = static_cast<JoinedTable*>(join_table))) {
    /*do nothing*/
  } else if (OB_FAIL(SMART_CALL(push_down_outer_join_condition(stmt, cur_joined->left_table_,
                                                               left_happend)))) {
    LOG_WARN("failed to push down outer join condition", K(ret));
  } else if (OB_FAIL(SMART_CALL(push_down_outer_join_condition(stmt, cur_joined->right_table_,
                                                               right_happend)))) {
    LOG_WARN("failed to push down outer join condition", K(ret));
  } else if (OB_FAIL(try_push_down_outer_join_conds(stmt, cur_joined, trans_happened))) {
    LOG_WARN("fail to get outer join push down conditions", K(ret));
  } else {
    trans_happened |= left_happend || right_happend;
  }
  return ret;
}

int ObTransformSimplifySubquery::get_push_down_conditions(ObDMLStmt *stmt,
                                                          JoinedTable *join_table,
                                                          ObIArray<ObRawExpr *> &join_conds,
                                                          ObIArray<ObRawExpr *> &push_down_conds) {
  int ret = OB_SUCCESS;
  ObSqlBitSet<> right_table_ids;
  if (OB_ISNULL(stmt) || OB_ISNULL(join_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret));
  } else if (OB_FAIL(stmt->get_table_rel_ids(*join_table->right_table_, right_table_ids))) {
    LOG_WARN("failed to get target table rel ids", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < join_conds.count(); ++i) {
    ObSEArray<ObQueryRefRawExpr *, 4> query_refs;
    if (OB_ISNULL(join_conds.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret));
    } else if (!join_conds.at(i)->get_relation_ids().is_subset(right_table_ids) ||
               !join_conds.at(i)->has_flag(CNT_SUB_QUERY)) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(join_conds.at(i), query_refs))) {
      LOG_WARN("extract_query_ref_expr failed", K(ret), K(join_conds), K(i));
    } else {
      bool can_push_down = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < query_refs.count(); ++j) {
        if (OB_ISNULL(query_refs.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL", K(ret));
        } else if (query_refs.at(j)->get_ref_count() == 1){
          can_push_down = true;
        } else {
          can_push_down = false;
        }
      }

      if (OB_SUCC(ret) && can_push_down && OB_FAIL(push_down_conds.push_back(join_conds.at(i)))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  return ret;
}

// 当 left join 右表为 basic/generate/join table 时
// 对 on condition 中包含 subquery（要求ref_count = 1）仅包含本层右表列的条件进行下压:
//  1. 由右表生成generate table;
//  2. 下压满足条件 on condition.
int ObTransformSimplifySubquery::try_push_down_outer_join_conds(ObDMLStmt *stmt,
                                                        JoinedTable *join_table,
                                                        bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(join_table) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret));
  } else if (!join_table->is_left_join()) {
    /*do nothing*/
  } else if (OB_ISNULL(join_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret));
  } else if (!join_table->right_table_->is_basic_table()
             && !join_table->right_table_->is_generated_table()
             && !join_table->right_table_->is_temp_table()
             && !join_table->right_table_->is_joined_table()) {
    /*do nothing*/
  } else {
    ObSEArray<ObRawExpr*, 16> push_down_conds;
    TableItem *view_item = NULL;
    TableItem *right_table = join_table->right_table_;
    if (OB_FAIL(get_push_down_conditions(stmt,
                                         join_table,
                                         join_table->get_join_conditions(),
                                         push_down_conds))) {
      LOG_WARN("failed to get_push_down_conditions", K(ret), K(join_table));
    } else if (push_down_conds.empty()) {
      /*do nothing*/
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(join_table->get_join_conditions(), push_down_conds))) {
      LOG_WARN("failed to remove item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx_,
                                                                 stmt,
                                                                 view_item,
                                                                 right_table))) {
      LOG_WARN("failed to create empty view table", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                            stmt,
                                                            view_item,
                                                            right_table,
                                                            &push_down_conds))) {
      LOG_WARN("failed to create inline view", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::add_limit_for_exists_subquery(ObDMLStmt *stmt,
                                                               bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool happened = false;
  trans_happened = false;
  ObSEArray<ObRawExpr*, 16> relation_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ptr", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to relation exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); i++) {
      ObRawExpr *expr = relation_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null ptr", K(ret));
      } else if (OB_FAIL(recursive_add_limit_for_exists_expr(expr, happened))) {
        LOG_WARN("failed recursive add limit for exists expr", K(ret), K(expr));
      } else {
        trans_happened |= happened;
      }
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::recursive_add_limit_for_exists_expr(ObRawExpr *expr,
                                                                     bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool happened = false;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ptr", K(ret), K(expr));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(is_stack_overflow), K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(is_stack_overflow), K(ret));
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      ObRawExpr *param_expr = expr->get_param_expr(i);
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null ptr", K(ret), K(param_expr));
      } else if (OB_FAIL(SMART_CALL(recursive_add_limit_for_exists_expr(param_expr, happened)))) {
        LOG_WARN("failed recursive add limit for exists expr", K(ret), K(param_expr));
      } else {
        trans_happened |= happened;
      }
    }
    if (OB_SUCC(ret) &&
        (T_OP_EXISTS == expr->get_expr_type() ||
         T_OP_NOT_EXISTS == expr->get_expr_type())) {
      if (expr->get_param_count() != 1 || OB_ISNULL(expr->get_param_expr(0)) ||
          !expr->get_param_expr(0)->is_query_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", KPC(expr));
      } else {
        ObSelectStmt *subquery = NULL;
        ObOpRawExpr *op = static_cast<ObOpRawExpr*>(expr);
        ObQueryRefRawExpr *subq_expr = static_cast<ObQueryRefRawExpr *>(op->get_param_expr(0));
        if (OB_ISNULL(subq_expr) || OB_ISNULL(subquery = subq_expr->get_ref_stmt())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null ptr", K(ret), K(subq_expr), K(subquery));
        } else if (!subquery->has_limit() && !subquery->is_contains_assignment()) {
          if (OB_FAIL(ObTransformUtils::set_limit_expr(subquery, ctx_))) {
            LOG_WARN("add limit expr failed", K(*subquery), K(ret));
          } else {
            trans_happened = true;
          }
        } else {
          /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::transform_any_all(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  ObSEArray<ObRawExprPointer, 16> relation_expr_pointers;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (!stmt->is_sel_del_upd()) {
    // do nothing
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_expr_pointers))) {
    LOG_WARN("failed to get_relation_exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_expr_pointers.count(); ++i) {
    ObRawExpr *target = NULL;
    if (OB_FAIL(relation_expr_pointers.at(i).get(target))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("find to get expr from group", K(ret), K(target));
    } else if (OB_FAIL(try_transform_any_all(stmt, target, is_happened))) {
      LOG_WARN("fail to transform expr", K(ret));
    } else if (!is_happened) {
      // do nothing
    } else if (OB_FAIL(relation_expr_pointers.at(i).set(target))) {
      LOG_WARN("failed to set expr", K(ret));
    } else {
      trans_happened = true;
    }
  } // for end
  return ret;
}

int ObTransformSimplifySubquery::try_transform_any_all(ObDMLStmt *stmt, ObRawExpr *&expr, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  bool is_stack_overflow = false;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(expr));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type())) {
    if (OB_FAIL(do_transform_any_all(stmt, expr, is_happened))) {
      LOG_WARN("failed to do_trans_any_all", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    //check children
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(try_transform_any_all(stmt, expr->get_param_expr(i), is_happened)))) {
        LOG_WARN("failed to try_transform_any_all for param", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::do_transform_any_all(ObDMLStmt *stmt, ObRawExpr *&expr, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(expr));
  } else if (IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type())) {

    if (OB_FAIL(eliminate_groupby_distinct_in_any_all(expr, is_happened))) {
      LOG_WARN("failed to eliminate groupby distinct in any all", K(ret));
    } else {
      trans_happened |= is_happened;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(transform_any_all_as_min_max(stmt, expr, is_happened))) {
      LOG_WARN("failed to trans_any_all_as_min_max", K(ret));
    } else {
      trans_happened |= is_happened;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(eliminate_any_all_before_subquery(stmt, expr, is_happened))) {
      LOG_WARN("failed to eliminate_any_all_before_scalar_query", K(ret));
    } else {
      trans_happened |= is_happened;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(add_limit_for_any_all_subquery(expr, is_happened))) {
      LOG_WARN("failed to add limit for any all subquery", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::check_any_all_as_min_max(ObRawExpr *expr, bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *child_stmt = NULL;
  is_valid = false;

  //check op_expr
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(expr));
  } else if (IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type())
             && T_OP_SQ_EQ != expr->get_expr_type()
             && T_OP_SQ_NSEQ != expr->get_expr_type()
             && T_OP_SQ_NE != expr->get_expr_type()) {
    if (OB_ISNULL(expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(ret));
    } else if (expr->get_param_expr(1)->is_query_ref_expr()) {
      child_stmt = static_cast<ObQueryRefRawExpr*>(
        expr->get_param_expr(1))->get_ref_stmt();
      if (OB_ISNULL(child_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child stmt is null", K(ret));
      }
    }
  }

  //check child_stmt
  if (OB_FAIL(ret) || OB_ISNULL(child_stmt)) {
  } else if (!child_stmt->has_group_by()
             && !child_stmt->has_having()
             && !child_stmt->is_set_stmt()
             && !child_stmt->has_limit()
             && !child_stmt->is_hierarchical_query()
             && 1 == child_stmt->get_select_item_size()
             && 1 == child_stmt->get_table_size()) {
    ObRawExpr *sel_expr = child_stmt->get_select_item(0).expr_;
    if (OB_ISNULL(sel_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is NULL", K(ret));
    } else if (sel_expr->is_column_ref_expr()) {
      ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(sel_expr);
      bool is_nullable = true;
      bool is_match = false;
      ObArenaAllocator alloc;
      EqualSets &equal_sets = ctx_->equal_sets_;
      ObSEArray<ObRawExpr *, 4> const_exprs;
      if (OB_FAIL(child_stmt->get_stmt_equal_sets(equal_sets, alloc, true))) {
        LOG_WARN("failed to get stmt equal sets", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(child_stmt->get_condition_exprs(),
                                                              const_exprs))) {
        LOG_WARN("failed to compute const equivalent exprs", K(ret));
      } else if (OB_FAIL(ObTransformUtils::is_match_index(ctx_->sql_schema_guard_,
                                                          child_stmt,
                                                          col_expr,
                                                          is_match,
                                                          &equal_sets, &const_exprs))) {
        LOG_WARN("failed to check is match index prefix", K(ret));
      } else if (!is_match) {
        /*不能利用基表索引，不改写*/
      } else if (expr->has_flag(IS_WITH_ANY)) {
        is_valid = true;
      } else if (expr->has_flag(IS_WITH_ALL)) {
        if (OB_FAIL(ObTransformUtils::is_column_nullable(child_stmt,
                                                         ctx_->schema_checker_,
                                                         col_expr,
                                                         ctx_->session_info_,
                                                         is_nullable))) {
          LOG_WARN("failed to check is column nullable", K(ret));
        } else if (!is_nullable) {
          is_valid = true;
        }
      }
      equal_sets.reuse();
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::transform_any_all_as_min_max(ObDMLStmt *stmt,
                                                              ObRawExpr *expr,
                                                              bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_valid = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(expr));
  } else if (OB_FAIL(check_any_all_as_min_max(expr, is_valid))) {
    LOG_WARN("failed to check_any_all_as_min_max", K(ret));
  } else if (!is_valid) {
    /* do nothing */
  } else if (OB_ISNULL(expr->get_param_expr(1))
             || OB_UNLIKELY(!expr->get_param_expr(1)->is_query_ref_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr has invalid param", K(ret), K(expr->get_param_expr(1)));
  } else {
    ObQueryRefRawExpr *query_ref =
      static_cast<ObQueryRefRawExpr*>(expr->get_param_expr(1));
    ObSelectStmt *child_stmt = query_ref->get_ref_stmt();
    ObItemType aggr_type = get_aggr_type(expr->get_expr_type(),
                                         expr->has_flag(IS_WITH_ALL));
    if (OB_FAIL(do_transform_any_all_as_min_max(child_stmt, aggr_type,
                                                expr->has_flag(IS_WITH_ALL),
                                                trans_happened))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to do_trans_any_all_as_min_max", K(ret));
    }
  }
  return ret;
}

ObItemType ObTransformSimplifySubquery::get_aggr_type(ObItemType op_type, bool is_with_all)
{
  ObItemType aggr_type = T_INVALID;
  if (T_OP_SQ_LE == op_type || T_OP_SQ_LT == op_type) {
    if (is_with_all) {
      aggr_type = T_FUN_MIN;
    } else { /*with_any*/
      aggr_type = T_FUN_MAX;
    }
  } else if (T_OP_SQ_GE == op_type || T_OP_SQ_GT == op_type) {
    if (is_with_all) {
      aggr_type = T_FUN_MAX;
    } else { /*with_all*/
      aggr_type = T_FUN_MIN;
    }
  }
  return aggr_type;
}

int ObTransformSimplifySubquery::do_transform_any_all_as_min_max(ObSelectStmt *stmt,
                                                                const ObItemType aggr_type,
                                                                bool is_with_all,
                                                                bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObRawExpr *col_expr = NULL;
  ObAggFunRawExpr *aggr_expr = NULL;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)
      || OB_ISNULL(ctx_->expr_factory_)
      || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params or data member is NULL", K(ret), K(stmt), K(ctx_));
  } else if (stmt->get_select_item_size() <= 0
             || OB_ISNULL(col_expr = stmt->get_select_item(0).expr_)
             || (T_FUN_MAX != aggr_type && T_FUN_MIN != aggr_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have incorrect value", K(ret),
             K(stmt->get_select_items()), K(col_expr), K(aggr_type));
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(aggr_type, aggr_expr))) {
    LOG_WARN("fail to create raw expr", K(ret), K(aggr_expr));
  } else if (OB_ISNULL(aggr_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to create aggr expr", K(ret), K(aggr_type));
  } else if (OB_FAIL(aggr_expr->add_real_param_expr(col_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (OB_FAIL(aggr_expr->formalize(ctx_->session_info_))) {
    LOG_WARN("failed to formalize expr", K(ret));
  } else if (OB_FAIL(aggr_expr->pull_relation_id())) {
    LOG_WARN("failed to pull relation id", K(ret));
  } else {
    stmt->get_select_item(0).expr_ = aggr_expr;
    if (OB_FAIL(stmt->add_agg_item(*aggr_expr))) {
      LOG_WARN("fail to add agg_item", K(ret));
    } else if (is_with_all) {
      ObOpRawExpr *is_not_expr = NULL;
      if (OB_FAIL(ObTransformUtils::add_is_not_null(ctx_, stmt, aggr_expr, is_not_expr))) {
        LOG_WARN("failed to add is not null", K(ret));
      } else if (OB_FAIL(stmt->add_having_expr(is_not_expr))) {
        LOG_WARN("failed to add having expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::eliminate_any_all_before_subquery(ObDMLStmt *stmt,
                                                                  ObRawExpr *&expr,
                                                                  bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool can_be_removed = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(ret), K(stmt), K(expr));
  } else if (OB_FAIL(check_any_all_removeable(expr,
                                              can_be_removed))) {
    LOG_WARN("failed to check is any all removeable", K(ret));
  } else if (!can_be_removed) {
    /* do nothing */
  } else if (OB_ISNULL(expr->get_param_expr(1))
             || OB_UNLIKELY(!expr->get_param_expr(1)->is_query_ref_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr has invalid param", K(ret), K(expr->get_param_expr(1)));
  } else {
    ObQueryRefRawExpr *query_ref =
      static_cast<ObQueryRefRawExpr*>(expr->get_param_expr(1));
    if (OB_FAIL(clear_any_all_flag(stmt, expr, query_ref))) {
      LOG_WARN("failed to clear any all flag", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::check_any_all_removeable(ObRawExpr *expr,
                                                          bool &can_be_removed)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *sub_stmt = NULL;
  bool is_expr_query = false;
  bool is_aggr_query = false;
  can_be_removed = false;
  //check op
  if (IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type())) {
    if (OB_ISNULL(expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(ret));
    } else if (expr->get_param_expr(1)->is_query_ref_expr()) {
      ObQueryRefRawExpr *query_ref =
        static_cast<ObQueryRefRawExpr*>(expr->get_param_expr(1));
      if (OB_ISNULL(sub_stmt = query_ref->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sub_stmt is null", K(ret));
      }
    }
  }

  //check sub_stmt
  if (OB_FAIL(ret) || OB_ISNULL(sub_stmt)) {
  } else if (OB_FAIL(ObTransformUtils::is_expr_query(sub_stmt, is_expr_query))) {
    LOG_WARN("failed to check sub stmt is expr query", K(ret));
  } else if (is_expr_query) {
    can_be_removed = true;
  } else if (OB_FAIL(ObTransformUtils::is_aggr_query(sub_stmt, is_aggr_query))) {
    LOG_WARN("failed to check sub stmt is aggr query", K(ret));
  } else if (is_aggr_query) {
    can_be_removed = true;
  }
  return ret;
}

int ObTransformSimplifySubquery::clear_any_all_flag(ObDMLStmt *stmt,
                                                    ObRawExpr *&expr,
                                                    ObQueryRefRawExpr *query_ref)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr) || OB_ISNULL(query_ref)
      || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)
      || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(ret), K(stmt), K(expr),
             K(query_ref), K(ctx_));
  } else if (IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type())) {
    query_ref->set_is_set(false);

    ObOpRawExpr *tmp_op = NULL;
    ObItemType op_type = query_cmp_to_value_cmp(expr->get_expr_type());
    if (T_INVALID == op_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op type is not correct", K(ret), K(op_type));
    } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(op_type, tmp_op))) {
      LOG_WARN("failed to create tmp op", K(ret));
    } else if (OB_ISNULL(tmp_op)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(tmp_op->set_param_exprs(expr->get_param_expr(0),
                                               expr->get_param_expr(1)))) {
      LOG_WARN("failed to set params", K(ret));
    } else if (OB_FAIL(tmp_op->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize tmp op", K(ret));
    } else if (OB_FAIL(tmp_op->pull_relation_id())) {
      LOG_WARN("failed to pull relation id", K(ret));
    } else {
      expr = tmp_op;
    }
  }
  return ret;
}

ObItemType ObTransformSimplifySubquery::query_cmp_to_value_cmp(const ObItemType cmp_type)
{
  ObItemType ret = T_INVALID;
  switch (cmp_type) {
    case T_OP_SQ_EQ:
      ret = T_OP_EQ;
      break;
    case T_OP_SQ_NSEQ:
      ret = T_OP_NSEQ;
      break;
    case T_OP_SQ_LE:
      ret = T_OP_LE;
      break;
    case T_OP_SQ_LT:
      ret = T_OP_LT;
      break;
    case T_OP_SQ_GE:
      ret = T_OP_GE;
      break;
    case T_OP_SQ_GT:
      ret = T_OP_GT;
      break;
    case T_OP_SQ_NE:
      ret = T_OP_NE;
      break;
    default:
      ret = T_INVALID;
      break;
  }
  return ret;
}

int ObTransformSimplifySubquery::transform_exists_query(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExprPointer, 16> relation_expr_pointers;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (!stmt->has_subquery()) {
    // do nothing
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_expr_pointers))) {
    LOG_WARN("failed to get_relation_exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_expr_pointers.count(); ++i) {
    ObRawExpr *target = NULL;
    bool is_happened = false;
    if (OB_FAIL(relation_expr_pointers.at(i).get(target))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("find to get expr from group", K(ret), K(target));
    } else if (OB_FAIL(try_eliminate_subquery(stmt, target, is_happened))) {
      LOG_WARN("fail to transform expr", K(ret));
    } else if (!is_happened) {
      // do nothing
    } else if (OB_FAIL(relation_expr_pointers.at(i).set(target))) {
      LOG_WARN("failed to set expr", K(ret));
    } else {
      trans_happened = true;
    }
  } // for end
  return ret;
}

int ObTransformSimplifySubquery::try_eliminate_subquery(ObDMLStmt *stmt, ObRawExpr *&expr, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(ret));
  //bug:
  } else if (expr->has_flag(CNT_ROWNUM)) {
    /*do nothing */
  } else if (OB_FAIL(recursive_eliminate_subquery(stmt, expr, trans_happened))) {
      LOG_WARN("failed to recursive eliminate subquery", KP(expr), K(ret));
  }
  return ret;
}

int ObTransformSimplifySubquery::recursive_eliminate_subquery(ObDMLStmt *stmt,
                                                        ObRawExpr *&expr,
                                                        bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(expr), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(recursive_eliminate_subquery(stmt, expr->get_param_expr(i),
                                                          trans_happened)))) {
        LOG_WARN("failed to recursive eliminate subquery", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(eliminate_subquery(stmt, expr, trans_happened))) {
      LOG_WARN("failed to eliminate subquery", K(ret));
    }
  } else { /*do nothing*/ }
  return ret;
}

//1. 如果是Exist/Not Exist
//    1.1 判断是否可以消除subquery
//    2.2 如果不能消除：
//        a. 消除select list --> select 1
//        b. 消除group by
//        c. 消除order by
//2. 如果是ANY/ALL
//  2.1 消除group by
//  2.2 非相关any子查询如果select item为const item，则添加limit 1，
//      eg: select * from t1 where c1 in (select 1 from t2);
//          ==> select * from t1 where c1 in (select 1 from t2 limit 1);
//  2.3 消除distinct
int ObTransformSimplifySubquery::eliminate_subquery(ObDMLStmt *stmt,
                                              ObRawExpr *&expr,
                                              bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool can_be_eliminated = false;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is NULL in eliminate subquery", K(ret));
  } else if (!expr->has_flag(CNT_SUB_QUERY)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no subquery in expr", K(ret));
  } else {
    ObQueryRefRawExpr *subq_expr = NULL;
    ObSelectStmt *subquery = NULL;
    const ObRawExpr *left_hand = NULL;
    bool check_status = false;
    if (T_OP_EXISTS == expr->get_expr_type() || T_OP_NOT_EXISTS == expr->get_expr_type()) {
      if (OB_ISNULL(subq_expr = static_cast<ObQueryRefRawExpr *>(expr->get_param_expr(0)))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Subquery expr is NULL", K(ret));
      } else if (OB_ISNULL(subquery = subq_expr->get_ref_stmt())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Subquery stmt is NULL", K(ret));
      } else if (subquery->is_contains_assignment()) {
        // do nothing
      } else if (subquery->is_values_table_query() &&
                 !ObTransformUtils::is_enable_values_table_rewrite(stmt->get_query_ctx()->optimizer_features_enable_version_)) {
        // do nothing
      } else if (OB_FAIL(subquery_can_be_eliminated_in_exists(expr->get_expr_type(),
                                                              subquery,
                                                              can_be_eliminated))) {
        LOG_WARN("Subquery elimination of select list in EXISTS fails", K(ret));
      } else if (can_be_eliminated){
        if (OB_FAIL(eliminate_subquery_in_exists(stmt, expr, trans_happened))) {
          LOG_WARN("failed to eliminate subquery in exists", K(ret), KP(expr));
        }
        LOG_TRACE("finish to eliminate subquery", K(can_be_eliminated), K(ret));

      } else if (OB_FAIL(empty_table_subquery_can_be_eliminated_in_exists(expr, can_be_eliminated))) {
        LOG_WARN("failed to check empty table subquery can be eliminate", K(ret));
      } else if (can_be_eliminated) {
        if (OB_FAIL(do_trans_empty_table_subquery_as_expr(expr, trans_happened))) {
          LOG_WARN("failed to do trans empty table subquery as expr", K(ret));
        }
        LOG_TRACE("finish to eliminate empty table subquery", K(ret));
      } else if (!can_be_eliminated) {
        if (OB_FAIL(simplify_select_items(stmt, expr->get_expr_type(), subquery, false, trans_happened))) {
          LOG_WARN("Simplify select items in EXISTS fails", K(ret));
        } else if (OB_FAIL(eliminate_groupby_in_exists(stmt, expr->get_expr_type(),
                                                       subquery, trans_happened))) {
          LOG_WARN("Subquery elimination of group by in EXISTS fails", K(ret));
        //EXISTS, NOT EXISTS子查询中的order by可以消除
        } else if (subquery->get_order_items().count() > 0) {
          trans_happened = true;
          subquery->get_order_items().reset();
        }
        if (OB_SUCC(ret) && trans_happened) {
          subq_expr->set_output_column(subquery->get_select_item_size());
        }
      }
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::subquery_can_be_eliminated_in_exists(const ObItemType op_type,
                                                                const ObSelectStmt *stmt,
                                                                bool &can_be_eliminated) const
{
  int ret = OB_SUCCESS;
  can_be_eliminated = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (stmt->is_set_stmt()) {
    if (ObSelectStmt::UNION == stmt->get_set_op() && !stmt->is_recursive_union()) {
      const ObIArray<ObSelectStmt*> &child_stmts = stmt->get_set_query();
      //loop child stmts and if one of them can be eliminated, then eliminate the whole set_stmt
      for (int64_t i = 0; OB_SUCC(ret) && !can_be_eliminated && i < child_stmts.count(); ++i) {
        ObSelectStmt *child = child_stmts.at(i);
        if (OB_FAIL(SMART_CALL(subquery_can_be_eliminated_in_exists(op_type, child, can_be_eliminated)))) {
          LOG_WARN("Subquery elimination of select list in EXISTS fails", K(ret));
        }
      }
    } else {
      /* other type of set query can no be eliminated */
    }
  } else if (is_subquery_not_empty(*stmt)) {
    bool has_limit = false;
    if (OB_FAIL(check_limit(op_type, stmt, has_limit))) {
      LOG_WARN("failed to check subquery has unremovable limit", K(ret));
    } else if (!has_limit) {
      can_be_eliminated = true;
    }
  }
  return ret;
}

bool ObTransformSimplifySubquery::is_subquery_not_empty(const ObSelectStmt &stmt) {

    /*
    situation 1:
      select 1+1 -> can be simplify
      select 1+1 from dual having 0 -> not satisfy
      select 1+1 from dual where 1=0 -> not satisfy
      select 1+1 from dual limit 0 -> not satisfy but limit will be checked later
    situation 2:
      select max(c1) from t1 ->always not empty
    */

    return  (0 == stmt.get_table_size()
            && 0 == stmt.get_having_expr_size()
            && 0 == stmt.get_condition_size())
          || (0 == stmt.get_group_expr_size()
             && !stmt.has_rollup()
             && stmt.get_aggr_item_size() > 0
             && !stmt.has_having());
}

int ObTransformSimplifySubquery::select_items_can_be_simplified(const ObItemType op_type,
                                                                const ObSelectStmt *stmt,
                                                                bool &can_be_simplified) const
{
  /*
  * 1. calculate max_select_item_size
  *    1.1 for set stmt, it depends on its child stmt, so only consider its child stmt
  * 2. decide whether need to simplify select_item
  *    2.1 for set stmt, if all child stmt can be simplied, then the union set stmt
  *        get max_select_item_size = 1(max_select_item_size will never be updated)
  *    2.2 any of one child stmt can be simplifed , the whole union set stmt can be simplified
  *
  */
  int ret = OB_SUCCESS;
  bool has_limit = false;
  can_be_simplified = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is invalid", K(ret), K(stmt));
  } else if (stmt->is_set_stmt() &&
             (ObSelectStmt::INTERSECT == stmt->get_set_op() ||
              ObSelectStmt::EXCEPT == stmt->get_set_op() ||
              stmt->is_recursive_union())) {
    //do nothing
  } else if (OB_FAIL(check_limit(op_type, stmt, has_limit))) {
    LOG_WARN("failed to check limit", K(ret));
  } else if (has_limit &&
            (stmt->has_distinct() ||
             (stmt->is_set_stmt() && stmt->is_set_distinct()))) {
    /*
    create table t1(a int);
    insert into t1 values (1),(2),(3),(4);
    select distinct 1 from t1 limit 1 offset 3; // is empty
    select distinct a from t1 limit 1 offset 3; // not empty
    */
  } else if (stmt->get_select_item_size() == 1 &&
             NULL != stmt->get_select_item(0).expr_ &&
             stmt->get_select_item(0).expr_->is_const_raw_expr()) {
    // do nothing
  } else {
    can_be_simplified = true;
  }
  return ret;
}

int ObTransformSimplifySubquery::groupby_can_be_eliminated_in_exists(const ObItemType op_type,
                                                               const ObSelectStmt *stmt,
                                                               bool &can_be_eliminated) const
{
  int ret = OB_SUCCESS;
  can_be_eliminated = false;
  // 当[not] exists(subq)满足以下所有条件时，可消除group by子句:
  // 1. 当前stmt不是set stmt
  // 2. 没有having子句
  // 3. 没有limit子句
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (0 == stmt->get_table_size() || stmt->is_set_stmt()) {
    // Only non-set stmt will be eliminated and do nothing for other DML stmts:
    // 1. set -> No elimination
    // 2. select 1 + floor(2) (table_size == 0) -> No elimination
  } else if (stmt->has_group_by()
             && 0 == stmt->get_aggr_item_size()
             && !stmt->has_having()){ // No having, no limit, no aggr
    bool has_limit = false;
    if (OB_FAIL(check_limit(op_type, stmt, has_limit))) {
      LOG_WARN("failed to check subquery has unremovable limit", K(ret));
    } else if (!has_limit) {
      can_be_eliminated = true;
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::groupby_can_be_eliminated_in_any_all(const ObSelectStmt *stmt,
                                                                bool &can_be_eliminated) const
{
  int ret = OB_SUCCESS;
  can_be_eliminated = false;
  // 当Any/all/in(subq)满足以下所有条件时，可消除group by子句:
  // 1. 当前stmt不是set stmt
  // 2. 没有having子句
  // 3. 没有limit子句
  // 4. 无聚集函数（select item中）
  // 5. 非常量select item列，全部包含在group exprs中
  if (OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (0 == stmt->get_table_size() || stmt->is_set_stmt()) {
    // Only non-set stmt will be eliminated and do nothing for other DML stmts:
    // 1. set -> No elimination
    // 2. select 1 + floor(2) (table_size == 0) -> No elimination
  } else if (stmt->is_values_table_query() &&
             !ObTransformUtils::is_enable_values_table_rewrite(stmt->get_query_ctx()->optimizer_features_enable_version_)) {
    /* do nothing */
  } else if (stmt->has_group_by()
             && !stmt->has_having()
             && !stmt->has_limit()
             && 0 == stmt->get_aggr_item_size()) {
    // Check if select list is involved in group exprs
    ObRawExpr *s_expr = NULL;
    bool all_in_group_exprs = true;
    for (int i = 0; OB_SUCC(ret) && all_in_group_exprs && i < stmt->get_select_item_size(); ++i) {
      if (OB_ISNULL(s_expr = stmt->get_select_item(i).expr_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("select list expr is NULL", K(ret));
      } else if ((s_expr)->has_flag(CNT_COLUMN)) {
        if (!ObOptimizerUtil::find_item(stmt->get_group_exprs(), s_expr)) {
          all_in_group_exprs = false;
        } else { /* do nothing */ }
      } else { /* do nothing */ }
    } // for
    if (OB_SUCCESS == ret && all_in_group_exprs) {
      can_be_eliminated = true;
    } else { /* do nothing */ }
  } else { /* do nothing */ }
  return ret;
}

int ObTransformSimplifySubquery::eliminate_subquery_in_exists(ObDMLStmt *stmt,
                                                        ObRawExpr *&expr,
                                                        bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  ObSelectStmt *subquery = NULL;
  bool add_limit_constraint = false;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer Error", KP(expr), KP_(ctx), KP(expr_factory), K(ret));
  } else if (T_OP_EXISTS == expr->get_expr_type() || T_OP_NOT_EXISTS == expr->get_expr_type()) {
    ObOpRawExpr *op = static_cast<ObOpRawExpr*>(expr);
    ObQueryRefRawExpr *subq_expr = static_cast<ObQueryRefRawExpr *>(op->get_param_expr(0));
    if (OB_ISNULL(subq_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid Query Ref Expr", K(ret));
    } else if (OB_ISNULL(subquery = subq_expr->get_ref_stmt())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Subquery stmt is NULL", K(ret));
    } else if (subquery->is_values_table_query() &&
               !ObTransformUtils::is_enable_values_table_rewrite(stmt->get_query_ctx()->optimizer_features_enable_version_)) { /* do nothing */
      //Just in case different parameters hit same plan, firstly we need add const param constraint
    } else if (OB_FAIL(need_add_limit_constraint(expr->get_expr_type(), subquery, add_limit_constraint))){
      LOG_WARN("failed to check limit constraints", K(ret));
    } else if (add_limit_constraint &&
              OB_FAIL(ObTransformUtils::add_const_param_constraints(subquery->get_limit_expr(), ctx_))) {
      LOG_WARN("failed to add const param constraints", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_subquery_exprs(), subq_expr))) {
      LOG_WARN("remove expr failed", K(ret));
    } else {
      ObRawExpr *c_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(expr_factory, c_expr, (T_OP_EXISTS == expr->get_expr_type())))) {
        LOG_WARN("failed to create expr", K(ret));
      } else if (OB_ISNULL(c_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("create expr error in eliminate_subquery_in_exists()", KP(c_expr), K(ret));
      } else if (OB_FAIL(c_expr->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize a new expr", K(ret));
      } else {
        expr = c_expr;
        trans_happened = true;
      }
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::simplify_select_items(ObDMLStmt *stmt,
                                                      const ObItemType op_type,
                                                      ObSelectStmt *subquery,
                                                      bool parent_is_set_query,
                                                      bool &trans_happened)
{
    int ret = OB_SUCCESS;
    bool has_limit = false;
    ObRawExprFactory *expr_factory = NULL;
    bool can_be_simplified = false;
    if (OB_ISNULL(subquery) || OB_ISNULL(stmt) || OB_ISNULL(ctx_) ||
        OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL pointer Error", KP(subquery), KP_(ctx), KP(expr_factory), K(ret));
    } else if (OB_FAIL(select_items_can_be_simplified(op_type,
                                                      subquery,
                                                      can_be_simplified))) {
      LOG_WARN("Checking if select list can be eliminated in subquery failed", K(ret));
    } else if (!can_be_simplified) {
      //do nothing
    } else if (ObSelectStmt::UNION == subquery->get_set_op() && !subquery->is_recursive_union()) {
        const ObIArray<ObSelectStmt*> &child_stmts = subquery->get_set_query();
        for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
          ObSelectStmt *child = child_stmts.at(i);
          if (OB_FAIL(SMART_CALL(simplify_select_items(stmt, op_type, child, true, trans_happened)))) {
            LOG_WARN("Simplify select list in EXISTS fails", K(ret));
          }
        }
        ObExprResType res_type;
        res_type.set_type(ObIntType);
        for(int64_t i = 0; OB_SUCC(ret) && i < subquery->get_select_item_size(); i++) {
          SelectItem &select_item = subquery->get_select_item(i);
          if(OB_ISNULL(select_item.expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null select expr", K(ret), K(select_item));
          } else {
            select_item.expr_->set_result_type(res_type);
          }
        }
        bool has_limit = false;
        bool add_limit_constraint = false;
        if (OB_FAIL(ret)){

        } else if (OB_FAIL(check_limit(op_type, subquery, has_limit))) {
          LOG_WARN("failed to check subquery has unremovable limit", K(ret));
        } else if(OB_FAIL(need_add_limit_constraint(op_type, subquery, add_limit_constraint))){
          LOG_WARN("failed to check limit constraints", K(ret));
        } else if(add_limit_constraint &&
                  OB_FAIL(ObTransformUtils::add_const_param_constraints(subquery->get_limit_expr(), ctx_))) {
          LOG_WARN("failed to add const param constraints", K(ret));
        } else if (!has_limit) {
          subquery->assign_set_all();
        }
    } else {
      // Add single select item with const int 1
        int64_t const_value = 1;
        ObSEArray<ObAggFunRawExpr*, 8> aggr_items_in_having;
        int max_select_item_size = parent_is_set_query ? subquery->get_select_item_size() : 1;
        subquery->clear_select_item();
        //save aggr item in having
        subquery->clear_aggr_item();
        if (OB_FAIL(ObTransformUtils::extract_aggr_expr(subquery->get_having_exprs(),aggr_items_in_having))) {
          LOG_WARN("failed to get aggr items", K(ret));
        } else if (OB_FAIL(append_array_no_dup(subquery->get_aggr_items(),aggr_items_in_having))) {
          LOG_WARN("failed to remove item", K(ret));
        }
        // Clear distinct flag
        subquery->assign_all();
        //reset window function
        subquery->get_window_func_exprs().reset();
        for(int64_t i = 0; OB_SUCC(ret) && i < max_select_item_size; i++) {
           ObConstRawExpr *c_expr = NULL;
          if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType,
                                                          const_value, c_expr))) {
            LOG_WARN("failed to create expr", K(ret));
          } else if (OB_ISNULL(c_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("create expr error in simplify select item", K(c_expr), K(ret));
          } else if (OB_FAIL(c_expr->formalize(ctx_->session_info_))) {
            LOG_WARN("failed to formalize a new expr", K(ret));
          } else {
            SelectItem select_item;
            select_item.alias_name_ = "1";
            select_item.expr_name_ = "1";
            select_item.expr_ = c_expr;
            if (OB_FAIL(subquery->add_select_item(select_item))) {
              LOG_WARN("Failed to add select item", K(select_item), K(ret));
            //Just in case different parameters hit same plan,firstly we need add const param constraint
            } else {
              trans_happened = true;
            }
          }
        }
        bool add_limit_constraint = false;
        if (OB_FAIL(ret)) {

        } else if(OB_FAIL(need_add_limit_constraint(op_type, subquery, add_limit_constraint))){
          LOG_WARN("failed to check limit constraints", K(ret));
        } else if(add_limit_constraint &&
                  OB_FAIL(ObTransformUtils::add_const_param_constraints(subquery->get_limit_expr(), ctx_))) {
          LOG_WARN("failed to add const param constraints", K(ret));
        }
      }
    return ret;
}
int ObTransformSimplifySubquery::eliminate_groupby_in_exists(ObDMLStmt *stmt,
                                                       const ObItemType op_type,
                                                       ObSelectStmt *&subquery,
                                                       bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool can_be_eliminated = false;
  if (OB_ISNULL(subquery) || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Subquery is NULL", K(ret));
  } else if (subquery->is_set_stmt()) {
    // for set stmt, it should consider whether its child stmt can remove groupby
    if (!subquery->is_recursive_union()) {
      const ObIArray<ObSelectStmt*> &child_stmts = subquery->get_set_query();
      //loop child stmts and if one of them can be eliminated
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
        ObSelectStmt *child = child_stmts.at(i);
        if (OB_FAIL(SMART_CALL(eliminate_groupby_in_exists(stmt,op_type, child, trans_happened)))) {
          LOG_WARN("eliminate groupby in child stmt of set query fails", K(ret));
        }
      }
    } else {
      /* union-recursive set stmt not consider */
    }
  } else if (OB_FAIL(groupby_can_be_eliminated_in_exists(op_type, subquery, can_be_eliminated))) {
    LOG_WARN("Checking if group by can be eliminated in subquery in exists failed", K(ret));
  } else if (!can_be_eliminated) {
    /*do nothing*/
  } else if (subquery->has_group_by()) {
      // Eliminate group by
      trans_happened = true;
      bool add_limit_constraint = false;
      //Just in case different parameters hit same plan, firstly we need add const param constraint
      if(OB_FAIL(need_add_limit_constraint(op_type, subquery, add_limit_constraint))){
        LOG_WARN("failed to check limit constraints", K(ret));
      } else if (add_limit_constraint &&
                 OB_FAIL(ObTransformUtils::add_const_param_constraints(subquery->get_limit_expr(), ctx_))) {
        LOG_WARN("failed to add const param constraints", K(ret));
      } else {
        subquery->get_group_exprs().reset();
        trans_happened = true;
      }
  } else { /* do nothing */ }
  return ret;
}

int ObTransformSimplifySubquery::eliminate_groupby_in_any_all(ObSelectStmt *&subquery, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool can_be_eliminated = false;
  if (OB_ISNULL(subquery)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Subquery is NULL", K(ret));
  } else if (OB_FAIL(groupby_can_be_eliminated_in_any_all(subquery, can_be_eliminated))) {
    LOG_WARN("Checking if group by can be eliminated in subquery failed", K(ret));
  } else if (can_be_eliminated) {
    if (subquery->has_group_by()) {
      // Eliminate group by
      trans_happened = true;
      subquery->get_group_exprs().reset();
    } else { /* do nothing */ }
  } else { /* do nothing */ }

  return ret;
}


int ObTransformSimplifySubquery::eliminate_groupby_distinct_in_any_all(ObRawExpr *expr, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObQueryRefRawExpr *subq_expr = NULL;
  ObSelectStmt *subquery = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(ret), K(expr));
  } else if (!expr->has_flag(IS_WITH_ALL) && !expr->has_flag(IS_WITH_ANY)) {
  } else if (OB_UNLIKELY(2 != expr->get_param_count())
             || OB_UNLIKELY(!expr->get_param_expr(1)->is_query_ref_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected expr", K(ret), KPC(expr));
  } else if (OB_ISNULL(subq_expr = static_cast<ObQueryRefRawExpr *>(expr->get_param_expr(1))) ||
             OB_ISNULL(static_cast<ObRawExpr *>(expr->get_param_expr(0)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Subquery or left_hand expr is NULL", K(ret));
  } else if (OB_ISNULL(subquery = subq_expr->get_ref_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subquery stmt is NULL", K(ret));
  } else if (OB_FAIL(eliminate_groupby_in_any_all(subquery, trans_happened))) {
        LOG_WARN("Subquery elimination of group by in ANY, ALL fails", K(ret));
  } else if (!subq_expr->get_exec_params().empty()) {
    // correlated subquery
    if (OB_FAIL(eliminate_distinct_in_any_all(subquery, trans_happened))) {
      LOG_WARN("Subquery elimination of distinct in ANY, ALL fails", K(ret));
    } else {/*do nothing*/}
  }

  return ret;
}
int ObTransformSimplifySubquery::eliminate_distinct_in_any_all(ObSelectStmt *subquery,
                                                         bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool contain_rownum = false;
  if (OB_ISNULL(subquery)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Subquery is NULL", K(ret));
  } else if (subquery->has_limit()) {
    /*do nothing*/
  } else if (OB_FAIL(subquery->has_rownum(contain_rownum))) {
    LOG_WARN("failed to check subquery has rownum", K(ret));
  } else if (contain_rownum) {
    /*do nothing*/
  } else if (subquery->has_distinct()) {
    subquery->assign_all();
    trans_happened = true;
  } else { /* do nothing */ }
  return ret;
}

int ObTransformSimplifySubquery::add_limit_for_any_all_subquery(ObRawExpr *expr, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObQueryRefRawExpr *subq_expr = NULL;
  ObSelectStmt *subquery = NULL;
  bool check_status = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!expr->has_flag(IS_WITH_ANY))  {
    /*do nothing*/
  } else if (OB_ISNULL(subq_expr = static_cast<ObQueryRefRawExpr *>(expr->get_param_expr(1)))
             || OB_ISNULL(static_cast<ObRawExpr *>(expr->get_param_expr(0)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Subquery or left_hand expr is NULL", K(ret));
  } else if (OB_ISNULL(subquery = subq_expr->get_ref_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Subquery stmt is NULL", K(ret));
  } else if (subq_expr->has_exec_param())  {
    //do nothing
  } else if (OB_FAIL(check_need_add_limit(subquery, check_status))) {
    LOG_WARN("check need add limit failed", K(ret));
  } else if (!check_status) {
    /*do nothing*/
  } else if (OB_FAIL(ObTransformUtils::set_limit_expr(subquery, ctx_))) {
    LOG_WARN("add limit expr failed", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformSimplifySubquery::check_need_add_limit(ObSelectStmt *subquery, bool &need_add_limit)
{
  int ret = OB_SUCCESS;
  need_add_limit = false;
  if (OB_ISNULL(subquery)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Subquery is NULL", K(ret));
  } else if (subquery->has_limit()) {
    /*do nothing*/
  } else if (subquery->get_from_item_size() > 0) {
    const ObRawExpr *select_expr = NULL;
    bool is_const = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_const && i < subquery->get_select_item_size(); ++i) {
      const SelectItem &select_item = subquery->get_select_item(i);
      if (OB_ISNULL(select_expr = select_item.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select expr is NULL", K(ret));
      } else if (select_expr->is_const_expr()) {
        /*do nothing*/
      } else {
        is_const = false;
      }
    }
    if (OB_SUCC(ret) && is_const) {
      need_add_limit = true;
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::need_add_limit_constraint(const ObItemType op_type,
                                       const ObSelectStmt *subquery,
                                       bool &add_limit_constraint) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  bool is_const_select = false;
  bool has_limit = false;
  add_limit_constraint = false;
  if (OB_ISNULL(subquery) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) ||
      OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(subquery), K(ctx_), K(ctx_->exec_ctx_), K(plan_ctx));
  } else if (OB_FAIL(check_const_select(*subquery, is_const_select))) {
    LOG_WARN("failed to check const select", K(ret));
  } else if (op_type != T_OP_EXISTS && op_type != T_OP_NOT_EXISTS && !is_const_select) {
    has_limit = subquery->has_limit();
  } else if (!subquery->has_limit() ||
             subquery->get_offset_expr() != NULL ||
             subquery->get_limit_percent_expr() != NULL) {
    has_limit = subquery->has_limit();
  } else {
    bool is_null_value = false;
    int64_t limit_value = 0;
    if (OB_FAIL(ObTransformUtils::get_limit_value(subquery->get_limit_expr(),
                                                  &plan_ctx->get_param_store(),
                                                  ctx_->exec_ctx_,
                                                  ctx_->allocator_,
                                                  limit_value,
                                                  is_null_value))) {
      LOG_WARN("failed to get_limit_value", K(ret));
    } else if (!is_null_value && limit_value >= 1) {
      has_limit = false;
      //Just in case different parameters hit same plan, firstly we need add const param constraint
      add_limit_constraint = true;
    } else {
      has_limit = true;
    }
  }
  return ret;
}
int ObTransformSimplifySubquery::check_limit(const ObItemType op_type,
                                       const ObSelectStmt *subquery,
                                       bool &has_limit) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  bool is_const_select = false;
  has_limit = false;
  if (OB_ISNULL(subquery) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) ||
      OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(subquery), K(ctx_), K(ctx_->exec_ctx_), K(plan_ctx));
  } else if (OB_FAIL(check_const_select(*subquery, is_const_select))) {
    LOG_WARN("failed to check const select", K(ret));
  } else if (op_type != T_OP_EXISTS && op_type != T_OP_NOT_EXISTS && !is_const_select) {
    has_limit = subquery->has_limit();
  } else if (!subquery->has_limit() ||
             subquery->get_offset_expr() != NULL ||
             subquery->get_limit_percent_expr() != NULL) {
    //not limit
    //limit 1,3
    //limit 20%
    has_limit = subquery->has_limit();
  } else {
    bool is_null_value = false;
    int64_t limit_value = 0;
    if (OB_FAIL(ObTransformUtils::get_limit_value(subquery->get_limit_expr(),
                                                  &plan_ctx->get_param_store(),
                                                  ctx_->exec_ctx_,
                                                  ctx_->allocator_,
                                                  limit_value,
                                                  is_null_value))) {
      LOG_WARN("failed to get_limit_value", K(ret));
    } else if (!is_null_value && limit_value >= 1) {
      //limit n
      has_limit = false;
    } else {
      //limit 0
      has_limit = true;
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::check_const_select(const ObSelectStmt &stmt,
                                              bool &is_const_select) const
{
  int ret = OB_SUCCESS;
  is_const_select = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_const_select && i < stmt.get_select_item_size(); ++i) {
    if (OB_ISNULL(stmt.get_select_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else {
      is_const_select = stmt.get_select_item(i).expr_->is_const_expr();
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::try_trans_any_all_as_exists(ObDMLStmt *stmt,
                                                             ObRawExpr *&expr,
                                                             ObNotNullContext *not_null_ctx,
                                                             bool is_bool_expr,
                                                             bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  bool is_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(expr));
  } else if (IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type())) {
    if (OB_FAIL(ObTransformUtils::check_can_trans_any_all_as_exists(ctx_,
                                                                    expr,
                                                                    is_bool_expr,
                                                                    true,
                                                                    is_valid))) {
      LOG_WARN("failed to check in can tras as exists", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::do_trans_any_all_as_exists(ctx_,
                                                                    expr,
                                                                    not_null_ctx,
                                                                    is_happened))) {
      LOG_WARN("failed to do trans any all as exists", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  } else if (!expr->has_flag(CNT_SUB_QUERY)) {
    // do nothing
  } else if (expr->get_expr_type() == T_OP_CASE) {
    ObCaseOpRawExpr* case_expr = static_cast<ObCaseOpRawExpr*>(expr);
    for (int64_t i = 0; OB_SUCC(ret) && i < case_expr->get_when_expr_size(); ++i) {
      if (OB_FAIL(SMART_CALL(try_trans_any_all_as_exists(stmt,
                                                         case_expr->get_when_param_expr(i),
                                                         not_null_ctx,
                                                         true,
                                                         is_happened)))) {
        LOG_WARN("failed to try_transform_any_all for param", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < case_expr->get_then_expr_size(); ++i) {
      if (OB_FAIL(SMART_CALL(try_trans_any_all_as_exists(stmt,
                                                         case_expr->get_then_param_expr(i),
                                                         not_null_ctx,
                                                         false,
                                                         is_happened)))) {
        LOG_WARN("failed to try_transform_any_all for param", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(case_expr->get_default_param_expr()) &&
               OB_FAIL(SMART_CALL(try_trans_any_all_as_exists(
                                  stmt,
                                  case_expr->get_default_param_expr(),
                                  not_null_ctx,
                                  false,
                                  is_happened)))) {
      LOG_WARN("failed to try_transform_any_all for param", K(ret));
    } else if (OB_FALSE_IT(trans_happened |= is_happened)) {
      //do nothing
    } else if (OB_NOT_NULL(case_expr->get_arg_param_expr()) &&
               OB_FAIL(SMART_CALL(try_trans_any_all_as_exists(
                                  stmt,
                                  case_expr->get_arg_param_expr(),
                                  not_null_ctx,
                                  false,
                                  is_happened)))) {
      LOG_WARN("failed to try_transform_any_all for param", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  } else {
    //check children
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(try_trans_any_all_as_exists(stmt,
                                                         expr->get_param_expr(i),
                                                         not_null_ctx,
                                                         false,
                                                         is_happened)))) {
        LOG_WARN("failed to try_transform_any_all for param", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::transform_any_all_as_exists(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  if (OB_ISNULL(stmt) ||
      OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObNotNullContext not_null_ctx(*ctx_, stmt);

    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_joined_tables().count(); ++i) {
      if (OB_FAIL(transform_any_all_as_exists_joined_table(stmt,
                                                           stmt->get_joined_tables().at(i),
                                                           is_happened))) {
        LOG_WARN("failed to flatten join condition exprs", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FALSE_IT(not_null_ctx.reset())) {
    } else if (OB_FAIL(not_null_ctx.generate_stmt_context(NULLABLE_SCOPE::NS_WHERE))) {
      LOG_WARN("failed to generate where context", K(ret));
    } else if (OB_FAIL(try_trans_any_all_as_exists(stmt,
                                                   stmt->get_condition_exprs(),
                                                   &not_null_ctx,
                                                   true,
                                                   is_happened))) {
      LOG_WARN("failed to trans any all as exists", K(ret));
    } else {
      trans_happened |= is_happened;
    }

    if (OB_SUCC(ret) && stmt->is_select_stmt()) {
      ObSelectStmt *select_stmt = static_cast<ObSelectStmt*>(stmt);
      not_null_ctx.reset();
      if (OB_FAIL(not_null_ctx.generate_stmt_context(NULLABLE_SCOPE::NS_TOP))) {
        LOG_WARN("failed to generate where context", K(ret));
      } else if (OB_FAIL(try_trans_any_all_as_exists(stmt,
                                                     select_stmt->get_having_exprs(),
                                                     &not_null_ctx,
                                                     true,
                                                     is_happened))) {
        LOG_WARN("failed to try trans any all as exists", K(ret));
      } else {
        trans_happened |= is_happened;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_items().count(); ++i) {
        if (OB_ISNULL(select_stmt->get_select_items().at(i).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(try_trans_any_all_as_exists(stmt,
                                                       select_stmt->get_select_items().at(i).expr_,
                                                       &not_null_ctx,
                                                       false,
                                                       is_happened))) {
          LOG_WARN("failed to try trans any all as exists", K(ret));
        } else {
          trans_happened |= is_happened;
        }
      }
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::transform_any_all_as_exists_joined_table(
                                 ObDMLStmt* stmt,
                                 TableItem *table,
                                 bool &trans_happened)
{
  int ret = OB_SUCCESS;
  JoinedTable *join_table = NULL;
  TableItem *left_table = NULL;
  TableItem *right_table = NULL;
  trans_happened = false;
  bool cur_happened = false;
  bool left_happened = false;
  bool right_happened = false;
  if (OB_ISNULL(table) ) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table));
  } else if (!table->is_joined_table()) {
    /*do nothing*/
  } else if (OB_ISNULL(join_table = static_cast<JoinedTable*>(table)) ||
             OB_ISNULL(left_table = join_table->left_table_) ||
             OB_ISNULL(right_table = join_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(join_table));
  } else {
    ObNotNullContext not_null_ctx(*ctx_, stmt);
    if (left_table->is_joined_table() &&
        OB_FAIL(not_null_ctx.add_joined_table(static_cast<JoinedTable *>(left_table)))) {
      LOG_WARN("failed to add context", K(ret));
    } else if (right_table->is_joined_table() &&
        OB_FAIL(not_null_ctx.add_joined_table(static_cast<JoinedTable *>(right_table)))) {
      LOG_WARN("failed to add context", K(ret));
    } else if (OB_FAIL(not_null_ctx.add_filter(join_table->get_join_conditions()))) {
      LOG_WARN("failed to add null reject conditions", K(ret));
    } else if (OB_FAIL(try_trans_any_all_as_exists(stmt, join_table->get_join_conditions(),
                                                   &not_null_ctx, true, cur_happened))) {
      LOG_WARN("failed to try trans any all as exists", K(ret));
    } else if (OB_FAIL(SMART_CALL(transform_any_all_as_exists_joined_table(stmt,
                                                                          join_table->left_table_,
                                                                          left_happened)))) {
      LOG_WARN("failed to flatten left child join condition exprs", K(ret));
    } else if (OB_FAIL(SMART_CALL(transform_any_all_as_exists_joined_table(stmt,
                                                                          join_table->right_table_,
                                                                          right_happened)))) {
      LOG_WARN("failed to flatten right child join condition exprs", K(ret));
    } else {
      trans_happened = cur_happened | left_happened | right_happened;
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::try_trans_any_all_as_exists(
                                 ObDMLStmt *stmt,
                                 ObIArray<ObRawExpr* > &exprs,
                                 ObNotNullContext *not_null_cxt,
                                 bool is_bool_expr,
                                 bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(try_trans_any_all_as_exists(stmt,
                                            exprs.at(i),
                                            not_null_cxt,
                                            is_bool_expr,
                                            is_happened))) {
      LOG_WARN("failed to try trans any all as exists", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  }
  return ret;
}

int ObTransformSimplifySubquery::empty_table_subquery_can_be_eliminated_in_exists(ObRawExpr *expr,
                                                                                  bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObQueryRefRawExpr* query_ref = NULL;
  ObSelectStmt* ref_stmt = NULL;
  bool contain_rownum = false;
  bool has_limit = false;
  is_valid = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (T_OP_EXISTS != expr->get_expr_type() &&
             T_OP_NOT_EXISTS != expr->get_expr_type()) {
    is_valid = false;
  } else if (OB_UNLIKELY(expr->get_param_count() != 1) ||
             OB_ISNULL(expr->get_param_expr(0)) ||
             OB_UNLIKELY(!expr->get_param_expr(0)->is_query_ref_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected exists/not exists expr", K(ret));
  } else if (OB_FALSE_IT(query_ref = static_cast<ObQueryRefRawExpr*>(expr->get_param_expr(0)))) {
  } else if (OB_ISNULL(ref_stmt = query_ref->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected ref stmt", K(ret));
  } else if (OB_FAIL(ref_stmt->has_rownum(contain_rownum))) {
    LOG_WARN("failed to check ref stmt has rownum", K(ret));
  } else if (OB_FAIL(check_limit(expr->get_expr_type(), ref_stmt, has_limit))) {
    LOG_WARN("failed to check limit", K(ret));
  } else if (contain_rownum || has_limit) {
    is_valid = false;
  } else if (query_ref->get_ref_count() > 1) {
    is_valid = false;
  } else if (0 != ref_stmt->get_from_item_size() ||
             0 == ref_stmt->get_condition_size() ||
             ref_stmt->is_set_stmt() ||
             ref_stmt->is_contains_assignment() ||
             ref_stmt->has_group_by() ||
             ref_stmt->has_having() ||
             ref_stmt->is_hierarchical_query() ||
             ref_stmt->has_sequence() ||
             ref_stmt->has_window_function()) {
    is_valid = false;
  } else {
    is_valid = true;
  }
  return ret;
}

int ObTransformSimplifySubquery::do_trans_empty_table_subquery_as_expr(ObRawExpr *&expr,
                                                                       bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObQueryRefRawExpr* query_ref = NULL;
  ObSelectStmt* ref_stmt = NULL;
  bool add_limit_constraint = false;
  ObSEArray<ObRawExpr*, 4> conditions;
  ObRawExpr *out_expr = NULL;
  trans_happened = false;
  if (OB_ISNULL(expr) ||
      OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(expr->get_param_count() != 1) ||
             OB_ISNULL(expr->get_param_expr(0)) ||
             OB_UNLIKELY(!expr->get_param_expr(0)->is_query_ref_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected exists/not exists expr", K(ret));
  } else if (OB_FALSE_IT(query_ref = static_cast<ObQueryRefRawExpr*>(expr->get_param_expr(0)))) {
  } else if (OB_ISNULL(ref_stmt = query_ref->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected ref stmt", K(ret));
  } else if (OB_FAIL(need_add_limit_constraint(expr->get_expr_type(), ref_stmt, add_limit_constraint))){
    LOG_WARN("failed to check limit constraints", K(ret));
  } else if (add_limit_constraint &&
            OB_FAIL(ObTransformUtils::add_const_param_constraints(ref_stmt->get_limit_expr(), ctx_))) {
    LOG_WARN("failed to add const param constraints", K(ret));
  } else if (OB_FAIL(conditions.assign(ref_stmt->get_condition_exprs()))) {
    LOG_WARN("failed to assign condition exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::decorrelate(conditions, query_ref->get_exec_params()))) {
    LOG_WARN("failed to decorrelate condition exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_and_expr(*ctx_->expr_factory_,
                                                    conditions,
                                                    out_expr))) {
    LOG_WARN("failed to formalize expr", K(ret));
  } else if (OB_ISNULL(out_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (T_OP_NOT_EXISTS == expr->get_expr_type() &&
             OB_FAIL(ObRawExprUtils::build_lnnvl_expr(*ctx_->expr_factory_,
                                                      out_expr,
                                                      out_expr))) {
    LOG_WARN("failed to build lnnvl expr", K(ret));
  } else if (OB_ISNULL(out_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(out_expr->formalize(ctx_->session_info_))) {
    LOG_WARN("failed to formalize expr", K(ret));
  } else if (OB_FAIL(out_expr->pull_relation_id())) {
    LOG_WARN("failed to pull relation id", K(ret));
  } else {
    expr = out_expr;
    trans_happened = true;
  }
  return ret;
}
