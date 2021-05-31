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
#include "ob_transform_where_subquery_pullup.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/ob_sql_context.h"
#include "common/ob_smart_call.h"
#include "sql/resolver/dml/ob_update_stmt.h"

namespace oceanbase {
using namespace common;
using share::schema::ObTableSchema;

namespace sql {
int ObWhereSubQueryPullup::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  bool is_anyall_happened = false;
  bool is_single_set_happened = false;
  bool is_add_limit_happened = false;
  trans_happened = false;
  if (OB_FAIL(transform_anyall_query(stmt, is_anyall_happened))) {
    LOG_WARN("failed to transform any all query", K(ret));
  } else if (OB_FAIL(transform_single_set_query(stmt, is_single_set_happened))) {
    LOG_WARN("failed to transform single set query", K(ret));
  } else if (OB_FAIL(add_limit_for_exists_subquery(stmt, is_add_limit_happened))) {
    LOG_WARN("failed to add limit for exists subquery", K(ret));
  } else {
    trans_happened = is_anyall_happened | is_single_set_happened | is_add_limit_happened;
    LOG_TRACE("succeed to transformer where subquery",
        K(is_anyall_happened),
        K(is_single_set_happened),
        K(is_add_limit_happened));
  }
  return ret;
}

int ObWhereSubQueryPullup::transform_anyall_query(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> conditions;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (!stmt->has_subquery()) {
    // do nothing
  } else if (OB_FAIL(conditions.assign(stmt->get_condition_exprs()))) {
    LOG_WARN("failed to assign a new condition exprs", K(ret));
  } else {
    bool temp_trans_happened = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < conditions.count(); ++i) {
      if (OB_FAIL(transform_one_expr(stmt, conditions.at(i), temp_trans_happened))) {
        LOG_WARN("failed to transform one subquery expr", K(ret));
      } else {
        trans_happened |= temp_trans_happened;
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::transform_one_expr(ObDMLStmt* stmt, ObRawExpr* expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(ret));
  } else if (expr->has_flag(CNT_ROWNUM)) {
    /*do nothing */
  } else {
    bool can_be = false;
    bool need_add_limit_constraint = false;
    ObRawExpr* old_expr = expr;
    if (OB_FAIL(recursive_eliminate_subquery(stmt, expr, can_be, need_add_limit_constraint, trans_happened))) {
      LOG_WARN("failed to recursive eliminate subquery", KP(expr), K(ret));
    } else if (can_be) {
      if (OB_FAIL(eliminate_subquery_in_exists(stmt, expr, need_add_limit_constraint, trans_happened))) {
        LOG_WARN("failed to eliminate subquery in exists", KP(expr), K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_condition_exprs(), old_expr))) {
        LOG_WARN("failed to remove condition expr", K(ret));
      } else if (OB_FAIL(stmt->add_condition_expr(expr))) {
        LOG_WARN("add new condition expr failed", KP(expr), K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      TransformParam trans_param;
      if (OB_FAIL(gather_transform_params(stmt, expr, trans_param))) {
        LOG_WARN("failed to check can be pulled up ", K(expr), K(stmt), K(ret));
      } else if (!trans_param.can_be_transform_) {
        // do nothing
      } else if (OB_FAIL(do_transform_pullup_subquery(stmt, expr, trans_param, trans_happened))) {
        LOG_WARN("failed to pull up subquery", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::gather_transform_params(ObDMLStmt* stmt, ObRawExpr* expr, TransformParam& trans_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(stmt), K(expr), K(ret));
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    // gather information
    trans_param.op_ = static_cast<ObOpRawExpr*>(expr);
    if (T_OP_EXISTS == expr->get_expr_type() || T_OP_NOT_EXISTS == expr->get_expr_type()) {
      if (OB_ISNULL(trans_param.op_->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL pointer error", K(trans_param.op_->get_param_expr(0)), K(ret));
      } else if (OB_UNLIKELY(!trans_param.op_->get_param_expr(0)->is_query_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected expr type", K(*trans_param.op_->get_param_expr(0)), K(ret));
      } else {
        trans_param.subquery_expr_ = static_cast<ObQueryRefRawExpr*>(trans_param.op_->get_param_expr(0));
        trans_param.subquery_ = trans_param.subquery_expr_->get_ref_stmt();
      }
    } else if (expr->has_flag(IS_WITH_ALL) || expr->has_flag(IS_WITH_ANY)) {
      if (OB_ISNULL(trans_param.op_->get_param_expr(0)) || OB_ISNULL(trans_param.op_->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN(
            "NULL pointer error", K(trans_param.op_->get_param_expr(0)), K(trans_param.op_->get_param_expr(1)), K(ret));
      } else if (OB_UNLIKELY(!trans_param.op_->get_param_expr(1)->is_query_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected expr type", K(*trans_param.op_->get_param_expr(1)), K(ret));
      } else if (OB_UNLIKELY(trans_param.op_->get_param_expr(0)->is_query_ref_expr())) {
        // subquery in subquery, subquery = all subquery do not transform
        trans_param.can_be_transform_ = false;
      } else {
        trans_param.left_hand_ = trans_param.op_->get_param_expr(0);
        trans_param.subquery_expr_ = static_cast<ObQueryRefRawExpr*>(trans_param.op_->get_param_expr(1));
        trans_param.subquery_ = trans_param.subquery_expr_->get_ref_stmt();
      }
    } else {  // not support other subqueries
      trans_param.can_be_transform_ = false;
    }
    if (OB_SUCC(ret) && trans_param.can_be_transform_) {
      bool has_ref_assign_user_var = false;
      if (OB_ISNULL(trans_param.subquery_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected expr type", K(trans_param.subquery_), K(ret));
      } else if (OB_FAIL(trans_param.subquery_->has_ref_assign_user_var(has_ref_assign_user_var))) {
        LOG_WARN("failed to check stmt has assignment ref user var", K(ret));
      } else if (has_ref_assign_user_var) {
        trans_param.can_be_transform_ = false;
        // check subquery is direct correlated
      } else if (OB_FAIL(is_subquery_direct_correlated(trans_param.subquery_, trans_param.direct_correlated_))) {
        LOG_WARN("failed to check subquery correlated", K(trans_param.subquery_), K(ret));
        // check can do unnest transform
      } else if (OB_FAIL(can_be_unnested(expr->get_expr_type(),
                     trans_param.subquery_,
                     trans_param.can_unnest_pullup_,
                     trans_param.need_add_limit_constraint_))) {
        LOG_WARN("failed to check subquery can be unnested", K(ret));
      } else { /*do nothing*/
      }
    }
  } else {
    trans_param.can_be_transform_ = false;
  }
  if (OB_SUCC(ret) && trans_param.can_be_transform_) {
    if (OB_FAIL(check_transform_validity(stmt, expr, trans_param))) {
      LOG_WARN("failed to check can be pulled up ", K(*expr), K(ret));
    } else {
      LOG_TRACE("finish to check where subquery pull up", K(trans_param), K(ret));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::can_be_unnested(
    const ObItemType op_type, const ObSelectStmt* subquery, bool& can_be, bool& need_add_limit_constraint)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool has_limit = false;
  can_be = true;
  need_add_limit_constraint = false;
  if (OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(subquery));
  } else if (subquery->has_distinct() || subquery->is_hierarchical_query() || subquery->has_group_by() ||
             subquery->has_window_function() || subquery->has_set_op()) {
    can_be = false;
  } else if (OB_FAIL(subquery->has_rownum(has_rownum))) {
    LOG_WARN("failed to check has rownum expr", K(ret));
  } else if (has_rownum) {
    can_be = false;
  } else if (OB_FAIL(check_limit(op_type, subquery, has_limit, need_add_limit_constraint))) {
    LOG_WARN("failed to check subquery has unremovable limit", K(ret));
  } else if (has_limit) {
    can_be = false;
  }
  return ret;
}

int ObWhereSubQueryPullup::check_limit(
    const ObItemType op_type, const ObSelectStmt* subquery, bool& has_limit, bool& need_add_limit_constraint) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  has_limit = false;
  need_add_limit_constraint = false;
  if (OB_ISNULL(subquery) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) ||
      OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(subquery), K(ctx_), K(ctx_->exec_ctx_), K(plan_ctx));
  } else if (op_type != T_OP_EXISTS && op_type != T_OP_NOT_EXISTS) {
    has_limit = subquery->has_limit();
  } else if (!subquery->has_limit() || subquery->get_offset_expr() != NULL ||
             subquery->get_limit_percent_expr() != NULL) {
    has_limit = subquery->has_limit();
  } else {
    bool is_null_value = false;
    int64_t limit_value = 0;
    if (OB_FAIL(ObTransformUtils::get_limit_value(subquery->get_limit_expr(),
            subquery,
            &plan_ctx->get_param_store(),
            ctx_->session_info_,
            ctx_->allocator_,
            limit_value,
            is_null_value))) {
      LOG_WARN("failed to get_limit_value", K(ret));
    } else if (!is_null_value && limit_value >= 1) {
      has_limit = false;
      // Just in case different parameters hit same plan, firstly we need add const param constraint
      need_add_limit_constraint = true;
    } else {
      has_limit = true;
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::recursive_eliminate_subquery(
    ObDMLStmt* stmt, ObRawExpr* expr, bool& can_be_eliminated, bool& need_add_limit_constraint, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer error", K(expr), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(recursive_eliminate_subquery(
              stmt, expr->get_param_expr(i), can_be_eliminated, need_add_limit_constraint, trans_happened)))) {
        LOG_WARN("failed to recursive eliminate subquery", K(ret));
      } else if (can_be_eliminated) {
        if (OB_FAIL(eliminate_subquery_in_exists(
                stmt, expr->get_param_expr(i), need_add_limit_constraint, trans_happened))) {
          LOG_WARN("failed to eliminate subquery in exists", K(ret), KP(expr));
        } else {
          LOG_TRACE("succeed to eliminate subquery", K(i), K(can_be_eliminated), K(need_add_limit_constraint), K(ret));
          can_be_eliminated = false;
          need_add_limit_constraint = false;
        }
      }
    }
    if (OB_SUCC(ret) &&
        OB_FAIL(eliminate_subquery(stmt, expr, can_be_eliminated, need_add_limit_constraint, trans_happened))) {
      LOG_WARN("failed to eliminate subquery", K(ret));
    } else { /*do nothing*/
    }
  } else { /*do nothing*/
  }
  LOG_TRACE("finish to eliminate subquery", K(can_be_eliminated), K(ret));
  return ret;
}

int ObWhereSubQueryPullup::check_transform_validity(ObDMLStmt* stmt, const ObRawExpr* expr, TransformParam& trans_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt) || OB_ISNULL(trans_param.subquery_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(expr), K(stmt), K(trans_param.subquery_), K(ret));
  } else if (OB_FAIL(check_basic_validity(stmt, expr, trans_param))) {
    LOG_WARN("check basic valid failed", K(ret));
  } else if (!trans_param.can_be_transform_) {
    /*do nothing*/
  } else if (!trans_param.direct_correlated_) {
    /*do nothing*/
  } else if (trans_param.can_unnest_pullup_) {
    /*do nothing*/
  } else if (OB_FAIL(can_unnest_with_spj(*trans_param.subquery_, trans_param.can_be_transform_))) {
    LOG_WARN("failed to check can unnest with spj", K(ret));
  } else if (!trans_param.can_be_transform_) {
    /*do nothing*/
  } else {
    trans_param.need_create_spj_ = true;
    trans_param.can_unnest_pullup_ = true;
  }
  return ret;
}

int ObWhereSubQueryPullup::check_basic_validity(ObDMLStmt* stmt, const ObRawExpr* expr, TransformParam& trans_param)
{
  int ret = OB_SUCCESS;
  trans_param.can_be_transform_ = true;
  bool check_status = false;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt) || OB_ISNULL(trans_param.subquery_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(ret), K(expr), K(stmt), K(trans_param.subquery_));
  } else if (trans_param.subquery_->get_stmt_hint().enable_no_unnest()) {
    trans_param.can_be_transform_ = false;
  } else if (stmt->get_table_items().count() == 0) {
    // do not trnasform for select from dual, eg:
    //  select 1 from dual where 1 >any (select c1 from t1);
    trans_param.can_be_transform_ = false;
  } else if ((expr->has_flag(IS_WITH_ANY) || expr->has_flag(IS_WITH_ALL)) &&
             trans_param.left_hand_->has_hierarchical_query_flag()) {
    trans_param.can_be_transform_ = false;
  } else if ((expr->has_flag(IS_WITH_ANY) || expr->has_flag(IS_WITH_ALL)) && trans_param.subquery_->has_set_op()) {
    if (trans_param.direct_correlated_) {
      trans_param.can_be_transform_ = false;
    } else {
      trans_param.can_be_transform_ = true;
    }
  } else if (0 == trans_param.subquery_->get_from_item_size()) {
    trans_param.can_be_transform_ = false;
  } else if (trans_param.direct_correlated_ &&
             OB_FAIL(is_join_conditions_correlated(trans_param.subquery_, check_status))) {
    LOG_WARN("failed to is joined table conditions correlated", K(ret));
  } else if (check_status) {
    trans_param.can_be_transform_ = false;
  } else if (OB_FAIL(is_where_subquery_correlated(trans_param.subquery_, check_status))) {
    LOG_WARN("failed to check select item contain subquery", K(trans_param.subquery_), K(ret));
  } else if (check_status) {
    trans_param.can_be_transform_ = false;
  } else if (OB_FAIL(is_select_item_contain_subquery(trans_param.subquery_, check_status))) {
    LOG_WARN("failed to check select item contain subquery", K(trans_param.subquery_), K(ret));
  } else if (check_status) {
    trans_param.can_be_transform_ = false;
  } else if (OB_FAIL(check_if_subquery_contains_correlated_table_item(*trans_param.subquery_, check_status))) {
    LOG_WARN("failed to check if subquery contain correlated subquery", K(ret));
  } else if (check_status) {
    trans_param.can_be_transform_ = false;
  } else if ((T_OP_EXISTS == expr->get_expr_type() || T_OP_NOT_EXISTS == expr->get_expr_type()) &&
             !trans_param.direct_correlated_) {
    trans_param.can_be_transform_ = false;
  }
  return ret;
}

int ObWhereSubQueryPullup::is_where_subquery_correlated(const ObSelectStmt* subquery, bool& is_correlated)
{
  int ret = OB_SUCCESS;
  is_correlated = false;
  if (OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(subquery), K(ret));
  } else {
    const ObRawExpr* cur_expr = NULL;
    const ObIArray<ObRawExpr*>& conds = subquery->get_condition_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && !is_correlated && i < conds.count(); ++i) {
      if (OB_ISNULL(cur_expr = conds.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL pointer error", K(cur_expr), K(ret));
      } else if (!cur_expr->has_flag(CNT_SUB_QUERY)) {
        /* do nothing */
      } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(
                     cur_expr, subquery->get_current_level() - 1, is_correlated))) {
        LOG_WARN("failed to check expr correlated", K(cur_expr), K(ret));
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::check_if_subquery_contains_correlated_table_item(
    const ObSelectStmt& subquery, bool& contains)
{
  int ret = OB_SUCCESS;
  int64_t N = subquery.get_table_size();
  contains = false;
  for (int64_t i = 0; OB_SUCC(ret) && !contains && i < N; ++i) {
    const TableItem* table = subquery.get_table_item(i);
    if (OB_ISNULL(table)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table should not be NULL", K(ret));
    } else if (table->is_generated_table() && table->ref_query_ != NULL) {
      if (OB_FAIL(is_subquery_direct_correlated(table->ref_query_, contains))) {
        LOG_WARN("check if subquery correlated failed", K(ret));
      }
    }
  }
  return ret;
}

// contain column of upper level
/**
 * create table t1 (c1 int primary key, c2 int);
 * create table t2 (a int primary key, b int);
 * (1) select * from t1 where c1 in (select a from t2);
 * (2) select * from t1 where c1 in (select 1 from t2 where a = c1);
 * (3) select * from t1 where 1 in (select a + c1 from t2);
 * (4) select * from t1 where 1 in (select 1 from t2 where a = c1);
 * both (1)~(4) can trans to semi join
 */
int ObWhereSubQueryPullup::has_upper_level_column(const ObSelectStmt* subquery, bool& has_upper_column)
{
  int ret = OB_SUCCESS;
  has_upper_column = false;
  if (OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery should not be null", K(ret), K(subquery));
  } else if (subquery->get_select_item_size() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("too many select items", K(ret));
  } else {
    bool has = false;
    const SelectItem& select_item = subquery->get_select_item(0);
    if (OB_FAIL(
            ObTransformUtils::has_current_level_column(select_item.expr_, subquery->get_current_level() - 1, has))) {
      LOG_WARN("check has_current_level_column failed", K(ret));
    } else if (!has) {
      const ObIArray<ObRawExpr*>& conditions = subquery->get_condition_exprs();
      for (int64_t i = 0; OB_SUCC(ret) && !has && i < conditions.count(); ++i) {
        ObRawExpr* expr = conditions.at(i);
        if (OB_ISNULL(expr)) {
          LOG_WARN("expr should not be null", K(ret));
        } else if (OB_FAIL(ObTransformUtils::has_current_level_column(expr, subquery->get_current_level() - 1, has))) {
          LOG_WARN("check has_current_level_column failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      has_upper_column = has;
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::recursive_find_subquery_column(
    const ObRawExpr* expr, const int64_t level, ObIArray<const ObColumnRefRawExpr*>& subquery_columns)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr should not be null");
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(is_stack_overflow), K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(is_stack_overflow), K(ret));
  } else if (expr->is_column_ref_expr()) {
    const ObColumnRefRawExpr* column_expr = static_cast<const ObColumnRefRawExpr*>(expr);
    if (OB_ISNULL(column_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr should not be null", K(ret));
    } else if (column_expr->get_expr_level() == level) {
      if (!ObRawExprUtils::find_expr(subquery_columns, column_expr)) {
        if (OB_FAIL(subquery_columns.push_back(column_expr))) {
          LOG_WARN("failed to push back column_expr", K(ret));
        }
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(recursive_find_subquery_column(expr->get_param_expr(i), level, subquery_columns)))) {
        LOG_WARN("recursive_find_upper_column failed", K(ret));
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::can_unnest_with_spj(ObSelectStmt& subquery, bool& can_create)
{
  int ret = OB_SUCCESS;
  bool can_pullup = false;
  bool has_special_expr = false;
  can_create = true;
  if (subquery.has_distinct() || subquery.is_hierarchical_query() || subquery.has_set_op()) {
    can_create = false;
  } else if (OB_FAIL(check_correlated_select_item_can_pullup(subquery, can_pullup))) {
    LOG_WARN("failed to check can pullup select item", K(ret));
  } else if (!can_pullup) {
    can_create = false;
  } else if (OB_FAIL(check_can_pullup_conds(subquery, has_special_expr))) {
    LOG_WARN("failed to check can pullup conds", K(ret));
  } else if (OB_FAIL(check_correlated_having_expr_can_pullup(subquery, has_special_expr, can_pullup))) {
    LOG_WARN("failed to check can pullup having expr", K(ret));
  } else if (!can_pullup) {
    can_create = false;
  } else if (OB_FAIL(check_correlated_where_expr_can_pullup(subquery, has_special_expr, can_pullup))) {
    LOG_WARN("failed to check can pullup where expr", K(ret));
  } else if (!can_pullup) {
    can_create = false;
  }
  bool is_correlated = false;
  // check group by exprs
  int64_t N = subquery.get_group_exprs().count();
  for (int64_t i = 0; OB_SUCC(ret) && can_create && i < N; ++i) {
    if (OB_FAIL(ObTransformUtils::is_correlated_expr(
            subquery.get_group_exprs().at(i), subquery.get_current_level() - 1, is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (is_correlated) {
      can_create = false;
    }
  }
  // check rollup exprs
  N = subquery.get_rollup_exprs().count();
  for (int64_t i = 0; OB_SUCC(ret) && can_create && i < N; ++i) {
    if (OB_FAIL(ObTransformUtils::is_correlated_expr(
            subquery.get_rollup_exprs().at(i), subquery.get_current_level() - 1, is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (is_correlated) {
      can_create = false;
    }
  }
  // check order by exprs
  ObSEArray<ObRawExpr*, 8> order_exprs;
  if (OB_FAIL(subquery.get_order_exprs(order_exprs))) {
    LOG_WARN("failed to get order exprs", K(ret));
  }
  N = order_exprs.count();
  for (int64_t i = 0; OB_SUCC(ret) && can_create && i < N; ++i) {
    if (OB_FAIL(
            ObTransformUtils::is_correlated_expr(order_exprs.at(i), subquery.get_current_level() - 1, is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (is_correlated) {
      can_create = false;
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::check_can_pullup_conds(ObSelectStmt& view, bool& has_special_expr)
{
  int ret = OB_SUCCESS;
  bool has_limit = view.has_limit();
  bool has_assign = view.is_contains_assignment();
  bool has_rownum = false;
  has_special_expr = false;
  if (OB_FAIL(view.has_rownum(has_rownum))) {
    LOG_WARN("failed to check stmt has rownum", K(ret));
  } else {
    has_special_expr = (has_limit | has_assign | has_rownum);
  }
  return ret;
}

int ObWhereSubQueryPullup::check_correlated_expr_can_pullup(ObRawExpr* expr, int level, bool& can_pullup)
{
  int ret = OB_SUCCESS;
  can_pullup = true;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (ObOptimizerUtil::has_hierarchical_expr(*expr) || ObOptimizerUtil::has_psedu_column(*expr) ||
             expr->is_set_op_expr() || expr->has_flag(IS_AGG) || expr->has_flag(IS_WINDOW_FUNC) ||
             expr->has_flag(IS_SUB_QUERY) || T_OP_EXISTS == expr->get_expr_type() ||
             T_OP_NOT_EXISTS == expr->get_expr_type() || IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type())) {
    bool is_correlated = false;
    if (OB_FAIL(ObTransformUtils::is_correlated_expr(expr, level, is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (is_correlated) {
      can_pullup = false;
    }
  } else if (0 == expr->get_param_count() || T_FUN_SYS_CAST == expr->get_expr_type()) {
    // do nothing
  } else {
    int64_t N = expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && can_pullup && i < N; ++i) {
      if (OB_FAIL(SMART_CALL(check_correlated_expr_can_pullup(expr->get_param_expr(i), level, can_pullup)))) {
        LOG_WARN("failed to check can pullup correlated expr", K(ret));
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::check_correlated_select_item_can_pullup(ObSelectStmt& subquery, bool& can_pullup)
{
  int ret = OB_SUCCESS;
  can_pullup = true;
  ObIArray<SelectItem>& select_items = subquery.get_select_items();
  for (int64_t i = 0; OB_SUCC(ret) && can_pullup && i < select_items.count(); ++i) {
    bool is_correlated = false;
    if (OB_FAIL(ObTransformUtils::is_correlated_expr(
            select_items.at(i).expr_, subquery.get_current_level() - 1, is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (!is_correlated) {
      // do nothing
    } else if (OB_FAIL(check_correlated_expr_can_pullup(
                   select_items.at(i).expr_, subquery.get_current_level() - 1, can_pullup))) {
      LOG_WARN("failed to check can pullup correlated expr", K(ret));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::check_correlated_having_expr_can_pullup(
    ObSelectStmt& subquery, bool has_special_expr, bool& can_pullup)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> common_part_exprs;
  can_pullup = true;

  for (int64_t i = 0; OB_SUCC(ret) && i < subquery.get_window_func_count(); ++i) {
    ObWinFunRawExpr* win_expr = NULL;
    if (OB_ISNULL(win_expr = subquery.get_window_func_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("window function expr is null", K(ret));
    } else if (i == 0) {
      if (OB_FAIL(common_part_exprs.assign(win_expr->get_partition_exprs()))) {
        LOG_WARN("failed to assign partition exprs", K(ret));
      }
    } else if (OB_FAIL(ObOptimizerUtil::intersect_exprs(
                   common_part_exprs, win_expr->get_partition_exprs(), common_part_exprs))) {
      LOG_WARN("failed to intersect expr array", K(ret));
    } else if (common_part_exprs.empty()) {
      break;
    }
  }
  ObIArray<ObRawExpr*>& having_exprs = subquery.get_having_exprs();
  for (int64_t i = 0; OB_SUCC(ret) && can_pullup && i < having_exprs.count(); ++i) {
    ObSEArray<ObRawExpr*, 4> column_exprs;
    ObRawExpr* expr = having_exprs.at(i);
    bool is_correlated = false;
    if (OB_FAIL(ObTransformUtils::is_correlated_expr(expr, subquery.get_current_level() - 1, is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (!is_correlated) {
      // do nothing
    } else if (has_special_expr) {
      can_pullup = false;
    } else if (0 == subquery.get_window_func_count()) {
      if (OB_FAIL(check_correlated_expr_can_pullup(expr, subquery.get_current_level() - 1, can_pullup))) {
        LOG_WARN("failed to check can pullup correlated expr", K(ret));
      }
    } else if (OB_FAIL(extract_current_level_column_exprs(expr, subquery.get_current_level(), column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (!ObOptimizerUtil::subset_exprs(column_exprs, common_part_exprs)) {
      can_pullup = false;
    } else if (OB_FAIL(check_correlated_expr_can_pullup(expr, subquery.get_current_level() - 1, can_pullup))) {
      LOG_WARN("failed to check can pullup correlated expr", K(ret));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::check_correlated_where_expr_can_pullup(
    ObSelectStmt& subquery, bool has_special_expr, bool& can_pullup)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  ObSEArray<ObRawExpr*, 4> common_part_exprs;
  can_pullup = true;

  if (OB_FAIL(ObOptimizerUtil::get_groupby_win_func_common_exprs(subquery, common_part_exprs, is_valid))) {
    LOG_WARN("failed to get common exprs", K(ret));
  }
  ObIArray<ObRawExpr*>& where_exprs = subquery.get_condition_exprs();
  for (int64_t i = 0; OB_SUCC(ret) && i < where_exprs.count(); ++i) {
    ObSEArray<ObRawExpr*, 4> column_exprs;
    ObRawExpr* expr = where_exprs.at(i);
    bool is_correlated = false;
    if (OB_FAIL(ObTransformUtils::is_correlated_expr(expr, subquery.get_current_level() - 1, is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (!is_correlated) {

    } else if (has_special_expr) {
      can_pullup = false;
    } else if (!is_valid) {

      if (OB_FAIL(check_correlated_expr_can_pullup(expr, subquery.get_current_level() - 1, can_pullup))) {
        LOG_WARN("failed to check can pullup correlated expr", K(ret));
      }
    } else if (OB_FAIL(extract_current_level_column_exprs(expr, subquery.get_current_level(), column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (!ObOptimizerUtil::subset_exprs(column_exprs, common_part_exprs)) {
      can_pullup = false;
    } else if (OB_FAIL(check_correlated_expr_can_pullup(expr, subquery.get_current_level() - 1, can_pullup))) {
      LOG_WARN("failed to check can pullup correlated expr", K(ret));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::create_spj(TransformParam& trans_param)
{
  int ret = OB_SUCCESS;
  ObStmtFactory* stmt_factory = NULL;
  ObSQLSessionInfo* session_info = NULL;
  ObSEArray<ObRawExpr*, 4> new_select_list;
  ObSEArray<ObRawExpr*, 4> new_column_list;
  TableItem* view_table_item = NULL;
  ObSelectStmt* view_stmt = trans_param.subquery_;
  ObSelectStmt* subquery = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(view_stmt) || OB_ISNULL(session_info = ctx_->session_info_) ||
      OB_ISNULL(stmt_factory = ctx_->stmt_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt_factory), K(view_stmt));
  } else if (OB_FAIL(ObTransformUtils::create_stmt_with_generated_table(ctx_, view_stmt, subquery))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (subquery->get_table_items().count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect one table item", K(*subquery), K(ret));
  } else if (OB_ISNULL(view_table_item = subquery->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (OB_FALSE_IT(subquery->get_select_items().reuse())) {
  } else if (OB_FALSE_IT(subquery->get_column_items().reuse())) {
  } else if (OB_FAIL(pullup_correlated_select_expr(*subquery, *view_stmt, new_select_list))) {
    LOG_WARN("failed to pullup correlated select expr", K(ret));
  } else if (OB_FAIL(pullup_correlated_having_expr(*subquery, *view_stmt, new_select_list))) {
    LOG_WARN("failed to check pullup having exprs", K(ret));
  } else if (OB_FAIL(pullup_correlated_where_expr(*subquery, *view_stmt, new_select_list))) {
    LOG_WARN("failed to check pullup having exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(
                 ctx_, *view_table_item, subquery, new_select_list, new_column_list))) {
    LOG_WARN("failed to create columns for view", K(ret));
  } else if (view_stmt->get_select_item_size() == 0 &&
             OB_FAIL(ObTransformUtils::create_dummy_select_item(*view_stmt, ctx_))) {
    LOG_WARN("failed to create dummy select item", K(ret));
  } else {
    subquery->get_table_items().pop_back();
    if (OB_FAIL(subquery->replace_inner_stmt_expr(new_select_list, new_column_list))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    } else if (OB_FAIL(subquery->get_table_items().push_back(view_table_item))) {
      LOG_WARN("failed to push back view table item", K(ret));
    } else if (OB_FAIL(subquery->formalize_stmt(session_info))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else {
      trans_param.subquery_ = subquery;
      trans_param.subquery_expr_->set_ref_stmt(subquery);
      LOG_TRACE("succeed to create spj", K(*subquery));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::pullup_correlated_expr(ObRawExpr* expr, int level, ObIArray<ObRawExpr*>& new_select_list)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (ObOptimizerUtil::has_hierarchical_expr(*expr) || ObOptimizerUtil::has_psedu_column(*expr) ||
             expr->is_set_op_expr() || expr->has_flag(IS_AGG) || expr->has_flag(IS_WINDOW_FUNC) ||
             expr->has_flag(IS_SUB_QUERY) || T_OP_EXISTS == expr->get_expr_type() ||
             T_OP_NOT_EXISTS == expr->get_expr_type() || IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type())) {
    // can not pullup
    if (ObOptimizerUtil::find_item(new_select_list, expr)) {
      // do nothing
    } else if (OB_FAIL(new_select_list.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  } else if (0 == expr->get_param_count() || T_FUN_SYS_CAST == expr->get_expr_type()) {
    bool is_correlated = false;
    if (OB_FAIL(ObTransformUtils::is_correlated_expr(expr, level, is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (is_correlated) {
      // will pullup do nothing
    } else if (ObOptimizerUtil::find_item(new_select_list, expr)) {
      // do nothing
    } else if (OB_FAIL(new_select_list.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  } else {
    int64_t N = expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_FAIL(SMART_CALL(pullup_correlated_expr(expr->get_param_expr(i), level, new_select_list)))) {
        LOG_WARN("failed to pullup correlated expr", K(ret));
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::extract_current_level_column_exprs(
    ObRawExpr* expr, int level, ObIArray<ObRawExpr*>& column_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> temp_column_exprs;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, temp_column_exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_column_exprs.count(); ++i) {
    ObRawExpr* col_expr = temp_column_exprs.at(i);
    if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (col_expr->get_expr_level() == level) {
      ret = column_exprs.push_back(col_expr);
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::pullup_correlated_select_expr(
    ObSelectStmt& stmt, ObSelectStmt& view, ObIArray<ObRawExpr*>& new_select_list)
{
  int ret = OB_SUCCESS;
  ObIArray<SelectItem>& select_items = view.get_select_items();
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
    bool is_correlated = false;
    if (OB_FAIL(ObTransformUtils::is_correlated_expr(
            select_items.at(i).expr_, stmt.get_current_level() - 1, is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (!is_correlated) {
      ret = new_select_list.push_back(select_items.at(i).expr_);
    } else if (OB_FAIL(
                   pullup_correlated_expr(select_items.at(i).expr_, stmt.get_current_level() - 1, new_select_list))) {
      LOG_WARN("failed to pullup correlated expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt.get_select_items().assign(select_items))) {
      LOG_WARN("failed to assign select items", K(ret));
    } else {
      view.get_select_items().reset();
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::pullup_correlated_having_expr(
    ObSelectStmt& stmt, ObSelectStmt& view, ObIArray<ObRawExpr*>& new_select_list)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> common_part_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < view.get_window_func_count(); ++i) {
    ObWinFunRawExpr* win_expr = NULL;
    if (OB_ISNULL(win_expr = view.get_window_func_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("window function expr is null", K(ret));
    } else if (i == 0) {
      if (OB_FAIL(common_part_exprs.assign(win_expr->get_partition_exprs()))) {
        LOG_WARN("failed to assign partition exprs", K(ret));
      }
    } else if (OB_FAIL(ObOptimizerUtil::intersect_exprs(
                   common_part_exprs, win_expr->get_partition_exprs(), common_part_exprs))) {
      LOG_WARN("failed to intersect expr array", K(ret));
    } else if (common_part_exprs.empty()) {
      break;
    }
  }
  ObSEArray<ObRawExpr*, 4> remain_exprs;
  ObIArray<ObRawExpr*>& having_exprs = view.get_having_exprs();
  for (int64_t i = 0; OB_SUCC(ret) && i < having_exprs.count(); ++i) {
    ObSEArray<ObRawExpr*, 4> column_exprs;
    ObRawExpr* expr = having_exprs.at(i);
    bool is_correlated = false;
    if (OB_FAIL(ObTransformUtils::is_correlated_expr(expr, stmt.get_current_level() - 1, is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (!is_correlated) {
      ret = remain_exprs.push_back(expr);
    } else if (0 == view.get_window_func_count()) {
      if (OB_FAIL(pullup_correlated_expr(expr, stmt.get_current_level() - 1, new_select_list))) {
        LOG_WARN("failed to pullup correlated expr", K(ret));
      } else if (OB_FAIL(stmt.get_condition_exprs().push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else if (OB_FAIL(extract_current_level_column_exprs(expr, view.get_current_level(), column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (!ObOptimizerUtil::subset_exprs(column_exprs, common_part_exprs)) {
      ret = remain_exprs.push_back(expr);
    } else if (OB_FAIL(pullup_correlated_expr(expr, stmt.get_current_level() - 1, new_select_list))) {
      LOG_WARN("failed to pullup correlated expr", K(ret));
    } else if (OB_FAIL(stmt.get_condition_exprs().push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(view.get_having_exprs().assign(remain_exprs))) {
      LOG_WARN("failed to assign having exprs", K(ret));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::pullup_correlated_where_expr(
    ObSelectStmt& stmt, ObSelectStmt& view, ObIArray<ObRawExpr*>& new_select_list)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  ObSEArray<ObRawExpr*, 4> common_part_exprs;
  if (OB_FAIL(ObOptimizerUtil::get_groupby_win_func_common_exprs(view, common_part_exprs, is_valid))) {
    LOG_WARN("failed to get common exprs", K(ret));
  }
  ObSEArray<ObRawExpr*, 4> remain_exprs;
  ObIArray<ObRawExpr*>& where_exprs = view.get_condition_exprs();
  for (int64_t i = 0; OB_SUCC(ret) && i < where_exprs.count(); ++i) {
    ObSEArray<ObRawExpr*, 4> column_exprs;
    ObRawExpr* expr = where_exprs.at(i);
    bool is_correlated = false;
    if (OB_FAIL(ObTransformUtils::is_correlated_expr(expr, stmt.get_current_level() - 1, is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (!is_correlated) {
      ret = remain_exprs.push_back(expr);
    } else if (!is_valid) {
      if (OB_FAIL(pullup_correlated_expr(expr, stmt.get_current_level() - 1, new_select_list))) {
        LOG_WARN("failed to pullup correlated expr", K(ret));
      } else if (OB_FAIL(stmt.get_condition_exprs().push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else if (OB_FAIL(extract_current_level_column_exprs(expr, view.get_current_level(), column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (!ObOptimizerUtil::subset_exprs(column_exprs, common_part_exprs)) {
      ret = remain_exprs.push_back(expr);
    } else if (OB_FAIL(pullup_correlated_expr(expr, stmt.get_current_level() - 1, new_select_list))) {
      LOG_WARN("failed to pullup correlated expr", K(ret));
    } else if (OB_FAIL(stmt.get_condition_exprs().push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(view.get_condition_exprs().assign(remain_exprs))) {
      LOG_WARN("failed to assign having exprs", K(ret));
    }
  }
  return ret;
}

// create select_item for subquery
int ObWhereSubQueryPullup::create_select_items(
    ObSelectStmt* subquery, const ObColumnRefRawExpr* expr, ObIArray<SelectItem>& select_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(subquery) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery or expr should not be null", K(ret), K(subquery), K(expr));
  } else {
    SelectItem select_item;
    select_item.alias_name_ = expr->get_column_name();
    select_item.expr_name_ = expr->get_column_name();
    select_item.expr_ = const_cast<ObColumnRefRawExpr*>(expr);
    if (OB_FAIL(select_items.push_back(select_item))) {
      LOG_WARN("failed to push back select_item", K(ret), K(select_item));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::find_select_item(
    const ObIArray<SelectItem>& new_select_items, const ObRawExpr* expr, int64_t& index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr should not be null", K(ret));
  } else {
    index = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCC(ret) && index == OB_INVALID_INDEX && i < new_select_items.count(); ++i) {
      if (expr == new_select_items.at(i).expr_) {
        index = i;
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::recursive_replace_subquery_column(ObRawExpr*& condition, const TableItem& table_item,
    ObIArray<SelectItem>& new_select_items, ObDMLStmt* stmt, ObSelectStmt* subquery)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(condition) || OB_ISNULL(stmt) || OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(is_stack_overflow), K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(is_stack_overflow), K(ret));
  } else if (!condition->has_flag(CNT_COLUMN)) {
    /*do nothing*/
  } else if (condition->has_flag(IS_COLUMN) && condition->get_expr_level() == subquery->get_current_level()) {
    int64_t index = OB_INVALID_INDEX;
    if (OB_FAIL(find_select_item(new_select_items, condition, index))) {
      LOG_WARN("find_select_item failed", K(ret));
    } else if (OB_INVALID_INDEX == index) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select item not found", K(ret));
    } else {
      const SelectItem& select_item = new_select_items.at(index);
      ObColumnRefRawExpr* column_expr = NULL;
      // start from 16
      const int64_t column_id = subquery->get_select_item_size() + OB_APP_MIN_COLUMN_ID + index;
      if (OB_NOT_NULL(column_expr = stmt->get_column_expr_by_id(table_item.table_id_, column_id))) {
        /*do nothing*/
      } else if (OB_FAIL(ObTransformUtils::create_new_column_expr(
                     ctx_, table_item, column_id, select_item, stmt, column_expr))) {
        LOG_WARN("create new column expr failed", K(ret));
      }
      if (OB_FAIL(ret)) {
        /*do nothing*/
      } else if (OB_ISNULL(column_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr should not be null", K(ret));
      } else {
        condition = column_expr;
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < condition->get_param_count(); i++) {
      if (OB_ISNULL(condition->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(recursive_replace_subquery_column(
                     condition->get_param_expr(i), table_item, new_select_items, stmt, subquery)))) {
        LOG_WARN("failed to replace subquery column", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::do_transform_pullup_subquery(
    ObDMLStmt* stmt, ObRawExpr* expr, TransformParam& trans_param, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObArray<ObSelectStmt*> from_stmts;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr) || OB_ISNULL(trans_param.subquery_expr_) || OB_ISNULL(trans_param.subquery_) ||
      OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(stmt), K(expr), K(trans_param.subquery_expr_), K(ctx_), K(trans_param.subquery_));
    // Just in case different parameters hit same plan, firstly we need add const param constraint.
    // In pullup_correlated_subquery_as_view limit expr will be set as null, so add constraint first.
  } else if (trans_param.need_add_limit_constraint_ &&
             OB_FAIL(ObTransformUtils::add_const_param_constraints(
                 trans_param.subquery_->get_limit_expr(), stmt->get_query_ctx(), ctx_))) {
    LOG_WARN("failed to add const param constraints", K(ret));
  } else if (trans_param.need_create_spj_ && OB_FAIL(create_spj(trans_param))) {
    LOG_WARN("failed to create spj", K(ret));
  } else if (trans_param.direct_correlated_ && OB_FAIL(pullup_correlated_subquery_as_view(
                                                   stmt, trans_param.subquery_, expr, trans_param.subquery_expr_))) {
    LOG_WARN("failed to pullup subquery as view", K(ret));
  } else if (!trans_param.direct_correlated_ && OB_FAIL(pullup_non_correlated_subquery_as_view(
                                                    stmt, trans_param.subquery_, expr, trans_param.subquery_expr_))) {
    LOG_WARN("failed to pullup subquery as view", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

// create new conds to subquery
// pullup conds and subquery
int ObWhereSubQueryPullup::pullup_correlated_subquery_as_view(
    ObDMLStmt* stmt, ObSelectStmt* subquery, ObRawExpr* expr, ObQueryRefRawExpr* subquery_expr)
{
  int ret = OB_SUCCESS;
  TableItem* right_table = NULL;
  ObSEArray<ObRawExpr*, 4> right_hand_exprs;
  ObSEArray<ObRawExpr*, 4> new_conditions;
  ObSEArray<ObRawExpr*, 4> correlated_conds;
  SemiInfo* info = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(subquery) || OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(subquery), K(expr));
  } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, stmt, subquery, right_table))) {
    LOG_WARN("failed to add new table_item", K(ret));
  } else if (OB_ISNULL(right_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("right_table should not be null", K(ret), K(right_table));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_condition_exprs(), expr))) {
    LOG_WARN("failed to remove condition expr", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_subquery_exprs(), subquery_expr))) {
    LOG_WARN("failed to remove subquery expr", K(ret));
  } else {
    // select * from t1 where exists (select 1 from t2 where t1.c1 = c1 and c2 = 2 limit 1);
    // for the query contains correlated subquery above, limit 1 should be removed after transform.
    // select * from t1 semi join (select c1 from t2 where c2 = 2) v on t1.c1 = v.c1;
    subquery->set_limit_offset(NULL, NULL);
  }

  if (OB_FAIL(subquery->get_select_exprs(right_hand_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(generate_conditions(stmt, right_hand_exprs, subquery, expr, new_conditions))) {
    // create conditions with left_hand and subquery's original targetlist
    LOG_WARN("failed to generate new condition exprs", K(ret));
  } else if (OB_FAIL(append(subquery->get_condition_exprs(), new_conditions))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else {
    ObSqlBitSet<> table_set;
    ObSEArray<ObDMLStmt*, 4> ignore_stmts;
    ObSEArray<ObRawExpr*, 4> column_exprs;
    ObSEArray<ObRawExpr*, 4> upper_column_exprs;
    if (OB_FAIL(ObTransformUtils::get_table_rel_ids(*subquery, subquery->get_table_items(), table_set))) {
      LOG_WARN("failed to get rel ids", K(ret));
    } else if (OB_FAIL(get_correlated_conditions(*subquery, correlated_conds))) {
      LOG_WARN("failed to get semi conditions", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(subquery->get_condition_exprs(), correlated_conds))) {
      LOG_WARN("failed to remove condition expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_column_exprs(
                   correlated_conds, subquery->get_current_level(), table_set, ignore_stmts, column_exprs))) {
      LOG_WARN("extract column exprs failed", K(ret));
    } else if (column_exprs.empty()) {
      /* do nothing */
    } else if (OB_FALSE_IT(subquery->get_select_items().reset())) {
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_, column_exprs, subquery))) {
      LOG_WARN("failed to create select item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *right_table, stmt, upper_column_exprs))) {
      LOG_WARN("failed to create columns for view", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_exprs(column_exprs, upper_column_exprs, correlated_conds))) {
      LOG_WARN("failed to replace pullup conds", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(subquery->pullup_stmt_level())) {
    LOG_WARN("failed to pullup stmt level", K(ret));
  } else if (OB_FAIL(subquery->adjust_view_parent_namespace_stmt(stmt->get_parent_namespace_stmt()))) {
    LOG_WARN("failed to adjust view parent namespace stmt", K(ret));
  } else if (OB_FAIL(pull_exprs_relation_id_and_levels(correlated_conds, stmt->get_current_level()))) {
    LOG_WARN("failed to pull exprs relation id and levels", K(ret));
  } else if (OB_FAIL(generate_semi_info(stmt, expr, right_table, correlated_conds, info))) {
    LOG_WARN("failed to generate semi info", K(ret));
    // zhanyue todo: need extract left table filters
  } else if (OB_FAIL(subquery->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("formalize child stmt failed", K(ret));
  }
  return ret;
}

int ObWhereSubQueryPullup::pull_exprs_relation_id_and_levels(ObIArray<ObRawExpr*>& exprs, const int32_t cur_stmt_level)
{
  int ret = OB_SUCCESS;
  ObRawExpr* expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(expr = exprs.at(i))) {
      LOG_WARN("expr should not be null", K(ret));
    } else if (OB_FAIL(expr->pull_relation_id_and_levels(cur_stmt_level))) {
      LOG_WARN("pull expr relation ids failed", K(ret), K(*expr));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::get_correlated_conditions(ObSelectStmt& subquery, ObIArray<ObRawExpr*>& correlated_conds)
{
  int ret = OB_SUCCESS;
  const int64_t upper_level = subquery.get_current_level() - 1;
  ObIArray<ObRawExpr*>& conds = subquery.get_condition_exprs();
  ObRawExpr* expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < conds.count(); ++i) {
    if (OB_ISNULL(expr = conds.at(i))) {
      LOG_WARN("expr should not be null", K(ret));
    } else if (!expr->get_expr_levels().has_member(upper_level)) {
      /* do nothing */
    } else if (OB_FAIL(correlated_conds.push_back(expr))) {
      LOG_WARN("failed to push back exprs", K(ret));
    }
  }
  return ret;
}

// create semi info with a generate table
int ObWhereSubQueryPullup::generate_semi_info(ObDMLStmt* stmt, ObRawExpr* expr, TableItem* right_table,
    ObIArray<ObRawExpr*>& semi_conditions, SemiInfo*& semi_info)
{
  int ret = OB_SUCCESS;
  semi_info = NULL;
  SemiInfo* info = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr) || OB_ISNULL(right_table) || OB_ISNULL(get_allocator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(stmt), K(expr), K(right_table), K(get_allocator()), K(ret));
  } else if (OB_UNLIKELY(!right_table->is_generated_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected right table type in semi info", K(ret), K(right_table->type_));
  } else if (OB_ISNULL(info = static_cast<SemiInfo*>(get_allocator()->alloc(sizeof(SemiInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc semi info", K(ret));
  } else if (OB_FALSE_IT(info = new (info) SemiInfo())) {
  } else if (OB_FAIL(info->semi_conditions_.assign(semi_conditions))) {
    LOG_WARN("failed to assign semi condition exprs", K(ret));
  } else if (OB_FALSE_IT(info->right_table_id_ = right_table->table_id_)) {
  } else if (OB_FAIL(fill_semi_left_table_ids(stmt, info))) {
    LOG_WARN("failed to fill semi left table ids", K(ret));
  } else if (OB_FAIL(stmt->add_semi_info(info))) {
    LOG_WARN("failed to add semi info", K(ret));
  } else if (T_OP_EXISTS == expr->get_expr_type() || expr->has_flag(IS_WITH_ANY)) {
    info->join_type_ = LEFT_SEMI_JOIN;
    // add as LEFT_SEMI/ANTI_JOIN,
    // RIGHT_SEMI/ANTI_JOIN path is added in ObJoinOrder::generate_join_paths()
  } else if (T_OP_NOT_EXISTS == expr->get_expr_type() || expr->has_flag(IS_WITH_ALL)) {
    info->join_type_ = LEFT_ANTI_JOIN;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr type", K(ret), K(expr->get_expr_type()));
  }

  if (OB_SUCC(ret)) {
    info->semi_id_ = stmt->get_query_ctx()->available_tb_id_--;
    semi_info = info;
  }
  return ret;
}

int ObWhereSubQueryPullup::fill_semi_left_table_ids(ObDMLStmt* stmt, SemiInfo* info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(ret), K(stmt), K(info));
  } else {
    info->left_table_ids_.reuse();
    ObSqlBitSet<> left_rel_ids;
    int32_t right_idx = stmt->get_table_bit_index(info->right_table_id_);
    TableItem* table = NULL;
    ObRawExpr* cond_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < info->semi_conditions_.count(); ++i) {
      if (OB_ISNULL(cond_expr = info->semi_conditions_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(cond_expr->pull_relation_id_and_levels(stmt->get_current_level()))) {
        LOG_WARN("pull expr relation ids failed", K(ret), K(*cond_expr));
      } else if (OB_FAIL(left_rel_ids.add_members(cond_expr->get_relation_ids()))) {
        LOG_WARN("failed to add members", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(left_rel_ids.del_member(right_idx))) {
      LOG_WARN("failed to delete members", K(ret));
    } else if (!left_rel_ids.is_empty()) {
      ret = ObTransformUtils::relids_to_table_ids(stmt, left_rel_ids, info->left_table_ids_);
    } else if (OB_UNLIKELY(0 == stmt->get_from_item_size()) ||
               OB_ISNULL(table = stmt->get_table_item(stmt->get_from_item(0)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt from item is empty", K(ret), K(table));
    } else if (table->is_joined_table()) {
      ret = append(info->left_table_ids_, static_cast<JoinedTable*>(table)->single_table_ids_);
    } else if (OB_FAIL(info->left_table_ids_.push_back(table->table_id_))) {
      LOG_WARN("failed to push back table id", K(ret));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::get_semi_oper_type(ObRawExpr* op, ObItemType& oper_type)
{
  int ret = OB_SUCCESS;
  oper_type = T_INVALID;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(op), K(ret));
  } else if (op->has_flag(IS_WITH_ALL)) {
    switch (op->get_expr_type()) {
      case T_OP_SQ_EQ:
        oper_type = T_OP_NE;
        break;
      case T_OP_SQ_LE:
        oper_type = T_OP_GT;
        break;
      case T_OP_SQ_LT:
        oper_type = T_OP_GE;
        break;
      case T_OP_SQ_GE:
        oper_type = T_OP_LT;
        break;
      case T_OP_SQ_GT:
        oper_type = T_OP_LE;
        break;
      case T_OP_SQ_NE:
        oper_type = T_OP_EQ;
        break;
      default:
        break;
    }
  } else {
    switch (op->get_expr_type()) {
      case T_OP_SQ_EQ:
        oper_type = T_OP_EQ;
        break;
      case T_OP_SQ_LE:
        oper_type = T_OP_LE;
        break;
      case T_OP_SQ_LT:
        oper_type = T_OP_LT;
        break;
      case T_OP_SQ_GE:
        oper_type = T_OP_GE;
        break;
      case T_OP_SQ_GT:
        oper_type = T_OP_GT;
        break;
      case T_OP_SQ_NE:
        oper_type = T_OP_NE;
        break;
      default:
        break;
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::generate_anti_condition(ObDMLStmt* stmt, const ObSelectStmt* subquery, ObRawExpr* cond_expr,
    ObRawExpr* subq_select_expr, ObRawExpr* left_arg, ObRawExpr* right_arg, ObRawExpr*& anti_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory* expr_factory = NULL;
  bool left_need_make_null = true;
  bool right_need_make_null = true;
  ObRawExpr* left_null = NULL;
  ObRawExpr* right_null = NULL;
  ObOpRawExpr* new_cond_expr = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(subquery) || OB_ISNULL(cond_expr) || OB_ISNULL(left_arg) || OB_ISNULL(right_arg) ||
      OB_ISNULL(subq_select_expr) || OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "invalid argument", K(stmt), K(subquery), K(cond_expr), K(left_arg), K(right_arg), K(ctx_), K(expr_factory));
  } else if (OB_FAIL(ObTransformUtils::check_expr_nullable(stmt, left_arg, left_need_make_null))) {
    LOG_WARN("fail to check need make null with left arg", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_stmt_output_nullable(subquery, subq_select_expr, right_need_make_null))) {
    LOG_WARN("fail to check need make null with right arg", K(ret));
  } else if (!left_need_make_null && !right_need_make_null) {
    anti_expr = cond_expr;
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_OP_OR, new_cond_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_ISNULL(anti_expr = new_cond_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_expr is null", K(new_cond_expr), K(ret));
  } else if (OB_FAIL(new_cond_expr->add_param_expr(cond_expr))) {
    LOG_WARN("failed to add param expr", K(ret));
  }
  if (OB_SUCC(ret) && left_need_make_null) {
    if (OB_FAIL(make_null_test(stmt, left_arg, left_null))) {
      LOG_WARN("failed to make null test", K(ret));
    } else if (OB_FAIL(new_cond_expr->add_param_expr(left_null))) {
      LOG_WARN("failed to add param expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && right_need_make_null) {
    if (OB_FAIL(make_null_test(stmt, right_arg, right_null))) {
      LOG_WARN("faield to make null test", K(ret));
    } else if (OB_FAIL(new_cond_expr->add_param_expr(right_null))) {
      LOG_WARN("failed to add param expr", K(ret));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::make_null_test(ObDMLStmt* stmt, ObRawExpr* in_expr, ObRawExpr*& out_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr* null_test = NULL;
  ObConstRawExpr* null_expr = NULL;
  ObRawExprFactory* expr_factory = NULL;
  // FIXME: third_arg = a is base table column && a is not null column && b is null
  // by now, we cant access schema info here. fix later
  ObConstRawExpr* third_arg = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(in_expr) || OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(stmt), K(in_expr), KP_(ctx), KP(expr_factory));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_OP_IS, null_test))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_NULL, null_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_BOOL, third_arg))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_ISNULL(null_test) || OB_ISNULL(null_expr) || OB_ISNULL(third_arg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to store expr", KP(null_test), KP(null_expr), KP(third_arg), K(ret));
  } else {
    ObObj val;
    val.set_null();
    null_expr->set_value(val);
    val.set_bool(false);
    third_arg->set_value(val);
    null_test->set_relation_ids(in_expr->get_relation_ids());
    if (OB_FAIL(null_test->set_param_exprs(in_expr, null_expr, third_arg))) {
      LOG_WARN("fail to set_param_expr", K(in_expr), K(null_expr), K(third_arg), K(ret));
    } else {
      out_expr = null_test;
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::is_subquery_direct_correlated(const ObSelectStmt* subquery, bool& is_correlated)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  is_correlated = false;
  if (OB_ISNULL(subquery)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(subquery), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (subquery->is_set_stmt()) {
    const ObIArray<ObSelectStmt*>& child_stmts = subquery->get_set_query();
    for (int64_t i = 0; OB_SUCC(ret) && !is_correlated && i < child_stmts.count(); ++i) {
      ret = is_subquery_direct_correlated(child_stmts.at(i), is_correlated);
    }
  } else {
    int64_t N = 0;
    int64_t i = 0;
    const ObRawExpr* expr = NULL;
    ObSEArray<ObRawExpr*, 4> relation_exprs;
    if (OB_FAIL(subquery->get_relation_exprs(relation_exprs))) {
      LOG_WARN("failed to get relation exprs", K(ret));
    } else {
      for (i = 0; OB_SUCC(ret) && !is_correlated && i < relation_exprs.count(); ++i) {
        expr = relation_exprs.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(expr), K(ret));
        } else if (OB_FAIL(ObTransformUtils::is_direct_correlated_expr(
                       expr, subquery->get_current_level() - 1, is_correlated))) {
          LOG_WARN("failed to check expr correlated", K(expr), K(subquery), K(ret));
        } else { /*do nothing*/
        }
      }
    }
    if (OB_SUCC(ret) && !is_correlated) {
      N = subquery->get_table_size();
      for (i = 0; OB_SUCC(ret) && !is_correlated && i < N; ++i) {
        const TableItem* table = subquery->get_table_item(i);
        if (OB_ISNULL(table)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("table should not be NULL", K(ret));
        } else if (table->is_generated_table() && table->ref_query_ != NULL) {
          if (OB_FAIL(is_subquery_direct_correlated(table->ref_query_, is_correlated))) {
            LOG_WARN("check if subquery correlated failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// check joined table on conditions and semi info semi condition correlated
int ObWhereSubQueryPullup::is_join_conditions_correlated(const ObSelectStmt* subquery, bool& is_correlated)
{
  int ret = OB_SUCCESS;
  is_correlated = false;
  if (OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(subquery), K(ret));
  } else {
    const ObIArray<JoinedTable*>& joined_tables = subquery->get_joined_tables();
    const ObIArray<SemiInfo*>& semi_infos = subquery->get_semi_infos();
    for (int64_t i = 0; OB_SUCC(ret) && !is_correlated && i < joined_tables.count(); ++i) {
      if (OB_FAIL(check_joined_conditions_correlated(
              joined_tables.at(i), subquery->get_current_level() - 1, is_correlated))) {
        LOG_WARN("failed to check joined conditions correlated", K(ret));
      } else { /*do nothing*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_correlated && i < semi_infos.count(); ++i) {
      if (OB_FAIL(
              check_semi_conditions_correlated(semi_infos.at(i), subquery->get_current_level() - 1, is_correlated))) {
        LOG_WARN("failed to check semi conditions correlated", K(ret));
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::check_semi_conditions_correlated(
    const SemiInfo* semi_info, int32_t stmt_level, bool& is_correlated)
{
  int ret = OB_SUCCESS;
  is_correlated = false;
  if (OB_ISNULL(semi_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(semi_info), K(ret));
  } else {
    const ObIArray<ObRawExpr*>& semi_conds = semi_info->semi_conditions_;
    for (int64_t i = 0; OB_SUCC(ret) && !is_correlated && i < semi_conds.count(); ++i) {
      if (OB_FAIL(ObTransformUtils::is_correlated_expr(semi_conds.at(i), stmt_level, is_correlated))) {
        LOG_WARN("failed to check expr correlated", K(ret));
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::check_joined_conditions_correlated(
    const JoinedTable* joined_table, int32_t stmt_level, bool& is_correlated)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  is_correlated = false;
  if (OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(joined_table), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check tack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_correlated && i < joined_table->join_conditions_.count(); ++i) {
      if (OB_FAIL(
              ObTransformUtils::is_correlated_expr(joined_table->join_conditions_.at(i), stmt_level, is_correlated))) {
        LOG_WARN("failed to check expr correlated", K(ret));
      }
    }
    if (OB_SUCC(ret) && !is_correlated) {
      if (OB_ISNULL(joined_table->left_table_) || OB_ISNULL(joined_table->right_table_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(joined_table->left_table_), K(joined_table->right_table_), K(ret));
      } else if (joined_table->left_table_->is_joined_table() &&
                 OB_FAIL(check_joined_conditions_correlated(
                     static_cast<JoinedTable*>(joined_table->left_table_), stmt_level, is_correlated))) {
        LOG_WARN("failed to check joined conditions correlated", K(ret));
      } else if (is_correlated) {
        /*do nothing */
      } else if (joined_table->right_table_->is_joined_table() &&
                 OB_FAIL(check_joined_conditions_correlated(
                     static_cast<JoinedTable*>(joined_table->right_table_), stmt_level, is_correlated))) {
        LOG_WARN("failed to check joined conditions correlated", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::is_correlated_join_expr(
    const ObSelectStmt* subquery, const ObRawExpr* expr, bool& is_correlated)
{
  int ret = OB_SUCCESS;
  is_correlated = false;
  if (OB_ISNULL(subquery)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(subquery), K(ret));
  } else if (OB_FAIL(ObTransformUtils::has_current_level_column(expr, subquery->get_current_level(), is_correlated))) {
    LOG_WARN("failed to check expr correlated", K(expr), K(*subquery), K(ret));
  } else if (!is_correlated) {
    // do nothing
  } else if (OB_FAIL(
                 ObTransformUtils::has_current_level_column(expr, subquery->get_current_level() - 1, is_correlated))) {
    LOG_WARN("failed to check expr correlated", K(expr), K(*subquery), K(ret));
  } else if (!is_correlated) {
    // do nothing
  } else { /*do nothing*/
  }
  return ret;
}

int ObWhereSubQueryPullup::is_select_item_contain_subquery(const ObSelectStmt* subquery, bool& contain)
{
  int ret = OB_SUCCESS;
  contain = false;
  if (OB_ISNULL(subquery)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(subquery), K(ret));
  } else {
    int64_t item_size = subquery->get_select_item_size();
    const ObRawExpr* expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && !contain && i < item_size; ++i) {
      expr = subquery->get_select_item(i).expr_;
      if (OB_ISNULL(expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(expr), K(ret));
      } else if (expr->has_flag(CNT_SUB_QUERY)) {
        contain = true;
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::eliminate_subquery(
    ObDMLStmt* stmt, ObRawExpr* expr, bool& can_be_eliminated, bool& need_add_limit_constraint, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is NULL in eliminate subquery", K(ret));
  } else if (!expr->has_flag(CNT_SUB_QUERY)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no subquery in expr", K(ret));
  } else {
    ObQueryRefRawExpr* subq_expr = NULL;
    ObSelectStmt* subquery = NULL;
    const ObRawExpr* left_hand = NULL;
    bool check_status = false;
    if (T_OP_EXISTS == expr->get_expr_type() || T_OP_NOT_EXISTS == expr->get_expr_type()) {
      if (OB_ISNULL(subq_expr = static_cast<ObQueryRefRawExpr*>(expr->get_param_expr(0)))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Subquery expr is NULL", K(ret));
      } else if (OB_ISNULL(subquery = subq_expr->get_ref_stmt())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Subquery stmt is NULL", K(ret));
      } else if (subquery->is_contains_assignment()) {
        // do nothing
      } else if (OB_FAIL(subquery_can_be_eliminated_in_exists(
                     expr->get_expr_type(), subquery, can_be_eliminated, need_add_limit_constraint))) {
        LOG_WARN("Subquery elimination of select list in EXISTS fails", K(ret));
      } else if (!can_be_eliminated) {
        if (OB_FAIL(eliminate_select_list_in_exists(stmt, expr->get_expr_type(), subquery, trans_happened))) {
          LOG_WARN("Subquery elimination of select list in EXISTS fails", K(ret));
        } else if (OB_FAIL(eliminate_groupby_in_exists(stmt, expr->get_expr_type(), subquery, trans_happened))) {
          LOG_WARN("Subquery elimination of group by in EXISTS fails", K(ret));
        } else if (subquery->get_order_items().count() > 0) {
          trans_happened = true;
          subquery->get_order_items().reset();
        }
      }
    } else if (expr->has_flag(IS_WITH_ANY) || expr->has_flag(IS_WITH_ALL)) {
      if (OB_ISNULL(subq_expr = static_cast<ObQueryRefRawExpr*>(expr->get_param_expr(1))) ||
          OB_ISNULL(left_hand = static_cast<ObRawExpr*>(expr->get_param_expr(0)))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Subquery or left_hand expr is NULL", K(ret));
      } else if (OB_ISNULL(subquery = subq_expr->get_ref_stmt())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Subquery stmt is NULL", K(ret));
      } else if (OB_FAIL(eliminate_groupby_in_any_all(subquery, trans_happened))) {
        LOG_WARN("Subquery elimination of group by in ANY, ALL fails", K(ret));
      } else if (OB_FAIL(is_subquery_direct_correlated(subquery, check_status))) {
        LOG_WARN("failed to check subquery correlated", K(subquery), K(ret));
      } else if (check_status) {
        if (OB_FAIL(eliminate_distinct_in_any_all(subquery, trans_happened))) {
          LOG_WARN("Subquery elimination of distinct in ANY, ALL fails", K(ret));
        } else { /*do nothing*/
        }
      } else if (!expr->has_flag(IS_WITH_ANY)) {
        /*do nothing*/
      } else if (OB_FAIL(check_need_add_limit(subquery, check_status))) {
        LOG_WARN("check need add limit failed", K(ret));
      } else if (!check_status) {
        /*do nothing*/
      } else if (OB_FAIL(ObTransformUtils::set_limit_expr(subquery, ctx_))) {
        LOG_WARN("add limit expr failed", K(ret));
      } else {
        trans_happened = true;
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::subquery_can_be_eliminated_in_exists(
    const ObItemType op_type, const ObSelectStmt* stmt, bool& can_be_eliminated, bool& need_add_limit_constraint) const
{
  int ret = OB_SUCCESS;
  can_be_eliminated = false;
  need_add_limit_constraint = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (stmt->get_stmt_hint().enable_no_unnest()) {
    // do nothing.
  } else if (0 == stmt->get_table_size() || stmt->has_set_op()) {
    // Only non-set stmt will be eliminated and do nothing for other DML stmts:
    // 1. set -> No elimination
    // 2. select 1 + floor(2) (table_size == 0) -> No elimination
  } else if (0 == stmt->get_group_expr_size() && !stmt->has_rollup() && stmt->get_aggr_item_size() > 0 &&
             !stmt->has_having()) {
    bool has_limit = false;
    if (OB_FAIL(check_limit(op_type, stmt, has_limit, need_add_limit_constraint))) {
      LOG_WARN("failed to check subquery has unremovable limit", K(ret));
    } else if (!has_limit) {
      can_be_eliminated = true;
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::select_list_can_be_eliminated(
    const ObItemType op_type, const ObSelectStmt* stmt, bool& can_be_eliminated, bool& need_add_limit_constraint) const
{
  int ret = OB_SUCCESS;
  bool has_limit = false;
  can_be_eliminated = false;
  need_add_limit_constraint = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is invalid", K(ret), K(stmt));
  } else if (0 == stmt->get_table_size() || stmt->has_set_op()) {
    // Only non-set stmt will be eliminated and do nothing for other DML stmts:
    // 1. set -> No elimination
    // 2. select 1 + floor(2) (table_size == 0) -> No elimination
  } else if ((0 == stmt->get_group_expr_size() && stmt->get_aggr_item_size() > 0) || stmt->has_having()) {
    /*do nothing*/
  } else if (OB_FAIL(check_limit(op_type, stmt, has_limit, need_add_limit_constraint))) {
    LOG_WARN("failed to check limit", K(ret));
  } else if (has_limit && stmt->has_distinct()) {
    // do nothing
  } else if (stmt->get_select_item_size() == 1 && NULL != stmt->get_select_item(0).expr_ &&
             stmt->get_select_item(0).expr_->is_const_expr()) {
    // do nothing
  } else {
    can_be_eliminated = true;
  }
  return ret;
}

int ObWhereSubQueryPullup::groupby_can_be_eliminated_in_exists(
    const ObItemType op_type, const ObSelectStmt* stmt, bool& can_be_eliminated, bool& need_add_limit_constraint) const
{
  int ret = OB_SUCCESS;
  can_be_eliminated = false;
  need_add_limit_constraint = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (0 == stmt->get_table_size() || stmt->has_set_op()) {
    // Only non-set stmt will be eliminated and do nothing for other DML stmts:
    // 1. set -> No elimination
    // 2. select 1 + floor(2) (table_size == 0) -> No elimination
  } else if (stmt->has_group_by() && 0 == stmt->get_aggr_item_size() &&
             !stmt->has_having()) {  // No having, no limit, no aggr
    bool has_limit = false;
    if (OB_FAIL(check_limit(op_type, stmt, has_limit, need_add_limit_constraint))) {
      LOG_WARN("failed to check subquery has unremovable limit", K(ret));
    } else if (!has_limit) {
      can_be_eliminated = true;
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::groupby_can_be_eliminated_in_any_all(const ObSelectStmt* stmt, bool& can_be_eliminated) const
{
  int ret = OB_SUCCESS;
  can_be_eliminated = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (0 == stmt->get_table_size() || stmt->has_set_op()) {
    // Only non-set stmt will be eliminated and do nothing for other DML stmts:
    // 1. set -> No elimination
    // 2. select 1 + floor(2) (table_size == 0) -> No elimination
  } else if (stmt->has_group_by() && !stmt->has_having() && !stmt->has_limit() && 0 == stmt->get_aggr_item_size()) {
    // Check if select list is involved in group exprs
    ObRawExpr* s_expr = NULL;
    bool all_in_group_exprs = true;
    for (int i = 0; OB_SUCC(ret) && all_in_group_exprs && i < stmt->get_select_item_size(); ++i) {
      if (OB_ISNULL(s_expr = stmt->get_select_item(i).expr_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("select list expr is NULL", K(ret));
      } else if ((s_expr)->has_flag(CNT_COLUMN)) {
        if (!ObOptimizerUtil::find_equal_expr(stmt->get_group_exprs(), s_expr)) {
          all_in_group_exprs = false;
        } else { /* do nothing */
        }
      } else { /* do nothing */
      }
    }  // for
    if (OB_SUCCESS == ret && all_in_group_exprs) {
      can_be_eliminated = true;
    } else { /* do nothing */
    }
  } else { /* do nothing */
  }
  return ret;
}

int ObWhereSubQueryPullup::eliminate_subquery_in_exists(
    ObDMLStmt* stmt, ObRawExpr*& expr, bool need_add_limit_constraint, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory* expr_factory = NULL;
  ObSelectStmt* subquery = NULL;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer Error", KP(expr), KP_(ctx), KP(expr_factory), K(ret));
  } else if (T_OP_EXISTS == expr->get_expr_type() || T_OP_NOT_EXISTS == expr->get_expr_type()) {
    ObOpRawExpr* op = static_cast<ObOpRawExpr*>(expr);
    ObQueryRefRawExpr* subq_expr = static_cast<ObQueryRefRawExpr*>(op->get_param_expr(0));
    if (OB_ISNULL(subq_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid Query Ref Expr", K(ret));
    } else if (OB_ISNULL(subquery = subq_expr->get_ref_stmt())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Subquery stmt is NULL", K(ret));
      // Just in case different parameters hit same plan, firstly we need add const param constraint
    } else if (need_add_limit_constraint && OB_FAIL(ObTransformUtils::add_const_param_constraints(
                                                subquery->get_limit_expr(), stmt->get_query_ctx(), ctx_))) {
      LOG_WARN("failed to add const param constraints", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_subquery_exprs(), subq_expr))) {
      LOG_WARN("remove expr failed", K(ret));
    } else {
      ObConstRawExpr* c_expr = NULL;
      subquery->set_eliminated(true);
      if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
              *expr_factory, ObIntType, (T_OP_EXISTS == expr->get_expr_type()), c_expr))) {
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
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected expr type", KP(expr), K(ret));
  }
  return ret;
}

int ObWhereSubQueryPullup::eliminate_select_list_in_exists(
    ObDMLStmt* stmt, const ObItemType op_type, ObSelectStmt* subquery, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory* expr_factory = NULL;
  if (OB_ISNULL(subquery) || OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer Error", KP(subquery), KP_(ctx), KP(expr_factory), K(ret));
  } else {
    bool can_be_eliminated = false;
    bool need_add_limit_constraint = false;
    if (OB_FAIL(select_list_can_be_eliminated(op_type, subquery, can_be_eliminated, need_add_limit_constraint))) {
      LOG_WARN("Checking if select list can be eliminated in subquery failed", K(ret));
    } else if (can_be_eliminated) {
      // Add single select item with const int 1
      ObConstRawExpr* c_expr = NULL;
      int64_t const_value = 1;
      // Clear existing select items first
      subquery->clear_select_item();
      // Clear distinct flag
      subquery->assign_all();
      // reset window function
      subquery->get_window_func_exprs().reset();
      // Clear the aggr items which only appear in select items
      if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, const_value, c_expr))) {
        LOG_WARN("failed to create expr", K(ret));
      } else if (OB_ISNULL(c_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("create expr error in subquery_select_list_eliminate()", K(c_expr), K(ret));
      } else if (OB_FAIL(c_expr->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize a new expr", K(ret));
      } else {
        SelectItem select_item;
        select_item.alias_name_ = "1";
        select_item.expr_name_ = "1";
        select_item.expr_ = c_expr;
        if (OB_FAIL(subquery->add_select_item(select_item))) {
          LOG_WARN("Failed to add select item", K(select_item), K(ret));
          // Just in case different parameters hit same plan,firstly we need add const param constraint
        } else if (need_add_limit_constraint && OB_FAIL(ObTransformUtils::add_const_param_constraints(
                                                    subquery->get_limit_expr(), stmt->get_query_ctx(), ctx_))) {
          LOG_WARN("failed to add const param constraints", K(ret));
        } else {
          trans_happened = true;
        }
      }
    } else { /* do nothing */
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::eliminate_groupby_in_exists(
    ObDMLStmt* stmt, const ObItemType op_type, ObSelectStmt*& subquery, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool can_be_eliminated = false;
  bool need_add_limit_constraint = false;
  if (OB_ISNULL(subquery) || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Subquery is NULL", K(ret));
  } else if (OB_FAIL(groupby_can_be_eliminated_in_exists(
                 op_type, subquery, can_be_eliminated, need_add_limit_constraint))) {
    LOG_WARN("Checking if group by can be eliminated in subquery in exists failed", K(ret));
  } else if (can_be_eliminated) {
    if (subquery->has_group_by()) {
      // Eliminate group by
      trans_happened = true;
      // Just in case different parameters hit same plan, firstly we need add const param constraint
      if (need_add_limit_constraint && OB_FAIL(ObTransformUtils::add_const_param_constraints(
                                           subquery->get_limit_expr(), stmt->get_query_ctx(), ctx_))) {
        LOG_WARN("failed to add const param constraints", K(ret));
      } else {
        subquery->get_group_exprs().reset();
        trans_happened = true;
      }
    } else { /* do nothing */
    }
  } else { /* do nothing */
  }

  return ret;
}

int ObWhereSubQueryPullup::eliminate_groupby_in_any_all(ObSelectStmt*& subquery, bool& trans_happened)
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
    } else { /* do nothing */
    }
  } else { /* do nothing */
  }

  return ret;
}

int ObWhereSubQueryPullup::eliminate_distinct_in_any_all(ObSelectStmt* subquery, bool& trans_happened)
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
  } else { /* do nothing */
  }
  return ret;
}

int ObWhereSubQueryPullup::check_need_add_limit(ObSelectStmt* subquery, bool& need_add_limit)
{
  int ret = OB_SUCCESS;
  need_add_limit = false;
  if (OB_ISNULL(subquery)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Subquery is NULL", K(ret));
  } else if (subquery->has_limit()) {
    /*do nothing*/
  } else if (subquery->get_from_item_size() > 0) {
    const ObRawExpr* select_expr = NULL;
    bool is_const = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_const && i < subquery->get_select_item_size(); ++i) {
      const SelectItem& select_item = subquery->get_select_item(i);
      if (OB_ISNULL(select_expr = select_item.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select expr is NULL", K(ret));
      } else if (select_expr->has_const_or_const_expr_flag()) {
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

int ObWhereSubQueryPullup::generate_conditions(ObDMLStmt* stmt, ObIArray<ObRawExpr*>& subq_exprs,
    ObSelectStmt* subquery, ObRawExpr* expr, ObIArray<ObRawExpr*>& new_conditions)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory* expr_factory = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr) || OB_ISNULL(subquery) || OB_ISNULL(ctx_) ||
      OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(stmt), K(expr), K(ctx_), K(expr_factory));
  } else if (expr->has_flag(IS_WITH_ALL) || expr->has_flag(IS_WITH_ANY)) {
    ObItemType oper_type = T_INVALID;
    int64_t N = subquery->get_select_item_size();
    ObRawExpr* cmp_expr = NULL;
    ObRawExpr* left_hand = expr->get_param_expr(0);
    if (OB_ISNULL(left_hand)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left param is null", K(ret));
    } else if (N > 1 && OB_UNLIKELY(left_hand->get_expr_type() != T_OP_ROW || left_hand->get_param_count() != N ||
                                    subq_exprs.count() != N)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid vector comparison", K(ret), K(N), K(subq_exprs.count()), K(*left_hand));
    } else if (OB_FAIL(get_semi_oper_type(expr, oper_type))) {
      LOG_WARN("failed to get oper type", K(ret));
    } else if (T_INVALID == oper_type) {
      ret = OB_ERR_ILLEGAL_TYPE;
      LOG_WARN("Invalid oper type in subquery", K(ret), K(expr->get_expr_type()));
    } else if (N == 1 || oper_type == T_OP_EQ) {
      // a = b, a > b, a !=b, (a,b) = (c,d) ...
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        ObRawExpr* left_arg = (N == 1) ? left_hand : left_hand->get_param_expr(i);
        ObRawExpr* right_arg = subq_exprs.at(i);
        ObRawExpr* select_expr = subquery->get_select_item(i).expr_;
        if (OB_SUCC(ret) && OB_FAIL(ObRawExprUtils::create_double_op_expr(
                                *expr_factory, ctx_->session_info_, oper_type, cmp_expr, left_arg, right_arg))) {
          LOG_WARN("failed to create comparison expr", K(ret));
        } else if (!expr->has_flag(IS_WITH_ALL)) {
          // do nothing
        } else if (OB_FAIL(
                       generate_anti_condition(stmt, subquery, cmp_expr, select_expr, left_arg, right_arg, cmp_expr))) {
          LOG_WARN("failed to create anti join condition", K(ret));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(cmp_expr->pull_relation_id_and_levels(subquery->get_current_level()))) {
            LOG_WARN("pull expr relation ids failed", K(ret));
          } else if (OB_FAIL(new_conditions.push_back(cmp_expr))) {
            LOG_WARN("failed to push back new expr", K(ret));
          }
        }
      }
    } else {
      // (a,b) != any (c,d) => (a,b) != (c,d) semi
      // (a,b)  = all (c,d) => lnnvl( (a,b) == (c,d) ) anit
      ObOpRawExpr* right_vector = NULL;
      ObRawExpr* lnnvl_expr = NULL;
      oper_type = expr->has_flag(IS_WITH_ALL) ? T_OP_EQ : T_OP_NE;
      if (OB_FAIL(expr_factory->create_raw_expr(T_OP_ROW, right_vector))) {
        LOG_WARN("failed to create a new expr", K(ret));
      } else if (OB_FAIL(right_vector->get_param_exprs().assign(subq_exprs))) {
        LOG_WARN("failed to add param expr", K(ret));
      } else if (lib::is_oracle_mode()) {
        ObOpRawExpr* row_expr = NULL;
        if (OB_FAIL(expr_factory->create_raw_expr(T_OP_ROW, row_expr))) {
          LOG_WARN("failed to create a new expr", K(oper_type), K(ret));
        } else if (OB_FAIL(row_expr->add_param_expr(right_vector))) {
          LOG_WARN("fail to set param expr", K(ret), K(right_vector), K(row_expr));
        }
        right_vector = row_expr;
      }
      if (OB_SUCC(ret) && OB_FAIL(ObRawExprUtils::create_double_op_expr(
                              *expr_factory, ctx_->session_info_, oper_type, cmp_expr, left_hand, right_vector))) {
        LOG_WARN("failed to create comparison expr", K(ret));
      } else if (!expr->has_flag(IS_WITH_ALL)) {
        // do nothing
      } else if (OB_FAIL(ObRawExprUtils::build_lnnvl_expr(*expr_factory, cmp_expr, lnnvl_expr))) {
        LOG_WARN("failed to build lnnvl expr", K(ret));
      } else {
        cmp_expr = lnnvl_expr;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(cmp_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("failed to formalize expr", K(ret));
        } else if (OB_FAIL(cmp_expr->pull_relation_id_and_levels(subquery->get_current_level()))) {
          LOG_WARN("failed to pull expr relation ids", K(ret));
        } else if (OB_FAIL(new_conditions.push_back(cmp_expr))) {
          LOG_WARN("failed to push back new condition", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::pullup_non_correlated_subquery_as_view(
    ObDMLStmt* stmt, ObSelectStmt* subquery, ObRawExpr* expr, ObQueryRefRawExpr* subquery_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory* expr_factory = NULL;
  TableItem* table_item = NULL;
  ObSEArray<ObRawExpr*, 4> column_exprs;
  ObSEArray<ObRawExpr*, 4> new_conditions;
  SemiInfo* info = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(subquery) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or subquery, query ctx should not be null", K(ret), K(stmt), K(subquery), K(expr));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx or expr_factory is null", K(ret), K(ctx_), K(expr_factory));
  } else if (!expr->has_flag(IS_WITH_ANY) && !expr->has_flag(IS_WITH_ALL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expression is invalid", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, stmt, subquery, table_item))) {
    LOG_WARN("failed to add new table_item", K(ret));
  } else if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_item should not be null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_condition_exprs(), expr))) {
    LOG_WARN("failed to remove condition expr", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_subquery_exprs(), subquery_expr))) {
    LOG_WARN("failed to remove subquery expr", K(ret));
  } else if (OB_FAIL(subquery->pullup_stmt_level())) {
    LOG_WARN("failed to pullup stmt level", K(ret));
  } else if (OB_FAIL(subquery->adjust_view_parent_namespace_stmt(stmt->get_parent_namespace_stmt()))) {
    LOG_WARN("failed to adjust view parent namespace stmt", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *table_item, stmt, column_exprs))) {
    LOG_WARN("failed to create columns for view", K(ret));
  } else if (OB_FAIL(generate_conditions(stmt, column_exprs, subquery, expr, new_conditions))) {
    LOG_WARN("failed to generate new condition exprs", K(ret));
  } else if (OB_FAIL(generate_semi_info(stmt, expr, table_item, new_conditions, info))) {
    LOG_WARN("generate semi info failed", K(ret));
  } else if (OB_FAIL(subquery->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("formalize child stmt failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::pushdown_semi_info_right_filter(stmt, ctx_, info))) {
    LOG_WARN("failed to pushdown semi info right filter", K(ret));
  }
  return ret;
}

int ObWhereSubQueryPullup::transform_single_set_query(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> cond_exprs;
  ObSEArray<ObRawExpr*, 4> post_join_exprs;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (0 == stmt->get_from_item_size()) {
    /*do dothing*/
  } else if (OB_FAIL(cond_exprs.assign(stmt->get_condition_exprs()))) {
    LOG_WARN("failed to get non semi conditions", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_post_join_exprs(stmt, post_join_exprs, true))) {
    LOG_WARN("failed to get post join exprs", K(ret));
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt* sel_stmt = static_cast<ObSelectStmt*>(stmt);
    if (OB_FAIL(sel_stmt->get_select_exprs(select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cond_exprs.count(); ++i) {
    ObSEArray<ObQueryRefRawExpr*, 4> queries;
    ObSEArray<const ObRawExpr*, 1> tmp;
    bool is_null_reject = false;
    if (OB_FAIL(get_single_set_subquery(stmt->get_current_level(), cond_exprs.at(i), queries))) {
      LOG_WARN("failed to get single set subquery", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < queries.count(); ++j) {
      if (is_vector_query(queries.at(j))) {
        // not necessary limitation
      } else if (OB_FAIL(ObTransformUtils::is_null_reject_condition(cond_exprs.at(i), tmp, is_null_reject))) {
        LOG_WARN("failed to check is null reject condition", K(ret));
      } else if (OB_FAIL(unnest_single_set_subquery(stmt, queries.at(j), !is_null_reject, false))) {
        LOG_WARN("failed to unnest single set subquery", K(ret));
      } else {
        trans_happened = true;
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < post_join_exprs.count(); ++i) {
    ObSEArray<ObQueryRefRawExpr*, 4> queries;
    bool is_vector_assign = false;
    bool is_select_expr = false;
    if (OB_FAIL(get_single_set_subquery(stmt->get_current_level(), post_join_exprs.at(i), queries))) {
      LOG_WARN("failed to get single set subquery", K(ret));
    } else {
      is_vector_assign = post_join_exprs.at(i)->has_flag(CNT_ALIAS);
      if (ObOptimizerUtil::find_item(select_exprs, post_join_exprs.at(i))) {
        is_select_expr = true;
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < queries.count(); ++j) {
      if (OB_FAIL(unnest_single_set_subquery(stmt, queries.at(j), true, is_vector_assign, is_select_expr))) {
        LOG_WARN("failed to unnest single set subquery", K(ret));
      } else {
        trans_happened = true;
      }
    }
  }
  return ret;
}

bool ObWhereSubQueryPullup::is_vector_query(ObQueryRefRawExpr* query)
{
  bool bret = false;
  if (NULL == query || NULL == query->get_ref_stmt()) {
    // do nothing
  } else {
    bret = query->get_ref_stmt()->get_select_item_size() >= 2;
  }
  return bret;
}

int ObWhereSubQueryPullup::get_single_set_subquery(
    const int32_t current_level, ObRawExpr* expr, ObIArray<ObQueryRefRawExpr*>& queries)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->has_flag(IS_WITH_ANY) || expr->has_flag(IS_WITH_ALL) || expr->has_hierarchical_query_flag() ||
             !expr->has_flag(CNT_SUB_QUERY)) {
  } else if (!expr->is_query_ref_expr()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(get_single_set_subquery(current_level, expr->get_param_expr(i), queries)))) {
        LOG_WARN("failed to get single set query", K(ret));
      }
    }
  } else {
    bool is_valid = false;
    ObQueryRefRawExpr* query = static_cast<ObQueryRefRawExpr*>(expr);
    if (current_level != query->get_expr_level()) {
    } else if (ObOptimizerUtil::find_item(queries, query)) {
      // do nothing
    } else if (OB_FAIL(check_subquery_validity(query->get_ref_stmt(), is_valid))) {
      LOG_WARN("failed to check subquery validity", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(queries.push_back(query))) {
      LOG_WARN("failed to push back query", K(ret));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::check_subquery_validity(ObSelectStmt* subquery, bool& is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObRawExpr*, 4> targets;
  ObSEArray<ObRawExpr*, 4> columns;
  ObSEArray<ObRawExpr*, 4> const_columns;
  ObSEArray<ObRawExpr*, 4> const_exprs;
  is_valid = true;
  if (OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery is null", K(ret), K(subquery));
  } else if (!subquery->is_spj() || subquery->has_subquery() || subquery->get_stmt_hint().enable_no_unnest()) {
    is_valid = false;
  } else if (OB_FAIL(subquery->get_column_exprs(columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(append(targets, columns))) {
    LOG_WARN("failed to append targets", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(
                 subquery->get_condition_exprs(), subquery->get_current_level(), const_exprs))) {
    LOG_WARN("failed to compute const exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < columns.count(); ++i) {
    if (!ObOptimizerUtil::find_item(const_exprs, columns.at(i))) {
      // do nothing
    } else if (OB_FAIL(const_columns.push_back(columns.at(i)))) {
      LOG_WARN("failed to push back const column", K(ret));
    }
  }
  if (OB_SUCC(ret) && is_valid) {
    if (OB_FAIL(ObTransformUtils::check_stmt_unique(
            subquery, ctx_->session_info_, ctx_->schema_checker_, const_columns, true /* strict */, is_valid))) {
      LOG_WARN("failed to check stmt unique", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery->get_select_item_size(); ++i) {
    if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(subquery->get_select_item(i).expr_, targets, is_valid))) {
      LOG_WARN("failed to check is null propagate expr", K(ret));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::unnest_single_set_subquery(ObDMLStmt* stmt, ObQueryRefRawExpr* query_expr,
    const bool use_outer_join, const bool is_vector_assign, const bool in_select_expr)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* subquery = NULL;
  ObSEArray<ObRawExpr*, 4> query_refs;
  ObSEArray<ObRawExpr*, 4> select_list;
  bool is_match_index = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(query_expr) ||
      OB_ISNULL(subquery = query_expr->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (in_select_expr && !subquery->get_stmt_hint().enable_unnest() &&
             OB_FAIL(ObTransformUtils::check_subquery_match_index(ctx_, subquery, is_match_index))) {
    LOG_WARN("failed to check subquery match index", K(ret));
  } else if (is_match_index) {
    LOG_TRACE("subquery in select expr match index, so we donot unnest it");
  } else if (OB_FAIL(subquery->get_select_exprs(select_list))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (is_vector_assign) {
    if (OB_UNLIKELY(!stmt->is_update_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update stmt is expected here", K(ret));
    } else if (OB_FAIL(static_cast<ObUpdateStmt*>(stmt)->get_vector_assign_values(query_expr, query_refs))) {
      LOG_WARN("failed to get vector assign values", K(ret));
    }
  } else {
    if (OB_FAIL(query_refs.push_back(query_expr))) {
      LOG_WARN("failed to push back query refs", K(ret));
    }
  }
  if (OB_SUCC(ret) && !is_match_index) {
    if (OB_UNLIKELY(query_refs.count() != select_list.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("size does not match", K(ret), K(query_refs.count()), K(select_list.count()), K(is_vector_assign));
    } else if (OB_FAIL(pull_up_tables_and_columns(stmt, subquery))) {
      LOG_WARN("failed to merge tables to parent stmt", K(ret));
    } else if (OB_FAIL(trans_from_list(stmt, subquery, use_outer_join))) {
      LOG_WARN("failed to transform from list", K(ret));
    } else if (OB_FAIL(subquery->pullup_stmt_level())) {
      LOG_WARN("failed to pullup stmt level", K(ret));
    } else if (OB_FAIL(subquery->adjust_view_parent_namespace_stmt(stmt->get_parent_namespace_stmt()))) {
      LOG_WARN("failed to adjust view parent namespace stmt", K(ret));
    } else if (OB_FAIL(append(stmt->get_deduced_exprs(), subquery->get_deduced_exprs()))) {
      LOG_WARN("failed to append deduced expr", K(ret));
    } else if (OB_FAIL(append(stmt->get_part_exprs(), subquery->get_part_exprs()))) {
      LOG_WARN("failed to append part expr", K(ret));
    } else if (OB_FAIL(append(stmt->get_related_part_expr_arrays(), subquery->get_related_part_expr_arrays()))) {
      LOG_WARN("failed to append related part expr array", K(ret));
    } else if (OB_FAIL(pull_up_semi_info(stmt, subquery))) {
      LOG_WARN("failed to merge others to parent stmt", K(ret));
    } else if (OB_FAIL(stmt->get_stmt_hint().add_view_merge_hint(&(subquery->get_stmt_hint())))) {
      LOG_WARN("Failed to add view merge hint", K(ret));
    } else if (OB_FAIL(stmt->replace_inner_stmt_expr(query_refs, select_list))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_subquery_exprs(), query_expr))) {
      LOG_WARN("remove expr failed", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else {
      subquery->set_eliminated(true);
      if (is_vector_assign) {
        if (OB_FAIL(static_cast<ObUpdateStmt*>(stmt)->check_assign())) {
          LOG_WARN("failed to check assign", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::pull_up_tables_and_columns(ObDMLStmt* stmt, ObSelectStmt* subquery)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(stmt), K(subquery), K(ret));
  } else if (OB_FAIL(append(stmt->get_table_items(), subquery->get_table_items()))) {
    LOG_WARN("failed to append table items", K(ret));
  } else if (OB_FAIL(append(stmt->get_joined_tables(), subquery->get_joined_tables()))) {
    LOG_WARN("failed to append joined tables", K(ret));
  } else if (OB_FAIL(append(stmt->get_column_items(), subquery->get_column_items()))) {
    LOG_WARN("failed to append column items", K(ret));
  } else if (OB_FAIL(stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
    LOG_WARN("failed to update column item rel id", K(ret));
  }
  return ret;
}

int ObWhereSubQueryPullup::pull_up_semi_info(ObDMLStmt* stmt, ObSelectStmt* subquery)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(subquery), K(ret));
  } else {
    ObSqlBitSet<> table_set;
    ObIArray<TableItem*>& table_items = subquery->get_table_items();
    ObSEArray<uint64_t, 4> left_tables;
    ObSEArray<ObDMLStmt*, 4> ignore_stmts;
    const int32_t upper_level = subquery->get_current_level() - 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
      if (OB_ISNULL(table_items.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (OB_FAIL(table_set.add_member(subquery->get_table_bit_index(table_items.at(i)->table_id_)))) {
        LOG_WARN("failed to add member to ObBitSet", K(ret));
      } else { /*do nothing*/
      }
    }

    ObRawExpr* cur_expr = NULL;
    bool check_status = false;
    for (int64_t j = 0; OB_SUCC(ret) && j < subquery->get_condition_size(); ++j) {
      ObSEArray<ObRawExpr*, 4> columns;
      if (OB_ISNULL(cur_expr = subquery->get_condition_expr(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null ptr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::is_direct_correlated_expr(cur_expr, upper_level, check_status))) {
        LOG_WARN("failed to check expr correlated", K(*cur_expr), K(ret));
      } else if (!check_status) {
        /*do nothing*/
      } else if (OB_FAIL(
                     ObTransformUtils::extract_column_exprs(cur_expr, upper_level, table_set, ignore_stmts, columns))) {
        LOG_WARN("extract column exprs failed", K(*cur_expr), K(ret));
      } else {
        for (int64_t k = 0; OB_SUCC(ret) && k < columns.count(); ++k) {
          const ObColumnRefRawExpr* column = static_cast<ObColumnRefRawExpr*>(columns.at(k));
          if (OB_ISNULL(column)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null ptr", K(ret));
          } else if (OB_FAIL(add_var_to_array_no_dup(left_tables, column->get_table_id()))) {
            LOG_WARN("failed to add var no dup", K(left_tables), K(ret));
          } else { /*do nothing*/
          }
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < subquery->get_semi_infos().count(); ++i) {
      SemiInfo* info = subquery->get_semi_infos().at(i);
      if (OB_ISNULL(info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL pointer error", K(info), K(ret));
      } else if (OB_FAIL(append_array_no_dup(info->left_table_ids_, left_tables))) {
        LOG_WARN("failed to refine semi info", K(ret));
      } else if (OB_FAIL(stmt->add_semi_info(info))) {
        LOG_WARN("failed to add semi info", K(info), K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::trans_from_list(ObDMLStmt* stmt, ObSelectStmt* subquery, const bool use_outer_join)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(subquery));
  } else if (use_outer_join) {
    TableItem* left_table = NULL;
    TableItem* right_table = NULL;
    TableItem* joined_table = NULL;
    if (OB_FAIL(ObTransformUtils::merge_from_items_as_inner_join(ctx_, *stmt, left_table))) {
      LOG_WARN("failed to merge from items ad inner join", K(ret));
    } else if (OB_FAIL(ObTransformUtils::merge_from_items_as_inner_join(ctx_, *subquery, right_table))) {
      LOG_WARN("failed to merge from items as inner join", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_new_joined_table(ctx_,
                   *stmt,
                   LEFT_OUTER_JOIN,
                   left_table,
                   right_table,
                   subquery->get_condition_exprs(),
                   joined_table))) {
      LOG_WARN("failed to add new joined table", K(ret));
    } else if (FALSE_IT(stmt->get_from_items().reset())) {
      // do nothing
    } else if (OB_FAIL(stmt->add_from_item(joined_table->table_id_, true))) {
      LOG_WARN("failed to add from items", K(ret));
    }
  } else {
    if (OB_FAIL(append(stmt->get_from_items(), subquery->get_from_items()))) {
      LOG_WARN("failed to append from items", K(ret));
    } else if (OB_FAIL(append(stmt->get_condition_exprs(), subquery->get_condition_exprs()))) {
      LOG_WARN("failed to append condition exprs", K(ret));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::add_limit_for_exists_subquery(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool happened = false;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ptr", K(ret), K(stmt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); ++i) {
      ObRawExpr* expr = stmt->get_condition_expr(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null ptr", K(ret), K(expr));
      } else if (OB_FAIL(recursive_add_limit_for_exists_expr(expr, happened))) {
        LOG_WARN("failed recursive add limit for exists expr", K(ret), K(expr));
      } else {
        trans_happened |= happened;
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::recursive_add_limit_for_exists_expr(ObRawExpr* expr, bool& trans_happened)
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
      ObRawExpr* param_expr = expr->get_param_expr(i);
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null ptr", K(ret), K(param_expr));
      } else if (OB_FAIL(SMART_CALL(recursive_add_limit_for_exists_expr(param_expr, happened)))) {
        LOG_WARN("failed recursive add limit for exists expr", K(ret), K(param_expr));
      } else {
        trans_happened |= happened;
      }
    }
    if (OB_SUCC(ret)) {
      if (T_OP_EXISTS == expr->get_expr_type() || T_OP_NOT_EXISTS == expr->get_expr_type()) {
        ObSelectStmt* subquery = NULL;
        ObOpRawExpr* op = static_cast<ObOpRawExpr*>(expr);
        ObQueryRefRawExpr* subq_expr = static_cast<ObQueryRefRawExpr*>(op->get_param_expr(0));
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

}  // namespace sql
}  // namespace oceanbase
