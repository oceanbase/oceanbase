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
#include "sql/rewrite/ob_transform_simplify_expr.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/rewrite/ob_stmt_comparer.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "common/ob_smart_call.h"

using namespace oceanbase::sql;

int ObTransformSimplifyExpr::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                ObDMLStmt *&stmt,
                                                bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  UNUSED(parent_stmts);
  if (OB_FAIL(flatten_stmt_exprs(stmt, is_happened))) {
    LOG_WARN("failed to flatten stmt exprs", K(is_happened));
  } else {
    trans_happened |= is_happened;
    LOG_TRACE("succeed to flatten stmt exprs", K(is_happened));
  }
  if (OB_FAIL(replace_is_null_condition(stmt, is_happened))) {
    LOG_WARN("failed to replace is null condition", K(is_happened));
  } else {
    trans_happened |= is_happened;
    OPT_TRACE("replace is null condition:", is_happened);
    LOG_TRACE("succeed to replace is null condition", K(is_happened));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(replace_op_null_condition(stmt, is_happened))) {
      LOG_WARN("failed to replace op null condition", K(is_happened));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("replace op null condition:", is_happened);
      LOG_TRACE("succeed to replace op null condition", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(transform_is_false_true_expr(stmt, is_happened))) {
      LOG_WARN("failed to transform is false true expr", K(is_happened));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("transform is false true expr:", is_happened);
      LOG_TRACE("succeed to transform is false true expr", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_preds_vector_to_scalar(stmt, is_happened))) {
      LOG_WARN("failed to convert vector predicate to scalar", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("convert vector predicate to scalar:", is_happened);
      LOG_TRACE("succeed to convert vector predicate to scalar", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(remove_dummy_exprs(stmt, is_happened))) {
      LOG_WARN("failed to remove dummy exprs", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("remove dummy exprs:", is_happened);
      LOG_TRACE("succeed to remove dummy exprs", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(remove_ora_decode(stmt, is_happened))) {
      LOG_WARN("failed to remove ora_decode", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("remove ora_decode", is_happened);
      LOG_TRACE("succeed to remove decode", K(is_happened));
    }
  }
  if (OB_SUCC(ret)){
    if (OB_FAIL(convert_nvl_predicate(stmt, is_happened))){
      LOG_WARN("failed to convert nvl predicate", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("convert nvl predicate:", is_happened);
      LOG_TRACE("succeed to convert nvl predicate", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(replace_like_condition(stmt, is_happened))) {
      LOG_WARN("failed to replace like condition", K(is_happened));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("replace like condition", is_happened);
      LOG_TRACE("succeed to replace like condition", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(remove_subquery_when_filter_is_false(stmt, is_happened))) {
      LOG_WARN("failed to remove subquery when filter is false", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("remove subquery when filter is false", is_happened);
      LOG_TRACE("succeed to remove subquery when filter is false", K(is_happened));
    }
  }
  if (OB_SUCC(ret) && trans_happened) {
    if (OB_FAIL(add_transform_hint(*stmt))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::flatten_stmt_exprs(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool where_happened = false;
  bool having_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(ret), K(stmt));
  } else if (OB_FAIL(flatten_exprs(stmt->get_condition_exprs(), where_happened))) {
    LOG_WARN("failed to flatten expr in where", K(ret));
  } else if (stmt->is_select_stmt()
             && OB_FAIL(flatten_exprs(static_cast<ObSelectStmt*>(stmt)->get_having_exprs(), having_happened))) {
    LOG_WARN("failed to flatten expr in having", K(ret));
  } else {
    trans_happened = where_happened | having_happened;
    ObIArray<JoinedTable*> &joined_table = stmt->get_joined_tables();
    bool is_happened = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table.count(); i++) {
      if (OB_FAIL(flatten_join_condition_exprs(joined_table.at(i), is_happened))) {
        LOG_WARN("failed to flatten join condition exprs", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::flatten_join_condition_exprs(TableItem *table, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  JoinedTable *join_table = NULL;
  trans_happened = false;
  bool cur_happened = false;
  bool left_happened = false;
  bool right_happened = false;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table));
  } else if (!table->is_joined_table()) {
    /*do nothing*/
  } else if (OB_ISNULL(join_table = static_cast<JoinedTable*>(table)) ||
             OB_ISNULL(join_table->left_table_) || OB_ISNULL(join_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(join_table));
  } else if (OB_FAIL(flatten_exprs(join_table->join_conditions_, cur_happened))) {
    LOG_WARN("failed to flatten expr in join conditions", K(ret));
  } else if (OB_FAIL(SMART_CALL(flatten_join_condition_exprs(join_table->left_table_,
                                                             left_happened)))) {
    LOG_WARN("failed to flatten left child join condition exprs", K(ret));
  } else if (OB_FAIL(SMART_CALL(flatten_join_condition_exprs(join_table->right_table_,
                                                             right_happened)))) {
    LOG_WARN("failed to flatten right child join condition exprs", K(ret));
  } else {
    trans_happened = cur_happened | left_happened | right_happened;
  }
  return ret;
}

int ObTransformSimplifyExpr::flatten_exprs(common::ObIArray<ObRawExpr*> &exprs, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> temp_exprs;
  trans_happened = false;
  if (OB_FAIL(temp_exprs.assign(exprs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else {
    exprs.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_exprs.count(); i++) {
      if (OB_ISNULL(temp_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::flatten_expr(temp_exprs.at(i), exprs))) {
        LOG_WARN("failed to flatten exprs", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      trans_happened = temp_exprs.count() != exprs.count();
    }
  }
  return ret;
}

// for select/update/delete
//
// 1.当condition中含有column is NULL(is not NULL)时，判断column是否为NOT NULL，如果是，则将column is NULL替换为false(true)
// 2.改写的条件：
//   1）column 含有not null属性
//   2）column的类型不能为datetime和date，含有auto_increament属性并且sql_auto_is_null非0(is not null不受这个限制),原因详见：http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#operator_is-null
//   3）column不能来自子查询，通过1）能够过滤
//   4）column不能来自left join的右枝，right join的左枝，full outer join的左右枝
int ObTransformSimplifyExpr::replace_is_null_condition(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data member or parameter is NULL", K(stmt), K(ctx_));
  } else if (stmt->is_sel_del_upd()) {
    ObNotNullContext not_null_ctx(*ctx_, stmt);
    if (OB_FAIL(not_null_ctx.generate_stmt_context(NULLABLE_SCOPE::NS_FROM))){
      LOG_WARN("failed to generate not null context", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); ++i) {
      if (OB_FAIL(inner_replace_is_null_condition(
                           stmt, stmt->get_condition_exprs().at(i), not_null_ctx, is_happened))) {
        LOG_WARN("failed to replace is null expr", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
    if (OB_SUCC(ret) && stmt->is_select_stmt() && !static_cast<ObSelectStmt *>(stmt)->is_scala_group_by()) {
      not_null_ctx.reset();
      if (OB_FAIL(not_null_ctx.generate_stmt_context(NULLABLE_SCOPE::NS_TOP))){
        LOG_WARN("failed to generate not null context", K(ret));
      }
      ObSelectStmt *sel_stmt = static_cast<ObSelectStmt *>(stmt);
      for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt->get_having_expr_size(); ++i) {
        if (OB_FAIL(not_null_ctx.remove_having_filter(sel_stmt->get_having_exprs().at(i)))){
          LOG_WARN("failed to remove filter", K(ret));
        } else if (OB_FAIL(inner_replace_is_null_condition(
                      sel_stmt, sel_stmt->get_having_exprs().at(i), not_null_ctx, is_happened))) {
          LOG_WARN("failed to replace is null expr", K(ret));
        } else if (OB_FAIL(not_null_ctx.add_having_filter(sel_stmt->get_having_exprs().at(i)))) {
          LOG_WARN("failed to add filter", K(ret));
        } else {
          trans_happened |= is_happened;
        }
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::inner_replace_is_null_condition(ObDMLStmt *stmt,
                                                             ObRawExpr *&expr,
                                                             ObNotNullContext &not_null_ctx,
                                                             bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(expr), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (expr->is_op_expr()) {
    // do transformation for child exprs first
    ObOpRawExpr *op_expr = static_cast<ObOpRawExpr*>(expr);
    for (int64_t i = 0; OB_SUCC(ret) && i < op_expr->get_param_count(); i++) {
      ObRawExpr *temp = NULL;
      if (OB_ISNULL(temp = op_expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(SMART_CALL(inner_replace_is_null_condition(stmt,
                                                                    temp,
                                                                    not_null_ctx,
                                                                    is_happened)))) {
        LOG_WARN("failed to replace is null expr", K(ret));
      } else {
        trans_happened |= is_happened;
        op_expr->get_param_expr(i) = temp;
      }
    }
  }

  if (OB_SUCC(ret)
      && (T_OP_IS == expr->get_expr_type()
          || T_OP_IS_NOT == expr->get_expr_type())) {
    // do transforamtion for its own exprs
    if (OB_FAIL(do_replace_is_null_condition(stmt, expr, not_null_ctx, is_happened))) {
      LOG_WARN("failed to replace is null condition", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  }

  if (OB_SUCC(ret) && trans_happened) {
    if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ctx_), K(ret));
    } else if (OB_FAIL(expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformSimplifyExpr::do_replace_is_null_condition(ObDMLStmt *stmt,
                                                          ObRawExpr *&expr,
                                                          ObNotNullContext &not_null_ctx,
                                                          bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(expr), K(ret));
  } else if (T_OP_IS == expr->get_expr_type() ||
             T_OP_IS_NOT == expr->get_expr_type()) {
    const ObOpRawExpr *op_expr = static_cast<ObOpRawExpr *>(expr);
    if (OB_UNLIKELY(op_expr->get_param_count() != 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected param numm", K(op_expr->get_param_count()), K(ret));
    } else {
      const ObRawExpr *child_0 = op_expr->get_param_expr(0);
      const ObRawExpr *child_1 = op_expr->get_param_expr(1);
      if (OB_ISNULL(child_0) || OB_ISNULL(child_1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpecte null", K(child_0), K(child_1), K(ret));
      } else if (child_1->get_expr_type() == T_NULL) {
        bool is_expected = false;
        const ObColumnRefRawExpr *col_expr = NULL;
        if (child_0->get_expr_type() == T_FUN_COUNT) {
          is_expected = true;
        } else if (child_0->is_column_ref_expr()) {
          col_expr = static_cast<const ObColumnRefRawExpr *>(child_0);
        }

        ObArray<ObRawExpr *> constraints;
        if (OB_SUCC(ret) && NULL != col_expr) {
          bool is_not_null = false;
          if (OB_FAIL(ObTransformUtils::is_expr_not_null(not_null_ctx, col_expr,
                                                         is_not_null, &constraints))) {
            LOG_WARN("failed to check expr not null", K(ret));
          } else if (is_not_null) {
            if (T_OP_IS_NOT == expr->get_expr_type()
                || T_OP_IS == expr->get_expr_type()) {
              is_expected = true;
            }
          }
        }
        if (OB_SUCC(ret) && is_expected) {
          bool b_value = (T_OP_IS_NOT == expr->get_expr_type());
          if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(ctx_->expr_factory_, expr, b_value))) {
            LOG_WARN("create const bool expr failed", K(ret));
          } else if (OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, constraints))) {
            LOG_WARN("failed to add param not null constraint", K(ret));
          } else {
            trans_happened = true;
          }
        }
      }
    }
  }
  return ret;
}

// rewrite null-reject conditions
int ObTransformSimplifyExpr::replace_op_null_condition(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) ||
      OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret), K(stmt), K(ctx_), K(plan_ctx));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); ++i) {
    ObRawExpr *cond = NULL;
    if (OB_ISNULL(cond = stmt->get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (OB_FAIL(replace_cmp_null_condition(stmt->get_condition_exprs().at(i),
                                                  *stmt,
                                                  plan_ctx->get_param_store(),
                                                  is_happened))) {
      LOG_WARN("function replace_cmp_null_condition is failure", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  }
  if (OB_SUCC(ret) && stmt->is_select_stmt() && !static_cast<ObSelectStmt *>(stmt)->is_scala_group_by()) {
    ObSelectStmt *sel_stmt = static_cast<ObSelectStmt *>(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt->get_having_expr_size(); ++i) {
      if (OB_FAIL(replace_cmp_null_condition(sel_stmt->get_having_exprs().at(i),
                                             *stmt,
                                             plan_ctx->get_param_store(),
                                             is_happened))) {
        LOG_WARN("function replace_cmp_null_condition is failure", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::replace_cmp_null_condition(ObRawExpr *&expr,
                                                        const ObDMLStmt &stmt,
                                                        const ParamStore &param_store,
                                                        bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_null_reject = false;
  bool is_happened = false;
  ObSEArray<const ObRawExpr *, 4> null_expr_lists;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(ret), K(expr));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (T_OP_OR == expr->get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(replace_cmp_null_condition(expr->get_param_expr(i), stmt,
                                                        param_store, is_happened)))) {
        LOG_WARN("or type execute failure", K(expr), K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  } else if (OB_FAIL(extract_null_expr(expr, stmt, param_store, null_expr_lists))) {
    LOG_WARN("failed to use extract_null_expr funciton", K(ret), K(ctx_));
  } else if (0 != null_expr_lists.count()) {
    if (OB_FAIL(ObTransformUtils::is_null_reject_condition(expr, null_expr_lists, is_null_reject))) {
      LOG_WARN("failed to use is_null_reject_condition function", K(ret));
    } else if (is_null_reject) {
      bool b_value = false;
      if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(ctx_->expr_factory_, expr, b_value))) {
        LOG_WARN("create const bool expr failed", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < null_expr_lists.count(); ++i) {
          if (OB_ISNULL(null_expr_lists.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is null", K(ret));
          } else if (null_expr_lists.at(i)->is_static_const_expr()) {
            ObExprConstraint cons(const_cast<ObRawExpr*>(null_expr_lists.at(i)), 
                                    PRE_CALC_RESULT_NULL);
            if (OB_FAIL(ctx_->expr_constraints_.push_back(cons))) {
              LOG_WARN("failed to push back pre calc constraints", K(ret));
            }
          }
        }
        trans_happened = true;
      }
    }
  } else {
    /*do nothing*/
  }
  return ret;
}

int ObTransformSimplifyExpr::extract_null_expr(ObRawExpr *expr,
                                               const ObDMLStmt &stmt,
                                               const ParamStore &param_store,
                                               ObIArray<const ObRawExpr *> &null_expr_lists)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  UNUSED(param_store);
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null param", K(expr), K_(ctx), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (expr->is_static_scalar_const_expr()) {
    ObObj result;
    bool got_result = false;
    if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_->exec_ctx_,
                                                          expr,
                                                          result,
                                                          got_result,
                                                          *ctx_->allocator_))) {
      LOG_WARN("failed to calc const or calculable expr", K(ret));
    } else if (got_result && 
               (result.is_null() || (lib::is_oracle_mode() && result.is_null_oracle())))  {
      if (OB_FAIL(null_expr_lists.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      ObRawExpr *temp = NULL;
      if (OB_ISNULL(temp = expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(SMART_CALL(extract_null_expr(temp, stmt, param_store, null_expr_lists)))) {
        LOG_WARN("failed to use extract_null_expr function", K(ret));
      } else {
        /*do nothing*/
      }
    }
  }
  return ret;
}

/* transfrom like to '=' in oracle mode when pattern doesn't have wildcards in oracle mode*/
int ObTransformSimplifyExpr::replace_like_condition(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSelectStmt *select_stmt = nullptr;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) ||
      OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret), K(stmt), K(ctx_), K(plan_ctx));
  } else if (is_mysql_mode()) {
    /* do nothing */
  } else {
    ObSEArray<DmlStmtScope, 4> scopes;
    ObSEArray<ObRawExpr*, 4> replace_exprs;
    ObSEArray<ObRawExpr*, 4> old_exprs;
    ObSEArray<ObRawExpr*, 4> new_exprs;
    ObSEArray<ObExprConstraint, 4> constraints;
    FastRelExprChecker expr_checker(replace_exprs);
    if (OB_FAIL(scopes.push_back(SCOPE_JOINED_TABLE)) ||
        OB_FAIL(scopes.push_back(SCOPE_SEMI_INFO)) ||
        OB_FAIL(scopes.push_back(SCOPE_WHERE)) ||
        OB_FAIL(scopes.push_back(SCOPE_HAVING))) {
      LOG_WARN("Fail to create scope array.", K(ret));
    }
    ObStmtExprGetter visitor(scopes);
    visitor.checker_ = &expr_checker;
    if (OB_FAIL(stmt->iterate_stmt_expr(visitor))) {
      LOG_WARN("get relation exprs failed", K(ret));
    }
    //collect like exprs which need to be replaced
    for (int64_t i = 0; OB_SUCC(ret) && i < replace_exprs.count(); i++) {
      if (OB_ISNULL(replace_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replace expr is null", K(ret));
      } else if (OB_FAIL(SMART_CALL(check_like_condition(replace_exprs.at(i),
                                                         old_exprs,
                                                         new_exprs,
                                                         constraints)))) {
        LOG_WARN("failed to check like condition", K(ret));
      } else {
        //do nothing
      }
    }
    //do replace and add constraints
    if (OB_SUCC(ret) && !old_exprs.empty()) {
      ObStmtExprReplacer replacer(scopes);
      replacer.set_recursive(false);
      if (OB_FAIL(replacer.add_replace_exprs(old_exprs, new_exprs))) {
        LOG_WARN("failed to add replace exprs", K(ret));
      } else if (OB_FAIL(stmt->iterate_stmt_expr(replacer))) {
        LOG_WARN("failed to iterate stmt expr", K(ret));
      } else if (OB_FAIL(append(ctx_->expr_constraints_, constraints))) {
        LOG_WARN("failed to add pre calc constraints", K(ret));
      } else {
        trans_happened = true;
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::convert_preds_vector_to_scalar(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> new_cond;
  bool is_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL stmt", K(ret));
  } else if (!stmt->is_sel_del_upd()) {
    //do nothing
  } else {
    /// 需要转换的谓词有: where, join condition.
    /// having 中能下降的都下降过了.
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); i++) {
      if (OB_FAIL(ObTransformUtils::convert_preds_vector_to_scalar(*ctx_,
                                             stmt->get_condition_expr(i), new_cond, is_happened))) {
        LOG_WARN("failed to convert predicate vector to scalar", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_happened) {
      trans_happened = true;
      stmt->get_condition_exprs().reset();
      if (OB_FAIL(append(stmt->get_condition_exprs(), new_cond))) {
        LOG_WARN("failed to append join conditions", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_joined_tables().count(); i++) {
        if (OB_FAIL(recursively_convert_join_preds_vector_to_scalar(stmt->get_joined_tables().at(i),
                                                                    trans_happened))) {
          LOG_WARN("failed to convert join vector condition to scalar", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::recursively_convert_join_preds_vector_to_scalar(TableItem *table_item,
                                                                             bool &trans_happened)
{
  int ret = OB_SUCCESS;
  JoinedTable *joined_table = NULL;
  if (OB_ISNULL(table_item) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table item", K(ret));
  } else if (!table_item->is_joined_table()) {
  } else if (FALSE_IT(joined_table = reinterpret_cast<JoinedTable*>(table_item))){
  } else if (OB_FAIL(recursively_convert_join_preds_vector_to_scalar(joined_table->left_table_,
                                                                     trans_happened))) {
    LOG_WARN("failed to convert join preds_vector to scalar", K(ret));
  } else if (OB_FAIL(recursively_convert_join_preds_vector_to_scalar(joined_table->right_table_,
                                                                     trans_happened))) {
    LOG_WARN("failed to convert join preds_vector to scalar", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 16> new_join_cond;
    bool is_happened = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->get_join_conditions().count(); i++) {
      if (OB_FAIL(ObTransformUtils::convert_preds_vector_to_scalar(*ctx_,
                                                          joined_table->get_join_conditions().at(i),
                                                          new_join_cond,
                                                          is_happened))) {
        LOG_WARN("failed to convert predicate vector to scalar", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_happened) {
      trans_happened = true;
      joined_table->get_join_conditions().reset();
      if (OB_FAIL(append(joined_table->get_join_conditions(), new_join_cond))) {
        LOG_WARN("failed to append join conditions", K(ret));
      }
    }
  }
  return ret;
}

/* check if like condition need to be replaced and create new equal exprs */
int ObTransformSimplifyExpr::check_like_condition(ObRawExpr *&expr,
                                                  ObIArray<ObRawExpr *> &old_exprs,
                                                  ObIArray<ObRawExpr *> &new_exprs,
                                                  ObIArray<ObExprConstraint> &constraints) {
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  bool is_replaced = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(ret), K(expr));
  } else if (ObOptimizerUtil::find_item(old_exprs, expr)) {
    //skip replaced exprs
    is_replaced = true;
  } else {
    // do transformation for child exprs first
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_ISNULL(expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(SMART_CALL(check_like_condition(expr->get_param_expr(i), old_exprs, new_exprs, constraints)))) {
        LOG_WARN("failed to replace like expr", K(ret));
      } else {
        //do nothing
      }
    }
  }

  if (OB_SUCC(ret) && T_OP_LIKE == expr->get_expr_type() && !is_replaced) {
    if (OB_FAIL(do_check_like_condition(expr, old_exprs, new_exprs, constraints))) {
      LOG_WARN("fail to do replace like condition", K(ret));
    } else { /*do nothing*/ }
  } else { /*do nothing*/ }
  return ret;
}

int ObTransformSimplifyExpr::do_check_like_condition(ObRawExpr *&expr,
                                                     ObIArray<ObRawExpr *> &old_exprs,
                                                     ObIArray<ObRawExpr *> &new_exprs,
                                                     ObIArray<ObExprConstraint> &constraints) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(expr), K(ctx_), K(ctx_->session_info_));
  } else if (T_OP_LIKE == expr->get_expr_type()) {
    ObRawExpr *text_expr = expr->get_param_expr(0);
    ObRawExpr *pattern_expr = expr->get_param_expr(1);
    ObRawExpr *escape_expr = expr->get_param_expr(2);
    bool can_replace = true;
    if (OB_ISNULL(text_expr) || OB_ISNULL(pattern_expr) || OB_ISNULL(escape_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(text_expr), K(pattern_expr), K(escape_expr));
    } else if (!pattern_expr->is_static_const_expr()) {
      /* not calculable*/
      can_replace = false;
    } else if (T_VARCHAR != escape_expr->get_expr_type()) {
      /* has escape node in oracle mode */
      can_replace = false;
    } else if (pattern_expr->get_result_type().is_lob()) {
      can_replace = false;
    } else {
      //check if has wildcard or lob column
      bool got_result = false;
      ObObj result;
      if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_->exec_ctx_,
                                                          pattern_expr,
                                                          result,
                                                          got_result,
                                                          *ctx_->allocator_))) {
        LOG_WARN("failed to calc const or calculable expr", K(ret));
      } else if (got_result) {
        //can't replace if pattern have wildcard or default escape character
        ObString val = result.get_string();
        can_replace = OB_ISNULL(val.find('_'))
                      && OB_ISNULL(val.find('%'))
                      && OB_ISNULL(val.find('\\'));
      } else {}
    }
    if (OB_SUCC(ret) && can_replace) {
      ObRawExpr *eq_expr = NULL;
      if ((ObCharType == pattern_expr->get_result_type().get_type() ||
           ObNCharType == pattern_expr->get_result_type().get_type())) {
        //for cases pattern has been cast to nchar or char
        ObRawExpr *new_pattern_expr = pattern_expr;
        bool need_cast = false;
        if (T_FUN_SYS_CAST == pattern_expr->get_expr_type()) {
          new_pattern_expr = pattern_expr->get_param_expr(0);
          if (OB_ISNULL(new_pattern_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", K(ret));
          } else if (ObVarcharType == new_pattern_expr->get_result_type().get_type()) {
            pattern_expr = new_pattern_expr;
          } else {
            need_cast = true;
          }
        } else {
          need_cast = true;
        }
        if (OB_SUCC(ret) && need_cast) {
          ObSysFunRawExpr *cast_expr = NULL;
          ObExprResType cast_type(pattern_expr->get_result_type());
          cast_type.set_calc_type(ObVarcharType);
          cast_type.set_type(ObVarcharType);
          if (OB_FAIL(ObRawExprUtils::create_cast_expr(*ctx_->expr_factory_, pattern_expr, cast_type, cast_expr, ctx_->session_info_))) {
            LOG_WARN("fail to create cast expr", K(ret));
          } else if (OB_ISNULL(cast_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", K(ret), K(cast_expr));
          } else {
            pattern_expr = cast_expr;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (text_expr->get_expr_type() == T_FUN_SYS_CAST &&
            OB_FAIL(text_expr->clear_flag(IS_INNER_ADDED_EXPR))) {  //avoid reconstuct sql is wrong.
          LOG_WARN("failed to clear flag", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(*ctx_->expr_factory_,
                                                              ctx_->session_info_,
                                                              pattern_expr,
                                                              text_expr,
                                                              eq_expr))) {
          LOG_WARN("create equal expr failed", K(ret), K(pattern_expr));
        } else if (OB_ISNULL(eq_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(eq_expr));
        } else if (OB_FAIL(eq_expr->add_relation_ids(expr->get_relation_ids()))) {
          LOG_WARN("failed to set relation ids", K(ret));
        } else if (OB_FAIL(old_exprs.push_back(expr))) {
          LOG_WARN("fail to add expr to old exprs", K(ret));
        } else if (OB_FAIL(new_exprs.push_back(eq_expr))) {
          LOG_WARN("fail to new expr to old exprs", K(ret));
        } else if (OB_FAIL(constraints.push_back(ObExprConstraint(pattern_expr,PRE_CALC_RESULT_NO_WILDCARD)))) {
          LOG_WARN("fail to add no wildcard constraint", K(ret));
        }
      }
    } else {}
  }
  return ret;
}

/**
 * Remove dummy filters
 * 1 false or filter -> filter
 * 2 true or filter -> true
 * 3 false and filter -> false
 * 4 true and filter -> filter
 */
int ObTransformSimplifyExpr::remove_dummy_exprs(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSEArray<ObExprConstraint, 4> constraints;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(ret), K(ctx_));
  } else if (OB_FAIL(remove_dummy_case_when(stmt, trans_happened))) {
    LOG_WARN("failed to remvoe dummy case when", K(ret));
  } else if (OB_FAIL(remove_dummy_nvl(stmt, trans_happened))) {
    LOG_WARN("failed to remove dummy nvl", K(ret));
  } else if (OB_FAIL(remove_dummy_filter_exprs(stmt->get_condition_exprs(), constraints))) {
    LOG_WARN("failed to post process filter exprs", K(ret));
  } else if (stmt->is_select_stmt() && !static_cast<ObSelectStmt*>(stmt)->is_scala_group_by() &&
             OB_FAIL(remove_dummy_filter_exprs(static_cast<ObSelectStmt*>(stmt)->get_having_exprs(),
                                               constraints))) {
    LOG_WARN("failed to post process filter exprs", K(ret));
  } else {
    ObIArray<JoinedTable*> &joined_table = stmt->get_joined_tables();
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table.count(); i++) {
      if (OB_FAIL(remove_dummy_join_condition_exprs(joined_table.at(i), constraints))) {
        LOG_WARN("failed to post process join condition exprs", K(ret));
      }
    }
    if (OB_SUCC(ret) && !constraints.empty()) {
      if (OB_FAIL(append(ctx_->expr_constraints_, constraints))) {
        LOG_WARN("failed to add pre calc constraints", K(ret));
      } else {
        trans_happened = true;
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::inner_remove_dummy_expr(common::ObIArray<ObRawExpr*> &exprs,
                                                     ObIArray<ObExprConstraint> &constraints)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    ObRawExpr *tmp = NULL;
    if (OB_ISNULL(tmp = exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else if (OB_FAIL(inner_remove_dummy_expr(tmp, constraints))) {
      LOG_WARN("failed to remove dummy filter", K(ret));
    } else if (OB_ISNULL(tmp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else {
      exprs.at(i) = tmp;
    }
  }
  return ret;
}

// when add pre calc constrains, cluster and create const predicate and create pre calc frame
//  1. and_op(1 = 1, c1 = 1, 2 = 1) create pre calc frame use and_op(1 = 1, 2 = 1) expect false;
//  2. or_op(1 = 1, c1 = 1, 2 = 1) create pre calc frame use and_op(1 = 1, 2 = 1) expect false;
int ObTransformSimplifyExpr::remove_dummy_filter_exprs(common::ObIArray<ObRawExpr*> &exprs,
                                                       ObIArray<ObExprConstraint> &constraints)
{
  int ret = OB_SUCCESS;
  bool is_valid_type = true;
  if (OB_FAIL(ObTransformUtils::check_integer_result_type(exprs, is_valid_type))) {
    LOG_WARN("check valid type fail", K(ret));
  } else if (!is_valid_type) {
    LOG_TRACE("expr list is not valid for removing dummy exprs", K(is_valid_type));
  } else if (OB_FAIL(inner_remove_dummy_expr(exprs, constraints))) {
    LOG_WARN("fail to remove dummy expr in exprs", K(ret));
  } else if (exprs.empty()) {
    /* do nothing */
  } else {
    ObSEArray<int64_t, 2> true_exprs;
    ObSEArray<int64_t, 2> false_exprs;
    ObRawExpr *transed_expr = NULL;
    if (OB_FAIL(ObTransformUtils::extract_const_bool_expr_info(ctx_,
                                                               exprs,
                                                               true_exprs,
                                                               false_exprs))) {
      LOG_WARN("failed to extract exprs info", K(ret));
    } else if (true_exprs.empty() && false_exprs.empty()) {
      /* do nothing */
    } else if (1 == exprs.count() && 1 == false_exprs.count()
               && exprs.at(false_exprs.at(0))->is_const_raw_expr()) {
      /* do nothing */
      /* exprs has only false condition, do not adjust */
    } else if (OB_FAIL(adjust_dummy_expr(true_exprs, false_exprs, true, exprs, transed_expr, constraints))) {
      LOG_WARN("failed to adjust dummy exprs", K(ret));
    } else if (transed_expr != NULL) {
      exprs.reset();
      bool is_true = false;
      if (!transed_expr->is_const_raw_expr()) {
        if (OB_FAIL(exprs.push_back(transed_expr))) {
          LOG_WARN("push back failed", K(ret));
        }
      } else if (OB_FAIL(ObObjEvaluator::is_true(
                           static_cast<ObConstRawExpr*>(transed_expr)->get_value(), is_true))) {
        LOG_WARN("failed to call is true", K(ret));
      } else if (is_true) {
      } else if (OB_FAIL(exprs.push_back(transed_expr))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::remove_dummy_join_condition_exprs(TableItem *table,
                                                               ObIArray<ObExprConstraint> &constraints)
{
  int ret = OB_SUCCESS;
  JoinedTable *join_table = NULL;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table));
  } else if (!table->is_joined_table()) {
    /*do nothing*/
  } else if (OB_ISNULL(join_table = static_cast<JoinedTable*>(table)) ||
             OB_ISNULL(join_table->left_table_) || OB_ISNULL(join_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(join_table));
  } else if (OB_FAIL(remove_dummy_filter_exprs(join_table->join_conditions_, constraints))) {
    LOG_WARN("failed to remove dummy exprs", K(ret));
  } else if (OB_FAIL(SMART_CALL(remove_dummy_join_condition_exprs(join_table->left_table_,
                                                                  constraints)))) {
    LOG_WARN ("failed to remove dummy join conditions", K(ret));
  } else if (OB_FAIL(SMART_CALL(remove_dummy_join_condition_exprs(join_table->right_table_,
                                                                  constraints)))) {
    LOG_WARN("failed to remove dummy join conditions", K(ret));
  }
  return ret;
}

int ObTransformSimplifyExpr::inner_remove_dummy_expr(ObRawExpr *&expr,
                                                     ObIArray<ObExprConstraint> &constraints)
{
  int ret = OB_SUCCESS;
  bool is_valid_type = false;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx_), K(ret));
  } else if (OB_FAIL(is_valid_transform_type(expr, is_valid_type))) {
    LOG_WARN("failed to check is valid transform type", K(ret));
  } else if (!is_valid_type) {
    LOG_TRACE("expr is not valid for removing dummy exprs", K(*expr));
  } else {
    // remove dummy filter for children exprs first
    LOG_TRACE("expr is valid for removing dummy exprs", K(*expr));
    ObOpRawExpr *op_expr = static_cast<ObOpRawExpr*>(expr);
    ObRawExpr *transed_expr = NULL;
    const int64_t old_cons_count = constraints.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < op_expr->get_param_count(); i++) {
      ObRawExpr *temp = NULL;
      if (OB_ISNULL(temp = op_expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(SMART_CALL(inner_remove_dummy_expr(temp, constraints)))) {
        LOG_WARN("failed to remove dummy filter", K(ret));
      } else if (OB_ISNULL(temp)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else {
        op_expr->get_param_expr(i) = temp;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(op_expr->get_param_count() <= 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should have at least one param", K(op_expr->get_param_count()), K(ret));
    } else if (op_expr->get_param_count() > 1) {
      ObSEArray<int64_t, 2> true_exprs;
      ObSEArray<int64_t, 2> false_exprs;
      ObRawExpr *bool_expr = NULL;
      if (OB_FAIL(ObTransformUtils::extract_const_bool_expr_info(ctx_,
                                                                 op_expr->get_param_exprs(),
                                                                 true_exprs,
                                                                 false_exprs))) {
        LOG_WARN("failed to extract filters info", K(ret));
      } else if (true_exprs.empty() && false_exprs.empty()) {
        /*do nothing*/
      } else if (OB_FAIL(adjust_dummy_expr(true_exprs, false_exprs,
                                           T_OP_AND == expr->get_expr_type(),
                                           op_expr->get_param_exprs(),
                                           transed_expr,
                                           constraints))) {
        LOG_WARN("failed to adjust dummy expr", K(ret));
      } else if (transed_expr != NULL) {
        expr = transed_expr;
      }
    }
    if (OB_SUCC(ret) && old_cons_count != constraints.count()) {
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(expr));
      } else if (OB_FAIL(expr->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::is_valid_transform_type(ObRawExpr *expr,
                                                     bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(expr), K(ret));
  } else if (T_OP_AND == expr->get_expr_type() ||
             T_OP_OR == expr->get_expr_type()) {
    is_valid = expr->get_result_type().is_integer_type();
    for (int64_t i = 0;
         OB_SUCC(ret) && is_valid && i < expr->get_param_count(); i++) {
      ObRawExpr *temp = NULL;
      if (OB_ISNULL(temp = expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else {
        is_valid &= temp->get_result_type().is_integer_type();
      }
    }
  } else { /*do nothing*/ }
  return ret;
}

int ObTransformSimplifyExpr::adjust_dummy_expr(const ObIArray<int64_t> &true_exprs,
                                               const ObIArray<int64_t> &false_exprs,
                                               const bool is_and_op,
                                               ObIArray<ObRawExpr *> &adjust_exprs,
                                               ObRawExpr *&transed_expr,
                                               ObIArray<ObExprConstraint> &constraints)
{
  int ret = OB_SUCCESS;
  transed_expr = NULL;
  const bool remove_all = is_and_op ? !false_exprs.empty() : !true_exprs.empty();
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx_), K(ret));
  } else {
    ObRawExpr *op_expr = NULL;
    ObSEArray<ObRawExpr*, 4> op_params;
    const PreCalcExprExpectResult expect_result = ((is_and_op && !false_exprs.empty())
                                                   || (!is_and_op && true_exprs.empty()))
        ? PreCalcExprExpectResult::PRE_CALC_RESULT_FALSE
        : PreCalcExprExpectResult::PRE_CALC_RESULT_TRUE;
    if (OB_FAIL(ObTransformUtils::extract_target_exprs_by_idx(adjust_exprs, true_exprs, op_params))) {
      LOG_WARN("failed to extract target exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_target_exprs_by_idx(adjust_exprs, false_exprs, op_params))) {
      LOG_WARN("failed to extract target exprs", K(ret));
    } else if (remove_all) {
      //to keep the or/and expr contains at least 2 params.
      const bool b_value = is_and_op ? false : true;
      if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(ctx_->expr_factory_, transed_expr, b_value))) {
        LOG_WARN("create const bool expr failed", K(ret));
      }
    } else if (adjust_exprs.count() - op_params.count() >=2) {
      if (OB_FAIL(ObOptimizerUtil::remove_item(adjust_exprs, op_params))) {
        LOG_WARN("failed to remove exprs", K(ret));
      }
    } else if (adjust_exprs.count() - op_params.count() == 1) {
      for (int64_t i = 0; OB_SUCC(ret) && i < adjust_exprs.count(); i++) {
        if (!ObOptimizerUtil::find_item(op_params, adjust_exprs.at(i))) {
          transed_expr = adjust_exprs.at(i);
          break;
        }
      }
    } else if (adjust_exprs.count() - op_params.count() == 0) {
      const bool b_value = is_and_op ? false_exprs.empty() : !true_exprs.empty();
      if (b_value != true) {
      } else if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(ctx_->expr_factory_, transed_expr, b_value))) {
        LOG_WARN("create const bool expr failed", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_and_op && OB_FAIL(ObRawExprUtils::build_and_expr(*ctx_->expr_factory_,
                                                                   op_params, op_expr))) {
      LOG_WARN("failed to build and expr", K(ret));
    } else if (!is_and_op && OB_FAIL(ObRawExprUtils::build_or_exprs(*ctx_->expr_factory_,
                                                                    op_params, op_expr))) {
      LOG_WARN("failed to build or expr", K(ret));
    } else if (OB_ISNULL(op_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(op_expr));
    } else if (OB_FAIL(op_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(constraints.push_back(ObExprConstraint(op_expr, expect_result)))) {
      LOG_WARN("failed to push back constraint", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}


int ObTransformSimplifyExpr::remove_dummy_case_when(ObDMLStmt *stmt,
                                                    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSEArray<ObRawExpr *, 8> relation_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else {
    ObStmtExprGetter visitor;
    visitor.set_relation_scope();
    if (stmt->is_select_stmt() && static_cast<ObSelectStmt*>(stmt)->is_scala_group_by()) {
      visitor.remove_scope(SCOPE_SELECT);
      visitor.remove_scope(SCOPE_HAVING);
    }
    if (OB_FAIL(stmt->get_relation_exprs(relation_exprs, visitor))) {
      LOG_WARN("failed to get relation exprs", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
      ObRawExpr *expr = relation_exprs.at(i);
      if (OB_FAIL(remove_dummy_case_when(stmt->get_query_ctx(),
                                         expr,
                                         trans_happened))) {
        LOG_WARN("failed to remove dummy case when", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::remove_dummy_case_when(ObQueryCtx* query_ctx,
                                                    ObRawExpr *expr,
                                                    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(ret));
  } else if (T_OP_CASE == expr->get_expr_type()) {
    ObCaseOpRawExpr *case_expr = static_cast<ObCaseOpRawExpr *>(expr);
    if (OB_FAIL(inner_remove_dummy_case_when(query_ctx,
                                             case_expr,
                                             trans_happened))) {
      LOG_WARN("failed to remove dummy case when", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    if (OB_FAIL(SMART_CALL(remove_dummy_case_when(query_ctx,
                                                  expr->get_param_expr(i),
                                                  trans_happened)))) {
      LOG_WARN("failed to remove dummy case when", K(ret));
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::inner_remove_dummy_case_when(ObQueryCtx* query_ctx,
                                                          ObCaseOpRawExpr *case_expr,
                                                          bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObStmtCompareContext context;
  if (OB_ISNULL(case_expr) || OB_ISNULL(ctx_) || OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(query_ctx), K(ctx_), K(case_expr), K(ret));
  } else if (case_expr->get_when_expr_size() != case_expr->get_then_expr_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("incorrect case when expr", K(*case_expr), K(ret));
  } else {
    context.init(&query_ctx->calculable_items_);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < case_expr->get_when_expr_size(); ++i) {
    ObRawExpr *when = case_expr->get_when_param_expr(i);
    ObRawExpr *then = case_expr->get_then_param_expr(i);
    ObCaseOpRawExpr *child_case_expr = NULL;
    ObRawExpr *child_when = NULL;
    context.equal_param_info_.reset();
    if (OB_ISNULL(when) || OB_ISNULL(then)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null case when expr", K(when), K(then), K(ret));
    } else if (T_OP_CASE != then->get_expr_type()) {
      //do nothing
    } else if (OB_FALSE_IT(child_case_expr = static_cast<ObCaseOpRawExpr*>(then))) {
    } else if (child_case_expr->get_when_expr_size() <= 0) {
      //do nothing
    } else if (OB_ISNULL(child_when = child_case_expr->get_when_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null case when expr", K(ret));
    } else if (when->same_as(*child_when, &context)) {
      ObRawExpr *child_then = child_case_expr->get_then_param_expr(0);
      if (OB_FAIL(case_expr->replace_then_param_expr(i, child_then))) {
        LOG_WARN("failed to replace then param expr", K(ret));
      } else if (OB_FAIL(append(ctx_->equal_param_constraints_, context.equal_param_info_))) {
        LOG_WARN("append equal param info failed", K(ret));
      } else {
        trans_happened = true;
      }
    }
  }
  return ret;
}

// Rewrite NVL(exp1, exp2)(or IFNULL):
//   1) IF exp1 is null, NVL(exp1, exp2) -> exp2
//   2) IF exp1 is not null, NVL(exp1, exp2) -> exp1
int ObTransformSimplifyExpr::remove_dummy_nvl(ObDMLStmt *stmt,
                                              bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(stmt));
  } else if (stmt->is_sel_del_upd()) {
    ObNotNullContext not_null_ctx(*ctx_, stmt);
    ObSEArray<ObRawExpr*, 4> ignore_exprs;
    if (OB_FAIL(not_null_ctx.generate_stmt_context(NULLABLE_SCOPE::NS_WHERE))){
      LOG_WARN("failed to generate not null context", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); ++i) {
      if (OB_FAIL(not_null_ctx.remove_filter(stmt->get_condition_exprs().at(i)))){
        LOG_WARN("failed to remove filter", K(ret));
      } else if (OB_FAIL(inner_remove_dummy_nvl(stmt,
                                                stmt->get_condition_exprs().at(i),
                                                not_null_ctx,
                                                ignore_exprs,
                                                trans_happened))) {
        LOG_WARN("failed to remove dummy nvl", K(ret));
      } else if (OB_FAIL(not_null_ctx.add_filter(stmt->get_condition_exprs().at(i)))) {
        LOG_WARN("failed to add filter", K(ret));
      }
    }

    /**
     * Do not simplify expr in rollup.
     * e.g.
     *     select nvl(NULL, a), a from t group by rollup(a, nvl(NULL, a));
     *  != select a, a from t group by rollup(a, a);
    */
    if (OB_SUCC(ret) && stmt->is_select_stmt()) {
      ObSelectStmt *sel_stmt = static_cast<ObSelectStmt *>(stmt);
      if (sel_stmt->has_rollup()) {
        if (OB_FAIL(append(ignore_exprs, sel_stmt->get_group_exprs()))) {
          LOG_WARN("failed to append exprs", K(ret));
        } else if (OB_FAIL(append(ignore_exprs, sel_stmt->get_rollup_exprs()))) {
          LOG_WARN("failed to append exprs", K(ret));
        }
      } else {
        not_null_ctx.reset();
        if (OB_FAIL(not_null_ctx.generate_stmt_context(NULLABLE_SCOPE::NS_GROUPBY))){
          LOG_WARN("failed to generate not null context", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt->get_group_expr_size(); ++i) {
          if (OB_FAIL(inner_remove_dummy_nvl(stmt,
                                            sel_stmt->get_group_exprs().at(i),
                                            not_null_ctx,
                                            ignore_exprs,
                                            trans_happened))) {
            LOG_WARN("failed to remove dummy nvl", K(ret));
          }
        }
      }
    }

    not_null_ctx.reset();
    if (OB_FAIL(not_null_ctx.generate_stmt_context(NULLABLE_SCOPE::NS_TOP))){
      LOG_WARN("failed to generate not null context", K(ret));
    }
    if (OB_SUCC(ret) && stmt->is_select_stmt() && !static_cast<ObSelectStmt *>(stmt)->is_scala_group_by()) {
      ObSelectStmt *sel_stmt = static_cast<ObSelectStmt *>(stmt);
      for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt->get_having_expr_size(); ++i) {
        if (OB_FAIL(not_null_ctx.remove_having_filter(sel_stmt->get_having_exprs().at(i)))){
          LOG_WARN("failed to remove filter", K(ret));
        } else if (OB_FAIL(inner_remove_dummy_nvl(stmt,
                                                  sel_stmt->get_having_exprs().at(i),
                                                  not_null_ctx,
                                                  ignore_exprs,
                                                  trans_happened))) {
          LOG_WARN("failed to remove dummy nvl", K(ret));
        } else if (OB_FAIL(not_null_ctx.add_having_filter(sel_stmt->get_having_exprs().at(i)))) {
          LOG_WARN("failed to add filter", K(ret));
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt->get_select_item_size(); ++i) {
        if (OB_FAIL(inner_remove_dummy_nvl(stmt,
                                           sel_stmt->get_select_item(i).expr_,
                                           not_null_ctx,
                                           ignore_exprs,
                                           trans_happened))) {
          LOG_WARN("failed to remove dummy nvl", K(ret));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_order_item_size(); ++i){
      if (OB_FAIL(inner_remove_dummy_nvl(stmt,
                                         stmt->get_order_item(i).expr_,
                                         not_null_ctx,
                                         ignore_exprs,
                                         trans_happened))) {
        LOG_WARN("failed to remove dummy nvl", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::inner_remove_dummy_nvl(ObDMLStmt *stmt,
                                                    ObRawExpr *&expr,
                                                    ObNotNullContext &not_null_ctx,
                                                    ObIArray<ObRawExpr *> &ignore_exprs,
                                                    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(expr));
  }
  // do transformation for child exprs first
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    if (OB_FAIL(SMART_CALL(inner_remove_dummy_nvl(stmt,
                                                  expr->get_param_expr(i),
                                                  not_null_ctx,
                                                  ignore_exprs,
                                                  trans_happened)))){
      LOG_WARN("failed to remove dummy nvl", K(ret));
    }
  }

  if (OB_SUCC(ret)
      && (T_FUN_SYS_NVL == expr->get_expr_type()
      || T_FUN_SYS_IFNULL == expr->get_expr_type())) {
    if (ObOptimizerUtil::find_item(ignore_exprs, expr)) {
      // nvl expr is rollup expr, do nothing
    } else if (OB_FAIL(do_remove_dummy_nvl(stmt,
                                    expr,
                                    not_null_ctx,
                                    trans_happened))){
      LOG_WARN("failed to remove dummy nvl", K(ret));
    }
  }

  return ret;
}

int ObTransformSimplifyExpr::do_remove_dummy_nvl(ObDMLStmt *stmt,
                                                 ObRawExpr *&expr,
                                                 ObNotNullContext &not_null_ctx,
                                                 bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt)
      || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(expr), K(ret));
  } else if (T_FUN_SYS_NVL == expr->get_expr_type() ||
             T_FUN_SYS_IFNULL == expr->get_expr_type()) {
    const ObOpRawExpr *op_expr = static_cast<ObOpRawExpr *>(expr);
    if (OB_UNLIKELY(op_expr->get_param_count() != 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected param numm", K(op_expr->get_param_count()), K(ret));
    } else {
      ObRawExpr *child_0 = op_expr->get_param_exprs().at(0);
      ObRawExpr *child_1 = op_expr->get_param_exprs().at(1);
      if (OB_ISNULL(child_0) || OB_ISNULL(child_1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpecte null", K(child_0), K(child_1), K(ret));
      } else {
        bool not_null = false;
        ObRawExpr *new_expr = NULL;
        ObArray<ObRawExpr *> not_null_constraints;
        if (OB_FAIL(ObTransformUtils::is_expr_not_null(not_null_ctx,
                                                       child_0,
                                                       not_null,
                                                       &not_null_constraints))) {
          LOG_WARN("failed to check expr not null", K(ret));
        } else if (not_null){
          // NVL(child_0, child_1) -> child_0  IF child_0 is not null
          if (OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, not_null_constraints))) {
            LOG_WARN("failed to add param not null constraint", K(ret));
          } else {
            new_expr = child_0;
          }
        } else if (child_0->is_static_const_expr()) {
          ObObj result;
          bool got_result = false;
          if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_->exec_ctx_,
                                                                child_0,
                                                                result,
                                                                got_result,
                                                                *ctx_->allocator_))) {
            LOG_WARN("failed to calc const or caculable expr", K(ret));
          } else if (got_result && (result.is_null()
                     || (lib::is_oracle_mode() && result.is_null_oracle()))) {
            // NVL(NULL, child_1) -> child_1
            ObExprConstraint expr_cons(child_0, PreCalcExprExpectResult::PRE_CALC_RESULT_NULL);
            if (OB_FAIL(ctx_->expr_constraints_.push_back(expr_cons))) {
              LOG_WARN("failed to push back constraint", K(ret));
            } else {
              new_expr = child_1;
            }
          }
        }
        if (OB_SUCC(ret) && new_expr != NULL) {
          if (OB_FAIL(ObTransformUtils::add_cast_for_replace_if_need(
                              *ctx_->expr_factory_, expr, new_expr, ctx_->session_info_))) {
            LOG_WARN("failed to add cast for replace", K(ret));
          } else if (OB_ISNULL(new_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null cast expr", K(ret));
          } else {
            expr = new_expr;
            trans_happened = true;
          }
        }
      }
    }
  }
  return ret;
}

// Rewrite NVL(exp1, exp2) ~ exp3 at the root of where condition or having condition:
//   IF exp2 ~ exp3 is true,
//      NVL(exp1, exp2) ~ exp3  =>  exp1 ~ exp3 or exp1 is null
//   If exp2 ~ exp3 is false,
//      NVL(exp1, exp2) ~ exp3  =>  exp1 ~ exp3
int ObTransformSimplifyExpr::convert_nvl_predicate(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(stmt));
  } else if (stmt->is_sel_del_upd()) {
    ObSEArray<ObRawExpr*, 4> ignore_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); ++i) {
      if (OB_FAIL(inner_convert_nvl_predicate(
                    stmt, stmt->get_condition_exprs().at(i), ignore_exprs, is_happened))) {
        LOG_WARN("failed to replace is null expr", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
    if (OB_SUCC(ret) && stmt->is_select_stmt()) {
      ObSelectStmt *sel_stmt = static_cast<ObSelectStmt *>(stmt);
      if (sel_stmt->has_rollup()) {
        if (OB_FAIL(append(ignore_exprs, sel_stmt->get_group_exprs()))) {
          LOG_WARN("failed to append exprs", K(ret));
        } else if (OB_FAIL(append(ignore_exprs, sel_stmt->get_rollup_exprs()))) {
          LOG_WARN("failed to append exprs", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt->get_having_expr_size(); ++i) {
        if (OB_FAIL(inner_convert_nvl_predicate(sel_stmt,
                                                sel_stmt->get_having_exprs().at(i),
                                                ignore_exprs,
                                                is_happened))) {
          LOG_WARN("failed to replace is null expr", K(ret));
        } else {
          trans_happened |= is_happened;
        }
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::inner_convert_nvl_predicate(ObDMLStmt *stmt,
                                                         ObRawExpr *&expr,
                                                         ObIArray<ObRawExpr *> &ignore_exprs,
                                                         bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  if (OB_ISNULL(expr)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(expr), K(ret));
  } else if (IS_COMMON_COMPARISON_OP(expr->get_expr_type())) {
    ObOpRawExpr *op_expr = static_cast<ObOpRawExpr*>(expr);
    ObRawExpr *child_0 = NULL, *child_1 = NULL;
    if (OB_UNLIKELY(op_expr->get_param_count() != 2)){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected param count", K(op_expr->get_param_count()), K(ret));
    } else if (OB_ISNULL(child_0 = op_expr->get_param_expr(0))
               || OB_ISNULL(child_1 = op_expr->get_param_expr(1))){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(child_0), K(child_1), K(ret));
    } else {
      bool is_nvl_at_left = false;
      bool is_nvl_cmp_const = false;
      ObRawExpr *nvl_expr = NULL;
      ObRawExpr *sibling_expr = NULL;
      if ((T_FUN_SYS_NVL == child_0->get_expr_type()
           || T_FUN_SYS_IFNULL == child_0->get_expr_type())
          && child_1->is_static_const_expr()){
        is_nvl_at_left = true;
        is_nvl_cmp_const = true;
        nvl_expr = child_0;
        sibling_expr = child_1;
      } else if ((T_FUN_SYS_NVL == child_1->get_expr_type()
                  || T_FUN_SYS_IFNULL == child_1->get_expr_type())
                 && child_0->is_static_const_expr()){
        is_nvl_at_left = false;
        is_nvl_cmp_const = true;
        nvl_expr = child_1;
        sibling_expr = child_0;
      }
      if (OB_SUCC(ret) && is_nvl_cmp_const){
        if (ObOptimizerUtil::find_item(ignore_exprs, expr)) {
          // nvl pred is rollup expr, do nothing
        } else if (OB_FAIL(do_convert_nvl_predicate(stmt,
                                             expr,
                                             nvl_expr,
                                             sibling_expr,
                                             is_nvl_at_left,
                                             is_happened))){
          LOG_WARN("failed to convert nvl predicate", K(ret));
        } else {
          trans_happened |= is_happened;
        }
      }
    }
  }

  if (OB_SUCC(ret)&& trans_happened){
    if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ctx_), K(ret));
    } else if (OB_FAIL(expr->formalize(ctx_->session_info_))){
      LOG_WARN("failed to formalize expr", K(ret));
    }
  }

  return ret;
}

// parent_expr should be at the root of where condition or having condition
// IF nvl_at_left is true, parent_expr := NVL(exp1, exp2) cmp exp3
// IF nvl_at_left is false, parent_expr := exp3 cmp NVL(exp1, exp2)
int ObTransformSimplifyExpr::do_convert_nvl_predicate(ObDMLStmt *stmt,
                                                      ObRawExpr *&parent_expr,
                                                      ObRawExpr *&nvl_expr,
                                                      ObRawExpr *&sibling_expr,
                                                      bool nvl_at_left,
                                                      bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(parent_expr) || OB_ISNULL(nvl_expr) || OB_ISNULL(sibling_expr)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(parent_expr), K(nvl_expr), K(sibling_expr));
  } else {
    ObOpRawExpr *op_nvl_expr = static_cast<ObOpRawExpr*>(nvl_expr);
    ObRawExpr *exp1 = op_nvl_expr->get_param_exprs().at(0);
    ObRawExpr *exp2 = op_nvl_expr->get_param_exprs().at(1);
    ObRawExpr *exp3 = sibling_expr;
    ObRawExpr *exp2_cmp_exp3 = NULL;

    if (OB_ISNULL(exp1) || OB_ISNULL(exp2)){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(exp1), K(exp2));
    } else if (OB_FAIL(ObTransformUtils::add_cast_for_replace_if_need(*ctx_->expr_factory_,
                                                                      nvl_expr,
                                                                      exp2,
                                                                      ctx_->session_info_))) {
      LOG_WARN("failed to add cast for replace", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*(ctx_->expr_factory_),
                                                              ctx_->session_info_,
                                                              parent_expr->get_expr_type(),
                                                              exp2_cmp_exp3,
                                                              nvl_at_left? exp2 : exp3,
                                                              nvl_at_left? exp3 : exp2))) {
      LOG_WARN("failed to build cmp expr", K(ret));
    } else if (exp2_cmp_exp3->is_static_const_expr()){
      ObObj result;
      bool got_result = false;
      if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_->exec_ctx_,
                                                            exp2_cmp_exp3,
                                                            result,
                                                            got_result,
                                                            *ctx_->allocator_))) {
        LOG_WARN("failed to calc const or caculable expr", K(ret));
      } else if (got_result && (result.is_true() || result.is_false())) {
        // IF exp2 ~ exp3 ≡ FALSE, NVL(exp1, exp2) ~ exp3 -> exp1 is not null and exp1 ~ exp3
        // IF exp2 ~ exp3 ≡ TRUE,  NVL(exp1, exp2) ~ exp3 -> exp1 is null or exp1 ~ exp3
        ObRawExpr *exp1_cmp_exp3 = NULL;
        if (OB_FAIL(ObTransformUtils::add_cast_for_replace_if_need(*ctx_->expr_factory_,
                                                                   nvl_expr,
                                                                   exp1,
                                                                   ctx_->session_info_))) {
          LOG_WARN("failed to add cast for replace", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*(ctx_->expr_factory_),
                                                          ctx_->session_info_,
                                                          parent_expr->get_expr_type(),
                                                          exp1_cmp_exp3,
                                                          nvl_at_left ? exp1 : exp3,
                                                          nvl_at_left ? exp3 : exp1))) {
          LOG_WARN("failed to build cmp expr", K(ret));
        } else if (result.is_false()){
          ObExprConstraint expr_cons(exp2_cmp_exp3, PreCalcExprExpectResult::PRE_CALC_RESULT_FALSE);
          if (OB_FAIL(ctx_->expr_constraints_.push_back(expr_cons))) {
            LOG_WARN("failed to push back constraint", K(ret));
          } else if (T_OP_NSEQ == parent_expr->get_expr_type()) {
            // only null safe eq will cause not null reject condition
            ObRawExpr* exp1_is_not_null = NULL;
            ObSEArray<ObRawExpr*, 2> op_params;
            if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(*(ctx_->expr_factory_),
                                                               exp1, /*is not null*/true,
                                                               exp1_is_not_null))) {
              LOG_WARN("failed to build is not null expr", K(ret));
            } else if (OB_FAIL(op_params.push_back(exp1_is_not_null))) {
              LOG_WARN("failed to push back param", K(ret));
            } else if (OB_FAIL(op_params.push_back(exp1_cmp_exp3))) {
              LOG_WARN("failed to push back param", K(ret));
            } else if (OB_FAIL(ObRawExprUtils::build_and_expr(*(ctx_->expr_factory_),
                                                              op_params,
                                                              parent_expr))) {
              LOG_WARN("failed to build or expr", K(ret));
            } else {
              trans_happened = true;
            }
          } else {
            // 'exp1 cmp exp3' is null reject,
            // so 'exp1 is not null' can be eliminated
            parent_expr = exp1_cmp_exp3;
            trans_happened = true;
          }
        } else if (result.is_true()){
          ObRawExpr* exp1_is_null = NULL;
          ObSEArray<ObRawExpr*, 2> op_params;
          ObExprConstraint expr_cons(exp2_cmp_exp3, PreCalcExprExpectResult::PRE_CALC_RESULT_TRUE);
          if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(*(ctx_->expr_factory_),
                                                            exp1, /*is not null*/false,
                                                            exp1_is_null))) {
            LOG_WARN("failed to build is not null expr", K(ret));
          } else if (OB_FAIL(op_params.push_back(exp1_is_null))) {
            LOG_WARN("failed to push back param", K(ret));
          } else if (OB_FAIL(op_params.push_back(exp1_cmp_exp3))) {
            LOG_WARN("failed to push back param", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::build_or_exprs(*(ctx_->expr_factory_),
                                                            op_params,
                                                            parent_expr))) {
            LOG_WARN("failed to build or expr", K(ret));
          } else if (OB_FAIL(ctx_->expr_constraints_.push_back(expr_cons))) {
            LOG_WARN("failed to push back constraint", K(ret));
          } else {
            trans_happened = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::remove_subquery_when_filter_is_false(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  ObSEArray<ObRawExprPointer, 16> relation_expr_pointers;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_expr_pointers))) {
    LOG_WARN("failed to get_relation_exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_expr_pointers.count(); ++i) {
    ObRawExpr* expr = NULL;
    if (OB_FAIL(relation_expr_pointers.at(i).get(expr))) {
      LOG_WARN("failed to get relation expr", K(ret));
    } else if (OB_FAIL(try_remove_subquery_in_expr(stmt, expr, is_happened))) {
      LOG_WARN("failed to remove subquery in expr", K(ret));
    } else {
      trans_happened |= is_happened;
      if (is_happened && OB_FAIL(relation_expr_pointers.at(i).set(expr))) {
        LOG_WARN("failed to set relation expr pointer", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) &&
      trans_happened &&
      OB_FAIL(stmt->adjust_subquery_list())) {
    LOG_WARN("failed to adjust subquery list", K(ret));
  }
  return ret;
}

int ObTransformSimplifyExpr::try_remove_subquery_in_expr(ObDMLStmt* stmt, ObRawExpr*& expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;

  if (OB_ISNULL(stmt) || OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(stmt), K(expr), K(ctx_));
  } else if (expr->is_query_ref_expr()) {
    bool is_empty = false;
    if (OB_FAIL(do_remove_subquery(stmt, expr, is_happened, is_empty))) {
      LOG_WARN("failed to do_remove_subquery_as_expr", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  } else if (IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type()) || T_OP_EXISTS == expr->get_expr_type() ||
            T_OP_NOT_EXISTS == expr->get_expr_type()) {
    ObRawExpr* param_expr_left = NULL;
    ObRawExpr* param_expr_right = NULL;
    bool left_transform_happened = false;
    bool right_transform_happened = false;
    bool is_empty_left = false;
    bool right_is_empty = false;
    if (IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type()) && expr->get_param_count() == 2 &&
        NULL != expr->get_param_expr(0) && NULL != expr->get_param_expr(1)) {
      param_expr_left = expr->get_param_expr(0);
      param_expr_right = expr->get_param_expr(1);
    } else if ((T_OP_EXISTS == expr->get_expr_type() || T_OP_NOT_EXISTS == expr->get_expr_type()) &&
               expr->get_param_count() == 1 && NULL != expr->get_param_expr(0) &&
               expr->get_param_expr(0)->is_query_ref_expr()) {
      param_expr_left = expr->get_param_expr(0);
    }

    if (OB_FAIL(ret)) {
    } else if (NULL != param_expr_left &&
               OB_FAIL(do_remove_subquery(stmt, param_expr_left, left_transform_happened, is_empty_left))) {
      LOG_WARN("failed to do_remove_subquery_as_expr", K(ret));
    } else if (NULL != param_expr_right &&
               OB_FAIL(do_remove_subquery(stmt, param_expr_right, right_transform_happened, right_is_empty))) {
      LOG_WARN("failed to do_remove_subquery_as_expr", K(ret));
    } else if (!left_transform_happened && !right_transform_happened) {
      // do nothing
    } else if (OB_FAIL(adjust_subquery_comparison_expr(expr, is_empty_left, right_is_empty, param_expr_left, param_expr_right))) {
      LOG_WARN("failed to adjust subquery comparison operator", K(ret));
    } else {
      trans_happened = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (expr->is_alias_ref_expr()) {
    //Eliminating the ref query in the alias ref will be risky,
    //and it will be processed in the future
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(try_remove_subquery_in_expr(stmt, expr->get_param_expr(i), is_happened)))) {
        LOG_WARN("failed to trans param expr", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  }
  if (OB_SUCC(ret) && trans_happened) {
    if (OB_FAIL(expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::adjust_subquery_comparison_expr(ObRawExpr*& expr,
                                                             bool is_empty_left,
                                                             bool is_empty_right,
                                                             ObRawExpr* param_expr_left,
                                                             ObRawExpr* param_expr_right)
{
  int ret = OB_SUCCESS;
  bool is_empty_cmp_value = (NULL != param_expr_left && NULL != param_expr_right) &&
                             ((!param_expr_left->is_query_ref_expr() && is_empty_right) ||
                             (!param_expr_right->is_query_ref_expr() && is_empty_left));
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(param_expr_left)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(expr), K(ctx_));
  } else if ((T_OP_NOT_EXISTS == expr->get_expr_type() && is_empty_left) ||
             (T_OP_EXISTS == expr->get_expr_type() && !is_empty_left) ||
             (is_empty_left && is_empty_right && T_OP_SQ_NSEQ == expr->get_expr_type()) ||
             (expr->has_flag(IS_WITH_ALL) && is_empty_right)) {
    ObRawExpr *bool_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(ctx_->expr_factory_, bool_expr, true))) {
      LOG_WARN("create const bool expr failed", K(ret));
    } else {
      expr = bool_expr;
    }
  } else if ((is_empty_cmp_value && T_OP_SQ_NSEQ != expr->get_expr_type()) ||
             (T_OP_NOT_EXISTS == expr->get_expr_type() && !is_empty_left) ||
             (T_OP_EXISTS == expr->get_expr_type() && is_empty_left)) {
    ObRawExpr *bool_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(ctx_->expr_factory_, bool_expr, false))) {
      LOG_WARN("create const bool expr failed", K(ret));
    } else {
      expr = bool_expr;
    }
  } else {
    ObOpRawExpr *new_expr = NULL;
    ObItemType op_type = expr->get_expr_type();
    if (OB_ISNULL(param_expr_right)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if ((!param_expr_left->is_query_ref_expr() && !param_expr_right->is_query_ref_expr() &&
        OB_FAIL(ObTransformUtils::query_cmp_to_value_cmp(expr->get_expr_type(), op_type)))
        || T_INVALID == op_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get op type", K(ret), K(op_type));
    } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(op_type, new_expr))) {
      LOG_WARN("create row expr failed", K(ret));
    } else if (OB_ISNULL(new_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row expr is null", K(ret));
    } else if (expr->get_param_count() != 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr param count != 2", K(ret));
    } else if (OB_FAIL(new_expr->add_param_expr(param_expr_left))) {
      LOG_WARN("add null expr failed", K(ret));
    } else if (OB_FAIL(new_expr->add_param_expr(param_expr_right))) {
      LOG_WARN("add null expr failed", K(ret));
    } else {
      expr = new_expr;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::is_valid_for_remove_subquery(const ObSelectStmt* stmt, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  bool has_rand = false;
  bool has_rownum = false;
  if (stmt->is_contains_assignment() || stmt->has_subquery() || 0 != stmt->get_window_func_count() ||
      stmt->is_hierarchical_query() || stmt->is_set_stmt() || 0 != stmt->get_pseudo_column_like_exprs().count()) {
    /* do nothing */
  } else if (OB_FAIL(stmt->has_rand(has_rand))) {
    LOG_WARN("failed to check if stmt has rand", K(ret));
  } else if (has_rand) {
    /* do nothing */
  } else if (OB_FAIL(stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to check if stmt has rownum", K(ret));
  } else if (has_rownum) {
    /* do nothing */
  } else {
    is_valid = true;
  }
  return ret;
}

int ObTransformSimplifyExpr::do_remove_subquery(ObDMLStmt* stmt, ObRawExpr*& expr, bool& trans_happened, bool& is_empty)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_where_false = false;
  bool is_having_false = false;
  bool is_having_true = false;
  bool is_limit_filter_false = false;
  bool is_valid = true;
  bool is_scalar_agg = false;
  ObRawExpr *limit_cons = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(ret), K(stmt), K(expr));
  } else if (expr->is_query_ref_expr() && !expr->is_multiset_expr()) {
    ObQueryRefRawExpr* query_ref = static_cast<ObQueryRefRawExpr*>(expr);
    ObSelectStmt* sub_stmt = query_ref->get_ref_stmt();
    if (OB_ISNULL(sub_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null stmt", K(ret));
    } else if (OB_FAIL(is_valid_for_remove_subquery(sub_stmt, is_valid))) {
      LOG_WARN("failed to check if sub stmt is valid for remove", K(ret));
    } else if (!is_valid) {
      /* do nothing */
    } else if (OB_FAIL(check_limit_value(sub_stmt, is_limit_filter_false, limit_cons))) {
      LOG_WARN("failed to check limit value", K(ret), K(*sub_stmt));
    } else if (OB_FAIL(is_filter_false(sub_stmt, is_where_false, is_having_false, is_having_true))) {
      LOG_WARN("failed to judge filter is false or not", K(ret), K(*stmt));
    } else if (FALSE_IT(is_scalar_agg = sub_stmt->is_scala_group_by())) {
      /* don nothing */
    } else if (is_having_false || is_limit_filter_false || (is_where_false && !is_scalar_agg)) {
      if (OB_FAIL(build_null_for_empty_set(sub_stmt, expr))) {
        LOG_WARN("failed to build empty set and cast", K(ret));
      } else {
        is_empty = true;
      }
    } else if (is_scalar_agg && is_where_false && (0 == sub_stmt->get_having_expr_size() || is_having_true)) {
      if (OB_FAIL(build_expr_for_not_empty_set(sub_stmt, expr))) {
        LOG_WARN("failed to build expr for not empty set", K(ret));
      }
    } else {
      is_valid = false;
    }
    if (OB_SUCC(ret) && is_valid) {
      if (is_limit_filter_false) {
        if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_,
                                                                limit_cons,
                                                                true/*is_true*/))) {
          LOG_WARN("failed to add constraints", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        trans_happened = true;
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::is_filter_false(ObSelectStmt* stmt, bool& is_where_false, bool& is_having_false, bool& is_having_true)
{
  int ret = OB_SUCCESS;
  bool is_true = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(ret));
  } else if (OB_FAIL(is_filter_exprs_false(stmt->get_having_exprs(), is_having_false, is_having_true))) {
    LOG_WARN("failed to check if 'having' condition is false", K(ret));
  } else if (!is_having_false) {
    if (OB_FAIL(is_filter_exprs_false(stmt->get_condition_exprs(), is_where_false, is_true))) {
      LOG_WARN("failed to check if 'where' condition is false", K(ret));
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::is_filter_exprs_false(common::ObIArray<ObRawExpr*>& filter_exprs, bool& is_false, bool& is_true)
{
  int ret = OB_SUCCESS;
  if (filter_exprs.count() == 1) {
    ObRawExpr* filter_expr = filter_exprs.at(0);
    if (OB_ISNULL(filter_expr)){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else if (filter_expr->get_result_type().is_integer_type() &&
               T_OP_AND != filter_expr->get_expr_type() &&
               T_OP_OR != filter_expr->get_expr_type() &&
               filter_expr->is_const_raw_expr()) {
      if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->exec_ctx_) ||
          OB_ISNULL(ctx_->exec_ctx_->get_physical_plan_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ctx_), K(ret));
      } else {
        ObObj result;
        bool is_cal_true = false;
        if (OB_FAIL(ObSQLUtils::calc_const_expr(
          filter_expr, &ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store(), result, is_cal_true))) {
          LOG_WARN("failed to compute const expr", K(ret), K(*filter_expr));
        } else if ((!is_cal_true) || (!result.is_integer_type())) {
        // do nothing if plan cache does not check the bool value, or the result is not integer type
          LOG_TRACE("plan cache does not check this bool value, ignore it", K(is_cal_true), K(result.get_type()));
        } else if (OB_FAIL(ObObjEvaluator::is_true(result, is_cal_true))) {
          LOG_WARN("failed to get bool value", K(ret));
        } else if (!is_cal_true) {
          is_false = true;
        } else {
          is_true = true;
        }
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::check_limit_value(ObSelectStmt* stmt,
                                               bool& is_limit_filter_false,
                                               ObRawExpr*& limit_cons)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  bool is_null_value = false;
  ObRawExpr* limit_expr = NULL;
  ObConstRawExpr *zero_expr = NULL;
  is_limit_filter_false = false;

  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) ||
      OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ctx_), K(ctx_->exec_ctx_), K(plan_ctx));
  } else if (stmt->get_limit_expr() != NULL) {
    int64_t limit_value = 0;
    limit_expr = stmt->get_limit_expr();
    if (OB_FAIL(ObTransformUtils::get_expr_int_value(limit_expr,
                                                     &plan_ctx->get_param_store(),
                                                     ctx_->exec_ctx_,
                                                     ctx_->allocator_,
                                                     limit_value,
                                                     is_null_value))) {
      LOG_WARN("failed to get limit int value", K(ret));
    } else if (is_null_value) {
      // do nothing
    } else if (limit_value == 0) {
      is_limit_filter_false = true;
      if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*(ctx_->expr_factory_),
                                                          ObIntType,
                                                          0,
                                                          zero_expr))) {
        LOG_WARN("failed to build int expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*(ctx_->expr_factory_),
                                                                ctx_->session_info_,
                                                                T_OP_EQ,
                                                                limit_cons,
                                                                limit_expr,
                                                                zero_expr))) {
        LOG_WARN("failed to build cmp expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::build_expr_for_not_empty_set(ObSelectStmt* sub_stmt, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  int64_t select_item_size;
  if (OB_ISNULL(sub_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(sub_stmt), K(ctx_), K(ctx_->expr_factory_));
  } else {
    int64_t select_item_size = sub_stmt->get_select_item_size();
    ObRawExpr* select_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_item_size; ++i) {
      select_expr = sub_stmt->get_select_item(i).expr_;
      if (OB_FAIL(replace_expr_when_filter_is_false(select_expr))) {
        LOG_WARN("failed to replace expr when filter is false", K(ret));
      } else {
        sub_stmt->get_select_item(i).expr_ = select_expr;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (select_item_size < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid select item size", K(*sub_stmt), K(ret));
    } else if (select_item_size == 1) {
      expr = sub_stmt->get_select_item(0).expr_;
    } else {
      ObOpRawExpr *row_expr = NULL;
      if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_ROW, row_expr))) {
        LOG_WARN("create row expr failed", K(ret));
      } else if (OB_ISNULL(row_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row expr is null", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < select_item_size; ++i) {
          if (OB_FAIL(row_expr->add_param_expr(sub_stmt->get_select_item(i).expr_))) {
            LOG_WARN("add new select expr failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          expr = row_expr;
        }
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::build_null_for_empty_set(const ObSelectStmt* sub_stmt, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  int64_t select_item_size;
  ObRawExpr* null_expr = NULL;
  if (OB_ISNULL(sub_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(sub_stmt), K(ctx_), K(ctx_->expr_factory_));
  } else if (FALSE_IT(select_item_size = sub_stmt->get_select_item_size())) {
  } else if (1 > select_item_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid select item size", K(*sub_stmt), K(ret));
  } else if (1 == select_item_size) {
    if (OB_FAIL(build_null_expr_and_cast(sub_stmt->get_select_item(0).expr_, null_expr))) {
      LOG_WARN("failed to build expr and cast", K(ret));
    } else {
      expr = null_expr;
    }
  } else {
    ObOpRawExpr* row_expr = NULL;
    if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_ROW, row_expr))){
      LOG_WARN("create row expr failed", K(ret));
    } else if (OB_ISNULL(row_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row expr is null", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < select_item_size; ++i) {
        if (OB_FAIL(build_null_expr_and_cast(sub_stmt->get_select_item(i).expr_, null_expr))) {
          LOG_WARN("failed to build expr and cast", K(ret));
        } else if (OB_FAIL(row_expr->add_param_expr(null_expr))) {
          LOG_WARN("add new select expr failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        expr = row_expr;
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::build_null_expr_and_cast(const ObRawExpr* expr, ObRawExpr*& cast_expr)
{
  int ret = OB_SUCCESS;
  cast_expr = NULL;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(expr), K(ctx_));
  } else if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx_->expr_factory_, cast_expr))) {
    LOG_WARN("failed to build null expr", K(ret));
  } else if (OB_ISNULL(cast_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_cast_for_replace_if_need(*ctx_->expr_factory_,
                                                                    expr,
                                                                    cast_expr,
                                                                    ctx_->session_info_))) {
    LOG_WARN("failed to add cast for replace if need", K(ret));
  }
  return ret;
}

int ObTransformSimplifyExpr::replace_expr_when_filter_is_false(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(expr), K(ctx_));
  } else if (expr->has_flag(IS_COLUMN)) {
    ObRawExpr* null_expr = NULL;
    if (OB_FAIL(build_null_expr_and_cast(expr, null_expr))) {
      LOG_WARN("failed to build expr and cast", K(ret));
    } else {
      expr = null_expr;
    }
  } else if (expr->has_flag(IS_AGG)) {
    ObAggFunRawExpr* aggr_expr = static_cast<ObAggFunRawExpr*>(expr);
    if (T_FUN_COUNT == aggr_expr->get_expr_type()) {
      ObConstRawExpr* zero_expr = NULL;
      ObRawExpr* cast_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_, ObIntType, 0, zero_expr))) {
        LOG_WARN("fail to build const int expr", K(ret));
      } else if (OB_ISNULL(zero_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (FALSE_IT(cast_expr = zero_expr)) {
      } else if (OB_FAIL(ObTransformUtils::add_cast_for_replace_if_need(*ctx_->expr_factory_,
                                                                   expr,
                                                                   cast_expr,
                                                                   ctx_->session_info_))) {
        LOG_WARN("failed to add cast for replace if need", K(ret));
      } else {
        expr = cast_expr;
      }
    } else {
      ObRawExpr* null_expr = NULL;
      if (OB_FAIL(build_null_expr_and_cast(expr, null_expr))) {
        LOG_WARN("failed to build expr and cast", K(ret));
      } else {
        expr = null_expr;
      }
    }
  } else if (expr->has_flag(CNT_WINDOW_FUNC) || expr->has_flag(CNT_SUB_QUERY)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr contains winfunc or subquery", K(expr));
  } else if (!expr->has_flag(IS_CONST_EXPR) && !expr->has_flag(IS_CONST)) {
    for (int64_t i = 0; i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(replace_expr_when_filter_is_false(expr->get_param_expr(i))))) {
        LOG_WARN("failed to replace expr when filter is false");
      }
    }
  }
  return ret;
}

/*
remove 'is false/true' from condition when expr is '(not) exists(...) is false/true'.
eg: exists(...) is false ---> not exists(...)

here is some arguements left either:
(NOT) IN, = ANY, != ALL have three return value (TRUE, FALSE, NOKNOW), it isn't rewrite this time.
*/
int ObTransformSimplifyExpr::transform_is_false_true_expr(ObDMLStmt *stmt, bool &trans_happened) {
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool where_happened = false;
  bool having_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(remove_false_true(stmt->get_condition_exprs(), where_happened))) {
    LOG_WARN("remove false true from condition expr failed", KR(ret));
  } else if (OB_FAIL(stmt->is_select_stmt() &&
                     remove_false_true(static_cast<ObSelectStmt*>(stmt)->get_having_exprs(), having_happened))) {
    LOG_WARN("remove false true from having expr failed", KR(ret));
  } else {
    trans_happened = where_happened | having_happened;
  }
  return ret;
}

int ObTransformSimplifyExpr::remove_false_true(common::ObIArray<ObRawExpr*> &exprs, bool &trans_happened) {
  int ret = OB_SUCCESS;
  trans_happened = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRawExpr *cond = exprs.at(i);
    bool is_valid = false;
    bool is_happened = false;
    if (OB_ISNULL(cond)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(is_valid_remove_false_true(cond, is_valid))) {
      LOG_WARN("is_valid_remove_false_true fail", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(remove_false_true(cond, exprs.at(i), is_happened))){
      LOG_WARN("remove_false_true failed", K(ret));
    } else if (is_happened) {
      trans_happened |= is_happened;
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::is_valid_remove_false_true(ObRawExpr *expr, bool &is_valid) {
  int ret = OB_SUCCESS;
  ObRawExpr *param_left = NULL;
  ObRawExpr *param_right = NULL;
  is_valid = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (T_OP_IS != expr->get_expr_type() && T_OP_IS_NOT != expr->get_expr_type()) {
    // do nothing
  } else if (2 > expr->get_param_count()) {
    // do nothing
  } else if (OB_ISNULL(param_left = expr->get_param_expr(0)) ||
             OB_ISNULL(param_right = expr->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param_left or param right is null", K(ret));
  } else if ((T_OP_EXISTS == param_left->get_expr_type() || T_OP_NOT_EXISTS == param_left->get_expr_type()) &&
             T_BOOL == param_right->get_expr_type()) {
    is_valid = true;
  }
  return ret;
}

int ObTransformSimplifyExpr::remove_false_true(ObRawExpr *expr,
                                               ObRawExpr *&ret_expr,
                                               bool &trans_happened) {
  int ret = OB_SUCCESS;
  ObRawExpr *param_left = NULL;
  ObRawExpr *param_right = NULL;
  ObRawExprFactory *factory = NULL;
  ObSQLSessionInfo *session = NULL;
  trans_happened = false;
  if (OB_ISNULL(expr) ||
      OB_ISNULL(ctx_) ||
      OB_ISNULL(factory = ctx_->expr_factory_) ||
      OB_ISNULL(session = ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL param", K(ret), K(expr), K_(ctx), K(factory), K(session));
  } else if (OB_ISNULL(param_left = expr->get_param_expr(0)) ||
             OB_ISNULL(param_right = expr->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param_left or param right is null", K(ret), K(param_left), K(param_right));
  } else {
    bool is_true = static_cast<const ObConstRawExpr *>(param_right)->get_value().get_bool();
    is_true = (is_true && T_OP_IS == expr->get_expr_type()) ||
              (!is_true && T_OP_IS_NOT == expr->get_expr_type());
    if (is_true) {
      ret_expr = param_left;
      trans_happened = true;
    } else {
      ObOpRawExpr* new_expr = NULL;
      ObItemType new_type = get_opposite_compare_type(param_left->get_expr_type());
      if (T_INVALID == new_type) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_opposite_expr_type is invalid", K(ret), K(param_left->get_expr_type()));
      } else if (OB_FAIL(factory->create_raw_expr(new_type, new_expr))) {
        LOG_WARN("failed to create raw expr", K(ret), K(new_type));
      } else if (OB_ISNULL(new_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new expr is NULL", K(ret));
      } else if (OB_FAIL(new_expr->set_param_expr(param_left->get_param_expr(0)))) {
        LOG_WARN("failed to set param expr", K(ret));
      } else if (OB_FAIL(new_expr->formalize(ctx_->session_info_))){
        LOG_WARN("failed to formalize new expr", K(ret));
      } else {
        ret_expr = new_expr;
        trans_happened = true;
      }
    }
  }
  return ret;
}
int ObTransformSimplifyExpr::remove_ora_decode(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 2> old_exprs;
  ObSEArray<ObRawExpr *, 2> new_exprs;
  ObSEArray<ObRawExpr *, 16> relation_exprs;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(stmt));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get stmt's relation exprs");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
    if (OB_FAIL(inner_remove_ora_decode(relation_exprs.at(i), old_exprs, new_exprs))) {
      LOG_WARN("failed to inner remove ora decode");
    }
  }
  if (OB_SUCC(ret) && !old_exprs.empty()) {
    if (OB_FAIL(stmt->replace_relation_exprs(old_exprs, new_exprs))) {
      LOG_WARN("select_stmt replace inner stmt expr failed", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::inner_remove_ora_decode(ObRawExpr *&expr,
                                                     ObIArray<ObRawExpr *> &old_exprs,
                                                     ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr *new_expr = NULL;
  if (OB_ISNULL(expr)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(expr));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(inner_remove_ora_decode(expr->get_param_expr(i),
                                                     old_exprs,
                                                     new_exprs)))) {
        LOG_WARN("failed to do remove ora_decode", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && T_FUN_SYS_ORA_DECODE == expr->get_expr_type()) {
    if (ObOptimizerUtil::find_item(old_exprs, expr)) {
    } else if (OB_FAIL(try_remove_ora_decode(expr, new_expr))){
      LOG_WARN("failed to do remove ora_decode", K(ret));
    } else if (NULL == new_expr) {
    } else if (OB_FAIL(old_exprs.push_back(expr))) {
      LOG_WARN("failed to push back into old_exprs", K(ret));
    } else if (OB_FAIL(new_exprs.push_back(new_expr))) {
      LOG_WARN("failed to push back into new_exprs", K(ret));
    }
  }
  return ret;
}

/*
  decode(com_expr, search_expr0, result_expr0, search_expr1, result_expr1 ... search_exprN, result_exprN, default)
*/
int ObTransformSimplifyExpr::check_remove_ora_decode_valid(ObRawExpr *&expr,
                                                           int64_t &result_idx,
                                                           bool &is_valid) {
  int ret = OB_SUCCESS;
  is_valid = false;
  int64_t param_num = 0;
  int64_t const_search_num = 0;
  ObRawExpr *com_expr = NULL;
  bool is_all_const = true;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) ||
      OB_ISNULL(ctx_->expr_factory_) || (OB_ISNULL(ctx_->allocator_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(expr), KP(ctx_));
  } else if (T_FUN_SYS_ORA_DECODE != expr->get_expr_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected type expr", K(ret), K(expr->get_expr_type()));
  } else if (expr->is_const_expr()) {
    is_valid = false; /* no need to replace */
  } else if (FALSE_IT(param_num = expr->get_param_count())) {
  } else if (OB_UNLIKELY(param_num < 3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param numm", K(ret), K(param_num));
  } else if (OB_ISNULL(com_expr = expr->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (com_expr->is_static_scalar_const_expr()) {
    const bool has_default = (param_num - 1) % 2;
    const int64_t search_num = (param_num - 1) / 2;
    for (int64_t i = 0; OB_SUCC(ret) && is_all_const && i < search_num; ++i) {
      ObRawExpr *search_expr = NULL;
      int64_t search_idx = 2 * i + 1;
      if (OB_ISNULL(search_expr = expr->get_param_expr(search_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(search_idx));
      } else if (!search_expr->is_static_scalar_const_expr()) {
        is_all_const = false;
        const_search_num = i;
      } else { /* search_expr is valid, continue */ }
    }
    if (OB_SUCC(ret) && is_all_const) {
      const_search_num = search_num;
    }
  }
  if (OB_SUCC(ret) && const_search_num > 0) {
    ObSEArray<ObRawExpr *, 3> param_exprs;
    ObConstRawExpr *int_expr = NULL;
    ObRawExpr *decode_expr = NULL;
    ObObj decode_obj;
    int64_t result = 0;
    bool calc_happened = false;
    bool is_oracle = lib::is_oracle_mode();
    if (OB_FAIL(param_exprs.push_back(com_expr))) {
      LOG_WARN("failed to push back into param_exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i <= const_search_num; ++i) {
      int64_t param_idx = 2 * i + 1;
      if (i < const_search_num && OB_FAIL(param_exprs.push_back(expr->get_param_expr(param_idx)))) {
        LOG_WARN("failed to push back into search_exprs", K(ret));
      } else if (is_oracle) {
        number::ObNumber num;
        if (OB_FAIL(num.from(static_cast<int64_t>(i), *ctx_->allocator_))) {
          LOG_WARN("failed to cast int64 to num");
        } else if (OB_FAIL(ObRawExprUtils::build_const_number_expr(*ctx_->expr_factory_,
                                                                    ObNumberType, num, int_expr))) {
          LOG_WARN("failed to build const expr", K(ret));
        }
      } else if (!is_oracle && ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                                          ObIntType, i, int_expr)) {
        LOG_WARN("failed to build const expr", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(int_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("build unexpected NULL expr", K(ret), KP(int_expr));
      } else if (OB_FAIL(param_exprs.push_back(int_expr))) {
        LOG_WARN("failed to push back into result_exprs", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObRawExprUtils::build_ora_decode_expr(ctx_->expr_factory_,
                                                             *ctx_->session_info_,
                                                             decode_expr,
                                                             param_exprs))) {
      LOG_WARN("failed to build ora_decode expr", K(ret));
    } else if (ObTransformUtils::calc_const_expr_result(decode_expr, ctx_, decode_obj, calc_happened)) {
      LOG_WARN("failed to calc const expr result", K(ret), K(*decode_expr));
    } else if (!calc_happened) {
      is_valid = false;
    } else if (OB_UNLIKELY(!decode_obj.is_numeric_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to calc const expr result", K(ret), K(is_valid), K(decode_obj));
    } else if (decode_obj.is_number() && OB_FAIL(decode_obj.get_number().cast_to_int64(result))) {
      LOG_WARN("failed to cast to int64", K(ret));
    } else if (decode_obj.is_integer_type()) {
      result = decode_obj.get_int();
    }
    if (OB_SUCC(ret) && calc_happened) {
      bool is_default = result == const_search_num;
      if (!is_all_const && is_default) {
        is_valid = false;
      } else {
        result_idx = is_default ? 2 * result + 1 : 2 * result + 2;
        if  (OB_UNLIKELY(result_idx < 0) || OB_UNLIKELY(result_idx >= param_exprs.count()) ||
             OB_UNLIKELY(result_idx > param_num)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected result", K(ret), K(result_idx));
        } else if (OB_FAIL(ObTransformUtils::add_equal_expr_value_constraint(ctx_, decode_expr,
                                                                     param_exprs.at(result_idx)))) {
          LOG_WARN("failed to add equal expr value constraint", K(ret));
        } else {
          is_valid = true;
        }
      }
    }
  } /* else is_valid = false, then return */
  return ret;
}

int ObTransformSimplifyExpr::try_remove_ora_decode(ObRawExpr *&expr,
                                                   ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  int64_t result_idx = 0;
  ObRawExpr *null_expr = NULL;
  new_expr = NULL;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(expr), KP(ctx_));
  } else if (OB_FAIL(check_remove_ora_decode_valid(expr, result_idx, is_valid))) {
    LOG_WARN("failed to check remove ora_decode", K(ret));
  } else if (is_valid) {
    if (result_idx < expr->get_param_count()) {
      new_expr = expr->get_param_expr(result_idx);
    } else if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx_->expr_factory_, null_expr))) {
      LOG_WARN("failed to build null expr", K(ret));
    } else if (OB_ISNULL(null_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), KP(null_expr));
    } else if (OB_FAIL(null_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize result expr", K(ret), K(null_expr));
    } else {
      new_expr = null_expr; /* using default null */
    }
    if (OB_SUCC(ret) && OB_FAIL(ObTransformUtils::add_cast_for_replace_if_need(*ctx_->expr_factory_,
                                                            expr, new_expr, ctx_->session_info_))) {
      LOG_WARN("try add cast expr above failed", K(ret));
    }
  }
  return ret;
}