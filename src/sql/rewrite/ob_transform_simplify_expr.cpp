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
    LOG_TRACE("succeed to replace is null condition", K(is_happened));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(replace_op_null_condition(stmt, is_happened))) {
      LOG_WARN("failed to replace op null condition", K(is_happened));
    } else {
      trans_happened |= is_happened;
      LOG_TRACE("succeed to replace op null condition", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_preds_vector_to_scalar(stmt, is_happened))) {
      LOG_WARN("failed to convert vector predicate to scalar", K(ret));
    } else {
      trans_happened |= is_happened;
      LOG_TRACE("succeed to convert vector predicate to scalar", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(remove_dummy_exprs(stmt, is_happened))) {
      LOG_WARN("failed to remove dummy exprs", K(ret));
    } else {
      trans_happened |= is_happened;
      LOG_TRACE("succeed to remove dummy exprs", K(is_happened));
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
    if (OB_UNLIKELY(op_expr->get_param_count() != 3)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected param numm", K(op_expr->get_param_count()), K(ret));
    } else {
      const ObRawExpr *child_0 = op_expr->get_param_expr(0);
      const ObRawExpr *child_1 = op_expr->get_param_expr(1);
      const ObRawExpr *child_2 = op_expr->get_param_expr(2);
      if (OB_ISNULL(child_0) || OB_ISNULL(child_1) || OB_ISNULL(child_2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpecte null", K(child_0), K(child_1),
                 K(child_2), K(ret));
      } else if (child_1->get_expr_type() == T_NULL &&
                 child_2->get_expr_type() == T_BOOL &&
                 !static_cast<const ObConstRawExpr *>(child_2)->get_value().get_bool()) {
        bool is_expected = false;
        const ObColumnRefRawExpr *col_expr = NULL;
        if (child_0->get_expr_type() == T_FUN_COUNT) {
          is_expected = true;
        } else if (child_0->is_column_ref_expr()) {
          col_expr = static_cast<const ObColumnRefRawExpr *>(child_0);
        }

        ObArray<ObRawExpr *> constraints;
        if (OB_SUCC(ret) && NULL != col_expr) {
          bool sql_auto_is_null = false;
          bool is_not_null = false;
          uint64_t table_id = col_expr->get_table_id();
          if (OB_FAIL(ctx_->session_info_->get_sql_auto_is_null(sql_auto_is_null))) {
            LOG_WARN("fail to get sql_auto_is_null system variable", K(ret));
          } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(not_null_ctx, col_expr,
                                                                is_not_null, &constraints))) {
            LOG_WARN("failed to check expr not null", K(ret));
          } else if (is_not_null) {
            if (T_OP_IS_NOT == expr->get_expr_type()
                || (T_OP_IS == expr->get_expr_type()
                    && !col_expr->get_result_type().is_datetime()
                    && !col_expr->get_result_type().is_date()
                    && !(sql_auto_is_null && col_expr->is_auto_increment()))) {
              if (OB_FAIL(is_expected_table_for_replace(stmt, table_id, is_expected))) {
                LOG_WARN("fail to judge expected table", K(ret), K(table_id),
                         K(is_expected));
              }
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

int ObTransformSimplifyExpr::is_expected_table_for_replace(ObDMLStmt *stmt, uint64_t table_id, bool &is_expected)
{
  int ret = OB_SUCCESS;
  is_expected = true;
  const TableItem *table_item = NULL;
  bool is_stack_overflow = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameter", K(stmt));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (NULL == (table_item = stmt->get_table_item_by_id(table_id))) {
    // table from upper stmt
    ObStmt *upper_stmt = NULL;
    if (OB_ISNULL(upper_stmt = stmt->get_parent_namespace_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("upper stmt shouldn't be NULL", K(ret));
    } else if (OB_UNLIKELY(!upper_stmt->is_dml_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("upper stmt must be dml", K(upper_stmt->get_stmt_type()));
    } else if (OB_FAIL(SMART_CALL(is_expected_table_for_replace(static_cast<ObDMLStmt *>(upper_stmt),
                                                                table_id, is_expected)))) {
      LOG_WARN("fail to judge expected table", K(ret));
    } else {/*do nothing*/}
  } else {
    //table from current stmt
    if (table_item->is_basic_table() || table_item->is_temp_table() || table_item->is_generated_table()) {
      bool is_on_null_side = false;
      if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(stmt, table_item->table_id_, is_on_null_side))) {
        LOG_WARN("check is table on null side failed", K(ret), K(*table_item));
      } else {
        is_expected = !is_on_null_side;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table item type", K_(table_item->type));
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::convert_preds_vector_to_scalar(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> new_cond;
  bool is_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL stmt", K(ret));
  } else if (!stmt->is_sel_del_upd()) {
    //do nothing
  } else {
    /// 需要转换的谓词有: where, join condition.
    /// having 中能下降的都下降过了.
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); i++) {
      if (OB_FAIL(inner_convert_preds_vector_to_scalar(
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
        if (OB_FAIL(convert_join_preds_vector_to_scalar(
                      stmt->get_joined_tables().at(i), trans_happened))) {
          LOG_WARN("failed to convert join preds vector to scalar", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformSimplifyExpr::convert_join_preds_vector_to_scalar(TableItem *table_item, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table item", K(ret));
  } else if (table_item->is_joined_table()) {
    JoinedTable *joined_table = reinterpret_cast<JoinedTable*>(table_item);
    ObSEArray<ObRawExpr*, 16> new_join_cond;
    bool is_happened = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->get_join_conditions().count(); i++) {
      if (OB_FAIL(inner_convert_preds_vector_to_scalar(
                    joined_table->get_join_conditions().at(i), new_join_cond, is_happened))) {
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

int ObTransformSimplifyExpr::inner_convert_preds_vector_to_scalar(ObRawExpr *expr,
                                                                  ObIArray<ObRawExpr*> &exprs,
                                                                  bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool need_push = true;
  ObRawExprFactory *factory = NULL;
  ObSQLSessionInfo *session = NULL;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(factory = ctx_->expr_factory_)
      || OB_ISNULL(session = ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL param", K(ret), K(expr), K_(ctx),
             K(factory), K(session));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if ((expr->get_expr_type() == T_OP_EQ) || (expr->get_expr_type() == T_OP_NSEQ)) {
    // 条件1: 是等号
    ObOpRawExpr *op_expr = reinterpret_cast<ObOpRawExpr*>(expr);
    ObRawExpr *param_expr1 = expr->get_param_expr(0);
    ObRawExpr *param_expr2 = expr->get_param_expr(1);

    if (OB_UNLIKELY(2 != op_expr->get_param_count())
        || OB_ISNULL(param_expr1) || OB_ISNULL(param_expr2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is wrong", K(op_expr->get_param_count()),
               K(param_expr1), K(param_expr2));
    } else if (T_OP_ROW == param_expr1->get_expr_type()
               && T_OP_ROW == param_expr2->get_expr_type()) {
      // 条件2: 两边都是 ROW
      need_push = false;
      trans_happened = true;
      if (OB_UNLIKELY(!is_oracle_mode() && param_expr1->get_param_count() != param_expr2->get_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param number not equal", K(ret), K(param_expr1->get_param_count()),
                 K(param_expr2->get_param_count()));

      } else if (OB_UNLIKELY(is_oracle_mode()
                             && 1 > param_expr2->get_param_count()
                             && param_expr1->get_param_count() != param_expr2->get_param_expr(0)->get_param_count()
                             && param_expr1->get_param_count() != param_expr2->get_param_count())) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_WARN("invalid relational operator on oracle mode", K(ret),
                 K(param_expr2->get_param_count()));
      } else {
        if (is_oracle_mode() && T_OP_ROW == param_expr2->get_param_expr(0)->get_expr_type()) {
          param_expr2 = param_expr2->get_param_expr(0);
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < param_expr1->get_param_count(); i++) {
          ObOpRawExpr *new_op_expr = NULL;
          if (OB_FAIL(factory->create_raw_expr(expr->get_expr_type(), new_op_expr))) {
            LOG_WARN("failed to create raw expr", K(ret));
          } else if (OB_ISNULL(new_op_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL new op expr", K(ret));
          } else if (OB_FAIL(new_op_expr->set_param_exprs(
                               param_expr1->get_param_expr(i), param_expr2->get_param_expr(i)))) {
            LOG_WARN("failed to set param expr", K(ret));
          } else if (OB_FAIL(new_op_expr->formalize(session))) {
            LOG_WARN("failed to formalize expr", K(ret));
          } else if (OB_FAIL(SMART_CALL(inner_convert_preds_vector_to_scalar(
                                          reinterpret_cast<ObRawExpr*>(new_op_expr), exprs, trans_happened)))) {
            /// 对拆分后 expr 继续递归
            LOG_WARN("failed to call inner convert recursive", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && need_push && OB_FAIL(exprs.push_back(expr))) {
    /// 不满足上述两个条件, 将该 expr 添加至输出.
    LOG_WARN("failed to push back expr", K(ret));
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
    int32_t ignore_scope = 0;
    if (stmt->is_select_stmt() && static_cast<ObSelectStmt*>(stmt)->is_scala_group_by()) {
      ignore_scope = RelExprCheckerBase::HAVING_SCOPE | RelExprCheckerBase::FIELD_LIST_SCOPE;
    }
    if (OB_FAIL(stmt->get_relation_exprs(relation_exprs, ignore_scope))) {
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
