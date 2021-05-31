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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/expr/ob_raw_expr_precalc_analyzer.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_context.h"
#include "common/ob_smart_call.h"
namespace oceanbase {
using namespace common;
namespace sql {
const ObItemType ObRawExprPrecalcAnalyzer::EXPR_FILTER_LIST[] = {T_OP_IN, T_OP_NOT_IN, T_OP_ARG_CASE, T_FUN_SYS_NULLIF};

int ObRawExprPrecalcAnalyzer::analyze_all_expr(ObDMLStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> relation_exprs;
  ObArray<ObSelectStmt*> child_stmts;
  if (OB_ISNULL(my_session_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null session", K(ret));
  } else if (OB_FAIL(stmt.get_relation_exprs(relation_exprs))) {
    LOG_WARN("get relation exprs failed", K(ret));
  } else if (OB_FAIL(stmt.get_child_stmts(child_stmts))) {
    LOG_WARN("get child stmt failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
    if (OB_FAIL(analyze_expr_tree(relation_exprs.at(i), stmt, my_session_->use_static_typing_engine()))) {
      LOG_WARN("pre cast expr recursively failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    ObDMLStmt* child_stmt = child_stmts.at(i);
    if (OB_ISNULL(child_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child stmt is null");
    } else if (OB_FAIL(SMART_CALL(analyze_all_expr(*child_stmt)))) {
      LOG_WARN("analyze child stmt all expr failed", K(ret));
    }
  }
  return ret;
}

int ObRawExprPrecalcAnalyzer::pre_cast_const_expr(ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is null");
  } else if (!expr->has_generalized_column() || !(expr->has_flag(CNT_CONST) || expr->has_flag(CNT_CALCULABLE_EXPR)) ||
             !can_be_pre_cast(expr->get_expr_type())) {
    // do nothing
  } else if (expr->is_op_expr() || expr->is_sys_func_expr() || expr->is_udf_expr()) {
    ObOpRawExpr* op_expr = static_cast<ObOpRawExpr*>(expr);
    if (op_expr->get_input_types().count() == op_expr->get_param_count()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < op_expr->get_param_count(); ++i) {
        ObRawExpr* param_expr = op_expr->get_param_expr(i);
        if (OB_ISNULL(param_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is null");
        } else if (param_expr->has_flag(IS_CONST) || param_expr->has_flag(IS_CALCULABLE_EXPR)) {
          ObObjType calc_type = op_expr->get_input_types().at(i).get_calc_type();
          if (calculation_need_cast(param_expr->get_data_type(), calc_type)) {
            ObSysFunRawExpr* func_expr = NULL;
            if (OB_FAIL(ObRawExprUtils::create_to_type_expr(
                    expr_factory_, param_expr, calc_type, func_expr, my_session_))) {
              LOG_WARN("create cast expr failed", K(ret));
            } else if (OB_FAIL(op_expr->replace_param_expr(i, func_expr))) {
              LOG_WARN("replace param expr failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObRawExprPrecalcAnalyzer::pre_cast_recursively(ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is null");
  } else if (!expr->has_generalized_column() || !(expr->has_flag(CNT_CONST) || expr->has_flag(CNT_CALCULABLE_EXPR))) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(pre_cast_recursively(expr->get_param_expr(i))))) {
        LOG_WARN("pre cast recursively failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(pre_cast_const_expr(expr))) {
      LOG_WARN("pre cast const expr failed", K(ret));
    }
  }
  return ret;
}

int ObRawExprPrecalcAnalyzer::extract_calculable_expr(ObRawExpr*& expr, ObDMLStmt& stmt, const bool filter_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt.get_query_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr), K(stmt.get_query_ctx()));
  } else if (expr->has_flag(IS_CALCULABLE_EXPR)) {
    if (filter_expr && is_filtered(*expr)) {
      // do nothing
    } else {
      ObHiddenColumnItem hidden_col;
      hidden_col.hidden_idx_ = stmt.get_pre_param_size();
      hidden_col.expr_ = expr;
      if (expr->has_flag(CNT_CUR_TIME)) {
        stmt.get_query_ctx()->fetch_cur_time_ = true;
      }
      ObRawExpr* orig_expr = expr;
      if (OB_FAIL(stmt.add_calculable_item(hidden_col))) {
        LOG_WARN("add calculable item to stmt failed", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_param_expr(expr_factory_, hidden_col.hidden_idx_, expr))) {
        LOG_WARN("create param expr failed", K(ret));
      } else if (expr != orig_expr) {
        expr->set_orig_expr(orig_expr);
      }
    }
  }
  return ret;
}

int ObRawExprPrecalcAnalyzer::extract_calculable_expr_recursively(
    ObRawExpr*& expr, ObDMLStmt& stmt, const bool filter_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is null");
  } else if (expr->has_flag(IS_CALCULABLE_EXPR)) {
    if (OB_FAIL(extract_calculable_expr(expr, stmt, filter_expr))) {
      LOG_WARN("extract calculabe expr failed", K(ret));
    }
  } else if (expr->has_flag(CNT_CALCULABLE_EXPR)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_calculable_expr_recursively(expr->get_param_expr(i), stmt)))) {
        LOG_WARN("extract calculabe expr recursively failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprPrecalcAnalyzer::analyze_expr_tree(ObRawExpr* expr, ObDMLStmt& stmt, const bool filter_expr /* false */)
{
  int ret = OB_SUCCESS;
  ObRawExpr* old_expr = expr;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is null");
  } else if (OB_FAIL(pre_cast_recursively(expr))) {
    LOG_WARN("pre cast expr recursively failed", K(ret));
  } else if (OB_FAIL(expr->extract_info())) {
    LOG_WARN("extract expr info failed", K(ret));
  } else if (OB_FAIL(extract_calculable_expr_recursively(expr, stmt, filter_expr))) {
    LOG_WARN("extract calculabe expr recursively failed", K(ret));
  } else if (OB_FAIL(expr->extract_info())) {
    LOG_WARN("extract expr info failed", K(ret));
  } else if (old_expr != expr) {
    if (OB_FAIL(stmt.replace_expr_in_stmt(old_expr, expr))) {
      LOG_WARN("replace expr in stmt failed", K(ret));
    }
  }
  return ret;
}

bool ObRawExprPrecalcAnalyzer::can_be_pre_cast(ObItemType expr_type) const
{
  bool bret = true;
  if (T_FUN_SYS_TO_TYPE == expr_type || T_OP_ARG_CASE == expr_type || T_OP_CASE == expr_type ||
      T_FUN_SYS_SET_COLLATION == expr_type || T_FUN_COLUMN_CONV == expr_type || T_FUN_SYS_DEFAULT == expr_type ||
      T_FUN_SYS_AUTOINC_NEXTVAL == expr_type) {
    bret = false;
  }
  return bret;
}

bool ObRawExprPrecalcAnalyzer::calculation_need_cast(ObObjType param_type, ObObjType calc_type) const
{
  bool bret = false;
  if (ObNullType == param_type || ObNullType == calc_type) {
    bret = false;
  } else if (ObMaxType == param_type || ObMaxType == calc_type) {
    bret = false;
  } else if (param_type != calc_type) {  // TODO:char vs varchar?
    bret = true;
  }
  return bret;
}

bool ObRawExprPrecalcAnalyzer::is_filtered(ObRawExpr& expr)
{
  bool is_filtered = false;
  for (int i = 0; !is_filtered && i < ARRAYSIZEOF(EXPR_FILTER_LIST); i++) {
    if (expr.get_expr_type() == EXPR_FILTER_LIST[i]) {
      is_filtered = true;
    }
  }
  return is_filtered;
}
}  // namespace sql
}  // namespace oceanbase
