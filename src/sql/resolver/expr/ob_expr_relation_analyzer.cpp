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
#include "sql/resolver/expr/ob_expr_relation_analyzer.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "common/ob_smart_call.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprRelationAnalyzer::ObExprRelationAnalyzer() : query_exprs_()
{
}

/**
 * @brief ObExprRelationAnalyzer::pull_expr_relation_id_and_levels
 * 假定以下表达式是第 i 层的表达式
 * ObQueryRefRawExpr:
     expr levels:  该子查询引用到的 ObExecParamRawExpr 的 expr level
     relation ids: 该子查询引用的所有第 i 层列的 relation id 集合
 * ObColumnRefRawExpr:
     expr levels:  仅包含 i
     relation ids: 对应表的 relation bit index
 * ObSetOpRawExpr
     expr levels:  仅包含 i
     relation ids: 空
 * ObAggRawExpr
     expr levels:  仅包含 i
     relation ids: 该表达式引用的所有第 i 层列的 relation id 集合
 * Others
     expr levels:  该表达式引用到的所有列的 expr level
     relation ids: 该表达式引用的所有第 i 层列的 relation id 集合
 * @return
 */
int ObExprRelationAnalyzer::pull_expr_relation_id_and_levels(ObRawExpr *expr,
                                                             int32_t cur_stmt_level)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (OB_FAIL(visit_expr(*expr, cur_stmt_level))) {
    LOG_WARN("failed to pull expr relation id and levels", K(ret));
  }
  return ret;
}

int ObExprRelationAnalyzer::visit_expr(ObRawExpr &expr, int32_t stmt_level)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_expr_info(expr))) {
    LOG_WARN("failed into init expr level and relation ids", K(ret));
  } else if (expr.is_query_ref_expr()) {
    ObQueryRefRawExpr &query = static_cast<ObQueryRefRawExpr &>(expr);
    if (!query.has_nl_param()) {
      // do not rebuild exec param array if the subquery contains nlparams.
      query.get_exec_params().reset();
    }
    if (OB_FAIL(query_exprs_.push_back(&query))) {
      LOG_WARN("failed to push back query ref", K(ret));
    } else if (OB_FAIL(visit_stmt(query.get_ref_stmt()))) {
      LOG_WARN("failed to visit subquery stmt", K(ret));
    } else if (OB_UNLIKELY(query_exprs_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query expr stack is invalid", K(ret), K(query));
    } else {
      query_exprs_.pop_back();
    }
  }
  int64_t param_count = expr.has_flag(IS_ONETIME) ? 1 : expr.get_param_count();
  // not sure whether we should visit onetime exec param
  for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
    ObRawExpr *param = expr.has_flag(IS_ONETIME) ?
          static_cast<ObExecParamRawExpr &>(expr).get_ref_expr() :
          expr.get_param_expr(i);
    if (OB_ISNULL(param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(ret), K(param), K(i), K(expr));
    } else if (OB_FAIL(SMART_CALL(visit_expr(*param, stmt_level)))) {
      LOG_WARN("failed to visit param", K(ret));
    } else if (OB_FAIL(expr.get_expr_levels().add_members(param->get_expr_levels()))) {
      LOG_WARN("failed to add expr levels", K(ret));
    } else if (!param->get_expr_levels().has_member(stmt_level)) {
      // skip
    } else if (OB_FAIL(expr.get_relation_ids().add_members(param->get_relation_ids()))) {
      LOG_WARN("failed to add relation ids", K(ret));
    }
  }
  return ret;
}

int ObExprRelationAnalyzer::init_expr_info(ObRawExpr &expr)
{
  int ret = OB_SUCCESS;
  expr.get_expr_levels().reuse();
  if (!expr.is_column_ref_expr() &&
      T_ORA_ROWSCN != expr.get_expr_type()) {
    expr.get_relation_ids().reuse();
  }
  if (expr.is_column_ref_expr() || expr.is_aggr_expr() || expr.is_set_op_expr() ||
      expr.is_win_func_expr() || expr.is_exec_param_expr() ||
      T_ORA_ROWSCN == expr.get_expr_type()) {
    int32_t expr_level = expr.get_expr_level();
    if (OB_UNLIKELY(expr_level >= OB_MAX_SUBQUERY_LAYER_NUM || expr_level < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("the subquery nested layers is out of range", K(ret), K(expr_level));
    } else if (OB_FAIL(expr.get_expr_levels().add_member(expr_level))) {
      LOG_WARN("failed to add expr level", K(ret));
    } else if (expr.is_exec_param_expr() && !expr.has_flag(IS_ONETIME)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < query_exprs_.count(); ++i) {
        if (OB_FAIL(query_exprs_.at(i)->get_expr_levels().add_member(expr.get_expr_level()))) {
          LOG_WARN("failed to add expr level", K(ret));
        } else if (query_exprs_.at(i)->has_nl_param()) {
          // does not rebuild exec param for the subquery expr
        } else if (expr.get_expr_level() == query_exprs_.at(i)->get_expr_level()) {
          bool found = false;
          for (int64_t j = 0; j < query_exprs_.at(i)->get_exec_params().count(); ++j) {
            if (query_exprs_.at(i)->get_exec_param(j) == &expr) {
              found = true;
              break;
            }
          }
          if (!found && OB_FAIL(query_exprs_.at(i)->add_exec_param_expr(
                                  static_cast<ObExecParamRawExpr *>(&expr)))) {
            LOG_WARN("failed to add exec param expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObExprRelationAnalyzer::visit_stmt(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> relation_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
      ObRawExpr *expr = NULL;
      if (OB_ISNULL(expr = relation_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("relation expr is null", K(ret), K(expr));
      } else if (OB_FAIL(visit_expr(*expr, stmt->get_current_level()))) {
        LOG_WARN("failed to visit expr", K(ret));
      } else if (OB_FAIL(expr->extract_info())) {
        LOG_WARN("failed to extract expr info");
      }
    }
    if (OB_SUCC(ret) && stmt->is_select_stmt()) {
      ObIArray<ObSelectStmt*> &child_stmts = static_cast<ObSelectStmt*>(stmt)->get_set_query();
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
        if (OB_FAIL(SMART_CALL(visit_stmt(child_stmts.at(i))))) {
          LOG_WARN("failed to visit stmt", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_size(); ++i) {
      TableItem *table = NULL;
      if (OB_ISNULL(table = stmt->get_table_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret), K(table));
      } else if (!table->is_generated_table()) {
        // skip
      } else if (OB_FAIL(SMART_CALL(visit_stmt(table->ref_query_)))) {
        LOG_WARN("failed to visit generated table", K(ret));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
