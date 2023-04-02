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
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/resolver/cmd/ob_get_diagnostics_resolver.h"
#include "sql/resolver/cmd/ob_get_diagnostics_stmt.h"
#include "sql/session/ob_sql_session_info.h"
namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

ObGetDiagnosticsResolver::ObGetDiagnosticsResolver(ObResolverParams &params)
  : ObStmtResolver(params)
{
}

ObGetDiagnosticsResolver::~ObGetDiagnosticsResolver()
{
}

int ObGetDiagnosticsResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObGetDiagnosticsStmt *diagnostics_stmt = NULL;
  ParseNode *is_condition = NULL;
  ParseNode *is_current = NULL;
  ParseNode *condition_node = NULL;
  ParseNode *item_list = NULL;
  ObString sel_sql;
  if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session info is not init", K(ret));
  } else if (OB_ISNULL(diagnostics_stmt = create_stmt<ObGetDiagnosticsStmt>())) {
    ret = OB_SQL_RESOLVER_NO_MEMORY;
    LOG_WARN("failed to create select stmt");
  } else if (FALSE_IT(params_.query_ctx_->set_literal_stmt_type(stmt::T_DIAGNOSTICS))) {
  } else if (OB_UNLIKELY(parse_tree.num_child_ != 4)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parser tree is wrong", K(ret));
  } else if (OB_ISNULL(is_condition = parse_tree.children_[0]) ||
             OB_ISNULL(is_current = parse_tree.children_[1]) ||
             OB_ISNULL(item_list = parse_tree.children_[3])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parser tree is wrong", K(ret));
  } else if (NULL == session_info_->get_pl_context() && is_current->value_ == 0) {
    ret = OB_ERR_GET_STACKED_DIAGNOSTICS;
    LOG_WARN("GET STACKED DIAGNOSTICS when handler not active", K(ret));
  } else if (FALSE_IT(condition_node = parse_tree.children_[2])) {
  } else if (OB_FAIL(set_diagnostics_type(diagnostics_stmt, is_current->value_, is_condition->value_))) {
    LOG_WARN("set diagnostic type failed", K(ret));
  } else {
    if (1 == is_condition->value_) {/* 表示是获取condition信息的语句*/
      CK(OB_NOT_NULL(condition_node));
      ObRawExpr* condition_num = NULL;
      if (OB_SUCC(ret) && NULL == session_info_->get_pl_context() && T_IDENT == condition_node->type_) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        diagnostics_stmt->set_invalid_condition_name(ObString(condition_node->str_len_,
                                                              condition_node->str_value_));
      }
      if (OB_FAIL(ret)) {
      } else if (T_INT == condition_node->type_ || T_QUESTIONMARK == condition_node->type_ 
                || T_VARCHAR == condition_node->type_) {
        if (OB_FAIL(ObResolverUtils::resolve_const_expr(params_, *condition_node, condition_num, NULL))) {
          LOG_WARN("Resolve condition number error", K(ret));
        } else if (T_QUESTIONMARK == condition_node->type_) {
          int64_t idx = strtoll(condition_node->str_value_, NULL, 10);
          if (INT64_MAX == idx) {
            ret = OB_SIZE_OVERFLOW;
            LOG_WARN("strtoll error", K(ret), K(ObString(condition_node->str_len_, condition_node->str_value_)));
          } else if (OB_FAIL(diagnostics_stmt->add_origin_param_index(idx))) {
            LOG_WARN("add_origin_param_index error", K(ret), K(idx));
          }
        }
      } else if (T_USER_VARIABLE_IDENTIFIER == condition_node->type_) {
        if (OB_FAIL(ObRawExprUtils::build_get_user_var(*params_.expr_factory_,
                                                        ObString(condition_node->str_len_,
                                                                condition_node->str_value_),
                                                        condition_num,
                                                        session_info_))) {
          LOG_WARN("Failed to build get user var", K(ret), K(ObString(condition_node->str_len_, condition_node->str_value_)));
        }
      } else if (T_IDENT == condition_node->type_) {
        diagnostics_stmt->set_invalid_condition_name(ObString(condition_node->str_len_,
                                                              condition_node->str_value_));
        ret = OB_ERR_BAD_FIELD_ERROR;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("condition node type is unexpected", K(ret), K(condition_node->type_));
      }
      if (OB_SUCC(ret) && OB_FAIL(diagnostics_stmt->add_param(condition_num))) {
        LOG_WARN("add conditon param error", K(ret));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < item_list->num_child_ ; ++i) {
      ParseNode *item = item_list->children_[i];
      ParseNode *var = NULL;
      ParseNode *val = NULL;
      ObRawExpr* info_expr = NULL;
      if (OB_ISNULL(item) || item->num_child_ != 2 || 
          OB_ISNULL(var = item->children_[0]) || OB_ISNULL(val = item->children_[1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parser tree is wrong", K(ret));
      } else if (T_IDENT == var->type_) {
        ret = OB_ERR_SP_UNDECLARED_VAR;
        LOG_WARN("undeclared var", K(ret));
        LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_VAR, static_cast<int>(var->str_len_), var->str_value_);
      } else if (T_QUESTIONMARK != var->type_ && T_USER_VARIABLE_IDENTIFIER != var->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("var type is unexpected", K(ret));
      } else if ((T_QUESTIONMARK == var->type_) &&
                 (OB_FAIL(ObResolverUtils::resolve_const_expr(params_, *var, info_expr, NULL)) ||
                 OB_FAIL(diagnostics_stmt->add_origin_param_index(strtoll(var->str_value_, NULL, 10))))) {
        LOG_WARN("resolve_const_expr failed", K(ret));
      } else if (T_USER_VARIABLE_IDENTIFIER == var->type_ &&
                 OB_FAIL(ObRawExprUtils::build_get_user_var(*params_.expr_factory_,
                                                      ObString(var->str_len_, var->str_value_),
                                                      info_expr,
                                                      session_info_))) {
        LOG_WARN("build_get_user_var failed", K(ret));
      } else if (OB_FAIL(diagnostics_stmt->add_param(info_expr))) {
        LOG_WARN("add param failed", K(ret));
      } else if (OB_FAIL(diagnostics_stmt->add_info_argument(ObString(val->str_len_, val->str_value_)))) {
        LOG_WARN("add argument failed", K(ret), K(ObString(var->str_len_, var->str_value_)));
      }
    }
  }

  if (OB_ERR_BAD_FIELD_ERROR == ret) {/* sql环境下使用非用户/系统变量作为condition参数时，get diagnostic语句能成功执行，只会在执行期报warning */
    ret = OB_SUCCESS;
  }
  
  return ret;
}

int ObGetDiagnosticsResolver::set_diagnostics_type(ObGetDiagnosticsStmt *diagnostics_stmt, 
                                                   const bool &is_current, const bool &is_condition)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(diagnostics_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (is_condition == 1 && is_current== 1) {
    diagnostics_stmt->set_diagnostics_type(DiagnosticsType::GET_CURRENT_COND);
  } else if (is_condition == 0 && is_current == 1) {
    diagnostics_stmt->set_diagnostics_type(DiagnosticsType::GET_CURRENT_INFO);
  } else if (is_condition == 1 && is_current == 0) {
    diagnostics_stmt->set_diagnostics_type(DiagnosticsType::GET_STACKED_COND);
  } else if (is_condition == 0 && is_current == 0) {
    diagnostics_stmt->set_diagnostics_type(DiagnosticsType::GET_STACKED_INFO);
  }
  return ret;
}

}/* ns sql*/
}/* ns oceanbase */
