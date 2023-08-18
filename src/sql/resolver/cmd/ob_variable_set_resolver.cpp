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
#include "sql/resolver/cmd/ob_variable_set_resolver.h"
#include "sql/resolver/cmd/ob_variable_set_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

ObVariableSetResolver::ObVariableSetResolver(ObResolverParams &params)
  : ObStmtResolver(params)
{
}

ObVariableSetResolver::~ObVariableSetResolver()
{
}

int ObVariableSetResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObVariableSetStmt *variable_set_stmt = NULL;
  if (OB_UNLIKELY(T_VARIABLE_SET != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse_tree.type_ must be T_VARIABLE_SET", K(ret), K(parse_tree.type_));
  } else if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session_info_ or allocator_ is NULL", K(ret), K(session_info_), K(allocator_),
              K(schema_checker_));
  } else if (OB_ISNULL(variable_set_stmt = create_stmt<ObVariableSetStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create variable set stmt failed", K(ret));
  } else {
    stmt_ = variable_set_stmt;
    variable_set_stmt->set_actual_tenant_id(session_info_->get_effective_tenant_id());
    ParseNode *set_node = NULL;
    ObVariableSetStmt::VariableSetNode var_node;
    for (int64_t i = 0; OB_SUCC(ret) && i < parse_tree.num_child_; ++i) {
      if (OB_ISNULL(set_node = parse_tree.children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("set node is NULL", K(ret));
      } else if (OB_UNLIKELY(T_VAR_VAL != set_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("set_node->type_ must be T_VAR_VAL", K(ret), K(set_node->type_));
      } else {
        ParseNode *var = NULL;
        switch (set_node->value_) {
          case 0:
            //为了兼容mysql，这里为SET_SCOPE_SESSION而不是SET_SCOPE_NEXT_TRANS
            //var_node.set_scope_ = ObSetVar::SET_SCOPE_NEXT_TRANS;
            var_node.set_scope_ = ObSetVar::SET_SCOPE_SESSION;
            break;
          case 1:
            var_node.set_scope_ = ObSetVar::SET_SCOPE_GLOBAL;
            variable_set_stmt->set_has_global_variable(true);
            break;
          case 2:
            var_node.set_scope_ = ObSetVar::SET_SCOPE_SESSION;
            break;
          default:
            var_node.set_scope_ = ObSetVar::SET_SCOPE_NEXT_TRANS;
            break;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(var = set_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("var is NULL", K(ret));
        } else {
          ObString var_name;
          if (T_IDENT == var->type_) {
            var_node.is_system_variable_ = true; //PL的set语句在PL resolver里解析，不会走到这里，所以到这里的肯定是系统变量的缺省写法
            var_name.assign_ptr(var->str_value_, static_cast<int32_t>(var->str_len_));
          } else if (T_OBJ_ACCESS_REF == var->type_) { //Oracle mode
            const ParseNode *name_node = NULL;
            if (OB_ISNULL(name_node = var->children_[0])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected null", K(ret));
            } else if (OB_UNLIKELY(name_node->type_ != T_IDENT) || OB_UNLIKELY(var->children_[1] != NULL)) {
              ret = OB_ERR_UNKNOWN_SET_OPTION;
              LOG_WARN("unknown SET option", K(ret), K(name_node->type_), K(var->children_[1]));
              LOG_USER_ERROR(OB_ERR_UNKNOWN_SET_OPTION, name_node->str_value_);
            } else {
              var_node.is_system_variable_ = true; //PL的set语句在PL resolver里解析，不会走到这里，所以到这里的肯定是系统变量的缺省写法
              var_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_));
            }
          } else {
            var_node.is_system_variable_ = (T_SYSTEM_VARIABLE == var->type_);
            var_name.assign_ptr(var->str_value_, static_cast<int32_t>(var->str_len_));
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(ob_write_string(*allocator_, var_name, var_node.variable_name_))) {
              LOG_WARN("Can not malloc space for variable name", K(ret));
            } else {
              ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, var_node.variable_name_);
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_ISNULL(set_node->children_[1])) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("value node is NULL", K(ret));
          } else if (T_DEFAULT == set_node->children_[1]->type_) {
            // set 系统变量 = default
            var_node.is_set_default_ = true;
          } else if (var_node.is_system_variable_) {
            ParseNode value_node;
            if (T_IDENT == set_node->children_[1]->type_) {
              MEMCPY(&value_node, set_node->children_[1], sizeof(ParseNode));
              value_node.type_ = T_VARCHAR;
            } else if (T_COLUMN_REF == set_node->children_[1]->type_) {
              if (NULL == set_node->children_[1]->children_[0] && NULL == set_node->children_[1]->children_[1] && NULL != set_node->children_[1]->children_[2]) {
                MEMCPY(&value_node, set_node->children_[1]->children_[2], sizeof(ParseNode));
                value_node.type_ = T_VARCHAR;
              } else {
                MEMCPY(&value_node, set_node->children_[1], sizeof(ParseNode));
              }
            } else if (T_OBJ_ACCESS_REF == set_node->children_[1]->type_) { //Oracle mode
              if (OB_ISNULL(set_node->children_[1]->children_[0]) || OB_UNLIKELY(set_node->children_[1]->children_[1] != NULL)) {
                ret = OB_ERR_UNKNOWN_SET_OPTION;
                LOG_WARN("unknown SET option", K(ret), K(set_node->children_[1]->children_[0]->type_));
                LOG_USER_ERROR(OB_ERR_UNKNOWN_SET_OPTION, var->str_value_);
              } else {
                MEMCPY(&value_node, set_node->children_[1]->children_[0], sizeof(ParseNode));
              }

              if (OB_SUCC(ret)) {
                if (T_IDENT == set_node->children_[1]->children_[0]->type_) {
                  value_node.type_ = T_VARCHAR;
                } else if (T_FUN_SYS == set_node->children_[1]->children_[0]->type_) {
                  //do nothing
                } else {
                  ret = OB_NOT_SUPPORTED;
                  LOG_WARN("Variable value type is not supported", K(ret), K(set_node->children_[1]->children_[0]->type_));
                  LOG_USER_ERROR(OB_NOT_SUPPORTED, "Variable value type");
                }
              }
            } else {
              MEMCPY(&value_node, set_node->children_[1], sizeof(ParseNode));
            }
            if (OB_SUCC(ret)) {
#ifndef OB_BUILD_ORACLE_PARSER
              if (0 == var_node.variable_name_.case_compare("ob_compatibility_mode") &&
                  0 == strncasecmp(value_node.str_value_, "oracle",
                                   std::min(static_cast<int32_t>(value_node.str_len_), 6))) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "Not support oracle mode");
              }
#endif
              if (OB_SUCC(ret) &&
                  OB_FAIL(ObResolverUtils::resolve_const_expr(params_, value_node, var_node.value_expr_, NULL))) {
                LOG_WARN("resolve variable value failed", K(ret));
              }
            }
          } else {
            // use WARN_ON_FAIL cast_mode if set user_variable
            const stmt::StmtType session_ori_stmt_type = session_info_->get_stmt_type();
            session_info_->set_stmt_type(stmt::T_SELECT);
            if (OB_FAIL(resolve_value_expr(*set_node->children_[1], var_node.value_expr_))) {
              LOG_WARN("failed to resolve value expr", K(ret));
            }
            session_info_->set_stmt_type(session_ori_stmt_type);
          }
          if (OB_SUCC(ret)) {
            if (OB_NOT_NULL(var_node.value_expr_) && var_node.value_expr_->has_flag(CNT_AGG)) {
              ret = OB_ERR_INVALID_GROUP_FUNC_USE;
              LOG_WARN("invalid scope for agg function", K(ret));
            } else if (OB_FAIL(variable_set_stmt->add_variable_node(var_node))) {
              LOG_WARN("Add set entry failed", K(ret));
            }
          }
        }
      }
    }

    /* set global variable need 'alter system' priv*/
    if (OB_SUCC(ret) &&
        ObSchemaChecker::is_ora_priv_check() && variable_set_stmt->has_global_variable()) {
      if (OB_FAIL(schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
                                                      session_info_->get_priv_user_id(),
                                                      ObString(""),
                                                      stmt::T_VARIABLE_SET,
                                                      session_info_->get_enable_role_array()))) {
        LOG_WARN("failed to check privilege", K(session_info_->get_effective_tenant_id()),
                                              K(session_info_->get_priv_user_id()));
      }
    }
  }
  return ret;
}

int ObVariableSetResolver::resolve_value_expr(ParseNode &val_node, ObRawExpr *&value_expr)
{
  int ret = OB_SUCCESS;
  ObArray<ObQualifiedName> columns;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObUDFInfo> udf_info;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObOpRawExpr*> op_exprs;
  ObSEArray<ObUserVarIdentRawExpr*, 1> user_var_exprs;
  ObCollationType collation_connection = CS_TYPE_INVALID;
  ObCharsetType character_set_connection = CHARSET_INVALID;
  if (OB_ISNULL(params_.expr_factory_) || OB_ISNULL(params_.session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolve status is invalid", K_(params_.expr_factory), K_(params_.session_info));
  } else if (OB_FAIL(params_.session_info_->get_collation_connection(collation_connection))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_FAIL(params_.session_info_->get_character_set_connection(character_set_connection))) {
    LOG_WARN("fail to get character_set_connection", K(ret));
  } else {
    ObExprResolveContext ctx(*params_.expr_factory_, params_.session_info_->get_timezone_info(),
                             OB_NAME_CASE_INVALID);
    ctx.dest_collation_ = collation_connection;
    ctx.connection_charset_ = character_set_connection;
    ctx.param_list_ = params_.param_list_;
    ctx.is_extract_param_type_ = !params_.is_prepare_protocol_; //when prepare do not extract
    ctx.schema_checker_ = params_.schema_checker_;
    ctx.session_info_ = params_.session_info_;
    ctx.secondary_namespace_ = params_.secondary_namespace_;
    ctx.query_ctx_ = params_.query_ctx_;
    ObRawExprResolverImpl expr_resolver(ctx);
    if (OB_FAIL(params_.session_info_->get_name_case_mode(ctx.case_mode_))) {
      LOG_WARN("fail to get name case mode", K(ret));
    } else if (OB_FAIL(expr_resolver.resolve(&val_node, value_expr, columns, sys_vars,
                                             sub_query_info, aggr_exprs, win_exprs,
                                             udf_info, op_exprs, user_var_exprs))) {
      LOG_WARN("resolve expr failed", K(ret));
    } else if (udf_info.count() > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("UDFInfo should not found be here!!!", K(ret));
    } else if (value_expr->get_expr_type() == T_SP_CPARAM) {
      ObCallParamRawExpr *call_expr = static_cast<ObCallParamRawExpr *>(value_expr);
      if (OB_ISNULL(call_expr->get_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(call_expr->get_expr()->formalize(params_.session_info_))) {
        LOG_WARN("failed to formalize call expr", K(ret));
      }
    } else if (value_expr->has_flag(CNT_SUB_QUERY)) {
      if (is_oracle_mode()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("subqueries or stored function calls here is not supported", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "subqueries or stored function calls here");
      } else if (is_mysql_mode()) {
        if (OB_FAIL(resolve_subquery_info(sub_query_info, value_expr))) {
          LOG_WARN("failed to resolve subquery info", K(ret));
        }
      }
      LOG_TRACE("set user variable with subquery", K(sub_query_info.count()), K(is_mysql_mode()));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObResolverUtils::resolve_columns_for_const_expr(value_expr, columns, params_))) {
      LOG_WARN("resolve columns for const expr failed", K(ret));
    } else if (OB_FAIL(value_expr->formalize(params_.session_info_))) {
      LOG_WARN("failed to formalize value expr", K(ret));
    } else {
      params_.prepare_param_count_ += ctx.prepare_param_count_; //prepare param count
    }
  }
  return ret;
}

int ObVariableSetResolver::resolve_subquery_info(const ObIArray<ObSubQueryInfo> &subquery_info,
                                                 ObRawExpr *&value_expr)
{
  int ret = OB_SUCCESS;
  int current_level = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_info.count(); i++) {
    const ObSubQueryInfo &info = subquery_info.at(i);
    ObSelectResolver subquery_resolver(params_);
    subquery_resolver.set_current_level(current_level + 1);
    subquery_resolver.set_current_view_level(current_level);
    ObSelectStmt *sub_stmt = NULL;
    if (OB_ISNULL(info.sub_query_) || OB_ISNULL(info.ref_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subquery info is invalid", K_(info.sub_query), K_(info.ref_expr));
    } else if (OB_UNLIKELY(T_SELECT != info.sub_query_->type_)) {
      ret = OB_ERR_ILLEGAL_TYPE;
      LOG_WARN("Unknown statement type in subquery", "stmt_type", info.sub_query_->type_);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(subquery_resolver.resolve_child_stmt(*(info.sub_query_)))) {
      LOG_WARN("resolve select subquery failed", K(ret));
    } else if (OB_ISNULL(sub_stmt = subquery_resolver.get_child_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      // for set stmt, the parent stmt of subquery is subquery itself
      // we do this only to make sure that the sub_stmt is not a root stmt
      ObDMLStmt *dml_stmt = subquery_resolver.get_select_stmt();
      info.ref_expr_->set_ref_stmt(sub_stmt);
      info.ref_expr_->set_output_column(sub_stmt->get_select_item_size());
      // the column type of ref_expr stores the target type of subquery
      for (int64_t j = 0; OB_SUCC(ret) && j < sub_stmt->get_select_item_size(); ++j) {
        ObRawExpr *target_expr = sub_stmt->get_select_item(j).expr_;
        if (OB_ISNULL(target_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("target expr is null", K(ret));
        } else {
          const ObExprResType &column_type = target_expr->get_result_type();
          if (OB_FAIL(info.ref_expr_->add_column_type(column_type))) {
            LOG_WARN("add column type to subquery ref expr failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

ObAlterSessionSetResolver::ObAlterSessionSetResolver(ObResolverParams &params)
  : ObStmtResolver(params)
{
}

ObAlterSessionSetResolver::~ObAlterSessionSetResolver()
{
}

// for oracle mode grammer: alter session set sys_var = val
int ObAlterSessionSetResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObVariableSetStmt *variable_set_stmt = NULL;
  if (OB_UNLIKELY(T_ALTER_SESSION_SET != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse_tree.type_ must be T_ALTER_SESSION_SET", K(ret), K(parse_tree.type_));
  } else if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session_info_ or allocator_ is NULL", K(ret), K(session_info_), K(allocator_));
  } else if (OB_ISNULL(variable_set_stmt = create_stmt<ObVariableSetStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create variable set stmt failed", K(ret));
  } else {
    // start resolve
    stmt_ = variable_set_stmt;
    variable_set_stmt->set_actual_tenant_id(session_info_->get_effective_tenant_id());
    ParseNode *set_clause_node = NULL;
    ParseNode *set_param_node = NULL;
    ObVariableSetStmt::VariableSetNode var_node;
    // resolve alter_session_set_clause
    if (OB_ISNULL(set_clause_node = parse_tree.children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set_clause_node is NULL", K(ret));
    } else if (T_ALTER_SESSION_SET_PARAMETER_LIST != set_clause_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set_node->type_ must be T_ALTER_SESSION_SET_PARAMETER_LIST", K(ret), K(set_clause_node->type_));
    } else {
      // resolve set_system_parameter_clause_list
      for (int64_t i = 0; OB_SUCC(ret) && i < set_clause_node->num_child_; ++i) {
        if (OB_ISNULL(set_param_node = set_clause_node->children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("set_param_node is null", K(ret));
        } else if (T_VAR_VAL != set_param_node->type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("set_node->type_ must be T_VAR_VAL", K(ret), K(set_param_node->type_));
        } else {
          // resolve set_system_parameter_clause
          ParseNode *var = NULL;
          var_node.set_scope_ = ObSetVar::SET_SCOPE_SESSION;
          if (OB_ISNULL(var = set_param_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("var is NULL", K(ret));
          } else {
            // resolve variable
            ObString var_name;
            if (T_IDENT != var->type_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Variable name not an identifier type", K(ret));
            } else {
              var_node.is_system_variable_ = true;
              var_name.assign_ptr(var->str_value_, static_cast<int32_t>(var->str_len_));
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(ob_write_string(*allocator_, var_name, var_node.variable_name_))) {
                LOG_WARN("Can not malloc space for variable name", K(ret));
              } else {
                ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, var_node.variable_name_);
              }
            }
            // resolve value
            if (OB_SUCC(ret)) {
              if (OB_ISNULL(set_param_node->children_[1])) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("value node is NULL", K(ret));
              } else if (var_node.is_system_variable_) {
                ParseNode value_node;
                MEMCPY(&value_node, set_param_node->children_[1], sizeof(ParseNode));
                if (OB_FAIL(ObResolverUtils::resolve_const_expr(params_, value_node, var_node.value_expr_, NULL))) {
                  LOG_WARN("resolve variable value failed", K(ret));
                }
              }
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(variable_set_stmt->add_variable_node(var_node))) {
            LOG_WARN("Add set entry failed", K(ret));
          }
        }
      } // end for
    }
  }
  return ret;
}

}/* ns sql*/
}/* ns oceanbase */
