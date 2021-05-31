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
namespace oceanbase {
using namespace common;
using namespace share;
namespace sql {

ObVariableSetResolver::ObVariableSetResolver(ObResolverParams& params) : ObStmtResolver(params)
{}

ObVariableSetResolver::~ObVariableSetResolver()
{}

int ObVariableSetResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObVariableSetStmt* variable_set_stmt = NULL;
  if (OB_UNLIKELY(T_VARIABLE_SET != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse_tree.type_ must be T_VARIABLE_SET", K(ret), K(parse_tree.type_));
  } else if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session_info_ or allocator_ is NULL", K(ret), K(session_info_), K(allocator_));
  } else if (OB_ISNULL(variable_set_stmt = create_stmt<ObVariableSetStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create variable set stmt failed", K(ret));
  } else {
    stmt_ = variable_set_stmt;
    variable_set_stmt->set_actual_tenant_id(session_info_->get_effective_tenant_id());
    ParseNode* set_node = NULL;
    ObVariableSetStmt::VariableSetNode var_node;
    for (int64_t i = 0; OB_SUCC(ret) && i < parse_tree.num_child_; ++i) {
      if (OB_ISNULL(set_node = parse_tree.children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("set node is NULL", K(ret));
      } else if (OB_UNLIKELY(T_VAR_VAL != set_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("set_node->type_ must be T_VAR_VAL", K(ret), K(set_node->type_));
      } else {
        ParseNode* var = NULL;
        switch (set_node->value_) {
          case 0:
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
            var_node.is_system_variable_ = true;
            var_name.assign_ptr(var->str_value_, static_cast<int32_t>(var->str_len_));
          } else if (T_OBJ_ACCESS_REF == var->type_) {  // Oracle mode
            const ParseNode* name_node = NULL;
            if (OB_ISNULL(name_node = var->children_[0]) || OB_UNLIKELY(var->children_[1] != NULL)) {
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "Variable name not an identifier type");
            } else if (OB_UNLIKELY(name_node->type_ != T_IDENT)) {
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "Variable name not an identifier type");
            } else {
              var_node.is_system_variable_ = true;
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
            // set system_variable = default
            var_node.is_set_default_ = true;
          } else if (var_node.is_system_variable_) {
            ParseNode value_node;
            if (T_IDENT == set_node->children_[1]->type_) {
              MEMCPY(&value_node, set_node->children_[1], sizeof(ParseNode));
              value_node.type_ = T_VARCHAR;
            } else if (T_COLUMN_REF == set_node->children_[1]->type_) {
              if (NULL == set_node->children_[1]->children_[0] && NULL == set_node->children_[1]->children_[1] &&
                  NULL != set_node->children_[1]->children_[2]) {
                MEMCPY(&value_node, set_node->children_[1]->children_[2], sizeof(ParseNode));
                value_node.type_ = T_VARCHAR;
              } else {
                MEMCPY(&value_node, set_node->children_[1], sizeof(ParseNode));
              }
            } else if (T_OBJ_ACCESS_REF == set_node->children_[1]->type_) {  // Oracle mode
              if (OB_ISNULL(set_node->children_[1]->children_[0]) ||
                  OB_UNLIKELY(set_node->children_[1]->children_[1] != NULL)) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "Variable value not a varchar nor identifier type");
              } else {
                MEMCPY(&value_node, set_node->children_[1]->children_[0], sizeof(ParseNode));
              }

              if (OB_SUCC(ret)) {
                if (T_IDENT == set_node->children_[1]->children_[0]->type_) {
                  value_node.type_ = T_VARCHAR;
                } else if (T_FUN_SYS == set_node->children_[1]->children_[0]->type_) {
                  // do nothing
                } else {
                  ret = OB_NOT_SUPPORTED;
                  LOG_USER_ERROR(OB_NOT_SUPPORTED, "Variable value type");
                }
              }
            } else {
              MEMCPY(&value_node, set_node->children_[1], sizeof(ParseNode));
            }
            if (OB_SUCC(ret)) {
              if (0 == var_node.variable_name_.case_compare("ob_compatibility_mode") &&
                  0 == strncasecmp(
                           value_node.str_value_, "oracle", std::min(static_cast<int32_t>(value_node.str_len_), 6))) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "Not support oracle mode");
              } else if (OB_FAIL(
                             ObResolverUtils::resolve_const_expr(params_, value_node, var_node.value_expr_, NULL))) {
                LOG_WARN("resolve variable value failed", K(ret));
              }
            }
          } else {
            if (OB_FAIL(ObResolverUtils::resolve_const_expr(
                    params_, *set_node->children_[1], var_node.value_expr_, NULL))) {
              LOG_WARN("resolve variable value failed", K(ret));
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(variable_set_stmt->add_variable_node(var_node))) {
            LOG_WARN("Add set entry failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

ObAlterSessionSetResolver::ObAlterSessionSetResolver(ObResolverParams& params) : ObStmtResolver(params)
{}

ObAlterSessionSetResolver::~ObAlterSessionSetResolver()
{}

// for oracle mode grammer: alter session set sys_var = val
int ObAlterSessionSetResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObVariableSetStmt* variable_set_stmt = NULL;
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
    ParseNode* set_clause_node = NULL;
    ParseNode* set_param_node = NULL;
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
          ParseNode* var = NULL;
          var_node.set_scope_ = ObSetVar::SET_SCOPE_SESSION;
          if (OB_ISNULL(var = set_param_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("var is NULL", K(ret));
          } else {
            // resolve variable
            ObString var_name;
            if (T_IDENT != var->type_) {
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "Variable name not an identifier type");
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
      }  // end for
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
