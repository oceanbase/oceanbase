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

#include "sql/resolver/cmd/ob_set_transaction_resolver.h"
#include "sql/resolver/cmd/ob_variable_set_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_trans_character.h"
using namespace oceanbase::common;
using namespace oceanbase::transaction;
namespace oceanbase
{
using namespace share;
namespace sql
{
ObSetTransactionResolver::ObSetTransactionResolver(ObResolverParams &params)
    :ObCMDResolver(params)
{}

ObSetTransactionResolver::~ObSetTransactionResolver()
{}

int ObSetTransactionResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObVariableSetStmt *stmt = NULL;
  if (OB_ISNULL(stmt = create_stmt<ObVariableSetStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create stmt failed");
  } else if (T_TRANSACTION != parse_tree.type_
             || OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid parse node", K(parse_tree.type_), K(parse_tree.children_));
  } else {
    ParseNode *characteristics_node = parse_tree.children_[1];
    ParseNode *scope_node = parse_tree.children_[0];
    ObVariableSetStmt::VariableSetNode access_var_node;
    ObVariableSetStmt::VariableSetNode isolation_var_node;
    ObSetVar::SetScopeType scope = ObSetVar::SET_SCOPE_NEXT_TRANS;
    int32_t isolation_level = ObTransIsolation::UNKNOWN;
    bool is_read_only = false;
    if (OB_ISNULL(scope_node) || OB_ISNULL(characteristics_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inalid scope node", K(scope_node), K(characteristics_node));
    } else if (OB_FAIL(scope_resolve(*scope_node, scope))) {
      LOG_WARN("fail to scope resolve", K(ret));
    } else if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid allocator in resolver", K(allocator_));
    } else {
      access_var_node.set_scope_ = scope;
      access_var_node.is_system_variable_ = true;
      isolation_var_node.set_scope_ = scope;
      isolation_var_node.is_system_variable_ = true;
      if (OB_FAIL(ob_write_string(
                  *allocator_, OB_SV_TX_ISOLATION, isolation_var_node.variable_name_))) {
        LOG_WARN("fail to write string", K(ret));
      } else if (OB_FAIL(ob_write_string(
                  *allocator_, OB_SV_TX_READ_ONLY, access_var_node.variable_name_))) {
        LOG_WARN("fail to write string", K(ret));
      } else {
        ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, isolation_var_node.variable_name_);
        ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, access_var_node.variable_name_);
      }
    }
    bool set_access_mode = false;
    bool set_isolation_level = false;
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(characteristics_node->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid characteristics node", K(characteristics_node->children_));
      } else {
        if (NULL != characteristics_node->children_[0]) {
          if (OB_FAIL(access_mode_resolve(*characteristics_node->children_[0], is_read_only))) {
            LOG_WARN("fail to resolve access mode", K(ret));
          } else {
            set_access_mode = true;
          }
        }
        if (NULL != characteristics_node->children_[1]) {
          if (OB_FAIL(isolation_level_resolve(*characteristics_node->children_[1], isolation_level))) {
            LOG_WARN("fail to resolve isolation level", K(ret));
          } else {
            set_isolation_level = true;
          }
        }
      }
    }
    if (OB_SUCC(ret) && set_isolation_level) {
      if (OB_FAIL(build_isolation_expr(isolation_var_node.value_expr_, isolation_level)) ) {
        LOG_WARN("fail to build isolation expr", K(ret));
      } else if (OB_FAIL(stmt->add_variable_node(isolation_var_node))) {
        LOG_WARN("fail to add variable node", K(ret));
      }
    }
    if (OB_SUCC(ret) && set_access_mode) {
      if (OB_FAIL(build_access_expr(access_var_node.value_expr_, is_read_only))) {
        LOG_WARN("fail to build access expr", K(ret));
      } else if (OB_FAIL(stmt->add_variable_node(access_var_node))) {
        LOG_WARN("fail to add variable node", K(ret));
      } else {
        LOG_DEBUG("add variable node", K(is_read_only));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(session_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid session info", K(ret), K(session_info_));
      } else {
        stmt->set_actual_tenant_id(session_info_->get_effective_tenant_id());
      }
    }
  }
  return ret;
}

int ObSetTransactionResolver::build_isolation_expr(ObRawExpr *&expr, int32_t level)
{
  int ret = OB_SUCCESS;
  ObObjParam val;
  ObConstRawExpr *c_expr = NULL;
  const ObString &level_name = ObTransIsolation::get_name(level);
  if (OB_ISNULL(params_.expr_factory_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid stmt", K_(stmt));
  } else if (level_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("isolation level is not invalid", K(ret), K(level));
  } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_VARCHAR, c_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else {
    // we use int type to represent isolation level, except for system variable tx_isolation,
    // which use varchar type, so we need cast int to varchar here.
    val.set_varchar(level_name);
    val.set_collation_type(ObCharset::get_system_collation());
    val.set_param_meta();
    c_expr->set_param(val);
    c_expr->set_value(val);
    c_expr->set_data_type(ObVarcharType);
    expr = c_expr;
  }
  return ret;
}

int ObSetTransactionResolver::build_access_expr(ObRawExpr *&expr, const bool is_read_only)
{
  int ret = OB_SUCCESS;
  ObObjParam val;
  ObConstRawExpr *c_expr = NULL;
  if (is_read_only) {
    val.set_int(1);
  } else {
    val.set_int(0);
  }
  val.set_scale(0);
  val.set_param_meta();
  if (OB_ISNULL(params_.expr_factory_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid stmt", K_(stmt));
  } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_INT, c_expr))) {
    LOG_WARN("create raw expr failed", K(ret));
  } else {
    c_expr->set_param(val);
    c_expr->set_value(val);
    c_expr->set_data_type(ObIntType);
    expr = c_expr;
  }
  return ret;
}

int ObSetTransactionResolver::scope_resolve(const ParseNode &parse_tree, ObSetVar::SetScopeType &scope)
{
  int ret = OB_SUCCESS;
  switch(parse_tree.value_) {
    case 0:
      scope = ObSetVar::SET_SCOPE_NEXT_TRANS;
      break;
    case 1:
      scope = ObSetVar::SET_SCOPE_GLOBAL;
      break;
    case 2:
      scope = ObSetVar::SET_SCOPE_SESSION;
      break;
    default:
      scope = ObSetVar::SET_SCOPE_NEXT_TRANS;
      break;
  }
  return ret;
}

int ObSetTransactionResolver::access_mode_resolve(const ParseNode &parse_tree, bool &is_read_only)
{
  int ret = OB_SUCCESS;
  if (IS_READ_ONLY(parse_tree.value_)) {
    is_read_only = true;
  } else if (IS_READ_WRITE(parse_tree.value_)) {
    is_read_only = false;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid access mode", "value", parse_tree.value_);
  }
  return ret;
}

int ObSetTransactionResolver::isolation_level_resolve(const ParseNode &parse_tree,
                                                      int32_t &isolation_level)
{
  int ret = OB_SUCCESS;
  switch (parse_tree.value_) {
    case 0:
      isolation_level = transaction::ObTransIsolation::READ_UNCOMMITTED;
      break;
    case 1:
      isolation_level = transaction::ObTransIsolation::READ_COMMITED;
      break;
    case 2:
      isolation_level = transaction::ObTransIsolation::REPEATABLE_READ;
      break;
    case 3:
      isolation_level = transaction::ObTransIsolation::SERIALIZABLE;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid isolation level", K(parse_tree.value_));
      break;
  }
  if (OB_SUCC(ret)) {
    if (!transaction::ObTransIsolation::is_valid(isolation_level)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid isolation level, we just supported READ_COMMIT and SERIALIZABLE", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "isolation level is");
    }
  }
  return ret;
}
}
}
