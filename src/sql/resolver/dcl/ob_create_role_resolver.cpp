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
#include "sql/resolver/dcl/ob_create_role_resolver.h"
#include "sql/resolver/dcl/ob_create_role_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_service.h"

namespace oceanbase {
using namespace common;

namespace sql {
ObCreateRoleResolver::ObCreateRoleResolver(ObResolverParams& params) : ObDCLResolver(params)
{}

ObCreateRoleResolver::~ObCreateRoleResolver()
{}

int ObCreateRoleResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  CHECK_COMPATIBILITY_MODE(session_info_);
  ObCreateRoleStmt* create_role_stmt = NULL;
  if (2 != parse_tree.num_child_ || T_CREATE_ROLE != parse_tree.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expect 2 child, create role type", "actual_num", parse_tree.num_child_, "type", parse_tree.type_, K(ret));
  } else if (OB_ISNULL(params_.session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Session info should not be NULL", K(ret));
  } else if (!lib::is_oracle_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "create role in non-Oracle mode");
  } else if (OB_ISNULL(create_role_stmt = create_stmt<ObCreateRoleStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Failed to create ObCreateRoleStmt", K(ret));
  } else if (NULL == parse_tree.children_[0]) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("role node is null", K(ret));
  } else {
    stmt_ = create_role_stmt;
    create_role_stmt->set_tenant_id(params_.session_info_->get_effective_tenant_id());
    ParseNode* role = const_cast<ParseNode*>(parse_tree.children_[0]);
    ObString role_name(role->str_len_, role->str_value_);
    create_role_stmt->set_role_name(role_name);

    // resolve password
    if (NULL != parse_tree.children_[1]) {
      ParseNode* pw_node = const_cast<ParseNode*>(parse_tree.children_[1]);
      ObString password(pw_node->str_len_, pw_node->str_value_);
      create_role_stmt->set_password(password);
    }
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      CK(params_.schema_checker_ != NULL);
      OZ(params_.schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
          session_info_->get_priv_user_id(),
          ObString(),
          stmt::T_CREATE_ROLE,
          session_info_->get_enable_role_array()));
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
