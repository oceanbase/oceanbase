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
#include "sql/resolver/dcl/ob_drop_role_resolver.h"
#include "sql/resolver/dcl/ob_drop_role_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_service.h"

namespace oceanbase
{
using namespace common;

namespace sql
{
ObDropRoleResolver::ObDropRoleResolver(ObResolverParams &params)
    : ObDCLResolver(params)
{
}

ObDropRoleResolver::~ObDropRoleResolver()
{
}

int ObDropRoleResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  CHECK_COMPATIBILITY_MODE(session_info_);
  ObDropRoleStmt *drop_role_stmt = NULL;
  if ((lib::is_oracle_mode() ? 1 : 2) != parse_tree.num_child_ || T_DROP_ROLE != parse_tree.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expect 2 child, drop role type",
             "actual_num", parse_tree.num_child_,
             "type", parse_tree.type_,
             K(ret));
  } else if (OB_ISNULL(params_.session_info_)
             || OB_ISNULL(params_.schema_checker_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Session info should not be NULL", K(ret));
	} else if (OB_ISNULL(drop_role_stmt = create_stmt<ObDropRoleStmt>())) {
		ret = OB_ALLOCATE_MEMORY_FAILED;
		LOG_ERROR("Failed to drop ObDropRoleStmt", K(ret));
	} else if (NULL == parse_tree.children_[0]) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("role node is null", K(ret));
  } else if (lib::is_oracle_mode()) {
    stmt_ = drop_role_stmt;
    drop_role_stmt->set_tenant_id(params_.session_info_->get_effective_tenant_id());
    ParseNode *role = const_cast<ParseNode*>(parse_tree.children_[0]);
    const share::schema::ObUserInfo *user_info = NULL;
    ObString role_name(role->str_len_, role->str_value_);
    if (OB_FAIL(schema_checker_->get_user_info(params_.session_info_->get_effective_tenant_id(), 
                                               role_name, 
                                               ObString::make_string(OB_DEFAULT_HOST_NAME), 
                                               user_info))) {
      LOG_WARN("failed to get user info", K(ret), K(role_name));
      if (OB_USER_NOT_EXIST == ret) {
        // 跳过, RS统一处理, 兼容MySQL行为
        ret = OB_SUCCESS;
      }
    } else if (is_inner_user_or_role(user_info->get_user_id())) {
      ret = OB_ERR_NO_PRIVILEGE;
      SQL_RESV_LOG(WARN, "Can not drop internal role", K(ret));
    }
    OX (drop_role_stmt->set_role_name(role_name));
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      OZ (params_.schema_checker_->check_ora_ddl_priv(
                                    session_info_->get_effective_tenant_id(),
                                    session_info_->get_priv_user_id(),
                                    ObString(),
                                    stmt::T_DROP_ROLE,
                                    session_info_->get_enable_role_array()));
    }
  } else {
    //mysql mode
    stmt_ = drop_role_stmt;
    drop_role_stmt->set_tenant_id(params_.session_info_->get_effective_tenant_id());
    ParseNode *users_node = const_cast<ParseNode*>(parse_tree.children_[0]);

    OZ (ObSQLUtils::compatibility_check_for_mysql_role_and_column_priv(params_.session_info_->get_effective_tenant_id()));

    if (OB_SUCC(ret) && NULL != parse_tree.children_[1]) {
      if (T_IF_NOT_EXISTS != parse_tree.children_[1]->type_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(parse_tree.children_[1]->type_), K(ret));
      } else {
        drop_role_stmt->set_if_exists();
      }
    }

    for (int i = 0; OB_SUCC(ret) && i < users_node->num_child_; i++) {
      ParseNode *cur_role = users_node->children_[i];
      ObString user_name;
      ObString host_name;
      if (OB_ISNULL(cur_role)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid role", K(ret));
      } else {
        OZ (resolve_user_list_node(cur_role, users_node, user_name, host_name));
        OZ (drop_role_stmt->get_user_names().push_back(user_name));
        OZ (drop_role_stmt->get_host_names().push_back(host_name));
      }
    }
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
