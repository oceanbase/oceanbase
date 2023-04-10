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

#include "sql/resolver/dcl/ob_alter_user_primary_zone_resolver.h"
#include "sql/resolver/ddl/ob_database_resolver.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObAlterUserPrimaryZoneResolver::ObAlterUserPrimaryZoneResolver(ObResolverParams &params)
    : ObDCLResolver(params)
{
}

ObAlterUserPrimaryZoneResolver::~ObAlterUserPrimaryZoneResolver()
{
}

int ObAlterUserPrimaryZoneResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObAlterUserPrimaryZoneStmt *stmt = NULL;

  if (OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not init", K(ret));
  } else if (T_ALTER_USER_PRIMARY_ZONE != parse_tree.type_
             || 2 != parse_tree.num_child_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong root", K(ret), K(parse_tree.type_), K(parse_tree.num_child_));
  } else if (OB_ISNULL(stmt = create_stmt<ObAlterUserPrimaryZoneStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to create ObAlterUserPrimaryZoneStmt", K(ret));
  } else {
    ParseNode *user_node = const_cast<ParseNode*>(parse_tree.children_[0]);
    ParseNode *primary_zone_node = const_cast<ParseNode*>(parse_tree.children_[1]);
    if (OB_ISNULL(user_node) || OB_ISNULL(primary_zone_node) || 
        OB_ISNULL(user_node->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null user or null primary zone", K(primary_zone_node), K(user_node), K(ret));
    } else {
      ObString user_name;
      ObString host_name;
      user_name.assign_ptr(user_node->children_[0]->str_value_,
                            static_cast<int32_t>(user_node->children_[0]->str_len_));
      host_name.assign_ptr(OB_DEFAULT_HOST_NAME, static_cast<int32_t>(STRLEN(OB_DEFAULT_HOST_NAME)));
      if (OB_FAIL(check_dcl_on_inner_user(parse_tree.type_,
                                          params_.session_info_->get_priv_user_id(),
                                          user_name,
                                          host_name))) {
        LOG_WARN("failed to check dcl on inner-user or unsupport to modify reserved user", K(ret),
                  K(params_.session_info_->get_user_name()), K(user_name));
      }
      OZ(ObDatabaseResolver<ObAlterUserPrimaryZoneStmt>::resolve_primary_zone(
          stmt, primary_zone_node));
      stmt->set_tenant_id(params_.session_info_->get_effective_tenant_id());
      OZ(stmt->set_database_name(user_name));
      OZ(stmt->add_primary_zone_option());
      if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
        OZ (schema_checker_->check_ora_ddl_priv(
              params_.session_info_->get_effective_tenant_id(),
              params_.session_info_->get_priv_user_id(),
              ObString(""),
              stmt::T_ALTER_USER_PRIMARY_ZONE,
              params_.session_info_->get_enable_role_array()),
              params_.session_info_->get_effective_tenant_id(),
              params_.session_info_->get_user_id());
      }
    }
  }
  return ret;
}
