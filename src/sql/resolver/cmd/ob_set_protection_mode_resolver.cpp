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
#include "sql/resolver/cmd/ob_set_protection_mode_resolver.h"
#include "sql/resolver/cmd/ob_set_protection_mode_stmt.h"
#include "sql/session/ob_sql_session_info.h"  // ObSQLSessionInfo
#include "sql/resolver/cmd/ob_alter_system_resolver.h"
namespace oceanbase
{
namespace sql
{
int ObSetProtectionModeResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObSetProtectionModeStmt *stmt = create_stmt<ObSetProtectionModeStmt>();
  if (OB_ISNULL(stmt)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create stmt", KR(ret));
  } else if (OB_UNLIKELY(T_SET_PROTECTION_MODE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", KR(ret), K(parse_tree.type_));
  } else if (OB_UNLIKELY(2 != parse_tree.num_child_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse tree", KR(ret), K(parse_tree.num_child_));
  } else if (OB_ISNULL(parse_tree.children_) || OB_ISNULL(parse_tree.children_[0]) ||
      parse_tree.children_[0]->type_ != T_INT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", KR(ret), KP(parse_tree.children_[0]));
  } else {
    const ParseNode *protection_mode_node = parse_tree.children_[0];
    const ParseNode *tenant_node = parse_tree.children_[1];
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    share::ObProtectionMode mode;
    // change src/sql/parser/sql_parser_mysql_mode.y when change the result->value
    switch (protection_mode_node->value_) {
      case 1:
        mode = share::ObProtectionMode(share::ObProtectionMode::MAXIMUM_PERFORMANCE_MODE);
        break;
      case 2:
        mode = share::ObProtectionMode(share::ObProtectionMode::MAXIMUM_AVAILABILITY_MODE);
        break;
      case 3:
        mode = share::ObProtectionMode(share::ObProtectionMode::MAXIMUM_PROTECTION_MODE);
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid protection mode", KR(ret), K(protection_mode_node->type_));
        break;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(tenant_node)) {
      tenant_id = session_info_->get_effective_tenant_id();
    } else if (OB_FAIL(ObAlterSystemResolverUtil::get_and_verify_tenant_name(tenant_node,
        false, /* allow_sys_meta_tenant */
        session_info_->get_effective_tenant_id(),
        tenant_id, "Set protection mode"))) {
      LOG_WARN("failed to resolve tenant_id", KR(ret), KP(tenant_node), K(tenant_node->str_value_));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!is_user_tenant(tenant_id) || !is_valid_tenant_id(tenant_id))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "set protection mode for SYS/META tenant is");
    } else if (OB_FAIL(stmt->get_arg().init(tenant_id, mode))) {
      LOG_WARN("failed to init set protection mode arg", KR(ret), K(tenant_id), K(mode));
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase