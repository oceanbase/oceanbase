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
#include "sql/resolver/cmd/ob_switch_tenant_resolver.h"
#include "sql/resolver/cmd/ob_switch_tenant_stmt.h"
#include "sql/session/ob_sql_session_info.h"  // ObSQLSessionInfo

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;

int ObSwitchTenantResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (T_SWITCHOVER == parse_tree.type_) {
    if (OB_FAIL(resolve_switch_tenant(parse_tree))) {
      LOG_WARN("failed to resolve switch cluster", KR(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node", KR(ret),
             "type", get_type_name(parse_tree.type_));
  }
  return ret;
}

int ObSwitchTenantResolver::resolve_switch_tenant(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObSwitchTenantStmt *stmt = create_stmt<ObSwitchTenantStmt>();
  ObString tenant_name;
  obrpc::ObSwitchTenantArg::OpType op_type = obrpc::ObSwitchTenantArg::OpType::INVALID;
  bool is_verify = false;
  if (OB_UNLIKELY(T_SWITCHOVER != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node, type is not T_SWITCHOVER", KR(ret), "type",
             get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create stmt fail", KR(ret));
  } else if (2 != parse_tree.num_child_
             || OB_ISNULL(parse_tree.children_)
             || OB_ISNULL(parse_tree.children_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse tree", KR(ret), "num_child", parse_tree.num_child_, 
                                   KP(parse_tree.children_), KP(parse_tree.children_[0]));
  } else {
    ParseNode *switch_node = parse_tree.children_[0];
    obrpc::ObSwitchTenantArg &arg = stmt->get_arg();

    if (OB_FAIL(resolve_tenant_name(switch_node->children_[0],
                                    session_info_->get_effective_tenant_id(), tenant_name))) {
      LOG_WARN("resolve_tenant_name fail", KR(ret), KP(stmt), KP(switch_node->children_[0]),
                                           K(session_info_->get_effective_tenant_id()));
    } else {
      switch(switch_node->type_) {
        case T_SWITCHOVER_TO_PRIMARY: {
          op_type = ObSwitchTenantArg::SWITCH_TO_PRIMARY;
          break;
        }
        case T_SWITCHOVER_TO_STANDBY: {
          op_type = ObSwitchTenantArg::SWITCH_TO_STANDBY;
          break;
        }
        case T_FAILOVER_TO_PRIMARY: {
          op_type = ObSwitchTenantArg::FAILOVER_TO_PRIMARY;
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid switch node type", KR(ret), "type", get_type_name(switch_node->type_));
          break;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(parse_tree.children_[1])) {
    // not verify
  } else if (T_VERIFY == parse_tree.children_[1]->type_) {
    is_verify = true;
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt->get_arg().init(session_info_->get_effective_tenant_id(), op_type, tenant_name, is_verify))) {
      LOG_WARN("fail to init arg", KR(ret), K(stmt->get_arg()),
                K(session_info_->get_effective_tenant_id()), K(tenant_name), K(op_type), K(is_verify));
    } else {
      stmt_ = stmt;
    }
  }

  return ret;
}

int resolve_tenant_name(
    const ParseNode *node,
    const uint64_t effective_tenant_id,
    ObString &tenant_name)
{
  int ret = OB_SUCCESS;
  tenant_name.reset();
  if (OB_ISNULL(node)) {
    if (OB_SYS_TENANT_ID == effective_tenant_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant name, should specify tenant name");
    }
  } else if (OB_UNLIKELY(T_TENANT_NAME != node->type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(node->type_));
  } else if (OB_UNLIKELY(node->num_child_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid num_child", "num_child", node->num_child_);
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node should not be null");
  } else {
    const ParseNode *tenant_name_node = node->children_[0];
    if (OB_ISNULL(tenant_name_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant_name_node should not be null");
    } else if (tenant_name_node->value_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty tenant string");
    } else {
      tenant_name.assign_ptr((char *)(tenant_name_node->str_value_),
                            static_cast<int32_t>(tenant_name_node->str_len_));
    }
  }
  return ret;
}

} //end sql
} //end oceanbase
