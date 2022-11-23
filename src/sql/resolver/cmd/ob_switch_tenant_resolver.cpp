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
  if (OB_UNLIKELY(T_SWITCHOVER != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node, type is not T_SWITCHOVER", KR(ret), "type",
             get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create stmt fail", KR(ret));
  } else if (1 != parse_tree.num_child_
             || OB_ISNULL(parse_tree.children_)
             || OB_ISNULL(parse_tree.children_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse tree", KR(ret), "num_child", parse_tree.num_child_, 
                                   KP(parse_tree.children_), KP(parse_tree.children_[0]));
  } else {
    ParseNode *switch_node = parse_tree.children_[0];
    obrpc::ObSwitchTenantArg &arg = stmt->get_arg();

    if (T_FAILOVER_TO_PRIMARY == switch_node->type_) {
      arg.set_op_type(ObSwitchTenantArg::FAILOVER_TO_PRIMARY);

      if (OB_FAIL(resolve_tenant_name(*stmt, switch_node->children_[0],
                                      session_info_->get_effective_tenant_id()))) {
        LOG_WARN("resolve_tenant_name", KR(ret), KP(stmt), KP(switch_node->children_[0]),
                                        K(session_info_->get_effective_tenant_id()));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid switch node type", KR(ret), "type", get_type_name(switch_node->type_));
    }
  }

  if (OB_SUCC(ret)) {
    obrpc::ObSwitchTenantArg &arg = stmt->get_arg();
    arg.set_exec_tenant_id(session_info_->get_effective_tenant_id());
    stmt_ = stmt;
  }

  return ret;
}

} //end sql
} //end oceanbase
