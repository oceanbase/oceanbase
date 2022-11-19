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

#ifndef OCEANBASE_RESOLVER_CMD_OB_SWITCHOVER_TENANT_RESOLVER_H
#define OCEANBASE_RESOLVER_CMD_OB_SWITCHOVER_TENANT_RESOLVER_H

#include "sql/resolver/cmd/ob_system_cmd_resolver.h"
#include "sql/resolver/cmd/ob_switch_tenant_stmt.h"

namespace oceanbase
{
namespace sql
{


class ObSwitchTenantResolver : public ObSystemCmdResolver
{
public:
  ObSwitchTenantResolver(ObResolverParams &params) : ObSystemCmdResolver(params)
  {
  }
  virtual ~ObSwitchTenantResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_switch_tenant(const ParseNode &parse_tree);
};

template<class T>
int resolve_tenant_name(
    T &stmt,
    const ParseNode *node,
    const uint64_t effective_tenant_id)
{
  int ret = OB_SUCCESS;
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
      ObString tenant_name;
      tenant_name.assign_ptr((char *)(tenant_name_node->str_value_),
                            static_cast<int32_t>(tenant_name_node->str_len_));
      stmt.set_tenant_name(tenant_name);
    }
  }
  return ret;
}

} //end sql
} //end oceanbase

#endif
