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

int resolve_tenant_name(
    const ParseNode *node,
    const uint64_t effective_tenant_id,
    ObString &tenant_name);

} //end sql
} //end oceanbase

#endif
