/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_CLONE_TENANT_RESOLVER_H
#define _OB_CLONE_TENANT_RESOLVER_H
#include "sql/resolver/cmd/ob_cmd_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObCloneTenantResolver : public ObCMDResolver
{
public:
  explicit ObCloneTenantResolver(ObResolverParams &params);
  virtual ~ObCloneTenantResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_option_list_(const ParseNode *node,
                           ObString &resource_pool_name,
                           ObString &unit_config_name);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCloneTenantResolver);
};

}
}

#endif /*_OB_CREATE_SNAPSHOT_RESOLVER_H*/
