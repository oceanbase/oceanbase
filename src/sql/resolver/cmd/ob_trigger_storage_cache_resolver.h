/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SQL_RESOLVER_CMD_OB_TRIGGER_STORAGE_CACHE_RESOLVER_H
#define SQL_RESOLVER_CMD_OB_TRIGGER_STORAGE_CACHE_RESOLVER_H

#include "sql/resolver/cmd/ob_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObTriggerStorageCacheStmt;

class ObTriggerStorageCacheResolver : public ObCMDResolver
{
public:
  explicit ObTriggerStorageCacheResolver(ObResolverParams &params);
  virtual ~ObTriggerStorageCacheResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_tenant_name_(ObTriggerStorageCacheStmt *stmt, ParseNode *name);
  DISALLOW_COPY_AND_ASSIGN(ObTriggerStorageCacheResolver);
};
} // end namespace sql
} // end namespace oceanbase

#endif /*_OB_TENANT_SNAPSHOT_RESOLVER_H*/
