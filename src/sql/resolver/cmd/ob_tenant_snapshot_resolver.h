/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_TENANT_SNAPSHOT_RESOLVER_H
#define _OB_TENANT_SNAPSHOT_RESOLVER_H

#include "sql/resolver/cmd/ob_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObCreateTenantSnapshotResolver : public ObCMDResolver
{
public:
  explicit ObCreateTenantSnapshotResolver(ObResolverParams &params);
  virtual ~ObCreateTenantSnapshotResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateTenantSnapshotResolver);
};

class ObDropTenantSnapshotResolver : public ObCMDResolver
{
public:
  explicit ObDropTenantSnapshotResolver(ObResolverParams &params);
  virtual ~ObDropTenantSnapshotResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropTenantSnapshotResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif /*_OB_TENANT_SNAPSHOT_RESOLVER_H*/
