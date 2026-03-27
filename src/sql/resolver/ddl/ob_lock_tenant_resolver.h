/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_LOCK_TENANT_RESOLVER_H
#define _OB_LOCK_TENANT_RESOLVER_H 1
#include "sql/resolver/ddl/ob_lock_tenant_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "lib/container/ob_se_array.h"
namespace oceanbase
{
namespace sql
{
class ObLockTenantResolver: public ObDDLResolver
{
public:
  explicit ObLockTenantResolver(ObResolverParams &params);
  virtual ~ObLockTenantResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLockTenantResolver);

private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_LOCK_TENANT_RESOLVER_H */
