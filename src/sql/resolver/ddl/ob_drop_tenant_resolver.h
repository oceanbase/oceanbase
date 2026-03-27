/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_RESOLVER_DDL_DROP_TENANT_RESOLVER_
#define OCEANBASE_RESOLVER_DDL_DROP_TENANT_RESOLVER_ 1
#include "sql/resolver/ddl/ob_drop_tenant_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "lib/container/ob_se_array.h"
namespace oceanbase
{
namespace sql
{
class ObDropTenantResolver: public ObDDLResolver
{
public:
  explicit ObDropTenantResolver(ObResolverParams &params);
  virtual ~ObDropTenantResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDropTenantResolver);

private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_RESOLVER_DDL_DROP_TENANT_RESOLVER_ */
