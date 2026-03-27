/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_CREATE_TENANT_RESOLVER_
#define OCEANBASE_CREATE_TENANT_RESOLVER_ 1

#include "sql/resolver/ddl/ob_create_tenant_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObCreateTenantResolver: public ObDDLResolver
{
public:
  explicit ObCreateTenantResolver(ObResolverParams &params);
  virtual ~ObCreateTenantResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateTenantResolver);
  // function members

private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_CREATE_TENANT_RESOLVER_ */
