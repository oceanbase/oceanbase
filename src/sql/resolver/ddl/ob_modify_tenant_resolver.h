/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_MODIFY_TENANT_RESOLVER_H
#define _OB_MODIFY_TENANT_RESOLVER_H 1
#include "sql/resolver/ddl/ob_modify_tenant_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "lib/container/ob_se_array.h"
namespace oceanbase
{
namespace sql
{
class ObModifyTenantResolver: public ObDDLResolver
{
public:
  explicit ObModifyTenantResolver(ObResolverParams &params);
  virtual ~ObModifyTenantResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObModifyTenantResolver);

private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_MODIFY_TENANT_RESOLVER_H */
