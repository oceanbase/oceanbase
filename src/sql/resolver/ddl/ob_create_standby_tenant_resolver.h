/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_CREATE_STANDBY_TENANT_RESOLVER_
#define OCEANBASE_CREATE_STANDBY_TENANT_RESOLVER_ 1

#include "sql/resolver/ddl/ob_create_tenant_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObCreateStandbyTenantResolver: public ObDDLResolver
{
public:
  explicit ObCreateStandbyTenantResolver(ObResolverParams &params);
  virtual ~ObCreateStandbyTenantResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_log_restore_source_(ObCreateTenantStmt *stmt, ParseNode *log_restore_source_node) const;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateStandbyTenantResolver);
  // function members

private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_CREATE_STANDBY_TENANT_RESOLVER_ */
