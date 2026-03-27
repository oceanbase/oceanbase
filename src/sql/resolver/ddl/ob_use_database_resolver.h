/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_USE_DATABASE_RESOLVER_H
#define OCEANBASE_SQL_OB_USE_DATABASE_RESOLVER_H
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObUseDatabaseResolver: public ObDDLResolver
{
public:
  explicit ObUseDatabaseResolver(ObResolverParams &params);
  virtual ~ObUseDatabaseResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObUseDatabaseResolver);
  int resolve_database_factor_(const ParseNode *node, uint64_t &catalog_id, common::ObString &database_name);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_USE_DATABASE_RESOLVER_H */
