/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_ALTER_DATABASE_RESOLVER_
#define OCEANBASE_SQL_OB_ALTER_DATABASE_RESOLVER_
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObAlterDatabaseResolver: public ObDDLResolver
{
  static const int64_t DBNAME = 0;
  static const int64_t DATABASE_OPTION = 1;
  static const int64_t DATABASE_NODE_COUNT = 2;
public:
  explicit ObAlterDatabaseResolver(ObResolverParams &params);
  virtual ~ObAlterDatabaseResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAlterDatabaseResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_ALTER_DATABASE_RESOLVER_*/
