/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DROP_DATABASE_RESOLVER_H
#define OCEANBASE_SQL_OB_DROP_DATABASE_RESOLVER_H
#include "sql/resolver/ddl/ob_ddl_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObDropDatabaseResolver: public ObDDLResolver
{
public:
  const static int64_t IF_EXIST = 0;
  const static int64_t DBNAME = 1;
  const static int64_t DB_NODE_COUNT = 2;

  explicit ObDropDatabaseResolver(ObResolverParams &params);
  virtual ~ObDropDatabaseResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDropDatabaseResolver);

private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_DROP_DATABASE_RESOLVER_H */
