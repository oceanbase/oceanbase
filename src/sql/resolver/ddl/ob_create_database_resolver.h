/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_CREATE_DATABASE_RESOLVER_H_
#define OCEANBASE_SQL_OB_CREATE_DATABASE_RESOLVER_H_
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObCreateDatabaseResolver: public ObDDLResolver
{
  static const int64_t IF_NOT_EXIST = 0;
  static const int64_t DBNAME = 1;
  static const int64_t DATABASE_OPTION = 2;
  static const int64_t DATABASE_NODE_COUNT = 3;
public:
  explicit ObCreateDatabaseResolver(ObResolverParams &params);
  virtual ~ObCreateDatabaseResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateDatabaseResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_CREATE_DATABASE_RESOLVER_H_*/
