/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_CREATE_TABLE_LIKE_RESOLVER_
#define OCEANBASE_SQL_OB_CREATE_TABLE_LIKE_RESOLVER_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ddl/ob_create_table_like_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObCreateTableLikeResolver : public ObDDLResolver
{
public:
  explicit ObCreateTableLikeResolver(ObResolverParams &params);
  virtual ~ObCreateTableLikeResolver();
  virtual int resolve(const ParseNode &parse_tree);
  ObCreateTableLikeStmt *get_create_table_like_stmt()
  {
    return static_cast<ObCreateTableLikeStmt*>(stmt_);
  };

private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateTableLikeResolver);
};


} //end namespace sql
} //end namespace oceanbase

#endif // OCEANBASE_SQL_OB_CREATE_TABLE_LIKE_RESOLVER_

