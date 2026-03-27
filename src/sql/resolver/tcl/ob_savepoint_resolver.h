/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_TCL_OB_SAVEPOINT_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_TCL_OB_SAVEPOINT_RESOLVER_

#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObSavePointStmt;
class ObSavePointResolver : public ObStmtResolver
{
public:
  explicit ObSavePointResolver(ObResolverParams &params)
    : ObStmtResolver(params)
  {}
  virtual ~ObSavePointResolver()
  {}
  virtual int resolve(const ParseNode &parse_tree);
  int create_savepoint_stmt(ObItemType stmt_type, ObSavePointStmt *&stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSavePointResolver);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESOLVER_TCL_OB_SAVEPOINT_RESOLVER_

