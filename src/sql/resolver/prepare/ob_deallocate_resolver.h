/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_DEALLOCATE_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DML_OB_DEALLOCATE_RESOLVER_H_

#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/prepare/ob_deallocate_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObDeallocateResolver : public ObStmtResolver
{
public:
  explicit ObDeallocateResolver(ObResolverParams &params) : ObStmtResolver(params){}
  virtual ~ObDeallocateResolver() {}

  virtual int resolve(const ParseNode &parse_tree);
  ObDeallocateStmt *get_deallocate_stmt() { return static_cast<ObDeallocateStmt*>(stmt_); }

private:
  DISALLOW_COPY_AND_ASSIGN(ObDeallocateResolver);
};

} // namespace sql
}  // namespace oceanbase

#endif /*OCEANBASE_SQL_RESOLVER_DML_OB_DEALLOCATE_RESOLVER_H_*/
