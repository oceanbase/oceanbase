/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_PREPARE_OB_EXECUTE_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_PREPARE_OB_EXECUTE_RESOLVER_

#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/prepare/ob_execute_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObExecuteResolver : public ObStmtResolver
{
public:
  explicit ObExecuteResolver(ObResolverParams &params) : ObStmtResolver(params) {}
  virtual ~ObExecuteResolver() {}

  virtual int resolve(const ParseNode &parse_tree);
  ObExecuteStmt *get_execute_stmt() { return static_cast<ObExecuteStmt*>(stmt_); }

private:

};

}
}
#endif /*OCEANBASE_SQL_RESOLVER_DML_OB_EXECUTE_RESOLVER_H_*/
