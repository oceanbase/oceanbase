/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_PREPARE_OB_PREPARE_RESOLVER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_PREPARE_OB_PREPARE_RESOLVER_H_

#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/prepare/ob_prepare_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObPrepareResolver : public ObStmtResolver
{
public:
  explicit ObPrepareResolver(ObResolverParams &params) : ObStmtResolver(params) {}
  virtual ~ObPrepareResolver() {}

  virtual int resolve(const ParseNode &parse_tree);
  ObPrepareStmt *get_prepare_stmt() { return static_cast<ObPrepareStmt*>(stmt_); }

private:

};

}
}


#endif /* OCEANBASE_SRC_SQL_RESOLVER_PREPARE_OB_PREPARE_RESOLVER_H_ */
