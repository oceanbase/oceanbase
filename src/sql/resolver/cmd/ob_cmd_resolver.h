/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_CMD_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_CMD_RESOLVER_

#include "share/schema/ob_table_schema.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ob_resolver_define.h"
namespace oceanbase
{
namespace sql
{
class ObCMDResolver : public ObStmtResolver
{
public:
  explicit ObCMDResolver(ObResolverParams &params) : ObStmtResolver(params) {}
  virtual ~ObCMDResolver() {}
private:
  DISALLOW_COPY_AND_ASSIGN(ObCMDResolver);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_CMD_OB_CMD_RESOLVER_*/
