/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_CMD_SYSTEM_CMD_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_CMD_SYSTEM_CMD_RESOLVER_

#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObSystemCmdResolver : public ObStmtResolver
{
public:
  explicit ObSystemCmdResolver(ObResolverParams &params) : ObStmtResolver(params)
  {}
  virtual ~ObSystemCmdResolver()
  {}
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObSystemCmdResolver);
};
}// namespace sql
}// namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_CMD_SYSTEM_CMD_RESOLVER_ */
