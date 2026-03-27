/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_SERVICE_NAME_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_SERVICE_NAME_RESOLVER_

#include "sql/resolver/cmd/ob_service_name_stmt.h"
#include "sql/resolver/cmd/ob_system_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObServiceNameStmt;
class ObServiceNameResolver : public ObSystemCmdResolver
{
public:
  ObServiceNameResolver(ObResolverParams &params) :  ObSystemCmdResolver(params) {}
  virtual ~ObServiceNameResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
};
}// namespace sql
}// namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_CMD_OB_SERVICE_NAME_RESOLVER_ */