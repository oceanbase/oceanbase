/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_FLASHBACK_STANDBY_LOG_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_FLASHBACK_STANDBY_LOG_RESOLVER_

#include "sql/resolver/cmd/ob_flashback_standby_log_stmt.h"
#include "sql/resolver/cmd/ob_system_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObFlashbackStandbyLogStmt;
class ObFlashbackStandbyLogResolver : public ObSystemCmdResolver
{
public:
  ObFlashbackStandbyLogResolver(ObResolverParams &params) :  ObSystemCmdResolver(params) {}
  virtual ~ObFlashbackStandbyLogResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
};
}// namespace sql
}// namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_CMD_OB_FLASHBACK_STANDBY_LOG_RESOLVER_ */