/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_FLASHBACK_STANDBY_LOG_STMT_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_FLASHBACK_STANDBY_LOG_STMT_

#include "share/ob_flashback_standby_log_struct.h"
#include "sql/resolver/cmd/ob_system_cmd_stmt.h"


namespace oceanbase
{
namespace sql
{
class ObFlashbackStandbyLogStmt : public ObSystemCmdStmt
{
public:
  ObFlashbackStandbyLogStmt() : ObSystemCmdStmt(stmt::T_FLASHBACK_STANDBY_LOG), arg_() {}
  virtual ~ObFlashbackStandbyLogStmt() {}
  share::ObFlashbackStandbyLogArg &get_arg() { return arg_; }
private:
  share::ObFlashbackStandbyLogArg arg_;
};
} // share
} // oceanbase
#endif // OCEANBASE_SQL_RESOLVER_CMD_OB_FLASHBACK_STANDBY_LOG_STMT_