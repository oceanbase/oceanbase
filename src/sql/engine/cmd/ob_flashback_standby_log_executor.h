/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_FLASHBACK_STANDBY_LOG_EXECUTOR_
#define OCEANBASE_SQL_ENGINE_CMD_OB_FLASHBACK_STANDBY_LOG_EXECUTOR_

#include "sql/resolver/cmd/ob_flashback_standby_log_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObFlashbackStandbyLogExecutor
{
public:
  ObFlashbackStandbyLogExecutor() {}
  virtual ~ObFlashbackStandbyLogExecutor() {}
  int execute(ObExecContext &ctx, ObFlashbackStandbyLogStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObFlashbackStandbyLogExecutor);
};
}
}
#endif // OCEANBASE_SQL_ENGINE_CMD_OB_FLASHBACK_STANDBY_LOG_EXECUTOR_