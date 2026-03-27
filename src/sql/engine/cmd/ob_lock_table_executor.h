/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCRANBASE_SQL_ENGINE_CMD_OB_LOCK_TABLE_EXECUTOR_
#define OCRANBASE_SQL_ENGINE_CMD_OB_LOCK_TABLE_EXECUTOR_

#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{

class ObExecContext;
class ObLockTableStmt;
class ObLockTableExecutor
{
public:
  ObLockTableExecutor() {}
  virtual ~ObLockTableExecutor() {}
  int execute(ObExecContext &ctx, ObLockTableStmt &stmt);
private:
  int execute_oracle_(ObExecContext &ctx, ObLockTableStmt &stmt);
  int execute_mysql_(ObExecContext &ctx, ObLockTableStmt &stmt);
  DISALLOW_COPY_AND_ASSIGN(ObLockTableExecutor);
};

} //sql
} // oceanbase
#endif
