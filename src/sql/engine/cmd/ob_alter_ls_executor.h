/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_ALTER_LS_EXECUTOR_
#define OCEANBASE_SQL_ENGINE_CMD_OB_ALTER_LS_EXECUTOR_

#include "sql/resolver/cmd/ob_alter_ls_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObAlterLSExecutor
{
public:
  ObAlterLSExecutor() {}
  virtual ~ObAlterLSExecutor() {}
  int execute(ObExecContext &ctx, ObAlterLSStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterLSExecutor);
};
}
}
#endif // OCEANBASE_SQL_ENGINE_CMD_OB_ALTER_LS_EXECUTOR_