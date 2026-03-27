/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_SERVICE_NAME_EXECUTOR_
#define OCEANBASE_SQL_ENGINE_CMD_OB_SERVICE_NAME_EXECUTOR_

#include "sql/resolver/cmd/ob_service_name_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObServiceNameExecutor
{
public:
  ObServiceNameExecutor() {}
  virtual ~ObServiceNameExecutor() {}
  int execute(ObExecContext &ctx, ObServiceNameStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObServiceNameExecutor);
};
}
}
#endif // OCEANBASE_SQL_ENGINE_CMD_OB_SERVICE_NAME_EXECUTOR_