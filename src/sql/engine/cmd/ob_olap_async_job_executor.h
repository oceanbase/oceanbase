/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OLAP_ASYNC_JOB_CMD_EXECUTOR_
#define OCEANBASE_SQL_ENGINE_CMD_OLAP_ASYNC_JOB_CMD_EXECUTOR_
#include "lib/string/ob_string.h"
#include "lib/container/ob_array_serialization.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObOLAPAsyncCancelJobStmt;

class ObOLAPAsyncCancelJobExecutor
{
public:
  ObOLAPAsyncCancelJobExecutor() {}
  virtual ~ObOLAPAsyncCancelJobExecutor() {}
  int execute(ObExecContext &ctx, ObOLAPAsyncCancelJobStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObOLAPAsyncCancelJobExecutor);
};
}
}
#endif //OCEANBASE_SQL_ENGINE_CMD_OLAP_ASYNC_JOB_CMD_EXECUTOR_
