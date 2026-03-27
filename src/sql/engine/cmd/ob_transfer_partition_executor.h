/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_TRANSFER_PARTITION_EXECUTOR_
#define OCEANBASE_SQL_ENGINE_CMD_OB_TRANSFER_PARTITION_EXECUTOR_

#include "sql/resolver/cmd/ob_transfer_partition_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObTransferPartitionExecutor
{
public:
  ObTransferPartitionExecutor() {}
  virtual ~ObTransferPartitionExecutor() {}
  int execute(ObExecContext &ctx, ObTransferPartitionStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransferPartitionExecutor);
};
}
}
#endif // OCEANBASE_SQL_ENGINE_CMD_OB_TRANSFER_PARTITION_EXECUTOR_