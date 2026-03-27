/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/cmd/ob_transfer_partition_executor.h"

#include "sql/engine/ob_exec_context.h"
#include "share/ob_common_rpc_proxy.h"


namespace oceanbase {
namespace sql {
int ObTransferPartitionExecutor::execute(ObExecContext& ctx, ObTransferPartitionStmt& stmt)
{
  int ret = OB_SUCCESS;
  const rootserver::ObTransferPartitionArg &arg = stmt.get_arg();
  rootserver::ObTransferPartitionCommand command;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invaid argument", KR(ret), K(arg));
  } else if (OB_FAIL(command.execute(arg))) {
    LOG_WARN("fail to execute command", KR(ret), K(arg));
  }
  return ret;
}
}
} // oceanbase