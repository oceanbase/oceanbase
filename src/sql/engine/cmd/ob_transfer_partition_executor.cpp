/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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