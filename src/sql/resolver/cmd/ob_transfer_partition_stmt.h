/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_TRANSFER_PARTITION_STMT_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_TRANSFER_PARTITION_STMT_

#include "rootserver/ob_transfer_partition_command.h"
#include "sql/resolver/cmd/ob_system_cmd_stmt.h"


namespace oceanbase
{
namespace sql
{
class ObTransferPartitionStmt : public ObSystemCmdStmt
{
public:
  ObTransferPartitionStmt()
    : ObSystemCmdStmt(stmt::T_TRANSFER_PARTITION),
      arg_() {}
  virtual ~ObTransferPartitionStmt() {}

  rootserver::ObTransferPartitionArg &get_arg() { return arg_; }
private:
  rootserver::ObTransferPartitionArg arg_;
};
} // share
} // oceanbase
#endif // OCEANBASE_SQL_RESOLVER_CMD_OB_TRANSFER_PARTITION_STMT_