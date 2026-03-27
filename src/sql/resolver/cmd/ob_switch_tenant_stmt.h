/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_RESOLVER_CMD_OB_SWITCHOVER_TENANT_STMT_H
#define OCEANBASE_RESOLVER_CMD_OB_SWITCHOVER_TENANT_STMT_H

#include "sql/resolver/cmd/ob_system_cmd_stmt.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace sql
{
class ObSwitchTenantStmt : public ObSystemCmdStmt
{
public:
  ObSwitchTenantStmt() : ObSystemCmdStmt(stmt::T_SWITCHOVER), arg_()
  {
  }
  virtual ~ObSwitchTenantStmt()
  {
  }

  obrpc::ObSwitchTenantArg  &get_arg() { return arg_; }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(arg));
private:
  obrpc::ObSwitchTenantArg arg_;
};

} //end sql
} //end oceanbase
#endif

