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

