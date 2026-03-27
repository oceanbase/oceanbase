/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_SERVICE_NAME_STMT_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_SERVICE_NAME_STMT_

#include "share/ob_service_name_proxy.h"
#include "sql/resolver/cmd/ob_system_cmd_stmt.h"


namespace oceanbase
{
namespace sql
{
class ObServiceNameStmt : public ObSystemCmdStmt
{
public:
  ObServiceNameStmt() : ObSystemCmdStmt(stmt::T_SERVICE_NAME), arg_() {}
  virtual ~ObServiceNameStmt() {}
  share::ObServiceNameArg &get_arg() { return arg_; }
private:
  share::ObServiceNameArg arg_;
};
} // share
} // oceanbase
#endif // OCEANBASE_SQL_RESOLVER_CMD_OB_SERVICE_NAME_STMT_