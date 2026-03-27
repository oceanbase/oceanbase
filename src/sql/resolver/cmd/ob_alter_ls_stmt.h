/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_ALTER_LS_STMT_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_ALTER_LS_STMT_

#include "share/ls/ob_alter_ls_struct.h"
#include "sql/resolver/cmd/ob_system_cmd_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObAlterLSStmt : public ObSystemCmdStmt
{
public:
  ObAlterLSStmt() : ObSystemCmdStmt(stmt::T_ALTER_LS), arg_() {}
  virtual ~ObAlterLSStmt() {}
  share::ObAlterLSArg &get_arg() { return arg_; }
private:
  share::ObAlterLSArg arg_;
};
} // share
} // oceanbase
#endif // OCEANBASE_SQL_RESOLVER_CMD_OB_ALTER_LS_STMT_