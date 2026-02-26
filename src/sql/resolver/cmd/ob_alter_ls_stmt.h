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