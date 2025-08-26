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