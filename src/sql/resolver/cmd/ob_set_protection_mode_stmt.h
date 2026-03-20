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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_SET_PROTECTION_MODE_STMT_H_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_SET_PROTECTION_MODE_STMT_H_
#include "sql/resolver/cmd/ob_system_cmd_stmt.h"
#include "share/ob_tenant_role.h"

namespace oceanbase
{
namespace sql
{
class ObSetProtectionModeStmt : public ObSystemCmdStmt
{
public:
  ObSetProtectionModeStmt() : ObSystemCmdStmt(stmt::T_SET_PROTECTION_MODE), arg_() {}
  virtual ~ObSetProtectionModeStmt() {}
  share::ObSetProtectionModeArg &get_arg() { return arg_; }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(arg));
private:
  share::ObSetProtectionModeArg arg_;
};
} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_RESOLVER_CMD_OB_SET_PROTECTION_MODE_STMT_H_