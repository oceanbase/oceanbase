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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_FLASHBACK_STANDBY_LOG_STMT_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_FLASHBACK_STANDBY_LOG_STMT_

#include "share/ob_flashback_standby_log_struct.h"
#include "sql/resolver/cmd/ob_system_cmd_stmt.h"


namespace oceanbase
{
namespace sql
{
class ObFlashbackStandbyLogStmt : public ObSystemCmdStmt
{
public:
  ObFlashbackStandbyLogStmt() : ObSystemCmdStmt(stmt::T_FLASHBACK_STANDBY_LOG), arg_() {}
  virtual ~ObFlashbackStandbyLogStmt() {}
  share::ObFlashbackStandbyLogArg &get_arg() { return arg_; }
private:
  share::ObFlashbackStandbyLogArg arg_;
};
} // share
} // oceanbase
#endif // OCEANBASE_SQL_RESOLVER_CMD_OB_FLASHBACK_STANDBY_LOG_STMT_