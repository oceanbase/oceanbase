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

#ifndef OCEANBASE_SRC_SQL_ENGINE_CMD_OB_SYS_DISPATCH_CALL_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_CMD_OB_SYS_DISPATCH_CALL_EXECUTOR_H_

#include <cstdint>

#include "lib/utility/ob_macro_utils.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{

using namespace common;

namespace sql
{
class ObExecContext;
class ObSysDispatchCallStmt;
class ObSQLSessionInfo;
class ObFreeSessionCtx;

class ObSysDispatchCallExecutor
{
public:
  ObSysDispatchCallExecutor() {}
  virtual ~ObSysDispatchCallExecutor() {}
  DISABLE_COPY_ASSIGN(ObSysDispatchCallExecutor);

  int execute(ObExecContext &ctx, ObSysDispatchCallStmt &stmt);

private:
  int create_session(const uint64_t tenant_id,
                     ObFreeSessionCtx &free_session_ctx,
                     ObSQLSessionInfo *&session_info);
  int init_session(ObSQLSessionInfo &session,
                   const uint64_t tenant_id,
                   const ObString &tenant_name,
                   const ObCompatibilityMode compat_mode);
  int destroy_session(ObFreeSessionCtx &free_session_ctx, ObSQLSessionInfo *session_info);
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SRC_SQL_ENGINE_CMD_OB_SYS_DISPATCH_CALL_EXECUTOR_H_
