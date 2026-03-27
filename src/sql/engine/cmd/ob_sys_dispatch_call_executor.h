/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
