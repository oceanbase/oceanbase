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

#ifndef OCEANBASE_SRC_SQL_ENGINE_CMD_OB_GET_DIAGNOSTICS_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_CMD_OB_GET_DIAGNOSTICS_EXECUTOR_H_
#include "sql/resolver/cmd/ob_variable_set_stmt.h"
#include "sql/engine/cmd/ob_variable_set_executor.h"
#include "sql/resolver/cmd/ob_get_diagnostics_stmt.h"
#include "sql/resolver/cmd/ob_get_diagnostics_resolver.h"
#include "share/ob_define.h"
#include "sql/session/ob_session_val_map.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObExprCtx;
// namespace sqlclient
// {
class ObMySQLProxy;
// }
}
namespace sql
{
class ObExecContext;
class ObSQLSessionInfo;
class ObPhysicalPlanCtx;
class ObGetDiagnosticsExecutor : public ObVariableSetExecutor
{
public:
  ObGetDiagnosticsExecutor();
  virtual ~ObGetDiagnosticsExecutor();
  int execute(ObExecContext &ctx, ObGetDiagnosticsStmt &stmt);
  int get_condition_num(ObExecContext &ctx, ObGetDiagnosticsStmt &stmt, int64_t &num);
  int assign_condition_val(ObExecContext &ctx, ObGetDiagnosticsStmt &stmt,
                          ObSQLSessionInfo *session_info,
                          sqlclient::ObISQLConnection *conn_write,
                          int64_t err_ret, ObString err_msg_c, ObString sql_state_c);
  
private:
  DISALLOW_COPY_AND_ASSIGN(ObGetDiagnosticsExecutor);
};
}
}
#endif /* OCEANBASE_SRC_SQL_ENGINE_CMD_OB_GET_DIAGNOSTICS_EXECUTOR_ */

