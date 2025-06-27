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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_sensitive_rule_executor.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/ddl/ob_sensitive_rule_stmt.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"

namespace oceanbase
{
namespace sql
{
int ObSensitiveRuleExecutor::execute(ObExecContext &ctx, ObSensitiveRuleStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;

  stmt.get_ddl_arg().ddl_stmt_str_ = stmt.get_query_ctx()->get_sql_stmt();
  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed");
  } else if (OB_FAIL(common_rpc_proxy->handle_sensitive_rule_ddl(stmt.get_ddl_arg()))) {
    LOG_WARN("handle catalog ddl error", K(ret));
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase