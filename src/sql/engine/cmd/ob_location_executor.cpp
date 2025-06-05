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

#include "sql/engine/cmd/ob_location_executor.h"
#include "sql/resolver/ddl/ob_create_location_stmt.h"
#include "sql/resolver/ddl/ob_drop_location_stmt.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{
int ObCreateLocationExecutor::execute(ObExecContext &ctx, ObCreateLocationStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObCreateLocationArg &create_location_arg = stmt.get_create_location_arg();
  const_cast<obrpc::ObCreateLocationArg&>(create_location_arg).ddl_stmt_str_ = stmt.get_masked_sql();
  if (OB_FAIL(ret)) {
    // do nothing.
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    SQL_ENG_LOG(WARN, "get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy) || OB_ISNULL(ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "fail to get physical plan ctx", K(ret), K(ctx), K(common_rpc_proxy));
  } else if (OB_FAIL(common_rpc_proxy->create_location(create_location_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy create location failed", K(ret), K(create_location_arg));
  } else {
    ctx.get_physical_plan_ctx()->set_affected_rows(1);
  }
  SQL_ENG_LOG(INFO, "finish execute create location.", K(ret), K(stmt));
  return ret;
}

int ObDropLocationExecutor::execute(ObExecContext &ctx, ObDropLocationStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObDropLocationArg &drop_location_arg = stmt.get_drop_location_arg();
  if (OB_ISNULL(ctx.get_stmt_factory()) || OB_ISNULL(ctx.get_stmt_factory()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "query ctx is null", K(ret));
  } else {
    const_cast<obrpc::ObDropLocationArg&>(drop_location_arg).ddl_stmt_str_ =
                                        ctx.get_stmt_factory()->get_query_ctx()->get_sql_stmt();
  }
  if (OB_FAIL(ret)) {
    // do nothing.
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    SQL_ENG_LOG(WARN, "get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy) || OB_ISNULL(ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "fail to get physical plan ctx", K(ret), K(ctx), K(common_rpc_proxy));
  } else if (OB_FAIL(common_rpc_proxy->drop_location(drop_location_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy drop location failed", K(ret), K(drop_location_arg));
  } else {
    ctx.get_physical_plan_ctx()->set_affected_rows(1);
  }
  SQL_ENG_LOG(INFO, "finish execute drop location.", K(ret), K(stmt));
  return ret;
}
} // namespace sql
} // namespace oceanbase
