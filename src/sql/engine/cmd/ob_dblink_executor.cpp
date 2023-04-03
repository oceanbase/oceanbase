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

#include "sql/engine/cmd/ob_dblink_executor.h"
#include "sql/resolver/ddl/ob_create_dblink_stmt.h"
#include "sql/resolver/ddl/ob_drop_dblink_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/ob_common_rpc_proxy.h"
#include "lib/worker.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObCreateDbLinkExecutor::execute(ObExecContext &ctx, ObCreateDbLinkStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObCreateDbLinkArg &create_dblink_arg = stmt.get_create_dblink_arg();
  if (OB_ISNULL(ctx.get_stmt_factory()) || OB_ISNULL(ctx.get_stmt_factory()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "query ctx is null", K(ret));
  } else {
    const_cast<obrpc::ObCreateDbLinkArg&>(create_dblink_arg).ddl_stmt_str_ =
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
   } else if (OB_FAIL(common_rpc_proxy->create_dblink(create_dblink_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy create dblink failed", K(ret));
   } else {
    ctx.get_physical_plan_ctx()->set_affected_rows(1);
  }
  SQL_ENG_LOG(INFO, "finish execute create dblink.", K(ret), K(stmt));
  return ret;
}

int ObDropDbLinkExecutor::execute(ObExecContext &ctx, ObDropDbLinkStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObDropDbLinkArg &drop_dblink_arg = stmt.get_drop_dblink_arg();
  if (OB_ISNULL(ctx.get_stmt_factory()) || OB_ISNULL(ctx.get_stmt_factory()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "query ctx is null", K(ret));
  } else {
    const_cast<obrpc::ObDropDbLinkArg&>(drop_dblink_arg).ddl_stmt_str_ =
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
   } else if (OB_FAIL(common_rpc_proxy->drop_dblink(drop_dblink_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy drop dblink failed", K(ret));
   } else {
    ctx.get_physical_plan_ctx()->set_affected_rows(1);
  }
  SQL_ENG_LOG(INFO, "finish execute drop dblink.", K(ret), K(stmt));
  return ret;
}

}
}
