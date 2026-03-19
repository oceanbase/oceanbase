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

#include "sql/engine/cmd/ob_alter_view_executor.h"
#include "sql/resolver/ddl/ob_alter_view_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
namespace sql
{

ObAlterViewExecutor::ObAlterViewExecutor()
{
}

ObAlterViewExecutor::~ObAlterViewExecutor()
{
}

int ObAlterViewExecutor::execute(ObExecContext &ctx, ObAlterViewStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  obrpc::ObAlterViewArg &alter_view_arg = stmt.get_alter_view_arg();
  obrpc::ObAlterViewRes res;
  LOG_DEBUG("start of alter view execute", K(alter_view_arg));
  if (OB_ISNULL(ctx.get_stmt_factory()) || OB_ISNULL(ctx.get_stmt_factory()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ctx is null", K(ret));
  } else {
    alter_view_arg.ddl_stmt_str_ = ctx.get_stmt_factory()->get_query_ctx()->get_sql_stmt();
    if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed", K(ret));
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("get common rpc proxy failed", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy should not be null", K(ret));
    } else if (alter_view_arg.is_valid() && OB_FAIL(common_rpc_proxy->alter_view(alter_view_arg, res))) {
      LOG_WARN("rpc proxy alter view failed", K(ret), "dst", common_rpc_proxy->get_server(), K(alter_view_arg));
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
