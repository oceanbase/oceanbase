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
#include "sql/engine/cmd/ob_udf_executor.h"
#include "sql/resolver/ddl/ob_create_func_stmt.h"
#include "sql/resolver/ddl/ob_drop_func_stmt.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

int ObCreateFuncExecutor::execute(ObExecContext &ctx, ObCreateFuncStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObCreateUserDefinedFunctionArg &create_udf_arg = stmt.get_create_func_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    const_cast<obrpc::ObCreateUserDefinedFunctionArg&>(create_udf_arg).ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->create_udf(create_udf_arg))) {
    LOG_WARN("rpc proxy create udf failed", K(ret),
                "dst", common_rpc_proxy->get_server());
  }
  return ret;
}

int ObDropFuncExecutor::execute(ObExecContext &ctx, ObDropFuncStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObDropUserDefinedFunctionArg &drop_func_arg = stmt.get_drop_func_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    const_cast<obrpc::ObDropUserDefinedFunctionArg&>(drop_func_arg).ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->drop_udf(drop_func_arg))) {
    LOG_WARN("rpc proxy create udf failed", K(ret),
                "dst", common_rpc_proxy->get_server());
  }
  return ret;
}
} //end namespace sql
} //end namespace oceanbase

