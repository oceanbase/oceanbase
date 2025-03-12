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
#include "sql/engine/cmd/ob_context_executor.h"
#include "sql/resolver/ddl/ob_context_stmt.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

#define DEF_SIMPLE_EXECUTOR_IMPL(name, func) \
int name##Executor::execute(ObExecContext &ctx, name##Stmt &stmt) \
{ \
  int ret = OB_SUCCESS; \
  ObTaskExecutorCtx *task_exec_ctx = NULL; \
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL; \
  const obrpc::ObContextDDLArg &context_arg = stmt.get_arg(); \
  ObString first_stmt; \
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) { \
    LOG_WARN("fail to get first stmt" , K(ret)); \
  } else { \
    const_cast<obrpc::ObContextDDLArg&>(context_arg).ddl_stmt_str_ = first_stmt; \
  } \
  if (OB_FAIL(ret)) { \
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) { \
    ret = OB_NOT_INIT; \
    LOG_WARN("get task executor context failed"); \
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) { \
    ret = OB_NOT_INIT; \
    LOG_WARN("get common rpc proxy failed"); \
  } else if (OB_FAIL(common_rpc_proxy->func(context_arg))) { \
    LOG_WARN("rpc proxy failed", K(context_arg),  K(ret)); \
  } \
  return ret; \
}

DEF_SIMPLE_EXECUTOR_IMPL(ObCreateContext, do_context_ddl);
DEF_SIMPLE_EXECUTOR_IMPL(ObDropContext, do_context_ddl);

#undef DEF_SIMPLE_EXECUTOR_IMPL
}  // namespace sql
}  // namespace oceanbase
