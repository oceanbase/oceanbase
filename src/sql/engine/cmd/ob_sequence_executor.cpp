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
#include "sql/engine/cmd/ob_sequence_executor.h"
#include "share/ob_common_rpc_proxy.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ddl/ob_sequence_stmt.h"
#include "sql/engine/ob_exec_context.h"

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
  const obrpc::ObSequenceDDLArg &sequence_arg = stmt.get_arg(); \
  ObString first_stmt; \
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) { \
    LOG_WARN("fail to get first stmt" , K(ret)); \
  } else { \
    const_cast<obrpc::ObSequenceDDLArg&>(sequence_arg).ddl_stmt_str_ = first_stmt; \
  } \
  if (OB_FAIL(ret)) { \
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) { \
    ret = OB_NOT_INIT; \
    LOG_WARN("get task executor context failed"); \
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) { \
    ret = OB_NOT_INIT; \
    LOG_WARN("get common rpc proxy failed"); \
  } else if (OB_FAIL(common_rpc_proxy->func(sequence_arg))) { \
    LOG_WARN("rpc proxy failed", K(sequence_arg),  K(ret)); \
  } \
  return ret; \
}

DEF_SIMPLE_EXECUTOR_IMPL(ObCreateSequence, do_sequence_ddl);
DEF_SIMPLE_EXECUTOR_IMPL(ObAlterSequence,  do_sequence_ddl);
DEF_SIMPLE_EXECUTOR_IMPL(ObDropSequence,   do_sequence_ddl);

#undef DEF_SIMPLE_EXECUTOR_IMPL
}  // namespace sql
}  // namespace oceanbase
