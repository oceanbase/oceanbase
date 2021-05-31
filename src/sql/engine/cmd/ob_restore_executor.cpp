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
#include "share/ob_common_rpc_proxy.h"
#include "sql/resolver/cmd/ob_alter_system_stmt.h"
#include "sql/engine/cmd/ob_restore_executor.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {

ObRestoreTenantExecutor::ObRestoreTenantExecutor()
{}

ObRestoreTenantExecutor::~ObRestoreTenantExecutor()
{}

int ObRestoreTenantExecutor::execute(ObExecContext& ctx, ObRestoreTenantStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
  const obrpc::ObRestoreTenantArg& restore_tenant_arg = stmt.get_rpc_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt", K(ret));
  } else {
    const_cast<obrpc::ObRestoreTenantArg&>(restore_tenant_arg).sql_text_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->restore_tenant(restore_tenant_arg))) {
    LOG_WARN("rpc proxy restore tenant failed", K(ret), "dst", common_rpc_proxy->get_server());
  }
  return ret;
}

ObPhysicalRestoreTenantExecutor::ObPhysicalRestoreTenantExecutor()
{}

ObPhysicalRestoreTenantExecutor::~ObPhysicalRestoreTenantExecutor()
{}

int ObPhysicalRestoreTenantExecutor::execute(ObExecContext& ctx, ObPhysicalRestoreTenantStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
  const obrpc::ObPhysicalRestoreTenantArg& restore_tenant_arg = stmt.get_rpc_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt", K(ret));
  } else {
    const_cast<obrpc::ObPhysicalRestoreTenantArg&>(restore_tenant_arg).sql_text_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->physical_restore_tenant(restore_tenant_arg))) {
    LOG_WARN("rpc proxy restore tenant failed", K(ret), "dst", common_rpc_proxy->get_server());
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
