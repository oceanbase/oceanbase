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

#include "sql/engine/ob_exec_context.h"
#include "sql/engine/cmd/ob_resource_executor.h"
#include "sql/resolver/cmd/ob_resource_stmt.h"
#include "share/ob_common_rpc_proxy.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObCreateResourcePoolExecutor::ObCreateResourcePoolExecutor()
{
}

ObCreateResourcePoolExecutor::~ObCreateResourcePoolExecutor()
{
}

int ObCreateResourcePoolExecutor::execute(ObExecContext &ctx,
                                          ObCreateResourcePoolStmt &stmt) {
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObCreateResourcePoolArg &create_resource_pool_arg = stmt.get_arg();

  if (NULL == (task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (NULL == (common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get common rpc proxy failed");
  } else if (OB_SUCCESS != (ret = common_rpc_proxy->create_resource_pool(
              create_resource_pool_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy create resource_pool failed", K(ret));
  }
  return ret;
}

ObAlterResourcePoolExecutor::ObAlterResourcePoolExecutor()
{
}

ObAlterResourcePoolExecutor::~ObAlterResourcePoolExecutor()
{
}

int ObAlterResourcePoolExecutor::execute(ObExecContext &ctx, ObAlterResourcePoolStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObAlterResourcePoolArg &alter_resource_pool_arg = stmt.get_arg();

  if (NULL == (task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (NULL == (common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get common rpc proxy failed");
  } else if (OB_SUCCESS != (ret = common_rpc_proxy->alter_resource_pool(alter_resource_pool_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy alter resource_pool failed", K(ret));
  }
  return ret;
}

ObSplitResourcePoolExecutor::ObSplitResourcePoolExecutor()
{
}

ObSplitResourcePoolExecutor::~ObSplitResourcePoolExecutor()
{
}

int ObSplitResourcePoolExecutor::execute(ObExecContext &ctx, ObSplitResourcePoolStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObSplitResourcePoolArg &split_resource_pool_arg = stmt.get_arg();

  if (NULL == (task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed", K(ret));
  } else if (NULL == (common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get common rpc proxy failed", K(ret));
  } else if (OB_SUCCESS != (ret = common_rpc_proxy->split_resource_pool(split_resource_pool_arg))) {
    SQL_ENG_LOG(WARN, "send split resource pool failed", K(ret));
  }
  return ret;
}

ObMergeResourcePoolExecutor::ObMergeResourcePoolExecutor()
{
}

ObMergeResourcePoolExecutor::~ObMergeResourcePoolExecutor()
{
}

int ObMergeResourcePoolExecutor::execute(ObExecContext &ctx, ObMergeResourcePoolStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObMergeResourcePoolArg &merge_resource_pool_arg = stmt.get_arg();

  if (NULL == (task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (NULL == (common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get common rpc proxy failed");
  } else if (OB_SUCCESS != (ret = common_rpc_proxy->merge_resource_pool(merge_resource_pool_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy merge resource_pool failed", K(ret));
  }
  return ret;
}

ObAlterResourceTenantExecutor::ObAlterResourceTenantExecutor()
{
}

ObAlterResourceTenantExecutor::~ObAlterResourceTenantExecutor()
{
}

int ObAlterResourceTenantExecutor::execute(
    ObExecContext &ctx, ObAlterResourceTenantStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = nullptr;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = nullptr;
  obrpc::ObAlterResourceTenantArg &arg = stmt.get_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    SQL_ENG_LOG(WARN, "fail to get first stmt" , KR(ret));
  } else if (OB_UNLIKELY(nullptr == (task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx)))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed", KR(ret));
  } else if (OB_UNLIKELY(nullptr == (common_rpc_proxy = task_exec_ctx->get_common_rpc()))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get common rpc proxy failed", KR(ret));
  } else {
    arg.ddl_stmt_str_ = first_stmt;
  }
  if (FAILEDx(common_rpc_proxy->alter_resource_tenant(arg))) {
    SQL_ENG_LOG(WARN, "fail to send alter resource tenant rpc", KR(ret));
  }
  return ret;
}

ObDropResourcePoolExecutor::ObDropResourcePoolExecutor()
{
}

ObDropResourcePoolExecutor::~ObDropResourcePoolExecutor()
{
}

int ObDropResourcePoolExecutor::execute(ObExecContext &ctx, ObDropResourcePoolStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObDropResourcePoolArg &drop_resource_pool_arg = stmt.get_arg();

  if (NULL == (task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (NULL == (common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get common rpc proxy failed");
  } else if (OB_SUCCESS != (ret = common_rpc_proxy->drop_resource_pool(drop_resource_pool_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy drop resource_pool failed", K(ret));
  }
  return ret;
}




ObCreateResourceUnitExecutor::ObCreateResourceUnitExecutor()
{
}

ObCreateResourceUnitExecutor::~ObCreateResourceUnitExecutor()
{
}

int ObCreateResourceUnitExecutor::execute(ObExecContext &ctx, ObCreateResourceUnitStmt &stmt) {
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObCreateResourceUnitArg &create_resource_unit_arg = stmt.get_arg();

  if (NULL == (task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (NULL == (common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get common rpc proxy failed");
  } else if (OB_SUCCESS != (ret = common_rpc_proxy->create_resource_unit(create_resource_unit_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy create resource_unit failed", K(ret));
  }
  return ret;
}

ObAlterResourceUnitExecutor::ObAlterResourceUnitExecutor()
{
}

ObAlterResourceUnitExecutor::~ObAlterResourceUnitExecutor()
{
}

int ObAlterResourceUnitExecutor::execute(ObExecContext &ctx, ObAlterResourceUnitStmt &stmt) {
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObAlterResourceUnitArg &alter_resource_unit_arg = stmt.get_arg();

  if (NULL == (task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (NULL == (common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get common rpc proxy failed");
  } else if (OB_SUCCESS != (ret = common_rpc_proxy->alter_resource_unit(alter_resource_unit_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy alter resource_unit failed", K(ret));
  }
  return ret;
}

ObDropResourceUnitExecutor::ObDropResourceUnitExecutor()
{
}

ObDropResourceUnitExecutor::~ObDropResourceUnitExecutor()
{
}

int ObDropResourceUnitExecutor::execute(ObExecContext &ctx, ObDropResourceUnitStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObDropResourceUnitArg &drop_resource_unit_arg = stmt.get_arg();

  if (NULL == (task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (NULL == (common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get common rpc proxy failed");
  } else if (OB_SUCCESS != (ret = common_rpc_proxy->drop_resource_unit(drop_resource_unit_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy drop resource_unit failed", K(ret));
  }
  return ret;
}


}/* ns sql*/
}/* ns oceanbase */
