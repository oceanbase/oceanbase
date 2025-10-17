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

#include "sql/engine/cmd/ob_alter_ls_executor.h"

#include "sql/engine/ob_exec_context.h"
#include "share/ob_common_rpc_proxy.h"


namespace oceanbase {
namespace sql {
int ObAlterLSExecutor::execute(ObExecContext& ctx, ObAlterLSStmt& stmt)
{
  int ret = OB_SUCCESS;
  int64_t begin_ts = ObTimeUtility::current_time();
  const share::ObAlterLSArg &arg = stmt.get_arg();
  share::ObAlterLSRes result;
  const uint64_t tenant_id = arg.get_tenant_id();
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObSrvRpcProxy *rpc_proxy =NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", KR(ret));
  } else if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service is null", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invaid argument", KR(ret), K(arg));
  } else {
    rpc_proxy = task_exec_ctx->get_srv_rpc();
    ObAddr leader;
    int64_t timeout = THIS_WORKER.get_timeout_remain();
    if (OB_ISNULL(rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rpc_proxy is null", KR(ret), K(arg));
    } else if (OB_FAIL(GCTX.location_service_->get_leader(
            GCONF.cluster_id, tenant_id, SYS_LS, false, leader))) {
      LOG_WARN("failed to get leader", KR(ret), K(tenant_id));
    } else if (OB_FAIL(rpc_proxy->to(leader).timeout(timeout)
          .by(tenant_id).admin_alter_ls(arg, result))) {
      LOG_WARN("failed to alter ls", KR(ret), K(arg), K(leader), K(timeout));
    }
  }
  int64_t end_ts = ObTimeUtility::current_time();
  LOG_INFO("[ALTER LS] execution end", KR(ret), K(result), "cost_time", end_ts - begin_ts);
  return ret;
}
}
} // oceanbase