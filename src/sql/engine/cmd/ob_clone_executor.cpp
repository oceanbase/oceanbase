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
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/cmd/ob_clone_executor.h"
#include "sql/resolver/cmd/ob_tenant_clone_stmt.h"
#include "share/restore/ob_tenant_clone_table_operator.h"
#include "rootserver/restore/ob_tenant_clone_util.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace rootserver;
namespace sql
{

int ObCloneTenantExecutor::execute(ObExecContext &ctx, ObCloneTenantStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObCloneTenantArg &clone_tenant_arg = stmt.get_clone_tenant_arg();
  obrpc::ObCloneTenantRes clone_tenant_res;
  const int64_t abs_timeout = ObTimeUtility::current_time() + GCONF._ob_ddl_timeout;
  THIS_WORKER.set_timeout_ts(abs_timeout);

  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", KR(ret));
  } else if (true == stmt.get_if_not_exists()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("if not exists is true", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "clone tenant with IF NOT EXISTS");
  } else if (OB_ISNULL(ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx is null", KR(ret));
  } else if (FALSE_IT(ctx.get_physical_plan_ctx()->set_timeout_timestamp(abs_timeout))) {
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", KR(ret));
  } else if (!clone_tenant_arg.get_tenant_snapshot_name().empty()) {
    // TODO: support tenant snapshot in future
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("clone tenant with using snapshot clause is not supported", KR(ret), K(clone_tenant_arg));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "clone tenant with using snapshot clause is");
  } else if (OB_FAIL(common_rpc_proxy->clone_tenant(clone_tenant_arg, clone_tenant_res))) {
    LOG_WARN("rpc proxy clone tenant failed", KR(ret), K(clone_tenant_arg));
  } else if (!clone_tenant_res.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is not valid", KR(ret), K(clone_tenant_res));
  } else if (OB_FAIL(wait_clone_tenant_finished_(ctx, clone_tenant_res.get_job_id()))) {
    LOG_WARN("wait clone tenant finish failed", KR(ret), K(clone_tenant_res));
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_WAIT_CLONE_TENANT_FINISHED_ERROR);
int ObCloneTenantExecutor::wait_clone_tenant_finished_(ObExecContext &ctx,
                                                       const int64_t job_id)
{
  int ret = OB_SUCCESS;
  ObCloneJob job;
  common::ObMySQLProxy *sql_proxy = nullptr;
  ObTimeoutCtx timeout_ctx;
  const int64_t trx_timeout = 10 * 60 * 1000 * 1000; // 10min
  const int64_t abs_timeout = ObTimeUtility::current_time() + OB_MAX_USER_SPECIFIED_TIMEOUT; // 102 years
  THIS_WORKER.set_timeout_ts(abs_timeout);

  if (OB_UNLIKELY(job_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id));
  } else if (OB_ISNULL(ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx is null", KR(ret));
  } else if (FALSE_IT(ctx.get_physical_plan_ctx()->set_timeout_timestamp(abs_timeout))) {
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(trx_timeout))) {
    LOG_WARN("failed to set trx timeout us", KR(ret));
  } else if (OB_FAIL(timeout_ctx.set_abs_timeout(abs_timeout))) {
    LOG_WARN("failed to set abs timeout", KR(ret));
  } else if (OB_ISNULL(sql_proxy = ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy must not be null", KR(ret));
  } else {
    // if the clone job is successful,
    // the according record will be moved to __all_clone_job_history from __all_clone_job;
    // if the clone job is failed,
    // the according record will be set as failed status in __all_clone_job and
    // will be moved to __all_clone_job_history after the related resource is recycled
    bool clone_over = false;
    while (OB_SUCC(ret) && !clone_over) {
      job.reset();
      ob_usleep(2 * 1000 * 1000L);  // 2s
      ObTenantCloneTableOperator table_op;
      ObMySQLTransaction trans;

      if (OB_UNLIKELY(ERRSIM_WAIT_CLONE_TENANT_FINISHED_ERROR)) {
        ret = ERRSIM_WAIT_CLONE_TENANT_FINISHED_ERROR;
      } else if (THIS_WORKER.is_timeout()) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait clone tenant timeout", KR(ret), K(job_id));
      } else if (OB_FAIL(ctx.check_status())) {
        LOG_WARN("check exec ctx failed", KR(ret));
      } else if (OB_FAIL(trans.start(sql_proxy, OB_SYS_TENANT_ID))) {
        LOG_WARN("fail to start trans", KR(ret));
      } else if (OB_FAIL(table_op.init(OB_SYS_TENANT_ID, &trans))) {
        LOG_WARN("failed to init table op", KR(ret));
      } else if (OB_FAIL(table_op.get_sys_clone_job_history(job_id, job))) {
        if (OB_ENTRY_NOT_EXIST == ret) { // clone job is running
          ret = OB_SUCCESS;

          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(ObTenantCloneUtil::notify_clone_scheduler(OB_SYS_TENANT_ID))) {
            LOG_WARN("notify clone scheduler failed", KR(tmp_ret));
          }
        } else {
          LOG_WARN("failed to get clone job history", KR(ret), K(job_id));
        }
      } else if (job.get_status().is_sys_success_status()) {
        clone_over = true;
        LOG_INFO("clone tenant successful", K(job));
      } else if (job.get_status().is_sys_failed_status()) {
        ret = OB_ERR_CLONE_TENANT;
        LOG_WARN("clone tenant failed", KR(ret), K(job));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status", KR(ret), K(job));
      }

      if (OB_UNLIKELY(OB_TIMEOUT == ret)) {
        ret = OB_CLONE_TENANT_TIMEOUT;
        LOG_WARN("wait clone tenant timeout", KR(ret), K(job_id));
        ObString msg("Please check the details of clone job by DBA_OB_CLONE_PROGRESS or DBA_OB_CLONE_HISTORY");
        LOG_USER_ERROR(OB_CLONE_TENANT_TIMEOUT, msg.length(), msg.ptr());
      } else if (OB_UNLIKELY(OB_ERR_CLONE_TENANT == ret)) {
        int tmp_ret = OB_SUCCESS;
        ObArenaAllocator allocator;
        ObString err_msg;
        if (OB_TMP_FAIL(ObTenantCloneUtil::get_clone_job_failed_message(*sql_proxy,
                                                                        job_id,
                                                                        MTL_ID(),
                                                                        allocator,
                                                                        err_msg))) {
          LOG_WARN("fail to get clone job failed message", KR(ret), K(job_id), K(MTL_ID()));
        } else if (!err_msg.empty()) {
          LOG_USER_ERROR(OB_ERR_CLONE_TENANT, err_msg.length(), err_msg.ptr());
        } else {
          ObSqlString format_msg;
          ObString failed_status(ObTenantCloneStatus::get_clone_status_str(job.get_status()));
          if (OB_TMP_FAIL(format_msg.append_fmt("Tenant clone job failed during the %.*s stage. "
                                                "Please check the details of clone job by "
                                                "DBA_OB_CLONE_PROGRESS or DBA_OB_CLONE_HISTORY",
                                                failed_status.length(), failed_status.ptr()))) {
            LOG_WARN("fail to append format", KR(tmp_ret), K(job_id), K(MTL_ID()));
          } else {
            LOG_USER_ERROR(OB_ERR_CLONE_TENANT, format_msg.string().length(), format_msg.ptr());
          }
        }
      }
    }
  }
  return ret;
}

} //end namespace sql
} //end namespace oceanbase
