/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX RS

#include "rootserver/mview/ob_mview_pending_task_rpc_processor.h"
#include "rootserver/mview/ob_mview_pending_task_define.h"
#include "rootserver/mview/ob_mview_pending_task_executor.h"
#include "rootserver/mview/ob_mview_maintenance_service.h"
#include "lib/oblog/ob_warning_buffer.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;

namespace oceanbase
{
namespace obrpc
{

static int register_recovery_on_rpc_timeout(const ObRunMViewPendingTaskArg &arg)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg.tenant_id_) {
    ObMViewMaintenanceService *service = MTL(ObMViewMaintenanceService *);
    if (OB_ISNULL(service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mview maintenance service is null, running task stuck",
               KR(ret), K(arg));
    } else if (OB_FAIL(service->get_pending_task_manager()
                       ->register_running_task_for_recovery(
                           arg.tenant_id_, arg.refresh_id_, arg.mview_id_,
                           arg.target_data_sync_scn_))) {
      LOG_WARN("register running task for recovery failed", KR(ret), K(arg));
    }
  }
  return ret;
}

static int report_task_result(const ObRunMViewPendingTaskArg &arg, int task_ret, const char *err_msg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  MTL_SWITCH(arg.tenant_id_) {
    ObMViewMaintenanceService *service = MTL(ObMViewMaintenanceService *);
    if (OB_ISNULL(service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("mview maintenance service is null, task result is lost",
                KR(ret), K(arg), K(task_ret));
    } else if (OB_TMP_FAIL(service->get_pending_task_manager()->push_task_result(arg.tenant_id_,
                                                                                 arg.refresh_id_,
                                                                                 arg.mview_id_,
                                                                                 arg.target_data_sync_scn_,
                                                                                 task_ret,
                                                                                 err_msg))) {
      LOG_WARN("push task result failed", K(tmp_ret), K(arg), K(task_ret));
    }
  }
  return ret;
}

int ObRpcRunMViewPendingTaskCB::process()
{
  int ret = OB_SUCCESS;
  const ObRunMViewPendingTaskArg &arg = arg_;
  const int task_ret = result_.ret_;
  LOG_TRACE("run mview pending task async rpc done", K(arg), K(task_ret));
  // Pass the worker-side LOG_USER_ERROR message directly through the result
  // chain, avoiding contamination of the RS thread's warning buffer.
  const char *err_msg = (OB_SUCCESS != task_ret && result_.msg_[0] != '\0') ? result_.msg_ : NULL;
  if (OB_FAIL(report_task_result(arg, task_ret, err_msg))) {
    LOG_WARN("report task result failed", KR(ret), K(arg), K(task_ret));
  }
  return ret;
}

void ObRpcRunMViewPendingTaskCB::on_timeout()
{
  int ret = OB_SUCCESS;
  // RPC timed out on the sender side. The remote worker may still be executing
  // (ObRpcRunMViewPendingTaskP::process() runs the full refresh before replying).
  // Do NOT re-dispatch: register for recovery inspection so the inspection task
  // checks data_sync_scn before deciding SUCCESS vs RETRY_WAIT.
  LOG_WARN("run mview pending task rpc timeout, registering for recovery",
           KR(ret), K(arg_));
  if (OB_FAIL(register_recovery_on_rpc_timeout(arg_))) {
    LOG_WARN("register for recovery failed, task may be stuck until leader restart",
             KR(ret), K(arg_));
  }
}

void ObRpcRunMViewPendingTaskCB::on_invalid()
{
  int ret = OB_SUCCESS;
  // RPC packet was malformed. This is a non-retriable programming error.
  LOG_ERROR("run mview pending task rpc invalid, marking failed", KR(ret), K(arg_));
  if (OB_FAIL(report_task_result(arg_, OB_ERR_UNEXPECTED, NULL))) {
    LOG_WARN("report task result failed", KR(ret), K(arg_));
  }
}

int ObRpcRunMViewPendingTaskP::process()
{
  int ret = OB_SUCCESS;
  const ObRunMViewPendingTaskArg &arg = arg_;
  ObMViewPendingTaskExecutor executor;

  LOG_TRACE("run mview pending task rpc process start", K(arg));

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(executor.init(gctx_.schema_service_))) {
    LOG_WARN("init pending task executor failed", KR(ret));
  } else if (OB_FAIL(executor.run_pending_task(arg))) {
    LOG_WARN("run pending task failed", KR(ret), K(arg));
  }

  // always record the execution result in response; the caller (CB) decides how to handle it
  result_.ret_ = ret;
  if (OB_SUCCESS != ret) {
    // LOG_USER_ERROR was called on this worker thread during refresh. The RPC
    // framework only auto-captures wb->get_err_msg() when process() returns non-zero.
    // Since we always return OB_SUCCESS, we must manually copy the message to result_.msg_.
    common::ObWarningBuffer *wb = common::ob_get_tsi_warning_buffer();
    if (OB_NOT_NULL(wb)) {
      const char *err_msg = wb->get_err_msg();
      if (OB_NOT_NULL(err_msg)) {
        (void)snprintf(result_.msg_, sizeof(result_.msg_), "%s", err_msg);
      }
    }
  }
  LOG_TRACE("run mview pending task rpc process end", KR(ret), K(arg));

  return OB_SUCCESS;
}

int ObRpcScheduleMViewRefreshP::process()
{
  int ret = OB_SUCCESS;
  const ObScheduleMViewRefreshArg &arg = arg_;

  LOG_TRACE("schedule mview refresh rpc process start", K(arg));

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else {
    MTL_SWITCH(arg.tenant_id_) {
      rootserver::ObMViewMaintenanceService *service = MTL(rootserver::ObMViewMaintenanceService *);
      if (OB_ISNULL(service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mview maintenance service is null", KR(ret), K(arg));
      } else if (OB_FAIL(service->get_pending_task_manager()->schedule_mview_refresh_local(
                     arg, result_))) {
        LOG_WARN("schedule mview refresh local failed", KR(ret), K(arg));
      }
    }
  }

  result_.ret_ = ret;
  LOG_TRACE("schedule mview refresh rpc process end", KR(ret), K(arg), K(result_));

  return OB_SUCCESS;
}

int ObRpcKillMViewRefreshP::process()
{
  int ret = OB_SUCCESS;
  const ObKillMViewRefreshArg &arg = arg_;
  LOG_TRACE("kill mview refresh rpc process start", K(arg));
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else {
    MTL_SWITCH(arg.tenant_id_) {
      rootserver::ObMViewMaintenanceService *service = MTL(rootserver::ObMViewMaintenanceService *);
      if (OB_ISNULL(service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mview maintenance service is null", KR(ret), K(arg));
      } else {
        rootserver::ObMViewPendingTaskManager *manager = service->get_pending_task_manager();
        if (arg.is_kill_by_mview_id_) {
          if (arg.is_drop_ &&
              OB_FAIL(manager->acquire_mview_block(arg.mview_id_,
                                                   ObMViewContext::BLOCK_FLAG_DROP))) {
            LOG_WARN("acquire drop block failed", KR(ret), K(arg));
          } else if (OB_FAIL(manager->kill_refreshes_by_mview_local(arg.tenant_id_, arg.mview_id_))) {
            LOG_WARN("kill refreshes by mview local failed", KR(ret), K(arg));
          }
        } else {
          if (OB_FAIL(manager->kill_refresh_local(arg.tenant_id_, arg.refresh_id_))) {
            LOG_WARN("kill refresh local failed", KR(ret), K(arg));
          }
        }
      }
    }
  }
  result_.ret_ = ret;
  LOG_TRACE("kill mview refresh rpc process end", KR(ret), K(arg), K(result_));
  return OB_SUCCESS;
}

} // namespace obrpc
} // namespace oceanbase
