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

#define USING_LOG_PREFIX SQL_DAS
#include "observer/ob_srv_network_frame.h"
#include "observer/mysql/ob_query_retry_ctrl.h"
#include "sql/das/ob_data_access_service.h"
#include "sql/das/ob_das_define.h"
#include "sql/das/ob_das_extra_data.h"
#include "sql/das/ob_das_ref.h"
#include "sql/das/ob_das_rpc_processor.h"
#include "sql/das/ob_das_utils.h"
#include "sql/ob_phy_table_location.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/das/ob_das_retry_ctrl.h"
#include "storage/tx/ob_trans_service.h"
namespace oceanbase
{
using namespace share;
using namespace storage;
using namespace common;
using namespace transaction;
using namespace observer;
namespace sql
{

ObDataAccessService::ObDataAccessService()
  : das_rpc_proxy_(),
    ctrl_addr_(),
    id_cache_(),
    task_result_mgr_(),
    das_concurrency_limit_(INT32_MAX)
{
}

ObDataAccessService &ObDataAccessService::get_instance()
{
  static ObDataAccessService instance;
  return instance;
}

int ObDataAccessService::mtl_init(ObDataAccessService *&das)
{
  int ret = OB_SUCCESS;
  const ObAddr &self = GCTX.self_addr();
  observer::ObSrvNetworkFrame *net_frame = GCTX.net_frame_;
  auto req_transport = net_frame->get_req_transport();
  if (OB_FAIL(das->id_cache_.init(self, req_transport))) {
    LOG_ERROR("init das id service failed", K(ret));
  } else if (OB_FAIL(das->init(net_frame->get_req_transport(), self))) {
    LOG_ERROR("init data access service failed", K(ret));
  }
  return ret;
}

void ObDataAccessService::mtl_destroy(ObDataAccessService *&das)
{
  if (das != nullptr) {
    das->~ObDataAccessService();
    oceanbase::common::ob_delete(das);
    das = nullptr;
  }
}

int ObDataAccessService::init(rpc::frame::ObReqTransport *transport, const ObAddr &self_addr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(das_rpc_proxy_.init(transport))) {
    LOG_WARN("init das rpc proxy failed", K(ret));
  } else if (OB_FAIL(task_result_mgr_.init())) {
    LOG_WARN("init das task result manager failed", KR(ret));
  } else {
    ctrl_addr_ = self_addr;
  }
  return ret;
}

int ObDataAccessService::execute_das_task(
    ObDASRef &das_ref, ObDasAggregatedTasks &task_ops, bool async) {
  int ret = OB_SUCCESS;
  if (OB_LIKELY(das_ref.is_execute_directly())) {
    common::ObSEArray<ObIDASTaskOp *, 2> task_wrapper;
    FLTSpanGuard(do_local_das_task);
    while (OB_SUCC(ret) && OB_SUCC(task_ops.get_aggregated_tasks(task_wrapper)) &&
        task_wrapper.count() != 0) {
      for (int i = 0; OB_SUCC(ret) && i < task_wrapper.count(); i++) {
        if (OB_FAIL(task_wrapper.at(i)->start_das_task())) {
          int tmp_ret = OB_SUCCESS;
          LOG_WARN("start das task failed", K(ret), K(*task_wrapper.at(i)));
          if (OB_TMP_FAIL(task_wrapper.at(i)->state_advance())) {
            LOG_WARN("failed to advance das task state.",K(ret));
          }
        } else {
          if (OB_FAIL(task_wrapper.at(i)->state_advance())) {
            LOG_WARN("failed to advance das task state.",K(ret));
          }
        }
      }
      task_wrapper.reuse();
    }
  } else if (OB_FAIL(execute_dist_das_task(das_ref, task_ops, async))) {
    LOG_WARN("failed to execute dist das task", K(ret));
  }
  DAS_CTX(das_ref.get_exec_ctx()).get_location_router().save_cur_exec_status(ret);
  return ret;
}

int ObDataAccessService::get_das_task_id(int64_t &das_id)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(get_das_id);
  const int MAX_RETRY_TIMES = 50;
  int64_t tmp_das_id = 0;
  bool force_renew = false;
  int64_t total_sleep_time = 0;
  int64_t cur_sleep_time = 1000; // 1ms
  int64_t max_sleep_time = ObDASIDCache::OB_DAS_ID_RPC_TIMEOUT_MIN * 2; // 200ms
  do {
    if (OB_SUCC(id_cache_.get_das_id(tmp_das_id, force_renew))) {
    } else if (OB_EAGAIN == ret) {
      if (total_sleep_time >= max_sleep_time) {
        // TODO chenxuan change error code
        ret = OB_GTI_NOT_READY;
        LOG_WARN("get das id not ready", K(ret), K(total_sleep_time), K(max_sleep_time));
      } else {
        force_renew = true;
        ob_usleep(cur_sleep_time);
        total_sleep_time += cur_sleep_time;
        cur_sleep_time = cur_sleep_time * 2;
      }
    } else {
      LOG_WARN("get das id failed", K(ret));
    }
  } while (OB_EAGAIN == ret);
  if (OB_SUCC(ret)) {
    das_id = tmp_das_id;
  }
  return ret;
}

void ObDataAccessService::calc_das_task_parallelism(const ObDASRef &das_ref,
    const ObDasAggregatedTasks &task_ops, int &target_parallelism)
{
  // const int divide_async_task_threshold = 1024;
  // if (task_ops.get_unstart_task_size() > divide_async_task_threshold) {
  //   target_parallelism = ceil(static_cast<double>(das_ref.get_max_concurrency()) / das_ref.get_aggregated_tasks_count());
  // } else {
  //   target_parallelism = 1;
  // }
  target_parallelism = 1;
}

OB_NOINLINE int ObDataAccessService::execute_dist_das_task(
    ObDASRef &das_ref, ObDasAggregatedTasks &task_ops, bool async) {
  int ret = OB_SUCCESS;
  ObExecContext &exec_ctx = das_ref.get_exec_ctx();
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  ObDASTaskArg task_arg;
  task_arg.set_timeout_ts(session->get_query_timeout_ts());
  task_arg.set_ctrl_svr(ctrl_addr_);
  task_arg.get_runner_svr() = task_ops.server_;
  int target_parallelism = 0;
  calc_das_task_parallelism(das_ref, task_ops, target_parallelism);
  common::ObSEArray<common::ObSEArray<ObIDASTaskOp *, 2>, 2> task_groups;
  if (OB_FAIL(task_ops.get_aggregated_tasks(task_groups, target_parallelism))) {
    LOG_WARN("failed to get das task groups", K(ret));
  }
  OB_ASSERT(target_parallelism >= task_groups.count());
  LOG_DEBUG("current dist das task group", K(ret), K(task_groups), K(task_ops));
  for (int i = 0; OB_SUCC(ret) && i < task_groups.count(); i++) {
    task_arg.get_task_ops() = task_groups.at(i);
    if (OB_FAIL(task_arg.get_task_ops().get_copy_assign_ret())) {
      LOG_WARN("failed to copy das task", K(ret));
    } else if (task_arg.is_local_task()) {
      if (OB_FAIL(do_local_das_task(das_ref, task_arg))) {
        LOG_WARN("do local das task failed", K(ret));
      }
    } else if (OB_FAIL(das_ref.acquire_task_execution_resource())) {
      LOG_WARN("failed to acquire execution resource", K(ret));
    } else {
     if (async) {
        if (OB_FAIL(do_async_remote_das_task(das_ref, task_ops, task_arg))) {
          das_ref.inc_concurrency_limit();
          LOG_WARN("do remote das task failed", K(ret));
        }
      } else {
        if (OB_FAIL(do_sync_remote_das_task(das_ref, task_ops, task_arg))) {
          LOG_WARN("do remote das task failed", K(ret));
        }
      }
    }
    task_arg.get_task_ops().reuse();
  }
  return ret;
}

int ObDataAccessService::clear_task_exec_env(ObDASRef &das_ref, ObIDASTaskOp &task_op)
{
  UNUSED(das_ref);
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_op.end_das_task())) {
    LOG_WARN("end das task failed", K(ret));
  }
  DAS_CTX(das_ref.get_exec_ctx()).get_location_router().save_cur_exec_status(OB_SUCCESS);
  return ret;
}

int ObDataAccessService::refresh_task_location_info(ObDASRef &das_ref, ObIDASTaskOp &task_op)
{
  int ret = OB_SUCCESS;
  ObExecContext &exec_ctx = das_ref.get_exec_ctx();
  ObDASTabletLoc *tablet_loc = const_cast<ObDASTabletLoc*>(task_op.get_tablet_loc());
  int64_t retry_cnt = DAS_CTX(exec_ctx).get_location_router().get_cur_retry_cnt();
  if (OB_FAIL(ObDASUtils::wait_das_retry(retry_cnt))) {
    LOG_WARN("wait das retry failed", K(ret));
  } else if (OB_FAIL(DAS_CTX(exec_ctx).get_location_router().get_tablet_loc(*tablet_loc->loc_meta_,
                                                                            tablet_loc->tablet_id_,
                                                                            *tablet_loc))) {
    LOG_WARN("get tablet location failed", K(ret), KPC(tablet_loc));
  } else {
    task_op.set_ls_id(tablet_loc->ls_id_);
    if (!task_op.is_local_task()) {
      int64_t task_id;
      if (OB_FAIL(MTL(ObDataAccessService*)->get_das_task_id(task_id))) {
        LOG_WARN("retry get das task id failed", KR(ret));
      } else {
        task_op.set_task_id(task_id);
      }
    }
  }
  return ret;
}

int ObDataAccessService::retry_das_task(ObDASRef &das_ref, ObIDASTaskOp &task_op)
{
  int ret = task_op.errcode_;
  ObArenaAllocator tmp_alloc;
  ObDasAggregatedTasks das_task_wrapper(tmp_alloc);
  bool retry_continue = false;
  ObDASLocationRouter &location_router = DAS_CTX(das_ref.get_exec_ctx()).get_location_router();
  location_router.reset_cur_retry_cnt();
  oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT_FOR_LOCAL_RETRY);
  do {
    ObDASRetryCtrl::retry_func retry_func = nullptr;

    retry_continue = false;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObQueryRetryCtrl::get_das_retry_func(task_op.errcode_, retry_func))) {
      LOG_WARN("get das retry func failed", K(tmp_ret), K(task_op.errcode_));
    } else if (retry_func != nullptr) {
      bool need_retry = false;
      const ObDASTabletLoc *tablet_loc = task_op.get_tablet_loc();
      const ObDASTableLocMeta *loc_meta = tablet_loc != nullptr ? tablet_loc->loc_meta_ : nullptr;
      retry_func(das_ref, task_op, need_retry);
      LOG_INFO("[DAS RETRY] check if need tablet level retry",
               KR(task_op.errcode_), K(need_retry), K(task_op.task_flag_),
               "continuous_retry_cnt", location_router.get_cur_retry_cnt(),
               "total_retry_cnt", location_router.get_total_retry_cnt(),
               KPC(loc_meta), KPC(tablet_loc));
      if (need_retry &&
          task_op.get_inner_rescan() &&
          location_router.get_total_retry_cnt() > 100) { //hard code retry 100 times.
        // disable das retry for rescan.
        need_retry = false;
        retry_continue = false;
        LOG_INFO("[DAS RETRY] The rescan task has retried too many times and has exited the DAS retry process");
      }
      if (need_retry) {
        task_op.in_part_retry_ = true;
        location_router.set_last_errno(task_op.get_errcode());
        location_router.inc_cur_retry_cnt();
        if (OB_TMP_FAIL(clear_task_exec_env(das_ref, task_op))) {
          LOG_WARN("clear task execution environment failed", K(tmp_ret));
        }
        if (OB_FAIL(das_ref.get_exec_ctx().check_status())) {
          LOG_WARN("query is timeout or interrupted, terminate retry", KR(ret));
        } else if (OB_FAIL(refresh_task_location_info(das_ref, task_op))) {
          LOG_WARN("refresh task location failed", K(ret));
        } else {
          LOG_INFO("[DAS RETRY] Start retrying the DAS task now", KPC(task_op.get_tablet_loc()));
          das_task_wrapper.reuse();
          task_op.set_task_status(ObDasTaskStatus::UNSTART);
          if (OB_FAIL(das_task_wrapper.push_back_task(&task_op))) {
            LOG_WARN("failed to push back task", K(ret));
          } else if (OB_FAIL(execute_dist_das_task(das_ref, das_task_wrapper, false))) {
            LOG_WARN("execute dist DAS task failed", K(ret));
          }
          if (OB_SUCCESS == ret) {
            LOG_INFO("[DAS RETRY] DAS Task succeeds after multiple retries",
                     "continuous_retry_cnt", location_router.get_cur_retry_cnt(),
                     "total_retry_cnt", location_router.get_total_retry_cnt(),
                     KPC(task_op.get_tablet_loc()));
          } else {
            int64_t cur_retry_cnt = location_router.get_cur_retry_cnt();
            int64_t total_retry_cnt = location_router.get_total_retry_cnt();
            if (cur_retry_cnt >= 100 && cur_retry_cnt % 50L == 0) {
              LOG_INFO("[DAS RETRY] The DAS task has been retried multiple times without success, "
                       "and the execution may be blocked by a specific exception", KR(ret),
                       "continuous_retry_cnt", cur_retry_cnt,
                       "total_retry_cnt", location_router.get_total_retry_cnt(),
                       KPC(task_op.get_tablet_loc()));
            }
          }
        }
        task_op.errcode_ = ret;
        retry_continue = (OB_SUCCESS != ret);
        if (!retry_continue) {
          location_router.accumulate_retry_count();
        }
        if (retry_continue && IS_INTERRUPTED()) {
          retry_continue = false;
          LOG_INFO("[DAS RETRY] Retry is interrupted by worker interrupt signal", KR(ret));
        }
      } else {
        ret = task_op.errcode_;
      }
    }
  } while (retry_continue);

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(task_op.state_advance())) {
      LOG_WARN("failed to reset das task to original agg list.", K(ret));
    }
  }
  OB_ASSERT(das_task_wrapper.has_unstart_tasks() == false &&
      das_task_wrapper.success_tasks_.get_size() == 0);
  return ret;
}

int ObDataAccessService::end_das_task(ObDASRef &das_ref, ObIDASTaskOp &task_op)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_op.end_das_task())) {
    LOG_WARN("end das task failed", K(ret), KPC(task_op.get_tablet_loc()));
  }
  return ret;
}

int ObDataAccessService::rescan_das_task(ObDASRef &das_ref, ObDASScanOp &scan_op)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(rescan_das_task);

  ObArenaAllocator tmp_alloc;
  ObDasAggregatedTasks das_task_wrapper(tmp_alloc);
  if (scan_op.is_local_task()) {
    if (OB_FAIL(scan_op.rescan())) {
      LOG_WARN("rescan das task failed", K(ret));
    }
  } else if (OB_FAIL(das_task_wrapper.push_back_task(&scan_op))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(execute_dist_das_task(das_ref, das_task_wrapper, false))) {
    scan_op.errcode_ = ret;
    LOG_WARN("execute dist das task failed", K(ret));
  }
  OB_ASSERT(scan_op.errcode_ == ret);
  if (OB_FAIL(ret) && GCONF._enable_partition_level_retry && scan_op.can_part_retry()) {
    //only fast select can be retry with partition level
    if (OB_FAIL(retry_das_task(das_ref, scan_op))) {
      LOG_WARN("failed to retry das task", K(ret));
    }
  }
  return ret;
}

int ObDataAccessService::do_local_das_task(ObDASRef &das_ref,
    ObDASTaskArg &task_arg) {
  UNUSED(das_ref);
  int ret = OB_SUCCESS;
  FLTSpanGuard(do_local_das_task);

  const common::ObSEArray<ObIDASTaskOp*, 2> &task_ops =  task_arg.get_task_ops();
  LOG_DEBUG("begin to do local das task", K(task_arg));
  for (int64_t i = 0; OB_SUCC(ret) && i < task_ops.count(); i++) {
    if (OB_FAIL(task_ops.at(i)->start_das_task())) {
      LOG_WARN("start local das task failed", K(ret));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(task_ops.at(i)->state_advance())) {
        LOG_WARN("failed to advance das task state.",K(ret));
      }
      break;
    } else {
      if (OB_FAIL(task_ops.at(i)->state_advance())) {
        LOG_WARN("failed to advance das task state.",K(ret));
      }
    }
  }
  return ret;
}

int ObDataAccessService::do_async_remote_das_task(
    ObDASRef &das_ref, ObDasAggregatedTasks &aggregated_tasks,
    ObDASTaskArg &task_arg) {
  int ret = OB_SUCCESS;
  void *resp_buf = nullptr;
  FLTSpanGuard(do_async_remote_das_task);
  ObSQLSessionInfo *session = das_ref.get_exec_ctx().get_my_session();
  ObPhysicalPlanCtx *plan_ctx = das_ref.get_exec_ctx().get_physical_plan_ctx();
  int64_t timeout_ts = plan_ctx->get_timeout_timestamp();
  int64_t current_ts = ObClockGenerator::getClock();
  int64_t timeout = timeout_ts - current_ts;
  int64_t simulate_timeout = - EVENT_CALL(EventTable::EN_DAS_SIMULATE_ASYNC_RPC_TIMEOUT);
  if (OB_UNLIKELY(simulate_timeout > 0)) {
    LOG_INFO("das async rpc simulate timeout", K(simulate_timeout),
             K(timeout), K(timeout_ts), K(current_ts));
    timeout = simulate_timeout;
    timeout_ts = current_ts + timeout;
  }
  uint64_t tenant_id = session->get_rpc_tenant_id();
  common::ObSEArray<ObIDASTaskOp*, 2> &task_ops = task_arg.get_task_ops();
  ObDASRemoteInfo remote_info;
  remote_info.exec_ctx_ = &das_ref.get_exec_ctx();
  remote_info.frame_info_ = das_ref.get_expr_frame_info();
  remote_info.trans_desc_ = session->get_tx_desc();
  remote_info.snapshot_ = *task_arg.get_task_op()->get_snapshot();
  remote_info.need_tx_ = (remote_info.trans_desc_ != nullptr);
  session->get_cur_sql_id(remote_info.sql_id_, sizeof(remote_info.sql_id_));
  remote_info.user_id_ = session->get_user_id();
  remote_info.session_id_ = session->get_sessid();
  remote_info.plan_id_ = session->get_current_plan_id();

  task_arg.set_remote_info(&remote_info);
  ObDASRemoteInfo::get_remote_info() = &remote_info;
  ObIDASTaskResult *op_result = nullptr;
  ObRpcDasAsyncAccessCallBack *das_async_cb = nullptr;
  if (OB_FAIL(das_ref.allocate_async_das_cb(das_async_cb, task_ops, timeout_ts))) {
    LOG_WARN("failed to allocate das async cb", K(ret));
  }
  // prepare op result in advance avoiding racing condition.
  for (int64_t i = 0; OB_SUCC(ret) && i < task_ops.count(); i++) {
    if (OB_UNLIKELY(ObDasTaskStatus::UNSTART != task_ops.at(i)->get_task_status())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task status unexpected", KR(ret), K(task_ops.at(i)->get_task_status()), KPC(task_ops.at(i)));
    } else if (NULL != (op_result = task_ops.at(i)->get_op_result())) {
      if (OB_FAIL(op_result->reuse())) {
        LOG_WARN("reuse task result failed", K(ret));
      }
    } else {
      if (OB_FAIL(das_ref.get_das_factory().create_das_task_result(task_ops.at(i)->get_type(), op_result))) {
            LOG_WARN("failed to create das task result", K(ret));
      } else if (OB_ISNULL(op_result)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get op result", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(das_async_cb->get_op_results().push_back(op_result))) {
        LOG_WARN("failed to add task result", K(ret));
      } else if (OB_FAIL(op_result->init(*task_ops.at(i), das_async_cb->get_result_alloc()))) {
        LOG_WARN("failed to init task result", K(ret));
      } else {
        task_ops.at(i)->set_op_result(op_result);
      }
    }
  }
  LOG_DEBUG("begin to do remote das task", K(task_arg));
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(collect_das_task_info(task_arg, remote_info))) {
    LOG_WARN("collect das task info failed", K(ret));
  } else if (OB_UNLIKELY(timeout <= 0)) {
    ret = OB_TIMEOUT;
    LOG_WARN("das is timeout", K(ret), K(plan_ctx->get_timeout_timestamp()), K(timeout));
  } else if (OB_FAIL(das_rpc_proxy_
                  .to(task_arg.get_runner_svr())
                  .by(tenant_id)
                  .timeout(timeout)
                  .das_async_access(task_arg, das_async_cb))) {
    LOG_WARN("rpc remote sync access failed", K(ret), K(task_arg));
    // RPC fail, add task's LSID to trans_result
    // indicate some transaction participant may touched
    for (int i = 0; i < task_ops.count(); i++) {
      session->get_trans_result().add_touched_ls(task_ops.at(i)->get_ls_id());
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != das_async_cb) {
      das_ref.remove_async_das_cb(das_async_cb);
    }
    for (int i = 0; i < task_ops.count(); i++) {
      task_ops.at(i)->errcode_ = ret;
      task_ops.at(i)->set_task_status(ObDasTaskStatus::FAILED);
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(task_ops.at(i)->state_advance())) {
        LOG_WARN("failed to advance das task state", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObDataAccessService::do_sync_remote_das_task(
    ObDASRef &das_ref, ObDasAggregatedTasks &aggregated_tasks,
    ObDASTaskArg &task_arg) {
  int ret = OB_SUCCESS;
  void *resp_buf = nullptr;
  FLTSpanGuard(do_sync_remote_das_task);
  ObSQLSessionInfo *session = das_ref.get_exec_ctx().get_my_session();
  ObPhysicalPlanCtx *plan_ctx = das_ref.get_exec_ctx().get_physical_plan_ctx();
  int64_t timeout = plan_ctx->get_timeout_timestamp() - ObClockGenerator::getClock();
  uint64_t tenant_id = session->get_rpc_tenant_id();
  common::ObSEArray<ObIDASTaskOp*, 2> &task_ops = task_arg.get_task_ops();
  ObIDASTaskResult *op_result = nullptr;
  ObDASExtraData *extra_result = nullptr;
  ObDASRemoteInfo remote_info;
  remote_info.exec_ctx_ = &das_ref.get_exec_ctx();
  remote_info.frame_info_ = das_ref.get_expr_frame_info();
  remote_info.trans_desc_ = session->get_tx_desc();
  remote_info.snapshot_ = *task_arg.get_task_op()->get_snapshot();
  remote_info.need_tx_ = (remote_info.trans_desc_ != nullptr);
  session->get_cur_sql_id(remote_info.sql_id_, sizeof(remote_info.sql_id_));
  remote_info.user_id_ = session->get_user_id();
  remote_info.session_id_ = session->get_sessid();
  remote_info.plan_id_ = session->get_current_plan_id();

  task_arg.set_remote_info(&remote_info);
  ObDASRemoteInfo::get_remote_info() = &remote_info;

  LOG_DEBUG("begin to do remote das task", K(task_arg));
  SMART_VAR(ObDASTaskResp, task_resp) {
    // we need das_factory to allocate task op result on receiving rpc response.
    task_resp.set_das_factory(&das_ref.get_das_factory());

    // prepare op result in advance avoiding racing condition.
    for (int64_t i = 0; OB_SUCC(ret) && i < task_ops.count(); i++) {
      if (OB_UNLIKELY(ObDasTaskStatus::UNSTART != task_ops.at(i)->get_task_status())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task status unexpected", KR(ret), K(task_ops.at(i)->get_task_status()), KPC(task_ops.at(i)));
      } else if (NULL != (op_result = task_ops.at(i)->get_op_result())) {
        if (OB_FAIL(op_result->reuse())) {
          LOG_WARN("reuse task result failed", K(ret));
        }
      } else {
        if (OB_FAIL(das_ref.get_das_factory().create_das_task_result(task_ops.at(i)->get_type(), op_result))) {
          LOG_WARN("failed to create das task result", K(ret));
        } else if (OB_ISNULL(op_result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get op result", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(task_resp.get_op_results().push_back(op_result))) {
          LOG_WARN("failed to add task result", K(ret));
        } else if (OB_FAIL(op_result->init(*task_ops.at(i), das_ref.get_das_alloc()))) {
          LOG_WARN("failed to init task result", K(ret));
        } else {
          task_ops.at(i)->set_op_result(op_result);
        }
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(collect_das_task_info(task_arg, remote_info))) {
      LOG_WARN("collect das task info failed", K(ret));
    } else if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("das is timeout", K(ret), K(plan_ctx->get_timeout_timestamp()), K(timeout));
    } else if (OB_FAIL(das_rpc_proxy_
                    .to(task_arg.get_runner_svr())
                    .by(tenant_id)
                    .timeout(timeout)
                    .remote_sync_access(task_arg, task_resp))) {
      LOG_WARN("rpc remote sync access failed", K(ret), K(task_arg));
      // RPC fail, add task's LSID to trans_result
      // indicate some transaction participant may touched
      for (int i = 0; i < task_ops.count(); i++) {
        session->get_trans_result().add_touched_ls(task_ops.at(i)->get_ls_id());
      }
    }
    if (OB_FAIL(ret)) {
      for (int i = 0; i < task_ops.count(); i++) {
        task_ops.at(i)->errcode_ = ret;
        task_ops.at(i)->set_task_status(ObDasTaskStatus::FAILED);
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(task_ops.at(i)->state_advance())) {
          LOG_WARN("failed to advance das task state", K(tmp_ret));
        }
      }
    } else if (OB_FAIL(process_task_resp(das_ref, task_resp, task_ops))) {
      LOG_WARN("failed to process task resp", K(ret), K(task_arg), K(task_resp));
    }
  }
  das_ref.inc_concurrency_limit();
  return ret;
}

int ObDataAccessService::process_task_resp(ObDASRef &das_ref, const ObDASTaskResp &task_resp, const common::ObSEArray<ObIDASTaskOp*, 2> &task_ops)
{
  int ret = OB_SUCCESS;
  int save_ret = OB_SUCCESS;
  ObSQLSessionInfo *session = das_ref.get_exec_ctx().get_my_session();
  ObIDASTaskOp *task_op = nullptr;
  ObIDASTaskResult *op_result = nullptr;
  ObDASExtraData *extra_result = nullptr;
  const common::ObSEArray<ObIDASTaskResult*, 2> &op_results = task_resp.get_op_results();
  ObDASLocationRouter &loc_router = DAS_CTX(das_ref.get_exec_ctx()).get_location_router();
  ObDASUtils::log_user_error_and_warn(task_resp.get_rcode());
  for (int i = 0; i < op_results.count() - 1; i++) {
    // even if error happened durning iteration, we should iter to the end.
    task_op = task_ops.at(i);
    op_result = op_results.at(i);
    OB_ASSERT(op_result != nullptr);
    if (OB_FAIL(task_op->decode_task_result(op_result))) {
      task_op->set_task_status(ObDasTaskStatus::FAILED);
      task_op->errcode_ = ret;
      LOG_WARN("decode das task result failed", K(ret));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(task_op->state_advance())) {
        LOG_WARN("failed to advance das task state.",K(ret));
      }
    } else {
      task_op->set_task_status(ObDasTaskStatus::FINISHED);
      if (OB_FAIL(task_op->state_advance())) {
        LOG_WARN("failed to advance das task state.",K(ret));
      }
    }
  }

  // special check for last valid op result
  if (OB_UNLIKELY(op_results.count() == 0)) {
    // no op response. first task op must have error.
    task_op = task_ops.at(0);
    op_result = nullptr;
    OB_ASSERT(task_resp.get_err_code() != OB_SUCCESS);
  } else {
    task_op = task_ops.at(op_results.count() - 1);
    op_result = op_results.at(op_results.count() - 1);
  }
  if (OB_FAIL(ret)) {
    save_ret = ret;
    ret = OB_SUCCESS;
  }
  // task_resp's error code indicate the last valid op result.
  if (OB_FAIL(task_resp.get_err_code())) {
    LOG_WARN("error occurring in remote das task, "
             "please use the current TRACE_ID to grep the original error message on the remote_addr.",
             K(ret), "remote_addr", task_resp.get_runner_svr());
    OB_ASSERT(op_results.count() <= task_ops.count());
  } else {
    // decode last op result
    if (OB_FAIL(task_op->decode_task_result(op_result))) {
      LOG_WARN("decode das task result failed", K(ret));
    }
  }
  // has_more flag only take effect on last valid op result.
  if (OB_FAIL(ret)) {
  } else if (task_resp.has_more()
              && OB_FAIL(setup_extra_result(das_ref, task_resp, task_op, extra_result))) {
    LOG_WARN("setup extra result failed", KR(ret));
  } else if (task_resp.has_more() && OB_FAIL(op_result->link_extra_result(*extra_result))) {
    LOG_WARN("link extra result failed", K(ret));
  }
  // even if trans result have a failure. It only take effect on the last valid op result.
  if (OB_NOT_NULL(session->get_tx_desc())) {
    int tmp_ret = MTL(transaction::ObTransService*)
      ->add_tx_exec_result(*session->get_tx_desc(),
                            task_resp.get_trans_result());
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("merge response partition failed", K(ret), K(tmp_ret), K(task_resp));
    }
    ret = COVER_SUCC(tmp_ret);
  }
  if (OB_FAIL(ret)) {
    // if error happened, it must be the last.
    // Special case is rpc error, because we cannot do a partition retry on rpc error.
    // So it's safe to only mark the first das task op failed.
    task_op->set_task_status(ObDasTaskStatus::FAILED);
    task_op->errcode_ = ret;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(task_op->state_advance())) {
      LOG_WARN("failed to advance das task state.",K(tmp_ret));
    }
  } else {
    // if no error happened, all tasks were executed successfully.
    task_op->set_task_status(ObDasTaskStatus::FINISHED);
    (void)loc_router.save_success_task(task_op->get_tablet_id());
    if (OB_FAIL(task_op->state_advance())) {
      LOG_WARN("failed to advance das task state.",K(ret));
    }
    if (OB_UNLIKELY(OB_SUCCESS != save_ret)) {
      ret = COVER_SUCC(save_ret);
    }
  }
  loc_router.save_cur_exec_status(ret);

  return ret;
}

int ObDataAccessService::collect_das_task_info(ObDASTaskArg &task_arg, ObDASRemoteInfo &remote_info)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObIDASTaskOp*, 2> &task_ops = task_arg.get_task_ops();
  ObIDASTaskOp *task_op = nullptr;
  for (int i = 0; OB_SUCC(ret) && i < task_ops.count(); i++) {
    task_op = task_ops.at(i);
    if (task_op->get_ctdef() != nullptr) {
      remote_info.has_expr_ |= task_op->get_ctdef()->has_expr();
      remote_info.need_calc_expr_ |= task_op->get_ctdef()->has_pdfilter_or_calc_expr();
      remote_info.need_calc_udf_ |= task_op->get_ctdef()->has_pl_udf();
      if (OB_FAIL(add_var_to_array_no_dup(remote_info.ctdefs_, task_op->get_ctdef()))) {
        LOG_WARN("store remote ctdef failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && task_op->get_rtdef() != nullptr) {
      if (OB_FAIL(add_var_to_array_no_dup(remote_info.rtdefs_, task_op->get_rtdef()))) {
        LOG_WARN("store remote rtdef failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(append_array_no_dup(remote_info.ctdefs_, task_op->get_related_ctdefs()))) {
        LOG_WARN("append task op related ctdefs to remote info failed", K(ret));
      } else if (OB_FAIL(append_array_no_dup(remote_info.rtdefs_, task_op->get_related_rtdefs()))) {
        LOG_WARN("append task op related rtdefs to remote info failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(collect_das_task_attach_info(remote_info, task_op->get_attach_rtdef()))) {
        LOG_WARN("collect das task attach info failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDataAccessService::collect_das_task_attach_info(ObDASRemoteInfo &remote_info,
                                                      ObDASBaseRtDef *attach_rtdef)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(attach_rtdef)) {
    if (attach_rtdef->ctdef_ != nullptr) {
      remote_info.has_expr_ |= attach_rtdef->ctdef_->has_expr();
      remote_info.need_calc_expr_ |= attach_rtdef->ctdef_->has_pdfilter_or_calc_expr();
      remote_info.need_calc_udf_ |= attach_rtdef->ctdef_->has_pl_udf();
      if (OB_FAIL(add_var_to_array_no_dup(remote_info.ctdefs_, attach_rtdef->ctdef_))) {
        LOG_WARN("store remote ctdef failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_var_to_array_no_dup(remote_info.rtdefs_, attach_rtdef))) {
        LOG_WARN("store remote rtdef failed", K(ret));
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < attach_rtdef->children_cnt_; ++i) {
      if (OB_FAIL(collect_das_task_attach_info(remote_info, attach_rtdef->children_[i]))) {
        LOG_WARN("recursively collect das task attach info failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDataAccessService::setup_extra_result(ObDASRef &das_ref,
                                            const ObDASTaskResp &task_resp,
                                            const ObIDASTaskOp *task_op,
                                            ObDASExtraData *&extra_result)
{
  int ret = OB_SUCCESS;
  extra_result = NULL;
  ObPhysicalPlanCtx *plan_ctx = das_ref.get_exec_ctx().get_physical_plan_ctx();
  int64_t timeout_ts = plan_ctx->get_timeout_timestamp();
  if (OB_UNLIKELY(!task_resp.has_more())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should not set up extra result", KR(ret), K(task_resp.has_more()));
  } else if (OB_FAIL(das_ref.get_das_factory().create_das_extra_data(extra_result))) {
    LOG_WARN("create das extra data failed", KR(ret));
  } else if (OB_FAIL(extra_result->init(task_op->get_task_id(),
                                        timeout_ts,
                                        task_resp.get_runner_svr(),
                                        GCTX.net_frame_->get_req_transport(),
                                        das_ref.enable_rich_format_))) {
    LOG_WARN("init extra data failed", KR(ret));
  } else {
    extra_result->set_has_more(true);
  }
  return ret;
}

void ObDataAccessService::set_max_concurrency(int32_t cpu_count)
{
  const int32_t das_concurrent_upper_limit = 8;
  const int32_t das_concurrent_factor = 5;
  if (OB_UNLIKELY(!is_user_tenant(MTL_ID()))) {
    das_concurrency_limit_ = 1;
  } else {
    das_concurrency_limit_ = min(cpu_count / das_concurrent_factor + 1, das_concurrent_upper_limit);
  }
  LOG_DEBUG("set current tenant's das max concurrency", K_(das_concurrency_limit), K(MTL_ID()));
}

}  // namespace sql
}  // namespace oceanbase
