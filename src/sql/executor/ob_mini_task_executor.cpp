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

#define USING_LOG_PREFIX SQL_EXE
#include "sql/executor/ob_mini_task_executor.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/executor/ob_task_spliter.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/engine/table/ob_multi_part_table_scan.h"
#include "sql/engine/table/ob_multi_part_table_scan_op.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/executor/ob_executor_rpc_processor.h"
namespace oceanbase {
using namespace common;
namespace sql {
REGISTER_CREATOR(ObAPMiniTaskMgrGFactory, ObAPMiniTaskMgr, ObAPMiniTaskMgr, 0);

int ObAPMiniTaskMgr::save_task_result(
    const ObAddr& task_addr, int64_t task_id, int32_t ret_code, const ObMiniTaskResult& result)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObMiniTaskEvent* mini_task_event = NULL;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMiniTaskEvent)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no more memory for mini task event", K(sizeof(ObMiniTaskEvent)));
  } else {
    mini_task_event = new (buf) ObMiniTaskEvent(task_addr, task_id, ret_code);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(mini_task_event->init())) {
      LOG_WARN("Failed to init result", K(*mini_task_event), K(ret));
    } else if (OB_FAIL(mini_task_event->set_mini_task_result(result))) {
      LOG_WARN("add task result to mini task event failed", K(*mini_task_event), K(ret), K(result));
    } else if (OB_FAIL(finish_queue_.push(mini_task_event))) {
      LOG_WARN("push finish task event failed", K(*mini_task_event), K(ret));
    }
  }
  if (OB_FAIL(ret) && mini_task_event != NULL) {
    mini_task_event->~ObMiniTaskEvent();
    allocator_.free(mini_task_event);
    mini_task_event = NULL;
  }
  return ret;
}

void ObAPMiniTaskMgr::close_task_event(ObMiniTaskEvent* task_event)
{
  if (task_event != NULL) {
    task_event->~ObMiniTaskEvent();
    allocator_.free(task_event);
    task_event = NULL;
  }
}

int ObAPMiniTaskMgr::pop_task_event(int64_t timeout, ObMiniTaskEvent*& complete_task)
{
  int ret = OB_SUCCESS;
  void* event_ptr = NULL;
  if (OB_FAIL(finish_queue_.pop(event_ptr, timeout))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail pop task event from finish queue", K(ret), K(timeout));
    }
  } else {
    complete_task = static_cast<ObMiniTaskEvent*>(event_ptr);
  }
  return ret;
}

int ObAPMiniTaskMgr::init(ObSQLSessionInfo& session, ObExecutorRpcImpl* exec_rpc)
{
  int ret = OB_SUCCESS;
  OZ(trans_result_.init(session, exec_rpc, NULL /*dist_task_mgr*/, this));
  OZ(finish_queue_.init(MAX_FINISH_QUEUE_CAPACITY));
  return ret;
}

void ObAPMiniTaskMgr::reset()
{
  int ret = OB_SUCCESS;
  ObDLinkBase<ObAPMiniTaskMgr>::reset();
  void* p = NULL;
  while (OB_SUCC(finish_queue_.pop(p, 0))) {
    if (p != NULL) {
      ObMiniTaskEvent* task_event = static_cast<ObMiniTaskEvent*>(p);
      task_event->~ObMiniTaskEvent();
      task_event = NULL;
      p = NULL;
    }
  }
  allocator_.reset();
  // here we can't call finish_queue_.reset(), because the reset() of light queue will only clear
  // the object in the light queue and not release the memory hold by finish_queue_
  // we must call destroy() to free the memory of finish_queue_
  finish_queue_.destroy();
  trans_result_.reset();
  ref_count_ = 0;
  mgr_rcode_ = OB_SUCCESS;
}

int ObAPMiniTaskMgr::merge_trans_result(const ObTaskID& task_id, const ObMiniTaskResult& result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(trans_result_.recv_result(task_id, result.get_task_result().get_trans_result()))) {
    LOG_WARN("fail to merge trans result",
        K(ret),
        K(task_id),
        "task_trans_result",
        result.get_task_result().get_trans_result());
  } else {
    LOG_DEBUG("mgr trans_result", "task_trans_result", result.get_task_result().get_trans_result());
  }
  return ret;
}

int ObAPMiniTaskMgr::set_task_status(const ObTaskID& task_id, ObTaskStatus status)
{
  return trans_result_.set_task_status(task_id, status);
}

int ObAPMiniTaskMgr::atomic_push_mgr_rcode_addr(const ObAddr& addr)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_FAIL(rcode_addrs_.push_back(addr))) {
    LOG_WARN("fail to push addr", K(ret));
  }
  return ret;
}

int ObMiniTaskExecutor::init(ObSQLSessionInfo& session, ObExecutorRpcImpl* exec_rpc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ap_mini_task_mgr_ != NULL)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ap_mini_task_mgr_ is null", K(ret));
  } else if (OB_UNLIKELY(NULL == (ap_mini_task_mgr_ = ObAPMiniTaskMgr::alloc()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for ap_mini_task_mgr_ failed", K(ret));
  } else {
    if (OB_FAIL(ap_mini_task_mgr_->init(session, exec_rpc))) {
      LOG_WARN("init ap_mini_task_mgr_ failed", K(ret));
    }
    // must inc ref count if alloc succ.
    ap_mini_task_mgr_->inc_ref_count();
  }
  return ret;
}

void ObMiniTaskExecutor::destroy()
{
  if (ap_mini_task_mgr_ != NULL) {
    ObAPMiniTaskMgr::free(ap_mini_task_mgr_);
    ap_mini_task_mgr_ = NULL;
  }
}

int ObMiniTaskExecutor::merge_trans_result(const ObTaskID& task_id, const ObMiniTaskResult& task_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ap_mini_task_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ap_mini_task_mgr is NULL", K(ret));
  } else {
    ap_mini_task_mgr_->merge_trans_result(task_id, task_result);
  }
  return ret;
}

int ObMiniTaskExecutor::wait_all_task(int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (!OB_ISNULL(ap_mini_task_mgr_) && OB_FAIL(ap_mini_task_mgr_->wait_all_task(timeout))) {
    LOG_WARN("wait all task failed", K(ret));
  }
  return ret;
}

int ObMiniTaskExecutor::add_invalid_servers_to_retry_info(
    const int ret, const ObIArray<ObAddr>& addrs, ObQueryRetryInfo& retry_info)
{
  int a_ret = OB_SUCCESS;
  if (OB_RPC_CONNECT_ERROR == ret) {
    for (int i = 0; i < addrs.count() && OB_SUCCESS == a_ret; ++i) {
      if (OB_UNLIKELY(OB_SUCCESS != (a_ret = retry_info.add_invalid_server_distinctly(addrs.at(i))))) {
        LOG_WARN("fail to add invalid server distinctly", K(a_ret), K(addrs.at(i)));
      }
    }
  } else if (OB_TIMEOUT == ret) {
    retry_info.set_is_rpc_timeout(true);
  }
  return a_ret;
}

int ObMiniTaskExecutor::wait_ap_task_finish(
    ObExecContext& ctx, int64_t ap_task_cnt, ObMiniTaskResult& result, ObMiniTaskRetryInfo& retry_info)
{
  int ret = OB_SUCCESS;
  ObMiniTaskEvent* complete_task = NULL;
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  ObSQLSessionInfo* session = ctx.get_my_session();
  CK(OB_NOT_NULL(plan_ctx));
  CK(OB_NOT_NULL(session));
  CK(OB_NOT_NULL(ap_mini_task_mgr_));
  for (int64_t i = 0; OB_SUCC(ret) && i < ap_task_cnt; ++i) {
    if (OB_FAIL(pop_ap_mini_task_event(ctx, complete_task))) {
      LOG_WARN("pop ap mini task event failed", K(ret), K(ap_task_cnt), K(i));
    } else if (OB_ISNULL(complete_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("complete task is null", K(ret));
    } else if (OB_FAIL(complete_task->get_errcode())) {
      LOG_WARN("while fetching task finish event, the task event rcode is not OB_SUCCESS", K(*complete_task), K(ret));
    } else if (OB_FAIL(check_scanner_errcode(*complete_task, retry_info))) {
      LOG_WARN("check scanner errcode failed", K(*complete_task), K(ret));
    } else if (OB_FAIL(result.append_mini_task_result(
                   complete_task->get_task_result(), session->use_static_typing_engine()))) {
      LOG_WARN("append task result failed", K(*complete_task), K(ret));
    } else {
      const ObScanner& meta_scanner = complete_task->get_task_result().get_task_result();
      ret = ObTaskExecutorCtxUtil::merge_task_result_meta(*plan_ctx, meta_scanner);
      LOG_DEBUG("Wait a ap task finish", KPC(complete_task), K(ap_task_cnt), K(i), K(meta_scanner.get_row_count()));
    }
    /**
     * ObRpcAPMiniDistExecuteCB::process() has called merge_result() before here, that is a
     * better place to call merge_result(), especially when any operation failed between
     * there and here.
     */
    ap_mini_task_mgr_->close_task_event(complete_task);
    complete_task = NULL;
    /*
     * need not continue wait all task here if we get any error, merge_result() will be called
     * in ObRpcAPMiniDistExecuteCB::process(), and ObAPMiniTaskMgr::free() will release all
     * complete_task when ref_count deduce to 0.
     * so we should call ObTransResultCollector::wait_all_task() A.S.A.P, who will kill all
     * running mini tasks to finish current execution A.S.A.P.
     */
  }
  return ret;
}

int ObMiniTaskExecutor::pop_ap_mini_task_event(ObExecContext& ctx, ObMiniTaskEvent*& complete_task)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_TIMEOUT_ONCE = 1000000;  // 1s
  bool is_pop_timeout = true;
  int64_t timeout_ts = 0;
  ObSQLSessionInfo* session = ctx.get_my_session();
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  CK(OB_NOT_NULL(session));
  CK(OB_NOT_NULL(plan_ctx));
  CK(OB_NOT_NULL(ap_mini_task_mgr_));
  if (OB_SUCC(ret)) {
    timeout_ts = plan_ctx->get_timeout_timestamp();
  }
  while (OB_SUCC(ret) && is_pop_timeout) {
    int64_t timeout_left = timeout_ts - ObTimeUtility::current_time();
    is_pop_timeout = false;
    if (OB_UNLIKELY(session->is_terminate(ret))) {
      LOG_WARN("query or session is killed", K(ret), K(timeout_left), K(timeout_ts));
    } else if (timeout_left <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("fail pop task event from finish queue, timeout", K(ret), K(timeout_left), K(timeout_ts));
    } else {
      int64_t timeout = MIN(timeout_left, MAX_TIMEOUT_ONCE);
      ret = ap_mini_task_mgr_->pop_task_event(timeout, complete_task);
      if (OB_ENTRY_NOT_EXIST == ret) {
        is_pop_timeout = true;
        ret = OB_SUCCESS;
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail pop task event from finish queue", K(ret), K(timeout_ts), K(timeout_left));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ap_mini_task_mgr_->get_mgr_rcode())) {
        ObQueryRetryInfo& retry_info = session->get_retry_info_for_update();
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = ObMiniTaskExecutor::add_invalid_servers_to_retry_info(
                               ret, ap_mini_task_mgr_->get_rcode_addr(), retry_info))) {
          LOG_WARN("fail to add invalid servers to retry info", K(tmp_ret));
        }
        LOG_WARN("ap mini task mgr status is invalid", K(ret));
      }
    }
  }
  return ret;
}

int ObMiniTaskExecutor::check_scanner_errcode(const ObMiniTaskEvent& complete_task, ObMiniTaskRetryInfo& retry_info)
{
  int ret = OB_SUCCESS;
  int result_ret = complete_task.get_task_result().get_task_result().get_err_code();
  int extend_ret = complete_task.get_task_result().get_extend_result().get_err_code();
  retry_info.process_minitask_event(complete_task, result_ret, extend_ret);
  if (OB_OVERSIZE_NEED_RETRY == result_ret && OB_SUCC(extend_ret)) {
    ret = OB_SUCCESS;
  } else if (OB_FAIL(check_scanner_errcode(complete_task.get_task_result()))) {
    LOG_WARN("Failed to check scanner errcode", K(ret));
  }
  return ret;
}

int ObMiniTaskExecutor::check_scanner_errcode(const ObMiniTaskResult& src)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(src.get_task_result().get_err_code())) {
    const char* err_msg = src.get_task_result().get_err_msg();
    FORWARD_USER_ERROR(ret, err_msg);
    LOG_WARN("while fetching scanner, the remote rcode is not OB_SUCCESS", K(ret), K(err_msg));
  } else if (OB_FAIL(src.get_extend_result().get_err_code())) {
    const char* err_msg = src.get_extend_result().get_err_msg();
    FORWARD_USER_ERROR(ret, err_msg);
    LOG_WARN("while fetching scanner, the remote rcode is not OB_SUCCESS", K(ret), K(err_msg));
  }
  return ret;
}

int ObMiniTaskExecutor::sync_fetch_local_result(ObExecContext& ctx, const ObOpSpec& root_spec, ObScanner& result)
{
  int ret = OB_SUCCESS;
  ObOperator* root_op = nullptr;
  if (OB_FAIL(root_spec.create_operator(ctx, root_op))) {
    LOG_WARN("create operator from spec failed", K(ret));
  } else {
    if (OB_FAIL(root_op->open())) {
      LOG_WARN("open root op failed", K(ret));
    }
    while (OB_SUCC(ret) && OB_SUCC(root_op->get_next_row())) {
      bool added = false;
      LOG_DEBUG("fetch local result", K(root_spec), "output", ROWEXPR2STR(*ctx.get_eval_ctx(), root_spec.output_));
      if (OB_FAIL(result.try_add_row(root_spec.output_, ctx.get_eval_ctx(), added))) {
        LOG_WARN("add row to result set failed", K(ret));
      } else if (!added) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("add row exceed memory limit", K(ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("get next row failed", K(ret));
    }
    int tmp_ret = root_op->close();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("close mini task query failed", K(tmp_ret));
    }
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  }

  return ret;
}

int ObMiniTaskExecutor::sync_fetch_local_result(ObExecContext& ctx, const ObPhyOperator& root_op, ObScanner& result)
{
  int ret = OB_SUCCESS;
  const ObNewRow* row = NULL;
  if (OB_FAIL(root_op.open(ctx))) {
    LOG_WARN("open root op failed", K(ret));
  }
  while (OB_SUCC(ret) && OB_SUCC(root_op.get_next_row(ctx, row))) {
    if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row is null");
    } else if (OB_FAIL(result.add_row(*row))) {
      LOG_WARN("add row to result set failed", K(ret));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("get next row failed", K(ret));
  }
  int tmp_ret = root_op.close(ctx);
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("close mini task query failed", K(tmp_ret));
  }
  ret = OB_SUCC(ret) ? tmp_ret : ret;
  return ret;
}

int ObMiniTaskExecutor::mini_task_local_execute(
    ObExecContext& query_ctx, ObMiniTask& task, ObMiniTaskResult& task_result)
{
  int ret = OB_SUCCESS;
  bool is_static_engine = task.get_ser_phy_plan().is_new_engine();
  if (is_static_engine) {
    const ObOpSpec* root_spec = task.get_root_spec();
    const ObOpSpec* extend_spec = task.get_extend_root_spec();
    ObExecContext* ctx = task.get_exec_context();
    ObPartitionLeaderArray pla;
    LOG_DEBUG("mini task local execute", K(task));
    if (OB_ISNULL(root_spec) || OB_ISNULL(ctx)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(root_spec), K(ctx));
    } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_participants(query_ctx, task, pla))) {
      LOG_WARN("get participants failed", K(ret));
    } else if (OB_FAIL(ObSqlTransControl::start_participant(query_ctx, pla.get_partitions()))) {
      LOG_WARN("start participant failed", K(ret), K(pla));
    } else {
      if (extend_spec != NULL) {
        if (OB_FAIL(sync_fetch_local_result(*ctx, *extend_spec, task_result.get_extend_result()))) {
          LOG_WARN("sync fetch local result failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sync_fetch_local_result(*ctx, *root_spec, task_result.get_task_result()))) {
          LOG_WARN("sync fetch local result", K(ret));
        }
      }
      /**
       * call end_participant() if and only if start_participant success,
       * no matter ret of other operations.
       */
      bool is_rollback = (OB_SUCCESS != ret);
      int ep_ret = ObSqlTransControl::end_participant(query_ctx, is_rollback, pla.get_partitions());
      if (OB_SUCCESS != ep_ret) {
        LOG_WARN("end participant failed", K(task.get_partition_keys()), K(ep_ret));
        if (OB_SUCCESS == ret) {
          ret = ep_ret;
        }
      }
    }
  } else {
    const ObPhyOperator* root_op = task.get_root_op();
    const ObPhyOperator* extend_op = task.get_extend_root();
    ObExecContext* ctx = task.get_exec_context();
    ObPartitionLeaderArray pla;
    LOG_DEBUG("mini task local execute", K(task));
    if (OB_ISNULL(root_op) || OB_ISNULL(ctx)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(root_op), K(ctx));
    } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_participants(query_ctx, task, pla))) {
      LOG_WARN("get participants failed", K(ret));
    } else if (OB_FAIL(ObSqlTransControl::start_participant(query_ctx, pla.get_partitions()))) {
      LOG_WARN("start participant failed", K(ret), K(pla));
    } else {
      if (extend_op != NULL) {
        if (OB_FAIL(sync_fetch_local_result(*ctx, *extend_op, task_result.get_extend_result()))) {
          LOG_WARN("sync fetch local result failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sync_fetch_local_result(*ctx, *root_op, task_result.get_task_result()))) {
          LOG_WARN("sync fetch local result", K(ret));
        }
      }
      /**
       * call end_participant() if and only if start_participant success,
       * no matter ret of other operations.
       */
      bool is_rollback = (OB_SUCCESS != ret);
      int ep_ret = ObSqlTransControl::end_participant(query_ctx, is_rollback, pla.get_partitions());
      if (OB_SUCCESS != ep_ret) {
        LOG_WARN("end participant failed", K(task.get_partition_keys()), K(ep_ret));
        if (OB_SUCCESS == ret) {
          ret = ep_ret;
        }
      }
    }
  }
  return ret;
}

int ObDMLMiniTaskExecutor::execute(ObExecContext& ctx, const ObMiniJob& mini_job, ObIArray<ObTaskInfo*>& task_list,
    bool table_first, ObMiniTaskResult& task_result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_list.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task list is empty");
  } else if (OB_LIKELY(table_first)) {
    if (OB_ISNULL(task_list.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("first mini task info is null");
    } else if (OB_FAIL(mini_task_execute(ctx, mini_job, *task_list.at(0), task_result))) {
      LOG_WARN("mini task execute failed", K(ret));
    } else {
      LOG_DEBUG("mini task execute task first", "task_info", *task_list.at(0));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t start_idx = table_first ? 1 : 0;
    if (OB_FAIL(mini_task_submit(ctx, mini_job, task_list, start_idx, task_result))) {
      LOG_WARN("mini task submit failed", K(ret), K(start_idx));
    }
  }
  return ret;
}

int ObDMLMiniTaskExecutor::mini_task_execute(
    ObExecContext& ctx, const ObMiniJob& mini_job, ObTaskInfo& task_info, ObMiniTaskResult& task_result)
{
  int ret = OB_SUCCESS;
  ObMiniTask mini_task;
  ObExecutorRpcImpl* rpc = NULL;
  ObSQLSessionInfo* session = ctx.get_my_session();
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(session) || OB_ISNULL(ap_mini_task_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan context is null", KP(plan_ctx), KP(session), KP(ap_mini_task_mgr_));
  } else if (OB_FAIL(build_mini_task_op_input(ctx, task_info, mini_job))) {
    LOG_WARN("build mini task op input failed", K(ret), K(mini_job));
  } else if (OB_FAIL(build_mini_task(ctx, mini_job, task_info, mini_task))) {
    LOG_WARN("build mini task failed", K(ret));
  } else if (mini_task.get_runner_server() == mini_task.get_ctrl_server()) {
    if (OB_FAIL(mini_task_local_execute(ctx, mini_task, task_result))) {
      LOG_WARN("fail to do mini task local execute", K(ret));
    } else if (mini_task.get_exec_context() != &ctx) {
      ObPhysicalPlanCtx* subplan_ctx = mini_task.get_exec_context()->get_physical_plan_ctx();
      if (OB_ISNULL(subplan_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("plan context is null", K(subplan_ctx));
      } else {
        ret = ObTaskExecutorCtxUtil::merge_task_result_meta(*plan_ctx, *subplan_ctx);
      }
    }
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(ctx, rpc))) {
    LOG_WARN("get task executor rpc failed", K(ret));
  } else if (OB_ISNULL(rpc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc is null");
  } else {
    ObMiniTaskResult remote_result;
    ObQueryRetryInfo* retry_info = &session->get_retry_info_for_update();
    ObExecutorRpcCtx rpc_ctx(session->get_rpc_tenant_id(),
        plan_ctx->get_timeout_timestamp(),
        ctx.get_task_exec_ctx().get_min_cluster_version(),
        retry_info,
        session,
        plan_ctx->is_plain_select_stmt());
    if (OB_FAIL(ap_mini_task_mgr_->send_task(task_info))) {
      LOG_WARN("send task failed", K(ret), K(task_info));
    } else if (OB_FAIL(rpc->mini_task_execute(rpc_ctx, mini_task, remote_result))) {
      LOG_WARN("mini task execute failed", K(ret), K(mini_task.get_ob_task_id()));
    } else if (OB_FAIL(merge_trans_result(task_info.get_ob_task_id(), remote_result))) {
      LOG_WARN("merge trans result failed",
          K(ret),
          K(mini_task.get_ob_task_id()),
          "session_trans_result",
          session->get_trans_result(),
          "remote_trans_result",
          remote_result.get_task_result().get_trans_result());
    } else if (OB_FAIL(check_scanner_errcode(remote_result))) {
      LOG_WARN("check scanner errcode failed", K(ret));
    } else if (OB_FAIL(
                   task_result.append_mini_task_result(remote_result, mini_task.get_ser_phy_plan().is_new_engine()))) {
      LOG_WARN("append task result failed", K(ret));
    } else {
      ret = ObTaskExecutorCtxUtil::merge_task_result_meta(*plan_ctx, remote_result.get_task_result());
    }
  }
  return ret;
}

int ObDMLMiniTaskExecutor::mini_task_submit(ObExecContext& ctx, const ObMiniJob& mini_job,
    ObIArray<ObTaskInfo*>& task_info_list, int64_t start_idx, ObMiniTaskResult& task_result)
{
  int ret = OB_SUCCESS;
  ObExecutorRpcImpl* rpc = NULL;
  ObSQLSessionInfo* session = ctx.get_my_session();
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  ObTaskInfo* local_task_info = NULL;
  int64_t ap_task_cnt = 0;
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(session) || OB_ISNULL(ap_mini_task_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan context is null", KP(plan_ctx), KP(session), KP(ap_mini_task_mgr_));
  }
  for (int64_t i = start_idx; OB_SUCC(ret) && i < task_info_list.count(); ++i) {
    ObTaskInfo* task_info = task_info_list.at(i);
    ObMiniTask mini_task;
    if (OB_ISNULL(task_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get root op of task failed", K(task_info));
    } else if (task_info->get_task_location().get_server() == ctx.get_addr()) {
      if (OB_UNLIKELY(local_task_info != NULL)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("local task don't alone");
      } else {
        local_task_info = task_info;
      }
    } else if (OB_FAIL(build_mini_task_op_input(ctx, *task_info, mini_job))) {
      LOG_WARN("build mini task op input failed", K(ret));
    } else if (OB_FAIL(build_mini_task(ctx, mini_job, *task_info, mini_task))) {
      LOG_WARN("build mini task failed", K(ret));
    } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(ctx, rpc))) {
      LOG_WARN("get task executor rpc failed", K(ret));
    } else if (OB_ISNULL(rpc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rpc is null");
    } else {
      ObQueryRetryInfo* retry_info = &session->get_retry_info_for_update();
      ObExecutorRpcCtx rpc_ctx(session->get_rpc_tenant_id(),
          plan_ctx->get_timeout_timestamp(),
          ctx.get_task_exec_ctx().get_min_cluster_version(),
          retry_info,
          session,
          plan_ctx->is_plain_select_stmt(),
          ap_mini_task_mgr_);
      if (OB_FAIL(ap_mini_task_mgr_->send_task(*task_info))) {
        LOG_WARN("send task failed", K(ret), K(*task_info));
      } else if (OB_FAIL(rpc->mini_task_submit(rpc_ctx, mini_task))) {
        LOG_WARN("mini task execute failed", K(ret), K(mini_task), KPC(task_info));
      } else {
        ++ap_task_cnt;
        LOG_DEBUG("mini task rpc submit", K(mini_task), K(ap_task_cnt));
      }
    }
  }
  if (OB_SUCC(ret) && local_task_info != NULL) {
    ObMiniTask local_task;
    if (OB_FAIL(build_mini_task_op_input(ctx, *local_task_info, mini_job))) {
      LOG_WARN("build mini task op input failed", K(ret), K(mini_job));
    } else if (OB_FAIL(build_mini_task(ctx, mini_job, *local_task_info, local_task))) {
      LOG_WARN("build mini task failed", K(ret));
    } else if (OB_FAIL(mini_task_local_execute(ctx, local_task, task_result))) {
      LOG_WARN("mini task local execute failed", K(ret), K(local_task), KPC(local_task_info));
    } else if (local_task.get_exec_context() != &ctx) {
      ObPhysicalPlanCtx* subplan_ctx = local_task.get_exec_context()->get_physical_plan_ctx();
      if (OB_ISNULL(subplan_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("plan context is null", K(subplan_ctx));
      } else if (OB_FAIL(ObTaskExecutorCtxUtil::merge_task_result_meta(*plan_ctx, *subplan_ctx))) {
        LOG_WARN("merge task result meta failed", K(ret), K(local_task.get_ob_task_id()));
      }
    }
  }
  int saved_ret = ret;
  if (/*OB_SUCC(ret) &&*/ ap_task_cnt > 0) {
    ObMiniTaskRetryInfo retry_info;
    if (OB_FAIL(wait_ap_task_finish(ctx, ap_task_cnt, task_result, retry_info))) {
      LOG_WARN("wait ap task finish failed", K(ret));
    }
  }
  ret = (OB_SUCCESS != saved_ret) ? saved_ret : ret;
  return ret;
}

int ObDMLMiniTaskExecutor::build_mini_task_op_input(
    ObExecContext& ctx, ObTaskInfo& task_info, const ObMiniJob& mini_job)
{
  int ret = OB_SUCCESS;
  const ObPhysicalPlan* phy_plan = mini_job.get_phy_plan();
  if (OB_ISNULL(phy_plan)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("phy_plan is null", K(ret));
  } else if (phy_plan->is_new_engine()) {
    if (OB_ISNULL(mini_job.get_root_spec())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("mini job main query root op is null");
    } else if (OB_FAIL(build_mini_task_op_input(ctx, task_info, *mini_job.get_root_spec()))) {
      LOG_WARN("build mini task op input failed", K(ret));
    } else if (mini_job.get_extend_spec() != NULL) {
      if (OB_FAIL(build_mini_task_op_input(ctx, task_info, *mini_job.get_extend_spec()))) {
        LOG_WARN("build mini task op input failed", K(ret));
      }
    }
  } else {
    if (OB_ISNULL(mini_job.get_root_op())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("mini job main query root op is null");
    } else if (OB_FAIL(build_mini_task_op_input(ctx, task_info, *mini_job.get_root_op()))) {
      LOG_WARN("build mini task op input failed", K(ret));
    } else if (mini_job.get_extend_op() != NULL) {
      if (OB_FAIL(build_mini_task_op_input(ctx, task_info, *mini_job.get_extend_op()))) {
        LOG_WARN("build mini task op input failed", K(ret));
      }
    }
  }

  return ret;
}

int ObDMLMiniTaskExecutor::build_mini_task_op_input(
    ObExecContext& ctx, ObTaskInfo& task_info, const ObOpSpec& root_spec)
{
  int ret = OB_SUCCESS;
  ObOpInput* op_input = NULL;
  int64_t index = root_spec.id_;
  ObOperatorKit* kit = ctx.get_kit_store().get_operator_kit(index);
  if (nullptr == kit) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op input is NULL", K(ret), K(index));
  } else if (OB_ISNULL(op_input = kit->input_)) {
    LOG_DEBUG("no op input", K(root_spec));
    // do nothing
  } else {
    op_input->reset();
    if (OB_FAIL(op_input->init(task_info))) {
      LOG_WARN("init operator input failed", K(ret));
    }
  }

  for (int32_t i = 0; OB_SUCC(ret) && i < root_spec.get_child_cnt(); ++i) {
    const ObOpSpec* child_spec = root_spec.get_child(i);
    if (OB_ISNULL(child_spec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null child operator", K(i), K(root_spec.type_));
    } else if (OB_FAIL(build_mini_task_op_input(ctx, task_info, *child_spec))) {
      LOG_WARN("fail to build child op input", K(ret), K(child_spec->get_id()), K(task_info));
    }
  }

  return ret;
}

int ObDMLMiniTaskExecutor::build_mini_task_op_input(
    ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& root_op)
{
  int ret = OB_SUCCESS;
  const ObPhyOperator* child_op = NULL;
  ObIPhyOperatorInput* op_input = NULL;
  op_input = GET_PHY_OP_INPUT(ObIPhyOperatorInput, ctx, root_op.get_id());
  if (OB_ISNULL(op_input)) {
    if (OB_FAIL(root_op.create_operator_input(ctx))) {
      LOG_WARN("create operator input failed", K(ret));
    } else {
      op_input = GET_PHY_OP_INPUT(ObIPhyOperatorInput, ctx, root_op.get_id());
    }
  }
  if (OB_SUCC(ret) && op_input != NULL) {
    op_input->reset();
    op_input->set_deserialize_allocator(&ctx.get_allocator());
    if (OB_FAIL(op_input->init(ctx, task_info, root_op))) {
      LOG_WARN("init operator input failed", K(ret));
    }
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < root_op.get_child_num(); ++i) {
    if (OB_ISNULL(child_op = root_op.get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child op is null");
    } else if (OB_FAIL(build_mini_task_op_input(ctx, task_info, *child_op))) {
      LOG_WARN("fail to build child op input", K(ret), K(task_info), K(child_op->get_id()));
    }
  }
  return ret;
}

inline int ObDMLMiniTaskExecutor::build_mini_task(
    ObExecContext& ctx, const ObMiniJob& mini_job, ObTaskInfo& task_info, ObMiniTask& task)
{
  int ret = OB_SUCCESS;
  const ObTaskInfo::ObRangeLocation& range_loc = task_info.get_range_location();
  if (OB_ISNULL(mini_job.get_phy_plan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("phy plan is null", K(ret));
  } else if (mini_job.get_phy_plan()->is_new_engine()) {
    if (OB_ISNULL(mini_job.get_root_spec())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("mini job is invalid", K(mini_job.get_root_spec()), K(ret));
    } else {
      task.set_extend_root_spec(const_cast<ObOpSpec*>(mini_job.get_extend_spec()));
      task.set_serialize_param(&ctx, const_cast<ObOpSpec*>(mini_job.get_root_spec()), mini_job.get_phy_plan());
    }
  } else {
    if (OB_ISNULL(mini_job.get_root_op())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("mini job is invalid", K(mini_job.get_root_op()));
    } else {
      task.set_extend_root(const_cast<ObPhyOperator*>(mini_job.get_extend_op()));
      task.set_serialize_param(ctx, const_cast<ObPhyOperator&>(*mini_job.get_root_op()), *mini_job.get_phy_plan());
    }
  }
  if (OB_SUCC(ret)) {
    task.set_ctrl_server(ctx.get_addr());
    task.set_ob_task_id(task_info.get_task_location().get_ob_task_id());
    task.set_runner_server(task_info.get_task_location().get_server());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < range_loc.part_locs_.count(); ++i) {
    const ObTaskInfo::ObPartLoc& part_loc = range_loc.part_locs_.at(i);
    if (OB_FAIL(task.add_partition_key(part_loc.partition_key_))) {
      LOG_WARN("fail to add partition key into ObTask", K(ret), K(i), K(part_loc.partition_key_));
    }
  }
  return ret;
}

int ObLookupMiniTaskExecutor::execute(ObExecContext& ctx, common::ObIArray<ObMiniTask>& task_list,
    common::ObIArray<ObTaskInfo*>& task_info_list, ObMiniTaskRetryInfo& retry_info, ObMiniTaskResult& task_result)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<int64_t, 64> not_master_tasks_id;
  ObTaskExecutorCtx* task_exec_ctx = ctx.get_task_executor_ctx();
  if (task_list.count() != task_info_list.count() || task_info_list.count() == 0 || task_list.count() == 0 ||
      OB_ISNULL(task_exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "every task must own a task info", K(ret), K(task_list.count()), K(task_info_list.count()), K(task_exec_ctx));
  } else {
    int64_t ap_task_cnt = 0;
    for (int64_t i = 1; i < task_list.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(execute_one_task(ctx, task_list.at(i), task_info_list.at(i), ap_task_cnt, retry_info))) {
        LOG_WARN("execute failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(task_info_list.at(0))) {
      ObMiniTask& task = task_list.at(0);
      ObTaskInfo* task_info = task_info_list.at(0);
      if (!task.get_ser_phy_plan().is_new_engine() &&
          OB_FAIL(fill_lookup_task_op_input(
              *task.get_exec_context(), task, task_info, *task.get_root_op(), retry_info.is_retry_execution()))) {
        LOG_WARN("build mini task op input failed", K(ret));
      } else if (task.get_ser_phy_plan().is_new_engine() &&
                 OB_FAIL(fill_lookup_task_op_input(
                     *task.get_exec_context(), task_info, *task.get_root_spec(), retry_info.is_retry_execution()))) {
        LOG_WARN("build mini task op input failed", K(ret));
      } else if (OB_FAIL(mini_task_local_execute(ctx, task_list.at(0), task_result))) {
        LOG_WARN("mini task local execute failed", K(ret));
      }
      LOG_TRACE("Got local mini task result", K(task_result.get_task_result().get_row_count()));
    }
    if (OB_SUCC(ret) && ap_task_cnt > 0) {
      if (OB_FAIL(wait_ap_task_finish(ctx, ap_task_cnt, task_result, retry_info))) {
        LOG_WARN("wait ap task finish failed", K(ret));
      }
    }
  }

  if (!retry_info.get_not_master_task_id().empty()) {
    int tmp_ret = ret;
    ret = OB_SUCCESS;
    common::ObIArray<int64_t>& not_master_task_id = retry_info.get_not_master_task_id();
    common::ObPartitionArray partition_keys;
    for (int64_t i = 0; OB_SUCC(ret) && i < not_master_task_id.count(); ++i) {
      int64_t task_id = not_master_task_id.at(i);
      if (task_id >= task_list.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected task id", K(ret), K(task_id));
      } else {
        ObTask& task = task_list.at(task_id);
        if (OB_FAIL(append_array_no_dup(partition_keys, task.get_partition_keys()))) {
          LOG_WARN("failed to append array no dup", K(ret));
        }
      }
    }
    IGNORE_RETURN ObTaskExecutorCtxUtil::try_nonblock_refresh_location_cache(task_exec_ctx, partition_keys);
    ret = tmp_ret;
  }
  return ret;
}

int ObLookupMiniTaskExecutor::execute_one_task(
    ObExecContext& ctx, ObMiniTask& task, ObTaskInfo* task_info, int64_t& ap_task_cnt, ObMiniTaskRetryInfo& retry_info)
{
  int ret = OB_SUCCESS;
  ObExecutorRpcImpl* rpc = NULL;
  ObSQLSessionInfo* session = ctx.get_my_session();
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  bool is_static_engine = task.get_ser_phy_plan().is_new_engine();
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan context is null", K(plan_ctx), K(session), K(ret));
  } else if (OB_ISNULL(task.get_exec_context()) || OB_ISNULL(ap_mini_task_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get root op of task failed", K(ret), K(task.get_exec_context()));
  } else if ((!is_static_engine && OB_ISNULL(task.get_root_op())) ||
             (is_static_engine && OB_ISNULL(task.get_root_spec()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(is_static_engine), K(task));
  } else if (!is_static_engine &&
             OB_FAIL(fill_lookup_task_op_input(
                 *task.get_exec_context(), task, task_info, *task.get_root_op(), retry_info.is_retry_execution()))) {
    LOG_WARN("build mini task op input failed", K(ret));
  } else if (is_static_engine &&
             OB_FAIL(fill_lookup_task_op_input(
                 *task.get_exec_context(), task_info, *task.get_root_spec(), retry_info.is_retry_execution()))) {
    LOG_WARN("build mini task op input failed", K(ret));
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(ctx, rpc))) {
    LOG_WARN("get task executor rpc failed", K(ret));
  } else if (OB_ISNULL(rpc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc is null", K(ret));
  } else {
    LOG_DEBUG("lookup mini task executor", KPC(task_info), K(ap_task_cnt));
    ObQueryRetryInfo* retry_info = &session->get_retry_info_for_update();
    ObExecutorRpcCtx rpc_ctx(session->get_rpc_tenant_id(),
        plan_ctx->get_timeout_timestamp(),
        ctx.get_task_exec_ctx().get_min_cluster_version(),
        retry_info,
        session,
        plan_ctx->is_plain_select_stmt(),
        ap_mini_task_mgr_);
    if (!task_info->get_ob_task_id().is_valid()) {
      task_info->get_task_location().set_ob_task_id(task.get_ob_task_id());
    }
    if (OB_FAIL(ap_mini_task_mgr_->send_task(*task_info))) {
      LOG_WARN("send task failed", K(ret), K(*task_info));
    } else if (OB_FAIL(rpc->mini_task_submit(rpc_ctx, task))) {
      LOG_WARN("mini task execute failed", K(ret));
    } else {
      ++ap_task_cnt;
    }
  }
  return ret;
}

int ObLookupMiniTaskExecutor::fill_lookup_task_op_input(ObExecContext& ctx, ObMiniTask& task, ObTaskInfo* task_info,
    const ObPhyOperator& root_op, const bool retry_execution)
{
  int ret = OB_SUCCESS;
  const ObPhyOperator* child_op = NULL;
  ObIPhyOperatorInput* op_input = NULL;
  // if (MULTI_PARTITION_TABLE_SCAN != root->op.get_type()) {
  //  ret = OB_NOT_SUPPORTED;
  //  LOG_WARN("this function is not sure work well with the op", K(ret), K(root_op.get_type()));
  //} else
  if (OB_ISNULL(op_input = GET_PHY_OP_INPUT(ObIPhyOperatorInput, ctx, root_op.get_id()))) {
    LOG_WARN("the op input can not be null", K(ret), K(root_op.get_type()));
  } else if (OB_ISNULL(task_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task_info can not be null", K(ret));
  } else {
    task_info->set_task_split_type(ObTaskSpliter::DISTRIBUTED_SPLIT);
    op_input->reset();
    if (OB_FAIL(op_input->init(ctx, *task_info, root_op))) {
      LOG_WARN("init operator input failed", K(ret));
    } else if (PHY_MULTI_PART_TABLE_SCAN == op_input->get_phy_op_type()) {
      ObMultiPartTableScanInput* input = static_cast<ObMultiPartTableScanInput*>(op_input);
      if (retry_execution) {
        input->partitions_ranges_.set_range_mode();
      }
    }
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < root_op.get_child_num(); ++i) {
    if (OB_ISNULL(child_op = root_op.get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child op is null");
    } else if (OB_FAIL(fill_lookup_task_op_input(ctx, task, task_info, *child_op, retry_execution))) {
      LOG_WARN("fail to build child op input", K(ret), K(child_op->get_id()));
    }
  }
  return ret;
}

int ObLookupMiniTaskExecutor::fill_lookup_task_op_input(
    ObExecContext& ctx, ObTaskInfo* task_info, const ObOpSpec& root_spec, const bool retry_execution)
{
  int ret = OB_SUCCESS;
  ObOpInput* op_input = NULL;
  int64_t index = root_spec.id_;
  ObOperatorKit* kit = ctx.get_kit_store().get_operator_kit(index);
  if (nullptr == kit) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op input is NULL", K(ret), K(index));
  } else if (OB_ISNULL(op_input = kit->input_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("op input is null", K(op_input), K(ret), K(root_spec));
  } else {
    task_info->set_task_split_type(ObTaskSpliter::DISTRIBUTED_SPLIT);
    op_input->reset();
    if (OB_FAIL(op_input->init(*task_info))) {
      LOG_WARN("init operator input failed", K(ret));
    } else if (PHY_MULTI_PART_TABLE_SCAN == root_spec.type_) {
      ObMultiPartTableScanOpInput* input = static_cast<ObMultiPartTableScanOpInput*>(op_input);
      if (retry_execution) {
        input->partitions_ranges_.set_range_mode();
      }
    }
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < root_spec.get_child_cnt(); ++i) {
    const ObOpSpec* child_op = root_spec.get_child(i);
    if (OB_ISNULL(child_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null child operator", K(i), K(root_spec.type_));
    } else if (OB_FAIL(fill_lookup_task_op_input(ctx, task_info, *child_op, retry_execution))) {
      LOG_WARN("fail to build child op input", K(ret), K(child_op->get_id()));
    }
  }

  return ret;
}

int ObLookupMiniTaskExecutor::retry_overflow_task(ObExecContext& ctx, common::ObIArray<ObMiniTask>& task_list,
    common::ObIArray<ObTaskInfo*>& task_info_list, ObMiniTaskRetryInfo& retry_info, ObMiniTaskResult& task_result)
{
  int ret = OB_SUCCESS;
  const int64_t parallel = 4;
  ObSEArray<ObMiniTask, parallel> parallel_task_list;
  ObSEArray<ObTaskInfo*, parallel> parallel_task_info_list;
  ObMiniTask empty_task;
  ObTaskInfo* empty_task_info = nullptr;
  retry_info.reuse_task_list();
  if (task_list.count() != task_info_list.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unmatched task list", K(ret), K(task_list.count()));
  }
  ARRAY_FOREACH(task_list, i)
  {
    if (0 == i) {
      // Never retry local task, do nothing
    } else if (parallel_task_list.empty() && OB_FAIL(parallel_task_list.push_back(empty_task))) {
      LOG_WARN("Failed to push back empty local task", K(ret));
    } else if (parallel_task_info_list.empty() && OB_FAIL(parallel_task_info_list.push_back(empty_task_info))) {
      LOG_WARN("Failed to push back empty local taskinfo", K(ret));
    } else if (parallel_task_list.count() < parallel && OB_FAIL(parallel_task_list.push_back(task_list.at(i)))) {
      LOG_WARN("Failed to push back remote task", K(ret));
    } else if (parallel_task_info_list.count() < parallel &&
               OB_FAIL(parallel_task_info_list.push_back(task_info_list.at(i)))) {
      LOG_WARN("Failed to push back remote taskinfo", K(ret));
    } else if (parallel_task_list.count() == parallel ||
               (task_list.count() - 1 == i && parallel_task_list.count() > 1)) {
      if (OB_FAIL(execute(ctx, parallel_task_list, parallel_task_info_list, retry_info, task_result))) {
        LOG_WARN("Failed to remote execute table scan", K(ret));
      }
      parallel_task_list.reuse();
      parallel_task_info_list.reuse();
    } else {
      // do nothing
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
