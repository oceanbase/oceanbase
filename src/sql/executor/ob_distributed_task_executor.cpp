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

#include "sql/session/ob_sql_session_info.h"
#include "sql/executor/ob_distributed_task_executor.h"
#include "sql/executor/ob_distributed_transmit.h"
#include "sql/executor/ob_receive.h"
#include "sql/executor/ob_job.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/executor/ob_executor_rpc_impl.h"
#include "sql/executor/ob_trans_result_collector.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_des_exec_context.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/executor/ob_bkgd_dist_task.h"
#include "sql/executor/ob_executor_rpc_processor.h"
#include "rootserver/ob_root_service.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

ObDistributedTaskExecutor::ObDistributedTaskExecutor(const uint64_t scheduler_id)
    : scheduler_id_(scheduler_id), trans_result_(NULL)
{}

ObDistributedTaskExecutor::~ObDistributedTaskExecutor()
{}

int ObDistributedTaskExecutor::execute(ObExecContext& query_ctx, ObJob* job, ObTaskInfo* task_info)
{
  int ret = OB_SUCCESS;
  ObPhyOperator* root_op = NULL;
  ObDistributedTransmitInput* trans_input = NULL;
  ObIPhyOperatorInput* op_input = NULL;
  ObExecutorRpcImpl* rpc = NULL;
  ObExecContext* exec_ctx_snap = NULL;
  ObSQLSessionInfo* session_snap = NULL;
  ObPhysicalPlanCtx* plan_ctx_snap = NULL;
  ObTask task;
  if (OB_ISNULL(exec_ctx_snap = query_ctx.get_scheduler_thread_ctx().get_dis_exec_ctx_for_update())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("exec ctx snap is NULL", K(ret));
  } else if (OB_ISNULL(session_snap = exec_ctx_snap->get_my_session()) ||
             OB_ISNULL(plan_ctx_snap = exec_ctx_snap->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session snap or plan ctx snap is NULL", K(ret), K(session_snap), K(plan_ctx_snap));
  } else if (OB_I(t1)(OB_ISNULL(job) || OB_ISNULL(task_info))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job or taskinfo is not set", K(ret), K(job), K(task_info));
  } else if (OB_I(t2)(OB_ISNULL(root_op = job->get_root_op()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail execute task. no root op found", K(ret), K(root_op));
  } else if (OB_I(t3)(OB_UNLIKELY(!IS_DIST_TRANSMIT(root_op->get_type())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("valid check fail. root op type must be ObTransmit", K(ret), K(root_op->get_type()));
  } else if (OB_I(t4)(OB_ISNULL(op_input = exec_ctx_snap->get_phy_op_input(root_op->get_id())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op input", K(ret), "op_id", root_op->get_id());
  } else if (OB_UNLIKELY(!IS_DIST_TRANSMIT(op_input->get_phy_op_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Optimizer should generate PHY_DISTRIBUTED_TRANSMIT for this plan",
        K(ret),
        "input_type",
        op_input->get_phy_op_type());
  } else if (OB_ISNULL(trans_input = static_cast<ObDistributedTransmitInput*>(op_input))) {
    ret = OB_ERR_UNEXPECTED;  // should never reach here
    LOG_WARN("fail cast op", K(ret), "trans_input", trans_input);
  } else if (OB_FAIL(OB_I(t6) ObTaskExecutorCtxUtil::get_task_executor_rpc(query_ctx, rpc))) {
    LOG_WARN("fail get rpc", K(ret));
  } else if (OB_ISNULL(rpc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc is NULL", K(ret));
  } else if (OB_FAIL(build_task(*exec_ctx_snap, *job, *task_info, task))) {
    LOG_WARN("fail to build task", K(ret), K(*job), K(*task_info));
  } else {
    ObExecutorRpcCtx rpc_ctx(session_snap->get_rpc_tenant_id(),
        plan_ctx_snap->get_timeout_timestamp(),
        exec_ctx_snap->get_task_exec_ctx().get_min_cluster_version(),
        &query_ctx.get_scheduler_thread_ctx().get_scheduler_retry_info_for_update(),
        query_ctx.get_my_session(),
        query_ctx.get_scheduler_thread_ctx().is_plain_select_stmt());
    task_info->set_task_send_begin(ObTimeUtility::current_time());
    task_info->set_state(OB_TASK_STATE_RUNNING);
    trans_input->set_ob_task_id(task_info->get_task_location().get_ob_task_id());
    if (OB_FAIL(task_dispatch(*exec_ctx_snap, *rpc, rpc_ctx, task, *task_info))) {
      bool skip_failed_tasks = false;
      int check_ret = OB_SUCCESS;
      if (OB_SUCCESS != (check_ret = should_skip_failed_tasks(*task_info, skip_failed_tasks))) {
        // check fail, set ret to check_ret
        LOG_WARN("fail to check if should skip failed tasks", K(ret), K(check_ret), K(*job), K(rpc_ctx));
        ret = check_ret;
      } else if (true == skip_failed_tasks) {
        // should skip failed tasks, log user warning and skip it, than return OB_ERR_TASK_SKIPPED
        LOG_WARN("fail to do task on some server, log user warning and skip it",
            K(ret),
            K(task_info->get_task_location().get_server()),
            K(*job),
            K(rpc_ctx));
        LOG_USER_WARN(OB_ERR_TASK_SKIPPED,
            to_cstring(task_info->get_task_location().get_server()),
            common::ob_errpkt_errno(ret, lib::is_oracle_mode()));
        ret = OB_ERR_TASK_SKIPPED;
      } else {
        // let user see this ret
        LOG_WARN("fail to submit task", K(ret), K(*task_info), K(rpc_ctx));
      }
    } else {
    }
    NG_TRACE_EXT(distributed_task_submited,
        OB_ID(ret),
        ret,
        OB_ID(runner_svr),
        task_info->get_task_location().get_server(),
        OB_ID(task),
        task);
  }
  return ret;
}

int ObDistributedTaskExecutor::kill(ObExecContext& query_ctx, ObJob* job, ObTaskInfo* task_info)
{
  int ret = OB_SUCCESS;
  ObPhyOperator* root_op = NULL;
  ObExecutorRpcImpl* rpc = NULL;
  const ObExecContext* exec_ctx_snap = NULL;
  ObSQLSessionInfo* session_snap = NULL;
  ObPhysicalPlanCtx* plan_ctx_snap = NULL;
  if (OB_ISNULL(exec_ctx_snap = query_ctx.get_scheduler_thread_ctx().get_dis_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("exec ctx snap is NULL", K(ret));
  } else if (OB_ISNULL(session_snap = exec_ctx_snap->get_my_session()) ||
             OB_ISNULL(plan_ctx_snap = exec_ctx_snap->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session snap or plan ctx snap is NULL", K(ret), K(session_snap), K(plan_ctx_snap));
  } else if (OB_I(t1)(OB_ISNULL(task_info) || OB_ISNULL(job))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is NULL", K(ret), K(task_info), K(job));
  } else if (OB_I(t2)(OB_ISNULL(root_op = job->get_root_op()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail execute task. no root op found", K(ret), K(*job));
  } else if (OB_I(t3) OB_UNLIKELY(!IS_TRANSMIT(root_op->get_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("valid check fail. root op type must be ObTransmit", K(ret), K(root_op->get_type()));
  } else if (OB_FAIL(OB_I(t5) ObTaskExecutorCtxUtil::get_task_executor_rpc(query_ctx, rpc))) {
    LOG_WARN("fail get rpc", K(ret));
  } else if (OB_ISNULL(rpc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc is NULL", K(ret));
  } else {
    ObExecutorRpcCtx rpc_ctx(session_snap->get_rpc_tenant_id(),
        plan_ctx_snap->get_timeout_timestamp(),
        exec_ctx_snap->get_task_exec_ctx().get_min_cluster_version(),
        &query_ctx.get_scheduler_thread_ctx().get_scheduler_retry_info_for_update(),
        query_ctx.get_my_session(),
        query_ctx.get_scheduler_thread_ctx().is_plain_select_stmt());
    if (OB_FAIL(rpc->task_kill(
            rpc_ctx, task_info->get_task_location().get_ob_task_id(), task_info->get_task_location().get_server()))) {
      LOG_WARN("fail to kill task", K(ret), K(*task_info), K(rpc_ctx));
    } else {
    }
  }
  return ret;
}

int ObDistributedTaskExecutor::close_result(ObExecContext& ctx, const ObTaskInfo* task_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("task info is NULL", K(ret));
  } else {
    const ObIArray<ObSliceEvent>& slice_events = task_info->get_slice_events();
    if (OB_UNLIKELY(slice_events.count() < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("slice events count < 0", K(ret), K(slice_events.count()));
    } else {
      //      const ObSliceEvent &slice_event = slice_events.at(0);
      //      if (OB_ISNULL(slice_event)) {
      //        ret = OB_ERR_UNEXPECTED;
      //        LOG_ERROR("slice event is NULL", K(ret), K(slice_events.count()));
      //      } else {
      // The result has been pulled back to the local situation,
      // and the remote end has released the result,
      // so there is no need to send the rpc to release the result;
      // if the results are not pulled back to the local situation,
      // the remote end still saves the results, so rpc must be sent to release the remote results
      // const ObTaskSmallResult sr = slice_event->get_small_result();
      // if (!sr.has_data()) {
      if (OB_FAIL(send_close_result_rpc(ctx, task_info))) {
        LOG_WARN("fail to send close result rpc", K(ret), K(*task_info));
      }
      //}
      //      }
    }
  }
  return ret;
}

int ObDistributedTaskExecutor::send_close_result_rpc(ObExecContext& ctx, const ObTaskInfo* task_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("task info is NULL", K(ret));
  } else {
    const ObExecContext* exec_ctx_snap = NULL;
    ObSQLSessionInfo* session_snap = NULL;
    ObPhysicalPlanCtx* plan_ctx_snap = NULL;
    ObExecutorRpcImpl* rpc = NULL;
    const ObTaskLocation& task_loc = task_info->get_task_location();
    ObSliceID ob_slice_id;
    int bak_ret = OB_SUCCESS;
    // When schedule error or early terminate (statement with limit clause), result events
    // are not processed (still in %response_task_events_ queue), there is no slice event in task.
    // In this case, we remove intermediate result with slice_id 0.
    const ObIArray<ObSliceEvent>& slices = task_info->get_slice_events();
    for (int64_t i = 0; i < std::max(slices.count(), 1L); i++) {
      ret = OB_SUCCESS;
      ob_slice_id.set_ob_task_id(task_loc.get_ob_task_id());
      ob_slice_id.set_slice_id(slices.empty() ? 0 : slices.at(i).get_ob_slice_id().get_slice_id());
      if (OB_ISNULL(exec_ctx_snap = ctx.get_scheduler_thread_ctx().get_dis_exec_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("exec ctx snap is NULL", K(ret));
      } else if (OB_ISNULL(session_snap = exec_ctx_snap->get_my_session()) ||
                 OB_ISNULL(plan_ctx_snap = exec_ctx_snap->get_physical_plan_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("session snap or plan ctx snap is NULL", K(ret), K(session_snap), K(plan_ctx_snap));
      } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(ctx, rpc))) {
        LOG_ERROR("fail get rpc", K(ret));
      } else if (OB_ISNULL(rpc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rpc is NULL", K(ret));
      } else {
        ObExecutorRpcCtx rpc_ctx(session_snap->get_rpc_tenant_id(),
            plan_ctx_snap->get_timeout_timestamp(),
            exec_ctx_snap->get_task_exec_ctx().get_min_cluster_version(),
            &ctx.get_scheduler_thread_ctx().get_scheduler_retry_info_for_update(),
            ctx.get_my_session(),
            ctx.get_scheduler_thread_ctx().is_plain_select_stmt());
        if (OB_FAIL(rpc->close_result(rpc_ctx, ob_slice_id, task_loc.get_server()))) {
          LOG_WARN("fail to rpc call close_result", K(ret), K(ob_slice_id), K(task_loc), K(rpc_ctx));
        }
      }
      if (OB_FAIL(ret)) {
        bak_ret = ret;
      }
    }
    ret = bak_ret;
  }
  return ret;
}

int ObDistributedTaskExecutor::build_task(ObExecContext& query_ctx, ObJob& job, ObTaskInfo& task_info, ObTask& task)
{
  int ret = OB_SUCCESS;
  ObPhyOperator* root_op = NULL;
  const ObPhysicalPlan* phy_plan = NULL;
  if (OB_ISNULL(root_op = job.get_root_op())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root op is NULL", K(ret), K(job), K(task_info));
  } else if (OB_ISNULL(phy_plan = root_op->get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan is NULL", K(ret), K(job), K(task_info));
  } else if (OB_FAIL(OB_I(t1) build_task_op_input(query_ctx, task_info, *root_op))) {
    LOG_WARN("fail to build op input", K(ret), K(task_info));
  } else if (OB_ISNULL(query_ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx is null", K(ret));
  } else {
    query_ctx.get_physical_plan_ctx()->set_phy_plan(phy_plan);
    const ObTaskInfo::ObRangeLocation& range_loc = task_info.get_range_location();
    for (int64_t i = 0; OB_SUCC(ret) && i < range_loc.part_locs_.count(); ++i) {
      const ObTaskInfo::ObPartLoc& part_loc = range_loc.part_locs_.at(i);
      if (OB_FAIL(task.add_partition_key(part_loc.partition_key_))) {
        LOG_WARN("fail to add partition key into ObTask", K(ret), K(i), K(part_loc.partition_key_));
      } else if (OB_FAIL(task.assign_ranges(part_loc.scan_ranges_))) {
        LOG_WARN("assign range failed", K(ret));
      } else {
        // Add the partition key of the right table of mv
        // so that it can be passed to the start participant interface at the remote end
        for (int64_t j = 0; OB_SUCC(ret) && j < part_loc.depend_table_keys_.count(); ++j) {
          if (OB_FAIL(task.add_partition_key(part_loc.depend_table_keys_.at(j)))) {
            LOG_WARN("fail to add partition key into ObTask", K(ret), K(j), K(part_loc.depend_table_keys_.at(j)));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("build task",
          "ctrl_svr",
          job.get_ob_job_id().get_server(),
          "ob_task_id",
          task_info.get_task_location().get_ob_task_id(),
          "runner_svr",
          task_info.get_task_location().get_server(),
          "range_loc",
          task_info.get_range_location());

      task.set_ctrl_server(job.get_ob_job_id().get_server());
      task.set_ob_task_id(task_info.get_task_location().get_ob_task_id());
      task.set_location_idx(task_info.get_location_idx());
      task.set_runner_server(task_info.get_task_location().get_server());
      task.set_serialize_param(query_ctx, *root_op, *phy_plan);
    }
  }
  return ret;
}

int ObDistributedTaskExecutor::task_dispatch(
    ObExecContext& exec_ctx, ObExecutorRpcImpl& rpc, ObExecutorRpcCtx& rpc_ctx, ObTask& task, ObTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (!task_info.is_background()) {
    if (OB_ISNULL(trans_result_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("trans result is NULL", K(ret));
    } else if (OB_FAIL(trans_result_->send_task(task_info))) {
      LOG_WARN("send task failed", K(ret), K(task_info));
    } else if (OB_FAIL(rpc.task_submit(
                   rpc_ctx, task, task_info.get_task_location().get_server(), trans_result_->get_trans_result()))) {
      LOG_WARN("task submit failed", K(ret));
    }
  } else {
    const int64_t size = task.get_serialize_size();
    int64_t pos = 0;
    char* buf = NULL;
    if (OB_ISNULL(exec_ctx.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else if (OB_ISNULL(buf = static_cast<char*>(exec_ctx.get_allocator().alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FAIL(task.serialize(buf, size, pos))) {
      LOG_WARN("task serialize failed", K(ret), K(task));
    } else {
      ObString serialized_task(static_cast<int32_t>(size), buf);
      ObSchedBKGDDistTask sched_task;
      if (OB_FAIL(sched_task.init(exec_ctx.get_my_session()->get_effective_tenant_id(),
              rpc_ctx.get_timeout_timestamp(),
              task.get_ob_task_id(),
              scheduler_id_,
              task_info.get_range_location().part_locs_.at(0).partition_key_,
              task_info.get_task_location().get_server(),
              serialized_task))) {
        LOG_WARN("init task failed", K(ret));
      } else if (OB_ISNULL(GCTX.root_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("root service is NULL", K(ret));
      } else if (OB_FAIL(GCTX.root_service_->schedule_sql_bkgd_task(sched_task))) {
        LOG_WARN("schedule background task failed", K(ret), K(sched_task));
      } else {
        LOG_INFO("start schedule background task", K(sched_task));
      }
    }
    if (NULL != buf) {
      exec_ctx.get_allocator().free(buf);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
