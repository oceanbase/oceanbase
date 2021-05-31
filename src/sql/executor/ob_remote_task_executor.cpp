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
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_executor_rpc_impl.h"
#include "sql/executor/ob_remote_task_executor.h"
#include "sql/executor/ob_task.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/engine/ob_phy_operator.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObRemoteTaskExecutor::ObRemoteTaskExecutor()
{}

ObRemoteTaskExecutor::~ObRemoteTaskExecutor()
{}

int ObRemoteTaskExecutor::execute(ObExecContext& query_ctx, ObJob* job, ObTaskInfo* task_info)
{
  int ret = OB_SUCCESS;
  RemoteExecuteStreamHandle* handler = NULL;
  ObSQLSessionInfo* session = query_ctx.get_my_session();
  ObPhysicalPlanCtx* plan_ctx = query_ctx.get_physical_plan_ctx();
  ObExecutorRpcImpl* rpc = NULL;
  ObQueryRetryInfo* retry_info = NULL;
  ObTask task;
  bool has_sent_task = false;
  bool has_transfer_err = false;
  bool has_merge_err = false;

  if (OB_ISNULL(task_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task info is NULL", K(ret));
  } else {
    if (OB_FAIL(ObTaskExecutorCtxUtil::get_stream_handler(query_ctx, handler))) {
      LOG_WARN("fail get task response handler", K(ret));
    } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(query_ctx, rpc))) {
      LOG_WARN("fail get executor rpc", K(ret));
    } else if (OB_ISNULL(session) || OB_ISNULL(plan_ctx) || OB_ISNULL(handler) || OB_ISNULL(rpc) ||
               OB_ISNULL(retry_info = &session->get_retry_info_for_update())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected null ptr", K(ret), K(session), K(plan_ctx), K(handler), K(rpc), K(retry_info));
    } else if (OB_FAIL(build_task(query_ctx, *job, *task_info, task))) {
      LOG_WARN("fail build task", K(ret), K(job), K(task_info));
    } else if (OB_FAIL(handler->reset_and_init_result())) {
      LOG_WARN("fail to reset and init result", K(ret));
    } else {
      ObScanner* scanner = NULL;
      task_info->set_state(OB_TASK_STATE_RUNNING);
      ObExecutorRpcCtx rpc_ctx(session->get_rpc_tenant_id(),
          plan_ctx->get_timeout_timestamp(),
          query_ctx.get_task_exec_ctx().get_min_cluster_version(),
          retry_info,
          query_ctx.get_my_session(),
          plan_ctx->is_plain_select_stmt());
      if (OB_FAIL(rpc->task_execute(
              rpc_ctx, task, task_info->get_task_location().get_server(), *handler, has_sent_task, has_transfer_err))) {
        bool skip_failed_tasks = false;
        int check_ret = OB_SUCCESS;
        int add_ret = OB_SUCCESS;
        if (is_data_not_readable_err(ret)) {
          // add server to retry info
          if (OB_UNLIKELY(OB_SUCCESS != (add_ret = retry_info->add_invalid_server_distinctly(
                                             task_info->get_task_location().get_server(), true)))) {
            LOG_WARN("fail to add remote addr to invalid servers distinctly",
                K(ret),
                K(add_ret),
                K(task_info->get_task_location().get_server()),
                K(*retry_info));
          }
        }
        if (OB_SUCCESS != (check_ret = should_skip_failed_tasks(*task_info, skip_failed_tasks))) {
          // check fail, set ret to check_ret
          LOG_WARN("fail to check if it should skip failed tasks", K(ret), K(check_ret), K(*job));
          ret = check_ret;
        } else if (true == skip_failed_tasks) {
          // should skip failed tasks, log user warning and skip it, and set handler's error code to
          // OB_ERR_TASK_SKIPPED, than return OB_SUCCESS
          task_info->set_state(OB_TASK_STATE_SKIPPED);
          LOG_WARN("fail to do task on the remote server, log user warning and skip it",
              K(ret),
              K(task_info->get_task_location().get_server()),
              K(*job));
          LOG_USER_WARN(OB_ERR_TASK_SKIPPED,
              to_cstring(task_info->get_task_location().get_server()),
              common::ob_errpkt_errno(ret, lib::is_oracle_mode()));
          handler->set_result_code(OB_ERR_TASK_SKIPPED);
          ret = OB_SUCCESS;
        } else {
          // let user see ret
          LOG_WARN("fail post task", K(ret));
        }
      }
      int saved_ret = ret;
      if (OB_ISNULL(scanner = handler->get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task result is NULL", K(ret));
      } else if (OB_FAIL(session->get_trans_result().merge_result(scanner->get_trans_result()))) {
        has_merge_err = true;
        LOG_WARN("fail to merge trans result",
            K(ret),
            "session_trans_result",
            session->get_trans_result(),
            "scanner_trans_result",
            scanner->get_trans_result());
      } else {
        LOG_DEBUG("execute trans_result",
            "session_trans_result",
            session->get_trans_result(),
            "scanner_trans_result",
            scanner->get_trans_result());
      }
      if (OB_SUCCESS != saved_ret) {
        ret = saved_ret;
      }
      NG_TRACE_EXT(remote_task_completed,
          OB_ID(ret),
          ret,
          OB_ID(runner_svr),
          task_info->get_task_location().get_server(),
          OB_ID(task),
          task);
    }

    if (OB_FAIL(ret)) {
      task_info->set_state(OB_TASK_STATE_FAILED);
      // set incomplete flag on rpc error
      if (has_sent_task && (has_transfer_err || has_merge_err)) {
        LOG_WARN("need set_incomplete", K(has_transfer_err), K(has_merge_err));
        session->get_trans_result().set_incomplete();
      }
    }
  }

  return ret;
}

int ObRemoteTaskExecutor::build_task(ObExecContext& query_ctx, ObJob& job, ObTaskInfo& task_info, ObTask& task)
{
  int ret = OB_SUCCESS;
  /* serialize:
   *  1. ObPhysicalPlanCtx
   *  2. ObPhyOperator Tree
   *  3. ObPhyOperator Tree Input
   */
  ObPhyOperator* root_op = NULL;
  const ObPhysicalPlan* phy_plan = NULL;
  if (OB_UNLIKELY(NULL == (root_op = job.get_root_op()))) {
    ret = OB_NOT_INIT;
    LOG_WARN("root op not set", K(ret));
  } else if (OB_UNLIKELY(NULL == (phy_plan = root_op->get_phy_plan()))) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical plan is NULL", K(ret));
  } else if (OB_FAIL(build_task_op_input(query_ctx, task_info, *root_op))) {
    LOG_WARN("fail build op inputs", K(ret));
  } else {
    const ObTaskInfo::ObRangeLocation& range_loc = task_info.get_range_location();
    for (int64_t i = 0; OB_SUCC(ret) && i < range_loc.part_locs_.count(); ++i) {
      if (OB_FAIL(task.add_partition_key(range_loc.part_locs_.at(i).partition_key_))) {
        LOG_WARN("fail to add partition key into ObTask", K(ret), K(i), K(range_loc.part_locs_.at(i).partition_key_));
      } else if (OB_FAIL(task.assign_ranges(range_loc.part_locs_.at(i).scan_ranges_))) {
        LOG_WARN("assign range failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      task.set_ctrl_server(job.get_ob_job_id().get_server());
      task.set_runner_server(task_info.get_task_location().get_server());
      task.set_ob_task_id(task_info.get_task_location().get_ob_task_id());
      task.set_serialize_param(query_ctx, *root_op, *phy_plan);
    }
  }
  return ret;
}
