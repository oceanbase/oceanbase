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
#include "storage/tx/ob_trans_service.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObRemoteTaskExecutor::ObRemoteTaskExecutor()
{
}

ObRemoteTaskExecutor::~ObRemoteTaskExecutor()
{
}

int ObRemoteTaskExecutor::execute(ObExecContext &query_ctx, ObJob *job, ObTaskInfo *task_info)
{
  int ret = OB_SUCCESS;
  RemoteExecuteStreamHandle *handler = NULL;
  ObSQLSessionInfo *session = query_ctx.get_my_session();
  ObPhysicalPlanCtx *plan_ctx = query_ctx.get_physical_plan_ctx();
  ObExecutorRpcImpl *rpc = NULL;
  ObQueryRetryInfo *retry_info = NULL;
  ObTask task;
  bool has_sent_task = false;
  bool has_transfer_err = false;

  if (OB_ISNULL(task_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task info is NULL", K(ret));
  } else {
    if (OB_FAIL(ObTaskExecutorCtxUtil::get_stream_handler(query_ctx, handler))) {
      LOG_WARN("fail get task response handler", K(ret));
    } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(query_ctx, rpc))) {
      LOG_WARN("fail get executor rpc", K(ret));
    } else if (OB_ISNULL(session) || OB_ISNULL(plan_ctx) || OB_ISNULL(handler) || OB_ISNULL(rpc)
        || OB_ISNULL(retry_info = &session->get_retry_info_for_update())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected null ptr", K(ret), K(session), K(plan_ctx), K(handler), K(rpc), K(retry_info));
    } else if (OB_FAIL(build_task(query_ctx, *job, *task_info, task))) {
      LOG_WARN("fail build task", K(ret), K(job), K(task_info));
    } else if (OB_FAIL(handler->reset_and_init_result())) {
      LOG_WARN("fail to reset and init result", K(ret));
    } else {
      // 将task_info设成OB_TASK_STATE_RUNNING状态，后面如果重试可能会用到该状态
      task_info->set_state(OB_TASK_STATE_RUNNING);
      const int32_t group_id = OB_INVALID_ID == session->get_expect_group_id() ? 0 : session->get_expect_group_id();
      ObExecutorRpcCtx rpc_ctx(session->get_rpc_tenant_id(),
                               plan_ctx->get_timeout_timestamp(),
                               query_ctx.get_task_exec_ctx().get_min_cluster_version(),
                               retry_info,
                               query_ctx.get_my_session(),
                               plan_ctx->is_plain_select_stmt(),
                               group_id);
      if (OB_FAIL(rpc->task_execute(rpc_ctx,
                                    task,
                                    task_info->get_task_location().get_server(),
                                    *handler,
                                    has_sent_task,
                                    has_transfer_err))) {
        bool skip_failed_tasks = false;
        int check_ret = OB_SUCCESS;
        if (OB_SUCCESS != (check_ret = should_skip_failed_tasks(*task_info, skip_failed_tasks))) {
          // check fail, set ret to check_ret
          LOG_WARN("fail to check if it should skip failed tasks", K(ret), K(check_ret), K(*job));
          ret = check_ret;
        } else if (true == skip_failed_tasks) {
          // should skip failed tasks, log user warning and skip it, and set handler's error code to
          // OB_ERR_TASK_SKIPPED, than return OB_SUCCESS
          // 将task_info设成OB_TASK_STATE_SKIPPED状态，后面如果重试可能会用到该状态
          task_info->set_state(OB_TASK_STATE_SKIPPED);
          LOG_WARN("fail to do task on the remote server, log user warning and skip it",
                   K(ret), K(task_info->get_task_location().get_server()), K(*job));
          LOG_USER_WARN(OB_ERR_TASK_SKIPPED,
                        to_cstring(task_info->get_task_location().get_server()), common::ob_errpkt_errno(ret, lib::is_oracle_mode()));
          handler->set_result_code(OB_ERR_TASK_SKIPPED);
          ret = OB_SUCCESS;
        } else {
          // let user see ret
          LOG_WARN("fail post task", K(ret));
        }
      }
      // handle tx relative info if plan involved in transaction
      const ObPhysicalPlan *plan = plan_ctx->get_phy_plan();
      if (plan && plan->is_need_trans()) {
        int tmp_ret = handle_tx_after_rpc(handler->get_result(),
                                        session,
                                        has_sent_task,
                                        has_transfer_err,
                                        plan,
                                          query_ctx);
        ret = COVER_SUCC(tmp_ret);
      }

      if (OB_SUCC(ret)) {
        ObExecFeedbackInfo &fb_info = handler->get_result()->get_feedback_info();
        if (OB_FAIL(query_ctx.get_feedback_info().merge_feedback_info(fb_info))) {
          LOG_WARN("fail to merge exec feedback info", K(ret));
        }
      }
      NG_TRACE_EXT(remote_task_completed, OB_ID(ret), ret,
                   OB_ID(runner_svr), task_info->get_task_location().get_server(),
                   OB_ID(task), task);
      // 说明：本函数返回后，最终控制权会进入到ObDirectReceive中,
      // 它通过get_stream_handler()来获得当前handler
      // 然后阻塞等待在handler上收取返回结果

      //
      // nothing more to do
      //
    }

    if (OB_FAIL(ret)) {
      // 如果失败了，则将task_info设成OB_TASK_STATE_FAILED状态，
      // 这样后面如果需要重试则可能会根据这个状态将该task_info中的partition信息
      // 加入到需要重试的partition里面
      task_info->set_state(OB_TASK_STATE_FAILED);
    }
  }

  return ret;
}

int ObRemoteTaskExecutor::build_task(ObExecContext &query_ctx,
                                     ObJob &job,
                                     ObTaskInfo &task_info,
                                     ObTask &task)
{
  int ret = OB_SUCCESS;
  /* 需要序列化的内容包括：
   *  1. ObPhysicalPlanCtx
   *  2. ObPhyOperator Tree
   *  3. ObPhyOperator Tree Input
   */
  ObOpSpec *root_spec = NULL;
  const ObPhysicalPlan *phy_plan = NULL;

  if (OB_ISNULL(root_spec = job.get_root_spec())) {
    ret = OB_NOT_INIT;
    LOG_WARN("root spec not init", K(ret));
  } else if (OB_UNLIKELY(NULL == (phy_plan = root_spec->get_phy_plan()))) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical plan is NULL", K(ret));
  } else if (OB_FAIL(build_task_op_input(query_ctx, task_info, *root_spec))) {
    LOG_WARN("fail build op inputs", K(ret));
  } else {
    const ObTaskInfo::ObRangeLocation &range_loc = task_info.get_range_location();
    for (int64_t i = 0; OB_SUCC(ret) && i < range_loc.part_locs_.count(); ++i) {
      if (OB_FAIL(task.assign_ranges(range_loc.part_locs_.at(i).scan_ranges_))) {
        LOG_WARN("assign range failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      task.set_ctrl_server(job.get_ob_job_id().get_server());
      task.set_runner_server(task_info.get_task_location().get_server());
      task.set_ob_task_id(task_info.get_task_location().get_ob_task_id());
      task.set_serialize_param(&query_ctx, root_spec, phy_plan);
    }
  }
  return ret;
}

int ObRemoteTaskExecutor::handle_tx_after_rpc(ObScanner *scanner,
                                              ObSQLSessionInfo *session,
                                              const bool has_sent_task,
                                              const bool has_transfer_err,
                                              const ObPhysicalPlan *phy_plan,
                                              ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  auto tx_desc = session->get_tx_desc();
  bool remote_trans =
    session->get_local_autocommit() && !session->has_explicit_start_trans();
  if (remote_trans) {
  } else if (OB_ISNULL(tx_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dml need acquire transaction", K(ret), KPC(session));
  } else if (phy_plan->is_stmt_modify_trans() && has_sent_task) {
    if (has_transfer_err) {
    } else if (OB_ISNULL(scanner)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("task result is NULL", K(ret));
    } else if (OB_FAIL(MTL(transaction::ObTransService*)
                       ->add_tx_exec_result(*tx_desc,
                                            scanner->get_trans_result()))) {
      LOG_WARN("fail to report tx result", K(ret),
                 "scanner_trans_result", scanner->get_trans_result(),
               K(tx_desc));
    } else {
      LOG_TRACE("report tx result",
                "scanner_trans_result", scanner->get_trans_result(),
                K(tx_desc));
    }
    if (has_transfer_err || OB_FAIL(ret)) {
      if (exec_ctx.use_remote_sql()) {
        LOG_WARN("remote execute use sql fail with transfer_error, tx will rollback", K(ret));
        session->get_trans_result().set_incomplete();
      } else {
        ObDASCtx &das_ctx = DAS_CTX(exec_ctx);
        share::ObLSArray ls_ids;
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(das_ctx.get_all_lsid(ls_ids))) {
          LOG_WARN("get all ls_ids failed", K(tmp_ret));
        } else if (OB_TMP_FAIL(session->get_trans_result().add_touched_ls(ls_ids))) {
          LOG_WARN("add touched ls to txn failed", K(tmp_ret));
        } else {
         LOG_INFO("add touched ls succ", K(ls_ids));
        }
        if (OB_TMP_FAIL(tmp_ret)) {
          LOG_WARN("remote execute use plan fail with transfer_error and try add touched ls failed, tx will rollback", K(tmp_ret));
          session->get_trans_result().set_incomplete();
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
  }
  return ret;
}
