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

#include "sql/executor/ob_remote_scheduler.h"
#include "sql/executor/ob_remote_job_control.h"
#include "sql/executor/ob_task_spliter_factory.h"
#include "sql/executor/ob_addrs_provider_factory.h"
#include "sql/executor/ob_remote_job_executor.h"
#include "sql/executor/ob_remote_task_executor.h"
#include "sql/executor/ob_local_job_executor.h"
#include "sql/executor/ob_local_task_executor.h"
#include "sql/executor/ob_job.h"
#include "share/partition_table/ob_partition_location.h"
#include "sql/executor/ob_job_parser.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "share/ob_define.h"
#include "lib/utility/utility.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_query_exec_ctx_mgr.h"

namespace oceanbase {
using namespace oceanbase::common;
namespace sql {
ObRemoteScheduler::ObRemoteScheduler()
{}

ObRemoteScheduler::~ObRemoteScheduler()
{}

int ObRemoteScheduler::schedule(ObExecContext& ctx, ObPhysicalPlan* phy_plan)
{
  int ret = OB_SUCCESS;
  if (ctx.use_remote_sql()) {
    if (OB_ISNULL(ctx.get_sql_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", K(ret), K(ctx.get_sql_ctx()));
    } else {
      if (ctx.get_sql_ctx()->is_execute_async_) {
        if (OB_FAIL(async_execute_with_sql(ctx, phy_plan))) {
          LOG_WARN("async execute with sql failed", K(ret));
        }
      } else if (OB_FAIL(execute_with_sql(ctx, phy_plan))) {
        LOG_WARN("execute with sql failed", K(ret));
      }
    }
  } else if (OB_FAIL(execute_with_plan(ctx, phy_plan))) {
    LOG_WARN("execute with plan failed", K(ret));
  }
  return ret;
}

int ObRemoteScheduler::execute_with_plan(ObExecContext& ctx, ObPhysicalPlan* phy_plan)
{
  // 1. Split and construct task using ObJobConf info
  // 2. Call job.schedule()
  int ret = OB_SUCCESS;
  ObJobParser parser;
  ObLocalTaskExecutor local_task_executor;
  ObLocalJobExecutor local_job_executor;
  ObRemoteTaskExecutor remote_task_executor;
  ObRemoteJobExecutor remote_job_executor;
  ObSEArray<ObJob*, 2> jobs;
  ObJob* root_job = NULL;
  ObJob* remote_job = NULL;
  ObRemoteJobControl jc;
  ObTaskSpliterFactory task_factory;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
  if (OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not init", K(phy_plan), K(ret));
  } else if (OB_FAIL(task_exec_ctx.reset_and_init_stream_handler())) {
    LOG_WARN("reset and init stream handler failed", K(ret));
  } else if (OB_UNLIKELY(NULL == (plan_ctx = ctx.get_physical_plan_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan ctx is NULL", K(ret));
  } else {
    ObExecutionID ob_execution_id;
    ob_execution_id.set_server(task_exec_ctx.get_self_addr());
    ob_execution_id.set_execution_id(OB_INVALID_ID);
    if (OB_FAIL(parser.parse_job(ctx, phy_plan, ob_execution_id, task_factory, jc))) {
      LOG_WARN("fail parse job for scheduler.", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(jc.get_ready_jobs(jobs))) {
      LOG_WARN("fail get jobs.", K(ret));
    } else if (OB_UNLIKELY(2 != jobs.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected job count. expect 2, actual", "job_count", jobs.count());
    } else if (OB_UNLIKELY(NULL == (root_job = jobs.at(0)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null job", K(ret));
    } else if (OB_UNLIKELY(NULL == (remote_job = jobs.at(1)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null job", K(ret));
    } else {
      local_job_executor.set_task_executor(local_task_executor);
      local_job_executor.set_job(*root_job);
      remote_job_executor.set_task_executor(remote_task_executor);
      remote_job_executor.set_job(*remote_job);
      if (OB_FAIL(remote_job_executor.execute(ctx))) {
        LOG_WARN("fail execute remote job", K(ret));
      } else if (OB_FAIL(local_job_executor.execute(ctx))) {
        LOG_WARN("fail execute local job", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    int print_ret = OB_SUCCESS;
    const static int64_t MAX_JC_STATUS_BUF_LEN = 4096;
    char jc_status_buf[MAX_JC_STATUS_BUF_LEN];
    if (OB_SUCCESS != (print_ret = jc.print_status(jc_status_buf, MAX_JC_STATUS_BUF_LEN))) {
      LOG_WARN("fail to print job control status", K(ret), K(print_ret), LITERAL_K(MAX_JC_STATUS_BUF_LEN));
    } else {
      LOG_WARN("fail to schedule, print job's status", K(ret), K(print_ret), "job_status", jc_status_buf);
    }
  }

  return ret;
}

int ObRemoteScheduler::build_remote_task(
    ObExecContext& ctx, ObRemoteTask& remote_task, const DependenyTableStore& dependency_tables)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
  ObSQLSessionInfo* session = nullptr;
  if (OB_FAIL(remote_task.assign_dependency_tables(dependency_tables))) {
    LOG_WARN("fail to assign dependency_tables", K(ret));
  }
  remote_task.set_ctrl_server(ctx.get_addr());
  remote_task.set_session(ctx.get_my_session());
  remote_task.set_query_schema_version(
      task_exec_ctx.get_query_tenant_begin_schema_version(), task_exec_ctx.get_query_sys_begin_schema_version());
  remote_task.set_remote_sql_info(&plan_ctx->get_remote_sql_info());
  const share::ObPartitionReplicaLocation* replica_loc = nullptr;
  ObPhyTableLocationIArray& table_locations = task_exec_ctx.get_table_locations();
  if (OB_UNLIKELY(table_locations.empty()) ||
      OB_ISNULL(replica_loc = table_locations.at(0).get_part_replic_by_index(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(table_locations));
  } else if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    remote_task.set_runner_svr(replica_loc->get_replica_location().server_);
    ObTaskID task_id;
    task_id.set_execution_id(session->get_current_execution_id());
    task_id.set_server(ctx.get_addr());
    task_id.set_task_id(0);
    remote_task.set_task_id(task_id);
    ObITaskExecCtx* cur_task_ctx = nullptr;
    ObQueryExecCtx* query_ctx = nullptr;
    if (OB_NOT_NULL(query_ctx = ctx.get_query_exec_ctx()) &&
        OB_NOT_NULL(cur_task_ctx = query_ctx->get_cur_task_ctx())) {
      ObRemoteTaskCtx* task_ctx = static_cast<ObRemoteTaskCtx*>(cur_task_ctx);
      task_ctx->set_runner_svr(remote_task.get_runner_svr());
    }
  }
  return ret;
}

int ObRemoteScheduler::execute_with_sql(ObExecContext& ctx, ObPhysicalPlan* phy_plan)
{
  int ret = OB_SUCCESS;
  RemoteExecuteStreamHandle* handler = NULL;
  ObSQLSessionInfo* session = ctx.get_my_session();
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
  ObExecuteResult& exec_result = task_exec_ctx.get_execute_result();
  ObExecutorRpcImpl* rpc = NULL;
  ObQueryRetryInfo* retry_info = NULL;
  ObRemoteTask task;
  bool has_sent_task = false;
  bool has_transfer_err = false;
  bool has_merge_err = false;

  if (OB_ISNULL(phy_plan) || OB_ISNULL(session) || OB_ISNULL(plan_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(phy_plan), K(session), K(plan_ctx));
  } else if (OB_FAIL(task_exec_ctx.reset_and_init_stream_handler())) {
    LOG_WARN("reset and init stream handler failed", K(ret));
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_stream_handler(ctx, handler))) {
    LOG_WARN("fail get task response handler", K(ret));
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(ctx, rpc))) {
    LOG_WARN("fail get executor rpc", K(ret));
  } else if (OB_ISNULL(session) || OB_ISNULL(plan_ctx) || OB_ISNULL(handler) || OB_ISNULL(rpc) ||
             OB_ISNULL(retry_info = &session->get_retry_info_for_update())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected null ptr", K(ret), K(session), K(plan_ctx), K(handler), K(rpc), K(retry_info));
  } else if (FALSE_IT(handler->set_use_remote_protocol_v2())) {
  } else if (OB_FAIL(handler->reset_and_init_result())) {
    LOG_WARN("fail to reset and init result", K(ret));
  } else if (OB_FAIL(build_remote_task(ctx, task, phy_plan->get_dependency_table()))) {
    LOG_WARN("build remote task failed", K(ret), K(task));
  } else {
    LOG_DEBUG("execute remote task", K(task));
    if (NULL == phy_plan->get_root_op_spec()) {
      exec_result.set_root_op(phy_plan->get_main_query());
    } else {
      LOG_DEBUG("static engine remote execute");
      ObOperator* op = NULL;
      if (OB_FAIL(phy_plan->get_root_op_spec()->create_operator(ctx, op))) {
        LOG_WARN("create operator from spec failed", K(ret));
      } else if (OB_ISNULL(op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("created operator is NULL", K(ret));
      } else {
        exec_result.set_static_engine_root(op);
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObScanner* scanner = NULL;
    ObExecutorRpcCtx rpc_ctx(session->get_rpc_tenant_id(),
        plan_ctx->get_timeout_timestamp(),
        ctx.get_task_exec_ctx().get_min_cluster_version(),
        retry_info,
        ctx.get_my_session(),
        plan_ctx->is_plain_select_stmt());
    if (OB_FAIL(
            rpc->task_execute_v2(rpc_ctx, task, task.get_runner_svr(), *handler, has_sent_task, has_transfer_err))) {
      bool skip_failed_tasks = false;
      int check_ret = OB_SUCCESS;
      int add_ret = OB_SUCCESS;
      if (is_data_not_readable_err(ret)) {
        if (OB_UNLIKELY(
                OB_SUCCESS != (add_ret = retry_info->add_invalid_server_distinctly(task.get_runner_svr(), true)))) {
          LOG_WARN(
              "fail to add remote addr to invalid servers distinctly", K(ret), K(add_ret), K(task), K(*retry_info));
        }
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
    NG_TRACE_EXT(remote_task_completed, OB_ID(ret), ret, OB_ID(runner_svr), task.get_runner_svr(), OB_ID(task), task);
  }

  if (OB_FAIL(ret)) {
    if (has_sent_task && (has_transfer_err || has_merge_err)) {
      LOG_WARN("need set_incomplete", K(has_transfer_err), K(has_merge_err));
      session->get_trans_result().set_incomplete();
    }
  }

  return ret;
}

int ObRemoteScheduler::async_execute_with_sql(ObExecContext& ctx, ObPhysicalPlan* phy_plan)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = ctx.get_my_session();
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  ObExecutorRpcImpl* rpc = NULL;
  ObQueryRetryInfo* retry_info = NULL;
  ObRemoteTask task;
  bool has_sent_task = false;
  bool has_transfer_err = false;
  bool has_merge_err = false;

  if (OB_FAIL(build_remote_task(ctx, task, phy_plan->get_dependency_table()))) {
    LOG_WARN("build remote task failed", K(ret), K(task));
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(ctx, rpc))) {
    LOG_WARN("fail get executor rpc", K(ret));
  } else {
    //    ObExecutorRpcCtx rpc_ctx(session->get_rpc_tenant_id(),
    //                             plan_ctx->get_timeout_timestamp(),
    //                             ctx.get_task_exec_ctx().get_min_cluster_version(),
    //                             retry_info,
    //                             ctx.get_my_session(),
    //                             plan_ctx->is_plain_select_stmt());
    //    if (OB_FAIL(rpc->remote_task_submit(rpc_ctx, task, task.get_runner_svr(), has_sent_task))) {
    //      LOG_WARN("remote task submit failed", K(ret));
    //    }
    if (OB_FAIL(rpc->remote_task_batch_submit(session->get_rpc_tenant_id(),
            task.get_runner_svr(),
            session->get_local_ob_org_cluster_id(),
            task,
            has_sent_task))) {
      LOG_WARN("remote task batch submit failed", K(ret), K(task));
    }
    if (OB_FAIL(ret)) {
      if (has_sent_task && (has_transfer_err || has_merge_err)) {
        LOG_WARN("need set_incomplete", K(has_transfer_err), K(has_merge_err));
        session->get_trans_result().set_incomplete();
      }
    }
    LOG_DEBUG("async execute with sql", K(ret), K(task));
  }

  return ret;
}
}  // namespace sql
}  // namespace oceanbase
