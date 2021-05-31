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

#include "sql/executor/ob_transmit.h"
#include "sql/executor/ob_addrs_provider.h"
#include "sql/executor/ob_local_scheduler.h"
#include "sql/executor/ob_local_job_executor.h"
#include "sql/executor/ob_local_task_executor.h"
#include "sql/executor/ob_job.h"
#include "sql/executor/ob_job_parser.h"
#include "share/ob_define.h"
#include "lib/utility/utility.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/profile/ob_perf_event.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObLocalScheduler::ObLocalScheduler()
{}

ObLocalScheduler::~ObLocalScheduler()
{}

int ObLocalScheduler::schedule(ObExecContext& ctx, ObPhysicalPlan* phy_plan)
{
  // 1. Split and construct task using ObJobConf info
  // 2. Call job.schedule()
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObPhyOperator* root_op = NULL;

  LOG_DEBUG("local scheduler start", K(ctx), K(*phy_plan));
  if (OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_plan not init", K(phy_plan), K(ret));
  } else if (OB_UNLIKELY(NULL == (root_op = phy_plan->get_main_query()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root_op not init", K(phy_plan), K(ret));
  } else if (OB_UNLIKELY(NULL == (executor_ctx = GET_TASK_EXECUTOR_CTX(ctx)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("executor ctx is NULL", K(ret));
  } else if (OB_UNLIKELY(NULL == (plan_ctx = ctx.get_physical_plan_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan ctx is NULL", K(ret));
  } else if (OB_UNLIKELY(false == IS_TRANSMIT(root_op->get_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root op is not transmit op", K(ret), K(*root_op));
  } else {
    LOG_DEBUG("execute params", K(plan_ctx->get_param_store()));

    ObTransmit* transmit_op = static_cast<ObTransmit*>(root_op);
    int task_split_type = transmit_op->get_job_conf().get_task_split_type();
    ObExecutionID ob_execution_id(executor_ctx->get_self_addr(), OB_INVALID_ID);
    /**
     * Since the data structure involved in splitting the job is relatively heavy,
     * and the process of splitting is time-consuming,
     * if only one local task is generated (LOCAL_IDENTITY_SPLIT means that only one local task is generated),
     * skip the step of splitting the job
     */
    if (ObTaskSpliter::LOCAL_IDENTITY_SPLIT == task_split_type) {
      if (OB_FAIL(direct_generate_task_and_execute(ctx, ob_execution_id, root_op))) {
        LOG_WARN("fail to directly generate task and execute", K(ret), K(ob_execution_id), K(*root_op));
      }
    } else {
      ObJobParser parser;
      ObLocalTaskExecutor task_executor;
      ObLocalJobExecutor job_executor;
      ObSEArray<ObJob*, 1> jobs;
      ObJob* local_job = NULL;
      ObLocalJobControl jc;
      ObTaskSpliterFactory spfactory;
      if (OB_FAIL(parser.parse_job(ctx, phy_plan, ob_execution_id, spfactory, jc))) {
        LOG_WARN("fail to parse job for scheduler", K(ret));
      } else if (OB_FAIL(jc.get_ready_jobs(jobs))) {
        LOG_WARN("fail get jobs", "jb_cnt", jc.get_job_count(), K(ret));
      } else if (OB_UNLIKELY(1 != jobs.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected job count");
      } else if (OB_UNLIKELY(NULL == (local_job = jobs.at(0)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null job");
      } else {
        job_executor.set_task_executor(task_executor);
        job_executor.set_job(*local_job);
        /**
         * The execute method finally constructs a Result,
         * which contains an executable Op Tree,
         * and finally starts to execute and fetch data,
         *  which is driven in ObResultSet.
         */
        if (OB_FAIL(job_executor.execute(ctx))) {
          LOG_WARN("fail execute local job");
        }
      }
    }
  }
  return ret;
}

int ObLocalScheduler::direct_generate_task_and_execute(
    ObExecContext& ctx, const ObExecutionID& ob_execution_id, ObPhyOperator* root_op)
{
  int ret = OB_SUCCESS;
  ObTaskInfo* task = NULL;
  void* ptr = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  if (OB_ISNULL(root_op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("root_op not init", K(ret));
  } else if (OB_UNLIKELY(NULL == (executor_ctx = GET_TASK_EXECUTOR_CTX(ctx)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("executor ctx is NULL", K(ret));
  } else if (OB_UNLIKELY(NULL == (ptr = ctx.get_allocator().alloc(sizeof(ObTaskInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_UNLIKELY(NULL == (task = new (ptr) ObTaskInfo(ctx.get_allocator())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to new ObTaskInfo", K(ret));
  } else {
    ObLocalTaskExecutor task_executor;
    ObJobID ob_job_id(ob_execution_id, 0);
    ObTaskID ob_task_id(ob_job_id, 0);
    ObTaskLocation task_loc(executor_ctx->get_self_addr(), ob_task_id);
    task->set_task_split_type(ObTaskSpliter::LOCAL_IDENTITY_SPLIT);
    task->set_location_idx(0);
    task->set_pull_slice_id(0);
    task->set_task_location(task_loc);
    task->set_root_op(root_op);
    task->set_state(OB_TASK_STATE_NOT_INIT);
    if (OB_UNLIKELY(false == IS_TRANSMIT(root_op->get_type()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("root op is not transmit op", K(ret), K(*root_op));
    } else if (OB_FAIL(ObJobControl::build_phy_op_input(ctx, root_op))) {
      LOG_WARN("fail to build physical operator input", K(ret));
    } else if (OB_FAIL(task_executor.execute(ctx, NULL, task))) {
      LOG_WARN("fail execute task.", K(ret));
    }
    task->~ObTaskInfo();
  }
  return ret;
}
