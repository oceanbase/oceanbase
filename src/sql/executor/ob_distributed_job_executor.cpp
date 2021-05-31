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

#include "sql/executor/ob_distributed_job_executor.h"
#include "sql/executor/ob_distributed_scheduler.h"
#include "sql/executor/ob_job.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/queue/ob_lighty_queue.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

ObDistributedJobExecutor::ObDistributedJobExecutor() : job_(NULL), executor_(NULL)
{}

ObDistributedJobExecutor::~ObDistributedJobExecutor()
{}

int ObDistributedJobExecutor::execute_step(ObExecContext& ctx)
{
  NG_TRACE(job_exec_step_begin);
  int ret = OB_SUCCESS;
  ObArray<ObTaskInfo*> ready_tasks;
  ObTaskInfo* task_info = NULL;
  ObDistributedSchedulerManager* sched_mgr = ObDistributedSchedulerManager::get_instance();
  if (OB_ISNULL(sched_mgr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail get ObDistributedSchedulerManager instance", K(ret));
  } else if (OB_ISNULL(job_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("job_ is NULL", K(ret));
  } else if (OB_ISNULL(executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("executor_ is NULL", K(ret));
  } else if (OB_I(t1)
                 OB_UNLIKELY(OB_JOB_STATE_INITED != job_->get_state() && OB_JOB_STATE_RUNNING != job_->get_state())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("job state must be inited or running", "job_state", job_->get_state());
  } else {
    if (OB_JOB_STATE_INITED == job_->get_state()) {
      job_->set_state(OB_JOB_STATE_RUNNING);
    } else {
      // job state is OB_JOB_STATE_RUNNING, do nothing
    }

    ready_tasks.reset();
    if (OB_FAIL(get_executable_tasks(ctx, ready_tasks))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail get executable tasks.", K(ret));
      }
    } else {
      ObTaskCompleteEvent task_event;
      for (int64_t i = 0; OB_SUCC(ret) && i < ready_tasks.count(); ++i) {
        if (OB_ISNULL(task_info = ready_tasks.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("task_info is NULL", K(ret), K(i), K(*job_));
        } else if (OB_FAIL(executor_->execute(ctx, job_, task_info))) {
          if (OB_ERR_TASK_SKIPPED == ret) {
            // The task is skipped, construct a virtual ObTaskCompleteEvent and record it
            // set ret to OB_SUCCESS to continue the loop
            int inner_ret = OB_SUCCESS;
            task_event.reset();
            if (OB_SUCCESS != (inner_ret = task_event.init(task_info->get_task_location(), OB_ERR_TASK_SKIPPED))) {
              LOG_WARN("fail to init task event", K(ret), K(inner_ret), K(*job_));
              ret = inner_ret;
            } else if (OB_SUCCESS != (inner_ret = sched_mgr->signal_scheduler(task_event))) {
              LOG_WARN("fail to signal scheduler", K(ret), K(inner_ret), K(task_event), K(*job_));
              ret = inner_ret;
            } else {
              ret = OB_SUCCESS;
            }
          } else {
            LOG_WARN("fail execute task. ret", K(ret));
          }
        }
      }  // for
    }
  }
  NG_TRACE(job_exec_step_end);
  return ret;
}

int ObDistributedJobExecutor::get_executable_tasks(const ObExecContext& ctx, ObArray<ObTaskInfo*>& ready_tasks)
{
  int ret = OB_SUCCESS;
  ObTaskControl* task_ctrl = NULL;
  if (OB_I(t1) OB_ISNULL(job_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job executor is not init, job is NULL", K(ret));
  } else if (OB_FAIL(OB_I(t2) job_->get_task_control(ctx, task_ctrl))) {
    LOG_WARN("fail get task control.", K(ret));
  } else if (OB_ISNULL(task_ctrl)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to get task ctrl, but task_ctrl is NULL", K(ret), K(*job_));
  } else if (OB_FAIL(OB_I(t3) task_ctrl->get_ready_tasks(ready_tasks))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail get ready task.", K(ret));
    }
  } else {
    // empty
  }
  return ret;
}

int ObDistributedJobExecutor::kill_job(ObExecContext& query_ctx)
{
  int ret = OB_SUCCESS;
  int kill_ret = OB_SUCCESS;
  ObArray<ObTaskInfo*> running_tasks;
  ObTaskControl* task_ctrl = NULL;

  if (OB_ISNULL(job_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job executor is not init, job is NULL", K(ret));
  } else if (OB_ISNULL(executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("executor_ is NULL", K(ret));
  } else if (OB_FAIL(OB_I(t1) job_->get_task_control(query_ctx, task_ctrl))) {
    LOG_WARN("fail get task control.", K(ret));
  } else if (OB_ISNULL(task_ctrl)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to get task ctrl, but task_ctrl is NULL", K(ret), K(*job_));
  } else {
    if (OB_FAIL(task_ctrl->get_running_tasks(running_tasks))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get running task.", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < running_tasks.count(); ++i) {
      if (OB_SUCCESS != (kill_ret = executor_->kill(query_ctx, job_, running_tasks.at(i)))) {
        LOG_WARN("fail to kill task", K(kill_ret));  // ignore error
      }
    }
  }
  return ret;
}

int ObDistributedJobExecutor::close_all_results(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObTaskInfo*> tasks;
  ObTaskControl* task_ctrl = NULL;
  if (OB_ISNULL(job_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job executor is not init, job is NULL", K(ret));
  } else if (OB_ISNULL(executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("executor_ is NULL", K(ret));
  } else if (OB_FAIL(job_->get_task_control(ctx, task_ctrl))) {
    LOG_WARN("fail get task control.", K(ret));
  } else if (OB_ISNULL(task_ctrl)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to get task ctrl, but task_ctrl is NULL", K(ret), K(*job_));
  } else if (OB_FAIL(task_ctrl->get_begin_running_tasks(tasks))) {
    LOG_WARN("fail get begin running tasks.", K(ret));
  } else {
    int close_ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      const ObTaskInfo* task = tasks.at(i);
      if (OB_SUCCESS != (close_ret = executor_->close_result(ctx, task))) {
        // ignore error
        if (OB_ISNULL(task)) {
          LOG_WARN("fail to close result, and task is NULL", K(close_ret), K(i));
        } else {
          LOG_WARN("fail to close result", K(close_ret), K(i), K(*task));
        }
      } else {
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
