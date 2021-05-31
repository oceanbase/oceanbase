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
#include "share/ob_thread_mgr.h"
#include "lib/ob_running_mode.h"
#include "sql/monitor/ob_exec_stat_collector.h"
#include "sql/executor/ob_distributed_scheduler.h"
#include "sql/executor/ob_distributed_job_executor.h"
#include "sql/executor/ob_distributed_task_executor.h"
#include "sql/executor/ob_local_job_executor.h"
#include "sql/executor/ob_local_task_executor.h"
#include "sql/executor/ob_executor_rpc_processor.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/monitor/ob_exec_stat_collector.h"
#include "sql/executor/ob_mini_task_executor.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::obrpc;
namespace oceanbase {
namespace sql {

ObDistributedSchedulerManager* ObDistributedSchedulerManager::instance_ = NULL;

ObDistributedScheduler::ObDistributedScheduler()
    : ObSqlScheduler(),
      allocator_(ObModIds::OB_SQL_EXECUTOR_SCHEDULER),
      execution_id_(OB_INVALID_ID),
      finish_queue_(),
      response_task_events_(),
      lock_(),
      spfactory_(),
      job_control_(),
      parser_(),
      exec_stat_collector_(NULL),
      trans_result_(),
      should_stop_(false),
      root_finish_(false),
      sche_finish_(false),
      sche_ret_(OB_SUCCESS),
      can_serial_exec_(false),
      sche_thread_started_(false),
      scheduler_id_(0),
      rpc_error_addrs_()
{}

ObDistributedScheduler::~ObDistributedScheduler()
{
  for (int64_t i = 0; i < response_task_events_.count(); ++i) {
    ObTaskCompleteEvent*& evt = response_task_events_.at(i);
    if (NULL != evt) {
      evt->~ObTaskCompleteEvent();
      evt = NULL;
    }
  }
}

int ObDistributedScheduler::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(finish_queue_.init(MAX_FINISH_QUEUE_CAPACITY))) {
    LOG_WARN("fail to init finish queue for all", K(ret), LITERAL_K(MAX_FINISH_QUEUE_CAPACITY));
  }
  return ret;
}

int ObDistributedScheduler::stop()
{
  should_stop_ = true;
  return OB_SUCCESS;
}

void ObDistributedScheduler::reset()
{
  execution_id_ = OB_INVALID_ID;
  for (int64_t i = 0; i < response_task_events_.count(); ++i) {
    ObTaskCompleteEvent*& evt = response_task_events_.at(i);
    if (NULL != evt) {
      evt->~ObTaskCompleteEvent();
      evt = NULL;
    }
  }
  response_task_events_.reset();
  // lock_.reset();
  //==============dont change reset order=============
  parser_.reset();
  job_control_.reset();
  spfactory_.reset();
  //====================================================
  exec_stat_collector_ = NULL;
  trans_result_.reset();
  should_stop_ = false;
  finish_queue_.reset();

  root_finish_ = false;
  sche_finish_ = false;
  sche_ret_ = OB_SUCCESS;
  can_serial_exec_ = false;
  sche_thread_started_ = false;
  allocator_.reset();
  scheduler_id_ = 0;
  rpc_error_addrs_.reset();
}

int ObDistributedScheduler::merge_trans_result(const ObTaskCompleteEvent& task_event)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(
          trans_result_.recv_result(task_event.get_task_location().get_ob_task_id(), task_event.get_trans_result()))) {
    LOG_WARN("fail to merge trans result",
        K(ret),
        "task_id",
        task_event.get_task_location().get_ob_task_id(),
        "task_trans_result",
        task_event.get_trans_result());
  } else {
    LOG_DEBUG("scheduler trans_result",
        "task_id",
        task_event.get_task_location().get_ob_task_id(),
        "task_trans_result",
        task_event.get_trans_result());
  }
  return ret;
}

int ObDistributedScheduler::set_task_status(const ObTaskID& task_id, ObTaskStatus status)
{
  return trans_result_.set_task_status(task_id, status);
}

int ObDistributedScheduler::signal_finish_queue(const ObTaskCompleteEvent& task_event)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  void* ptr_casted_from_idx = NULL;
  if (OB_UNLIKELY(!task_event.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task event is invalid", K(ret), K(task_event));
  } else {
    void* evt_ptr = NULL;
    ObTaskCompleteEvent* evt = NULL;
    {
      ObLockGuard<ObSpinLock> lock_guard(lock_);
      if (OB_ISNULL(evt_ptr = allocator_.alloc(sizeof(ObTaskCompleteEvent)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ObTaskCompleteEvent", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(evt = new (evt_ptr) ObTaskCompleteEvent())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to new ObTaskCompleteEvent", K(ret));
    } else {
      ObLockGuard<ObSpinLock> lock_guard(lock_);
      if (OB_FAIL(evt->assign(allocator_, task_event))) {
        LOG_WARN("fail to assign task event", K(ret));
        evt->~ObTaskCompleteEvent();
        evt = NULL;
      }
    }
    if (OB_SUCC(ret)) {
      ObLockGuard<ObSpinLock> lock_guard(lock_);
      if (OB_FAIL(response_task_events_.push_back(evt))) {
        LOG_WARN("fail to push back task event into response_task_events_ array", K(ret), K(evt), K(task_event));
        evt->~ObTaskCompleteEvent();
        evt = NULL;
      } else {
        idx = response_task_events_.count() - 1;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(idx < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid idx, it is < 0", K(ret), K(idx));
  } else if (OB_ISNULL(ptr_casted_from_idx = idx_to_ptr(idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ptr casted from idx is NULL", K(ret), K(idx));
  } else {
    int64_t finish_queue_size = finish_queue_.size();
    if (OB_UNLIKELY(finish_queue_size > 128)) {
      LOG_ERROR("finish queue size > 128", K(finish_queue_size));
    }
    if (OB_FAIL(finish_queue_.push(ptr_casted_from_idx))) {
      LOG_WARN("fail to push task event into finish queue", K(ret), K(ptr_casted_from_idx), K(idx), K(task_event));
    }
  }
  return ret;
}

int ObDistributedScheduler::atomic_push_err_rpc_addr(const common::ObAddr& addr)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_FAIL(rpc_error_addrs_.push_back(addr))) {
    LOG_WARN("fail to push addr", K(ret));
  }
  return ret;
}

int ObDistributedScheduler::pop_task_result_for_root(
    ObExecContext& ctx, uint64_t root_op_id, ObTaskResult& task_result, int64_t timeout_timestamp)
{
  int ret = OB_SUCCESS;
  ObJob* job = NULL;
  int64_t task_id = -1;
  if (OB_FAIL(job_control_.find_job_by_root_op_id(root_op_id, job))) {
    LOG_WARN("fail to find job", K(ret), K(root_op_id));
  } else if (OB_ISNULL(job)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job is null", K(ret));
  } else if (OB_FAIL(pop_task_idx(ctx,
                 job->get_finish_queue(),
                 timeout_timestamp,
                 &ObDistributedScheduler::check_schedule_error,
                 task_id))) {
    ObQueryRetryInfo& retry_info = ctx.get_scheduler_thread_ctx().get_scheduler_retry_info_for_update();
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS !=
        (tmp_ret = ObMiniTaskExecutor::add_invalid_servers_to_retry_info(ret, rpc_error_addrs_, retry_info))) {
      LOG_WARN("fail to add invalid servers to retry info", K(tmp_ret));
    }
    LOG_WARN("fail to pop task event idx", K(ret));
  } else if (FALSE_IT(sche_finish_)) {
  } else if (OB_FAIL(check_schedule_error())) {
    ObQueryRetryInfo& retry_info = ctx.get_scheduler_thread_ctx().get_scheduler_retry_info_for_update();
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS !=
        (tmp_ret = ObMiniTaskExecutor::add_invalid_servers_to_retry_info(ret, rpc_error_addrs_, retry_info))) {
      LOG_WARN("fail to add invalid servers to retry info", K(tmp_ret));
    }
    LOG_WARN("schecule error", K(ret));
  } else if (SCHE_ITER_END == task_id) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(job->get_task_result(task_id, task_result))) {
    LOG_WARN("fail to get task result", K(ret), K(task_id));
  }
  return ret;
}

int ObDistributedScheduler::pop_task_event_for_sche(
    const ObExecContext& ctx, ObTaskCompleteEvent*& task_event, int64_t timeout_timestamp)
{
  int ret = OB_SUCCESS;
  int64_t task_event_idx = -1;
  if (OB_FAIL(pop_task_idx(
          ctx, finish_queue_, timeout_timestamp, &ObDistributedScheduler::check_root_finish, task_event_idx))) {
    LOG_WARN("fail to pop task event idx", K(ret));
  } else if (OB_FAIL(check_root_finish())) {
    LOG_WARN("root finish", K(ret));
  } else if (FALSE_IT(task_event_idx < 0)) {
  } else if (OB_FAIL(get_task_event(task_event_idx, task_event))) {
    LOG_WARN("fail to get task event", K(ret));
  }
  return ret;
}

int ObDistributedScheduler::pop_task_idx(const ObExecContext& ctx, ObLightyQueue& finish_queue,
    int64_t timeout_timestamp, ObCheckStatus check_func, int64_t& task_idx)
{
  int ret = OB_SUCCESS;
  void* ptr_casted_from_idx = NULL;
  static const int64_t MAX_TIMEOUT_ONCE = 100000;  // 100ms
  const ObSQLSessionInfo* session = ctx.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session is NULL", K(ret));
  } else {
    bool is_pop_timeout = true;
    int64_t wait_count = 0;  // for debug purpose
    while (is_pop_timeout) {
      int64_t timeout_left = timeout_timestamp - ObTimeUtility::current_time();
      is_pop_timeout = false;
      if (OB_UNLIKELY(should_stop_)) {
        ret = OB_ERR_INTERRUPTED;
        LOG_WARN("distributed scheduler is interrupted, server is stopping",
            K(ret),
            K(wait_count),
            K(timeout_left),
            K(timeout_timestamp));
      } else if (OB_UNLIKELY(session->is_terminate(ret))) {
        LOG_WARN("query or session is killed", K(ret), K(wait_count), K(timeout_left), K(timeout_timestamp));
      } else if (!OB_ISNULL(check_func) && OB_FAIL((this->*check_func)())) {
        LOG_WARN("check some status to exit", K(wait_count), K(ret));
      } else if (timeout_left <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("fail pop task event from finish queue, timeout",
            K(ret),
            K(wait_count),
            K(timeout_left),
            K(timeout_timestamp));
      } else {
        int64_t timeout = MIN(timeout_left, MAX_TIMEOUT_ONCE);
        ret = finish_queue.pop(ptr_casted_from_idx, timeout);
        if (OB_ENTRY_NOT_EXIST == ret) {
          is_pop_timeout = true;
          ret = OB_SUCCESS;
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("fail pop task event from finish queue", K(ret), K(timeout_timestamp));
        }
      }
      wait_count++;
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    task_idx = ptr_to_idx(ptr_casted_from_idx);
  }
  return ret;
}

int ObDistributedScheduler::get_task_event(int64_t task_event_idx, ObTaskCompleteEvent*& task_event)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_event_idx < 0 || task_event_idx >= response_task_events_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid task event idx", K(ret), K(task_event_idx));
  } else if (OB_ISNULL(task_event = response_task_events_.at(task_event_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("task event is NULL", K(ret), K(task_event_idx));
  } else if (OB_UNLIKELY(!task_event->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task event is invalid", K(ret), K(*task_event));
  } else if (OB_UNLIKELY(!task_event->get_task_location().is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task location is invalid", K(ret), K(*task_event));
  }
  return ret;
}

int ObDistributedScheduler::signal_root_finish()
{
  root_finish_ = true;
  return finish_queue_.push(idx_to_ptr(NOP_EVENT));
}

int ObDistributedScheduler::signal_job_iter_end(ObLightyQueue& finish_queue)
{
  return finish_queue.push(idx_to_ptr(SCHE_ITER_END));
}

int ObDistributedScheduler::signal_schedule_error(int sche_ret)
{
  int ret = OB_SUCCESS;
  sche_ret_ = sche_ret;
  ObArray<ObJob*> all_jobs;
  if (OB_FAIL(job_control_.get_all_jobs(all_jobs))) {
    LOG_WARN("fail to get all jobs", K(ret));
  } else {
    ObJob* job = NULL;
    int signal_ret = OB_SUCCESS;
    // root may wait on any finish queue in all jobs, so signal all jobs.
    for (int64_t i = 0; OB_SUCC(ret) && i < all_jobs.count(); i++) {
      // continue even if this job is NULL, or something wrong.
      if (OB_ISNULL(job = all_jobs.at(i))) {
        LOG_WARN("job is NULL", K(i));
      } else if (OB_SUCCESS != (signal_ret = job->signal_schedule_error(NOP_EVENT))) {
        LOG_WARN("fail to signal schedule error", K(signal_ret), K(i), K(*job));
      }
    }
  }
  return ret;
}

int ObDistributedScheduler::wait_root_use_up_data(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_TIMEOUT_STEP = 1000000;  // 1s
  ObPhysicalPlanCtx* plan_ctx = NULL;
  void* ptr_casted_from_idx = NULL;
  int64_t wait_count = 0;

  if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("plan ctx is NULL", K(ret));
  }
  while (OB_SUCC(ret) && !root_finish_) {
    ret = OB_ENTRY_NOT_EXIST;
    int64_t timeout_timestamp = plan_ctx->get_timeout_timestamp();
    while (OB_ENTRY_NOT_EXIST == ret) {
      int64_t timeout_left = timeout_timestamp - ObTimeUtility::current_time();
      if (root_finish_) {
        ret = OB_SUCCESS;
      } else if (timeout_left <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("fail pop task event from finish queue, timeout",
            K(ret),
            K(timeout_left),
            K(timeout_timestamp),
            K(root_finish_),
            K(wait_count));
      } else {
        int64_t timeout = MIN(timeout_left, MAX_TIMEOUT_STEP);
        ret = finish_queue_.pop(ptr_casted_from_idx, timeout);
      }
    }
    if (!root_finish_) {
      if (OB_FAIL(ret)) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_TIMEOUT;
        }
        LOG_WARN("fail pop task event from finish queue", K(ret), K(timeout_timestamp), K(wait_count));
      } else {
        int64_t task_idx = ptr_to_idx(ptr_casted_from_idx);
        if (OB_UNLIKELY(NOP_EVENT == task_idx)) {
          LOG_ERROR("root not finish, but can pop NOP_EVENT", K(ret), K(task_idx), K(root_finish_), K(wait_count));
        }
      }
    }
    wait_count++;
  }
  return ret;
}

int ObDistributedScheduler::signal_schedule_finish()
{
  signal_can_serial_exec();
  sche_finish_ = true;
  sche_finish_cond_.signal();
  return OB_SUCCESS;
}

int ObDistributedScheduler::wait_schedule_finish(/*int64_t timeout_timestamp*/)
{
  int ret = OB_SUCCESS;
  if (sche_thread_started_) {
    int64_t wait_count = 0;
    while (OB_SUCC(ret) && !sche_finish_) {
      sche_finish_cond_.timedwait(1000000);
      if (!sche_finish_) {
        wait_count++;
        LOG_INFO("wait for schedule finish", K(wait_count), K_(sche_finish));
      }
    }
  }
  return ret;
}

int ObDistributedScheduler::signal_can_serial_exec()
{
  can_serial_exec_ = true;
  can_serial_exec_cond_.signal();
  return OB_SUCCESS;
}

int ObDistributedScheduler::wait_can_serial_exec(ObExecContext& ctx, int64_t timeout_timestamp)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_TIMEOUT_ONCE = 100000;  // 100ms
  const ObSQLSessionInfo* session = ctx.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    int64_t wait_count = 0;  // for debug purpose
    while (OB_SUCC(ret) && !can_serial_exec_) {
      int64_t timeout_left = timeout_timestamp - ObTimeUtility::current_time();
      if (OB_UNLIKELY(should_stop_)) {
        ret = OB_ERR_INTERRUPTED;
        LOG_WARN("distributed scheduler is interrupted, server is stopping",
            K(ret),
            K(wait_count),
            K(timeout_left),
            K(timeout_timestamp));
      } else if (OB_UNLIKELY(session->is_terminate(ret))) {
        ret = OB_ERR_INTERRUPTED;
        LOG_WARN("query or session is killed", K(ret), K(wait_count), K(timeout_left), K(timeout_timestamp));
      } else if (OB_FAIL(check_schedule_error())) {
        LOG_WARN("check some status to exit", K(wait_count), K(ret));
      } else if (timeout_left <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("fail pop task event from finish queue, timeout",
            K(ret),
            K(wait_count),
            K(timeout_left),
            K(timeout_timestamp));
      } else {
        int64_t timeout = MIN(timeout_left, MAX_TIMEOUT_ONCE);
        can_serial_exec_cond_.timedwait(timeout);
      }
      wait_count++;
    }
    if (OB_SUCC(ret) && can_serial_exec_) {
      ctx.merge_final_trans_result();
    }
  }
  return ret;
}

int ObDistributedScheduler::parse_all_jobs_and_start_root_job(ObExecContext& ctx, ObPhysicalPlan* phy_plan)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exe_ctx = ctx.get_task_executor_ctx();
  ObJob* root_job = NULL;
  if (OB_ISNULL(task_exe_ctx) || OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("executor_ctx or phy_plan is NULL", K(ret), K(task_exe_ctx), K(phy_plan));
  } else {
    ObExecutionID ob_execution_id;
    ob_execution_id.set_server(ObSqlExecutionIDMap::is_outer_id(execution_id_) ? ObExecutionID::global_id_addr()
                                                                               : task_exe_ctx->get_self_addr());
    ob_execution_id.set_execution_id(execution_id_);
    if (OB_FAIL(parser_.parse_job(ctx, phy_plan, ob_execution_id, spfactory_, job_control_))) {
      LOG_WARN("fail parse job for scheduler", K(ret));
    } else if (OB_FAIL(job_control_.get_root_job(root_job))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail get root job", K(ret));
    } else {
      ObLocalTaskExecutor local_task_executor;
      ObLocalJobExecutor local_job_executor;
      local_job_executor.set_task_executor(local_task_executor);
      local_job_executor.set_job(*root_job);
      if (OB_FAIL(local_job_executor.execute(ctx))) {
        LOG_WARN("fail execute root job");
      }
    }
  }
  return ret;
}

int ObDistributedScheduler::schedule(ObExecContext& ctx, ObPhysicalPlan* phy_plan)
{
  UNUSED(phy_plan);
  NG_TRACE(distributed_schedule_begin);
  int ret = OB_SUCCESS;

  scheduler_id_ = next_scheduler_id();

  ObSchedulerThreadCtx& sche_thread_ctx = ctx.get_scheduler_thread_ctx();
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  ObSQLSessionInfo* my_session = ctx.get_my_session();
  bool need_serial_exec = false;
  const ObPhysicalPlan* phy_plan_ptr = NULL;
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(my_session) || OB_ISNULL(phy_plan_ptr = plan_ctx->get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan_ctx is NULL", K(ret), K(plan_ctx));
  } else {
    need_serial_exec = (phy_plan_ptr->get_need_serial_exec() || my_session->need_serial_exec());
  }
  int kill_ret = OB_SUCCESS;
  ObArray<ObJob*> ready_jobs;
  ObDistributedTaskExecutor task_exe(scheduler_id_);
  ObDistributedJobExecutor job_exe;
  ObDistributedTaskExecutor notify_task_exe(scheduler_id_);
  ObDistributedJobExecutor notify_job_exe;
  task_exe.set_trans_result(&trans_result_);
  notify_task_exe.set_trans_result(&trans_result_);
  while (OB_SUCC(ret)) {
    if (OB_FAIL(job_control_.get_ready_jobs(ready_jobs, need_serial_exec))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_ERROR("fail get ready jobs", K(ret));
      }
    } else {
      // not all finished, must continue running
      LOG_DEBUG("ready jobs", K(ready_jobs), "count", ready_jobs.count());
      for (int64_t i = 0; OB_SUCC(ret) && i < ready_jobs.count(); ++i) {
        task_exe.reset();
        job_exe.reset();
        ObJob* ready_job = ready_jobs.at(i);
        job_exe.set_task_executor(task_exe);
        job_exe.set_job(*ready_job);
        if (OB_FAIL(job_exe.execute_step(ctx))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail execute task", K(ret));
          }
        }
      }

      if (OB_FAIL(ret) && OB_ITER_END != ret) {
        if (OB_SUCCESS != (kill_ret = kill_all_jobs(ctx, job_control_))) {
          LOG_WARN("fail to kill all jobs", K(kill_ret));
        }
      } else {
        // wait one object
        ObTaskCompleteEvent* task_event = NULL;
        if (OB_FAIL(pop_task_event_for_sche(ctx, task_event, plan_ctx->get_timeout_timestamp()))) {
          LOG_WARN("fail to pop task event", K(ret));
        } else if (OB_ISNULL(task_event)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("task event is NULL", K(ret));
        } else if (OB_FAIL(plan_ctx->merge_implicit_cursors(task_event->get_implicit_cursors()))) {
          LOG_WARN("merge implicit cursor failed", K(ret), K(task_event->get_implicit_cursors()));
        } else {
          // receive a task event, then update job state
          ObJob* notify_job = NULL;
          ObTaskLocation task_loc = task_event->get_task_location();
          bool job_finished = false;
          if (OB_FAIL(job_control_.find_job_by_job_id(task_loc.get_job_id(), notify_job))) {
            LOG_ERROR("can not find job", K(ret), K(task_loc.get_job_id()));
          } else if (OB_ISNULL(notify_job)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("notify job is NULL", K(ret), K(task_loc.get_job_id()));
          } else if (OB_FAIL(notify_job->update_job_state(ctx, *task_event, job_finished))) {
            LOG_WARN("fail to update job state", K(task_loc.get_job_id()), K(task_loc.get_task_id()), K(ret));
          } else if (OB_FAIL(static_cast<int>(task_event->get_err_code()))) {
            if (OB_ERR_TASK_SKIPPED == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("task event error code is not success or skipped", K(ret), K(*task_event));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (job_finished) {
            signal_job_iter_end(notify_job->get_finish_queue());
          } else if (!notify_job->all_tasks_run()) {
            notify_task_exe.reset();
            notify_job_exe.reset();
            notify_job_exe.set_task_executor(notify_task_exe);
            notify_job_exe.set_job(*notify_job);
            if (OB_FAIL(notify_job_exe.execute_step(ctx))) {
              LOG_WARN("fail to executor step", K(ret), K(*notify_job));
            }
          }
        }
      }
    }
  }

  if (OB_FAIL(ret) && OB_UNLIKELY(OB_ITER_END != ret)) {
    int fail_ret = OB_SUCCESS;
    fail_ret = OB_SUCCESS;
    ObSEArray<ObTaskInfo*, 32> last_failed_task_infos;
    if (OB_SUCCESS != (fail_ret = job_control_.get_last_failed_task_infos(last_failed_task_infos))) {
      LOG_WARN("fail to get last failed task infos", K(ret), K(fail_ret));
    }
    if (last_failed_task_infos.count() > 0 &&
        (fail_ret = sche_thread_ctx.init_last_failed_partition(last_failed_task_infos.count())) != OB_SUCCESS) {
      LOG_WARN("init last failed partition info failed", K(ret), K(last_failed_task_infos.count()));
    }
    for (int64_t i = 0; OB_SUCCESS == fail_ret && i < last_failed_task_infos.count(); ++i) {
      ObTaskInfo* last_failed_task_info = last_failed_task_infos.at(i);
      if (OB_ISNULL(last_failed_task_info)) {
        fail_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("last_failed_task_info is NULL", K(ret), K(fail_ret), K(i));
      } else if (OB_SUCCESS !=
                 (fail_ret = sche_thread_ctx.add_last_failed_partition(last_failed_task_info->get_range_location()))) {
        LOG_WARN("fail to add last failed partition", K(ret), K(fail_ret), K(i), K(*last_failed_task_info));
      }
    }
    if (OB_SUCCESS != fail_ret) {
      sche_thread_ctx.clear_last_failed_partitions();
    }
    fail_ret = OB_SUCCESS;
    const static int64_t MAX_JC_STATUS_BUF_LEN = 4096;
    char jc_status_buf[MAX_JC_STATUS_BUF_LEN];
    if (OB_SUCCESS != (fail_ret = job_control_.print_status(jc_status_buf, MAX_JC_STATUS_BUF_LEN, true))) {
      LOG_WARN("fail to print job control status", K(ret), K(fail_ret), LITERAL_K(MAX_JC_STATUS_BUF_LEN));
    } else {
      LOG_WARN("fail to schedule, print jobs' status", K(ret), K(fail_ret), "jobs_status", jc_status_buf);
    }
    if (OB_SUCCESS != (fail_ret = signal_schedule_error(ret))) {
      LOG_WARN("fail to signal schedule error", K(fail_ret));
    }
  }
  NG_TRACE(distributed_schedule_end);
  return ret;
}

int ObDistributedScheduler::close_all_results(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObJob*, 2> all_jobs_except_root_job;
  if (OB_FAIL(job_control_.get_all_jobs_except_root_job(all_jobs_except_root_job))) {
    LOG_WARN("fail get all jobs");
  } else {
    int close_ret = OB_SUCCESS;
    ObDistributedTaskExecutor task_exe(scheduler_id_);
    ObDistributedJobExecutor job_exe;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_jobs_except_root_job.count(); ++i) {
      ObJob* job = all_jobs_except_root_job.at(i);
      if (OB_ISNULL(job)) {
        LOG_ERROR("job is NULL", K(ret), K(i));
      } else {
        task_exe.reset();
        job_exe.reset();
        job_exe.set_task_executor(task_exe);
        job_exe.set_job(*job);
        if (OB_SUCCESS != (close_ret = job_exe.close_all_results(ctx))) {
          LOG_WARN("fail close all results", K(close_ret), K(ret), K(i), K(*job));
        }
      }
    }
  }
  return ret;
}

int ObDistributedScheduler::kill_all_jobs(ObExecContext& ctx, ObJobControl& jc)
{
  int ret = OB_SUCCESS;
  ObArray<ObJob*> running_jobs;

  if (OB_FAIL(jc.get_running_jobs(running_jobs))) {
    LOG_WARN("fail to get running jobs", K(ret));
  } else {
    ObDistributedTaskExecutor distributed_task_executor(scheduler_id_);
    ObDistributedJobExecutor distributed_job_executor;
    for (int64_t i = 0; OB_SUCC(ret) && i < running_jobs.count(); ++i) {
      ObJob* running_job = running_jobs.at(i);
      if (OB_ISNULL(running_job)) {
        LOG_WARN("running job is NULL", K(ret), K(i));
      } else {
        int kill_ret = OB_SUCCESS;
        distributed_task_executor.reset();
        distributed_job_executor.reset();
        distributed_job_executor.set_task_executor(distributed_task_executor);
        distributed_job_executor.set_job(*running_job);
        if (OB_SUCCESS != (kill_ret = distributed_job_executor.kill_job(ctx))) {
          LOG_WARN("fail to kill job", K(kill_ret), K(ret), K(i), K(*running_job));
        }
      }
    }
  }
  return ret;
}

uint64_t ObDistributedScheduler::next_scheduler_id()
{
  static volatile uint64_t g_scheduler_id = 0;
  uint64_t id = 0;
  while (0 == (id = ATOMIC_AAF(&g_scheduler_id, 1))) {}
  return id;
}

void ObSchedulerThreadPool::handle(void* task)
{
  int ret = OB_SUCCESS;
  ObDistributedSchedulerManager* scheduler_mgr = NULL;
  ObDistributedSchedulerCtx* scheduler_ctx = NULL;
  const uint64_t* trace_id = NULL;
  ObDistributedExecContext dis_exec_ctx(GCTX.session_mgr_);
  if (OB_ISNULL(scheduler_mgr = ObDistributedSchedulerManager::get_instance())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("scheduler manager is NULL", K(ret));
  } else if (OB_ISNULL(scheduler_ctx = static_cast<ObDistributedSchedulerCtx*>(task))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("scheduler ctx is null", K(ret));
  } else if (OB_ISNULL(trace_id = scheduler_ctx->trace_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("trace id is null", K(ret), K(trace_id));
  } else {
    ObCurTraceId::set(trace_id);
    if (OB_FAIL(scheduler_mgr->do_schedule(*scheduler_ctx, dis_exec_ctx))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to schedule", K(ret));
      }
    }
  }
  return;
}

ObDistributedSchedulerManager::ObDistributedSchedulerHolder::ObDistributedSchedulerHolder()
    : inited_(false), execution_id_(OB_INVALID_ID), scheduler_(NULL), execution_id_map_(NULL)
{}

ObDistributedSchedulerManager::ObDistributedSchedulerHolder::~ObDistributedSchedulerHolder()
{
  reset();
}

void ObDistributedSchedulerManager::ObDistributedSchedulerHolder::reset()
{
  if (inited_) {
    if (OB_ISNULL(execution_id_map_)) {
      LOG_ERROR("execution_id_map_ is NULL", K(execution_id_), K(scheduler_));
    } else if (OB_UNLIKELY(OB_INVALID_ID == execution_id_)) {
      LOG_ERROR("execution_id_ is invalid", K(execution_id_map_), K(scheduler_));
    } else {
      execution_id_map_->revert(execution_id_);
    }
    inited_ = false;
    execution_id_ = OB_INVALID_ID;
    scheduler_ = NULL;
    execution_id_map_ = NULL;
  }
}

int ObDistributedSchedulerManager::ObDistributedSchedulerHolder::init(
    ObDistributedScheduler* scheduler, uint64_t execution_id, ExecutionIDMap& execution_id_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", K(ret), K(execution_id_), K(execution_id));
  } else if (OB_ISNULL(scheduler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("scheduler is NULL", K(ret), K(scheduler), K(execution_id));
  } else if (OB_UNLIKELY(OB_INVALID_ID == execution_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("execution_id is invalid", K(ret), K(scheduler), K(execution_id));
  } else {
    inited_ = true;
    execution_id_ = execution_id;
    scheduler_ = scheduler;
    execution_id_map_ = &execution_id_map;
  }
  return ret;
}

int ObDistributedSchedulerManager::ObDistributedSchedulerHolder::get_scheduler(ObDistributedScheduler*& scheduler)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("scheduler holder not init", K(ret), K(execution_id_), K(scheduler_));
  } else if (OB_ISNULL(scheduler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("scheduler_ is NULL", K(ret), K(execution_id_), K(scheduler_));
  } else {
    scheduler = scheduler_;
  }
  return ret;
}

void ObDistributedSchedulerManager::ObDistributedSchedulerKiller::operator()(const uint64_t execution_id)
{
  int ret = OB_SUCCESS;
  ObDistributedSchedulerManager* scheduler_mgr = ObDistributedSchedulerManager::get_instance();
  ObDistributedScheduler* scheduler = NULL;
  if (OB_ISNULL(scheduler_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("scheduler manager is NULL", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == execution_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid execution id", K(ret), K(execution_id));
  } else if (OB_ISNULL(scheduler = scheduler_mgr->execution_id_map_.fetch(execution_id))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to fetch scheduler from id map, maybe it has been deleted", K(ret), K(execution_id));
  } else {
    if (OB_FAIL(scheduler->stop())) {
      LOG_WARN("fail to stop scheduler", K(ret), K(execution_id));
    }
    scheduler_mgr->execution_id_map_.revert(execution_id);
  }
}

ObDistributedSchedulerManager::ObDistributedSchedulerManager()
    : inited_(false), execution_id_map_(), is_stopping_(false), distributed_scheduler_killer_()
{}

ObDistributedSchedulerManager::~ObDistributedSchedulerManager()
{}

void ObDistributedSchedulerManager::reset()
{
  inited_ = false;
  execution_id_map_.destroy();
  is_stopping_ = false;
  distributed_scheduler_killer_.reset();
}

int ObDistributedSchedulerManager::build_instance()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL != instance_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("instance is not NULL, build twice", K(ret));
  } else if (OB_UNLIKELY(NULL == (instance_ = OB_NEW(ObDistributedSchedulerManager, ObModIds::OB_SQL_EXECUTOR)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("instance is NULL, unexpected", K(ret));
  } else if (OB_FAIL(instance_->init())) {
    OB_DELETE(ObDistributedSchedulerManager, ObModIds::OB_SQL_EXECUTOR, instance_);
    instance_ = NULL;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to init distributed scheduler", K(ret));
  } else {
    // empty
  }
  return ret;
}

ObDistributedSchedulerManager* ObDistributedSchedulerManager::get_instance()
{
  ObDistributedSchedulerManager* instance = NULL;
  if (OB_ISNULL(instance_) || OB_UNLIKELY(!instance_->inited_)) {
    LOG_ERROR("instance is NULL or not inited", K(instance_));
  } else {
    instance = instance_;
  }
  return instance;
}

int ObDistributedSchedulerManager::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("distributed scheduler manager init twice", K(ret));
  } else if (OB_FAIL(execution_id_map_.init(!lib::is_mini_mode() ? DEFAULT_ID_MAP_SIZE : MINI_MODE_ID_MAP_SIZE))) {
    LOG_WARN("fail to init execution id map", K(ret), LITERAL_K(DEFAULT_ID_MAP_SIZE));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(lib::TGDefIDs::SqlDistSched, scheduler_pool_))) {
    LOG_WARN("fail to init scheduler pool", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObDistributedSchedulerManager::alloc_scheduler(ObExecContext& ctx, uint64_t& execution_id)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObDistributedScheduler* scheduler = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("manager not init", K(ret));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopping_))) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("server is stopping", K(ret));
  } else {
    const uint64_t outer_id = ctx.get_execution_id();
    execution_id = OB_INVALID_ID;
    if (OB_ISNULL(scheduler = OB_NEW(ObDistributedScheduler, ObModIds::OB_SQL_EXECUTOR_SCHEDULER))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail allocate memory for distributed scheduler", K(ret));
    } else if (OB_FAIL(scheduler->init())) {
      OB_DELETE(ObDistributedScheduler, ObModIds::OB_SQL_EXECUTOR_SCHEDULER, scheduler);
      LOG_WARN("fail to init scheduler", K(ret));
    } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopping_))) {
      OB_DELETE(ObDistributedScheduler, ObModIds::OB_SQL_EXECUTOR_SCHEDULER, scheduler);
      ret = OB_SERVER_IS_STOPPING;
      LOG_WARN("server is stopping", K(ret));
    } else {
      if (OB_INVALID_ID != outer_id && ObSqlExecutionIDMap::is_outer_id(outer_id)) {
        if (OB_FAIL(execution_id_map_.assign_external_id(outer_id, scheduler))) {
          LOG_WARN("assign id to idmap failed", K(ret));
        } else {
          execution_id = outer_id;
        }
      } else {
        if (OB_FAIL(execution_id_map_.assign(scheduler, execution_id))) {
          LOG_WARN("fail set scheduler to map", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        OB_DELETE(ObDistributedScheduler, ObModIds::OB_SQL_EXECUTOR_SCHEDULER, scheduler);
      } else if (OB_UNLIKELY(OB_INVALID_ID == execution_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("scheduler id is invalid", K(ret), K(execution_id));
      } else {
        scheduler->set_execution_id(execution_id);
      }
    }
  }
  return ret;
}

int ObDistributedSchedulerManager::close_scheduler(ObExecContext& ctx, uint64_t execution_id)
{
  int ret = OB_SUCCESS;
  ObDistributedScheduler* scheduler = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == execution_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid execution id", K(ret), K(execution_id));
  } else if (OB_ISNULL(scheduler = execution_id_map_.fetch(execution_id))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to fetch scheduler from id map", K(ret), K(execution_id));
  } else {
    if (OB_FAIL(scheduler->signal_root_finish())) {
      LOG_WARN("signal root finish failed", K(ret));
    }
    if (OB_FAIL(scheduler->wait_schedule_finish())) {
      LOG_WARN("wait schedule finish failed", K(ret));
    }
    execution_id_map_.revert(execution_id);
    int merge_ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS != (merge_ret = ctx.merge_scheduler_info()))) {
      LOG_WARN("fail to merge scheduler_retry info", K(ret), K(merge_ret));
    }
  }
  return ret;
}

int ObDistributedSchedulerManager::free_scheduler(uint64_t execution_id)
{
  int ret = OB_SUCCESS;
  ObDistributedScheduler* scheduler = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == execution_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid execution id", K(ret), K(execution_id));
  } else if (OB_ISNULL(scheduler = execution_id_map_.fetch(execution_id, FM_MUTEX_BLOCK))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to fetch scheduler from id map", K(ret), K(execution_id));
  } else {
    OB_DELETE(ObDistributedScheduler, ObModIds::OB_SQL_EXECUTOR_SCHEDULER, scheduler);
    execution_id_map_.revert(execution_id, true);
  }
  return ret;
}

int ObDistributedSchedulerManager::get_scheduler(uint64_t execution_id, ObDistributedSchedulerHolder& scheduler_holder)
{
  int ret = OB_SUCCESS;
  ObDistributedScheduler* scheduler = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == execution_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid execution id", K(ret), K(execution_id));
  } else if (OB_ISNULL(scheduler = execution_id_map_.fetch(execution_id))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to fetch scheduler from id map", K(ret), K(execution_id));
  } else if (OB_FAIL(scheduler_holder.init(scheduler, execution_id, execution_id_map_))) {
    LOG_WARN("fail to init scheduler holder", K(ret), K(execution_id));
  }
  return ret;
}

int ObDistributedSchedulerManager::parse_jobs_and_start_sche_thread(
    uint64_t execution_id, ObExecContext& ctx, ObPhysicalPlan* phy_plan, int64_t timeout_timestamp)
{
  int ret = OB_SUCCESS;
  ObDistributedScheduler* scheduler = NULL;
  ObExecutorRpcImpl* exec_rpc = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == execution_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid execution id", K(ret), K(execution_id));
  } else if (OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan is NULL", K(ret));
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(ctx, exec_rpc))) {
    LOG_WARN("get task executor rpc failed", K(ret));
  } else if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_ISNULL(scheduler = execution_id_map_.fetch(execution_id))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to fetch scheduler", K(ret), K(execution_id));
  } else {
    if (OB_FAIL(scheduler->parse_all_jobs_and_start_root_job(ctx, phy_plan))) {
      LOG_WARN("fail to parse all jobs", K(ret), K(execution_id));
    } else if (OB_FAIL(scheduler->init_trans_result(*ctx.get_my_session(), exec_rpc))) {
      LOG_WARN("fail to init trans result", K(ret), K(execution_id));
    } else {
      scheduler->set_exec_stat_collector(&ctx.get_exec_stat_collector());
    }
    execution_id_map_.revert(execution_id);
    if (OB_FAIL(ret)) {
    } else {
      ObDistributedSchedulerCtx* scheduler_ctx =
          static_cast<ObDistributedSchedulerCtx*>(ctx.get_allocator().alloc(sizeof(ObDistributedSchedulerCtx)));
      int64_t buf_len = ctx.get_serialize_size();
      char* exec_ctx_buf = static_cast<char*>(ctx.get_allocator().alloc(buf_len));
      ObPhysicalPlanCtx* phy_plan_ctx = ctx.get_physical_plan_ctx();
      ObSQLSessionInfo* my_session = ctx.get_my_session();
      int64_t pos = 0;
      if (OB_ISNULL(scheduler_ctx) || OB_ISNULL(exec_ctx_buf) || OB_ISNULL(my_session)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("some value is null", K(ret), K(scheduler_ctx), K(exec_ctx_buf), K(execution_id));
      } else if (OB_ISNULL(phy_plan_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("physical plan context is null", K(ret), K(phy_plan_ctx), K(execution_id));
      } else if (OB_FAIL(ctx.serialize(exec_ctx_buf, buf_len, pos))) {
        LOG_WARN("fail to serialize exec ctx", K(ret));
      } else {
        scheduler_ctx->trace_id_ = ObCurTraceId::get();
        scheduler_ctx->execution_id_ = execution_id;
        scheduler_ctx->exec_ctx_ = &ctx;
        scheduler_ctx->exec_ctx_buf_ = exec_ctx_buf;
        scheduler_ctx->buf_len_ = buf_len;
        ctx.get_scheduler_thread_ctx().set_build_index_plan(stmt::T_BUILD_INDEX_SSTABLE == phy_plan->get_stmt_type());
        ctx.get_scheduler_thread_ctx().set_plain_select_stmt(phy_plan_ctx->is_plain_select_stmt());
        if (OB_FAIL(TG_PUSH_TASK(lib::TGDefIDs::SqlDistSched, static_cast<void*>(scheduler_ctx)))) {
          LOG_WARN("fail to start scheduler", K(ret), K(execution_id));
          ret = (OB_EAGAIN == ret) ? OB_ERR_SCHEDULER_THREAD_NOT_ENOUGH : ret;
        } else if (FALSE_IT(scheduler->set_sche_thread_started(true))) {
        } else if ((phy_plan->get_need_serial_exec() || my_session->need_serial_exec()) &&
                   OB_FAIL(scheduler->wait_can_serial_exec(ctx, timeout_timestamp))) {
          LOG_WARN("fail to wait can serial exec", K(ret), K(execution_id));
        }
      }
    }
  }
  return ret;
}

int ObDistributedSchedulerManager::do_schedule(
    ObDistributedSchedulerCtx& sched_ctx, ObDistributedExecContext& dis_exec_ctx)
{
  int ret = OB_SUCCESS;
  ObDistributedScheduler* scheduler = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == sched_ctx.execution_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("execution id is invalid", K(ret), K(sched_ctx.execution_id_));
  } else if (OB_ISNULL(scheduler = execution_id_map_.fetch(sched_ctx.execution_id_))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_ERROR("fail to fetch scheduler", K(ret), K(sched_ctx.execution_id_));
  } else {
    ObExecContext* exec_ctx = NULL;
    char* exec_ctx_buf = NULL;
    int64_t buf_len = 0;
    int64_t pos = 0;
    ObPhysicalPlanCtx* dis_plan_ctx = nullptr;
    if (OB_ISNULL(exec_ctx = sched_ctx.exec_ctx_) || OB_ISNULL(exec_ctx_buf = sched_ctx.exec_ctx_buf_) ||
        (buf_len = sched_ctx.buf_len_) <= 0 || OB_ISNULL(plan_ctx = sched_ctx.exec_ctx_->get_physical_plan_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("exec ctx or exec ctx buf is invalid",
          K(ret),
          K(exec_ctx),
          "exec ctx buf",
          sched_ctx.exec_ctx_buf_,
          "buf len",
          sched_ctx.buf_len_);
    } else if (OB_FAIL(dis_exec_ctx.deserialize(exec_ctx_buf, buf_len, pos))) {
      LOG_WARN("fail to deserialize exec ctx", K(ret));
    } else if (OB_ISNULL(dis_plan_ctx = dis_exec_ctx.get_physical_plan_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("distributed plan ctx is null", K(ret));
    } else if (OB_FAIL(dis_plan_ctx->assign_batch_stmt_param_idxs(plan_ctx->get_batched_stmt_param_idxs()))) {
      LOG_WARN("assign batch stmt param idxs failed", K(ret));
    } else if (NULL == dis_exec_ctx.get_my_session()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else if (OB_FAIL(dis_exec_ctx.get_part_row_manager().assign(sched_ctx.exec_ctx_->get_part_row_manager()))) {
      LOG_WARN("assign part row manager failed", K(ret));
    } else if (FALSE_IT(exec_ctx->get_scheduler_thread_ctx().set_dis_exec_ctx(&dis_exec_ctx))) {
    } else {
      ObWorkerSessionGuard worker_session_guard(dis_exec_ctx.get_my_session());
      ObSQLSessionInfo::LockGuard lock_guard(dis_exec_ctx.get_my_session()->get_query_lock());
      // Only switch compatibility mode, do not switch tenant context by WITH_CONTEXT,
      // because WITH_CONTEXT will fail if tenant has no unit on this scheduler in global index
      // building which scheduled by RS.
      share::CompatModeGuard compat_mode_guard(ORACLE_MODE == dis_exec_ctx.get_my_session()->get_compatibility_mode()
                                                   ? share::ObWorker::CompatMode::ORACLE
                                                   : share::ObWorker::CompatMode::MYSQL);
      if (OB_FAIL(scheduler->schedule(*exec_ctx, NULL))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to do schedule", K(ret), K(sched_ctx.execution_id_));
        }
      }
    }
    int wait_ret = OB_SUCCESS;
    int close_ret = OB_SUCCESS;
    int signal_ret = OB_SUCCESS;
    if (OB_FAIL(ret) && OB_UNLIKELY(OB_ITER_END != ret)) {
      if (OB_SUCCESS != (signal_ret = scheduler->signal_schedule_error(ret))) {
        LOG_WARN("fail to signal schedule error", K(signal_ret));
      }
    }
    if (!OB_ISNULL(plan_ctx)) {
      if (OB_SUCCESS != (wait_ret = scheduler->wait_all_task(plan_ctx->get_timeout_timestamp(),
                             sched_ctx.exec_ctx_->get_scheduler_thread_ctx().is_build_index_plan()))) {
        LOG_WARN("wait all task failed", K(wait_ret));
      }
    }
    if (OB_SUCCESS != (signal_ret = scheduler->signal_can_serial_exec())) {
      LOG_WARN("fail to signal can serial exec", K(signal_ret));
    }
    if (OB_SUCCESS != (wait_ret = scheduler->wait_root_use_up_data(*exec_ctx))) {
      LOG_WARN("fail to wait root use up data", K(wait_ret), K(sched_ctx.execution_id_));
    }
    // Remove interm result after all task finished,
    // make sure no interm result is added after remove.
    if (NULL != exec_ctx && !exec_ctx->is_reusable_interm_result()) {
      if (OB_UNLIKELY(OB_SUCCESS != (close_ret = scheduler->close_all_results(*exec_ctx)))) {
        LOG_WARN("fail to close all results", K(close_ret), K(sched_ctx.execution_id_));
      }
    }
    if (OB_LIKELY(NULL != exec_ctx)) {
      exec_ctx->get_scheduler_thread_ctx().set_dis_exec_ctx(NULL);
    }
    if (OB_SUCCESS != (signal_ret = scheduler->signal_schedule_finish())) {
      LOG_WARN("fail to signal schedule finish", K(signal_ret), K(sched_ctx.execution_id_));
    }
    execution_id_map_.revert(sched_ctx.execution_id_);
  }
  return ret;
}

int ObDistributedSchedulerManager::collect_extent_info(ObTaskCompleteEvent& task_event)
{
  int ret = OB_SUCCESS;
  ObTaskLocation task_loc;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("manager not init", K(ret));
  } else if (OB_UNLIKELY(!task_event.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("task event is invalid", K(ret), K(task_event));
  } else if (OB_UNLIKELY(!(task_loc = task_event.get_task_location()).is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("task location is invalid", K(ret), K(task_loc));
  } else {
    uint64_t execution_id = task_loc.get_execution_id();
    ObDistributedScheduler* scheduler = NULL;
    if (OB_UNLIKELY(OB_INVALID_ID == execution_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid scheduler id", K(ret), K(execution_id));
    } else if (OB_ISNULL(scheduler = execution_id_map_.fetch(execution_id))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to fetch scheduler from id map", K(ret), K(execution_id));
    } else {
      ObExecStatCollector* collector = scheduler->get_exec_stat_collector();
      if (NULL != collector) {
        int cret = collector->add_raw_stat(task_event.get_extend_info());
        if (OB_SUCCESS != cret) {
          LOG_DEBUG("fail add raw stat to stat collector", K(cret));
        }
      }
      execution_id_map_.revert(execution_id);
    }
  }
  return ret;
}

int ObDistributedSchedulerManager::signal_schedule_error(
    uint64_t execution_id, int sched_ret, const ObAddr addr, const uint64_t scheduler_id /* = 0 */)
{
  int ret = OB_SUCCESS;
  ObDistributedScheduler* scheduler = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == execution_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid scheduler id", K(ret), K(execution_id));
  } else if (OB_ISNULL(scheduler = execution_id_map_.fetch(execution_id))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to fetch scheduler from id map", K(ret), K(execution_id));
  } else {
    if (0 != scheduler_id && scheduler_id != scheduler->get_scheduler_id()) {
      LOG_WARN("scheduler id mismatch, may be retry of index building, ignore",
          K(execution_id),
          K(scheduler_id),
          K(scheduler->get_scheduler_id()));
    } else {
      if (addr.is_valid() && OB_RPC_CONNECT_ERROR == sched_ret && OB_FAIL(scheduler->atomic_push_err_rpc_addr(addr))) {
        LOG_WARN("fail to atomic push err rpc addr", K(ret));
      } else if (OB_FAIL(scheduler->signal_schedule_error(sched_ret))) {
        LOG_ERROR("fail signal error to scheduler, may block scheduler thread", K(sched_ret), K(ret));
      }
    }
    execution_id_map_.revert(execution_id);
  }
  return ret;
}

int ObDistributedSchedulerManager::signal_scheduler(
    ObTaskCompleteEvent& task_event, const uint64_t scheduler_id /* = 0 */)
{
  int ret = OB_SUCCESS;
  ObTaskLocation task_loc;
  ObDistributedScheduler* scheduler = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("manager not init", K(ret));
  } else if (OB_UNLIKELY(!task_event.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("task event is invalid", K(ret), K(task_event));
  } else if (OB_UNLIKELY(!(task_loc = task_event.get_task_location()).is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("task location is invalid", K(ret), K(task_loc));
  } else {
    uint64_t execution_id = task_loc.get_execution_id();
    if (OB_UNLIKELY(OB_INVALID_ID == execution_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid scheduler id", K(ret), K(execution_id));
    } else if (OB_ISNULL(scheduler = execution_id_map_.fetch(execution_id))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to fetch scheduler from id map", K(ret), K(execution_id));
    } else {
      if (0 != scheduler_id && scheduler_id != scheduler->get_scheduler_id()) {
        LOG_WARN("scheduler id mismatch, may be retry of index building, ignore",
            K(task_loc),
            K(scheduler_id),
            K(scheduler->get_scheduler_id()));
      } else {
        if (OB_FAIL(scheduler->signal_finish_queue(task_event))) {
          LOG_WARN("fail to signal finish queue", K(ret), K(task_event));
        }
      }
      execution_id_map_.revert(execution_id);
    }
  }
  return ret;
}

int ObDistributedSchedulerManager::merge_trans_result(const ObTaskCompleteEvent& task_event)
{
  int ret = OB_SUCCESS;
  ObTaskLocation task_loc;
  ObDistributedScheduler* scheduler = NULL;
  /**
   * TODO:
   * we can refactor collect_extent_info() / signal_scheduler() / merge_trans_result()
   * later by adding a function get_execution_id() for their common code.
   * but remember that do not add execution_id_map_.fetch() into get_execution_id(),
   * it looks like a lock operation followed by a necessary unlock operation by calling
   * execution_id_map_.revert().
   */
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("manager not init", K(ret));
  } else if (OB_UNLIKELY(!task_event.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("task event is invalid", K(ret), K(task_event));
  } else if (OB_UNLIKELY(!(task_loc = task_event.get_task_location()).is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("task location is invalid", K(ret), K(task_loc));
  } else {
    uint64_t execution_id = task_loc.get_execution_id();
    if (OB_UNLIKELY(OB_INVALID_ID == execution_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid scheduler id", K(ret), K(execution_id));
    } else if (OB_ISNULL(scheduler = execution_id_map_.fetch(execution_id))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to fetch scheduler from id map", K(ret), K(execution_id));
    } else {
      if (OB_FAIL(scheduler->merge_trans_result(task_event))) {
        LOG_WARN("fail to merge trans result", K(ret), K(task_event));
      }
      execution_id_map_.revert(execution_id);
    }
  }
  return ret;
}

int ObDistributedSchedulerManager::set_task_status(const ObTaskID& task_id, ObTaskStatus status)
{
  int ret = OB_SUCCESS;
  uint64_t execution_id = task_id.get_execution_id();
  ObDistributedScheduler* scheduler = NULL;
  OV(inited_, OB_NOT_INIT);
  OV(execution_id != OB_INVALID_ID, OB_INVALID_ARGUMENT);
  OV(OB_NOT_NULL(scheduler = execution_id_map_.fetch(execution_id)), OB_INVALID_ARGUMENT, execution_id);
  OZ(scheduler->set_task_status(task_id, status));
  if (OB_NOT_NULL(scheduler)) {
    // must revert if fetch success.
    execution_id_map_.revert(execution_id);
  }
  return ret;
}

int ObDistributedSchedulerManager::stop()
{
  if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopping_))) {
    LOG_WARN("server is already stopping, do nothing");
  } else {
    ATOMIC_STORE(&is_stopping_, true);
    execution_id_map_.traverse(distributed_scheduler_killer_);
  }
  return OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase
