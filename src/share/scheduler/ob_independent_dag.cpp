/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX COMMON
#include "ob_independent_dag.h"

namespace oceanbase
{
namespace share
{

#define KTASK(task) KP(&task), "type", task.get_type(), "status", task.get_status(), "dag", task.get_dag()
#define KTASK_PTR(task) KP(task), "type", task->get_type(), "status", task->get_status(), "dag", task->get_dag()

ObIndependentDag::ObIndependentDag(const ObDagType::ObDagTypeEnum type)
  : ObIDag(type),
    compat_mode_(THIS_WORKER.get_compatibility_mode())
{
  is_independent_ = true;
}

ObIndependentDag::~ObIndependentDag()
{
  reset();
}

bool ObIndependentDag::operator == (const ObIDag &other) const
{
  UNUSED(other);
  OB_ASSERT_MSG(false, "ObIndependentDag dose not promise operator ==");
  return false;
}

uint64_t ObIndependentDag::hash() const
{
  OB_ASSERT_MSG(false, "ObIndependentDag dose not promise hash");
  return 0;
}

int ObIndependentDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  UNUSEDx(out_param, allocator);
  OB_ASSERT_MSG(false, "ObIndependentDag dose not promise fill_info_param");
  return OB_NOT_SUPPORTED;
}

int ObIndependentDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  UNUSEDx(buf, buf_len);
  OB_ASSERT_MSG(false, "ObIndependentDag dose not promise fill_dag_key");
  return OB_NOT_SUPPORTED;
}

int ObIndependentDag::basic_init(ObIAllocator &allocator)
{
   // independent dag may create many tasks, and the memory of finished task need to be released as soon as possible.
   // If the input allocator is an ObArenaAllocator, the memory will not be released until the ObArenaAllocator destructs.
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  ObTenantDagScheduler* dag_scheduler = MTL(ObTenantDagScheduler*);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "dag init twice", K(ret));
  } else if (OB_ISNULL(dag_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag scheduler is nullptr", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::INDEPENDENT_DAG_COND_WAIT))) {
    COMMON_LOG(WARN, "failed to init cond", K(ret));
  } else {
    allocator_ = &dag_scheduler->get_independent_allocator();
    is_inited_ = true;
  }
  return ret;
}

int ObIndependentDag::add_task(ObITask &task)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "dag is not inited", K(ret));
  } else {
    // need protected by lock_, otherwise task may be scheduled before print log
    lib::ObMutexGuard guard(lock_);
    task.set_status(ObITask::TASK_STATUS_WAITING);
    if (OB_FAIL(check_task_status())) {
      COMMON_LOG(WARN, "check task status failed", K(ret), K(task));
    } else {
      if (0 == task.get_indegree()) {
        ObThreadCondGuard guard(cond_);
        if (OB_TMP_FAIL(cond_.signal())) {
          COMMON_LOG(WARN, "failed to signal cond", K(tmp_ret));
        } else {
          COMMON_LOG(TRACE, "add task and wake up cond", K(task));
        }
      }
      COMMON_LOG(INFO, "independent dag add task success", K(ret), KTASK(task));
    }
  }
  return ret;
}

int ObIndependentDag::batch_add_task(const ObIArray<ObITask *> &task_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "dag is not inited", K(ret));
  } else {
    // after add task, the task maybe schedule and destroy immediately, so print log first
    // COMMON_LOG(INFO, "independent dag batch add task", K(ret), K(task_array.count()), K(task_array));
    if (OB_FAIL(ObIDag::batch_add_task(task_array))) {
      LOG_WARN("batch add task failed", K(ret));
    } else {
      ObThreadCondGuard guard(cond_);
      if (OB_TMP_FAIL(cond_.broadcast())) {
        COMMON_LOG(WARN, "failed to broadcast cond", K(tmp_ret));
      } else {
        COMMON_LOG(TRACE, "batch add task and wake up cond");
      }
    }
  }
  return ret;
}

int ObIndependentDag::init(
    const ObIDagInitParam *param,
    const ObDagId *dag_id /*= nullptr*/,
    const bool inherit_trace_id /*= false*/)
{
  int ret = OB_SUCCESS;
  ObDagId cur_dag_id;
  if (IS_NOT_INIT) {
    // It is a little confusing here, since is_inited_ is set true when allocing dag and do basic_init
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "is not basic inited", K(ret));
  } else if (OB_ISNULL(param) || OB_UNLIKELY(!param->is_valid())
            || (OB_NOT_NULL(dag_id) && OB_UNLIKELY(!dag_id->is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid args", K(ret), KP(param), KPC(dag_id));
  } else if (OB_FAIL(init_by_param(param))) {
    COMMON_LOG(WARN, "failed to init dag", K(ret));
  } else if (OB_FAIL(create_first_task())) {
    COMMON_LOG(WARN, "failed to create first task", K(ret), KPC(this));
  } else if (OB_NOT_NULL(dag_id)) {
    cur_dag_id.set(*dag_id);
  } else if (inherit_trace_id) {
    if (OB_NOT_NULL(ObCurTraceId::get_trace_id())) {
      cur_dag_id.set(*ObCurTraceId::get_trace_id());
    } else {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "current trace id is not set", K(ret));
    }
  } else if (OB_FAIL(ObSysTaskStatMgr::get_instance().generate_task_id(cur_dag_id))) {
    COMMON_LOG(WARN, "failed to generate dag id", K(ret));
  } else {
    // no need to start a background sys task like ObIDag
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(set_dag_id(cur_dag_id))) {
    COMMON_LOG(WARN, "failed to set dag id", K(ret), K(cur_dag_id));
  } else {
    set_dag_status(ObIDag::DAG_STATUS_NODE_RUNNING);
  }
  return ret;
}

void ObIndependentDag::dump_dag_status(const char *log_info /*= "Print the status of independent dag"*/)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "dag is not inited, can not dump dag status", K(ret));
  } else {
    lib::ObMutexGuard guard(lock_);
    COMMON_LOG(INFO, log_info, K(ObPrintIndependentDag(*this)));
  }
}

int ObIndependentDag::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "dag is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_independent_ || !cond_.is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid dag status", K(ret), K_(is_independent));
  } else {
    ObTraceIdGuard trace_id_guard(id_);
    COMMON_LOG(INFO, "dag start process", KPC(this));
    int task_ret = OB_SUCCESS;
    while (OB_SUCC(ret) && OB_SUCCESS == task_ret && !is_final_status()) {
      ObITask *task = nullptr;
      task_ret = THIS_WORKER.check_status();
      if (OB_SUCCESS != task_ret) {
        LOG_WARN("check status failed", K(task_ret));
      } else if (OB_FAIL(schedule_one(task))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          (void) wait_signal();
        } else {
          COMMON_LOG(WARN, "failed to schedule one task", K(ret));
        }
      } else if (OB_FAIL(process_task(task, task_ret))) {
        COMMON_LOG(WARN, "failed to process task", K(ret));
      }
      if (OB_FAIL(ret)) {
        simply_set_stop(ret); // set this dag stop to notify other threads
        COMMON_LOG(WARN, "task schedule failed in this thread", K(ret), KPC(this));
      }
    }

    // if task execute failed, return the retcode of task first
    if (OB_SUCCESS != task_ret) {
      ret = task_ret;
    } else if (OB_FAIL(ret)) {
    } else if (has_set_stop()) {
      ret = OB_CANCELED;
    }
    COMMON_LOG(INFO, "dag finish process", K(ret), "final_status", is_final_status(), KPC(this));
  }
  return ret;
}

bool ObIndependentDag::inner_add_task_into_list(ObITask *task)
{
  task->set_list_idx(ObITask::WAITING_TASK_LIST);
  return waiting_task_list_.add_last(task);
}

int ObIndependentDag::inner_remove_task(ObITask &task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task.get_list_idx() == ObITask::TASK_LIST_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "task is not in task list", K(ret), K(task));
  } else {
    TaskList &task_list = task.get_list_idx() == ObITask::WAITING_TASK_LIST ? waiting_task_list_ : task_list_;
    if (OB_ISNULL(task_list.remove(&task))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "failed to remove task from task_list", K(ret), K_(id), K(task));
    }
  }
  return ret;
}

void ObIndependentDag::clear_task_list()
{
  ObIDag::clear_task_list();
  inner_clear_task_list(waiting_task_list_);
}

void ObIndependentDag::reset()
{
  cond_.destroy();
  inner_clear_task_list(waiting_task_list_);
  is_independent_ = true;
  ObIDag::reset();
}

int ObIndependentDag::check_cycle()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITask *, DEFAULT_TASK_NUM> stack;
  // at the beginning of cycle detection, reset everyone's color and last_visit_child
  prepare_check_cycle(task_list_);
  prepare_check_cycle(waiting_task_list_);
  if (OB_FAIL(do_check_cycle(stack, task_list_))) {
    COMMON_LOG(WARN, "failed to do check cycle", K(ret));
  } else if (OB_FAIL(do_check_cycle(stack, waiting_task_list_))) {
    COMMON_LOG(WARN, "failed to do check cycle", K(ret));
  }
  return ret;
}

int ObIndependentDag::update_ready_task_list()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  lib::ObMutexGuard guard(lock_);

  TaskList tmp_ready_lists[ObITask::TASK_PRIO_MAX - 1]; // except ObITask::TASK_PRIO_0
  ObITask *cur_task = waiting_task_list_.get_first();
  ObITask *next_task = nullptr;
  const ObITask *head = waiting_task_list_.get_header();
  int64_t new_ready_cnt = 0;
  while (OB_SUCC(ret) && new_ready_cnt < READY_TASK_BATCH_MOVE_SIZE && head != cur_task) {
    next_task = cur_task->get_next();
    const ObITask::ObITaskPriority prio = cur_task->get_priority();
    if (prio > ObITask::TASK_PRIO_0 && prio < ObITask::TASK_PRIO_MAX) {
      if (OB_ISNULL(waiting_task_list_.remove(cur_task))) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, "failed to remove task from task_list", K(ret));
      } else if (!OB_UNLIKELY(tmp_ready_lists[prio - 1].add_last(cur_task))) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, "failed to add task to ready list", K(ret));
        ob_abort();
      } else {
        cur_task->set_list_idx(ObITask::READY_TASK_LIST);
        new_ready_cnt++;
      }
    }
    cur_task = next_task;
  }

  if (OB_SUCC(ret) && new_ready_cnt > 0) {
    for (int64_t i = ObITask::TASK_PRIO_MAX - 2; i >= 0; i--) {
      (void) task_list_.push_back_range(tmp_ready_lists[i]);
    }
    {
      ObThreadCondGuard guard(cond_);
      if (OB_TMP_FAIL(new_ready_cnt > 1 ? cond_.broadcast() : cond_.signal())) {
        COMMON_LOG(WARN, "failed to wake up cond", K(tmp_ret));
      }
    }
    COMMON_LOG(TRACE, "finish update ready task list and wake up cond", K(ret), K(new_ready_cnt), "ready_task_list_cnt", task_list_.get_size(), "waiting_task_list_cnt", waiting_task_list_.get_size());
  }
  return ret;
}

void ObIndependentDag::post_signal()
{
  ObThreadCondGuard guard(cond_);
  (void) cond_.signal();
}

// If there is no ready task but waiting task list is not empty, try to update ready task list and get next ready task
int ObIndependentDag::try_get_next_ready_task(ObITask *&task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_next_ready_task(task))) {
    if (OB_ITER_END != ret) {
      COMMON_LOG(WARN, "failed to get next ready task", K(ret));
    } else if (waiting_task_list_.is_empty()) {
    } else if (FALSE_IT(ret = OB_SUCCESS)) {
    } else if (OB_FAIL(update_ready_task_list())) {
      COMMON_LOG(WARN, "failed to update ready task list", K(ret));
    } else if (OB_FAIL(get_next_ready_task(task))) {
      if (OB_ITER_END != ret) {
        COMMON_LOG(WARN, "failed to get next ready task", K(ret));
      }
    }
  }
  return ret;
} // To better git diff result, add a comment here, meaningless

int ObIndependentDag::schedule_one(ObITask *&task)
{
  int ret = OB_SUCCESS;
  task = nullptr;
  // ATTENTION !!! Don't hold lock_ in this function, since task->generate_next_task() will acquire lock_ to add new task into this dag.
  // So when get_next_ready_task for independent dag, the status of runnable task should be pre-set.
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "dag is not inited", K(ret));
  } else if (OB_FAIL(try_get_next_ready_task(task))) {
    if (OB_ITER_END != ret) {
      COMMON_LOG(WARN, "failed to get next ready task", K(ret));
    } else if (REACH_THREAD_TIME_INTERVAL(DAG_SCHEDULE_ONE_PRINT_INTERVAL)) {
      (void) dump_dag_status("There is no ready task, wait for a while");
    }
  } else if (OB_ISNULL(task)) {
    ret = OB_ENTRY_NOT_EXIST;
    COMMON_LOG(WARN, "next ready task is null", K(ret));
  } else if (ObITask::TASK_STATUS_RE_RUNNING != task->get_status()
              && OB_FAIL(task->generate_next_task())) { // TODO(chengkong): not all task need do this
    reset_task_running_status(*task, ObITask::TASK_STATUS_FAILED);
    COMMON_LOG(WARN, "failed to generate next task", K(ret), KPC(task));
  } else {
    const bool is_retry = ObITask::TASK_STATUS_RE_RUNNING == task->get_status();
    task->set_status(ObITask::TASK_STATUS_RUNNING);
    COMMON_LOG(INFO, "success to schedule one task", K(ret), K_(running_task_cnt), K(is_retry), KTASK_PTR(task));
  }
  return ret;
}

void ObIndependentDag::wait_signal()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObThreadCondGuard guard(cond_);
  const int64_t begin_wait_time = ObTimeUtility::fast_current_time();
  if (OB_TMP_FAIL(cond_.wait(DAG_COND_TIMEOUT_MS))) {
    if (OB_TIMEOUT != tmp_ret) {
      COMMON_LOG(WARN, "failed to wait cond", K(tmp_ret));
    }
  } else {
    const int64_t cost_time_us = ObTimeUtility::fast_current_time() - begin_wait_time;
    COMMON_LOG(TRACE, "wait cond success", K(cost_time_us));
  }
}
int ObIndependentDag::execute_task(ObITask &task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task.do_work(false /*is_sys_task*/))) {
    if (OB_DAG_TASK_IS_SUSPENDED != ret) {
      COMMON_LOG(WARN, "failed to execute task", K(ret), K(task));
    }
  } else if (OB_FAIL(task.post_generate_next_task())) {
    COMMON_LOG(WARN, "failed to generate next task", K(ret), K(task));
  }

  if (OB_DAG_TASK_IS_SUSPENDED == ret) {
    lib::ObMutexGuard guard(lock_);
    if (is_dag_failed()) {
      // dag is failed, no need to retry this task, just take it as failure
      ret = dag_ret_;
    }
  }

  return ret;
}

int ObIndependentDag::process_task(ObITask *&task, int &task_ret)
{
  int ret = OB_SUCCESS;
  task_ret = OB_SUCCESS;
  bool task_is_suspended = false;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "task is null", K(ret));
  } else if (FALSE_IT(task_ret = execute_task(*task))) {
  } else if (OB_FAIL(deal_with_finish_task(task, task_ret /*error_code*/, task_is_suspended))) {
    COMMON_LOG(WARN, "failed to deal with finish task", K(ret), K(task_ret), KPC(task));
  } else if (OB_DAG_TASK_IS_SUSPENDED == task_ret && task_is_suspended) {
    task_ret = OB_SUCCESS;
  }
  return ret;
}

int ObIndependentDag::deal_with_finish_task(
    ObITask *&task,
    const int error_code,
    bool &task_is_suspended)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  task_is_suspended = false;
  ObITask *cur_task = task;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "dag is not inited", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "task is nullptr", K(ret));
  } else if (OB_DAG_TASK_IS_SUSPENDED == error_code && OB_FAIL(deal_with_suspended_task(*task, error_code, task_is_suspended))) {
    COMMON_LOG(WARN, "failed to deal with suspended task", K(ret));
  } else if (task_is_suspended) {
    // do nothing, wait for resume
  } else {
    lib::ObMutexGuard guard(lock_);
    int64_t ready_task_cnt = 0;
    if (OB_SUCCESS != error_code && !is_dag_failed()) {
      inner_set_dag_failed(error_code);
    }

    // not support retry independent dag now
    // ATTENTION!! if task is finished, it will be set to nullptr here
    if (OB_FAIL(inner_finish_task(task, &ready_task_cnt))) {
      COMMON_LOG(WARN, "failed to finish task", K(ret));
    } else if (ready_task_cnt >= 1) {
      ObThreadCondGuard guard(cond_);
      if (OB_TMP_FAIL(ready_task_cnt == 1 ? cond_.signal() : cond_.broadcast())) {
        COMMON_LOG(WARN, "failed to wake up cond", K(tmp_ret));
      } else {
        COMMON_LOG(TRACE, "task execute success, wake up cond", K(ready_task_cnt));
      }
    }

    if (inner_has_finished()) {
      ObIDag::ObDagStatus status = is_dag_failed() ? ObIDag::DAG_STATUS_ABORT : ObIDag::DAG_STATUS_FINISH;
      set_dag_status(status);
      COMMON_LOG(INFO, "dag finished", K(ret), K_(dag_ret), K(status), "runtime", ObTimeUtility::fast_current_time() - start_time_, KPC(this));
    }
  }
  // ATTENTION!! Only print address for cur_task, since its memory is freed after finish_task
  COMMON_LOG(INFO, "finish deal with finish task", K(ret), KP(cur_task), K(error_code), K(task_is_suspended));
  return ret;
}

int ObIndependentDag::deal_with_suspended_task(
    ObITask &task,
    const int error_code,
    bool &task_is_suspended)
{
  int ret = OB_SUCCESS;
  task_is_suspended = false;
  lib::ObMutexGuard guard(lock_);
  if (OB_FAIL(task.reset_status_for_suspend())) {
    COMMON_LOG(WARN, "failed to reset status for suspend", K(ret), K(task));
  } else if (OB_ISNULL(task_list_.remove(&task))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "failed to remove suspended task from task list", K(ret), K(task));
  } else if (OB_UNLIKELY(!waiting_task_list_.add_last(&task))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "failed to add task into waiting list", K(ret), K(task));
  } else {
    task_is_suspended = true;
    task.set_status(ObITask::TASK_STATUS_RETRY);
    task.set_list_idx(ObITask::WAITING_TASK_LIST);
    dec_running_task_cnt();
    COMMON_LOG(INFO, "task is suspended, retry later", K(ret), KTASK(task));
  }
  return ret;
} // To better git diff result, add a comment here, meaningless

/* --------------------------------- ObDagExecutor --------------------------------- */
ObDagExecutor::ObDagExecutor():
  is_inited_(false),
  allocator_(nullptr),
  lock_(ObLatchIds::DAG_EXECUTOR_LOCK),
  dag_(nullptr),
  dag_status_(ObIDag::DAG_STATUS_MAX),
  ref_cnt_(0)
{
}

ObDagExecutor::~ObDagExecutor()
{
  if (0 != ref_cnt_) {
    COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "dag executor is not released", KPC(this));
  }
  if (OB_NOT_NULL(dag_)) {
    dag_->~ObIndependentDag();
    allocator_->free(dag_);
    dag_ = nullptr;
  }
}

int ObDagExecutor::run()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "dag executor is not inited", K(ret));
  } else if (OB_FAIL(acquire_())) {
    if (OB_ITER_END != ret) {
      COMMON_LOG(WARN, "failed to acquire", K(ret));
    } else {
      ret = OB_SUCCESS; // dag is executed successfully by other thread
    }
  } else {
    if (OB_FAIL(dag_->process())) {
      COMMON_LOG(WARN, "failed to process dag", K(ret), K_(dag_status), K_(ref_cnt), KPC_(dag));
    } else {
      COMMON_LOG(INFO, "dag executor process success", K(ret), K_(dag_status), K_(ref_cnt), KPC_(dag));
    }
    (void) release_();
  }
  return ret;
}

int ObDagExecutor::acquire_()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(lock_);
  if (OB_UNLIKELY(ref_cnt_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "ref cnt is invalid", K(ret), K_(dag_status), K_(ref_cnt));
  } else if (OB_NOT_NULL(dag_)) {
    ++ref_cnt_;
    if (0 == dag_->get_start_time()) {
      dag_->set_start_time();
    }
  } else if (ObIDag::DAG_STATUS_FINISH != dag_status_) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is failed and released", K(ret), K_(dag_status));
  } else {
    ret = OB_ITER_END;
    COMMON_LOG(INFO, "dag is finished and no need to process", K(ret));
  }
  return ret;
}

void ObDagExecutor::release_()
{
  lib::ObMutexGuard guard(lock_);
  if (OB_UNLIKELY(ref_cnt_ <= 0) || OB_ISNULL(dag_)) {
    COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid executor status", K_(dag_status), K(ref_cnt_), KPC_(dag));
    ob_abort();
  } else {
    --ref_cnt_;
    if (0 == ref_cnt_) {
      dag_status_ = dag_->get_dag_status();
      COMMON_LOG(INFO, "dag run finish and destory it", K_(dag_status));
      dag_->~ObIndependentDag();
      allocator_->free(dag_);
      dag_ = nullptr;
    }
  }
}

/* --------------------------------- ObPrintIndependentDag --------------------------------- */
int64_t ObPrintIndependentDag::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_NAME("ObIndependentDag_Pretty");
    J_COLON();
    J_KV("id", dag_.id_,
         "type", ObIDag::get_dag_type_str(dag_.get_type()),
         "status", ObIDag::get_dag_status_str(dag_.get_dag_status()),
         "add_time", dag_.add_time_,
         "start_time", dag_.start_time_,
         "dag_ret", dag_.dag_ret_,
         "is_inited", dag_.is_inited_,
         "is_stop", dag_.is_stop_,
         "running_task_cnt", dag_.running_task_cnt_);
    {
      int64_t type_count[ObITask::TASK_TYPE_MAX] = {0};
      int64_t orphan_type_count[ObITask::TASK_TYPE_MAX] = {0}; // allocated but not added into dag
      ObIndependentDag::TaskList *task_lists[2] = {&dag_.task_list_, &dag_.waiting_task_list_};
      for (uint8_t i = 0; i < 2; ++i) {
        ObITask *cur = task_lists[i]->get_first();
        const ObITask *head = task_lists[i]->get_header();
        while (head != cur && nullptr != cur) {
          if (cur->get_type() < ObITask::TASK_TYPE_MAX) {
            type_count[cur->get_type()]++;
            if (ObITask::TASK_STATUS_INITING == cur->get_status()) {
              orphan_type_count[cur->get_type()]++;
            }
          }
          cur = cur->get_next();
        }
      }
      BUF_PRINTF(", \"task_type_count\": {");
      bool first = true;
      for (int i = 0; i < ObITask::TASK_TYPE_MAX; ++i) {
        if (type_count[i] > 0) {
          if (!first) BUF_PRINTF(", ");
          BUF_PRINTF("%s: %ld", ObITask::ObITaskTypeStr[i], type_count[i]);
          first = false;
        }
      }
      BUF_PRINTF("}");
      J_COMMA();
      BUF_PRINTF(", \"orphan_task_type_count\": {");
      first = true;
      for (int i = 0; i < ObITask::TASK_TYPE_MAX; ++i) {
        if (orphan_type_count[i] > 0) {
          if (!first) BUF_PRINTF(", ");
          BUF_PRINTF("%s: %ld", ObITask::ObITaskTypeStr[i], orphan_type_count[i]);
          first = false;
        }
      }
      BUF_PRINTF("}");
    }
    J_COMMA();
    BUF_PRINTF("task_lists");
    J_COLON();
    J_OBJ_START();
    if (!dag_.task_list_.is_empty() || !dag_.waiting_task_list_.is_empty()) {
      ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();
      J_NEWLINE();
      // task_list_name|task_ptr|task_type|task_status|task_priority|dag_ptr
      BUF_PRINTF("[%ld][%s][T%ld] [", GETTID(), GETTNAME(), GET_TENANT_ID());
      BUF_PRINTO(PC(trace_id));
      BUF_PRINTF("] ");
      BUF_PRINTF(" %-10s %-16s %-5s %-16s %-10s %-10s %-10s %-50s \n",
          "list", "ptr", "type", "type_str", "status", "priority", "indegree", "debug_info");
      bool is_print = false;
      print_task_list(dag_.task_list_, "READY", buf, buf_len, pos, is_print);
      print_task_list(dag_.waiting_task_list_, "WAITING", buf, buf_len, pos, is_print);
    } else {
      J_EMPTY_OBJ();
    }
    J_OBJ_END();
    J_OBJ_END();
  }
  return pos;
}

void ObPrintIndependentDag::print_task_list(
    common::ObDList<ObITask> &task_list,
    const char* task_list_name,
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    bool &is_print) const
{
  int64_t i = 0;
  ObITask *cur = task_list.get_first();
  const ObITask *head = task_list.get_header();
  while (head != cur && nullptr != cur && i < MAX_PRINT_TASK_CNT) {
    if (is_print && 0 == i) {
      J_NEWLINE();
    }
    task_to_string(cur, i == 0 ? task_list_name : " ", buf, buf_len, pos);
    if (i < task_list.get_size() - 1) {
      J_NEWLINE();
    }
    i++;
    cur = cur->get_next();
  }
  if (task_list.get_size() > 0) {
    is_print = true;
  }
}

void ObPrintIndependentDag::task_to_string(
     ObITask *task,
     const char* task_list_name,
     char *buf,
     const int64_t buf_len,
     int64_t &pos) const
{
  if (OB_NOT_NULL(task)) {
    ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();
    const char* task_status_str = ObITask::get_task_status_str(task->get_status());
    BUF_PRINTF("[%ld][%s][T%ld] [", GETTID(), GETTNAME(), GET_TENANT_ID());
    BUF_PRINTO(PC(trace_id));
    BUF_PRINTF("] ");
    BUF_PRINTF(" %-10s %-16p %-5d %-16s %-10s %-10d %-10ld ",
      task_list_name,
      task,
      task->get_type(),
      ObITask::ObITaskTypeStr[task->get_type()],
      task_status_str,
      task->get_priority(),
      task->get_indegree());
    task->task_debug_info_to_string(buf, buf_len, pos);
  }
}

} //namespace share
} //namespace oceanbase
