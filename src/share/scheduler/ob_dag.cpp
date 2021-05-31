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

#include "share/scheduler/ob_dag.h"
#include "share/scheduler/ob_tenant_thread_pool.h"
#include "share/rc/ob_context.h"
#include "lib/profile/ob_trace_id.h"

namespace oceanbase {
using namespace lib;
using namespace common;
using namespace omt;
namespace share {

ObITaskNew::ObITaskNew(const int64_t type_id)
    : dag_(NULL),
      type_id_(type_id),
      status_(ObITaskNew::TASK_STATUS_INITING),
      indegree_(0),
      last_visit_child_(0),
      color_(ObITaskNewColor::BLACK),
      switch_start_timestamp_(0),
      next_task_(NULL),
      worker_(NULL)
{}

ObITaskNew::~ObITaskNew()
{
  reset();
}

void ObITaskNew::reset()
{
  children_.reset();
  next_task_ = NULL;
  if (OB_NOT_NULL(worker_)) {
    if (OB_ISNULL(dag_)) {
      COMMON_LOG(WARN, "have worker but dag is NULL", K(this), K_(worker));
    } else if (OB_ISNULL(dag_->get_tenant_thread_pool())) {
      COMMON_LOG(WARN, "have worker but dag is NULL", K(this), K_(worker), K_(dag));
    } else {
      dag_->get_tenant_thread_pool()->return_worker(this);  // return worker
    }
  }
  type_id_ = 0;
  status_ = ObITaskNewStatus::TASK_STATUS_INITING;
  indegree_ = 0;
  last_visit_child_ = 0;
  color_ = ObITaskNewColor::BLACK;
  switch_start_timestamp_ = 0;
  dag_ = NULL;
}

bool ObITaskNew::is_valid() const
{
  bool bret = false;
  if (type_id_ >= 0 && OB_NOT_NULL(dag_)) {
    bret = true;
  }
  return bret;
}

int ObITaskNew::do_work()
{
  int ret = OB_SUCCESS;
  bool is_cancel = false;
  ObWorker::CompatMode compat_mode = ObWorker::CompatMode::INVALID;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "task is invalid", K(ret), K_(type_id), KP_(dag));
  } else if (OB_ISNULL(dag_) || OB_ISNULL(worker_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag or worker is null", K(ret), K(this));
  } else {
    ObCurTraceId::set(dag_->get_dag_id());
    if (OB_UNLIKELY(ObWorker::CompatMode::INVALID ==
                    (compat_mode = static_cast<ObWorker::CompatMode>(dag_->get_compat_mode())))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "invalid compat mode", K(ret), K(*dag_));
    } else {
      THIS_WORKER.set_compatibility_mode(compat_mode);
    }
    const ObDagId& dag_id = dag_->get_dag_id();
    const int64_t start_time = ObTimeUtility::current_time();  // start time
    COMMON_LOG(INFO, "task start process", K(start_time), K(*this));
    if (OB_FAIL(SYS_TASK_STATUS_MGR.is_task_cancel(dag_id, is_cancel))) {
      STORAGE_LOG(ERROR, "failed to check is task canceled", K(ret), K(*this));
    } else if (is_cancel) {  // canceled
      ret = OB_CANCELED;
      COMMON_LOG(WARN, "task is canceled", K(ret), K(dag_id), K_(*dag));
    } else if (OB_FAIL(process())) {  // process work
      COMMON_LOG(WARN, "failed to process task", K(ret));
    }
    const int64_t end_time = ObTimeUtility::current_time();  // end time
    COMMON_LOG(INFO,
        "task finish process",
        K(ret),
        K(this),
        K(start_time),
        K(end_time),
        "runtime",
        end_time - start_time,
        K(*this));
    if (OB_FAIL(ret)) {
      dag_->set_dag_status(ObIDagNew::DAG_STATUS_NODE_FAILED);
      dag_->set_dag_ret(ret);
    }
    ObCurTraceId::reset();
    if (OB_FAIL(finish_task())) {
      COMMON_LOG(WARN, "finish task failed", K(ret), K(*this));
    }
  }
  return ret;
}

int ObITaskNew::finish_task()
{
  int ret = OB_SUCCESS;
  // call finish_task func to finish this task
  if (OB_ISNULL(dag_) || OB_ISNULL(dag_->get_tenant_thread_pool()) || OB_ISNULL(worker_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "task don't have dag or tenant thread pool", K(ret), K(dag_), K(*this));
  } else if (OB_FAIL(dag_->get_tenant_thread_pool()->finish_task(this, worker_))) {  // finish task
    COMMON_LOG(WARN, "finish task failed", K(ret), K(*this), K(worker_));
  } else {
    COMMON_LOG(INFO, "call func to finish task success", K(ret));
  }
  return ret;
}

int ObITaskNew::generate_next_task()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObITaskNew* next_task = NULL;  // generate task and put into next_task
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "task is invalid", K(ret), K_(type_id), KP_(dag));
  } else {
    if (OB_SUCCESS != (tmp_ret = generate_next_task(next_task))) {
      if (OB_ITER_END != tmp_ret) {
        ret = tmp_ret;
        COMMON_LOG(WARN, "failed to generate_next_task");
      }
    } else if (OB_ISNULL(next_task) || OB_ISNULL(dag_)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "next_task or dag_ is null", K(ret));
    } else if (OB_FAIL(copy_dep_to(*next_task))) {
      COMMON_LOG(WARN, "failed to copy dependency to new task", K(ret));
    } else if (OB_FAIL(dag_->add_task(*next_task))) {  // add into dag
      COMMON_LOG(WARN, "failed to add next task", K(ret), K(*next_task));
    }
    if (dag_ && OB_FAIL(ret)) {
      dag_->set_dag_status(ObIDagNew::DAG_STATUS_NODE_FAILED);
      dag_->set_dag_ret(ret);
    }
  }
  return ret;
}

int ObITaskNew::add_child(ObITaskNew& child)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "task is invalid", K(ret), K_(type_id), KP_(dag));
  } else if (this == &child) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "can not add self loop", K(ret));
  } else {
    {
      ObMutexGuard guard(lock_);
      if (OB_FAIL(children_.push_back(&child))) {  // add child into children list
        COMMON_LOG(WARN, "failed to add child", K(child));
      }
    }
    if (OB_SUCC(ret)) {
      child.inc_indegree();
    }
  }
  return ret;
}
// copy all the children
int ObITaskNew::copy_dep_to(ObITaskNew& other_task) const
{
  // copy dependency of this task to other_task
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "task is invalid", K(ret), K_(type_id), KP_(dag));
  } else if (this == &other_task) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "can not copy to self", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < children_.count(); ++i) {
      if (OB_FAIL(other_task.add_child(*children_.at(i)))) {
        COMMON_LOG(WARN, "failed to copy dependency to task", K(ret), K(i));
      }
    }
  }
  return ret;
}

void ObITaskNew::prepare_check_cycle()
{
  if (ObITaskNewStatus::TASK_STATUS_INITING != status_) {
    color_ = ObITaskNewColor::WHITE;
    last_visit_child_ = 0;
  } else {
    color_ = ObITaskNewColor::BLACK;
  }
}

bool ObITaskNew::judge_delay_penalty()
{
  int bret = false;
  if (0 == switch_start_timestamp_) {
    COMMON_LOG(WARN, "task with no switch_start_timestamp_", K(this));
  } else if (ObTimeUtility::current_time() - switch_start_timestamp_ > SWITCH_TIME_UPLIMIT) {  // exceed the upper limit
    bret = true;                                                                               // execute_penalty
    switch_start_timestamp_ = 0;  // reset switch_start_timestamp_
  }
  return bret;
}

void ObITaskNew::set_status(const ObITaskNewStatus status)
{
  if (OB_NOT_NULL(dag_)) {
    ObMutexGuard guard(lock_);
    status_ = status;
  } else {
    COMMON_LOG(WARN, "set status task with no dag", K(this), K(status));
  }
}

void ObITaskNew::set_next_task(ObITaskNew* next_task)
{
  if (OB_NOT_NULL(dag_)) {
    ObMutexGuard guard(lock_);
    next_task_ = next_task;
  } else {
    COMMON_LOG(WARN, "set next task with no dag", K(this), K(next_task));
  }
}

ObITaskNew* ObITaskNew::get_next_task()
{
  ObITaskNew* task = NULL;
  if (OB_NOT_NULL(dag_)) {
    ObMutexGuard guard(lock_);
    task = next_task_;
  } else {
    COMMON_LOG(WARN, "get next task with no dag", K(this), K_(next_task));
  }
  return task;
}

/********************************************ObIDagNew impl******************************************/

ObIDagNew::ObIDagNew(int64_t type_id, ObIDagNewPriority priority)
    : dag_ret_(OB_SUCCESS),
      is_inited_(false),
      priority_(priority),
      type_id_(type_id),
      dag_status_(ObIDagNew::DAG_STATUS_INITING),
      running_task_cnt_(0),
      start_time_(0),
      tenant_thread_pool_(NULL)
{}

ObIDagNew::~ObIDagNew()
{
  reset();
}

int ObIDagNew::init(const int64_t total, const int64_t hold, const int64_t page_size)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "dag init twice", K(ret));
  } else if (OB_FAIL(allocator_.init(total, hold, page_size))) {  // init allocator
    COMMON_LOG(WARN, "failed to init allocator", K(ret), K(total), K(hold), K(page_size));
  } else {
    allocator_.set_label(ObModIds::OB_SCHEDULER);
    is_inited_ = true;
  }
  if (!is_inited_) {
    COMMON_LOG(WARN, "failed to init ObIDagNew", K(ret));
  } else {
    COMMON_LOG(INFO, "ObIDagNew is inited", K(ret));
  }
  return ret;
}

void ObIDagNew::reset()
{
  if (is_inited_) {
    ObITaskNew* cur = task_list_.get_first();
    ObITaskNew* next = NULL;
    const ObITaskNew* head = task_list_.get_header();
    while (NULL != cur && head != cur) {  // clear all task in tasl_list_
      next = cur->get_next();
      cur->~ObITaskNew();
      COMMON_LOG(INFO, "~ObITaskNew", K(cur));
      allocator_.free(cur);
      cur = next;
    }
    task_list_.reset();

    dag_ret_ = OB_SUCCESS;
    is_inited_ = false;
    priority_ = ObIDagNewPriority::DAG_PRIO_MAX;
    type_id_ = 0;
    id_.reset();
    dag_status_ = ObDagStatus::DAG_STATUS_INITING;
    running_task_cnt_ = 0;
    start_time_ = 0;
    tenant_thread_pool_ = NULL;
    allocator_.destroy();
    COMMON_LOG(INFO, "ObIDagNew reset success", K(this));
  }
}

int ObIDagNew::add_task(ObITaskNew& task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {  // not inited
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "dag is not inited", K(ret));
  } else {
    ObMutexGuard guard(lock_);
    task.set_status_(ObITaskNew::TASK_STATUS_WAITING);
    if (OB_FAIL(check_cycle_())) {  // check whether is cycle
      COMMON_LOG(WARN, "check_cycle failed", K(ret), K_(id));
      if (OB_ISNULL(task_list_.remove(&task))) {
        COMMON_LOG(WARN, "failed to remove task from task_list", K_(id));
      }
    } else {
      task.set_dag(*this);
      COMMON_LOG(INFO, "add task success", K(&task));
    }
  }
  return ret;
}

int ObIDagNew::check_cycle_()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITaskNew*, DEFAULT_TASK_NUM> stack;
  ObITaskNew* cur_task = task_list_.get_first();
  const ObITaskNew* head = task_list_.get_header();

  // at the beginning of cycle detection, reset everyone's color and last_visit_child
  while (NULL != cur_task && head != cur_task) {
    cur_task->prepare_check_cycle();
    cur_task = cur_task->get_next();
  }
  cur_task = task_list_.get_first();
  // make sure every task in the dag has visited
  while (OB_SUCC(ret) && NULL != cur_task && head != cur_task) {
    if (ObITaskNew::GRAY == cur_task->get_color()) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "color can not be gray here", K(ret), K(*cur_task));
    } else if (ObITaskNew::WHITE == cur_task->get_color()) {
      // start dfs, gray means this task is currently on the stack
      // if you meet a gray task while traversing the graph, then you got a cycle
      cur_task->set_color(ObITaskNew::GRAY);
      if (OB_FAIL(stack.push_back(cur_task))) {
        COMMON_LOG(WARN, "failed to push back stack", K(ret));
      }
      while (OB_SUCC(ret) && !stack.empty()) {
        ObITaskNew* pop_task = stack.at(stack.count() - 1);  // get the last task to pop
        int64_t child_idx = pop_task->get_last_visit_child();
        const ObIArray<ObITaskNew*>& children = pop_task->get_child_tasks();
        bool has_push = false;
        while (OB_SUCC(ret) && !has_push && child_idx < children.count()) {
          ObITaskNew* child_task = children.at(child_idx);
          if (OB_ISNULL(child_task)) {
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(WARN, "child task is null", K(ret));
          } else if (ObITaskNew::GRAY == child_task->get_color()) {
            ret = OB_INVALID_ARGUMENT;
            COMMON_LOG(WARN, "this dag has cycle", K(ret), K(child_task));
          } else if (ObITaskNew::WHITE == child_task->get_color()) {  // not visited
            child_task->set_color(ObITaskNew::GRAY);
            pop_task->set_last_visit_child(child_idx + 1);  // update the last visit child
            if (OB_FAIL(stack.push_back(child_task))) {
              COMMON_LOG(WARN, "failed to push back stack", K(ret));
            } else {
              has_push = true;
            }
          } else {
            ++child_idx;
          }
        }
        if (OB_SUCC(ret) && !has_push) {
          pop_task->set_color(ObITaskNew::BLACK);
          stack.pop_back();
        }
      }
    }
    cur_task = cur_task->get_next();
  }
  return ret;
}

bool ObIDagNew::has_finished()
{
  bool bret = false;
  ObMutexGuard guard(lock_);
  if (ObIDagNew::DAG_STATUS_NODE_RUNNING == dag_status_) {
    bret = task_list_.is_empty() && 0 == running_task_cnt_;  // no task
  } else {
    bret = 0 == running_task_cnt_;
  }

  if (!bret) {
    ObITaskNew* cur_task = task_list_.get_first();
    const ObITaskNew* head = task_list_.get_header();
    ObITaskNew::ObITaskNewStatus status = ObITaskNew::TASK_STATUS_MAX;
  }

  COMMON_LOG(INFO, "ObIDagNew::has_finished", K(bret), K_(running_task_cnt), K_(dag_status), K(task_list_.get_size()));
  return bret;
}
// get next ready task and update *task
int ObIDagNew::get_next_ready_task(const GetTaskFlag flag, ObITaskNew*& task)
{
  int ret = OB_SUCCESS;
  bool found = false;
  bool call_generate_flag = false;
  {
    ObMutexGuard guard(lock_);                                // lock for task_list_
    if (ObIDagNew::DAG_STATUS_NODE_RUNNING == dag_status_) {  // dag is running
      ObITaskNew* cur_task = task_list_.get_first();
      const ObITaskNew* head = task_list_.get_header();
      ObITaskNew::ObITaskNewStatus status = ObITaskNew::TASK_STATUS_MAX;
      while (!found && NULL != cur_task && head != cur_task) {  // loop the task_list_
        status = cur_task->get_status();
        if (0 == cur_task->get_indegree() &&
            ObITaskNew::TASK_STATUS_WAIT_TO_SWITCH != status) {  // not be chosen this time
          if (ObITaskNew::TASK_STATUS_WAITING == status) {       // ready task
            found = true;
            task = cur_task;
            inc_running_task_cnt();
            call_generate_flag = true;  // need to call generate_next_task
            COMMON_LOG(INFO, "get next ready task", K(ret), K(flag), K(task), K(task->get_status()));
          } else if (ObITaskNew::TASK_STATUS_RUNNABLE == status  // have be choosen but not run
                     || (RUNNABLE == flag && ObITaskNew::TASK_STATUS_RUNNING == status)) {  // is running
            found = true;
            task = cur_task;
            COMMON_LOG(INFO, "get_next_ready_task", K(ret), K(flag), K(task), K(task->get_status()));
          } else {
            cur_task = cur_task->get_next();
          }
        } else {
          cur_task = cur_task->get_next();
        }
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ITER_END;
    } else {
      task->set_status_(ObITaskNew::TASK_STATUS_WAIT_TO_SWITCH);  // set status to avoid be selected again this schedule
    }
  }  // end of lock
  if (found && call_generate_flag) {
    task->generate_next_task();  // generate next task
  }
  if (task) {
    COMMON_LOG(INFO, "get_next_ready_task", K(ret), K(flag), K(task), K(task->get_status()));
  }
  return ret;
}

int ObIDagNew::finish_task(ObITaskNew& task)
{
  int ret = OB_SUCCESS;
  // remove finished task from task list and update indegree
  {
    ObMutexGuard guard(lock_);                  // lock for task_list_
    if (OB_ISNULL(task_list_.remove(&task))) {  // remove from dag's task_list
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "failed to remove finished task from task_list", K(ret));
    } else {
      dec_running_task_cnt();
    }
    free_task(task);  // call destructor free task
  }
  return ret;
}

void ObIDagNew::dec_children_indegree(ObITaskNew* task)
{
  if (OB_ISNULL(task)) {
    COMMON_LOG(WARN, "decrease children indegree with null task", K(task));
  } else if (ObIDagNew::DAG_STATUS_NODE_RUNNING == dag_status_) {  // dag is still running
    const ObIArray<ObITaskNew*>& children = task->get_child_tasks();
    for (int64_t i = 0; i < children.count(); ++i) {
      children.at(i)->dec_indegree();  // decrease children's indegree
    }
  }
}

void ObIDagNew::free_task(ObITaskNew& task)
{
  if (IS_NOT_INIT) {
    COMMON_LOG(WARN, "dag is not inited");
  } else {
    task.~ObITaskNew();
    COMMON_LOG(INFO, "~free_task", K(task));
    allocator_.free(&task);
  }
}

bool ObIDagNew::is_valid()
{
  return is_inited_ && !task_list_.is_empty() && OB_SUCCESS == check_cycle_() && is_valid_type();
}

bool ObIDagNew::is_valid_type() const
{
  return type_id_ >= 0;
}

int ObIDagNew::set_dag_id(const ObDagId& dag_id)
{
  int ret = OB_SUCCESS;
  if (dag_id.is_invalid()) {
    ret = OB_INVALID_ERROR;
    COMMON_LOG(WARN, "dag id invalid", K(ret));
  } else if (id_.is_invalid()) {
    id_ = dag_id;
  } else if (id_.equals(dag_id)) {
    // do nothing
  } else {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "dag id set twice", K(ret));
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
