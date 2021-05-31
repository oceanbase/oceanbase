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

#define USING_LOG_PREFIX COMMON
#include "lib/thread/ob_thread_name.h"
#include "share/ob_force_print_log.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "share/rc/ob_context.h"
#include "observer/omt/ob_tenant.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/config/ob_server_config.h"
#include "storage/ob_dag_warning_history_mgr.h"
#include <sys/sysinfo.h>
#include <algorithm>

namespace oceanbase {
using namespace lib;
using namespace common;
using namespace omt;
namespace share {

ObITask::ObITask(ObITask::ObITaskType type)
    : dag_(NULL),
      type_(type),
      status_(ObITask::TASK_STATUS_INITING),
      indegree_(0),
      last_visit_child_(0),
      color_(ObITaskColor::BLACK)
{}

ObITask::~ObITask()
{
  reset();
}

void ObITask::reset()
{
  children_.reset();
  indegree_ = 0;
  last_visit_child_ = 0;
  color_ = ObITaskColor::BLACK;
  dag_ = NULL;
  status_ = ObITaskStatus::TASK_STATUS_INITING;
  type_ = ObITaskType::TASK_TYPE_MAX;
}

bool ObITask::is_valid() const
{
  bool bret = false;
  if (type_ < TASK_TYPE_MAX && NULL != dag_) {
    bret = true;
  }
  return bret;
}

int ObITask::do_work()
{
  int ret = OB_SUCCESS;
  bool is_cancel = false;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "task is invalid", K(ret), K_(type), KP_(dag));
  } else {
    const ObDagId& dag_id = dag_->get_dag_id();
    const int64_t start_time = ObTimeUtility::current_time();
    COMMON_LOG(DEBUG, "task start process", K(start_time), K(*this));
    if (OB_FAIL(SYS_TASK_STATUS_MGR.is_task_cancel(dag_id, is_cancel))) {
      STORAGE_LOG(WARN, "failed to check is task canceled", K(ret), K(*this));
    }
    if (OB_SUCC(ret)) {
      if (is_cancel) {
        ret = OB_CANCELED;
        COMMON_LOG(WARN, "task is canceled", K(ret), K(dag_id), K_(*dag));
      }
    } else {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret) && OB_FAIL(process())) {
      COMMON_LOG(WARN, "failed to process task", K(ret));
    }
    const int64_t end_time = ObTimeUtility::current_time();
    COMMON_LOG(
        INFO, "task finish process", K(ret), K(start_time), K(end_time), "runtime", end_time - start_time, K(*this));
  }
  return ret;
}

int ObITask::generate_next_task()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObITask* next_task = NULL;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "task is invalid", K(ret), K_(type), KP_(dag));
  } else {
    if (OB_SUCCESS != (tmp_ret = generate_next_task(next_task))) {
      if (OB_ITER_END != tmp_ret) {
        ret = tmp_ret;
        COMMON_LOG(WARN, "failed to generate_next_task");
      }
    } else if (OB_ISNULL(next_task)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "next_task is null", K(ret));
    } else if (OB_FAIL(copy_dep_to(*next_task))) {
      COMMON_LOG(WARN, "failed to copy dependency to new task", K(ret));
    } else if (OB_FAIL(dag_->add_task(*next_task))) {
      COMMON_LOG(WARN, "failed to add next task", K(ret), K(*next_task));
    }
    if (OB_FAIL(ret)) {
      dag_->set_dag_status(ObIDag::DAG_STATUS_NODE_FAILED);
      dag_->set_dag_ret(ret);
    }
  }
  return ret;
}

int ObITask::add_child(ObITask& child)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "task is invalid", K(ret), K_(type), KP_(dag));
  } else if (this == &child) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "can not add self loop", K(ret));
  } else {
    {
      ObIDag::ObDagGuard guard(*dag_);
      if (OB_FAIL(children_.push_back(&child))) {
        COMMON_LOG(WARN, "failed to add child", K(child));
      }
    }
    if (OB_SUCC(ret)) {
      child.inc_indegree();
    }
  }
  return ret;
}

int ObITask::copy_dep_to(ObITask& other_task) const
{
  // copy dependency of this task to other_task
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "task is invalid", K(ret), K_(type), KP_(dag));
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

void ObITask::prepare_check_cycle()
{
  if (ObITaskStatus::TASK_STATUS_INITING != status_) {
    color_ = ObITaskColor::WHITE;
    last_visit_child_ = 0;
  } else {
    color_ = ObITaskColor::BLACK;
  }
}

/********************************************ObIDag impl******************************************/

const common::ObString ObIDag::ObIDagPriorityStr[ObIDag::DAG_PRIO_MAX] = {"DAG_PRIO_TRANS_TABLE_MERGE",
    "DAG_PRIO_SSTABLE_MINI_MERGE",
    "DAG_PRIO_SSTABLE_MINOR_MERGE",
    "DAG_PRIO_GROUP_MIGRATE",
    "DAG_PRIO_MIGRATE_HIGH",
    "DAG_PRIO_MIGRATE_MID",
    "DAG_PRIO_SSTABLE_MAJOR_MERGE",
    "DAG_PRIO_BACKUP",
    "DAG_PRIO_MIGRATE_LOW",
    "DAG_PRIO_CREATE_INDEX",
    "DAG_PRIO_SSTABLE_SPLIT",
    "DAG_PRIO_VALIDATE"};

const common::ObString ObIDag::ObIDagUpLimitTypeStr[ObIDag::DAG_ULT_MAX] = {
    "DAG_ULT_MINI_MERGE",
    "DAG_ULT_MINOR_MERGE",
    "DAG_ULT_GROUP_MIGRATE",
    "DAG_ULT_MIGRATE",
    "DAG_ULT_MAJOR_MERGE",
    "DAG_ULT_CREATE_INDEX",
    "DAG_ULT_SPLIT",
    "DAG_ULT_BACKUP",
};

const char* ObIDag::ObIDagTypeStr[ObIDag::DAG_TYPE_MAX] = {"DAG_UT",
    "DAG_MINOR_MERGE",
    "DAG_MAJOR_MERGE",
    "DAG_CREATE_INDEX",
    "DAG_SSTABLE_SPLIT",
    "DAG_UNIQUE_CHECKING",
    "DAG_MIGRATE",
    "DAG_MAJOR_FINISH",
    "DAG_GROUP_MIGRATE",
    "DAG_BUILD_INDEX",
    "DAG_MINI_MERGE",
    "DAG_TRANS_MERGE",
    "DAG_RECOVERY_SPLIT",
    "DAG_RECOVERY_RECOVER",
    "DAG_TYPE_BACKUP",
    "DAG_SERVER_PREPROCESS",
    "DAG_FAST_RECOVERY"
    "DAG_TYPE_VALIDATE"};

ObIDag::ObIDag(ObIDagType type, ObIDagPriority priority)
    : dag_ret_(OB_SUCCESS),
      is_inited_(false),
      priority_(priority),
      type_(type),
      dag_status_(ObIDag::DAG_STATUS_INITING),
      running_task_cnt_(0),
      start_time_(0),
      is_stop_(false)
{}

ObIDag::~ObIDag()
{
  reset();
}

void ObIDag::reset()
{
  ObITask* cur = task_list_.get_first();
  ObITask* next = NULL;

  while (NULL != cur && task_list_.get_header() != cur) {
    next = cur->get_next();
    cur->~ObITask();
    allocator_.free(cur);
    cur = next;
  }
  task_list_.reset();
  start_time_ = 0;
  running_task_cnt_ = 0;
  dag_status_ = ObDagStatus::DAG_STATUS_INITING;
  dag_ret_ = OB_SUCCESS;
  id_.reset();
  type_ = ObIDagType::DAG_TYPE_MAX;
  priority_ = ObIDagPriority::DAG_PRIO_MAX;
  is_stop_ = false;
  is_inited_ = false;
}

int ObIDag::add_task(ObITask& task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "dag is not inited", K(ret));
  } else {
    ObMutexGuard guard(lock_);
    task.set_status(ObITask::TASK_STATUS_WAITING);
    if (OB_FAIL(check_cycle())) {
      COMMON_LOG(WARN, "check_cycle failed", K(ret), K_(id));
      if (OB_ISNULL(task_list_.remove(&task))) {
        COMMON_LOG(WARN, "failed to remove task from task_list", K_(id));
      }
    }
  }
  return ret;
}

int ObIDag::check_cycle()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITask*, DEFAULT_TASK_NUM> stack;
  ObITask* cur_task = task_list_.get_first();
  const ObITask* head = task_list_.get_header();

  // at the beginning of cycle detection, reset everyone's color and last_visit_child
  while (NULL != cur_task && head != cur_task) {
    cur_task->prepare_check_cycle();
    cur_task = cur_task->get_next();
  }
  cur_task = task_list_.get_first();
  // make sure every task in the dag has visited
  while (OB_SUCC(ret) && NULL != cur_task && head != cur_task) {
    if (ObITask::GRAY == cur_task->get_color()) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "color can not be gray here", K(ret), K(*cur_task));
    } else if (ObITask::WHITE == cur_task->get_color()) {
      // start dfs, gray means this task is currently on the stack
      // if you meet a gray task while traversing the graph, then you got a cycle
      cur_task->set_color(ObITask::GRAY);
      if (OB_FAIL(stack.push_back(cur_task))) {
        COMMON_LOG(WARN, "failed to push back stack", K(ret));
      }
      while (OB_SUCC(ret) && !stack.empty()) {
        ObITask* pop_task = stack.at(stack.count() - 1);
        int64_t child_idx = pop_task->get_last_visit_child();
        const ObIArray<ObITask*>& children = pop_task->get_child_tasks();
        bool has_push = false;
        while (OB_SUCC(ret) && !has_push && child_idx < children.count()) {
          ObITask* child_task = children.at(child_idx);
          if (OB_ISNULL(child_task)) {
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(WARN, "child task is null", K(ret));
          } else if (ObITask::GRAY == child_task->get_color()) {
            ret = OB_INVALID_ARGUMENT;
            COMMON_LOG(WARN, "this dag has cycle", K(ret));
          } else if (ObITask::WHITE == child_task->get_color()) {
            child_task->set_color(ObITask::GRAY);
            pop_task->set_last_visit_child(child_idx + 1);
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
          pop_task->set_color(ObITask::BLACK);
          stack.pop_back();
        }
      }
    }
    cur_task = cur_task->get_next();
  }
  return ret;
}

bool ObIDag::has_finished() const
{
  bool bret = false;
  if (ObIDag::DAG_STATUS_NODE_RUNNING == dag_status_) {
    bret = task_list_.is_empty() && 0 == running_task_cnt_;
  } else {
    bret = 0 == running_task_cnt_;
  }
  return bret;
}

int ObIDag::get_next_ready_task(ObITask*& task)
{
  int ret = OB_SUCCESS;
  bool found = false;

  ObMutexGuard guard(lock_);
  if (ObIDag::DAG_STATUS_NODE_RUNNING == dag_status_) {
    ObITask* cur_task = task_list_.get_first();
    const ObITask* head = task_list_.get_header();
    while (!found && head != cur_task) {
      if (0 == cur_task->get_indegree() && ObITask::TASK_STATUS_WAITING == cur_task->get_status()) {
        found = true;
        cur_task->set_status(ObITask::TASK_STATUS_RUNNING);
        inc_running_task_cnt();
        task = cur_task;
      } else {
        cur_task = cur_task->get_next();
      }
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObIDag::finish_task(ObITask& task, int64_t& available_cnt)
{
  int ret = OB_SUCCESS;
  // remove finished task from task list and update indegree
  {
    ObMutexGuard guard(lock_);
    if (OB_ISNULL(task_list_.remove(&task))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "failed to remove finished task from task_list", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    available_cnt = 0;
    if (ObIDag::DAG_STATUS_NODE_RUNNING == dag_status_) {
      const ObIArray<ObITask*>& children = task.get_child_tasks();
      for (int64_t i = 0; i < children.count(); ++i) {
        if (0 == children.at(i)->dec_indegree()) {
          ++available_cnt;
        }
      }
    }
    free_task(task);
  }
  return ret;
}

void ObIDag::free_task(ObITask& task)
{
  if (IS_NOT_INIT) {
    COMMON_LOG(WARN, "dag is not inited");
  } else {
    task.~ObITask();
    allocator_.free(&task);
  }
}

bool ObIDag::is_valid()
{
  return is_inited_ && !task_list_.is_empty() && OB_SUCCESS == check_cycle() && is_valid_type();
}

bool ObIDag::is_valid_type() const
{
  return type_ >= 0 && type_ < DAG_TYPE_MAX;
}

int ObIDag::init(const int64_t total, const int64_t hold, const int64_t page_size)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "dag init twice", K(ret));
  } else if (OB_FAIL(allocator_.init(total, hold, page_size))) {
    COMMON_LOG(WARN, "failed to init allocator", K(ret), K(total), K(hold), K(page_size));
  } else {
    allocator_.set_label(ObModIds::OB_SCHEDULER);
    is_inited_ = true;
  }
  return ret;
}

int ObIDag::set_dag_id(const ObDagId& dag_id)
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

void ObIDag::restart_task(ObITask& task)
{
  ObMutexGuard guard(lock_);
  dec_running_task_cnt();
  task.set_status(ObITask::TASK_STATUS_WAITING);
}

int64_t ObIDag::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    const int64_t tenant_id = get_tenant_id();
    J_OBJ_START();
    J_KV(KP(this), K_(type), K_(id), K_(dag_ret), K_(dag_status), K_(start_time), K(tenant_id));
    J_OBJ_END();
  }
  return pos;
}

void ObIDag::gene_basic_warning_info(storage::ObDagWarningInfo& info)
{
  info.dag_type_ = type_;
  info.tenant_id_ = get_tenant_id();
  info.gmt_create_ = info.gmt_modified_;
}

void ObIDag::gene_warning_info(storage::ObDagWarningInfo& info)
{
  info.dag_ret_ = dag_ret_;
  info.task_id_ = id_;
  info.gmt_modified_ = ObTimeUtility::current_time();
  fill_comment(info.warning_info_, OB_DAG_WARNING_INFO_LENGTH);
}

/*************************************ObDagWorker***********************************/

__thread ObDagWorker* ObDagWorker::self_ = NULL;

ObDagWorker::ObDagWorker() : task_(NULL), status_(DWS_FREE), check_period_(0), last_check_time_(0), is_inited_(false)
{}

ObDagWorker::~ObDagWorker()
{
  destroy();
}

int ObDagWorker::init(const int64_t check_period)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "dag worker is inited twice", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::DAG_WORKER_COND_WAIT))) {
    COMMON_LOG(WARN, "failed to init cond", K(ret));
  } else if (OB_FAIL(start())) {
    COMMON_LOG(WARN, "failed to start dag worker", K(ret));
  } else {
    check_period_ = check_period;
    is_inited_ = true;
  }
  return ret;
}

void ObDagWorker::destroy()
{
  if (is_inited_) {
    stop_worker();
    wait();
    task_ = NULL;
    status_ = DWS_FREE;
    check_period_ = 0;
    last_check_time_ = 0;
    self_ = NULL;
    is_inited_ = false;
    ThreadPool::destroy();
  }
}

void ObDagWorker::stop_worker()
{
  stop();
  notify(DWS_STOP);
}

void ObDagWorker::notify(DagWorkerStatus status)
{
  ObThreadCondGuard guard(cond_);
  status_ = status;
  cond_.signal();
}

void ObDagWorker::resume()
{
  notify(DWS_RUNNABLE);
}

bool ObDagWorker::need_wake_up() const
{
  return (ObTimeUtility::current_time() - last_check_time_) > check_period_ * 10;
}

void ObDagWorker::run1()
{
  self_ = this;
  int ret = OB_SUCCESS;
  ObIDag* dag = NULL;
  ObWorker::CompatMode compat_mode = ObWorker::CompatMode::INVALID;
  lib::set_thread_name("DAG");
  while (!has_set_stop()) {
    ret = OB_SUCCESS;
    if (DWS_RUNNABLE == status_ && NULL != task_) {
      status_ = DWS_RUNNING;
      last_check_time_ = ObTimeUtility::current_time();
      if (OB_ISNULL(dag = task_->get_dag())) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "dag is null", K(ret), K(task_));
      } else {
        ObCurTraceId::set(dag->get_dag_id());
        lib::set_thread_name(dag->get_name());
        if (OB_UNLIKELY(ObWorker::CompatMode::INVALID ==
                        (compat_mode = static_cast<ObWorker::CompatMode>(dag->get_compat_mode())))) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "invalid compat mode", K(ret), K(*dag));
        } else {
          THIS_WORKER.set_compatibility_mode(compat_mode);
          if (OB_FAIL(task_->do_work())) {
            COMMON_LOG(WARN, "failed to do work", K(ret), K(*task_), K(compat_mode));
          }
        }
      }
      if (OB_FAIL(ret)) {
        dag->set_dag_status(ObIDag::DAG_STATUS_NODE_FAILED);
        dag->set_dag_ret(ret);
      }
      {
        const int64_t curr_time = ObTimeUtility::current_time();
        const int64_t elapsed_time = curr_time - last_check_time_;
        EVENT_ADD(SYS_TIME_MODEL_DB_TIME, elapsed_time);
        EVENT_ADD(SYS_TIME_MODEL_DB_CPU, elapsed_time);
        EVENT_ADD(SYS_TIME_MODEL_BKGD_TIME, elapsed_time);
        EVENT_ADD(SYS_TIME_MODEL_BKGD_CPU, elapsed_time);
      }
      status_ = DWS_FREE;
      if (OB_FAIL(ObDagScheduler::get_instance().finish_task(*task_, *this))) {
        COMMON_LOG(WARN, "failed to finish task", K(ret), K(*task_));
      }
      ObCurTraceId::reset();
      lib::set_thread_name("DAG");
    } else {
      ObThreadCondGuard guard(cond_);
      while (NULL == task_ && DWS_FREE == status_ && !has_set_stop()) {
        cond_.wait(SLEEP_TIME_MS);
      }
    }
  }
}

void ObDagWorker::yield()
{
  static __thread uint64_t counter = 0;
  const static uint64_t CHECK_INTERVAL = (1UL << 12) - 1;
  if (!((++counter) & CHECK_INTERVAL)) {
    int64_t curr_time = ObTimeUtility::current_time();
    if (last_check_time_ + check_period_ <= curr_time) {
      int64_t elapsed_time = curr_time - last_check_time_;
      EVENT_ADD(SYS_TIME_MODEL_DB_TIME, elapsed_time);
      EVENT_ADD(SYS_TIME_MODEL_DB_CPU, elapsed_time);
      EVENT_ADD(SYS_TIME_MODEL_BKGD_TIME, elapsed_time);
      EVENT_ADD(SYS_TIME_MODEL_BKGD_CPU, elapsed_time);
      last_check_time_ = curr_time;
      ObThreadCondGuard guard(cond_);
      if (DWS_RUNNING == status_ && ObDagScheduler::get_instance().try_switch(*this)) {
        status_ = DWS_WAITING;
        while (DWS_WAITING == status_) {
          cond_.wait(SLEEP_TIME_MS);
        }
        ObCurTraceId::set(task_->get_dag()->get_dag_id());
        COMMON_LOG(INFO, "worker continues to run", K(*task_));
        curr_time = ObTimeUtility::current_time();
        elapsed_time = curr_time - last_check_time_;
        EVENT_ADD(SYS_TIME_MODEL_DB_TIME, elapsed_time);
        EVENT_ADD(SYS_TIME_MODEL_BKGD_TIME, elapsed_time);
        last_check_time_ = curr_time;
        if (DWS_RUNNABLE == status_) {
          status_ = DWS_RUNNING;
        }
      }
    }
  }
}

/***************************************ObDagScheduler impl********************************************/

constexpr int32_t ObDagScheduler::DEFAULT_LOW_LIMIT[ObIDag::DAG_PRIO_MAX];
constexpr int32_t ObDagScheduler::DEFAULT_UP_LIMIT[ObIDag::DAG_ULT_MAX];
constexpr ObIDag::ObIDagUpLimitType ObDagScheduler::UP_LIMIT_MAP[ObIDag::DAG_PRIO_MAX];
const int32_t ObDagScheduler::DEFAULT_WORK_THREAD_NUM;
const int32_t ObDagScheduler::DEFAULT_MINOR_MERGE_CONCURRENCY;
const int32_t ObDagScheduler::DEFAULT_MAX_CONCURRENCY;
const int32_t ObDagScheduler::MAX_MIGRATE_THREAD_NUM;
const int32_t ObDagScheduler::MAX_GROUP_MIGRATE_THREAD_NUM;
const int32_t ObDagScheduler::MAX_THREAD_LIMIT;
const int32_t ObDagScheduler::MAX_VALIDATE_THREAD_NUM;

ObDagScheduler::ObDagScheduler()
    : is_inited_(false),
      dag_cnt_(0),
      dag_limit_(0),
      check_period_(0),
      total_worker_cnt_(0),
      work_thread_num_(0),
      default_thread_num_(0),
      thread2reserve_(0),
      total_running_task_cnt_(0),
      load_shedder_()
{}

ObDagScheduler::~ObDagScheduler()
{
  destroy();
}

ObDagScheduler& ObDagScheduler::get_instance()
{
  static ObDagScheduler scheduler;
  return scheduler;
}

int ObDagScheduler::init(const ObAddr& addr, const int64_t check_period /* =DEFAULT_CHECK_PERIOD */,
    const int32_t work_thread_num /* = 0 */, const int64_t dag_limit /*= DEFAULT_MAX_DAG_NUM*/,
    const int64_t total_mem_limit /*= TOTAL_LIMIT*/, const int64_t hold_mem_limit /*= HOLD_LIMIT*/,
    const int64_t page_size /*= PAGE_SIZE*/)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "scheduler init twice", K(ret));
  } else if (!addr.is_valid() || 0 >= dag_limit || 0 > work_thread_num || 0 >= total_mem_limit || 0 >= hold_mem_limit ||
             hold_mem_limit > total_mem_limit || 0 >= page_size) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN,
        "init ObDagScheduler with invalid arguments",
        K(ret),
        K(dag_limit),
        K(work_thread_num),
        K(total_mem_limit),
        K(hold_mem_limit),
        K(page_size));
  } else if (OB_FAIL(allocator_.init(total_mem_limit, hold_mem_limit, page_size))) {
    COMMON_LOG(WARN, "failed to init allocator", K(ret), K(total_mem_limit), K(hold_mem_limit), K(page_size));
  } else if (OB_FAIL(dag_map_.create(dag_limit, ObModIds::OB_HASH_BUCKET_DAG_MAP))) {
    COMMON_LOG(WARN, "failed to create dap map", K(ret), K(dag_limit));
  } else if (OB_FAIL(scheduler_sync_.init(ObWaitEventIds::SCHEDULER_COND_WAIT))) {
    COMMON_LOG(WARN, "failed to init task queue sync", K(ret));
  } else if (OB_FAIL(storage::ObDagWarningHistoryManager::get_instance().init())) {
    COMMON_LOG(WARN, "failed to init dag warning history manager", K(ret));
  } else {
    check_period_ = check_period;
    allocator_.set_label(ObModIds::OB_SCHEDULER);
    for (int64_t i = 0; i < ObIDag::DAG_PRIO_MAX; ++i) {
      low_limits_[i] = DEFAULT_LOW_LIMIT[i];
      thread2reserve_ += low_limits_[i];
      running_task_cnts_[i] = 0;
    }
    MEMSET(dag_cnts_, 0, sizeof(dag_cnts_));
    MEMSET(running_task_cnts_per_ult_, 0, sizeof(running_task_cnts_per_ult_));
    work_thread_num_ = default_thread_num_ = get_default_work_thread_cnt(work_thread_num);
    for (int64_t i = 0; i < ObIDag::DAG_ULT_MAX; ++i) {
      up_limits_[i] = 0 == work_thread_num ? std::min(DEFAULT_UP_LIMIT[i], work_thread_num_)
                                           : std::min(work_thread_num, std::min(DEFAULT_UP_LIMIT[i], work_thread_num_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < work_thread_num_; ++i) {
      if (OB_FAIL(create_worker())) {
        COMMON_LOG(WARN, "failed to create worker", K(ret));
      }
    }
    addr_ = addr;
    dag_limit_ = dag_limit;
    if (OB_FAIL(start())) {
      COMMON_LOG(WARN, "failed to start scheduler");
    } else {
      load_shedder_.refresh_stat();
      is_inited_ = true;
    }
  }
  if (!is_inited_) {
    destroy();
    COMMON_LOG(WARN, "failed to init ObDagScheduler", K(ret));
  } else {
    COMMON_LOG(INFO, "ObDagScheduler is inited", K_(work_thread_num));
  }
  return ret;
}

void ObDagScheduler::destroy()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObDagScheduler starts to destroy");
    stop();
    notify();
    wait();

    destroy_all_workers();

    for (int64_t i = 0; i < PriorityDagList::PRIO_CNT; ++i) {
      const ObIDag* head = ready_dag_list_.get_head(i);
      const ObIDag* cur_dag = head->get_next();
      const ObIDag* next = NULL;
      while (NULL != cur_dag && head != cur_dag) {
        next = cur_dag->get_next();
        cur_dag->~ObIDag();
        allocator_.free((void*)cur_dag);
        cur_dag = next;
      }
    }
    ready_dag_list_.reset();

    if (dag_map_.created()) {
      dag_map_.destroy();
    }

    allocator_.destroy();
    scheduler_sync_.destroy();
    addr_.reset();
    dag_cnt_ = 0;
    dag_limit_ = 0;
    total_worker_cnt_ = 0;
    work_thread_num_ = 0;
    thread2reserve_ = 0;
    total_running_task_cnt_ = 0;
    MEMSET(running_task_cnts_, 0, sizeof(running_task_cnts_));
    MEMSET(running_task_cnts_per_ult_, 0, sizeof(running_task_cnts_per_ult_));
    MEMSET(dag_cnts_, 0, sizeof(dag_cnts_));
    waiting_workers_.reset();
    running_workers_.reset();
    free_workers_.reset();
    load_shedder_.reset();
    is_inited_ = false;
    COMMON_LOG(INFO, "ObDagScheduler destroyed");
  }
}

void ObDagScheduler::free_dag(ObIDag& dag)
{
  dag.~ObIDag();
  allocator_.free(&dag);
}

int32_t ObDagScheduler::get_default_work_thread_cnt(const int32_t work_thread_num) const
{
  int32_t thread_cnt = 0;
  int32_t cpu_cnt = static_cast<int32_t>(get_cpu_num());
  if (0 == work_thread_num) {
    thread_cnt = std::max(
        thread2reserve_, std::min(DEFAULT_WORK_THREAD_NUM, std::max(1, cpu_cnt * WORK_THREAD_CNT_PERCENT / 100)));
  } else {
    thread_cnt = std::max(work_thread_num, thread2reserve_);
  }
  return thread_cnt;
}

int ObDagScheduler::set_mini_merge_concurrency(const int32_t mini_merge_concurrency)
{
  int ret = OB_SUCCESS;
  int32_t min_trans_thread = mini_merge_concurrency == 0 ? 0 : MAX(1, mini_merge_concurrency / 2);
  int32_t min_mini_thread = mini_merge_concurrency == 0 ? 0 : MAX(1, mini_merge_concurrency - min_trans_thread);
  int32_t max_thread = MAX(mini_merge_concurrency, min_mini_thread + min_trans_thread);

  if (OB_FAIL(set_min_thread(ObIDag::DAG_PRIO_SSTABLE_MINI_MERGE, min_mini_thread))) {
    COMMON_LOG(WARN, "failed to set mini merge concurrency", K(ret), K(min_mini_thread), K(mini_merge_concurrency));
  } else if (OB_FAIL(set_min_thread(ObIDag::DAG_PRIO_TRANS_TABLE_MERGE, min_trans_thread))) {
    COMMON_LOG(WARN, "failed to set trans merge concurrency", K(ret), K(min_trans_thread), K(mini_merge_concurrency));
  } else if (OB_FAIL(set_max_thread(ObIDag::DAG_ULT_MINI_MERGE, max_thread))) {
    COMMON_LOG(WARN, "failed to set min merge max thread", K(ret), K(max_thread), K(mini_merge_concurrency));
  } else {
    COMMON_LOG(INFO,
        "set min_merge_concurrency successfully",
        "mini_merge thread low limit",
        low_limits_[ObIDag::DAG_PRIO_SSTABLE_MINI_MERGE],
        "mini_merge thread up limit",
        up_limits_[ObIDag::DAG_ULT_MINI_MERGE],
        K_(work_thread_num),
        K(mini_merge_concurrency));
  }
  return ret;
}

int ObDagScheduler::set_minor_merge_concurrency(const int32_t minor_merge_concurrency)
{
  int ret = OB_SUCCESS;
  int32_t min_thread = minor_merge_concurrency == 0 ? 0 : std::max(1, minor_merge_concurrency / 2);
  if (OB_FAIL(set_min_thread(ObIDag::DAG_PRIO_SSTABLE_MINOR_MERGE, min_thread))) {
    COMMON_LOG(WARN, "failed to set minor merge concurrency", K(ret), K(minor_merge_concurrency));
  } else if (OB_FAIL(set_max_thread(ObIDag::DAG_ULT_MINOR_MERGE, minor_merge_concurrency))) {
    COMMON_LOG(WARN, "failed to set minor merge max thread", K(ret), K(minor_merge_concurrency));
  } else {
    COMMON_LOG(INFO,
        "set minor_merge_concurrency successfully",
        "minor_merge thread low limit",
        low_limits_[ObIDag::DAG_PRIO_SSTABLE_MINOR_MERGE],
        "minor_merge thread up limit",
        up_limits_[ObIDag::DAG_ULT_MINOR_MERGE],
        K_(work_thread_num),
        K(minor_merge_concurrency));
  }
  return ret;
}

int ObDagScheduler::set_major_merge_concurrency(const int32_t major_merge_concurrency)
{
  int ret = OB_SUCCESS;
  int32_t min_thread = major_merge_concurrency == 0 ? 0 : MAX(1, major_merge_concurrency / 2);
  if (OB_FAIL(set_min_thread(ObIDag::DAG_PRIO_SSTABLE_MAJOR_MERGE, min_thread))) {
    COMMON_LOG(WARN, "failed to set_major_merge min thread", K(ret), K(major_merge_concurrency), K(min_thread));
  } else if (OB_FAIL(set_max_thread(ObIDag::DAG_ULT_MAJOR_MERGE, major_merge_concurrency))) {
    COMMON_LOG(WARN, "failed to set_major_merge max thread", K(ret), K(major_merge_concurrency));
  } else {
    COMMON_LOG(INFO,
        "set major_merge_concurrency successfully",
        "major_merge thread low limit",
        low_limits_[ObIDag::DAG_PRIO_SSTABLE_MAJOR_MERGE],
        "major_merge thread up limit",
        up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE],
        K_(work_thread_num),
        K(major_merge_concurrency));
  }
  return ret;
}

int ObDagScheduler::set_create_index_concurrency(const int32_t create_index_concurrency)
{
  int ret = OB_SUCCESS;
  int32_t cpu_cnt = static_cast<int32_t>(get_cpu_num());
  int32_t max_thread = std::min(create_index_concurrency, cpu_cnt);
  int32_t min_thread = create_index_concurrency == 0 ? 0 : std::max(1, max_thread / 2);
  if (OB_FAIL(set_min_thread(ObIDag::DAG_PRIO_CREATE_INDEX, min_thread))) {
    COMMON_LOG(WARN, "failed to set create index min thread", K(ret), K(create_index_concurrency), K(min_thread));
  } else if (OB_FAIL(set_max_thread(ObIDag::DAG_ULT_CREATE_INDEX, max_thread))) {
    COMMON_LOG(WARN, "failed to set create index max thread", K(ret), K(create_index_concurrency));
  } else {
    COMMON_LOG(INFO,
        "set create_index_concurrency successfully",
        "create_index thread low limit",
        low_limits_[ObIDag::DAG_PRIO_CREATE_INDEX],
        "create_index thread up limit",
        up_limits_[ObIDag::DAG_ULT_CREATE_INDEX],
        K_(work_thread_num),
        K(create_index_concurrency));
  }
  return ret;
}

int ObDagScheduler::set_migrate_concurrency(const int32_t migrate_concurrency)
{
  int ret = OB_SUCCESS;
  int32_t max_thread = std::min(MAX_MIGRATE_THREAD_NUM, migrate_concurrency);
  int32_t min_thread = max_thread;
  int32_t min_thread_low_prio = 1;
  int32_t min_thread_mid_prio = 1;
  int32_t min_thread_high_prio = std::max(1, min_thread - min_thread_low_prio - min_thread_mid_prio);

  if (max_thread <= 0) {
    max_thread = 1;
  }

  if (OB_FAIL(set_min_thread(ObIDag::DAG_PRIO_MIGRATE_HIGH, min_thread_high_prio))) {
    COMMON_LOG(
        WARN, "failed to set high prio migrate min thread", K(ret), K(migrate_concurrency), K(min_thread_high_prio));
  } else if (OB_FAIL(set_min_thread(ObIDag::DAG_PRIO_MIGRATE_LOW, min_thread_low_prio))) {
    COMMON_LOG(
        WARN, "failed to set low prio migrate min thread", K(ret), K(migrate_concurrency), K(min_thread_low_prio));
  } else if (OB_FAIL(set_min_thread(ObIDag::DAG_PRIO_MIGRATE_MID, min_thread_mid_prio))) {
    COMMON_LOG(
        WARN, "failed to set mid prio migrate min thread", K(ret), K(migrate_concurrency), K(min_thread_mid_prio));
  } else if (OB_FAIL(set_max_thread(ObIDag::DAG_ULT_MIGRATE, max_thread))) {
    COMMON_LOG(WARN, "failed to set mid prio migrate max thread", K(ret), K(migrate_concurrency), K(max_thread));
  } else {
    COMMON_LOG(INFO,
        "set migrate concurrency successfully",
        "high prio migrate thread low limit",
        low_limits_[ObIDag::DAG_PRIO_MIGRATE_HIGH],
        "mid prio migrate thread low limit",
        low_limits_[ObIDag::DAG_PRIO_MIGRATE_MID],
        "low prio migrate thread low limit",
        low_limits_[ObIDag::DAG_PRIO_MIGRATE_LOW],
        "migrate thread up limit",
        up_limits_[ObIDag::DAG_ULT_MIGRATE],
        K_(work_thread_num),
        K(migrate_concurrency));
  }
  return ret;
}

int ObDagScheduler::set_group_migrate_concurrency(const int32_t migrate_concurrency)
{
  int ret = OB_SUCCESS;
  int32_t max_thread = std::min(MAX_GROUP_MIGRATE_THREAD_NUM, migrate_concurrency);
  int32_t min_thread = max_thread;

  if (max_thread <= 0) {
    max_thread = 1;
  }
  if (OB_FAIL(set_min_thread(ObIDag::DAG_PRIO_GROUP_MIGRATE, min_thread))) {
    COMMON_LOG(WARN, "failed to set group migrate min thread", K(ret), K(migrate_concurrency), K(min_thread));
  } else if (OB_FAIL(set_max_thread(ObIDag::DAG_ULT_GROUP_MIGRATE, max_thread))) {
    COMMON_LOG(WARN, "failed to set group migrate max thread", K(ret), K(migrate_concurrency), K(max_thread));
  } else {
    COMMON_LOG(INFO,
        "set group migrate concurrency successfully",
        "migrate thread low limit",
        low_limits_[ObIDag::DAG_PRIO_GROUP_MIGRATE],
        "migrate thread up limit",
        up_limits_[ObIDag::DAG_ULT_GROUP_MIGRATE],
        K_(work_thread_num),
        K(migrate_concurrency));
  }
  return ret;
}

int ObDagScheduler::set_backup_concurrency(const int32_t backup_concurrency)
{
  int ret = OB_SUCCESS;
  int32_t max_thread = backup_concurrency;
  int32_t min_thread = 0;
  if (max_thread > 0) {
    int32_t mid_value = max_thread / 4;
    min_thread = mid_value > 0 ? mid_value : 1;
  } else {
    min_thread = 0;
  }

  if (OB_FAIL(set_min_thread(ObIDag::DAG_PRIO_BACKUP, min_thread))) {
    COMMON_LOG(WARN, "failed to backup min thread", K(ret), K(backup_concurrency), K(min_thread));
  } else if (OB_FAIL(set_min_thread(ObIDag::DAG_PRIO_VALIDATE, min_thread))) {
    COMMON_LOG(WARN, "failed to set validate min thread", K(ret), K(backup_concurrency), K(min_thread));
  } else if (OB_FAIL(set_max_thread(ObIDag::DAG_ULT_BACKUP, max_thread))) {
    COMMON_LOG(WARN, "failed to set backup max thread", K(ret), K(backup_concurrency), K(max_thread));
  } else {
    COMMON_LOG(INFO,
        "set backup concurrency successfully",
        "backup thread low limit",
        low_limits_[ObIDag::DAG_PRIO_BACKUP],
        "validate thread low limit",
        low_limits_[ObIDag::DAG_PRIO_VALIDATE],
        "backup thread up limit",
        up_limits_[ObIDag::DAG_ULT_BACKUP],
        "validate thread up limit",
        up_limits_[ObIDag::DAG_ULT_BACKUP],
        K_(work_thread_num),
        K(backup_concurrency));
  }
  return ret;
}

int ObDagScheduler::add_dag(ObIDag* dag, const bool emergency)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObDagScheduler is not inited", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arugment", KP(dag));
  } else if (OB_UNLIKELY(!dag->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(*dag));
  } else {
    ObThreadCondGuard guard(scheduler_sync_);
    if (dag_cnts_[dag->get_type()] >= dag_limit_) {
      ret = OB_SIZE_OVERFLOW;
      COMMON_LOG(WARN, "ObDagScheduler is full", K(ret), K_(dag_limit), K(*dag));
    } else if (OB_SUCCESS != (hash_ret = dag_map_.set_refactored(dag, dag))) {
      if (OB_HASH_EXIST == hash_ret) {
        ret = OB_EAGAIN;
      } else {
        ret = hash_ret;
        COMMON_LOG(WARN, "failed to set dag_map", K(ret), K(*dag));
      }
    } else {
      bool add_ret = false;
      if (!emergency) {
        add_ret = ready_dag_list_.add_last(dag, dag->get_priority());
      } else {
        add_ret = ready_dag_list_.add_first(dag, dag->get_priority());
      }
      if (!add_ret) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "failed to add dag to ready_dag_list", K(ret), K(dag->get_priority()), K(*dag));
        if (OB_SUCCESS != (hash_ret = dag_map_.erase_refactored(dag))) {
          COMMON_LOG(ERROR, "failed to erase dag from dag_map", K(hash_ret), K(*dag));
          ob_abort();
        }
      }
      if (OB_SUCC(ret)) {
        ++dag_cnt_;
        ++dag_cnts_[dag->get_type()];
        dag->set_dag_status(ObIDag::DAG_STATUS_READY);
        dag->start_time_ = ObTimeUtility::current_time();
        scheduler_sync_.signal();
        COMMON_LOG(INFO,
            "add dag success",
            KP(dag),
            "start_time",
            dag->start_time_,
            "id",
            dag->id_,
            K(*dag),
            K(dag->hash()),
            K_(dag_cnt),
            "dag_type_cnts",
            dag_cnts_[dag->get_type()]);
      }
    }
  }
  return ret;
}

void ObDagScheduler::dump_dag_status()
{
  if (REACH_TIME_INTERVAL(DUMP_DAG_STATUS_INTERVAL)) {
    int32_t running_task[ObIDag::DAG_PRIO_MAX];
    int32_t running_task_per_ult[ObIDag::DAG_ULT_MAX];
    int32_t low_limits[ObIDag::DAG_PRIO_MAX];
    int32_t up_limits[ObIDag::DAG_ULT_MAX];
    int64_t dag_count[ObIDag::DAG_TYPE_MAX];
    {
      ObThreadCondGuard guard(scheduler_sync_);
      for (int64_t i = 0; i < ObIDag::DAG_PRIO_MAX; ++i) {
        running_task[i] = running_task_cnts_[i];
        low_limits[i] = low_limits_[i];
      }
      for (int64_t i = 0; i < ObIDag::DAG_ULT_MAX; ++i) {
        running_task_per_ult[i] = running_task_cnts_per_ult_[i];
        up_limits[i] = up_limits_[i];
      }
      for (int64_t i = 0; i < ObIDag::DAG_TYPE_MAX; ++i) {
        dag_count[i] = dag_cnts_[i];
      }
      COMMON_LOG(INFO, "dump_dag_status", K_(load_shedder));
    }

    for (int64_t i = 0; i < ObIDag::DAG_PRIO_MAX; ++i) {
      COMMON_LOG(INFO,
          "dump_dag_status",
          "priority",
          ObIDag::ObIDagPriorityStr[i],
          "low_limit",
          low_limits[i],
          "running_task",
          running_task[i]);
    }
    for (int64_t i = 0; i < ObIDag::DAG_ULT_MAX; ++i) {
      COMMON_LOG(INFO,
          "dump_dag_status",
          "up_limit_type",
          ObIDag::ObIDagUpLimitTypeStr[i],
          "up_limit",
          up_limits[i],
          "running_task_per_ult",
          running_task_per_ult[i]);
    }
    for (int64_t i = 0; i < ObIDag::DAG_TYPE_MAX; ++i) {
      COMMON_LOG(INFO, "dump_dag_status", "type", ObIDag::ObIDagTypeStr[i], "dag_count", dag_count[i]);
    }
    COMMON_LOG(INFO, "dump_dag_status", K_(total_worker_cnt), K_(total_running_task_cnt), K_(work_thread_num));
  }
}

int ObDagScheduler::check_need_load_shedding(const int64_t priority, const bool for_schedule, bool& need_shedding)
{
  int ret = OB_SUCCESS;
  const int64_t up_limit_type = UP_LIMIT_MAP[priority];
  const int64_t extra_limit = for_schedule ? 0 : 1;
  need_shedding = false;
  // ensure caller hold the scheduler_sync_
  if (load_shedder_.get_shedding_factor() <= 1) {
    // no need load shedding
  } else if (OB_UNLIKELY(priority < 0 || priority >= ObIDag::DAG_PRIO_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument to check load shedding", K(priority));
  } else if (up_limit_type != ObIDag::DAG_ULT_MAJOR_MERGE) {
  } else {
    const int64_t shedding_factor = MAX(1, load_shedder_.get_shedding_factor());
    const int64_t shedding_low_limit = MAX(1, low_limits_[priority] / shedding_factor);
    const int64_t shedding_up_limit = MAX(shedding_low_limit, up_limits_[up_limit_type] / shedding_factor);
    if (running_task_cnts_[priority] >= (shedding_low_limit + extra_limit) &&
        running_task_cnts_per_ult_[up_limit_type] >= (shedding_up_limit + extra_limit)) {
      need_shedding = true;
      if (REACH_TIME_INTERVAL(DUMP_DAG_STATUS_INTERVAL)) {
        COMMON_LOG(INFO,
            "Dag need to load shedding",
            K_(load_shedder),
            K(shedding_factor),
            K(for_schedule),
            K(shedding_low_limit),
            K(shedding_up_limit),
            "priority",
            ObIDag::ObIDagPriorityStr[priority],
            "up_limit_task_cnt",
            running_task_cnts_per_ult_[up_limit_type],
            "running_task_cnt",
            running_task_cnts_[priority]);
      }
    }
  }

  return ret;
}

void ObDagScheduler::run1()
{
  int ret = OB_SUCCESS;
  lib::set_thread_name("DagScheduler");
  while (!has_set_stop()) {
    dump_dag_status();
    ObThreadCondGuard guard(scheduler_sync_);
    load_shedder_.refresh_stat();
    if (!has_set_stop()) {
      if (OB_FAIL(schedule())) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          try_reclaim_threads();
          scheduler_sync_.wait(SCHEDULER_WAIT_TIME_MS);
        } else {
          COMMON_LOG(WARN, "failed to schedule", K(ret));
        }
      }
    }
  }
}

void ObDagScheduler::notify()
{
  ObThreadCondGuard cond_guard(scheduler_sync_);
  scheduler_sync_.signal();
}

int ObDagScheduler::finish_task(ObITask& task, ObDagWorker& worker)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIDag* dag = NULL;
  if (OB_ISNULL(dag = task.get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (!dag->is_valid_type()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "invalid dag", K(ret), K(*dag));
  } else {
    int64_t new_avail_cnt = 0;
    if (OB_FAIL(dag->finish_task(task, new_avail_cnt))) {
      dag->set_dag_status(ObIDag::DAG_STATUS_NODE_FAILED);
      dag->set_dag_ret(ret);
      COMMON_LOG(WARN, "failed to finish task", K(ret));
    }
    const int64_t prio = dag->get_priority();
    bool free_flag = false;
    {
      ObThreadCondGuard guard(scheduler_sync_);
      dag->dec_running_task_cnt();
      if (dag->has_finished()) {
        dag->set_dag_status(ObIDag::DAG_STATUS_FINISH);
        if (OB_SUCCESS != (tmp_ret = dag_map_.erase_refactored(dag))) {
          COMMON_LOG(ERROR, "failed to erase dag from dag_map", K(tmp_ret), KP(dag), K(*dag));
        }
        if (!ready_dag_list_.remove(dag, prio)) {
          COMMON_LOG(WARN, "failed to remove dag from dag list", K(*dag));
        }
        --dag_cnt_;
        --dag_cnts_[dag->get_type()];
        free_flag = true;
        COMMON_LOG(INFO,
            "dag finished",
            K(*dag),
            "runtime",
            ObTimeUtility::current_time() - dag->start_time_,
            K_(dag_cnt),
            K(dag_cnts_[dag->get_type()]));
        storage::ObDagWarningHistoryManager::get_instance().add_dag_warning_info(dag);  // ignore failure
      }
    }
    if (free_flag) {
      if (OB_SUCCESS != (tmp_ret = ObSysTaskStatMgr::get_instance().del_task(dag->get_dag_id()))) {
        STORAGE_LOG(WARN, "failed to del sys task", K(tmp_ret), K(dag->get_dag_id()));
      }
      free_dag(*dag);
    }
    {
      // free worker after free dag since add_dag may be called in the deconstructor
      // of dag, which may lead to dead lock
      ObThreadCondGuard guard(scheduler_sync_);
      --running_task_cnts_[prio];
      --running_task_cnts_per_ult_[UP_LIMIT_MAP[prio]];
      --total_running_task_cnt_;
      running_workers_.remove(&worker, prio);
      free_workers_.add_last(&worker);
      worker.set_task(NULL);
      scheduler_sync_.signal();
    }
  }
  return ret;
}

int ObDagScheduler::try_switch(ObDagWorker& worker, const int64_t src_prio, const int64_t dest_prio, bool& need_pause)
{
  int ret = OB_SUCCESS;
  need_pause = false;
  if (OB_FAIL(schedule_one(dest_prio))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "failed to schedule one task", K(ret), K(dest_prio));
    }
  }
  ObCurTraceId::set(worker.get_task()->get_dag()->get_dag_id());
  if (OB_SUCC(ret)) {
    need_pause = true;
    pause_worker(worker, src_prio);
  }
  return ret;
}

bool ObDagScheduler::try_switch(ObDagWorker& worker)
{
  bool need_pause = false;
  const int64_t priority = worker.get_task()->get_dag()->get_priority();
  const int64_t up_limit_type = UP_LIMIT_MAP[priority];
  ObThreadCondGuard guard(scheduler_sync_);
  // forbid switching after stop sign has been set, which means running workers won't pause any more
  if (!has_set_stop()) {
    if (total_running_task_cnt_ > work_thread_num_ && running_task_cnts_[priority] > low_limits_[priority]) {
      need_pause = true;
      for (int64_t i = priority + 1; need_pause && i < ObIDag::DAG_PRIO_MAX; ++i) {
        // a lower priority who have execessive threads is a better candidate to stop
        if (running_task_cnts_[i] > low_limits_[i]) {
          need_pause = false;
        }
      }
      if (need_pause) {
        pause_worker(worker, priority);
      }
    } else if (running_task_cnts_[priority] > low_limits_[priority] && total_running_task_cnt_ <= work_thread_num_) {
      for (int64_t i = 0; !need_pause && i < ObIDag::DAG_PRIO_MAX; ++i) {
        const int64_t tmp_ult = UP_LIMIT_MAP[i];
        if (i != priority && running_task_cnts_[i] < low_limits_[i] &&
            (running_task_cnts_per_ult_[tmp_ult] < up_limits_[tmp_ult] ||
                (tmp_ult == up_limit_type && running_task_cnts_per_ult_[tmp_ult] == up_limits_[tmp_ult]))) {
          try_switch(worker, priority, i, need_pause);
        }
      }
      for (int64_t i = 0; !need_pause && i < priority; ++i) {
        const int64_t tmp_ult = UP_LIMIT_MAP[i];
        if (running_task_cnts_per_ult_[tmp_ult] < up_limits_[tmp_ult] ||
            (tmp_ult == up_limit_type && running_task_cnts_per_ult_[tmp_ult] == up_limits_[tmp_ult])) {
          try_switch(worker, priority, i, need_pause);
        }
      }
      // if no switch candidate is found, we still need to pause the worker if max thread
      // limit is exceeded
      if (!need_pause && running_task_cnts_per_ult_[up_limit_type] > up_limits_[up_limit_type]) {
        need_pause = true;
        for (int64_t i = priority + 1; need_pause && i < ObIDag::DAG_PRIO_MAX; ++i) {
          if (up_limit_type == UP_LIMIT_MAP[i]) {
            // a lower priority who have the same UpLimitType and execessive threads
            // is a better candidate to stop
            if (running_task_cnts_[i] > low_limits_[i]) {
              need_pause = false;
            }
          }
        }
        if (need_pause) {
          pause_worker(worker, priority);
        }
      }
    }
    if (!need_pause) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = check_need_load_shedding(priority, false, need_pause))) {
        COMMON_LOG(WARN, "Failed to check need load shedding", K(tmp_ret));
      } else if (need_pause) {
        pause_worker(worker, priority);
      }
    }
    if (!need_pause && !waiting_workers_.is_empty(priority)) {
      if (waiting_workers_.get_first(priority)->need_wake_up()) {
        // schedule_one will schedule the first worker on the waiting list first
        try_switch(worker, priority, priority, need_pause);
      }
    }
  }
  return need_pause;
}

void ObDagScheduler::pause_worker(ObDagWorker& worker, const int64_t priority)
{
  --running_task_cnts_[priority];
  --total_running_task_cnt_;
  --running_task_cnts_per_ult_[UP_LIMIT_MAP[priority]];
  running_workers_.remove(&worker, priority);
  waiting_workers_.add_last(&worker, priority);
  COMMON_LOG(INFO,
      "pause worker",
      K(*worker.get_task()),
      "priority",
      ObIDag::ObIDagPriorityStr[priority],
      K(running_task_cnts_[priority]),
      K(total_running_task_cnt_));
}

int ObDagScheduler::sys_task_start(ObIDag* dag)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret), KP(dag));
  } else if (dag->get_dag_status() != ObIDag::DAG_STATUS_READY) {
    COMMON_LOG(ERROR, " dag status error", K(ret), K(dag->get_dag_status()));
  } else {
    dag->set_dag_status(ObIDag::DAG_STATUS_NODE_RUNNING);
    ObSysTaskStat sys_task_status;

    sys_task_status.start_time_ = ObTimeUtility::current_time();
    sys_task_status.task_id_ = dag->get_dag_id();
    sys_task_status.tenant_id_ = dag->get_tenant_id();

    switch (dag->get_type()) {
      case ObIDag::DAG_TYPE_UT:
        sys_task_status.task_type_ = UT_TASK;
        break;
      case ObIDag::DAG_TYPE_SSTABLE_MINOR_MERGE:
        sys_task_status.task_type_ = SSTABLE_MINOR_MERGE_TASK;
        break;
      case ObIDag::DAG_TYPE_SSTABLE_MINI_MERGE:
        sys_task_status.task_type_ = SSTABLE_MINI_MERGE_TASK;
        break;
      case ObIDag::DAG_TYPE_SSTABLE_MAJOR_MERGE:
        sys_task_status.task_type_ = SSTABLE_MAJOR_MERGE_TASK;
        break;
      case ObIDag::DAG_TYPE_CREATE_INDEX:
      case ObIDag::DAG_TYPE_UNIQUE_CHECKING:
      case ObIDag::DAG_TYPE_SQL_BUILD_INDEX:
        sys_task_status.task_type_ = CREATE_INDEX_TASK;
        break;
      case ObIDag::DAG_TYPE_SSTABLE_SPLIT:
        sys_task_status.task_type_ = PARTITION_SPLIT_TASK;
        break;
      case ObIDag::DAG_TYPE_MIGRATE:
        sys_task_status.task_type_ = PARTITION_MIGRATION_TASK;
        break;
      case ObIDag::DAG_TYPE_GROUP_MIGRATE:
        sys_task_status.task_type_ = GROUP_PARTITION_MIGRATION_TASK;
        break;
      case ObIDag::DAG_TYPE_MAJOR_MERGE_FINISH:
        sys_task_status.task_type_ = MAJOR_MERGE_FINISH_TASK;
        break;
      case ObIDag::DAG_TYPE_TRANS_TABLE_MERGE:
        sys_task_status.task_type_ = TRANS_TABLE_MERGE_TASK;
        break;
      case ObIDag::DAG_TYPE_SERVER_PREPROCESS:
      case ObIDag::DAG_TYPE_FAST_RECOVERY:
        sys_task_status.task_type_ = FAST_RECOVERY_TASK;
        break;
      case ObIDag::DAG_TYPE_BACKUP:
        sys_task_status.task_type_ = PARTITION_BACKUP_TASK;
        break;
      case ObIDag::DAG_TYPE_VALIDATE:
        sys_task_status.task_type_ = BACKUP_VALIDATION_TASK;
        break;
      default:
        COMMON_LOG(ERROR, "sys task type error", K(ret), K(dag->get_type()));
        break;
    }

    // allow comment truncation, no need to set ret
    (void)dag->fill_comment(sys_task_status.comment_, sizeof(sys_task_status.comment_));
    if (OB_SUCCESS != (ret = ObSysTaskStatMgr::get_instance().add_task(sys_task_status))) {
      COMMON_LOG(WARN, "failed to add sys task", K(ret), K(sys_task_status));
    } else if (OB_SUCCESS != (ret = dag->set_dag_id(sys_task_status.task_id_))) {
      COMMON_LOG(WARN, "failed to set dag id", K(ret), K(sys_task_status.task_id_));
    }
  }
  return ret;
}

int64_t ObDagScheduler::get_dag_count(const ObIDag::ObIDagType type) const
{
  int64_t count = -1;
  if (type >= 0 && type < ObIDag::ObIDag::DAG_TYPE_MAX) {
    count = dag_cnts_[type];
  } else {
    COMMON_LOG(ERROR, "invalid type", K(type));
  }
  return count;
}

int ObDagScheduler::pop_task(const int64_t priority, ObITask*& task)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool found = false;
  if (!ready_dag_list_.is_empty(priority)) {
    ObIDag* head = ready_dag_list_.get_head(priority);
    ObIDag* cur = head->get_next();
    ObITask* ready_task = NULL;
    while (!found && head != cur && OB_SUCC(ret)) {
      if (cur->get_dag_status() == ObIDag::DAG_STATUS_READY) {
        if (OB_SUCCESS != (tmp_ret = sys_task_start(cur))) {
          COMMON_LOG(WARN, "failed to start sys task", K(tmp_ret));
        }
      }
      if (OB_SUCCESS != (tmp_ret = cur->get_next_ready_task(ready_task))) {
        if (OB_ITER_END == tmp_ret) {
          cur = cur->get_next();
        } else {
          ret = tmp_ret;
          COMMON_LOG(WARN, "failed to get next ready task", K(ret), K(*cur));
        }
      } else {
        task = ready_task;
        found = true;
      }
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObDagScheduler::schedule_one(const int64_t priority)
{
  int ret = OB_SUCCESS;
  ObDagWorker* worker = NULL;
  ObITask* task = NULL;
  if (!waiting_workers_.is_empty(priority)) {
    worker = waiting_workers_.remove_first(priority);
  } else if (OB_FAIL(pop_task(priority, task))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "failed to pop task", K(ret), K(priority));
    }
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "task is null", K(ret));
  } else {
    ObCurTraceId::set(task->get_dag()->get_dag_id());
    if (OB_FAIL(task->generate_next_task())) {
      COMMON_LOG(WARN, "failed to generate_next_task", K(ret));
    } else if (OB_FAIL(dispatch_task(*task, worker))) {
      task->get_dag()->restart_task(*task);
      COMMON_LOG(WARN, "failed to dispatch task", K(ret));
    }
  }
  if (OB_SUCC(ret) && NULL != worker) {
    ++running_task_cnts_[priority];
    ++total_running_task_cnt_;
    ++running_task_cnts_per_ult_[UP_LIMIT_MAP[priority]];
    running_workers_.add_last(worker, priority);
    if (task != NULL)
      COMMON_LOG(INFO,
          "schedule one task",
          K(*task),
          "priority",
          ObIDag::ObIDagPriorityStr[priority],
          K_(total_running_task_cnt),
          K(running_task_cnts_[priority]),
          K(running_task_cnts_per_ult_[UP_LIMIT_MAP[priority]]),
          K(low_limits_[priority]),
          K(up_limits_[UP_LIMIT_MAP[priority]]));
    worker->resume();
  }
  ObCurTraceId::reset();
  return ret;
}

int ObDagScheduler::schedule()
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  bool need_shedding = false;
  if (total_running_task_cnt_ < work_thread_num_) {
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < ObIDag::DAG_PRIO_MAX; ++i) {
      if (running_task_cnts_[i] < low_limits_[i] &&
          running_task_cnts_per_ult_[UP_LIMIT_MAP[i]] < up_limits_[UP_LIMIT_MAP[i]]) {
        if (OB_FAIL(check_need_load_shedding(i, true, need_shedding))) {
          COMMON_LOG(WARN, "Failed to check need load shedding", K(ret), K(i));
        } else if (!need_shedding) {
          is_found = (OB_SUCCESS == schedule_one(i));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < ObIDag::DAG_PRIO_MAX; ++i) {
      if (running_task_cnts_per_ult_[UP_LIMIT_MAP[i]] < up_limits_[UP_LIMIT_MAP[i]]) {
        if (OB_FAIL(check_need_load_shedding(i, true, need_shedding))) {
          COMMON_LOG(WARN, "Failed to check need load shedding", K(ret), K(i));
        } else if (!need_shedding) {
          is_found = (OB_SUCCESS == schedule_one(i));
        }
      }
    }
  }
  if (!is_found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

OB_INLINE bool ObDagScheduler::has_remain_task(const int64_t priority)
{
  return !waiting_workers_.is_empty(priority) || !ready_dag_list_.is_empty(priority);
}

int ObDagScheduler::dispatch_task(ObITask& task, ObDagWorker*& ret_worker)
{
  int ret = OB_SUCCESS;
  ret_worker = NULL;
  if (free_workers_.is_empty()) {
    if (OB_FAIL(create_worker())) {
      COMMON_LOG(WARN, "failed to create worker", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ret_worker = free_workers_.remove_first();
    ret_worker->set_task(&task);
  }
  return ret;
}

int ObDagScheduler::create_worker()
{
  int ret = OB_SUCCESS;
  ObDagWorker* worker = OB_NEW(ObDagWorker, ObModIds::OB_SCHEDULER);
  if (OB_ISNULL(worker)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "failed to allocate ObDagWorker", K(ret));
  } else if (OB_FAIL(worker->init(check_period_))) {
    COMMON_LOG(WARN, "failed to init worker", K(ret));
  } else if (!free_workers_.add_last(worker)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "failed to add new worker to worker list", K(ret));
  } else {
    ++total_worker_cnt_;
  }
  if (OB_FAIL(ret)) {
    if (NULL != worker) {
      ob_delete(worker);
    }
  }
  return ret;
}

int ObDagScheduler::try_reclaim_threads()
{
  int ret = OB_SUCCESS;
  ObDagWorker* worker2delete = NULL;
  int32_t free_cnt = 0;
  while (total_worker_cnt_ > work_thread_num_ && !free_workers_.is_empty()) {
    worker2delete = free_workers_.remove_first();
    ob_delete(worker2delete);
    --total_worker_cnt_;
    ++free_cnt;
  }
  if (free_cnt > 0) {
    COMMON_LOG(INFO, "reclaim threads", K(free_cnt), K_(total_worker_cnt), K_(work_thread_num));
  }
  return ret;
}

void ObDagScheduler::destroy_all_workers()
{
  {
    // resume all waiting workers
    // all workers will run to complete since switch is forbedden after stop sign is set
    ObThreadCondGuard guard(scheduler_sync_);
    for (int64_t i = 0; i < ObIDag::DAG_PRIO_MAX; ++i) {
      DagList& dl = ready_dag_list_.get_list(i);
      DLIST_FOREACH_NORET(dag, dl)
      {
        dag->set_stop();
      }
    }
    for (int64_t i = 0; i < ObIDag::DAG_PRIO_MAX; ++i) {
      WorkerList& wl = waiting_workers_.get_list(i);
      DLIST_FOREACH_NORET(worker, wl)
      {
        worker->resume();
      }
    }
  }
  // wait all workers finish
  while (total_worker_cnt_ > free_workers_.get_size()) {
    // 100ms
    this_routine::usleep(100 * 1000);
  }
  // we can safely delete all workers here
  DLIST_REMOVE_ALL_NORET(worker, free_workers_)
  {
    ob_delete(worker);
  }
}

int ObDagScheduler::set_min_thread(const int64_t priority, const int32_t concurrency)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObDagScheduler is not inited", K(ret));
  } else if (OB_UNLIKELY(priority < 0 || priority >= ObIDag::DAG_PRIO_MAX || concurrency < 0 ||
                         concurrency > MAX_THREAD_LIMIT)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(priority), K(concurrency));
  } else {
    const int64_t up_limit_type = UP_LIMIT_MAP[priority];
    int32_t min_up_limit = 0;
    ObThreadCondGuard guard(scheduler_sync_);
    int32_t old_min = low_limits_[priority];
    int32_t new_max = 0;
    if (0 == concurrency) {
      low_limits_[priority] = DEFAULT_LOW_LIMIT[priority];
    } else {
      low_limits_[priority] = concurrency;
    }
    for (int64_t i = 0; i < ObIDag::DAG_PRIO_MAX; ++i) {
      if (up_limit_type == UP_LIMIT_MAP[i]) {
        min_up_limit += low_limits_[i];
      }
    }
    if (up_limits_[up_limit_type] < min_up_limit) {
      up_limits_[up_limit_type] = min_up_limit;
    }
    thread2reserve_ += low_limits_[priority] - old_min;
    // find the largest max_thread among all dag types
    for (int64_t i = 0; i < ObIDag::DAG_ULT_MAX; ++i) {
      if (up_limits_[i] > new_max) {
        new_max = up_limits_[i];
      }
    }
    work_thread_num_ = std::max(thread2reserve_, new_max);
    scheduler_sync_.signal();
    COMMON_LOG(INFO,
        "set min thread successfully",
        K(concurrency),
        "priority",
        ObIDag::ObIDagPriorityStr[priority],
        K(low_limits_[priority]),
        K(up_limits_[up_limit_type]),
        K_(work_thread_num),
        K_(thread2reserve),
        K(new_max));
  }
  return ret;
}

int ObDagScheduler::set_max_thread(const int64_t up_limit_type, const int32_t concurrency)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObDagScheduler is not inited", K(ret));
  } else if (OB_UNLIKELY(up_limit_type < 0 || up_limit_type >= ObIDag::DAG_ULT_MAX || concurrency < 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(up_limit_type), K(concurrency));
  } else {
    int32_t low_limit_sum = 0;
    int32_t count = 0;
    ObThreadCondGuard guard(scheduler_sync_);
    int32_t new_max = 0;
    if (0 == concurrency) {
      up_limits_[up_limit_type] = std::min(default_thread_num_, DEFAULT_UP_LIMIT[up_limit_type]);
    } else {
      up_limits_[up_limit_type] = std::min(concurrency, MAX_THREAD_LIMIT);
    }
    for (int64_t i = 0; i < ObIDag::DAG_PRIO_MAX; ++i) {
      if (up_limit_type == UP_LIMIT_MAP[i]) {
        low_limit_sum += low_limits_[i];
        ++count;
      }
    }
    if (up_limits_[up_limit_type] < count) {
      SHARE_LOG(INFO,
          "[DagScheduler] up limit is too small, set to minimum number",
          K(ret),
          K(up_limits_[up_limit_type]),
          K(up_limit_type),
          K(count));
      up_limits_[up_limit_type] = count;
    }
    if (low_limit_sum > up_limits_[up_limit_type]) {
      const int32_t avg_diff = (low_limit_sum - up_limits_[up_limit_type]) / count;
      int32_t total_diff = 0;
      for (int64_t i = 0; i < ObIDag::DAG_PRIO_MAX; ++i) {
        if (up_limit_type == UP_LIMIT_MAP[i]) {
          if (--count > 0) {
            low_limits_[i] -= avg_diff;
            total_diff += avg_diff;
          } else {
            low_limits_[i] -= (low_limit_sum - up_limits_[up_limit_type]) - total_diff;
          }
        }
      }
      thread2reserve_ -= (low_limit_sum - up_limits_[up_limit_type]);
    }
    // find the largest max_thread among all dag types
    for (int64_t i = 0; i < ObIDag::DAG_ULT_MAX; ++i) {
      if (up_limits_[i] > new_max) {
        new_max = up_limits_[i];
      }
    }
    work_thread_num_ = std::max(thread2reserve_, new_max);
    scheduler_sync_.signal();
    COMMON_LOG(INFO,
        "set max thread successfully",
        K(concurrency),
        "up_limit_type",
        ObIDag::ObIDagUpLimitTypeStr[up_limit_type],
        K(up_limits_[up_limit_type]),
        K_(work_thread_num),
        K_(thread2reserve),
        K(new_max));
  }
  return ret;
}

int32_t ObDagScheduler::get_running_task_cnt(const ObIDag::ObIDagPriority priority)
{
  int32_t count = -1;
  if (priority >= 0 && priority < ObIDag::ObIDag::DAG_PRIO_MAX) {
    ObThreadCondGuard guard(scheduler_sync_);
    count = running_task_cnts_[priority];
  } else {
    COMMON_LOG(ERROR, "invalid priority", K(priority));
  }
  return count;
}

int ObDagScheduler::get_up_limit(const int64_t up_limit_type, int32_t& up_limit)
{
  int ret = OB_SUCCESS;
  up_limit = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObDagScheduler is not inited", K(ret));
  } else if (OB_UNLIKELY(up_limit_type < 0 || up_limit_type >= ObIDag::DAG_ULT_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(up_limit_type));
  } else {
    ObThreadCondGuard guard(scheduler_sync_);
    up_limit = up_limits_[up_limit_type];
  }
  return ret;
}

int ObDagScheduler::check_dag_exist(const ObIDag* dag, bool& exist)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  exist = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObDagScheduler is not inited", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arugment", KP(dag));
  } else {
    ObThreadCondGuard guard(scheduler_sync_);
    ObIDag* stored_dag = nullptr;
    if (OB_SUCCESS != (hash_ret = dag_map_.get_refactored(dag, stored_dag))) {
      if (OB_HASH_NOT_EXIST == hash_ret) {
        exist = false;
      } else {
        ret = hash_ret;
        LOG_WARN("failed to get from dag map", K(ret));
      }
    } else if (OB_ISNULL(stored_dag)) {
      ret = OB_ERR_SYS;
      LOG_WARN("dag is null", K(ret));
    }
  }
  return ret;
}

int ObDagScheduler::cancel_dag(const ObIDag* dag)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  bool free_flag = false;
  ObIDag* cur_dag = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObDagScheduler is not inited", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arugment", KP(dag));
  } else {
    {
      ObThreadCondGuard guard(scheduler_sync_);
      if (OB_SUCCESS != (hash_ret = dag_map_.get_refactored(dag, cur_dag))) {
        if (OB_HASH_NOT_EXIST != hash_ret) {
          ret = hash_ret;
          LOG_WARN("failed to get from dag map", K(ret));
        }
      } else if (OB_ISNULL(cur_dag)) {
        ret = OB_ERR_SYS;
        LOG_WARN("dag should not be null", K(ret));
      } else if (cur_dag->get_dag_status() == ObIDag::DAG_STATUS_READY) {
        cur_dag->set_dag_status(ObIDag::DAG_STATUS_ABORT);
        if (OB_SUCCESS != (hash_ret = dag_map_.erase_refactored(cur_dag))) {
          COMMON_LOG(ERROR, "failed to erase dag from dag_map", K(hash_ret), KP(cur_dag), K(*cur_dag));
          ob_abort();
        }
        if (!ready_dag_list_.remove(cur_dag, cur_dag->get_priority())) {
          COMMON_LOG(ERROR, "failed to remove dag from dag list", K(*cur_dag));
        }
        --dag_cnt_;
        --dag_cnts_[cur_dag->get_type()];
        free_flag = true;
      }
    }
    if (free_flag && OB_NOT_NULL(cur_dag)) {
      free_dag(*cur_dag);
    }
  }
  return ret;
}

int ObFakeTask::process()
{
  COMMON_LOG(INFO, "ObFakeTask process");
  return OB_SUCCESS;
}

constexpr int64_t ObLoadShedder::LOAD_SHEDDING_FACTOR[ObLoadShedder::LOAD_TYPE_MAX];
ObLoadShedder::ObLoadShedder()
{
  reset();
}

void ObLoadShedder::reset()
{
  MEMSET(this, 0, sizeof(ObLoadShedder));
  load_shedding_factor_ = 1;
}

void ObLoadShedder::refresh_stat()
{
  load_per_cpu_threshold_ = GCONF._ob_sys_high_load_per_cpu_threshold;
  cpu_cnt_online_ = get_nprocs();
  cpu_cnt_configure_ = get_nprocs_conf();
  if (OB_UNLIKELY(cpu_cnt_configure_ < cpu_cnt_online_)) {
    COMMON_LOG(WARN,
        "Unexpected configured cpu count which less than online cpu count",
        K_(cpu_cnt_online),
        K_(cpu_cnt_configure));
  }
  if (LOAD_TYPE_MAX != getloadavg(load_avg_, LOAD_TYPE_MAX)) {
    // failed to get all avg load
    COMMON_LOG(WARN, "Failed to get average load");
    MEMSET(load_avg_, 0, sizeof(load_avg_));
  }
  refresh_load_shedding_factor();
}

void ObLoadShedder::refresh_load_shedding_factor()
{
  if (load_per_cpu_threshold_ > 0) {
    if (load_shedding_factor_ > DEFAULT_LOAD_SHEDDING_FACTOR) {
      load_shedding_factor_ = DEFAULT_LOAD_SHEDDING_FACTOR;
    } else {
      load_shedding_factor_ = 1;
    }
    if (cpu_cnt_online_ > 0) {
      for (int64_t i = 0; i < LOAD_TYPE_MAX; i++) {
        if (load_avg_[i] * 100 / cpu_cnt_online_ > load_per_cpu_threshold_) {
          load_shedding_factor_ *= LOAD_SHEDDING_FACTOR[i];
        }
      }
    } else {
      COMMON_LOG(WARN, "Invalid status of ObLoadShedder", K(*this));
    }
  } else {
    load_shedding_factor_ = 1;
  }
}

}  // namespace share
}  // namespace oceanbase
