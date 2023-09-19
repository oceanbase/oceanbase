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
#include "share/ob_thread_mgr.h"
#include "share/ob_thread_define.h"
#include "lib/thread/thread_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_force_print_log.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "share/rc/ob_context.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "observer/omt/ob_tenant.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/config/ob_server_config.h"
#include "ob_dag_warning_history_mgr.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/ddl/ob_complement_data_task.h"
#include "storage/multi_data_source/ob_mds_table_merge_dag.h"
#include <sys/sysinfo.h>
#include <algorithm>

namespace oceanbase
{
using namespace lib;
using namespace common;
using namespace omt;

namespace lib
{
  void common_yield()
  {
    share::dag_yield();
  }
}

namespace share
{

#define DEFINE_TASK_ADD_KV(n)                                                               \
  template <LOG_TYPENAME_TN##n>                                                                  \
  int ADD_TASK_INFO_PARAM(char *buf, const int64_t buf_size, LOG_PARAMETER_KV##n)                  \
  {                                                                                              \
    int ret = OB_SUCCESS;                                                                      \
    int64_t __pos = strlen(buf);                                                                  \
    if (__pos >= buf_size) {                                                                       \
    } else {                                                                                       \
        SIMPLE_TO_STRING_##n                                                                        \
        if (__pos > 0) {                                                                            \
          buf[__pos - 1] = ';';                                                                      \
        }                                                                                             \
        buf[__pos] = '\0';                                                                             \
    }                                                                                            \
    return ret;                                                                                   \
  }

DEFINE_TASK_ADD_KV(1)
DEFINE_TASK_ADD_KV(2)
DEFINE_TASK_ADD_KV(3)
DEFINE_TASK_ADD_KV(4)
DEFINE_TASK_ADD_KV(5)

/********************************************ObINodeWithChild impl******************************************/

int ObINodeWithChild::add_child_node(ObINodeWithChild &child)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(children_.push_back(&child))) {
    COMMON_LOG(WARN, "failed to add child", K(ret), K(child));
  } else {
    child.inc_indegree();
    COMMON_LOG(DEBUG, "success to add child", K(ret), K(this), K(&child), K(child));
  }
  return ret;
}

int ObINodeWithChild::dec_indegree_for_children()
{
  int ret = OB_SUCCESS;
  int64_t new_available_cnt = 0;
  for (int64_t i = 0; i < children_.count(); ++i) {
    if (0 == children_.at(i)->dec_indegree()) {
      ++new_available_cnt;
    }
    COMMON_LOG(DEBUG, "dec_indegree_for_children", K(ret), K(this), K(i), K(children_.at(i)),
        K(children_.at(i)->get_indegree()), K(new_available_cnt));
  }
  return ret;
}

int ObINodeWithChild::deep_copy_children(const ObIArray<ObINodeWithChild*> &other)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < other.count(); ++i) {
    if (OB_FAIL(add_child_node(*other.at(i)))) {
      COMMON_LOG(WARN, "failed to copy dependency to task", K(ret), K(i));
    }
  }
  return ret;
}

void ObINodeWithChild::reset_children()
{
  dec_indegree_for_children();
  children_.reset();
}

//The caller is required to ensure that children can be safely deleted
int ObINodeWithChild::erase_children(const ObINodeWithChild *child)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("erase children get invalid argument", K(ret), KP(child));
  } else {
    bool found_flag = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < children_.count(); ++i) {
      ObINodeWithChild *tmp_children = children_.at(i);
      if (child == tmp_children) {
        if (OB_FAIL(children_.remove(i))) {
          LOG_WARN("failed to remove child", K(ret), K(i), KPC(child));
        } else {
          found_flag = true;
          tmp_children->dec_indegree();
          COMMON_LOG(DEBUG, "erase children", K(ret), K(this), K(i), K(tmp_children), K(child));
          break;
        }
      }
    }
    if (OB_SUCC(ret) && !found_flag) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObINodeWithChild::check_child_exist(
    const ObINodeWithChild *child,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;

  if (OB_ISNULL(child)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("erase children get invalid argument", K(ret), KP(child));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < children_.count(); ++i) {
      ObINodeWithChild *tmp_children = children_.at(i);
      if (child == tmp_children) {
        is_exist = true;
        break;
      }
    }
  }
  return ret;
}

/********************************************ObITask impl******************************************/

ObITask::ObITask(ObITask::ObITaskType type)
  : ObINodeWithChild(),
    dag_(NULL),
    type_(type),
    status_(ObITask::TASK_STATUS_INITING),
    last_visit_child_(0),
    color_(ObITaskColor::BLACK),
    max_retry_times_(0),
    running_times_(0)
{
}

ObITask::~ObITask()
{
  reset();
}

void ObITask::reset()
{
  ObINodeWithChild::reset();
  dag_ = NULL;
  type_ = ObITaskType::TASK_TYPE_MAX;
  status_ = ObITaskStatus::TASK_STATUS_INITING;
  last_visit_child_ = 0;
  color_ = ObITaskColor::BLACK;
  max_retry_times_ = 0;
  running_times_ = 0;
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
    const ObDagId &dag_id = dag_->get_dag_id();
    const int64_t start_time = ObTimeUtility::fast_current_time();
    COMMON_LOG(DEBUG, "task start process", K(start_time), K(*this));
    if (OB_FAIL(SYS_TASK_STATUS_MGR.is_task_cancel(dag_id, is_cancel))) {
      COMMON_LOG(WARN, "failed to check is task canceled", K(ret), K(*this));
    }
    if (OB_SUCC(ret)) {
      if (is_cancel) {
        ret = OB_CANCELED;
        COMMON_LOG(WARN, "task is canceled", K(ret), K(dag_id), K_(*dag));
      }
    } else {
      ret = OB_SUCCESS;
    }
    const int32_t saved_worker_level = THIS_WORKER.get_worker_level();
    const int32_t saved_request_level = THIS_WORKER.get_curr_request_level();
    THIS_WORKER.set_worker_level(5);
    THIS_WORKER.set_curr_request_level(5);
    if (OB_SUCC(ret) && OB_FAIL(process())) {
      COMMON_LOG(WARN, "failed to process task", K(ret));
    }
    THIS_WORKER.set_worker_level(saved_worker_level);
    THIS_WORKER.set_curr_request_level(saved_request_level);
    const int64_t end_time = ObTimeUtility::fast_current_time();
    COMMON_LOG(INFO, "task finish process", K(ret), K(start_time), K(end_time),
        "runtime", end_time-start_time, K(*this));
  }
  return ret;
}

int ObITask::generate_next_task()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObITask *next_task = NULL;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "task is invalid", K(ret), K_(type), KP_(dag));
  } else {
    if (OB_TMP_FAIL(generate_next_task(next_task))) {
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

int ObITask::add_child(ObITask &child)
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
      if (OB_FAIL(add_child_node(child))) {
        COMMON_LOG(WARN, "failed to add child", K(child));
      }
    }
  }
  return ret;
}

int ObITask::copy_dep_to(ObITask &other_task) const
{
  // copy dependency of this task to other_task
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "task is invalid", K(ret), K_(type), KP_(dag));
  } else if (this == &other_task) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "can not copy to self", K(ret));
  } else if (OB_FAIL(other_task.deep_copy_children(get_child_nodes()))) {
    COMMON_LOG(WARN, "failed to deep copy children", K(ret));
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

OB_INLINE bool ObITask::check_can_retry()
{
  bool bret = false;
  if (running_times_ < max_retry_times_) {
    running_times_++;
    bret = true;
  }
  return bret;
}

/********************************************ObIDag impl******************************************/

const char *ObIDag::ObIDagStatusStr[] = {
    "INITING",
    "READY",
    "NODE_RUNNING",
    "FINISH",
    "NODE_FAILED",
    "ABORT",
    "RM",
    "HALTED",
    "RETRY",
};

const ObDagPrio::ObDagPrioEnum ObIDag::MergeDagPrio[] = {
    ObDagPrio::DAG_PRIO_COMPACTION_HIGH,
    ObDagPrio::DAG_PRIO_COMPACTION_MID,
    ObDagPrio::DAG_PRIO_COMPACTION_LOW,
};
const ObDagType::ObDagTypeEnum ObIDag::MergeDagType[] = {
    ObDagType::DAG_TYPE_MERGE_EXECUTE,
    ObDagType::DAG_TYPE_MAJOR_MERGE,
    ObDagType::DAG_TYPE_MINI_MERGE,
};

ObIDag::ObIDag(const ObDagType::ObDagTypeEnum type)
  : ObINodeWithChild(),
    dag_ret_(OB_SUCCESS),
    add_time_(0),
    start_time_(0),
    consumer_group_id_(0),
    allocator_(nullptr),
    is_inited_(false),
    type_(type),
    priority_(OB_DAG_TYPES[type].init_dag_prio_),
    dag_status_(ObIDag::DAG_STATUS_INITING),
    running_task_cnt_(0),
    lock_(common::ObLatchIds::WORK_DAG_LOCK),
    is_stop_(false),
    max_retry_times_(0),
    running_times_(0),
    dag_net_(nullptr),
    list_idx_(DAG_LIST_MAX)
{
  STATIC_ASSERT(static_cast<int64_t>(DAG_STATUS_MAX) == ARRAYSIZEOF(ObIDagStatusStr), "dag status str len is mismatch");
  STATIC_ASSERT(MergeDagPrioCnt == ARRAYSIZEOF(MergeDagPrio), "merge dag prio len is mismatch");
  STATIC_ASSERT(MergeDagTypeCnt == ARRAYSIZEOF(MergeDagType), "merge dag type len is mismatch");
}

ObIDag::~ObIDag()
{
  reset();
}

OB_INLINE bool ObIDag::check_can_retry()
{
  bool bret = false;
  ObMutexGuard guard(lock_);
  if (running_times_ < max_retry_times_) {
    running_times_++;
    bret = true;
  }
  return bret;
}

void ObIDag::clear_task_list()
{
  ObITask *cur = task_list_.get_first();
  ObITask *next = NULL;

  while (NULL != cur && task_list_.get_header() != cur) {
    next = cur->get_next();
    cur->~ObITask();
    allocator_->free(cur);
    cur = next;
  }
  task_list_.reset();
}

void ObIDag::clear_running_info()
{
  add_time_ = 0;
  start_time_ = 0;
  consumer_group_id_ = 0;
  running_task_cnt_ = 0;
  dag_status_ = ObDagStatus::DAG_STATUS_INITING;
  dag_ret_ = OB_SUCCESS;
}

void ObIDag::reset()
{
  ObMutexGuard guard(lock_);
  is_inited_ = false;
  ObINodeWithChild::reset();
  clear_task_list();
  clear_running_info();
  type_ = ObDagType::DAG_TYPE_MAX;
  priority_ = ObDagPrio::DAG_PRIO_MAX;
  is_stop_ = false;
  dag_net_ = nullptr;
  list_idx_ = DAG_LIST_MAX;
  allocator_ = nullptr;
}

int ObIDag::add_task(ObITask &task)
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

// Dag_A(child: Dag_E/Dag_F) call add_child(Dag_B)
// result:
// Dag_A(child: Dag_E/Dag_F/Dag_B)
// Dag_B(child: Dag_E/Dag_F) will deep copy previous children of Dag_A, and join in the dag_net which contains Dag_A
int ObIDag::add_child(ObIDag &child)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "current dag is invalid", K(ret), KPC(this));
  } else if (this == &child) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "can not add self loop", K(ret));
  } else if (OB_UNLIKELY(DAG_STATUS_INITING != child.get_dag_status())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag status is not valid", K(ret), K(child));
  } else if (OB_NOT_NULL(dag_net_)) {
    if (OB_NOT_NULL(child.get_dag_net())) { // already belongs to other dag_net
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "child's dag net is not null", K(ret), K(child));
    } else if (OB_FAIL(child.deep_copy_children(get_child_nodes()))) {
      COMMON_LOG(WARN, "failed to deep copy child", K(ret), K(child));
    } else if (OB_FAIL(dag_net_->add_dag_into_dag_net(child))) {
      COMMON_LOG(WARN, "failed to add dag into dag net", K(ret), K(child), KPC(dag_net_));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(add_child_node(child))) {
    COMMON_LOG(WARN, "failed to add child", K(ret), K(child));
    child.reset_children();
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(dag_net_) && OB_TMP_FAIL(dag_net_->erase_dag_from_dag_net(child))) {
      COMMON_LOG(WARN, "failed to erase from dag_net", K(tmp_ret), K(child));
    }
  }
  return ret;
}

int ObIDag::update_status_in_dag_net()
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);
  if (OB_NOT_NULL(dag_net_)) {
    dag_net_->update_dag_status(*this);
  }
  return ret;
}

int ObIDag::check_cycle()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITask *, DEFAULT_TASK_NUM> stack;
  ObITask *cur_task = task_list_.get_first();
  const ObITask *head = task_list_.get_header();

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
        ObITask *pop_task = stack.at(stack.count() - 1);
        int64_t child_idx = pop_task->get_last_visit_child();
        const ObIArray<ObINodeWithChild*> &children = pop_task->get_child_nodes();
        bool has_push = false;
        while (OB_SUCC(ret) && !has_push && child_idx < children.count()) {
          ObITask *child_task = static_cast<ObITask*>(children.at(child_idx));
          if (OB_ISNULL(child_task)) {
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(WARN, "child task is null", K(ret));
          } else if (ObITask::GRAY == child_task->get_color()) {
            ret = OB_INVALID_ARGUMENT;
            COMMON_LOG(WARN, "this dag has cycle", K(ret));
          } else if (ObITask::WHITE == child_task->get_color()) {
            child_task->set_color(ObITask::GRAY);
            pop_task->set_last_visit_child(child_idx+1);
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

bool ObIDag::has_finished()
{
  bool bret = false;
  ObMutexGuard guard(lock_);
  if (ObIDag::DAG_STATUS_NODE_RUNNING == dag_status_) {
    bret = task_list_.is_empty()
           && 0 == running_task_cnt_;
  } else {
    bret = 0 == running_task_cnt_;
  }
  return bret;
}

int ObIDag::get_next_ready_task(ObITask *&task)
{
  int ret = OB_SUCCESS;
  bool found = false;

  ObMutexGuard guard(lock_);
  if (ObIDag::DAG_STATUS_NODE_RUNNING == dag_status_) {
    ObITask *cur_task = task_list_.get_first();
    const ObITask *head = task_list_.get_header();
    while (!found && head != cur_task && nullptr != cur_task) {
      if (0 == cur_task->get_indegree()
          && (ObITask::TASK_STATUS_WAITING == cur_task->get_status()
              || ObITask::TASK_STATUS_RETRY == cur_task->get_status())) {
        found = true;
        task = cur_task;
        inc_running_task_cnt();
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

int ObIDag::finish_task(ObITask &task)
{
  int ret = OB_SUCCESS;
  // remove finished task from task list and update indegree
  {
    ObMutexGuard guard(lock_);
    if (OB_ISNULL(task_list_.remove(&task))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "failed to remove finished task from task_list", K(ret));
    } else {
      dec_running_task_cnt();
    }

    if (OB_SUCC(ret)) {
      if (ObIDag::DAG_STATUS_NODE_RUNNING == dag_status_) {
        if (OB_FAIL(task.dec_indegree_for_children())) {
          COMMON_LOG(WARN, "failed to dec indegree for children", K(ret));
        }
      }
      free_task(task);
    }
  }
  return ret;
}

void ObIDag::free_task(ObITask &task)
{
  if (IS_NOT_INIT) {
    COMMON_LOG_RET(WARN, OB_NOT_INIT, "dag is not inited");
  } else {
    task.~ObITask();
    allocator_->free(&task);
  }
}

int ObIDag::remove_task(ObITask &task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "dag is not inited", K(ret));
  } else if (OB_ISNULL(task_list_.remove(&task))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "failed to remove task from task_list", K_(id));
  } else {
    task.~ObITask();
    allocator_->free(&task);
  }
  return ret;
}

bool ObIDag::is_valid()
{
  return is_inited_
         && !task_list_.is_empty()
         && OB_SUCCESS == check_cycle()
         && is_valid_type();
}

bool ObIDag::is_valid_type() const
{
  return type_ >= 0 && type_ < ObDagType::DAG_TYPE_MAX;
}

int ObIDag::basic_init(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "dag init twice", K(ret));
  } else {
    allocator_ = &allocator;
    is_inited_ = true;
  }
  return ret;
}

int ObIDag::set_dag_id(const ObDagId &dag_id)
{
  int ret = OB_SUCCESS;
  if (dag_id.is_invalid()){
    ret = OB_INVALID_ERROR;
    COMMON_LOG(WARN,"dag id invalid",K(ret));
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

void ObIDag::reset_task_running_status(ObITask &task, ObITask::ObITaskStatus task_status)
{
  ObMutexGuard guard(lock_);
  // this func is called after task returned by get_next_ready_task(inc_running_task_cnt)
  // need dec_running_task_cnt
  dec_running_task_cnt();
  task.set_status(task_status);
}

int64_t ObIDag::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(KP(this), K_(type), "name", get_dag_type_str(type_), K_(id), K_(dag_ret), K_(dag_status),
        K_(start_time), K_(running_task_cnt), K_(indegree), K_(consumer_group_id), "hash", hash());
    J_OBJ_END();
  }
  return pos;
}

int ObIDag::gene_warning_info(ObDagWarningInfo &info, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  info.dag_ret_ = dag_ret_;
  info.task_id_ = id_;
  info.gmt_modified_ = ObTimeUtility::fast_current_time();
  info.dag_type_ = type_;
  info.tenant_id_ = MTL_ID();
  info.gmt_create_ = info.gmt_modified_;
  info.dag_status_ = ObDagWarningInfo::ODS_WARNING;
  if (OB_FAIL(fill_info_param(info.info_param_, allocator))) {
    COMMON_LOG(WARN, "failed to fill info param into dag warning info", K(ret));
  }
  return ret;
}

int ObIDag::reset_status_for_retry()
{
  int ret = OB_SUCCESS;
  {
    ObMutexGuard guard(lock_);
    clear_task_list();
    clear_running_info();
  }
  if (OB_FAIL(inner_reset_status_for_retry())) { // will call alloc_task()
    COMMON_LOG(WARN, "failed to inner reset status", K(ret));
  } else {
    set_dag_status(ObIDag::DAG_STATUS_RETRY);
    start_time_ = ObTimeUtility::fast_current_time();
  }
  return ret;
}

int ObIDag::finish(const ObDagStatus status)
{
  int ret = OB_SUCCESS;
  {
    ObMutexGuard guard(lock_);
    if (OB_FAIL(dec_indegree_for_children())) {
      COMMON_LOG(WARN, "failed to dec indegree for children", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    set_dag_status(status);
    update_status_in_dag_net();
  }
  return ret;
}

int ObIDag::add_child_without_inheritance(ObIDag &child)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);

  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "current dag is invalid, unexpected", K(ret), KPC(this));
  } else if (OB_FAIL(inner_add_child_without_inheritance(child))) {
    COMMON_LOG(WARN, "failed to add child without inheritance", K(ret), KPC(this));
  }
  return ret;
}

int ObIDag::add_child_without_inheritance(
    const common::ObIArray<ObINodeWithChild*> &child_array)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);

  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "current dag is invalid, unexpected", K(ret), KPC(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < child_array.count(); ++i) {
      ObIDag *child_dag = static_cast<ObIDag *>(child_array.at(i));
      if (OB_FAIL(inner_add_child_without_inheritance(*child_dag))) {
        LOG_WARN("failed to add child without inheritance", K(ret), KPC(child_dag));
      }
    }
  }
  return ret;
}

int ObIDag::inner_add_child_without_inheritance(ObIDag &child)
{
  int ret = OB_SUCCESS;
  ObIDagNet *child_dag_net = child.get_dag_net();

  if (this == &child) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "can not add self loop", K(ret));
  } else if ((OB_UNLIKELY(DAG_STATUS_INITING != child.get_dag_status())
        && OB_UNLIKELY(DAG_STATUS_READY != child.get_dag_status()))
      || ((DAG_STATUS_READY == child.get_dag_status()) && 0 == child.get_indegree())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag status is not valid", K(ret), K(child));
  } else if (dag_net_ != child_dag_net) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag and child dag net not same", K(ret), KPC(dag_net_), KPC(child_dag_net));
  }

  if (OB_SUCC(ret) && OB_FAIL(add_child_node(child))) {
    COMMON_LOG(WARN, "failed to add child", K(ret), K(child));
    child.reset_children();
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(dag_net_) && OB_TMP_FAIL(dag_net_->erase_dag_from_dag_net(child))) {
      COMMON_LOG(WARN, "failed to erase from dag_net", K(tmp_ret), K(child));
    }
  }
  return ret;
}

int ObIDag::fill_comment(char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  MEMSET(buf, '\0', buf_len);
  compaction::ObInfoParamBuffer allocator;
  compaction::ObIBasicInfoParam *param = nullptr;
  if (OB_FAIL(fill_info_param(param, allocator))) {
    COMMON_LOG(WARN, "failed to fill info param", K(ret));
  } else if (OB_ISNULL(param)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "param is null", K(ret));
  } else if (OB_FAIL(param->fill_comment(buf, buf_len))) {
    COMMON_LOG(WARN, "failed to fill comment", K(ret));
  }
  return ret;
}

/*************************************ObIDagNet***********************************/

ObIDagNet::ObIDagNet(
    const ObDagNetType::ObDagNetTypeEnum type)
   : is_stopped_(false),
     lock_(common::ObLatchIds::WORK_DAG_NET_LOCK),
     allocator_(nullptr),
     type_(type),
     add_time_(0),
     start_time_(0),
     dag_record_map_(),
     is_cancel_(false)
{
}

int ObIDagNet::add_dag_into_dag_net(ObIDag &dag)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObDagRecord *dag_record = nullptr;
  int hash_ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);
  WEAK_BARRIER();
  const bool is_stop = is_stopped_;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "dag net not basic init", K(ret), K(this));
  } else if (OB_NOT_NULL(dag.get_dag_net())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "dag already belongs to a dag_net", K(ret), K(dag));
  } else if (is_stop) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("dag_net is in stop state, not allowed to add dag", K(ret), K(is_stop));
  } else if (is_cancel_) {
    ret = OB_CANCELED;
    LOG_WARN("dag net is cancel, do not allow to add new dag", K(ret), K(is_cancel_));
  } else {
    if (!dag_record_map_.created() && OB_FAIL(dag_record_map_.create(DEFAULT_DAG_BUCKET, "DagRecordMap", "DagRecordNode", MTL_ID()))) {
      COMMON_LOG(WARN, "failed to create dag record map", K(ret), K(dag));
    } else if (OB_HASH_NOT_EXIST != (hash_ret = dag_record_map_.get_refactored(&dag, dag_record))) {
      ret = OB_SUCCESS == hash_ret ? OB_ERR_UNEXPECTED : hash_ret;
      COMMON_LOG(WARN, "dag record maybe already exist in map", K(ret), K(hash_ret), K(dag));
    } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObDagRecord)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "allocate memory failed", K(ret), K(buf));
    } else {
      dag_record = new(buf) ObDagRecord();
      dag_record->dag_ptr_ = &dag;
      dag_record->dag_type_ = dag.get_type();

      if (OB_FAIL(dag_record_map_.set_refactored(&dag, dag_record))) {
        COMMON_LOG(WARN, "Failed to set dag record", K(ret), KP(this), KPC(dag_record));
      } else {
        dag.set_dag_net(*this);
        COMMON_LOG(INFO, "success to add into dag array", K(ret), KP(this), K(dag_record));
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(buf)) {
        allocator_->free(buf);
        buf = nullptr;
      }
    }
  } // end of lock
  return ret;
}

void ObIDagNet::reset()
{
  ObMutexGuard guard(lock_);
  is_stopped_ = true;
  if (dag_record_map_.created()) {
    for (DagRecordMap::iterator iter = dag_record_map_.begin();
        iter != dag_record_map_.end(); ++iter) {
      ObDagRecord *dag_record = iter->second;
      if (OB_ISNULL(dag_record)) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "dag record should not be NULL", KPC(this), KPC(dag_record));
      } else  {
        if (!ObIDag::is_finish_status(dag_record->dag_status_)) {
          LOG_ERROR_RET(OB_ERR_UNEXPECTED, "dag net should not be reset, dag in dag_net is not finish!!!", KPC(this), KPC(dag_record));
        }
        allocator_->free(dag_record);
      }
    }
    dag_record_map_.destroy();
  }

  type_ = ObDagNetType::DAG_NET_TYPE_MAX;
  add_time_ = 0;
  start_time_ = 0;
}

int ObIDagNet::basic_init(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "dag net is inited", K(ret), K(this));
  } else {
    allocator_ = &allocator;
  }
  return ret;
}

bool ObIDagNet::check_finished_and_mark_stop()
{
  ObMutexGuard guard(lock_);
  if (dag_record_map_.empty()) {
    WEAK_BARRIER();
    is_stopped_ = true;
  }
  return is_stopped_;
}

int ObIDagNet::update_dag_status(ObIDag &dag)
{
  int ret = OB_SUCCESS;
  bool found = false;
  ObDagRecord *dag_record = nullptr;

  ObMutexGuard guard(lock_);
  WEAK_BARRIER();
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "dag net not basic init", K(ret), K(this));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net is in stop state", K(ret), K(dag), KP(this));
  } else if (OB_FAIL(dag_record_map_.get_refactored(&dag, dag_record))) {
    COMMON_LOG(WARN, "dag is not exist in this dag_net", K(ret), KP(this), KP(&dag));
  } else if (OB_ISNULL(dag_record)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag record should not be NULL", K(ret), KP(this), KP(&dag));
  } else if (dag_record->dag_ptr_ != &dag) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag record has unexpected dag value", K(ret), KP(this), KP(&dag), KPC(dag_record));
  } else if (OB_UNLIKELY(ObIDag::is_finish_status(dag_record->dag_status_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dag is in finish status", K(ret), KPC(dag_record), K(dag));
  } else if (FALSE_IT(found = true)) {
  } else if (FALSE_IT(dag_record->dag_status_ = dag.get_dag_status())) {
  } else if (ObIDag::DAG_STATUS_NODE_RUNNING == dag.get_dag_status()) {
    dag_record->dag_id_ = dag.get_dag_id();
  } else if (ObIDag::is_finish_status(dag.get_dag_status())) {
    remove_dag_record_(*dag_record);
  }
  return ret;
}

void ObIDagNet::remove_dag_record_(ObDagRecord &dag_record)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dag_record_map_.erase_refactored(dag_record.dag_ptr_))) {
    COMMON_LOG(WARN, "failed to remove dag record", K(dag_record));
    ob_abort();
  }
  allocator_->free(&dag_record);
}

// called when ObIDag::add_child failed
int ObIDagNet::erase_dag_from_dag_net(ObIDag &dag)
{
  int ret = OB_SUCCESS;
  ObDagRecord *dag_record = nullptr;
  ObMutexGuard guard(lock_);
  WEAK_BARRIER();
  if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net is in stop state", K(ret), K(dag), KP(this));
  } else if (OB_FAIL(dag_record_map_.get_refactored(&dag, dag_record))) {
    COMMON_LOG(WARN, "dag is not exist in this dag_net", K(ret), KP(this), KP(&dag));
  } else if (OB_ISNULL(dag_record)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag record should not be NULL", K(ret), KP(this), KP(&dag));
  } else if (OB_UNLIKELY(dag_record->dag_ptr_ != &dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag record has unexpected dag value", K(ret), KP(this), KP(&dag), KPC(dag_record));
  } else if (OB_UNLIKELY(ObIDag::is_finish_status(dag_record->dag_status_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dag status is invalid when erase", K(ret), KPC(dag_record));
  } else {
    COMMON_LOG(DEBUG, "success to update status", K(ret), KPC(dag_record));
    remove_dag_record_(*dag_record);
    dag.clear_dag_net();
  }
  return ret;
}


int64_t ObIDagNet::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_NAME("ObIDagNet");
    J_COLON();
    J_KV(KP(this), K_(type), K_(dag_id), "dag_record_cnt", dag_record_map_.size(),
        K_(is_stopped), K_(is_cancel), KP_(allocator));
    J_OBJ_END();
  }
  return pos;
}

void ObIDagNet::init_dag_id_()
{
  if (dag_id_.is_invalid()) {
    dag_id_.init(GCONF.self_addr_);
  }
}

int ObIDagNet::set_dag_id(const ObDagId &dag_id)
{
  int ret = OB_SUCCESS;
  if (dag_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "set dag id get invalid argument", K(ret), K(dag_id));
  } else {
    dag_id_ = dag_id;
  }
  return ret;
}

void ObIDagNet::set_cancel()
{
  ObMutexGuard guard(lock_);
  is_cancel_ = true;
}

bool ObIDagNet::is_cancel()
{
  ObMutexGuard guard(lock_);
  return is_cancel_;
}

bool ObIDagNet::is_inited()
{
  return OB_NOT_NULL(allocator_);
}


void ObIDagNet::gene_dag_info(ObDagInfo &info, const char *list_info)
{
  ObMutexGuard guard(lock_);
  info.tenant_id_ = MTL_ID();
  info.dag_net_type_ = type_;
  info.dag_status_ = ObIDag::DAG_STATUS_NODE_RUNNING;
  info.running_task_cnt_ = dag_record_map_.size();
  info.add_time_ = add_time_;
  info.start_time_ = start_time_;
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(fill_dag_net_key(info.dag_net_key_, OB_DAG_KEY_LENGTH))) {
    COMMON_LOG_RET(WARN, tmp_ret, "failed to fill dag key", K(tmp_ret));
  }
  if (OB_TMP_FAIL(fill_comment(info.comment_, OB_DAG_COMMET_LENGTH))) {
    COMMON_LOG_RET(WARN, tmp_ret, "failed to fill comment", K(tmp_ret));
  }
  ADD_TASK_INFO_PARAM(info.comment_, OB_DAG_COMMET_LENGTH,
      "list_info", list_info);
  if (!dag_record_map_.empty() && strlen(info.comment_) < OB_DAG_COMMET_LENGTH) {
    int64_t str_len = strlen(info.comment_);
    info.comment_[str_len] = '|';
    info.comment_[str_len + 1] = '\0';

    for (DagRecordMap::iterator iter = dag_record_map_.begin();
        iter != dag_record_map_.end(); ++iter) {
      ObDagRecord *dag_record = iter->second;
      tmp_ret = ADD_TASK_INFO_PARAM(info.comment_, OB_DAG_COMMET_LENGTH,
          "type", ObIDag::get_dag_type_str(dag_record->dag_type_),
          "status", ObIDag::get_dag_status_str(dag_record->dag_status_));
    }
  }
}

void ObIDag::gene_dag_info(ObDagInfo &info, const char *list_info)
{
  ObMutexGuard guard(lock_);
  info.tenant_id_ = MTL_ID();
  info.dag_type_ = type_;
  info.dag_id_ = id_;
  info.dag_status_ = dag_status_;
  info.running_task_cnt_ = running_task_cnt_;
  info.add_time_ = add_time_;
  info.start_time_ = start_time_;
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(fill_dag_key(info.dag_key_, OB_DAG_KEY_LENGTH))) {
    COMMON_LOG_RET(WARN, tmp_ret, "failed to fill dag key", K(tmp_ret));
  }

  if (OB_TMP_FAIL(fill_comment(info.comment_, sizeof(info.comment_)))) {
    COMMON_LOG_RET(WARN, tmp_ret, "failed to fill comment");
  }

  ADD_TASK_INFO_PARAM(info.comment_, OB_DAG_COMMET_LENGTH,
      "check_can_schedule", check_can_schedule(),
      "indegree", indegree_,
      "list_info", list_info,
      "dag_ptr", this);

  // comment will print cur running task & task list
  if (ObIDag::DAG_STATUS_NODE_RUNNING == dag_status_) {
    ObITask *cur_task = task_list_.get_first();
    const ObITask *head = task_list_.get_header();
    tmp_ret = OB_SUCCESS;
    int idx = 0;
    while (head != cur_task && OB_SUCCESS == tmp_ret) {
      tmp_ret = ADD_TASK_INFO_PARAM(info.comment_, OB_DAG_COMMET_LENGTH,
          "idx", idx++,
          "type", cur_task->get_type(),
          "status", cur_task->get_status(),
          "indegree", cur_task->indegree_);
      cur_task = cur_task->get_next();
    } // end while
  }
  if (OB_NOT_NULL(dag_net_)) {
    info.dag_net_type_ = dag_net_->get_type();
    if (OB_TMP_FAIL(dag_net_->fill_dag_net_key(info.dag_net_key_, OB_DAG_KEY_LENGTH))) {
      COMMON_LOG_RET(WARN, tmp_ret, "failed to fill dag key", K(tmp_ret));
    }
  }
}

const char *ObIDag::get_dag_status_str(enum ObDagStatus status)
{
  const char *str = "";
  if (status >= DAG_STATUS_MAX || status < DAG_STATUS_INITING) {
    str = "invalid_type";
  } else {
    str = ObIDagStatusStr[status];
  }
  return str;
}

const char *ObIDag::get_dag_type_str(const enum ObDagType::ObDagTypeEnum type)
{
  const char *str = "";
  if (type >= ObDagType::DAG_TYPE_MAX || type < ObDagType::DAG_TYPE_MINI_MERGE) {
    str = "invalid_type";
  } else {
    str = OB_DAG_TYPES[type].dag_type_str_;
  }
  return str;
}

const char *ObIDag::get_dag_module_str(const enum ObDagType::ObDagTypeEnum type)
{
  const char *str = "";
  if (type >= ObDagType::DAG_TYPE_MAX || type < ObDagType::DAG_TYPE_MINI_MERGE) {
    str = "invalid_type";
  } else {
    str = OB_DAG_TYPES[type].dag_module_str_;
  }
  return str;
}

const char *ObIDag::get_dag_prio_str(const ObDagPrio::ObDagPrioEnum prio)
{
  const char *str = "";
  if (prio >= ObDagPrio::DAG_PRIO_MAX || prio < ObDagPrio::DAG_PRIO_COMPACTION_HIGH) {
    str = "invalid_type";
  } else {
    str = OB_DAG_PRIOS[prio].dag_prio_str_;
  }
  return str;
}

const char *ObIDagNet::get_dag_net_type_str(ObDagNetType::ObDagNetTypeEnum type)
{
  const char *str = "";
  if (type >= ObDagNetType::DAG_NET_TYPE_MAX || type < ObDagNetType::DAG_NET_TYPE_MIGARTION) {
    str = "invalid_type";
  } else {
    str = OB_DAG_NET_TYPES[type].dag_net_type_str_;
  }
  return str;
}

ObDagInfo::ObDagInfo()
  : tenant_id_(0),
    dag_type_(ObDagType::DAG_TYPE_MAX),
    dag_net_type_(ObDagNetType::DAG_NET_TYPE_MAX),
    dag_key_(),
    dag_net_key_(),
    dag_id_(),
    dag_status_(ObIDag::DAG_STATUS_INITING),
    running_task_cnt_(0),
    add_time_(0),
    start_time_(0),
    indegree_(0),
    comment_()
{
  MEMSET(comment_,'\0', sizeof(comment_));
}

ObDagInfo & ObDagInfo::operator = (const ObDagInfo &other)
{
  tenant_id_ = other.tenant_id_;
  dag_type_ = other.dag_type_;
  dag_net_type_ = other.dag_net_type_;
  strncpy(dag_key_, other.dag_key_, strlen(other.dag_key_) + 1);
  strncpy(dag_net_key_, other.dag_net_key_, strlen(other.dag_net_key_) + 1);
  dag_id_ = other.dag_id_;
  dag_status_ = other.dag_status_;
  running_task_cnt_ = other.running_task_cnt_;
  add_time_ = other.add_time_;
  start_time_ = other.start_time_;
  indegree_ = other.indegree_;
  strncpy(comment_, other.comment_, strlen(other.comment_) + 1);
  return *this;
}

bool ObDagInfo::is_valid() const
{
  return (tenant_id_ > 0
      && dag_status_ >= share::ObIDag::DAG_STATUS_INITING
      && dag_status_ < share::ObIDag::DAG_STATUS_MAX
      && running_task_cnt_ >= 0
      && add_time_ >= 0
      && start_time_ >= 0
      && indegree_ >= 0
      && ((dag_net_type_ >= ObDagNetType::DAG_NET_TYPE_MIGARTION
          && dag_net_type_ < ObDagNetType::DAG_NET_TYPE_MAX)
        || (dag_type_ >= share::ObDagType::DAG_TYPE_MINI_MERGE
          && dag_type_ < share::ObDagType::DAG_TYPE_MAX)));
}

const char *ObDagSchedulerInfo::ObValueTypeStr[VALUE_TYPE_MAX] =
{
    "GENERAL",
    "UP_LIMIT",
    "LOW_LIMIT",
    "DAG_COUNT",
    "DAG_NET_COUNT",
    "RUNNING_TASK_CNT",
};

const char* ObDagSchedulerInfo::get_value_type_str(ObValueType type)
{
  const char *str = "";
  if (type >= VALUE_TYPE_MAX || type < GENERAL) {
    str = "invalid_type";
  } else {
    str = ObValueTypeStr[type];
  }
  return str;
}

ObDagSchedulerInfo::ObDagSchedulerInfo()
  : tenant_id_(0),
    value_type_(VALUE_TYPE_MAX),
    key_(),
    value_(0)
{}

ObDagSchedulerInfo & ObDagSchedulerInfo::operator = (const ObDagSchedulerInfo &other)
{
  tenant_id_ = other.tenant_id_;
  value_type_ = other.value_type_;
  strncpy(key_, other.key_, strlen(other.key_) + 1);
  value_ = other.value_;
  return *this;
}

/*************************************ObTenantDagWorker***********************************/

_RLOCAL(ObTenantDagWorker *, ObTenantDagWorker::self_);

ObTenantDagWorker::ObTenantDagWorker()
  : task_(NULL),
    status_(DWS_FREE),
    check_period_(0),
    last_check_time_(0),
    function_type_(0),
    group_id_(0),
    tg_id_(-1),
    is_inited_(false)
{
}

ObTenantDagWorker::~ObTenantDagWorker()
{
  destroy();
}

int ObTenantDagWorker::init(const int64_t check_period)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "dag worker is inited twice", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::DAG_WORKER_COND_WAIT))) {
    COMMON_LOG(WARN, "failed to init cond", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::DagWorker, tg_id_))) {
    COMMON_LOG(WARN, "TG create dag worker failed", K(ret));
  } else {
    check_period_ = check_period;
    is_inited_ = true;
  }
  if (!is_inited_) {
    reset();
  }
  return ret;
}

int ObTenantDagWorker::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantDagWorker not init", K(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(tg_id_, *this))) {
    LOG_WARN("failed to start ObTenantDagWorker", K(ret));
  }
  return ret;
}

void ObTenantDagWorker::stop()
{
  TG_STOP(tg_id_);
  notify(DWS_STOP);
}

void ObTenantDagWorker::wait()
{
  TG_WAIT(tg_id_);
}

void ObTenantDagWorker::destroy()
{
  if (IS_INIT) {
    reset();
  }
}
void ObTenantDagWorker::reset()
{
  stop();
  wait();
  task_ = NULL;
  status_ = DWS_FREE;
  check_period_ = 0;
  last_check_time_ = 0;
  function_type_ = 0;
  group_id_ = 0;
  self_ = NULL;
  is_inited_ = false;
  TG_DESTROY(tg_id_);
}

void ObTenantDagWorker::notify(DagWorkerStatus status)
{
  ObThreadCondGuard guard(cond_);
  status_ = status;
  cond_.signal();
}

void ObTenantDagWorker::resume()
{
  notify(DWS_RUNNABLE);
}

int ObTenantDagWorker::set_dag_resource(const uint64_t group_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.cgroup_ctrl_)) {
    //cgroup not init, cannot bind thread and control resource
  } else {
    uint64_t consumer_group_id = 0;
    if (group_id != 0) {
      //user level
      consumer_group_id = group_id;
    } else if (OB_FAIL(G_RES_MGR.get_mapping_rule_mgr().get_group_id_by_function_type(MTL_ID(), function_type_, consumer_group_id))) {
      //function level
      LOG_WARN("fail to get group id by function", K(ret), K(MTL_ID()), K(function_type_), K(consumer_group_id));
    }
    if (OB_SUCC(ret) && consumer_group_id != group_id_) {
      // for CPU isolation, depend on cgroup
      if (GCTX.cgroup_ctrl_->is_valid() && OB_FAIL(GCTX.cgroup_ctrl_->add_self_to_group(MTL_ID(), consumer_group_id))) {
        LOG_WARN("bind back thread to group failed", K(ret), K(GETTID()), K(MTL_ID()), K(group_id));
      } else {
        // for IOPS isolation, only depend on consumer_group_id
        ATOMIC_SET(&group_id_, consumer_group_id);
        THIS_WORKER.set_group_id(static_cast<int32_t>(consumer_group_id));
      }
    }
  }
  return ret;
}

bool ObTenantDagWorker::need_wake_up() const
{
  return (ObTimeUtility::fast_current_time() - last_check_time_) > check_period_ * 10;
}

void ObTenantDagWorker::run1()
{
  self_ = this;
  int ret = OB_SUCCESS;
  ObIDag *dag = NULL;
  lib::set_thread_name("DAG");
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  while (!has_set_stop()) {
    ret = OB_SUCCESS;
    if (DWS_RUNNABLE == status_ && NULL != task_) {
      status_ = DWS_RUNNING;
      last_check_time_ = ObTimeUtility::fast_current_time();
      if (OB_ISNULL(dag = task_->get_dag())) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "dag is null", K(ret), K(task_));
      } else {
        ObCurTraceId::set(dag->get_dag_id());
        lib::set_thread_name(dag->get_dag_type_str(dag->get_type()));
        if (OB_UNLIKELY(lib::Worker::CompatMode::INVALID == (compat_mode = dag->get_compat_mode()))) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "invalid compat mode", K(ret), K(*dag));
        } else {
#ifdef ERRSIM
          const ObErrsimModuleType type(dag->get_module_type());
          THIS_WORKER.set_module_type(type);
#endif
          THIS_WORKER.set_compatibility_mode(compat_mode);
          if (OB_FAIL(set_dag_resource(dag->get_consumer_group_id()))) {
            LOG_WARN("isolate dag CPU and IOPS failed", K(ret));
          } else if (OB_FAIL(task_->do_work())) {
            if (!dag->ignore_warning()) {
              COMMON_LOG(WARN, "failed to do work", K(ret), K(*task_), K(compat_mode));
            }
          }
        }
      }

      {
        const int64_t curr_time = ObTimeUtility::fast_current_time();
        const int64_t elapsed_time = curr_time - last_check_time_;
        EVENT_ADD(SYS_TIME_MODEL_DB_TIME, elapsed_time);
        EVENT_ADD(SYS_TIME_MODEL_DB_CPU, elapsed_time);
        EVENT_ADD(SYS_TIME_MODEL_BKGD_TIME, elapsed_time);
        EVENT_ADD(SYS_TIME_MODEL_BKGD_CPU, elapsed_time);
      }
      status_ = DWS_FREE;
      if (OB_FAIL(MTL(ObTenantDagScheduler*)->deal_with_finish_task(*task_, *this, ret/*task error_code*/))) {
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
  } // end of while
}

void ObTenantDagWorker::yield()
{
  RLOCAL(uint64_t, counter);
  const static uint64_t CHECK_INTERVAL = (1UL << 12) - 1;
  if (!((++counter) & CHECK_INTERVAL)) {
    int64_t curr_time = ObTimeUtility::fast_current_time();
    if (last_check_time_ + check_period_ <= curr_time) {
      int64_t elapsed_time = curr_time - last_check_time_;
      EVENT_ADD(SYS_TIME_MODEL_DB_TIME, elapsed_time);
      EVENT_ADD(SYS_TIME_MODEL_DB_CPU, elapsed_time);
      EVENT_ADD(SYS_TIME_MODEL_BKGD_TIME, elapsed_time);
      EVENT_ADD(SYS_TIME_MODEL_BKGD_CPU, elapsed_time);
      last_check_time_ = curr_time;
      ObThreadCondGuard guard(cond_);
      if (DWS_RUNNING == status_ && MTL(ObTenantDagScheduler*)->try_switch(*this)) {
        status_ = DWS_WAITING;
        while (DWS_WAITING == status_) {
          cond_.wait(SLEEP_TIME_MS);
        }
        ObCurTraceId::set(task_->get_dag()->get_dag_id());
        COMMON_LOG(INFO, "worker continues to run", K(*task_));
        curr_time = ObTimeUtility::fast_current_time();
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

/***************************************ObTenantDagScheduler impl********************************************/

int ObTenantDagScheduler::mtl_init(ObTenantDagScheduler* &scheduler)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(scheduler->init(MTL_ID()))) {
    COMMON_LOG(WARN, "failed to init ObTenantDagScheduler for tenant", K(ret), K(MTL_ID()));
  } else {
    FLOG_INFO("success to init ObTenantDagScheduler for tenant", K(ret), K(MTL_ID()), KP(scheduler));
  }
  return ret;
}

ObTenantDagScheduler::ObTenantDagScheduler()
  : is_inited_(false),
    dag_net_map_lock_(common::ObLatchIds::WORK_DAG_NET_LOCK),
    dag_cnt_(0),
    dag_limit_(0),
    check_period_(0),
    loop_waiting_dag_list_period_(0),
    total_worker_cnt_(0),
    work_thread_num_(0),
    default_work_thread_num_(0),
    total_running_task_cnt_(0),
    scheduled_task_cnt_(0),
    tg_id_(-1)
{
}

ObTenantDagScheduler::~ObTenantDagScheduler()
{
  destroy();
}

void ObTenantDagScheduler::stop()
{
  TG_STOP(tg_id_);
}

void ObTenantDagScheduler::wait()
{
  TG_WAIT(tg_id_);
}

void ObTenantDagScheduler::reload_config()
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    set_thread_score(ObDagPrio::DAG_PRIO_COMPACTION_HIGH, tenant_config->compaction_high_thread_score);
    set_thread_score(ObDagPrio::DAG_PRIO_COMPACTION_MID, tenant_config->compaction_mid_thread_score);
    set_thread_score(ObDagPrio::DAG_PRIO_COMPACTION_LOW, tenant_config->compaction_low_thread_score);
    set_thread_score(ObDagPrio::DAG_PRIO_HA_HIGH, tenant_config->ha_high_thread_score);
    set_thread_score(ObDagPrio::DAG_PRIO_HA_MID, tenant_config->ha_mid_thread_score);
    set_thread_score(ObDagPrio::DAG_PRIO_HA_LOW, tenant_config->ha_low_thread_score);
    set_thread_score(ObDagPrio::DAG_PRIO_DDL, tenant_config->ddl_thread_score);
    set_thread_score(ObDagPrio::DAG_PRIO_TTL, tenant_config->ttl_thread_score);
    compaction_dag_limit_ = tenant_config->compaction_dag_cnt_limit;
  }
}

int ObTenantDagScheduler::init_allocator(
    const uint64_t tenant_id,
    const lib::ObLabel &label,
    lib::MemoryContext &mem_context)
{
  int ret = OB_SUCCESS;
  ContextParam param;
  param.set_mem_attr(tenant_id, label, common::ObCtxIds::DEFAULT_CTX_ID)
      .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE)
      .set_properties(ALLOC_THREAD_SAFE)
      .set_parallel(8);
  if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_context, param))) {
    COMMON_LOG(WARN, "fail to create entity", K(ret));
  } else if (nullptr == mem_context) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "memory entity is null", K(ret));
  }
  return ret;
}

int ObTenantDagScheduler::init(
    const uint64_t tenant_id,
    const int64_t check_period /* =DEFAULT_CHECK_PERIOD */,
    const int64_t loop_waiting_list_period /* = LOOP_WAITING_DAG_LIST_INTERVAL*/,
    const int64_t dag_limit /*= DEFAULT_MAX_DAG_NUM*/)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "scheduler init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || 0 >= dag_limit) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "init ObTenantDagScheduler with invalid arguments", K(ret), K(tenant_id), K(dag_limit));
  } else if (OB_FAIL(dag_map_.create(dag_limit, "DagMap", "DagNode", tenant_id))) {
    COMMON_LOG(WARN, "failed to create dap map", K(ret), K(dag_limit));
  } else if (OB_FAIL(dag_net_map_[RUNNING_DAG_NET_MAP].create(dag_limit, "DagNetMap", "DagNetNode", tenant_id))) {
    COMMON_LOG(WARN, "failed to create running dap net map", K(ret), K(dag_limit));
  } else if (OB_FAIL(dag_net_id_map_.create(dag_limit, "DagNetIdMap", "DagNetIdNode", tenant_id))) {
    COMMON_LOG(WARN, "failed to create dap net id map", K(ret), K(dag_limit));
  } else if (OB_FAIL(scheduler_sync_.init(ObWaitEventIds::SCHEDULER_COND_WAIT))) {
    COMMON_LOG(WARN, "failed to init task queue sync", K(ret));
  } else if (OB_FAIL(init_allocator(tenant_id, ObModIds::OB_SCHEDULER, mem_context_))) {
    COMMON_LOG(WARN, "failed to init scheduler allocator", K(ret));
  } else if (OB_FAIL(init_allocator(tenant_id, "HAScheduler", ha_mem_context_))) {
    COMMON_LOG(WARN, "failed to init ha scheduler allocator", K(ret));
  }
  if (OB_SUCC(ret)) {
    check_period_ = check_period;
    loop_waiting_dag_list_period_ = loop_waiting_list_period;
    MEMSET(dag_cnts_, 0, sizeof(dag_cnts_));
    MEMSET(scheduled_task_cnts_, 0, sizeof(scheduled_task_cnts_));
    MEMSET(dag_net_cnts_, 0, sizeof(dag_net_cnts_));
    MEMSET(running_task_cnts_, 0, sizeof(running_task_cnts_));

    get_default_config();
    dag_limit_ = dag_limit;
    compaction_dag_limit_ = dag_limit;
    if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::DagScheduler, tg_id_))) {
      COMMON_LOG(WARN, "TG create dag scheduler failed", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < work_thread_num_; ++i) {
      if (OB_FAIL(create_worker())) {
        COMMON_LOG(WARN, "failed to create worker", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(TG_SET_RUNNABLE_AND_START(tg_id_, *this))) {
        COMMON_LOG(WARN, "failed to start dag scheduler", K(ret));
      } else {
        is_inited_ = true;
      }
    }
  }
  if (!is_inited_) {
    reset();
    COMMON_LOG(WARN, "failed to init ObTenantDagScheduler", K(ret));
  } else {
    dump_dag_status();
    COMMON_LOG(INFO, "ObTenantDagScheduler is inited", K(ret), K_(work_thread_num));
  }
  return ret;
}

void ObTenantDagScheduler::destroy()
{
  if (IS_INIT) {
    reset();
  }
}

void ObTenantDagScheduler::reset()
{
  COMMON_LOG(INFO, "ObTenantDagScheduler starts to destroy");
  stop();
  notify();
  wait();

  destroy_all_workers();
  is_inited_ = false; // avoid alloc dag/dag_net
  WEAK_BARRIER();
  int tmp_ret = OB_SUCCESS;
  int64_t abort_dag_cnt = 0;
  dump_dag_status(true);
  for (int64_t j = 0; j < DAG_LIST_MAX; ++j) {
    for (int64_t i = 0; i < PriorityDagList::PRIO_CNT; ++i) {
      ObIDag *head = dag_list_[j].get_head(i);
      ObIDag *cur_dag = head->get_next();
      ObIDag *next = NULL;
      ObIDagNet *tmp_dag_net = nullptr;
      while (NULL != cur_dag && head != cur_dag) {
        next = cur_dag->get_next();
        FLOG_INFO("destroy dag", "dag_list", j, "prio", i,  KPC(cur_dag));
        if (cur_dag->get_dag_id().is_valid()
          && OB_TMP_FAIL(ObSysTaskStatMgr::get_instance().del_task(cur_dag->get_dag_id()))) {
          if (OB_ENTRY_NOT_EXIST != tmp_ret) {
            STORAGE_LOG_RET(WARN, tmp_ret, "failed to del sys task", K(tmp_ret), K(cur_dag->get_dag_id()));
          }
        }
        if (OB_TMP_FAIL(finish_dag_(ObIDag::DAG_STATUS_ABORT, *cur_dag, tmp_dag_net, false/*try_move_child*/))) {
          STORAGE_LOG_RET(WARN, tmp_ret, "failed to abort dag", K(tmp_ret), KPC(cur_dag));
        } else {
          ++abort_dag_cnt;
        }
        cur_dag = next;
      } // end of while
    } // end of prio loop
    dag_list_[j].reset();
  } // end of for
  blocking_dag_net_list_.reset();

  if (dag_map_.created()) {
    dag_map_.destroy();
  }
  if (dag_net_map_[RUNNING_DAG_NET_MAP].created()) {
    for (DagNetMap::iterator iter = dag_net_map_[RUNNING_DAG_NET_MAP].begin(); iter != dag_net_map_[RUNNING_DAG_NET_MAP].end(); ++iter) {
      ObIAllocator &allocator = get_allocator(iter->second->is_ha_dag_net());
      iter->second->~ObIDagNet();
      allocator.free(iter->second);
    } // end of for
    dag_net_map_[RUNNING_DAG_NET_MAP].destroy();
  }
  if (dag_net_id_map_.created()) {
    dag_net_id_map_.destroy();
  }
  COMMON_LOG(INFO, "ObTenantDagScheduler before allocator destroyed", K(abort_dag_cnt));
  // there will be 'HAS UNFREE PTR' log with label when some ptrs haven't been free
  if (NULL != mem_context_) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  if (NULL != ha_mem_context_) {
    DESTROY_CONTEXT(ha_mem_context_);
    ha_mem_context_ = nullptr;
  }
  scheduler_sync_.destroy();
  dag_cnt_ = 0;
  dag_limit_ = 0;
  total_worker_cnt_ = 0;
  work_thread_num_ = 0;
  total_running_task_cnt_ = 0;
  scheduled_task_cnt_ = 0;
  MEMSET(running_task_cnts_, 0, sizeof(running_task_cnts_));
  MEMSET(dag_cnts_, 0, sizeof(dag_cnts_));
  MEMSET(scheduled_task_cnts_, 0, sizeof(scheduled_task_cnts_));
  MEMSET(dag_net_cnts_, 0, sizeof(dag_net_cnts_));
  waiting_workers_.reset();
  running_workers_.reset();
  TG_DESTROY(tg_id_);
  COMMON_LOG(INFO, "ObTenantDagScheduler destroyed");
}

void ObTenantDagScheduler::free_dag(ObIDag &dag, ObIDag *parent_dag)
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  // free dag from parent_dag::children
  if (OB_NOT_NULL(parent_dag)) {
    const ObINodeWithChild *child_node = static_cast<const ObINodeWithChild *>(&dag);
    if (OB_FAIL(parent_dag->erase_children(child_node))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to erase child", K(ret), KP(child_node), KP(parent_dag));
        ob_abort();
      }
    }
  }
  // erase from dag_net
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(dag_net = dag.get_dag_net())) {
    if (OB_FAIL(dag_net->erase_dag_from_dag_net(dag))) {
      LOG_WARN("failed to erase dag from dag_net", K(ret), KP(dag_net), K(dag));
      ob_abort();
    }
  }
  // clear children indegree
  dag.reset_children();
  // free
  inner_free_dag(dag);
}

void ObTenantDagScheduler::inner_free_dag(ObIDag &dag)
{
  if (OB_UNLIKELY(nullptr != dag.prev_ || nullptr != dag.next_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "dag is in dag_list", K(dag), K(dag.prev_), K(dag.next_));
  }
  ObIAllocator &allocator = get_allocator(dag.is_ha_dag());
  dag.~ObIDag();
  allocator.free(&dag);
}

void ObTenantDagScheduler::get_default_config()
{
  int64_t threads_sum = 0;
  for (int64_t i = 0; i < ObDagPrio::DAG_PRIO_MAX; ++i) { // calc sum of default_low_limit
    low_limits_[i] = OB_DAG_PRIOS[i].score_; // temp solution
    up_limits_[i] = OB_DAG_PRIOS[i].score_;
    threads_sum += up_limits_[i];
  }
  work_thread_num_ = threads_sum;
  default_work_thread_num_ = threads_sum;

  COMMON_LOG(INFO, "calc default config", K_(work_thread_num), K_(default_work_thread_num));
}

int ObTenantDagScheduler::add_dag_into_list_and_map_(
    const ObDagListIndex list_index,
    ObIDag *dag,
    const bool emergency)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(list_index >= DAG_LIST_MAX || nullptr == dag)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(list_index), K(dag));
  } else if (OB_FAIL(dag_map_.set_refactored(dag, dag))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_EAGAIN;
    } else {
      COMMON_LOG(WARN, "failed to set dag_map", K(ret), KPC(dag));
    }
  } else {
    bool add_ret = false;
    if (!emergency) {
      add_ret = dag_list_[list_index].add_last(dag, dag->get_priority());
    } else {
      add_ret = dag_list_[list_index].add_first(dag, dag->get_priority());
    }
    if (!add_ret) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "failed to add dag to ready_dag_list", K(ret), K(dag->get_priority()),
          K(list_index), KPC(dag));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(dag_map_.erase_refactored(dag))) {
        COMMON_LOG(ERROR, "failed to erase dag from dag_map", K(tmp_ret), KPC(dag));
        ob_abort();
      }
    } else {
      dag->set_list_idx(list_index);
    }
  }
  return ret;
}

int ObTenantDagScheduler::add_dag_net(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arugment", KP(dag_net));
  } else if (OB_UNLIKELY(!dag_net->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KPC(dag_net));
  } else {
    ObMutexGuard guard(dag_net_map_lock_);
    if (OB_FAIL(dag_net_map_[RUNNING_DAG_NET_MAP].set_refactored(dag_net, dag_net))) {
      if (OB_HASH_EXIST != ret) {
        COMMON_LOG(WARN, "failed to set running_dag_net_map", K(ret), KPC(dag_net));
      }
    } else if (OB_FAIL(dag_net_id_map_.set_refactored(dag_net->dag_id_, dag_net))) {
      COMMON_LOG(WARN, "failed to add dag net id into map", K(ret), KPC(dag_net));

      if (OB_HASH_EXIST == ret) {
        const ObIDagNet *exist_dag_net = nullptr;
        if (OB_TMP_FAIL(dag_net_id_map_.get_refactored(dag_net->dag_id_, exist_dag_net))) {
          COMMON_LOG(WARN, "failed to get dag net from dag net id map", K(tmp_ret), KPC(dag_net));
        } else {
          COMMON_LOG(WARN, "exist dag net is", KPC(dag_net), KPC(exist_dag_net));
        }
      }

      if (OB_TMP_FAIL(dag_net_map_[RUNNING_DAG_NET_MAP].erase_refactored(dag_net))) {
        COMMON_LOG(ERROR, "failed to erase from running_dag_net_map", K(tmp_ret), KPC(dag_net));
        ob_abort();
      }
    } else if (!blocking_dag_net_list_.add_last(dag_net)) {// add into blocking_dag_net_list
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "failed to add into blocking_dag_net_list", K(ret), KPC(dag_net));

      if (OB_TMP_FAIL(dag_net_id_map_.erase_refactored(dag_net->dag_id_))) {
        COMMON_LOG(ERROR, "failed to erase from dag net id from map", K(tmp_ret), KPC(dag_net));
        ob_abort();
      }

      if (OB_TMP_FAIL(dag_net_map_[RUNNING_DAG_NET_MAP].erase_refactored(dag_net))) {
        COMMON_LOG(ERROR, "failed to erase from running_dag_net_map", K(tmp_ret), KPC(dag_net));
        ob_abort();
      }
    } else {
      ++dag_net_cnts_[dag_net->get_type()];
      dag_net->add_time_ = ObTimeUtility::fast_current_time();
      COMMON_LOG(INFO, "add dag net success", KP(dag_net), "add_time", dag_net->add_time_,
          "dag_net_type_cnts", dag_net_cnts_[dag_net->get_type()]);
    }
  }
  return ret;
}

int ObTenantDagScheduler::add_dag(
     ObIDag *dag,
     const bool emergency,
     const bool check_size_overflow/* = true*/)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", KP(dag));
  } else if (OB_UNLIKELY(!dag->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KPC(dag));
  } else {
    ObThreadCondGuard guard(scheduler_sync_);
    if (OB_FAIL(inner_add_dag(emergency, check_size_overflow, dag))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("failed to inner add dag", K(ret), KPC(dag));
      }
    }
  }
  return ret;
}

void ObTenantDagScheduler::dump_dag_status(const bool force_dump/*false*/)
{
  if (force_dump || REACH_TENANT_TIME_INTERVAL(DUMP_DAG_STATUS_INTERVAL)) {
    int64_t scheduled_task_cnt = 0;
    int64_t running_task[ObDagPrio::DAG_PRIO_MAX];
    int64_t low_limits[ObDagPrio::DAG_PRIO_MAX];
    int64_t up_limits[ObDagPrio::DAG_PRIO_MAX];
    int64_t dag_count[ObDagType::DAG_TYPE_MAX];
    int64_t scheduled_task_count[ObDagType::DAG_TYPE_MAX];
    int64_t dag_net_count[ObDagNetType::DAG_NET_TYPE_MAX];
    int64_t ready_dag_count[ObDagPrio::DAG_PRIO_MAX];
    int64_t waiting_dag_count[ObDagPrio::DAG_PRIO_MAX];
    {
      ObThreadCondGuard guard(scheduler_sync_);
      scheduled_task_cnt = scheduled_task_cnt_;
      scheduled_task_cnt_ = 0;
      for (int64_t i = 0; i < ObDagPrio::DAG_PRIO_MAX; ++i) {
        running_task[i] = running_task_cnts_[i];
        low_limits[i] = low_limits_[i];
        up_limits[i] = up_limits_[i];
        ready_dag_count[i] = dag_list_[READY_DAG_LIST].size(i);
        waiting_dag_count[i] = dag_list_[WAITING_DAG_LIST].size(i);
      }
      for (int64_t i = 0; i < ObDagType::DAG_TYPE_MAX; ++i) {
        dag_count[i] = dag_cnts_[i];
        scheduled_task_count[i] = scheduled_task_cnts_[i];
      }
      MEMSET(scheduled_task_cnts_, 0, sizeof(scheduled_task_cnts_));
      COMMON_LOG(INFO, "dump_dag_status", K_(dag_cnt), "map_size", dag_map_.size());
    }
    {
      ObMutexGuard guard(dag_net_map_lock_);
      for (int64_t i = 0; i < ObDagNetType::DAG_NET_TYPE_MAX; ++i) {
        dag_net_count[i] = dag_net_cnts_[i];
      }

      COMMON_LOG(INFO, "dump_dag_status",
          "running_dag_net_map_size", dag_net_map_[RUNNING_DAG_NET_MAP].size(),
          "blocking_dag_net_list_size", blocking_dag_net_list_.get_size());
    }

    for (int64_t i = 0; i < ObDagPrio::DAG_PRIO_MAX; ++i) {
      COMMON_LOG(INFO, "dump_dag_status", "priority", OB_DAG_PRIOS[i].dag_prio_str_,
          "low_limit", low_limits[i],
          "up_limit", up_limits[i],
          "running_task", running_task[i],
          "ready_dag_count", ready_dag_count[i],
          "waiting_dag_count", waiting_dag_count[i]);
    }
    for (int64_t i = 0; i < ObDagType::DAG_TYPE_MAX; ++i) {
      COMMON_LOG(INFO, "dump_dag_status", "type", OB_DAG_TYPES[i], "dag_count", dag_count[i]);
      COMMON_LOG(INFO, "dump_dag_status", "type", OB_DAG_TYPES[i], "scheduled_task_count", scheduled_task_count[i]);
    }
    for (int64_t i = 0; i < ObDagNetType::DAG_NET_TYPE_MAX; ++i) {
      COMMON_LOG(INFO, "dump_dag_status[DAG_NET]", "type", OB_DAG_NET_TYPES[i].dag_net_type_str_,
          "dag_net_count", dag_net_count[i]);
    }

    COMMON_LOG(INFO, "dump_dag_status", K_(total_worker_cnt), K_(total_running_task_cnt), K_(work_thread_num), K(scheduled_task_cnt));
  }

}

#define ADD_DAG_SCHEDULER_INFO(value_type, key_str, value) \
  { \
    info_list[idx].tenant_id_ = MTL_ID(); \
    info_list[idx].value_type_ = value_type; \
    strcpy(info_list[idx].key_, key_str); \
    info_list[idx].value_ = value; \
    (void)scheduler_infos.push_back(&info_list[idx++]); \
  }

int ObTenantDagScheduler::gene_basic_info(
    ObDagSchedulerInfo *info_list,
    common::ObIArray<void *> &scheduler_infos,
    int64_t &idx)
{
  int ret = OB_SUCCESS;
  ADD_DAG_SCHEDULER_INFO(ObDagSchedulerInfo::GENERAL, "TOTAL_WORKER_CNT", total_worker_cnt_);
  ADD_DAG_SCHEDULER_INFO(ObDagSchedulerInfo::GENERAL, "TOTAL_DAG_CNT", dag_cnt_);
  ADD_DAG_SCHEDULER_INFO(ObDagSchedulerInfo::GENERAL, "TOTAL_RUNNING_TASK_CNT", total_running_task_cnt_);
  return ret;
}

common::ObIAllocator &ObTenantDagScheduler::get_allocator(const bool is_ha)
{
  common::ObIAllocator *allocator = &mem_context_->get_malloc_allocator();
  if (is_ha) {
    allocator = &ha_mem_context_->get_malloc_allocator();
  }
  return *allocator;
}

int ObTenantDagScheduler::get_all_dag_scheduler_info(
    common::ObIAllocator &allocator,
    common::ObIArray<void *> &scheduler_infos)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  int64_t total_cnt = 3 + 3 * ObDagPrio::DAG_PRIO_MAX + ObDagType::DAG_TYPE_MAX + ObDagNetType::DAG_NET_TYPE_MAX;
  void *buf = nullptr;
  ObDagSchedulerInfo *info_list = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObDagSchedulerInfo) * total_cnt))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "failed to alloc scheduler info", K(ret));
  } else {
    info_list = reinterpret_cast<ObDagSchedulerInfo *>(new (buf) ObDagSchedulerInfo[total_cnt]);
  }
  if (OB_SUCC(ret)) {
    ObThreadCondGuard guard(scheduler_sync_);
    if (OB_FAIL(gene_basic_info(info_list, scheduler_infos, idx))) {
      COMMON_LOG(WARN, "failed to generate basic info", K(ret));
    } else {
      for (int64_t i = 0; i < ObDagPrio::DAG_PRIO_MAX; ++i) {
        ADD_DAG_SCHEDULER_INFO(ObDagSchedulerInfo::LOW_LIMIT, OB_DAG_PRIOS[i].dag_prio_str_, low_limits_[i]);
        ADD_DAG_SCHEDULER_INFO(ObDagSchedulerInfo::UP_LIMIT, OB_DAG_PRIOS[i].dag_prio_str_, up_limits_[i]);
        ADD_DAG_SCHEDULER_INFO(ObDagSchedulerInfo::RUNNING_TASK_CNT, OB_DAG_PRIOS[i].dag_prio_str_, running_task_cnts_[i]);
      }
      for (int64_t i = 0; i < ObDagType::DAG_TYPE_MAX; ++i) {
        ADD_DAG_SCHEDULER_INFO(ObDagSchedulerInfo::DAG_COUNT, OB_DAG_TYPES[i].dag_type_str_, dag_cnts_[i]);
      }
      for (int64_t i = 0; i < ObDagNetType::DAG_NET_TYPE_MAX; ++i) {
        ADD_DAG_SCHEDULER_INFO(ObDagSchedulerInfo::DAG_NET_COUNT, OB_DAG_NET_TYPES[i].dag_net_type_str_, dag_net_cnts_[i]);
      }
    }
  }
  return ret;
}

#define ADD_DAG_INFO(cur, list_info) \
  { \
    cur->gene_dag_info(info_list[idx], list_info); \
    (void)dag_infos.push_back(&info_list[idx++]); \
  }

int ObTenantDagScheduler::get_all_dag_info(
    common::ObIAllocator &allocator,
    common::ObIArray<void *> &dag_infos)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  int64_t total_dag_cnt = 0;
  {
    ObThreadCondGuard guard(scheduler_sync_);
    for (int64_t i = 0; i < ObDagType::DAG_TYPE_MAX; ++i) {
      total_dag_cnt += dag_cnts_[i];
    }
  }
  if (total_dag_cnt > 0) {
    total_dag_cnt = MIN(total_dag_cnt, MAX_SHOW_DAG_CNT_PER_PRIO * 2 * ObDagPrio::DAG_PRIO_MAX + MAX_SHOW_DAG_NET_CNT_PER_PRIO);
    void * buf = nullptr;
    ObDagInfo *info_list = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObDagInfo) * total_dag_cnt))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to alloc scheduler info", K(ret));
    } else {
      info_list = reinterpret_cast<ObDagInfo *>(new (buf) ObDagInfo[total_dag_cnt]);
    }
    if (OB_SUCC(ret)) {
      {
        ObThreadCondGuard guard(scheduler_sync_);
        for (int64_t i = 0; i < ObDagPrio::DAG_PRIO_MAX && idx < total_dag_cnt; ++i) {
          int64_t prio_cnt = 0;
          // get ready dag list
          ObIDag *head = dag_list_[READY_DAG_LIST].get_head(i);
          ObIDag *cur = head->get_next();
          while (head != cur && idx < total_dag_cnt && prio_cnt++ < MAX_SHOW_DAG_CNT_PER_PRIO) {
            ADD_DAG_INFO(cur, "READY_DAG_LIST");
            cur = cur->get_next();
          }
          // get waiting dag list
          head = dag_list_[WAITING_DAG_LIST].get_head(i);
          cur = head->get_next();
          while (head != cur && idx < total_dag_cnt && prio_cnt++ < MAX_SHOW_DAG_CNT_PER_PRIO * 2) {
            ADD_DAG_INFO(cur, "WAITING_DAG_LIST");
            cur = cur->get_next();
          }
        }
      } // end of scheduler_sync_
      {
        ObMutexGuard guard(dag_net_map_lock_);
        DagNetMap::iterator iter = dag_net_map_[RUNNING_DAG_NET_MAP].begin();
        for (; iter != dag_net_map_[RUNNING_DAG_NET_MAP].end() && idx < total_dag_cnt; ++iter) { // ignore failure
          ADD_DAG_INFO(iter->second, "RUNNING_DAG_NET_MAP");
        }

        ObIDagNet *head = blocking_dag_net_list_.get_header();
        ObIDagNet *cur = head->get_next();
        while (NULL != cur && head != cur && idx < total_dag_cnt) {
          ADD_DAG_INFO(cur, "BLOCKING_DAG_NET_MAP");
          cur = cur->get_next();
        }
      } // end of dag_net_map_lock_
    }
  }
  return ret;
}

int ObTenantDagScheduler::get_all_compaction_dag_info(
    ObIAllocator &allocator,
    ObIArray<compaction::ObTabletCompactionProgress *> &progress_array)
{
  int ret = OB_SUCCESS;
  int64_t total_dag_cnt = 0;

  { // get dag total count
    ObThreadCondGuard guard(scheduler_sync_);
    for (int64_t i = 0; i < ObIDag::MergeDagTypeCnt; ++i) {
      total_dag_cnt += dag_cnts_[ObIDag::MergeDagType[i]];
    }
  }
  if (total_dag_cnt > 0) {
    total_dag_cnt = MIN(total_dag_cnt, MAX_SHOW_DAG_CNT_PER_PRIO * ObIDag::MergeDagTypeCnt);
    void * buf = nullptr;
    if (NULL == (buf = allocator.alloc(sizeof(ObDagInfo) * total_dag_cnt))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to alloc dag", K(ret), K(total_dag_cnt));
    } else {
      compaction::ObTabletCompactionProgress *progress =
          reinterpret_cast<compaction::ObTabletCompactionProgress*>(new (buf) compaction::ObTabletCompactionProgress[total_dag_cnt]);
      int tmp_ret = OB_SUCCESS;
      int idx = 0;
      {
        ObThreadCondGuard guard(scheduler_sync_);
        for (int64_t i = 0; i < ObIDag::MergeDagPrioCnt && idx < total_dag_cnt; ++i) {
          int64_t prio_cnt = 0;
          ObIDag *head = dag_list_[READY_DAG_LIST].get_head(ObIDag::MergeDagPrio[i]);
          ObIDag *cur = head->get_next();
          while (head != cur && idx < total_dag_cnt && prio_cnt < MAX_SHOW_DAG_CNT_PER_PRIO) {
            if (OB_UNLIKELY(OB_TMP_FAIL(cur->gene_compaction_info(progress[idx])))) {
              if (OB_EAGAIN != tmp_ret && OB_NOT_IMPLEMENT != tmp_ret) {
                COMMON_LOG(WARN, "failed to generate compaction dag info", K(tmp_ret), KPC(cur));
              }
            } else {
              (void)progress_array.push_back(&progress[idx++]);
              prio_cnt++;
            }
            cur = cur->get_next();
          }
        }
      }
    }
    // the allocated buf will be release when select ends
  }
  return ret;
}

int ObTenantDagScheduler::get_minor_exe_dag_info(
    const compaction::ObTabletMergeDagParam &param,
    ObIArray<share::ObScnRange> &merge_range_array)
{
  int ret = OB_SUCCESS;
  compaction::ObTabletMergeExecuteDag dag;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObDagScheduler is not inited", K(ret));
  } else if (OB_FAIL(dag.init_by_param(&param))) {
    STORAGE_LOG(WARN, "failed to init dag", K(ret), K(param));
  } else {
    ObThreadCondGuard guard(scheduler_sync_);
    ObIDag *head = dag_list_[READY_DAG_LIST].get_head(ObDagPrio::DAG_PRIO_COMPACTION_MID);
    ObIDag *cur = head->get_next();
    while (head != cur && OB_SUCC(ret)) {
      if (cur->get_type() == ObDagType::DAG_TYPE_MERGE_EXECUTE) {
        compaction::ObTabletMergeExecuteDag *other_dag = static_cast<compaction::ObTabletMergeExecuteDag *>(cur);
        if (other_dag->belong_to_same_tablet(&dag)) {
          if (OB_FAIL(merge_range_array.push_back(other_dag->get_merge_range()))) {
            LOG_WARN("failed to push merge range into array", K(ret), K(other_dag->get_merge_range()));
          }
        }
      }
      cur = cur->get_next();
    } // end of while

    // get meta major
    ObIDag *stored_dag = nullptr;
    dag.merge_type_ = META_MAJOR_MERGE;
    compaction::ObTabletMergeExecuteDag *other_dag = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dag_map_.get_refactored(&dag, stored_dag))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get from dag map", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(other_dag = static_cast<compaction::ObTabletMergeExecuteDag *>(stored_dag))) {
      ret = OB_ERR_SYS;
      LOG_WARN("dag is null", K(ret));
    } else if (OB_FAIL(merge_range_array.push_back(other_dag->get_merge_range()))) {
      LOG_WARN("failed to push merge range into array", K(ret), K(other_dag->get_merge_range()));
    }
  }
  return ret;
}

int ObTenantDagScheduler::check_ls_compaction_dag_exist_with_cancel(
  const ObLSID &ls_id,
  bool &exist)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  exist = false;
  compaction::ObTabletMergeDag *dag = nullptr;
  ObIDagNet *unused_erase_dag_net = nullptr;
  ObIDag *cancel_dag = nullptr;
  bool cancel_flag = false;
  int64_t cancel_dag_cnt = 0;
  if (has_set_stop()) { // scheduler thread is stopped
    exist = false;
    COMMON_LOG(INFO, "dag scheduler is stopped", KR(ret), K(exist));
  } else {
    ObThreadCondGuard guard(scheduler_sync_);
    for (int64_t i = 0; i < ObIDag::MergeDagPrioCnt; ++i) {
      ObIDag *head = dag_list_[READY_DAG_LIST].get_head(ObIDag::MergeDagPrio[i]);
      ObIDag *cur = head->get_next();
      while (head != cur) {
        dag = static_cast<compaction::ObTabletMergeDag *>(cur);
        cancel_flag = (ls_id == dag->get_ls_id());

        if (cancel_flag) {
          if (cur->get_dag_status() == ObIDag::DAG_STATUS_READY) {
            cancel_dag = cur;
            cur = cur->get_next();
            if (OB_UNLIKELY(nullptr != cancel_dag->get_dag_net())) {
              tmp_ret = OB_ERR_UNEXPECTED;
              COMMON_LOG(WARN, "compaction dag should not in dag net", KR(tmp_ret));
            } else if (OB_TMP_FAIL(finish_dag_(ObIDag::DAG_STATUS_ABORT, *cancel_dag, unused_erase_dag_net, false/*try_move_child*/))) {
              COMMON_LOG(WARN, "failed to erase dag", K(tmp_ret), KPC(cancel_dag));
              ob_abort();
            } else {
              ++cancel_dag_cnt;
            }
          } else {
            exist = true;
            cur = cur->get_next();
          }
        } else {
          cur = cur->get_next();
        }
      }
    }
  } // end of scheduler_sync_
  if (OB_SUCC(ret)) {
    COMMON_LOG(INFO, "cancel dag when check ls compaction dag exist", KR(ret), K(cancel_dag_cnt), K(exist));
  }
  return ret;
}

// get oldest minor execute dag
int ObTenantDagScheduler::diagnose_minor_exe_dag(
    const compaction::ObMergeDagHash *merge_dag_info,
    compaction::ObDiagnoseTabletCompProgress &progress)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObDagScheduler is not inited", K(ret));
  } else if (OB_ISNULL(merge_dag_info)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arugment", K(ret), KP(merge_dag_info));
  } else {
    ObThreadCondGuard guard(scheduler_sync_);
    ObIDag *head = dag_list_[READY_DAG_LIST].get_head(ObDagPrio::DAG_PRIO_COMPACTION_MID);
    ObIDag *cur = head->get_next();
    bool find = false;
    while (head != cur && OB_SUCC(ret)) {
      if (cur->get_type() == ObDagType::DAG_TYPE_MERGE_EXECUTE) {
        compaction::ObTabletMergeExecuteDag *exe_dag = static_cast<compaction::ObTabletMergeExecuteDag *>(cur);
        if (exe_dag->belong_to_same_tablet(merge_dag_info)) {
          if (OB_FAIL(exe_dag->diagnose_compaction_info(progress))) {
            if (OB_NOT_IMPLEMENT != ret) {
              LOG_WARN("failed to diagnose compaction dag", K(ret), K(exe_dag));
            } else {
              ret = OB_SUCCESS;
            }
          } else {
            find = true;
            break;
          }
        }
      }
      cur = cur->get_next();
    } // end of while
    if (OB_SUCC(ret) && !find) {
      ret = OB_HASH_NOT_EXIST;
    }
  }
  return ret;
}

// get max estimated_finish_time to update server_progress
int ObTenantDagScheduler::get_max_major_finish_time(const int64_t version, int64_t &estimated_finish_time)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObDagScheduler is not inited", K(ret));
  } else if (OB_UNLIKELY(version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(version));
  } else {
    compaction::ObTabletMergeDag *dag = nullptr;
    estimated_finish_time = 0;
    ObThreadCondGuard guard(scheduler_sync_);
    ObIDag *head = dag_list_[READY_DAG_LIST].get_head(ObDagPrio::DAG_PRIO_COMPACTION_LOW);
    ObIDag *cur = head->get_next();
    compaction::ObTabletMergeCtx *ctx = nullptr;
    while (head != cur) {
      if (ObDagType::DAG_TYPE_MAJOR_MERGE == cur->get_type()) {
        dag = static_cast<compaction::ObTabletMergeDag *>(cur);
        if (ObIDag::DAG_STATUS_NODE_RUNNING == dag->get_dag_status()) {
          if (nullptr != (ctx = dag->get_ctx()) && ctx->param_.merge_version_ == version) {
            if (OB_NOT_NULL(ctx->merge_progress_)
                && ctx->merge_progress_->get_estimated_finish_time() > estimated_finish_time) {
              estimated_finish_time = ctx->merge_progress_->get_estimated_finish_time();
            }
          }
        } else {
          break;
        }
      }
      cur = cur->get_next();
    }
  }
  return ret;
}

int ObTenantDagScheduler::diagnose_dag(
    const ObIDag *dag,
    compaction::ObDiagnoseTabletCompProgress &progress)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObDagScheduler is not inited", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arugment", K(ret), KP(dag));
  } else {
    ObThreadCondGuard guard(scheduler_sync_);
    ObIDag *stored_dag = nullptr;
    if (OB_FAIL(dag_map_.get_refactored(dag, stored_dag))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get from dag map", K(ret));
      }
    } else if (OB_ISNULL(stored_dag)) {
      ret = OB_ERR_SYS;
      LOG_WARN("dag is null", K(ret));
    } else if (OB_FAIL(stored_dag->diagnose_compaction_info(progress))) {
      if (OB_NOT_IMPLEMENT != ret) {
        LOG_WARN("failed to generate compaction info", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

void ObTenantDagScheduler::run1()
{
  int ret = OB_SUCCESS;
  lib::set_thread_name("DagScheduler");
  while (!has_set_stop()) {
    dump_dag_status();
    loop_dag_net();
    {
      ObThreadCondGuard guard(scheduler_sync_);
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
    } // end of lock
  }
}

void ObTenantDagScheduler::notify()
{
  ObThreadCondGuard cond_guard(scheduler_sync_);
  scheduler_sync_.signal();
}

int ObTenantDagScheduler::deal_with_fail_task(
    ObITask &task,
    ObIDag &dag,
    const int error_code,
    bool &retry_flag)
{
  int ret = OB_SUCCESS;
  retry_flag = false;
  if (task.check_can_retry() && ObIDag::DAG_STATUS_NODE_FAILED != dag.get_dag_status()) {
    // first failed task in dag
    COMMON_LOG(INFO, "task retry", K(ret), K(dag), K(&task));
    retry_flag = true;
    dag.reset_task_running_status(task, ObITask::TASK_STATUS_RETRY); // need protect by lock
    if (0 == dag.get_running_task_count()
        && OB_FAIL(move_dag_to_list_(&dag, READY_DAG_LIST, WAITING_DAG_LIST))) {
      COMMON_LOG(WARN, "failed to move dag to waiting list", K(ret), K(dag));
    } else {
      COMMON_LOG(DEBUG, "success to move dag to waiting list", K(ret), K(dag), K(task),
          "list_cnt", dag_list_[READY_DAG_LIST].size(dag.get_priority()));
    }
  }
  if (!retry_flag && ObIDag::DAG_STATUS_NODE_FAILED != dag.get_dag_status()) {
    // set error_code when first failed
    dag.set_dag_status(ObIDag::DAG_STATUS_NODE_FAILED);
    dag.set_dag_ret(error_code);
  }

  return ret;
}

int ObTenantDagScheduler::deal_with_fail_dag(ObIDag &dag, bool &retry_flag)
{
  int ret = OB_SUCCESS;
  // dag retry is triggered by last finish task
  if (1 == dag.get_running_task_count() && dag.check_can_retry()) {
    COMMON_LOG(INFO, "dag retry", K(ret), K(dag));
    if (OB_FAIL(dag.reset_status_for_retry())) { // clear task/running_info and init again
      COMMON_LOG(WARN, "failed to reset status for retry", K(ret), K(dag));
    } else if (OB_FAIL(move_dag_to_list_(&dag, READY_DAG_LIST, WAITING_DAG_LIST))) {
      COMMON_LOG(WARN, "failed to move dag to waiting list", K(ret), K(dag));
    } else {
      retry_flag = true;
    }
  }
  return ret;
}

int ObTenantDagScheduler::deal_with_finish_task(ObITask &task, ObTenantDagWorker &worker, int error_code)
{
  int ret = OB_SUCCESS;
  int64_t tmp_ret = OB_SUCCESS;
  ObIDag *dag = nullptr;
  ObIDagNet *erase_dag_net = nullptr;
  int64_t prio = 0;
  bool finish_task_flag = true;
  bool retry_flag = false;
  if (OB_ISNULL(dag = task.get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_UNLIKELY(!dag->is_valid_type())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "invalid dag", K(ret), K(*dag));
  } else if (FALSE_IT(prio = dag->get_priority())) {
  } else {
    ObThreadCondGuard guard(scheduler_sync_);

    if (OB_SUCCESS != error_code) {
      if (OB_FAIL(deal_with_fail_task(task, *dag, error_code, retry_flag))) {
        COMMON_LOG(WARN, "failed to deal with fail task", K(ret), K(task), KPC(dag));
      }
    }

    if (OB_FAIL(ret) || retry_flag) {
    } else if (dag->is_dag_failed()) {
      // dag can retry & this task is the last running task
      if (OB_FAIL(deal_with_fail_dag(*dag, retry_flag))) {
        COMMON_LOG(WARN, "failed to deal with fail dag", K(ret), KPC(dag));
      }
    }

    if (OB_FAIL(ret)) {
      dag->set_dag_status(ObIDag::DAG_STATUS_NODE_FAILED);
      dag->set_dag_ret(ret);
      finish_task_flag = false;
      COMMON_LOG(WARN, "failed to deal with finish task in retry process", K(ret), KPC(dag));
      ret = OB_SUCCESS;
    } else if (retry_flag) {
      finish_task_flag = false;
    }

    if (finish_task_flag
        && OB_FAIL(finish_task_in_dag(task, *dag, erase_dag_net))) {
      COMMON_LOG(WARN, "failed to finish task", K(ret), KPC(dag));
    }
  }
  if (OB_SUCC(ret) && nullptr != erase_dag_net) {
    if (OB_FAIL(finish_dag_net(erase_dag_net))) {
      COMMON_LOG(WARN, "failed to finish dag net", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // free worker after free dag since add_dag may be called in the deconstructor
    // of dag, which may lead to dead lock
    ObThreadCondGuard guard(scheduler_sync_);
    --running_task_cnts_[prio];
    --total_running_task_cnt_;
    running_workers_.remove(&worker, prio);
    free_workers_.add_last(&worker);
    worker.set_task(NULL);
    scheduler_sync_.signal();
  }

  return ret;
}

int ObTenantDagScheduler::finish_task_in_dag(ObITask &task, ObIDag &dag, ObIDagNet *&finish_dag_net)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (0 == dag.get_task_list_count()) {
    // dag::task_list is empty means dag have entered retry process, but failed
  } else if (OB_TMP_FAIL(dag.finish_task(task))) {
    dag.set_dag_status(ObIDag::DAG_STATUS_NODE_FAILED);
    dag.set_dag_ret(tmp_ret);
    COMMON_LOG(WARN, "failed to finish task", K(tmp_ret));
  } else {
    COMMON_LOG(DEBUG, "success to finish task", K(tmp_ret), K(&dag));
  }

  if (dag.has_finished()) {
    ObIDag::ObDagStatus status =
        dag.is_dag_failed() ? ObIDag::DAG_STATUS_ABORT : ObIDag::DAG_STATUS_FINISH;
    if (OB_FAIL(finish_dag_(status, dag, finish_dag_net, true/*try_move_child*/))) {
      COMMON_LOG(WARN, "failed to finish dag", K(ret), K(dag));
      ob_abort();
    }
  }
  return ret;
}

int ObTenantDagScheduler::finish_dag_net(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(dag_net)) {
    if (OB_TMP_FAIL(dag_net->clear_dag_net_ctx())) {
      COMMON_LOG(WARN, "failed to clear dag net ctx", K(tmp_ret), KPC(dag_net));
    }

    {
      ObMutexGuard guard(dag_net_map_lock_);
      if (OB_FAIL(dag_net_map_[RUNNING_DAG_NET_MAP].erase_refactored(dag_net))) {
        COMMON_LOG(ERROR, "failed to erase dag from running_dag_net_map", K(ret), KPC(dag_net));
        ob_abort();
      } else if (OB_FAIL(dag_net_id_map_.erase_refactored(dag_net->dag_id_))) {
        COMMON_LOG(ERROR, "failed to erase dag from running_dag_net_map", K(ret), KPC(dag_net));
        ob_abort();
      }
      --dag_net_cnts_[dag_net->get_type()];
    }
    if (OB_SUCC(ret)) {
      COMMON_LOG(INFO, "dag net finished", K(ret), KPC(dag_net));
      free_dag_net(dag_net);
    }
  }
  return ret;
}

int ObTenantDagScheduler::finish_dag_(
    const ObIDag::ObDagStatus status,
    ObIDag &dag,
    ObIDagNet *&finish_dag_net,
    const bool try_move_child)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  finish_dag_net = nullptr;

  if (OB_FAIL(dag.finish(status))) {
    COMMON_LOG(WARN, "dag finished failed", K(ret), K(dag), "dag_ret", dag.get_dag_ret());
  } else if (try_move_child && OB_FAIL(try_move_child_to_ready_list(dag))) {
    LOG_WARN("failed to try move child to ready list", K(ret), K(&dag));
  } else if (OB_FAIL(erase_dag_(dag))) {
    COMMON_LOG(ERROR, "failed to erase dag from dag_map", K(ret), K(dag));
  } else {
    LOG_INFO("dag finished", "dag_ret", dag.get_dag_ret(),
        "runtime", ObTimeUtility::fast_current_time() - dag.start_time_,
        K_(dag_cnt), K(dag_cnts_[dag.get_type()]), K(&dag), K(dag));
    if (OB_TMP_FAIL(MTL(ObDagWarningHistoryManager *)->add_dag_warning_info(&dag))) { // ignore failure
      COMMON_LOG(WARN, "failed to add dag warning info", K(tmp_ret), K(dag));
    }
    if (OB_TMP_FAIL(ObSysTaskStatMgr::get_instance().del_task(dag.get_dag_id()))) {
      STORAGE_LOG(WARN, "failed to del sys task", K(tmp_ret), K(dag.get_dag_id()));
    }
    if (OB_TMP_FAIL(dag.report_result())) {
      COMMON_LOG(WARN, "failed to report result", K(tmp_ret), K(dag));
    }

    ObIDagNet *dag_net = dag.get_dag_net();
    if (OB_ISNULL(dag_net)) {
    } else if (dag_net->check_finished_and_mark_stop()) {
      finish_dag_net = dag_net;
    }

    inner_free_dag(dag); // free after log print
  }
  return ret;
}

int ObTenantDagScheduler::move_dag_to_list_(
    ObIDag *dag,
    enum ObDagListIndex from_list_index,
    enum ObDagListIndex to_list_index)
{
  int ret = OB_SUCCESS;
  const int64_t prio = dag->get_priority();
  if (OB_UNLIKELY(from_list_index >= DAG_LIST_MAX || to_list_index >= DAG_LIST_MAX
      || nullptr == dag
      || from_list_index != dag->get_list_idx())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "list index is invalid", K(ret), KPC(dag), K(from_list_index));
  } else if (!dag_list_[from_list_index].remove(dag, prio)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "failed to remove dag from dag list", K(from_list_index), K(prio), KPC(dag));
  } else if (!dag_list_[to_list_index].add_last(dag, prio)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "failed to add dag to dag list", K(ret), K(to_list_index), K(prio), KPC(dag));
    ob_abort();
  } else {
    dag->set_list_idx(to_list_index);
  }
  return ret;
}

int ObTenantDagScheduler::try_switch(ObTenantDagWorker &worker, const int64_t src_prio, const int64_t dest_prio, bool &need_pause)
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

bool ObTenantDagScheduler::try_switch(ObTenantDagWorker &worker)
{
  bool need_pause = false;
  const int64_t priority = worker.get_task()->get_dag()->get_priority();
  ObThreadCondGuard guard(scheduler_sync_);
  // forbid switching after stop sign has been set, which means running workers won't pause any more
  if (!has_set_stop()) {
    if (total_running_task_cnt_ > work_thread_num_
        && running_task_cnts_[priority] > low_limits_[priority]) {
      need_pause = true;
      for (int64_t i = priority + 1; need_pause && i < ObDagPrio::DAG_PRIO_MAX; ++i) {
        // a lower priority who have excessive threads is a better candidate to stop
        if (running_task_cnts_[i] > low_limits_[i]) {
          need_pause = false;
        }
      }
      if (need_pause) {
        pause_worker(worker, priority);
      }
    } else if (running_task_cnts_[priority] > low_limits_[priority]
        && total_running_task_cnt_ <= work_thread_num_) {
      for (int64_t i = 0; !need_pause && i < ObDagPrio::DAG_PRIO_MAX; ++i) {
        if (i != priority && running_task_cnts_[i] < low_limits_[i]) {
          try_switch(worker, priority, i, need_pause);
        }
      }
      for (int64_t i = 0; !need_pause && i < priority; ++i) {
        if (running_task_cnts_[i] < up_limits_[i]) {
          try_switch(worker, priority, i, need_pause);
        }
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

void ObTenantDagScheduler::pause_worker(ObTenantDagWorker &worker, const int64_t priority)
{
  --running_task_cnts_[priority];
  --total_running_task_cnt_;
  running_workers_.remove(&worker, priority);
  waiting_workers_.add_last(&worker, priority);
  COMMON_LOG(INFO, "pause worker", K(*worker.get_task()),
      "priority", OB_DAG_PRIOS[priority].dag_prio_str_,
      K(running_task_cnts_[priority]), K(total_running_task_cnt_));
}

int ObTenantDagScheduler::sys_task_start(ObIDag *dag)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN,"dag is null", K(ret),KP(dag));
  } else if (dag->get_dag_status() != ObIDag::DAG_STATUS_READY) {
    COMMON_LOG(ERROR," dag status error",K(ret),K(dag->get_dag_status()));
  } else {
    ObSysTaskStat sys_task_status;
    sys_task_status.start_time_ = ObTimeUtility::fast_current_time();
    sys_task_status.task_id_ = dag->get_dag_id();
    sys_task_status.tenant_id_ = MTL_ID();
    sys_task_status.task_type_ = OB_DAG_TYPES[dag->get_type()].sys_task_type_;

    int tmp_ret = OB_SUCCESS;
    // allow comment truncation, no need to set ret
    if (OB_TMP_FAIL(dag->fill_comment(sys_task_status.comment_, sizeof(sys_task_status.comment_)))) {
      COMMON_LOG(WARN, "failed to fill comment", K(tmp_ret));
    }
    if (OB_SUCCESS != (ret = ObSysTaskStatMgr::get_instance().add_task(sys_task_status))) {
      COMMON_LOG(WARN, "failed to add sys task", K(ret), K(sys_task_status));
    } else if (OB_SUCCESS != (ret = dag->set_dag_id(sys_task_status.task_id_))) { // may generate task_id in ObSysTaskStatMgr::add_task
      COMMON_LOG(WARN,"failed to set dag id",K(ret), K(sys_task_status.task_id_));
    }
  }
  return ret;
}

int64_t ObTenantDagScheduler::get_dag_count(const ObDagType::ObDagTypeEnum type)
{
  int64_t count = -1;
  if (type >= 0 && type < ObDagType::DAG_TYPE_MAX) {
    ObThreadCondGuard guard(scheduler_sync_);
    count = dag_cnts_[type];
  } else {
    COMMON_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid type", K(type));
  }
  return count;
}

int64_t ObTenantDagScheduler::get_dag_net_count(const ObDagNetType::ObDagNetTypeEnum type)
{
  int64_t count = -1;
  if (type >= 0 && type < ObDagNetType::DAG_NET_TYPE_MAX) {
    ObMutexGuard guard(dag_net_map_lock_);
    count = dag_net_cnts_[type];
  } else {
    COMMON_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid type", K(type));
  }
  return count;

}

int ObTenantDagScheduler::loop_waiting_dag_list(
    const int64_t priority)
{
  int ret = OB_SUCCESS;
  if (!dag_list_[WAITING_DAG_LIST].is_empty(priority)) {
    int64_t moving_dag_cnt = 0;
    ObIDag *head = dag_list_[WAITING_DAG_LIST].get_head(priority);
    ObIDag *cur = head->get_next();
    ObIDag *move_dag = nullptr;
    while (NULL != cur && head != cur && OB_SUCC(ret)) {
      if (0 == cur->get_indegree() && cur->check_can_schedule()) {
        move_dag = cur;
        cur = cur->get_next();
        if (OB_FAIL(move_dag_to_list_(move_dag, WAITING_DAG_LIST, READY_DAG_LIST))) {
          COMMON_LOG(WARN, "failed to move dag to low priority list", K(ret), KPC(move_dag));
        } else {
          ++moving_dag_cnt;
          COMMON_LOG(DEBUG, "move dag to ready list", K(ret), KPC(move_dag),
              "waiting_list_size", dag_list_[WAITING_DAG_LIST].size(priority),
              "ready_list_size", dag_list_[READY_DAG_LIST].size(priority));
        }
      } else {
        if (ObTimeUtility::fast_current_time() - cur->add_time_ > DUMP_DAG_STATUS_INTERVAL)  {
          COMMON_LOG(DEBUG, "print remain waiting dag", K(ret), KPC(cur), "indegree", cur->get_indegree(),
              "check_can_schedule", cur->check_can_schedule());
        }
        cur = cur->get_next();
      }
    } // end of while
    if (OB_SUCC(ret)) {
      COMMON_LOG(INFO, "loop_waiting_dag_list", K(ret), K(priority), K(moving_dag_cnt),
          "remain waiting_list_size", dag_list_[WAITING_DAG_LIST].size(priority),
          "ready_list_size", dag_list_[READY_DAG_LIST].size(priority));
    }
  }
  return ret;
}

bool ObTenantDagScheduler::is_dag_map_full()
{
  bool bret = false;
  if (((double)dag_map_.size() / DEFAULT_MAX_DAG_MAP_CNT) >= ((double)STOP_ADD_DAG_PERCENT / 100)) {
    bret = true;
    if (REACH_TENANT_TIME_INTERVAL(LOOP_PRINT_LOG_INTERVAL))  {
      COMMON_LOG(INFO, "dag map is almost full, stop loop blocking dag_net_map",
          K(dag_map_.size()), "dag_net_map_size", dag_net_map_[RUNNING_DAG_NET_MAP].size(),
          "blocking_dag_net_list_size", blocking_dag_net_list_.get_size());
    }
  }
  return bret;
}

int ObTenantDagScheduler::loop_running_dag_net_map()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  bool erase_map_flag = false;
  int64_t slow_dag_net_cnt = 0;

  ObMutexGuard guard(dag_net_map_lock_);
  DagNetMap::iterator iter = dag_net_map_[RUNNING_DAG_NET_MAP].begin();
  for (; iter != dag_net_map_[RUNNING_DAG_NET_MAP].end(); ++iter) { // ignore failure
    erase_map_flag = false;
    dag_net = iter->second;
    if (OB_ISNULL(dag_net)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag net is unepxected null", K(ret), KP(dag_net));
    } else if (dag_net->check_finished_and_mark_stop()) {
      LOG_WARN("dag net is in finish state", K(ret), KPC(dag_net));
    } else if (dag_net->start_time_ + PRINT_SLOW_DAG_NET_THREASHOLD < ObTimeUtility::fast_current_time()) {
      ++slow_dag_net_cnt;
      LOG_DEBUG("slow dag net", K(ret), KP(dag_net), "dag_net_start_time", dag_net->start_time_);
    }
  }
  COMMON_LOG(INFO, "loop running dag net map", K(ret),
      "running_dag_net_map_size", dag_net_map_[RUNNING_DAG_NET_MAP].size(), K(slow_dag_net_cnt));

  return ret;
}

int ObTenantDagScheduler::loop_blocking_dag_net_list()
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(dag_net_map_lock_);
  ObIDagNet *head = blocking_dag_net_list_.get_header();
  ObIDagNet *cur = head->get_next();
  ObIDagNet *tmp = nullptr;
  int64_t rest_cnt = DEFAULT_MAX_RUNNING_DAG_NET_CNT - (dag_net_map_[RUNNING_DAG_NET_MAP].size() - blocking_dag_net_list_.get_size());
  while (NULL != cur && head != cur && rest_cnt > 0 && !is_dag_map_full()) {
    LOG_DEBUG("loop blocking dag net list", K(ret), KPC(cur), K(rest_cnt));
    if (OB_FAIL(cur->start_running())) { // call start_running function
      COMMON_LOG(WARN, "failed to start running", K(ret), KPC(cur));
      int64_t tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(dag_net_map_[RUNNING_DAG_NET_MAP].erase_refactored(cur))) {
        COMMON_LOG(ERROR, "failed to erase from running_dag_net_map", K(tmp_ret), KPC(cur));
        ob_abort();
      } else if (OB_TMP_FAIL(dag_net_id_map_.erase_refactored(cur->dag_id_))) {
        COMMON_LOG(ERROR, "failed to erase from running_dag_net_id_map", K(tmp_ret), KPC(cur));
        ob_abort();
      } else {
        tmp = cur;
        cur = cur->get_next();
        if (OB_TMP_FAIL(tmp->clear_dag_net_ctx())) {
          COMMON_LOG(ERROR, "failed to clear dag net ctx", K(tmp));
        }
        if (!blocking_dag_net_list_.remove(tmp)) {
          COMMON_LOG(WARN, "failed to remove dag_net from blocking_dag_net_list", K(tmp));
          ob_abort();
        }
        --dag_net_cnts_[tmp->get_type()];
        free_dag_net(tmp);
      }
    } else {
      cur->start_time_ = ObTimeUtility::fast_current_time();
      tmp = cur;
      cur = cur->get_next();
      --rest_cnt;
      if (!blocking_dag_net_list_.remove(tmp)) {
        COMMON_LOG(WARN, "failed to remove dag_net from blocking_dag_net_list", K(tmp));
        ob_abort();
      }
    }
  }
  return ret;
}


int ObTenantDagScheduler::erase_dag_(ObIDag &dag)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dag_map_.erase_refactored(&dag))) {
    COMMON_LOG(ERROR, "failed to erase dag from dag_map", K(ret), K(dag));
    ob_abort();
  } else if (!dag_list_[dag.get_list_idx()].remove(&dag, dag.get_priority())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "failed to remove dag from dag list", K(dag));
  } else {
    --dag_cnt_;
    --dag_cnts_[dag.get_type()];
  }
  return ret;
}

int ObTenantDagScheduler::pop_task_from_ready_list(
    const int64_t priority,
    ObITask *&task)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  task = nullptr;
  if (!dag_list_[READY_DAG_LIST].is_empty(priority)) {
    ObIDag *head = dag_list_[READY_DAG_LIST].get_head(priority);
    ObIDag *cur = head->get_next();
    ObIDag *tmp_dag = nullptr;
    ObITask *ready_task = NULL;
    ObIDag::ObDagStatus dag_status = ObIDag::DAG_STATUS_MAX;
    ObIDagNet *erase_dag_net = nullptr;
    while (NULL != cur && head != cur && OB_SUCC(ret)) {
      bool move_dag_to_waiting_list = false;
      dag_status = cur->get_dag_status();
      if (cur->get_indegree() > 0) {
        move_dag_to_waiting_list = true;
      } else if (ObIDag::DAG_STATUS_READY == dag_status
          || ObIDag::DAG_STATUS_RETRY == dag_status) { // first schedule this dag
        ObIDag::ObDagStatus next_dag_status = dag_status;
        if (OB_NOT_NULL(cur->get_dag_net()) && cur->get_dag_net()->is_cancel()) {
          next_dag_status = ObIDag::DAG_STATUS_NODE_FAILED;
        } else if (!cur->check_can_schedule()) { // cur dag can't be scheduled now
          move_dag_to_waiting_list = true;
        } else { // dag can be scheduled
          if (ObIDag::DAG_STATUS_READY == dag_status) {
            if (OB_TMP_FAIL(sys_task_start(cur))) {
              COMMON_LOG(WARN, "failed to start sys task", K(tmp_ret));
            }
            if (OB_TMP_FAIL(generate_next_dag_(cur))) {
              LOG_WARN("failed to generate next dag", K(tmp_ret), K(cur));
            }
          }
          next_dag_status = ObIDag::DAG_STATUS_NODE_RUNNING;
        }
        cur->set_dag_status(next_dag_status);
        cur->update_status_in_dag_net();
        cur->start_time_ = ObTimeUtility::current_time(); // dag start running
        COMMON_LOG(DEBUG, "dag start running", K(ret), KPC(cur));
      } else if (ObIDag::DAG_STATUS_NODE_FAILED == dag_status
          && 0 == cur->get_running_task_count()) { // no task running failed dag, need free
        tmp_dag = cur;
        cur = cur->get_next();
        if (OB_FAIL(tmp_dag->report_result())) {
          LOG_WARN("failed to report reuslt", K(ret), KPC(tmp_dag));
        } else if (OB_FAIL(finish_dag_(ObIDag::DAG_STATUS_ABORT, *tmp_dag, erase_dag_net, true/*try_move_child*/))) {
          COMMON_LOG(ERROR, "failed to finish dag", K(ret), KPC(tmp_dag));
          ob_abort();
        } else if (OB_NOT_NULL(erase_dag_net)) {
          if (OB_FAIL(finish_dag_net(erase_dag_net))) {
            COMMON_LOG(WARN, "failed to erase dag net", K(ret), KPC(erase_dag_net));
          }
        }
        continue;
      }
      if (move_dag_to_waiting_list) {
        tmp_dag = cur;
        cur = cur->get_next();
        if (OB_FAIL(move_dag_to_list_(tmp_dag, READY_DAG_LIST, WAITING_DAG_LIST))) {
          COMMON_LOG(WARN, "failed to move dag to low priority list", K(ret), KPC(tmp_dag));
        } else {
          COMMON_LOG(DEBUG, "cur dag can't schedule", K(ret), KPC(tmp_dag));
        }
      } else if (OB_TMP_FAIL(cur->get_next_ready_task(ready_task))) {
        if (OB_ITER_END == tmp_ret) {
          cur = cur->get_next();
        } else {
          ret = tmp_ret;
          COMMON_LOG(WARN, "failed to get next ready task", K(ret), KPC(cur));
        }
      } else {
        task = ready_task;
        break;
      }
    } // end of while
  }
  if (OB_SUCC(ret) && OB_ISNULL(task)) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObTenantDagScheduler::schedule_one(const int64_t priority)
{
  int ret = OB_SUCCESS;
  ObTenantDagWorker *worker = NULL;
  ObITask *task = NULL;
  if (!waiting_workers_.is_empty(priority)) {
    worker = waiting_workers_.remove_first(priority);
  } else if (OB_FAIL(pop_task_from_ready_list(priority, task))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "failed to pop task", K(ret), K(priority));
    }
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "task is null", K(ret));
  } else {
    ObCurTraceId::set(task->get_dag()->get_dag_id());
    if (ObITask::TASK_STATUS_RETRY != task->get_status()
        && OB_FAIL(task->generate_next_task())) {
      task->get_dag()->reset_task_running_status(*task, ObITask::TASK_STATUS_FAILED);
      COMMON_LOG(WARN, "failed to generate_next_task", K(ret));
    } else if (OB_FAIL(dispatch_task(*task, worker, priority))) {
      task->get_dag()->reset_task_running_status(*task, ObITask::TASK_STATUS_WAITING);
      COMMON_LOG(WARN, "failed to dispatch task", K(ret));
    } else {
      task->set_status(ObITask::TASK_STATUS_RUNNING);
    }
  }
  if (OB_SUCC(ret) && NULL != worker) {
    ++running_task_cnts_[priority];
    ++total_running_task_cnt_;
    ++scheduled_task_cnt_;
    ++scheduled_task_cnts_[worker->get_task()->get_dag()->get_type()];
    running_workers_.add_last(worker, priority);
    if (task != NULL) {
      COMMON_LOG(INFO, "schedule one task", KP(task), "priority", OB_DAG_PRIOS[priority].dag_prio_str_,
          "group id", worker->get_group_id(), K_(total_running_task_cnt), K(running_task_cnts_[priority]),
          K(low_limits_[priority]), K(up_limits_[priority]), KP(task->get_dag()->get_dag_net()));
    }
    worker->resume();
  }
  ObCurTraceId::reset();
  return ret;
}

int ObTenantDagScheduler::schedule()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ret = loop_ready_dag_lists(); // loop waiting dag lists

  if (REACH_TENANT_TIME_INTERVAL(loop_waiting_dag_list_period_))  {
    for (int i = 0; i < ObDagPrio::DAG_PRIO_MAX; ++i) {
      if (OB_TMP_FAIL(loop_waiting_dag_list(i))) {
        COMMON_LOG(WARN, "failed to loop waiting task list", K(tmp_ret), K(i));
      }
    }
  }

  return ret;
}

void ObTenantDagScheduler::loop_dag_net()
{
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(loop_blocking_dag_net_list())) {
    COMMON_LOG_RET(WARN, tmp_ret, "failed to loop blocking dag net list", K(tmp_ret));
  }

  if (REACH_TENANT_TIME_INTERVAL(LOOP_RUNNING_DAG_NET_MAP_INTERVAL))  {
    if (OB_TMP_FAIL(loop_running_dag_net_map())) {
      COMMON_LOG_RET(WARN, tmp_ret, "failed to add dag from running_dag_net_map", K(tmp_ret));
    }
  }
}

int ObTenantDagScheduler::loop_ready_dag_lists()
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  if (total_running_task_cnt_ < work_thread_num_) {
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < ObDagPrio::DAG_PRIO_MAX; ++i) {
      if (running_task_cnts_[i] < low_limits_[i]) {
        is_found = (OB_SUCCESS == schedule_one(i));
        while (running_task_cnts_[i] < low_limits_[i] && is_found) {
          if (OB_SUCCESS != schedule_one(i)) {
            break;
          }
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < ObDagPrio::DAG_PRIO_MAX; ++i) {
      if (running_task_cnts_[i] < up_limits_[i]) {
        is_found = (OB_SUCCESS == schedule_one(i));
        while (running_task_cnts_[i] < up_limits_[i] && is_found) {
          if (OB_SUCCESS != schedule_one(i)) {
            break;
          }
        }
      }
    }
  }
  if (!is_found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObTenantDagScheduler::dispatch_task(ObITask &task, ObTenantDagWorker *&ret_worker, const int64_t priority)
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
    ret_worker->set_function_type(priority);
  }
  return ret;
}

int ObTenantDagScheduler::create_worker()
{
  int ret = OB_SUCCESS;
  // TODO add upper worker cnt limit, each tenant should have a max_worker_cnt
  ObTenantDagWorker *worker = OB_NEW(ObTenantDagWorker, SET_USE_500(ObModIds::OB_SCHEDULER));
  if (OB_ISNULL(worker)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "failed to allocate ObTenantDagWorker", K(ret));
  } else if (OB_FAIL(worker->init(check_period_))) {
    COMMON_LOG(WARN, "failed to init worker", K(ret));
  } else if (OB_FAIL(worker->start())) {
    COMMON_LOG(WARN, "failed to start worker", K(ret));
  } else if (!free_workers_.add_last(worker)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "failed to add new worker to worker list", K(ret));
  } else {
    ++total_worker_cnt_;
  }
  if (OB_FAIL(ret)) {
    if (NULL != worker) {
      worker->destroy();
      ob_delete(worker);
    }
  }
  return ret;
}

int ObTenantDagScheduler::try_reclaim_threads()
{
  int ret = OB_SUCCESS;
  ObTenantDagWorker *worker2delete = NULL;
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

void ObTenantDagScheduler::destroy_all_workers()
{
  {
    // resume all waiting workers
    // all workers will run to complete since switch is forbedden after stop sign is set
    ObThreadCondGuard guard(scheduler_sync_);

    for (int64_t j = 0; j < DAG_LIST_MAX; ++j) {
      for (int64_t i = 0; i < ObDagPrio::DAG_PRIO_MAX; ++i) {
        DagList &dl = dag_list_[j].get_list(i);
        DLIST_FOREACH_NORET(dag, dl) {
          dag->set_stop();
        }
      }
    }
    for (int64_t i = 0; i < ObDagPrio::DAG_PRIO_MAX; ++i) {
      WorkerList &wl = waiting_workers_.get_list(i);
      DLIST_FOREACH_NORET(worker, wl) {
        worker->resume();
      }
    }
  }

  // wait all workers finish, then delete all workers
  while (true) {
    // 100ms
    ob_usleep(100 * 1000);
    ObThreadCondGuard guard(scheduler_sync_);
    if (total_worker_cnt_ == free_workers_.get_size()) {
      DLIST_REMOVE_ALL_NORET(worker, free_workers_) {
        ob_delete(worker);
      }
      free_workers_.reset();
      break;
    } else {
      continue;
    }
  }
}

// call this func with lock
void ObTenantDagScheduler::update_work_thread_num()
{
  // find the largest max_thread among all dag types
  int32_t threads_sum = 0;
  for (int64_t i = 0; i < ObDagPrio::DAG_PRIO_MAX; ++i) {
   threads_sum += up_limits_[i];
  }
  work_thread_num_ = threads_sum;
}

int ObTenantDagScheduler::set_thread_score(const int64_t priority, const int64_t score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_UNLIKELY(priority < 0 || priority >= ObDagPrio::DAG_PRIO_MAX || score < 0 || score > INT32_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(priority), K(score));
  } else {
    ObThreadCondGuard guard(scheduler_sync_);
    const int64_t old_val = up_limits_[priority];
    up_limits_[priority] = 0 == score ? OB_DAG_PRIOS[priority].score_ : score;
    low_limits_[priority] = up_limits_[priority];
    if (old_val != up_limits_[priority]) {
      update_work_thread_num();
    }
    scheduler_sync_.signal();
    COMMON_LOG(INFO, "set thread score successfully", K(score),
        "prio", OB_DAG_PRIOS[priority].dag_prio_str_, K(up_limits_[priority]), K_(work_thread_num),
        K_(default_work_thread_num));
  }
  return ret;
}

int64_t ObTenantDagScheduler::get_running_task_cnt(const ObDagPrio::ObDagPrioEnum priority)
{
  int64_t count = -1;
  if (priority >= 0 && priority < ObDagPrio::DAG_PRIO_MAX) {
    ObThreadCondGuard guard(scheduler_sync_);
    count = running_task_cnts_[priority];
  } else {
    COMMON_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid priority", K(priority));
  }
  return count;
}

int ObTenantDagScheduler::get_up_limit(const int64_t prio, int64_t &up_limit)
{
  int ret = OB_SUCCESS;
  up_limit = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_UNLIKELY(prio < 0 || prio >= ObDagPrio::DAG_PRIO_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(prio));
  } else {
    ObThreadCondGuard guard(scheduler_sync_);
    // TODO get max thread concurrency of current prio type
    up_limit = up_limits_[prio];
  }
  return ret;
}

int ObTenantDagScheduler::check_dag_exist(const ObIDag *dag, bool &exist)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  exist = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arugment", KP(dag));
  } else {
    ObThreadCondGuard guard(scheduler_sync_);
    ObIDag *stored_dag = nullptr;
    if (OB_SUCCESS != (hash_ret = dag_map_.get_refactored(dag, stored_dag))) {
      if (OB_HASH_NOT_EXIST == hash_ret) {
        exist = false;
      } else {
        ret = hash_ret;
        COMMON_LOG(WARN, "failed to get from dag map", K(ret));
      }
    } else if (OB_ISNULL(stored_dag)) {
      ret = OB_ERR_SYS;
      COMMON_LOG(WARN, "dag is null", K(ret));
    }
  }
  return ret;
}

// MAKE SURE parent_dag is valid
int ObTenantDagScheduler::cancel_dag(const ObIDag *dag, ObIDag *parent_dag)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  bool free_flag = false;
  ObIDag *cur_dag = nullptr;
  ObIDagNet *erase_dag_net = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
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
        LOG_INFO("cancel dag", K(ret), KP(dag), KP(parent_dag));
        if (OB_NOT_NULL(parent_dag)) {
          const ObINodeWithChild *child_node = static_cast<const ObINodeWithChild *>(dag);
          if (OB_FAIL(parent_dag->erase_children(child_node))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to erase child", K(ret), KP(child_node), KPC(parent_dag));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(finish_dag_(ObIDag::DAG_STATUS_ABORT, *cur_dag, erase_dag_net, true/*try_move_child*/))) {
          COMMON_LOG(WARN, "failed to erase dag", K(ret), KPC(cur_dag));
          ob_abort();
        }
      }
    } // end of lock
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(erase_dag_net)) {
      if (OB_FAIL(finish_dag_net(erase_dag_net))) {
        LOG_WARN("failed to finish dag net", K(ret), KP(erase_dag_net));
      }
    }
  }
  return ret;
}

int ObTenantDagScheduler::check_dag_net_exist(
    const ObDagId &dag_id, bool &exist)
{
  int ret = OB_SUCCESS;
  const ObIDagNet *dag_net = nullptr;
  ObMutexGuard guard(dag_net_map_lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_FAIL(dag_net_id_map_.get_refactored(dag_id, dag_net))) {
    if (OB_HASH_NOT_EXIST == ret) {
      exist = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get dag id from dag net", K(ret), K(dag_id));
    }
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else {
    exist = true;
  }
  return ret;
}

OB_INLINE int64_t ObTenantDagScheduler::get_dag_limit(const ObDagPrio::ObDagPrioEnum dag_prio)
{
  int64_t dag_limit = dag_limit_;
  if (ObDagPrio::DAG_PRIO_COMPACTION_HIGH == dag_prio
    || ObDagPrio::DAG_PRIO_COMPACTION_MID == dag_prio
    || ObDagPrio::DAG_PRIO_COMPACTION_LOW == dag_prio) {
    dag_limit = compaction_dag_limit_;
  }
  return dag_limit;
}

int ObTenantDagScheduler::inner_add_dag(
    const bool emergency,
    const bool check_size_overflow,
    ObIDag *&dag)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_ISNULL(dag) || OB_UNLIKELY(!dag->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner dag dag get invalid argument", K(ret), KPC(dag));
  } else {
    if (check_size_overflow && dag_cnts_[dag->get_type()] >= get_dag_limit((ObDagPrio::ObDagPrioEnum)dag->get_priority())) {
      ret = OB_SIZE_OVERFLOW;
      COMMON_LOG(WARN, "ObTenantDagScheduler is full", K(ret), K_(dag_limit), KPC(dag));
    } else if (OB_FAIL(add_dag_into_list_and_map_(
            is_waiting_dag_type(dag->get_type()) ? WAITING_DAG_LIST : READY_DAG_LIST,
            dag,
            emergency))) {
      if (OB_EAGAIN != ret) {
        COMMON_LOG(WARN, "failed to add dag into list and map", K(ret), KPC(dag));
      }
    } else {
      ++dag_cnt_;
      ++dag_cnts_[dag->get_type()];
      dag->set_dag_status(ObIDag::DAG_STATUS_READY);
      dag->add_time_ = ObTimeUtility::fast_current_time();
      scheduler_sync_.signal();
      COMMON_LOG(INFO, "add dag success", KP(dag), "start_time", dag->start_time_,
          "id", dag->id_, K(dag->hash()), K_(dag_cnt),
          "dag_type_cnts", dag_cnts_[dag->get_type()]);

      dag = nullptr;
    }
  }
  return ret;
}

int ObTenantDagScheduler::generate_next_dag_(ObIDag *dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIDag *next_dag = nullptr;
  ObIDag *child_dag = nullptr;
  ObIDagNet *dag_net = nullptr;
  const bool emergency = false;
  const bool check_size_overflow = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate next dag get invalid argument", K(ret), KP(dag));
  } else {
    if (OB_FAIL(dag->generate_next_dag(next_dag))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        COMMON_LOG(WARN, "failed to generate_next_dag", K(ret), K(tmp_ret));
      }
    } else if (OB_ISNULL(next_dag)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "next dag should not be NULL", K(ret), KP(next_dag));
    } else {
      ObCurTraceId::set(next_dag->get_dag_id());
      if (FALSE_IT(dag_net = dag->get_dag_net())) {
      } else if (OB_NOT_NULL(dag_net) && OB_FAIL(dag_net->add_dag_into_dag_net(*next_dag))) {
        LOG_WARN("failed to add dag into dag net", K(ret), KPC(next_dag));
      } else if (OB_FAIL(next_dag->create_first_task())) {
        LOG_WARN("failed to create first task", K(ret), KPC(next_dag));
      } else {
        COMMON_LOG(INFO, "succeed generate next dag", KP(dag), KP(next_dag));
        const common::ObIArray<ObINodeWithChild*> &child_array = dag->get_child_nodes();
        if (child_array.empty()) {
          //do nothing
        } else if (OB_FAIL(next_dag->add_child_without_inheritance(child_array))) {
          LOG_WARN("failed to add child without inheritance", K(ret), KPC(dag));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(inner_add_dag(emergency, check_size_overflow, next_dag))) {
        LOG_WARN("failed to add next dag", K(ret), KPC(next_dag));
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(next_dag)) {
        if (OB_TMP_FAIL(dag->report_result())) {
          COMMON_LOG(WARN, "failed to report result", K(tmp_ret), KPC(dag));
        }
        free_dag(*next_dag, dag/*parent dag*/);
      }
    }
  }
  return ret;
}

int ObTenantDagScheduler::try_move_child_to_ready_list(
    ObIDag &dag)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<ObINodeWithChild*> &child_array = dag.get_child_nodes();

  if (child_array.empty()) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < child_array.count(); ++i) {
      ObIDag *child_dag = static_cast<ObIDag *>(child_array.at(i));
      if (OB_ISNULL(child_dag)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child dag should not be NULL", K(ret), KP(child_dag));
      } else if (WAITING_DAG_LIST == child_dag->get_list_idx()
          && 0 == child_dag->get_indegree()
          && child_dag->check_can_schedule()) {
        if (OB_FAIL(move_dag_to_list_(child_dag, WAITING_DAG_LIST, READY_DAG_LIST))) {
          COMMON_LOG(WARN, "failed to move dag from waitinig list to ready list", K(ret), KPC(child_dag));
        }
      }
    }
  }
  return ret;
}

int ObTenantDagScheduler::cancel_dag_net(const ObDagId &dag_id)
{
  int ret = OB_SUCCESS;
  const ObIDagNet *dag_net_key = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObArray<ObIDag*> dag_array;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (dag_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cancel dag net get invalid argument", K(ret), K(dag_id));
  } else {
    {
      ObMutexGuard dag_net_guard(dag_net_map_lock_);
      if (OB_FAIL(dag_net_id_map_.get_refactored(dag_id, dag_net_key))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get dag id from dag net", K(ret), K(dag_id));
        }
      } else if (OB_ISNULL(dag_net_key)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dag net key should not be NULL", K(ret), K(dag_id), KP(dag_net));
      } else if (OB_FAIL(dag_net_map_[RUNNING_DAG_NET_MAP].get_refactored(dag_net_key, dag_net))) {
        LOG_WARN("failed to get dag net", K(ret), KPC(dag_net_key));
      } else {
        dag_net->set_cancel();
        if (OB_FAIL(dag_net->deal_with_cancel())) {
          LOG_WARN("failed to deal with cancel", K(ret), KPC(dag_net));
        }
      }
    }

    // Donot call notify(), may cause dead lock.
  }
  return ret;
}

int ObTenantDagScheduler::get_complement_data_dag_progress(const ObIDag *dag,
                                                           int64_t &row_scanned,
                                                           int64_t &row_inserted)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObDagScheduler is not inited", K(ret));
  } else if (OB_ISNULL(dag) || dag->get_type() != ObDagType::DAG_TYPE_DDL) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arugment", K(ret), KP(dag));
  } else {
    ObThreadCondGuard guard(scheduler_sync_);
    ObIDag *stored_dag = nullptr;
    if (OB_FAIL(dag_map_.get_refactored(dag, stored_dag))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get from dag map", K(ret));
      }
    } else if (OB_ISNULL(stored_dag)) {
      ret = OB_ERR_SYS;
      LOG_WARN("dag is null", K(ret));
    } else {
      row_scanned = static_cast<ObComplementDataDag*>(stored_dag)->get_context().row_scanned_;
      row_inserted = static_cast<ObComplementDataDag*>(stored_dag)->get_context().row_inserted_;
    }
  }
  return ret;
}

// for unittest
int ObTenantDagScheduler::get_first_dag_net(ObIDagNet *&dag_net)
{
  int ret = OB_SUCCESS;
  dag_net = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObDagScheduler is not inited", K(ret));
  } else {
    ObMutexGuard guard(dag_net_map_lock_);
    DagNetMap::iterator iter = dag_net_map_[RUNNING_DAG_NET_MAP].begin();
    if (iter != dag_net_map_[RUNNING_DAG_NET_MAP].end()) {
      dag_net = iter->second;
    }
  }
  return ret;
}

int ObFakeTask::process()
{
  COMMON_LOG(INFO, "ObFakeTask process");
  return OB_SUCCESS;
}

} //namespace share
} //namespace oceanbase
