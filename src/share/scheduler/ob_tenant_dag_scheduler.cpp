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
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "share/rc/ob_context.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "observer/omt/ob_tenant.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/config/ob_server_config.h"
#include "ob_dag_warning_history_mgr.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/compaction/ob_compaction_dag_ranker.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/compaction/ob_compaction_memory_context.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/ddl/ob_complement_data_task.h"
#include "storage/column_store/ob_co_merge_dag.h"
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
  int common_yield()
  {
    return share::dag_yield();
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

/********************************************ObDiagnoseLocation impl******************************************/
void ObDiagnoseLocation::set(const char* filename, const int line, const char* function)
{
  if (!is_valid() && INVALID_LINE != line) {
    filename_ = filename;
    function_ = function;
    line_ = line;
  }
}

void ObDiagnoseLocation::set(const ObDiagnoseLocation &new_location)
{
  if (!is_valid()) {
    *this = new_location;
  }
}

bool ObDiagnoseLocation::is_valid() const
{
  return INVALID_LINE != line_;
}

ObDiagnoseLocation & ObDiagnoseLocation::operator = (const ObDiagnoseLocation &other)
{
  filename_ = other.filename_;
  function_ = other.function_;
  line_ = other.line_;
  return *this;
}

/********************************************ObINodeWithChild impl******************************************/
bool ObINodeWithChild::check_with_lock()
{
  bool ret = true;
  ObMutexGuard guard(lock_);
  if (indegree_ != parent_.count()) { // defense
    ret = false;
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "parent count is not equal to indegree", KPC(this));
  }
  return ret;
}

int64_t ObINodeWithChild::get_indegree() const
{
  return indegree_;
}

int ObINodeWithChild::add_parent_node(ObINodeWithChild &parent)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);
  if (OB_FAIL(parent_.push_back(&parent))) {
    COMMON_LOG(WARN, "failed to add parent", K(ret), K(parent));
  } else {
    inc_indegree();
  }
  return ret;
}

// if failed, child will not be in the children_ array
int ObINodeWithChild::add_child_without_lock(ObINodeWithChild &child)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(child.add_parent_node(*this))) {
    COMMON_LOG(WARN, "failed to add parent to child", K(ret), K(child), K(this));
  } else if (OB_FAIL(children_.push_back(&child))) {
    COMMON_LOG(WARN, "failed to add child", K(ret), K(child));
  } else {
    COMMON_LOG(DEBUG, "success to add child", K(ret), K(this), K(&child), K(child));
  }
  return ret;
}

int ObINodeWithChild::deep_copy_children(const ObIArray<ObINodeWithChild*> &other)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < other.count(); ++i) {
    if (OB_FAIL(add_child_without_lock(*other.at(i)))) {
      COMMON_LOG(WARN, "failed to copy dependency to task", K(ret), K(i));
    }
  }
  return ret;
}

// only affect children array // remove this from children's parent array
int ObINodeWithChild::remove_parent_for_children()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < children_.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(children_.at(i)->erase_node(this, true/*node_is_parent*/))) {
      COMMON_LOG(WARN, "fail to erase parent for children", K(ret), K(this), K(i), K(children_.at(i)));
    } else {
      COMMON_LOG(DEBUG, "remove_parent_for_children", K(ret), K(this), K(i), K(children_.at(i)),
          K(children_.at(i)->get_indegree()));
    }
  }
  return ret;
}

// only affect parent array // remove this from parent's children array
int ObINodeWithChild::remove_child_for_parents()
{
  int ret = OB_SUCCESS;
  if (indegree_ != parent_.count()) { // defense
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "parent count is not equal to indegree", KPC(this));
  }
  for (int64_t i = 0; i < parent_.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(parent_.at(i)->erase_node(this, false/*node_is_parent*/))) {
      COMMON_LOG(WARN, "fail to erase children for parents", K(ret), K(this), K(i), K(parent_.at(i)));
    } else {
      dec_indegree();
      COMMON_LOG(DEBUG, "remove_child_for_parents", K(ret), K(this), K(i));
    }
  }
  return ret;
}

// only for add_child failed
// parent is locked, can't remove_chlid_for_parents
void ObINodeWithChild::reset_children()
{
  int tmp_ret = OB_SUCCESS;
  if (!parent_.empty()) { // defense
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "node should not have parent", KPC(this));
  }
  if (OB_TMP_FAIL(remove_parent_for_children())) {
    LOG_WARN_RET(tmp_ret, "failed to remove parent for children");
  } else {
    children_.reset();
  }
}

// clear parent and children // use for free dag
void ObINodeWithChild::reset_node()
{
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(remove_child_for_parents())) {
    LOG_WARN_RET(tmp_ret, "failed to remove children for parents");
  } else if (OB_TMP_FAIL(remove_parent_for_children())) {
    LOG_WARN_RET(tmp_ret, "failed to remove parent for children");
  } else {
    parent_.reset();
    children_.reset();
  }
}

// won't affect node
// node_is_parent = false, remove node from this children_ array
// node_is_parent = true, remove node from this parent_ array and dec_indegree
int ObINodeWithChild::erase_node(const ObINodeWithChild *node, const bool node_is_parent)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("erase children get invalid argument", K(ret), KP(node));
  } else {
    ObMutexGuard guard(lock_);
    ObIArray<ObINodeWithChild*> &node_array = node_is_parent ? parent_ : children_;
    bool found_flag = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < node_array.count(); ++i) {
      ObINodeWithChild *tmp_node = node_array.at(i);
      if (node == tmp_node) {
        if (OB_FAIL(node_array.remove(i))) {
          LOG_WARN("failed to remove child", K(ret), K(i), KPC(node));
        } else {
          found_flag = true;
          if (node_is_parent) { // this is child
            dec_indegree();
          }
          COMMON_LOG(DEBUG, "erase node", K(ret), K(node_is_parent), K(this), K(i), K(tmp_node), K(node));
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
    color_(ObITaskColor::BLACK)
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
    } else if (OB_FAIL(copy_children_to(*next_task))) { // next_task is a new node, no concurrency
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
      lib::ObMutexGuard guard(lock_);
      if (OB_FAIL(add_child_without_lock(child))) {
        COMMON_LOG(WARN, "failed to add child", K(child));
      }
    }
  }
  return ret;
}

int ObITask::copy_children_to(ObITask &next_task) const
{
  // copy children nodes of this task to next_task
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "task is invalid", K(ret), K_(type), KP_(dag));
  } else if (this == &next_task) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "can not copy to self", K(ret));
  } else if (OB_FAIL(next_task.deep_copy_children(get_child_nodes()))) {
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
    ObDagType::DAG_TYPE_CO_MERGE_BATCH_EXECUTE,
    ObDagType::DAG_TYPE_CO_MERGE_PREPARE,
    ObDagType::DAG_TYPE_CO_MERGE_SCHEDULE,
    ObDagType::DAG_TYPE_CO_MERGE_FINISH,
};

ObIDag::ObIDag(const ObDagType::ObDagTypeEnum type)
  : ObINodeWithChild(),
    dag_ret_(OB_SUCCESS),
    add_time_(0),
    start_time_(0),
    consumer_group_id_(USER_RESOURCE_OTHER_GROUP_ID),
    error_location_(),
    allocator_(nullptr),
    is_inited_(false),
    type_(type),
    priority_(OB_DAG_TYPES[type].init_dag_prio_),
    dag_status_(ObIDag::DAG_STATUS_INITING),
    running_task_cnt_(0),
    is_stop_(false),
    force_cancel_flag_(false),
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

void ObIDag::clear_task_list_with_lock()
{
  ObMutexGuard guard(lock_);
  clear_task_list();
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
  consumer_group_id_ = USER_RESOURCE_OTHER_GROUP_ID;
  running_task_cnt_ = 0;
  dag_status_ = ObDagStatus::DAG_STATUS_INITING;
  dag_ret_ = OB_SUCCESS;
  error_location_.reset();
}

void ObIDag::reset()
{
  ObMutexGuard guard(lock_);
  is_inited_ = false;
  ObINodeWithChild::reset();
  if (task_list_.get_size() > 0) {
    clear_task_list();
  }
  clear_running_info();
  type_ = ObDagType::DAG_TYPE_MAX;
  priority_ = ObDagPrio::DAG_PRIO_MAX;
  is_stop_ = false;
  force_cancel_flag_ = false;
  dag_net_ = nullptr;
  list_idx_ = DAG_LIST_MAX;
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
        COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "failed to remove task from task_list", K_(id));
      }
    }
  }
  return ret;
}

// Dag_A(child: Dag_E/Dag_F) call add_child(Dag_B)
// result:
// Dag_A(child: Dag_E/Dag_F/Dag_B)
// Dag_B(child: Dag_E/Dag_F) will deep copy previous children of Dag_A, and join in the dag_net which contains Dag_A

// ATTENTION!!! for same priority dag, cuold move child_dag from waiting_list to ready list when parent finish
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
    if (OB_UNLIKELY(nullptr != child.get_dag_net() && dag_net_ != child.get_dag_net())) { // already belongs to other dag_net
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "child's dag net is not null", K(ret), K(child), KP_(dag_net));
    } else if (OB_FAIL(child.deep_copy_children(get_child_nodes()))) { // child is a new node, no concurrency
      COMMON_LOG(WARN, "failed to deep copy child", K(ret), K(child));
    } else if (nullptr == child.get_dag_net() && OB_FAIL(dag_net_->add_dag_into_dag_net(child))) {
      COMMON_LOG(WARN, "failed to add dag into dag net", K(ret), K(child), KPC(dag_net_));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(add_child_without_lock(child))) {
    COMMON_LOG(WARN, "failed to add child", K(ret), K(child));
    child.reset_children();
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(dag_net_) && OB_TMP_FAIL(dag_net_->erase_dag_from_dag_net(child))) {
      COMMON_LOG_RET(WARN, tmp_ret, "failed to erase from dag_net", K(child));
    }
  }
  return ret;
}

int ObIDag::update_status_in_dag_net(bool &dag_net_finished)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);
  if (OB_NOT_NULL(dag_net_)) {
    dag_net_->update_dag_status(*this, dag_net_finished);
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
        if (OB_FAIL(task.remove_parent_for_children())) {
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
    COMMON_LOG(WARN,"dag id invalid", K(ret), K(dag_id));
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
    J_KV(KP(this), K_(is_inited), K_(type), "name", get_dag_type_str(type_), K_(id), KPC_(dag_net), K_(dag_ret), K_(dag_status),
        K_(add_time), K_(start_time), K_(running_task_cnt), K_(indegree), K_(consumer_group_id), "hash", hash(), K(task_list_.get_size()));
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
  info.location_ = error_location_;
  info.dag_type_ = type_;
  info.tenant_id_ = MTL_ID();
  info.priority_ = static_cast<uint32_t>(ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_HIGH);
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

int ObIDag::finish(const ObDagStatus status, bool &dag_net_finished)
{
  int ret = OB_SUCCESS;
  {
    if (OB_FAIL(remove_child_for_parents())) {
      COMMON_LOG(WARN, "failed to remove child for parents", K(ret));
    } else if (OB_FAIL(remove_parent_for_children())) {
      COMMON_LOG(WARN, "failed to remove parent for children", K(ret));
    } // remain children_ array in this dag to try_move_child_to_ready_list
  }
  if (OB_SUCC(ret)) {
    set_dag_status(status);
    set_dag_error_location();
    update_status_in_dag_net(dag_net_finished);
  }
  return ret;
}

void ObIDag::set_force_cancel_flag()
{
  force_cancel_flag_ = true;
  // dag_net and dags in the same dag net should be canceled too.
  if (OB_NOT_NULL(dag_net_)) {
    dag_net_->set_cancel();
  }
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

  if (OB_UNLIKELY(this == &child)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "can not add self loop", K(ret));
  } else if (priority_ != child.get_priority()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "can not add child with different priority", K(ret), K_(priority), K(child.get_priority()));
  } else if ((OB_UNLIKELY(DAG_STATUS_INITING != child.get_dag_status())
        && OB_UNLIKELY(DAG_STATUS_READY != child.get_dag_status()))
      || ((DAG_STATUS_READY == child.get_dag_status()) && 0 == child.get_indegree())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag status is not valid", K(ret), K(child));
  } else if (OB_UNLIKELY(dag_net_ != child_dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag and child dag net not same", K(ret), KPC(dag_net_), KPC(child_dag_net));
  } else if (OB_FAIL(add_child_without_lock(child))) {
    COMMON_LOG(WARN, "failed to add child", K(ret), K(child));
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

const static char * ObDagNetListStr[] = {
    "BLOCKING_DAG_NET_LIST",
    "RUNNING_DAG_NET_LIST",
    "FINISHED_DAG_NET_LIST"
};

const char *dag_net_list_to_str(const ObDagNetListIndex &dag_net_list_index)
{
  STATIC_ASSERT(static_cast<int64_t>(DAG_NET_LIST_MAX) == ARRAYSIZEOF(ObDagNetListStr), "dag net list str len is mismatch");
  const char *str = "";
  if (is_valid_dag_net_list(dag_net_list_index)) {
    str = ObDagNetListStr[dag_net_list_index];
  } else {
    str = "invalid_dag_net_list";
  }
  return str;
}

ObIDagNet::ObIDagNet(
    const ObDagNetType::ObDagNetTypeEnum type)
   : is_stopped_(false),
     lock_(common::ObLatchIds::WORK_DAG_NET_LOCK),
     allocator_(nullptr),
     type_(type),
     add_time_(0),
     start_time_(0),
     dag_record_map_(),
     first_fail_dag_info_(nullptr),
     is_cancel_(false),
     is_finishing_last_dag_(false)
{
}

/*
 * ATTENTION: DO NOT call this function if if this dag has parent dag.
 * When parent adds child, child will be add into the same dag net with parent.
 */
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
      COMMON_LOG(WARN, "dag record maybe already exist in map", K(ret), K(hash_ret), K(dag), KPC(dag_record));
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
        COMMON_LOG(INFO, "success to add into dag array", K(ret), KP(this), KPC(dag_record));
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
  is_finishing_last_dag_ = false;
  if (dag_record_map_.created()) {
    for (DagRecordMap::iterator iter = dag_record_map_.begin();
        iter != dag_record_map_.end(); ++iter) {
      ObDagRecord *dag_record = iter->second;
      if (OB_ISNULL(dag_record)) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "dag record should not be NULL", KPC(this), KPC(dag_record));
      } else {
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
  if (OB_NOT_NULL(first_fail_dag_info_)) {
    if (OB_NOT_NULL(first_fail_dag_info_->info_param_)) {
      allocator_->free(first_fail_dag_info_->info_param_);
      first_fail_dag_info_->info_param_ = nullptr;
    }
    first_fail_dag_info_->~ObDagWarningInfo();
    allocator_->free(first_fail_dag_info_);
    first_fail_dag_info_ = nullptr;
  }
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
  int ret = OB_SUCCESS;
  if (inner_check_finished_without_lock() && !is_finishing_last_dag_) {
    WEAK_BARRIER();
    is_stopped_ = true;
  }
  return is_stopped_;
}

int ObIDagNet::update_dag_status(ObIDag &dag, bool &dag_net_finished)
{
  int ret = OB_SUCCESS;
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
  } else if (FALSE_IT(dag_record->dag_status_ = dag.get_dag_status())) {
  } else if (ObIDag::is_finish_status(dag.get_dag_status())) {
    remove_dag_record_(*dag_record);
    if (inner_check_finished_without_lock()) {
      dag_net_finished = true;
      is_finishing_last_dag_ = true;
      COMMON_LOG(INFO, "last dag in dag_net is finishing", K(dag), KP(this));
    }
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
    J_KV(KP(this), K_(type), K_(dag_net_id), "dag_record_cnt", dag_record_map_.size(),
        K_(is_stopped), K_(is_cancel), K_(is_finishing_last_dag), KP_(allocator));
    J_OBJ_END();
  }
  return pos;
}

void ObIDagNet::init_dag_id()
{
  if (dag_net_id_.is_invalid()) {
    dag_net_id_.init(GCONF.self_addr_);
  }
}

int ObIDagNet::set_dag_id(const ObDagId &dag_net_id)
{
  int ret = OB_SUCCESS;
  if (dag_net_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "set dag id get invalid argument", K(ret), K(dag_net_id));
  } else {
    dag_net_id_ = dag_net_id;
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

void ObIDagNet::set_last_dag_finished()
{
  ObMutexGuard guard(lock_);
  is_finishing_last_dag_ = false;
}

bool ObIDagNet::is_inited()
{
  return OB_NOT_NULL(allocator_);
}

bool ObIDagNet::is_started()
{
  return start_time_ != 0;
}

void ObIDagNet::diagnose_dag(common::ObIArray<compaction::ObDiagnoseTabletCompProgress> &progress_list)
{
  int tmp_ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);
  for (DagRecordMap::iterator iter = dag_record_map_.begin();
      iter != dag_record_map_.end(); ++iter) {
    ObDagRecord *dag_record = iter->second;
    compaction::ObDiagnoseTabletCompProgress progress;
    if (OB_TMP_FAIL(dag_record->dag_ptr_->diagnose_compaction_info(progress))) {
      LOG_WARN_RET(tmp_ret, "failed to diagnose compaction info");
    } else if (progress.is_valid()) {
      if (OB_TMP_FAIL(progress_list.push_back(progress))) {
        LOG_WARN_RET(tmp_ret, "failed to add compaction progress");
      }
    }
  }
}

int ObIDagNet::add_dag_warning_info(ObIDag *dag)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "dag net not basic init", K(ret), K(this));
  } else if (OB_ISNULL(first_fail_dag_info_) && OB_SUCCESS != dag->get_dag_ret() && !dag->ignore_warning()) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObDagWarningInfo)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      first_fail_dag_info_ = new (buf) ObDagWarningInfo();
      first_fail_dag_info_->hash_ = hash();
      if (OB_FAIL(dag->gene_warning_info(*first_fail_dag_info_, *allocator_))) {
        COMMON_LOG(WARN, "failed to gene dag warning info", K(ret));
      }
    }
  }
  return ret;
}

int ObIDagNet::add_dag_warning_info()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  compaction::ObCOMergeDagNet *dag_net = nullptr;
  if (is_co_dag_net()) {
    dag_net = static_cast<compaction::ObCOMergeDagNet*>(this);
  }
  if (OB_NOT_NULL(first_fail_dag_info_) && is_cancel()) { // is_cancel means co dag net failed in the end
    if (OB_ISNULL(first_fail_dag_info_->info_param_)) { // maybe caused by 4013
      COMMON_LOG(INFO, "info param is null", K_(first_fail_dag_info));
    } else if (OB_FAIL(MTL(ObDagWarningHistoryManager*)->add_dag_warning_info(*first_fail_dag_info_))) {
      COMMON_LOG(WARN, "failed to add dag warning info", K(ret), KPC(this), KPC_(first_fail_dag_info));
    } else if (OB_NOT_NULL(dag_net)) {
      if (OB_TMP_FAIL(MTL(compaction::ObDiagnoseTabletMgr *)->add_diagnose_tablet(
            dag_net->ls_id_, dag_net->tablet_id_, ObDiagnoseTabletType::TYPE_MEDIUM_MERGE))) {
        COMMON_LOG(WARN, "failed to add diagnose tablet", K(tmp_ret),
            "ls_id", dag_net->ls_id_, "tablet_id", dag_net->tablet_id_);
      }
    }
  } else if (OB_FAIL(MTL(ObDagWarningHistoryManager*)->delete_info(hash()))) {
    if (OB_HASH_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "failed to delete dag warning info", K(ret), KPC(this));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_NOT_NULL(dag_net)) {
    if (OB_TMP_FAIL(MTL(compaction::ObDiagnoseTabletMgr *)->delete_diagnose_tablet(
          dag_net->ls_id_, dag_net->tablet_id_, ObDiagnoseTabletType::TYPE_MEDIUM_MERGE))) {
      COMMON_LOG(WARN, "failed to delete diagnose tablet", K(tmp_ret),
          "ls_id", dag_net->ls_id_, "tablet_id", dag_net->tablet_id_);
    }
  }
  return ret;
}

void ObIDagNet::gene_dag_info(ObDagInfo &info, const char *list_info)
{
  ObMutexGuard guard(lock_);
  info.tenant_id_ = MTL_ID();
  info.dag_net_type_ = type_;
  info.dag_id_ = dag_net_id_;
  info.dag_status_ = ObIDag::DAG_STATUS_NODE_RUNNING;
  info.running_task_cnt_ = dag_record_map_.size();
  info.add_time_ = add_time_;
  info.start_time_ = start_time_;
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(fill_dag_net_key(info.dag_net_key_, OB_DAG_KEY_LENGTH))) {
    COMMON_LOG_RET(WARN, tmp_ret, "failed to fill dag key");
  }
  if (OB_TMP_FAIL(fill_comment(info.comment_, OB_DAG_COMMET_LENGTH))) {
    COMMON_LOG_RET(WARN, tmp_ret, "failed to fill comment");
  }
  ADD_TASK_INFO_PARAM(info.comment_, OB_DAG_COMMET_LENGTH,
      " list_info", list_info);
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
    COMMON_LOG_RET(WARN, tmp_ret, "failed to fill dag key");
  }

  if (OB_TMP_FAIL(fill_comment(info.comment_, sizeof(info.comment_)))) {
    COMMON_LOG_RET(WARN, tmp_ret, "failed to fill dag comment");
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
          "indegree", cur_task->get_indegree());
      cur_task = cur_task->get_next();
    } // end while
  }
  if (OB_NOT_NULL(dag_net_)) {
    info.dag_net_type_ = dag_net_->get_type();
    if (OB_TMP_FAIL(dag_net_->fill_dag_net_key(info.dag_net_key_, OB_DAG_KEY_LENGTH))) {
      COMMON_LOG_RET(WARN, tmp_ret, "failed to fill dag key");
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
  if (type >= ObDagNetType::DAG_NET_TYPE_MAX || type < ObDagNetType::DAG_NET_TYPE_MIGRATION) {
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
      && ((dag_net_type_ >= ObDagNetType::DAG_NET_TYPE_MIGRATION
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
_RLOCAL(bool, ObTenantDagWorker::is_reserve_mode_);
_RLOCAL(compaction::ObCompactionMemoryContext *, ObTenantDagWorker::mem_ctx_);

ObTenantDagWorker::ObTenantDagWorker()
  : task_(NULL),
    status_(DWS_FREE),
    check_period_(0),
    last_check_time_(0),
    function_type_(0),
    group_id_(OB_INVALID_GROUP_ID),
    tg_id_(-1),
    hold_by_compaction_dag_(false),
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
  group_id_ = OB_INVALID_GROUP_ID;
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
  uint64_t consumer_group_id = USER_RESOURCE_OTHER_GROUP_ID;
  if (is_user_group(group_id)) {
    //user level
    consumer_group_id = group_id;
  } else if (OB_FAIL(G_RES_MGR.get_mapping_rule_mgr().get_group_id_by_function_type(MTL_ID(), function_type_, consumer_group_id))) {
    //function level
    LOG_WARN("fail to get group id by function", K(ret), K(MTL_ID()), K(function_type_), K(consumer_group_id));
  }

  if (OB_SUCC(ret) && consumer_group_id != group_id_) {
    // for CPU isolation, depend on cgroup
    if (OB_NOT_NULL(GCTX.cgroup_ctrl_) && GCTX.cgroup_ctrl_->is_valid() &&
        OB_FAIL(GCTX.cgroup_ctrl_->add_self_to_cgroup(
            MTL_ID(),
            consumer_group_id,
            GCONF.enable_global_background_resource_isolation ? BACKGROUND_CGROUP
                                                              : ""))) {
      LOG_WARN("bind back thread to group failed", K(ret), K(GETTID()), K(MTL_ID()), K(group_id));
    } else {
      // for IOPS isolation, only depend on consumer_group_id
      ATOMIC_SET(&group_id_, consumer_group_id);
      THIS_WORKER.set_group_id(static_cast<int32_t>(consumer_group_id));
    }
  }
  return ret;
}

bool ObTenantDagWorker::need_wake_up() const
{
  return (ObTimeUtility::fast_current_time() - last_check_time_) > check_period_ * 10;
}

bool ObTenantDagWorker::get_force_cancel_flag()
{
  int ret = OB_SUCCESS; // Just for COMMON_LOG
  bool flag = false;
  ObIDag *dag = nullptr;
  if (DWS_FREE == status_ || DWS_STOP == status_) {
    COMMON_LOG(WARN, "the status of worker is invalid to get task", K(status_), K(task_));
  } else if (OB_ISNULL(task_)) {
    // ignore ret
    COMMON_LOG(WARN, "worker contains nullptr task");
  } else if (OB_ISNULL(dag = task_->get_dag())) {
    // ignore ret
    COMMON_LOG(WARN, "task does not belong to dag");
  } else {
    flag = dag->get_force_cancel_flag();
  }
  return flag;
}

void ObTenantDagWorker::set_task(ObITask *task)
{
  task_ = task;
  hold_by_compaction_dag_ = false;

  ObIDag *dag = nullptr;
  if (OB_NOT_NULL(task_) && OB_NOT_NULL(dag = task_->get_dag())) {
    hold_by_compaction_dag_ = is_compaction_dag(dag->get_type());
  }
}

void ObTenantDagWorker::run1()
{
  self_ = this;
  reset_compaction_thread_locals();

  int ret = OB_SUCCESS;
  ObIDag *dag = NULL;
  ObITask *cur_task = NULL;
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
      } else if (ObDagType::DAG_TYPE_MINI_MERGE == dag->get_type()
              && static_cast<compaction::ObTabletMergeDag *>(dag)->is_reserve_mode()) {
        is_reserve_mode_ = true;
        COMMON_LOG(INFO, "Mini compaction enter reserve mode", KPC(dag));
      }

      if (OB_SUCC(ret)) {
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
      cur_task = task_;
      task_ = NULL;
      if (OB_FAIL(MTL(ObTenantDagScheduler*)->deal_with_finish_task(*cur_task, *this, ret/*task error_code*/))) {
        COMMON_LOG(WARN, "failed to finish task", K(ret), K(*task_));
      }
      ObCurTraceId::reset();
      lib::set_thread_name("DAG");
      reset_compaction_thread_locals();
    } else {
      ObThreadCondGuard guard(cond_);
      while (NULL == task_ && DWS_FREE == status_ && !has_set_stop()) {
        cond_.wait(SLEEP_TIME_MS);
      }
    }
  } // end of while
}

int ObTenantDagWorker::yield()
{
  int ret = OB_SUCCESS;
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
      if (get_force_cancel_flag()) {
        ret = OB_CANCELED;
        COMMON_LOG(INFO, "Cancel this worker since the whole dag is canceled", K(ret));
      } else if (DWS_RUNNING == status_ && MTL(ObTenantDagScheduler*)->try_switch(*this)) {
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
        if (get_force_cancel_flag()) {
          ret = OB_CANCELED;
          COMMON_LOG(INFO, "Cancel this worker since the whole dag is canceled", K(ret));
        }
      }
    }
  }

  return ret;
}

/***************************************ObDagPrioScheduler impl********************************************/
void ObDagPrioScheduler::destroy()
{
  int tmp_ret = OB_SUCCESS;
  int64_t abort_dag_cnt = 0;
  for (int64_t j = 0; j < DAG_LIST_MAX; ++j) {
    ObIDag *head = dag_list_[j].get_header();
    ObIDag *cur_dag = head->get_next();
    ObIDag *next = NULL;
    while (NULL != cur_dag && head != cur_dag) {
      next = cur_dag->get_next();
      if (OB_TMP_FAIL(ObSysTaskStatMgr::get_instance().del_task(cur_dag->get_dag_id()))) {
        if (OB_ENTRY_NOT_EXIST != tmp_ret) {
          STORAGE_LOG_RET(WARN, tmp_ret, "failed to del sys task", K(cur_dag->get_dag_id()));
        }
      }

      if (OB_TMP_FAIL(finish_dag_(ObIDag::DAG_STATUS_ABORT, *cur_dag, false/*try_move_child*/))) {
        STORAGE_LOG_RET(WARN, tmp_ret, "failed to abort dag", K(tmp_ret), KPC(cur_dag));
      } else {
        ++abort_dag_cnt;
      }
      cur_dag = next;
    }
    dag_list_[j].reset();
  }

  if (dag_map_.created()) {
    dag_map_.destroy();
  }
  running_task_cnts_ = 0;
  allocator_ = nullptr;
  ha_allocator_ = nullptr;
  running_workers_.reset();
  waiting_workers_.reset();
  COMMON_LOG(INFO, "ObDagPrioScheduler destroyed", K(abort_dag_cnt));
}

void ObDagPrioScheduler::destroy_workers()
{
  ObMutexGuard guard(prio_lock_);
  for (int64_t j = 0; j < DAG_LIST_MAX; ++j) {
    DagList &dl = dag_list_[j];
    DLIST_FOREACH_NORET(dag, dl) {
      dag->set_stop();
    }
  }
  WorkerList &wl = waiting_workers_;
  DLIST_FOREACH_NORET(worker, wl) {
    worker->resume();
  }
}

int ObDagPrioScheduler::init(
    const uint64_t tenant_id,
    const int64_t dag_limit,
    const int64_t priority,
    ObIAllocator &allocator,
    ObIAllocator &ha_allocator,
    ObTenantDagScheduler &scheduler)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || 0 >= dag_limit) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "init ObDagPrioScheduler with invalid arguments", K(ret), K(tenant_id), K(dag_limit));
  } else if (OB_FAIL(dag_map_.create(dag_limit, "DagMap", "DagNode", tenant_id))) {
    COMMON_LOG(WARN, "failed to create dap map", K(ret), K(dag_limit));
  } else {
    allocator_ = &allocator;
    ha_allocator_ = &ha_allocator;
    scheduler_ = &scheduler;
    priority_ = priority;
    running_task_cnts_ = 0;
    limits_ = OB_DAG_PRIOS[priority].score_;
    adaptive_task_limit_ = limits_;
  }
  return ret;
}

// call this func with locked
int ObDagPrioScheduler::move_dag_to_list_(
    ObIDag &dag,
    enum ObDagListIndex from_list_index,
    enum ObDagListIndex to_list_index,
    const bool add_last)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(from_list_index >= DAG_LIST_MAX || to_list_index >= DAG_LIST_MAX
      || from_list_index != dag.get_list_idx())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "list index is invalid", K(ret), K(from_list_index));
  } else if (OB_UNLIKELY(dag.get_priority() != priority_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpect priority", K(ret), K(dag.get_priority()), K_(priority));
  } else if (!dag_list_[from_list_index].remove(&dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "failed to remove dag from dag list", K(from_list_index), K(dag.get_priority()), K(dag));
  } else {
    bool add_ret = false;
    if (add_last) {
      add_ret = dag_list_[to_list_index].add_last(&dag);
    } else {
      add_ret = dag_list_[to_list_index].add_first(&dag);
    }
    if (OB_UNLIKELY(!add_ret)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "failed to move dag to dag list", K(ret), K(to_list_index), K(dag.get_priority()), K(dag));
      ob_abort();
    } else {
      dag.set_list_idx(to_list_index);
    }
  }
  return ret;
}

int ObDagPrioScheduler::get_stored_dag_(ObIDag &dag, ObIDag *&stored_dag)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dag_map_.get_refactored(&dag, stored_dag))) {
    COMMON_LOG(WARN, "failed to get stored dag", K(ret));
  } else if (OB_ISNULL(stored_dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "stored_dag is unexpected null", K(ret), KP(stored_dag));
  } else if (stored_dag->get_priority() != dag.get_priority()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpected priority value", K(ret), K(stored_dag->get_priority()), K(dag.get_priority()));
  }
  return ret;
}

// call this func with locked
int ObDagPrioScheduler::add_dag_into_list_and_map_(
    const ObDagListIndex list_index,
    ObIDag &dag,
    const bool emergency)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(list_index >= DAG_LIST_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(list_index));
  } else if (is_rank_dag_type(dag.get_type()) && RANK_DAG_LIST != list_index) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "compaction dag should be add into rank dag list at first", K(ret), K(list_index), K(dag));
  } else if (OB_UNLIKELY(dag.get_priority() != priority_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpect priority", K(ret), K(dag.get_priority()), K_(priority));
  } else if (OB_FAIL(dag_map_.set_refactored(&dag, &dag))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_EAGAIN;
      ObIDag *stored_dag = nullptr;
      compaction::ObTabletMergeDag *merge_dag = nullptr;
      // Accumulate compaction weights, only Mini Compaction needs.
      if (is_mini_compaction_dag(dag.get_type())) {
        if (OB_TMP_FAIL(get_stored_dag_(dag, stored_dag))) {
          COMMON_LOG(WARN, "failed to get stored dag", K(tmp_ret));
        } else if (OB_ISNULL(merge_dag = static_cast<compaction::ObTabletMergeDag *>(stored_dag))) {
          tmp_ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "get unexpected null stored dag", K(tmp_ret));
        } else if (OB_TMP_FAIL(merge_dag->update_compaction_param(
            static_cast<compaction::ObTabletMergeDag *>(&dag)->get_param()))) {
          COMMON_LOG(WARN, "failed to add compaction param", K(tmp_ret));
        }
      }
    } else {
      COMMON_LOG(WARN, "failed to set dag_map", K(ret), K(dag));
    }
  } else {
    bool add_ret = false;
    ObDagListIndex add_list_index = emergency ? READY_DAG_LIST : list_index; // skip to rank emergency dag
    if (!emergency) {
      add_ret = dag_list_[add_list_index].add_last(&dag);
    } else {
      add_ret = dag_list_[add_list_index].add_first(&dag);
    }
    if (!add_ret) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "failed to add dag to dag_list", K(ret), K(dag.get_priority()),
          K(list_index), K(add_list_index), K(emergency), K(dag));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(dag_map_.erase_refactored(&dag))) {
        COMMON_LOG(ERROR, "failed to erase dag from dag_map", K(tmp_ret), K(dag));
        ob_abort();
      }
    } else {
      dag.set_list_idx(add_list_index);
      dag.set_dag_status(ObIDag::DAG_STATUS_READY);
      dag.set_add_time();
    }
  }
  return ret;
}

// call this func with locked
int ObDagPrioScheduler::inner_add_dag_(
    const bool emergency,
    const bool check_size_overflow,
    ObIDag *&dag)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dag) || OB_UNLIKELY(!dag->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner dag dag get invalid argument", K(ret), KPC(dag));
  } else if (OB_UNLIKELY(dag->get_priority() != priority_ || OB_ISNULL(scheduler_))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpected value", K(ret), K(dag->get_priority()), K_(priority), KP_(scheduler));
  } else if (check_size_overflow && scheduler_->dag_count_overflow(dag->get_type())) {
    ret = OB_SIZE_OVERFLOW;
    COMMON_LOG(WARN, "ObTenantDagScheduler is full", K(ret), "dag_limit", scheduler_->get_dag_limit(), KPC(dag));
  } else if (OB_FAIL(add_dag_into_list_and_map_(
          is_waiting_dag_type(dag->get_type()) ? WAITING_DAG_LIST :
          is_rank_dag_type(dag->get_type()) ? RANK_DAG_LIST : READY_DAG_LIST, // compaction dag should add into RANK_LIST first.
          *dag,
          emergency))) {
    if (OB_EAGAIN != ret) {
      COMMON_LOG(WARN, "failed to add dag into list and map", K(ret), KPC(dag));
    }
  } else {
    add_added_info_(dag->get_type());
    COMMON_LOG(INFO, "add dag success", KP(dag), "id", dag->get_dag_id(), K(dag->hash()), "dag_cnt", scheduler_->get_cur_dag_cnt(),
        "dag_type_cnts", scheduler_->get_type_dag_cnt(dag->get_type()));

    dag = nullptr;
  }
  return ret;
}

void ObDagPrioScheduler::add_schedule_info_(const ObDagType::ObDagTypeEnum dag_type, const int64_t data_size)
{
  scheduler_->add_total_running_task_cnt();
  scheduler_->add_scheduled_task_cnt();
  scheduler_->add_running_dag_cnts(dag_type);
  scheduler_->add_scheduled_task_cnts(dag_type);
  scheduler_->add_scheduled_data_size(dag_type, data_size);
  ++running_task_cnts_;
}

void ObDagPrioScheduler::add_added_info_(const ObDagType::ObDagTypeEnum dag_type)
{
  scheduler_->add_cur_dag_cnt();
  scheduler_->add_type_dag_cnt(dag_type);
  scheduler_->add_added_dag_cnts(dag_type);
}

int ObDagPrioScheduler::schedule_one_()
{
  int ret = OB_SUCCESS;
  ObTenantDagWorker *worker = NULL;
  ObITask *task = NULL;
  if (OB_UNLIKELY(OB_ISNULL(scheduler_))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpected null scheduler", K(ret), KP_(scheduler));
  } else if (!waiting_workers_.is_empty()) {
    worker = waiting_workers_.remove_first();
  } else if (OB_FAIL(pop_task_from_ready_list_(task))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "failed to pop task", K(ret), K_(priority));
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
    } else if (OB_FAIL(scheduler_->dispatch_task(*task, worker, priority_))) {
      task->get_dag()->reset_task_running_status(*task, ObITask::TASK_STATUS_WAITING);
      COMMON_LOG(WARN, "failed to dispatch task", K(ret));
    } else {
      task->set_status(ObITask::TASK_STATUS_RUNNING);
    }
  }
  if (OB_SUCC(ret) && NULL != worker) {
    add_schedule_info_(worker->get_task()->get_dag()->get_type(), worker->get_task()->get_dag()->get_data_size());
    running_workers_.add_last(worker);
    if (task != NULL) {
      COMMON_LOG(INFO, "schedule one task", KPC(task), "priority", OB_DAG_PRIOS[priority_].dag_prio_str_,
          "total_running_task_cnt", scheduler_->get_total_running_task_cnt(),
          "running_task_cnts_", running_task_cnts_, "limits_", limits_,
          "adaptive_task_limit", adaptive_task_limit_, KP(task->get_dag()->get_dag_net()));
    }
    worker->resume();
  }
  ObCurTraceId::reset();
  return ret;
}

int ObDagPrioScheduler::sys_task_start(ObIDag &dag)
{
  int ret = OB_SUCCESS;
  if (dag.get_dag_status() != ObIDag::DAG_STATUS_READY) {
    COMMON_LOG(ERROR," dag status error",K(ret),K(dag.get_dag_status()));
  } else {
    ObSysTaskStat sys_task_status;
    sys_task_status.start_time_ = ObTimeUtility::fast_current_time();
    sys_task_status.task_id_ = dag.get_dag_id();
    sys_task_status.tenant_id_ = MTL_ID();
    sys_task_status.task_type_ = OB_DAG_TYPES[dag.get_type()].sys_task_type_;

    // allow comment truncation, no need to set ret
    (void) dag.fill_comment(sys_task_status.comment_,sizeof(sys_task_status.comment_));
    if (OB_SUCCESS != (ret = ObSysTaskStatMgr::get_instance().add_task(sys_task_status))) {
      COMMON_LOG(WARN, "failed to add sys task", K(ret), K(sys_task_status));
    } else if (OB_SUCCESS != (ret = dag.set_dag_id(sys_task_status.task_id_))) { // may generate task_id in ObSysTaskStatMgr::add_task
      COMMON_LOG(WARN,"failed to set dag id",K(ret), K(sys_task_status.task_id_));
    }
  }
  return ret;
}

int ObDagPrioScheduler::schedule_dag_(ObIDag &dag, bool &move_dag_to_waiting_list)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool unused_tmp_bret = false;
  ObIDag::ObDagStatus next_dag_status = dag.get_dag_status();
  if (OB_NOT_NULL(dag.get_dag_net()) && dag.get_dag_net()->is_cancel()) {
    next_dag_status = ObIDag::DAG_STATUS_NODE_FAILED;
    dag.set_dag_ret(OB_CANCELED);
    LOG_INFO("dag net is cancel", K(dag));
  } else if (!dag.check_can_schedule()) { // cur dag can't be scheduled now
    move_dag_to_waiting_list = true;
  } else { // dag can be scheduled
    if (ObIDag::DAG_STATUS_READY == dag.get_dag_status()) {
      if (OB_TMP_FAIL(sys_task_start(dag))) {
        COMMON_LOG(WARN, "failed to start sys task", K(tmp_ret));
      }
      if (OB_TMP_FAIL(generate_next_dag_(dag))) {
        LOG_WARN("failed to generate next dag", K(ret), K(dag));
      }
    }
    next_dag_status = ObIDag::DAG_STATUS_NODE_RUNNING;
  }
  dag.set_dag_status(next_dag_status);
  dag.update_status_in_dag_net(unused_tmp_bret /* dag_net_finished */);
  dag.set_start_time(); // dag start running
  scheduler_->add_scheduled_dag_cnts(dag.get_type());
  COMMON_LOG(DEBUG, "dag start running", K(ret), K(dag));
  return ret;
}

// should hold prio_lock_ before calling this func
bool ObDagPrioScheduler::check_need_compaction_rank_() const
{
  bool bret = true;
  if (!is_rank_dag_prio()) {
    bret = false;
  } else if (dag_list_[RANK_DAG_LIST].is_empty()) {
    bret = false;
  } else if (dag_list_[READY_DAG_LIST].get_size() >= adaptive_task_limit_ * COMPACTION_DAG_RERANK_FACTOR) {
    bret = false;
  }
  return bret;
}

int ObDagPrioScheduler::do_rank_compaction_dags_(
    const int64_t batch_size,
    common::ObSEArray<compaction::ObTabletMergeDag *, 32> &rank_dags)
{
  int ret = OB_SUCCESS;
  rank_dags.reset();
  compaction::ObCompactionDagRanker ranker;
  common::ObSEArray<compaction::ObTabletMergeDag *, 32> need_rank_dags;
  const int64_t cur_time = common::ObTimeUtility::current_time();

  if (OB_UNLIKELY(batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(batch_size));
  } else if (OB_FAIL(ranker.init(priority_, cur_time))) {
    LOG_WARN("failed to init dag ranker", K(ret), K_(priority));
  } else {
    ObIDag *head = dag_list_[RANK_DAG_LIST].get_header();
    ObIDag *cur = head->get_next();
    compaction::ObTabletMergeDag *compaction_dag = nullptr;
    ObIDag::ObDagStatus dag_status = ObIDag::DAG_STATUS_MAX;
    while (OB_SUCC(ret) && NULL != cur && head != cur) {
      const ObIDag::ObDagStatus &dag_status = cur->get_dag_status();
      if (OB_ISNULL(compaction_dag = static_cast<compaction::ObTabletMergeDag *>(cur))) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "get unexpected null dag", K(ret), K_(priority), KPC(cur));
      } else if (ObIDag::DAG_STATUS_INITING == dag_status) {
        // do nothing
      } else if (ObIDag::DAG_STATUS_READY != dag_status) { // dag status must be INITING or READY
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "dag in rank list must be ready", K(ret), K(dag_status), KPC(compaction_dag));
      } else if (is_meta_major_merge(compaction_dag->get_param().merge_type_)) {
        if (OB_FAIL(rank_dags.push_back(compaction_dag))) {
          COMMON_LOG(WARN, "failed to add meta merge dag", K(ret), KPC(compaction_dag));
        }
      } else if (OB_FAIL(need_rank_dags.push_back(compaction_dag))) {
        COMMON_LOG(WARN, "failed to add compaction dag", K(ret), KPC(compaction_dag));
      } else {
        ranker.update(cur_time, compaction_dag->get_param().compaction_param_);
      }

      if (OB_SUCC(ret)) {
        cur = cur->get_next();
        if (rank_dags.count() + need_rank_dags.count() >= batch_size) {
          break; // reached max rank count, stop adding ready dags
        }
      }
    } // end while

    if (OB_SUCC(ret) && !need_rank_dags.empty() && ranker.is_valid()) {
      if (OB_FAIL(ranker.sort(need_rank_dags))) {
        COMMON_LOG(WARN, "failed to sort compaction dags, move dags to ready list directly",
            K(ret), K(need_rank_dags));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < need_rank_dags.count(); ++i) {
          if (OB_FAIL(rank_dags.push_back(need_rank_dags.at(i)))) {
            COMMON_LOG(WARN, "failed to add rank dags", K(ret));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret) && !rank_dags.empty()) {
    rank_dags.reset();
  }
  return ret;
}

int ObDagPrioScheduler::batch_move_compaction_dags_(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObIDag *head = dag_list_[RANK_DAG_LIST].get_header();
  ObIDag *cur = head->get_next();
  ObIDag *tmp_dag = nullptr;
  int64_t move_cnt = 0;

  while (OB_SUCC(ret) && NULL != cur && head != cur) {
    tmp_dag = cur;
    cur = cur->get_next();
    if (OB_FAIL(move_dag_to_list_(*tmp_dag, RANK_DAG_LIST, READY_DAG_LIST))) {
      COMMON_LOG(ERROR, "failed to move dag from RANK_LIST to READY_LIST", K(ret), K_(priority), KPC(tmp_dag));
    } else if (++move_cnt >= batch_size) {
      break;
    }
  }
  return ret;
}

// should hold prio_lock_ before calling this func
int ObDagPrioScheduler::rank_compaction_dags_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t batch_size = adaptive_task_limit_ * COMPACTION_DAG_RERANK_FACTOR;
  const bool need_adaptive_schedule = MTL(compaction::ObTenantTabletScheduler *)->enable_adaptive_merge_schedule();

  if (!check_need_compaction_rank_()) {
    // ready list has plenty of dags, no need to rank new dags
  } else if (ObDagPrio::DAG_PRIO_COMPACTION_LOW == priority_) {
    int64_t prepare_co_dag_cnt = 0;
    ObIDag *head = dag_list_[READY_DAG_LIST].get_header();
    ObIDag *cur = head->get_next();
    while (NULL != cur && head != cur) {
      if (ObDagType::DAG_TYPE_CO_MERGE_PREPARE == cur->get_type() ||
          ObDagType::DAG_TYPE_CO_MERGE_SCHEDULE == cur->get_type()) {
        ++prepare_co_dag_cnt;
      }
      cur = cur->get_next();
    }

    if (prepare_co_dag_cnt * 2 >= adaptive_task_limit_) {
      // exist co prepare dags wait to schedule
    } else if (OB_FAIL(batch_move_compaction_dags_(0 == prepare_co_dag_cnt ? batch_size : limits_))) {
      COMMON_LOG(WARN, "failed to move co prepare dag from wait list to ready list", K(ret), K(limits_));
    }

  } else if (!need_adaptive_schedule) {
    // not allow rerank, move all dags in rank_list to ready list directly
    if (OB_FAIL(batch_move_compaction_dags_(dag_list_[RANK_DAG_LIST].get_size()))) {
      COMMON_LOG(WARN, "failed to move co prepare dag from wait list to ready list", K(ret), K(limits_));
    }
  } else {
    const int64_t cur_time = common::ObTimeUtility::fast_current_time();
    ObSEArray<compaction::ObTabletMergeDag *, 32> ranked_dags;

    if (OB_TMP_FAIL(do_rank_compaction_dags_(batch_size, ranked_dags))) {
      COMMON_LOG(WARN, "failed to sort compaction dags", K(tmp_ret), K_(priority));
    }

    if (!ranked_dags.empty()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < ranked_dags.count(); ++i) {
        ObIDag *cur_dag = ranked_dags.at(i);
        if (OB_FAIL(move_dag_to_list_(*cur_dag, RANK_DAG_LIST, READY_DAG_LIST))) {
          COMMON_LOG(ERROR, "failed to move dag from RANK_LIST to READY_LIST", K(ret), KPC(cur_dag));
        }
      }
    } else if (OB_FAIL(batch_move_compaction_dags_(batch_size))) {
      COMMON_LOG(WARN, "failed to move compaction dags to ready list", K(ret), K(batch_size));
    }

    // Time Statistics
    int64_t cost_time = common::ObTimeUtility::fast_current_time() - cur_time;
    if (REACH_TENANT_TIME_INTERVAL(DUMP_STATUS_INTERVAL)) {
      COMMON_LOG(INFO, "[ADAPTIVE_SCHED] Ranking compaction dags costs: ", K(cost_time), K_(priority), K(ret), K(tmp_ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (need_adaptive_schedule) {
    try_update_adaptive_task_limit_(batch_size);
  } else {
    adaptive_task_limit_ = limits_;
  }

  return ret;
}

// should be called under prio_lock_
void ObDagPrioScheduler::try_update_adaptive_task_limit_(const int64_t batch_size)
{
  int tmp_ret = OB_SUCCESS;
  double min_cpu = 0.0;
  double max_cpu = 0.0;

  if (dag_list_[READY_DAG_LIST].get_size() <= batch_size) {
    adaptive_task_limit_ = limits_; // dag count is OK, reset to the default value
  } else if (OB_TMP_FAIL(GCTX.omt_->get_tenant_cpu(MTL_ID(), min_cpu, max_cpu))) {
    COMMON_LOG_RET(WARN, tmp_ret, "failed to get tenant cpu count");
  } else if (std::round(max_cpu) * ADAPTIVE_PERCENT <= adaptive_task_limit_) {
    // reaches the 40% cpu, no need to expand
  } else {
    // we assume all compaction tasks are serial
    const int64_t estimate_mem_per_thread = compaction::ObCompactionEstimator::MAX_MEM_PER_THREAD;
    const int64_t mem_allow_max_thread = lib::get_tenant_memory_remain(MTL_ID()) * ADAPTIVE_PERCENT / estimate_mem_per_thread;
    if (mem_allow_max_thread >= adaptive_task_limit_ * 5) {
      ++adaptive_task_limit_;
      FLOG_INFO("[ADAPTIVE_SCHED] increment adaptive task limit", K(priority_), K(adaptive_task_limit_));
    }
  }
}

// under prio_lock_
int ObDagPrioScheduler::pop_task_from_ready_list_(ObITask *&task)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  task = nullptr;

  // adaptive compaction scheduling
  if (is_rank_dag_prio() && OB_TMP_FAIL(rank_compaction_dags_())) {
    COMMON_LOG(WARN, "[ADAPTIVE_SCHED] Failed to rank compaction dags", K(tmp_ret), K_(priority));
  }

  if (!dag_list_[READY_DAG_LIST].is_empty()) {
    ObIDag *head = dag_list_[READY_DAG_LIST].get_header();
    ObIDag *cur = head->get_next();
    ObIDag *tmp_dag = nullptr;
    ObITask *ready_task = nullptr;
    ObIDag::ObDagStatus dag_status = ObIDag::DAG_STATUS_MAX;

    while (NULL != cur && head != cur && OB_SUCC(ret)) {
      bool move_dag_to_waiting_list = false;
      dag_status = cur->get_dag_status();
#ifdef ERRSIM
      ObIDagNet *tmp_dag_net = nullptr;
      ret = OB_E(EventTable::EN_CO_MREGE_DAG_READY_FOREVER) ret;
      if (OB_FAIL(ret)) {
        ret = OB_SUCCESS;
        if (OB_NOT_NULL(tmp_dag_net = cur->get_dag_net()) && tmp_dag_net->is_co_dag_net()) {
          LOG_INFO("ERRSIM EN_CO_MREGE_DAG_READY_FOREVER", K(ret));
          if (tmp_dag_net->is_cancel()) {
            LOG_INFO("ERRSIM EN_CO_MREGE_DAG_READY_FOREVER CO MERGE DAG IS CANCELED", K(ret));
          } else {
            cur = cur->get_next();
            continue;
          }
        }
      }
#endif
      if (!cur->check_with_lock()) {
        // TODO(@jingshui) cancel dag
      } else if (cur->get_indegree() > 0) {
        move_dag_to_waiting_list = true;
      } else if (ObIDag::DAG_STATUS_READY == dag_status
          || ObIDag::DAG_STATUS_RETRY == dag_status) { // first schedule this dag
        if (OB_FAIL(schedule_dag_(*cur, move_dag_to_waiting_list))) {
          COMMON_LOG(WARN, "failed to schedule dag", K(ret), KPC(cur));
        }
      } else if (ObIDag::DAG_STATUS_NODE_FAILED == dag_status
          && 0 == cur->get_running_task_count()) { // no task running failed dag, need free
        tmp_dag = cur;
        cur = cur->get_next();
        if (OB_FAIL(finish_dag_(ObIDag::DAG_STATUS_ABORT, *tmp_dag, true/*try_move_child*/))) { // will report result
          COMMON_LOG(WARN, "failed to deal with failed dag", K(ret), KPC(tmp_dag));
          ob_abort();
        }
        continue;
      }

      if (move_dag_to_waiting_list) {
        tmp_dag = cur;
        cur = cur->get_next();
        if (OB_FAIL(move_dag_to_list_(*tmp_dag, READY_DAG_LIST, WAITING_DAG_LIST))) {
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

int ObDagPrioScheduler::generate_next_dag_(ObIDag &dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIDag *next_dag = nullptr;
  ObIDag *child_dag = nullptr;
  ObIDagNet *dag_net = nullptr;
  const bool emergency = false;
  const bool check_size_overflow = true;

  if (OB_UNLIKELY(dag.get_priority() != priority_ || OB_ISNULL(scheduler_))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpect value", K(ret), K(dag.get_priority()), K_(priority), KP_(scheduler));
  } else {
    if (OB_FAIL(dag.generate_next_dag(next_dag))) {
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
      if (FALSE_IT(dag_net = dag.get_dag_net())) {
      } else if (OB_NOT_NULL(dag_net) && OB_FAIL(dag_net->add_dag_into_dag_net(*next_dag))) {
        LOG_WARN("failed to add dag into dag net", K(ret), KPC(next_dag));
      } else if (OB_FAIL(next_dag->create_first_task())) {
        LOG_WARN("failed to create first task", K(ret), KPC(next_dag));
      } else {
        COMMON_LOG(INFO, "succeed generate next dag", K(dag), KP(next_dag));
        const common::ObIArray<ObINodeWithChild*> &child_array = dag.get_child_nodes();
        if (child_array.empty()) {
          //do nothing
        } else if (OB_FAIL(next_dag->add_child_without_inheritance(child_array))) {
          LOG_WARN("failed to add child without inheritance", K(ret), K(dag));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(inner_add_dag_(emergency, check_size_overflow, next_dag))) {
        LOG_WARN("failed to add next dag", K(ret), KPC(next_dag));
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(next_dag)) {
        if (OB_TMP_FAIL(dag.report_result())) {
          COMMON_LOG(WARN, "failed to report result", K(tmp_ret), K(dag));
        }
        scheduler_->free_dag(*next_dag);
      }
    }
  }
  return ret;
}

int ObDagPrioScheduler::add_dag_warning_info_into_dag_net_(ObIDag &dag, bool &need_add)
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = dag.get_dag_net();
  if (OB_NOT_NULL(dag_net) && dag_net->is_co_dag_net()) {
    if (OB_FAIL(dag_net->add_dag_warning_info(&dag))) {
      COMMON_LOG(WARN, "failed to add dag warning info into dag net", K(ret), K(dag), KPC(dag_net));
    } else {
      need_add = false;
    }
  }
  return ret;
}

// when cancel_dag(), the dag may have parent
int ObDagPrioScheduler::finish_dag_(
    const ObIDag::ObDagStatus status,
    ObIDag &dag,
    const bool try_move_child)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_add = is_compaction_dag(dag.get_type()); // need add compaction dag warning info
  bool dag_net_finished = false;
  ObIDagNet *dag_net = dag.get_dag_net();
  if (OB_UNLIKELY(dag.get_priority() != priority_ || OB_ISNULL(scheduler_))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpect value", K(ret), K(dag.get_priority()), K_(priority), KP_(scheduler));
  } else if (FALSE_IT(add_dag_warning_info_into_dag_net_(dag, need_add))) {
  } else if (OB_FAIL(dag.finish(status, dag_net_finished))) { // dag record will be erase, making this dag_net could be free.
    COMMON_LOG(WARN, "dag finished failed", K(ret), K(dag), "dag_ret", dag.get_dag_ret());
  } else if (try_move_child && OB_FAIL(try_move_child_to_ready_list_(dag))) {
    LOG_WARN("failed to try move child to ready list", K(ret), K(&dag));
  } else if (OB_FAIL(erase_dag_(dag))) {
    COMMON_LOG(ERROR, "failed to erase dag from dag_map", K(ret), K(dag));
  } else {
    LOG_INFO("dag finished", "dag_ret", dag.get_dag_ret(),
        "runtime", ObTimeUtility::fast_current_time() - dag.get_start_time(),
        "dag_cnt", scheduler_->get_cur_dag_cnt(), "dag_type_cnt", scheduler_->get_type_dag_cnt(dag.get_type()),
        K(&dag), K(dag));
    if (OB_TMP_FAIL(ObSysTaskStatMgr::get_instance().del_task(dag.get_dag_id()))) {
      STORAGE_LOG(WARN, "failed to del sys task", K(tmp_ret), K(dag.get_dag_id()));
    }
    if (OB_TMP_FAIL(dag.report_result())) {
      COMMON_LOG(WARN, "failed to report result", K(tmp_ret), K(dag));
    }
    compaction::ObCompactionSuggestionMgr *suggestion_mgr = MTL(compaction::ObCompactionSuggestionMgr *);
    if (OB_NOT_NULL(suggestion_mgr)
      && OB_TMP_FAIL(suggestion_mgr->update_finish_cnt(
        dag.get_type(),
        OB_SUCCESS != dag.get_dag_ret(),
        ObTimeUtility::fast_current_time() - dag.get_start_time()))) {
      COMMON_LOG(WARN, "failed to update finish cnt", K(tmp_ret), K_(priority), K(dag));
    }

    // compaction dag success
    if (ObIDag::DAG_STATUS_FINISH == status && is_compaction_dag(dag.get_type())) {
      ObTenantCompactionMemPool *mem_pool = MTL(ObTenantCompactionMemPool *);
      if (OB_NOT_NULL(mem_pool)) {
        mem_pool->set_memory_mode(ObTenantCompactionMemPool::NORMAL_MODE);
      }
    }

    if (need_add) {
      if (OB_TMP_FAIL(MTL(ObDagWarningHistoryManager*)->add_dag_warning_info(&dag))) {
        COMMON_LOG(WARN, "failed to add dag warning info", K(tmp_ret), K(dag));
      } else if (ObDagType::DAG_TYPE_BATCH_FREEZE_TABLETS == dag.get_type()) {
        // no need to add diagnose
      } else {
        compaction::ObTabletMergeDag *merge_dag = static_cast<compaction::ObTabletMergeDag*>(&dag);
        if (OB_SUCCESS != dag.get_dag_ret()) {
          if (OB_TMP_FAIL(MTL(compaction::ObDiagnoseTabletMgr *)->add_diagnose_tablet(
                merge_dag->ls_id_, merge_dag->tablet_id_, ObIDag::get_diagnose_tablet_type(dag.get_type())))) {
            COMMON_LOG(WARN, "failed to add diagnose tablet", K(tmp_ret),
                "ls_id", merge_dag->ls_id_, "tablet_id", merge_dag->tablet_id_);
          }
        } else if (OB_TMP_FAIL(MTL(compaction::ObDiagnoseTabletMgr *)->delete_diagnose_tablet(
              merge_dag->ls_id_, merge_dag->tablet_id_, ObIDag::get_diagnose_tablet_type(dag.get_type())))) {
          if (OB_HASH_NOT_EXIST != tmp_ret) {
            COMMON_LOG(WARN, "failed to delete diagnose tablet", K(tmp_ret),
              "ls_id", merge_dag->ls_id_, "tablet_id", merge_dag->tablet_id_);
          }
        }
      }
    }

    scheduler_->inner_free_dag(dag); // free after log print
    if (OB_NOT_NULL(dag_net) && dag_net_finished) {
      dag_net->set_last_dag_finished();
      scheduler_->notify_when_dag_net_finish();
    }
  }
  return ret;
}

int ObDagPrioScheduler::try_move_child_to_ready_list_(ObIDag &dag)
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
      } else if (dag.get_priority() == child_dag->get_priority() // for same priority dag, could move list under curr lock
          && WAITING_DAG_LIST == child_dag->get_list_idx()
          && 0 == child_dag->get_indegree()
          && child_dag->check_can_schedule()) {
        if (OB_FAIL(move_dag_to_list_(*child_dag, WAITING_DAG_LIST, READY_DAG_LIST, false/*add_last*/))) {
          COMMON_LOG(WARN, "failed to move dag from waitinig list to ready list", K(ret), KPC(child_dag));
        }
      }
    }
  }
  return ret;
}

int ObDagPrioScheduler::erase_dag_(ObIDag &dag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(scheduler_))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpected null scheduler", K(ret), KP_(scheduler));
  } else if (OB_FAIL(dag_map_.erase_refactored(&dag))) {
    COMMON_LOG(ERROR, "failed to erase dag from dag_map", K(ret), K(dag));
    ob_abort();
  } else if (!dag_list_[dag.get_list_idx()].remove(&dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "failed to remove dag from dag list", K(dag));
  } else {
    scheduler_->sub_cur_dag_cnt();
    scheduler_->sub_type_dag_cnt(dag.get_type());
  }
  return ret;
}

int ObDagPrioScheduler::deal_with_fail_dag_(ObIDag &dag, bool &retry_flag)
{
  int ret = OB_SUCCESS;
  // dag retry is triggered by last finish task
  if (OB_UNLIKELY(dag.get_force_cancel_flag())) {
  } else if (1 == dag.get_running_task_count() && dag.check_can_retry()) {
    COMMON_LOG(INFO, "dag retry", K(ret), K(dag));
    if (OB_FAIL(dag.reset_status_for_retry())) { // clear task/running_info and init again
      COMMON_LOG(WARN, "failed to reset status for retry", K(ret), K(dag));
    } else {
      if (OB_FAIL(move_dag_to_list_(dag, READY_DAG_LIST, WAITING_DAG_LIST))) {
        COMMON_LOG(WARN, "failed to move dag to waiting list", K(ret), K(dag));
      } else {
        retry_flag = true;
      }
    }
  }
  return ret;
}

int ObDagPrioScheduler::finish_task_in_dag_(
    ObITask &task,
    ObIDag &dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (0 == dag.get_task_list_count()) {
    // dag::task_list is empty means dag have entered retry process, but failed
  } else if (OB_TMP_FAIL(dag.finish_task(task))) {
    dag.set_dag_status(ObIDag::DAG_STATUS_NODE_FAILED);
    dag.set_dag_ret(tmp_ret);
    COMMON_LOG_RET(WARN, tmp_ret, "failed to finish task");
  } else {
    COMMON_LOG_RET(DEBUG, tmp_ret, "success to finish task", K(&dag));
  }

  if (dag.has_finished()) {
    ObIDag::ObDagStatus status =
        dag.is_dag_failed() ? ObIDag::DAG_STATUS_ABORT : ObIDag::DAG_STATUS_FINISH;
    if (OB_FAIL(finish_dag_(status, dag, true/*try_move_child*/))) {
      COMMON_LOG(WARN, "failed to finish dag and dag net", K(ret), K(dag));
      ob_abort();
    }
  }
  return ret;
}

void ObDagPrioScheduler::pause_worker_(ObTenantDagWorker &worker)
{
  if (OB_UNLIKELY(OB_ISNULL(scheduler_))) {
    COMMON_LOG_RET(WARN, OB_ERR_UNEXPECTED, "unexpected null scheduler", KP_(scheduler));
  } else {
    scheduler_->sub_total_running_task_cnt();
    // dag still exist now
    scheduler_->sub_running_dag_cnts(worker.get_task()->get_dag()->get_type());
    --running_task_cnts_;
    running_workers_.remove(&worker);
    waiting_workers_.add_last(&worker);
    COMMON_LOG(INFO, "pause worker", K(*worker.get_task()),
        "priority", OB_DAG_PRIOS[priority_].dag_prio_str_,
        K(running_task_cnts_), "total_running_task_cnt", scheduler_->get_total_running_task_cnt());
  }
}

int ObDagPrioScheduler::loop_ready_dag_list(bool &is_found)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  {
    ObMutexGuard guard(prio_lock_);
    if (running_task_cnts_ < adaptive_task_limit_) {
      // if extra_erase_dag_net not null, the is_found must be false.
      if (!check_need_load_shedding_(true/*for_schedule*/)) {
        is_found = (OB_SUCCESS == schedule_one_());
      }

      while (running_task_cnts_ < adaptive_task_limit_ && is_found) {
        if (check_need_load_shedding_(true/*for_schedule*/)) {
          break;
        } else if (OB_SUCCESS != schedule_one_()) {
          break;
        }
      }
    }
  }

  return ret;
}

int ObDagPrioScheduler::loop_waiting_dag_list()
{
  int ret = OB_SUCCESS;
  {
    ObMutexGuard guard(prio_lock_);
    if (!dag_list_[WAITING_DAG_LIST].is_empty()) {
      int64_t moving_dag_cnt = 0;
      ObIDag *head = dag_list_[WAITING_DAG_LIST].get_header();
      ObIDag *cur = head->get_next();
      ObIDag *move_dag = nullptr;
      while (NULL != cur && head != cur && OB_SUCC(ret)) {
        if (0 == cur->get_indegree() && OB_NOT_NULL(cur->get_dag_net()) && cur->get_dag_net()->is_cancel()) {
          move_dag = cur;
          cur = cur->get_next();
          if (OB_FAIL(finish_dag_(ObIDag::DAG_STATUS_ABORT, *move_dag, true/*try_move_child*/))) { // will report result
            COMMON_LOG(WARN, "failed to deal with failed dag", K(ret), KPC(cur));
            ob_abort();
          }
        } else if (0 == cur->get_indegree() && cur->check_can_schedule()) {
          move_dag = cur;
          cur = cur->get_next();
          if (OB_FAIL(move_dag_to_list_(*move_dag, WAITING_DAG_LIST, READY_DAG_LIST))) {
            COMMON_LOG(WARN, "failed to move dag to low priority list", K(ret), KPC(move_dag));
          } else {
            ++moving_dag_cnt;
            COMMON_LOG(DEBUG, "move dag to ready list", K(ret), KPC(move_dag),
                "waiting_list_size", dag_list_[WAITING_DAG_LIST].get_size(),
                "ready_list_size", dag_list_[READY_DAG_LIST].get_size());
          }
        } else {
          if (ObTimeUtility::fast_current_time() - cur->get_add_time() > DUMP_STATUS_INTERVAL)  {
            COMMON_LOG(DEBUG, "print remain waiting dag", K(ret), KPC(cur), "indegree", cur->get_indegree(),
                "check_can_schedule", cur->check_can_schedule());
          }
          cur = cur->get_next();
        }
      } // end of while
      if (OB_SUCC(ret)) {
        COMMON_LOG(INFO, "loop_waiting_dag_list", K(ret), K_(priority), K(moving_dag_cnt),
            "remain waiting_list_size", dag_list_[WAITING_DAG_LIST].get_size(),
            "ready_list_size", dag_list_[READY_DAG_LIST].get_size());
      }
    }
  } // prio_lock_ unlock
  return ret;
}

void ObDagPrioScheduler::dump_dag_status()
{
  int64_t running_task;
  int64_t limits;
  int64_t adaptive_task_limit;
  int64_t ready_dag_count;
  int64_t waiting_dag_count;
  int64_t rank_dag_count;
  {
    ObMutexGuard guard(prio_lock_);
    running_task = running_task_cnts_;
    limits = limits_;
    adaptive_task_limit = adaptive_task_limit_;
    ready_dag_count = dag_list_[READY_DAG_LIST].get_size();
    waiting_dag_count = dag_list_[WAITING_DAG_LIST].get_size();
    rank_dag_count = dag_list_[RANK_DAG_LIST].get_size();
  }
  COMMON_LOG(INFO, "dump_dag_status", "priority", OB_DAG_PRIOS[priority_].dag_prio_str_,
          K(limits), K(running_task), K(adaptive_task_limit), K(ready_dag_count),
          K(waiting_dag_count), K(rank_dag_count));
}

int ObDagPrioScheduler::inner_add_dag(
    const bool emergency,
    const bool check_size_overflow,
    ObIDag *&dag)
{
  ObMutexGuard guard(prio_lock_);
  return inner_add_dag_(emergency, check_size_overflow, dag);
}

#define ADD_DAG_SCHEDULER_INFO(value_type, key_str, value) \
  { \
    info_list[idx].tenant_id_ = MTL_ID(); \
    info_list[idx].value_type_ = value_type; \
    strncpy(info_list[idx].key_, key_str, MIN(common::OB_DAG_KEY_LENGTH - 1, strlen(key_str))); \
    info_list[idx].value_ = value; \
    (void)scheduler_infos.push_back(&info_list[idx++]); \
  }

void ObDagPrioScheduler::get_all_dag_scheduler_info(
    ObDagSchedulerInfo *info_list,
    common::ObIArray<void *> &scheduler_infos,
    int64_t &idx)
{
  if (OB_NOT_NULL(info_list)) {
    ObMutexGuard guard(prio_lock_);
    ADD_DAG_SCHEDULER_INFO(ObDagSchedulerInfo::UP_LIMIT, OB_DAG_PRIOS[priority_].dag_prio_str_, limits_);
    ADD_DAG_SCHEDULER_INFO(ObDagSchedulerInfo::RUNNING_TASK_CNT, OB_DAG_PRIOS[priority_].dag_prio_str_, running_task_cnts_);
  }
}

#define ADD_DAG_INFO(cur, list_info) \
  { \
    cur->gene_dag_info(info_list[idx], list_info); \
    (void)dag_infos.push_back(&info_list[idx++]); \
  }

void ObDagPrioScheduler::get_all_dag_info(
    ObDagInfo *info_list,
    common::ObIArray<void *> &dag_infos,
    int64_t &idx, const int64_t total_cnt)
{
  if (OB_NOT_NULL(info_list)) {
    ObMutexGuard guard(prio_lock_);
    int64_t prio_cnt = 0;
    // get ready dag list
    ObIDag *head = dag_list_[READY_DAG_LIST].get_header();
    ObIDag *cur = head->get_next();
    while (head != cur && idx < total_cnt && prio_cnt++ < MAX_SHOW_DAG_CNT) {
      ADD_DAG_INFO(cur, "READY_DAG_LIST");
      cur = cur->get_next();
    }
    // get rank dag list
    head = dag_list_[RANK_DAG_LIST].get_header();
    cur = head->get_next();
    while (head != cur && idx < total_cnt && prio_cnt++ < MAX_SHOW_DAG_CNT * 2) {
      ADD_DAG_INFO(cur, "RANK_DAG_LIST");
      cur = cur->get_next();
    }
    // get waiting dag list
    head = dag_list_[WAITING_DAG_LIST].get_header();
    cur = head->get_next();
    while (head != cur && idx < total_cnt && prio_cnt++ < MAX_SHOW_DAG_CNT * 2) {
      ADD_DAG_INFO(cur, "WAITING_DAG_LIST");
      cur = cur->get_next();
    }
  }
}

int ObDagPrioScheduler::get_minor_exe_dag_info(
    compaction::ObTabletMergeExecuteDag &dag,
    ObIArray<share::ObScnRange> &merge_range_array)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(prio_lock_);
  ObIDag *head = dag_list_[READY_DAG_LIST].get_header();
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
  dag.merge_type_ = compaction::META_MAJOR_MERGE;
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
  return ret;
}

void ObDagPrioScheduler::add_compaction_info(
    int64_t &idx,
    const int64_t total_cnt,
    const ObDagListIndex list_index,
    compaction::ObTabletCompactionProgress *progress,
    ObIArray<compaction::ObTabletCompactionProgress *> &progress_array)
{
  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(progress)) {
    ObMutexGuard guard(prio_lock_);
    ObIDag *head = dag_list_[list_index].get_header();
    ObIDag *cur = head->get_next();
    int64_t prio_cnt = 0;
    while (head != cur && idx < total_cnt && prio_cnt < MAX_SHOW_DAG_CNT) {
      if (OB_UNLIKELY(OB_TMP_FAIL(cur->gene_compaction_info(progress[idx])))) {
        if (OB_EAGAIN != tmp_ret) {
          COMMON_LOG_RET(WARN, tmp_ret, "failed to generate compaction dag info", KPC(cur));
        }
      } else {
        (void)progress_array.push_back(&progress[idx++]);
        ++prio_cnt;
      }
      cur = cur->get_next();
    }
  }
}

int ObDagPrioScheduler::check_ls_compaction_dag_exist_with_cancel(
    const ObLSID &ls_id,
    bool &exist)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  exist = false;
  ObDagListIndex loop_list[2] = { READY_DAG_LIST, RANK_DAG_LIST };
  ObIDag *cancel_dag = nullptr;
  ObIDagNet *tmp_dag_net = nullptr;
  bool cancel_flag = false;
  int64_t cancel_dag_cnt = 0;

  ObMutexGuard guard(prio_lock_);
  for (int64_t i = 0; i < 2; ++i) {
    ObDagListIndex list_idx = loop_list[i];
    ObIDag *head = dag_list_[list_idx].get_header();
    ObIDag *cur = head->get_next();
    while (head != cur) {
      cancel_flag = ObDagType::DAG_TYPE_BATCH_FREEZE_TABLETS == cur->get_type()
                  ? (ls_id == static_cast<compaction::ObBatchFreezeTabletsDag *>(cur)->get_param().ls_id_)
                  : (ls_id == static_cast<compaction::ObTabletMergeDag *>(cur)->get_ls_id());

      if (cancel_flag) {
        if (cur->get_dag_status() == ObIDag::DAG_STATUS_READY) {
          cancel_dag = cur;
          cur = cur->get_next();
          tmp_dag_net = cancel_dag->get_dag_net();
          if (OB_NOT_NULL(tmp_dag_net) && !tmp_dag_net->is_co_dag_net())  {
            tmp_ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(WARN, "compaction dag can only in co merge dag net", KR(tmp_ret), KPC(cancel_dag), KPC(tmp_dag_net));
          } else if (OB_TMP_FAIL(finish_dag_(ObIDag::DAG_STATUS_ABORT, *cancel_dag, false/*try_move_child*/))) {
            COMMON_LOG(WARN, "failed to erase dag", K(tmp_ret), KPC(cancel_dag));
            ob_abort();
          } else {
            ++cancel_dag_cnt;
          }
        } else { // for running dag
          exist = true;
          cur->set_force_cancel_flag(); // dag exists before finding force_cancel_flag, need check exists again
          cur = cur->get_next();
        }
      } else {
        cur = cur->get_next();
      }
    }
  }
  if (OB_SUCC(ret)) {
    COMMON_LOG(INFO, "cancel dag when check ls compaction dag exist", KR(ret), K(cancel_dag_cnt), K(exist));
  }
  return OB_SUCCESS;
}

int ObDagPrioScheduler::get_compaction_dag_count(int64_t dag_count)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(prio_lock_);
  for (int64_t i = 0; i < ObDagListIndex::DAG_LIST_MAX; ++i) {
    dag_count += dag_list_[i].get_size();
  }
  return ret;
}

int ObDagPrioScheduler::get_max_major_finish_time(
    const int64_t version, int64_t &estimated_finish_time)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(version));
  } else {
    compaction::ObTabletMergeDag *dag = nullptr;
    compaction::ObCOMergeBatchExeDag *co_dag = nullptr;
    estimated_finish_time = 0;
    ObMutexGuard guard(prio_lock_);
    ObIDag *head = dag_list_[READY_DAG_LIST].get_header();
    ObIDag *cur = head->get_next();
    while (head != cur) {
      if (ObIDag::DAG_STATUS_NODE_RUNNING == cur->get_dag_status()) {
        compaction::ObPartitionMergeProgress *progress = nullptr;
        if (ObDagType::DAG_TYPE_MAJOR_MERGE == cur->get_type()) {
          dag = static_cast<compaction::ObTabletMergeDag *>(cur);
          if (OB_ISNULL(dag->get_ctx())) {
          } else if (dag->get_ctx()->get_merge_version() == version) {
            progress = dag->get_ctx()->info_collector_.merge_progress_;
          }
        } else if (ObDagType::DAG_TYPE_CO_MERGE_BATCH_EXECUTE == cur->get_type()) {
          co_dag = static_cast<compaction::ObCOMergeBatchExeDag *>(cur);
          if (OB_NOT_NULL(co_dag->get_merge_progress()) && co_dag->get_param().merge_version_ == version) {
            progress = co_dag->get_merge_progress();
          }
        }
        if (OB_NOT_NULL(progress) && progress->get_estimated_finish_time() > estimated_finish_time) {
          estimated_finish_time = estimated_finish_time;
        }
      } else {
        break;
      }
      cur = cur->get_next();
    }
  }
  return ret;
}

int ObDagPrioScheduler::diagnose_dag(
    const ObIDag &dag,
    compaction::ObDiagnoseTabletCompProgress &progress)
{
  int ret = OB_SUCCESS;
  // need dag for hash
  if (OB_UNLIKELY(dag.get_priority() != priority_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpect priority", K(ret), K(dag.get_priority()), K_(priority));
  } else {
    ObMutexGuard guard(prio_lock_);
    ObIDag *stored_dag = nullptr;
    if (OB_FAIL(dag_map_.get_refactored(&dag, stored_dag))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get from dag map", K(ret));
      }
    } else if (OB_ISNULL(stored_dag)) {
      ret = OB_ERR_SYS;
      LOG_WARN("dag is null", K(ret));
    } else if (stored_dag->get_priority() != dag.get_priority()) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "unexpected priority value", K(ret), K(stored_dag->get_priority()), K(dag.get_priority()));
    } else if (OB_FAIL(stored_dag->diagnose_compaction_info(progress))) {
      LOG_WARN("failed to generate compaction info", K(ret));
    }
  }
  return ret;
}

int ObDagPrioScheduler::diagnose_minor_exe_dag(
    const compaction::ObMergeDagHash &merge_dag_info,
    compaction::ObDiagnoseTabletCompProgress &progress)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(prio_lock_);
  ObIDag *head = dag_list_[READY_DAG_LIST].get_header();
  ObIDag *cur = head->get_next();
  while (head != cur && OB_SUCC(ret)) {
    if (cur->get_type() == ObDagType::DAG_TYPE_MERGE_EXECUTE) {
      compaction::ObTabletMergeExecuteDag *exe_dag = static_cast<compaction::ObTabletMergeExecuteDag *>(cur);
      if (exe_dag->belong_to_same_tablet(&merge_dag_info)) {
        if (OB_FAIL(exe_dag->diagnose_compaction_info(progress))) {
          LOG_WARN("failed to diagnose compaction dag", K(ret), K(exe_dag));
        } else {
          break;
        }
      }
    }
    cur = cur->get_next();
  } // end of while
  return ret;
}

int ObDagPrioScheduler::diagnose_all_dags()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
#ifdef ERRSIM
  const int64_t task_may_hang_interval = 30 * 1000L * 1000L; // 30s
#else
  const int64_t task_may_hang_interval = TASK_MAY_HANG_INTERVAL;
#endif
  ObMutexGuard guard(prio_lock_);
  for (int64_t j = 0; j < DAG_LIST_MAX; ++j) {
    DagList &dl = dag_list_[j];
    DLIST_FOREACH_NORET(dag, dl) {
      if (ObIDag::DAG_STATUS_NODE_RUNNING == dag->get_dag_status()) {
        if (common::ObClockGenerator::getClock() - dag->get_start_time() > task_may_hang_interval) {
          compaction::ObTabletMergeDag *merge_dag = nullptr;
          // for diagnose // add suspect abormal dag's tablet
          if (is_compaction_dag(dag->get_type())) {
            if (OB_ISNULL(merge_dag = static_cast<compaction::ObTabletMergeDag *>(dag))) {
              tmp_ret = OB_ERR_UNEXPECTED;
              COMMON_LOG(WARN, "get unexpected null stored dag", K(tmp_ret));
            } else if (OB_TMP_FAIL(MTL(compaction::ObDiagnoseTabletMgr *)->add_diagnose_tablet(
                  merge_dag->ls_id_, merge_dag->tablet_id_, ObIDag::get_diagnose_tablet_type(merge_dag->get_type())))) {
              COMMON_LOG(WARN, "failed to add diagnose tablet", K(tmp_ret),
                  "ls_id", merge_dag->ls_id_, "tablet_id", merge_dag->tablet_id_);
            } else {
              COMMON_LOG(TRACE, "dag maybe abormal", KPC(dag));
            }
          }
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObDagPrioScheduler::get_complement_data_dag_progress(const ObIDag &dag,
    int64_t &row_scanned,
    int64_t &row_inserted)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(prio_lock_);
  ObIDag *stored_dag = nullptr;
  if (dag.get_type() != ObDagType::DAG_TYPE_DDL) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arugment", K(ret), K(dag));
  } else if (OB_FAIL(dag_map_.get_refactored(&dag, stored_dag))) {
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
  return ret;
}

int ObDagPrioScheduler::deal_with_finish_task(
    ObITask &task,
    ObTenantDagWorker &worker,
    int error_code)
{
  int ret = OB_SUCCESS;
  ObIDag *dag = nullptr;

  if (OB_ISNULL(dag = task.get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_UNLIKELY(!dag->is_valid_type() || dag->get_priority() != priority_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "invalid dag", K(ret), K(*dag), K_(priority));
  } else if (OB_UNLIKELY(OB_ISNULL(scheduler_))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpected null scheduler", K(ret), KP_(scheduler));
  } else {
    ObMutexGuard guard(prio_lock_);
    ObDagType::ObDagTypeEnum dag_type = dag->get_type();
    if (OB_SUCCESS != error_code
      && ObIDag::DAG_STATUS_NODE_FAILED != dag->get_dag_status()) {
      // set errno on first failure
      dag->set_dag_status(ObIDag::DAG_STATUS_NODE_FAILED);
      dag->set_dag_ret(error_code);
    }

    bool retry_flag = false;
    if (dag->is_dag_failed()) {
      // dag can retry & this task is the last running task
      if (OB_ALLOCATE_MEMORY_FAILED == error_code && is_mini_compaction_dag(dag->get_type())) {
        if (static_cast<compaction::ObTabletMergeDag *>(dag)->is_reserve_mode() &&
            MTL(ObTenantCompactionMemPool *)->is_emergency_mode() && !MTL_IS_MINI_MODE()) {
          COMMON_LOG(ERROR, "reserve mode dag failed to alloc mem unexpectly", KPC(dag)); // tmp debug log for reserve mode, remove later.
        }
        MTL(ObTenantCompactionMemPool *)->set_memory_mode(ObTenantCompactionMemPool::EMERGENCY_MODE);
      }
      if (OB_FAIL(deal_with_fail_dag_(*dag, retry_flag))) {
        COMMON_LOG(WARN, "failed to deal with fail dag", K(ret), KPC(dag));
      }
    }

    bool finish_task_flag = true;
    if (OB_FAIL(ret)) {
      dag->set_dag_status(ObIDag::DAG_STATUS_NODE_FAILED);
      dag->set_dag_ret(ret);
      finish_task_flag = false;
      COMMON_LOG(WARN, "failed to deal with finish task in retry process", K(ret), KPC(dag));
      ret = OB_SUCCESS;
    } else if (retry_flag) {
      finish_task_flag = false;
    }

    if (finish_task_flag && OB_FAIL(finish_task_in_dag_(task, *dag))) {
      COMMON_LOG(WARN, "failed to finish task", K(ret), KPC(dag));
    }
    scheduler_->sub_running_dag_cnts(dag_type);
  }

  if (OB_SUCC(ret)) {
    scheduler_->sub_total_running_task_cnt();
    ObMutexGuard guard(prio_lock_);
    --running_task_cnts_;
    running_workers_.remove(&worker);
  }
  return ret;
}

int ObDagPrioScheduler::cancel_dag(const ObIDag &dag, const bool force_cancel)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  bool free_flag = false;
  ObIDag *cur_dag = nullptr;
  {
    ObMutexGuard guard(prio_lock_);
    if (OB_SUCCESS != (hash_ret = dag_map_.get_refactored(&dag, cur_dag))) {
      if (OB_HASH_NOT_EXIST != hash_ret) {
        ret = hash_ret;
        LOG_WARN("failed to get from dag map", K(ret));
      } else {
        LOG_INFO("dag is not in dag_map", K(ret));
      }
    } else if (OB_ISNULL(cur_dag)) {
      ret = OB_ERR_SYS;
      LOG_WARN("dag should not be null", K(ret));
    } else if (cur_dag->get_priority() != dag.get_priority()) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "unexpected priority value", K(ret), K(cur_dag->get_priority()), K(dag.get_priority()));
    } else if (cur_dag->get_dag_status() == ObIDag::DAG_STATUS_READY) {
      LOG_INFO("cancel dag", K(ret), KP(cur_dag));
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(finish_dag_(ObIDag::DAG_STATUS_ABORT, *cur_dag, true/*try_move_child*/))) {
        COMMON_LOG(WARN, "failed to erase dag and dag net", K(ret), KPC(cur_dag));
        ob_abort();
      }
    } else if (force_cancel && cur_dag->get_dag_status() == ObIDag::DAG_STATUS_NODE_RUNNING) {
      LOG_INFO("cancel running dag", K(ret), KP(cur_dag), K(force_cancel));
      cur_dag->set_force_cancel_flag();
    }
  }
  return ret;
}

int ObDagPrioScheduler::check_dag_exist(const ObIDag &dag, bool &exist)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  exist = true;
  ObMutexGuard guard(prio_lock_);
  ObIDag *stored_dag = nullptr;
  if (OB_SUCCESS != (hash_ret = dag_map_.get_refactored(&dag, stored_dag))) {
    if (OB_HASH_NOT_EXIST == hash_ret) {
      exist = false;
    } else {
      ret = hash_ret;
      COMMON_LOG(WARN, "failed to get from dag map", K(ret));
    }
  } else if (OB_ISNULL(stored_dag)) {
    ret = OB_ERR_SYS;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (stored_dag->get_priority() != dag.get_priority()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpected priority value", K(ret), K(stored_dag->get_priority()), K(dag.get_priority()));
  }
  return ret;
}

int64_t ObDagPrioScheduler::get_limit()
{
  ObMutexGuard guard(prio_lock_);
  return limits_;
}

int64_t ObDagPrioScheduler::get_adaptive_limit()
{
  ObMutexGuard guard(prio_lock_);
  return adaptive_task_limit_;
}

int64_t ObDagPrioScheduler::get_running_task_cnt()
{
  ObMutexGuard guard(prio_lock_);
  return running_task_cnts_;
}

int ObDagPrioScheduler::set_thread_score(const int64_t score, int64_t &old_val, int64_t &new_val)
{
  int ret = OB_SUCCESS;
  if (score < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K_(priority), K(score));
  } else {
    ObMutexGuard guard(prio_lock_);
    old_val = limits_;
    limits_ = 0 == score ? OB_DAG_PRIOS[priority_].score_ : score;
    new_val = limits_;
    adaptive_task_limit_ = limits_; // reset adaptive task limit
  }
  return ret;
}

bool ObDagPrioScheduler::try_switch(ObTenantDagWorker &worker)
{
  bool need_pause = false;
  int tmp_ret = OB_SUCCESS;

  {
    ObMutexGuard guard(prio_lock_);
    if (running_task_cnts_ > adaptive_task_limit_) {
      need_pause = true;
    } else if (is_rank_dag_prio() && check_need_load_shedding_(false /*for_schedule*/)) {
      need_pause = true;
      FLOG_INFO("[ADAPTIVE_SCHED]tenant cpu is at high level, pause current compaction task", K(priority_));
    }

    if (need_pause) {
      pause_worker_(worker);
    } else if (!waiting_workers_.is_empty()) {
      if (waiting_workers_.get_first()->need_wake_up()) {
        // schedule_one will schedule the first worker on the waiting list first
        if (OB_TMP_FAIL(schedule_one_())) {
          if (OB_ENTRY_NOT_EXIST != tmp_ret) {
            COMMON_LOG_RET(WARN, tmp_ret, "failed to schedule one task", K_(priority));
          }
        }
        ObCurTraceId::set(worker.get_task()->get_dag()->get_dag_id());
        if (OB_SUCCESS == tmp_ret) {
          need_pause = true;
          pause_worker_(worker);
        }
      }
    }
  }

  return need_pause;
}

// under prio lock
bool ObDagPrioScheduler::check_need_load_shedding_(const bool for_schedule)
{
  bool need_shedding = false;
  compaction::ObTenantTabletScheduler *tablet_scheduler = nullptr;

  if (OB_ISNULL(tablet_scheduler = MTL(compaction::ObTenantTabletScheduler *))) {
    // may be during the start phase
  } else if (tablet_scheduler->enable_adaptive_merge_schedule()) {
    ObTenantTabletStatMgr *stat_mgr = MTL(ObTenantTabletStatMgr *);
    int64_t load_shedding_factor = 1;
    const int64_t extra_limit = for_schedule ? 0 : 1;

    if (OB_ISNULL(stat_mgr)) {
    } else if (FALSE_IT(load_shedding_factor = MAX(1, stat_mgr->get_load_shedding_factor()))) {
    } else if (load_shedding_factor <= 1 || !is_rank_dag_prio()) {
      // no need to load shedding
    } else {
      const int64_t load_shedding_limit = MAX(2, adaptive_task_limit_ / load_shedding_factor);
      if (running_task_cnts_ > load_shedding_limit + extra_limit) {
        need_shedding = true;
        if (REACH_TENANT_TIME_INTERVAL(30_s)) {
          FLOG_INFO("[ADAPTIVE_SCHED] DagScheduler needs to load shedding", K(load_shedding_factor), K(for_schedule),
              K(extra_limit), K_(adaptive_task_limit), K_(running_task_cnts), K_(priority));
        }
      }
    }
  }
  return need_shedding;
}


/***************************************ObDagNetScheduler impl********************************************/
void ObDagNetScheduler::destroy()
{
  for (int i = 0; i < DAG_NET_LIST_MAX; i++ ) {
    dag_net_list_[i].reset();
  }

  const ObIDagNet *cur_dag_net = nullptr;
  ObIAllocator *allocator = nullptr;
  DagNetMap::iterator iter = dag_net_map_.begin();
  for (; iter != dag_net_map_.end(); ++iter) { // ignore failure
    cur_dag_net = iter->second;
    allocator = nullptr;
    if (OB_NOT_NULL(cur_dag_net)) {
      allocator = cur_dag_net->is_ha_dag_net() ? ha_allocator_ : allocator_;
      cur_dag_net->~ObIDagNet();
      if (OB_NOT_NULL(allocator_)) {
        allocator_->free((void*)cur_dag_net);
      }
    }
  }

  if (dag_net_map_.created()) {
    dag_net_map_.destroy();
  }
  if (dag_net_id_map_.created()) {
    dag_net_id_map_.destroy();
  }

  MEMSET(dag_net_cnts_, 0, sizeof(dag_net_cnts_));
}

int ObDagNetScheduler::init(
    const uint64_t tenant_id,
    const int64_t dag_limit,
    ObIAllocator &allocator,
    ObIAllocator &ha_allocator,
    ObTenantDagScheduler &scheduler)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || 0 >= dag_limit) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "init ObDagNetScheduler with invalid arguments", K(ret), K(tenant_id), K(dag_limit));
  } else if (OB_FAIL(dag_net_map_.create(dag_limit, "DagNetMap", "DagNetNode", tenant_id))) {
    COMMON_LOG(WARN, "failed to create running dap net map", K(ret), K(dag_limit));
  } else if (OB_FAIL(dag_net_id_map_.create(dag_limit, "DagNetIdMap", "DagNetIdNode", tenant_id))) {
    COMMON_LOG(WARN, "failed to create dap net id map", K(ret), K(dag_limit));
  } else {
    allocator_ = &allocator;
    ha_allocator_ = &ha_allocator;
    scheduler_ = &scheduler;
    MEMSET(dag_net_cnts_, 0, sizeof(dag_net_cnts_));
  }
  return ret;
}

void ObDagNetScheduler::erase_dag_net_or_abort(ObIDagNet &dag_net)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dag_net_map_.erase_refactored(&dag_net))) {
    COMMON_LOG(ERROR, "failed to erase dag net from dag_net_map_", K(ret), K(dag_net));
    ob_abort();
  }
}

void ObDagNetScheduler::erase_dag_net_id_or_abort(ObIDagNet &dag_net)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dag_net_id_map_.erase_refactored(dag_net.get_dag_id()))) {
    COMMON_LOG(ERROR, "failed to erase dag_net_id from dag_net_id_map_", K(ret), K(dag_net));
    ob_abort();
  }
  (void) erase_dag_net_or_abort(dag_net);
}

void ObDagNetScheduler::erase_dag_net_list_or_abort(const ObDagNetListIndex &dag_net_list_index, ObIDagNet *dag_net)
{
  if (!is_valid_dag_net_list(dag_net_list_index) || !dag_net_list_[dag_net_list_index].remove(dag_net)) {
    COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "failed to remove dag_net from",
        "list", dag_net_list_to_str(dag_net_list_index), K(dag_net_list_index), K(dag_net));
    ob_abort();
  }
}

void ObDagNetScheduler::add_dag_net_list_or_abort(const ObDagNetListIndex &dag_net_list_index, ObIDagNet *dag_net)
{
  if (!is_valid_dag_net_list(dag_net_list_index) || !dag_net_list_[dag_net_list_index].add_last(dag_net)) {
    COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "failed to add dag_net into",
        "list", dag_net_list_to_str(dag_net_list_index), K(dag_net_list_index), K(dag_net));
    ob_abort();
  }
}

bool ObDagNetScheduler::is_empty() {
  bool bret = true;
  ObMutexGuard guard(dag_net_map_lock_);
  if (!dag_net_list_[BLOCKING_DAG_NET_LIST].is_empty()) {
    bret = false;
  } else if (!dag_net_list_[RUNNING_DAG_NET_LIST].is_empty()) {
    ObIDagNet *head = dag_net_list_[RUNNING_DAG_NET_LIST].get_header();
    ObIDagNet *cur = head->get_next();
    while (NULL != cur && head != cur && bret) {
      if (!cur->check_finished_and_mark_stop()) {
        bret = false;
      } else {
        cur = cur->get_next();
      }
    }
  }
  return bret;
}

int ObDagNetScheduler::add_dag_net(ObIDagNet &dag_net)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!dag_net.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(dag_net));
  } else {
    ObMutexGuard guard(dag_net_map_lock_);
    if (OB_FAIL(dag_net_map_.set_refactored(&dag_net, &dag_net))) {
      if (OB_HASH_EXIST != ret) {
        COMMON_LOG(WARN, "failed to set running_dag_net_map", K(ret), K(dag_net));
      }
    } else if (OB_FAIL(dag_net_id_map_.set_refactored(dag_net.get_dag_id(), &dag_net))) {
      COMMON_LOG(WARN, "failed to add dag net id into map", K(ret), K(dag_net));
      int tmp_ret = OB_SUCCESS;
      if (OB_HASH_EXIST == ret) {
        const ObIDagNet *exist_dag_net = nullptr;
        if (OB_TMP_FAIL(dag_net_id_map_.get_refactored(dag_net.get_dag_id(), exist_dag_net))) {
          COMMON_LOG(WARN, "failed to get dag net from dag net id map", K(tmp_ret), K(dag_net));
        } else {
          COMMON_LOG(WARN, "exist dag net is", K(dag_net), KPC(exist_dag_net));
        }
      }
      (void) erase_dag_net_or_abort(dag_net);
    } else if (!dag_net_list_[BLOCKING_DAG_NET_LIST].add_last(&dag_net)) {// add into blocking_dag_net_list
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "failed to add into blocking_dag_net_list", K(ret), K(dag_net));
      (void) erase_dag_net_id_or_abort(dag_net);
    } else {
      ++dag_net_cnts_[dag_net.get_type()];
      dag_net.set_add_time();
      COMMON_LOG(INFO, "add dag net success", K(dag_net), "add_time", dag_net.get_add_time(),
          "dag_net_type_cnts", dag_net_cnts_[dag_net.get_type()]);
    }
  }
  return ret;
}

void ObDagNetScheduler::finish_dag_net_without_lock(ObIDagNet &dag_net)
{
  (void) erase_dag_net_id_or_abort(dag_net);
  --dag_net_cnts_[dag_net.get_type()];
}

void ObDagNetScheduler::finish_dag_net(ObIDagNet &dag_net)
{
  ObMutexGuard guard(dag_net_map_lock_);
  (void) finish_dag_net_without_lock(dag_net);
}

void ObDagNetScheduler::dump_dag_status()
{
  int64_t running_dag_net_map_size = 0;
  int64_t blocking_dag_net_list_size = 0;
  int64_t running_dag_net_list_size = 0;

  int64_t dag_net_count[ObDagNetType::DAG_NET_TYPE_MAX];
  {
    ObMutexGuard guard(dag_net_map_lock_);
    for (int64_t i = 0; i < ObDagNetType::DAG_NET_TYPE_MAX; ++i) {
      dag_net_count[i] = dag_net_cnts_[i];
    }
    running_dag_net_map_size = dag_net_map_.size();
    blocking_dag_net_list_size = dag_net_list_[BLOCKING_DAG_NET_LIST].get_size();
    running_dag_net_list_size = dag_net_list_[RUNNING_DAG_NET_LIST].get_size();
  }
  COMMON_LOG(INFO, "dump_dag_status[DAG_NET]", K(running_dag_net_map_size), K(blocking_dag_net_list_size), K(running_dag_net_list_size));
  for (int64_t i = 0; i < ObDagNetType::DAG_NET_TYPE_MAX; ++i) {
    COMMON_LOG(INFO, "dump_dag_status[DAG_NET]", "type", OB_DAG_NET_TYPES[i].dag_net_type_str_,
        "dag_count", dag_net_count[i]);
  }
}

int64_t ObDagNetScheduler::get_dag_net_count()
{
  ObMutexGuard guard(dag_net_map_lock_);
  return dag_net_map_.size();
}

void ObDagNetScheduler::get_all_dag_scheduler_info(
    ObDagSchedulerInfo *info_list,
    common::ObIArray<void *> &scheduler_infos,
    int64_t &idx)
{
  if (OB_NOT_NULL(info_list)) {
    ObMutexGuard guard(dag_net_map_lock_);
    for (int64_t i = 0; i < ObDagNetType::DAG_NET_TYPE_MAX; ++i) {
      ADD_DAG_SCHEDULER_INFO(ObDagSchedulerInfo::DAG_NET_COUNT, OB_DAG_NET_TYPES[i].dag_net_type_str_, dag_net_cnts_[i]);
    }
  }
}

void ObDagNetScheduler::get_all_dag_info(
    ObDagInfo *info_list,
    common::ObIArray<void *> &dag_infos,
    int64_t &idx, const int64_t total_cnt)
{
  if (OB_NOT_NULL(info_list)) {
    ObMutexGuard guard(dag_net_map_lock_);

    ObIDagNet *head = dag_net_list_[BLOCKING_DAG_NET_LIST].get_header();
    ObIDagNet *cur = head->get_next();
    while (NULL != cur && head != cur && idx < total_cnt) {
      ADD_DAG_INFO(cur, "BLOCKING_DAG_NET_LIST");
      cur = cur->get_next();
    }

    head = dag_net_list_[RUNNING_DAG_NET_LIST].get_header();
    cur = head->get_next();
    while (NULL != cur && head != cur && idx < total_cnt) {
      ADD_DAG_INFO(cur, "RUNNING_DAG_NET_LIST");
      cur = cur->get_next();
    }

  }
}

int ObDagNetScheduler::diagnose_dag_net(
    ObIDagNet &dag_net,
    common::ObIArray<compaction::ObDiagnoseTabletCompProgress> &progress_list,
    ObDagId &dag_net_id,
    int64_t &start_time)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(dag_net_map_lock_);
  ObIDagNet *stored_dag_net = nullptr;
  if (OB_FAIL(dag_net_map_.get_refactored(&dag_net, stored_dag_net))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get from dag map", K(ret));
    }
  } else if (OB_ISNULL(stored_dag_net)) {
    ret = OB_ERR_SYS;
    LOG_WARN("dag is null", K(ret));
  } else {
    stored_dag_net->diagnose_dag(progress_list);
    start_time = stored_dag_net->get_start_time();
    dag_net_id = stored_dag_net->get_dag_id();
  }
  return ret;
}

int64_t ObDagNetScheduler::get_dag_net_count(const ObDagNetType::ObDagNetTypeEnum type)
{
  int64_t count = -1;
  if (type >= 0 && type < ObDagNetType::DAG_NET_TYPE_MAX) {
    ObMutexGuard guard(dag_net_map_lock_);
    count = dag_net_cnts_[type];
  } else {
    COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid type", K(type));
  }
  return count;
}

bool ObDagNetScheduler::is_dag_map_full()
{
  bool bret = false;
  if (OB_UNLIKELY(OB_ISNULL(scheduler_))) {
    COMMON_LOG_RET(WARN, OB_ERR_UNEXPECTED, "scheduler is null", KP(scheduler_));
  } else {
    if (((double)scheduler_->get_cur_dag_cnt() / DEFAULT_MAX_DAG_MAP_CNT) >= ((double)STOP_ADD_DAG_PERCENT / 100)) {
      bret = true;
      if (REACH_TENANT_TIME_INTERVAL(LOOP_PRINT_LOG_INTERVAL))  {
        ObMutexGuard guard(dag_net_map_lock_);
        COMMON_LOG(INFO, "dag map is almost full, stop loop blocking dag_net_map",
            "dag_map_size", scheduler_->get_cur_dag_cnt(), "dag_net_map_size", dag_net_map_.size(),
            "blocking_dag_net_list_size", dag_net_list_[BLOCKING_DAG_NET_LIST].get_size(),
            "running_dag_net_list_size", dag_net_list_[RUNNING_DAG_NET_LIST].get_size());
      }
    }
  }

  return bret;
}

int ObDagNetScheduler::loop_running_dag_net_list()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t slow_dag_net_cnt = 0;

  ObMutexGuard guard(dag_net_map_lock_);
  ObIDagNet *head = dag_net_list_[RUNNING_DAG_NET_LIST].get_header();
  ObIDagNet *cur = head->get_next();
  ObIDagNet *dag_net = nullptr;

  while (NULL != cur && head != cur) { // ignore failure
    LOG_DEBUG("loop running dag net list", K(ret), KPC(cur));
    dag_net = cur;
    cur = cur->get_next();
    if (dag_net->is_started() && OB_TMP_FAIL(dag_net->schedule_rest_dag())) {
      LOG_WARN("failed to schedule rest dag", K(tmp_ret));
    } else if (dag_net->check_finished_and_mark_stop()) {
      LOG_INFO("dag net is in finish state, move to finished list", K(ret), KPC(dag_net));
      (void) erase_dag_net_list_or_abort(RUNNING_DAG_NET_LIST, dag_net);
      (void) add_dag_net_list_or_abort(FINISHED_DAG_NET_LIST, dag_net);
    } else if (dag_net->is_co_dag_net()
        && dag_net->get_start_time() + SLOW_COMPACTION_DAG_NET_THREASHOLD < ObTimeUtility::fast_current_time()) {
      ++slow_dag_net_cnt;
      compaction::ObCOMergeDagNet *co_dag_net = static_cast<compaction::ObCOMergeDagNet*>(dag_net);
      if (nullptr != co_dag_net && OB_TMP_FAIL(MTL(compaction::ObDiagnoseTabletMgr *)->add_diagnose_tablet(
          co_dag_net->ls_id_, co_dag_net->tablet_id_, ObDiagnoseTabletType::TYPE_MEDIUM_MERGE))) {
        COMMON_LOG(WARN, "failed to add diagnose tablet", K(tmp_ret),
            "ls_id", co_dag_net->ls_id_, "tablet_id", co_dag_net->tablet_id_);
      }
    } else if (dag_net->get_start_time() + PRINT_SLOW_DAG_NET_THREASHOLD < ObTimeUtility::fast_current_time()) {
      ++slow_dag_net_cnt;
      if (REACH_TENANT_TIME_INTERVAL(LOOP_PRINT_LOG_INTERVAL)) {
        LOG_INFO("slow dag net", K(ret), KP(dag_net), "dag_net_start_time", dag_net->get_start_time());
      }
    }
  }
  COMMON_LOG(INFO, "loop running dag net list", K(ret),
      "running_dag_net_list_size", dag_net_list_[RUNNING_DAG_NET_LIST].get_size(), K(slow_dag_net_cnt),
      "finished_dag_net_list_size", dag_net_list_[FINISHED_DAG_NET_LIST].get_size());

  return ret;
}

int ObDagNetScheduler::loop_finished_dag_net_list()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scheduler_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "scheduler is null", KP(scheduler_));
  } else {
    ObIDagNet *head = dag_net_list_[FINISHED_DAG_NET_LIST].get_header();
    ObIDagNet *cur = head->get_next();
    ObIDagNet *dag_net = nullptr;
    while (NULL != cur && head != cur) {
      LOG_DEBUG("loop blocking dag net list", K(ret), KPC(cur));
      dag_net = cur;
      cur = cur->get_next();
      (void) erase_dag_net_list_or_abort(FINISHED_DAG_NET_LIST, dag_net);
      (void) scheduler_->finish_dag_net(dag_net);
    }
  }
  COMMON_LOG(INFO, "loop finsihed dag net list", K(ret), "finished_dag_net_list_size", dag_net_list_[FINISHED_DAG_NET_LIST].get_size());
  return ret;
}

int ObDagNetScheduler::loop_blocking_dag_net_list()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scheduler_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "scheduler is null", KP(scheduler_));
  } else {
    ObMutexGuard guard(dag_net_map_lock_);
    ObIDagNet *head = dag_net_list_[BLOCKING_DAG_NET_LIST].get_header();
    ObIDagNet *cur = head->get_next();
    ObIDagNet *tmp = nullptr;
    int64_t rest_cnt = DEFAULT_MAX_RUNNING_DAG_NET_CNT - (dag_net_map_.size() - dag_net_list_[BLOCKING_DAG_NET_LIST].get_size());
    while (NULL != cur && head != cur && rest_cnt > 0 && !is_dag_map_full()) {
      LOG_DEBUG("loop blocking dag net list", K(ret), KPC(cur), K(rest_cnt));
      tmp = cur;
      cur = cur->get_next();
      if (tmp->is_cancel() || OB_FAIL(tmp->start_running())) { // call start_running function
        if (OB_FAIL(ret)) {
          COMMON_LOG(WARN, "failed to start running or be canceled", K(ret), KPC(cur));
        }
        (void) finish_dag_net_without_lock(*tmp);
        (void) erase_dag_net_list_or_abort(BLOCKING_DAG_NET_LIST, tmp);
        (void) scheduler_->free_dag_net(tmp); // set tmp nullptr
      } else {
        tmp->set_start_time();
        --rest_cnt;
        (void) erase_dag_net_list_or_abort(BLOCKING_DAG_NET_LIST, tmp);
        (void) add_dag_net_list_or_abort(RUNNING_DAG_NET_LIST, tmp);
      }
    }
  }
  return ret;
}

int ObDagNetScheduler::check_dag_net_exist(
    const ObDagId &dag_id, bool &exist)
{
  int ret = OB_SUCCESS;
  const ObIDagNet *dag_net = nullptr;
  ObMutexGuard guard(dag_net_map_lock_);

  if (OB_FAIL(dag_net_id_map_.get_refactored(dag_id, dag_net))) {
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

int ObDagNetScheduler::cancel_dag_net(const ObDagId &dag_id)
{
  int ret = OB_SUCCESS;
  const ObIDagNet *dag_net_key = nullptr;
  ObIDagNet *dag_net = nullptr;
  if (dag_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cancel dag net get invalid argument", K(ret), K(dag_id));
  } else {
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
    } else if (OB_FAIL(dag_net_map_.get_refactored(dag_net_key, dag_net))) {
      LOG_WARN("failed to get dag net", K(ret), KPC(dag_net_key));
    } else {
      dag_net->set_cancel();
      if (OB_FAIL(dag_net->deal_with_cancel())) {
        LOG_WARN("failed to deal with cancel", K(ret), KPC(dag_net));
      }
    }
  }
  return ret;
}

// for unittest
int ObDagNetScheduler::get_first_dag_net(ObIDagNet *&dag_net)
{
  int ret = OB_SUCCESS;
  dag_net = nullptr;
  ObMutexGuard guard(dag_net_map_lock_);
  DagNetMap::iterator iter = dag_net_map_.begin();
  if (iter != dag_net_map_.end()) {
    dag_net = iter->second;
  }
  return ret;
}

int ObDagNetScheduler::check_ls_compaction_dag_exist_with_cancel(const ObLSID &ls_id, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  ObMutexGuard dag_net_guard(dag_net_map_lock_);
  int64_t cancel_dag_cnt = 0;
  ObIDagNet *head = nullptr;
  ObIDagNet *cur = nullptr;
  ObIDagNet *cur_dag_net = nullptr;

  for (int i = BLOCKING_DAG_NET_LIST; i <= RUNNING_DAG_NET_LIST; i++) {
    head = dag_net_list_[i].get_header();
    cur = head->get_next();
    cur_dag_net = nullptr;

    while (NULL != cur && head != cur) {
      cur_dag_net = cur;
      cur = cur->get_next();
      if (cur_dag_net->is_co_dag_net()) {
        compaction::ObCOMergeDagNet *co_dag_net = static_cast<compaction::ObCOMergeDagNet*>(cur_dag_net);
        if (ls_id == co_dag_net->get_dag_param().ls_id_) {
          cur_dag_net->set_cancel();
          ++cancel_dag_cnt;
          exist = true;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("success to cancel dag net", KR(ret), K(ls_id), K(cancel_dag_cnt), K(exist));
  }
  return ret;
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
    fast_schedule_dag_net_(false),
    tg_id_(-1),
    dag_cnt_(0),
    dag_limit_(0),
    check_period_(0),
    loop_waiting_dag_list_period_(0),
    total_worker_cnt_(0),
    work_thread_num_(0),
    default_work_thread_num_(0),
    total_running_task_cnt_(0),
    scheduled_task_cnt_(0),
    scheduler_sync_(),
    mem_context_(nullptr),
    ha_mem_context_(nullptr)
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
  } else if (OB_FAIL(scheduler_sync_.init(ObWaitEventIds::SCHEDULER_COND_WAIT))) {
    COMMON_LOG(WARN, "failed to init scheduler sync", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::DagScheduler, tg_id_))) {
    COMMON_LOG(WARN, "TG create dag scheduler failed", K(ret));
  } else if (OB_FAIL(init_allocator(tenant_id, ObModIds::OB_SCHEDULER, mem_context_))) {
    COMMON_LOG(WARN, "failed to init scheduler allocator", K(ret));
  } else if (OB_FAIL(init_allocator(tenant_id, "HAScheduler", ha_mem_context_))) {
    COMMON_LOG(WARN, "failed to init ha scheduler allocator", K(ret));
  } else if (OB_FAIL(dag_net_sche_.init(
      tenant_id, dag_limit, get_allocator(false/*is_ha*/), get_allocator(true/*is_ha*/), *this))) {
    COMMON_LOG(WARN, "failed to init dag net scheduler", K(ret), K(dag_limit));
  } else {
    check_period_ = check_period;
    loop_waiting_dag_list_period_ = loop_waiting_list_period;
    dag_limit_ = dag_limit;
    work_thread_num_ = default_work_thread_num_ = 0;
    MEMSET(dag_cnts_, 0, sizeof(dag_cnts_));
    MEMSET(running_dag_cnts_, 0, sizeof(running_dag_cnts_));
    MEMSET(added_dag_cnts_, 0, sizeof(added_dag_cnts_));
    MEMSET(scheduled_dag_cnts_, 0, sizeof(scheduled_dag_cnts_));
    MEMSET(scheduled_task_cnts_, 0, sizeof(scheduled_task_cnts_));
    MEMSET(scheduled_data_size_, 0, sizeof(scheduled_data_size_));
  }

  // init prio schedulers
  for (int64_t i = 0; OB_SUCC(ret) && i < ObDagPrio::DAG_PRIO_MAX; ++i) {
    if (OB_FAIL(prio_sche_[i].init(
        tenant_id, dag_limit, i, get_allocator(false/*is_ha*/), get_allocator(true/*is_ha*/), *this))) {
      COMMON_LOG(WARN, "failed to init prio_sche_", K(ret), K(dag_limit));
    } else {
      work_thread_num_ += prio_sche_[i].get_limit();
      default_work_thread_num_ += prio_sche_[i].get_limit();
    }
  }

  // create dag workers
  for (int64_t i = 0; OB_SUCC(ret) && i < work_thread_num_; ++i) {
    if (OB_FAIL(create_worker())) {
      COMMON_LOG(WARN, "failed to create worker", K(ret));
    }
  }

  if (FAILEDx(TG_SET_RUNNABLE_AND_START(tg_id_, *this))) {
    COMMON_LOG(WARN, "failed to start dag scheduler", K(ret));
  } else {
    is_inited_ = true;
    dump_dag_status();
    COMMON_LOG(INFO, "ObTenantDagScheduler is inited", K(ret), K_(work_thread_num), K_(default_work_thread_num));
  }

  if (!is_inited_) {
    reset();
    COMMON_LOG(WARN, "failed to init ObTenantDagScheduler", K(ret));
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

  while (is_inited_) {
    ObThreadCondGuard guard(scheduler_sync_);
    if (OB_SUCCESS == guard.get_ret()) {
      is_inited_ = false; // avoid alloc dag/dag_net
    } else {
      // 100ms
      ob_usleep(100 * 1000);
    }
  }
  destroy_all_workers();
  WEAK_BARRIER();
  dump_dag_status(true);

  for (int64_t i = 0; i < ObDagPrio::DAG_PRIO_MAX; ++i) {
    prio_sche_[i].destroy();
  }
  dag_net_sche_.destroy();

  // there will be 'HAS UNFREE PTR' log with label when some ptrs haven't been free
  if (NULL != mem_context_) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  if (NULL != ha_mem_context_) {
    DESTROY_CONTEXT(ha_mem_context_);
    ha_mem_context_ = nullptr;
  }
  dag_cnt_ = 0;
  dag_limit_ = 0;
  total_worker_cnt_ = 0;
  work_thread_num_ = 0;
  total_running_task_cnt_ = 0;
  scheduled_task_cnt_ = 0;
  MEMSET(dag_cnts_, 0, sizeof(dag_cnts_));
  MEMSET(running_dag_cnts_, 0, sizeof(running_dag_cnts_));
  MEMSET(added_dag_cnts_, 0, sizeof(added_dag_cnts_));
  MEMSET(scheduled_dag_cnts_, 0, sizeof(scheduled_dag_cnts_));
  MEMSET(scheduled_task_cnts_, 0, sizeof(scheduled_task_cnts_));
  MEMSET(scheduled_data_size_, 0, sizeof(scheduled_data_size_));
  TG_DESTROY(tg_id_);
  tg_id_ = -1;
  COMMON_LOG(INFO, "ObTenantDagScheduler destroyed");
}

// no need to lock before call this func
void ObTenantDagScheduler::free_dag(ObIDag &dag)
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  // erase from dag_net
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(dag_net = dag.get_dag_net())) {
    if (OB_FAIL(dag_net->erase_dag_from_dag_net(dag))) {
      LOG_WARN("failed to erase dag from dag_net", K(ret), KP(dag_net), K(dag));
      ob_abort();
    }
  }
  // clear children and parent
  dag.reset_node();
  // free
  inner_free_dag(dag);
}

void ObTenantDagScheduler::inner_free_dag(ObIDag &dag)
{
  if (OB_UNLIKELY(nullptr != dag.get_prev() || nullptr != dag.get_next())) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "dag is in dag_list", K(dag), K(dag.get_prev()), K(dag.get_next()));
  }
  ObIAllocator &allocator = get_allocator(dag.is_ha_dag());
  dag.~ObIDag();
  allocator.free(&dag);
}

int ObTenantDagScheduler::add_dag_net(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arugment", KP(dag_net));
  } else if (OB_UNLIKELY(!dag_net->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KPC(dag_net));
  } else if (dag_net->is_co_dag_net() && FALSE_IT(
      static_cast<compaction::ObCOMergeDagNet *>(dag_net)->update_merge_status(compaction::ObCOMergeDagNet::INITED))) {
  } else if (OB_FAIL(dag_net_sche_.add_dag_net(*dag_net))) {
    if (OB_HASH_EXIST != ret) {
      COMMON_LOG(WARN, "fail to add dag net", K(ret), KPC(dag_net));
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

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", KP(dag));
  } else if (OB_UNLIKELY(!dag->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KPC(dag));
  } else if (OB_FAIL(prio_sche_[dag->get_priority()].inner_add_dag(emergency, check_size_overflow, dag))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("failed to inner add dag", K(ret), KPC(dag));
    }
  } else {
    ObThreadCondGuard guard(scheduler_sync_);
    if (OB_SUCC(guard.get_ret())) {
      if(OB_FAIL(scheduler_sync_.signal())) {
        COMMON_LOG(WARN, "Failed to signal", K(ret), KPC(dag));
      }
    }
  }
  return ret;
}

void ObTenantDagScheduler::get_suggestion_reason(
    const int64_t priority,
    int64_t &reason)
{
  reason = compaction::ObCompactionSuggestionMgr::ObCompactionSuggestionReason::MAX_REASON;
  if (ObDagPrio::DAG_PRIO_COMPACTION_HIGH == priority) {
    inner_get_suggestion_reason(ObDagType::DAG_TYPE_MINI_MERGE, reason);
  } else if (ObDagPrio::DAG_PRIO_COMPACTION_MID == priority) {
    inner_get_suggestion_reason(ObDagType::DAG_TYPE_MERGE_EXECUTE, reason);
  } else if (ObDagPrio::DAG_PRIO_COMPACTION_LOW == priority) {
    inner_get_suggestion_reason(ObDagType::DAG_TYPE_MAJOR_MERGE, reason);
    if (compaction::ObCompactionSuggestionMgr::ObCompactionSuggestionReason::MAX_REASON == reason) {
      inner_get_suggestion_reason(ObDagType::DAG_TYPE_CO_MERGE_BATCH_EXECUTE, reason);
    }
  }
}

void ObTenantDagScheduler::inner_get_suggestion_reason(const ObDagType::ObDagTypeEnum type, int64_t &reason)
{
  reason = compaction::ObCompactionSuggestionMgr::ObCompactionSuggestionReason::MAX_REASON;
  if (MANY_DAG_COUNT < get_type_dag_cnt(type) && get_added_dag_cnts(type) > 2 * get_scheduled_dag_cnts(type)) {
    reason = compaction::ObCompactionSuggestionMgr::ObCompactionSuggestionReason::SCHE_SLOW;
  } else if (dag_count_overflow(type)) {
    reason = compaction::ObCompactionSuggestionMgr::ObCompactionSuggestionReason::DAG_FULL;
  }
}

void ObTenantDagScheduler::diagnose_for_suggestion()
{
  int tmp_ret = OB_SUCCESS;
  if (REACH_TENANT_TIME_INTERVAL(DUMP_DAG_STATUS_INTERVAL)) {
    ObSEArray<int64_t, ObIDag::MergeDagPrioCnt> reasons;
    ObSEArray<int64_t, ObIDag::MergeDagPrioCnt> thread_limits;
    ObSEArray<int64_t, ObIDag::MergeDagPrioCnt> running_cnts;
    ObSEArray<int64_t, compaction::ObCompactionDagStatus::COMPACTION_DAG_MAX> dag_running_cnts;
    int64_t value = 0;
    for (int64_t i = 0; i < ObIDag::MergeDagPrioCnt; ++i) {
      if (FALSE_IT(get_suggestion_reason(ObIDag::MergeDagPrio[i], value))) {
      } else if (OB_TMP_FAIL(reasons.push_back(value))) {
        LOG_WARN_RET(tmp_ret, "failed to push back reasons");
      } else if (FALSE_IT(value = prio_sche_[ObIDag::MergeDagPrio[i]].get_adaptive_limit())) {
      } else if (OB_TMP_FAIL(thread_limits.push_back(value))) {
        LOG_WARN_RET(tmp_ret, "failed to push back thread_limits");
      } else if (FALSE_IT(value = prio_sche_[ObIDag::MergeDagPrio[i]].get_running_task_cnt())) {
      } else if (OB_TMP_FAIL(running_cnts.push_back(value))) {
        LOG_WARN_RET(tmp_ret, "failed to push back running_cnts");
      }
    }
    for (int64_t i = 0; i < compaction::ObCompactionDagStatus::COMPACTION_DAG_MAX; ++i) {
      if (FALSE_IT(value = get_running_dag_cnts(i))) {
      } else if (OB_TMP_FAIL(dag_running_cnts.push_back(value))) {
        LOG_WARN_RET(tmp_ret, "failed to push back reasons");
      }
    }
    compaction::ObCompactionSuggestionMgr *suggestion_mgr = MTL(compaction::ObCompactionSuggestionMgr *);
    if (OB_NOT_NULL(suggestion_mgr)
        && OB_TMP_FAIL(suggestion_mgr->diagnose_for_suggestion(
          reasons, running_cnts, thread_limits, dag_running_cnts))) {
      LOG_WARN_RET(tmp_ret, "fail to diagnose for suggestion");
    }
  }
}

void ObTenantDagScheduler::dump_dag_status(const bool force_dump/*false*/)
{
  int tmp_ret = OB_SUCCESS;
  if (force_dump || REACH_TENANT_TIME_INTERVAL(DUMP_DAG_STATUS_INTERVAL)) {
    int64_t total_worker_cnt = 0;
    int64_t work_thread_num = 0;

    for (int64_t i = 0; i < ObDagPrio::DAG_PRIO_MAX; ++i) {
      prio_sche_[i].dump_dag_status();
    }

    for (int64_t i = 0; i < ObDagType::DAG_TYPE_MAX; ++i) {
      COMMON_LOG(INFO, "dump_dag_status", "type", OB_DAG_TYPES[i],
          "dag_count", get_type_dag_cnt(i),
          "running_dag_count", get_running_dag_cnts(i),
          "added_dag_count", get_added_dag_cnts(i),
          "scheduled_dag_count", get_scheduled_dag_cnts(i),
          "scheduled_task_count", get_scheduled_task_cnts(i),
          "scheduled_data_size", get_scheduled_data_size(i));
      clear_added_dag_cnts(i);
      clear_scheduled_dag_cnts(i);
      clear_scheduled_task_cnts(i);
      clear_scheduled_data_size(i);
    }

    dag_net_sche_.dump_dag_status();

    COMMON_LOG(INFO, "dump_dag_status",
        "dag_cnt", get_cur_dag_cnt(),
        K(total_worker_cnt_),
        "total_running_task_cnt", get_total_running_task_cnt(),
        K(work_thread_num_),
        "scheduled_task_cnt", get_scheduled_task_cnt());
    clear_scheduled_task_cnt();
  }
}

int ObTenantDagScheduler::gene_basic_info(
    ObDagSchedulerInfo *info_list,
    common::ObIArray<void *> &scheduler_infos,
    int64_t &idx)
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(scheduler_sync_);
  if (OB_SUCC(guard.get_ret()))  {
    ADD_DAG_SCHEDULER_INFO(ObDagSchedulerInfo::GENERAL, "TOTAL_WORKER_CNT", total_worker_cnt_);
    ADD_DAG_SCHEDULER_INFO(ObDagSchedulerInfo::GENERAL, "TOTAL_DAG_CNT", get_cur_dag_cnt());
    ADD_DAG_SCHEDULER_INFO(ObDagSchedulerInfo::GENERAL, "TOTAL_RUNNING_TASK_CNT", get_total_running_task_cnt());
  }
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
    if (OB_FAIL(gene_basic_info(info_list, scheduler_infos, idx))) {
      COMMON_LOG(WARN, "failed to generate basic info", K(ret));
    } else {
      for (int64_t i = 0; i < ObDagPrio::DAG_PRIO_MAX; ++i) {
        prio_sche_[i].get_all_dag_scheduler_info(info_list, scheduler_infos, idx);
      }
      for (int64_t i = 0; i < ObDagType::DAG_TYPE_MAX; ++i) {
        ADD_DAG_SCHEDULER_INFO(ObDagSchedulerInfo::DAG_COUNT, OB_DAG_TYPES[i].dag_type_str_, get_type_dag_cnt(i));
      }
      dag_net_sche_.get_all_dag_scheduler_info(info_list, scheduler_infos, idx);
    }
  }
  return ret;
}

int ObTenantDagScheduler::get_all_dag_info(
    common::ObIAllocator &allocator,
    common::ObIArray<void *> &dag_infos)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  int64_t total_dag_cnt = 0;

  for (int64_t i = 0; i < ObDagType::DAG_TYPE_MAX; ++i) {
    total_dag_cnt += get_type_dag_cnt(i);
  }
  total_dag_cnt += dag_net_sche_.get_dag_net_count();

  if (total_dag_cnt > 0) {
    // TODO(@jingshui) : show all dags
    total_dag_cnt = MIN(total_dag_cnt,
        ObDagPrioScheduler::MAX_SHOW_DAG_CNT * 2 * ObDagPrio::DAG_PRIO_MAX + MAX_SHOW_DAG_NET_CNT_PER_PRIO);
    void * buf = nullptr;
    ObDagInfo *info_list = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObDagInfo) * total_dag_cnt))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to alloc scheduler info", K(ret));
    } else {
      info_list = reinterpret_cast<ObDagInfo *>(new (buf) ObDagInfo[total_dag_cnt]);
      for (int64_t i = 0; i < ObDagPrio::DAG_PRIO_MAX && idx < total_dag_cnt; ++i) {
        prio_sche_[i].get_all_dag_info(info_list, dag_infos, idx, total_dag_cnt);
      }
      dag_net_sche_.get_all_dag_info(info_list, dag_infos, idx, total_dag_cnt);
    }
  }
  return ret;
}

int ObTenantDagScheduler::get_all_compaction_dag_info(
    ObIAllocator &allocator,
    ObIArray<compaction::ObTabletCompactionProgress *> &progress_array)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  int64_t total_dag_cnt = 0;

  for (int64_t i = 0; i < ObIDag::MergeDagTypeCnt; ++i) {
    total_dag_cnt += get_type_dag_cnt(ObIDag::MergeDagType[i]);
  }
  if (total_dag_cnt > 0) {
    total_dag_cnt = MIN(total_dag_cnt, ObDagPrioScheduler::MAX_SHOW_DAG_CNT * ObIDag::MergeDagTypeCnt);
    void * buf = nullptr;
    if (NULL == (buf = allocator.alloc(sizeof(ObDagInfo) * total_dag_cnt))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to alloc dag", K(ret), K(total_dag_cnt));
    } else {
      compaction::ObTabletCompactionProgress *progress =
          reinterpret_cast<compaction::ObTabletCompactionProgress*>(new (buf) compaction::ObTabletCompactionProgress[total_dag_cnt]);
      // add ready dag list
      for (int64_t i = 0; i < ObIDag::MergeDagPrioCnt && idx < total_dag_cnt; ++i) {
        prio_sche_[ObIDag::MergeDagPrio[i]].add_compaction_info(idx, total_dag_cnt, READY_DAG_LIST, progress, progress_array);
      }
      // add rand dag list
      for (int64_t i = 0; i < ObIDag::MergeDagPrioCnt && idx < total_dag_cnt; ++i) {
        prio_sche_[ObIDag::MergeDagPrio[i]].add_compaction_info(idx, total_dag_cnt, RANK_DAG_LIST, progress, progress_array);
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
  } else if (OB_FAIL(prio_sche_[ObDagPrio::DAG_PRIO_COMPACTION_MID].get_minor_exe_dag_info(dag, merge_range_array))) {
    COMMON_LOG(WARN, "fail to get minor exe dag info", K(ret), K(dag));
  }
  return ret;
}

int ObTenantDagScheduler::check_ls_compaction_dag_exist_with_cancel(const ObLSID &ls_id, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  bool tmp_exist = false;
  if (OB_FAIL(dag_net_sche_.check_ls_compaction_dag_exist_with_cancel(ls_id, tmp_exist))) {
    LOG_WARN("failed to check ls compaction dag exist", K(ret), K(ls_id));
  } else if (tmp_exist) {
    exist = true;
  }
  if (OB_FAIL(ret)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ObIDag::MergeDagPrioCnt; ++i) {
      tmp_exist = false;
      if (OB_FAIL(prio_sche_[ObIDag::MergeDagPrio[i]].check_ls_compaction_dag_exist_with_cancel(ls_id, tmp_exist))) {
        LOG_WARN("failed to check ls compaction dag exist", K(ret), K(ls_id));
      } else if (tmp_exist) {
        exist = true;
      }
    }
  }
  return ret;
}

int ObTenantDagScheduler::get_compaction_dag_count(int64_t dag_count)
{
  int ret = OB_SUCCESS;
  dag_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < ObIDag::MergeDagPrioCnt; ++i) {
    if (OB_FAIL(prio_sche_[ObIDag::MergeDagPrio[i]].get_compaction_dag_count(dag_count))) {
      LOG_WARN("failed to get compaction dag count", K(ret), K(i));
    }
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
  } else if (OB_FAIL(prio_sche_[ObDagPrio::DAG_PRIO_COMPACTION_MID].diagnose_minor_exe_dag(*merge_dag_info, progress))) {
    COMMON_LOG(WARN, "fail to diagnose minor exe dag", K(ret), KP(merge_dag_info));
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
  } else if (OB_FAIL(prio_sche_[ObDagPrio::DAG_PRIO_COMPACTION_LOW].get_max_major_finish_time(
      version, estimated_finish_time))) {
    COMMON_LOG(WARN, "failed to get max finish_time", K(ret), K(version));
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
    COMMON_LOG(WARN, "invalid arugment", KP(dag));
  } else if (OB_FAIL(prio_sche_[dag->get_priority()].diagnose_dag(*dag, progress))) {
    COMMON_LOG(WARN, "fail to diagnose dag", K(ret), KPC(dag));
  }
  return ret;
}

int ObTenantDagScheduler::diagnose_dag_net(
    ObIDagNet *dag_net,
    common::ObIArray<compaction::ObDiagnoseTabletCompProgress> &progress_list,
    ObDagId &dag_net_id,
    int64_t &start_time)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObDagScheduler is not inited", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arugment", KP(dag_net));
  } else if (OB_FAIL(dag_net_sche_.diagnose_dag_net(*dag_net, progress_list, dag_net_id, start_time))) {
    COMMON_LOG(WARN, "fail to diagnose dag net", K(ret), KPC(dag_net));
  }
  return ret;
}

int ObTenantDagScheduler::diagnose_all_dags()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObDagScheduler is not inited", K(ret));
  } else {
    for (int64_t i = 0; i < ObDagPrio::DAG_PRIO_MAX; ++i) {
      if (OB_TMP_FAIL(prio_sche_[i].diagnose_all_dags())) {
        COMMON_LOG(WARN, "fail to diagnose running task", K(tmp_ret), "priority", i);
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
    diagnose_for_suggestion();
    dump_dag_status();
    loop_dag_net();
    {
      if (!has_set_stop()) {
        if (OB_FAIL(schedule())) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ObThreadCondGuard guard(scheduler_sync_);
            if (OB_SUCC(guard.get_ret())) {
              try_reclaim_threads();
              scheduler_sync_.wait(SCHEDULER_WAIT_TIME_MS);
            }
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
  if (OB_SUCCESS == cond_guard.get_ret()) {
    scheduler_sync_.signal();
  }
}

void ObTenantDagScheduler::notify_when_dag_net_finish()
{
  set_fast_schedule_dag_net();
  notify();
}

int ObTenantDagScheduler::deal_with_finish_task(ObITask &task, ObTenantDagWorker &worker, int error_code)
{
  int ret = OB_SUCCESS;
  ObIDag *dag = nullptr;
  if (OB_ISNULL(dag = task.get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_UNLIKELY(!dag->is_valid_type())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "invalid dag", K(ret), K(*dag));
  } else if (OB_FAIL(prio_sche_[dag->get_priority()].deal_with_finish_task(task, worker, error_code))) {
    COMMON_LOG(WARN, "fail to finish task", K(ret), K(*dag));
  } else {
    ObThreadCondGuard guard(scheduler_sync_);
    if (OB_SUCC(guard.get_ret())) {
      free_workers_.add_last(&worker);
      worker.set_task(NULL);
      if (OB_FAIL(scheduler_sync_.signal())) {
        COMMON_LOG(WARN, "Failed to signal", K(ret), KPC(dag));
      }
    }
  }

  return ret;
}

void ObTenantDagScheduler::finish_dag_net(ObIDagNet *dag_net)
{
  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(dag_net)) {
    COMMON_LOG(INFO, "start finish dag net", KPC(dag_net));
    if (OB_TMP_FAIL(dag_net->add_dag_warning_info())) {
      COMMON_LOG_RET(WARN, tmp_ret, "failed to add dag warning info in dag net into mgr", K(tmp_ret), KPC(dag_net));
    }
    if (OB_TMP_FAIL(dag_net->clear_dag_net_ctx())) {
      COMMON_LOG_RET(WARN, tmp_ret, "failed to clear dag net ctx", K(tmp_ret), KPC(dag_net));
    }
    (void) dag_net_sche_.finish_dag_net(*dag_net);
    (void) free_dag_net(dag_net);
  }
}

bool ObTenantDagScheduler::try_switch(ObTenantDagWorker &worker)
{
  bool need_pause = false;
  const int64_t priority = worker.get_task()->get_dag()->get_priority();

  // forbid switching after stop sign has been set, which means running workers won't pause any more
  if (!has_set_stop()) {
    need_pause = prio_sche_[priority].try_switch(worker);
  }
  return need_pause;
}

int ObTenantDagScheduler::generate_dag_id(ObDagId &dag_id)
{
  int ret = OB_SUCCESS;
  dag_id.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_FAIL(ObSysTaskStatMgr::get_instance().generate_task_id(dag_id))) {
    LOG_WARN("failed to generate task id", K(ret), K(dag_id));
  }
  return ret;
}

bool ObTenantDagScheduler::dag_count_overflow(const ObDagType::ObDagTypeEnum type)
{
  return get_dag_count(type) >= get_dag_limit();
}

int64_t ObTenantDagScheduler::allowed_schedule_dag_count(const ObDagType::ObDagTypeEnum type)
{
  int64_t count = get_dag_limit() - get_dag_count(type);
  return count < 0 ? 0 : count;
}

int64_t ObTenantDagScheduler::get_dag_count(const ObDagType::ObDagTypeEnum type)
{
  int64_t count = -1;
  if (type >= 0 && type < ObDagType::DAG_TYPE_MAX) {
    count = get_type_dag_cnt(type);
  } else {
    COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid type", K(type));
  }
  return count;
}

int64_t ObTenantDagScheduler::get_dag_net_count(const ObDagNetType::ObDagNetTypeEnum type)
{
  return dag_net_sche_.get_dag_net_count(type);
}

int ObTenantDagScheduler::schedule()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ret = loop_ready_dag_lists(); // loop ready dag lists

  if (REACH_TENANT_TIME_INTERVAL(loop_waiting_dag_list_period_))  {
    for (int i = 0; i < ObDagPrio::DAG_PRIO_MAX; ++i) {
      if (OB_TMP_FAIL(prio_sche_[i].loop_waiting_dag_list())) {
        COMMON_LOG(WARN, "failed to loop waiting task list", K(tmp_ret), K(i));
      }
    }
  }

  return ret;
}

void ObTenantDagScheduler::loop_dag_net()
{
  int tmp_ret = OB_SUCCESS;
  bool need_loop_running_dag_net = false;
  if (OB_TMP_FAIL(dag_net_sche_.loop_blocking_dag_net_list())) {
    COMMON_LOG_RET(WARN, tmp_ret, "failed to loop blocking dag net list");
  }
  if (need_fast_schedule_dag_net()) {
    need_loop_running_dag_net = true;
    clear_fast_schedule_dag_net();
  }
  if (need_loop_running_dag_net || REACH_TENANT_TIME_INTERVAL(LOOP_RUNNING_DAG_NET_MAP_INTERVAL))  {
    if (OB_TMP_FAIL(dag_net_sche_.loop_running_dag_net_list())) {
      COMMON_LOG_RET(WARN, tmp_ret, "failed to loop running_dag_net_list");
    }
    if (OB_TMP_FAIL(dag_net_sche_.loop_finished_dag_net_list())) {
      COMMON_LOG_RET(WARN, tmp_ret, "failed to loop finished_dag_net_list");
    }
  }
}

int ObTenantDagScheduler::loop_ready_dag_lists()
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  if (get_total_running_task_cnt() < get_work_thread_num()) {
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < ObDagPrio::DAG_PRIO_MAX; ++i) {
      if (OB_FAIL(prio_sche_[i].loop_ready_dag_list(is_found))) {
        COMMON_LOG(WARN, "fail to loop ready dag list", K(ret), "priority", i);
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

  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    COMMON_LOG(WARN, "scheduler is not init", K(ret));
  } else {
    ObThreadCondGuard guard(scheduler_sync_);
    if (OB_SUCC(guard.get_ret())) {
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
    }
  }
  return ret;
}

// call this func locked by scheduler_sync_
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

// call this func locked by scheduler_sync_
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
    for (int64_t i = 0; i < ObDagPrio::DAG_PRIO_MAX; ++i) {
      prio_sche_[i].destroy_workers();
    }
  }

  // wait all workers finish, then delete all workers
  while (true) {
    // 100ms
    ob_usleep(100 * 1000);
    if (total_worker_cnt_ == free_workers_.get_size()) {
      DLIST_REMOVE_ALL_NORET(worker, free_workers_) {
        ob_delete(worker);
      }
      free_workers_.reset();
      break;
    }
  }
}

int ObTenantDagScheduler::set_thread_score(const int64_t priority, const int64_t score)
{
  int ret = OB_SUCCESS;
  int64_t old_val = 0;
  int64_t new_val = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_UNLIKELY(priority < 0 || priority >= ObDagPrio::DAG_PRIO_MAX || score < 0 || score > INT32_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(priority), K(score));
  } else if (OB_FAIL(prio_sche_[priority].set_thread_score(score, old_val, new_val))){
    COMMON_LOG(WARN, "fail to set thread score", K(ret));
  } else {
    ObThreadCondGuard guard(scheduler_sync_);
    if (OB_SUCC(ret)) {
      if (old_val != new_val) {
        work_thread_num_ -= old_val;
        work_thread_num_ += new_val;
      }
      if (OB_FAIL(scheduler_sync_.signal())) {
        STORAGE_LOG(WARN, "Failed to signal", K(ret), K(priority), K(score));
      } else {
        COMMON_LOG(INFO, "set thread score successfully", K(score),
            "prio", OB_DAG_PRIOS[priority].dag_prio_str_,
            "limits_", new_val, K_(work_thread_num),
            K_(default_work_thread_num));
      }
    }
  }
  return ret;
}

int64_t ObTenantDagScheduler::get_running_task_cnt(const ObDagPrio::ObDagPrioEnum priority)
{
  int64_t count = -1;
  if (priority >= 0 && priority < ObDagPrio::DAG_PRIO_MAX) {
    count = prio_sche_[priority].get_running_task_cnt();
  } else {
    COMMON_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid priority", K(priority));
  }
  return count;
}

int ObTenantDagScheduler::get_limit(const int64_t prio, int64_t &limit)
{
  int ret = OB_SUCCESS;
  limit = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_UNLIKELY(prio < 0 || prio >= ObDagPrio::DAG_PRIO_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(prio));
  } else if (FALSE_IT(limit = prio_sche_[prio].get_limit())) {
    // TODO get max thread concurrency of current prio type
  }
  return ret;
}

int ObTenantDagScheduler::get_adaptive_limit(const int64_t prio, int64_t &limit)
{
  int ret = OB_SUCCESS;
  limit = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_UNLIKELY(prio < 0 || prio >= ObDagPrio::DAG_PRIO_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(prio));
  } else if (FALSE_IT(limit = prio_sche_[prio].get_adaptive_limit())) {
    // TODO get max thread concurrency of current prio type
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
  } else if (OB_FAIL(prio_sche_[dag->get_priority()].check_dag_exist(*dag, exist))) {
    COMMON_LOG(WARN, "fail to check dag exist", K(ret));
  }
  return ret;
}

// MAKE SURE parent_dag is valid
// If dag is destructor and free, KPC(dag) will abort. So if the dag maybe finished, you should use a new dag with the same hash key.
// See the 7th case in test_dag_scheduler.test_cancel_running_dag for details.
int ObTenantDagScheduler::cancel_dag(const ObIDag *dag, const bool force_cancel)
{
  int ret = OB_SUCCESS;
  ObIDagNet *erase_dag_net = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arugment", KP(dag));
  } else if (OB_FAIL(prio_sche_[dag->get_priority()].cancel_dag(*dag, force_cancel))) {
    COMMON_LOG(WARN, "fail to cancel dag", K(ret), KPC(dag));
  }
  return ret;
}

int ObTenantDagScheduler::check_dag_net_exist(
    const ObDagId &dag_id, bool &exist)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_FAIL(dag_net_sche_.check_dag_net_exist(dag_id, exist))) {
    COMMON_LOG(WARN, "fail to check dag net exist", K(ret));
  }
  return ret;
}

int ObTenantDagScheduler::cancel_dag_net(const ObDagId &dag_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_FAIL(dag_net_sche_.cancel_dag_net(dag_id))) {
    LOG_WARN("fail to cancel dag net", K(ret), K(dag_id));
  } else {
    notify();
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
  } else if (OB_FAIL(prio_sche_[dag->get_priority()].get_complement_data_dag_progress(*dag, row_scanned, row_inserted))) {
    COMMON_LOG(WARN, "fail to get complement data dag progress", K(ret), KPC(dag));
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
  } else if (OB_FAIL(dag_net_sche_.get_first_dag_net(dag_net))) {
    LOG_WARN("fail to cancel dag net", K(ret));
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
