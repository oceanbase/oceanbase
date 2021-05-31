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

#include "share/scheduler/ob_tenant_thread_pool.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/scheduler/ob_worker_obj_pool.h"
#include "share/scheduler/ob_dag_worker.h"
#include "share/scheduler/ob_dag.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "share/rc/ob_context.h"

namespace oceanbase {
using namespace lib;
using namespace common;
using namespace omt;
namespace share {

/***************************************ObTenantThreadPool impl********************************************/
ObTenantThreadPool::ObTenantThreadPool(int64_t tenant_id)
    : is_inited_(false),
      is_stoped_(false),
      tenant_id_(tenant_id),
      max_thread_num_(0),
      last_schedule_time_(0),
      running_tasks_(0, NULL, ObModIds::OB_SCHEDULER)
#ifndef NDEBUG
      ,
      schedule_info_(NULL)
#endif
{}
/*
// for test
void print_task_list(common::ObSortedVector<ObITaskNew *> *list)
{
  COMMON_LOG(INFO, "print_task_list", K(list->count()));
  for (int i = 0; i < list->count(); ++i) {
    COMMON_LOG(INFO, "print_task_list task", K(i), K(list->at(i)));
  }
}
*/

ObTenantThreadPool::~ObTenantThreadPool()
{
  destroy();
}

void ObTenantThreadPool::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    tenant_id_ = 0;
    max_thread_num_ = 0;
    COMMON_LOG(INFO, "ObTenantThreadPool start to destroy", K(this));
    // wait for all running_tasks to finish
    while (running_tasks_.count() > 0) {}

    {
      obsys::CWLockGuard guard(rwlock_);
      // clear type_info_list_ & dynamic_score_list_
      dynamic_score_list_.reset();  // all dynamic_score_ is destory in type_info_list_
      for (int i = 0; i < type_info_list_.count(); ++i) {
        if (!OB_ISNULL(type_info_list_.at(i)) && !OB_ISNULL(type_info_list_.at(i)->dynamic_score_)) {
          ob_delete(type_info_list_.at(i)->dynamic_score_);
        }
      }
      type_info_list_.reset();

      running_tasks_.reset();
      for (int i = 0; i < waiting_tasks_.count(); ++i) {
        waiting_tasks_.at(i)->clear();
        ob_delete(waiting_tasks_.at(i));  // delete OB_NEW list
      }
      waiting_tasks_.reset();
      // clear ready_dag_list_
      for (int i = 0; i < ready_dag_list_.count(); ++i) {
        ObSortList<ObIDagNew>* list = ready_dag_list_.at(i);
        ObIDagNew* cur = list->get_first();
        ObIDagNew* head = list->get_header();
        ObIDagNew* next = NULL;
        while (NULL != cur && head != cur) {
          next = cur->get_next();
          cur->~ObIDagNew();
          allocator_.free((void*)cur);
          cur = next;
        }
        ready_dag_list_.at(i)->clear();
        ob_delete(ready_dag_list_.at(i));  // delete OB_NEW list
      }
      ready_dag_list_.reset();

      if (dag_map_.created()) {
        dag_map_.destroy();
      }
    }
#ifndef NDEBUG
    if (!OB_ISNULL(schedule_info_)) {
      schedule_info_->destroy();
      schedule_info_ = NULL;
    }
#endif
    allocator_.destroy();
    COMMON_LOG(INFO, "ObTenantThreadPool is destroyed", K(this));
  }
}

int ObTenantThreadPool::init(const int64_t hash_bucket_num /*= DEFAULT_BUCKET_NUM*/,
    const int64_t total_mem_limit /*= TOTAL_LIMIT*/, const int64_t hold_mem_limit /*= HOLD_LIMIT*/,
    const int64_t page_size /*= PAGE_SIZE*/)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "ObTenantThreadPool init twice", K(ret), K_(tenant_id));
  } else if (0 >= total_mem_limit || 0 >= hold_mem_limit || hold_mem_limit > total_mem_limit || 0 >= page_size) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN,
        "init ObTenantThreadPool with invalid arguments",
        K(ret),
        K(total_mem_limit),
        K(hold_mem_limit),
        K(page_size));
  } else if (OB_FAIL(allocator_.init(total_mem_limit, hold_mem_limit, page_size))) {
    COMMON_LOG(WARN, "failed to init allocator", K(ret), K(total_mem_limit), K(hold_mem_limit), K(page_size));
  } else if (OB_FAIL(dag_map_.create(hash_bucket_num, ObModIds::OB_HASH_BUCKET_DAG_MAP))) {
    COMMON_LOG(WARN, "failed to create dap map", K(ret), K(hash_bucket_num));
  } else {
    allocator_.set_label(ObModIds::OB_SCHEDULER);
    is_inited_ = true;
    MEMSET(type_penalty_, 0, sizeof(type_penalty_));
    MEMSET(dag_cnts_, 0, sizeof(dag_cnts_));
    if (OB_FAIL(add_types_())) {  // add types from the list
      COMMON_LOG(WARN, "failed to add types", K(ret));
    }
  }
#ifndef NDEBUG
  schedule_info_ = NULL;
#endif
  if (!is_inited_) {
    destroy();
    COMMON_LOG(WARN, "failed to init ObDagSchedulerNew", K(ret));
  } else {
    COMMON_LOG(INFO, "ObTenantThreadPool is inited successful", K(ret), K(this));
  }
  return ret;
}

int ObTenantThreadPool::add_dag(ObIDagNew* dag)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {  // not init
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantThreadPool is not inited", K(ret));
  } else if (!is_type_id_valid_(dag->get_type_id()) || !dag->is_valid()) {  // type_id didn't regist
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid type_id", K(ret), K(*dag), K(dag->get_type_id()), K(type_info_list_.count()));
  } else {
    obsys::CWLockGuard guard(rwlock_);  // write lock for dag container
    if (OB_SUCCESS != (hash_ret = dag_map_.set_refactored(dag, dag))) {
      if (OB_HASH_EXIST == hash_ret) {
        COMMON_LOG(WARN, "add dag map failed, exists", K(hash_ret), K(*dag));
        ret = OB_EAGAIN;
      } else {
        ret = hash_ret;
        COMMON_LOG(WARN, "failed to set dag_map", K(hash_ret), K(*dag));
      }
    } else if (!ready_dag_list_.at(dag->get_type_id())->insert(dag)) {  // insert into the dag list
      if (OB_SUCCESS != (hash_ret = dag_map_.erase_refactored(dag))) {
        COMMON_LOG(ERROR, "failed to erase dag from dag_map", K(hash_ret), K(*dag));
        ob_abort();
      } else {
        COMMON_LOG(INFO, "erase dag from dag_map");
      }
    } else {
      dag->set_dag_status(ObIDagNew::DAG_STATUS_READY);
      dag->start_time_ = ObTimeUtility::current_time();
      dag->set_tenant_thread_pool(this);
      ++dag_cnts_[dag->get_type_id()];  // increase the count
      COMMON_LOG(INFO,
          "add dag success",
          K(dag),
          K_(dag->start_time),
          K_(dag->id),
          K(ready_dag_list_.at(dag->get_type_id())->get_size()),
          K(*dag));
    }
  }  // end of lock
  return ret;
}
// finish_task call remove_dag with lock
int ObTenantThreadPool::remove_dag_(ObIDagNew* dag)
{
  int ret = OB_SUCCESS;
  if (!dag || !dag->is_valid_type()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(*dag));
  } else if (!ready_dag_list_.at(dag->get_type_id())->remove(dag)) {
    ret = OB_ENTRY_NOT_EXIST;
    COMMON_LOG(WARN, "dag is not in ready_dag_list_", K(ret), K(*dag));
  } else if (OB_FAIL(dag_map_.erase_refactored(dag))) {
    COMMON_LOG(WARN, "dag is not in dag_map_", K(ret), K(*dag));
  } else {
    --dag_cnts_[dag->get_type_id()];  // increase the count
    COMMON_LOG(INFO, "remove dag success", K(ret), K(dag), K(*dag));
  }
  return ret;
}
// add lock to copy then print
void ObTenantThreadPool::dump_dag_status()
{
  // print all info
  if (REACH_TIME_INTERVAL(DUMP_DAG_STATUS_INTERVAL)) {
    int type_init_score[MAX_TYPE_CNT];
    double dynamic_score[MAX_TYPE_CNT];
    int32_t up_limits[MAX_TYPE_CNT];
    int64_t dag_count[MAX_TYPE_CNT];
    int64_t running_task_cnt = 0;

    {                                     // dump the info
      obsys::CRLockGuard guard(rwlock_);  // read lock for dag container
      running_task_cnt = running_tasks_.count();
      TypeInfo* info_ptr = NULL;
      for (int i = 0; i < type_info_list_.count(); ++i) {
        info_ptr = type_info_list_.at(i);
        type_init_score[i] = info_ptr->score_;
        dynamic_score[i] = info_ptr->dynamic_score_->d_score_;
        up_limits[i] = info_ptr->up_limit_;
        dag_count[i] = dag_cnts_[i];
      }
    }  // end of lock
    COMMON_LOG(INFO, "dump_dag_status", "running_task_cnt", running_task_cnt);
    for (int i = 0; i < type_info_list_.count(); ++i) {
      COMMON_LOG(INFO,
          "dump_dag_status",
          "type",
          ObDagTypeStr[i],
          "score",
          type_init_score[i],
          "dynamic_score",
          dynamic_score[i],
          "up_limits",
          up_limits[i],
          "dag_count",
          dag_count[i]);
    }
  }
}

int ObTenantThreadPool::sys_task_start_(ObIDagNew* dag)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret), KP(dag));
  } else if (dag->get_dag_status() != ObIDagNew::DAG_STATUS_READY) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "dag status error", K(ret), K(dag->get_dag_status()));
  } else {
    dag->set_dag_status(ObIDagNew::DAG_STATUS_NODE_RUNNING);
    ObSysTaskStat sys_task_status;

    sys_task_status.start_time_ = ObTimeUtility::current_time();
    sys_task_status.task_id_ = dag->get_dag_id();  // useless
    sys_task_status.tenant_id_ = dag->get_tenant_id();
    // sys_task_status.task_type_ = static_cast<int>(dag->get_type_id());
    // TODO update sys_task_status

    // allow comment truncation, no need to set ret
    (void)dag->fill_comment(sys_task_status.comment_, sizeof(sys_task_status.comment_));
    if (OB_FAIL(ObSysTaskStatMgr::get_instance().add_task(sys_task_status))) {
      COMMON_LOG(WARN, "failed to add sys task", K(ret), K(sys_task_status));
    } else if (OB_FAIL(dag->set_dag_id(sys_task_status.task_id_))) {
      COMMON_LOG(WARN, "failed to set dag id", K(ret), K(sys_task_status.task_id_));
    }
  }
  return ret;
}

int ObTenantThreadPool::get_dag_count(const int64_t type_id, int64_t& dag_cnt)
{
  int ret = OB_SUCCESS;
  dag_cnt = 0;
  if (!is_type_id_valid_(type_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "get_dag_count with wrong argument", K(type_id));
  } else {
    obsys::CRLockGuard guard(rwlock_);  // read lock for dag container
    dag_cnt = dag_cnts_[type_id];
  }
  return ret;
}

int ObTenantThreadPool::set_tenant_setting(ObTenantSetting& tenant_setting)
{
  int ret = OB_SUCCESS;
  obsys::CWLockGuard guard(rwlock_);
  if (OB_FAIL(set_max_thread_num_(tenant_setting.max_thread_num_))) {
    COMMON_LOG(WARN, "set max thread num failed", K(ret));
  } else {
    ObTenantTypeSetting* type_setting = NULL;
    int64_t type_id = 0;
    for (int i = 0; i < tenant_setting.type_settings_.size(); ++i) {
      type_setting = &(tenant_setting.type_settings_.at(i));
      type_id = type_setting->type_id_;
      if (OB_ISNULL(type_setting)) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid TenantTypeSetting");
      } else if (type_id < 0 || (ObTenantSetting::NOT_SET != type_setting->score_ && type_setting->score_ <= 0) ||
                 (ObTenantSetting::NOT_SET != type_setting->up_limit_ && type_setting->up_limit_ <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN,
            "invalid type_setting or type_id_",
            K(ret),
            "type_id",
            type_setting->type_id_,
            "score",
            type_setting->score_,
            "uplimit",
            type_setting->up_limit_);
      }
      if (OB_SUCC(ret) && ObTenantSetting::NOT_SET != type_setting->score_) {
        type_info_list_.at(type_id)->score_ = type_setting->score_;
        reset_dynamic_score_();
        COMMON_LOG(INFO, "set score success", K(type_id), "score", type_setting->score_);
      }
      if (OB_SUCC(ret) && ObTenantSetting::NOT_SET != type_setting->up_limit_) {
        type_info_list_.at(type_id)->up_limit_ = type_setting->up_limit_;
        COMMON_LOG(INFO, "set up limit success", K(type_id), "up_limit", type_setting->up_limit_);
      }
    }
  }
  return ret;
}

int ObTenantThreadPool::set_max_thread_num_(int32_t max_thread_num)
{
  int ret = OB_SUCCESS;
  // set max thread num
  if (ObTenantSetting::NOT_SET != max_thread_num && max_thread_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid max thread num", K(ret), K(max_thread_num));
  } else if (ObTenantSetting::NOT_SET != max_thread_num) {
    int32_t old_max_thread_num = max_thread_num_;  // record the old max thread num
    max_thread_num_ = max_thread_num;
    ObWorkerObjPool::get_instance().set_init_worker_cnt_inc(max_thread_num - old_max_thread_num);
    COMMON_LOG(INFO, "set max thread num success", K(ret), K_(tenant_id), K_(max_thread_num));
  }
  return ret;
}

int ObTenantThreadPool::get_tenant_setting(ObTenantSetting& tenant_setting)
{
  int ret = OB_SUCCESS;
  obsys::CWLockGuard guard(rwlock_);
  tenant_setting.tenant_id_ = tenant_id_;
  tenant_setting.max_thread_num_ = max_thread_num_;
  for (int i = 0; i < ObDagTypeIds::TYPE_SETTING_END; ++i) {
    ObTenantTypeSetting setting;
    setting.type_id_ = i;
    if (OB_ISNULL(type_info_list_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "type info in list is NULL", K(ret));
    } else {
      setting.score_ = type_info_list_.at(i)->score_;
      setting.up_limit_ = type_info_list_.at(i)->up_limit_;
      tenant_setting.type_settings_.push_back(setting);
    }
  }
  return ret;
}

// call this func with lock
int ObTenantThreadPool::add_types_()
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < ObDagTypeIds::TYPE_SETTING_END; ++i) {
    if (OB_FAIL(add_type_(i, OB_DAG_TYPE[i].default_score_))) {
      COMMON_LOG(WARN, "add type into tenant thread pool failed", K(ret), "type", ObDagTypeStr[i]);
    } else {
      COMMON_LOG(INFO,
          "add type into tenant thread pool success",
          K(ret),
          "type",
          ObDagTypeStr[i],
          "default_score",
          OB_DAG_TYPE[i].default_score_);
    }
  }
  return ret;
}

int ObTenantThreadPool::add_type_(int64_t type_id, int64_t default_score)
{
  int ret = OB_SUCCESS;
  if (type_id > MAX_TYPE_CNT || default_score <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "add_type with wrong argument", K(type_id), K(default_score));
  } else if (type_id < type_info_list_.count()) {  // can't excced count
    ret = OB_ENTRY_EXIST;
    COMMON_LOG(WARN, "add_type twice", K(type_id), K(default_score));
  } else if (type_id != type_info_list_.count()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "didn't add type in sequence", K(type_id), "last_type_id", type_info_list_.count() - 1);
  } else {
    TypeInfo* new_type_info = OB_NEW(TypeInfo, ObModIds::OB_SCHEDULER);  // new type info struct
    if (OB_ISNULL(new_type_info)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "allocate TypeInfo failed");
    } else {
      new_type_info->score_ = default_score;
      new_type_info->dynamic_score_ = OB_NEW(DynamicScore, ObModIds::OB_SCHEDULER);
      new_type_info->dynamic_score_->type_id_ = type_id;
      if (OB_FAIL(type_info_list_.push_back(new_type_info))) {
        COMMON_LOG(WARN, "add type into list failed", K(type_id));
        ob_delete(new_type_info);
      } else if (!dynamic_score_list_.insert(new_type_info->dynamic_score_)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "add dynamic score into list failed", K(type_id));
      } else {
        reset_dynamic_score_();
        // init ready_dag_list_[type_id] & waiting_tasks_[type_id]
        ObSortList<ObIDagNew>* dag_list = OB_NEW(ObSortList<ObIDagNew>, ObModIds::OB_SCHEDULER);
        if (OB_FAIL(ready_dag_list_.push_back(dag_list))) {
          COMMON_LOG(WARN, "add ready dag list failed", K(ret), K(type_id));
          ob_abort();
        } else {
          TaskList* task_list = OB_NEW(TaskList, ObModIds::OB_SCHEDULER);
          if (OB_ISNULL(task_list)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            COMMON_LOG(WARN, "alloc waiting_task_ failed", K(type_id));
          } else if (OB_FAIL(waiting_tasks_.push_back(task_list))) {
            COMMON_LOG(WARN, "add list into waiting task list failed", K(ret));
            ob_abort();
          }
        }
      }
    }
  }
  return ret;
}

// only be called by set_score/add_type with lock
void ObTenantThreadPool::reset_dynamic_score_()
{
  dynamic_score_list_.reset();  // clear all dynamic_scores
  TypeInfo* ptr = NULL;
  double score_sum = 0;
  for (int i = 0; i < type_info_list_.count(); ++i) {  // calc score sum
    score_sum += type_info_list_.at(i)->score_;
  }
  for (int i = 0; i < type_info_list_.count(); ++i) {
    ptr = type_info_list_.at(i);
    ptr->dynamic_score_->d_score_ = score_sum - ptr->score_;
    dynamic_score_list_.insert(ptr->dynamic_score_);
  }
  calc_penalty_();
}

// schedule call reset_switch_flag with lock
int ObTenantThreadPool::reset_switch_flag_()
{
  int ret = OB_SUCCESS;
  TaskListIterator iter = running_tasks_.begin();
  COMMON_LOG(INFO, "reset_switch_flag", K(running_tasks_.count()), K(running_tasks_));
  // print_task_list(&running_tasks_);
  ObITaskNew* task = NULL;
  while (running_tasks_.end() != iter) {  // ignore the failure of resetting one task
    task = *iter;
    if (OB_ISNULL(task) || OB_ISNULL(task->get_worker())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "running task don't have worker", K(task));
    } else if (ObDagWorkerNew::SF_SWITCH == task->get_worker()->get_switch_flag()) {  // should switch but not
      if (OB_FAIL(execute_penalty_(task->get_type_id()))) {                           // execute the penalty
        COMMON_LOG(WARN, "execute penalty failed", K(task), "type_id", task->get_type_id());
      }
      task->get_worker()->set_switch_flag(ObDagWorkerNew::SF_CONTINUE);  // let it run one more time_slice
      ObITaskNew* next_task = NULL;
      if (OB_ISNULL(next_task = task->get_next_task())) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "task with switch flag doesn't have next_task_", K(task));
      } else if (OB_FAIL(recover_penalty_(next_task->get_type_id()))) {  // recover penalty of next_task_
        COMMON_LOG(WARN, "recover penalty failed", K(next_task), "type_id", next_task->get_type_id());
      }
      next_task->set_status(ObITaskNew::TASK_STATUS_RUNNABLE);  // can be selected
      task->set_next_task(NULL);                                // cancel switch
    }
    ++iter;
  }
  return ret;
}
// schedule call get_schedule_list with lock
int ObTenantThreadPool::get_schedule_list_(TaskList* schedule_list)
{
  int ret = OB_SUCCESS;
  int64_t type_id = 0;
  ObITaskNew* next_task = NULL;
  int not_visited_cnt = type_info_list_.count();
  int type_visited[type_info_list_.count()];
  int type_cnts[type_info_list_.count()];  // record the running task cnt in each type
  MEMSET(type_visited, 0, sizeof(type_visited));
  MEMSET(type_cnts, 0, sizeof(type_cnts));
  // count is less than max_thread_num_ OR all type are not visited
  // ignore the failure of one round
  while (schedule_list->count() < max_thread_num_ && not_visited_cnt > 0) {
    type_id = dynamic_score_list_.get_last()->type_id_;  // get type of the lowest score
    if (OB_FAIL(execute_penalty_(type_id))) {            // execute penalty
      COMMON_LOG(WARN, "execute penalty failed", K(type_id));
    }
#ifndef NDEBUG
    if (schedule_info_) {
      schedule_info_->choose_type_id_list_.push_back(type_id);
    }
#endif
    if (0 == type_visited[type_id]) {  // set visited flag
      type_visited[type_id] = 1;
      --not_visited_cnt;
    }
    if (ready_dag_list_.at(type_id)->is_empty()) {  // empty dag list
      COMMON_LOG(INFO, "schedule_list ready_dag_list_ empty", K(type_id));
      continue;
    } else if (type_cnts[type_id] >= type_info_list_.at(type_id)->up_limit_) {  // reach upper limit
      COMMON_LOG(INFO, "schedule_list reach up limit", K(type_id), K(type_cnts[type_id]));
      continue;
    } else if (OB_FAIL(get_ready_task_(type_id, ObIDagNew::RUNNABLE, next_task))) {
      COMMON_LOG(INFO, "schedule_list no runnable task", K(ret));
      continue;
    }
    ++type_cnts[type_id];  // increase the count
    TaskListIterator insert_iter;
    if (OB_FAIL(
            schedule_list->insert(next_task, insert_iter, ObITaskNew::compare_task))) {  // insert into schedule_list
      COMMON_LOG(WARN, "insert task into schedule_list failed", K(ret));
    }
    MEMSET(type_visited, 0, sizeof(type_visited));  // reset visited array
    not_visited_cnt = type_info_list_.count();

    COMMON_LOG(INFO, "schedule_list add a task", K(next_task), K(schedule_list->count()));
  }  // end of while
  if (0 == schedule_list->count()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    ret = OB_SUCCESS;
  }
  return ret;
}

void ObTenantThreadPool::copy_task_list_(TaskList* listA, TaskList* listB)
{
  for (int i = 0; i < listA->count(); ++i) {
    listB->push_back(listA->at(i));
  }
}

int ObTenantThreadPool::schedule(int64_t schedule_period)
{
  int ret = OB_SUCCESS;
  if (need_schedule(schedule_period)) {
    obsys::CWLockGuard guard(rwlock_);  // write lock
    if (OB_FAIL(schedule_())) {
      COMMON_LOG(INFO, "ObTenantThreadPool schedule failed", K_(tenant_id));
    }
  }
  return ret;
}

int ObTenantThreadPool::schedule_()
{
  COMMON_LOG(INFO, "ObTenantThreadPool::schedule", K(*this));
  set_last_schedule_time(ObTimeUtility::current_time());

  int ret = OB_SUCCESS;
  TaskList schedule_list(max_thread_num_, NULL, ObModIds::OB_SCHEDULER);

  // step1.reset the switch_flag_
  if (OB_FAIL(reset_switch_flag_())) {
    COMMON_LOG(WARN, "reset switch flag failed", K(ret), K(*this));
  } else {
    // step2.schedule to get a new task list
    if (OB_FAIL(get_schedule_list_(&schedule_list))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        COMMON_LOG(WARN, "get schedule list failed", K(ret), K(*this));
      } else {  // don't have runnable task
        ret = OB_SUCCESS;
      }
    }
  }

  ObITaskNew* continue_task = NULL;
  TaskList old_running_tasks(max_thread_num_, NULL, ObModIds::OB_SCHEDULER);
  copy_task_list_(&running_tasks_, &old_running_tasks);  // copy running_tasks_ into old_running_tasks
  running_tasks_.clear();  // clear running_tasks_ to store running tasks in next time slice
  TaskListIterator found_iter = NULL;
  TaskListIterator insert_iter = NULL;
  // print_task_list(&schedule_list);
  // step3.loop the scheduled list
  TaskListIterator it = schedule_list.begin();
  while (it != schedule_list.end()) {
    continue_task = *it;
    if (OB_FAIL(find_in_task_list_(continue_task, &old_running_tasks, found_iter))) {
      ++it;
    } else {  // found in both old_running_tasks and schedule_list
      if (OB_FAIL(running_tasks_.insert(continue_task, insert_iter, ObITaskNew::compare_task))) {
        COMMON_LOG(WARN, "insert task into running tasks list failed", K(continue_task));
      } else {
        continue_task->set_status(ObITaskNew::TASK_STATUS_RUNNING);
        if (OB_FAIL(old_running_tasks.remove(found_iter))) {
          COMMON_LOG(WARN, "remove task from old running tasks list failed", K(continue_task));
        } else if (OB_FAIL(schedule_list.remove(it))) {  // remove this pos, elements followed will move forward
          COMMON_LOG(WARN, "remove task from schedule list failed", K(continue_task));
        } else if (OB_ISNULL(continue_task->get_worker())) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "continue_task doesn't have worker", K(continue_task));
        }
      }
    }
  }
  if (OB_ENTRY_NOT_EXIST == ret) {  // not found in old running task is not an error
    ret = OB_SUCCESS;
  }
  // print_task_list(&schedule_list);
  if (old_running_tasks.count() || schedule_list.count()) {  // need to build switch relation
    TaskListIterator old_iter = old_running_tasks.begin();
    TaskListIterator sch_iter = schedule_list.begin();
    TaskListIterator ins_iter = NULL;
    ObITaskNew* task_op = NULL;
    // build switch relation
    while (OB_SUCC(ret) && old_running_tasks.end() != old_iter && schedule_list.end() != sch_iter) {
      task_op = *old_iter;
      if (OB_ISNULL(task_op->get_worker())) {
        COMMON_LOG(WARN, "switch task don't have worker", K(task_op));
        ret = OB_ERR_UNEXPECTED;
      } else {
        task_op->get_worker()->set_switch_flag(ObDagWorkerNew::SF_SWITCH);  // set switch flag
        task_op->set_next_task(*sch_iter);  // set next task into the task should be switched
        task_op->set_switch_start_timestamp(ObTimeUtility::current_time());  // set switch start time
        if (OB_FAIL(running_tasks_.insert(task_op, ins_iter, ObITaskNew::compare_task))) {
          COMMON_LOG(WARN, "switch task insert switch_task failed", K(task_op));
        }
      }
      ++old_iter;
      ++sch_iter;
    }
    // the rest task in scheduler_list need start
    while (OB_SUCC(ret) && schedule_list.end() != sch_iter && running_tasks_.count() < max_thread_num_) {  // need start
      task_op = *sch_iter;
      if (OB_ISNULL(task_op->get_worker())) {    // with no worker
        if (OB_FAIL(assign_worker_(task_op))) {  // assign new worker
          COMMON_LOG(WARN, "start task assign_worker failed", K(task_op));
        } else {
          COMMON_LOG(INFO, "start task assign_worker success", K(task_op));
        }
      }
      if (OB_NOT_NULL(task_op->get_worker())) {
        task_op->set_status(ObITaskNew::TASK_STATUS_RUNNING);
        task_op->get_worker()->set_switch_flag(ObDagWorkerNew::SF_CONTINUE);  // start run
        task_op->get_worker()->resume();
        if (OB_FAIL(running_tasks_.insert(task_op, ins_iter, ObITaskNew::compare_task))) {
          COMMON_LOG(WARN, "start task insert into running_tasks_ failed", K(ret), K(task_op));
        } else {
          COMMON_LOG(INFO, "start task insert into running_tasks_ success", K(ret), K(task_op));
        }
      }
      ++sch_iter;
    }
    // rest task in old_running_tasks need to stop
    while (OB_SUCC(ret) && old_running_tasks.end() != old_iter) {  // need stop
      task_op = *old_iter;
      if (OB_ISNULL(task_op->get_worker())) {  // with no worker
        COMMON_LOG(WARN, "stop task assign_worker failed", K(task_op));
        ret = OB_ERR_UNEXPECTED;
      } else {
        task_op->get_worker()->set_switch_flag(ObDagWorkerNew::SF_PAUSE);  // pause
        if (OB_FAIL(running_tasks_.insert(task_op, ins_iter, ObITaskNew::compare_task))) {
          COMMON_LOG(WARN, "start task insert into running_tasks_ failed", K(ret), K(task_op));
        } else {
          COMMON_LOG(INFO, "start task insert into running_tasks_ success", K(ret), K(task_op));
        }
      }
      ++old_iter;
    }
    old_running_tasks.clear();
    schedule_list.clear();
  }
#ifndef NDEBUG
  if (schedule_info_) {
    schedule_info_->running_tasks_cnt_.push_back(running_tasks_.count());
  }
#endif
  return ret;
}

int ObTenantThreadPool::find_in_task_list_(ObITaskNew* task, TaskList* task_list, TaskListIterator& iterator)
{
  int ret = OB_ENTRY_NOT_EXIST;
  TaskListIterator found_iter = NULL;
  bool found = false;
  if (OB_SUCCESS == task_list->find(task, found_iter, ObITaskNew::compare_task)) {
    while (!found && NULL != found_iter && task_list->end() != found_iter) {
      if (task->is_equal(*(*found_iter))) {  // pointer is the same
        iterator = found_iter;
        found = true;
        break;
      } else if (*task > *(*found_iter)) {  // type_id excced
        break;
      } else {
        ++found_iter;
      }
    }
  }
  if (found) {
    ret = OB_SUCCESS;
  } else {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObTenantThreadPool::remove_from_task_list_(ObITaskNew* task, TaskList* task_list)
{
  int ret = OB_SUCCESS;
  TaskListIterator found_iter = NULL;
  if (OB_FAIL(find_in_task_list_(task, task_list, found_iter))) {
    COMMON_LOG(WARN, "task is not in task_list", K(ret), K(task));
  } else if (OB_FAIL(task_list->remove(found_iter))) {
    COMMON_LOG(WARN, "remove from task list failed", K(ret), K(task));
  } else {
    COMMON_LOG(INFO, "remove from task list success", K(task), K(task_list->count()));
  }
  return ret;
}

int ObTenantThreadPool::remove_running_task_(ObITaskNew* task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "remove_running_task with wrong argument", K(*this), K(task));
  } else {
    if (OB_FAIL(remove_from_task_list_(task, &running_tasks_))) {  // remove task from running_tasks_
      COMMON_LOG(WARN, "remove running task failed", K(ret), K(*this), K(task));
    } else {
      COMMON_LOG(INFO, "remove running task success", K(*this), K(task));
    }
  }
  return ret;
}

int ObTenantThreadPool::remove_waiting_task_(ObITaskNew* task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task) || !is_type_id_valid_(task->get_type_id())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "remove_waiting_task with wrong argument", K(*this), K(task));
  } else {
    if (OB_FAIL(
            remove_from_task_list_(task, waiting_tasks_.at(task->get_type_id())))) {  // remove task from waiting_tasks_
      COMMON_LOG(WARN, "remove waiting task failed", K(*this), K(task));
    } else {
      COMMON_LOG(INFO, "remove waiting task success", K(*this), K(task));
    }
  }
  return ret;
}

int ObTenantThreadPool::add_running_task_(ObITaskNew* task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "add_running_task with wrong argument", K(*this), K(task));
  } else {
    TaskListIterator insert_iter = NULL;
    if (OB_FAIL(running_tasks_.insert(task, insert_iter, ObITaskNew::compare_task))) {  // add task into running_tasks_
      COMMON_LOG(INFO, "insert into running task failed", K(*this), K(task));
    } else {
      COMMON_LOG(INFO, "add running task success", K(*this), K(task));
    }
  }
  return ret;
}

int ObTenantThreadPool::add_waiting_task_(ObITaskNew* task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "add_waiting_task task is null", K(this), K(task));
  } else if (!is_type_id_valid_(task->get_type_id())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "add_waiting_task task type_id is invalid", K(this), K(task), "type_id", task->get_type_id());
  } else {
    TaskListIterator insert_iter = NULL;
    // add task into waiting_tasks_
    if (OB_FAIL(waiting_tasks_.at(task->get_type_id())->insert(task, insert_iter, ObITaskNew::compare_task))) {
      COMMON_LOG(INFO, "insert into waiting task failed", K(this), K(task));
    } else {
      COMMON_LOG(INFO, "add waiting task success", K(this), K(task));
    }
  }
  return ret;
}

void ObTenantThreadPool::calc_penalty_()
{
  double score_sum = 0;
  double penalty = 0;
  for (int i = 0; i < type_info_list_.count(); ++i) {  // calc sum
    score_sum += type_info_list_.at(i)->score_;
  }
  for (int i = 0; i < type_info_list_.count(); ++i) {  // calc penalty of each type
    if (0 == type_info_list_.at(i)->score_) {
      type_penalty_[i] = score_sum;
    } else {
      type_penalty_[i] = score_sum / type_info_list_.at(i)->score_;
    }
  }
}

int ObTenantThreadPool::execute_penalty_(int64_t type_id)
{
  int ret = OB_SUCCESS;
  if (!is_type_id_valid_(type_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "execute_penalty with wrong argument", K(type_id));
  } else {  // plus penalty score
    type_info_list_.at(type_id)->dynamic_score_->d_score_ += type_penalty_[type_id];
    // adjust the choosen type_id's dynamic_score
    dynamic_score_list_.adjust(type_info_list_.at(type_id)->dynamic_score_);
  }
  return ret;
}

int ObTenantThreadPool::recover_penalty_(int64_t type_id)
{
  int ret = OB_SUCCESS;
  if (!is_type_id_valid_(type_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "recover_penalty with wrong argument", K(type_id));
  } else {  // minus penalty score
    type_info_list_.at(type_id)->dynamic_score_->d_score_ -= type_penalty_[type_id];
    dynamic_score_list_.adjust(type_info_list_.at(type_id)->dynamic_score_);
  }
  return ret;
}

bool ObTenantThreadPool::is_type_id_valid_(int64_t type_id) const
{
  bool bret = true;
  if (type_id >= type_info_list_.count() || type_id < 0) {
    bret = false;
  }
  return bret;
}

int ObTenantThreadPool::get_ready_task_(const int64_t type_id, const ObIDagNew::GetTaskFlag flag, ObITaskNew*& task)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool found = false;
  // tenant thread pool stoped can only switch to waiting task
  if (is_stoped_) {
    for (int i = 0; i < waiting_tasks_.count(); ++i) {
      if (waiting_tasks_.at(i)->count() > 0) {
        ObITaskNew* tmp_task = *(waiting_tasks_.at(i)->begin());
        if (OB_FAIL(remove_waiting_task_(tmp_task))) {  // remove from waiting_task_
          COMMON_LOG(WARN, "remove from waiting task list failed", K(ret), K(*task), K(*tmp_task));
        }
        tmp_task->set_status(ObITaskNew::TASK_STATUS_WAIT_TO_SWITCH);  // avoid be chosen by others
        task = tmp_task;
        found = true;
        break;
      }
    }
  } else {  // tenant thread pool not stop
    if (!ready_dag_list_.at(type_id)->is_empty()) {
      ObIDagNew* head = ready_dag_list_.at(type_id)->get_header();
      ObIDagNew* cur = head->get_next();
      ObITaskNew* ready_task = NULL;
      // step1.loop the ready_dag_list_ to get a task
      while (!found && NULL != cur && head != cur && OB_SUCC(ret)) {
        if (cur->get_dag_status() == ObIDagNew::DAG_STATUS_READY) {  // not running
          if (OB_SUCCESS != (tmp_ret = sys_task_start_(cur))) {      // get the dag start running
            COMMON_LOG(WARN, "failed to start sys task", K(tmp_ret));
          }
        }
        if (OB_SUCCESS != (tmp_ret = cur->get_next_ready_task(flag, ready_task))) {  // get ready task
          if (OB_ITER_END == tmp_ret) {                                              // check next dag
            cur = cur->get_next();
          } else {
            ret = tmp_ret;
            COMMON_LOG(WARN, "failed to get next ready task", K(ret), K(*cur));
          }
        } else {
          task = ready_task;
          COMMON_LOG(INFO, "get next ready task", K(ret), K(task), K(*task));
          found = true;
        }
      }
    }
    // step2.loop the waiting_tasks_
    if (waiting_tasks_.at(type_id)->count() > 0) {
      ObITaskNew* tmp_task = *(waiting_tasks_.at(type_id)->begin());
      if (!found) {  // no task in ready_task_list
        found = true;
        task = tmp_task;
        remove_waiting_task_(tmp_task);
        if (OB_ISNULL(tmp_task->get_dag())) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "task in waiting task list don't have dag", K(ret), K(task), K(tmp_task));
        } else {
          tmp_task->set_status(ObITaskNew::TASK_STATUS_WAIT_TO_SWITCH);  // avoid be chosen by other thread
        }
      } else if (OB_ISNULL(tmp_task) || OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "task is null", K(ret), K(task), K(tmp_task));
      } else if (OB_ISNULL(tmp_task->get_dag()) || OB_ISNULL(task->get_dag())) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "task don't have dag", K(ret), K(*task), K(*tmp_task));
      } else if (tmp_task == task) {                    // same task
        if (OB_FAIL(remove_waiting_task_(tmp_task))) {  // remove from waiting_task_
          COMMON_LOG(WARN, "remove from waiting task list failed", K(ret), K(*task), K(*tmp_task));
        }
      } else if (tmp_task->get_dag()->get_priority() >= task->get_dag()->get_priority()) {
        task->set_status(ObITaskNew::TASK_STATUS_RUNNABLE);
        task = tmp_task;
        if (OB_FAIL(remove_waiting_task_(tmp_task))) {  // remove from waiting_task_
          COMMON_LOG(WARN, "remove from waiting task list failed", K(ret), K(*task), K(*tmp_task));
        }
        tmp_task->set_status(ObITaskNew::TASK_STATUS_WAIT_TO_SWITCH);  // avoid be chosen by other thread
        COMMON_LOG(INFO, "choose task in waiting_task_", K(ret), K(*task), K(*tmp_task));
      } else {
        COMMON_LOG(INFO, "didn't choose task in waiting_task_", K(ret), K(*task), K(*tmp_task));
      }
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

bool ObTenantThreadPool::has_remain_task(const int64_t type_id)
{
  return waiting_tasks_.at(type_id)->count() > 0 || !ready_dag_list_.at(type_id)->is_empty();
}

int ObTenantThreadPool::finish_task(ObITaskNew* task, ObDagWorkerNew* worker)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIDagNew* dag = NULL;
  bool free_flag = false;
  if (OB_ISNULL(task) || OB_ISNULL(worker)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "finish task with invalid arguments", K(ret), K(task), K(worker));
  } else if (OB_ISNULL(dag = task->get_dag())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "finish task don't have dag", K(ret), K(task), K(worker));
  } else {
    obsys::CWLockGuard guard(rwlock_);  // write lock for dag container
    dag->dec_children_indegree(task);   // decrease all children's indegree
    // check switch flag of worker
    if (ObDagWorkerNew::SF_PAUSE == worker->get_switch_flag()) {
      // try to remove from waiting task
      if (ObITaskNew::TASK_STATUS_WAITING == task->get_status() && OB_FAIL(remove_waiting_task_(task))) {
        COMMON_LOG(WARN, "remove from waiting task list failed", K(ret), K(*task));
      } else if (OB_FAIL(remove_running_task_(task))) {
        COMMON_LOG(WARN, "remove from running task list failed", K(ret), K(*task));
      }
    } else if (ObDagWorkerNew::SF_SWITCH == worker->get_switch_flag() &&
               OB_NOT_NULL(task->get_next_task())) {  // have next_task_ to switch
      if (OB_FAIL(switch_task_(task, ObITaskNew::THIS_TASK_FINISHED))) {
        COMMON_LOG(WARN, "finish task switch task failed", K(ret));
      }
    } else {  // no switch next_task
      // get another task(same type_id) to run
      if (OB_SUCCESS != (tmp_ret = try_switch_task_(task))) {
        if (OB_ENTRY_NOT_EXIST != tmp_ret) {  // OB_ENTRY_NOT_EXIST: don't have task in the same type_id, not a error
          COMMON_LOG(WARN, "failed to try_switch_task", K(tmp_ret), K(*task));
          ret = tmp_ret;
        }
      }
    }
    if (OB_FAIL(dag->finish_task(*task))) {  // decrese all children's indegree
      COMMON_LOG(WARN, "failed to finish task", K(ret), K(task), K(*task));
    } else {
      COMMON_LOG(INFO, "finish task success", K(ret), K(task), K(*task));
    }
    if (dag->has_finished()) {          // dag is finished
      free_flag = true;                 // need to free dag
      if (OB_FAIL(remove_dag_(dag))) {  // delete from container
        COMMON_LOG(WARN, "remove dag failed", K(ret), K(dag));
      }
      COMMON_LOG(INFO, "dag is finished", K(ret), K(*dag));
    }
  }  // end of write lock
  if (free_flag) {
    if (OB_SUCCESS != (tmp_ret = ObSysTaskStatMgr::get_instance().del_task(dag->get_dag_id()))) {
      STORAGE_LOG(WARN, "failed to del sys task", K(tmp_ret), K(dag->get_dag_id()));
    }
    allocator_.free((void*)dag);
    COMMON_LOG(INFO, "free dag success", K(ret), K(dag), K(*dag));
  }
  return ret;
}

int ObTenantThreadPool::assign_worker_(ObITaskNew* task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "assign_worker task is NULL", K(ret), K(*task));
  } else {
    ObDagWorkerNew* worker = NULL;
    if (OB_FAIL(ObWorkerObjPool::get_instance().get_worker_obj(worker))) {
      COMMON_LOG(WARN, "assign_worker get worker obj failed", K(ret), K(*task));
    } else {
      task->set_worker(worker);
      worker->set_task(task);
      COMMON_LOG(INFO, "assign_worker success", K(ret), K(*task), K(worker));
    }
  }
  return ret;
}

int ObTenantThreadPool::return_worker(ObITaskNew* task)
{
  int ret = OB_SUCCESS;
  ObDagWorkerNew* worker = NULL;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "return_worker task is NULL", K(ret), K(*task));
  } else if (OB_ISNULL(worker = task->get_worker())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "return_worker get_worker_obj failed", K(ret), K(*task));
  } else {
    COMMON_LOG(INFO, "return_worker get_worker_obj success", K(ret), K(*task));
    task->set_worker(NULL);  // clear relation
    worker->reset();
    if (OB_FAIL(ObWorkerObjPool::get_instance().release_worker_obj(worker))) {
      COMMON_LOG(WARN, "return_worker release_worker_obj failed", K(ret), K(task));
    } else {
      COMMON_LOG(INFO, "return_worker release_worker_obj success", K(ret), K(task));
    }
  }
  return ret;
}

bool ObTenantThreadPool::is_empty()
{
  bool bret = true;
  {
    obsys::CRLockGuard guard(rwlock_);  // read lock for dag container
    for (int i = 0; i < type_info_list_.count(); ++i) {
      if (!ready_dag_list_.at(i)->is_empty()) {
        bret = false;
        break;
      }
    }
  }
  return bret;
}

bool ObTenantThreadPool::need_schedule(int64_t schedule_period)
{
  bool bret = false;
  if (!ready_dag_list_.empty() && running_tasks_.count() < max_thread_num_) {  // have free core
    bret = true;
  } else if (running_tasks_.count() > max_thread_num_) {  // decrese max_thread_num
    bret = true;
  } else if (ObTimeUtility::current_time() - last_schedule_time_ > schedule_period) {
    bret = true;
  }
  return bret;
}

int ObTenantThreadPool::pause_task_(ObITaskNew* task, ObITaskNew::SwitchTaskFlag flag)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task) || OB_ISNULL(task->get_worker())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "pause_worker task or task's worker is null", K(ret), K(task));
  } else {
    task->set_status(ObITaskNew::TASK_STATUS_RUNNABLE);
    if (OB_FAIL(remove_running_task_(task))) {  // remove this task from running_tasks_
      COMMON_LOG(WARN, "remove task from running task list failed", K(ret), K(task));
    } else if (ObITaskNew::THIS_TASK_FINISHED != flag && OB_FAIL(add_waiting_task_(task))) {
      COMMON_LOG(WARN, "add task into waiting task list failed", K(ret), K(task));
    } else {
      task->set_status(ObITaskNew::TASK_STATUS_RUNNABLE);
    }
  }
  return ret;
}

int ObTenantThreadPool::pause_task(ObITaskNew* task, const ObITaskNew::SwitchTaskFlag flag)
{
  int ret = OB_SUCCESS;
  obsys::CWLockGuard guard(rwlock_);  // write lock for dag container
  if (ObDagWorkerNew::SF_PAUSE != task->get_worker()->get_switch_flag()) {
    COMMON_LOG(WARN, "pause task worker switch flag changed", K(ret), K(task));
  } else if (OB_FAIL(pause_task_(task, flag))) {
    COMMON_LOG(WARN, "pause task failed", K(ret), K(task));
  }
  return ret;
}

int ObTenantThreadPool::switch_task(ObITaskNew* task, const ObITaskNew::SwitchTaskFlag flag)
{
  obsys::CWLockGuard guard(rwlock_);  // write lock for dag container
  return switch_task_(task, flag);
}

int ObTenantThreadPool::switch_task_(ObITaskNew* task, const ObITaskNew::SwitchTaskFlag flag)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task) || OB_ISNULL(task->get_worker()) || OB_ISNULL(task->get_next_task())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "switch_task task/task's worker/next_task is null", K(ret), K(task));
  } else if (ObDagWorkerNew::SF_SWITCH != task->get_worker()->get_switch_flag()) {
    COMMON_LOG(INFO, "switch_task worker switch flag changed", K(ret), K(task));
  } else {
    if (OB_FAIL(pause_task_(task, flag))) {  // pause task first
      COMMON_LOG(WARN, "pause task failed", K(ret), K(task));
    } else {
      if (ObITaskNew::THIS_TASK_FINISHED != flag) {  // switch not because of finish
        if (task->judge_delay_penalty()) {           // Determine if the switching time exceeds the threshold
          execute_penalty_(task->get_type_id());
          if (OB_NOT_NULL(task->get_next_task())) {  // recover penalty for next task
            recover_penalty_(task->get_next_task()->get_type_id());
          }
        }
      }
      ObITaskNew* next_task = task->get_next_task();  // have checked next_task above
      if (OB_ISNULL(next_task->get_worker())) {       // didn't have worker
        if (OB_FAIL(assign_worker_(next_task))) {     // assign worker for next_task_
          COMMON_LOG(WARN, "switch task with no worker and assign failed", K(ret), K(this), K(next_task));
        } else {
          COMMON_LOG(INFO, "assign worker for next_task_ success", K(ret), K(next_task), K(next_task->get_worker()));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(add_running_task_(next_task))) {  // add next_task_ into running_tasks_
          COMMON_LOG(WARN, "add running task failed", K(next_task), K(this));
        } else {
          COMMON_LOG(INFO, "add running task success", K(next_task), K(this));
          if (OB_ISNULL(next_task->get_dag())) {
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(WARN, "next_task don't have dag", K(next_task));
          } else {
            next_task->set_status(ObITaskNew::TASK_STATUS_RUNNING);
          }
          next_task->get_worker()->resume();                                      // wake up next_task_'s thread
          next_task->get_worker()->set_switch_flag(ObDagWorkerNew::SF_CONTINUE);  // set switch_flag_
          task->get_worker()->set_switch_flag(ObDagWorkerNew::SF_INIT);
          task->set_next_task(NULL);
          COMMON_LOG(INFO, "switch to next task success", K(ret), K(this), K(next_task));
        }
      }
    }
  }
  return ret;
}

// when task finish running, call this func to get a task of same type_id
int ObTenantThreadPool::try_switch_task_(ObITaskNew* task)
{
  int ret = OB_SUCCESS;
  ObITaskNew* next_task = NULL;
  // get a ready task to switch
  if (OB_FAIL(get_ready_task_(task->get_type_id(), ObIDagNew::ONLY_READY, next_task))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "failed to try_switch_task", K(ret), K(*this), K(*task), K(task->get_type_id()));
    } else {
      COMMON_LOG(INFO, "try_switch_task don't have runnable task", K(ret), K(*this), K(*task));
    }
    if (OB_FAIL(remove_running_task_(task))) {  // no next_task, just remove from running_tasks_
      COMMON_LOG(WARN, "remove task from runnint task list failed", K(ret), K(task));
    }
  } else {  // get next_task_ to run
    task->set_next_task(next_task);
    if (OB_ISNULL(task->get_worker())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "try switch task don't have worker", K(this), K(task), K(*task));
    } else {
      task->get_worker()->set_switch_flag(ObDagWorkerNew::SF_SWITCH);
      COMMON_LOG(INFO, "try switch task get a ready task", K(ret), K(*this), K(task), K(next_task));
      // call switch_task() to switch run permission to next_task_
      if (OB_FAIL(switch_task_(task, ObITaskNew::THIS_TASK_FINISHED))) {
        COMMON_LOG(WARN, "failed to switch_task", K(ret), K(*this), K(task), K(next_task));
        if (OB_ISNULL(next_task->get_dag())) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "next_task don't have dag", K(next_task));
        }
        next_task->set_status(ObITaskNew::TASK_STATUS_RUNNABLE);
        task->set_next_task(NULL);
        task->get_worker()->set_switch_flag(ObDagWorkerNew::SF_INIT);
      }
    }
  }
  return ret;
}

/*
int32_t ObTenantThreadPool::get_running_task_cnt(const ObIDagNew::ObIDagNewPriority priority)
{
  int32_t count = -1;
  if (priority >= 0 && priority < ObIDagNew::ObIDagNew::DAG_PRIO_MAX) {
    ObThreadCondGuard guard(scheduler_sync_);
    count = running_task_cnts_[priority];
  } else {
    COMMON_LOG(ERROR, "invalid priority", K(priority));
  }
  return count;
}
*/
}  // namespace share
}  // namespace oceanbase
