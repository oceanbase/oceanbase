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

#define USING_LOG_PREFIX RS

#include "ob_disaster_recovery_task_mgr.h"

#include "lib/lock/ob_mutex.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "share/ob_debug_sync.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "ob_disaster_recovery_task_executor.h"
#include "rootserver/ob_root_balancer.h"
#include "ob_rs_event_history_table_operator.h"
#include "share/ob_rpc_struct.h"
#include "observer/ob_server_struct.h"
#include "sql/executor/ob_executor_rpc_proxy.h"
#include "rootserver/ob_disaster_recovery_task.h"    // for ObDRTaskType
#include "share/ob_share_util.h"                     // for ObShareUtil
#include "lib/lock/ob_tc_rwlock.h"                   // for common::RWLock
#include "rootserver/ob_disaster_recovery_task.h"
#include "rootserver/tenant_snapshot/ob_tenant_snapshot_util.h" // for ObTenantSnapshotUtil
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_all_server_tracer.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h" // for ObInnerConnectionLockUtil
#include "observer/ob_inner_sql_connection.h"

namespace oceanbase
{
using namespace common;
using namespace lib;
using namespace obrpc;
using namespace transaction::tablelock;
using namespace share;

namespace rootserver
{
ObDRTaskQueue::ObDRTaskQueue() : inited_(false),
                                 config_(nullptr),
                                 task_alloc_(),
                                 wait_list_(),
                                 schedule_list_(),
                                 task_map_(),
                                 rpc_proxy_(nullptr),
                                 priority_(ObDRTaskPriority::MAX_PRI)
{
}

//TODO@jingyu.cr: need to make clear resue() and reset()
ObDRTaskQueue::~ObDRTaskQueue()
{
  reuse();
}

void ObDRTaskQueue::reuse()
{
  while (!wait_list_.is_empty()) {
    ObDRTask *t = wait_list_.remove_first();
    remove_task_from_map_and_free_it_(t);
  }
  while (!schedule_list_.is_empty()) {
    ObDRTask *t = schedule_list_.remove_first();
    remove_task_from_map_and_free_it_(t);
  }
  task_map_.clear();
}

void ObDRTaskQueue::reset()
{
  wait_list_.reset();
  schedule_list_.reset();
  task_map_.clear();
}

void ObDRTaskQueue::free_task_(ObDRTask *&task)
{
  if (OB_NOT_NULL(task)) {
    task->~ObDRTask();
    task_alloc_.free(task);
    task = nullptr;
  }
}

void ObDRTaskQueue::remove_task_from_map_and_free_it_(ObDRTask *&task)
{
  if (OB_NOT_NULL(task)) {
    task_map_.erase_refactored(task->get_task_key());
    free_task_(task);
  }
}

int ObDRTaskQueue::init(
    common::ObServerConfig &config,
    const int64_t bucket_num,
    obrpc::ObSrvRpcProxy *rpc_proxy,
    ObDRTaskPriority priority)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(bucket_num <= 0)
             || OB_ISNULL(rpc_proxy)
             || (ObDRTaskPriority::LOW_PRI != priority && ObDRTaskPriority::HIGH_PRI != priority)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bucket_num), KP(rpc_proxy), K(priority));
  } else if (OB_FAIL(task_map_.create(bucket_num, "DRTaskMap"))) {
    LOG_WARN("fail to create task map", KR(ret), K(bucket_num));
  } else if (OB_FAIL(task_alloc_.init(
          ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE,
          ObMemAttr(common::OB_SERVER_TENANT_ID, "DRTaskAlloc")))) {
    LOG_WARN("fail to init task allocator", KR(ret));
  } else {
    config_ = &config;
    rpc_proxy_ = rpc_proxy;
    priority_ = priority;
    inited_ = true;
  }
  return ret;
}

int ObDRTaskQueue::check_task_in_scheduling(
    const ObDRTaskKey &task_key,
    bool &task_in_scheduling) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!task_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_key));
  } else {
    ObDRTask *task = nullptr;
    int tmp_ret = task_map_.get_refactored(task_key, task);
    if (OB_SUCCESS == tmp_ret) {
      if (OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("a null task ptr getted from task_map", KR(ret), K(task_key));
      } else {
        task_in_scheduling = task->in_schedule();
      }
    } else if (OB_HASH_NOT_EXIST == tmp_ret) {
      // task not exist means task not executing
      task_in_scheduling = false;
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to get from map", KR(ret), K(task_key));
    }
  }
  return ret;
}

int ObDRTaskQueue::check_task_exist(
    const ObDRTaskKey &task_key,
    bool &task_exist)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!task_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_key));
  } else {
    ObDRTask *task = nullptr;
    int tmp_ret = task_map_.get_refactored(task_key, task);
    if (OB_SUCCESS == tmp_ret) {
      task_exist = true;
    } else if (OB_HASH_NOT_EXIST == tmp_ret) {
      task_exist = false;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get from task_map", KR(ret), K(task_key));
    }
  }
  return ret;
}

int ObDRTaskQueue::push_task_in_wait_list(
    ObDRTaskMgr &task_mgr,
    const ObDRTaskQueue &sibling_queue,
    const ObDRTask &task,
    bool &has_task_in_schedule)
{
  int ret = OB_SUCCESS;
  has_task_in_schedule = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    const ObDRTaskKey &task_key = task.get_task_key();
    ObDRTask *task_ptr = nullptr;
    int tmp_ret = task_map_.get_refactored(task_key, task_ptr);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      if (OB_FAIL(do_push_task_in_wait_list_(task_mgr, sibling_queue, task, has_task_in_schedule))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    } else if (OB_SUCCESS == tmp_ret) {
      ret = OB_ENTRY_EXIST;
      LOG_INFO("disaster recovery task exist", KR(ret), K(task_key), K(task), KP(this));
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to check task exist", KR(ret), K(task_key));
    }
  }
  return ret;
}

int ObDRTaskQueue::push_task_in_schedule_list(
    ObDRTask &task)
{
  // STEP 1: push task into schedule list
  // STEP 2: push task into task_map
  // STEP 3: set task in schedule
  int ret = OB_SUCCESS;
  void *raw_ptr = nullptr;
  ObDRTask *new_task = nullptr;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(raw_ptr = task_alloc_.alloc(task.get_clone_size()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate task", KR(ret));
  } else if (OB_FAIL(task.clone(raw_ptr, new_task))) {
    LOG_WARN("fail to clone task", KR(ret), K(task));
  } else if (OB_ISNULL(new_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_task is nullptr", KR(ret));
  } else if (OB_FAIL(task_map_.set_refactored(new_task->get_task_key(), new_task))) {
    LOG_WARN("fail to set map", KR(ret), "task_key", new_task->get_task_key());
  } else {
    // set schedule_time for this task
    new_task->set_schedule();
    if (!schedule_list_.add_last(new_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to add task to schedule list", KR(ret), KPC(new_task));
    } else {
      FLOG_INFO("[DRTASK_NOTICE] finish add task into schedule list", KPC(new_task));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(new_task)) {
      remove_task_from_map_and_free_it_(new_task);
    } else if (OB_NOT_NULL(raw_ptr)) {
      task_alloc_.free(raw_ptr);
      raw_ptr = nullptr;
    }
  }
  return ret;
}

int ObDRTaskQueue::do_push_task_in_wait_list_(
    ObDRTaskMgr &task_mgr,
    const ObDRTaskQueue &sibling_queue,
    const ObDRTask &task,
    bool &has_task_in_schedule)
{
  int ret = OB_SUCCESS;
  void *raw_ptr = nullptr;
  ObDRTask *new_task = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config_ ptr is null", KR(ret), KP(config_));
  } else if (OB_ISNULL(raw_ptr = task_alloc_.alloc(
            task.get_clone_size()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc task", KR(ret), "size", task.get_clone_size());
  } else if (OB_FAIL(task.clone(raw_ptr, new_task))) {
    LOG_WARN("fail to clone task", KR(ret), K(task));
  } else if (OB_ISNULL(new_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new task ptr is null", KR(ret));
  } else if (!wait_list_.add_last(new_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to add new task to wait list", KR(ret), "task", *new_task);
  } else {
    has_task_in_schedule = false;
    bool sibling_in_schedule = false;
    if (OB_FAIL(sibling_queue.check_task_in_scheduling(
            new_task->get_task_key(), sibling_in_schedule))) {
      LOG_WARN("fail to check has in schedule task", KR(ret),
               "task_key", new_task->get_task_key());
    } else if (OB_FAIL(task_map_.set_refactored(
            new_task->get_task_key(), new_task))) {
      LOG_WARN("fail to set map", KR(ret), "task_key", new_task->get_task_key());
    } else if (task_mgr.get_reach_concurrency_limit() && !sibling_in_schedule) {
      task_mgr.clear_reach_concurrency_limit();
    }
    if (OB_SUCC(ret)) {
      has_task_in_schedule = sibling_in_schedule;
      if (OB_FAIL(set_sibling_in_schedule(new_task->get_task_key(), sibling_in_schedule))) {
        LOG_WARN("fail to set sibling in schedule", KR(ret),
                 "task_key", new_task->get_task_key(), K(sibling_in_schedule));
      }
    }

    if (OB_FAIL(ret)) {
      wait_list_.remove(new_task);
    } else {
      LOG_INFO("success to push a task in waiting list", K(task), K(has_task_in_schedule));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(new_task)) {
      remove_task_from_map_and_free_it_(new_task);
    } else if (OB_NOT_NULL(raw_ptr)) {
      task_alloc_.free(raw_ptr);
      raw_ptr = nullptr;
    }
  }
  return ret;
}

int ObDRTaskQueue::pop_task(
    ObDRTask *&task)
{
  int ret = OB_SUCCESS;
  task = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not init", KR(ret));
  } else if (OB_ISNULL(config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config_ ptr is null", KR(ret), KP(config_));
  } else {
    DLIST_FOREACH(t, wait_list_) {
      if (t->is_sibling_in_schedule()) {
        // task can not pop
        LOG_INFO("can not pop this task because a sibling task already in schedule", KPC(t));
      } else {
        task = t;
        break;
      }
    }
    if (OB_NOT_NULL(task)) {
      // when task not empty, we move it from wait to schedule list,
      LOG_INFO("a task from queue to pop found", KPC(task));
      wait_list_.remove(task);
      if (!schedule_list_.add_last(task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to add task to schedule list", KR(ret));
      } else {
        task->set_schedule();
        LOG_INFO("success to set task in schedule normally", KPC(task));
      }
      // if fail to add to schedule list, clean it directly
      if (OB_FAIL(ret)) {
        remove_task_from_map_and_free_it_(task);
      }
    }
  }
  return ret;
}

int ObDRTaskQueue::get_task(
    const share::ObTaskId &task_id,
    const ObDRTaskKey &task_key,
    ObDRTask *&task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObDRTask *my_task = nullptr;
    int tmp_ret = task_map_.get_refactored(task_key, my_task);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      task = nullptr;
    } else if (OB_SUCCESS == tmp_ret) {
      if (OB_ISNULL(my_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("my_task ptr is null", KR(ret), KP(my_task));
      } else if (my_task->get_task_id() == task_id) {
        task = my_task;
      } else {
        task = nullptr;
      }
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to get task from map", KR(ret), K(task_key), K(task_id));
    }
  }
  return ret;
}

int ObDRTaskQueue::check_task_need_cleaning_(
    const ObDRTask &task,
    bool &need_cleanning,
    ObDRTaskRetComment &ret_comment)
{
  int ret = OB_SUCCESS;
  // do not clean this task by default
  // need_cleanning = true under these cases
  //   (1) server not exist
  //   (2) server is permanant offline
  //   (3) rpc ls_check_dr_task_exist successfully told us task not exist
  //   (4) task is timeout while any failure during whole procedure
  need_cleanning = false;
  Bool task_exist = false;
  const ObAddr &dst_server = task.get_dst_server();
  share::ObServerInfoInTable server_info;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not init", KR(ret));
  } else if (OB_ISNULL(rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("some ptr is null", KR(ret), KP(rpc_proxy_));
  } else if (OB_FAIL(SVR_TRACER.get_server_info(dst_server, server_info))) {
    LOG_WARN("fail to get server_info", KR(ret), "server", dst_server);
     // case 1. server not exist
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      FLOG_INFO("the reason to clean this task: server not exist", K(task));
      need_cleanning = true;
      ret_comment = ObDRTaskRetComment::CLEAN_TASK_DUE_TO_SERVER_NOT_EXIST;
    }
  } else if (server_info.is_permanent_offline()) {
    // case 2. server is permanant offline
    FLOG_INFO("the reason to clean this task: server permanent offline", K(task), K(server_info));
    need_cleanning = true;
    ret_comment = ObDRTaskRetComment::CLEAN_TASK_DUE_TO_SERVER_PERMANENT_OFFLINE;
  } else if (server_info.is_alive()) {
    ObDRTaskExistArg arg;
    arg.task_id_ = task.get_task_id();
    arg.tenant_id_ = task.get_tenant_id();
    arg.ls_id_ = task.get_ls_id();
    if (OB_FAIL(rpc_proxy_->to(task.get_dst_server()).by(task.get_tenant_id())
                .ls_check_dr_task_exist(arg, task_exist))) {
      LOG_WARN("fail to check task exist", KR(ret), "tenant_id", task.get_tenant_id(),
               "task_id", task.get_task_id(), "dst", task.get_dst_server());
    } else if (!task_exist) {
      // case 3. rpc ls_check_dr_task_exist successfully told us task not exist
      FLOG_INFO("the reason to clean this task: task not running", K(task));
      need_cleanning = true;
      ret_comment = ObDRTaskRetComment::CLEAN_TASK_DUE_TO_TASK_NOT_RUNNING;
    }
  } else if (server_info.is_temporary_offline()) {
    ret = OB_SERVER_NOT_ALIVE;
    LOG_WARN("server status is not alive, task may be cleanned later", KR(ret), "server", task.get_dst_server(), K(server_info), K(task));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected server status", KR(ret), "server", task.get_dst_server(), K(server_info), K(task));
  }

  // case 4. task is timeout while any OB_FAIL occurs
  if (OB_FAIL(ret) && task.is_already_timeout()) {
    FLOG_INFO("the reason to clean this task: task is timeout", KR(ret), K(task));
    ret = OB_SUCCESS;
    need_cleanning = true;
    ret_comment = ObDRTaskRetComment::CLEAN_TASK_DUE_TO_TASK_TIMEOUT;
  }
  return ret;
}

int ObDRTaskQueue::handle_not_in_progress_task(
    ObDRTaskMgr &task_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not init", KR(ret));
  } else {
    const int ret_code = OB_LS_REPLICA_TASK_RESULT_UNCERTAIN;
    ObDRTaskRetComment ret_comment = ObDRTaskRetComment::MAX;
    bool need_cleaning = false;
    DLIST_FOREACH(t, schedule_list_) {
      int tmp_ret = OB_SUCCESS;
      need_cleaning = false;
      DEBUG_SYNC(BEFORE_CHECK_CLEAN_DRTASK);
      if (OB_SUCCESS != (tmp_ret = check_task_need_cleaning_(*t, need_cleaning, ret_comment))) {
        LOG_WARN("fail to check this task exist for cleaning", KR(tmp_ret), KPC(t));
      } else if (need_cleaning
                 && OB_SUCCESS != (tmp_ret = task_mgr.async_add_cleaning_task_to_updater(
                                         t->get_task_id(),
                                         t->get_task_key(),
                                         ret_code,
                                         true,/*need_record_event*/
                                         ret_comment))) {
        LOG_WARN("do execute over failed", KR(tmp_ret), KPC(t), K(ret_comment));
      }
    }
  }
  return ret;
}

int ObDRTaskQueue::finish_schedule(
    ObDRTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not init", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(task));
  } else if (OB_UNLIKELY(!task->in_schedule())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task state not match", KR(ret), KPC(task));
  } else {
    // remove from schedule_list_
    schedule_list_.remove(task);
    FLOG_INFO("[DRTASK_NOTICE] success to finish schedule task", KR(ret), KPC(task));
    remove_task_from_map_and_free_it_(task);
  }
  return ret;
}

int ObDRTaskQueue::set_sibling_in_schedule(
    const ObDRTask &task,
    const bool in_schedule)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    const ObDRTaskKey &task_key = task.get_task_key();
    ObDRTask *my_task = nullptr;
    int tmp_ret = task_map_.get_refactored(task_key, my_task);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      // bypass
    } else if (OB_SUCCESS == tmp_ret) {
      if (OB_ISNULL(my_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("my_task ptr is null", KR(ret), K(task));
      } else {
        my_task->set_sibling_in_schedule(in_schedule);
      }
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to get task from map", KR(ret), K(task_key));
    }
  }
  return ret;
}

int ObDRTaskQueue::set_sibling_in_schedule(
    const ObDRTaskKey &task_key,
    const bool in_schedule)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!task_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_key));
  } else {
    ObDRTask *my_task = nullptr;
    int tmp_ret = task_map_.get_refactored(task_key, my_task);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      // bypass
    } else if (OB_SUCCESS == tmp_ret) {
      if (OB_ISNULL(my_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("my_task ptr is null", KR(ret), K(task_key), KP(my_task));
      } else {
        my_task->set_sibling_in_schedule(in_schedule);
      }
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to get task from map", KR(ret), K(task_key));
    }
  }
  return ret;
}

int ObDRTaskQueue::dump_statistic() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    DLIST_FOREACH(t, schedule_list_) {
      FLOG_INFO("[DRTASK_NOTICE] tasks in schedule list", "priority", get_priority_str(), "task_key", t->get_task_key(),
                "task_id", t->get_task_id(), "task_type", t->get_disaster_recovery_task_type());
    }
    DLIST_FOREACH(t, wait_list_) {
      FLOG_INFO("[DRTASK_NOTICE] tasks in wait list", "priority", get_priority_str(), "task_key", t->get_task_key(),
                "task_id", t->get_task_id(), "task_type", t->get_disaster_recovery_task_type());
    }
  }
  return ret;
}

int ObDRTaskMgr::init(
    const common::ObAddr &server,
    common::ObServerConfig &config,
    ObDRTaskExecutor &task_executor,
    obrpc::ObSrvRpcProxy *rpc_proxy,
    common::ObMySQLProxy *sql_proxy,
    share::schema::ObMultiVersionSchemaService *schema_service)
{
  int ret = OB_SUCCESS;
  static const int64_t thread_count = 1;
  if (OB_UNLIKELY(inited_ || !stopped_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(inited_), K_(stopped));
  } else if (OB_UNLIKELY(!server.is_valid())
          || OB_ISNULL(rpc_proxy)
          || OB_ISNULL(sql_proxy)
          || OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), KP(rpc_proxy),
             KP(sql_proxy), KP(schema_service));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::REBALANCE_TASK_MGR_COND_WAIT))) {
    LOG_WARN("fail to init cond", KR(ret));
  } else if (OB_FAIL(create(thread_count, "DRTaskMgr"))) {
    LOG_WARN("fail to create disaster recovery task mgr", KR(ret));
  } else {
    config_ = &config;
    self_ = server;
    task_executor_ = &task_executor;
    rpc_proxy_ = rpc_proxy;
    sql_proxy_ = sql_proxy;
    schema_service_ = schema_service;
    if (OB_FAIL(high_task_queue_.init(
            config, TASK_QUEUE_LIMIT, rpc_proxy_, ObDRTaskPriority::HIGH_PRI))) {
      LOG_WARN("fail to init high priority task queue", KR(ret));
    } else if (OB_FAIL(low_task_queue_.init(
            config, TASK_QUEUE_LIMIT, rpc_proxy_, ObDRTaskPriority::LOW_PRI))) {
      LOG_WARN("fail to init low priority task queue", KR(ret));
    } else if (OB_FAIL(disaster_recovery_task_table_updater_.init(sql_proxy, this))) {
      LOG_WARN("fail to init a ObDRTaskTableUpdater", KR(ret));
    } else {
      inited_ = true;
    }
  }
  return ret;
}

int ObDRTaskMgr::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task mgr not inited", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!stopped_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not start ObDRTaskMgr twice", KR(ret), K_(stopped));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(ObRsReentrantThread::start())) {
    LOG_WARN("fail to start ObRsReentrantThread", KR(ret));
  } else if (OB_FAIL(disaster_recovery_task_table_updater_.start())) {
    LOG_WARN("fail to start disaster_recovery_task_table_updater", KR(ret));
  } else {
    stopped_ = false;
    FLOG_INFO("success to start ObDRTaskMgr");
  }
  return ret;
}

void ObDRTaskMgr::stop()
{
  loaded_ = false;
  stopped_ = true;
  ObRsReentrantThread::stop();
  disaster_recovery_task_table_updater_.stop();
  ObThreadCondGuard guard(cond_);
  cond_.broadcast();
  for (int64_t i = 0; i < static_cast<int64_t>(ObDRTaskPriority::MAX_PRI); ++i) {
    queues_[i].reuse();
  }
  FLOG_INFO("success to stop ObDRTaskMgr");
}

void ObDRTaskMgr::wait()
{
  ObRsReentrantThread::wait();
  disaster_recovery_task_table_updater_.wait();
}

int ObDRTaskMgr::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || stopped_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDRTaskMgr is not inited or is stopped or not loaded", KR(ret), K_(inited), K_(stopped), K_(loaded));
  }
  return ret;
}

void ObDRTaskMgr::run3()
{
  FLOG_INFO("Disaster recovery task mgr start");
  if (OB_UNLIKELY(!inited_ || stopped_)) {
    LOG_WARN_RET(OB_NOT_INIT, "ObDRTaskMgr not init", K(inited_), K_(stopped));
  } else {
    int64_t last_dump_ts = ObTimeUtility::current_time();
    int64_t last_check_task_in_progress_ts = ObTimeUtility::current_time();
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    while (!stop_) {
      // thread detect
      if (!loaded_ && OB_FAIL(load_task_to_schedule_list_())) {
        LOG_WARN("fail to load task infos into schedule list, will retry until success", KR(ret));
      } else {
        update_last_run_timestamp();

        common::ObArenaAllocator allocator;
        ObDRTask *task = nullptr;
        if (OB_FAIL(try_pop_task(allocator, task))) {
          LOG_WARN("fail to try pop task", KR(ret));
        } else if (OB_NOT_NULL(task)) {
          const ObAddr &dst_server = task->get_dst_server();
          share::ObServerInfoInTable server_info;
          if (OB_FAIL(SVR_TRACER.get_server_info(dst_server, server_info))) {
            LOG_WARN("fail to get server_info", KR(ret), K(dst_server));
          } else if (server_info.is_permanent_offline()) {
            // dest server permanent offline, do not execute this task, just clean it
            LOG_INFO("[DRTASK_NOTICE] dest server is permanent offline, task can not execute", K(dst_server), K(server_info));
            ObThreadCondGuard guard(cond_);
            if (OB_SUCCESS != (tmp_ret = async_add_cleaning_task_to_updater(
                                  task->get_task_id(),
                                  task->get_task_key(),
                                  OB_REBALANCE_TASK_CANT_EXEC,
                                  false/*need_record_event*/,
                                  ObDRTaskRetComment::CANNOT_EXECUTE_DUE_TO_SERVER_PERMANENT_OFFLINE,
                                  false/*reach_data_copy_concurrency*/))) {
              LOG_WARN("fail to do execute over", KR(tmp_ret), KPC(task));
            }
          } else {
            if (OB_SUCCESS != (tmp_ret = task->log_execute_start())) {
              LOG_WARN("fail to log task start", KR(tmp_ret), KPC(task));
            }
            if (OB_FAIL(execute_task(*task))) {
              LOG_WARN("fail to send", KR(ret), KPC(task));
            }
          }
          free_task_(allocator, task);
        } else {
          LOG_TRACE("task is nullptr after try_pop_task");
        }
        if (OB_SUCCESS != (tmp_ret = try_dump_statistic_(
              last_dump_ts))) {
          LOG_WARN("fail to try dump statistic", KR(tmp_ret), K(last_dump_ts));
        }
        if (OB_SUCCESS != (tmp_ret = try_clean_not_in_schedule_task_in_schedule_list_(
              last_check_task_in_progress_ts))) {
           LOG_WARN("fail to try check task in progress", KR(tmp_ret), K(last_check_task_in_progress_ts));
        }
      }
    }
  }
  FLOG_INFO("disaster task mgr exits");
}

int ObDRTaskMgr::check_task_in_executing(
    const ObDRTaskKey &task_key,
    const ObDRTaskPriority priority,
    bool &task_in_executing)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else if (ObDRTaskPriority::HIGH_PRI != priority
             && ObDRTaskPriority::LOW_PRI != priority) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(priority));
  } else {
    ObThreadCondGuard guard(cond_);
    ObDRTaskQueue &queue = ObDRTaskPriority::LOW_PRI == priority
                           ? low_task_queue_
                           : high_task_queue_;
    if (OB_FAIL(queue.check_task_in_scheduling(task_key, task_in_executing))) {
      LOG_WARN("fail to check task exist", KR(ret), K(task_key));
    }
  }
  return ret;
}

int ObDRTaskMgr::check_task_exist(
    const ObDRTaskKey &task_key,
    const ObDRTaskPriority priority,
    bool &task_exist)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else if (ObDRTaskPriority::HIGH_PRI != priority
             && ObDRTaskPriority::LOW_PRI != priority) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(priority));
  } else {
    ObThreadCondGuard guard(cond_);
    ObDRTaskQueue &queue = ObDRTaskPriority::LOW_PRI == priority
                           ? low_task_queue_
                           : high_task_queue_;
    if (OB_FAIL(queue.check_task_exist(task_key, task_exist))) {
      LOG_WARN("fail to check task exist", KR(ret), K(task_key));
    }
  }
  return ret;
}

int ObDRTaskMgr::add_task(
    const ObDRTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(loaded), K_(stopped));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dr task", KR(ret), K(task));
  } else {
    ObThreadCondGuard guard(cond_);
    ObDRTaskQueue &queue = task.is_high_priority_task()
                           ? high_task_queue_
                           : low_task_queue_;
    ObDRTaskQueue &sibling_queue = task.is_high_priority_task()
                                   ? low_task_queue_
                                   : high_task_queue_;
    bool has_task_in_schedule = false;
    if (OB_UNLIKELY(queue.task_cnt() >= TASK_QUEUE_LIMIT)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("disaster recovery task queue is full", KR(ret), "task_cnt", queue.task_cnt());
    } else if (OB_FAIL(queue.push_task_in_wait_list(*this, sibling_queue, task, has_task_in_schedule))) {
      if (OB_ENTRY_EXIST != ret) {
        LOG_WARN("fail to push task", KR(ret), K(task));
      } else {
        ret = OB_SUCCESS;
        LOG_INFO("task already exist in queue", K(task));
      }
    } else {
      int64_t wait_cnt = 0;
      int64_t schedule_cnt = 0;
      if (OB_FAIL(inner_get_task_cnt_(wait_cnt, schedule_cnt))) {
        LOG_WARN("fail to get task cnt", KR(ret));
      } else if (!has_task_in_schedule
                 && 0 == get_reach_concurrency_limit()) {
        cond_.broadcast();
        LOG_INFO("success to broad cast cond_", K(wait_cnt), K(schedule_cnt));
      }
      clear_reach_concurrency_limit();
      LOG_INFO("[DRTASK_NOTICE] add task to disaster recovery task mgr finish", KR(ret), K(task));
    }
  }
  return ret;
}

int ObDRTaskMgr::deal_with_task_reply(
    const ObDRTaskReplyResult &reply)
{
  int ret = OB_SUCCESS;
  ObDRTaskKey task_key;
  if (OB_FAIL(check_inner_stat_())) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(loaded), K_(stopped));
  } else if (OB_FAIL(task_key.init(
          reply.tenant_id_,
          reply.ls_id_.id(),
          0, /* set to 0 */
          0, /* set to 0 */
          ObDRTaskKeyType::FORMAL_DR_KEY))) {
    LOG_WARN("fail to init task key", KR(ret), K(reply));
  } else {
    int tmp_ret = OB_SUCCESS;
    ObDRTask *task = nullptr;
    ObThreadCondGuard guard(cond_);
    if (OB_SUCCESS != (tmp_ret = get_task_by_id_(reply.task_id_, task_key, task))) {
      if (OB_ENTRY_NOT_EXIST == tmp_ret) {
        // task not exist, try record this reply result
        ROOTSERVICE_EVENT_ADD("disaster_recovery", "finish_disaster_recovery_task",
                            "tenant_id", reply.tenant_id_,
                            "ls_id", reply.ls_id_.id(),
                            "task_id", reply.task_id_,
                            "execute_result", reply.result_,
                            "ret_comment", ob_disaster_recovery_task_ret_comment_strs(ObDRTaskRetComment::RECEIVE_FROM_STORAGE_RPC));
      } else {
        LOG_WARN("fail to get task from task manager", KR(tmp_ret), K(reply), K(task_key));
      }
    } else if (OB_SUCCESS != (tmp_ret = task->log_execute_result(reply.result_, ObDRTaskRetComment::RECEIVE_FROM_STORAGE_RPC))){
      LOG_WARN("fail to log execute result", KR(tmp_ret), K(reply));
    }

    if (OB_FAIL(async_add_cleaning_task_to_updater(
                    reply.task_id_,
                    task_key,
                    reply.result_,
                    false,/*need_record_event*/
                    ObDRTaskRetComment::RECEIVE_FROM_STORAGE_RPC,
                    true/*need_clear_server_data_in_limit*/))) {
      LOG_WARN("fail to do execute over", KR(ret), K(reply));
    }
  }
  return ret;
}

int ObDRTaskMgr::async_add_cleaning_task_to_updater(
    const share::ObTaskId &task_id,
    const ObDRTaskKey &task_key,
    const int ret_code,
    const bool need_record_event,
    const ObDRTaskRetComment &ret_comment,
    const bool need_clear_server_data_in_limit)
{
  int ret = OB_SUCCESS;
  ObDRTask *task = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else if (OB_FAIL(get_task_by_id_(task_id, task_key, task))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("fail to get task, task may be cleaned earlier", KR(ret), K(task_id), K(task_key));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get task from task manager", KR(ret), K(task_id), K(task_key));
    }
  }
  if (OB_SUCC(ret)
      && OB_NOT_NULL(task)
      && OB_FAIL(disaster_recovery_task_table_updater_.async_update(
                      task->get_tenant_id(),
                      task->get_ls_id(),
                      task->get_disaster_recovery_task_type(),
                      task_key,
                      ret_code,
                      need_clear_server_data_in_limit,
                      task_id,
                      need_record_event,
                      ret_comment))) {
    LOG_WARN("fail to async update a dr task", KR(ret), "tenant_id", task->get_tenant_id(),
             "ls_id", task->get_ls_id(), K(task_id), K(need_record_event), K(ret_comment));
  }
  return ret;
}

int ObDRTaskMgr::do_cleaning(
    const share::ObTaskId &task_id,
    const ObDRTaskKey &task_key,
    const int ret_code,
    const bool need_clear_server_data_in_limit,
    const bool need_record_event,
    const ObDRTaskRetComment &ret_comment)
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(cond_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else {
    ObDRTaskQueue *task_queue = nullptr;
    ObDRTask *task = nullptr;
    common::ObAddr dst_server;
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(queues_); ++i) {
      if (OB_FAIL(queues_[i].get_task(task_id, task_key, task))) {
        LOG_WARN("fail to get schedule task from queue", KR(ret), "priority", queues_[i].get_priority_str());
      } else if (OB_NOT_NULL(task)) {
        task_queue = &queues_[i];
        break;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(task)) {
        LOG_INFO("in schedule taks not found, maybe not sync because of network traffic",
                 K(task_id), K(task_key), K(ret_code));
      } else {
        if (need_record_event) {
          (void)log_task_result(*task, ret_code, ret_comment);
        }
        dst_server = task->get_dst_server();
        if (OB_FAIL(set_sibling_in_schedule(*task, false/* not in schedule*/))) {
          LOG_WARN("fail to set sibling in schedule", KR(ret), KPC(task));
        } else if (OB_ISNULL(task_queue)) {
          LOG_INFO("task_queue is null"); // by pass
        } else if (OB_FAIL(task_queue->finish_schedule(task))) {
          LOG_WARN("fail to finish scheduling task", KR(ret), KPC(task));
        }
      }
      clear_reach_concurrency_limit();
    }
  }
  return ret;
}

int ObDRTaskMgr::get_all_task_count(
    int64_t &high_wait_cnt,
    int64_t &high_schedule_cnt,
    int64_t &low_wait_cnt,
    int64_t &low_schedule_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(loaded), K_(stopped));
  } else {
    ObThreadCondGuard guard(cond_);
    high_wait_cnt = get_high_priority_queue_().get_wait_list().get_size();
    high_schedule_cnt = get_high_priority_queue_().get_schedule_list().get_size();
    low_wait_cnt = get_low_priority_queue_().get_wait_list().get_size();
    low_schedule_cnt = get_low_priority_queue_().get_schedule_list().get_size();
  }
  return ret;
}

int ObDRTaskMgr::log_task_result(
    const ObDRTask &task,
    const int ret_code,
    const ObDRTaskRetComment &ret_comment)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task.log_execute_result(ret_code, ret_comment))) {
    LOG_WARN("fail to log execute task", KR(ret), K(task), KR(ret_code), K(ret_comment));
  }
  return ret;
}

int ObDRTaskMgr::get_task_by_id_(
    const share::ObTaskId &task_id,
    const ObDRTaskKey &task_key,
    ObDRTask *&task)
{
  int ret = OB_SUCCESS;
  ObDRTask *task_to_get = nullptr;
  void *raw_ptr = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(queues_); ++i) {
    if (OB_FAIL(queues_[i].get_task(task_id, task_key, task_to_get))) {
      LOG_WARN("fail to get schedule task from queue", KR(ret), "priority", queues_[i].get_priority_str());
    } else if (OB_NOT_NULL(task_to_get)) {
      break;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(task_to_get)) {
    task = nullptr;
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("task not exist, maybe cleaned earier", KR(ret), K(task_id), K(task_key));
  } else {
    task = task_to_get;
  }
  return ret;
}

void ObDRTaskMgr::free_task_(
     common::ObIAllocator &allocator,
     ObDRTask *&task)
{
  if (OB_NOT_NULL(task)) {
    task->~ObDRTask();
    allocator.free(task);
    task = nullptr;
  }
}

int ObDRTaskMgr::load_task_to_schedule_list_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObThreadCondGuard guard(cond_);
  ObArray<uint64_t> tenant_id_array;

  if (OB_UNLIKELY(!inited_ || stopped_)) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped));
  } else if (OB_ISNULL(schema_service_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_service_ or sql_proxy_ is nullptr", KR(ret), KP(schema_service_), KP(sql_proxy_));
  } else if (OB_UNLIKELY(ObTenantUtils::get_tenant_ids(schema_service_, tenant_id_array))) {
    LOG_WARN("fail to get tenant id array", KR(ret));
  } else {
    // clear schedule_list and wait_list in two queues
    for (int64_t i = 0; i < static_cast<int64_t>(ObDRTaskPriority::MAX_PRI); ++i) {
      queues_[i].reuse();
    }
    clear_reach_concurrency_limit();
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_id_array.count(); ++i) {
      // load this tenant's task info into schedule_list
      // TODO@jingyu.cr: need to isolate different tenant
      const uint64_t tenant_id = tenant_id_array.at(i);
      const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
      ObSqlString sql;
      ObTimeoutCtx ctx;
      SMART_VAR(ObISQLClient::ReadResult, result) {
        if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
          LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
        } else if (OB_FAIL(sql.append_fmt(
            "SELECT * FROM %s WHERE tenant_id = %ld",
            share::OB_ALL_LS_REPLICA_TASK_TNAME, tenant_id))) {
          LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(sql_tenant_id));
        } else if (OB_FAIL(sql_proxy_->read(result, sql_tenant_id, sql.ptr()))) {
          LOG_WARN("execute sql failed", KR(ret),
              K(tenant_id), K(sql_tenant_id), "sql", sql.ptr());
        } else if (OB_ISNULL(result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get mysql result failed", KR(ret), "sql", sql.ptr());
        } else if (OB_FAIL(load_single_tenant_task_infos_(*result.get_result()))) {
          LOG_WARN("load single tenant's task info failed", KR(ret), K(tenant_id), K(sql_tenant_id));
        } else {
          FLOG_INFO("success to load single tenant's task info", K(tenant_id));
        }
      }
    }
    if (OB_SUCC(ret)) {
      loaded_ = true;
    }
  }
  return ret;
}

int ObDRTaskMgr::load_single_tenant_task_infos_(
    sqlclient::ObMySQLResult &res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || stopped_)) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(res.next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next result failed", KR(ret));
        }
        break;
      } else if (OB_FAIL(load_task_info_(res))) {
        LOG_WARN("fail to build and load this task info", KR(ret));
      }
    }
  }
  return ret;
}

int ObDRTaskMgr::load_task_info_(
    sqlclient::ObMySQLResult &res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || stopped_)) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped));
  } else {
    common::ObString task_type;
    int64_t priority = 2;
    (void)GET_COL_IGNORE_NULL(res.get_varchar, "task_type", task_type);
    (void)GET_COL_IGNORE_NULL(res.get_int, "priority", priority);
    if (OB_FAIL(ret)) {
    } else if (task_type == common::ObString("MIGRATE REPLICA")) {
      SMART_VAR(ObMigrateLSReplicaTask, tmp_task) {
        if (OB_FAIL(tmp_task.build_task_from_sql_result(res))) {
          LOG_WARN("fail to build migrate task info from res", KR(ret));
        } else if (OB_FAIL(queues_[priority].push_task_in_schedule_list(tmp_task))) {
          LOG_WARN("fail to load a ObMigrateLSReplicaTask into schedule list", KR(ret));
        }
      }
    } else if (task_type == common::ObString("ADD REPLICA")) {
      SMART_VAR(ObAddLSReplicaTask, tmp_task) {
        if (OB_FAIL(tmp_task.build_task_from_sql_result(res))) {
          LOG_WARN("fail to build ObAddLSReplicaTask from res", KR(ret));
        } else if (OB_FAIL(queues_[priority].push_task_in_schedule_list(tmp_task))) {
          LOG_WARN("fail to load ObAddLSReplicaTask into schedule list", KR(ret));
        }
      }
    } else if (task_type == common::ObString("TYPE TRANSFORM")) {
      SMART_VAR(ObLSTypeTransformTask, tmp_task) {
        if (OB_FAIL(tmp_task.build_task_from_sql_result(res))) {
          LOG_WARN("fail to build ObLSTypeTransformTask from res", KR(ret));
        } else if (OB_FAIL(queues_[priority].push_task_in_schedule_list(tmp_task))) {
          LOG_WARN("fail to load ObLSTypeTransformTask into schedule list", KR(ret));
        }
      }
    } else if (0 == task_type.case_compare(ob_disaster_recovery_task_type_strs(ObDRTaskType::LS_REMOVE_NON_PAXOS_REPLICA))
               || 0 == task_type.case_compare(ob_disaster_recovery_task_type_strs(ObDRTaskType::LS_REMOVE_PAXOS_REPLICA))) {
      SMART_VAR(ObRemoveLSReplicaTask, tmp_task) {
        if (OB_FAIL(tmp_task.build_task_from_sql_result(res))) {
          LOG_WARN("fail to build ObRemoveLSReplicaTask from res", KR(ret));
        } else if (OB_FAIL(queues_[priority].push_task_in_schedule_list(tmp_task))) {
          LOG_WARN("fail to load ObRemoveLSReplicaTask into schedule list", KR(ret));
        }
      }
    } else if (task_type == common::ObString("MODIFY PAXOS REPLICA NUMBER")) {
      SMART_VAR(ObLSModifyPaxosReplicaNumberTask, tmp_task) {
        if (OB_FAIL(tmp_task.build_task_from_sql_result(res))) {
          LOG_WARN("fail to build ObLSModifyPaxosReplicaNumberTask from res", KR(ret));
        } else if (OB_FAIL(queues_[priority].push_task_in_schedule_list(tmp_task))) {
          LOG_WARN("fail to load ObLSModifyPaxosReplicaNumberTask into schedule list", KR(ret));
        }
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected task type", KR(ret), K(task_type));
    }
  }
  return ret;
}

int ObDRTaskMgr::persist_task_info_(
    const ObDRTask &task,
    ObDRTaskRetComment &ret_comment)
{
  int ret = OB_SUCCESS;
  ret_comment = ObDRTaskRetComment::MAX;
  share::ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const uint64_t sql_tenant_id = gen_meta_tenant_id(task.get_tenant_id());
  ObMySQLTransaction trans;
  const int64_t timeout = GCONF.internal_sql_execute_timeout;
  observer::ObInnerSQLConnection *conn = NULL;
  ObConflictCaseWithClone case_to_check(ObConflictCaseWithClone::MODIFY_REPLICA);

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, sql_tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(sql_tenant_id));
  } else if (OB_FAIL(task.fill_dml_splicer(dml))) {
    LOG_WARN("fill dml splicer failed", KR(ret));
  } else if (OB_FAIL(dml.splice_insert_sql(share::OB_ALL_LS_REPLICA_TASK_TNAME, sql))) {
    LOG_WARN("fail to splice batch insert update sql", KR(ret), K(sql));
  } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn_ is NULL", KR(ret));
  } else if (OB_FAIL(ObInnerConnectionLockUtil::lock_table(sql_tenant_id,
                                                           OB_ALL_LS_REPLICA_TASK_TID,
                                                           EXCLUSIVE,
                                                           timeout,
                                                           conn))) {
    LOG_WARN("lock dest table failed", KR(ret), K(sql_tenant_id));
  } else if (OB_FAIL(ObTenantSnapshotUtil::check_tenant_not_in_cloning_procedure(task.get_tenant_id(), case_to_check))) {
    LOG_WARN("fail to check whether tenant is in cloning procedure", KR(ret));
    ret_comment = CANNOT_PERSIST_TASK_DUE_TO_CLONE_CONFLICT;
  } else if (OB_FAIL(trans.write(sql_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), "tenant_id",task.get_tenant_id(), K(sql_tenant_id), K(sql));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  FLOG_INFO("[DRTASK_NOTICE] finish persist task into inner table", KR(ret), K(task));
  return ret;
}

int ObDRTaskMgr::try_dump_statistic_(
    int64_t &last_dump_ts) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else {
    ObThreadCondGuard guard(cond_);
    const int64_t now = ObTimeUtility::current_time();
    if (now > last_dump_ts + config_->balancer_log_interval) {
      last_dump_ts = now;
      int tmp_ret = inner_dump_statistic_();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("task manager dump statistics failed", KR(tmp_ret));
      }
    };
  }
  return ret;
}

int ObDRTaskMgr::inner_dump_statistic_() const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else {
    LOG_INFO("[DRTASK_NOTICE] disaster recovery task manager statistics",
        "waiting_high_priority_task_cnt", high_task_queue_.wait_task_cnt(),
        "executing_high_priority_task_cnt", high_task_queue_.in_schedule_task_cnt(),
        "waiting_low_priority_task_cnt", low_task_queue_.wait_task_cnt(),
        "executing_low_priority_task_cnt", low_task_queue_.in_schedule_task_cnt());
    for (int64_t i = 0; i < ARRAYSIZEOF(queues_); ++i) {
      // ignore error to make sure checking two queues 
      if (OB_FAIL(queues_[i].dump_statistic())) {
        LOG_WARN("fail to dump statistic for this queue", KR(ret), "priority", queues_[i].get_priority_str());
      }
    }
  }
  return ret;
}

int ObDRTaskMgr::try_clean_not_in_schedule_task_in_schedule_list_(
    int64_t &last_check_task_in_progress_ts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else {
    int64_t wait = 0;
    int64_t schedule = 0;
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(inner_get_task_cnt_(wait, schedule))) {
      LOG_WARN("fail to get task cnt", KR(ret));
    } else if (schedule <= 0) {
      // bypass
    } else {
      const int64_t now = ObTimeUtility::current_time();
      if (now > last_check_task_in_progress_ts + schedule * CHECK_IN_PROGRESS_INTERVAL_PER_TASK) {
        last_check_task_in_progress_ts = now;
        int tmp_ret = inner_clean_not_in_schedule_task_in_schedule_list_();
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("fail to do check task in progress", KR(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObDRTaskMgr::inner_clean_not_in_schedule_task_in_schedule_list_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(queues_); ++i) {
      // ignore error to make sure checking two queues 
      if (OB_FAIL(queues_[i].handle_not_in_progress_task(*this))) {
        LOG_WARN("fail to handle not in progress task in this queue", KR(ret),
                 "priority", queues_[i].get_priority_str());
      }
    }
  }
  FLOG_INFO("finish inner check task in progress", KR(ret));
  return ret;
}

int ObDRTaskMgr::inner_get_task_cnt_(
    int64_t &wait_cnt,
    int64_t &in_schedule_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else {
    wait_cnt = 0;
    in_schedule_cnt = 0;
    for (int64_t i = 0; i < ARRAYSIZEOF(queues_); ++i) {
      wait_cnt += queues_[i].wait_task_cnt();
      in_schedule_cnt += queues_[i].in_schedule_task_cnt();
    }
  }
  return ret;
}

int ObDRTaskMgr::try_pop_task(
    common::ObIAllocator &allocator,
    ObDRTask *&task)
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(cond_);
  int64_t wait_cnt = 0;
  int64_t in_schedule_cnt = 0;
  ObDRTask *my_task = nullptr;
  void *raw_ptr = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else if (OB_FAIL(inner_get_task_cnt_(wait_cnt, in_schedule_cnt))) {
    LOG_WARN("fail to get task cnt", KR(ret));
  } else if (wait_cnt > 0
             && 0 == concurrency_limited_ts_) {
    if (OB_FAIL(pop_task(my_task))) {
      LOG_WARN("fail to pop task", KR(ret));
    } else if (OB_ISNULL(my_task)) {
      task = nullptr;
    } else if (OB_ISNULL(raw_ptr = allocator.alloc(my_task->get_clone_size()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate task", KR(ret));
    } else if (OB_FAIL(my_task->clone(raw_ptr, task))) {
      LOG_WARN("fail to clone task", KR(ret), "source_task", *my_task);
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task ptr is null", KR(ret));
    } else {
      my_task->set_execute_time(ObTimeUtility::current_time());
    }
    
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(task)) {
        free_task_(allocator, task);
      } else if (OB_NOT_NULL(raw_ptr)) {
        allocator.free(raw_ptr);
        raw_ptr = nullptr;
      }
    }
  } else {
    int64_t now = ObTimeUtility::current_time();
    cond_.wait(get_schedule_interval());
    if (get_reach_concurrency_limit() + CONCURRENCY_LIMIT_INTERVAL < now) {
      clear_reach_concurrency_limit();
      LOG_TRACE("success to clear concurrency limit");
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(task)) {
    LOG_INFO("[DRTASK_NOTICE] success to pop a task", KPC(task), K_(concurrency_limited_ts),
             K(in_schedule_cnt));
  }
  return ret;
}

int ObDRTaskMgr::pop_task(
    ObDRTask *&task)
{
  int ret = OB_SUCCESS;
  int64_t wait_cnt = 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else {
    task = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(queues_); ++i) {
      if (queues_[i].wait_task_cnt() > 0) {
        wait_cnt += queues_[i].wait_task_cnt();
        if (OB_FAIL(queues_[i].pop_task(task))) {
          LOG_WARN("pop_task from queue failed", KR(ret), "priority", queues_[i].get_priority_str());
        } else if (OB_NOT_NULL(task)) {
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(task)) {
        if (wait_cnt > 0) {
          set_reach_concurrency_limit();
        }
      } else {
        const bool in_schedule = true;
        if (OB_FAIL(set_sibling_in_schedule(*task, in_schedule))) {
          LOG_WARN("set sibling in schedule failed", KR(ret), KPC(task));
        }
      }
    }
  }
  return ret;
}

int ObDRTaskMgr::execute_task(
    const ObDRTask &task)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(self_);
  FLOG_INFO("execute disaster recovery task", K(task));
  int dummy_ret = OB_SUCCESS;
  ObDRTaskRetComment ret_comment = ObDRTaskRetComment::MAX;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else if (OB_FAIL(persist_task_info_(task, ret_comment))) {
    LOG_WARN("fail to persist task info into table", KR(ret));
  } else if (OB_FAIL(task_executor_->execute(task, dummy_ret, ret_comment))) {
    LOG_WARN("fail to execute disaster recovery task", KR(ret));
  }
  if (OB_FAIL(ret)) {
    //TODO@jingyu.cr:
    // (1) use rwlock instead of threadcond
    // (2) deal with block in status
    (void)log_task_result(task, ret, ret_comment);
    ObThreadCondGuard guard(cond_);
    const bool data_in_limit = (OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT == ret);
    if (OB_SUCCESS != async_add_cleaning_task_to_updater(
          task.get_task_id(),
          task.get_task_key(),
          ret,
          false,/*need_record_event*/
          ret_comment,
          !data_in_limit)) {
      LOG_WARN("fail to do execute over", KR(ret), K(task));
    }
  }
  return ret;
}

int ObDRTaskMgr::set_sibling_in_schedule(
    const ObDRTask &task,
    const bool in_schedule)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(queues_); ++i) {
      if (OB_FAIL(queues_[i].set_sibling_in_schedule(task, in_schedule))) {
        if (i == 0) {
          LOG_WARN("fail to set sibling in schedule in high priority queue", KR(ret), K(task));
        } else {
          LOG_WARN("fail to set sibling in schedule in low priority queue", KR(ret), K(task));
        }
      }
    }
  }
  return ret;
}
} // end namespace rootserver
} // end namespace oceanbase
