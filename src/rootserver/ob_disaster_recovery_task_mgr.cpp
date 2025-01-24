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
#include "ob_disaster_recovery_task_executor.h"
#include "ob_disaster_recovery_task_table_operator.h"
#include "rootserver/tenant_snapshot/ob_tenant_snapshot_util.h" // for ObTenantSnapshotUtil
#include "storage/tablelock/ob_lock_inner_connection_util.h" // for ObInnerConnectionLockUtil
#include "observer/ob_inner_sql_connection.h"
#include "src/rootserver/ob_root_utils.h"
#include "share/ob_all_server_tracer.h"

namespace oceanbase
{
using namespace common;
using namespace lib;
using namespace obrpc;
using namespace transaction::tablelock;
using namespace share;

namespace rootserver
{

static const char* ls_replica_parallel_migration_mode[] = {
  "AUTO",
  "ON",
  "OFF"
};

const char* ObParallelMigrationMode::get_mode_str() const {
  STATIC_ASSERT(ARRAYSIZEOF(ls_replica_parallel_migration_mode) == (int64_t)MAX,
                "ls_replica_parallel_migration_mode string array size mismatch enum ParallelMigrationMode count");
  const char *str = NULL;
  if (mode_ >= AUTO && mode_ < MAX) {
    str = ls_replica_parallel_migration_mode[static_cast<int64_t>(mode_)];
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid ParallelMigrationMode", K_(mode));
  }
  return str;
}

int64_t ObParallelMigrationMode::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(mode), "mode", get_mode_str());
  J_OBJ_END();
  return pos;
}

int ObParallelMigrationMode::parse_from_string(const ObString &mode)
{
  int ret = OB_SUCCESS;
  bool found = false;
  STATIC_ASSERT(ARRAYSIZEOF(ls_replica_parallel_migration_mode) == (int64_t)MAX,
                "ls_replica_parallel_migration_mode string array size mismatch enum ParallelMigrationMode count");
  for (int64_t i = 0; i < ARRAYSIZEOF(ls_replica_parallel_migration_mode) && !found; i++) {
    if (0 == mode.case_compare(ls_replica_parallel_migration_mode[i])) {
      mode_ = static_cast<ParallelMigrationMode>(i);
      found = true;
      break;
    }
  }
  if (!found) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to parse type from string", KR(ret), K(mode), K_(mode));
  }
  return ret;
}

ObDRTaskQueue::ObDRTaskQueue() : inited_(false),
                                 config_(nullptr),
                                 task_alloc_(),
                                 wait_list_(),
                                 schedule_list_(),
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
    free_task_(t);
  }
  while (!schedule_list_.is_empty()) {
    ObDRTask *t = schedule_list_.remove_first();
    free_task_(t);
  }
}

void ObDRTaskQueue::reset()
{
  wait_list_.reset();
  schedule_list_.reset();
}

void ObDRTaskQueue::free_task_(ObDRTask *&task)
{
  if (OB_NOT_NULL(task)) {
    task->~ObDRTask();
    task_alloc_.free(task);
    task = nullptr;
  }
}

int ObDRTaskQueue::init(
    common::ObServerConfig &config,
    obrpc::ObSrvRpcProxy *rpc_proxy,
    ObDRTaskPriority priority)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(rpc_proxy)
          || (ObDRTaskPriority::LOW_PRI != priority && ObDRTaskPriority::HIGH_PRI != priority)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(rpc_proxy), K(priority));
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

int ObDRTaskQueue::push_task_in_schedule_list(
    const ObDRTask &task)
{
  // STEP 1: push task into schedule list
  // STEP 2: set task in schedule
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
      free_task_(new_task);
    } else if (OB_NOT_NULL(raw_ptr)) {
      task_alloc_.free(raw_ptr);
      raw_ptr = nullptr;
    }
  }
  return ret;
}

int ObDRTaskQueue::do_push_task_in_wait_list(
    ObDRTaskMgr &task_mgr,
    const ObDRTask &task)
{
  int ret = OB_SUCCESS;
  void *raw_ptr = nullptr;
  ObDRTask *new_task = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(raw_ptr = task_alloc_.alloc(task.get_clone_size()))) {
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
    LOG_INFO("success to push a task in waiting list", K(task));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(new_task)) {
      free_task_(new_task);
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
      task = t; // any task can be pop, no other task in double queue is conflict with it.
      break;
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
        free_task_(task);
      }
    }
  }
  return ret;
}

int ObDRTaskQueue::get_task_by_task_id(
    const share::ObTaskId &task_id,
    ObDRTask *&task)
{
  int ret = OB_SUCCESS;
  task = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_id));
  } else if (OB_FAIL(get_task_by_task_id_in_list_(task_id, wait_list_, task))) {
    LOG_WARN("check task exist in wait list failed", KR(ret), K(task_id), K(wait_list_));
  } else if (OB_NOT_NULL(task)) {
  } else if (OB_FAIL(get_task_by_task_id_in_list_(task_id, schedule_list_, task))) {
    LOG_WARN("check task exist in schedule list failed", KR(ret), K(task_id), K(schedule_list_));
  }
  return ret;
}

int ObDRTaskQueue::get_task_by_task_id_in_list_(
    const share::ObTaskId &task_id,
    TaskList &list,
    ObDRTask *&task)
{
  int ret = OB_SUCCESS;
  task = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_id));
  } else {
    DLIST_FOREACH(task_in_list, list) {
      if (OB_ISNULL(task_in_list)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task_in_list is null ptr", KR(ret), K(task_id), K(list));
      } else if (task_in_list->get_task_id() == task_id) {
        task = task_in_list;
        break;
      }
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

int ObDRTaskQueue::try_clean_and_cancel_task(
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
    bool need_cancel = false;
    DLIST_FOREACH(t, schedule_list_) {
      int tmp_ret = OB_SUCCESS;
      need_cleaning = false;
      need_cancel = false;
      DEBUG_SYNC(BEFORE_CHECK_CLEAN_DRTASK);
      if (OB_ISNULL(t)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("t is null ptr", KR(tmp_ret), KP(t));
      } else if (OB_SUCCESS != (tmp_ret = check_task_need_cleaning_(*t, need_cleaning, ret_comment))) {
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
      // ignore ret code
      if (need_cleaning) {
        LOG_TRACE("skip, task will be cleared", K(*t));
      } else if (OB_TMP_FAIL(task_mgr.check_need_cancel_migrate_task(*t, need_cancel))) {
        LOG_WARN("fail to check need cancel migrate task", KR(tmp_ret), K(*t));
      } else if (need_cancel) {
        FLOG_INFO("need cancel migrate task in schedule list", K(*t));
        if (OB_TMP_FAIL(task_mgr.send_rpc_to_cancel_migrate_task(*t))) {
          LOG_WARN("fail to send rpc to cancel migrate task", KR(tmp_ret), K(*t));
        }
      }
    }
    DLIST_FOREACH_REMOVESAFE(t, wait_list_) {
      // check wait list to cancel migrate task
      int tmp_ret = OB_SUCCESS;
      need_cancel = false;
      if (OB_ISNULL(t)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("t is null ptr", KR(tmp_ret), KP(t));
      } else if (OB_TMP_FAIL(task_mgr.check_need_cancel_migrate_task(*t, need_cancel))) {
        LOG_WARN("fail to check need cancel migrate task", KR(tmp_ret), K(*t));
      } else if (need_cancel) {
        FLOG_INFO("need cancel migrate task in wait list", K(*t));
        // remove from wait_list_, need use DLIST_FOREACH_REMOVESAFE
        wait_list_.remove(t);
        free_task_(t);
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
    free_task_(task);
  }
  return ret;
}

int ObDRTaskQueue::check_whether_task_conflict_in_list_(
    const ObDRTaskKey &task_key,
    const TaskList &list,
    const bool enable_parallel_migration,
    bool &is_conflict) const
{
  int ret = OB_SUCCESS;
  is_conflict = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!task_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_key));
  } else {
    DLIST_FOREACH(task_in_list, list) {
      if (OB_ISNULL(task_in_list)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task_in_list is null ptr", KR(ret), K(task_key), K(list));
      } else if (task_in_list->get_task_key().get_tenant_id() == task_key.get_tenant_id()
              && task_in_list->get_task_key().get_ls_id() == task_key.get_ls_id()) {
        // tenant_id + ls_id is same
        if (!enable_parallel_migration) {
          LOG_INFO("enable_parallel_migration is false, tenant_id + ls_id is the same, prohibit parallel",
                    KR(ret), K(enable_parallel_migration), K(task_key));
          // if tenant_id + ls_id is the same, then task conflict
          is_conflict = true;
        } else if (ObDRTaskType::LS_MIGRATE_REPLICA != task_key.get_task_type()
                || ObDRTaskType::LS_MIGRATE_REPLICA != task_in_list->get_disaster_recovery_task_type()) {
          // if one of the two task is not a migration task, the task is conflict.
          LOG_INFO("there is a task is not migration task, prohibit parallel", KR(ret),
                    K(task_key), K(task_in_list->get_disaster_recovery_task_type()));
          is_conflict = true;
        } else if (task_in_list->get_task_key().get_zone() == task_key.get_zone()) {
          LOG_INFO("two migrate task conflict, task execution zone is the same, prohibit parallel",
                  KR(ret), K(task_key), K(task_in_list->get_task_key()));
          is_conflict = true;
        }
        if (is_conflict) {
          break;
        }
      } // end with tenant_id + ls_id is same
    }
  }
  return ret;
}

int ObDRTaskQueue::check_whether_task_conflict(
    const ObDRTaskKey &task_key,
    const bool enable_parallel_migration,
    bool &is_conflict)
{
  int ret = OB_SUCCESS;
  is_conflict = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!task_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_key));
  } else if (OB_FAIL(check_whether_task_conflict_in_list_(task_key, wait_list_, enable_parallel_migration, is_conflict))) {
    LOG_WARN("check task conflict in wait list failed", KR(ret), K(task_key), K(wait_list_), K(enable_parallel_migration));
  } else if (is_conflict) {
    LOG_INFO("task conflict in wait list", KR(ret), K(task_key), K(enable_parallel_migration), K(priority_));
  } else if (OB_FAIL(check_whether_task_conflict_in_list_(task_key, schedule_list_, enable_parallel_migration, is_conflict))) {
    LOG_WARN("check task conflict in schedule list failed", KR(ret), K(task_key), K(schedule_list_), K(enable_parallel_migration));
  } else if (is_conflict) {
    LOG_INFO("task conflict in schedule list", KR(ret), K(task_key), K(enable_parallel_migration), K(priority_));
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
  } else if (OB_FAIL(create(thread_count, "DRTaskMgr", ObWaitEventIds::REBALANCE_TASK_MGR_COND_WAIT))) {
    LOG_WARN("fail to create disaster recovery task mgr", KR(ret));
  } else {
    config_ = &config;
    self_ = server;
    task_executor_ = &task_executor;
    rpc_proxy_ = rpc_proxy;
    sql_proxy_ = sql_proxy;
    schema_service_ = schema_service;
    if (OB_FAIL(high_task_queue_.init(
            config, rpc_proxy_, ObDRTaskPriority::HIGH_PRI))) {
      LOG_WARN("fail to init high priority task queue", KR(ret));
    } else if (OB_FAIL(low_task_queue_.init(
            config, rpc_proxy_, ObDRTaskPriority::LOW_PRI))) {
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
  ObThreadCondGuard guard(get_cond());
  get_cond().broadcast();
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
            ObThreadCondGuard guard(get_cond());
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
        if (OB_SUCCESS != (tmp_ret = try_clean_and_cancel_task_in_schedule_list_(
              last_check_task_in_progress_ts))) {
           LOG_WARN("fail to try check task in progress", KR(tmp_ret), K(last_check_task_in_progress_ts));
        }
      }
    }
  }
  FLOG_INFO("disaster task mgr exits");
}

int ObDRTaskMgr::add_task_in_queue_and_execute(ObDRTask &task)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(loaded), K_(stopped));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dr task", KR(ret), K(task));
  } else {
    ObThreadCondGuard guard(get_cond());
    bool is_conflict = false;
    ObDRTaskQueue &queue = task.is_high_priority_task() ? high_task_queue_ : low_task_queue_;
    if (OB_UNLIKELY(queue.task_cnt() >= TASK_QUEUE_LIMIT)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("disaster recovery task queue is full", KR(ret), "task_cnt", queue.task_cnt());
    } else if (OB_FAIL(check_whether_task_conflict_(task, is_conflict))) {
      LOG_WARN("fail to check whether task conflict", KR(ret), K(task));
    } else if (is_conflict) {
      // task conflict return error code, report an error to user
      ret = OB_ENTRY_EXIST;
      LOG_WARN("ls disaster recovery task has existed in queue", KR(ret), K(task), K(is_conflict));
    } else if (OB_FAIL(queue.push_task_in_schedule_list(task))) {
      LOG_WARN("fail to add task to schedule list", KR(ret), K(task));
    } else {
      if (OB_SUCCESS != (tmp_ret = task.log_execute_start())) {
        LOG_WARN("fail to log task start", KR(tmp_ret), K(task));
      }
      if (OB_FAIL(execute_manual_task_(task))) {
        //must under cond
        LOG_WARN("fail to execute manual task", KR(ret), K(task));
      }
    }
  }
  return ret;
}

int ObDRTaskMgr::add_task(
    ObDRTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(loaded), K_(stopped));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dr task", KR(ret), K(task));
  } else {
    ObThreadCondGuard guard(get_cond());
    ObDRTaskQueue &queue = task.is_high_priority_task() ? high_task_queue_ : low_task_queue_;
    bool is_conflict = false;
    if (OB_UNLIKELY(queue.task_cnt() >= TASK_QUEUE_LIMIT)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("disaster recovery task queue is full", KR(ret), "task_cnt", queue.task_cnt());
    } else if (OB_FAIL(check_whether_task_conflict_(task, is_conflict))) {
      LOG_WARN("fail to check whether task conflict", KR(ret), K(task));
    } else if (is_conflict) {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("ls disaster recovery task has existed in queue", KR(ret), K(task), K(is_conflict));
    } else if (OB_FAIL(queue.do_push_task_in_wait_list(*this, task))) {
      LOG_WARN("fail to push task", KR(ret), K(task));
    } else {
      get_cond().broadcast();
      LOG_INFO("[DRTASK_NOTICE] add task to disaster recovery task mgr finish", KR(ret), K(task));
    }
  }
  return ret;
}

int ObDRTaskMgr::deal_with_task_reply(
    const ObDRTaskReplyResult &reply)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(loaded), K_(stopped));
  } else {
    int tmp_ret = OB_SUCCESS;
    ObDRTask *task = nullptr;
    ObThreadCondGuard guard(get_cond());
    if (OB_SUCCESS != (tmp_ret = get_task_by_id_(reply.task_id_, task))) {
      if (OB_ENTRY_NOT_EXIST == tmp_ret) {
        // task not exist, try record this reply result
        ROOTSERVICE_EVENT_ADD("disaster_recovery", "finish_disaster_recovery_task",
                            "tenant_id", reply.tenant_id_,
                            "ls_id", reply.ls_id_.id(),
                            "task_id", reply.task_id_,
                            "execute_result", reply.result_,
                            "ret_comment", ob_disaster_recovery_task_ret_comment_strs(ObDRTaskRetComment::RECEIVE_FROM_STORAGE_RPC));
      } else {
        LOG_WARN("fail to get task from task manager", KR(tmp_ret), K(reply));
      }
    } else if (OB_SUCCESS != (tmp_ret = task->log_execute_result(reply.result_, ObDRTaskRetComment::RECEIVE_FROM_STORAGE_RPC))){
      LOG_WARN("fail to log execute result", KR(tmp_ret), K(reply));
    }

    if (OB_FAIL(async_add_cleaning_task_to_updater(
                    reply.task_id_,
                    task->get_task_key(),
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
  } else if (OB_FAIL(get_task_by_id_(task_id, task))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("fail to get task, task may be cleaned earlier", KR(ret), K(task_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get task from task manager", KR(ret), K(task_id));
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
    const int ret_code,
    const bool need_clear_server_data_in_limit,
    const bool need_record_event,
    const ObDRTaskRetComment &ret_comment)
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(get_cond());
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else {
    ObDRTaskQueue *task_queue = nullptr;
    ObDRTask *task = nullptr;
    common::ObAddr dst_server;
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(queues_); ++i) {
      if (OB_FAIL(queues_[i].get_task_by_task_id(task_id, task))) {
        LOG_WARN("fail to get schedule task from queue", KR(ret), "priority", queues_[i].get_priority_str());
      } else if (OB_NOT_NULL(task)) {
        task_queue = &queues_[i];
        break;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(task)) {
        LOG_INFO("in schedule taks not found, maybe not sync because of network traffic",
                 K(task_id), K(ret_code));
      } else {
        if (need_record_event) {
          (void)log_task_result(*task, ret_code, ret_comment);
        }
        dst_server = task->get_dst_server();
        if (OB_ISNULL(task_queue)) {
          LOG_INFO("task_queue is null"); // by pass
        } else if (OB_FAIL(task_queue->finish_schedule(task))) {
          LOG_WARN("fail to finish scheduling task", KR(ret), KPC(task));
        }
      }
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
    LOG_WARN("not init", KR(ret), K_(inited), K_(loaded), K_(stopped));
  } else {
    ObThreadCondGuard guard(get_cond());
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
    ObDRTask *&task)
{
  int ret = OB_SUCCESS;
  ObDRTask *task_to_get = nullptr;
  void *raw_ptr = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(queues_); ++i) {
    if (OB_FAIL(queues_[i].get_task_by_task_id(task_id, task_to_get))) {
      LOG_WARN("fail to get schedule task from queue", KR(ret), "priority", queues_[i].get_priority_str());
    } else if (OB_NOT_NULL(task_to_get)) {
      break;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(task_to_get)) {
    task = nullptr;
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("task not exist, maybe cleaned earier", KR(ret), K(task_id));
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
  ObThreadCondGuard guard(get_cond());
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
  ObLSReplicaTaskTableOperator task_table_operator;
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
  } else if (OB_FAIL(task_table_operator.insert_task(trans, task))) {
    LOG_WARN("task_table_operator insert_task failed", KR(ret), K(task));
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
    int64_t &last_dump_ts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else {
    ObThreadCondGuard guard(get_cond());
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

int ObDRTaskMgr::try_clean_and_cancel_task_in_schedule_list_(
    int64_t &last_check_task_in_progress_ts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else {
    int64_t wait = 0;
    int64_t schedule = 0;
    DEBUG_SYNC(BEFORE_CHECK_CLEAN_AND_CANCEL_DRTASK); // need before cond guard
    ObThreadCondGuard guard(get_cond());
    if (OB_FAIL(inner_get_task_cnt_(wait, schedule))) {
      LOG_WARN("fail to get task cnt", KR(ret));
    } else if (schedule <= 0) {
      // bypass
    } else {
      const int64_t now = ObTimeUtility::current_time();
      if (now > last_check_task_in_progress_ts + schedule * CHECK_IN_PROGRESS_INTERVAL_PER_TASK) {
        last_check_task_in_progress_ts = now;
        int tmp_ret = inner_clean_and_cancel_task_in_schedule_list_();
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("fail to do check task in progress", KR(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObDRTaskMgr::inner_clean_and_cancel_task_in_schedule_list_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(queues_); ++i) {
      // ignore error to make sure checking two queues 
      if (OB_FAIL(queues_[i].try_clean_and_cancel_task(*this))) {
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
  ObThreadCondGuard guard(get_cond());
  int64_t wait_cnt = 0;
  int64_t in_schedule_cnt = 0;
  ObDRTask *my_task = nullptr;
  void *raw_ptr = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else if (OB_FAIL(inner_get_task_cnt_(wait_cnt, in_schedule_cnt))) {
    LOG_WARN("fail to get task cnt", KR(ret));
  } else if (wait_cnt > 0) {
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
    idle_wait(get_schedule_interval());

  }
  if (OB_SUCC(ret) && OB_NOT_NULL(task)) {
    LOG_INFO("[DRTASK_NOTICE] success to pop a task", KPC(task),
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
  }
  return ret;
}

int ObDRTaskMgr::execute_manual_task_(
    const ObDRTask &task)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("execute manual disaster recovery task", K(task));
  int dummy_ret = OB_SUCCESS;
  ObDRTaskRetComment ret_comment = ObDRTaskRetComment::MAX;
  DEBUG_SYNC(BEFORE_ADD_MANUAL_REPLICA_TASK_IN_INNER_TABLE);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (OB_ISNULL(task_executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task_executor_ is nullptr", KR(ret), K(task));
  } else if (OB_FAIL(persist_task_info_(task, ret_comment))) {
    LOG_WARN("fail to persist task info into table", KR(ret), K(task));
  } else if (OB_FAIL(task_executor_->execute(task, dummy_ret, ret_comment))) {
    LOG_WARN("fail to execute disaster recovery task", KR(ret), K(task));
  }
  if (OB_FAIL(ret)) {
    (void)log_task_result(task, ret, ret_comment);
    const bool data_in_limit = (OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT == ret);
    if (OB_SUCCESS != async_add_cleaning_task_to_updater(
          task.get_task_id(),
          task.get_task_key(),
          ret, false/*need_record_event*/, ret_comment,
          !data_in_limit)) {
      LOG_WARN("fail to do execute over", KR(ret), K(task));
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
    ObThreadCondGuard guard(get_cond());
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

int ObDRTaskMgr::check_whether_task_conflict_(
    ObDRTask &task,
    bool &is_conflict)
{
  int ret = OB_SUCCESS;
  is_conflict = false;
  bool enable_parallel_migration = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (OB_FAIL(check_tenant_enable_parallel_migration_(task.get_tenant_id(), enable_parallel_migration))) {
    LOG_WARN("check tenant enable parallel migration failed", KR(ret), K(task), K(enable_parallel_migration));
  } else if (OB_FAIL(high_task_queue_.check_whether_task_conflict(task.get_task_key(), enable_parallel_migration, is_conflict))) {
    LOG_WARN("fail to check task conflict", KR(ret), K(task), K(enable_parallel_migration));
  } else if (is_conflict) {
    LOG_INFO("task conflict in high_task_queue", K(task.get_task_key()), K(enable_parallel_migration));
  } else if (OB_FAIL(low_task_queue_.check_whether_task_conflict(task.get_task_key(), enable_parallel_migration, is_conflict))) {
    LOG_WARN("fail to check task conflict", KR(ret), K(task), K(enable_parallel_migration));
  } else if (is_conflict) {
    LOG_INFO("task conflict in low_task_queue", K(task.get_task_key()), K(enable_parallel_migration));
  } else if (enable_parallel_migration && OB_FAIL(set_migrate_task_prioritize_src_(enable_parallel_migration, task))) {
    // migrate task need set prioritize_same_zone_src_
    LOG_WARN("failed to set enable_parallel_migration", KR(ret), K(enable_parallel_migration), K(task));
  }
  return ret;
}

int ObDRTaskMgr::check_tenant_enable_parallel_migration_(
    const uint64_t &tenant_id,
    bool &enable_parallel_migration)
{
  int ret = OB_SUCCESS;
  const char *str = "auto";
  ObParallelMigrationMode mode;
  enable_parallel_migration = false;
  uint64_t tenant_data_version = 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(loaded), K_(stopped));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    share::ObTenantRole tenant_role;
    if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(tenant_id), tenant_data_version))) {
      LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
    } else if (!((tenant_data_version >= DATA_VERSION_4_3_5_0)
              || (tenant_data_version >= MOCK_DATA_VERSION_4_2_5_0 && tenant_data_version < DATA_VERSION_4_3_0_0))) {
      enable_parallel_migration = false;
    } else if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant config is invalid", KR(ret), K(tenant_id));
    } else if (FALSE_IT(str = tenant_config->replica_parallel_migration_mode.str())) {
    } else if (OB_FAIL(mode.parse_from_string(str))) {
      LOG_WARN("mode parse failed", KR(ret), K(str));
    } else if (!mode.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parallel migration mode is invalid", KR(ret), K(mode));
    } else if (mode.is_on_mode()) {
      enable_parallel_migration = true;
    } else if (mode.is_off_mode()) {
      enable_parallel_migration = false;
    } else if (mode.is_auto_mode()) {
      if (!is_user_tenant(tenant_id)) {
        // sys and meta tenant is primary tenant
        enable_parallel_migration = false;
      } else if (OB_FAIL(ObAllTenantInfoProxy::get_tenant_role(GCTX.sql_proxy_, tenant_id, tenant_role))) {
        LOG_WARN("fail to get tenant_role", KR(ret), K(tenant_id));
      } else if (!tenant_role.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant_role is invalid", KR(ret), K(tenant_role));
      } else if (tenant_role.is_primary()) {
        enable_parallel_migration = false;
      } else {
        enable_parallel_migration = true;
        // in auto mode, other tenant(clone restore standby) enable_parallel_migration is true
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parallel migration mode is invalid", KR(ret), K(mode));
    }
    LOG_INFO("check tenant enable_parallel_migration over", KR(ret),
              K(tenant_id), K(enable_parallel_migration), K(tenant_role), K(mode));
  }
  return ret;
}

int ObDRTaskMgr::set_migrate_task_prioritize_src_(
    const bool enable_parallel_migration,
    ObDRTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(loaded), K_(stopped));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dr task", KR(ret), K(task));
  } else if (ObDRTaskType::LS_MIGRATE_REPLICA == task.get_task_key().get_task_type()) {
    ObMigrateLSReplicaTask *migrate_task = static_cast<ObMigrateLSReplicaTask*>(&task);
    if (OB_ISNULL(migrate_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("migrate_task is nullptr", KR(ret), K(task));
    } else {
      migrate_task->set_prioritize_same_zone_src(enable_parallel_migration);
    }
  }
  return ret;
}

int ObDRTaskMgr::check_need_cancel_migrate_task(
    const ObDRTask &task,
    bool &need_cancel)
{
  int ret = OB_SUCCESS;
  need_cancel = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dr task", KR(ret), K(task));
  } else if (ObDRTaskType::LS_MIGRATE_REPLICA != task.get_disaster_recovery_task_type()) {
    LOG_TRACE("skip, task is not a migration task", K(task));
  } else if ((0 == task.get_comment().case_compare(drtask::MIGRATE_REPLICA_DUE_TO_UNIT_NOT_MATCH)
           || 0 == task.get_comment().case_compare(drtask::REPLICATE_REPLICA))) {
    // only surpport cancel unit not match migration task
    // not include manual migration tasks
    bool dest_server_has_unit = false;
    uint64_t tenant_data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(task.get_tenant_id()), tenant_data_version))) {
      LOG_WARN("fail to get min data version", KR(ret), K(task));
    } else if (!(tenant_data_version >= DATA_VERSION_4_3_3_0 || (tenant_data_version >= MOCK_DATA_VERSION_4_2_3_0 && tenant_data_version < DATA_VERSION_4_3_0_0))) {
      need_cancel = false;
      LOG_INFO("tenant data_version is not match", KR(ret), K(task), K(tenant_data_version));
    } else if (OB_FAIL(check_tenant_has_unit_in_server_(task.get_tenant_id(), task.get_dst_server(), dest_server_has_unit))) {
      LOG_WARN("fail to check tenant has unit in server", KR(ret), K(task));
    } else if (!dest_server_has_unit) {
      need_cancel = true;
      FLOG_INFO("need cancel migrate task", KR(ret), K(need_cancel), K(task));
    }
  }
  return ret;
}

int ObDRTaskMgr::check_tenant_has_unit_in_server_(
    const uint64_t tenant_id,
    const common::ObAddr &server_addr,
    bool &has_unit)
{
  int ret = OB_SUCCESS;
  has_unit = false;
  share::ObUnitTableOperator unit_operator;
  common::ObArray<share::ObUnit> unit_info_array;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else if (OB_UNLIKELY(!server_addr.is_valid() || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server_addr), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(unit_operator.init(*GCTX.sql_proxy_))) {
    LOG_WARN("unit operator init failed", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(unit_operator.get_units_by_tenant(gen_user_tenant_id(tenant_id), unit_info_array))) {
    LOG_WARN("fail to get unit info array", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !has_unit && i < unit_info_array.count(); ++i) {
      if (unit_info_array.at(i).server_ == server_addr) {
        has_unit = true;
      }
    }
  }
  return ret;
}

int ObDRTaskMgr::send_rpc_to_cancel_migrate_task(
    const ObDRTask &task)
{
  int ret = OB_SUCCESS;
  ObLSCancelReplicaTaskArg rpc_arg;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped), K_(loaded));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (OB_ISNULL(rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc_proxy_ ptr is null", KR(ret), KP(rpc_proxy_));
  } else if (OB_FAIL(rpc_arg.init(task.get_task_id(), task.get_ls_id(), task.get_tenant_id()))) {
    LOG_WARN("fail to init arg", KR(ret), K(task));
  } else if (OB_FAIL(rpc_proxy_->to(task.get_dst_server()).by(task.get_tenant_id()).timeout(GCONF.rpc_timeout)
                                .ls_cancel_replica_task(rpc_arg))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("task not exist", KR(ret), K(task));
      ret = OB_SUCCESS; // ignore OB_ENTRY_NOT_EXIST
    } else {
      LOG_WARN("fail to execute cancel task rpc", KR(ret), K(rpc_arg), K(task));
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
