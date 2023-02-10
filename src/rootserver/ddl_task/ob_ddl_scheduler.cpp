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

#include "observer/ob_server_struct.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_snapshot_info_manager.h"
#include "rootserver/ddl_task/ob_column_redefinition_task.h"
#include "rootserver/ddl_task/ob_constraint_task.h"
#include "rootserver/ddl_task/ob_ddl_retry_task.h"
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "rootserver/ddl_task/ob_drop_index_task.h"
#include "rootserver/ddl_task/ob_drop_primary_key_task.h"
#include "rootserver/ddl_task/ob_index_build_task.h"
#include "rootserver/ddl_task/ob_modify_autoinc_task.h"
#include "rootserver/ddl_task/ob_table_redefinition_task.h"
#include "share/ob_ddl_common.h"
#include "share/ob_rpc_struct.h"
#include "share/longops_mgr/ob_longops_mgr.h"
#include "share/scheduler/ob_sys_task_stat.h"

namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace common;

namespace rootserver
{

ObDDLTaskQueue::ObDDLTaskQueue()
  : task_list_(), task_map_(), lock_(), is_inited_(false)
{
}

ObDDLTaskQueue::~ObDDLTaskQueue()
{
}

void ObDDLTaskQueue::destroy()
{
  task_map_.destroy();
  task_id_map_.destroy();
  is_inited_ = false;
}

int ObDDLTaskQueue::init(const int64_t bucket_num)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDDLTaskQueue has already been inited", K(ret));
  } else if (bucket_num <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(bucket_num));
  } else if (OB_FAIL(task_map_.create(bucket_num, lib::ObLabel("DdlQue")))) {
    LOG_WARN("fail to create task set", K(ret), K(bucket_num));
  } else if (OB_FAIL(task_id_map_.create(bucket_num, lib::ObLabel("DdlQue")))) {
    LOG_WARN("fail to create task set", K(ret), K(bucket_num));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObDDLTaskQueue::push_task(ObDDLTask *task)
{
  int ret = OB_SUCCESS;
  bool task_add_to_list = false;
  bool task_add_to_map = false;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(task));
  } else if (!task_list_.add_last(task)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected error, add build index task failed", K(ret));
  } else {
    int is_overwrite = 0; // do not overwrite
    task_add_to_list = true;
    if (OB_FAIL(task_map_.set_refactored(task->get_task_key(), task, is_overwrite))) {
      if (common::OB_HASH_EXIST == ret) {
        ret = common::OB_ENTRY_EXIST;
      } else {
        LOG_WARN("fail to set task to task set", K(ret));
      }
    } else {
      task_add_to_map = true;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(task_id_map_.set_refactored(task->get_task_id(), task, is_overwrite))) {
        if (common::OB_HASH_EXIST == ret) {
          ret = common::OB_ENTRY_EXIST;
        } else {
          LOG_WARN("set task to task set", K(ret));
        }
      }
      LOG_INFO("add task", K(*task), KP(task), K(common::lbt()));
    }
  }
  if (OB_FAIL(ret) && task_add_to_list) {
    int tmp_ret = OB_SUCCESS;
    if (!task_list_.remove(task)) {
      tmp_ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("fail to remove task", K(tmp_ret), K(*task));
    }
    if (task_add_to_map) {
      if (OB_SUCCESS != (tmp_ret = task_map_.erase_refactored(task->get_task_key()))) {
        LOG_WARN("erase from task map failed", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObDDLTaskQueue::get_next_task(ObDDLTask *&task)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (0 == task_list_.get_size()) {
    ret = common::OB_ENTRY_NOT_EXIST;
  } else if (OB_ISNULL(task = task_list_.remove_first())) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, task must not be NULL", K(ret));
  }
  return ret;
}

int ObDDLTaskQueue::remove_task(ObDDLTask *task)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task));
  } else if (OB_FAIL(task_map_.erase_refactored(task->get_task_key()))) {
    LOG_WARN("fail to erase from task set", K(ret));
  } else {
    LOG_INFO("succ to remove task", K(*task), KP(task));
  }
  if (OB_SUCCESS != (tmp_ret = task_id_map_.erase_refactored(task->get_task_id()))) {
    LOG_WARN("erase task from map failed", K(ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  return ret;
}

int ObDDLTaskQueue::add_task_to_last(ObDDLTask *task)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task));
  } else if (!task_list_.add_last(task)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "error unexpected, fail to move task to last", K(ret));
  }
  return ret;
}

template<typename F>
int ObDDLTaskQueue::modify_task(const ObDDLTaskKey &task_key, F &&op)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  ObDDLTask *task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (OB_UNLIKELY(!task_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_key));
  } else if (OB_FAIL(task_map_.get_refactored(task_key, task))) {
    ret = OB_HASH_NOT_EXIST == ret ? OB_ENTRY_NOT_EXIST : ret;
    LOG_WARN("get from task map failed", K(ret), K(task_key));
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_SYS;
    LOG_WARN("invalid task", K(ret), K(task_key));
  } else if (OB_FAIL(op(*task))) {
    LOG_WARN("failed to modify task", K(ret));
  }
  return ret;
}

template<typename F>
int ObDDLTaskQueue::modify_task(const int64_t task_id, F &&op)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  ObDDLTask *task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (OB_UNLIKELY(task_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_id));
  } else if (OB_FAIL(task_id_map_.get_refactored(task_id, task))) {
    ret = OB_HASH_NOT_EXIST == ret ? OB_ENTRY_NOT_EXIST : ret;
    LOG_WARN("get from task map failed", K(ret), K(task_id));
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_SYS;
    LOG_WARN("invalid task", K(ret), K(task_id));
  } else if (OB_FAIL(op(*task))) {
    LOG_WARN("failed to modify task", K(ret), K(task_id));
  }
  return ret;
}

int ObDDLTaskQueue::update_task_copy_deps_setting(const int64_t task_id,
                                    const bool is_copy_constraints,
                                    const bool is_copy_indexes,
                                    const bool is_copy_triggers,
                                    const bool is_copy_foreign_keys,
                                    const bool is_ignore_errors)
{
  int ret = OB_SUCCESS;
  ObDDLTask *task = nullptr;
  ObTableRedefinitionTask *table_redefinition_task = nullptr;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (OB_UNLIKELY(task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_id));
  } else if (OB_FAIL(task_id_map_.get_refactored(task_id, task))) {
    ret = OB_HASH_NOT_EXIST == ret ? OB_ENTRY_NOT_EXIST : ret;
    LOG_WARN("get from task map failed", K(ret), K(task_id));
  } else if (OB_ISNULL(table_redefinition_task = static_cast<ObTableRedefinitionTask*>(task))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl_task is null", K(ret));
  } else {
    table_redefinition_task->set_is_copy_constraints(is_copy_constraints);
    table_redefinition_task->set_is_copy_indexes(is_copy_indexes);
    table_redefinition_task->set_is_copy_triggers(is_copy_triggers);
    table_redefinition_task->set_is_copy_foreign_keys(is_copy_foreign_keys);
    table_redefinition_task->set_is_ignore_errors(is_ignore_errors);
  }
  return ret;
}

int ObDDLTaskQueue::update_task_process_schedulable(const int64_t task_id)
{
  int ret = OB_SUCCESS;
  ObDDLTask *ddl_task = nullptr;
  ObTableRedefinitionTask *table_redefinition_task = nullptr;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (OB_UNLIKELY(task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_id));
  } else if (OB_FAIL(task_id_map_.get_refactored(task_id, ddl_task))) {
    ret = OB_HASH_NOT_EXIST == ret ? OB_ENTRY_NOT_EXIST : ret;
    LOG_WARN("get from task map failed", K(ret), K(task_id));
  } else if (OB_ISNULL(table_redefinition_task = static_cast<ObTableRedefinitionTask*>(ddl_task))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl_task is null", K(ret));
  } else {
    table_redefinition_task->set_is_do_finish(true);
  }
  return ret;
}

int ObDDLTaskQueue::abort_task(const int64_t task_id, common::ObMySQLProxy &mysql_proxy)
{
  int ret = OB_SUCCESS;
  share::ObTaskId trace_id;
  ObDDLTask *ddl_task = nullptr;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (OB_UNLIKELY(task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_id));
  } else if (OB_FAIL(task_id_map_.get_refactored(task_id, ddl_task))) {
    if (OB_HASH_NOT_EXIST == ret) {
      bool exist = false;
      if (OB_FAIL(ObDDLTaskRecordOperator::check_task_id_exist(mysql_proxy, task_id, exist))) {
        LOG_WARN("check task id exist fail", K(ret));
      } else {
        if (exist) {
          ret = OB_EAGAIN;
          LOG_INFO("entry exist, the ddl scheduler hasn't recovered the task yet", K(ret), K(task_id));
        } else {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("this task does not exist in the hash table", K(ret), K(task_id));
        }
      }
    }
    LOG_WARN("get from task map failed", K(ret), K(task_id));
  } else if (OB_ISNULL(ddl_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl_task is null", K(ret));
  } else {
    trace_id.set(ddl_task->get_trace_id());
    if (OB_UNLIKELY(trace_id.is_invalid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(trace_id));
    } else if (OB_FAIL(SYS_TASK_STATUS_MGR.cancel_task(trace_id))) {
      LOG_WARN("cancel task failed", K(ret));
    } else {
      LOG_INFO("succeed to abort task", K(task_id));
    }
  }
  return ret;
}

ObDDLTaskHeartBeatMananger::ObDDLTaskHeartBeatMananger()
  : is_inited_(false), bucket_lock_()
{}

ObDDLTaskHeartBeatMananger::~ObDDLTaskHeartBeatMananger()
{
  bucket_lock_.destroy();
}

int ObDDLTaskHeartBeatMananger::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObManagerRegisterHeartBeatTask inited twice", K(ret));
  } else if (OB_FAIL(register_task_time_.create(BUCKET_LOCK_BUCKET_CNT, "register_task", "register_task"))) {
    LOG_WARN("failed to create register_task_time map", K(ret));
  } else if (OB_FAIL(bucket_lock_.init(BUCKET_LOCK_BUCKET_CNT))) {
    LOG_WARN("fail to init bucket lock", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObDDLTaskHeartBeatMananger::update_task_active_time(const int64_t task_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObManagerRegisterHeartBeatTask not inited", K(ret));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, task_id);
    // setting flag=1 to update the old time-value in the hash map with current time
    if (OB_FAIL(register_task_time_.set_refactored(task_id,
        ObTimeUtility::current_time(), 1, 0, 0))) {
      LOG_WARN("set register task time failed", K(ret));
    }
  }
  return ret;
}

int ObDDLTaskHeartBeatMananger::remove_task(const int64_t task_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObManagerRegisterHeartBeatTask not inited", K(ret));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, task_id);
    if (OB_FAIL(register_task_time_.erase_refactored(task_id))) {
      LOG_WARN("remove register task time failed", K(ret));
    }
  }
  return ret;
}

int ObDDLTaskHeartBeatMananger::get_inactive_ddl_task_ids(ObArray<int64_t>& remove_task_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObManagerRegisterHeartBeatTask not inited", K(ret));
  } else {
    const int64_t TIME_OUT_THRESHOLD = 5L * 60L * 1000L * 1000L;
    ObBucketTryRLockAllGuard all_ddl_task_guard(bucket_lock_);
    if (OB_FAIL(all_ddl_task_guard.get_ret())) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
      }
    } else {
      for (common::hash::ObHashMap<int64_t, int64_t>::iterator it = register_task_time_.begin(); OB_SUCC(ret) && it != register_task_time_.end(); it++) {
        if (ObTimeUtility::current_time() - it->second > TIME_OUT_THRESHOLD) {
          if (OB_FAIL(remove_task_ids.push_back(it->first))) {
            LOG_WARN("remove_task_ids push_back task_id fail", K(ret), K(it->first));
          }
        }
      }
    }
  }
  return ret;
}

int ObPrepareAlterTableArgParam::init(const uint64_t session_id,
                                      const ObSQLMode &sql_mode,
                                      const ObString &ddl_stmt_str,
                                      const ObString &orig_table_name,
                                      const ObString &orig_database_name,
                                      const ObString &target_database_name,
                                      const ObTimeZoneInfo &tz_info,
                                      const ObTimeZoneInfoWrap &tz_info_wrap,
                                      const ObString *nls_formats)
{
  int ret = OB_SUCCESS;
  if (FALSE_IT(session_id_ = session_id)) {
    // do nothing
  } else if (FALSE_IT(sql_mode_ = sql_mode)) {
    // do nothing
  } else if (FALSE_IT(ddl_stmt_str_.assign_ptr(ddl_stmt_str.ptr(), ddl_stmt_str.length()))) {
    // do nothing
  } else if (FALSE_IT(orig_table_name_.assign_ptr(orig_table_name.ptr(), orig_table_name.length()))) {
    // do nothing
  } else if (FALSE_IT(orig_database_name_.assign_ptr(orig_database_name.ptr(), orig_database_name.length()))) {
    // do nothing
  } else if (FALSE_IT(target_database_name_.assign_ptr(target_database_name.ptr(), target_database_name.length()))) {
    // do nothinh
  } else if (OB_FAIL(tz_info_.assign(tz_info))) {
    LOG_WARN("tz_info assign failed", K(ret));
  } else if (OB_FAIL(tz_info_wrap_.deep_copy(tz_info_wrap))) {
    LOG_WARN("failed to deep_copy tz info wrap", K(ret), "tz_info_wrap", tz_info_wrap);
  } else if (OB_FAIL(set_nls_formats(nls_formats))) {
    LOG_WARN("failed to set nls formats", K(ret));
  }
  return ret;
}
int ObPrepareAlterTableArgParam::set_nls_formats(const common::ObString *nls_formats)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(nls_formats)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("nls_formats is nullptr", K(ret));
  } else {
    char *tmp_ptr[ObNLSFormatEnum::NLS_MAX] = {};
    for (int64_t i = 0; OB_SUCC(ret) && i < ObNLSFormatEnum::NLS_MAX; ++i) {
      if (OB_ISNULL(tmp_ptr[i] = (char *)allocator_.alloc(nls_formats[i].length()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(ERROR, "failed to alloc memory!", "size", nls_formats[i].length(), K(ret));
      } else {
        MEMCPY(tmp_ptr[i], nls_formats[i].ptr(), nls_formats[i].length());
        nls_formats_[i].assign_ptr(tmp_ptr[i], nls_formats[i].length());
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; ++i) {
        allocator_.free(tmp_ptr[i]);
      }
    }
  }
  return ret;
}
int ObDDLScheduler::DDLScanTask::schedule(int tg_id)
{
  return TG_SCHEDULE(tg_id, *this, DDL_TASK_SCAN_PERIOD, true);
}

void ObDDLScheduler::DDLScanTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ddl_scheduler_.recover_task())) {
    LOG_WARN("failed to recover ddl tasks", K(ret));
  }
}

int ObDDLScheduler::HeartBeatCheckTask::schedule(int tg_id)
{
  return TG_SCHEDULE(tg_id, *this, DDL_TASK_CHECK_PERIOD, true);
}

void ObDDLScheduler::HeartBeatCheckTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ddl_scheduler_.remove_inactive_ddl_task())) {
    LOG_WARN("failed to check register task", K(ret));
  }
}

ObDDLScheduler::ObDDLScheduler()
  : is_inited_(false),
    is_started_(false),
    tg_id_(-1),
    root_service_(nullptr),
    idle_stop_(false),
    idler_(idle_stop_),
    scan_task_(*this),
    heart_beat_check_task_(*this)
{

}

ObDDLScheduler::~ObDDLScheduler()
{

}

int ObDDLScheduler::init(ObRootService *root_service)
{
  static const int64_t MAX_TASK_NUM = 10000;
  static const int64_t DDL_TASK_MEMORY_LIMIT = 1024L * 1024L * 1024L;
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(root_service));
  } else if (OB_FAIL(allocator_.init(
      OB_MALLOC_BIG_BLOCK_SIZE, "ddl_task", OB_SYS_TENANT_ID, DDL_TASK_MEMORY_LIMIT))) {
    LOG_WARN("init allocator failed", K(ret));
  } else if (OB_FAIL(task_queue_.init(MAX_TASK_NUM))) {
    LOG_WARN("init task queue failed", K(ret));
  } else if (OB_FAIL(manager_reg_heart_beat_task_.init())) {
    LOG_WARN("init manager register heart beat task failed", K(ret));
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::DDLTaskExecutor3, tg_id_))) {
    LOG_WARN("tg create failed", K(ret));
  } else {
    root_service_ = root_service;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLScheduler::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_started_) {
    // do nothing
  } else if (OB_FAIL(TG_SET_RUNNABLE(tg_id_, *this))) {
    LOG_WARN("tg set runnable failed", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("start ddl task scheduler failed", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::DDLScanTask))) {
    LOG_WARN("start ddl scan task failed", K(ret));
  } else if (OB_FAIL(scan_task_.schedule(lib::TGDefIDs::DDLScanTask))) {
    LOG_WARN("failed to schedule ddl scan task", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::HeartBeatCheckTask))) {
    LOG_WARN("start heart beat check task failed", K(ret));
  } else if (OB_FAIL(heart_beat_check_task_.schedule(lib::TGDefIDs::HeartBeatCheckTask))) {
    LOG_WARN("failed to schedule heart beat check task", K(ret));
  } else {
    is_started_ = true;
    idle_stop_ = false;
  }
  return ret;
}

void ObDDLScheduler::stop()
{
  TG_STOP(tg_id_);
  TG_STOP(lib::TGDefIDs::DDLScanTask);
  TG_STOP(lib::TGDefIDs::HeartBeatCheckTask);
  idle_stop_ = true;
  is_started_ = false;
  destroy_all_tasks();
}

void ObDDLScheduler::wait()
{
  TG_WAIT(tg_id_);
  TG_WAIT(lib::TGDefIDs::DDLScanTask);
  TG_WAIT(lib::TGDefIDs::HeartBeatCheckTask);
}

void ObDDLScheduler::run1()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t thread_cnt = TG_GET_THREAD_CNT(lib::TGDefIDs::DDLTaskExecutor3);
    int ret = OB_SUCCESS;
    ObDDLTask *task = nullptr;
    ObDDLTask *first_retry_task = nullptr;
    (void)prctl(PR_SET_NAME, "DDLTaskExecutor", 0, 0, 0);
    while (!has_set_stop()) {
      THIS_WORKER.set_worker_level(1);
      THIS_WORKER.set_curr_request_level(1);
      while (!has_set_stop()) {
        if (OB_FAIL(task_queue_.get_next_task(task))) {
          if (common::OB_ENTRY_NOT_EXIST == ret) {
            break;
          } else {
            LOG_WARN("fail to get next task", K(ret));
            break;
          }
        } else if (OB_ISNULL(task)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, task must not be NULL", K(ret));
        } else if (task == first_retry_task || !task->need_schedule()) {
          // add the task back to the queue
          if (OB_FAIL(task_queue_.add_task_to_last(task))) {
            STORAGE_LOG(ERROR, "fail to add task to last", K(ret), K(*task));
          }
          break;
        } else {
          ObCurTraceId::set(task->get_trace_id());
          int task_ret = task->process();
          task->calc_next_schedule_ts(task_ret, task_queue_.get_task_cnt() + thread_cnt);
          if (task->need_retry() && !has_set_stop() && !ObIDDLTask::is_ddl_force_no_more_process(task_ret)) {
            if (OB_FAIL(task_queue_.add_task_to_last(task))) {
              STORAGE_LOG(ERROR, "fail to add task to last, which should not happen", K(ret), K(*task));
            }
            first_retry_task = nullptr == first_retry_task ? task : first_retry_task;
          } else if (OB_FAIL(remove_ddl_task(task))) {
            LOG_WARN("remove ddl task failed", K(ret));
          }
        }
      }
      first_retry_task = nullptr;
      idler_.idle(100 * 1000L);
    }
  }
}

int ObDDLScheduler::create_ddl_task(const ObCreateDDLTaskParam &param,
                                    ObISQLClient &proxy,
                                    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  task_record.reset();
  const obrpc::ObAlterTableArg *alter_table_arg = nullptr;
  const obrpc::ObCreateIndexArg *create_index_arg = nullptr;
  const obrpc::ObDropIndexArg *drop_index_arg = nullptr;
  ObRootService *root_service = GCTX.root_service_;
  uint64_t tenant_id = param.tenant_id_;
  uint64_t compat_version = 0;
  LOG_INFO("create ddl task", K(param));
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_1_0_0 && GCONF.in_upgrade_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("4.0 is being upgrade to 4.1, create_ddl_task not supported", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else {
    switch (param.type_) {
      case DDL_CREATE_INDEX:
        create_index_arg = static_cast<const obrpc::ObCreateIndexArg *>(param.ddl_arg_);
        if (OB_FAIL(create_build_index_task(proxy,
                                            param.src_table_schema_,
                                            param.dest_table_schema_,
                                            param.parallelism_,
                                            param.parent_task_id_,
                                            create_index_arg,
                                            *param.allocator_,
                                            task_record))) {
          LOG_WARN("fail to create build index task", K(ret));
        }
        break;
      case DDL_DROP_INDEX:
        // in this case, src_table_schema is data table, dest_table_schema is index table
        drop_index_arg = static_cast<const obrpc::ObDropIndexArg *>(param.ddl_arg_);
        if (OB_FAIL(create_drop_index_task(proxy,
                                           param.src_table_schema_,
                                           param.parent_task_id_,
                                           drop_index_arg,
                                           *param.allocator_,
                                           task_record))) {
          LOG_WARN("fail to create drop index task failed", K(ret));
        }
        break;
      case DDL_MODIFY_COLUMN:
      case DDL_ADD_PRIMARY_KEY:
      case DDL_ALTER_PRIMARY_KEY:
      case DDL_ALTER_PARTITION_BY:
      case DDL_CONVERT_TO_CHARACTER:
      case DDL_TABLE_REDEFINITION:
      case DDL_DIRECT_LOAD:
        if (OB_FAIL(create_table_redefinition_task(proxy,
                                                   param.type_,
                                                   param.src_table_schema_,
                                                   param.dest_table_schema_,
                                                   param.parallelism_,
                                                   static_cast<const obrpc::ObAlterTableArg *>(param.ddl_arg_),
                                                   *param.allocator_,
                                                   task_record))) {
          LOG_WARN("fail to create table redefinition task", K(ret));
        }
        break;
      case DDL_DROP_PRIMARY_KEY:
        alter_table_arg = static_cast<const obrpc::ObAlterTableArg *>(param.ddl_arg_);
        if (OB_FAIL(create_drop_primary_key_task(proxy,
                                                 param.type_,
                                                 param.src_table_schema_,
                                                 param.dest_table_schema_,
                                                 param.parallelism_,
                                                 alter_table_arg,
                                                 *param.allocator_,
                                                 task_record))) {
          LOG_WARN("fail to create table redefinition task", K(ret));
        }
        break;
      case DDL_CHECK_CONSTRAINT:
      case DDL_FOREIGN_KEY_CONSTRAINT:
      case DDL_ADD_NOT_NULL_COLUMN:
        if (OB_FAIL(create_constraint_task(proxy,
                                           param.src_table_schema_,
                                           param.object_id_,
                                           param.type_,
                                           param.schema_version_,
                                           static_cast<const obrpc::ObAlterTableArg *>(param.ddl_arg_),
                                           param.parent_task_id_,
                                           *param.allocator_,
                                           task_record))) {
          LOG_WARN("fail to create constraint task failed", K(ret));
        }
        break;
      case DDL_DROP_COLUMN:
      case DDL_ADD_COLUMN_OFFLINE:
      case DDL_COLUMN_REDEFINITION:
        if (OB_FAIL(create_column_redefinition_task(proxy,
                                                    param.type_,
                                                    param.src_table_schema_,
                                                    param.dest_table_schema_,
                                                    param.parallelism_,
                                                    static_cast<const obrpc::ObAlterTableArg *>(param.ddl_arg_),
                                                    *param.allocator_,
                                                    task_record))) {
          LOG_WARN("fail to create column redefinition task", K(ret));
        }
        break;
      case DDL_MODIFY_AUTO_INCREMENT:
        if (OB_FAIL(create_modify_autoinc_task(proxy,
                                               param.tenant_id_,
                                               param.src_table_schema_->get_table_id(),
                                               param.schema_version_,
                                               static_cast<const obrpc::ObAlterTableArg *>(param.ddl_arg_),
                                               *param.allocator_,
                                               task_record))) {
          LOG_WARN("fail to create modify autoinc task", K(ret));
        }
        break;
      case DDL_DROP_DATABASE:
      case DDL_DROP_TABLE:
      case DDL_TRUNCATE_TABLE:
      case DDL_DROP_PARTITION:
      case DDL_DROP_SUB_PARTITION:
      case DDL_TRUNCATE_PARTITION:
      case DDL_TRUNCATE_SUB_PARTITION: {
        if (OB_FAIL(create_ddl_retry_task(proxy,
                                          param.tenant_id_,
                                          param.object_id_,
                                          param.schema_version_,
                                          param.type_,
                                          param.ddl_arg_,
                                          *param.allocator_,
                                          task_record))) {
          LOG_WARN("fail to create ddl retry task", K(ret));
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("error unexpected, ddl type is not supported", K(ret), K(param.type_));
    }
    LOG_INFO("create ddl task", K(ret), K(param), K(task_record));
  }
  return ret;
}

int ObDDLScheduler::add_sys_task(ObDDLTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(task));
  } else {
    const int64_t parent_task_id = task->get_parent_task_id();
    const uint64_t tenant_id = task->get_tenant_id();
    if (0 == parent_task_id) {
      share::ObSysTaskStat sys_task_status;
      sys_task_status.start_time_ = ObTimeUtility::fast_current_time();
      sys_task_status.task_id_ = *ObCurTraceId::get_trace_id();
      sys_task_status.tenant_id_ = tenant_id;
      sys_task_status.task_type_ = DDL_TASK;
      if (OB_FAIL(SYS_TASK_STATUS_MGR.add_task(sys_task_status))) {
        if (OB_ENTRY_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("sys task already exist", K(sys_task_status.task_id_));
        } else {
          LOG_WARN("add task failed", K(ret));
        }
      } else {
        task->set_sys_task_id(sys_task_status.task_id_);
        LOG_INFO("add sys task", K(sys_task_status.task_id_));
      }
    }
  }

  return ret;
}

int ObDDLScheduler::remove_sys_task(ObDDLTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(task));
  } else {
    const ObCurTraceId::TraceId &task_id = task->get_sys_task_id();
    if (!task_id.is_invalid()) {
      if (OB_FAIL(SYS_TASK_STATUS_MGR.del_task(task_id))) {
        LOG_WARN("del task failed", K(ret), K(task_id));
      } else {
        LOG_INFO("remove sys task", K(task_id));
      }
    }
  }
  return ret;
}
int ObDDLScheduler::prepare_alter_table_arg(const ObPrepareAlterTableArgParam &param,
                                            const ObTableSchema *target_table_schema,
                                            obrpc::ObAlterTableArg &alter_table_arg)
{
  int ret = OB_SUCCESS;
  AlterTableSchema *alter_table_schema = &alter_table_arg.alter_table_schema_;
  alter_table_schema->alter_type_ = OB_DDL_ALTER_TABLE;
  const ObString &ddl_stmt_str = param.ddl_stmt_str_;
  if (OB_UNLIKELY(!param.is_valid() || OB_ISNULL(target_table_schema))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param), KP(target_table_schema));
  } else if (FALSE_IT(alter_table_arg.session_id_ = param.session_id_)) {
    // do nothing
  } else if (FALSE_IT(alter_table_arg.sql_mode_ = param.sql_mode_)) {
    // do nothing
  } else if (FALSE_IT(alter_table_arg.ddl_stmt_str_.assign_ptr(ddl_stmt_str.ptr(), ddl_stmt_str.length()))) {
    // do nothing
  } else if (OB_FAIL(alter_table_arg.tz_info_.assign(param.tz_info_))) {
    LOG_WARN("tz_info assign failed", K(ret));
  } else if (OB_FAIL(alter_table_arg.tz_info_wrap_.deep_copy(param.tz_info_wrap_))) {
    LOG_WARN("failed to deep_copy tz info wrap", K(ret), "tz_info_wrap", param.tz_info_wrap_);
  } else if (OB_FAIL(alter_table_arg.set_nls_formats(param.nls_formats_))) {
    LOG_WARN("failed to set_nls_formats", K(ret));
  } else if (OB_FAIL(alter_table_schema->assign(*target_table_schema))) {
    LOG_WARN("failed to assign alter table schema", K(ret));
  } else if (OB_FAIL(alter_table_schema->set_origin_table_name(param.orig_table_name_))) {
    LOG_WARN("failed to set origin table name", K(ret));
  } else if (OB_FAIL(alter_table_schema->set_origin_database_name(param.orig_database_name_))) {
    LOG_WARN("failed to set origin database name", K(ret));
  } else if (OB_FAIL(alter_table_schema->set_table_name(target_table_schema->get_table_name_str()))) {
    LOG_WARN("failed to set table name", K(ret));
  } else if (OB_FAIL(alter_table_schema->set_database_name(param.target_database_name_))) {
    LOG_WARN("failed to set database name", K(ret));
  } else if (!target_table_schema->is_mysql_tmp_table()
            && OB_FAIL(alter_table_schema->alter_option_bitset_.add_member(obrpc::ObAlterTableArg::SESSION_ID))) {
    LOG_WARN("failed to add member SESSION_ID for alter table schema", K(ret), K(alter_table_arg));
  } else if (OB_FAIL(alter_table_schema->alter_option_bitset_.add_member(obrpc::ObAlterTableArg::TABLE_NAME))) {
    LOG_WARN("failed to add member TABLE_NAME for alter table schema", K(ret), K(alter_table_arg));
  } else {
    LOG_DEBUG("alter table arg preparation complete!", K(ret), K(*alter_table_schema));
  }
  return ret;
}

int ObDDLScheduler::abort_redef_table(const int64_t task_id)
{
  int ret = OB_SUCCESS;
  share::ObTaskId trace_id;
  ObDDLTask *ddl_task = nullptr;
  if (OB_UNLIKELY(task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_id));
  } else if (OB_FAIL(task_queue_.abort_task(task_id, root_service_->get_sql_proxy()))) {
    LOG_WARN("abort redef table task failed", K(ret));
  }
  return ret;
}

int ObDDLScheduler::copy_table_dependents(const int64_t task_id,
                                          const uint64_t tenant_id,
                                          const bool is_copy_constraints,
                                          const bool is_copy_indexes,
                                          const bool is_copy_triggers,
                                          const bool is_copy_foreign_keys,
                                          const bool is_ignore_errors)
{
  int ret = OB_SUCCESS;
  ObDDLTask *task = nullptr;
  int64_t table_task_status = 0;
  int64_t table_execution_id = 0;
  int64_t pos = 0;
  ObString message;
  ObMySQLTransaction trans;
  if (OB_UNLIKELY(0 >= task_id || OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_id), K(tenant_id));
  } else if (OB_FAIL(trans.start(&root_service_->get_sql_proxy(), tenant_id))) {
    LOG_WARN("start transaction failed", K(ret));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::select_for_update(trans,
                                                                tenant_id,
                                                                task_id,
                                                                table_task_status,
                                                                table_execution_id))) {
    LOG_WARN("select for update failed", K(ret), K(tenant_id), K(task_id));
  } else {
    HEAP_VAR(ObTableRedefinitionTask, redefinition_task) {
      ObDDLTaskRecord task_record;
      common::ObArenaAllocator allocator(lib::ObLabel("copy_table_dep"));
      task_record.reset();
      if (OB_FAIL(task_queue_.modify_task(task_id, [&task_record, &allocator](ObDDLTask &task) -> int {
            int ret = OB_SUCCESS;
            ObTableRedefinitionTask *table_redefinition_task = static_cast<ObTableRedefinitionTask*>(&task);
            if (OB_UNLIKELY(!table_redefinition_task->is_valid())) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("table rdefinition task is not valid", K(ret));
            } else if (OB_FAIL(table_redefinition_task->convert_to_record(task_record, allocator))) {
              LOG_WARN("convert to ddl task record failed", K(ret), K(*table_redefinition_task));
            }
            return ret;
          }))) {
        LOG_WARN("failed to modify task", K(ret));
      } else if (OB_FAIL(redefinition_task.init(task_record))) {
        LOG_WARN("init table redefinition task failed", K(ret));
      } else if (OB_FAIL(redefinition_task.set_trace_id(task_record.trace_id_))) {
        LOG_WARN("set trace id failed", K(ret));
      } else {
        redefinition_task.set_is_copy_constraints(is_copy_constraints);
        redefinition_task.set_is_copy_indexes(is_copy_indexes);
        redefinition_task.set_is_copy_triggers(is_copy_triggers);
        redefinition_task.set_is_copy_foreign_keys(is_copy_foreign_keys);
        redefinition_task.set_is_ignore_errors(is_ignore_errors);
        if (OB_FAIL(redefinition_task.convert_to_record(task_record, allocator))) {
          LOG_WARN("convert to ddl task record failed", K(ret), K(redefinition_task));
        } else if (OB_UNLIKELY(!task_record.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ddl task record is invalid", K(ret), K(task_record));
        } else {
          message.assign(task_record.message_.ptr(), task_record.message_.length());
          if (OB_FAIL(ObDDLTaskRecordOperator::update_message(trans, tenant_id, task_id, message))) {
            LOG_WARN("update task message failed", K(ret), K(tenant_id), K(task_id), K(message));
          } else {
            bool commit = (OB_SUCCESS == ret);
            int tmp_ret = trans.end(commit);
            if (OB_SUCCESS != tmp_ret) {
              ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(task_queue_.update_task_copy_deps_setting(task_id,
                                                                    is_copy_constraints,
                                                                    is_copy_indexes,
                                                                    is_copy_triggers,
                                                                    is_copy_foreign_keys,
                                                                    is_ignore_errors))) {
                LOG_WARN("update task process setting failed", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLScheduler::finish_redef_table(const int64_t task_id, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObDDLTask *task = nullptr;
  int64_t table_task_status = 0;
  int64_t table_execution_id = 0;
  int64_t pos = 0;
  ObString message;
  ObMySQLTransaction trans;
  if (OB_UNLIKELY(0 >= task_id || OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_id), K(tenant_id));
  } else if (OB_FAIL(trans.start(&root_service_->get_sql_proxy(), tenant_id))) {
    LOG_WARN("start transaction failed", K(ret));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::select_for_update(trans,
                                                                tenant_id,
                                                                task_id,
                                                                table_task_status,
                                                                table_execution_id))) {
    LOG_WARN("select for update failed", K(ret), K(tenant_id), K(task_id));
  } else {
    HEAP_VAR(ObTableRedefinitionTask, redefinition_task) {
      ObDDLTaskRecord task_record;
      common::ObArenaAllocator allocator(lib::ObLabel("finish_redef"));
      task_record.reset();
      if (OB_FAIL(task_queue_.modify_task(task_id, [&task_record, &allocator](ObDDLTask &task) -> int {
            int ret = OB_SUCCESS;
            ObTableRedefinitionTask *table_redefinition_task = static_cast<ObTableRedefinitionTask*>(&task);
            if (OB_UNLIKELY(!table_redefinition_task->is_valid())) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("table rdefinition task is not valid", K(ret));
            } else if (OB_FAIL(table_redefinition_task->convert_to_record(task_record, allocator))) {
              LOG_WARN("convert to ddl task record failed", K(ret), K(*table_redefinition_task));
            }
            return ret;
          }))) {
        LOG_WARN("failed to modify task", K(ret));
      } else if (OB_FAIL(redefinition_task.init(task_record))) {
        LOG_WARN("init table redefinition task failed", K(ret));
      } else if (OB_FAIL(redefinition_task.set_trace_id(task_record.trace_id_))) {
        LOG_WARN("set trace id failed", K(ret));
      } else {
        redefinition_task.set_is_do_finish(true);
        if (OB_FAIL(redefinition_task.convert_to_record(task_record, allocator))) {
          LOG_WARN("convert to ddl task record failed", K(ret), K(redefinition_task));
        } else if (OB_UNLIKELY(!task_record.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ddl task record is invalid", K(ret), K(task_record));
        } else {
          message.assign(task_record.message_.ptr(), task_record.message_.length());
          if (OB_FAIL(ObDDLTaskRecordOperator::update_message(trans, tenant_id, task_id, message))) {
            LOG_WARN("update task message failed", K(ret), K(tenant_id), K(task_id), K(message));
          } else {
            bool commit = (OB_SUCCESS == ret);
            int tmp_ret = trans.end(commit);
            if (OB_SUCCESS != tmp_ret) {
              ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(task_queue_.update_task_process_schedulable(task_id))) {
                LOG_WARN("update task process setting failed", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLScheduler::start_redef_table(const obrpc::ObStartRedefTableArg &arg, obrpc::ObStartRedefTableRes &res)
{
  int ret = OB_SUCCESS;
  ObDDLTaskRecord task_record;
  ObSchemaGetterGuard orig_schema_guard;
  ObSchemaGetterGuard target_schema_guard;
  ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  const int64_t tenant_id = arg.orig_tenant_id_;
  const int64_t table_id = arg.orig_table_id_;
  const int64_t dest_tenant_id = arg.target_tenant_id_;
  const int64_t dest_table_id = arg.target_table_id_;
  const ObTableSchema *orig_table_schema = nullptr;
  const ObTableSchema *target_table_schema = nullptr;
  const ObDatabaseSchema *orig_database_schema = nullptr;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, orig_schema_guard))) {
    LOG_WARN("fail to get orig schema guard with version in inner table", K(ret), K(tenant_id));
  } else if (OB_FAIL(orig_schema_guard.get_table_schema(tenant_id, table_id, orig_table_schema))) {
    LOG_WARN("fail to get orig table schema", K(ret));
  } else if (OB_ISNULL(orig_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("orig_table_schema is nullptr", K(ret));
  } else if (OB_FAIL(orig_schema_guard.get_database_schema(tenant_id, orig_table_schema->get_database_id(), orig_database_schema))) {
    LOG_WARN("fail to get orig database schema", K(ret));
  } else if (OB_ISNULL(orig_database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig_database_schema is nullptr", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(dest_tenant_id, target_schema_guard))) {
    LOG_WARN("fail to get orig schema guard with version in inner table", K(ret), K(dest_tenant_id));
  } else if (OB_FAIL(target_schema_guard.get_table_schema(dest_tenant_id, dest_table_id, target_table_schema))) {
    LOG_WARN("fail to get target table schema", K(ret));
  } else if (OB_ISNULL(target_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("target_table_schema is nullptr", K(ret));
  } else {
    HEAP_VAR(obrpc::ObAlterTableArg, alter_table_arg) {
      ObPrepareAlterTableArgParam param;
      if (OB_FAIL(param.init(arg.session_id_,
                            arg.sql_mode_,
                            arg.ddl_stmt_str_,
                            orig_table_schema->get_table_name_str(),
                            orig_database_schema->get_database_name_str(),
                            orig_database_schema->get_database_name_str(),
                            arg.tz_info_,
                            arg.tz_info_wrap_,
                            arg.nls_formats_))) {
        LOG_WARN("param init failed", K(ret));
      } else if (OB_FAIL(prepare_alter_table_arg(param, target_table_schema, alter_table_arg))) {
        LOG_WARN("failed to build alter table arg", K(ret));
      } else {
        common::ObArenaAllocator allocator(lib::ObLabel("StartRedefTable"));
        ObCreateDDLTaskParam param(tenant_id,
                                      arg.ddl_type_,
                                      orig_table_schema,
                                      target_table_schema,
                                      orig_table_schema->get_table_id(),
                                      orig_table_schema->get_schema_version(),
                                      arg.parallelism_,
                                      &allocator,
                                      &alter_table_arg,
                                      0);
        if (OB_FAIL(create_ddl_task(param, root_service_->get_sql_proxy(), task_record)))  {
          LOG_WARN("submit ddl task failed", K(ret), K(alter_table_arg));
        } else if (OB_FAIL(schedule_ddl_task(task_record))) {
          LOG_WARN("fail to schedule ddl task", K(ret), K(task_record));
        } else {
          res.task_id_ = task_record.task_id_;
          res.tenant_id_ = task_record.tenant_id_;
          res.schema_version_ = task_record.schema_version_;
        }
      }
    }
  }
  return ret;
}

int ObDDLScheduler::create_build_index_task(
    common::ObISQLClient &proxy,
    const ObTableSchema *data_table_schema,
    const ObTableSchema *index_schema,
    const int64_t parallelism,
    const int64_t parent_task_id,
    const obrpc::ObCreateIndexArg *create_index_arg,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  SMART_VAR(ObIndexBuildTask, index_task) {
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_ISNULL(create_index_arg) || OB_ISNULL(data_table_schema) || OB_ISNULL(index_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(create_index_arg), K(data_table_schema), K(index_schema));
    } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), task_id))) {
      LOG_WARN("fetch new task id failed", K(ret));
    } else if (OB_FAIL(index_task.init(data_table_schema->get_tenant_id(),
                                      task_id,
                                      data_table_schema,
                                      index_schema,
                                      index_schema->get_schema_version(),
                                      parallelism,
                                      *create_index_arg,
                                      parent_task_id))) {
      LOG_WARN("init global index task failed", K(ret), K(data_table_schema), K(index_schema));
    } else if (OB_FAIL(index_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
      LOG_WARN("set trace id failed", K(ret));
    } else if (OB_FAIL(insert_task_record(proxy, index_task, allocator, task_record))) {
      LOG_WARN("fail to insert task record", K(ret));
    }

    LOG_INFO("ddl_scheduler create build index task finished", K(ret), K(index_task));
  }
  return ret;
}

int ObDDLScheduler::create_drop_index_task(
    common::ObISQLClient &proxy,
    const share::schema::ObTableSchema *index_schema,
    const int64_t parent_task_id,
    const obrpc::ObDropIndexArg *drop_index_arg,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObDropIndexTask index_task;
  int64_t task_id = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(index_schema) || OB_ISNULL(drop_index_arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(index_schema), KP(drop_index_arg));
  } else if (index_schema->is_domain_index()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("drop domain index is not supported", K(ret));
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else {
    const uint64_t data_table_id = index_schema->get_data_table_id();
    const uint64_t index_table_id = index_schema->get_table_id();
    if (OB_FAIL(index_task.init(index_schema->get_tenant_id(),
                                task_id,
                                data_table_id,
                                index_table_id,
                                index_schema->get_schema_version(),
                                parent_task_id,
                                *drop_index_arg))) {
      LOG_WARN("init drop index task failed", K(ret), K(data_table_id), K(index_table_id));
    } else if (OB_FAIL(index_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
      LOG_WARN("set trace id failed", K(ret));
    } else if (OB_FAIL(insert_task_record(proxy, index_task, allocator, task_record))) {
      LOG_WARN("fail to insert task record", K(ret));
    }
  }
  LOG_INFO("ddl_scheduler create drop index task finished", K(ret), K(index_task));
  return ret;
}
int ObDDLScheduler::create_constraint_task(
    common::ObISQLClient &proxy,
    const share::schema::ObTableSchema *table_schema,
    const int64_t constraint_id,
    const ObDDLType ddl_type,
    const int64_t schema_version,
    const obrpc::ObAlterTableArg *arg,
    const int64_t parent_task_id,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObConstraintTask, constraint_task) {
  int64_t task_id = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == table_schema || OB_INVALID_ID == constraint_id || schema_version <= 0
                         || nullptr == arg || !arg->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_schema), K(constraint_id), K(schema_version), K(arg));
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else if (OB_FAIL(constraint_task.init(task_id, table_schema, constraint_id, ddl_type, schema_version, *arg, parent_task_id))) {
    LOG_WARN("init constraint task failed", K(ret), K(table_schema), K(constraint_id));
  } else if (OB_FAIL(constraint_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(insert_task_record(proxy, constraint_task, allocator, task_record))) {
    LOG_WARN("fail to insert task record", K(ret));
  }
  LOG_INFO("ddl_scheduler create constraint task finished", K(ret), K(constraint_task));
  }
  return ret;
}

int ObDDLScheduler::create_table_redefinition_task(
    common::ObISQLClient &proxy,
    const share::ObDDLType &type,
    const share::schema::ObTableSchema *src_schema,
    const share::schema::ObTableSchema *dest_schema,
    const int64_t parallelism,
    const obrpc::ObAlterTableArg *alter_table_arg,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  SMART_VAR(ObTableRedefinitionTask, redefinition_task) {
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObDDLScheduler has not been inited", K(ret));
    } else if (OB_ISNULL(alter_table_arg) || OB_ISNULL(src_schema) || OB_ISNULL(dest_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), KP(alter_table_arg), KP(src_schema), KP(dest_schema));
    } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), task_id))) {
      LOG_WARN("fetch new task id failed", K(ret));
    } else if (OB_FAIL(redefinition_task.init(src_schema->get_tenant_id(),
                                              task_id,
                                              type,
                                              src_schema->get_table_id(),
                                              dest_schema->get_table_id(),
                                              dest_schema->get_schema_version(),
                                              parallelism,
                                              *alter_table_arg))) {
      LOG_WARN("fail to init redefinition task", K(ret));
    } else if (OB_FAIL(redefinition_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
      LOG_WARN("set trace id failed", K(ret));
    } else if (OB_FAIL(insert_task_record(proxy, redefinition_task, allocator, task_record))) {
      LOG_WARN("fail to insert task record", K(ret));
    }
    LOG_INFO("ddl_scheduler create table redefinition task finished", K(ret), K(redefinition_task), K(common::lbt()));
  }
  return ret;
}

int ObDDLScheduler::create_drop_primary_key_task(
    common::ObISQLClient &proxy,
    const share::ObDDLType &type,
    const ObTableSchema *src_schema,
    const ObTableSchema *dest_schema,
    const int64_t parallelism,
    const obrpc::ObAlterTableArg *alter_table_arg,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  SMART_VAR(ObDropPrimaryKeyTask, drop_pk_task) {
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_ISNULL(alter_table_arg) || OB_ISNULL(src_schema) || OB_ISNULL(dest_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(alter_table_arg), KP(src_schema), KP(dest_schema));
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else if (OB_FAIL(drop_pk_task.init(src_schema->get_tenant_id(),
                                       task_id,
                                       type,
                                       src_schema->get_table_id(),
                                       dest_schema->get_table_id(),
                                       dest_schema->get_schema_version(),
                                       parallelism,
                                       *alter_table_arg))) {
    LOG_WARN("fail to init redefinition task", K(ret));
  } else if (OB_FAIL(drop_pk_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(insert_task_record(proxy, drop_pk_task, allocator, task_record))) {
    LOG_WARN("fail to insert task record", K(ret));
  }
  LOG_INFO("ddl_scheduler create drop primary key task finished", K(ret), K(drop_pk_task));
  }
  return ret;
}

int ObDDLScheduler::create_column_redefinition_task(
    common::ObISQLClient &proxy,
    const share::ObDDLType &type,
    const share::schema::ObTableSchema *src_schema,
    const share::schema::ObTableSchema *dest_schema,
    const int64_t parallelism,
    const obrpc::ObAlterTableArg *alter_table_arg,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  SMART_VAR(ObColumnRedefinitionTask, redefinition_task) {
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_ISNULL(alter_table_arg) || OB_ISNULL(src_schema) || OB_ISNULL(dest_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(alter_table_arg), KP(src_schema), KP(dest_schema));
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else if (OB_FAIL(redefinition_task.init(src_schema->get_tenant_id(),
                                            task_id,
                                            type,
                                            src_schema->get_table_id(),
                                            dest_schema->get_table_id(),
                                            dest_schema->get_schema_version(),
                                            parallelism,
                                            *alter_table_arg))) {
    LOG_WARN("fail to init redefinition task", K(ret));
  } else if (OB_FAIL(redefinition_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(insert_task_record(proxy, redefinition_task, allocator, task_record))) {
    LOG_WARN("fail to insert task record", K(ret));
  }
  LOG_INFO("ddl_scheduler create column redefinition task finished", K(ret), K(redefinition_task));
  }
  return ret;
}

int ObDDLScheduler::create_modify_autoinc_task(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const int64_t table_id,
    const int64_t schema_version,
    const obrpc::ObAlterTableArg *arg,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObModifyAutoincTask, modify_autoinc_task) {
    int64_t task_id = 0;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id
                          || schema_version <= 0 || nullptr == arg || !arg->is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_id), K(schema_version), K(arg));
    } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), task_id))) {
      LOG_WARN("fetch new task id failed", K(ret));
    } else if (OB_FAIL(modify_autoinc_task.init(tenant_id, task_id, table_id, schema_version, *arg))) {
      LOG_WARN("init global index task failed", K(ret), K(table_id), K(arg));
    } else if (OB_FAIL(modify_autoinc_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
      LOG_WARN("set trace id failed", K(ret));
    } else if (OB_FAIL(insert_task_record(proxy, modify_autoinc_task, allocator, task_record))) {
      LOG_WARN("fail to insert task record", K(ret));
    }
    LOG_INFO("ddl_scheduler create modify autoinc task finished", K(ret), K(modify_autoinc_task));
  }
  return ret;
}

// for drop database, drop table, drop partition, drop subpartition, 
// truncate table, truncate partition and truncate sub partition.
int ObDDLScheduler::create_ddl_retry_task(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const uint64_t object_id,
    const int64_t schema_version,
    const share::ObDDLType &type,
    const obrpc::ObDDLArg *arg,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObDDLRetryTask ddl_retry_task;
  int64_t task_id = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == object_id
                         || schema_version <= 0) || OB_ISNULL(arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(object_id), K(schema_version), K(arg));
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else if (OB_FAIL(ddl_retry_task.init(tenant_id, task_id, object_id, schema_version, type, arg))) {
    LOG_WARN("init ddl retry task failed", K(ret), K(arg));
  } else if (OB_FAIL(ddl_retry_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(insert_task_record(proxy, ddl_retry_task, allocator, task_record))) {
    LOG_WARN("fail to insert task record", K(ret));
  }
  LOG_INFO("ddl_scheduler create ddl retry task finished", K(ret), K(ddl_retry_task));
  return ret;
}

int ObDDLScheduler::insert_task_record(
    common::ObISQLClient &proxy,
    ObDDLTask &ddl_task,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  task_record.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ddl_task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_task));
  } else if (OB_FAIL(ddl_task.convert_to_record(task_record, allocator))) {
    LOG_WARN("convert to ddl task record failed", K(ret), K(ddl_task));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::insert_record(proxy, task_record))) {
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      ret = OB_ENTRY_EXIST;
    } else {
      LOG_WARN("insert ddl task record failed", K(ret), K(task_record));
    }
  }
  return ret;
}

int ObDDLScheduler::recover_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObArray<ObDDLTaskRecord> task_records;
    ObArenaAllocator allocator(lib::ObLabel("DdlTasRecord"));
    share::schema::ObMultiVersionSchemaService &schema_service = root_service_->get_schema_service();
    if (OB_FAIL(ObDDLTaskRecordOperator::get_all_record(root_service_->get_sql_proxy(), allocator, task_records))) {
      LOG_WARN("get all task records failed", K(ret));
    }
    LOG_INFO("start processing ddl recovery", K(task_records));
    for (int64_t i = 0; OB_SUCC(ret) && i < task_records.count(); ++i) {
      const ObDDLTaskRecord &cur_record = task_records.at(i);
      int64_t tenant_schema_version = 0;
      int64_t table_task_status = 0;
      int64_t execution_id = -1;
      ObMySQLTransaction trans;
      if (OB_FAIL(schema_service.get_tenant_schema_version(cur_record.tenant_id_, tenant_schema_version))) {
        LOG_WARN("failed to get tenant schema version", K(ret), K(cur_record));
      } else if (tenant_schema_version < cur_record.schema_version_) {
        // schema has not publish, by pass now
      } else if (OB_FAIL(trans.start(&root_service_->get_sql_proxy(), cur_record.tenant_id_))) {
        LOG_WARN("start transaction failed", K(ret));
      } else if (OB_FAIL(ObDDLTaskRecordOperator::select_for_update(trans,
                                                                    cur_record.tenant_id_,
                                                                    cur_record.task_id_,
                                                                    table_task_status,
                                                                    execution_id))) {
        LOG_WARN("select for update failed", K(ret), K(cur_record));
      } else if (OB_FAIL(schedule_ddl_task(cur_record))) {
        LOG_WARN("failed to schedule ddl task", K(ret), K(cur_record));
      }
      bool commit = (OB_SUCCESS == ret);
      int tmp_ret = trans.end(commit);
      if (OB_SUCCESS != tmp_ret) {
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      }
      ret = OB_SUCCESS; // ignore ret
    }
  }
  return ret;
}

int ObDDLScheduler::remove_inactive_ddl_task()
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> remove_task_ids;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_FAIL(manager_reg_heart_beat_task_.get_inactive_ddl_task_ids(remove_task_ids))){
      LOG_WARN("failed to check register time", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < remove_task_ids.size(); i++) {
        int64_t remove_task_id = 0;
        if (OB_FAIL(remove_task_ids.at(i, remove_task_id))) {
          LOG_WARN("get remove task id fail", K(ret));
        } else if (OB_FAIL(abort_redef_table(remove_task_id))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            LOG_INFO("abort_redef_table() success, but manager_reg_heart_beat_task last deletion failed", K(ret));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("remove ddl task fail", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(manager_reg_heart_beat_task_.remove_task(remove_task_id))) {
            LOG_WARN("RegTaskTime map erase remove_task_id fail", K(ret));
          }
        }
      }
      LOG_INFO("remove all timeout ddl task succeed");
    }
  }
  return ret;
}

int ObDDLScheduler::schedule_ddl_task(const ObDDLTaskRecord &record)
{
  int ret = OB_SUCCESS;
  ObTraceIDGuard guard(record.trace_id_);
  if (OB_UNLIKELY(!record.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl task record is invalid", K(ret), K(record));
  } else {
    switch (record.ddl_type_) {
      case ObDDLType::DDL_CREATE_INDEX: {
        if (OB_FAIL(schedule_build_index_task(record))) {
          LOG_WARN("schedule global index task failed", K(ret), K(record));
        }
        break;
      }
      case ObDDLType::DDL_DROP_INDEX:
        if (OB_FAIL(schedule_drop_index_task(record))) {
          LOG_WARN("schedule drop index task failed", K(ret));
        }
        break;
      case DDL_DROP_PRIMARY_KEY:
        if (OB_FAIL(schedule_drop_primary_key_task(record))) {
          LOG_WARN("schedule drop primary key task failed", K(ret));
        }
        break;
      case DDL_MODIFY_COLUMN:
      case DDL_ADD_PRIMARY_KEY:
      case DDL_ALTER_PRIMARY_KEY:
      case DDL_ALTER_PARTITION_BY:
      case DDL_CONVERT_TO_CHARACTER:
      case DDL_TABLE_REDEFINITION:
      case DDL_DIRECT_LOAD:
        if (OB_FAIL(schedule_table_redefinition_task(record))) {
          LOG_WARN("schedule table redefinition task failed", K(ret));
        }
        break;
      case DDL_DROP_COLUMN:
      case DDL_ADD_COLUMN_OFFLINE:
      case DDL_COLUMN_REDEFINITION:
        if(OB_FAIL(schedule_column_redefinition_task(record))) {
          LOG_WARN("schedule column redefinition task failed", K(ret));
        }
        break;

      case DDL_CHECK_CONSTRAINT:
      case DDL_FOREIGN_KEY_CONSTRAINT:
      case DDL_ADD_NOT_NULL_COLUMN:
        if (OB_FAIL(schedule_constraint_task(record))) {
          LOG_WARN("schedule constraint task failed", K(ret));
        }
        break;
      case DDL_MODIFY_AUTO_INCREMENT:
        if (OB_FAIL(schedule_modify_autoinc_task(record))) {
          LOG_WARN("schedule modify autoinc task failed", K(ret));
        }
        break;
      case DDL_DROP_DATABASE:
      case DDL_DROP_TABLE:
      case DDL_TRUNCATE_TABLE:
      case DDL_DROP_PARTITION:
      case DDL_DROP_SUB_PARTITION:
      case DDL_TRUNCATE_PARTITION:
      case DDL_TRUNCATE_SUB_PARTITION:
        if (OB_FAIL(schedule_ddl_retry_task(record))) {
          LOG_WARN("schedule ddl retry task failed", K(ret));
        }
        break;
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported task type", K(ret), K(record));
        break;
      }
    }
    LOG_INFO("schedule ddl task", K(ret), K(record));
  }
  return ret;
}

int ObDDLScheduler::schedule_build_index_task(
    const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObIndexBuildTask *build_index_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(build_index_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else {
    if (OB_FAIL(build_index_task->init(task_record))) {
      LOG_WARN("init global_index_task failed", K(ret), K(task_record));
    } else if (OB_FAIL(build_index_task->set_trace_id(task_record.trace_id_))) {
      LOG_WARN("init build index task failed", K(ret));
    } else if (OB_FAIL(inner_schedule_ddl_task(build_index_task))) {
      if (OB_ENTRY_EXIST != ret) {
        LOG_WARN("inner schedule task failed", K(ret), K(*build_index_task));
      }
    }
  }
  if (OB_FAIL(ret) && nullptr != build_index_task) {
    build_index_task->~ObIndexBuildTask();
    allocator_.free(build_index_task);
    build_index_task = nullptr;
  }
  if (OB_ENTRY_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDDLScheduler::schedule_drop_primary_key_task(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObDropPrimaryKeyTask *drop_pk_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(drop_pk_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else if (OB_FAIL(drop_pk_task->ObTableRedefinitionTask::init(task_record))) {
    LOG_WARN("init drop primary key task failed", K(ret));
  } else if (OB_FAIL(drop_pk_task->set_trace_id(task_record.trace_id_))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(inner_schedule_ddl_task(drop_pk_task))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret), K(*drop_pk_task));
    }
  }
  if (OB_FAIL(ret) && nullptr != drop_pk_task) {
    drop_pk_task->~ObDropPrimaryKeyTask();
    allocator_.free(drop_pk_task);
    drop_pk_task = nullptr;
  }
  if (OB_ENTRY_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDDLScheduler::schedule_table_redefinition_task(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObTableRedefinitionTask *redefinition_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(redefinition_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else if (OB_FAIL(redefinition_task->init(task_record))) {
    LOG_WARN("init table redefinition task failed", K(ret));
  } else if (OB_FAIL(redefinition_task->set_trace_id(task_record.trace_id_))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(inner_schedule_ddl_task(redefinition_task))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret), K(*redefinition_task));
    }
  } else if (ObDDLType::DDL_DIRECT_LOAD == task_record.ddl_type_
            && OB_FAIL(manager_reg_heart_beat_task_.update_task_active_time(task_record.task_id_))) {
    LOG_WARN("register_task_time recover fail", K(ret));
  }
  if (OB_FAIL(ret) && nullptr != redefinition_task) {
    redefinition_task->~ObTableRedefinitionTask();
    allocator_.free(redefinition_task);
    redefinition_task = nullptr;
  }
  if (OB_ENTRY_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDDLScheduler::schedule_column_redefinition_task(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObColumnRedefinitionTask *redefinition_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(redefinition_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else if (OB_FAIL(redefinition_task->init(task_record))) {
    LOG_WARN("init column redefinition task failed", K(ret));
  } else if (OB_FAIL(redefinition_task->set_trace_id(task_record.trace_id_))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(inner_schedule_ddl_task(redefinition_task))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret), K(*redefinition_task));
    }
  }
  if (OB_FAIL(ret) && nullptr != redefinition_task) {
    redefinition_task->~ObColumnRedefinitionTask();
    allocator_.free(redefinition_task);
    redefinition_task = nullptr;
  }
  if (OB_ENTRY_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDDLScheduler::schedule_ddl_retry_task(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObDDLRetryTask *ddl_retry_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(ddl_retry_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else if (OB_FAIL(ddl_retry_task->init(task_record))) {
    LOG_WARN("init ddl retry task failed", K(ret));
  } else if (OB_FAIL(ddl_retry_task->set_trace_id(task_record.trace_id_))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(inner_schedule_ddl_task(ddl_retry_task))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret));
    }
  }
  if (OB_FAIL(ret) && nullptr != ddl_retry_task) {
    ddl_retry_task->~ObDDLRetryTask();
    allocator_.free(ddl_retry_task);
    ddl_retry_task = nullptr;
  }
  if (OB_ENTRY_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDDLScheduler::schedule_constraint_task(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObConstraintTask *constraint_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(constraint_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else if (OB_FAIL(constraint_task->init(task_record))) {
    LOG_WARN("init constraint task failed", K(ret));
  } else if (OB_FAIL(constraint_task->set_trace_id(task_record.trace_id_))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(inner_schedule_ddl_task(constraint_task))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret));
    }
  }
  if (OB_FAIL(ret) && nullptr != constraint_task) {
    constraint_task->~ObConstraintTask();
    allocator_.free(constraint_task);
    constraint_task = nullptr;
  }
  if (OB_ENTRY_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDDLScheduler::schedule_modify_autoinc_task(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObModifyAutoincTask *modify_autoinc_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(modify_autoinc_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else if (OB_FAIL(modify_autoinc_task->init(task_record))) {
    LOG_WARN("init constraint task failed", K(ret));
  } else if (OB_FAIL(modify_autoinc_task->set_trace_id(task_record.trace_id_))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(inner_schedule_ddl_task(modify_autoinc_task))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret));
    }
  }
  if (OB_FAIL(ret) && nullptr != modify_autoinc_task) {
    modify_autoinc_task->~ObModifyAutoincTask();
    allocator_.free(modify_autoinc_task);
    modify_autoinc_task = nullptr;
  }
  if (OB_ENTRY_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDDLScheduler::schedule_drop_index_task(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObDropIndexTask *drop_index_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(drop_index_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else if (OB_FAIL(drop_index_task->init(task_record))) {
    LOG_WARN("init drop index task failed", K(ret));
  } else if (OB_FAIL(drop_index_task->set_trace_id(task_record.trace_id_))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(inner_schedule_ddl_task(drop_index_task))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret));
    }
  }
  if (OB_FAIL(ret) && nullptr != drop_index_task) {
    drop_index_task->~ObDropIndexTask();
    allocator_.free(drop_index_task);
    drop_index_task = nullptr;
  }
  if (OB_ENTRY_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDDLScheduler::add_task_to_longops_mgr(ObDDLTask *ddl_task)
{
  int ret = OB_SUCCESS;
  ObLongopsMgr &longops_mgr = ObLongopsMgr::get_instance();
  ObDDLLongopsStat *longops_stat = nullptr;
  bool registered = false;
  if (OB_ISNULL(ddl_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ddl_task));
  } else if (ddl_task->support_longops_monitoring()) {
    if (OB_FAIL(longops_mgr.alloc_longops(longops_stat))) {
      LOG_WARN("failed to allocate longops stat", K(ret));
    } else if (OB_FAIL(longops_stat->init(ddl_task))) {
      LOG_WARN("failed to init longops stat", K(ret), KPC(ddl_task));
    } else if (OB_FAIL(longops_mgr.register_longops(longops_stat))) {
      LOG_WARN("failed to register longops", K(ret));
      if (OB_ENTRY_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    } else {
      ddl_task->set_longops_stat(longops_stat);
      longops_stat = nullptr;
    }
    if (nullptr != longops_stat) {
      longops_mgr.free_longops(longops_stat);
    }
  }
  return ret;
}

int ObDDLScheduler::remove_ddl_task(ObDDLTask *ddl_task)
{
  int ret = OB_SUCCESS;
  ObLongopsMgr &longops_mgr = ObLongopsMgr::get_instance();
  if (OB_ISNULL(ddl_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ddl_task));
  } else if (OB_FAIL(task_queue_.remove_task(ddl_task))) {
    LOG_WARN("fail to remove task, which should not happen", K(ret), KPC(ddl_task));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (ddl_task->support_longops_monitoring() && OB_TMP_FAIL(longops_mgr.unregister_longops(ddl_task->get_longops_stat()))) {
      LOG_WARN("failed to unregister longops", K(tmp_ret));
    }
    remove_sys_task(ddl_task);
    free_ddl_task(ddl_task);
  }
  return ret;
}

int ObDDLScheduler::inner_schedule_ddl_task(ObDDLTask *ddl_task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ddl_task));
  } else if (OB_FAIL(task_queue_.push_task(ddl_task))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("push back task to task queue failed", K(ret));
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(add_sys_task(ddl_task))) {
      LOG_WARN("add sys task failed", K(tmp_ret));
    } else if (OB_TMP_FAIL(add_task_to_longops_mgr(ddl_task))) {
      LOG_WARN("add task to longops mgr failed", K(tmp_ret));
    }
    idler_.wakeup();
  }
  return ret;
}

int ObDDLScheduler::on_column_checksum_calc_reply(
    const common::ObTabletID &tablet_id,
    const ObDDLTaskKey &task_key,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive column checksum response", K(tablet_id), K(task_key), K(ret_code));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!(task_key.is_valid() && tablet_id.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_key), K(tablet_id), K(ret_code));
  } else if (OB_FAIL(task_queue_.modify_task(task_key, [&tablet_id, &ret_code](ObDDLTask &task) -> int {
        int ret = OB_SUCCESS;
        if (OB_UNLIKELY(ObDDLType::DDL_CREATE_INDEX != task.get_task_type())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ddl task type not global index", K(ret), K(task));
        } else if (OB_FAIL(reinterpret_cast<ObIndexBuildTask *>(&task)->update_column_checksum_calc_status(tablet_id, ret_code))) {
          LOG_WARN("update column checksum calc status failed", K(ret));
        }
        return ret;
      }))) {
    LOG_WARN("failed to modify task", K(ret));
  }
  return ret;
}

int ObDDLScheduler::on_sstable_complement_job_reply(
    const common::ObTabletID &tablet_id,
    const ObDDLTaskKey &task_key,
    const int64_t snapshot_version,
    const int64_t execution_id,
    const int ret_code,
    const ObDDLTaskInfo &addition_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!(task_key.is_valid() && snapshot_version > 0 && execution_id >= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_key), K(snapshot_version), K(execution_id), K(ret_code));
  } else if (OB_FAIL(task_queue_.modify_task(task_key, [&tablet_id, &snapshot_version, &execution_id, &ret_code, &addition_info](ObDDLTask &task) -> int {
        int ret = OB_SUCCESS;
        const int64_t task_type = task.get_task_type();
        switch (task_type) {
          case ObDDLType::DDL_CREATE_INDEX:
            if (OB_FAIL(static_cast<ObIndexBuildTask *>(&task)->update_complete_sstable_job_status(tablet_id, snapshot_version, execution_id, ret_code, addition_info))) {
              LOG_WARN("update complete sstable job status failed", K(ret));
            }
            break;
          case ObDDLType::DDL_DROP_PRIMARY_KEY:
            if (OB_FAIL(static_cast<ObDropPrimaryKeyTask *>(&task)->update_complete_sstable_job_status(tablet_id, snapshot_version, execution_id, ret_code, addition_info))) {
              LOG_WARN("update complete sstable job status", K(ret));
            }
            break;
          case ObDDLType::DDL_ADD_PRIMARY_KEY:
          case ObDDLType::DDL_ALTER_PRIMARY_KEY:
          case ObDDLType::DDL_ALTER_PARTITION_BY:
          case ObDDLType::DDL_MODIFY_COLUMN:
          case ObDDLType::DDL_CONVERT_TO_CHARACTER:
          case ObDDLType::DDL_TABLE_REDEFINITION:
          case ObDDLType::DDL_DIRECT_LOAD:
            if (OB_FAIL(static_cast<ObTableRedefinitionTask *>(&task)->update_complete_sstable_job_status(tablet_id, snapshot_version, execution_id, ret_code, addition_info))) {
              LOG_WARN("update complete sstable job status", K(ret));
            }
            break;
          case ObDDLType::DDL_CHECK_CONSTRAINT:
          case ObDDLType::DDL_FOREIGN_KEY_CONSTRAINT:
          case ObDDLType::DDL_ADD_NOT_NULL_COLUMN:
            if (OB_FAIL(static_cast<ObConstraintTask *>(&task)->update_check_constraint_finish(ret_code))) {
              LOG_WARN("update check constraint finish", K(ret));
            }
            break;
          case ObDDLType::DDL_DROP_COLUMN:
          case ObDDLType::DDL_ADD_COLUMN_OFFLINE:
          case ObDDLType::DDL_COLUMN_REDEFINITION:
            if (OB_FAIL(static_cast<ObColumnRedefinitionTask *>(&task)->update_complete_sstable_job_status(tablet_id, snapshot_version, execution_id, ret_code, addition_info))) {
              LOG_WARN("update complete sstable job status", K(ret), K(tablet_id), K(snapshot_version), K(ret_code));
            }
            break;
          default:
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not supported ddl task", K(ret), K(task));
            break;
        }
        return ret;
      }))) {
    LOG_WARN("failed to modify task", K(ret));
  }
  return ret;
}

int ObDDLScheduler::on_ddl_task_finish(
    const int64_t parent_task_id, 
    const ObDDLTaskKey &child_task_key, 
    const int ret_code,
    const ObCurTraceId::TraceId &parent_task_trace_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(parent_task_id <= 0 || !child_task_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(parent_task_id), K(child_task_key));
  } else {
    ObDDLTask *ddl_task = nullptr;
    if (OB_FAIL(task_queue_.modify_task(parent_task_id, [&child_task_key, &ret_code](ObDDLTask &task) -> int {
          ObDDLRedefinitionTask *redefinition_task = static_cast<ObDDLRedefinitionTask *>(&task);
          return redefinition_task->on_child_task_finish(child_task_key.object_id_, ret_code);
        }))) {
      LOG_WARN("failed to modify task", K(ret));
    }
  }
  return ret;
}

int ObDDLScheduler::notify_update_autoinc_end(const ObDDLTaskKey &task_key,
                                              const uint64_t autoinc_val,
                                              const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!task_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_key), K(ret_code));
  } else if (OB_FAIL(task_queue_.modify_task(task_key, [&autoinc_val, &ret_code](ObDDLTask &task) -> int {
        int ret = OB_SUCCESS;
        const int64_t task_type = task.get_task_type();
        switch (task_type) {
          case ObDDLType::DDL_MODIFY_COLUMN:
          case ObDDLType::DDL_ALTER_PARTITION_BY:
          case ObDDLType::DDL_TABLE_REDEFINITION:
          case ObDDLType::DDL_DIRECT_LOAD:
            if (OB_FAIL(static_cast<ObTableRedefinitionTask *>(&task)->notify_update_autoinc_finish(autoinc_val, ret_code))) {
              LOG_WARN("update complete sstable job status", K(ret));
            }
            break;
          case ObDDLType::DDL_MODIFY_AUTO_INCREMENT:
            if (OB_FAIL(static_cast<ObModifyAutoincTask *>(&task)->notify_update_autoinc_finish(autoinc_val, ret_code))) {
              LOG_WARN("update complete sstable job status", K(ret), K(ret_code));
            }
            break;
          default:
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not supported ddl task", K(ret), K(task));
            break;
        }
        return ret;
      }))) {
    LOG_WARN("failed to modify task", K(ret));
  }
  return ret;
}

void ObDDLScheduler::free_ddl_task(ObDDLTask *ddl_task)
{
  if (nullptr != ddl_task) {
    ddl_task->~ObDDLTask();
    allocator_.free(ddl_task);
  }
}

void ObDDLScheduler::destroy_all_tasks()
{
  int ret = OB_SUCCESS;
  ObDDLTask *ddl_task = nullptr;
  ObLongopsMgr &longops_mgr = ObLongopsMgr::get_instance();
  while (OB_SUCC(ret)) {
    if (OB_FAIL(task_queue_.get_next_task(ddl_task))) {
      if (common::OB_ENTRY_NOT_EXIST == ret) {
        break;
      }
    } else if (OB_FAIL(remove_ddl_task(ddl_task))) {
      LOG_WARN("remove ddl task failed", K(ret));
    }
  }
}

int ObDDLScheduler::update_ddl_task_active_time(const int64_t task_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (task_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_id));
  } else if (OB_FAIL(manager_reg_heart_beat_task_.update_task_active_time(task_id))) {
    LOG_WARN("fail to set RegTaskTime map", K(ret), K(task_id));
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase


