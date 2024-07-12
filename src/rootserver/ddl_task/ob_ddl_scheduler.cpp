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
#include "rootserver/ddl_task/ob_drop_fts_index_task.h"
#include "rootserver/ddl_task/ob_drop_primary_key_task.h"
#include "rootserver/ddl_task/ob_index_build_task.h"
#include "rootserver/ddl_task/ob_build_mview_task.h"
#include "rootserver/ddl_task/ob_fts_index_build_task.h"
#include "rootserver/ddl_task/ob_modify_autoinc_task.h"
#include "rootserver/ddl_task/ob_table_redefinition_task.h"
#include "rootserver/ddl_task/ob_recover_restore_table_task.h"
#include "rootserver/ddl_task/ob_build_mview_task.h"
#include "share/ob_ddl_common.h"
#include "share/ob_rpc_struct.h"
#include "share/longops_mgr/ob_longops_mgr.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "share/ob_ddl_sim_point.h"
#include "share/restore/ob_import_util.h"

namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace common;

namespace rootserver
{

ObDDLTaskQueue::ObDDLTaskQueue()
  : task_list_(), task_map_(), lock_(), stop_(true), is_inited_(false)
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
    stop_ = true;
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
  } else if (has_set_stop()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("scheduler has stopped", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(task));
  } else if (OB_FAIL(DDL_SIM(task->get_tenant_id(), task->get_task_id(), PUSH_TASK_INTO_QUEUE_FAILED))) {
    LOG_WARN("ddl sim failure when push_task", K(ret), KPC(task));
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
      if (OB_FAIL(task_id_map_.set_refactored(task->get_ddl_task_id(), task, is_overwrite))) {
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
  } else if (OB_FAIL(DDL_SIM(task->get_tenant_id(), task->get_task_id(), REMOVE_TASK_FROM_QUEUE_FAILED))) {
    LOG_WARN("ddl sim failure: remove_ddl_task failed", K(ret), K(task->get_ddl_task_id()));
  } else if (OB_FAIL(task_map_.erase_refactored(task->get_task_key()))) {
    LOG_WARN("fail to erase from task set", K(ret));
  } else {
    LOG_INFO("succ to remove task", K(*task), KP(task));
  }
  if (OB_SUCCESS != (tmp_ret = task_id_map_.erase_refactored(task->get_ddl_task_id()))) {
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
  } else if (has_set_stop()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("scheduler has stopped", K(ret));
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
int ObDDLTaskQueue::modify_task(const ObDDLTaskID &task_id, F &&op)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  ObDDLTask *task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (OB_UNLIKELY(!task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_id));
  } else if (OB_FAIL(DDL_SIM(task_id.tenant_id_, task_id.task_id_, GET_TASK_FROM_QUEUE_FAILED))) {
    LOG_WARN("ddl sim failure: get task from queue failed", K(ret), K(task_id));
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

template<typename F>
int ObDDLTaskQueue::get_task(const ObDDLTaskKey &task_key, F &&op)
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
    LOG_WARN("failed to do task callback", K(ret));
  }
  return ret;
}

int ObDDLTaskQueue::update_task_copy_deps_setting(const ObDDLTaskID &task_id,
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
  } else if (OB_UNLIKELY(!task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_id));
  } else if (OB_FAIL(DDL_SIM(task_id.tenant_id_, task_id.task_id_, GET_TASK_FROM_QUEUE_FAILED))) {
    LOG_WARN("ddl sim failure: get task from queue failed", K(ret), K(task_id));
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

int ObDDLTaskQueue::update_task_process_schedulable(const ObDDLTaskID &task_id)
{
  int ret = OB_SUCCESS;
  ObDDLTask *ddl_task = nullptr;
  ObTableRedefinitionTask *table_redefinition_task = nullptr;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (OB_UNLIKELY(!task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_id));
  } else if (OB_FAIL(DDL_SIM(task_id.tenant_id_, task_id.task_id_, GET_TASK_FROM_QUEUE_FAILED))) {
    LOG_WARN("ddl sim failure: get task from queue failed", K(ret), K(task_id));
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

int ObDDLTaskQueue::update_task_ret_code(const ObDDLTaskID &task_id, const int ret_code)
{
  int ret = OB_SUCCESS;
  ObDDLTask *ddl_task = nullptr;
  ObTableRedefinitionTask *table_redefinition_task = nullptr;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (OB_UNLIKELY(!task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_id));
  } else if (OB_FAIL(task_id_map_.get_refactored(task_id, ddl_task))) {
    ret = OB_HASH_NOT_EXIST == ret ? OB_ENTRY_NOT_EXIST : ret;
    LOG_WARN("get from task map failed", K(ret), K(task_id));
  } else if (OB_ISNULL(table_redefinition_task = static_cast<ObTableRedefinitionTask*>(ddl_task))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl_task is null", K(ret));
  } else {
    const ObTabletID unused_tablet_id;
    const int64_t unused_snapshot_version = 0;
    const int64_t unused_execution_id = 0;
    const ObDDLTaskInfo unused_task_info;
    ret = table_redefinition_task->update_complete_sstable_job_status(unused_tablet_id, unused_snapshot_version,
        unused_execution_id, ret_code, unused_task_info);
  }
  return ret;
}

int ObDDLTaskQueue::abort_task(const ObDDLTaskID &task_id)
{
  int ret = OB_SUCCESS;
  share::ObTaskId trace_id;
  ObDDLTask *ddl_task = nullptr;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (OB_UNLIKELY(!task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_id));
  } else if (OB_FAIL(DDL_SIM(task_id.tenant_id_, task_id.task_id_, GET_TASK_FROM_QUEUE_FAILED))) {
    LOG_WARN("ddl sim failure: get task from queue failed", K(ret), K(task_id));
  } else if (OB_FAIL(task_id_map_.get_refactored(task_id, ddl_task))) {
    ret = OB_HASH_NOT_EXIST == ret ? OB_ENTRY_NOT_EXIST : ret;
    LOG_WARN("get from task map failed", K(ret), K(task_id));
  } else if (OB_ISNULL(ddl_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl_task is null", K(ret));
  } else {
    ddl_task->set_is_abort(true);
    trace_id.set(ddl_task->get_trace_id());
    if (OB_UNLIKELY(trace_id.is_invalid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(trace_id));
    } else if (OB_FAIL(SYS_TASK_STATUS_MGR.cancel_task(trace_id))) {
      LOG_WARN("cancel task failed", K(ret));
    } else if (OB_FAIL(DDL_SIM(task_id.tenant_id_, task_id.task_id_, CANCEL_SYS_TASK_FAILED))) {
      LOG_WARN("ddl sim failure: get task from queue failed", K(ret), K(task_id));
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

int ObDDLTaskHeartBeatMananger::update_task_active_time(const ObDDLTaskID &task_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObManagerRegisterHeartBeatTask not inited", K(ret));
  } else if (OB_UNLIKELY(!task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_INFO("invalid argument", K(ret), K(task_id));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, task_id.task_id_);
    // setting flag=1 to update the old time-value in the hash map with current time
    if (OB_FAIL(DDL_SIM(task_id.tenant_id_, task_id.task_id_, HEART_BEAT_UPDATE_ACTIVE_TIME))) {
      LOG_WARN("ddl sim failed", K(ret), K(task_id));
    } else if (OB_FAIL(register_task_time_.set_refactored(task_id,
        ObTimeUtility::current_time(), 1, 0, 0))) {
      LOG_WARN("set register task time failed", K(ret), K(task_id));
    }
  }
  return ret;
}

int ObDDLTaskHeartBeatMananger::remove_task(const ObDDLTaskID &task_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObManagerRegisterHeartBeatTask not inited", K(ret));
  } else if (OB_UNLIKELY(!task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_INFO("invalid argument", K(ret), K(task_id));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, task_id.task_id_);
    if (OB_FAIL(register_task_time_.erase_refactored(task_id))) {
      LOG_WARN("remove register task time failed", K(ret));
    }
  }
  return ret;
}

int ObDDLTaskHeartBeatMananger::get_inactive_ddl_task_ids(ObArray<ObDDLTaskID>& remove_task_ids)
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
      for (common::hash::ObHashMap<ObDDLTaskID, int64_t>::iterator it = register_task_time_.begin(); OB_SUCC(ret) && it != register_task_time_.end(); it++) {
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

int ObRedefCallback::modify_info(ObTableRedefinitionTask &redef_task,
                                ObDDLTaskQueue &task_queue,
                                ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!redef_task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("redef task is not valid", K(ret), K(redef_task));
  } else if (OB_FAIL(update_redef_task_info(redef_task))) {
    LOG_WARN("update redef task info failed", K(ret));
  } else {
    ObString message;
    const int64_t serialize_param_size = redef_task.get_serialize_param_size();
    if (serialize_param_size > 0) {
      char *buf = nullptr;
      int64_t pos = 0;
      common::ObArenaAllocator allocator("DdLSchedulerTmp");
      if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(serialize_param_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(serialize_param_size));
      } else if (OB_FAIL(redef_task.serialize_params_to_message(buf, serialize_param_size, pos))) {
        LOG_WARN("serialize params to message failed", K(ret));
      } else {
        message.reset();
        message.assign(buf, serialize_param_size);
        if (OB_FAIL(ObDDLTaskRecordOperator::update_ret_code_and_message(trans,
                                                            redef_task.get_tenant_id(),
                                                            redef_task.get_task_id(),
                                                            redef_task.get_ret_code(),
                                                            message))) {
          LOG_WARN("update task message failed", K(ret), K(redef_task.get_tenant_id()),
                                                 K(redef_task.get_task_id()), K(message));
        } else if (OB_FAIL(update_task_info_in_queue(redef_task, task_queue))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            bool exist = false;
            if (OB_FAIL(ObDDLTaskRecordOperator::check_task_id_exist(*GCTX.sql_proxy_, redef_task.get_tenant_id(), redef_task.get_task_id(), exist))) {
              LOG_WARN("check task id exist fail", K(ret), K(redef_task.get_tenant_id()));
            } else {
              if (exist) {
                ret = OB_EAGAIN;
                LOG_INFO("entry exist, the ddl scheduler hasn't recovered the task yet", K(ret), K(redef_task.get_task_id()));
              } else {
                LOG_WARN("this task does not exist int hash table", K(ret), K(redef_task.get_task_id()));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAbortRedefCallback::update_redef_task_info(ObTableRedefinitionTask& redef_task)
{
  int ret = OB_SUCCESS;
  redef_task.set_is_abort(true);
  return ret;
}

int ObAbortRedefCallback::update_task_info_in_queue(ObTableRedefinitionTask& redef_task,
                                                  ObDDLTaskQueue &ddl_task_queue)
{
  return ddl_task_queue.abort_task(ObDDLTaskID(redef_task.get_tenant_id(),redef_task.get_task_id()));
}

int ObCopyTableDepCallback::set_infos(common::hash::ObHashMap<ObString, bool> *infos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(infos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("infos is nullptr", K(ret));
  } else {
    infos_ = infos;
  }
  return ret;
}

int ObCopyTableDepCallback::update_redef_task_info(ObTableRedefinitionTask& redef_task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(infos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("infos is nullptr", K(ret));
  } else if (infos_->size() != 5) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("infos count error", K(ret), K(infos_->size()));
  } else {
    bool is_copy_constraints = false;
    bool is_copy_indexes = false;
    bool is_copy_triggers = false;
    bool is_copy_foreign_keys = false;
    bool is_ignore_errors = false;
    if (OB_FAIL(infos_->get_refactored("is_copy_constraints", is_copy_constraints))) {
      LOG_WARN("get item failed", K(ret));
    } else if (OB_FAIL(infos_->get_refactored("is_copy_indexes", is_copy_indexes))) {
      LOG_WARN("get item failed", K(ret));
    } else if (OB_FAIL(infos_->get_refactored("is_copy_triggers", is_copy_triggers))) {
      LOG_WARN("get item failed", K(ret));
    } else if (OB_FAIL(infos_->get_refactored("is_copy_foreign_keys", is_copy_foreign_keys))) {
      LOG_WARN("get item failed", K(ret));
    } else if (OB_FAIL(infos_->get_refactored("is_ignore_errors", is_ignore_errors))) {
      LOG_WARN("get item failed", K(ret));
    } else {
      redef_task.set_is_copy_constraints(is_copy_constraints);
      redef_task.set_is_copy_indexes(is_copy_indexes);
      redef_task.set_is_copy_triggers(is_copy_triggers);
      redef_task.set_is_copy_foreign_keys(is_copy_foreign_keys);
      redef_task.set_is_ignore_errors(is_ignore_errors);
    }
  }
  return ret;
}

int ObCopyTableDepCallback::update_task_info_in_queue(ObTableRedefinitionTask& redef_task,
                                                    ObDDLTaskQueue &ddl_task_queue)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(infos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("infos is nullptr", K(ret));
  } else if (infos_->size() != 5) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("infos count error", K(ret), K(infos_->size()));
  } else {
    bool is_copy_constraints = false;
    bool is_copy_indexes = false;
    bool is_copy_triggers = false;
    bool is_copy_foreign_keys = false;
    bool is_ignore_errors = false;
    if (OB_FAIL(infos_->get_refactored("is_copy_constraints", is_copy_constraints))) {
      LOG_WARN("get item failed", K(ret));
    } else if (OB_FAIL(infos_->get_refactored("is_copy_indexes", is_copy_indexes))) {
      LOG_WARN("get item failed", K(ret));
    } else if (OB_FAIL(infos_->get_refactored("is_copy_triggers", is_copy_triggers))) {
      LOG_WARN("get item failed", K(ret));
    } else if (OB_FAIL(infos_->get_refactored("is_copy_foreign_keys", is_copy_foreign_keys))) {
      LOG_WARN("get item failed", K(ret));
    } else if (OB_FAIL(infos_->get_refactored("is_ignore_errors", is_ignore_errors))) {
      LOG_WARN("get item failed", K(ret));
    } else if (OB_FAIL(ddl_task_queue.update_task_copy_deps_setting(ObDDLTaskID(redef_task.get_tenant_id(),redef_task.get_task_id()),
                                                                    is_copy_constraints,
                                                                    is_copy_indexes,
                                                                    is_copy_triggers,
                                                                    is_copy_foreign_keys,
                                                                    is_ignore_errors))) {
      LOG_WARN("update_task_copy_deps_setting failed", K(ret));
    }
  }
  return ret;
}

int ObFinishRedefCallback::update_redef_task_info(ObTableRedefinitionTask& redef_task)
{
  int ret = OB_SUCCESS;
  redef_task.set_is_do_finish(true);
  return ret;
}

int ObFinishRedefCallback::update_task_info_in_queue(ObTableRedefinitionTask& redef_task,
                                                  ObDDLTaskQueue &ddl_task_queue)
{
  return ddl_task_queue.update_task_process_schedulable(ObDDLTaskID(redef_task.get_tenant_id(),redef_task.get_task_id()));
}

int ObUpdateSSTableCompleteStatusCallback::update_redef_task_info(ObTableRedefinitionTask &redef_task)
{
  int ret = OB_SUCCESS;
  const ObTabletID unused_tablet_id;
  const int64_t unused_snapshot_version = 0;
  const int64_t unused_execution_id = 0;
  const ObDDLTaskInfo unused_task_info;
  ret = redef_task.update_complete_sstable_job_status(unused_tablet_id, unused_snapshot_version,
      unused_execution_id, ret_code_, unused_task_info);
  return ret;
}

int ObUpdateSSTableCompleteStatusCallback::update_task_info_in_queue(ObTableRedefinitionTask &redef_task,
                                                    ObDDLTaskQueue &ddl_task_queue)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ddl_task_queue.update_task_ret_code(ObDDLTaskID(redef_task.get_tenant_id(), redef_task.get_task_id()), ret_code_))) {
    LOG_WARN("update_task_copy_deps_setting failed", K(ret));
  }
  return ret;
}

int ObPrepareAlterTableArgParam::init(const int64_t consumer_group_id,
                                      const uint64_t session_id,
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
  if (FALSE_IT(consumer_group_id_ = consumer_group_id)) {
    // do nothing
  } else if (FALSE_IT(session_id_ = session_id)) {
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
  } else if (OB_FALSE_IT(task_queue_.set_stop(false))) {
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
  if (is_inited_) {
    TG_STOP(tg_id_);
    TG_STOP(lib::TGDefIDs::DDLScanTask);
    TG_STOP(lib::TGDefIDs::HeartBeatCheckTask);
    task_queue_.set_stop(true);
    idle_stop_ = true;
    is_started_ = false;
    destroy_all_tasks();
  }
}

void ObDDLScheduler::wait()
{
  if (is_inited_) {
    TG_WAIT(tg_id_);
    TG_WAIT(lib::TGDefIDs::DDLScanTask);
    TG_WAIT(lib::TGDefIDs::HeartBeatCheckTask);
  }
}

void ObDDLScheduler::destroy()
{
  if (is_inited_) {
    TG_DESTROY(tg_id_);
    TG_DESTROY(lib::TGDefIDs::DDLScanTask);
    TG_DESTROY(lib::TGDefIDs::HeartBeatCheckTask);
    allocator_.destroy();
    task_queue_.destroy();
    root_service_ = nullptr;
    tg_id_ = -1;
    is_inited_ = false;
  }
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
    lib::set_thread_name("DDLTaskExecutor");
    THIS_WORKER.set_worker_level(1);
    THIS_WORKER.set_curr_request_level(1);
    while (!has_set_stop() && !lib::Thread::current().has_set_stop()) {
      const bool stop = task_queue_.has_set_stop();
      bool do_idle = false;
      if (OB_FAIL(task_queue_.get_next_task(task))) {
        if (common::OB_ENTRY_NOT_EXIST == ret) {
          if (stop) {
            // Task queue remains empty after the last scheduler exit here.
            // Otherwise, a successful push_task must happen after this get_next_task,
            // which must have seen the stop flag (modification order consistency of atomic operation)
            // and failed, contradition.
            break;
          }
        } else {
          LOG_WARN("fail to get next task", K(ret));
        }
        do_idle = true;
      } else if (OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, task must not be NULL", K(ret));
      } else if (task == first_retry_task || !task->need_schedule()) {
        // add the task back to the queue
        if (OB_FAIL(task_queue_.add_task_to_last(task))) {
          if (OB_STATE_NOT_MATCH == ret) {
            LOG_INFO("rootserver stopped, remove this task", K(*task));
            // overwrite ret
            if (OB_FAIL(remove_ddl_task(task))) {
              LOG_WARN("remove ddl task failed", K(ret));
            }
          }
        }
        do_idle = true;
      } else {
        ObCurTraceId::set(task->get_trace_id());
        int task_ret = task->process();
        task->calc_next_schedule_ts(task_ret, task_queue_.get_task_cnt() + thread_cnt);
        if (task->need_retry() && !stop && !ObIDDLTask::is_ddl_force_no_more_process(task_ret)) {
          if (OB_FAIL(task_queue_.add_task_to_last(task))) {
            if (OB_STATE_NOT_MATCH == ret) {
              LOG_INFO("rootserver stopped, remove this task", K(*task));
              // overwrite ret
              if (OB_FAIL(remove_ddl_task(task))) {
                LOG_WARN("remove ddl task failed", K(ret));
              }
            }
          }
          first_retry_task = nullptr == first_retry_task ? task : first_retry_task;
        } else if (OB_FAIL(remove_ddl_task(task))) {
          LOG_WARN("remove ddl task failed", K(ret));
        }
      }
      if (do_idle) {
        first_retry_task = nullptr;
        idler_.idle(ObDDLTask::DEFAULT_TASK_IDLE_TIME_US);
      }
    }
  }
}

int ObDDLScheduler::check_conflict_with_upgrade(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_id));
  } else if (GCONF.in_upgrade_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Ddl task is disallowed to create when upgrading", K(ret));
  }
  return ret;
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
  const obrpc::ObMViewCompleteRefreshArg *mview_complete_refresh_arg = nullptr;
  ObRootService *root_service = GCTX.root_service_;
  LOG_INFO("create ddl task", K(param));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_FAIL(check_conflict_with_upgrade(param.tenant_id_))) {
    LOG_WARN("conflict with upgrade", K(ret), K(param));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else if (ObDDLUtil::is_verifying_checksum_error_needed(param.type_)
      && OB_FAIL(ObDDLUtil::check_table_compaction_checksum_error(param.tenant_id_,param.src_table_schema_->get_table_id()))) {
    if (OB_NOT_SUPPORTED != ret) {
      LOG_WARN("unexpected error in check_table_compaction_checksum_error, choose to suppress the error", K(ret), K(param));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("check_table_compaction_checksum_error fail", K(ret), K(param));
    }
  }
  if (OB_SUCC(ret)) {
    switch (param.type_) {
      case DDL_CREATE_INDEX:
      case DDL_CREATE_MLOG:
      case DDL_CREATE_PARTITIONED_LOCAL_INDEX:
        create_index_arg = static_cast<const obrpc::ObCreateIndexArg *>(param.ddl_arg_);
        if (OB_FAIL(create_build_index_task(proxy,
                                            param.type_,
                                            param.src_table_schema_,
                                            param.dest_table_schema_,
                                            param.parallelism_,
                                            param.parent_task_id_,
                                            param.consumer_group_id_,
                                            param.sub_task_trace_id_,
                                            create_index_arg,
                                            param.type_,
                                            param.tenant_data_version_,
                                            *param.allocator_,
                                            task_record))) {
          LOG_WARN("fail to create build index task", K(ret));
        }
        break;
      case DDL_CREATE_FTS_INDEX:
        create_index_arg = static_cast<const obrpc::ObCreateIndexArg *>(param.ddl_arg_);
        if (OB_FAIL(create_build_fts_index_task(proxy,
                                                param.src_table_schema_,
                                                param.dest_table_schema_,
                                                param.parallelism_,
                                                param.parent_task_id_,
                                                param.consumer_group_id_,
                                                create_index_arg,
                                                *param.allocator_,
                                                task_record))) {
          LOG_WARN("fail to create build fts index task", K(ret));
        }
        break;
      case DDL_DROP_INDEX:
      case DDL_DROP_MLOG:
        // in this case, src_table_schema is data table, dest_table_schema is index table
        drop_index_arg = static_cast<const obrpc::ObDropIndexArg *>(param.ddl_arg_);
        if (OB_FAIL(create_drop_index_task(proxy,
                                           param.type_,
                                           param.src_table_schema_,
                                           param.parent_task_id_,
                                           param.consumer_group_id_,
                                           param.sub_task_trace_id_,
                                           drop_index_arg,
                                           *param.allocator_,
                                           task_record))) {
          LOG_WARN("fail to create drop index task failed", K(ret));
        }
        break;
      case DDL_DROP_FTS_INDEX:
      case DDL_DROP_MULVALUE_INDEX:
        if (OB_FAIL(create_drop_fts_index_task(proxy,
                                               param.src_table_schema_,
                                               param.schema_version_,
                                               param.consumer_group_id_,
                                               param.aux_rowkey_doc_schema_,
                                               param.aux_doc_rowkey_schema_,
                                               param.aux_doc_word_schema_,
                                               *param.allocator_,
                                               task_record))) {
          LOG_WARN("fail to create drop fts index task", K(ret));
        }
        break;
      case DDL_MODIFY_COLUMN:
      case DDL_ADD_PRIMARY_KEY:
      case DDL_ALTER_PRIMARY_KEY:
      case DDL_ALTER_PARTITION_BY:
      case DDL_CONVERT_TO_CHARACTER:
      case DDL_TABLE_REDEFINITION:
      case DDL_DIRECT_LOAD:
      case DDL_DIRECT_LOAD_INSERT:
      case DDL_ALTER_COLUMN_GROUP:
      case DDL_MVIEW_COMPLETE_REFRESH:
      case DDL_MODIFY_AUTO_INCREMENT_WITH_REDEFINITION:
        if (OB_FAIL(create_table_redefinition_task(proxy,
                                                   param.type_,
                                                   param.src_table_schema_,
                                                   param.dest_table_schema_,
                                                   param.parallelism_,
                                                   param.consumer_group_id_,
                                                   param.parent_task_id_,
                                                   param.task_id_,
                                                   param.sub_task_trace_id_,
                                                   static_cast<const obrpc::ObAlterTableArg *>(param.ddl_arg_),
                                                   param.tenant_data_version_,
                                                   param.ddl_need_retry_at_executor_,
                                                   *param.allocator_,
                                                   task_record))) {
          LOG_WARN("fail to create table redefinition task", K(ret));
        }
        break;
      case DDL_CREATE_MVIEW:
        mview_complete_refresh_arg = static_cast<const obrpc::ObMViewCompleteRefreshArg *>(param.ddl_arg_);
        if (OB_FAIL(create_build_mview_task(proxy,
                                            param.src_table_schema_,
                                            param.parallelism_,
                                            param.parent_task_id_,
                                            param.consumer_group_id_,
                                            mview_complete_refresh_arg,
                                            *param.allocator_,
                                            task_record))) {
          LOG_WARN("fail to create build mview task", K(ret));
        }
        break;
      case DDL_TABLE_RESTORE:
        if (OB_FAIL(create_recover_restore_table_task(proxy,
                                                   param.type_,
                                                   param.src_table_schema_,
                                                   param.dest_table_schema_,
                                                   param.parallelism_,
                                                   param.consumer_group_id_,
                                                   param.task_id_,
                                                   param.sub_task_trace_id_,
                                                   static_cast<const obrpc::ObAlterTableArg *>(param.ddl_arg_),
                                                   param.tenant_data_version_,
                                                   *param.allocator_,
                                                   task_record))) {
          LOG_WARN("fail to create recover restore table task", K(ret));
        }
        break;
      case DDL_DROP_PRIMARY_KEY:
        alter_table_arg = static_cast<const obrpc::ObAlterTableArg *>(param.ddl_arg_);
        if (OB_FAIL(create_drop_primary_key_task(proxy,
                                                 param.type_,
                                                 param.src_table_schema_,
                                                 param.dest_table_schema_,
                                                 param.parallelism_,
                                                 param.consumer_group_id_,
                                                 param.task_id_,
                                                 param.sub_task_trace_id_,
                                                 alter_table_arg,
                                                 param.tenant_data_version_,
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
                                           param.consumer_group_id_,
                                           param.sub_task_trace_id_,
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
                                                    param.consumer_group_id_,
                                                    param.task_id_,
                                                    param.sub_task_trace_id_,
                                                    static_cast<const obrpc::ObAlterTableArg *>(param.ddl_arg_),
                                                    param.tenant_data_version_,
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
                                               param.consumer_group_id_,
                                               param.task_id_,
                                               param.sub_task_trace_id_,
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
      case DDL_RENAME_PARTITION:
      case DDL_RENAME_SUB_PARTITION:
      case DDL_TRUNCATE_PARTITION:
      case DDL_TRUNCATE_SUB_PARTITION:
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
      } else if (OB_FAIL(DDL_SIM(task->get_tenant_id(), task->get_task_id(), DDL_SCHEDULER_ADD_SYS_TASK_FAILED))) {
        LOG_WARN("ddl sim failure: add sys task failed", K(ret));
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
      } else if (OB_FAIL(DDL_SIM(task->get_tenant_id(), task->get_task_id(), DDL_SCHEDULER_REMOVE_SYS_TASK_FAILED))) {
        LOG_WARN("ddl sim failure: remove ddl task", K(ret), K(task->get_ddl_task_id()));
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
  } else if (FALSE_IT(alter_table_arg.consumer_group_id_ = param.consumer_group_id_)) {
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

int ObDDLScheduler::get_task_record(const ObDDLTaskID &task_id,
                                    ObISQLClient &trans,
                                    ObDDLTaskRecord &task_record,
                                    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_id));
  } else {
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
      if (OB_ENTRY_NOT_EXIST == ret) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(ObDDLTaskRecordOperator::get_ddl_task_record(task_id.tenant_id_,
                                                                    task_id.task_id_,
                                                                    root_service_->get_sql_proxy(),
                                                                    allocator,
                                                                    task_record))) {
          LOG_WARN("get ddl task record failed", K(ret), K(task_id));
        } else {
          LOG_INFO("get ddl task record success", K(ret), K(task_record));
        }
      } else {
        LOG_WARN("failed to modify task", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLScheduler::modify_redef_task(const ObDDLTaskID &task_id, ObRedefCallback &cb)
{
  int ret = OB_SUCCESS;
  int64_t table_task_status = 0;
  int64_t table_execution_id = 0;
  int64_t table_ret_code = OB_SUCCESS;
  ObMySQLTransaction trans;
  common::ObArenaAllocator allocator(lib::ObLabel("task_info"));
  if (OB_UNLIKELY(!task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_id));
  } else if (OB_FAIL(DDL_SIM(task_id.tenant_id_, task_id.task_id_, REDEF_TABLE_ABORT_FAILED))) {
    LOG_WARN("ddl sim failure: abort_redef_table", K(ret), K(task_id));
  } else if (OB_FAIL(trans.start(&root_service_->get_sql_proxy(), task_id.tenant_id_))) {
    LOG_WARN("start transaction failed", K(ret), K_(task_id.tenant_id));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::select_for_update(trans,
                                                                task_id.tenant_id_,
                                                                task_id.task_id_,
                                                                table_task_status,
                                                                table_execution_id,
                                                                table_ret_code))) {
    LOG_WARN("select for update failed", K(ret), K(task_id.tenant_id_), K(task_id.task_id_));
  } else {
    bool need_reschedule = false;
    ObDDLTaskRecord task_record;
    HEAP_VAR(ObTableRedefinitionTask, redefinition_task) {
      if (OB_FAIL(get_task_record(task_id, trans, task_record, allocator))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          need_reschedule = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to set redefinition task info", K(ret), K(task_id));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(redefinition_task.init(task_record))) {
        LOG_WARN("fail to init redefinition_task", K(ret), K(task_record));
      } else if (OB_FAIL(redefinition_task.set_trace_id(task_record.trace_id_))) {
        LOG_WARN("set trace id failed", K(ret));
      } else if (OB_FAIL(cb.modify_info(redefinition_task, task_queue_, trans))) {
        LOG_WARN("fail to modify info", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (need_reschedule) {
          if (OB_FAIL(schedule_ddl_task(task_record))) {
            LOG_WARN("fail to schedule ddl task", K(ret), K(task_record));
          }
        }
      }
    }
  }
  if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  if (trans.is_started()) {
    bool commit = (OB_SUCCESS == ret);
    int tmp_ret = trans.end(commit);
    if (OB_SUCCESS != tmp_ret) {
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObDDLScheduler::abort_redef_table(const ObDDLTaskID &task_id)
{
  int ret = OB_SUCCESS;
  ObAbortRedefCallback cb;
  if (OB_FAIL(modify_redef_task(task_id, cb))) {
    LOG_WARN("fail to modify redef task", K(ret), K(task_id));
  }
  return ret;
}

int ObDDLScheduler::copy_table_dependents(const ObDDLTaskID &task_id,
                                          const bool is_copy_constraints,
                                          const bool is_copy_indexes,
                                          const bool is_copy_triggers,
                                          const bool is_copy_foreign_keys,
                                          const bool is_ignore_errors)
{
  int ret = OB_SUCCESS;
  ObCopyTableDepCallback cb;
  common::hash::ObHashMap<ObString, bool> infos;
  lib::ObMemAttr attr(OB_SERVER_TENANT_ID, "TableDep");
  SET_USE_500(attr);
  if (OB_FAIL(infos.create(5, attr, attr))) {
    LOG_WARN("fail to create map", K(ret));
  } else if (OB_FAIL(infos.set_refactored("is_copy_constraints", is_copy_constraints))) {
    LOG_WARN("set item failed", K(ret));
  } else if (OB_FAIL(infos.set_refactored("is_copy_indexes", is_copy_indexes))) {
    LOG_WARN("set item failed", K(ret));
  } else if (OB_FAIL(infos.set_refactored("is_copy_triggers", is_copy_triggers))) {
    LOG_WARN("set item failed", K(ret));
  } else if (OB_FAIL(infos.set_refactored("is_copy_foreign_keys", is_copy_foreign_keys))) {
    LOG_WARN("set item failed", K(ret));
  } else if (OB_FAIL(infos.set_refactored("is_ignore_errors", is_ignore_errors))) {
    LOG_WARN("set item failed", K(ret));
  } else if (FALSE_IT(cb.set_infos(&infos))) {
  } else if (OB_FAIL(DDL_SIM(task_id.tenant_id_, task_id.task_id_, REDEF_TABLE_COPY_DEPES_FAILED))) {
    LOG_WARN("ddl sim failure: copy_table_dependents", K(ret), K(task_id));
  } else if (OB_FAIL(modify_redef_task(task_id, cb))) {
    LOG_WARN("fail to modify redef task", K(ret), K(task_id));
  }
  return ret;
}

int ObDDLScheduler::finish_redef_table(const ObDDLTaskID &task_id)
{
  int ret = OB_SUCCESS;
  ObFinishRedefCallback cb;
  if (OB_FAIL(DDL_SIM(task_id.tenant_id_, task_id.task_id_, REDEF_TABLE_FINISH_FAILED))) {
    LOG_WARN("ddl sim failure: copy_table_dependents", K(ret), K(task_id));
  } else if (OB_FAIL(modify_redef_task(task_id, cb))) {
    LOG_WARN("fail to modify redef task", K(ret), K(task_id));
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
      if (OB_FAIL(param.init(THIS_WORKER.get_group_id(),
                             arg.session_id_,
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
        int64_t group_id = THIS_WORKER.get_group_id(); //TODO qilu: pass id when directload_arg_init
        ObCreateDDLTaskParam param(tenant_id,
                                      arg.ddl_type_,
                                      orig_table_schema,
                                      target_table_schema,
                                      orig_table_schema->get_table_id(),
                                      orig_table_schema->get_schema_version(),
                                      arg.parallelism_,
                                      group_id,
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
        add_event_info(task_record, "ddl_scheduler start redef table");
        LOG_INFO("ddl_scheduler start redef table", K(ret), "ddl_event_info", ObDDLEventInfo(), K(task_record));
      }
    }
  }
  return ret;
}

int ObDDLScheduler::create_build_fts_index_task(
    common::ObISQLClient &proxy,
    const ObTableSchema *data_table_schema,
    const ObTableSchema *index_schema,
    const int64_t parallelism,
    const int64_t parent_task_id,
    const int64_t consumer_group_id,
    const obrpc::ObCreateIndexArg *create_index_arg,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  SMART_VAR(ObFtsIndexBuildTask, index_task) {
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_ISNULL(create_index_arg) || OB_ISNULL(data_table_schema) || OB_ISNULL(index_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), KPC(create_index_arg),
          KPC(data_table_schema), KPC(index_schema));
    } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), data_table_schema->get_tenant_id(), task_id))) {
      LOG_WARN("fetch new task id failed", K(ret));
    } else if (OB_FAIL(index_task.init(data_table_schema->get_tenant_id(),
                                       task_id,
                                       data_table_schema,
                                       index_schema,
                                       data_table_schema->get_schema_version(),
                                       parallelism,
                                       consumer_group_id,
                                       *create_index_arg,
                                       parent_task_id))) {
      LOG_WARN("init fts index task failed", K(ret), K(data_table_schema), K(index_schema));
    } else if (OB_FAIL(index_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
      LOG_WARN("set trace id failed", K(ret));
    } else if (OB_FAIL(insert_task_record(proxy, index_task, allocator, task_record))) {
      LOG_WARN("fail to insert task record", K(ret));
    }

    LOG_INFO("ddl_scheduler create build index task finished", K(ret), K(index_task));
  }
  return ret;
}

int ObDDLScheduler::create_build_index_task(
    common::ObISQLClient &proxy,
    const share::ObDDLType &ddl_type,
    const ObTableSchema *data_table_schema,
    const ObTableSchema *index_schema,
    const int64_t parallelism,
    const int64_t parent_task_id,
    const int64_t consumer_group_id,
    const int32_t sub_task_trace_id,
    const obrpc::ObCreateIndexArg *create_index_arg,
    const share::ObDDLType task_type,
    const uint64_t tenant_data_version,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  SMART_VAR(ObIndexBuildTask, index_task) {
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_ISNULL(create_index_arg) || OB_ISNULL(data_table_schema) || OB_ISNULL(index_schema)
        || OB_UNLIKELY(tenant_data_version <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), KPC(create_index_arg), KPC(data_table_schema), KPC(index_schema), K(tenant_data_version));
    } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), data_table_schema->get_tenant_id(), task_id))) {
      LOG_WARN("fetch new task id failed", K(ret));
    } else if (OB_FAIL(index_task.init(data_table_schema->get_tenant_id(),
                                      task_id,
                                      ddl_type,
                                      data_table_schema,
                                      index_schema,
                                      index_schema->get_schema_version(),
                                      parallelism,
                                      consumer_group_id,
                                      sub_task_trace_id,
                                      *create_index_arg,
                                      task_type,
                                      parent_task_id,
                                      tenant_data_version))) {
      LOG_WARN("init global index task failed", K(ret), K(data_table_schema), K(index_schema));
    } else if (OB_FAIL(index_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
      LOG_WARN("set trace id failed", K(ret));
    } else if (OB_FAIL(insert_task_record(proxy, index_task, allocator, task_record))) {
      LOG_WARN("fail to insert task record", K(ret));
    }
    LOG_INFO("ddl_scheduler create build index task finished", K(ret), "ddl_event_info", ObDDLEventInfo(), K(task_record));
  }
  return ret;
}

int ObDDLScheduler::create_drop_index_task(
    common::ObISQLClient &proxy,
    const share::ObDDLType &ddl_type,
    const share::schema::ObTableSchema *index_schema,
    const int64_t parent_task_id,
    const int64_t consumer_group_id,
    const int32_t sub_task_trace_id,
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
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), index_schema->get_tenant_id(), task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else {
    const uint64_t data_table_id = index_schema->get_data_table_id();
    const uint64_t index_table_id = index_schema->get_table_id();
    if (OB_FAIL(index_task.init(index_schema->get_tenant_id(),
                                task_id,
                                ddl_type,
                                data_table_id,
                                index_table_id,
                                index_schema->get_schema_version(),
                                parent_task_id,
                                consumer_group_id,
                                sub_task_trace_id,
                                *drop_index_arg))) {
      LOG_WARN("init drop index task failed", K(ret), K(data_table_id), K(index_table_id));
    } else if (OB_FAIL(index_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
      LOG_WARN("set trace id failed", K(ret));
    } else if (OB_FAIL(insert_task_record(proxy, index_task, allocator, task_record))) {
      LOG_WARN("fail to insert task record", K(ret));
    }
  }
  LOG_INFO("ddl_scheduler create drop index task finished", K(ret), "ddl_event_info", ObDDLEventInfo(), K(task_record));
  return ret;
}

int ObDDLScheduler::create_drop_fts_index_task(
    common::ObISQLClient &proxy,
    const share::schema::ObTableSchema *index_schema,
    const int64_t schema_version,
    const int64_t consumer_group_id,
    const share::schema::ObTableSchema *rowkey_doc_schema,
    const share::schema::ObTableSchema *doc_rowkey_schema,
    const share::schema::ObTableSchema *doc_word_schema,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  ObDropFTSIndexTask index_task;
  common::ObString domain_index_name;
  common::ObString fts_doc_word_name;
  common::ObString rowkey_doc_name;
  common::ObString doc_rowkey_name;
  // multivalue index may run here, need calc index type first
  bool is_fts_index = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(index_schema));
  } else if (FALSE_IT(is_fts_index = index_schema->is_fts_index_aux())) {
  } else if (OB_UNLIKELY(schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(index_schema), K(schema_version));
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), index_schema->get_tenant_id(),
          task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else if (OB_FAIL(index_schema->get_index_name(domain_index_name))) {
    LOG_WARN("fail to get domain index name", K(ret), KPC(index_schema));
  } else {
    if (is_fts_index) {
      if (OB_FAIL(ret) || OB_ISNULL(doc_word_schema)) {
      } else if (OB_FAIL(doc_word_schema->get_index_name(fts_doc_word_name))) {
        LOG_WARN("fail to get fts doc word name", K(ret), KPC(doc_word_schema));
      }
    }
    if (OB_FAIL(ret) || OB_ISNULL(rowkey_doc_schema)) {
    } else if (OB_FAIL(rowkey_doc_schema->get_index_name(rowkey_doc_name))) {
      LOG_WARN("fail to get rowkey doc name", K(ret), KPC(rowkey_doc_schema));
    }
    if (OB_FAIL(ret) || OB_ISNULL(doc_rowkey_schema)) {
    } else if (OB_FAIL(doc_rowkey_schema->get_index_name(doc_rowkey_name))) {
      LOG_WARN("fail to get doc rowkey name", K(ret), KPC(doc_rowkey_schema));
    }
    const uint64_t data_table_id = index_schema->get_data_table_id();
    const ObFTSDDLChildTaskInfo domain_index(domain_index_name, index_schema->get_table_id(), 0/*task_id*/);
    uint64_t rowkey_doc_table_id = OB_ISNULL(rowkey_doc_schema) ? OB_INVALID_ID :
                                   rowkey_doc_schema->get_table_id();
    uint64_t doc_rowkey_table_id = OB_ISNULL(doc_rowkey_schema) ? OB_INVALID_ID :
                                   doc_rowkey_schema->get_table_id();
    uint64_t doc_word_table_id = OB_ISNULL(doc_word_schema) ? OB_INVALID_ID :
                                   doc_word_schema->get_table_id();
    const ObFTSDDLChildTaskInfo fts_doc_word(fts_doc_word_name,
      is_fts_index ? doc_word_table_id : OB_INVALID_ID, 0/*task_id*/);
    const ObFTSDDLChildTaskInfo rowkey_doc(rowkey_doc_name, rowkey_doc_table_id, 0/*task_id*/);
    const ObFTSDDLChildTaskInfo doc_rowkey(doc_rowkey_name, doc_rowkey_table_id, 0/*task_id*/);
    const ObDDLType ddl_type = is_fts_index ? DDL_DROP_FTS_INDEX : DDL_DROP_MULVALUE_INDEX;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(index_task.init(index_schema->get_tenant_id(),
                                task_id,
                                data_table_id,
                                ddl_type,
                                rowkey_doc,
                                doc_rowkey,
                                domain_index,
                                fts_doc_word,
                                schema_version,
                                consumer_group_id))) {
      LOG_WARN("init drop index task failed", K(ret), K(data_table_id), K(domain_index));
    } else if (OB_FAIL(index_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
      LOG_WARN("set trace id failed", K(ret));
    } else if (OB_FAIL(insert_task_record(proxy, index_task, allocator, task_record))) {
      LOG_WARN("fail to insert task record", K(ret));
    }
  }
  LOG_INFO("ddl_scheduler create drop fts index task finished", K(ret), K(index_task));
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
    const int64_t consumer_group_id,
    const int32_t sub_task_trace_id,
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
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), table_schema->get_tenant_id(), task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else if (OB_FAIL(constraint_task.init(task_id, table_schema, constraint_id, ddl_type, schema_version, *arg, consumer_group_id, sub_task_trace_id, parent_task_id))) {
    LOG_WARN("init constraint task failed", K(ret), K(table_schema), K(constraint_id));
  } else if (OB_FAIL(constraint_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(insert_task_record(proxy, constraint_task, allocator, task_record))) {
    LOG_WARN("fail to insert task record", K(ret));
  }
  LOG_INFO("ddl_scheduler create constraint task finished", K(ret), "ddl_event_info", ObDDLEventInfo(), K(task_record));
  }
  return ret;
}

int ObDDLScheduler::create_table_redefinition_task(
    common::ObISQLClient &proxy,
    const share::ObDDLType &type,
    const share::schema::ObTableSchema *src_schema,
    const share::schema::ObTableSchema *dest_schema,
    const int64_t parallelism,
    const int64_t consumer_group_id,
    const int64_t parent_task_id,
    const int64_t task_id,
    const int32_t sub_task_trace_id,
    const obrpc::ObAlterTableArg *alter_table_arg,
    const uint64_t tenant_data_version,
    const bool ddl_need_retry_at_executor,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t target_cg_cnt = 0;
  SMART_VAR(ObTableRedefinitionTask, redefinition_task) {
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObDDLScheduler has not been inited", K(ret));
    } else if (OB_UNLIKELY(0 == task_id || tenant_data_version <= 0) || OB_ISNULL(alter_table_arg) || OB_ISNULL(src_schema) || OB_ISNULL(dest_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(task_id), KP(alter_table_arg), KP(src_schema), KP(dest_schema),  K(tenant_data_version));
    } else if (OB_FAIL(dest_schema->get_store_column_group_count(target_cg_cnt))) {
      LOG_WARN("fail to get target_cg_cnt", K(ret), K(dest_schema));
    } else if (OB_FAIL(redefinition_task.init(src_schema,
                                              dest_schema,
                                              parent_task_id,
                                              task_id,
                                              type,
                                              parallelism,
                                              consumer_group_id,
                                              sub_task_trace_id,
                                              *alter_table_arg,
                                              tenant_data_version,
                                              ddl_need_retry_at_executor))) {
      LOG_WARN("fail to init redefinition task", K(ret));
    } else if (OB_FAIL(redefinition_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
      LOG_WARN("set trace id failed", K(ret));
    } else if (OB_FAIL(insert_task_record(proxy, redefinition_task, allocator, task_record))) {
      LOG_WARN("fail to insert task record", K(ret));
    }
    LOG_INFO("ddl_scheduler create table redefinition task finished", K(ret), "ddl_event_info", ObDDLEventInfo(), K(common::lbt()), K(task_record));
  }
  return ret;
}

int ObDDLScheduler::create_drop_primary_key_task(
    common::ObISQLClient &proxy,
    const share::ObDDLType &type,
    const ObTableSchema *src_schema,
    const ObTableSchema *dest_schema,
    const int64_t parallelism,
    const int64_t consumer_group_id,
    const int64_t task_id,
    const int32_t sub_task_trace_id,
    const obrpc::ObAlterTableArg *alter_table_arg,
    const uint64_t tenant_data_version,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t target_cg_cnt = 0;
  SMART_VAR(ObDropPrimaryKeyTask, drop_pk_task) {
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(0 == task_id || tenant_data_version <= 0) || OB_ISNULL(alter_table_arg) || OB_ISNULL(src_schema) || OB_ISNULL(dest_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_id), KP(alter_table_arg), KP(src_schema), KP(dest_schema), K(tenant_data_version));
  } else if (OB_FAIL(dest_schema->get_store_column_group_count(target_cg_cnt))) {
    LOG_WARN("fail to get target_store_cg_cnt", K(ret), KPC(dest_schema));
  } else if (OB_FAIL(drop_pk_task.init(src_schema,
                                       dest_schema,
                                       task_id,
                                       type,
                                       parallelism,
                                       consumer_group_id,
                                       sub_task_trace_id,
                                       *alter_table_arg,
                                       tenant_data_version))) {
    LOG_WARN("fail to init redefinition task", K(ret));
  } else if (OB_FAIL(drop_pk_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(insert_task_record(proxy, drop_pk_task, allocator, task_record))) {
    LOG_WARN("fail to insert task record", K(ret));
  }
  LOG_INFO("ddl_scheduler create drop primary key task finished", K(ret), "ddl_event_info", ObDDLEventInfo(), K(task_record));
  }
  return ret;
}

int ObDDLScheduler::create_column_redefinition_task(
    common::ObISQLClient &proxy,
    const share::ObDDLType &type,
    const share::schema::ObTableSchema *src_schema,
    const share::schema::ObTableSchema *dest_schema,
    const int64_t parallelism,
    const int64_t consumer_group_id,
    const int64_t task_id,
    const int32_t sub_task_trace_id,
    const obrpc::ObAlterTableArg *alter_table_arg,
    const uint64_t tenant_data_version,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t target_cg_cnt = 0;
  SMART_VAR(ObColumnRedefinitionTask, redefinition_task) {
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(0 == task_id || tenant_data_version <= 0)
    || OB_ISNULL(alter_table_arg) || OB_ISNULL(src_schema) || OB_ISNULL(dest_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_id), KP(alter_table_arg), KP(src_schema), KP(dest_schema), K(tenant_data_version));
  } else if (OB_FAIL(redefinition_task.init(src_schema->get_tenant_id(),
                                            task_id,
                                            type,
                                            src_schema->get_table_id(),
                                            dest_schema->get_table_id(),
                                            dest_schema->get_schema_version(),
                                            parallelism,
                                            consumer_group_id,
                                            sub_task_trace_id,
                                            *alter_table_arg,
                                            tenant_data_version))) {
    LOG_WARN("fail to init redefinition task", K(ret));
  } else if (OB_FAIL(redefinition_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(insert_task_record(proxy, redefinition_task, allocator, task_record))) {
    LOG_WARN("fail to insert task record", K(ret));
  }
  LOG_INFO("ddl_scheduler create column redefinition task finished", K(ret), "ddl_event_info", ObDDLEventInfo(), K(task_record));
  }
  return ret;
}

int ObDDLScheduler::create_modify_autoinc_task(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const int64_t table_id,
    const int64_t schema_version,
    const int64_t consumer_group_id,
    const int64_t task_id,
    const int32_t sub_task_trace_id,
    const obrpc::ObAlterTableArg *arg,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObModifyAutoincTask, modify_autoinc_task) {
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id
                          || schema_version <= 0 || 0 == task_id || nullptr == arg || !arg->is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_id), K(schema_version), K(task_id), K(arg));
    } else if (OB_FAIL(modify_autoinc_task.init(tenant_id, task_id, table_id, schema_version, consumer_group_id, sub_task_trace_id, *arg))) {
      LOG_WARN("init global index task failed", K(ret), K(table_id), K(arg));
    } else if (OB_FAIL(modify_autoinc_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
      LOG_WARN("set trace id failed", K(ret));
    } else if (OB_FAIL(insert_task_record(proxy, modify_autoinc_task, allocator, task_record))) {
      LOG_WARN("fail to insert task record", K(ret));
    }
    LOG_INFO("ddl_scheduler create modify autoinc task finished", K(ret), "ddl_event_info", ObDDLEventInfo(), K(task_record));
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
    const int64_t consumer_group_id,
    const int32_t sub_task_trace_id,
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
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), tenant_id, task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else if (OB_FAIL(ddl_retry_task.init(tenant_id, task_id, object_id, schema_version, consumer_group_id, sub_task_trace_id,type, arg))) {
    LOG_WARN("init ddl retry task failed", K(ret), K(arg));
  } else if (OB_FAIL(ddl_retry_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(insert_task_record(proxy, ddl_retry_task, allocator, task_record))) {
    LOG_WARN("fail to insert task record", K(ret));
  }
  LOG_INFO("ddl_scheduler create ddl retry task finished", K(ret), "ddl_event_info", ObDDLEventInfo(), K(task_record));
  return ret;
}

int ObDDLScheduler::create_recover_restore_table_task(
    common::ObISQLClient &proxy,
    const share::ObDDLType &type,
    const share::schema::ObTableSchema *src_schema,
    const share::schema::ObTableSchema *dest_schema,
    const int64_t parallelism,
    const int64_t consumer_group_id,
    const int64_t task_id,
    const int32_t sub_task_trace_id,
    const obrpc::ObAlterTableArg *alter_table_arg,
    const uint64_t tenant_data_version,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t target_cg_cnt = 0;
  SMART_VAR(ObRecoverRestoreTableTask, redefinition_task) {
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObDDLScheduler has not been inited", K(ret));
    } else if (OB_UNLIKELY(0 == task_id || tenant_data_version <= 0)
        || OB_ISNULL(alter_table_arg) || OB_ISNULL(src_schema) || OB_ISNULL(dest_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(task_id), KP(alter_table_arg), KP(src_schema), KP(dest_schema), K(tenant_data_version));
    } else if (OB_FAIL(dest_schema->get_store_column_group_count(target_cg_cnt))) {
      LOG_WARN("fail to get store cg cnt", K(ret), KPC(dest_schema));
    } else if (OB_FAIL(redefinition_task.init(src_schema,
                                              dest_schema,
                                              task_id,
                                              type,
                                              parallelism,
                                              consumer_group_id,
                                              sub_task_trace_id,
                                              *alter_table_arg,
                                              tenant_data_version))) {
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
  } else if (OB_FAIL(DDL_SIM_WHEN(ddl_task.get_parent_task_id() > 0,
          ddl_task.get_tenant_id(), ddl_task.get_task_id(), INSERT_CHILD_DDL_TASK_RECORD_EXIST))) {
    LOG_WARN("sim ddl task record exist", K(ret), K(ddl_task));
  }
  return ret;
}

static bool is_tenant_primary(const ObIArray<uint64_t> &primary_tenant_ids, const uint64_t tenant_id)
{
  bool is_primary = false;
  for (int64_t i = 0; i < primary_tenant_ids.count(); ++i) {
    if (primary_tenant_ids.at(i) == tenant_id) {
      is_primary = true;
      break;
    }
  }
  return is_primary;
}

int ObDDLScheduler::recover_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSqlString sql_string;
    ObArray<ObDDLTaskRecord> task_records;
    ObArray<uint64_t> primary_tenant_ids;
    ObArenaAllocator allocator(lib::ObLabel("DdlTasRecord"));
    share::schema::ObMultiVersionSchemaService &schema_service = root_service_->get_schema_service();
    if (OB_FAIL(ObDDLTaskRecordOperator::get_all_ddl_task_record(root_service_->get_sql_proxy(), allocator, task_records))) {
      LOG_WARN("get task record failed", K(ret), K(sql_string));
    } else if (OB_FAIL(ObAllTenantInfoProxy::get_primary_tenant_ids(&root_service_->get_sql_proxy(), primary_tenant_ids))) {
      LOG_WARN("get primary tenant id failed", K(ret));
    }
    LOG_INFO("start processing ddl recovery", "ddl_event_info", ObDDLEventInfo(), K(task_records), K(primary_tenant_ids));
    for (int64_t i = 0; OB_SUCC(ret) && i < task_records.count(); ++i) {
      const ObDDLTaskRecord &cur_record = task_records.at(i);
      int64_t tenant_schema_version = 0;
      int64_t table_task_status = 0;
      int64_t execution_id = -1;
      int64_t ret_code = OB_SUCCESS;
      bool is_recover_table_aux_tenant = false;
      ObMySQLTransaction trans;
      if (OB_FAIL(schema_service.get_tenant_schema_version(cur_record.tenant_id_, tenant_schema_version))) {
        LOG_WARN("failed to get tenant schema version", K(ret), K(cur_record));
      } else if (!is_tenant_primary(primary_tenant_ids, cur_record.tenant_id_) && OB_SYS_TENANT_ID != cur_record.tenant_id_) {
        LOG_INFO("tenant not primary, skip schedule ddl task", K(cur_record));
      } else if (tenant_schema_version < cur_record.schema_version_) {
        // schema has not publish, by pass now
        LOG_INFO("skip schedule ddl task, because tenant schema version too old", K(tenant_schema_version), K(cur_record));
      } else if (OB_FAIL(ObImportTableUtil::check_is_recover_table_aux_tenant(schema_service,
                                                                              cur_record.tenant_id_,
                                                                              is_recover_table_aux_tenant))) {
        LOG_WARN("failed to check is recover table aux tenant", K(ret), K(cur_record));
      } else if (is_recover_table_aux_tenant) {
        LOG_INFO("tenant is recover table aux tenant, skip schedule ddl task", K(cur_record));
      } else if (OB_FAIL(trans.start(&root_service_->get_sql_proxy(), cur_record.tenant_id_))) {
        LOG_WARN("start transaction failed", K(ret));
      } else if (OB_FAIL(ObDDLTaskRecordOperator::select_for_update(trans,
                                                                    cur_record.tenant_id_,
                                                                    cur_record.task_id_,
                                                                    table_task_status,
                                                                    execution_id,
                                                                    ret_code))) {
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

      LOG_INFO("recover ddl task", K(ret), "ddl_event_info", ObDDLEventInfo(), K(cur_record));
    }
  }
  return ret;
}

void ObDDLScheduler::add_event_info(const ObDDLTaskRecord &ddl_record, const ObString &ddl_event_stmt)
{
  char object_id_buffer[256];
  snprintf(object_id_buffer, sizeof(object_id_buffer), "%ld %ld", ddl_record.object_id_, ddl_record.target_object_id_);
  ROOTSERVICE_EVENT_ADD("ddl scheduler", ddl_event_stmt.ptr(),
    "tenant_id", ddl_record.tenant_id_,
    "ret", ddl_record.ret_code_,
    "trace_id", ddl_record.trace_id_,
    "task_id", ddl_record.task_id_,
    "task_status", ddl_record.task_status_,
    "schema_version_", ddl_record.schema_version_,
    object_id_buffer);
}

int ObDDLScheduler::remove_inactive_ddl_task()
{
  int ret = OB_SUCCESS;
  ObArray<ObDDLTaskID> remove_task_ids;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_FAIL(manager_reg_heart_beat_task_.get_inactive_ddl_task_ids(remove_task_ids))){
      LOG_WARN("failed to check register time", K(ret));
    } else {
      LOG_INFO("need remove task", K(remove_task_ids));
      for (int64_t i = 0; i < remove_task_ids.size(); i++) {
        ObDDLTaskID remove_task_id;
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
  } else if (OB_FAIL(DDL_SIM(record.tenant_id_, record.task_id_, SCHEDULE_DDL_TASK_FAILED))) {
    LOG_WARN("sim schedule ddl task failed", K(ret));
  } else {
    switch (record.ddl_type_) {
      case ObDDLType::DDL_CREATE_INDEX:
      case ObDDLType::DDL_CREATE_MLOG:
      case ObDDLType::DDL_CREATE_PARTITIONED_LOCAL_INDEX:
        ret = schedule_build_index_task(record);
        break;
      case ObDDLType::DDL_DROP_INDEX:
      case ObDDLType::DDL_DROP_MLOG:
        ret = schedule_drop_index_task(record);
        break;
      case ObDDLType::DDL_CREATE_FTS_INDEX:
        ret = schedule_build_fts_index_task(record);
        break;
      case ObDDLType::DDL_DROP_FTS_INDEX:
      case ObDDLType::DDL_DROP_MULVALUE_INDEX:
        ret = schedule_drop_fts_index_task(record);
        break;
      case DDL_DROP_PRIMARY_KEY:
        ret = schedule_drop_primary_key_task(record);
        break;
      case DDL_MODIFY_COLUMN:
      case DDL_ADD_PRIMARY_KEY:
      case DDL_ALTER_PRIMARY_KEY:
      case DDL_ALTER_PARTITION_BY:
      case DDL_CONVERT_TO_CHARACTER:
      case DDL_TABLE_REDEFINITION:
      case DDL_DIRECT_LOAD:
      case DDL_DIRECT_LOAD_INSERT:
      case DDL_ALTER_COLUMN_GROUP:
      case DDL_MVIEW_COMPLETE_REFRESH:
      case DDL_MODIFY_AUTO_INCREMENT_WITH_REDEFINITION:
        ret = schedule_table_redefinition_task(record);
        break;
      case DDL_CREATE_MVIEW:
        ret = schedule_build_mview_task(record);
        break;
      case DDL_DROP_COLUMN:
      case DDL_ADD_COLUMN_OFFLINE:
      case DDL_COLUMN_REDEFINITION:
        ret = schedule_column_redefinition_task(record);
        break;
      case DDL_CHECK_CONSTRAINT:
      case DDL_FOREIGN_KEY_CONSTRAINT:
      case DDL_ADD_NOT_NULL_COLUMN:
        ret = schedule_constraint_task(record);
        break;
      case DDL_MODIFY_AUTO_INCREMENT:
        ret = schedule_modify_autoinc_task(record);
        break;
      case DDL_DROP_DATABASE:
      case DDL_DROP_TABLE:
      case DDL_TRUNCATE_TABLE:
      case DDL_DROP_PARTITION:
      case DDL_DROP_SUB_PARTITION:
      case DDL_RENAME_PARTITION:
      case DDL_RENAME_SUB_PARTITION:
      case DDL_TRUNCATE_PARTITION:
      case DDL_TRUNCATE_SUB_PARTITION:
        ret = schedule_ddl_retry_task(record);
        break;
      case DDL_TABLE_RESTORE:
        ret = schedule_recover_restore_table_task(record);
        break;
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported task type", K(ret), K(record));
        break;
      }
    }
    LOG_INFO("schedule ddl task", K(ret), K(record));
    if (OB_ENTRY_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObDDLScheduler::schedule_build_fts_index_task(
    const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObFtsIndexBuildTask *build_index_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(build_index_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else if (OB_FAIL(build_index_task->init(task_record))) {
    LOG_WARN("init global_index_task failed", K(ret), K(task_record));
  } else if (OB_FAIL(build_index_task->set_trace_id(task_record.trace_id_))) {
    LOG_WARN("init build index task failed", K(ret));
  } else if (OB_FAIL(inner_schedule_ddl_task(build_index_task, task_record))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret), K(*build_index_task));
    }
  }
  if (OB_FAIL(ret) && nullptr != build_index_task) {
    build_index_task->~ObFtsIndexBuildTask();
    allocator_.free(build_index_task);
    build_index_task = nullptr;
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
    } else if (OB_FAIL(inner_schedule_ddl_task(build_index_task, task_record))) {
      if (OB_ENTRY_EXIST != ret) {
        LOG_WARN("inner schedule task failed", K(ret), K(*build_index_task));
      }
    }
  }
  if (nullptr != build_index_task) {
    LOG_INFO("ddl_scheduler schedule build index task", K(ret), "ddl_event_info", ObDDLEventInfo(), K(task_record));
  }
  if (OB_FAIL(ret) && nullptr != build_index_task) {
    build_index_task->~ObIndexBuildTask();
    allocator_.free(build_index_task);
    build_index_task = nullptr;
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
  } else if (OB_FAIL(inner_schedule_ddl_task(drop_pk_task, task_record))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret), K(*drop_pk_task));
    }
  }
  if (nullptr != drop_pk_task) {
    LOG_INFO("ddl_scheduler schedule drop primary key task", K(ret), "ddl_event_info", ObDDLEventInfo(), K(task_record));
  }
  if (OB_FAIL(ret) && nullptr != drop_pk_task) {
    drop_pk_task->~ObDropPrimaryKeyTask();
    allocator_.free(drop_pk_task);
    drop_pk_task = nullptr;
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
  } else if (OB_FAIL(inner_schedule_ddl_task(redefinition_task, task_record))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret), K(*redefinition_task));
    }
  } else if (ObDDLTask::check_is_load_data(task_record.ddl_type_)
            && OB_FAIL(manager_reg_heart_beat_task_.update_task_active_time(ObDDLTaskID(task_record.tenant_id_, task_record.task_id_)))) {
    LOG_WARN("register_task_time recover fail", K(ret));
  }
  LOG_INFO("ddl_scheduler schedule table redefinition task", K(ret), "ddl_event_info", ObDDLEventInfo(), K(task_record));
  if (OB_FAIL(ret) && nullptr != redefinition_task) {
    redefinition_task->~ObTableRedefinitionTask();
    allocator_.free(redefinition_task);
    redefinition_task = nullptr;
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
  } else if (OB_FAIL(inner_schedule_ddl_task(redefinition_task, task_record))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret), K(*redefinition_task));
    }
  }
  if (nullptr != redefinition_task) {
    LOG_INFO("ddl_scheduler schedule column redefinition task", K(ret), "ddl_event_info", ObDDLEventInfo(), K(task_record));
  }
  if (OB_FAIL(ret) && nullptr != redefinition_task) {
    redefinition_task->~ObColumnRedefinitionTask();
    allocator_.free(redefinition_task);
    redefinition_task = nullptr;
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
  } else if (OB_FAIL(inner_schedule_ddl_task(ddl_retry_task, task_record))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret));
    }
  }
  if (nullptr != ddl_retry_task) {
    LOG_INFO("ddl_scheduler schedule ddl retry task", K(ret), "ddl_event_info", ObDDLEventInfo(), K(task_record));
  }
  if (OB_FAIL(ret) && nullptr != ddl_retry_task) {
    ddl_retry_task->~ObDDLRetryTask();
    allocator_.free(ddl_retry_task);
    ddl_retry_task = nullptr;
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
  } else if (OB_FAIL(inner_schedule_ddl_task(constraint_task, task_record))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret));
    }
  }
  if (nullptr != constraint_task) {
    LOG_INFO("ddl_scheduler schedule constraint task", K(ret), "ddl_event_info", ObDDLEventInfo(), K(task_record));
  }
  if (OB_FAIL(ret) && nullptr != constraint_task) {
    constraint_task->~ObConstraintTask();
    allocator_.free(constraint_task);
    constraint_task = nullptr;
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
  } else if (OB_FAIL(inner_schedule_ddl_task(modify_autoinc_task, task_record))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret));
    }
  }
  if (nullptr != modify_autoinc_task) {
    LOG_INFO("ddl_scheduler schedule modify autoinc task", K(ret), "ddl_event_info", ObDDLEventInfo(), K(task_record));
  }
  if (OB_FAIL(ret) && nullptr != modify_autoinc_task) {
    modify_autoinc_task->~ObModifyAutoincTask();
    allocator_.free(modify_autoinc_task);
    modify_autoinc_task = nullptr;
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
  } else if (OB_FAIL(inner_schedule_ddl_task(drop_index_task, task_record))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret));
    }
  }
  if (nullptr != drop_index_task) {
    LOG_INFO("ddl_scheduler schedule drop index task", K(ret), "ddl_event_info", ObDDLEventInfo(), K(task_record));
  }
  if (OB_FAIL(ret) && nullptr != drop_index_task) {
    drop_index_task->~ObDropIndexTask();
    allocator_.free(drop_index_task);
    drop_index_task = nullptr;
  }
  return ret;
}

int ObDDLScheduler::schedule_drop_fts_index_task(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObDropFTSIndexTask *drop_fts_index_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(drop_fts_index_task))) {
    LOG_WARN("fail to alloc drop fts index task", K(ret));
  } else if (OB_FAIL(drop_fts_index_task->init(task_record))) {
    LOG_WARN("fail to init drop fts index task", K(ret));
  } else if (OB_FAIL(drop_fts_index_task->set_trace_id(task_record.trace_id_))) {
    LOG_WARN("fail to set trace id", K(ret));
  } else if (OB_FAIL(inner_schedule_ddl_task(drop_fts_index_task, task_record))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("fail to inner schedule task", K(ret));
    }
  }
  if (OB_FAIL(ret) && nullptr != drop_fts_index_task) {
    drop_fts_index_task->~ObDropFTSIndexTask();
    allocator_.free(drop_fts_index_task);
    drop_fts_index_task = nullptr;
  }

  return ret;
}

int ObDDLScheduler::schedule_recover_restore_table_task(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObRecoverRestoreTableTask *redefinition_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(redefinition_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else if (OB_FAIL(redefinition_task->init(task_record))) {
    LOG_WARN("init table redefinition task failed", K(ret));
  } else if (OB_FAIL(redefinition_task->set_trace_id(task_record.trace_id_))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(inner_schedule_ddl_task(redefinition_task, task_record))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret), K(*redefinition_task));
    }
  }
  if (OB_FAIL(ret) && nullptr != redefinition_task) {
    redefinition_task->~ObRecoverRestoreTableTask();
    allocator_.free(redefinition_task);
    redefinition_task = nullptr;
  }
  return ret;
}

int ObDDLScheduler::create_build_mview_task(
    common::ObISQLClient &proxy,
    const ObTableSchema *mview_schema,
    const int64_t parallelism,
    const int64_t parent_task_id,
    const int64_t consumer_group_id,
    const obrpc::ObMViewCompleteRefreshArg *mview_complete_refresh_arg,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  SMART_VAR(ObBuildMViewTask, mview_task) {
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", KR(ret));
    } else if (OB_ISNULL(mview_complete_refresh_arg)
               || OB_ISNULL(mview_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(mview_complete_refresh_arg),
          K(mview_schema));
    } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(),
        mview_schema->get_tenant_id(), task_id))) {
      LOG_WARN("failed to fetch new task id", KR(ret));
    } else if (OB_FAIL(mview_task.init(mview_schema->get_tenant_id(),
                                      task_id,
                                      mview_schema,
                                      mview_schema->get_schema_version(),
                                      parallelism,
                                      consumer_group_id,
                                      *mview_complete_refresh_arg,
                                      parent_task_id))) {
      LOG_WARN("failed to init mview task", KR(ret));
    } else if (OB_FAIL(mview_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
      LOG_WARN("failed to set trace id", KR(ret));
    } else if (OB_FAIL(insert_task_record(proxy, mview_task, allocator, task_record))) {
      LOG_WARN("failed to insert task record", KR(ret));
    }
  }
  return ret;
}

int ObDDLScheduler::schedule_build_mview_task(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObBuildMViewTask *build_mview_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(alloc_ddl_task(build_mview_task))) {
    LOG_WARN("failed to alloc dll task", KR(ret));
  } else {
    if (OB_FAIL(build_mview_task->init(task_record))) {
      LOG_WARN("failed to init build mview task", KR(ret), K(task_record));
    } else if (OB_FAIL(build_mview_task->set_trace_id(task_record.trace_id_))) {
      LOG_WARN("failed to set trace id", KR(ret), K(task_record));
    } else if (OB_FAIL(inner_schedule_ddl_task(build_mview_task, task_record))) {
      if (OB_ENTRY_EXIST != ret) {
        LOG_WARN("failed to inner schedule task", KR(ret), KPC(build_mview_task));
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(build_mview_task)) {
    build_mview_task->~ObBuildMViewTask();
    allocator_.free(build_mview_task);
    build_mview_task = nullptr;
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
      if (OB_ENTRY_EXIST == ret) {
        LOG_INFO("longops for this ddl task already registered", K(*ddl_task));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to register longops", K(ret));
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

int ObDDLScheduler::remove_task_from_longops_mgr(ObDDLTask *ddl_task)
{
  int ret = OB_SUCCESS;
  ObLongopsMgr &longops_mgr = ObLongopsMgr::get_instance();
  if (OB_ISNULL(ddl_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ddl_task));
  } else if (ddl_task->support_longops_monitoring()) {
    if (OB_NOT_NULL(ddl_task->get_longops_stat())) {
      if (OB_FAIL(longops_mgr.unregister_longops(ddl_task->get_longops_stat()))) {
        LOG_WARN("failed to unregister longops", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLScheduler::remove_ddl_task(ObDDLTask *ddl_task)
{
  int ret = OB_SUCCESS;
  ObLongopsMgr &longops_mgr = ObLongopsMgr::get_instance();
  ObDDLTaskKey task_key;
  if (OB_ISNULL(ddl_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ddl_task));
  } else if (OB_FALSE_IT(task_key = ddl_task->get_task_key())) {
  } else if (OB_FAIL(task_queue_.remove_task(ddl_task))) {
    LOG_WARN("fail to remove task, which should not happen", K(ret), KPC(ddl_task));
  } else {
    if (OB_FAIL(remove_task_from_longops_mgr(ddl_task))) {
      LOG_WARN("failed to unregister longops", K(ret));
    }
    remove_sys_task(ddl_task);
    free_ddl_task(ddl_task);
  }
  LOG_INFO("remove ddl task", K(task_key));
  return ret;
}

int ObDDLScheduler::inner_schedule_ddl_task(ObDDLTask *ddl_task,
                                            const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ddl_task));
  } else if (!is_started_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ddl schedule is not start", K(ret));
  } else if (OB_FAIL(DDL_SIM(ddl_task->get_tenant_id(), ddl_task->get_task_id(), DDL_SCHEDULER_STOPPED))) {
    LOG_WARN("ddl sim failure: ddl scheduler not running", K(ret), K(ddl_task->get_ddl_task_id()));
  } else {
    int tmp_ret = OB_SUCCESS;
    bool longops_added = true;
    ddl_task->set_gmt_create(task_record.gmt_create_);
    if (OB_TMP_FAIL(add_task_to_longops_mgr(ddl_task))) {
      longops_added = false;
      LOG_WARN("add task to longops mgr failed", K(tmp_ret));
    }
    ddl_task->disable_schedule();
    if (OB_FAIL(task_queue_.push_task(ddl_task))) {
      if (OB_ENTRY_EXIST != ret) {
        LOG_WARN("push back task to task queue failed", K(ret));
      }
      if (longops_added) {
        if (OB_TMP_FAIL(remove_task_from_longops_mgr(ddl_task))) {
          LOG_WARN("failed to unregister longops", K(tmp_ret));
        }
      }
    } else {
      if (OB_TMP_FAIL(add_sys_task(ddl_task))) {
        LOG_WARN("add sys task failed", K(tmp_ret));
      }
      ddl_task->enable_schedule();
      idler_.wakeup();
    }
  }
  return ret;
}

int ObDDLScheduler::on_column_checksum_calc_reply(
    const common::ObTabletID &tablet_id,
    const ObDDLTaskKey &task_key,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!(task_key.is_valid() && tablet_id.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_key), K(tablet_id), K(ret_code));
  } else if (OB_FAIL(task_queue_.modify_task(task_key, [&tablet_id, &ret_code](ObDDLTask &task) -> int {
        int ret = OB_SUCCESS;
        if (OB_UNLIKELY(!is_create_index(task.get_task_type()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ddl task type not global index", K(ret), K(task));
        } else if (OB_FAIL(DDL_SIM(task.get_tenant_id(), task.get_task_id(), ON_COLUMN_CHECKSUM_REPLY_FAILED))) {
          LOG_WARN("sim column checksum reply failed", K(ret));
        } else if (OB_FAIL(reinterpret_cast<ObIndexBuildTask *>(&task)->update_column_checksum_calc_status(tablet_id, ret_code))) {
          LOG_WARN("update column checksum calc status failed", K(ret));
        }
        return ret;
      task.add_event_info("on column checksum calc reply");
      }))) {
    LOG_WARN("failed to modify task", K(ret));
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_NEED_RETRY;
  }
  LOG_INFO("receive column checksum response", K(ret), "ddl_event_info", ObDDLEventInfo(), K(tablet_id), K(task_key), K(ret_code));
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
  ObDDLType ddl_type = DDL_INVALID;
  ObDDLTaskID task_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!(task_key.is_valid() && snapshot_version > 0 && execution_id >= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_key), K(snapshot_version), K(execution_id), K(ret_code));
  } else if (OB_FAIL(task_queue_.get_task(task_key, [&ddl_type, &task_id](ObDDLTask &task) -> int {
      ddl_type = task.get_task_type();
      task_id = task.get_ddl_task_id();
      return OB_SUCCESS;
      }))) {
    LOG_WARN("get task failed", K(ret), K(task_key));
  } else if (is_direct_load_task(ddl_type)) {
    ObUpdateSSTableCompleteStatusCallback callback;
    callback.set_ret_code(ret_code);
    if (OB_FAIL(modify_redef_task(task_id, callback))) {
      LOG_WARN("fail to modify redef task", K(ret), K(task_id));
    }
  } else if (OB_FAIL(task_queue_.modify_task(task_key, [&tablet_id, &snapshot_version, &execution_id, &ret_code, &addition_info](ObDDLTask &task) -> int {
        int ret = OB_SUCCESS;
        const int64_t task_type = task.get_task_type();
        switch (task_type) {
          case ObDDLType::DDL_CREATE_INDEX:
          case ObDDLType::DDL_CREATE_PARTITIONED_LOCAL_INDEX:
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
          case ObDDLType::DDL_ALTER_COLUMN_GROUP:
          case ObDDLType::DDL_MVIEW_COMPLETE_REFRESH:
          case ObDDLType::DDL_MODIFY_AUTO_INCREMENT_WITH_REDEFINITION:
            if (OB_FAIL(static_cast<ObTableRedefinitionTask *>(&task)->update_complete_sstable_job_status(tablet_id, snapshot_version, execution_id, ret_code, addition_info))) {
              LOG_WARN("update complete sstable job status", K(ret));
            }
            break;
          case ObDDLType::DDL_TABLE_RESTORE:
            if (OB_FAIL(static_cast<ObRecoverRestoreTableTask *>(&task)->update_complete_sstable_job_status(tablet_id, snapshot_version, execution_id, ret_code, addition_info))) {
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
        task.add_event_info("on sstable complement job reply");
      }))) {
    LOG_WARN("failed to modify task", K(ret));
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_NEED_RETRY;
  }
  LOG_INFO("ddl sstable complement job reply", K(ret), "ddl_event_info", ObDDLEventInfo(), K(tablet_id), K(task_key), K(ret_code));
  return ret;
}

int ObDDLScheduler::on_ddl_task_prepare(
    const ObDDLTaskID &parent_task_id,
    const int64_t task_id,
    const ObCurTraceId::TraceId &parent_task_trace_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(!parent_task_id.is_valid()) || task_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(parent_task_id), K(task_id));
  } else {
    ObDDLTask *ddl_task = nullptr;
    struct Func {
      Func(const int64_t task_id) :task_id_(task_id) {}
      int operator()(ObDDLTask &task) {
        ObBuildMViewTask *build_mv_task = static_cast<ObBuildMViewTask *>(&task);
        return build_mv_task->on_child_task_prepare(task_id_);
      }
      int64_t task_id_;
    } func(task_id);

    ret = task_queue_.modify_task(parent_task_id, func);
    if (OB_ENTRY_NOT_EXIST == ret) {
      bool exist = false;
      if (OB_FAIL(ObDDLTaskRecordOperator::check_task_id_exist(*GCTX.sql_proxy_, parent_task_id.tenant_id_, parent_task_id.task_id_, exist))) {
        LOG_WARN("check task id exist fail", K(ret), K(parent_task_id));
      } else {
        if (exist) {
          ret = OB_EAGAIN;
          LOG_INFO("entry exist, the ddl scheduler hasn't recovered the task yet", K(ret), K(parent_task_id));
        } else {
          LOG_WARN("this task does not exist int hash table", K(ret), K(parent_task_id));
        }
      }
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to modify task", K(ret));
    }
  }
  LOG_INFO("ddl task on prepare", K(ret), "ddl_event_info", ObDDLEventInfo(), K(parent_task_id), K(task_id), K(parent_task_trace_id));
  return ret;
}

int ObDDLScheduler::on_ddl_task_finish(
    const ObDDLTaskID &parent_task_id,
    const ObDDLTaskKey &child_task_key, 
    const int ret_code,
    const ObCurTraceId::TraceId &parent_task_trace_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(!parent_task_id.is_valid() || !child_task_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(parent_task_id), K(child_task_key));
  } else {
    ObDDLTask *ddl_task = nullptr;
    if (OB_FAIL(task_queue_.modify_task(parent_task_id, [&child_task_key, &ret_code](ObDDLTask &task) -> int {
          return task.on_child_task_finish(child_task_key.object_id_, ret_code);
        task.add_event_info("ddl task finish");
        }))) {
      LOG_WARN("failed to modify task", K(ret));
    }
  }
  LOG_INFO("ddl task on finish", K(ret), K(ret_code), "ddl_event_info", ObDDLEventInfo(), K(parent_task_id), K(child_task_key), K(parent_task_trace_id));
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
          case ObDDLType::DDL_DIRECT_LOAD_INSERT:
          case ObDDLType::DDL_ALTER_COLUMN_GROUP:
          case ObDDLType::DDL_MVIEW_COMPLETE_REFRESH:
          case ObDDLType::DDL_MODIFY_AUTO_INCREMENT_WITH_REDEFINITION:
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

int ObDDLScheduler::update_ddl_task_active_time(const ObDDLTaskID &task_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!task_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_id));
  } else if (OB_FAIL(manager_reg_heart_beat_task_.update_task_active_time(task_id))) {
    LOG_WARN("fail to set RegTaskTime map", K(ret), K(task_id));
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase


