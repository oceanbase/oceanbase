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

#include "ob_sys_task_stat.h"

namespace oceanbase
{
using namespace common;
namespace share
{
const static char *ObSysTaskTypeStr[] = {
    "GROUP_MIGRATION",
    "SINGLE_MIGRATION",
    "DDL",
    "SSTABLE_MINI_MERGE",
    "SPECIAL_TABLE_MERGE",
    "SSTABLE_MINOR_MERGE",
    "SSTABLE_MAJOR_MERGE",
    "WRITE_CKPT",
    "BACKUP",
    "BACKUP_VALIDATION",
    "BACKUP_BACKUPSET",
    "BACKUP_ARCHIVELOG",
    "DDL_KV_MERGE",
    "COMPLEMENT_DATA",
    "RESTORE",
    "BACKUP_CLEAN",
    "BACKFILL_TX",
    "REMOVE_MEMBER",
    "TRANSFER",
    "MDS_MINI_MERGE",
    "TTL_TASK",
    "TENANT_SNAPSHOT_CREATE",
    "TENANT_SNAPSHOT_GC",
    "BATCH_FREEZE_TABLET_TASK"
};

const char *sys_task_type_to_str(const ObSysTaskType &type)
{
  STATIC_ASSERT(static_cast<int64_t>(MAX_SYS_TASK_TYPE) == ARRAYSIZEOF(ObSysTaskTypeStr), "sys_task_type str len is mismatch");
  const char *str = "";
  if (OB_UNLIKELY(type < 0 || type >= MAX_SYS_TASK_TYPE)) {
    str = "invalid task type";
  } else {
    str = ObSysTaskTypeStr[type];
  }
  return str;
}

ObSysTaskStat::ObSysTaskStat()
  : start_time_(0),
    task_id_(),
    task_type_(MAX_SYS_TASK_TYPE),
    svr_ip_(),
    tenant_id_(0),
    comment_(),
    is_cancel_(false)
{
  comment_[0] = '\0';
}

ObSysTaskStatMgr::ObSysTaskStatMgr()
  : lock_(common::ObLatchIds::SYS_TASK_STAT_LOCK),
    task_array_()
{
  task_array_.set_label(ObModIds::OB_SYS_TASK_STATUS);
  task_array_.reserve(DEFAULT_SYS_TASK_STATUS_COUNT); // ignore ret
}

ObSysTaskStatMgr::~ObSysTaskStatMgr()
{
}

ObSysTaskStatMgr &ObSysTaskStatMgr::get_instance()
{
  static ObSysTaskStatMgr mgr_;
  return mgr_;
}

int ObSysTaskStatMgr::get_iter(ObSysStatMgrIter &iter)
{
  int ret = OB_SUCCESS;
  iter.reset();

  SpinRLockGuard guard(lock_);

  for (int64_t i = 0; OB_SUCC(ret) && i < task_array_.count(); ++i) {
    if (OB_FAIL(iter.push(task_array_.at(i)))) {
      SERVER_LOG(WARN, "failed to add task status", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(iter.set_ready())) {
      SERVER_LOG(WARN, "failed to set iter ready", K(ret));
    }
  }

  return ret;
}

int ObSysTaskStatMgr::generate_task_id(ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  if (!self_addr_.is_valid()) {
    ret = OB_INVALID_ERROR;
    SERVER_LOG(ERROR, "self_addr_ is invalid", K(ret), K(self_addr_));
  } else {
    task_id.reset();
    task_id.init(self_addr_);
  }
  return ret;
}

int ObSysTaskStatMgr::add_task(ObSysTaskStat &task)
{
  int ret = OB_SUCCESS;

  if (!self_addr_.is_valid()) {
    ret = OB_INVALID_ERROR;
    SERVER_LOG(ERROR, "self_addr_ is invalid", K(ret), K(self_addr_));
  } else {
    task.svr_ip_ = self_addr_;
    if (!task.task_id_.is_invalid()) {
      SERVER_LOG(INFO, "task id is valid, no need set new", K(ret), K(task));
    } else {
      task.task_id_.init(self_addr_);
    }
  }


  SpinWLockGuard guard(lock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < task_array_.count(); ++i) {
    if (task_array_.at(i).task_id_.equals(task.task_id_)) {
      ret = OB_ENTRY_EXIST;
      if (DDL_TASK != task.task_type_) {
        SERVER_LOG(ERROR, "task id is exist, cannot add again",
            K(ret), K(task), K(i), K(task_array_.at(i)));
      } else {
        SERVER_LOG(WARN, "ddl task id is exist, cannot add again",
            K(ret), K(task), K(i), K(task_array_.at(i)));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(task_array_.push_back(task))) {
      SERVER_LOG(WARN, "failed to add task status", K(ret));
    } else {
      SERVER_LOG(INFO, "succeed to add sys task", K(task));
    }
  }

  return ret;
}

int ObSysTaskStatMgr::del_task(const ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  int64_t status_idx = -1;

  if (task_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid task_id", K(ret), K(task_id));
  } else {
    SpinWLockGuard guard(lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < task_array_.count(); ++i) {
      if (task_array_.at(i).task_id_.equals(task_id)) {
        status_idx = i;
      }
    }

    if (OB_SUCC(ret)) {
      if (status_idx != -1) {
        ObSysTaskStat removed_task = task_array_.at(status_idx);
        if (OB_FAIL(task_array_.remove(status_idx))) {
          SERVER_LOG(WARN, "failed to del task status", K(ret), K(status_idx));
        } else {
          SERVER_LOG(INFO, "succeed to del sys task", K(removed_task));
        }
      } else {
        ret = OB_ENTRY_NOT_EXIST;
        SERVER_LOG(WARN, "sys task not exist", K(ret), K(task_id));
      }
    }
  }

  return ret;
}

int ObSysTaskStatMgr::update_task(const ObTaskId &task_id, const char *msg, const int64_t msg_len)
{
  int ret = OB_SUCCESS;
  ObSysTaskStat *updated_task = nullptr;
  if (task_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "get invalid argument in ObSysTaskStatMgr::update_task", K(ret), K(task_id));
  } else {
    SpinWLockGuard guard(lock_);
    int64_t i = 0;
    for ( ; OB_SUCC(ret) && i < task_array_.count(); ++i) {
      updated_task = &task_array_.at(i);
      if (OB_ISNULL(updated_task)) {
      } else if (updated_task->task_id_.equals(task_id)) {
        if (-1 == (snprintf(updated_task->comment_, msg_len, "%s", msg))) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "failed to update task", K(ret), K(task_id), K(updated_task->comment_), K(msg));
        }
        break;
      }
    }

    if (i == task_array_.count()) {
      ret = OB_ENTRY_NOT_EXIST;
      SERVER_LOG(WARN, "sys task not exist", K(ret), K(task_id));
    }
  }
  return ret;
}

int ObSysTaskStatMgr::set_self_addr(const ObAddr addr)
{
  int ret = OB_SUCCESS;
  if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
      self_addr_ = addr;
  }
  return ret;
}

int ObSysTaskStatMgr::task_exist(const ObTaskId &task_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;

  if (task_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid task id", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < task_array_.count(); ++i) {
      if (task_id.equals(task_array_.at(i).task_id_)) {
        is_exist = true;
      }
    }
  }

  return ret;
}

int ObSysTaskStatMgr::cancel_task(const ObTaskId &task_id)
{
  int ret = OB_SUCCESS;

  if (task_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid task id", K(ret));
  } else {
    SpinWLockGuard guard(lock_);
    bool found_task = false;
    for (int64_t i = 0; i < task_array_.count(); ++i) {
      if (task_id.equals(task_array_.at(i).task_id_)) {
        found_task = true;
        task_array_.at(i).is_cancel_ = true;
        SERVER_LOG(INFO, "cancel task", "task", task_array_.at(i), K(i));
      }
    }

    if (!found_task) {
      ret = OB_ENTRY_NOT_EXIST;
      SERVER_LOG(WARN, "task not exist, cannot cancel", K(ret), K(task_id));
    }
  }

  return ret;
}

int ObSysTaskStatMgr::is_task_cancel(const ObTaskId &task_id, bool &is_cancel)
{
  int ret = OB_SUCCESS;
  is_cancel = false;

  if (task_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid task id", K(ret), K(task_id));
  } else {
    SpinRLockGuard guard(lock_);
    bool found_task = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < task_array_.count(); ++i) {
      if (task_id.equals(task_array_.at(i).task_id_)) {
        found_task = true;
        is_cancel = task_array_.at(i).is_cancel_;
      }
    }
    if (!found_task) {
      ret = OB_ENTRY_NOT_EXIST;
      SERVER_LOG(WARN, "task not exist, cannot check is cancel", K(ret), K(task_id));
    }
  }

  return ret;
}
}//common
}//oceanbase
