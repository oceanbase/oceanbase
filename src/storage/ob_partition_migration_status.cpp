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

#include "ob_partition_migration_status.h"

namespace oceanbase {
namespace storage {

ObPartitionMigrationStatus::ObPartitionMigrationStatus()
    : task_id_(),
      pkey_(),
      clog_parent_(),
      src_(),
      dest_(),
      result_(-1),
      start_time_(-1),
      action_(ObMigrateCtx::UNKNOWN),
      replica_state_(OB_UNKNOWN_REPLICA),
      doing_task_count_(-1),
      total_task_count_(-1),
      rebuild_count_(-1),
      continue_fail_count_(-1),
      comment_(),
      finish_time_(0),
      data_statics_()
{
  migrate_type_ = "";
}

ObPartitionMigrationStatusGuard::ObPartitionMigrationStatusGuard() : status_(NULL), lock_(NULL)
{}

ObPartitionMigrationStatusGuard::~ObPartitionMigrationStatusGuard()
{
  if (NULL != lock_) {
    lock_->unlock();
  }
}

int ObPartitionMigrationStatusGuard::set_status(ObPartitionMigrationStatus& status, common::SpinRWLock& lock)
{
  int ret = OB_SUCCESS;

  if (NULL != status_ || NULL != lock_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else {
    status_ = &status;
    lock_ = &lock;  // lock should be locked by caller
  }

  return ret;
}

ObPartitionMigrationStatusMgr::ObPartitionMigrationStatusMgr() : lock_(), status_array_()
{
  status_array_.set_label(ObModIds::OB_PARTITION_MIGRATION_STATUS);
  int64_t max_migration_status_count =
      lib::is_mini_mode() ? MAX_MIGRATION_STATUS_COUNT_MINI_MODE : MAX_MIGRATION_STATUS_COUNT;
  status_array_.reserve(max_migration_status_count);  // ignore ret
}

ObPartitionMigrationStatusMgr::~ObPartitionMigrationStatusMgr()
{}

ObPartitionMigrationStatusMgr& ObPartitionMigrationStatusMgr::get_instance()
{
  static ObPartitionMigrationStatusMgr mgr;
  return mgr;
}

int ObPartitionMigrationStatusMgr::add_status(const ObPartitionMigrationStatus& status)
{
  int ret = OB_SUCCESS;
  int64_t max_migration_status_count =
      lib::is_mini_mode() ? MAX_MIGRATION_STATUS_COUNT_MINI_MODE : MAX_MIGRATION_STATUS_COUNT;

#ifdef ERRSIM
  max_migration_status_count = GCONF._max_migration_status_count;
  if (0 == max_migration_status_count) {
    max_migration_status_count = MAX_MIGRATION_STATUS_COUNT;
  }
#endif

  SpinWLockGuard guard(lock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < status_array_.count(); ++i) {
    const ObPartitionMigrationStatus& tmp = status_array_.at(i);
    if (tmp.task_id_.equals(status.task_id_)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "task id must not same", K(ret), K(tmp), K(status));
    }
  }

  if (OB_SUCC(ret)) {
    if (status_array_.count() >= max_migration_status_count && OB_FAIL(remove_oldest_status())) {
      STORAGE_LOG(WARN, "failed to remove oldest status", K(ret));
    } else if (OB_FAIL(status_array_.push_back(status))) {
      STORAGE_LOG(WARN, "failed to add status", K(ret));
    }
  }

  return ret;
}

// caller need to hold write lock
int ObPartitionMigrationStatusMgr::remove_oldest_status()
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret) && status_array_.count() > 0) {
    if (OB_FAIL(status_array_.remove(0))) {
      STORAGE_LOG(WARN, "failed to remove status", K(ret));
    }
  }

  return ret;
}

int ObPartitionMigrationStatusMgr::get_status(const share::ObTaskId& task_id, ObPartitionMigrationStatusGuard& guard)
{
  int ret = OB_SUCCESS;
  lock_.wrlock();

  for (int64_t i = status_array_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    if (status_array_.at(i).task_id_.equals(task_id)) {
      if (OB_FAIL(guard.set_status(status_array_.at(i), lock_))) {
        STORAGE_LOG(WARN, "failed to set status", K(ret));
      }
      break;
    }
  }

  if (NULL == guard.getLock()) {
    lock_.unlock();
  }

  return ret;
}

int ObPartitionMigrationStatusMgr::get_iter(ObPartitionMigrationStatusMgrIter& iter)
{
  int ret = OB_SUCCESS;
  iter.reset();

  SpinRLockGuard guard(lock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < status_array_.count(); ++i) {
    if (OB_FAIL(iter.push(status_array_.at(i)))) {
      STORAGE_LOG(WARN, "failed to add status iter", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    iter.set_ready();
  }
  return ret;
}

int ObPartitionMigrationStatusMgr::del_status(const share::ObTaskId& task_id)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;

  SpinWLockGuard guard(lock_);

  for (int64_t i = 0; OB_SUCC(ret) && i < status_array_.count(); ++i) {
    if (status_array_.at(i).task_id_.equals(task_id)) {
      idx = i;
      break;
    }
  }

  if (OB_SUCC(ret) && idx >= 0) {
    if (OB_FAIL(status_array_.remove(idx))) {
      STORAGE_LOG(WARN, "failed to remove status", K(ret), K(idx), K(status_array_.count()));
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
