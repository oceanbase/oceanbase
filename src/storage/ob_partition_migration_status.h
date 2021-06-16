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

#ifndef SRC_STORAGE_OB_PARTITION_MIGRATEIONSTATUS_H_
#define SRC_STORAGE_OB_PARTITION_MIGRATEIONSTATUS_H_
#include "ob_partition_migrator.h"

namespace oceanbase {
namespace storage {

static const int64_t MAX_MIGRATION_STATUS_COUNT = 10240;
static const int64_t MAX_MIGRATION_STATUS_COUNT_MINI_MODE = 1024;

class ObPartitionMigrationStatus;
typedef common::ObSimpleIterator<ObPartitionMigrationStatus, common::ObModIds::OB_PARTITION_MIGRATION_STATUS,
    MAX_MIGRATION_STATUS_COUNT>
    ObPartitionMigrationStatusMgrIter;

struct ObPartitionMigrationStatus {
  ObPartitionMigrationStatus();

  share::ObTaskId task_id_;
  const char* migrate_type_;
  common::ObPartitionKey pkey_;
  common::ObAddr clog_parent_;
  common::ObAddr src_;
  common::ObAddr dest_;
  int32_t result_;
  int64_t start_time_;
  ObMigrateCtx::MigrateAction action_;
  ObPartitionReplicaState replica_state_;
  int64_t doing_task_count_;
  int64_t total_task_count_;
  int64_t rebuild_count_;
  int64_t continue_fail_count_;
  char comment_[common::OB_MAX_TASK_COMMENT_LENGTH];
  int64_t finish_time_;
  ObPartitionMigrationDataStatics data_statics_;
  TO_STRING_KV(K_(task_id), K_(*migrate_type), K_(pkey), K_(clog_parent), K_(src), K_(dest), K_(result), K_(start_time),
      K_(action), "replica_state", partition_replica_state_to_str(replica_state_), K_(doing_task_count),
      K_(total_task_count), K_(rebuild_count), K_(continue_fail_count), K_(comment), K_(finish_time), "cost_ts",
      finish_time_ - start_time_, K_(data_statics), "migration_status_size", sizeof(*this));
};

class ObPartitionMigrationStatusGuard {
public:
  explicit ObPartitionMigrationStatusGuard();
  virtual ~ObPartitionMigrationStatusGuard();

  int set_status(ObPartitionMigrationStatus& status, common::SpinRWLock& lock);

  ObPartitionMigrationStatus*& getStatus()
  {
    return status_;
  }
  common::SpinRWLock*& getLock()
  {
    return lock_;
  }

private:
  ObPartitionMigrationStatus* status_;
  common::SpinRWLock* lock_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionMigrationStatusGuard);
};

class ObPartitionMigrationStatusMgr {
public:
  ObPartitionMigrationStatusMgr();
  virtual ~ObPartitionMigrationStatusMgr();
  static ObPartitionMigrationStatusMgr& get_instance();

  int add_status(const ObPartitionMigrationStatus& status);
  int get_status(const share::ObTaskId& task_id, ObPartitionMigrationStatusGuard& guard);
  int get_iter(ObPartitionMigrationStatusMgrIter& iter);
  int del_status(const share::ObTaskId& task_id);

private:
  int remove_oldest_status();

private:
  common::SpinRWLock lock_;
  common::ObArray<ObPartitionMigrationStatus> status_array_;

  DISALLOW_COPY_AND_ASSIGN(ObPartitionMigrationStatusMgr);
};

}  // namespace storage
}  // namespace oceanbase
#endif /* SRC_STORAGE_OB_PARTITION_MIGRATEIONSTATUS_H_ */
