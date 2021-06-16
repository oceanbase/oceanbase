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

#ifndef __OB_PHYSICAL_RESTORE_INFO_H__
#define __OB_PHYSICAL_RESTORE_INFO_H__

#include "share/backup/ob_backup_struct.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace share {
enum PhysicalRestoreMod {
  PHYSICAL_RESTORE_MOD_RS = 0,
  PHYSICAL_RESTORE_MOD_CLOG = 1,
  PHYSICAL_RESTORE_MOD_STORAGE = 2,
  PHYSICAL_RESTORE_MOD_MAX_NUM
};

/* physical restore related */
enum PhysicalRestoreStatus {
  PHYSICAL_RESTORE_CREATE_TENANT = 0,           // restore tenant schema
  PHYSICAL_RESTORE_SYS_REPLICA = 1,             // restore sys table replica in tenant space
  PHYSICAL_RESTORE_UPGRADE_PRE = 2,             // upgrade pre & publish schema
  PHYSICAL_RESTORE_UPGRADE_POST = 3,            // upgrade post
  PHYSICAL_RESTORE_MODIFY_SCHEMA = 4,           // modify schema
  PHYSICAL_RESTORE_CREATE_USER_PARTITIONS = 5,  // create pg, standalone partition, partition in pg
  PHYSICAL_RESTORE_USER_REPLICA = 6,            // restore user replica
  PHYSICAL_RESTORE_REBUILD_INDEX = 7,           // schedule create index tasks
  PHYSICAL_RESTORE_POST_CHECK = 8,              // check index status and finish
  PHYSICAL_RESTORE_SUCCESS = 9,                 // restore success
  PHYSICAL_RESTORE_FAIL = 10,                   // restore fail
  PHYSICAL_RESTORE_MAX_STATUS
};

struct ObSimplePhysicalRestoreJob;
struct ObPhysicalRestoreJob final {
public:
  ObPhysicalRestoreJob();
  ~ObPhysicalRestoreJob()
  {}
  int assign(const ObPhysicalRestoreJob& other);
  int assign(share::ObRestoreBackupInfo& backup_info);
  int copy_to(share::ObPhysicalRestoreInfo& restore_info) const;
  int copy_to(ObSimplePhysicalRestoreJob& restore_info) const;
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(job_id), K_(tenant_id), K_(restore_data_version), K_(status), K_(restore_start_ts),
      K_(restore_job_id), K_(restore_timestamp), K_(cluster_id), "restore_option",
      common::ObString(common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH, restore_option_), "backup_dest",
      common::ObString(share::OB_MAX_BACKUP_DEST_LENGTH, backup_dest_), "tenant_name",
      common::ObString(common::OB_MAX_TENANT_NAME_LENGTH_STORE, tenant_name_), "backup_tenant_name",
      common::ObString(common::OB_MAX_TENANT_NAME_LENGTH_STORE, backup_tenant_name_), "backup_cluster_name",
      common::ObString(common::OB_MAX_CLUSTER_NAME_LENGTH, backup_cluster_name_), "pool_list",
      common::ObString(common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH, pool_list_), "locality",
      common::ObString(common::MAX_LOCALITY_LENGTH, locality_), "primary_zone",
      common::ObString(common::MAX_ZONE_LENGTH, primary_zone_), "backup_locality",
      common::ObString(common::MAX_LOCALITY_LENGTH, backup_locality_), "backup_primary_zone",
      common::ObString(common::MAX_ZONE_LENGTH, backup_primary_zone_), "info",
      common::ObString(common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH, info_), K_(compat_mode), K_(backup_tenant_id),
      K_(incarnation), K_(full_backup_set_id), K_(inc_backup_set_id), K_(log_archive_round), K_(snapshot_version),
      K_(schema_version), K_(frozen_data_version), K_(frozen_snapshot_version), K_(frozen_schema_version),
      K_(passwd_array), K_(source_cluster_version), K_(pre_cluster_version), K_(post_cluster_version), K_(compatible));

public:
  /* from rs */
  int64_t job_id_;
  uint64_t tenant_id_;
  int64_t restore_data_version_;
  PhysicalRestoreStatus status_;
  int64_t restore_start_ts_;
  char info_[common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH];
  uint64_t pre_cluster_version_;
  uint64_t post_cluster_version_;
  /* from restore tenant cmd*/
  int64_t restore_job_id_;
  int64_t restore_timestamp_;
  int64_t cluster_id_;
  char restore_option_[common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH];  // from cmd
  char backup_dest_[share::OB_MAX_BACKUP_DEST_LENGTH];
  char tenant_name_[common::OB_MAX_TENANT_NAME_LENGTH_STORE];         // from cmd
  char backup_tenant_name_[common::OB_MAX_TENANT_NAME_LENGTH_STORE];  // from restore option
  char backup_cluster_name_[common::OB_MAX_CLUSTER_NAME_LENGTH];
  char pool_list_[common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH];
  char locality_[common::MAX_LOCALITY_LENGTH];
  char primary_zone_[common::MAX_ZONE_LENGTH];
  char passwd_array_[common::OB_MAX_PASSWORD_ARRAY_LENGTH];
  /* from oss */
  char backup_locality_[common::MAX_LOCALITY_LENGTH];
  char backup_primary_zone_[common::MAX_ZONE_LENGTH];
  lib::Worker::CompatMode compat_mode_;
  uint64_t backup_tenant_id_;
  int64_t incarnation_;
  int64_t full_backup_set_id_;
  int64_t inc_backup_set_id_;
  int64_t log_archive_round_;
  int64_t snapshot_version_;
  int64_t schema_version_;
  int64_t frozen_data_version_;
  int64_t frozen_snapshot_version_;
  int64_t frozen_schema_version_;
  uint64_t source_cluster_version_;
  int64_t compatible_;
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalRestoreJob);
};
/* physical restore related end */

struct ObSimplePhysicalRestoreJob final {
public:
  ObPhysicalRestoreInfo restore_info_;
  int64_t snapshot_version_;
  int64_t schema_version_;
  int64_t job_id_;

  ObSimplePhysicalRestoreJob();
  virtual ~ObSimplePhysicalRestoreJob() = default;
  bool is_valid() const;
  int assign(const ObSimplePhysicalRestoreJob& other);
  int copy_to(share::ObPhysicalRestoreInfo& restore_info) const;
  TO_STRING_KV(K_(restore_info), K_(snapshot_version), K_(schema_version), K_(job_id));
  DISALLOW_COPY_AND_ASSIGN(ObSimplePhysicalRestoreJob);
};

struct ObRestoreProgressInfo {
public:
  ObRestoreProgressInfo()
  {
    reset();
  }
  ~ObRestoreProgressInfo()
  {}
  void reset();
  TO_STRING_KV(K_(total_pg_cnt), K_(finish_pg_cnt), K_(total_partition_cnt), K_(finish_partition_cnt));

public:
  int64_t total_pg_cnt_;  // standalone pg cnt
  int64_t finish_pg_cnt_;
  int64_t total_partition_cnt_;  // partition cnt (sql view)
  int64_t finish_partition_cnt_;
};
}  // namespace share
}  // namespace oceanbase
#endif /* __OB_PHYSICAL_RESTORE_INFO_H__ */
//// end of header file
