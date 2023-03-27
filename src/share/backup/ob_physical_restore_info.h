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
#include "share/backup/ob_multi_backup_dest_util.h"
#include "share/ob_errno.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace obrpc {
class ObTableItem;
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

class ObPhysicalRestoreWhiteList {
public:
  ObPhysicalRestoreWhiteList();
  virtual ~ObPhysicalRestoreWhiteList();

  int assign(const ObPhysicalRestoreWhiteList& other);
  // str without '\0'
  int assign_with_hex_str(const common::ObString& str);
  void reset();

  int add_table_item(const obrpc::ObTableItem& item);
  // length without '\0'
  int64_t get_format_str_length() const;
  // str without '\0'
  int get_format_str(common::ObIAllocator& allocator, common::ObString& str) const;
  // str without '\0'
  int get_hex_str(common::ObIAllocator& allocator, common::ObString& str) const;
  const common::ObSArray<obrpc::ObTableItem>& get_table_white_list() const
  {
    return table_items_;
  }
  DECLARE_TO_STRING;

private:
  ObArenaAllocator allocator_;
  common::ObSArray<obrpc::ObTableItem> table_items_;
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

  DECLARE_TO_STRING;

public:
  /* from rs */
  int64_t job_id_;
  uint64_t tenant_id_;
  int64_t restore_data_version_;
  PhysicalRestoreStatus status_;
  int64_t restore_start_ts_;
  int64_t restore_schema_version_;  // the lasted schema version from inner table after sys replicas have restored.
  int64_t rebuild_index_schema_version_;  // the lasted schema version from inner table before modify index status.
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
  // maybe from user specified restore source or from get from oss with backup_dest
  ObPhysicalRestoreBackupDestList multi_restore_path_list_;
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
  int64_t backup_date_;
  ObPhysicalRestoreWhiteList white_list_;
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
  void reset();
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
