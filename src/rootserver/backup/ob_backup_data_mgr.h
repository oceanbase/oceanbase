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

#ifndef OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_DATA_MGR_H_
#define OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_DATA_MGR_H_

#include "share/ob_define.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/ob_partition_base_data_physical_restore.h"
#include "share/backup/ob_backup_path.h"

namespace oceanbase {
namespace rootserver {

class ObBackupDataMgr {
public:
  ObBackupDataMgr();
  virtual ~ObBackupDataMgr();
  int init(const share::ObClusterBackupDest& backup_dest, const uint64_t tenant_id, const int64_t full_backup_set_id,
      const int64_t inc_backup_set_id);
  int get_base_data_table_id_list(common::ObIArray<int64_t>& table_id_array);
  int get_table_pg_meta_index(const int64_t table_id, common::ObIArray<ObBackupMetaIndex>& meta_index_array);
  int get_pg_meta_index(const ObPartitionKey& pkey, ObBackupMetaIndex& meta_index);
  int get_pg_meta(const ObPartitionKey& pkey, storage::ObPartitionGroupMeta& pg_meta);

private:
  int get_clog_pkey_list(common::ObIArray<common::ObPartitionKey>& pkeys);

private:
  bool is_inited_;
  storage::ObPhyRestoreMetaIndexStore meta_index_;
  share::ObClusterBackupDest cluster_backup_dest_;
  int64_t full_backup_set_id_;
  int64_t inc_backup_set_id_;
  uint64_t tenant_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataMgr);
};

class ObBackupListDataMgr {
public:
  ObBackupListDataMgr();
  virtual ~ObBackupListDataMgr();
  int init(const share::ObClusterBackupDest& backup_dest, const int64_t log_archive_round, const uint64_t tenant_id);
  int get_clog_pkey_list(common::ObIArray<common::ObPartitionKey>& pkeys);

private:
  bool is_inited_;
  share::ObClusterBackupDest cluster_backup_dest_;
  int64_t log_archive_round_;
  uint64_t tenant_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupListDataMgr);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_DATA_MGR_H_
