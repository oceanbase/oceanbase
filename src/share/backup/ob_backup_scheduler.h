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

#ifndef OCEANBASE_SHARE_BACKUP_OB_BACKUP_SCHEDULER_H_
#define OCEANBASE_SHARE_BACKUP_OB_BACKUP_SCHEDULER_H_

#include "ob_backup_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_rpc_struct.h"
#include "ob_backup_manager.h"
#include "rootserver/ob_freeze_info_manager.h"
#include "rootserver/ob_root_backup.h"
#include "rootserver/ob_restore_point_service.h"

namespace oceanbase {
namespace share {

class ObBackupScheduler {
public:
  ObBackupScheduler();
  virtual ~ObBackupScheduler();
  int init(const obrpc::ObBackupDatabaseArg& arg, schema::ObMultiVersionSchemaService& schema_service,
      common::ObMySQLProxy& proxy, rootserver::ObRootBackup& root_backup,
      rootserver::ObFreezeInfoManager& freeze_info_manager, rootserver::ObRestorePointService& restore_point_service);
  int start_schedule_backup();

private:
  int get_tenant_ids(common::ObIArray<uint64_t>& tenant_ids);
  int check_can_backup(const common::ObIArray<ObBaseBackupInfoStruct>& infos);
  int schedule_backup(const common::ObIArray<uint64_t>& tenant_ids, ObBackupInfoManager& info_manager);
  int schedule_sys_tenant_backup(
      const int64_t backup_snapshot_version, const uint64_t tenant_id, ObBackupInfoManager& info_manager);
  int schedule_tenant_backup(const int64_t backup_snapshot_version, const uint64_t tenant_id,
      const ObBaseBackupInfoStruct::BackupDest& backup_dest, common::ObISQLClient& sys_tenant_trans,
      ObBackupInfoManager& info_manager);
  int schedule_tenants_backup(const int64_t backup_snapshot_version, const common::ObIArray<uint64_t>& tenant_ids,
      ObBackupInfoManager& info_manager);

  int get_tenant_schema_version(const uint64_t tenant_id, int64_t& schema_version);
  int fetch_schema_version(const uint64_t tenant_id, int64_t& schema_version);
  int start_backup(ObBackupInfoManager& info_manager);
  int get_max_backup_set_id(const common::ObIArray<ObBaseBackupInfoStruct>& infos);
  int rollback_backup_infos(ObBackupInfoManager& info_manager);
  int rollback_backup_info(const ObBaseBackupInfoStruct& info, ObBackupInfoManager& info_manager);
  int check_backup_task_infos_status(const ObBaseBackupInfoStruct& info);
  int check_tenant_can_backup(const uint64_t tenant_id, const int64_t backup_schema_version,
      ObBackupInfoManager& info_manager, bool& can_backup);
  int check_gts_(const common::ObIArray<uint64_t>& tenant_ids);
  int check_gts_(const uint64_t tenant_id);
  int init_frozen_schema_versions_(rootserver::ObFreezeInfoManager& freeze_info_manager, const int64_t frozen_version);
  int check_backup_schema_version_(const uint64_t tenant_id, const int64_t backup_schema_version);
  int create_backup_point(const uint64_t tenant_id);
  int check_log_archive_status();

private:
  static const int64_t MAX_TENANT_BUCKET = 1024;
  bool is_inited_;
  obrpc::ObBackupDatabaseArg arg_;
  schema::ObMultiVersionSchemaService* schema_service_;
  common::ObMySQLProxy* proxy_;
  bool is_cluster_backup_;
  hash::ObHashMap<uint64_t, int64_t> schema_version_map_;
  hash::ObHashMap<uint64_t, int64_t> frozen_schema_version_map_;
  int64_t backup_snapshot_version_;
  int64_t backup_data_version_;
  int64_t frozen_timestamp_;
  int64_t max_backup_set_id_;
  rootserver::ObRootBackup* root_backup_;
  rootserver::ObFreezeInfoManager* freeze_info_manager_;
  rootserver::ObRestorePointService* restore_point_service_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupScheduler);
};

}  // namespace share
}  // namespace oceanbase

#endif /* OCEANBASE_SHARE_BACKUP_OB_BACKUP_SCHEDULER_H_ */
