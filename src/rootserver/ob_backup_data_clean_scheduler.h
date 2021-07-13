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

#ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_DATA_CLEAN_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_OB_BACKUP_DATA_CLEAN_SCHEDULER_H_

#include "share/ob_rpc_struct.h"
#include "share/backup/ob_backup_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/backup/ob_backup_manager.h"
#include "rootserver/ob_freeze_info_manager.h"
#include "rootserver/ob_backup_data_clean.h"

namespace oceanbase {
namespace rootserver {

class ObBackupDataCleanScheduler {
public:
  ObBackupDataCleanScheduler();
  virtual ~ObBackupDataCleanScheduler();
  int init(const obrpc::ObBackupManageArg& arg, share::schema::ObMultiVersionSchemaService& schema_service,
      common::ObMySQLProxy& sql_proxy, ObBackupDataClean* data_clean);
  int start_schedule_backup_data_clean();

private:
  int prepare_backup_clean_infos(const common::ObIArray<uint64_t>& tenant_ids);
  int get_backup_clean_info(
      const uint64_t tenant_id, common::ObISQLClient& sql_proxy, share::ObBackupCleanInfo& clean_info);
  int insert_backup_clean_info(
      const uint64_t tenant_id, const share::ObBackupCleanInfo& clean_info, common::ObISQLClient& sql_proxy);
  int update_backup_clean_info(const share::ObBackupCleanInfo& src_clean_info,
      const share::ObBackupCleanInfo& dest_clean_info, common::ObISQLClient& sql_proxy);
  int get_tenant_ids(common::ObIArray<uint64_t>& tenant_ids);
  int schedule_backup_data_clean(const common::ObIArray<uint64_t>& tenant_ids);
  int schedule_sys_tenant_backup_data_clean();
  int schedule_tenants_backup_data_clean(const common::ObIArray<uint64_t>& tenant_ids);
  int schedule_tenant_backup_data_clean(const uint64_t tenant_id, common::ObISQLClient& sys_tenant_trans);
  int set_backup_clean_info(const uint64_t tenant_id, share::ObBackupCleanInfo& clean_info);
  int start_backup_clean();
  int rollback_backup_clean_infos(const common::ObIArray<uint64_t>& tenant_ids);
  int rollback_backup_clean_info(const uint64_t tenant_id);
  // delete backup set need to know incarnation
  int get_backup_incarnation(const uint64_t tenant_id, const int64_t backup_set_id);

private:
  bool is_inited_;
  obrpc::ObBackupManageArg arg_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  common::ObMySQLProxy* sql_proxy_;
  ObBackupDataClean* data_clean_;
  bool is_cluster_clean_;
  int64_t max_job_id_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataCleanScheduler);
};

}  // namespace rootserver
}  // namespace oceanbase

#endif /* OCEANBASE_SHARE_BACKUP_OB_BACKUP_SCHEDULER_H_ */
