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

#ifndef OCEANBASE_SHARE_BACKUP_OB_PG_BACKUP_TASK_UPDATER_H_
#define OCEANBASE_SHARE_BACKUP_OB_PG_BACKUP_TASK_UPDATER_H_

#include "share/ob_define.h"
#include "ob_backup_struct.h"
#include "ob_backup_operator.h"

namespace oceanbase {
namespace share {

class ObPGBackupTaskUpdater {
public:
  ObPGBackupTaskUpdater();
  virtual ~ObPGBackupTaskUpdater() = default;
  int init(common::ObISQLClient& sql_proxy);
  int update_pg_backup_task_status(
      const common::ObIArray<common::ObPartitionKey>& pkeys, const ObPGBackupTaskInfo::BackupStatus& status);
  int update_pg_task_info(const common::ObIArray<common::ObPartitionKey>& pkeys, const common::ObAddr& addr,
      const share::ObTaskId& task_id, const common::ObReplicaType& type,
      const ObPGBackupTaskInfo::BackupStatus& status);
  int get_pg_backup_tasks(const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id,
      common::ObIArray<ObPGBackupTaskInfo>& task_infos);
  int update_status_and_result(const common::ObIArray<common::ObPartitionKey>& pkeys,
      const common::ObIArray<int32_t>& results, const ObPGBackupTaskInfo::BackupStatus& status);
  int delete_all_pg_tasks(const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id,
      const int64_t max_delete_rows, int64_t& affected_rows);
  int get_total_pg_task_count(const uint64_t tenant_id, int64_t& total_task_cnt);
  int get_latest_backup_task(const uint64_t tenant_id, ObPGBackupTaskInfo& pg_task_info);
  int batch_report_pg_task(const common::ObIArray<ObPGBackupTaskInfo>& pg_task_infos);
  int get_finished_backup_task(const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id,
      common::ObIArray<ObPGBackupTaskInfo>& pg_task_infos);
  int get_one_doing_pg_task(const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id,
      common::ObIArray<ObPGBackupTaskInfo>& pg_task_infos);
  int get_pending_pg_task(const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id,
      common::ObIArray<ObPGBackupTaskInfo>& pg_task_infos);
  int get_pg_backup_task(const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id,
      const common::ObPartitionKey& pkey, ObPGBackupTaskInfo& pg_task_info);
  int get_pg_backup_tasks(const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id,
      const int64_t backup_task_id, common::ObIArray<ObPGBackupTaskInfo>& task_infos);
  int update_status_and_result_without_trans(const common::ObIArray<common::ObPartitionKey>& pkeys,
      const common::ObIArray<int32_t>& results, const ObPGBackupTaskInfo::BackupStatus& status);
  int get_one_pg_task(const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id,
      ObPGBackupTaskInfo& pg_task_info);
  int update_status_and_result_and_statics(const common::ObIArray<ObPGBackupTaskInfo>& pg_task_info_array);

private:
  static const int64_t MAX_BATCH_COUNT = 1024;
  bool is_inited_;
  common::ObISQLClient* sql_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObPGBackupTaskUpdater);
};

}  // namespace share
}  // namespace oceanbase

#endif /* OCEANBASE_SHARE_BACKUP_OB_PG_BACKUP_TASK_UPDATER_H_ */
