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

#ifndef OCEANBASE_SHARE_BACKUP_OB_PG_VALIDATE_TASK_UPDATER_H_
#define OCEANBASE_SHARE_BACKUP_OB_PG_VALIDATE_TASK_UPDATER_H_

#include <stdint.h>
#include "lib/container/ob_iarray.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase {
namespace common {
class ObAddr;
class ObISQLClient;
}  // namespace common
namespace share {

class ObPGValidateTaskUpdater {
public:
  ObPGValidateTaskUpdater();
  virtual ~ObPGValidateTaskUpdater();
  int init(common::ObISQLClient& sql_client, bool dropped_tenant = false);
  // not thread safe
  void set_dropped_tenant(const bool is_dropped_tenant)
  {
    is_dropped_tenant_ = is_dropped_tenant;
  }
  int update_pg_task_info(const common::ObIArray<common::ObPartitionKey>& pkeys, const common::ObAddr& addr,
      const share::ObTaskId& task_id, const ObPGValidateTaskInfo::ValidateStatus& status);
  int update_pg_validate_task_status(
      const common::ObIArray<common::ObPartitionKey>& pkeys, const ObPGValidateTaskInfo::ValidateStatus& status);
  int update_status_and_result(const int64_t backup_set_id, const common::ObIArray<common::ObPartitionKey>& pkeys,
      const common::ObIArray<int32_t>& results, const ObPGValidateTaskInfo::ValidateStatus& status);

  int get_pg_validate_task(const int64_t job_id, const uint64_t task_tenant_id, const int64_t incarnation,
      const int64_t backup_set_id, const common::ObPartitionKey& pkey, ObPGValidateTaskInfo& pg_task_info);
  int get_pg_validate_tasks(const int64_t job_id, const uint64_t task_tenant_id, const int64_t incarnation,
      const int64_t backup_set_id, common::ObIArray<ObPGValidateTaskInfo>& pg_task_infos);

  int get_doing_pg_tasks(const int64_t job_id, const uint64_t task_tenant_id, const int64_t incarnation,
      const int64_t backup_set_id, common::ObIArray<ObPGValidateTaskInfo>& pg_task_infos);
  int get_pending_pg_tasks(const int64_t job_id, const uint64_t task_tenant_id, const int64_t incarnation,
      const int64_t backup_set_id, common::ObIArray<ObPGValidateTaskInfo>& pg_task_infos);
  int get_finished_pg_tasks(const int64_t job_id, const uint64_t task_tenant_id, const int64_t incarnation,
      const int64_t backup_set_id, common::ObIArray<ObPGValidateTaskInfo>& pg_task_infos);

  int get_pending_pg_tasks(const int64_t job_id, const uint64_t task_tenant_id, const int64_t incarnation,
      common::ObIArray<ObPGValidateTaskInfo>& pg_task_infos);
  int get_finished_pg_tasks(const int64_t job_id, const uint64_t task_tenant_id, const int64_t incarnation,
      common::ObIArray<ObPGValidateTaskInfo>& pg_task_infos);

  int get_one_doing_pg_tasks(const uint64_t tenant_id, const ObPGValidateTaskInfo* pg_task,
      common::ObIArray<ObPGValidateTaskInfo>& pg_task_infos);

  int delete_all_pg_tasks(const int64_t job_id, const uint64_t task_tenant_id);

  int batch_report_pg_task(const common::ObIArray<ObPGValidateTaskInfo>& pg_task_infos);

private:
  static const int64_t MAX_BATCH_SIZE = 1024;
  bool is_inited_;
  bool is_dropped_tenant_;
  common::ObISQLClient* sql_client_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPGValidateTaskUpdater);
};

}  // end namespace share
}  // end namespace oceanbase

#endif
