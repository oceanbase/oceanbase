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

#ifndef OCEANBASE_SHARE_BACKUP_OB_VALIDATE_TASK_UPDATER_H_
#define OCEANBASE_SHARE_BACKUP_OB_VALIDATE_TASK_UPDATER_H_

#include <stdint.h>
#include "lib/container/ob_iarray.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase {
namespace common {
class ObISQLClient;
}
namespace share {

class ObBackupValidateTaskUpdater {
public:
  ObBackupValidateTaskUpdater();
  virtual ~ObBackupValidateTaskUpdater();

  int init(common::ObISQLClient& sql_client);
  int insert_task(const ObBackupValidateTaskInfo& validate_task);
  int get_task(const int64_t job_id, ObBackupValidateTaskInfo& validate_task);
  int get_task(const int64_t job_id, const uint64_t tenant_id, const int64_t backup_set_id,
      ObBackupValidateTaskInfo& validate_task);
  int get_tasks(
      const int64_t job_id, const uint64_t tenant_id, common::ObIArray<ObBackupValidateTaskInfo>& validate_tasks);
  int get_all_tasks(common::ObIArray<ObBackupValidateTaskInfo>& validate_tasks);
  int get_not_finished_tasks(common::ObIArray<ObBackupValidateTaskInfo>& validate_tasks);
  int update_task(const ObBackupValidateTaskInfo& src_task_info, const ObBackupValidateTaskInfo& dst_task_info);
  int remove_task(const int64_t job_id);

private:
  bool is_inited_;
  common::ObISQLClient* sql_proxy_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateTaskUpdater);
};

class ObBackupValidateHistoryUpdater {
public:
  ObBackupValidateHistoryUpdater();
  virtual ~ObBackupValidateHistoryUpdater();

  int init(common::ObISQLClient& sql_client);
  int insert_task(const ObBackupValidateTaskInfo& validate_task);
  int get_task(const int64_t job_id, const uint64_t tenant_id, const int64_t backup_set_id,
      ObBackupValidateTaskInfo& validate_task);
  int get_tasks(
      const int64_t job_id, const uint64_t tenant_id, common::ObIArray<ObBackupValidateTaskInfo>& validate_tasks);
  int remove_task(const int64_t job_id);

private:
  bool is_inited_;
  common::ObISQLClient* sql_proxy_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateHistoryUpdater);
};

}  // end namespace share
}  // end namespace oceanbase

#endif
