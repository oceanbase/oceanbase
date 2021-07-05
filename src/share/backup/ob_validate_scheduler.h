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

#ifndef OCEANBASE_SHARE_BACKUP_OB_VALIDATE_SCHEDULER_H_
#define OCEANBASE_SHARE_BACKUP_OB_VALIDATE_SCHEDULER_H_

#include <stdint.h>
#include "lib/container/ob_iarray.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_manager.h"
#include "share/backup/ob_validate_task_updater.h"
#include "share/backup/ob_tenant_backup_task_updater.h"
#include "share/backup/ob_log_archive_backup_info_mgr.h"
#include "share/schema/ob_multi_version_schema_service.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace rootserver {
class ObRootValidate;
}
namespace share {
class ObValidateInfoManager;

class ObValidateScheduler {
public:
  ObValidateScheduler();
  virtual ~ObValidateScheduler();
  int init(const uint64_t tenant_id, const int64_t backup_set_id_, common::ObMySQLProxy& sql_proxy,
      rootserver::ObRootValidate& root_validate);
  int start_schedule_validate();

private:
  int get_log_archive_time_range(const uint64_t tenant_id, int64_t& start_ts, int64_t& checkpoint_ts);
  int check_backup_set_id_valid(const uint64_t tenant_id, const int64_t backup_set_id);
  int start_validate();

private:
  bool is_inited_;
  share::ObBackupValidateTaskInfo task_info_;
  common::ObMySQLProxy* sql_proxy_;
  rootserver::ObRootValidate* root_validate_;
  share::ObBackupValidateTaskUpdater task_updater_;
  share::ObBackupInfoManager backup_info_mgr_;
  share::ObLogArchiveBackupInfoMgr log_archive_mgr_;
  share::ObBackupTaskHistoryUpdater backup_history_updater_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObValidateScheduler);
};

}  // end namespace share
}  // end namespace oceanbase

#endif
