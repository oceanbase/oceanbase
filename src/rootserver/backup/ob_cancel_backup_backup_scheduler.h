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

#ifndef OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_CANCEL_BACKUP_BACKUP_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_CANCEL_BACKUP_BACKUP_SCHEDULER_H_

#include "share/backup/ob_backup_struct.h"
#include "rootserver/backup/ob_backup_backupset.h"
#include "rootserver/backup/ob_backup_archive_log_scheduler.h"

namespace oceanbase {
namespace rootserver {

enum ObCancelBackupBackupType {
  CANCEL_BACKUP_BACKUPSET,
  CANCEL_BACKUP_BACKUPPIECE,
  CANCEL_MAX,
};

class ObCancelBackupBackupScheduler {
public:
  ObCancelBackupBackupScheduler();
  virtual ~ObCancelBackupBackupScheduler();
  int init(const uint64_t tenant_id, common::ObMySQLProxy& proxy, ObCancelBackupBackupType cancel_type,
      rootserver::ObBackupBackupset* backup_backupset, rootserver::ObBackupArchiveLogScheduler* backup_backuppiece);
  int start_schedule_cancel_backup_backup();

private:
  int schedule_cancel_backup_backupset();
  int schedule_cancel_backup_backuppiece();

private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy* sql_proxy_;
  ObCancelBackupBackupType cancel_type_;
  rootserver::ObBackupBackupset* backup_backupset_;
  rootserver::ObBackupArchiveLogScheduler* backup_backuppiece_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCancelBackupBackupScheduler);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif