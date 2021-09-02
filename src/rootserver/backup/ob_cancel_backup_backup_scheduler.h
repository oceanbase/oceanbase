// Copyright (c) 2021 OceanBase Inc. All Rights Reserved.
//
// Author:
//    yangyi.yyy <yangyi.yyy@antgroup.com>

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