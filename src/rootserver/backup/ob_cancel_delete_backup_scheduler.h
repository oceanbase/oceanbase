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

#ifndef OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_CANCEL_DELETE_BACKUP_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_CANCEL_DELETE_BACKUP_SCHEDULER_H_

#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_manager.h"
#include "rootserver/ob_backup_data_clean.h"
#include "rootserver/ob_root_backup.h"

namespace oceanbase {
namespace rootserver {

class ObCancelDeleteBackupScheduler {
public:
  ObCancelDeleteBackupScheduler();
  virtual ~ObCancelDeleteBackupScheduler();
  int init(const uint64_t tenant_id, common::ObMySQLProxy& proxy, rootserver::ObBackupDataClean* backup_data_clean);
  int start_schedule_cacel_delete_backup();

private:
  int inner_schedule_cancel_delete_backup();

private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy* proxy_;
  rootserver::ObBackupDataClean* backup_data_clean_;
  DISALLOW_COPY_AND_ASSIGN(ObCancelDeleteBackupScheduler);
};

}  // namespace rootserver
}  // namespace oceanbase

#endif /* OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_CANCEL_SCHEDULER_H_ */
