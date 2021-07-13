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

#ifndef OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_AUTO_DELETE_EXPIRED_BACKUP_H_
#define OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_AUTO_DELETE_EXPIRED_BACKUP_H_

#include "share/ob_define.h"
#include "rootserver/ob_thread_idling.h"
#include "rootserver/ob_root_utils.h"
#include "lib/thread/ob_async_task_queue.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_manager.h"
#include "share/backup/ob_extern_backup_info_mgr.h"
#include "share/backup/ob_backup_lease_info_mgr.h"
#include "rootserver/ob_i_backup_scheduler.h"
#include "rootserver/ob_backup_data_clean.h"

namespace oceanbase {
namespace common {
class ModulePageArena;
class ObServerConfig;
}  // namespace common
namespace share {
class ObPartitionInfo;
class ObIBackupLeaseService;
namespace schema {
class ObTableSchema;
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share
namespace rootserver {

class OBackupAutoDeleteExpiredIdling : public ObThreadIdling {
public:
  explicit OBackupAutoDeleteExpiredIdling(volatile bool& stop) : ObThreadIdling(stop)
  {}
  virtual int64_t get_idle_interval_us();
};

class ObBackupAutoDeleteExpiredData : public ObRsReentrantThread, public ObIBackupScheduler {
public:
  ObBackupAutoDeleteExpiredData();
  virtual ~ObBackupAutoDeleteExpiredData();
  int init(common::ObServerConfig& cfg, ObMySQLProxy& sql_proxy,
      share::schema::ObMultiVersionSchemaService& schema_service, ObBackupDataClean& backup_data_clean,
      share::ObIBackupLeaseService& backup_lease_service);
  virtual void run3() override;
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }
  void stop();
  void wakeup();
  int idle() const;
  virtual bool is_working() const
  {
    return is_working_;
  }
  int start();
  ;

private:
  int check_can_auto_delete(
      const bool auto_delete_expired_backup, const int64_t backup_recovery_window, bool& can_delete);
  int get_last_succeed_delete_expired_snapshot(int64_t& last_succ_delete_expired_snapshot);
  int schedule_auto_delete_expired_data(const int64_t backup_recovery_window);

private:
  bool is_inited_;
  common::ObServerConfig* config_;
  common::ObMySQLProxy* sql_proxy_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  mutable OBackupAutoDeleteExpiredIdling idling_;
  ObBackupDataClean* backup_data_clean_;
  bool is_working_;
  share::ObIBackupLeaseService* backup_lease_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupAutoDeleteExpiredData);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_AUTO_DELETE_EXPIRED_BACKUP_H_
