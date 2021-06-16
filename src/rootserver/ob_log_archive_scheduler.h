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

#ifndef SRC_ROOTSERVER_OB_LOG_ARCHIVE_SCHEDULER_H_
#define SRC_ROOTSERVER_OB_LOG_ARCHIVE_SCHEDULER_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_tenant_name_mgr.h"
#include "ob_thread_idling.h"
#include "ob_rs_reentrant_thread.h"
#include "ob_i_backup_scheduler.h"

namespace oceanbase {

namespace obrpc {
class ObSrvRpcProxy;
}

namespace common {
class ObMySQLProxy;
class ObMySQLTransaction;
}  // namespace common
namespace share {
class ObIBackupLeaseService;
}

namespace rootserver {

class ObServerManager;
class ObLeaderCoordinator;

class ObLogArchiveThreadIdling final : public ObThreadIdling {
public:
  explicit ObLogArchiveThreadIdling(volatile bool& stop);
  const int64_t DEFAULT_IDLE_INTERVAL_US = 10 * 1000 * 1000;  // 10s
  const int64_t MAX_IDLE_INTERVAL_US = 10 * 1000 * 1000;      // 60s
  virtual int64_t get_idle_interval_us();
  void set_log_archive_checkpoint_interval(const int64_t interval_us);

private:
  int64_t idle_time_us_;
  DISALLOW_COPY_AND_ASSIGN(ObLogArchiveThreadIdling);
};

class ObLogArchiveScheduler final : public ObRsReentrantThread, public ObIBackupScheduler {
public:
  const int64_t OB_START_LOG_ARCHIVE_ROUND = 0;

  ObLogArchiveScheduler();
  ~ObLogArchiveScheduler();

  int init(ObServerManager& server_mgr, share::schema::ObMultiVersionSchemaService* schema_service,
      obrpc::ObSrvRpcProxy& rpc_proxy, common::ObMySQLProxy& sql_proxy,
      share::ObIBackupLeaseService& backup_lease_info);
  void set_active();
  void stop() override;
  void wakeup();

  int handle_enable_log_archive(const bool is_enable);
  virtual void run3() override;
  virtual int blocking_run() override
  {
    BLOCKING_RUN_IMPLEMENT();
  }
  int start() override;
  virtual bool is_working() const override
  {
    return is_working_;
  }

private:
  void cleanup_();
  int handle_stop_log_archive_();
  int handle_start_log_archive_();
  int wait_backup_dest_(char* buf, const int64_t buf_len);

  int check_sys_backup_info_();
  int check_backup_info_();

  int do_schedule_();

  int start_log_archive_backup_(const share::ObLogArchiveBackupInfo& info);
  int start_tenant_log_archive_bakcup_(
      const share::ObLogArchiveBackupInfo& sys_info, const share::ObTenantLogArchiveStatus& tenant_status);
  int update_log_archive_backup_process_(const share::ObLogArchiveBackupInfo& sys_info);
  int update_tenant_log_archive_backup_process_(
      const share::ObLogArchiveBackupInfo& sys_info, const share::ObTenantLogArchiveStatus& tenant_status);
  int stop_log_archive_backup_(const share::ObLogArchiveBackupInfo& sys_info);
  int stop_tenant_log_archive_backup_(
      const uint64_t tenant_id, const share::ObLogArchiveBackupInfo& sys_info, const bool force_stop);
  int force_stop_log_archive_backup_(const share::ObLogArchiveBackupInfo& sys_info);
  int set_log_archive_backup_interrupted_(const share::ObLogArchiveBackupInfo& info);
  int set_tenants_log_archive_backup_interrupted_(share::ObLogArchiveBackupInfo& info);
  int set_tenant_log_archive_backup_interrupted_(
      const uint64_t tenant_id, const share::ObLogArchiveBackupInfo& sys_info);

  // set enable log archive sys parameter
  int set_enable_log_archive_(const bool is_enable);

  int prepare_tenant_beginning_status_(
      const share::ObLogArchiveBackupInfo& sys_info, common::ObArray<uint64_t>& tenant_ids);
  int prepare_tenant_beginning_status_(const share::ObLogArchiveBackupInfo& new_info);

  int fetch_log_archive_backup_status_map_(const share::ObLogArchiveBackupInfo& sys_info,
      common::ObArenaAllocator& allocator,
      common::hash::ObHashMap<uint64_t, share::ObTenantLogArchiveStatus*>& log_archive_status_map);
  int start_tenant_log_archive_backup_info_(share::ObLogArchiveBackupInfo& info);
  int commit_trans_(common::ObMySQLTransaction& trans);
  int update_sys_backup_info_(const share::ObLogArchiveBackupInfo& cur_info, share::ObLogArchiveBackupInfo& new_info);
  int check_can_do_work_();
  int check_mount_file_(share::ObLogArchiveBackupInfo& sys_info);

private:
  bool is_inited_;
  mutable ObLogArchiveThreadIdling idling_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObServerManager* server_mgr_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  common::ObMySQLProxy* sql_proxy_;
  bool is_working_;
  common::ObArray<uint64_t> tenant_ids_;
  int64_t last_update_tenant_ts_;
  share::ObTenantNameMgr tenant_name_mgr_;  // only used in run3 single thread, not multi-thread safe
  share::ObIBackupLeaseService* backup_lease_service_;
  DISALLOW_COPY_AND_ASSIGN(ObLogArchiveScheduler);
};

}  // namespace rootserver
}  // namespace oceanbase
#endif /* SRC_ROOTSERVER_OB_LOG_ARCHIVE_SCHEDULER_H_ */
