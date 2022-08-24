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
class ObZoneManager;
class ObLeaderCoordinator;

class ObLogArchiveThreadIdling final : public ObThreadIdling {
public:
  explicit ObLogArchiveThreadIdling(volatile bool& stop);
  const int64_t RESERVED_FETCH_US = 10 * 1000 * 1000;      // 10s, used for fetch observer log archive status
  const int64_t MIN_IDLE_INTERVAL_US = 2 * 1000 * 1000;    // 2s
  const int64_t FAST_IDLE_INTERVAL_US = 10 * 1000 * 1000;  // 10s, used during BEGINNING or STOPPING
  const int64_t MAX_IDLE_INTERVAL_US = 60 * 1000 * 1000;   // 60s
  virtual int64_t get_idle_interval_us();
  void set_log_archive_checkpoint_interval(const int64_t interval_us, const share::ObLogArchiveStatus::STATUS& status);

private:
  int64_t idle_time_us_;
  DISALLOW_COPY_AND_ASSIGN(ObLogArchiveThreadIdling);
};

class ObLogArchiveScheduler final : public ObIBackupScheduler {
public:
  const int64_t OB_START_LOG_ARCHIVE_ROUND = 0;
  typedef hash::ObHashMap<uint64_t, share::ObTenantLogArchiveStatus*> COMPAT_TENANT_ARCHIVE_STATUS_MAP;
  typedef hash::ObHashMap<uint64_t, share::ObServerTenantLogArchiveStatus*> TENANT_ARCHIVE_STATUS_MAP;

  ObLogArchiveScheduler();
  ~ObLogArchiveScheduler();

  int init(ObServerManager& server_mgr, ObZoneManager& zone_mgr,
      share::schema::ObMultiVersionSchemaService* schema_service, obrpc::ObSrvRpcProxy& rpc_proxy,
      common::ObMySQLProxy& sql_proxy, share::ObIBackupLeaseService& backup_lease_info);
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
  virtual int force_cancel(const uint64_t tenant_id);

private:
  void cleanup_();
  int handle_stop_log_archive_(const share::ObBackupInnerTableVersion& inner_table_version);
  int handle_start_log_archive_(const share::ObBackupInnerTableVersion& inner_table_version);
  int check_gts_();
  int init_new_sys_info_(const char* backup_dest, const int64_t max_piece_id, share::ObLogArchiveBackupInfo& sys_info);
  int init_new_sys_piece_(const share::ObLogArchiveBackupInfo& sys_info, share::ObBackupPieceInfo& sys_piece_info);
  int init_new_sys_piece_for_compat_(
      const share::ObLogArchiveBackupInfo& sys_info, share::ObBackupPieceInfo& sys_piece_info);
  int wait_backup_dest_(char* buf, const int64_t buf_len);
  int check_backup_info_();
  int do_schedule_(share::ObLogArchiveStatus::STATUS& last_log_archive_status);

  // for ObLogArchiveStatus::BEGINNING status
  int start_log_archive_backup_(
      share::ObLogArchiveBackupInfo& cur_sys_info, share::ObNonFrozenBackupPieceInfo& sys_non_frozen_piece);
  int set_backup_piece_start_ts_(const share::ObBackupPieceInfoKey& piece_key, const int64_t start_ts);
  int start_tenant_log_archive_bakcup_(
      const share::ObLogArchiveBackupInfo& sys_info, const share::ObTenantLogArchiveStatus& tenant_status);

  // for ObLogArchiveStatus::DOING status
  int update_log_archive_backup_process_(
      share::ObLogArchiveBackupInfo& cur_sys_info, share::ObNonFrozenBackupPieceInfo& sys_non_frozen_piece);
  int try_update_checkpoit_ts_(share::ObLogArchiveBackupInfo& cur_sys_info,
      share::ObNonFrozenBackupPieceInfo& sys_non_frozen_piece, common::ObIAllocator& allocator,
      TENANT_ARCHIVE_STATUS_MAP& log_archive_status_map, int64_t& min_piece_id, int64_t& inactive_server_count);
  int try_update_backup_piece_(const share::ObLogArchiveBackupInfo& sys_info,
      const TENANT_ARCHIVE_STATUS_MAP& log_archive_status_map, const int64_t inactive_server_count,
      share::ObNonFrozenBackupPieceInfo& sys_non_frozen_piece, int64_t& min_piece_id);
  int init_next_sys_piece_(const share::ObLogArchiveBackupInfo& sys_info, const share::ObBackupPieceInfo& sys_piece,
      share::ObLogArchiveBackupInfo& new_sys_info, share::ObNonFrozenBackupPieceInfo& new_sys_pieces);
  int init_next_user_piece_array_(const share::ObNonFrozenBackupPieceInfo& new_sys_piece,
      const TENANT_ARCHIVE_STATUS_MAP& log_archive_status_map,
      common::ObArray<share::ObNonFrozenBackupPieceInfo>& new_user_piece_array);
  int init_next_user_piece_(const share::ObNonFrozenBackupPieceInfo& new_sys_piece, const uint64_t tenant_id,
      const int64_t checkpoint_ts, share::ObNonFrozenBackupPieceInfo& user_piece);
  int update_external_user_piece_(const common::ObIArray<share::ObNonFrozenBackupPieceInfo>& new_user_piece_array);
  int update_inner_freeze_piece_info_(const share::ObLogArchiveBackupInfo& cur_sys_info,
      const share::ObLogArchiveBackupInfo& new_sys_info, const share::ObNonFrozenBackupPieceInfo& new_sys_piece,
      const ObArray<share::ObNonFrozenBackupPieceInfo>& new_user_piece_array);
  int trigger_freeze_pieces_(const share::ObLogArchiveBackupInfo& sys_info,
      const share::ObBackupPieceInfo& sys_piece_info, const TENANT_ARCHIVE_STATUS_MAP& log_archive_status_map);
  int frozen_old_piece_(const bool force_stop, const common::ObIArray<uint64_t>& tenant_ids,
      const TENANT_ARCHIVE_STATUS_MAP& log_archive_status_map, share::ObNonFrozenBackupPieceInfo& sys_non_frozen_piece);
  int frozen_old_piece_(const bool force_stop, const share::ObNonFrozenBackupPieceInfo& sys_non_frozen_piece,
      const uint64_t tenant_id, const int64_t server_tenant_max_ts, int64_t& max_ts);
  int frozen_sys_old_piece_(
      const bool force_stop, const int64_t max_ts, share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece);
  int update_sys_log_archive_backup_process_(const share::ObLogArchiveBackupInfo &cur_info,
      share::ObLogArchiveBackupInfo &new_info, const share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece);
  int update_tenant_log_archive_backup_process_(const share::ObLogArchiveBackupInfo &sys_info,
      const share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece,
      const share::ObServerTenantLogArchiveStatus &tenant_status);
  int update_active_piece_checkpoint_ts_(const share::ObBackupPieceInfoKey &piece_key, const int64_t checkpoint_ts);
  int update_sys_active_piece_checkpoint_ts_(const int64_t checkpoint_ts, share::ObBackupPieceInfo &sys_piece_info);

  // for ObLogArchiveStatus::STOPPING status
  int stop_log_archive_backup_(const bool force_stop, share::ObLogArchiveBackupInfo& cur_sys_info,
      share::ObNonFrozenBackupPieceInfo& sys_non_frozen_piece);
  int get_tenant_ids_from_schema_(common::ObIArray<uint64_t>& tenant_ids);
  int check_server_stop_backup_(
      share::ObLogArchiveBackupInfo& sys_info, common::ObIArray<uint64_t>& tenant_ids, bool& is_all_stop);
  int frozen_active_piece_before_stop_(const bool force_stop, share::ObLogArchiveBackupInfo& cur_sys_info,
      share::ObNonFrozenBackupPieceInfo& sys_non_frozen_piece);
  int frozen_active_piece_before_stop_(const bool force_stop,
      const share::ObNonFrozenBackupPieceInfo& sys_non_frozen_piece, const uint64_t tenant_id, int64_t& max_gts);
  int frozen_sys_active_piece_before_stop_(const bool force_stop, const int64_t max_gts,
      share::ObLogArchiveBackupInfo& cur_sys_info, share::ObNonFrozenBackupPieceInfo& sys_non_frozen_piece);
  int do_stop_log_archive_backup_(const bool force_stop, const common::ObIArray<uint64_t>& tenant_ids,
      const share::ObLogArchiveBackupInfo& cur_sys_info);
  int do_stop_tenant_log_archive_backup_(
      const uint64_t tenant_id, const share::ObLogArchiveBackupInfo& sys_info, const bool force_stop);
  int do_stop_log_archive_backup_v2_(const bool force_stop, const common::ObIArray<uint64_t>& tenant_ids,
      const share::ObLogArchiveBackupInfo& cur_sys_info, const share::ObNonFrozenBackupPieceInfo& sys_non_frozen_piece);
  int do_stop_tenant_log_archive_backup_v2_(
      const bool force_stop, const bool is_dropped_tenant, const share::ObBackupPieceInfoKey& cur_key, int64_t& max_ts);
  int do_stop_sys_log_archive_backup_v2_(const bool force_stop, const share::ObLogArchiveBackupInfo& cur_sys_info,
      const share::ObBackupPieceInfoKey& cur_key, const int64_t max_ts);
  int check_dropped_tenant_(
      const common::ObIArray<uint64_t>& tenant_ids, const share::ObNonFrozenBackupPieceInfo& sys_non_frozen_piece);
  int set_log_archive_backup_interrupted_(const share::ObLogArchiveBackupInfo& info);
  int set_tenants_log_archive_backup_interrupted_(share::ObLogArchiveBackupInfo& info);
  int set_tenant_log_archive_backup_interrupted_(
      const uint64_t tenant_id, const share::ObLogArchiveBackupInfo& sys_info);

  // set enable log archive sys parameter
  int set_enable_log_archive_(const bool is_enable);
  int prepare_new_tenant_info_(const share::ObLogArchiveBackupInfo& sys_info,
      const share::ObNonFrozenBackupPieceInfo& non_frozen_piece, const common::ObArray<uint64_t>& tenant_ids);
  int check_need_prepare_new_tenant_status(const uint64_t tenant_id, bool& need_prepare);
  int prepare_new_tenant_info_(const share::ObLogArchiveBackupInfo& sys_info,
      const share::ObNonFrozenBackupPieceInfo& non_frozen_sys_piece, const uint64_t tenant_id);
  int update_tenant_non_frozen_backup_piece_(const share::ObNonFrozenBackupPieceInfo& non_frozen_sys_piece);
  int prepare_new_tenant_info_(const share::ObLogArchiveBackupInfo& sys_info,
      const share::ObNonFrozenBackupPieceInfo& sys_non_frozen_piece, const uint64_t tenant_id,
      share::ObLogArchiveBackupInfo& tenant_info, share::ObNonFrozenBackupPieceInfo& tenant_non_frozen_piece);

  int fetch_log_archive_backup_status_map_(const share::ObLogArchiveBackupInfo& sys_info,
      common::ObIAllocator& allocator, TENANT_ARCHIVE_STATUS_MAP& log_archive_status_map,
      int64_t& inactive_server_count);
  int fetch_server_tenant_log_archive_status_(const common::ObAddr& addr,
      const share::ObGetTenantLogArchiveStatusArg& arg, share::ObServerTenantLogArchiveStatusWrapper& result);
  int fetch_server_tenant_log_archive_status_v1_(const common::ObAddr& addr,
      const share::ObGetTenantLogArchiveStatusArg& arg, share::ObServerTenantLogArchiveStatusWrapper& result);
  int fetch_server_tenant_log_archive_status_v2_(const common::ObAddr& addr,
      const share::ObGetTenantLogArchiveStatusArg& arg, share::ObServerTenantLogArchiveStatusWrapper& result);
  int check_server_status_(common::ObArray<common::ObAddr>& inactive_server_list);
  int start_tenant_log_archive_backup_info_(share::ObLogArchiveBackupInfo& info);
  int commit_trans_(common::ObMySQLTransaction& trans);
  int update_sys_backup_info_(const share::ObLogArchiveBackupInfo& cur_info, share::ObLogArchiveBackupInfo& new_info);
  int check_can_do_work_();
  int check_mount_file_(share::ObLogArchiveBackupInfo& sys_info);

  int check_backup_inner_table_();
  int upgrade_backup_inner_table_();
  int upgrade_log_archive_status_();
  int upgrade_log_archive_status_(const uint64_t tenant_id, const share::ObBackupPieceInfo& sys_piece);
  int upgrade_frozen_piece_files_();
  int upgrade_backupset_files_();
  int upgrade_backup_info_();
  int clean_discard_log_archive_status_();

  bool is_force_cancel_() const;

private:
  bool is_inited_;
  mutable ObLogArchiveThreadIdling idling_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObServerManager* server_mgr_;
  ObZoneManager* zone_mgr_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  common::ObMySQLProxy* sql_proxy_;
  bool is_working_;
  common::ObArray<uint64_t> tenant_ids_;
  int64_t last_update_tenant_ts_;
  share::ObTenantNameMgr tenant_name_mgr_;  // only used in run3 single thread, not multi-thread safe
  share::ObIBackupLeaseService* backup_lease_service_;
  share::ObBackupInnerTableVersion inner_table_version_;  // only used in run3 single thread, not multi-thread safe
  DISALLOW_COPY_AND_ASSIGN(ObLogArchiveScheduler);
};

}  // namespace rootserver
}  // namespace oceanbase
#endif /* SRC_ROOTSERVER_OB_LOG_ARCHIVE_SCHEDULER_H_ */
