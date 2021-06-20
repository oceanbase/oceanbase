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

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_BACKUP_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_BACKUP_H_

#include "share/ob_define.h"
#include "ob_thread_idling.h"
#include "ob_partition_group_coordinator.h"
#include "ob_server_checker.h"
#include "ob_root_utils.h"
#include "ob_shrink_resource_pool_checker.h"
#include "lib/thread/ob_async_task_queue.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_manager.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/backup/ob_tenant_backup_task_updater.h"
#include "share/backup/ob_pg_backup_task_updater.h"
#include "share/backup/ob_extern_backup_info_mgr.h"
#include "ob_freeze_info_manager.h"
#include "share/backup/ob_backup_lease_info_mgr.h"
#include "ob_i_backup_scheduler.h"
#include "ob_restore_point_service.h"

namespace oceanbase {
namespace common {
class ModulePageArena;
class ObServerConfig;
}  // namespace common
namespace share {
class ObPartitionInfo;
class ObPartitionTableOperator;
class ObIBackupLeaseService;

namespace schema {
class ObTableSchema;
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share
namespace rootserver {
class ObServerManager;
class ObLeaderCoordinator;
class ObZoneManager;

struct ObBreakPointPGInfo {
  ObBreakPointPGInfo() : pkey_()
  {}
  virtual ~ObBreakPointPGInfo() = default;
  common::ObPartitionKey pkey_;
  bool has_breakpoint_pg_task()
  {
    return pkey_.is_valid();
  }
  bool is_tablegroup_key() const
  {
    return pkey_.is_valid() && pkey_.is_pg();
  }
  bool is_standalone_key() const
  {
    return pkey_.is_valid() && !pkey_.is_pg();
  }
  bool is_valid() const
  {
    return pkey_.is_valid();
  }
  void reset()
  {
    pkey_.reset();
  }
  TO_STRING_KV(K_(pkey));
};

struct ObTenantBackupMetaInfo {
  ObTenantBackupMetaInfo() : break_point_info_(), pg_count_(0), partition_count_(0)
  {}
  virtual ~ObTenantBackupMetaInfo() = default;
  void reset()
  {
    break_point_info_.reset();
    pg_count_ = 0;
    partition_count_ = 0;
  }
  TO_STRING_KV(K_(break_point_info), K_(pg_count), K_(partition_count));

  ObBreakPointPGInfo break_point_info_;
  int64_t pg_count_;
  int64_t partition_count_;
};

class ORootBackupIdling : public ObThreadIdling {
public:
  explicit ORootBackupIdling(volatile bool& stop) : ObThreadIdling(stop)
  {}
  virtual int64_t get_idle_interval_us();
};

class ObRootBackup : public ObRsReentrantThread, public ObIBackupScheduler {
public:
  ObRootBackup();
  virtual ~ObRootBackup();
  int init(common::ObServerConfig& cfg, share::schema::ObMultiVersionSchemaService& schema_service,
      ObMySQLProxy& sql_proxy, ObRootBalancer& root_balancer, ObFreezeInfoManager& freeze_info_mgr,
      ObServerManager& server_mgr_, ObRebalanceTaskMgr& rebalancer_mgr, ObZoneManager& zone_mgr,
      obrpc::ObSrvRpcProxy& rpc_proxy, share::ObIBackupLeaseService& backup_lease_service,
      ObRestorePointService& restore_point_service);
  virtual void run3() override;
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }
  void stop();
  void wakeup();
  int idle() const;
  int get_lease_time(const uint64_t tenant_id, int64_t& lease_time);
  int update_lease_time(const uint64_t tenant_id);
  void update_prepare_flag(const bool is_prepare_flag);
  bool get_prepare_flag() const;
  virtual bool is_working() const
  {
    return is_working_;
  }
  int start();
  int update_tenant_backup_meta_info(
      const common::ObPartitionKey& pkey, const int64_t pg_count, const int64_t partition_count);
  int get_tenant_backup_meta_info(ObTenantBackupMetaInfo& meta_info);
  void reset_tenant_backup_meta_info();
  int check_can_backup();

private:
  int get_need_backup_tenant_ids(common::ObIArray<uint64_t>& tenant_ids);
  int get_need_backup_info(const uint64_t tenant_id, share::ObBackupInfoManager& info_manager, bool& need_add);
  int get_all_tenant_ids(common::ObIArray<uint64_t>& tenant_ids);
  int do_with_status(share::ObBackupInfoManager& info_manager, const share::ObBaseBackupInfoStruct& info);
  int do_root_scheduler(const common::ObIArray<uint64_t>& tenant_ids);
  int do_tenant_scheduler(const uint64_t tenant_id, share::ObBackupInfoManager& info_manager);
  int do_scheduler(const share::ObBaseBackupInfoStruct& info, share::ObBackupInfoManager& info_manager);
  int do_backup(const share::ObBaseBackupInfoStruct& info, share::ObBackupInfoManager& info_manager);
  int do_sys_tenant_backup(const share::ObBaseBackupInfoStruct& info, share::ObBackupInfoManager& info_manager);
  int do_tenant_backup(const share::ObBaseBackupInfoStruct& info, share::ObBackupInfoManager& info_manager);

  int do_cleanup(const share::ObBaseBackupInfoStruct& info, share::ObBackupInfoManager& info_manager);
  int do_cancel(const share::ObBaseBackupInfoStruct& info, share::ObBackupInfoManager& info_manager);

  int get_tenant_backup_task(
      ObMySQLTransaction& trans, const share::ObBaseBackupInfoStruct& info, share::ObTenantBackupTaskInfo& task_info);
  int get_tenant_backup_task(const uint64_t tenant_id, const int64_t backup_set_id, const int64_t incarnation,
      common::ObISQLClient& trans, share::ObTenantBackupTaskInfo& task_info);
  int insert_tenant_backup_task(ObMySQLTransaction& trans, const share::ObBaseBackupInfoStruct& info,
      const share::ObExternBackupInfo& extern_backup_info);
  int update_tenant_backup_info(const share::ObBaseBackupInfoStruct& src_info,
      const share::ObBaseBackupInfoStruct& dest_info, share::ObBackupInfoManager& info_manager,
      share::ObBackupItemTransUpdater& updater);
  int get_tenant_total_partition_cnt(const uint64_t tenant_id, int64_t& total_partition_cnt);
  int do_cleanup_pg_backup_tasks(
      const share::ObTenantBackupTaskInfo& task_info, bool& all_task_deleted, common::ObISQLClient& trans);
  int do_cleanup_tenant_backup_task(const share::ObBaseBackupInfoStruct& info, common::ObISQLClient& trans);
  int do_insert_tenant_backup_task_his(const share::ObTenantBackupTaskInfo& task_info, common::ObISQLClient& trans);
  int update_extern_backup_infos(const share::ObBaseBackupInfoStruct& info,
      const share::ObExternBackupInfo::ExternBackupInfoStatus& status, const bool is_force_stop,
      share::ObExternBackupInfo& extern_backup_info);
  int do_extern_backup_set_infos(const share::ObBaseBackupInfoStruct& info,
      const share::ObTenantBackupTaskInfo& tenant_task_info, const share::ObExternBackupInfo& extern_backup_info,
      const bool is_force_stop, share::ObExternBackupSetInfo& extern_backup_set_info);
  int do_extern_backup_tenant_locality_infos(const share::ObBaseBackupInfoStruct& info,
      const share::ObExternBackupInfo& exter_backup_info, const bool is_force_stop,
      share::ObExternTenantLocalityInfo& extern_tenant_locality_info);
  int do_extern_tenant_infos(const share::ObBaseBackupInfoStruct& info, share::ObBackupInfoManager& info_manager);
  int get_stopped_backup_tenant_task_infos(const common::ObIArray<share::ObBaseBackupInfoStruct>& tenant_backup_infos,
      common::ObIArray<share::ObTenantBackupTaskInfo>& tenant_task_infos);
  int cleanup_stopped_backup_task_infos();
  int cleanup_stopped_tenant_infos(const share::ObBaseBackupInfoStruct& info);
  int cleanup_stopped_tenant_infos(const uint64_t tenant_id, share::ObBackupInfoManager& info_manager);

  int get_stopped_backup_tenant_result(
      const common::ObIArray<share::ObBaseBackupInfoStruct>& tenant_backup_infos, int32_t& result);
  int insert_lease_time(const uint64_t tenant_id);
  void cleanup_prepared_infos();
  int check_need_cleanup_prepared_infos(const share::ObBaseBackupInfoStruct& sys_backup_info, bool& need_clean);
  int cleanup_tenant_prepared_infos(
      const uint64_t tenant_id, common::ObISQLClient& sys_tenant_trans, share::ObBackupInfoManager& info_manager);
  int check_tenants_backup_task_failed(const share::ObBaseBackupInfoStruct& info,
      share::ObBackupInfoManager& info_manager, common::ObISQLClient& sys_tenant_trans);
  int update_tenant_backup_task(common::ObISQLClient& trans, const share::ObTenantBackupTaskInfo& src_info,
      const share::ObTenantBackupTaskInfo& dest_info);
  int do_normal_tenant_cancel(const share::ObBaseBackupInfoStruct& info, share::ObBackupInfoManager& info_manager);
  int do_sys_tenant_cancel(const share::ObBaseBackupInfoStruct& info, share::ObBackupInfoManager& info_manager);
  int set_normal_tenant_cancel(
      const uint64_t tenant_id, share::ObBackupInfoManager& sys_info_manager, common::ObISQLClient& sys_tenant_tran);
  int update_sys_tenant_backup_task(
      common::ObMySQLTransaction& sys_tenant_trans, const share::ObBaseBackupInfoStruct& info, const int32_t result);
  int do_extern_diagnose_info(const share::ObBaseBackupInfoStruct& info,
      const share::ObExternBackupInfo& extern_backup_info, const share::ObExternBackupSetInfo& extern_backup_set_info,
      const share::ObExternTenantLocalityInfo& tenant_locality_info, const bool is_force_stop);
  int check_tenant_is_dropped(const uint64_t tenant_id, bool& is_dropped);
  int do_with_all_finished_info(const share::ObBaseBackupInfoStruct& info, share::ObBackupItemTransUpdater& updater,
      share::ObBackupInfoManager& info_manager);
  int check_tenant_backup_inner_error(const share::ObBaseBackupInfoStruct& info);
  int insert_tenant_backup_task_failed(
      ObMySQLTransaction& trans, const share::ObBaseBackupInfoStruct& info, share::ObTenantBackupTaskInfo& task_info);
  int drop_backup_point(const uint64_t tenant_id, const int64_t backup_snapshot_version);
  int commit_trans(share::ObBackupItemTransUpdater& updater);
  int add_backup_info_lock(const share::ObBaseBackupInfoStruct& info, share::ObBackupItemTransUpdater& updater,
      share::ObBackupInfoManager& info_manager);
  int start_trans(ObTimeoutCtx& timeout_ctx, share::ObBackupItemTransUpdater& updater);

private:
  bool is_inited_;
  common::ObServerConfig* config_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  common::ObMySQLProxy* sql_proxy_;
  mutable ORootBackupIdling idling_;
  ObRootBalancer* root_balancer_;
  ObFreezeInfoManager* freeze_info_mgr_;
  ObServerManager* server_mgr_;
  ObRebalanceTaskMgr* rebalancer_mgr_;
  ObZoneManager* zone_mgr_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  hash::ObHashMap<uint64_t, int64_t> lease_time_map_;
  bool is_prepare_flag_;
  bool need_switch_tenant_;
  bool is_working_;
  int32_t inner_error_;
  int32_t extern_device_error_;
  ObTenantBackupMetaInfo backup_meta_info_;
  share::ObIBackupLeaseService* backup_lease_service_;
  ObRestorePointService* restore_point_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRootBackup);
};

class ObTenantBackup {
public:
  ObTenantBackup();
  virtual ~ObTenantBackup()
  {}
  int init(const share::ObBaseBackupInfoStruct& info, share::schema::ObMultiVersionSchemaService& schema_service,
      ObRootBalancer& root_balancer, common::ObMySQLProxy& sql_proxy, ObServerManager& server_mgr_,
      ObRebalanceTaskMgr& rebalancer_mgr, obrpc::ObSrvRpcProxy& rpc_proxy, ObRootBackup& root_backup,
      share::ObIBackupLeaseService& backup_lease_service);
  int do_backup();

private:
  int get_tenant_backup_task_info(share::ObTenantBackupTaskInfo& task_info, common::ObISQLClient& trans);
  int do_scheduler();
  int do_generate(const share::ObTenantBackupTaskInfo& task_info, common::ObISQLClient& trans);
  int do_backup(const share::ObTenantBackupTaskInfo& task_info, common::ObISQLClient& trans);
  int do_finish(const share::ObTenantBackupTaskInfo& task_info);
  int do_cancel(const share::ObTenantBackupTaskInfo& task_info, common::ObISQLClient& trans);
  int generate_tablegroup_backup_task(const share::ObTenantBackupTaskInfo& task_info,
      const ObBreakPointPGInfo& point_pg_info, const int64_t max_batch_generate_task_num,
      common::ObIArray<share::ObPGBackupTaskInfo>& pg_backup_task_infos, bool& is_finish);
  int generate_standalone_backup_task(const share::ObTenantBackupTaskInfo& task_info,
      const ObBreakPointPGInfo& point_pg_info, const int64_t max_batch_generate_task_num,
      common::ObIArray<share::ObPGBackupTaskInfo>& pg_backup_task_infos);
  int inner_generate_pg_backup_task(const share::ObTenantBackupTaskInfo& task_info, const ObPGKey& pg_key,
      share::ObPGBackupTaskInfo& pg_backup_task_info);
  int update_tenant_backup_task(const share::ObTenantBackupTaskInfo& src_info,
      const share::ObTenantBackupTaskInfo& dest_info, common::ObISQLClient& trans);
  int get_finished_backup_task(const share::ObTenantBackupTaskInfo& task_info,
      common::ObIArray<share::ObPGBackupTaskInfo>& pg_task_infos, common::ObISQLClient& trans);
  int clean_pg_backup_task(common::ObISQLClient& trans, const share::ObTenantBackupTaskInfo& task_info);
  int get_breakpoint_pg_info(ObBreakPointPGInfo& breakpoint_pg_info);
  int find_break_table_id_index(
      const common::ObIArray<uint64_t>& table_ids, const ObBreakPointPGInfo& breakpoint_pg_info, int64_t& index);
  int find_tg_partition_index(
      const ObBreakPointPGInfo& breakpoint_pg_info, share::schema::ObTablegroupPartitionKeyIter& pkey_iter);
  int find_sd_partition_index(
      const ObBreakPointPGInfo& breakpoint_pg_info, share::schema::ObTablePartitionKeyIter& pkey_iter);
  int upload_pg_list(const share::ObTenantBackupTaskInfo& task_info);
  int add_tablegroup_key_to_extern_list(share::ObExternPGListMgr& pg_list_mgr);
  int add_standalone_key_to_extern_list(share::ObExternPGListMgr& pg_list_mgr);
  int check_doing_pg_tasks(const share::ObTenantBackupTaskInfo& task_info, common::ObISQLClient& trans);
  int check_doing_pg_task(const share::ObPGBackupTaskInfo& pg_backup_task, common::ObISQLClient& trans);
  int check_backup_task_on_progress(const share::ObPGBackupTaskInfo& pg_task_info, bool& is_exist);
  int check_task_in_rebalancer_mgr(const share::ObPGBackupTaskInfo& pg_task_info, bool& is_exist);
  int check_doing_task_finished(
      const share::ObPGBackupTaskInfo& pg_task_info, common::ObISQLClient& trans, bool& is_finished);
  int update_lost_task_finished(const share::ObPGBackupTaskInfo& pg_task_infos, common::ObISQLClient& trans);
  int do_with_finished_task(const share::ObTenantBackupTaskInfo& task_info,
      const common::ObIArray<share::ObPGBackupTaskInfo>& pg_task_infos, common::ObISQLClient& trans,
      bool& can_report_task);
  int check_finished_task_result(const share::ObTenantBackupTaskInfo& task_info,
      const common::ObIArray<share::ObPGBackupTaskInfo>& pg_task_infos, common::ObISQLClient& trans, bool& need_retry,
      bool& can_report_task);
  int reset_pg_backup_tasks(
      const common::ObIArray<share::ObPGBackupTaskInfo>& pg_task_infos, common::ObISQLClient& trans);
  int update_tenant_backup_task_result(
      const share::ObTenantBackupTaskInfo& task_info, const int32_t result, common::ObISQLClient& trans);
  int do_tenat_backup_when_succeed(const share::ObTenantBackupTaskInfo& task_info,
      const common::ObIArray<share::ObPGBackupTaskInfo>& pg_task_infos, common::ObISQLClient& trans,
      bool& can_report_task);
  int do_tenant_backup_when_failed(const share::ObTenantBackupTaskInfo& task_info,
      const common::ObIArray<share::ObPGBackupTaskInfo>& pg_task_infos, common::ObISQLClient& trans,
      bool& can_report_task);
  int cancel_doing_pg_tasks(const share::ObTenantBackupTaskInfo& task_info, common::ObISQLClient& trans);
  int get_table_count_with_partition(const uint64_t tenant_id, const int64_t tablegroup_id,
      share::schema::ObSchemaGetterGuard& schema_guard, int64_t& table_count);
  int check_standalone_table_need_backup(const share::schema::ObTableSchema* table_schema, bool& need_backup);
  int commit_trans(ObMySQLTransaction& trans);
  int start_trans(ObTimeoutCtx& timeout_ctx, ObMySQLTransaction& trans);

private:
  static const int64_t MAX_CHECK_INTERVAL = 10 * 1000 * 1000;  // 10s
  static const int64_t PG_TASK_MAX_RETRY_NUM = 64;

private:
  bool is_inited_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  common::ObMySQLProxy* sql_proxy_;
  uint64_t tenant_id_;
  int64_t backup_set_id_;
  int64_t incarnation_;
  int64_t backup_snapshot_version_;
  int64_t backup_schema_version_;
  share::ObBackupType backup_type_;
  int64_t total_pg_count_;
  ObRootBalancer* root_balancer_;
  ObServerManager* server_mgr_;
  ObRebalanceTaskMgr* rebalancer_mgr_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  ObRootBackup* root_backup_;
  share::ObBaseBackupInfoStruct::BackupDest backup_dest_;
  int64_t total_partition_count_;
  share::ObIBackupLeaseService* backup_lease_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantBackup);
};

class ObBackupUtil {
public:
  ObBackupUtil()
  {}
  virtual ~ObBackupUtil()
  {}
  static int check_sys_tenant_trans_alive(share::ObBackupInfoManager& info_manager, common::ObISQLClient& trans);
  static int check_sys_clean_info_trans_alive(common::ObISQLClient& trans);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_ROOT_BACKUP_H_
