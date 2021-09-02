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

#ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_BACKUPSET_H_
#define OCEANBASE_ROOTSERVER_OB_BACKUP_BACKUPSET_H_

#include "common/ob_region.h"
#include "common/ob_zone.h"
#include "rootserver/ob_thread_idling.h"
#include "rootserver/ob_rebalance_task.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_meta_store.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/backup/ob_backup_lease_info_mgr.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace rootserver {
class ObZoneManager;
class ObServerManager;
class ObRootBalancer;
class ObRebalanceTaskMgr;

class ObBackupBackupsetIdling final : public ObThreadIdling {
public:
  explicit ObBackupBackupsetIdling(volatile bool& stop) : ObThreadIdling(stop)
  {}
  virtual int64_t get_idle_interval_us() override;
};

class ObBackupBackupsetLoadBalancer final {
public:
  ObBackupBackupsetLoadBalancer();
  virtual ~ObBackupBackupsetLoadBalancer();

  int init(ObZoneManager& zone_mgr, ObServerManager& server_mgr);
  int get_next_server(common::ObAddr& server);

private:
  int get_all_server_in_region(const common::ObRegion& region, common::ObIArray<common::ObAddr>& server_list);
  int get_zone_list(const common::ObRegion& region, common::ObIArray<common::ObZone>& zone_list);
  int choose_server(const common::ObIArray<common::ObAddr>& server_list, common::ObAddr& server);

private:
  bool is_inited_;
  ObRandom random_;
  ObZoneManager* zone_mgr_;
  ObServerManager* server_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupBackupsetLoadBalancer);
};

class ObBackupBackupset final : public ObRsReentrantThread {
  friend class ObTenantBackupBackupset;
  typedef ObArray<share::ObTenantBackupTaskItem>::const_iterator BackupArrayIter;

public:
  ObBackupBackupset();
  virtual ~ObBackupBackupset();
  int init(ObMySQLProxy& sql_proxy, ObZoneManager& zone_mgr, ObServerManager& server_mgr, ObRootBalancer& root_balancer,
      ObRebalanceTaskMgr& task_mgr, obrpc::ObSrvRpcProxy& rpc_proxy,
      share::schema::ObMultiVersionSchemaService& schema_service, share::ObIBackupLeaseService& backup_lease_service);
  int start();
  int idle();
  void wakeup();
  void stop();
  virtual void run3() override;
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }
  int get_check_time(const uint64_t tenant_id, int64_t& check_time);
  int update_check_time(const uint64_t tenant_id);

private:
  int insert_check_time(const uint64_t tenant_id);
  int check_can_do_work();
  int do_work(const common::ObIArray<int64_t>& job_ids);
  int get_all_jobs(common::ObIArray<int64_t>& job_ids);
  int get_job_info(const int64_t job_id, share::ObBackupBackupsetJobInfo& job_info);
  int update_job_info(const share::ObBackupBackupsetJobInfo& job_info);
  int do_all_tenant_tasks(const common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& task_infos);
  int do_tenant_task(
      const uint64_t tenant_id, const int64_t job_id, const int64_t backup_set_id, const int64_t copy_id);
  int filter_success_backup_task(common::ObIArray<share::ObTenantBackupTaskItem>& backup_tasks);
  int get_all_backup_set_id(const uint64_t tenant_id, const share::ObBackupBackupsetJobInfo& info,
      common::ObArray<int64_t>& backup_set_ids,
      common::ObArray<share::ObTenantBackupBackupsetTaskInfo>& no_need_schedule_tasks);
  int filter_backup_set_ids(const uint64_t tenant_id, const share::ObBackupBackupsetJobInfo& info,
      const common::ObArray<int64_t>& backup_set_ids, common::ObArray<int64_t>& filered_backup_set_ids);
  int get_full_backup_set_id_list(const share::ObBackupBackupsetJobInfo& info,
      const common::ObArray<share::ObTenantBackupBackupsetTaskItem>& backup_backupset_tasks,
      const common::ObArray<share::ObTenantBackupTaskItem>& backup_tasks, const uint64_t tenant_id,
      const int64_t backup_set_id, common::ObArray<int64_t>& backup_set_ids,
      common::ObArray<share::ObTenantBackupBackupsetTaskInfo>& no_need_schedule_tasks);
  int check_backup_tasks_is_continuous(const uint64_t tenant_id, const share::ObBackupBackupsetJobInfo& info,
      const common::ObArray<share::ObTenantBackupTaskItem>& backup_tasks, bool& is_continuous);
  int get_single_backup_set_id_list(const share::ObBackupBackupsetJobInfo& info,
      const common::ObArray<share::ObTenantBackupBackupsetTaskItem>& backup_backupset_tasks,
      const common::ObArray<share::ObTenantBackupTaskItem>& backup_tasks, const uint64_t tenant_id,
      const int64_t backup_set_id, common::ObArray<int64_t>& backup_set_ids,
      common::ObArray<share::ObTenantBackupBackupsetTaskInfo>& no_need_schedule_tasks);
  int check_backup_backup_has_been_deleted_before(const share::ObBackupBackupsetJobInfo& info,
      const int64_t backup_set_id, const int64_t incarnation, const uint64_t tenant_id, bool& deleted_before);
  int deal_with_no_need_schedule_tasks(const share::ObBackupBackupsetJobInfo& info,
      const share::SimpleBackupBackupsetTenant& tenant,
      const common::ObArray<share::ObTenantBackupBackupsetTaskInfo>& no_need_schedule_tasks);
  int check_already_backup_backupset(
      const common::ObArray<share::ObTenantBackupBackupsetTaskItem>& backup_backupset_tasks,
      const int64_t backup_set_id, bool& already_backup_backupset,
      common::ObArray<share::ObTenantBackupBackupsetTaskInfo>& no_need_schedule_tasks);
  int find_backup_task_lower_bound(const common::ObArray<share::ObTenantBackupTaskItem>& tasks,  // already sorted
      const int64_t backup_set_id, BackupArrayIter& output_iter);
  int get_largest_already_backup_backupset_id(
      const common::ObIArray<share::ObTenantBackupBackupsetTaskItem>& tasks,  // already sorted
      int64_t& backup_set_id);
  int get_all_tenants(const share::ObBackupBackupsetJobInfo& job_info,
      common::ObIArray<share::SimpleBackupBackupsetTenant>& tenant_list);
  int get_all_job_tasks(const share::ObBackupBackupsetJobInfo& job_info,
      common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& task_infos);
  int get_all_job_tasks_from_history(const share::ObBackupBackupsetJobInfo& job_info,
      common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& task_infos);
  int get_current_round_tasks(const share::ObBackupBackupsetJobInfo& job_info,
      common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& task_infos);
  int check_backup_backup_dest_changed(
      const common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& task_infos, bool& changed);
  int find_smallest_unfinished_task(const common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& task_infos,
      share::ObTenantBackupBackupsetTaskInfo& task_info);
  int check_tenant_is_dropped(const uint64_t tenant_id, bool& is_dropped);
  int check_all_tenant_task_finished(const share::ObBackupBackupsetJobInfo& job_info, bool& all_finished);
  int move_job_info_to_history(const int64_t job_id, const int64_t result);
  int update_cluster_data_backup_info(const share::ObBackupBackupsetJobInfo& job_info);
  int do_extern_cluster_backup_info(const share::ObBackupBackupsetJobInfo& job_info,
      const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst);

private:
  int do_job(const int64_t job_id);
  int do_schedule(const share::ObBackupBackupsetJobInfo& job_info);
  int do_backup(const share::ObBackupBackupsetJobInfo& job_info);
  int do_clean(const share::ObBackupBackupsetJobInfo& job_info);
  int do_cancel(const share::ObBackupBackupsetJobInfo& job_info);
  int do_finish(const share::ObBackupBackupsetJobInfo& job_info);

private:
  int check_dest_same_with_gconf(const share::ObBackupBackupsetJobInfo& job_info, bool& is_same);
  int get_job_copy_id_level(
      const share::ObBackupBackupsetJobInfo& job_info, share::ObBackupBackupCopyIdLevel& copy_id_level);
  int do_if_src_data_unavailable(const share::ObBackupBackupsetJobInfo& job_info, bool& is_available);
  int check_src_data_available(const common::ObArray<share::ObTenantBackupBackupsetTaskInfo>& tasks,
      common::ObArray<share::ObTenantBackupBackupsetTaskInfo>& unavailable_task,
      common::ObArray<share::ObTenantBackupBackupsetTaskInfo>& available_task, bool& is_available);
  int update_tenant_tasks_result(
      const common::ObArray<share::ObTenantBackupBackupsetTaskInfo>& tasks, const int result);
  int inner_do_finish_if_device_error(const share::ObBackupBackupsetJobInfo& job_info);
  int inner_schedule_tenant_task(const share::ObBackupBackupsetJobInfo& job_info,
      const common::ObArray<share::SimpleBackupBackupsetTenant>& tenant_list);
  int inner_schedule_tenant_backupset_task(const share::ObBackupBackupsetJobInfo& job_info,
      const share::SimpleBackupBackupsetTenant& tenant, const common::ObIArray<int64_t>& backup_set_ids);
  int check_backup_set_id_valid_for_normal_tenant(const share::ObBackupBackupsetJobInfo& job_info,
      const share::SimpleBackupBackupsetTenant& tenant, const int64_t backup_set_id, bool& is_valid);
  int check_need_insert_backup_set_file_info(
      const share::ObTenantBackupBackupsetTaskInfo& task_info, bool& need_insert);
  int calc_backup_set_copy_id(
      const share::ObBackupBackupsetJobInfo& job_info, const int64_t backup_set_id, int64_t& copy_id);
  int get_valid_backup_backup_file_info_count(
      const common::ObArray<share::ObBackupSetFileInfo>& file_infos, int64_t& count);
  int get_backup_set_copy_id(
      const share::ObBackupBackupsetJobInfo& job_info, const int64_t backup_set_id, int64_t& copy_id);
  int build_tenant_task_info(const uint64_t tenant_id, const int64_t backup_set_id, const int64_t copy_id,
      const share::ObBackupBackupsetJobInfo& job_info, share::ObTenantBackupBackupsetTaskInfo& task_info);
  int get_tenant_backup_task_info(
      const uint64_t tenant_id, const int64_t backup_set_id, share::ObTenantBackupTaskInfo& task_info);
  int get_dst_backup_dest_str(const share::ObBackupBackupsetJobInfo& job_info, char* buf, const int64_t buf_size) const;
  int construct_sys_tenant_backup_backupset_info(share::ObTenantBackupBackupsetTaskInfo& info);
  int insert_tenant_backup_set_file_info(const share::ObTenantBackupBackupsetTaskInfo& info);
  int update_tenant_backup_set_file_info(
      const share::ObTenantBackupBackupsetTaskInfo& info, common::ObISQLClient& client);
  int update_extern_backup_set_file_info(const share::ObBackupSetFileInfo& backup_set_file_info);
  // only called at job finish stage
  int check_job_tasks_is_complete(const share::ObBackupBackupsetJobInfo& job_info,
      common::ObArray<share::SimpleBackupBackupsetTenant>& missing_tenants, bool& is_complete);
  int inner_check_job_tasks_is_complete(const share::ObBackupBackupsetJobInfo& job_info,
      const common::ObArray<share::ObTenantBackupBackupsetTaskInfo>& task_infos,
      const common::ObArray<share::SimpleBackupBackupsetTenant>& tenant_list,
      common::ObArray<share::SimpleBackupBackupsetTenant>& missing_tenants, bool& is_complete);
  int filter_all_sys_job_tasks(const common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& task_infos,
      common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& sys_task_infos);
  int check_tenant_task_real_missing(const share::ObBackupBackupsetJobInfo& job_info,
      const share::SimpleBackupBackupsetTenant& missing_tenant,
      const common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& sys_task_infos, bool& real_missing);
  int inner_check_tenant_task_real_missing(const share::ObBackupBackupsetJobInfo& job_info,
      const share::SimpleBackupBackupsetTenant& missing_tenant,
      const share::ObTenantBackupBackupsetTaskInfo& sys_task_infos, bool& real_missing);
  int do_if_job_tasks_is_not_complete(const share::ObBackupBackupsetJobInfo& job_info, const int64_t result);
  int deal_with_missing_tenants(const share::ObBackupBackupsetJobInfo& job_info,
      const common::ObArray<share::SimpleBackupBackupsetTenant>& missing_tenants);
  int refill_missing_bb_tasks_history_for_clean(const share::ObBackupBackupsetJobInfo& job_info,
      const common::ObArray<share::SimpleBackupBackupsetTenant>& missing_tenants);
  int do_external_backup_set_file_info(const share::ObTenantBackupBackupsetTaskInfo& bb_task,
      const share::ObBackupSetFileInfo::BackupSetStatus& status, const share::ObBackupFileStatus::STATUS& file_status,
      const int64_t result);

private:
  struct CompareBackupTaskBackupSetId {
    bool operator()(const share::ObTenantBackupTaskItem& item, const int64_t backup_set_id)
    {
      return item.backup_set_id_ < backup_set_id;
    }
  };
  struct CompareBackupBackupsetTaskBackupSetId {
    bool operator()(const share::ObTenantBackupBackupsetTaskItem& item, const int64_t backup_set_id)
    {
      return item.backup_set_id_ < backup_set_id;
    }
  };
  struct CompareHistoryBackupTaskItem {
    bool operator()(const share::ObTenantBackupTaskItem& lhs, const share::ObTenantBackupTaskItem& rhs)
    {
      return lhs.backup_set_id_ < rhs.backup_set_id_;
    }
  };
  struct CompareHistoryBackupBackupsetTaskItem {
    bool operator()(
        const share::ObTenantBackupBackupsetTaskItem& lhs, const share::ObTenantBackupBackupsetTaskItem& rhs)
    {
      return lhs.backup_set_id_ < rhs.backup_set_id_;
    }
  };

  struct CompareTenantId {
    bool operator()(const share::SimpleBackupBackupsetTenant& lhs, const share::SimpleBackupBackupsetTenant& rhs)
    {
      return lhs.tenant_id_ < rhs.tenant_id_;
    }
  };

  struct CompareBBTaskTenantIdLarger {
    bool operator()(
        const share::ObTenantBackupBackupsetTaskItem& lhs, const share::ObTenantBackupBackupsetTaskItem& rhs)
    {
      return lhs.tenant_id_ > rhs.tenant_id_;
    }
  };

private:
  bool is_inited_;
  bool extern_device_error_;
  ObMySQLProxy* sql_proxy_;
  ObZoneManager* zone_mgr_;
  ObServerManager* server_mgr_;
  ObRootBalancer* root_balancer_;
  ObRebalanceTaskMgr* rebalance_task_mgr_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  share::ObIBackupLeaseService* backup_lease_service_;
  mutable ObBackupBackupsetIdling idling_;
  common::hash::ObHashMap<uint64_t, int64_t> check_time_map_;  // map for checking pg task
  share::ObBackupInnerTableVersion inner_table_version_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupBackupset);
};

class ObTenantBackupBackupset final {
public:
  ObTenantBackupBackupset();
  virtual ~ObTenantBackupBackupset();
  int init(const bool is_dropped, const uint64_t tenant_id, const int64_t job_id, const int64_t backup_set_id,
      const int64_t copy_id, ObMySQLProxy& sql_proxy, ObZoneManager& zone_mgr, ObServerManager& server_mgr,
      ObRootBalancer& root_balancer, ObRebalanceTaskMgr& task_mgr, ObBackupBackupset& backup_backupset,
      obrpc::ObSrvRpcProxy& rpc_proxy, share::ObIBackupLeaseService& backup_lease_service);
  int do_task();

private:
  int do_with_status(const share::ObTenantBackupBackupsetTaskInfo& task_info);
  int do_generate(const share::ObTenantBackupBackupsetTaskInfo& task_info);
  int do_backup(const share::ObTenantBackupBackupsetTaskInfo& task_info);
  int do_cancel(const share::ObTenantBackupBackupsetTaskInfo& task_info);
  int do_finish(const share::ObTenantBackupBackupsetTaskInfo& task_info);

private:
  int check_need_generate_pg_tasks(const share::ObTenantBackupBackupsetTaskInfo& task_info, bool& need_generate);
  int check_doing_pg_tasks(const share::ObTenantBackupBackupsetTaskInfo& task_info);
  int check_doing_pg_task(const share::ObPGBackupBackupsetTaskInfo& task_info, bool& actual_doing);
  int check_in_rebalance_task_mgr(const share::ObPGBackupBackupsetTaskInfo& task_info, bool& exists);
  int check_task_in_progress(const share::ObPGBackupBackupsetTaskInfo& task_info, bool& in_progress);
  int check_pg_task_finished(const share::ObPGBackupBackupsetTaskInfo& task_info, bool& finished);
  int reset_doing_pg_tasks(const common::ObIArray<share::ObPGBackupBackupsetTaskInfo>& task_infos);
  int inner_reset_doing_pg_tasks(const share::ObPGBackupBackupsetTaskInfo& task_info);
  int get_same_trace_id_tasks(const share::ObPGBackupBackupsetTaskInfo& task_info,
      common::ObIArray<share::ObPGBackupBackupsetTaskInfo>& task_infos);

private:
  int deal_with_finished_failed_pg_task(const share::ObTenantBackupBackupsetTaskInfo& task_info);

private:
  int check_meta_file_complete(const share::ObTenantBackupTaskInfo& backup_info, const int64_t full_backup_set_id,
      const int64_t inc_backup_set_id, const common::ObIArray<common::ObPGKey>& pg_list, int64_t& total_partition_count,
      int64_t& total_macro_block_count, bool& complete);
  int check_pg_list_complete(share::ObBackupMetaFileStore& file_store,
      const common::ObIArray<common::ObPGKey>& pg_key_list, int64_t& total_partition_count,
      int64_t& total_macro_block_count, bool& complete);

private:
  int get_tenant_backup_task_info(const int64_t backup_set_id, share::ObTenantBackupTaskInfo& task_info);
  int get_backup_set_id_info(
      const share::ObTenantBackupTaskInfo& task_info, int64_t& full_backup_set_id, int64_t& inc_backup_set_id);
  int fetch_all_pg_list(const share::ObClusterBackupDest& src_backup_dest, const int64_t full_backup_set_id,
      const int64_t inc_backup_set_id, const int64_t backup_snapshot_version, const int64_t compatible,
      common::ObIArray<common::ObPGKey>& pg_keys);
  int generate_backup_backupset_task(
      const int64_t copy_id, const int64_t backup_set_id, const common::ObIArray<common::ObPGKey>& pg_keys);
  int inner_generate_backup_backupset_task(const int64_t copy_id, const int64_t backup_set_id,
      const common::ObPGKey& pg_key, share::ObPGBackupBackupsetTaskInfo& pg_task_info);
  int check_backup_backupset_task_finished(bool& finished);
  int get_finished_pg_stat(
      const share::ObTenantBackupBackupsetTaskInfo& task_info, share::ObPGBackupBackupsetTaskStat& pg_stat);

private:
  int get_tenant_task_info(share::ObTenantBackupBackupsetTaskInfo& task_info);
  int update_tenant_task_info(const share::ObTenantBackupBackupsetTaskInfo& task_info);
  int transfer_backup_set_meta(const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst,
      const int64_t full_backup_set_id, const int64_t inc_backup_set_id, const int64_t backup_date,
      const int64_t compatible);
  int transfer_single_file(const common::ObString& src_uri, const common::ObString& src_storage_info,
      const common::ObString& dst_uri, const common::ObString& dst_storage_info);
  int upload_pg_list(const share::ObClusterBackupDest& backup_dest, const int64_t full_backup_set_id,
      const int64_t inc_backup_set_id, const int64_t backup_snapshot_version, const int64_t compatible,
      const common::ObIArray<common::ObPGKey>& pg_keys);
  int transfer_pg_list(const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst,
      const int64_t full_backup_set_id, const int64_t inc_backup_set_id, const int64_t backup_date,
      const int64_t compatible);
  int transfer_tenant_locality_info(const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst,
      const int64_t full_backup_set_id, const int64_t inc_backup_set_id, const int64_t backup_date,
      const int64_t compatible);
  int do_backup_set_info(const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst,
      const int64_t full_backup_set_id, const int64_t inc_backup_set_id, const int64_t backup_date,
      const int64_t compatible);
  int check_backup_info_exist(
      const common::ObIArray<share::ObExternBackupSetInfo>& infos, const int64_t backup_set_id, bool& exist);
  int do_extern_data_backup_info(const share::ObExternBackupInfo::ExternBackupInfoStatus& status,
      const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst, const int64_t full_backup_set_id,
      const int64_t inc_backup_set_id);
  int check_backup_info_exist(const common::ObIArray<share::ObExternBackupInfo>& extern_backup_infos,
      const int64_t full_backup_set_id, const int64_t inc_backup_set_id, bool& exist);

private:
  int convert_array_to_set(const common::ObIArray<common::ObPGKey>& pg_key, hash::ObHashSet<common::ObPGKey>& pg_set);

private:
  static const int64_t CHECK_TIME_INTERVAL = 5 * 60 * 1000 * 1000;  // 5min

private:
  bool is_inited_;
  bool is_dropped_;
  uint64_t tenant_id_;
  int64_t job_id_;
  int64_t backup_set_id_;
  int64_t copy_id_;
  ObMySQLProxy* sql_proxy_;
  ObZoneManager* zone_mgr_;
  ObServerManager* server_mgr_;
  ObRootBalancer* root_balancer_;
  ObRebalanceTaskMgr* rebalance_task_mgr_;
  ObBackupBackupset* backup_backupset_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;

  share::ObBaseBackupInfoStruct::BackupDest src_backup_dest_;
  share::ObBaseBackupInfoStruct::BackupDest dst_backup_dest_;
  share::ObIBackupLeaseService* backup_lease_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantBackupBackupset);
};

class ObPartitionBackupBackupset final {
public:
  ObPartitionBackupBackupset();
  virtual ~ObPartitionBackupBackupset();
  int init(ObMySQLProxy& sql_proxy, ObZoneManager& zone_mgr, ObServerManager& server_mgr, ObRebalanceTaskMgr& task_mgr);
  int partition_backup_backupset(const uint64_t tenant_id);

private:
  int get_one_job(int64_t& job_id);
  int get_one_tenant_if_sys(const int64_t job_id, share::SimpleBackupBackupsetTenant& tenant, bool& exists);
  int get_smallest_unfinished_task(
      const int64_t job_id, const share::SimpleBackupBackupsetTenant& tenant, int64_t& backup_set_id);
  int get_tenant_backup_task(
      const uint64_t tenant_id, const int64_t backup_set_id, share::ObTenantBackupTaskInfo& backup_info);
  int get_tenant_backup_backupset_task(const share::SimpleBackupBackupsetTenant& tennat, const int64_t job_id,
      const int64_t backup_set_id, share::ObTenantBackupBackupsetTaskInfo& backup_backup_info);
  int get_pending_tasks(const share::SimpleBackupBackupsetTenant& tenant, const int64_t job_id,
      const int64_t backup_set_id, common::ObIArray<share::ObPGBackupBackupsetTaskInfo>& task_infos);
  int send_backup_backupset_rpc(const share::SimpleBackupBackupsetTenant& tenant,
      const share::ObTenantBackupTaskInfo& backup_info,
      const share::ObTenantBackupBackupsetTaskInfo& backup_backup_info,
      const common::ObIArray<share::ObPGBackupBackupsetTaskInfo>& task_infos);
  int build_backup_backupset_arg(const common::ObAddr& addr, const share::ObTenantBackupTaskInfo& backup_info,
      const share::ObTenantBackupBackupsetTaskInfo& backup_backup_info,
      const share::ObPGBackupBackupsetTaskInfo& pg_task_info, share::ObBackupBackupsetArg& arg);
  int build_backup_backupset_task_infos(const common::ObAddr& addr, const share::ObTenantBackupTaskInfo& backup_info,
      const share::ObTenantBackupBackupsetTaskInfo& backup_backup_info,
      const common::ObIArray<share::ObPGBackupBackupsetTaskInfo>& pg_task_infos,
      common::ObIArray<rootserver::ObBackupBackupsetTaskInfo>& task_infos);
  int build_pg_backup_backupset_task_infos(const common::ObAddr& server,
      const common::ObIArray<share::ObPGBackupBackupsetTaskInfo>& task_infos,
      common::ObIArray<share::ObPGBackupBackupsetTaskInfo>& new_task_infos);

private:
  static const int64_t BATCH_TASK_COUNT = 1024;

private:
  bool is_inited_;
  ObMySQLProxy* sql_proxy_;
  ObZoneManager* zone_mgr_;
  ObServerManager* server_mgr_;
  ObRebalanceTaskMgr* rebalance_task_mgr_;
  ObBackupBackupsetLoadBalancer load_balancer_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionBackupBackupset);
};

class ObBackupBackupHelper {
public:
  ObBackupBackupHelper();
  ~ObBackupBackupHelper();
  int init(const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst, const uint64_t tenant_id,
      share::ObIBackupLeaseService& backup_lease_service);
  int do_extern_backup_set_file_info(const share::ObBackupSetFileInfo::BackupSetStatus& status,
      const share::ObBackupFileStatus::STATUS& file_status, const int64_t backup_set_id, const int64_t copy_id,
      const int64_t result);
  int do_extern_single_backup_set_info(const share::ObBackupSetFileInfo::BackupSetStatus& status,
      const share::ObBackupFileStatus::STATUS& file_status, const int64_t full_backup_set_id,
      const int64_t inc_backup_set_id, const int64_t copy_id, const int64_t backup_date, const int64_t result);
  int sync_extern_backup_set_file_info();

private:
  bool is_inited_;
  share::ObClusterBackupDest src_;
  share::ObClusterBackupDest dst_;
  uint64_t tenant_id_;
  share::ObIBackupLeaseService* backup_lease_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupBackupHelper);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif
