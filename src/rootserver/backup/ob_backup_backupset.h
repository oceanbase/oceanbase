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

// TODO: if src replica is removed, need to exit and record failure
class ObBackupBackupset final : public ObRsReentrantThread {
  friend class ObTenantBackupBackupset;
  typedef ObArray<share::ObTenantBackupTaskItem>::const_iterator BackupArrayIter;

public:
  ObBackupBackupset();
  virtual ~ObBackupBackupset();
  int init(ObMySQLProxy& sql_proxy, ObZoneManager& zone_mgr, ObServerManager& server_mgr, ObRootBalancer& root_balancer,
      ObRebalanceTaskMgr& task_mgr, share::schema::ObMultiVersionSchemaService& schema_service,
      share::ObIBackupLeaseService& backup_lease_service);
  int start();
  int idle();
  void wakeup();
  void stop();
  virtual void run3() override;
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }

private:
  int check_can_do_work();
  int do_work(const common::ObIArray<int64_t>& job_ids);
  int get_all_jobs(common::ObIArray<int64_t>& job_ids);
  int get_job_info(const int64_t job_id, share::ObBackupBackupsetJobInfo& job_info);
  int update_job_info(const int64_t job_id, const share::ObBackupBackupsetJobInfo& job_info);
  int do_all_tenant_tasks(const common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& task_infos);
  int do_tenant_task(const uint64_t tenant_id, const int64_t job_id, const int64_t backup_set_id);
  int get_all_backup_set_id(
      const uint64_t tenant_id, const share::ObBackupBackupsetJobInfo& info, common::ObArray<int64_t>& backup_set_ids);
  int get_full_backup_set_id_list(const common::ObArray<share::ObTenantBackupBackupsetTaskItem>& backup_backupset_tasks,
      const common::ObArray<share::ObTenantBackupTaskItem>& backup_tasks, const int64_t backup_set_id,
      common::ObArray<int64_t>& backup_set_ids);
  int get_single_backup_set_id_list(
      const common::ObArray<share::ObTenantBackupBackupsetTaskItem>& backup_backupset_tasks,
      const common::ObArray<share::ObTenantBackupTaskItem>& backup_tasks, const int64_t backup_set_id,
      common::ObArray<int64_t>& backup_set_ids);
  int find_backup_task_lower_bound(const common::ObArray<share::ObTenantBackupTaskItem>& tasks,  // already sorted
      const int64_t backup_set_id, BackupArrayIter& output_iter);
  int get_largest_already_backup_backupset_id(
      const common::ObIArray<share::ObTenantBackupBackupsetTaskItem>& tasks,  // already sorted
      int64_t& backup_set_id);
  int get_all_tenant_ids(const int64_t backup_set_id, common::ObIArray<uint64_t>& tenant_ids);
  int get_all_job_tasks(const share::ObBackupBackupsetJobInfo& job_info,
      common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& task_infos);
  int get_current_round_tasks(const share::ObBackupBackupsetJobInfo& job_info,
      common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& task_infos);
  int find_smallest_unfinished_task(const common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& task_infos,
      share::ObTenantBackupBackupsetTaskInfo& task_info);
  int check_tenant_is_dropped(const uint64_t tenant_id, bool& is_dropped);
  int check_all_tenant_task_finished(const share::ObBackupBackupsetJobInfo& job_info, bool& all_finished);
  int move_job_info_to_history(const int64_t job_id);
  int update_cluster_data_backup_info(const share::ObBackupBackupsetJobInfo& job_info);
  int do_extern_cluster_backup_info(const share::ObBackupBackupsetJobInfo& job_info,
      const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst);

private:
  int do_job(const int64_t job_id);
  int do_schedule(const share::ObBackupBackupsetJobInfo& job_info);
  int do_backup(const share::ObBackupBackupsetJobInfo& job_info);
  int do_cancel(const share::ObBackupBackupsetJobInfo& job_info);
  int do_finish(const share::ObBackupBackupsetJobInfo& job_info);

private:
  int inner_schedule_tenant_task(
      const share::ObBackupBackupsetJobInfo& job_info, const common::ObIArray<uint64_t>& tenant_ids);
  int inner_schedule_tenant_backupset_task(const share::ObBackupBackupsetJobInfo& job_info, const uint64_t tenant_id,
      const common::ObIArray<int64_t>& backup_set_ids);
  int build_tenant_task_info(const uint64_t tenant_id, const int64_t backup_set_id,
      const share::ObBackupBackupsetJobInfo& job_info, share::ObTenantBackupBackupsetTaskInfo& task_info);
  int get_tenant_backup_task_info(
      const uint64_t tenant_id, const int64_t backup_set_id, share::ObTenantBackupTaskInfo& task_info);
  int get_dst_backup_dest_str(char* buf, const int64_t buf_size) const;
  int construct_sys_tenant_backup_backupset_info(share::ObTenantBackupBackupsetTaskInfo& info);

private:
  struct CompareBackupTaskBackupSetId {
    bool operator()(const share::ObTenantBackupTaskItem& item, const int64_t backup_set_id)
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

private:
  bool is_inited_;
  ObMySQLProxy* sql_proxy_;
  ObZoneManager* zone_mgr_;
  ObServerManager* server_mgr_;
  ObRootBalancer* root_balancer_;
  ObRebalanceTaskMgr* rebalance_task_mgr_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  share::ObIBackupLeaseService* backup_lease_service_;
  mutable ObBackupBackupsetIdling idling_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupBackupset);
};

class ObTenantBackupBackupset final {
public:
  ObTenantBackupBackupset();
  virtual ~ObTenantBackupBackupset();
  int init(const uint64_t tenant_id, const int64_t job_id, const int64_t backup_set_id, ObMySQLProxy& sql_proxy,
      ObZoneManager& zone_mgr, ObServerManager& server_mgr, ObRootBalancer& root_balancer, ObRebalanceTaskMgr& task_mgr,
      ObBackupBackupset& backup_backupset, share::ObIBackupLeaseService& backup_lease_service);
  int do_task();

private:
  int do_with_status(const share::ObTenantBackupBackupsetTaskInfo& task_info);
  int do_generate(const share::ObTenantBackupBackupsetTaskInfo& task_info);
  int do_backup(const share::ObTenantBackupBackupsetTaskInfo& task_info);
  int do_cancel(const share::ObTenantBackupBackupsetTaskInfo& task_info);
  int do_finish(const share::ObTenantBackupBackupsetTaskInfo& task_info);

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
      const int64_t inc_backup_set_id, common::ObIArray<common::ObPGKey>& pg_keys);
  int generate_backup_backupset_task(const int64_t backup_set_id, const common::ObIArray<common::ObPGKey>& pg_keys);
  int inner_generate_backup_backupset_task(
      const int64_t backup_set_id, const common::ObPGKey& pg_key, share::ObPGBackupBackupsetTaskInfo& pg_task_info);
  int check_backup_backupset_task_finished(bool& finished);
  int get_finished_pg_stat(share::ObPGBackupBackupsetTaskStat& pg_stat);

private:
  int get_tenant_task_info(share::ObTenantBackupBackupsetTaskInfo& task_info);
  int update_tenant_task_info(const share::ObTenantBackupBackupsetTaskInfo& task_info);
  int transfer_backup_set_meta(const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst,
      const int64_t full_backup_set_id, const int64_t inc_backup_set_id);
  int transfer_single_file(const common::ObString& src_uri, const common::ObString& src_storage_info,
      const common::ObString& dst_uri, const common::ObString& dst_storage_info);
  int upload_pg_list(const share::ObClusterBackupDest& backup_dest, const int64_t full_backup_set_id,
      const int64_t inc_backup_set_id, const common::ObIArray<common::ObPGKey>& pg_keys);
  int transfer_pg_list(const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst,
      const int64_t full_backup_set_id, const int64_t inc_backup_set_id);
  int transfer_tenant_locality_info(const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst,
      const int64_t full_backup_set_id, const int64_t inc_backup_set_id);
  int do_backup_set_info(const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst,
      const int64_t full_backup_set_id, const int64_t inc_backup_set_id);
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
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t job_id_;
  int64_t backup_set_id_;
  ObMySQLProxy* sql_proxy_;
  ObZoneManager* zone_mgr_;
  ObServerManager* server_mgr_;
  ObRootBalancer* root_balancer_;
  ObRebalanceTaskMgr* rebalance_task_mgr_;
  ObBackupBackupset* backup_backupset_;

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
  int get_smallest_unfinished_task(
      const int64_t job_id, const uint64_t tenant_id, const int64_t copy_id, int64_t& backup_set_id);
  int get_tenant_backup_task(
      const uint64_t tenant_id, const int64_t backup_set_id, share::ObTenantBackupTaskInfo& backup_info);
  int get_tenant_backup_backupset_task(const uint64_t tenant_id, const int64_t job_id, const int64_t backup_set_id,
      const int64_t copy_id, share::ObTenantBackupBackupsetTaskInfo& backup_backup_info);
  int get_pending_tasks(const uint64_t tenant_id, const int64_t job_id, const int64_t backup_set_id,
      const int64_t copy_id, common::ObIArray<share::ObPGBackupBackupsetTaskInfo>& task_infos);
  int send_backup_backupset_rpc(const share::ObTenantBackupTaskInfo& backup_info,
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
  bool is_inited_;
  ObMySQLProxy* sql_proxy_;
  ObZoneManager* zone_mgr_;
  ObServerManager* server_mgr_;
  ObRebalanceTaskMgr* rebalance_task_mgr_;
  ObBackupBackupsetLoadBalancer load_balancer_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionBackupBackupset);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif
