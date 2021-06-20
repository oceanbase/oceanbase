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

#ifndef OCEANBASE_ROOTSERVER_OB_PARTITION_VALIDATE_H_
#define OCEANBASE_ROOTSERVER_OB_PARTITION_VALIDATE_H_

#include <stdint.h>
#include "lib/container/ob_iarray.h"
#include "share/ob_check_stop_provider.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_extern_backup_info_mgr.h"
#include "share/backup/ob_pg_validate_task_updater.h"
#include "rootserver/ob_rebalance_task.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace rootserver {
class ObRootValidate;
class ObServerManager;
class ObRebalanceTaskMgr;
class ObZoneManager;
class TenantBalanceStat;

class ObBackupValidateLoadBalancer {
public:
  ObBackupValidateLoadBalancer() = default;
  virtual ~ObBackupValidateLoadBalancer() = default;

  int init(rootserver::ObServerManager& server_mgr, rootserver::ObZoneManager& zone_mgr);
  int get_next_server(common::ObAddr& server);

private:
  int get_all_server_in_region(const common::ObRegion& region, common::ObIArray<common::ObAddr>& server_list);
  int get_zone_list(const common::ObRegion& region, common::ObIArray<common::ObZone>& zone_list);
  int choose_server(const common::ObIArray<common::ObAddr>& server_list, common::ObAddr& server);

private:
  bool is_inited_;
  rootserver::ObServerManager* server_mgr_;
  rootserver::ObZoneManager* zone_mgr_;
};

class ObPartitionValidate {
public:
  ObPartitionValidate();
  virtual ~ObPartitionValidate();
  int init(common::ObMySQLProxy& sql_proxy, ObRebalanceTaskMgr& task_mgr, ObServerManager& server_mgr,
      ObZoneManager& zone_mgr);
  int partition_validate(const uint64_t tenant_id, int64_t& task_cnt);

private:
  struct ObBackupSetPGTaskList {
    ObBackupSetPGTaskList(){};
    virtual ~ObBackupSetPGTaskList(){};
    TO_STRING_KV(K_(backup_set_id));

    void reset()
    {
      pg_task_infos_.reset();
    }
    int64_t backup_set_id_;
    common::ObArray<share::ObPGValidateTaskInfo> pg_task_infos_;
  };

  struct CompareLogArchiveBackupInfo {
    bool operator()(const share::ObLogArchiveBackupInfo& lhs, const share::ObLogArchiveBackupInfo& rhs) const
    {
      return lhs.status_.start_ts_ < rhs.status_.start_ts_;
    }
  };

  struct CompareLogArchiveSnapshotVersion {
    bool operator()(const share::ObLogArchiveBackupInfo& log_info, const int64_t snapshot_version) const
    {
      return log_info.status_.start_ts_ < snapshot_version;
    }
  };

  struct CompareTenantBackupTaskInfo {
    bool operator()(const share::ObTenantBackupTaskInfo& lhs, const share::ObTenantBackupTaskInfo& rhs) const
    {
      return lhs.backup_set_id_ < rhs.backup_set_id_;
    }
  };

  struct CompareTenantBackupTaskBackupSetId {
    bool operator()(const share::ObTenantBackupTaskInfo& backup_info, const int64_t backup_set_id) const
    {
      return backup_info.backup_set_id_ < backup_set_id;
    }
  };

private:
  int get_backup_validate_task_info(const int64_t job_id, share::ObBackupValidateTaskInfo& task_info);
  int get_log_archive_backup_info_(const int64_t snapshot_version,
      const common::ObArray<share::ObLogArchiveBackupInfo>& log_infos, share::ObLogArchiveBackupInfo& log_info);
  int get_tenant_backup_task_info_(const int64_t backup_set_id,
      const common::ObArray<share::ObTenantBackupTaskInfo>& backup_infos, share::ObTenantBackupTaskInfo& backup_info);
  int get_all_tenant_log_archive_infos(
      const uint64_t tenant_id, common::ObArray<share::ObLogArchiveBackupInfo>& log_infos);
  int get_all_tenant_backup_task_infos(
      const uint64_t tenant_id, common::ObArray<share::ObTenantBackupTaskInfo>& backup_infos);
  int get_extern_backup_set_infos(const int64_t backup_set_id,
      const common::ObArray<share::ObTenantBackupTaskInfo>& backup_infos, int64_t& full_backup_set_id,
      int64_t& inc_backup_set_id, int64_t& cluster_version);
  int check_need_validate_clog(const share::ObBackupValidateTaskInfo& task_info,
      const share::ObPGValidateTaskInfo& pg_task_info, const common::ObArray<share::ObLogArchiveBackupInfo>& log_infos,
      const common::ObArray<share::ObTenantBackupTaskInfo>& backup_infos, bool& need_validate_clog);
  int check_snapshot_version_in_log_archive_round(const int64_t snapshot_version,
      const common::ObArray<share::ObLogArchiveBackupInfo>& log_infos, bool& in_log_archive_round);
  int get_prev_validate_backup_set_id(const share::ObTenantBackupTaskInfo& cur_backup_info,
      const common::ObArray<share::ObTenantBackupTaskInfo>& backup_infos, int64_t& prev_backup_set_id);
  int check_prev_backup_set_id_in_same_log_archive_round(const int64_t backup_set_id,
      const common::ObArray<share::ObLogArchiveBackupInfo>& log_infos,
      const common::ObArray<share::ObTenantBackupTaskInfo>& backup_infos,
      bool& prev_backup_set_in_same_log_archive_round);
  int get_need_validate_backup_set_ids(const int64_t job_id, const share::ObBackupValidateTenant& validate_tenant,
      common::ObIArray<int64_t>& backup_set_ids);
  int get_tenant_validate_info(const int64_t job_id, const share::ObBackupValidateTenant& validate_tenant,
      const int64_t incarnation, share::ObTenantValidateTaskInfo& tenant_task_info);
  int get_tenant_validate_infos(const int64_t job_id, const share::ObBackupValidateTenant& validate_tenant,
      const int64_t incarnation, common::ObIArray<share::ObTenantValidateTaskInfo>& tenant_task_infos);
  int validate_pg(const int64_t job_id, const share::ObBackupValidateTenant& validate_tenant);
  int get_not_finished_tenant_validate_task(const share::ObBackupValidateTenant& validate_tenant,
      common::ObIArray<share::ObTenantValidateTaskInfo>& tenant_task_infos);
  int get_pending_pg_validate_task(const int64_t job_id, const share::ObBackupValidateTenant& validate_tenant,
      common::ObIArray<ObBackupSetPGTaskList>& backup_set_pg_list);
  int get_pending_pg_validate_task(const int64_t job_id, const share::ObBackupValidateTenant& validate_tenant,
      common::ObIArray<share::ObPGValidateTaskInfo>& pg_task_infos);
  int get_pending_pg_validate_task(const int64_t job_id, const share::ObBackupValidateTenant& validate_tenant,
      const int64_t backup_set_id, common::ObIArray<share::ObPGValidateTaskInfo>& pg_task_infos);
  int get_log_archive_max_next_time(
      const uint64_t tenant_id, const int64_t incarnation, const int64_t log_archive_round, int64_t& max_next_time);
  int generate_batch_pg_task(const bool is_dropped_tenant, const share::ObBackupValidateTaskInfo& validate_job,
      const share::ObTenantValidateTaskInfo& tenant_task_info,
      const common::ObIArray<share::ObPGValidateTaskInfo>& pg_task_infos);
  int build_physical_validate_arg(const bool is_dropped_tenant, const bool need_validate_clog,
      const int64_t backup_set_id, const int64_t log_archive_round,
      const share::ObTenantValidateTaskInfo& tenant_task_info, const common::ObPartitionKey& pg_key,
      const common::ObAddr& server, const share::ObTaskId& trace_id, const int64_t cluster_version,
      share::ObPhysicalValidateArg& arg);
  int build_pg_validate_task_info(const common::ObAddr& addr, const share::ObTaskId& trace_id,
      const share::ObPGValidateTaskInfo& src_pg_info, share::ObPGValidateTaskInfo& pg_task_info);
  int batch_update_pg_task_infos(
      const bool is_dropped_tenant, const common::ObIArray<share::ObPGValidateTaskInfo>& task_info);

private:
  bool is_inited_;
  common::ObMySQLProxy* sql_proxy_;
  rootserver::ObRootValidate* root_validate_;
  rootserver::ObRebalanceTaskMgr* task_mgr_;
  rootserver::ObServerManager* server_mgr_;
  rootserver::ObZoneManager* zone_mgr_;
  rootserver::ObBackupValidateLoadBalancer load_balancer_;
  share::ObPGValidateTaskUpdater pg_task_updater_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionValidate);
};

}  // namespace rootserver
}  // namespace oceanbase

#endif
