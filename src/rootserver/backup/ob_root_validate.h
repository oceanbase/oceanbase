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

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_VALIDATE_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_VALIDATE_H_

#include <stdint.h>
#include "lib/container/ob_iarray.h"
#include "rootserver/ob_root_balancer.h"
#include "rootserver/ob_thread_idling.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "rootserver/backup/ob_partition_validate.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_validate_task_updater.h"
#include "share/backup/ob_tenant_validate_task_updater.h"
#include "share/backup/ob_pg_validate_task_updater.h"
#include "storage/ob_partition_base_data_validate.h"

namespace oceanbase {
namespace common {
class ObServerConfig;
class ObMySQLProxy;
class ObISQLClient;
}  // namespace common
namespace obrpc {
class ObSrvRpcProxy;
}
namespace storage {
class ObBackupMetaIndexStore;
class ObIBackupLeaseService;
}  // namespace storage
namespace rootserver {
class ObServerManager;
class ObRebalanceTaskMgr;

class ObRootValidateIdling : public ObThreadIdling {
public:
  explicit ObRootValidateIdling(volatile bool& stop) : ObThreadIdling(stop)
  {}
  virtual int64_t get_idle_interval_us() override;
};

class ObRootValidate : public ObRsReentrantThread {
  friend class ObTenantValidate;

public:
  ObRootValidate();
  virtual ~ObRootValidate();
  int init(common::ObServerConfig& config, common::ObMySQLProxy& sql_proxy, ObRootBalancer& root_balancer,
      ObServerManager& server_manager, ObRebalanceTaskMgr& rebalance_mgr, obrpc::ObSrvRpcProxy& rpc_proxy,
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
  virtual bool is_working() const
  {
    return is_working_;
  }
  void update_prepare_flag(const bool is_prepare);
  bool get_prepare_flag() const;

private:
  int check_can_do_work(bool& can);

  int check_tenant_has_been_dropped(uint64_t tenant_id, bool& dropped);
  int get_all_validate_tenants(const share::ObBackupValidateTaskInfo& task_info,
      common::ObIArray<share::ObBackupValidateTenant>& validate_tenants);
  int get_tenant_ids(const share::ObBackupValidateTaskInfo& task_info, common::ObIArray<uint64_t>& tenant_ids);
  int get_all_tenant_ids(common::ObIArray<uint64_t>& tenant_ids);
  int get_need_validate_tasks(common::ObIArray<share::ObBackupValidateTaskInfo>& tasks);

  int do_root_scheduler(const common::ObIArray<share::ObBackupValidateTaskInfo>& tasks);
  int do_scheduler(const share::ObBackupValidateTaskInfo& task);
  int do_with_status(const share::ObBackupValidateTaskInfo& task);

  int do_schedule(const share::ObBackupValidateTaskInfo& task_info);
  int do_sys_tenant_schedule(const share::ObBackupValidateTaskInfo& task_info);
  int do_normal_tenant_schedule(const share::ObBackupValidateTaskInfo& task_info);

  int do_validate(const share::ObBackupValidateTaskInfo& task_info);
  int do_sys_tenant_validate(const share::ObBackupValidateTaskInfo& task_info);
  int do_normal_tenant_validate(const share::ObBackupValidateTaskInfo& task_info);

  int do_finish(const share::ObBackupValidateTaskInfo& task_info);
  int do_sys_tenant_finish(const share::ObBackupValidateTaskInfo& task_info);
  int do_normal_tenant_finish(const share::ObBackupValidateTaskInfo& task_info);

  int do_cancel(const share::ObBackupValidateTaskInfo& task_info);
  int do_sys_tenant_cancel(const share::ObBackupValidateTaskInfo& task_info);
  int do_normal_tenant_cancel(const share::ObBackupValidateTaskInfo& task_info);

  int do_tenant_scheduler(const share::ObBackupValidateTenant& validate_tenant,
      const share::ObBackupValidateTaskInfo& task_info, share::ObTenantValidateTaskInfo& tenant_info);
  int do_tenant_scheduler_when_dropped(
      const share::ObBackupValidateTenant& validate_tenant, const share::ObBackupValidateTaskInfo& task_info);
  int do_tenant_scheduler_when_not_dropped(
      const share::ObBackupValidateTenant& validate_tenant, const share::ObBackupValidateTaskInfo& task_info);
  int schedule_tenants_validate(const share::ObBackupValidateTaskInfo& task_info,
      const common::ObIArray<share::ObTenantBackupTaskInfo>& tenant_backup_tasks);
  int schedule_tenant_validate(const share::ObBackupValidateTaskInfo& task_info,
      const share::ObTenantBackupTaskInfo& tenant_backup_task, const int64_t task_id, bool is_dropped_tenant = false);
  int get_log_archive_time_range(const uint64_t tenant_id, int64_t& start_ts, int64_t& checkpoint_ts);
  int get_log_archive_round_of_snapshot_version(
      const uint64_t tenant_id, const int64_t snapshot_version, int64_t& log_archive_round);
  int get_all_log_archive_backup_infos(
      const uint64_t tenant_id, common::ObIArray<share::ObLogArchiveBackupInfo>& log_infos);
  int get_all_tenant_backup_tasks(common::ObIArray<share::ObTenantBackupTaskInfo>& tenant_backup_tasks);
  int get_tenant_backup_tasks(
      const uint64_t tenant_id, common::ObIArray<share::ObTenantBackupTaskInfo>& tenant_backup_tasks);
  int get_tenant_backup_task(const uint64_t tenant_id, const int64_t backup_set_id,
      common::ObIArray<share::ObTenantBackupTaskInfo>& tenant_backup_tasks);
  int get_tenant_validate_task(const int64_t job_id, const uint64_t tenant_id, const int64_t incarnation,
      const int64_t backup_set_id, share::ObTenantValidateTaskInfo& task_info);
  int get_tenant_validate_tasks(const int64_t job_id, common::ObIArray<share::ObTenantValidateTaskInfo>& task_infos);
  int insert_validate_task(const share::ObBackupValidateTaskInfo& task_info);
  int move_validate_task_to_his(const common::ObIArray<share::ObBackupValidateTenant>& validate_tenants,
      const share::ObBackupValidateTaskInfo& task_info);
  int do_insert_validate_task_his(const share::ObTenantValidateTaskInfo& task_info);
  int update_validate_task(
      const share::ObBackupValidateTaskInfo& src_info, const share::ObBackupValidateTaskInfo& dst_info);

private:
  bool is_inited_;
  bool is_working_;
  bool is_prepare_flag_;

  share::ObBackupDest backup_dest_;

  common::ObServerConfig* config_;
  common::ObMySQLProxy* sql_proxy_;
  ObRootBalancer* root_balancer_;
  ObServerManager* server_mgr_;
  ObRebalanceTaskMgr* rebalance_mgr_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  share::ObBackupValidateTaskUpdater task_updater_;
  storage::ObBackupMetaIndexStore meta_index_store_;
  ObPartitionValidate pg_validate_;
  share::ObIBackupLeaseService* backup_lease_service_;
  mutable ObRootValidateIdling idling_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRootValidate);
};

class ObTenantValidate {
public:
  ObTenantValidate();
  virtual ~ObTenantValidate();

  int init(const bool is_dropped, const int64_t job_id, const uint64_t tenant_id, const int64_t backup_set_id,
      ObRootValidate& root_validate, ObRootBalancer& root_balancer, ObRebalanceTaskMgr& task_mgr,
      ObServerManager& server_mgr, common::ObISQLClient& trans, obrpc::ObSrvRpcProxy& rpc_proxy,
      share::ObIBackupLeaseService& backup_lease_service);

public:
  int do_scheduler();

  int do_generate(const share::ObTenantValidateTaskInfo& task_info);
  int do_validate(const share::ObTenantValidateTaskInfo& task_info);
  int do_finish(const share::ObTenantValidateTaskInfo& task_info);
  int do_cancel(const share::ObTenantValidateTaskInfo& task_info);

  int get_backup_infos_from_his(const share::ObTenantValidateTaskInfo& task_info,
      common::ObIArray<share::ObTenantBackupTaskInfo>& backup_task_infos);
  int build_pg_validate_task_infos(const int64_t archive_round, const int64_t backup_set_id,
      const share::ObTenantValidateTaskInfo& task_info, const common::ObIArray<common::ObPartitionKey>& pkeys,
      common::ObIArray<share::ObPGValidateTaskInfo>& pg_task_infos);
  int get_tenant_validate_task_info(share::ObTenantValidateTaskInfo& task_info);
  int update_tenant_validate_task(
      const share::ObTenantValidateTaskInfo& src_info, const share::ObTenantValidateTaskInfo& dst_info);
  int get_finished_pg_validate_task(const share::ObTenantValidateTaskInfo& tenant_task_info,
      common::ObIArray<share::ObPGValidateTaskInfo>& pg_task_infos, bool& all_finished, bool& need_report);

  int check_doing_task_finished(const share::ObPGValidateTaskInfo& pg_task_info, bool& finished);
  int cancel_doing_pg_tasks(const share::ObTenantValidateTaskInfo& task_info);

  int fetch_all_pg_list(const share::ObClusterBackupDest& backup_dest, const int64_t log_archive_round,
      const int64_t full_backup_set_id, const int64_t inc_backup_set_id,
      const share::ObTenantValidateTaskInfo& tenant_task_info, common::ObIArray<common::ObPartitionKey>& normal_pkeys);

private:
  bool is_inited_;
  bool is_dropped_;  // this tenant has been dropped
  int64_t job_id_;
  uint64_t tenant_id_;
  int64_t backup_set_id_;
  ObRootValidate* root_validate_;
  ObRootBalancer* root_balancer_;
  ObRebalanceTaskMgr* rebalance_mgr_;
  ObServerManager* server_mgr_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  common::ObISQLClient* sql_proxy_;

  share::ObTenantValidateTaskUpdater tenant_task_updater_;
  share::ObPGValidateTaskUpdater pg_task_updater_;
  share::ObIBackupLeaseService* backup_lease_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantValidate);
};

}  // namespace rootserver
}  // namespace oceanbase

#endif
