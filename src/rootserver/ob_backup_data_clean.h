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

#ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_DATA_CLEAN_H_
#define OCEANBASE_ROOTSERVER_OB_BACKUP_DATA_CLEAN_H_

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
#include "share/backup/ob_tenant_backup_clean_info_updater.h"
#include "share/backup/ob_extern_backup_info_mgr.h"
#include "archive/ob_archive_path.h"
#include "backup/ob_tenant_backup_data_clean_mgr.h"

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

class ObBackupDataCleanIdling : public ObThreadIdling {
public:
  explicit ObBackupDataCleanIdling(volatile bool& stop) : ObThreadIdling(stop)
  {}
  virtual int64_t get_idle_interval_us();
};

class ObBackupDataClean : public ObRsReentrantThread {
public:
  ObBackupDataClean();
  virtual ~ObBackupDataClean();
  int init(share::schema::ObMultiVersionSchemaService& schema_service, ObMySQLProxy& sql_proxy,
      share::ObIBackupLeaseService& backup_lease_service);
  virtual void run3() override;
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }
  void stop();
  void wakeup();
  int idle() const;
  void update_prepare_flag(const bool is_prepare_flag);
  bool get_prepare_flag() const;
  int check_can_do_task();
  virtual bool is_working() const
  {
    return is_working_;
  }
  int start();
  share::ObIBackupLeaseService* get_backup_lease_service()
  {
    return backup_lease_service_;
  }

private:
  int get_need_clean_tenants(common::ObIArray<ObBackupDataCleanTenant>& clean_tenants);
  int get_server_clean_tenants(common::ObIArray<ObBackupDataCleanTenant>& clean_tenants);
  int get_extern_clean_tenants(hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant>& clean_tenants_map);
  int get_server_need_clean_info(const uint64_t tenant_id, bool& need_add);
  int get_all_tenant_ids(common::ObIArray<uint64_t>& tenant_ids);
  int get_tenant_backup_task_his_info(const share::ObBackupCleanInfo& clean_info, common::ObISQLClient& trans,
      common::ObIArray<share::ObTenantBackupTaskInfo>& tenant_infos);
  int get_tenant_backup_task_info(const share::ObBackupCleanInfo& clean_info, common::ObISQLClient& trans,
      common::ObIArray<share::ObTenantBackupTaskInfo>& tenant_infos);
  int inner_get_tenant_backup_task_his_info(const share::ObBackupCleanInfo& clean_info, common::ObISQLClient& trans,
      common::ObIArray<share::ObTenantBackupTaskInfo>& tenant_infos);
  int get_tenant_backup_backupset_task_his_info(const share::ObBackupCleanInfo& clean_info, common::ObISQLClient& trans,
      common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& tenant_infos);
  int convert_backup_backupset_task_to_backup_task(
      const common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& backup_backupset_tasks,
      common::ObIArray<share::ObTenantBackupTaskInfo>& backup_tasks);
  int get_log_archive_info(const uint64_t tenant_id, common::ObISQLClient& trans,
      common::ObIArray<share::ObLogArchiveBackupInfo>& log_archive_infos);
  int get_log_archive_history_info(const uint64_t tenant_id, common::ObISQLClient& trans,
      common::ObIArray<share::ObLogArchiveBackupInfo>& log_archive_infos);

  int get_backup_clean_tenant(const share::ObTenantBackupTaskInfo& task_info,
      hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant>& clean_tenants_map);
  int get_archive_clean_tenant(const share::ObLogArchiveBackupInfo& log_archive_info,
      hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant>& clean_tenants_map);

  int get_backup_clean_elements(const share::ObBackupCleanInfo& clean_info,
      const common::ObIArray<share::ObTenantBackupTaskInfo>& task_infos,
      const common::ObArray<share::ObLogArchiveBackupInfo>& log_archive_infos, ObBackupDataCleanTenant& clean_tenant);
  int do_tenant_backup_clean(const share::ObBackupCleanInfo& clean_info, ObBackupDataCleanTenant& clean_tenant);
  int do_with_finished_tenant_clean_task(const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants,
      const share::ObBackupCleanInfo& sys_clean_info, const ObBackupDataCleanTenant& sys_clean_tenant,
      const int32_t clean_result);
  int reset_backup_clean_infos(const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants,
      common::ObISQLClient& sys_tenant_trans);
  int insert_clean_infos_into_history(const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants,
      common::ObISQLClient& sys_tenant_trans);
  int check_all_tenant_clean_tasks_stopped(
      const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants, bool& is_all_tasks_stopped);

  int do_with_failed_tenant_clean_task(
      const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants, int32_t& clean_result);
  int check_tenant_backup_clean_task_failed(const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants,
      const share::ObBackupCleanInfo& sys_clean_info, common::ObISQLClient& sys_tenant_trans, int32_t& result);
  int update_tenant_backup_clean_task_failed(
      const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants,
      common::ObISQLClient& sys_tenant_trans, const int32_t result);

  int do_normal_tenant_backup_clean(
      const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant);
  int mark_backup_meta_data_deleted(
      const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant);
  int mark_inner_table_his_data_deleted(
      const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant);

  int mark_backup_task_his_data_deleted(const share::ObBackupCleanInfo& clean_info,
      const ObBackupDataCleanElement& clean_element, common::ObISQLClient& trans);
  int inner_mark_backup_task_his_data_deleted(const uint64_t tenant_id, const int64_t incarnation,
      const int64_t backup_set_id, const share::ObBackupDest& backup_dest, common::ObISQLClient& trans);
  int mark_log_archive_stauts_his_data_deleted(
      const share::ObBackupCleanInfo& clean_info, const int64_t clog_gc_snapshot, common::ObISQLClient& trans);

  int mark_extern_backup_infos_deleted(
      const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant);

  int mark_extern_backup_info_deleted(
      const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanElement& clean_element);
  int mark_extern_clog_info_deleted(const share::ObBackupCleanInfo& clean_info,
      const ObBackupDataCleanElement& clean_element, const int64_t clog_gc_snapshot);

  int delete_backup_data(const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant);
  int delete_tenant_backup_meta_data(
      const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant);
  int delete_backup_extern_infos(
      const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant);
  int delete_inner_table_his_data(const share::ObBackupCleanInfo& clean_info, common::ObISQLClient& trans);
  int delete_backup_extern_info(
      const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanElement& clean_element);

  int delete_extern_backup_info_deleted(
      const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanElement& clean_element);
  int delete_extern_clog_info_deleted(
      const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanElement& clean_element);
  int delete_marked_backup_task_his_data(const uint64_t tenant_id, const int64_t job_id, common::ObISQLClient& trans);
  int delete_marked_log_archive_status_his_data(const uint64_t tenant_id, common::ObISQLClient& trans);

  int update_clean_info(const uint64_t tenant_id, const share::ObBackupCleanInfo& src_clean_info,
      const share::ObBackupCleanInfo& dest_clean_info);
  int update_clean_info(const uint64_t tenant_id, const share::ObBackupCleanInfo& src_clean_info,
      const share::ObBackupCleanInfo& dest_clean_info, common::ObISQLClient& trans);
  int do_with_status(const share::ObBackupCleanInfo& clean_info, ObBackupDataCleanTenant& clean_tenant);

  int do_clean_scheduler(common::ObIArray<ObBackupDataCleanTenant>& clean_tenants);
  int do_schedule_clean_tenants(common::ObIArray<ObBackupDataCleanTenant>& clean_tenants);
  int do_check_clean_tenants_finished(const common::ObIArray<ObBackupDataCleanTenant>& clean_tenants);

  int do_tenant_clean_scheduler(const hash::ObHashSet<ObClusterBackupDest>& cluster_backup_dest_set,
      share::ObBackupCleanInfo& clean_info, ObBackupDataCleanTenant& clean_tenant);
  int get_backup_clean_info(
      const uint64_t tenant_id, common::ObISQLClient& sql_proxy, share::ObBackupCleanInfo& clean_info);
  int get_backup_clean_info(const uint64_t tenant_id, share::ObBackupCleanInfo& clean_info);

  void cleanup_prepared_infos();
  int check_need_cleanup_prepared_infos(const share::ObBackupCleanInfo& sys_backup_info, bool& need_clean);
  int cleanup_tenant_prepared_infos(const uint64_t tenant_id, common::ObISQLClient& sys_tenant_trans);
  int prepare_tenant_backup_clean();
  int mark_sys_tenant_backup_meta_data_deleted();
  int schedule_tenants_backup_data_clean(const common::ObIArray<uint64_t>& tenant_ids);
  int schedule_tenant_backup_data_clean(
      const uint64_t tenant_id, const share::ObBackupCleanInfo& sys_clean_info, ObISQLClient& sys_tenant_trans);
  int insert_tenant_backup_clean_info_history(
      const ObSimpleBackupDataCleanTenant& clean_tenant, common::ObISQLClient& sys_trans);
  int update_clog_gc_snaphost(const int64_t cluster_clog_gc_snapshot, share::ObBackupCleanInfo& clean_info,
      ObBackupDataCleanTenant& clean_tenant);
  int get_clog_gc_snapshot(
      const ObBackupDataCleanTenant& clean_tenant, const int64_t cluster_clog_gc_snapshot, int64_t& clog_gc_snapshot);
  int get_deleted_clean_tenants(common::ObIArray<ObSimpleBackupDataCleanTenant>& deleted_tenants);
  int get_deleted_tenant_clean_infos(
      ObISQLClient& trans, common::ObIArray<share::ObBackupCleanInfo>& deleted_tenant_clean_infos);
  int schedule_deleted_clean_tenants(const common::ObIArray<ObSimpleBackupDataCleanTenant>& deleted_clean_tenants);
  int get_clean_tenants(const common::ObIArray<ObBackupDataCleanTenant>& clean_tenants,
      common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants, ObBackupDataCleanTenant& sys_clean_tenant);
  int delete_backup_clean_info(
      const uint64_t tenant_id, const share::ObBackupCleanInfo& clean_info, ObISQLClient& trans);
  int set_sys_clean_info_stop(const share::ObBackupCleanInfo& backup_clean_info, ObISQLClient& trans);
  int try_clean_tenant_backup_dir(const ObBackupDataCleanTenant& clean_tenant);
  int clean_tenant_backup_dir(const uint64_t tenant_id, const ObBackupDataCleanElement& clean_element);

  int clean_backup_tenant_info(const ObBackupDataCleanTenant& sys_clean_tenant,
      const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants);
  int inner_clean_backup_tenant_info(const ObBackupDataCleanElement& clean_element,
      const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants);
  int delete_cluster_backup_meta_data(const share::ObBackupCleanInfo& clean_info,
      const ObBackupDataCleanTenant& clean_tenant,
      const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants);
  int touch_extern_tenant_name(const ObBackupDataCleanTenant& clean_tenant);
  int touch_extern_clog_info(const ObBackupDataCleanTenant& clean_tenant);
  int add_log_archive_infos(
      const common::ObIArray<share::ObLogArchiveBackupInfo>& log_archive_infos, ObBackupDataCleanTenant& clean_tenant);
  int add_delete_backup_set(const share::ObBackupCleanInfo& clean_info,
      const common::ObIArray<share::ObTenantBackupTaskInfo>& task_infos,
      const common::ObArray<share::ObLogArchiveBackupInfo>& log_archive_infos, ObBackupDataCleanTenant& clean_tenant,
      share::ObTenantBackupTaskInfo& min_include_task_info);
  int add_expired_backup_set(const share::ObBackupCleanInfo& clean_info,
      const common::ObIArray<share::ObTenantBackupTaskInfo>& task_infos,
      const common::ObArray<share::ObLogArchiveBackupInfo>& log_archive_infos, ObBackupDataCleanTenant& clean_tenant,
      share::ObTenantBackupTaskInfo& min_include_task_info);
  int update_normal_tenant_clean_result(const share::ObBackupCleanInfo& clean_info,
      const ObBackupDataCleanTenant& clean_tenant, const int32_t clean_result);
  int get_cluster_max_succeed_backup_set(int64_t& backup_set_id);
  int get_log_archive_info(const int64_t backup_snapshot_version,
      const common::ObArray<share::ObLogArchiveBackupInfo>& log_archive_infos,
      share::ObLogArchiveBackupInfo& log_archvie_info);
  int check_backupset_continue_with_clog_data(const share::ObTenantBackupTaskInfo& backup_task_info,
      const common::ObArray<share::ObLogArchiveBackupInfo>& log_archive_infos, bool& is_continue);
  int do_tenant_cancel_delete_backup(
      const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant);
  int do_sys_tenant_cancel_delete_backup(const share::ObBackupCleanInfo& clean_info);
  int do_normal_tenant_cancel_delete_backup(
      const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant);
  int set_tenant_clean_info_cancel(const share::ObBackupCleanInfo& clean_info);
  int set_normal_tenant_cancel(const uint64_t tenant_id, common::ObISQLClient& sys_tenant_trans);
  int set_deleted_tenant_cancel(const ObBackupCleanInfo& clean_info, common::ObISQLClient& sys_tenant_trans);
  int get_sys_tenant_backup_dest(common::hash::ObHashSet<ObClusterBackupDest>& cluster_backup_dest_set);
  int do_scheduler_normal_tenant(share::ObBackupCleanInfo& clean_info, ObBackupDataCleanTenant& clean_tenant,
      common::ObIArray<ObTenantBackupTaskInfo>& task_infos,
      common::ObIArray<ObLogArchiveBackupInfo>& log_archive_infos);
  int do_scheduler_deleted_tenant(const common::hash::ObHashSet<ObClusterBackupDest>& cluster_backup_dest_set,
      share::ObBackupCleanInfo& clean_info, ObBackupDataCleanTenant& clean_tenant,
      common::ObIArray<ObTenantBackupTaskInfo>& task_infos,
      common::ObIArray<ObLogArchiveBackupInfo>& log_archive_infos);
  int do_inner_scheduler_delete_tenant(const ObClusterBackupDest& cluster_backup_dest,
      ObBackupDataCleanTenant& clean_tenant, common::ObIArray<ObTenantBackupTaskInfo>& task_infos,
      common::ObIArray<ObLogArchiveBackupInfo>& log_archive_infos);
  void set_inner_error(const int32_t result);
  bool is_result_need_retry(const int32_t result);

private:
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
  struct CompareBackupTaskInfo {
    bool operator()(const share::ObTenantBackupTaskInfo& lhs, const share::ObTenantBackupTaskInfo& rhs) const
    {
      return lhs.backup_set_id_ < rhs.backup_set_id_;
    }
  };

private:
  static const int64_t MAX_BUCKET_NUM = 1024;
  bool is_inited_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  common::ObMySQLProxy* sql_proxy_;
  mutable ObBackupDataCleanIdling idling_;
  bool is_prepare_flag_;
  int32_t inner_error_;
  bool is_working_;
  share::ObIBackupLeaseService* backup_lease_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataClean);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_BACKUP_DATA_CLEAN_H_
