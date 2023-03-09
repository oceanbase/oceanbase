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
#include "ob_i_backup_scheduler.h"
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
  explicit ObBackupDataCleanIdling(volatile bool &stop) : ObThreadIdling(stop)
  {}
  virtual int64_t get_idle_interval_us();
};

class ObBackupDataClean : public ObIBackupScheduler {
public:
  ObBackupDataClean();
  virtual ~ObBackupDataClean();
  int init(share::schema::ObMultiVersionSchemaService &schema_service, ObMySQLProxy &sql_proxy,
      share::ObIBackupLeaseService &backup_lease_service);
  virtual void run3() override;
  virtual int blocking_run() override
  {
    BLOCKING_RUN_IMPLEMENT();
  }
  void stop() override;
  void wakeup();
  int idle() const;
  void update_prepare_flag(const bool is_prepare_flag);
  bool get_prepare_flag() const;
  int check_can_do_task();
  virtual bool is_working() const
  {
    return is_working_;
  }
  virtual int force_cancel(const uint64_t tenant_id);
  int start();

  share::ObIBackupLeaseService *get_backup_lease_service()
  {
    return backup_lease_service_;
  }
  bool is_update_reserved_backup_timestamp() const
  {
    return is_update_reserved_backup_timestamp_;
  }
  share::ObBackupDest &get_backup_dest()
  {
    return backup_dest_;
  }
  share::ObBackupDest &get_backup_backup_dest()
  {
    return backup_backup_dest_;
  }
  const hash::ObHashSet<ObSimplePieceKey> &get_sys_tenant_deleted_backup_piece()
  {
    return sys_tenant_deleted_backup_piece_;
  }
  const hash::ObHashSet<ObSimpleArchiveRound> &get_sys_tenant_deleted_backup_round()
  {
    return sys_tenant_deleted_backup_round_;
  }

private:
  int get_need_clean_tenants(common::ObIArray<ObBackupDataCleanTenant> &clean_tenants);
  int get_server_clean_tenants(common::ObIArray<ObBackupDataCleanTenant> &clean_tenants);
  int get_server_need_clean_info(const uint64_t tenant_id, bool &need_add);
  int get_all_tenant_ids(common::ObIArray<uint64_t> &tenant_ids);
  int get_backup_clean_elements(const share::ObBackupCleanInfo &clean_info,
      const common::ObIArray<share::ObTenantBackupTaskInfo> &task_infos,
      const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos, const bool is_prepare_stage,
      ObBackupDataCleanTenant &clean_tenant);
  int do_tenant_backup_clean(const share::ObBackupCleanInfo &clean_info, ObBackupDataCleanTenant &clean_tenant);
  int do_with_finished_tenant_clean_task(const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants,
      const share::ObBackupCleanInfo &sys_clean_info, const ObBackupDataCleanTenant &sys_clean_tenant,
      const int32_t clean_result);
  int reset_backup_clean_infos(const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants,
      common::ObISQLClient &sys_tenant_trans);
  int insert_clean_infos_into_history(const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants,
      common::ObISQLClient &sys_tenant_trans);
  int check_all_tenant_clean_tasks_stopped(
      const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants, bool &is_all_tasks_stopped);

  int do_with_failed_tenant_clean_task(
      const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants, int32_t &clean_result);
  int check_tenant_backup_clean_task_failed(const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants,
      const share::ObBackupCleanInfo &sys_clean_info, common::ObISQLClient &sys_tenant_trans, int32_t &result);
  int update_tenant_backup_clean_task_failed(
      const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants,
      common::ObISQLClient &sys_tenant_trans, const int32_t result);

  int do_normal_tenant_backup_clean(
      const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant);
  int mark_backup_meta_data_deleting(
      const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant);

  // for new interface
  int mark_backup_set_infos_deleting(
      const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant);
  int mark_backup_set_info_deleting(
      const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanElement &clean_element);
  int mark_backup_set_info_inner_table_deleting(const share::ObBackupCleanInfo &clean_info,
      const ObBackupDataCleanElement &clean_element, const common::ObIArray<ObBackupSetId> &backup_set_ids);
  int mark_extern_backup_set_info_deleting(const share::ObBackupCleanInfo &clean_info,
      const ObBackupDataCleanElement &clean_element, const common::ObIArray<ObBackupSetId> &backup_set_ids);
  int mark_log_archive_infos_deleting(
      const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant);
  int mark_log_archive_info_deleting(
      const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanElement &clean_element);
  int mark_log_archive_info_inner_table_deleting(const share::ObBackupCleanInfo &clean_info,
      const common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys,
      const common::ObIArray<ObLogArchiveRound> &log_archive_rounds);
  int mark_extern_log_archive_info_deleting(const share::ObBackupCleanInfo &clean_info,
      const ObBackupDataCleanElement &clean_element, const common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys,
      const common::ObIArray<ObLogArchiveRound> &log_archive_rounds);
  int get_need_delete_backup_set_ids(
      const ObBackupDataCleanElement &clean_element, common::ObIArray<ObBackupSetId> &backup_set_ids);
  int get_need_delete_clog_round_and_piece(const share::ObBackupCleanInfo &clean_info,
      const ObBackupDataCleanElement &clean_element, common::ObIArray<ObLogArchiveRound> &log_archive_rounds,
      common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys);

  int delete_backup_data(const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant);
  int get_sys_tenant_delete_clog_round_and_piece(const share::ObBackupCleanInfo &clean_info,
      const ObBackupDataCleanElement &clean_element, common::ObIArray<ObLogArchiveRound> &log_archive_rounds,
      common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys);
  int get_normal_tenant_delete_clog_round_and_piece(const share::ObBackupCleanInfo &clean_info,
      const ObBackupDataCleanElement &clean_element, common::ObIArray<ObLogArchiveRound> &log_archive_rounds,
      common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys);
  int get_sys_tenant_prepare_clog_round_and_piece(
      const share::ObBackupCleanInfo &clean_info,
      const ObBackupDataCleanElement &clean_element,
      common::ObIArray<ObLogArchiveRound> &log_archive_rounds,
      common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys);
  int get_sys_tenant_prepare_clog_round(
      const share::ObBackupCleanInfo &clean_info,
      const ObLogArchiveRound &log_archive_round,
      const ObBackupDataCleanElement &clean_element,
      bool &is_delete_inorder,
      common::ObIArray<ObLogArchiveRound> &log_archive_rounds);
  int get_sys_tenant_prepare_clog_piece(
      const share::ObBackupCleanInfo &clean_info,
      const ObLogArchiveRound &log_archive_round,
      const ObBackupDataCleanElement &clean_element,
      bool &is_delete_inorder,
      common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys);
  int get_sys_tenant_doing_clog_round_and_piece(
      const share::ObBackupCleanInfo &clean_info,
      const ObBackupDataCleanElement &clean_element,
      common::ObIArray<ObLogArchiveRound> &log_archive_rounds,
      common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys);
  int get_tenant_delete_piece(const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanElement &clean_element,
      common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys);

  int delete_tenant_backup_meta_data(
      const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant);
  int delete_backup_extern_infos(
      const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant);
  int delete_inner_table_his_data(const share::ObBackupCleanInfo &clean_info,
      const ObBackupDataCleanTenant &clean_tenant, common::ObISQLClient &trans);
  int delete_extern_backup_info_deleted(const share::ObBackupCleanInfo &clean_info,
      const ObBackupDataCleanElement &clean_element, const common::ObIArray<ObBackupSetId> &backup_set_ids);
  int delete_extern_clog_info_deleted(const share::ObBackupCleanInfo &clean_info,
      const ObBackupDataCleanElement &clean_element, const common::ObIArray<ObLogArchiveRound> &log_archive_rounds);
  int delete_marked_backup_task_his_data(const share::ObBackupCleanInfo &clean_info,
      const ObBackupDataCleanElement &clean_element, common::ObISQLClient &trans);
  int delete_marked_log_archive_status_his_data(const share::ObBackupCleanInfo &clean_info,
      const ObBackupDataCleanElement &clean_element, common::ObISQLClient &trans);
  // new interface
  int delete_extern_tmp_files(
      const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanElement &clean_element);
  int mark_extern_backup_set_file_info_deleted(const share::ObBackupCleanInfo &clean_info,
      const ObBackupDataCleanElement &clean_element, const common::ObIArray<ObBackupSetId> &backup_set_ids);
  int mark_extern_backup_piece_file_info_deleted(const share::ObBackupCleanInfo &clean_info,
      const ObBackupDataCleanElement &clean_element, const common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys);

  int update_clean_info(const uint64_t tenant_id, const share::ObBackupCleanInfo &src_clean_info,
      const share::ObBackupCleanInfo &dest_clean_info);
  int update_clean_info(const uint64_t tenant_id, const share::ObBackupCleanInfo &src_clean_info,
      const share::ObBackupCleanInfo &dest_clean_info, common::ObISQLClient &trans);
  int do_with_status(const share::ObBackupCleanInfo &clean_info, ObBackupDataCleanTenant &clean_tenant);
  int do_clean_scheduler(common::ObIArray<ObBackupDataCleanTenant> &clean_tenants);
  int do_schedule_clean_tenants(common::ObIArray<ObBackupDataCleanTenant> &clean_tenants);
  int do_check_clean_tenants_finished(const common::ObIArray<ObBackupDataCleanTenant> &clean_tenants);
  int do_tenant_clean_scheduler(share::ObBackupCleanInfo &clean_info, ObBackupDataCleanTenant &clean_tenant);
  int get_backup_clean_info(const uint64_t tenant_id, const bool for_update, common::ObISQLClient &sql_proxy,
      share::ObBackupCleanInfo &clean_info);
  int get_backup_clean_info(
      const uint64_t tenant_id, common::ObISQLClient &sql_proxy, share::ObBackupCleanInfo &clean_info);
  int get_backup_clean_info(const uint64_t tenant_id, share::ObBackupCleanInfo &clean_info);
  int get_source_backup_set_file_info(const uint64_t tenant_id, const int64_t incarnation,
      const ObBackupSetId &backup_set_id, ObBackupSetFileInfo &backup_set_file_info, bool &is_need_modify);
  int get_source_backup_dest_from_piece_file(const common::ObIArray<ObBackupPieceInfoKey> &piece_keys,
      ObClusterBackupDest &cluster_backup_dest, bool &is_need_modify);
  int mark_extern_source_backup_set_info_of_backup_backup(const uint64_t tenant_id, const int64_t incarnation,
      const ObBackupSetFileInfo &backup_set_file_info, const ObArray<ObBackupSetIdPair> &backup_set_id_pairs,
      const bool is_deleting);

  void cleanup_prepared_infos();
  int check_need_cleanup_prepared_infos(const share::ObBackupCleanInfo &sys_backup_info, bool &need_clean);
  int cleanup_tenant_prepared_infos(const uint64_t tenant_id, common::ObISQLClient &sys_tenant_trans);
  int prepare_tenant_backup_clean();
  int mark_sys_tenant_backup_meta_data_deleting();
  int schedule_tenants_backup_data_clean(const common::ObIArray<uint64_t> &tenant_ids);
  int schedule_tenant_backup_data_clean(
      const uint64_t tenant_id, const share::ObBackupCleanInfo &sys_clean_info, ObISQLClient &sys_tenant_trans);
  int insert_tenant_backup_clean_info_history(
      const ObSimpleBackupDataCleanTenant &clean_tenant, common::ObISQLClient &sys_trans);
  int update_clog_gc_snaphost(const int64_t cluster_clog_gc_snapshot, share::ObBackupCleanInfo &clean_info,
      ObBackupDataCleanTenant &clean_tenant);
  int get_clog_gc_snapshot(const ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant,
      const int64_t cluster_clog_gc_snapshot, int64_t &clog_gc_snapshot);
  int get_deleted_clean_tenants(common::ObIArray<ObSimpleBackupDataCleanTenant> &deleted_tenants);
  int get_deleted_tenant_clean_infos(
      ObISQLClient &trans, common::ObIArray<share::ObBackupCleanInfo> &deleted_tenant_clean_infos);
  int schedule_deleted_clean_tenants(const common::ObIArray<ObSimpleBackupDataCleanTenant> &deleted_clean_tenants);
  int get_clean_tenants(const common::ObIArray<ObBackupDataCleanTenant> &clean_tenants,
      common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants, ObBackupDataCleanTenant &sys_clean_tenant);
  int delete_backup_clean_info(
      const uint64_t tenant_id, const share::ObBackupCleanInfo &clean_info, ObISQLClient &trans);
  int set_sys_clean_info_stop(const share::ObBackupCleanInfo &backup_clean_info, ObISQLClient &trans);
  int try_clean_tenant_backup_dir(const ObBackupDataCleanTenant &clean_tenant);
  int clean_tenant_backup_dir(const uint64_t tenant_id, const ObBackupDataCleanElement &clean_element);
  int clean_backup_tenant_info(const ObBackupDataCleanTenant &sys_clean_tenant,
      const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants);
  int inner_clean_backup_tenant_info(const ObBackupDataCleanElement &clean_element,
      const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants);
  int delete_cluster_backup_meta_data(const share::ObBackupCleanInfo &clean_info,
      const ObBackupDataCleanTenant &clean_tenant,
      const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants);
  int touch_extern_tenant_name(const ObBackupDataCleanTenant &clean_tenant);
  int touch_extern_clog_info(const ObBackupDataCleanTenant &clean_tenant);
  void set_inner_error(const int32_t result);
  int update_sys_clean_info();
  int add_log_archive_infos(
      const common::ObIArray<ObLogArchiveBackupInfo> &log_archive_infos, ObBackupDataCleanTenant &clean_tenant);
  int add_log_archive_info(const ObLogArchiveBackupInfo &log_archive_info, ObBackupDataCleanTenant &clean_tenant);
  int add_delete_backup_set(const share::ObBackupCleanInfo &clean_info,
      const common::ObIArray<share::ObTenantBackupTaskInfo> &task_infos,
      const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos, const bool is_prepare_stage,
      ObBackupDataCleanTenant &clean_tenant, share::ObTenantBackupTaskInfo &min_include_task_info);
  int add_obsolete_backup_sets(const share::ObBackupCleanInfo &clean_info,
      const common::ObIArray<share::ObTenantBackupTaskInfo> &task_infos,
      const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos, const bool is_prepare_stage,
      ObBackupDataCleanTenant &clean_tenant, share::ObTenantBackupTaskInfo &min_include_task_info);
  int add_obsolete_backup_sets_in_doing(const share::ObBackupCleanInfo &clean_info,
      const common::ObIArray<share::ObTenantBackupTaskInfo> &task_infos,
      const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos, ObBackupDataCleanTenant &clean_tenant,
      share::ObTenantBackupTaskInfo &min_include_task_info);
  int add_obsolete_backup_sets_in_prepare(const share::ObBackupCleanInfo &clean_info,
      const common::ObIArray<share::ObTenantBackupTaskInfo> &task_infos,
      const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos, ObBackupDataCleanTenant &clean_tenant,
      share::ObTenantBackupTaskInfo &min_include_task_info);
  int deal_with_obsolete_backup_set(const share::ObBackupCleanInfo &clean_info, const ObTenantBackupTaskInfo &task_info,
      const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos, const int64_t cluster_max_backup_set_id,
      ObBackupSetId &backup_set_id, bool &has_kept_last_succeed_data,
      share::ObTenantBackupTaskInfo &clog_data_clean_point);
  int deal_with_effective_backup_set(const share::ObBackupCleanInfo &clean_info,
      const ObTenantBackupTaskInfo &task_info, const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos,
      ObBackupSetId &backup_set_id, bool &has_kept_last_succeed_data,
      share::ObTenantBackupTaskInfo &clog_data_clean_point);
  int add_obsolete_backup_set_with_order(const share::ObBackupCleanInfo &clean_info,
      const common::ObIArray<share::ObTenantBackupTaskInfo> &reverse_task_infos,
      const common::ObIArray<ObBackupSetId> &reverse_backup_set_ids, ObBackupDataCleanTenant &clean_tenant,
      share::ObTenantBackupTaskInfo &clog_data_clean_point);

  int add_delete_backup_piece(const share::ObBackupCleanInfo &clean_info,
      const common::ObIArray<share::ObTenantBackupTaskInfo> &task_infos,
      const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos, ObBackupDataCleanTenant &clean_tenant);
  int add_delete_backup_round(const share::ObBackupCleanInfo &clean_info,
      const common::ObIArray<share::ObTenantBackupTaskInfo> &task_infos,
      const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos, ObBackupDataCleanTenant &clean_tenant);

  int update_normal_tenant_clean_result(const share::ObBackupCleanInfo &clean_info,
      const ObBackupDataCleanTenant &clean_tenant, const int32_t clean_result);
  int get_cluster_max_succeed_backup_set(const int64_t copy_id, int64_t &backup_set_id);
  int inner_get_cluster_max_succeed_backup_set(int64_t &backup_set_id);
  int inner_get_cluster_max_succeed_backup_backup_set(const int64_t copy_id, int64_t &backup_set_id);
  int get_log_archive_info(const int64_t backup_snapshot_version,
      const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos,
      share::ObLogArchiveBackupInfo &log_archvie_info);
  int check_backupset_continue_with_clog_data(const share::ObTenantBackupTaskInfo &backup_task_info,
      const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos, bool &is_continue);
  int do_tenant_cancel_delete_backup(
      const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant);
  int do_sys_tenant_cancel_delete_backup(const share::ObBackupCleanInfo &clean_info);
  int do_normal_tenant_cancel_delete_backup(
      const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant);
  int set_tenant_clean_info_cancel(const share::ObBackupCleanInfo &clean_info);
  int set_normal_tenant_cancel(const uint64_t tenant_id, common::ObISQLClient &sys_tenant_trans);
  int set_deleted_tenant_cancel(const ObBackupCleanInfo &clean_info, common::ObISQLClient &sys_tenant_trans);
  int do_scheduler_normal_tenant(share::ObBackupCleanInfo &clean_info, ObBackupDataCleanTenant &clean_tenant,
      common::ObIArray<ObTenantBackupTaskInfo> &task_infos,
      common::ObIArray<ObLogArchiveBackupInfo> &log_archive_infos);
  int get_all_tenant_backup_infos(const share::ObBackupCleanInfo &clean_info,
      const ObSimpleBackupDataCleanTenant &simple_clean_tenant, common::ObISQLClient &trans,
      common::ObIArray<ObTenantBackupTaskInfo> &tenant_backup_infos,
      common::ObIArray<ObLogArchiveBackupInfo> &tenant_backup_log_infos);
  int get_delete_backup_infos(const share::ObBackupCleanInfo &clean_info,
      const ObSimpleBackupDataCleanTenant &simple_clean_tenant, common::ObISQLClient &trans,
      common::ObIArray<share::ObTenantBackupTaskInfo> &tenant_backup_infos,
      common::ObIArray<share::ObLogArchiveBackupInfo> &tenant_backup_log_infos);
  int get_delete_obsolete_backup_set_infos(const share::ObBackupCleanInfo &clean_info,
      const ObSimpleBackupDataCleanTenant &simple_clean_tenant, common::ObISQLClient &trans,
      common::ObIArray<share::ObTenantBackupTaskInfo> &tenant_backup_infos,
      common::ObIArray<share::ObLogArchiveBackupInfo> &tenant_backup_log_infos);
  int get_delete_obsolete_backup_backupset_infos(const share::ObBackupCleanInfo &clean_info,
      const ObSimpleBackupDataCleanTenant &simple_clean_tenant, common::ObISQLClient &trans,
      common::ObIArray<share::ObTenantBackupTaskInfo> &tenant_backup_infos,
      common::ObIArray<share::ObLogArchiveBackupInfo> &tenant_backup_log_infos);
  int get_tenant_backup_task_infos(const share::ObBackupCleanInfo &clean_info,
      const ObSimpleBackupDataCleanTenant &simple_clean_tenant, common::ObISQLClient &trans,
      common::ObIArray<share::ObTenantBackupTaskInfo> &tenant_infos);
  int get_tenant_backup_backupset_task_infos(const share::ObBackupCleanInfo &clean_info,
      const ObSimpleBackupDataCleanTenant &simple_clean_tenant, common::ObISQLClient &trans,
      common::ObIArray<share::ObTenantBackupTaskInfo> &tenant_infos);
  int get_tenant_backup_log_infos(const share::ObBackupCleanInfo &clean_info,
      const ObSimpleBackupDataCleanTenant &simple_clean_tenant, common::ObISQLClient &trans,
      common::ObIArray<ObLogArchiveBackupInfo> &tenant_backup_log_infos);
  int get_tenant_backup_backuplog_infos(const share::ObBackupCleanInfo &clean_info,
      const ObSimpleBackupDataCleanTenant &simple_clean_tenant, common::ObISQLClient &trans,
      common::ObIArray<ObLogArchiveBackupInfo> &tenant_backup_log_infos);
  int get_backup_dest_option(const ObBackupDest &backup_dest, ObBackupDestOpt &backup_dest_option);
  int get_clean_tenants_from_history_table(
      const ObBackupCleanInfo &clean_info, hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant> &clean_tenants_map);
  int get_delete_backup_set_tenants_from_history_table(
      const ObBackupCleanInfo &clean_info, hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant> &clean_tenants_map);
  int get_delete_backup_piece_tenants_from_history_table(
      const ObBackupCleanInfo &clean_info, hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant> &clean_tenants_map);
  int get_delete_backup_round_tenants_from_history_table(
      const ObBackupCleanInfo &clean_info, hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant> &clean_tenants_map);
  int get_delete_obsolete_backup_tenants_from_history_table(
      const ObBackupCleanInfo &clean_info, hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant> &clean_tenants_map);
  int set_history_tenant_info_into_map(const common::ObIArray<uint64_t> &tenant_ids,
      hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant> &clean_tenants_map);
  int set_current_backup_dest();
  int check_backup_dest_lifecycle(const ObBackupDataCleanTenant &clean_tenant);

  bool is_result_need_retry(const int32_t result);
  int check_inner_table_version_();
  int commit_trans(ObMySQLTransaction &trans);
  int start_trans(ObTimeoutCtx &timeout_ctx, ObMySQLTransaction &trans);
  int check_can_delete_extern_info_file(const uint64_t tenant_id, const ObClusterBackupDest &current_backup_dest,
      const bool is_backup_backup, const ObBackupPath &path, bool &can_delete_file);
  int get_backup_set_file_copies_num(const ObTenantBackupTaskInfo &task_info, int64_t &copies_num);
  int get_backup_piece_file_copies_num(const ObBackupPieceInfo &backup_piece_info, int64_t &copies_num);
  int get_backup_round_copies_num(const ObLogArchiveBackupInfo &archive_backup_info, int64_t &copies_num);
  int prepare_deleted_tenant_backup_infos();
  int add_backup_infos_for_compatible(const ObClusterBackupDest &cluster_backup_dest,
      const common::ObIArray<ObSimpleBackupDataCleanTenant> &simple_tenants);
  int add_backup_infos_for_compatible_(const ObClusterBackupDest &cluster_backup_dest,
      const ObSimpleBackupDataCleanTenant &simple_tenant, hash::ObHashMap<int64_t, int64_t> &min_backup_set_log_ts);

  int add_log_archive_infos_for_compatible(const ObClusterBackupDest &cluster_backup_dest,
      const common::ObIArray<ObSimpleBackupDataCleanTenant> &simple_tenants);
  int add_log_archive_infos_for_compatible_(
      const ObClusterBackupDest &cluster_backup_dest, const ObSimpleBackupDataCleanTenant &simple_tenant);
  int get_backup_task_info_from_extern_info(const uint64_t tenant_id, const ObClusterBackupDest &cluster_backup_dest,
      const ObExternBackupInfo &extern_backup_info, ObTenantBackupTaskInfo &backup_task_info);
  int upgrade_backup_info();
  int add_deleting_backup_set_id_into_set(const uint64_t tenant_id, const ObBackupSetId &backup_set_id);
  int check_backup_set_id_can_be_deleted(
      const uint64_t tenant_id, const ObBackupSetId &backup_set_id, bool &can_deleted);
  int remove_delete_expired_data_snapshot_(const ObSimpleBackupDataCleanTenant &simple_tenant);
  int set_comment(ObBackupCleanInfo::Comment &comment);
  int set_error_msg(const int32_t result, ObBackupCleanInfo::ErrorMsg &error_msg);
  int prepare_delete_backup_set(const ObBackupCleanInfo &sys_clean_info);
  int prepare_delete_backup_piece_and_round(const ObBackupCleanInfo &sys_clean_info);
  int duplicate_task_info(common::ObIArray<share::ObTenantBackupTaskInfo> &task_infos);
  int check_clog_data_point(
      const ObBackupCleanInfo &sys_clean_info, const ObBackupDataCleanTenant &normal_clean_tenant);

private:
  struct CompareLogArchiveBackupInfo {
    bool operator()(const share::ObLogArchiveBackupInfo &lhs, const share::ObLogArchiveBackupInfo &rhs) const
    {
      return lhs.status_.start_ts_ < rhs.status_.start_ts_;
    }
  };
  struct CompareLogArchiveSnapshotVersion {
    bool operator()(const share::ObLogArchiveBackupInfo &log_info, const int64_t snapshot_version) const
    {
      return log_info.status_.start_ts_ < snapshot_version;
    }
  };
  struct CompareBackupTaskInfo {
    bool operator()(const share::ObTenantBackupTaskInfo &lhs, const share::ObTenantBackupTaskInfo &rhs) const
    {
      return lhs.backup_set_id_ < rhs.backup_set_id_;
    }
  };

private:
  static const int64_t MAX_BUCKET_NUM = 1024;
  bool is_inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy *sql_proxy_;
  mutable ObBackupDataCleanIdling idling_;
  bool is_prepare_flag_;
  int32_t inner_error_;
  bool is_working_;
  int64_t reserve_min_backup_set_id_;
  share::ObIBackupLeaseService *backup_lease_service_;
  ObBackupDest backup_dest_;
  ObBackupDestOpt backup_dest_option_;
  ObBackupDest backup_backup_dest_;
  ObBackupDestOpt backup_backup_dest_option_;
  // not use anymore
  bool is_update_reserved_backup_timestamp_;
  share::ObBackupInnerTableVersion inner_table_version_;
  hash::ObHashSet<int64_t> sys_tenant_deleted_backup_set_;
  int64_t retry_count_;
  hash::ObHashSet<ObSimpleArchiveRound> sys_tenant_deleted_backup_round_;
  hash::ObHashSet<ObSimplePieceKey> sys_tenant_deleted_backup_piece_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataClean);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_BACKUP_DATA_CLEAN_H_
