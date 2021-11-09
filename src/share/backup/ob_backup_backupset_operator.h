// Copyright 2020 Alibaba Inc. All Rights Reserved
// Author:
//     yanfeng <yangyi.yyy@alibaba-inc.com>
// Normalizer:
//     yanfeng <yangyi.yyy@alibaba-inc.com>

#ifndef OCEANBASE_SHARE_BACKUP_OB_BACKUP_BACKUPSET_OPERATOR_H_
#define OCEANBASE_SHARE_BACKUP_OB_BACKUP_BACKUPSET_OPERATOR_H_

#include "share/ob_dml_sql_splicer.h"
#include "share/backup/ob_backup_struct.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase {
namespace share {

class ObIBackupBackupsetOperator {
public:
  ObIBackupBackupsetOperator() = default;
  virtual ~ObIBackupBackupsetOperator() = default;
  static int fill_one_item(const ObBackupBackupsetJobItem& item, ObDMLSqlSplicer& splicer);
  static int get_task_items(const common::ObSqlString& sql, common::ObIArray<ObBackupBackupsetJobItem>& item_list,
      common::ObISQLClient& proxy);
  static int extract_task_item(sqlclient::ObMySQLResult* result, ObBackupBackupsetJobItem& item);
};

class ObBackupBackupsetOperator {
public:
  ObBackupBackupsetOperator() = default;
  virtual ~ObBackupBackupsetOperator() = default;
  static int insert_job_item(const ObBackupBackupsetJobItem& item, common::ObISQLClient& proxy);
  static int report_job_item(const ObBackupBackupsetJobItem& item, common::ObISQLClient& proxy);
  static int remove_job_item(const int64_t job_id, common::ObISQLClient& proxy);
  static int get_task_item(const int64_t job_id, ObBackupBackupsetJobItem& item, common::ObISQLClient& proxy);
  static int get_all_task_items(common::ObIArray<ObBackupBackupsetJobItem>& item_list, common::ObISQLClient& proxy);
  static int get_one_task(common::ObIArray<ObBackupBackupsetJobItem>& item_list, common::ObISQLClient& proxy);
};

class ObBackupBackupsetHistoryOperator {
public:
  ObBackupBackupsetHistoryOperator() = default;
  virtual ~ObBackupBackupsetHistoryOperator() = default;
  static int insert_job_item(const ObBackupBackupsetJobItem& item, common::ObISQLClient& proxy);
  static int report_job_item(const ObBackupBackupsetJobItem& item, common::ObISQLClient& proxy);
  static int remove_job_item(const int64_t job_id, common::ObISQLClient& proxy);
  static int get_task_item(const int64_t job_id, ObBackupBackupsetJobItem& item, common::ObISQLClient& proxy);
  static int get_all_task_items(common::ObIArray<ObBackupBackupsetJobItem>& item_list, common::ObISQLClient& proxy);
};

class ObITenantBackupBackupsetOperator {
public:
  ObITenantBackupBackupsetOperator() = default;
  virtual ~ObITenantBackupBackupsetOperator() = default;
  static int fill_one_item(const ObTenantBackupBackupsetTaskItem& item, ObDMLSqlSplicer& splicer);
  static int fill_one_item(
      const ObTenantBackupBackupsetTaskItem& item, const bool need_fill_is_mark_deleted, ObDMLSqlSplicer& splicer);
  static int get_task_items(const share::SimpleBackupBackupsetTenant& tenant, const common::ObSqlString& sql,
      const bool need_get_is_mark_deleted, common::ObIArray<ObTenantBackupBackupsetTaskItem>& item_list,
      common::ObISQLClient& sql_proxy);
  static int extract_task_item(
      const bool need_extract_is_mark_deleted, sqlclient::ObMySQLResult* result, ObTenantBackupBackupsetTaskItem& item);
  static int fill_one_item(
      const ObTenantBackupTaskInfo& task_info, const bool need_fill_is_mark_deleted, ObDMLSqlSplicer& splicer);
};

class ObTenantBackupBackupsetOperator {
public:
  ObTenantBackupBackupsetOperator() = default;
  virtual ~ObTenantBackupBackupsetOperator() = default;
  static int get_job_task_count(
      const ObBackupBackupsetJobInfo& job_info, common::ObISQLClient& sql_proxy, int64_t& task_count);
  static int insert_task_item(const share::SimpleBackupBackupsetTenant& tenant,
      const ObTenantBackupBackupsetTaskItem& item, common::ObISQLClient& proxy);
  static int report_task_item(const share::SimpleBackupBackupsetTenant& tenant,
      const ObTenantBackupBackupsetTaskItem& item, common::ObISQLClient& proxy);
  static int remove_task_item(const share::SimpleBackupBackupsetTenant& tenant, const int64_t job_id,
      const int64_t backup_set_id, common::ObISQLClient& proxy);
  static int get_task_item(const share::SimpleBackupBackupsetTenant& tenant, const int64_t job_id,
      const int64_t backup_set_id, ObTenantBackupBackupsetTaskItem& item, common::ObISQLClient& proxy);
  static int get_task_items(const share::SimpleBackupBackupsetTenant& tenant, const int64_t job_id,
      common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy);
  static int get_sys_unfinished_task_items(
      const int64_t job_id, common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy);
  static int get_unfinished_task_items(const share::SimpleBackupBackupsetTenant& tenant, const int64_t job_id,
      common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy);
  static int get_task_items(const share::SimpleBackupBackupsetTenant& tenant,
      common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy);

private:
  static const bool NEED_EXTRACT_IS_MARK_DELETED = false;
};

class ObTenantBackupBackupsetHistoryOperator {
public:
  ObTenantBackupBackupsetHistoryOperator() = default;
  virtual ~ObTenantBackupBackupsetHistoryOperator() = default;
  static int insert_task_item(const ObTenantBackupBackupsetTaskItem& item, common::ObISQLClient& proxy);
  static int report_task_item(const ObTenantBackupBackupsetTaskItem& item, common::ObISQLClient& proxy);
  static int get_task_item(const uint64_t tenant_id, const int64_t backup_set_id, const int64_t copy_id,
      const bool for_update, common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy);
  static int get_task_items(const uint64_t tenant_id, const int64_t copy_id,
      common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy);
  static int get_task_items_with_same_dest(const uint64_t tenant_id, const share::ObBackupDest& dest,
      common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy);
  static int get_full_task_items(const uint64_t tenant_id, const int64_t copy_id,
      common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy);
  static int get_need_mark_deleted_tasks_items(const uint64_t tenant_id, const int64_t copy_id,
      const int64_t backup_set_id, const share::ObBackupDest& backup_dest,
      common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy);
  static int get_marked_deleted_task_items(
      const uint64_t tenant_id, common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy);
  static int mark_task_item_deleted(const uint64_t tenant_id, const int64_t incarnation, const int64_t copy_id,
      const int64_t backup_set_id, common::ObISQLClient& proxy);
  static int delete_task_item(const ObTenantBackupBackupsetTaskItem& task_item, common::ObISQLClient& client);
  static int get_max_succeed_task(const uint64_t tenant_id, const int64_t copy_id,
      ObTenantBackupBackupsetTaskItem& item, common::ObISQLClient& client);
  // 支持换backup backup dest
  static int get_same_backup_set_id_tasks(const bool is_tenant_level, const uint64_t tenant_id,
      const int64_t backup_set_id, common::ObIArray<ObTenantBackupBackupsetTaskItem>& items,
      common::ObISQLClient& client);
  static int get_all_tasks_backup_set_id_smaller_then(const int64_t backup_set_id, const uint64_t tenant_id,
      common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& client);
  static int get_all_job_tasks(const int64_t job_id, const bool for_update,
      common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& client);
  static int get_all_sys_job_tasks(const int64_t job_id, const bool for_update,
      common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& client);

  // for backup data clean
  static int get_full_task_items(const uint64_t tenant_id, const bool for_update,
      common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy);
  static int get_task_items(
      const uint64_t tenant_id, common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy);
  static int get_tenant_ids_with_backup_set_id(const int64_t copy_id, const int64_t backup_set_id,
      common::ObIArray<uint64_t>& tenant_ids, common::ObISQLClient& proxy);
  static int get_tenant_ids_with_snapshot_version(
      const int64_t snapshot_version, common::ObIArray<uint64_t>& tenant_ids, common::ObISQLClient& proxy);
  static int update_backup_task_info(
      const ObTenantBackupTaskInfo& backup_task_info, const bool fill_mark_delete_item, common::ObISQLClient& proxy);
  static int delete_task_item(const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id,
      const int64_t copy_id, common::ObISQLClient& client);

private:
  static const bool NEED_EXTRACT_IS_MARK_DELETED = true;
};

class ObPGBackupBackupsetOperator {
public:
  ObPGBackupBackupsetOperator() = default;
  virtual ~ObPGBackupBackupsetOperator() = default;

  static int batch_report_task(const share::SimpleBackupBackupsetTenant& tenant,
      const common::ObIArray<ObPGBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy);
  static int get_pending_tasks(const share::SimpleBackupBackupsetTenant& tenant, const int64_t job_id,
      const int64_t backup_set_id, common::ObIArray<ObPGBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy);
  static int get_pending_tasks_with_limit(const share::SimpleBackupBackupsetTenant& tenant, const int64_t job_id,
      const int64_t backup_set_id, const int64_t limit, common::ObIArray<ObPGBackupBackupsetTaskItem>& items,
      common::ObISQLClient& proxy);
  static int get_finished_tasks(const share::SimpleBackupBackupsetTenant& tenant, const int64_t job_id,
      const int64_t backup_set_id, const int64_t copy_id, common::ObIArray<ObPGBackupBackupsetTaskItem>& items,
      common::ObISQLClient& proxy);
  static int get_finished_task_count(const share::SimpleBackupBackupsetTenant& tenant, const int64_t job_id,
      const int64_t backup_set_id, const int64_t copy_id, int64_t& finished_count, common::ObISQLClient& proxy);
  static int update_result_and_status(const int64_t job_id, const share::SimpleBackupBackupsetTenant& tenant,
      const int64_t backup_set_id, const int64_t copy_id, const common::ObPartitionKey& pg_key, const int32_t result,
      const ObPGBackupBackupsetTaskItem::TaskStatus& status, common::ObISQLClient& proxy);
  static int update_task_stat(const share::SimpleBackupBackupsetTenant& tenant,
      const ObPGBackupBackupsetTaskRowKey& row_key, const ObPGBackupBackupsetTaskStat& stat,
      common::ObISQLClient& proxy);
  static int batch_update_result_and_status(const share::SimpleBackupBackupsetTenant& tenant,
      const common::ObIArray<share::ObBackupBackupsetArg>& args, const common::ObIArray<int32_t>& results,
      const ObPGBackupBackupsetTaskItem::TaskStatus& status, common::ObISQLClient& proxy);
  static int get_doing_pg_tasks(const share::SimpleBackupBackupsetTenant& tenant, const int64_t job_id,
      const int64_t backup_set_id, common::ObIArray<ObPGBackupBackupsetTaskItem>& pg_tasks,
      common::ObISQLClient& proxy);
  static int get_failed_pg_tasks(const share::SimpleBackupBackupsetTenant& tenant, const int64_t job_id,
      const int64_t backup_set_id, common::ObIArray<ObPGBackupBackupsetTaskItem>& pg_tasks,
      common::ObISQLClient& proxy);
  static int get_pg_task(const share::SimpleBackupBackupsetTenant& tenant, const int64_t job_id,
      const int64_t backup_set_id, const uint64_t table_id, const int64_t partition_id,
      ObPGBackupBackupsetTaskItem& task, common::ObISQLClient& proxy);
  static int get_same_trace_id_tasks(const share::SimpleBackupBackupsetTenant& tenant,
      const ObPGBackupBackupsetTaskItem& task, common::ObIArray<ObPGBackupBackupsetTaskItem>& tasks,
      common::ObISQLClient& proxy);
  static int get_next_end_key_for_remove(const share::SimpleBackupBackupsetTenant& tenant,
      const ObPGBackupBackupsetTaskRowKey& prev_row_key, ObPGBackupBackupsetTaskRowKey& next_row_key,
      common::ObISQLClient& proxy);
  static int get_batch_end_key_for_remove(const share::SimpleBackupBackupsetTenant& tenant,
      common::ObIArray<ObPGBackupBackupsetTaskRowKey>& row_key_list, common::ObISQLClient& proxy);
  static int batch_remove_pg_tasks(const share::SimpleBackupBackupsetTenant& tenant,
      const ObPGBackupBackupsetTaskRowKey& left_row_key, const ObPGBackupBackupsetTaskRowKey& right_row_key,
      common::ObISQLClient& proxy);
  static int remove_all_pg_tasks(const share::SimpleBackupBackupsetTenant& tenant, common::ObISQLClient& proxy);

private:
  static const int64_t MAX_BATCH_COUNT = 128;
  static int fill_one_item(const ObPGBackupBackupsetTaskItem& item, ObDMLSqlSplicer& splicer);
  static int get_task_items(const share::SimpleBackupBackupsetTenant& tenant, const common::ObSqlString& sql,
      common::ObIArray<ObPGBackupBackupsetTaskItem>& item_list, common::ObISQLClient& sql_proxy);
  static int extract_task_item(sqlclient::ObMySQLResult* result, ObPGBackupBackupsetTaskItem& item);
};

}  // end namespace share
}  // end namespace oceanbase

#endif
