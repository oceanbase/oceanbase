// Copyright 2020 Alibaba Inc. All Rights Reserved
// Author:
//     yanfeng <yangyi.yyy@alibaba-inc.com>
// Normalizer:
//     yanfeng <yangyi.yyy@alibaba-inc.com>

#define USING_LOG_PREFIX SHARE
#include "share/backup/ob_backup_backupset_operator.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/backup/ob_backup_operator.h"

using namespace oceanbase::share;

namespace oceanbase {
namespace share {

/* ObIBackupBackupsetOperator */

int ObIBackupBackupsetOperator::fill_one_item(const ObBackupBackupsetJobItem& item, ObDMLSqlSplicer& splicer)
{
  int ret = OB_SUCCESS;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(item));
  } else if (OB_FAIL(item.backup_dest_.get_backup_dest_str(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(item));
  } else if (OB_FAIL(splicer.add_pk_column("tenant_id", item.tenant_id_)) ||
             OB_FAIL(splicer.add_pk_column("job_id", item.job_id_)) ||
             OB_FAIL(splicer.add_pk_column("incarnation", item.incarnation_)) ||
             OB_FAIL(splicer.add_pk_column("backup_set_id", item.backup_set_id_)) ||
             OB_FAIL(splicer.add_pk_column(
                 "copy_id", OB_START_COPY_ID))  // copy id of backup backupset job is not used any more
             || OB_FAIL(splicer.add_column("backup_backupset_type", item.type_.get_type_str())) ||
             OB_FAIL(splicer.add_column("tenant_name", item.tenant_name_.ptr())) ||
             OB_FAIL(splicer.add_column("status", ObBackupBackupsetJobItem::get_status_str(item.job_status_))) ||
             OB_FAIL(splicer.add_column("backup_dest", backup_dest_str)) ||
             OB_FAIL(splicer.add_column("max_backup_times", item.max_backup_times_)) ||
             OB_FAIL(splicer.add_column("result", item.result_)) ||
             OB_FAIL(splicer.add_column("comment", item.comment_.ptr()))) {
    LOG_WARN("failed to fill backup backupset job item", KR(ret), K(item));
  }
  return ret;
}

int ObIBackupBackupsetOperator::get_task_items(
    const common::ObSqlString& sql, common::ObIArray<ObBackupBackupsetJobItem>& item_list, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_UNLIKELY(!sql.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", KR(ret), K(sql));
    } else if (OB_FAIL(proxy.read(res, sql.ptr()))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", KR(ret));
    } else {
      while (OB_SUCC(ret)) {
        ObBackupBackupsetJobItem item;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get next row", KR(ret));
          }
        } else if (OB_FAIL(extract_task_item(result, item))) {
          LOG_WARN("failed to extract backup backupset job", KR(ret), K(item));
        } else if (OB_FAIL(item_list.push_back(item))) {
          LOG_WARN("failed to push back item", KR(ret), K(item));
        }
      }
    }
  }
  return ret;
}

int ObIBackupBackupsetOperator::extract_task_item(sqlclient::ObMySQLResult* result, ObBackupBackupsetJobItem& item)
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0;
  char backup_backupset_type_str[OB_INNER_TABLE_BACKUP_TYPE_LENTH + 1] = "";
  char tenant_name_str[OB_MAX_TENANT_NAME_LENGTH_STORE] = "";
  char status_str[OB_DEFAULT_STATUS_LENTH] = "";
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  char comment_str[MAX_TABLE_COMMENT_LENGTH] = "";
  item.reset();
  int64_t copy_id = 0;
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extract task item get invalid argument", KR(ret), KP(result));
  } else {
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", item.tenant_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "job_id", item.job_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "incarnation", item.incarnation_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "backup_set_id", item.backup_set_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "copy_id", copy_id, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result,
        "backup_backupset_type",
        backup_backupset_type_str,
        OB_INNER_TABLE_BACKUP_TYPE_LENTH + 1,
        tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(
        *result, "tenant_name", tenant_name_str, OB_MAX_TENANT_NAME_LENGTH_STORE, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "status", status_str, OB_DEFAULT_STATUS_LENTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "backup_dest", backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(*result, "max_backup_times", item.max_backup_times_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "result", item.result_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "comment", comment_str, MAX_TABLE_COMMENT_LENGTH, tmp_real_str_len);
    UNUSEDx(tmp_real_str_len, copy_id);

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(item.type_.set_type(backup_backupset_type_str))) {
      LOG_WARN("failed to set backup backupset type", KR(ret));
    } else if (OB_FAIL(item.set_status(status_str))) {
      LOG_WARN("failed to set status", KR(ret));
    } else if (OB_FAIL(item.tenant_name_.assign(tenant_name_str))) {
      LOG_WARN("failed to assign tenant name", KR(ret), K(tenant_name_str));
    } else if (OB_FAIL(item.backup_dest_.set(backup_dest_str))) {
      LOG_WARN("failed to set backup dest", KR(ret), K(backup_dest_str));
    } else if (OB_FAIL(item.comment_.assign(comment_str))) {
      LOG_WARN("failed to assign comment", KR(ret), K(comment_str));
    }
  }
  return ret;
}

/* ObBackupBackupsetOperator */
int ObBackupBackupsetOperator::insert_job_item(const ObBackupBackupsetJobItem& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObDMLSqlSplicer splicer;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObIBackupBackupsetOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", KR(ret));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column name", KR(ret));
    } else if (OB_FAIL(splicer.splice_insert_update_sql(OB_ALL_BACKUP_BACKUPSET_JOB_TNAME, sql))) {
      LOG_WARN("failed to splice insert update sql", KR(ret), K(item));
    } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObBackupBackupsetOperator::report_job_item(const ObBackupBackupsetJobItem& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer splicer;

  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObIBackupBackupsetOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", KR(ret), K(item));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", KR(ret));
    } else if (OB_FAIL(splicer.splice_update_sql(OB_ALL_BACKUP_BACKUPSET_JOB_TNAME, sql))) {
      LOG_WARN("failed to splice insert sql", KR(ret), K(item));
    } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret));
    } else if (1 != affected_rows && 0 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows));
    } else {
      LOG_INFO("report backup backupset job item", K(sql));
    }
  }
  return ret;
}

int ObBackupBackupsetOperator::remove_job_item(const int64_t job_id, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer splicer;
  ObDMLExecHelper exec(proxy, OB_SYS_TENANT_ID);
  if (OB_FAIL(splicer.add_pk_column("job_id", job_id))) {
    LOG_WARN("failed to add pk column", KR(ret), K(job_id));
  } else if (OB_FAIL(exec.exec_delete(OB_ALL_BACKUP_BACKUPSET_JOB_TNAME, splicer, affected_rows))) {
    LOG_WARN("failed to exec delete", KR(ret));
  }
  return ret;
}

int ObBackupBackupsetOperator::get_task_item(
    const int64_t job_id, ObBackupBackupsetJobItem& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  item.reset();
  ObArray<ObBackupBackupsetJobItem> items;

  if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup backupset job get invalid argument", KR(ret), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT * FROM %s WHERE job_id = %ld FOR UPDATE", OB_ALL_BACKUP_BACKUPSET_JOB_TNAME, job_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(job_id));
  } else if (OB_FAIL(ObIBackupBackupsetOperator::get_task_items(sql, items, proxy))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", KR(ret), K(items.count()));
  } else if (0 == items.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("backup backupset job do not exist", KR(ret), K(job_id));
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObBackupBackupsetOperator::get_all_task_items(
    common::ObIArray<ObBackupBackupsetJobItem>& item_list, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  item_list.reset();
  ObArray<ObBackupBackupsetJobItem> items;

  if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s FOR UPDATE", OB_ALL_BACKUP_BACKUPSET_JOB_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else if (OB_FAIL(ObIBackupBackupsetOperator::get_task_items(sql, items, proxy))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql));
  } else if (OB_FAIL(item_list.assign(items))) {
    LOG_WARN("failed to assign items to item list", KR(ret));
  }
  return ret;
}

int ObBackupBackupsetOperator::get_one_task(
    common::ObIArray<ObBackupBackupsetJobItem>& item_list, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  item_list.reset();
  ObArray<ObBackupBackupsetJobItem> items;

  if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s LIMIT 1", OB_ALL_BACKUP_BACKUPSET_JOB_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else if (OB_FAIL(ObIBackupBackupsetOperator::get_task_items(sql, items, proxy))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql));
  } else if (OB_FAIL(item_list.assign(items))) {
    LOG_WARN("failed to assign items to item list", KR(ret));
  }
  return ret;
}

/* ObBackupBackupsetHistoryOperator */

int ObBackupBackupsetHistoryOperator::insert_job_item(const ObBackupBackupsetJobItem& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObDMLSqlSplicer splicer;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObIBackupBackupsetOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", KR(ret));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column name", KR(ret));
    } else if (OB_FAIL(splicer.splice_insert_update_sql(OB_ALL_BACKUP_BACKUPSET_JOB_HISTORY_TNAME, sql))) {
      LOG_WARN("failed to splice insert update sql", KR(ret), K(item));
    } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObBackupBackupsetHistoryOperator::report_job_item(const ObBackupBackupsetJobItem& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer splicer;

  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObIBackupBackupsetOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", KR(ret), K(item));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", KR(ret));
    } else if (OB_FAIL(splicer.splice_update_sql(OB_ALL_BACKUP_BACKUPSET_JOB_HISTORY_TNAME, sql))) {
      LOG_WARN("failed to splice insert sql", KR(ret), K(item));
    } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret));
    } else if (1 != affected_rows && 0 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows));
    } else {
      LOG_INFO("report backup backupset job item", K(sql));
    }
  }
  return ret;
}

int ObBackupBackupsetHistoryOperator::remove_job_item(const int64_t job_id, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer splicer;
  ObDMLExecHelper exec(proxy, OB_SYS_TENANT_ID);
  if (OB_FAIL(splicer.add_pk_column("job_id", job_id))) {
    LOG_WARN("failed to add pk column", KR(ret), K(job_id));
  } else if (OB_FAIL(exec.exec_delete(OB_ALL_BACKUP_BACKUPSET_JOB_HISTORY_TNAME, splicer, affected_rows))) {
    LOG_WARN("failed to exec delete", KR(ret));
  }
  return ret;
}

int ObBackupBackupsetHistoryOperator::get_task_item(
    const int64_t job_id, ObBackupBackupsetJobItem& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  item.reset();
  ObArray<ObBackupBackupsetJobItem> items;

  if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup backupset job get invalid argument", KR(ret), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld FOR UPDATE",
                 OB_ALL_BACKUP_BACKUPSET_JOB_HISTORY_TNAME,
                 job_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(job_id));
  } else if (OB_FAIL(ObIBackupBackupsetOperator::get_task_items(sql, items, proxy))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", KR(ret), K(items.count()));
  } else if (0 == items.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("backup backupset job do not exist", KR(ret), K(job_id));
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObBackupBackupsetHistoryOperator::get_all_task_items(
    common::ObIArray<ObBackupBackupsetJobItem>& item_list, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  item_list.reset();
  ObArray<ObBackupBackupsetJobItem> items;

  if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s FOR UPDATE", OB_ALL_BACKUP_BACKUPSET_JOB_HISTORY_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else if (OB_FAIL(ObIBackupBackupsetOperator::get_task_items(sql, items, proxy))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql));
  } else if (OB_FAIL(item_list.assign(items))) {
    LOG_WARN("failed to assign items to item list", KR(ret));
  }
  return ret;
}

/* ObITenantBackupBackupsetOperator */

int ObITenantBackupBackupsetOperator::fill_one_item(
    const ObTenantBackupBackupsetTaskItem& item, ObDMLSqlSplicer& splicer)
{
  int ret = OB_SUCCESS;
  char src_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  char dst_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  char cluster_version_display_str[OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH] = "";
  const int64_t pos = ObClusterVersion::get_instance().print_vsn(
      cluster_version_display_str, OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH, item.cluster_version_);

  if (!item.is_valid() || pos <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(item), K(pos));
  } else if (OB_FAIL(item.src_backup_dest_.get_backup_dest_str(src_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get src backup dest str", KR(ret), K(item));
  } else if (OB_FAIL(item.dst_backup_dest_.get_backup_dest_str(dst_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get dst backup dest str", KR(ret), K(item));
  } else if (OB_FAIL(splicer.add_pk_column("tenant_id", item.tenant_id_)) ||
             OB_FAIL(splicer.add_pk_column("job_id", item.job_id_)) ||
             OB_FAIL(splicer.add_pk_column("incarnation", item.incarnation_)) ||
             OB_FAIL(splicer.add_pk_column("backup_set_id", item.backup_set_id_)) ||
             OB_FAIL(splicer.add_pk_column("copy_id", item.copy_id_)) ||
             OB_FAIL(splicer.add_column("backup_type", item.backup_type_.get_backup_type_str())) ||
             OB_FAIL(splicer.add_column("snapshot_version", item.snapshot_version_)) ||
             OB_FAIL(splicer.add_column("prev_full_backup_set_id", item.prev_full_backup_set_id_)) ||
             OB_FAIL(splicer.add_column("prev_inc_backup_set_id", item.prev_inc_backup_set_id_)) ||
             OB_FAIL(splicer.add_column("prev_backup_data_version", item.prev_backup_data_version_)) ||
             OB_FAIL(splicer.add_column("input_bytes", item.input_bytes_)) ||
             OB_FAIL(splicer.add_column("output_bytes", item.output_bytes_)) ||
             OB_FAIL(splicer.add_time_column("start_time", item.start_ts_)) ||
             OB_FAIL(splicer.add_time_column("end_time", item.end_ts_)) ||
             OB_FAIL(splicer.add_column("compatible", item.compatible_)) ||
             OB_FAIL(splicer.add_column("cluster_id", item.cluster_id_)) ||
             OB_FAIL(splicer.add_column("cluster_version", item.cluster_version_)) ||
             OB_FAIL(splicer.add_column("cluster_version_display", cluster_version_display_str)) ||
             OB_FAIL(
                 splicer.add_column("status", ObTenantBackupBackupsetTaskItem::get_status_str(item.task_status_))) ||
             OB_FAIL(splicer.add_column("src_backup_dest", src_backup_dest_str)) ||
             OB_FAIL(splicer.add_column("dst_backup_dest", dst_backup_dest_str)) ||
             OB_FAIL(splicer.add_column("src_device_type", item.src_backup_dest_.get_type_str())) ||
             OB_FAIL(splicer.add_column("dst_device_type", item.dst_backup_dest_.get_type_str())) ||
             OB_FAIL(splicer.add_column("backup_data_version", item.backup_data_version_)) ||
             OB_FAIL(splicer.add_column("backup_schema_version", item.backup_schema_version_)) ||
             OB_FAIL(splicer.add_column("total_pg_count", item.total_pg_count_)) ||
             OB_FAIL(splicer.add_column("finish_pg_count", item.finish_pg_count_)) ||
             OB_FAIL(splicer.add_column("total_partition_count", item.total_partition_count_)) ||
             OB_FAIL(splicer.add_column("finish_partition_count", item.finish_partition_count_)) ||
             OB_FAIL(splicer.add_column("total_macro_block_count", item.total_macro_block_count_)) ||
             OB_FAIL(splicer.add_column("finish_macro_block_count", item.finish_macro_block_count_)) ||
             OB_FAIL(splicer.add_column("result", item.result_)) ||
             OB_FAIL(splicer.add_column("encryption_mode", ObBackupEncryptionMode::to_str(item.encryption_mode_))) ||
             OB_FAIL(splicer.add_column("passwd", item.passwd_.ptr())) ||
             OB_FAIL(splicer.add_column("start_replay_log_ts", item.start_replay_log_ts_)) ||
             OB_FAIL(splicer.add_column("date", item.date_))) {
    LOG_WARN("failed to fill tenant backup backupset task item", KR(ret), K(item));
  }
  return ret;
}

int ObITenantBackupBackupsetOperator::fill_one_item(
    const ObTenantBackupBackupsetTaskItem& item, const bool need_fill_is_mark_deleted, ObDMLSqlSplicer& splicer)
{
  int ret = OB_SUCCESS;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(item));
  } else if (OB_FAIL(fill_one_item(item, splicer))) {
    LOG_WARN("failed to fill one base item", KR(ret), K(item));
  } else if (need_fill_is_mark_deleted && OB_FAIL(splicer.add_column("is_mark_deleted", false))) {
    LOG_WARN("failed to fill backup backupset task item", KR(ret), K(item));
  }
  return ret;
}

int ObITenantBackupBackupsetOperator::get_task_items(const share::SimpleBackupBackupsetTenant& tenant,
    const common::ObSqlString& sql, const bool need_get_is_mark_deleted,
    common::ObIArray<ObTenantBackupBackupsetTaskItem>& item_list, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  const uint64_t real_tenant_id = tenant.is_dropped_ ? OB_SYS_TENANT_ID : tenant.tenant_id_;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_UNLIKELY(!sql.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", KR(ret), K(sql));
    } else if (OB_FAIL(proxy.read(res, real_tenant_id, sql.ptr()))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", KR(ret));
    } else {
      while (OB_SUCC(ret)) {
        ObTenantBackupBackupsetTaskItem item;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get next row", KR(ret));
          }
        } else if (OB_FAIL(extract_task_item(need_get_is_mark_deleted, result, item))) {
          LOG_WARN("failed to extract tenant backup backupset task", KR(ret), K(item));
        } else if (OB_FAIL(item_list.push_back(item))) {
          LOG_WARN("failed to push back item", KR(ret), K(item));
        }
      }
    }
  }
  return ret;
}

int ObITenantBackupBackupsetOperator::extract_task_item(
    const bool need_extract_is_mark_deleted, sqlclient::ObMySQLResult* result, ObTenantBackupBackupsetTaskItem& item)
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0;
  item.reset();
  char status_str[OB_DEFAULT_STATUS_LENTH] = "";
  char backup_type_str[common::OB_INNER_TABLE_BACKUP_TYPE_LENTH + 1] = "";
  char src_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  char dst_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  char encryption_mode_str[OB_MAX_ENCRYPTION_MODE_LENGTH] = "";
  char passwd_str[OB_MAX_PASSWORD_LENGTH] = "";

  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extract task item get invalid argument", KR(ret), KP(result));
  } else {
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", item.tenant_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "job_id", item.job_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "incarnation", item.incarnation_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "backup_set_id", item.backup_set_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "copy_id", item.copy_id_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(
        *result, "backup_type", backup_type_str, OB_INNER_TABLE_BACKUP_TYPE_LENTH + 1, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(*result, "snapshot_version", item.snapshot_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "prev_full_backup_set_id", item.prev_full_backup_set_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "prev_inc_backup_set_id", item.prev_inc_backup_set_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "prev_backup_data_version", item.prev_backup_data_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "input_bytes", item.input_bytes_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "output_bytes", item.output_bytes_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "compatible", item.compatible_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "cluster_id", item.cluster_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "cluster_version", item.cluster_version_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "status", status_str, OB_DEFAULT_STATUS_LENTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(
        *result, "src_backup_dest", src_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(
        *result, "dst_backup_dest", dst_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(*result, "backup_data_version", item.backup_data_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "backup_schema_version", item.backup_schema_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "total_pg_count", item.total_pg_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_pg_count", item.finish_pg_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "total_partition_count", item.total_partition_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_partition_count", item.finish_partition_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "total_macro_block_count", item.total_macro_block_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_macro_block_count", item.finish_macro_block_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "result", item.result_, int32_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "start_replay_log_ts", item.start_replay_log_ts_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "date", item.date_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(
        *result, "encryption_mode", encryption_mode_str, OB_MAX_ENCRYPTION_MODE_LENGTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "passwd", passwd_str, OB_MAX_PASSWORD_LENGTH, tmp_real_str_len);
    if (need_extract_is_mark_deleted) {
      EXTRACT_BOOL_FIELD_MYSQL(*result, "is_mark_deleted", item.is_mark_deleted_);
    }
    UNUSED(tmp_real_str_len);

    if (OB_FAIL(ret)) {
      LOG_WARN("failed to extract task item", KR(ret));
    } else if (OB_FAIL(result->get_timestamp("start_time", NULL, item.start_ts_))) {
      LOG_WARN("failed to get start timestamp", KR(ret));
    } else if (OB_FAIL(result->get_timestamp("end_time", NULL, item.end_ts_))) {
      LOG_WARN("failed to get end timestamp", KR(ret));
    } else if (OB_FAIL(item.backup_type_.set_backup_type(backup_type_str))) {
      LOG_WARN("failed to set backup type", K(ret), K(backup_type_str), K(OB_INNER_TABLE_BACKUP_TYPE_LENTH));
    } else if (OB_FAIL(item.set_status(status_str))) {
      LOG_WARN("failed to set status for tenant backup backupset task item", KR(ret));
    } else if (OB_FAIL(item.src_backup_dest_.set(src_backup_dest_str))) {
      LOG_WARN("failed to set src backup dest", KR(ret), K(src_backup_dest_str));
    } else if (OB_FAIL(item.dst_backup_dest_.set(dst_backup_dest_str))) {
      LOG_WARN("failed to set dst backup dest", KR(ret), K(dst_backup_dest_str));
    } else if (FALSE_IT(item.encryption_mode_ = ObBackupEncryptionMode::parse_str(encryption_mode_str))) {
    } else if (!ObBackupEncryptionMode::is_valid(item.encryption_mode_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid encryption_mode", K(ret), K(encryption_mode_str), "mode", item.encryption_mode_);
    } else if (OB_FAIL(item.passwd_.assign(passwd_str))) {
      LOG_WARN("failed to assign passwd", K(ret), K(passwd_str));
    }
  }
  return ret;
}

int ObITenantBackupBackupsetOperator::fill_one_item(
    const ObTenantBackupTaskInfo& task_info, const bool need_fill_is_mark_deleted, ObDMLSqlSplicer& splicer)
{
  int ret = OB_SUCCESS;
  char dst_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  char cluster_version_display_str[OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH] = "";
  const int64_t pos = ObClusterVersion::get_instance().print_vsn(
      cluster_version_display_str, OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH, task_info.cluster_version_);

  if (!task_info.is_valid() || pos <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(task_info), K(pos));
  } else if (OB_FAIL(task_info.backup_dest_.get_backup_dest_str(dst_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get dst backup dest str", KR(ret), K(task_info));
  } else if (OB_FAIL(splicer.add_pk_column("tenant_id", task_info.tenant_id_)) ||
             OB_FAIL(splicer.add_pk_column("incarnation", task_info.incarnation_)) ||
             OB_FAIL(splicer.add_pk_column("backup_set_id", task_info.backup_set_id_)) ||
             OB_FAIL(splicer.add_pk_column("copy_id", task_info.copy_id_)) ||
             OB_FAIL(splicer.add_column("backup_type", task_info.backup_type_.get_backup_type_str())) ||
             OB_FAIL(splicer.add_column("snapshot_version", task_info.snapshot_version_)) ||
             OB_FAIL(splicer.add_column("prev_full_backup_set_id", task_info.prev_full_backup_set_id_)) ||
             OB_FAIL(splicer.add_column("prev_inc_backup_set_id", task_info.prev_inc_backup_set_id_)) ||
             OB_FAIL(splicer.add_column("prev_backup_data_version", task_info.prev_backup_data_version_)) ||
             OB_FAIL(splicer.add_column("input_bytes", task_info.input_bytes_)) ||
             OB_FAIL(splicer.add_column("output_bytes", task_info.output_bytes_)) ||
             OB_FAIL(splicer.add_time_column("start_time", task_info.start_time_)) ||
             OB_FAIL(splicer.add_time_column("end_time", task_info.end_time_)) ||
             OB_FAIL(splicer.add_column("compatible", task_info.compatible_)) ||
             OB_FAIL(splicer.add_column("cluster_id", task_info.cluster_id_)) ||
             OB_FAIL(splicer.add_column("cluster_version", task_info.cluster_version_)) ||
             OB_FAIL(splicer.add_column("cluster_version_display", cluster_version_display_str)) ||
             OB_FAIL(splicer.add_column("status", task_info.get_backup_task_status_str())) ||
             OB_FAIL(splicer.add_column("dst_backup_dest", dst_backup_dest_str)) ||
             OB_FAIL(splicer.add_column("dst_device_type", task_info.backup_dest_.get_type_str())) ||
             OB_FAIL(splicer.add_column("backup_data_version", task_info.backup_data_version_)) ||
             OB_FAIL(splicer.add_column("backup_schema_version", task_info.backup_schema_version_)) ||
             OB_FAIL(splicer.add_column("total_pg_count", task_info.pg_count_)) ||
             OB_FAIL(splicer.add_column("finish_pg_count", task_info.finish_pg_count_)) ||
             OB_FAIL(splicer.add_column("total_partition_count", task_info.partition_count_)) ||
             OB_FAIL(splicer.add_column("finish_partition_count", task_info.finish_partition_count_)) ||
             OB_FAIL(splicer.add_column("total_macro_block_count", task_info.macro_block_count_)) ||
             OB_FAIL(splicer.add_column("finish_macro_block_count", task_info.finish_macro_block_count_)) ||
             OB_FAIL(splicer.add_column("result", task_info.result_)) ||
             OB_FAIL(
                 splicer.add_column("encryption_mode", ObBackupEncryptionMode::to_str(task_info.encryption_mode_))) ||
             OB_FAIL(splicer.add_column("passwd", task_info.passwd_.ptr())) ||
             OB_FAIL(splicer.add_column("start_replay_log_ts", task_info.start_replay_log_ts_)) ||
             OB_FAIL(splicer.add_column("date", task_info.date_))) {
    LOG_WARN("failed to fill tenant backup backupset task item", KR(ret), K(task_info));
  } else {
    if (need_fill_is_mark_deleted && OB_FAIL(splicer.add_column("is_mark_deleted", task_info.is_mark_deleted_))) {
      LOG_WARN("failed to fill backup backupset task item", KR(ret), K(task_info));
    }
  }
  return ret;
}

/* ObTenantBackupBackupsetOperator */
int ObTenantBackupBackupsetOperator::get_job_task_count(
    const ObBackupBackupsetJobInfo& job_info, common::ObISQLClient& sql_proxy, int64_t& task_count)
{
  int ret = OB_SUCCESS;
  const int64_t job_id = job_info.job_id_;
  const uint64_t tenant_id = job_info.tenant_id_;
  task_count = 0;
  ObSqlString sql;
  sqlclient::ObMySQLResult* result = NULL;
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    if (OB_FAIL(sql.assign_fmt("select count(*) as count from %s where job_id = %ld",
            OB_ALL_TENANT_BACKUP_BACKUPSET_TASK_TNAME,
            job_id))) {
      LOG_WARN("failed to init backup info sql", KR(ret));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is NULL", KR(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("failed to get next result", KR(ret), K(sql));
    } else if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", KR(ret), K(sql));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "count", task_count, int64_t);
    }
  }
  return ret;
}

int ObTenantBackupBackupsetOperator::insert_task_item(const share::SimpleBackupBackupsetTenant& tenant,
    const ObTenantBackupBackupsetTaskItem& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer splicer;
  const uint64_t real_tenant_id = tenant.is_dropped_ ? OB_SYS_TENANT_ID : tenant.tenant_id_;

  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObITenantBackupBackupsetOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", KR(ret), K(item));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", KR(ret));
    } else if (OB_FAIL(splicer.splice_insert_update_sql(OB_ALL_TENANT_BACKUP_BACKUPSET_TASK_TNAME, sql))) {
      LOG_WARN("failed to splice insert sql", KR(ret), K(item));
    } else if (OB_FAIL(proxy.write(real_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    } else if (OB_FAIL(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows));
    } else {
      LOG_INFO("insert tenant backup backupset task item", K(sql));
    }
  }
  return ret;
}

int ObTenantBackupBackupsetOperator::report_task_item(const share::SimpleBackupBackupsetTenant& tenant,
    const ObTenantBackupBackupsetTaskItem& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer splicer;
  const uint64_t real_tenant_id = tenant.is_dropped_ ? OB_SYS_TENANT_ID : tenant.tenant_id_;

  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObITenantBackupBackupsetOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", KR(ret), K(item));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", KR(ret));
    } else if (OB_FAIL(splicer.splice_update_sql(OB_ALL_TENANT_BACKUP_BACKUPSET_TASK_TNAME, sql))) {
      LOG_WARN("failed to splice insert sql", KR(ret), K(item));
    } else if (OB_FAIL(proxy.write(real_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret));
    } else if (1 != affected_rows && 0 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows));
    } else {
      LOG_INFO("report tenant backup backupset task item", K(sql));
    }
  }
  return ret;
}

int ObTenantBackupBackupsetOperator::remove_task_item(const share::SimpleBackupBackupsetTenant& tenant,
    const int64_t job_id, const int64_t backup_set_id, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  const uint64_t real_tenant_id = tenant.is_dropped_ ? OB_SYS_TENANT_ID : tenant.tenant_id_;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer splicer;
  ObDMLExecHelper exec(proxy, real_tenant_id);
  if (OB_FAIL(splicer.add_pk_column("tenant_id", tenant.tenant_id_)) ||
      OB_FAIL(splicer.add_pk_column("job_id", job_id)) ||
      OB_FAIL(splicer.add_pk_column("backup_set_id", backup_set_id))) {
    LOG_WARN("failed to add pk column", KR(ret), K(tenant), K(job_id), K(backup_set_id));
  } else if (OB_FAIL(exec.exec_delete(OB_ALL_TENANT_BACKUP_BACKUPSET_TASK_TNAME, splicer, affected_rows))) {
    LOG_WARN("failed to exec delete", KR(ret));
  }
  return ret;
}

int ObTenantBackupBackupsetOperator::get_task_item(const share::SimpleBackupBackupsetTenant& tenant,
    const int64_t job_id, const int64_t backup_set_id, ObTenantBackupBackupsetTaskItem& item,
    common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  item.reset();
  ObArray<ObTenantBackupBackupsetTaskItem> items;

  if (!tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup backupset task get invalid argument", KR(ret), K(tenant));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT * FROM %s WHERE tenant_id = %lu AND job_id = %ld AND backup_set_id = %ld FOR UPDATE",
                 OB_ALL_TENANT_BACKUP_BACKUPSET_TASK_TNAME,
                 tenant.tenant_id_,
                 job_id,
                 backup_set_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant), K(job_id), K(backup_set_id));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 tenant, sql, NEED_EXTRACT_IS_MARK_DELETED, items, proxy))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql), K(tenant));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", KR(ret), K(items.count()));
  } else if (0 == items.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_INFO("tenant backup backupset task do not exist", KR(ret), K(tenant), K(job_id));
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObTenantBackupBackupsetOperator::get_task_items(const share::SimpleBackupBackupsetTenant& tenant,
    const int64_t job_id, common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  items.reset();

  if (!tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup backupset task get invalid argument", KR(ret), K(tenant));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND job_id = %ld FOR UPDATE",
                 OB_ALL_TENANT_BACKUP_BACKUPSET_TASK_TNAME,
                 tenant.tenant_id_,
                 job_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant), K(job_id));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 tenant, sql, NEED_EXTRACT_IS_MARK_DELETED, items, proxy))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql), K(tenant));
  }
  return ret;
}

int ObTenantBackupBackupsetOperator::get_sys_unfinished_task_items(
    const int64_t job_id, common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  items.reset();
  SimpleBackupBackupsetTenant sys_tenant;
  sys_tenant.tenant_id_ = OB_SYS_TENANT_ID;
  sys_tenant.is_dropped_ = false;
  if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld and status <> 'FINISH'",
          OB_ALL_TENANT_BACKUP_BACKUPSET_TASK_TNAME,
          job_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(job_id));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 sys_tenant, sql, NEED_EXTRACT_IS_MARK_DELETED, items, proxy))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql));
  }
  return ret;
}

int ObTenantBackupBackupsetOperator::get_unfinished_task_items(const share::SimpleBackupBackupsetTenant& tenant,
    const int64_t job_id, common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  items.reset();

  if (!tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup backupset task get invalid argument", KR(ret), K(tenant));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND job_id = %ld AND status <> 'FINISH'",
                 OB_ALL_TENANT_BACKUP_BACKUPSET_TASK_TNAME,
                 tenant.tenant_id_,
                 job_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant), K(job_id));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 tenant, sql, NEED_EXTRACT_IS_MARK_DELETED, items, proxy))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql), K(tenant));
  }
  return ret;
}

int ObTenantBackupBackupsetOperator::get_task_items(const share::SimpleBackupBackupsetTenant& tenant,
    common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  items.reset();

  if (!tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup backupset task get invalid argument", KR(ret), K(tenant));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu FOR UPDATE",
                 OB_ALL_TENANT_BACKUP_BACKUPSET_TASK_TNAME,
                 tenant.tenant_id_))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 tenant, sql, NEED_EXTRACT_IS_MARK_DELETED, items, proxy))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql), K(tenant));
  }
  return ret;
}

/* ObTenantBackupBackupsetHistoryOperator */

int ObTenantBackupBackupsetHistoryOperator::insert_task_item(
    const ObTenantBackupBackupsetTaskItem& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer splicer;
  const bool need_fill_is_mark_deleted = true;

  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObITenantBackupBackupsetOperator::fill_one_item(item, need_fill_is_mark_deleted, splicer))) {
      LOG_WARN("failed to fill one item", KR(ret), K(item));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", KR(ret));
    } else if (OB_FAIL(splicer.splice_insert_update_sql(OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME, sql))) {
      LOG_WARN("failed to splice insert sql", KR(ret), K(item));
    } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    } else {
      LOG_INFO("insert tenant backup backupset task item to history", K(sql));
    }
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::report_task_item(
    const ObTenantBackupBackupsetTaskItem& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer splicer;

  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObITenantBackupBackupsetOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", KR(ret), K(item));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", KR(ret));
    } else if (OB_FAIL(splicer.splice_update_sql(OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME, sql))) {
      LOG_WARN("failed to splice insert sql", KR(ret), K(item));
    } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret));
    } else if (1 != affected_rows && 0 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows));
    } else {
      LOG_INFO("report tenant backup backupset task item", K(sql));
    }
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::get_task_item(const uint64_t tenant_id, const int64_t backup_set_id,
    const int64_t copy_id, const bool for_update, common::ObIArray<ObTenantBackupBackupsetTaskItem>& items,
    common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  items.reset();
  SimpleBackupBackupsetTenant sys_tenant;
  sys_tenant.tenant_id_ = OB_SYS_TENANT_ID;
  sys_tenant.is_dropped_ = false;

  if (OB_INVALID_ID == tenant_id || backup_set_id <= 0 || 0 == copy_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "get tenant backup backupset task get invalid argument", KR(ret), K(tenant_id), K(backup_set_id), K(copy_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND backup_set_id = %ld AND copy_id = %ld",
                 OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME,
                 tenant_id,
                 backup_set_id,
                 copy_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(backup_set_id), K(copy_id));
  } else if (for_update && OB_FAIL(sql.append(" FOR UPDATE"))) {
    LOG_WARN("failed to append update sql", K(ret), K(sql));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 sys_tenant, sql, NEED_EXTRACT_IS_MARK_DELETED, items, proxy))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql), K(tenant_id));
  } else if (0 == items.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("tenant backup backupset task do not exist", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::get_task_items(const uint64_t tenant_id, const int64_t copy_id,
    common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  items.reset();
  ObSqlString sql;
  SimpleBackupBackupsetTenant sys_tenant;
  sys_tenant.tenant_id_ = OB_SYS_TENANT_ID;
  sys_tenant.is_dropped_ = false;

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup backupset task get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND copy_id = %ld FOR UPDATE",
                 OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME,
                 tenant_id,
                 copy_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(copy_id));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 sys_tenant, sql, NEED_EXTRACT_IS_MARK_DELETED, items, proxy))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql), K(tenant_id));
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::get_task_items_with_same_dest(const uint64_t tenant_id,
    const share::ObBackupDest& dest, common::ObIArray<ObTenantBackupBackupsetTaskItem>& items,
    common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  items.reset();
  ObSqlString sql;
  SimpleBackupBackupsetTenant sys_tenant;
  sys_tenant.tenant_id_ = OB_SYS_TENANT_ID;
  sys_tenant.is_dropped_ = false;
  char backup_dest_str[share::OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (OB_INVALID_ID == tenant_id || !dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id), K(dest));
  } else if (OB_FAIL(dest.get_backup_dest_str(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND dst_backup_dest = '%s'",
                 OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME,
                 tenant_id,
                 backup_dest_str))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(dest));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 sys_tenant, sql, NEED_EXTRACT_IS_MARK_DELETED, items, proxy))) {
    LOG_WARN("failed to get tenant backup backupset history task items", KR(ret), K(sql));
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::get_full_task_items(const uint64_t tenant_id, const int64_t copy_id,
    common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SimpleBackupBackupsetTenant sys_tenant;
  sys_tenant.tenant_id_ = OB_SYS_TENANT_ID;
  sys_tenant.is_dropped_ = false;
  items.reset();
  if (OB_INVALID_ID == tenant_id || copy_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get full task items get invalid argument", KR(ret), K(tenant_id), K(copy_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND"
                                    " copy_id = %ld AND backup_type = 'D' FOR UPDATE",
                 OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME,
                 tenant_id,
                 copy_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(copy_id));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 sys_tenant, sql, NEED_EXTRACT_IS_MARK_DELETED, items, proxy))) {
    LOG_WARN("failed to get full backup task items", KR(ret), K(sql));
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::get_need_mark_deleted_tasks_items(const uint64_t tenant_id,
    const int64_t copy_id, const int64_t backup_set_id, const share::ObBackupDest& backup_dest,
    common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SimpleBackupBackupsetTenant sys_tenant;
  sys_tenant.tenant_id_ = OB_SYS_TENANT_ID;
  sys_tenant.is_dropped_ = false;
  char backup_dest_str[share::OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (OB_INVALID_ID == tenant_id || copy_id < 0 || backup_set_id < 0 || !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get need mark deleted tasks get invalid argument",
        KR(ret),
        K(tenant_id),
        K(copy_id),
        K(backup_set_id),
        K(backup_dest));
  } else if (OB_FAIL(backup_dest.get_backup_dest_str(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(backup_dest));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT * FROM %s WHERE"
                 " tenant_id = %lu AND copy_id = %ld AND ( backup_set_id = %ld OR prev_full_backup_set_id = %ld )"
                 " AND dst_backup_dest = '%s' FOR UPDATE",
                 OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME,
                 tenant_id,
                 copy_id,
                 backup_set_id,
                 backup_set_id,
                 backup_dest_str))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(copy_id), K(backup_set_id), K(backup_dest));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 sys_tenant, sql, NEED_EXTRACT_IS_MARK_DELETED, items, proxy))) {
    LOG_WARN("failed to get tenant backup backupset history task items", KR(ret), K(sql));
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::get_marked_deleted_task_items(
    const uint64_t tenant_id, common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SimpleBackupBackupsetTenant sys_tenant;
  sys_tenant.tenant_id_ = OB_SYS_TENANT_ID;
  sys_tenant.is_dropped_ = false;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get marked deleted task items get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND is_mark_deleted = true",
                 OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME,
                 tenant_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 sys_tenant, sql, NEED_EXTRACT_IS_MARK_DELETED, items, proxy))) {
    LOG_WARN("failed to get tennat backup backupset history task items", KR(ret), K(sql));
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::mark_task_item_deleted(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t copy_id, const int64_t backup_set_id, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  if (OB_INVALID_ID == tenant_id || copy_id < 0 || backup_set_id < 0 || incarnation <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mark task item deleted get invalid argument",
        KR(ret),
        K(tenant_id),
        K(copy_id),
        K(backup_set_id),
        K(incarnation));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET is_mark_deleted = true WHERE"
                                    " tenant_id = %lu AND copy_id = %ld AND backup_set_id = %ld and incarnation = %ld",
                 OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME,
                 tenant_id,
                 copy_id,
                 backup_set_id,
                 incarnation))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(copy_id), K(backup_set_id));
  } else if (OB_FAIL(proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(sql));
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::delete_task_item(
    const ObTenantBackupBackupsetTaskItem& task_item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (!task_item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete task items get invalid argument", KR(ret), K(task_item));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND is_mark_deleted = true",
                 OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME,
                 task_item.tenant_id_))) {
    LOG_WARN("failed to assign sql", KR(ret), K(task_item));
  } else if (OB_FAIL(proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(sql));
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::get_max_succeed_task(const uint64_t tenant_id, const int64_t copy_id,
    ObTenantBackupBackupsetTaskItem& item, common::ObISQLClient& client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  item.reset();
  SimpleBackupBackupsetTenant sys_tenant;
  sys_tenant.tenant_id_ = OB_SYS_TENANT_ID;
  sys_tenant.is_dropped_ = false;
  const bool need_get_is_mark_deleted = false;
  ObArray<ObTenantBackupBackupsetTaskItem> items;

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get max succeed task invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu "
                                    " AND copy_id = %ld AND result = 0 ORDER BY backup_set_id DESC LIMIT 1",
                 OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME,
                 tenant_id,
                 copy_id))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 sys_tenant, sql, need_get_is_mark_deleted, items, client))) {
    LOG_WARN("fail to get pg backup task", K(ret), K(sql), K(tenant_id));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", K(ret), K(items.count()));
  } else if (0 == items.count()) {
    // do nothing
    ret = OB_INVALID_BACKUP_SET_ID;
    LOG_WARN("backup set do not exist", K(ret), K(tenant_id));
  } else {
    item = items.at(0);
  }

  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::get_same_backup_set_id_tasks(const bool is_tenant_level,
    const uint64_t tenant_id, const int64_t backup_set_id, common::ObIArray<ObTenantBackupBackupsetTaskItem>& items,
    common::ObISQLClient& client)
{
  int ret = OB_SUCCESS;
  items.reset();
  ObSqlString sql;
  const bool need_get_is_mark_deleted = false;
  SimpleBackupBackupsetTenant sys_tenant;
  sys_tenant.tenant_id_ = OB_SYS_TENANT_ID;
  sys_tenant.is_dropped_ = false;
  if (OB_INVALID_ID == tenant_id || backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id), K(backup_set_id));
  } else if (!is_tenant_level &&
             OB_FAIL(sql.assign_fmt(
                 "SELECT * FROM %s WHERE tenant_id = %lu AND backup_set_id = %ld AND copy_id < %ld ORDER BY copy_id",
                 OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME,
                 tenant_id,
                 backup_set_id,
                 OB_TENANT_GCONF_DEST_START_COPY_ID))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(backup_set_id));
  } else if (is_tenant_level &&
             OB_FAIL(sql.assign_fmt(
                 "SELECT * FROM %s WHERE tenant_id = %lu AND backup_set_id = %ld AND copy_id >= %ld ORDER BY copy_id",
                 OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME,
                 tenant_id,
                 backup_set_id,
                 OB_TENANT_GCONF_DEST_START_COPY_ID))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 sys_tenant, sql, need_get_is_mark_deleted, items, client))) {
    LOG_WARN("fail to get pg backup task", K(ret), K(sql), K(tenant_id));
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::get_all_tasks_backup_set_id_smaller_then(const int64_t backup_set_id,
    const uint64_t tenant_id, common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& client)
{
  int ret = OB_SUCCESS;
  items.reset();
  ObSqlString sql;
  const bool need_get_is_mark_deleted = false;
  SimpleBackupBackupsetTenant sys_tenant;
  sys_tenant.tenant_id_ = OB_SYS_TENANT_ID;
  sys_tenant.is_dropped_ = false;
  if (OB_INVALID_ID == tenant_id || backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND backup_set_id <= %ld",
                 OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME,
                 tenant_id,
                 backup_set_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 sys_tenant, sql, need_get_is_mark_deleted, items, client))) {
    LOG_WARN("fail to get pg backup task", K(ret), K(sql), K(tenant_id));
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::get_all_job_tasks(const int64_t job_id, const bool for_update,
    common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& client)
{
  int ret = OB_SUCCESS;
  items.reset();
  ObSqlString sql;
  const bool need_get_is_mark_deleted = false;
  SimpleBackupBackupsetTenant sys_tenant;
  sys_tenant.tenant_id_ = OB_SYS_TENANT_ID;
  sys_tenant.is_dropped_ = false;
  if (job_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT * FROM %s WHERE job_id = %ld", OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME, job_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sys_tenant), K(job_id));
  } else if (for_update && OB_FAIL(sql.append(" FOR UPDATE"))) {
    LOG_WARN("failed to append fmt", KR(ret), K(sql));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 sys_tenant, sql, need_get_is_mark_deleted, items, client))) {
    LOG_WARN("fail to get pg backup task", K(ret), K(sql));
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::get_all_sys_job_tasks(const int64_t job_id, const bool for_update,
    common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& client)
{
  int ret = OB_SUCCESS;
  items.reset();
  ObSqlString sql;
  const bool need_get_is_mark_deleted = false;
  SimpleBackupBackupsetTenant sys_tenant;
  sys_tenant.tenant_id_ = OB_SYS_TENANT_ID;
  sys_tenant.is_dropped_ = false;
  if (job_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND job_id = %ld",
                 OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME,
                 OB_SYS_TENANT_ID,
                 job_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sys_tenant), K(job_id));
  } else if (for_update && OB_FAIL(sql.append(" FOR UPDATE"))) {
    LOG_WARN("failed to append fmt", KR(ret), K(sql));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 sys_tenant, sql, need_get_is_mark_deleted, items, client))) {
    LOG_WARN("fail to get pg backup task", K(ret), K(sql));
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::get_full_task_items(const uint64_t tenant_id, const bool for_update,
    common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SimpleBackupBackupsetTenant sys_tenant;
  sys_tenant.tenant_id_ = OB_SYS_TENANT_ID;
  sys_tenant.is_dropped_ = false;
  items.reset();
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get full task items get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND backup_type = 'D'",
                 OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME,
                 tenant_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id));
  } else if (for_update && OB_FAIL(sql.append(" FOR UPDATE"))) {
    LOG_WARN("failed to apend sql", K(ret), K(sql));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 sys_tenant, sql, NEED_EXTRACT_IS_MARK_DELETED, items, proxy))) {
    LOG_WARN("failed to get full backup task items", KR(ret), K(sql));
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::get_task_items(
    const uint64_t tenant_id, common::ObIArray<ObTenantBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  items.reset();
  ObSqlString sql;
  SimpleBackupBackupsetTenant sys_tenant;
  sys_tenant.tenant_id_ = OB_SYS_TENANT_ID;
  sys_tenant.is_dropped_ = false;

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup backupset task get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT * FROM %s WHERE tenant_id = %lu", OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME, tenant_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::get_task_items(
                 sys_tenant, sql, NEED_EXTRACT_IS_MARK_DELETED, items, proxy))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql), K(tenant_id));
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::get_tenant_ids_with_backup_set_id(const int64_t copy_id,
    const int64_t backup_set_id, common::ObIArray<uint64_t>& tenant_ids, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  ObSqlString sql;
  const int64_t tenant_id = OB_SYS_TENANT_ID;

  if (copy_id <= 0 || backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get task items get invalid argument", KR(ret), K(copy_id), K(backup_set_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE backup_set_id = %ld and copy_id = %ld",
                 OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME,
                 backup_set_id,
                 copy_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(backup_set_id), K(copy_id));
  } else if (OB_FAIL(ObITenantBackupTaskOperator::get_tenant_ids(tenant_id, sql, tenant_ids, proxy))) {
    LOG_WARN("failed to get tenant ids", K(ret), K(sql));
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::get_tenant_ids_with_snapshot_version(
    const int64_t snapshot_version, common::ObIArray<uint64_t>& tenant_ids, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  ObSqlString sql;
  const int64_t tenant_id = OB_SYS_TENANT_ID;

  if (snapshot_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get task items get invalid argument", KR(ret), K(snapshot_version));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE snapshot_version <= %ld",
                 OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME,
                 snapshot_version))) {
    LOG_WARN("failed to assign sql", KR(ret), K(snapshot_version));
  } else if (OB_FAIL(ObITenantBackupTaskOperator::get_tenant_ids(tenant_id, sql, tenant_ids, proxy))) {
    LOG_WARN("failed to get tenant ids", K(ret), K(sql));
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::update_backup_task_info(
    const ObTenantBackupTaskInfo& backup_task_info, const bool fill_mark_delete_item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = OB_SYS_TENANT_ID;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;

  if (!backup_task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get task items get invalid argument", KR(ret), K(backup_task_info));
  } else if (OB_FAIL(ObITenantBackupBackupsetOperator::fill_one_item(backup_task_info, fill_mark_delete_item, dml))) {
    LOG_WARN("failed to fill task history item", K(ret), K(backup_task_info));
  } else if (OB_FAIL(dml.splice_column_names(columns))) {
    LOG_WARN("failed to splice column names", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME, sql))) {
    LOG_WARN("failed to splice insert sql", KR(ret), K(backup_task_info));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(backup_task_info));
  } else {
    LOG_INFO("update tenant backup task info success", K(sql));
  }
  return ret;
}

int ObTenantBackupBackupsetHistoryOperator::delete_task_item(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, const int64_t copy_id, common::ObISQLClient& client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (incarnation < 0 || backup_set_id <= 0 || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "delete task item get invalid argument", KR(ret), K(tenant_id), K(incarnation), K(backup_set_id), K(copy_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "DELETE FROM %s WHERE tenant_id = %lu AND incarnation = %ld AND backup_set_id = %ld AND copy_id = %ld",
                 OB_ALL_BACKUP_BACKUPSET_TASK_HISTORY_TNAME,
                 tenant_id,
                 incarnation,
                 backup_set_id,
                 copy_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(incarnation), K(backup_set_id), K(copy_id));
  } else if (OB_FAIL(client.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(sql));
  }
  return ret;
}

/* ObPGBackupBackupsetOperator */

int ObPGBackupBackupsetOperator::batch_report_task(const share::SimpleBackupBackupsetTenant& tenant,
    const common::ObIArray<ObPGBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer splicer;
  const int64_t BATCH_CNT = 512;
  if (OB_UNLIKELY(items.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(items));
  } else {
    int64_t report_idx = 0;
    const uint64_t tenant_id = tenant.is_dropped_ ? OB_SYS_TENANT_ID : tenant.tenant_id_;
    while (OB_SUCC(ret) && report_idx < items.count()) {
      sql.reuse();
      columns.reuse();
      const int64_t remain_cnt = items.count() - report_idx;
      int64_t cur_batch_cnt = remain_cnt < BATCH_CNT ? remain_cnt : BATCH_CNT;
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_batch_cnt; ++i) {
        const ObPGBackupBackupsetTaskItem& item = items.at(report_idx + i);
        splicer.reuse();
        if (OB_FAIL(fill_one_item(item, splicer))) {
          LOG_WARN("failed to fill one item", KR(ret), K(item));
        } else {
          if (0 == i) {
            if (OB_FAIL(splicer.splice_column_names(columns))) {
              LOG_WARN("failed to splice column names", KR(ret));
            } else if (OB_FAIL(sql.assign_fmt("INSERT /*+ use_plan_cache(none) */ INTO %s (%s) VALUES",
                           OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TNAME,
                           columns.ptr()))) {
              LOG_WARN("fail to assign sql string", KR(ret));
            }
          }

          if (OB_SUCC(ret)) {
            values.reset();
            if (OB_FAIL(splicer.splice_values(values))) {
              LOG_WARN("failed to splice values", KR(ret));
            } else if (OB_FAIL(sql.append_fmt("%s(%s)", 0 == i ? " " : " , ", values.ptr()))) {
              LOG_WARN("failed to assign sql string", KR(ret));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append(" ON DUPLICATE KEY UPDATE "))) {
          LOG_WARN("failed to append to sql", KR(ret), K(sql));
        } else if (OB_FAIL(sql.append("  status = values(status) ")) ||
                   OB_FAIL(sql.append(", trace_id = values(trace_id)")) ||
                   OB_FAIL(sql.append(", svr_ip = values(svr_ip)")) ||
                   OB_FAIL(sql.append(", svr_port = values(svr_port)")) ||
                   OB_FAIL(sql.append(", total_partition_count = values(total_partition_count)")) ||
                   OB_FAIL(sql.append(", finish_partition_count = values(finish_partition_count)")) ||
                   OB_FAIL(sql.append(", total_macro_block_count = values(total_macro_block_count)")) ||
                   OB_FAIL(sql.append(", finish_macro_block_count = values(finish_macro_block_count)")) ||
                   OB_FAIL(sql.append(", result = values(result)")) ||
                   OB_FAIL(sql.append(", comment = values(comment)"))) {
          LOG_WARN("failed to append to sql", KR(ret), K(sql));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t affected_rows = 0;
        if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("failed to execute sql", KR(ret));
        } else if (OB_UNLIKELY(affected_rows > 2 * cur_batch_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows), K(cur_batch_cnt));
        } else {
          report_idx += cur_batch_cnt;
          LOG_INFO("batch report pg task", K(sql));
        }
      }
    }
  }
  return ret;
}

int ObPGBackupBackupsetOperator::get_pending_tasks(const share::SimpleBackupBackupsetTenant& tenant,
    const int64_t job_id, const int64_t backup_set_id, common::ObIArray<ObPGBackupBackupsetTaskItem>& items,
    common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!tenant.is_valid() || job_id < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant), K(job_id), K(backup_set_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND job_id = %ld "
                                    "AND backup_set_id = %ld AND status = 'PENDING'",
                 OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TNAME,
                 tenant.tenant_id_,
                 job_id,
                 backup_set_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant), K(job_id), K(backup_set_id));
  } else if (OB_FAIL(get_task_items(tenant, sql, items, proxy))) {
    LOG_WARN("failed to get pg backup task", KR(ret), K(tenant), K(sql));
  }
  return ret;
}

int ObPGBackupBackupsetOperator::get_pending_tasks_with_limit(const share::SimpleBackupBackupsetTenant& tenant,
    const int64_t job_id, const int64_t backup_set_id, const int64_t limit,
    common::ObIArray<ObPGBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!tenant.is_valid() || job_id < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant), K(job_id), K(backup_set_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND job_id = %ld "
                                    "AND backup_set_id = %ld AND status = 'PENDING' LIMIT %ld",
                 OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TNAME,
                 tenant.tenant_id_,
                 job_id,
                 backup_set_id,
                 limit))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant), K(job_id), K(backup_set_id));
  } else if (OB_FAIL(get_task_items(tenant, sql, items, proxy))) {
    LOG_WARN("failed to get pg backup task", KR(ret), K(tenant), K(sql));
  }
  return ret;
}

int ObPGBackupBackupsetOperator::get_finished_tasks(const share::SimpleBackupBackupsetTenant& tenant,
    const int64_t job_id, const int64_t backup_set_id, const int64_t copy_id,
    common::ObIArray<ObPGBackupBackupsetTaskItem>& items, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!tenant.is_valid() || job_id < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant), K(job_id), K(backup_set_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND job_id = %ld "
                                    "AND backup_set_id = %ld AND copy_id = %ld AND status = 'FINISH'",
                 OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TNAME,
                 tenant.tenant_id_,
                 job_id,
                 backup_set_id,
                 copy_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant), K(job_id), K(backup_set_id), K(copy_id));
  } else if (OB_FAIL(get_task_items(tenant, sql, items, proxy))) {
    LOG_WARN("failed to get pg backup task", KR(ret), K(tenant), K(sql));
  }
  return ret;
}

int ObPGBackupBackupsetOperator::get_finished_task_count(const share::SimpleBackupBackupsetTenant& tenant,
    const int64_t job_id, const int64_t backup_set_id, const int64_t copy_id, int64_t& finished_count,
    common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArray<ObPGBackupBackupsetTaskItem> items;
  if (!tenant.is_valid() || job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND job_id = %ld "
                                    " AND backup_set_id = %ld AND copy_id = %ld AND status = 'FINISH' AND result = 0",
                 OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TNAME,
                 tenant.tenant_id_,
                 job_id,
                 backup_set_id,
                 copy_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant), K(job_id), K(backup_set_id), K(copy_id));
  } else if (OB_FAIL(get_task_items(tenant, sql, items, proxy))) {
    LOG_WARN("failed to get pg backup task", KR(ret), K(tenant), K(sql));
  } else {
    finished_count = items.count();
  }
  return ret;
}

int ObPGBackupBackupsetOperator::update_result_and_status(const int64_t job_id,
    const share::SimpleBackupBackupsetTenant& tenant, const int64_t backup_set_id, const int64_t copy_id,
    const common::ObPartitionKey& pg_key, const int32_t result, const ObPGBackupBackupsetTaskItem::TaskStatus& status,
    common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const uint64_t real_tenant_id = tenant.is_dropped_ ? OB_SYS_TENANT_ID : tenant.tenant_id_;
  if (!pg_key.is_valid() || status >= ObPGBackupBackupsetTaskInfo::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update result and status get invalid argument", KR(ret), K(pg_key));
  } else if (OB_FAIL(sql.append_fmt("UPDATE %s SET result = %d, status = '%s' "
                                    " WHERE job_id = %ld AND tenant_id = %lu AND incarnation = %ld AND backup_set_id = "
                                    "%ld AND copy_id = %ld "
                                    " AND table_id = %ld AND partition_id = %ld",
                 OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TNAME,
                 result,
                 ObPGBackupBackupsetTaskInfo::get_status_str(status),
                 job_id,
                 tenant.tenant_id_,
                 OB_START_INCARNATION,
                 backup_set_id,
                 copy_id,
                 pg_key.get_table_id(),
                 pg_key.get_partition_id()))) {
    LOG_WARN("failed to append to sql", KR(ret), K(sql));
  } else if (OB_FAIL(proxy.write(real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(tenant), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected rows unexpected", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObPGBackupBackupsetOperator::update_task_stat(const share::SimpleBackupBackupsetTenant& tenant,
    const ObPGBackupBackupsetTaskRowKey& row_key, const ObPGBackupBackupsetTaskStat& stat, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const uint64_t real_tenant_id = tenant.is_dropped_ ? OB_SYS_TENANT_ID : tenant.tenant_id_;
  if (!row_key.is_valid() || !stat.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update result and status get invalid argument", KR(ret), K(row_key));
  } else if (OB_FAIL(sql.append_fmt("UPDATE %s SET total_partition_count = %ld, finish_partition_count = %ld, "
                                    " total_macro_block_count = %ld, finish_macro_block_count = %ld "
                                    " WHERE job_id = %ld AND tenant_id = %lu AND backup_set_id = %ld AND copy_id = %ld "
                                    " AND incarnation = %ld AND table_id = %ld AND partition_id = %ld",
                 OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TNAME,
                 stat.total_partition_count_,
                 stat.finish_partition_count_,
                 stat.total_macro_block_count_,
                 stat.finish_macro_block_count_,
                 row_key.job_id_,
                 row_key.tenant_id_,
                 row_key.backup_set_id_,
                 row_key.copy_id_,
                 row_key.incarnation_,
                 row_key.table_id_,
                 row_key.partition_id_))) {
    LOG_WARN("failed to append to sql", KR(ret), K(sql));
  } else if (OB_FAIL(proxy.write(real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(tenant), K(sql));
  } else if (1 != affected_rows && 0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected rows unexpected", KR(ret), K(affected_rows), K(sql));
  }
  return ret;
}

int ObPGBackupBackupsetOperator::batch_update_result_and_status(const share::SimpleBackupBackupsetTenant& tenant,
    const common::ObIArray<share::ObBackupBackupsetArg>& args, const common::ObIArray<int32_t>& results,
    const ObPGBackupBackupsetTaskItem::TaskStatus& status, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  int64_t report_idx = 0;

  if ((args.count() != results.count()) || status >= ObPGBackupBackupsetTaskInfo::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update status and result get invalid argument", KR(ret), K(args.count()), K(results.count()), K(status));
  } else {
    while (OB_SUCC(ret) && report_idx < args.count()) {
      ObMySQLTransaction trans;
      const int64_t remain_cnt = args.count() - report_idx;
      int64_t cur_batch_cnt = remain_cnt < MAX_BATCH_COUNT ? remain_cnt : MAX_BATCH_COUNT;
      if (OB_FAIL(trans.start(&proxy))) {
        LOG_WARN("failed to start trans", KR(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < cur_batch_cnt; ++i) {
          const share::ObBackupBackupsetArg& arg = args.at(i + report_idx);
          const int64_t job_id = arg.job_id_;
          const int64_t backup_set_id = arg.backup_set_id_;
          const int64_t copy_id = arg.copy_id_;
          const ObPartitionKey& pg_key = arg.pg_key_;
          const int32_t result = results.at(i + report_idx);
          if (OB_FAIL(ObPGBackupBackupsetOperator::update_result_and_status(
                  job_id, tenant, backup_set_id, copy_id, pg_key, result, status, trans))) {
            LOG_WARN("failed to update result and status", KR(ret), K(arg), K(status));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true /*commit*/))) {
            OB_LOG(WARN, "failed to end transaction", KR(ret));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false /*not commit*/))) {
            OB_LOG(WARN, "failed to end transaction", KR(ret), K(tmp_ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        report_idx += cur_batch_cnt;
      }
    }
  }
  return ret;
}

int ObPGBackupBackupsetOperator::get_doing_pg_tasks(const share::SimpleBackupBackupsetTenant& tenant,
    const int64_t job_id, const int64_t backup_set_id, common::ObIArray<ObPGBackupBackupsetTaskItem>& pg_tasks,
    common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const int64_t MAX_GET_NUM = 1024;
  if (!tenant.is_valid() || job_id < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant), K(job_id), K(backup_set_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND job_id = %ld "
                                    "AND backup_set_id = %ld AND status = 'DOING' group by trace_id limit %ld",
                 OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TNAME,
                 tenant.tenant_id_,
                 job_id,
                 backup_set_id,
                 MAX_GET_NUM))) {
    LOG_WARN("failed to assign sql", K(ret), K(tenant), K(job_id), K(backup_set_id));
  } else if (OB_FAIL(get_task_items(tenant, sql, pg_tasks, proxy))) {
    LOG_WARN("failed to get pg backup task", K(ret), K(tenant), K(sql));
  }
  return ret;
}

int ObPGBackupBackupsetOperator::get_failed_pg_tasks(const share::SimpleBackupBackupsetTenant& tenant,
    const int64_t job_id, const int64_t backup_set_id, common::ObIArray<ObPGBackupBackupsetTaskItem>& pg_tasks,
    common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const int64_t MAX_GET_NUM = 1024;
  if (!tenant.is_valid() || job_id < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant), K(job_id), K(backup_set_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND job_id = %ld "
                                    "AND backup_set_id = %ld AND status = 'FINISH' AND result <> 0 limit %ld",
                 OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TNAME,
                 tenant.tenant_id_,
                 job_id,
                 backup_set_id,
                 MAX_GET_NUM))) {
    LOG_WARN("failed to assign sql", K(ret), K(tenant), K(job_id), K(backup_set_id));
  } else if (OB_FAIL(get_task_items(tenant, sql, pg_tasks, proxy))) {
    LOG_WARN("failed to get pg backup task", K(ret), K(tenant), K(sql));
  }
  return ret;
}

int ObPGBackupBackupsetOperator::get_pg_task(const share::SimpleBackupBackupsetTenant& tenant, const int64_t job_id,
    const int64_t backup_set_id, const uint64_t table_id, const int64_t partition_id, ObPGBackupBackupsetTaskItem& task,
    common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArray<ObPGBackupBackupsetTaskItem> tasks;
  if (!tenant.is_valid() || job_id < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant), K(job_id), K(backup_set_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND job_id = %ld "
                                    "AND backup_set_id = %ld AND table_id = %lu AND partition_id = %ld",
                 OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TNAME,
                 tenant.tenant_id_,
                 job_id,
                 backup_set_id,
                 table_id,
                 partition_id))) {
    LOG_WARN("failed to assign sql", K(ret), K(tenant), K(job_id), K(backup_set_id));
  } else if (OB_FAIL(get_task_items(tenant, sql, tasks, proxy))) {
    LOG_WARN("failed to get pg backup task", K(ret), K(tenant), K(sql));
  } else if (tasks.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("do not exist such doing pg task", KR(ret));
  } else if (tasks.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not exist two such task", KR(ret), K(tasks));
  } else {
    task = tasks.at(0);
  }
  return ret;
}

int ObPGBackupBackupsetOperator::fill_one_item(const ObPGBackupBackupsetTaskItem& item, ObDMLSqlSplicer& splicer)
{
  int ret = OB_SUCCESS;
  char svr_ip_str[OB_MAX_SERVER_ADDR_SIZE] = "";
  char trace_id_str[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(item));
  } else if (!item.server_.ip_to_string(svr_ip_str, sizeof(svr_ip_str))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to convert ip to string", KR(ret));
  } else if (OB_FAIL(item.get_trace_id(trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE))) {
    LOG_WARN("failed to get trace id", KR(ret), K(item));
  } else if (OB_FAIL(splicer.add_pk_column("tenant_id", item.tenant_id_)) ||
             OB_FAIL(splicer.add_pk_column("job_id", item.job_id_))  // TODO add more filed here
             || OB_FAIL(splicer.add_pk_column("incarnation", item.incarnation_)) ||
             OB_FAIL(splicer.add_pk_column("backup_set_id", item.backup_set_id_)) ||
             OB_FAIL(splicer.add_pk_column("copy_id", item.copy_id_)) ||
             OB_FAIL(splicer.add_pk_column("table_id", item.table_id_)) ||
             OB_FAIL(splicer.add_pk_column("partition_id", item.partition_id_)) ||
             OB_FAIL(splicer.add_column("status", ObPGBackupBackupsetTaskItem::get_status_str(item.status_))) ||
             OB_FAIL(splicer.add_column("trace_id", trace_id_str)) ||
             OB_FAIL(splicer.add_column("svr_ip", svr_ip_str)) ||
             OB_FAIL(splicer.add_column("svr_port", item.server_.get_port())) ||
             OB_FAIL(splicer.add_column("total_partition_count", item.total_partition_count_)) ||
             OB_FAIL(splicer.add_column("finish_partition_count", item.finish_partition_count_)) ||
             OB_FAIL(splicer.add_column("total_macro_block_count", item.total_macro_block_count_)) ||
             OB_FAIL(splicer.add_column("finish_macro_block_count", item.finish_macro_block_count_)) ||
             OB_FAIL(splicer.add_column("result", item.result_)) || OB_FAIL(splicer.add_column("comment", ""))) {
    LOG_WARN("failed to fill pg backup backupset task item", KR(ret), K(item));
  }
  return ret;
}

int ObPGBackupBackupsetOperator::get_task_items(const share::SimpleBackupBackupsetTenant& tenant,
    const common::ObSqlString& sql, common::ObIArray<ObPGBackupBackupsetTaskItem>& item_list,
    common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  const uint64_t real_tenant_id = tenant.is_dropped_ ? OB_SYS_TENANT_ID : tenant.tenant_id_;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_UNLIKELY(!sql.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", KR(ret), K(sql));
    } else if (OB_FAIL(proxy.read(res, real_tenant_id, sql.ptr()))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", KR(ret));
    } else {
      while (OB_SUCC(ret)) {
        ObPGBackupBackupsetTaskItem item;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get next row", KR(ret));
          }
        } else if (OB_FAIL(extract_task_item(result, item))) {
          LOG_WARN("failed to extract pg backup backupset task", KR(ret), K(item));
        } else if (OB_FAIL(item_list.push_back(item))) {
          LOG_WARN("failed to push back item", KR(ret), K(item));
        }
      }
    }
  }
  return ret;
}

int ObPGBackupBackupsetOperator::extract_task_item(sqlclient::ObMySQLResult* result, ObPGBackupBackupsetTaskItem& item)
{
  int ret = OB_SUCCESS;
  item.reset();
  int64_t tmp_real_str_len = 0;
  int32_t svr_port = 0;
  char status_str[OB_DEFAULT_STATUS_LENTH] = "";
  char trace_id_str[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  char svr_ip_str[OB_MAX_SERVER_ADDR_SIZE] = "";
  char comment_str[MAX_TABLE_COMMENT_LENGTH] = "";

  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extract task item get invalid argument", KR(ret), KP(result));
  } else {
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", item.tenant_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "job_id", item.job_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "incarnation", item.incarnation_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "backup_set_id", item.backup_set_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "copy_id", item.copy_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "table_id", item.table_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "partition_id", item.partition_id_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "status", status_str, OB_DEFAULT_STATUS_LENTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "trace_id", trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "svr_ip", svr_ip_str, OB_MAX_SERVER_ADDR_SIZE, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", svr_port, int32_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "total_partition_count", item.total_partition_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_partition_count", item.finish_partition_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "total_macro_block_count", item.total_macro_block_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_macro_block_count", item.finish_macro_block_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "result", item.result_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "comment", comment_str, MAX_TABLE_COMMENT_LENGTH, tmp_real_str_len);
    UNUSED(tmp_real_str_len);

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(item.set_status(status_str))) {
      LOG_WARN("failed to set status str", KR(ret), K(status_str));
    } else if (OB_FAIL(item.set_trace_id(trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE))) {
      LOG_WARN("failed to set trace id", KR(ret), K(trace_id_str));
    } else if (svr_port > 0 && !item.server_.set_ip_addr(svr_ip_str, svr_port)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to set server address", KR(ret), K(svr_ip_str), K(svr_port));
    }
  }
  return ret;
}

int ObPGBackupBackupsetOperator::get_same_trace_id_tasks(const share::SimpleBackupBackupsetTenant& tenant,
    const ObPGBackupBackupsetTaskItem& task, common::ObIArray<ObPGBackupBackupsetTaskItem>& tasks,
    common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  tasks.reset();
  ObSqlString sql;
  char trace_id_str[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  if (OB_FAIL(task.get_trace_id(trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE))) {
    LOG_WARN("failed to get trace id", KR(ret), K(task));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE trace_id = '%s'",
                 OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TNAME,
                 trace_id_str))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else if (OB_FAIL(get_task_items(tenant, sql, tasks, proxy))) {
    LOG_WARN("failed to get pg backup task", K(ret), K(tenant), K(sql));
  }
  return ret;
}

int ObPGBackupBackupsetOperator::get_next_end_key_for_remove(const share::SimpleBackupBackupsetTenant& tenant,
    const share::ObPGBackupBackupsetTaskRowKey& prev_row_key, share::ObPGBackupBackupsetTaskRowKey& next_row_key,
    common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  next_row_key.reset();
  ObSqlString sql;
  ObArray<ObPGBackupBackupsetTaskItem> tasks;
  const int64_t BATCH_SIZE = 1024;
  if (!tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s", OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(
                 " WHERE (tenant_id, job_id, incarnation, backup_set_id, copy_id, table_id, partition_id) "
                 " > (%lu, %ld, %ld, %ld, %ld, %ld, %ld)",
                 prev_row_key.tenant_id_,
                 prev_row_key.job_id_,
                 prev_row_key.incarnation_,
                 prev_row_key.backup_set_id_,
                 prev_row_key.copy_id_,
                 prev_row_key.table_id_,
                 prev_row_key.partition_id_))) {
    LOG_WARN("failed to append sql", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(
                 " ORDER BY tenant_id, job_id, incarnation, backup_set_id, copy_id, table_id, partition_id"
                 " LIMIT %ld ",
                 BATCH_SIZE))) {
    LOG_WARN("failed to append sql", KR(ret));
  } else if (OB_FAIL(get_task_items(tenant, sql, tasks, proxy))) {
    LOG_WARN("failed to get pg backup task", K(ret), K(tenant), K(sql));
  } else if (tasks.empty()) {
    ret = OB_ITER_END;
    LOG_WARN("no next end row key", KR(ret), K(sql), K(tenant));
  } else {
    const ObPGBackupBackupsetTaskItem& last_item = tasks.at(tasks.count() - 1);
    next_row_key.tenant_id_ = last_item.tenant_id_;
    next_row_key.job_id_ = last_item.job_id_;
    next_row_key.incarnation_ = last_item.incarnation_;
    next_row_key.backup_set_id_ = last_item.backup_set_id_;
    next_row_key.copy_id_ = last_item.copy_id_;
    next_row_key.table_id_ = last_item.table_id_;
    next_row_key.partition_id_ = last_item.partition_id_;
  }
  return ret;
}

int ObPGBackupBackupsetOperator::get_batch_end_key_for_remove(const share::SimpleBackupBackupsetTenant& tenant,
    common::ObIArray<share::ObPGBackupBackupsetTaskRowKey>& row_key_list, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  row_key_list.reset();
  ObPGBackupBackupsetTaskRowKey prev_row_key;
  ObPGBackupBackupsetTaskRowKey next_row_key;
  prev_row_key.reuse();
  if (!tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret));
  } else if (OB_FAIL(row_key_list.push_back(prev_row_key))) {
    LOG_WARN("failed to push back", KR(ret), K(prev_row_key));
  } else {
    do {
      next_row_key.reset();
      if (OB_FAIL(get_next_end_key_for_remove(tenant, prev_row_key, next_row_key, proxy))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next end key for remove", KR(ret), K(prev_row_key));
        }
      } else if (OB_FAIL(row_key_list.push_back(next_row_key))) {
        LOG_WARN("failed to push back", KR(ret), K(next_row_key));
      } else {
        prev_row_key = next_row_key;
      }
    } while (OB_SUCC(ret));
  }
  return ret;
}

int ObPGBackupBackupsetOperator::batch_remove_pg_tasks(const share::SimpleBackupBackupsetTenant& tenant,
    const share::ObPGBackupBackupsetTaskRowKey& left_row_key, const share::ObPGBackupBackupsetTaskRowKey& right_row_key,
    common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  const uint64_t tenant_id = tenant.is_dropped_ ? OB_SYS_TENANT_ID : tenant.tenant_id_;
  if (!tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE", OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant));
  } else if (OB_FAIL(sql.append_fmt("(tenant_id, job_id, incarnation, backup_set_id, copy_id, table_id, partition_id)"
                                    "> (%lu, %ld, %ld, %ld, %ld, %ld, %ld)",
                 left_row_key.tenant_id_,
                 left_row_key.job_id_,
                 left_row_key.incarnation_,
                 left_row_key.backup_set_id_,
                 left_row_key.copy_id_,
                 left_row_key.table_id_,
                 left_row_key.partition_id_))) {
    LOG_WARN("failed to append sql", KR(ret), K(left_row_key));
  } else if (OB_FAIL(sql.append_fmt(" AND "))) {
    LOG_WARN("failed to append sql", KR(ret));
  } else if (OB_FAIL(sql.append_fmt("(tenant_id, job_id, incarnation, backup_set_id, copy_id, table_id, partition_id)"
                                    "<= (%lu, %ld, %ld, %ld, %ld, %ld, %ld)",
                 right_row_key.tenant_id_,
                 right_row_key.job_id_,
                 right_row_key.incarnation_,
                 right_row_key.backup_set_id_,
                 right_row_key.copy_id_,
                 right_row_key.table_id_,
                 right_row_key.partition_id_))) {
    LOG_WARN("failed to append sql", KR(ret), K(right_row_key));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(sql));
  }
  return ret;
}

int ObPGBackupBackupsetOperator::remove_all_pg_tasks(
    const share::SimpleBackupBackupsetTenant& tenant, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObPGBackupBackupsetTaskRowKey> row_key_list;
  if (!tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret));
  } else if (OB_FAIL(get_batch_end_key_for_remove(tenant, row_key_list, proxy))) {
    LOG_WARN("failed to get batch end key for remove", KR(ret), K(tenant));
  } else if (1 == row_key_list.count()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_key_list.count() - 1; ++i) {
      const ObPGBackupBackupsetTaskRowKey& left_row_key = row_key_list.at(i);
      const ObPGBackupBackupsetTaskRowKey& right_row_key = row_key_list.at(i + 1);
      if (OB_FAIL(batch_remove_pg_tasks(tenant, left_row_key, right_row_key, proxy))) {
        LOG_WARN("failed to batch remove pg tasks", KR(ret), K(tenant), K(left_row_key), K(right_row_key));
      }
    }
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
