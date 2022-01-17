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

#define USING_LOG_PREFIX SERVER

#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/backup/ob_backup_struct.h"
#include "lib/utility/utility.h"
#include "ob_backup_operator.h"

namespace oceanbase {
namespace share {
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;

int ObITenantBackupTaskOperator::fill_one_item(const ObTenantBackupTaskItem& item, share::ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  char backup_dest_str[share::OB_MAX_BACKUP_DEST_LENGTH] = "";
  char cluster_version_display[share::OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH] = "";
  const int64_t pos = ObClusterVersion::get_instance().print_vsn(
      cluster_version_display, OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH, item.cluster_version_);

  if (!item.is_valid() || pos <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item), K(pos));
  } else if (OB_FAIL(item.backup_dest_.get_backup_dest_str(backup_dest_str, share::OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", K(ret), K(item));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", item.tenant_id_)) ||
             OB_FAIL(dml.add_pk_column("backup_set_id", item.backup_set_id_)) ||
             OB_FAIL(dml.add_pk_column("incarnation", item.incarnation_)) ||
             OB_FAIL(dml.add_column("backup_type", item.backup_type_.get_backup_type_str())) ||
             OB_FAIL(dml.add_column("device_type", get_storage_type_str(item.device_type_))) ||
             OB_FAIL(dml.add_column("snapshot_version", item.snapshot_version_)) ||
             OB_FAIL(dml.add_column("prev_full_backup_set_id", item.prev_full_backup_set_id_)) ||
             OB_FAIL(dml.add_column("prev_inc_backup_set_id", item.prev_inc_backup_set_id_)) ||
             OB_FAIL(dml.add_column("prev_backup_data_version", item.prev_backup_data_version_)) ||
             OB_FAIL(dml.add_column("pg_count", item.pg_count_)) ||
             OB_FAIL(dml.add_column("macro_block_count", item.macro_block_count_)) ||
             OB_FAIL(dml.add_column("finish_pg_count", item.finish_pg_count_)) ||
             OB_FAIL(dml.add_column("finish_macro_block_count", item.finish_macro_block_count_)) ||
             OB_FAIL(dml.add_column("input_bytes", item.input_bytes_)) ||
             OB_FAIL(dml.add_column("output_bytes", item.output_bytes_)) ||
             OB_FAIL(dml.add_time_column("start_time", item.start_time_)) ||
             OB_FAIL(dml.add_time_column("end_time", item.end_time_)) ||
             OB_FAIL(dml.add_column("compatible", item.compatible_)) ||
             OB_FAIL(dml.add_column("cluster_version", item.cluster_version_)) ||
             OB_FAIL(dml.add_column("status", item.get_backup_task_status_str())) ||
             OB_FAIL(dml.add_column("result", item.result_)) ||
             OB_FAIL(dml.add_column("cluster_id", item.cluster_id_)) ||
             OB_FAIL(dml.add_column("backup_dest", backup_dest_str)) ||
             OB_FAIL(dml.add_column("backup_data_version", item.backup_data_version_)) ||
             OB_FAIL(dml.add_column("backup_schema_version", item.backup_schema_version_)) ||
             OB_FAIL(dml.add_column("cluster_version_display", cluster_version_display)) ||
             OB_FAIL(dml.add_column("partition_count", item.partition_count_)) ||
             OB_FAIL(dml.add_column("finish_partition_count", item.finish_partition_count_)) ||
             OB_FAIL(dml.add_column("start_replay_log_ts", item.start_replay_log_ts_)) ||
             OB_FAIL(dml.add_column("date", item.date_))) {
    LOG_WARN("fail to fill backup task info", K(ret), K(item));
  }
  return ret;
}

int ObITenantBackupTaskOperator::fill_task_item(const ObTenantBackupTaskItem& item, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;

  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else if (OB_FAIL(fill_one_item(item, dml))) {
    LOG_WARN("failed to fill one base item", K(item));
  } else if (OB_FAIL(dml.add_column(
                 OB_STR_BACKUP_ENCRYPTION_MODE, ObBackupEncryptionMode::to_str(item.encryption_mode_))) ||
             OB_FAIL(dml.add_column(OB_STR_BACKUP_PASSWD, item.passwd_.ptr()))) {
    LOG_WARN("fail to fill backup task info", K(ret), K(item));
  }
  return ret;
}

int ObITenantBackupTaskOperator::fill_task_history_item(
    const ObTenantBackupTaskItem& item, const bool need_fill_mark_deleted_item, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else if (OB_FAIL(fill_one_item(item, dml))) {
    LOG_WARN("failed to fill one base item", K(item));
  } else if (OB_FAIL(dml.add_column(
                 OB_STR_BACKUP_ENCRYPTION_MODE, ObBackupEncryptionMode::to_str(item.encryption_mode_))) ||
             OB_FAIL(dml.add_column(OB_STR_BACKUP_PASSWD, item.passwd_.ptr())) ||
             (need_fill_mark_deleted_item && OB_FAIL(dml.add_column("is_mark_deleted", item.is_mark_deleted_)))) {
    LOG_WARN("fail to fill backup task info", K(ret), K(item));
  }
  return ret;
}

int ObITenantBackupTaskOperator::fill_task_clean_history(
    const int64_t copy_id, const ObTenantBackupTaskItem& item, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else if (OB_FAIL(fill_one_item(item, dml))) {
    LOG_WARN("failed to fill one base item", K(item));
  } else if (OB_FAIL(dml.add_column("copy_id", copy_id))) {
    LOG_WARN("failed to fill copy id", K(ret), K(copy_id));
  }
  return ret;
}

int ObITenantBackupTaskOperator::get_tenant_backup_task(const uint64_t tenant_id, const common::ObSqlString& sql,
    common::ObIArray<ObTenantBackupTaskItem>& items, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_UNLIKELY(!sql.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        ObTenantBackupTaskItem item;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else if (OB_FAIL(extract_tenant_backup_task(result, item))) {
          LOG_WARN("failed to extract tenant backup task", K(ret), K(item));
        } else if (OB_FAIL(items.push_back(item))) {
          LOG_WARN("fail to push back item", K(ret), K(item));
        }
      }
    }
  }
  return ret;
}

int ObITenantBackupTaskOperator::get_tenant_backup_history_task(const uint64_t tenant_id,
    const common::ObSqlString& sql, common::ObIArray<ObTenantBackupTaskItem>& items, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_UNLIKELY(!sql.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        ObTenantBackupTaskItem item;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else if (OB_FAIL(extract_tenant_backup_task(result, item))) {
          LOG_WARN("failed to extract tenant backup task", K(ret), K(item));
        } else {
          EXTRACT_INT_FIELD_MYSQL(*result, "is_mark_deleted", item.is_mark_deleted_, bool);
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(items.push_back(item))) {
            LOG_WARN("fail to push back item", K(ret), K(item));
          }
        }
      }
    }
  }
  return ret;
}

int ObITenantBackupTaskOperator::extract_tenant_backup_task(
    sqlclient::ObMySQLResult* result, ObTenantBackupTaskItem& item)

{
  int ret = OB_SUCCESS;
  char backup_type[common::OB_INNER_TABLE_BACKUP_TYPE_LENTH + 1] = "";
  char backup_status[common::OB_DEFAULT_STATUS_LENTH] = "";
  char backup_device_type[common::OB_DEFAULT_OUTPUT_DEVICE_TYPE_LENTH] = "";
  char backup_dest[share::OB_MAX_BACKUP_DEST_LENGTH] = "";
  char encryption_mode[OB_MAX_ENCRYPTION_MODE_LENGTH] = "";
  char passwd[OB_MAX_PASSWORD_LENGTH] = "";
  int64_t tmp_real_str_len = 0;
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extract tenant backup task get invlid argument", K(ret), KP(result));
  } else {
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", item.tenant_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "backup_set_id", item.backup_set_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "incarnation", item.incarnation_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "snapshot_version", item.snapshot_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "prev_full_backup_set_id", item.prev_full_backup_set_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "prev_inc_backup_set_id", item.prev_inc_backup_set_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "prev_backup_data_version", item.prev_backup_data_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "pg_count", item.pg_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "macro_block_count", item.macro_block_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_pg_count", item.finish_pg_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_macro_block_count", item.finish_macro_block_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "input_bytes", item.input_bytes_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "output_bytes", item.output_bytes_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "result", item.result_, int32_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "compatible", item.compatible_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "cluster_version", item.cluster_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "cluster_id", item.cluster_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "backup_data_version", item.backup_data_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "backup_schema_version", item.backup_schema_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "partition_count", item.partition_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_partition_count", item.finish_partition_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "start_replay_log_ts", item.start_replay_log_ts_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "date", item.date_, int64_t);

    EXTRACT_STRBUF_FIELD_MYSQL(
        *result, "backup_type", backup_type, OB_INNER_TABLE_BACKUP_TYPE_LENTH + 1, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "status", backup_status, OB_DEFAULT_STATUS_LENTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(
        *result, "device_type", backup_device_type, OB_DEFAULT_OUTPUT_DEVICE_TYPE_LENTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "backup_dest", backup_dest, OB_MAX_BACKUP_DEST_LENGTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(
        *result, OB_STR_BACKUP_ENCRYPTION_MODE, encryption_mode, OB_MAX_ENCRYPTION_MODE_LENGTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_BACKUP_PASSWD, passwd, OB_MAX_PASSWORD_LENGTH, tmp_real_str_len);

    UNUSED(tmp_real_str_len);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(result->get_timestamp("start_time", NULL, item.start_time_))) {
      LOG_WARN("failed to get start time", K(ret));
    } else if (OB_FAIL(result->get_timestamp("end_time", NULL, item.end_time_))) {
      LOG_WARN("failed to get end time", K(ret));
    } else if (OB_FAIL(item.backup_type_.set_backup_type(backup_type))) {
      LOG_WARN("failed to set backup type", K(ret), K(backup_type), K(OB_INNER_TABLE_BACKUP_TYPE_LENTH));
    } else if (OB_FAIL(item.set_backup_task_status(backup_status))) {
      LOG_WARN("failed to set backup status", K(ret), K(backup_status), K(OB_DEFAULT_STATUS_LENTH));
    } else if (OB_FAIL(get_storage_type_from_name(backup_device_type, item.device_type_))) {
      LOG_WARN(
          "failed to set backup device type", K(ret), K(backup_device_type), K(OB_DEFAULT_OUTPUT_DEVICE_TYPE_LENTH));
    } else if (OB_FAIL(item.backup_dest_.set(backup_dest))) {
      LOG_WARN("failed to set backup dest", K(ret), K(backup_dest), K(OB_MAX_BACKUP_DEST_LENGTH));
    } else if (FALSE_IT(item.encryption_mode_ = ObBackupEncryptionMode::parse_str(encryption_mode))) {
    } else if (!ObBackupEncryptionMode::is_valid(item.encryption_mode_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid encryption_mode", K(ret), K(encryption_mode), "mode", item.encryption_mode_);
    } else if (OB_FAIL(item.passwd_.assign(passwd))) {
      LOG_WARN("failed to assign passwd", K(ret));
    }
  }
  return ret;
}

int ObITenantBackupTaskOperator::get_tenant_ids(const uint64_t tenant_id, const common::ObSqlString& sql,
    common::ObIArray<uint64_t>& tenant_ids, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_UNLIKELY(!sql.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        int64_t tmp_tenant_id = 0;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else {
          EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tmp_tenant_id, int64_t);
          if (OB_FAIL(tenant_ids.push_back(tmp_tenant_id))) {
            LOG_WARN("failed to push tenant id into array", K(ret), K(tmp_tenant_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantBackupTaskOperator::get_tenant_backup_task(const uint64_t tenant_id, const int64_t backup_set_id,
    const int64_t incarnation, ObTenantBackupTaskItem& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  item.reset();
  ObArray<ObTenantBackupTaskItem> items;

  if (OB_INVALID_ID == tenant_id || backup_set_id < 0 || incarnation < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task get invalid argument", K(ret), K(tenant_id), K(backup_set_id), K(incarnation));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                                    "WHERE tenant_id = %lu AND backup_set_id = %ld AND incarnation = %ld FOR UPDATE",
                 OB_ALL_TENANT_BACKUP_TASK_TNAME,
                 tenant_id,
                 backup_set_id,
                 incarnation))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id), K(backup_set_id), K(incarnation));
  } else if (OB_FAIL(ObITenantBackupTaskOperator::get_tenant_backup_task(tenant_id, sql, items, proxy))) {
    LOG_WARN("fail to get pg backup task", K(ret), K(sql), K(tenant_id), K(backup_set_id), K(incarnation));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", K(ret), K(items.count()));
  } else if (0 == items.count()) {
    // do nothing
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("failed to get tenant backup task", K(ret), K(tenant_id), K(backup_set_id));
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObTenantBackupTaskOperator::insert_task(const ObTenantBackupTaskItem& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = item.tenant_id_;

  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObITenantBackupTaskOperator::fill_task_item(item, dml))) {
      LOG_WARN("failed to fill on item", K(ret), K(item));
    } else if (OB_FAIL(dml.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", K(ret));
    } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_TENANT_BACKUP_TASK_TNAME, sql))) {
      LOG_WARN("failed to splice insert update sql", K(ret), K(item));
    } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_FAIL(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows));
    } else {
      LOG_INFO("batch report tenant backup task", K(sql));
    }
  }
  return ret;
}

int ObTenantBackupTaskOperator::report_task(const ObTenantBackupTaskItem& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = item.tenant_id_;

  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObITenantBackupTaskOperator::fill_task_item(item, dml))) {
      LOG_WARN("failed to fill on item", K(ret), K(item));
    } else if (OB_FAIL(dml.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", K(ret));
    } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_TENANT_BACKUP_TASK_TNAME, sql))) {
      LOG_WARN("failed to splice insert update sql", K(ret), K(item));
    } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(ret));
    } else if (1 != affected_rows && 0 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows));
    } else {
      LOG_INFO("batch report tenant backup task", K(sql));
    }
  }
  return ret;
}

int ObTenantBackupTaskOperator::remove_task(
    const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id, ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (tenant_id == OB_INVALID_ARGUMENT || backup_set_id < 0 || incarnation < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("remove task get invalid argument", K(ret), K(tenant_id), K(backup_set_id), K(incarnation));
  } else if (OB_FAIL(remove_one_item(tenant_id, incarnation, backup_set_id, sql_proxy))) {
    LOG_WARN("failed to remove on item", K(ret), K(tenant_id), K(backup_set_id));
  }
  return ret;
}

int ObTenantBackupTaskOperator::remove_one_item(
    const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id)) || OB_FAIL(dml.add_pk_column("incarnation", incarnation)) ||
      OB_FAIL(dml.add_pk_column("backup_set_id", backup_set_id))) {
    LOG_WARN("fail to add column", K(ret), K(tenant_id), K(backup_set_id), K(incarnation));
  } else {
    ObDMLExecHelper exec(sql_proxy, tenant_id);
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_delete(OB_ALL_TENANT_BACKUP_TASK_TNAME, dml, affected_rows))) {
      LOG_WARN("fail to exec delete", K(ret));
    }
  }
  return ret;
}

int ObTenantBackupTaskOperator::get_tenant_backup_task(
    const uint64_t tenant_id, const bool for_update, ObTenantBackupTaskItem& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  item.reset();
  ObArray<ObTenantBackupTaskItem> items;

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                                    "WHERE tenant_id = %lu",
                 OB_ALL_TENANT_BACKUP_TASK_TNAME,
                 tenant_id))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else if (for_update && OB_FAIL(sql.append(" FOR UPDATE"))) {
    LOG_WARN("failed to apend sql", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObITenantBackupTaskOperator::get_tenant_backup_task(tenant_id, sql, items, proxy))) {
    LOG_WARN("fail to get pg backup task", K(ret), K(sql), K(tenant_id));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", K(ret), K(items.count()));
  } else if (0 == items.count()) {
    // do nothing
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObPGBackupTaskOperator::get_pg_backup_task(ObPGBackupTaskItem& item, ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArray<ObPGBackupTaskItem> items;
  const uint64_t tenant_id = item.tenant_id_;

  if (!item.is_key_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task get invalid argument", K(ret), K(item));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                                    "WHERE tenant_id = %ld AND table_id = %ld AND partition_id = %ld "
                                    "AND incarnation = %ld AND backup_set_id = %ld",
                 OB_ALL_TENANT_PG_BACKUP_TASK_TNAME,
                 tenant_id,
                 item.table_id_,
                 item.partition_id_,
                 item.incarnation_,
                 item.backup_set_id_))) {
    LOG_WARN("fail to assign sql", K(ret), K(item));
  } else if (OB_FAIL(get_pg_backup_task(tenant_id, sql, items, sql_proxy))) {
    LOG_WARN("fail to get pg backup task", K(ret), K(sql), K(item));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", K(ret), K(items.count()));
  } else if (0 == items.count()) {
    // do nothing
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObPGBackupTaskOperator::get_pg_backup_task(ObIArray<ObPGBackupTaskItem>& items, common::ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObMySQLTransaction trans;
  ObDMLSqlSplicer dml;
  const int64_t BATCH_CNT = 500;
  if (OB_UNLIKELY(items.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(items));
  } else {
    const uint64_t tenant_id = items.at(0).tenant_id_;
    int64_t get_idx = 0;
    while (OB_SUCC(ret) && get_idx < items.count()) {
      sql.reuse();
      columns.reuse();
      const int64_t remain_cnt = items.count() - get_idx;
      int64_t cur_batch_cnt = remain_cnt < BATCH_CNT ? remain_cnt : BATCH_CNT;
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE ", OB_ALL_TENANT_PG_BACKUP_TASK_TNAME))) {
        LOG_WARN("fail to assign sql", K(ret));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < cur_batch_cnt; ++i) {
        ObPGBackupTaskItem& item = items.at(get_idx + i);
        if (!item.is_key_valid()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("pg key is invalid", K(ret), K(item));
        } else if (i > 0 && OB_FAIL(sql.append("OR "))) {
          LOG_WARN("fail to assign sql", K(ret), K(item));
        } else if (OB_FAIL(sql.assign_fmt("(tenant_id = %lu AND table_id = %lu AND partition_id = %ld "
                                          "AND incarnation = %ld AND backup_set_id = %ld) ",
                       tenant_id,
                       item.table_id_,
                       item.partition_id_,
                       item.incarnation_,
                       item.backup_set_id_))) {
          LOG_WARN("fail to assign sql", K(ret), K(item));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(get_pg_backup_task(tenant_id, sql, items, sql_proxy))) {
          LOG_WARN("fail to get pg backup task", K(ret), K(sql));
        }
      }

      if (OB_SUCC(ret)) {
        get_idx += cur_batch_cnt;
        LOG_INFO("batch get pg task sql", K(sql));
      }
    }
  }
  return ret;
}

int ObPGBackupTaskOperator::get_pg_backup_task(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, common::ObIArray<ObPGBackupTaskItem>& items, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const int64_t BATCH_CNT = 1024;
  ObArray<ObPGBackupTaskItem> tmp_items;

  if (OB_INVALID_ID == tenant_id || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND incarnation = %ld AND "
                                    "backup_set_id = %ld ",
                 OB_ALL_TENANT_PG_BACKUP_TASK_TNAME,
                 tenant_id,
                 incarnation,
                 backup_set_id))) {
    LOG_WARN("fail to assign sql", K(ret));
  } else if (OB_FAIL(get_pg_backup_task(tenant_id, sql, items, sql_proxy))) {
    LOG_WARN("failed to get pg backup task", K(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObPGBackupTaskOperator::get_finished_backup_task(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, common::ObIArray<ObPGBackupTaskItem>& items, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  items.reset();
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;
    if (OB_INVALID_ID == tenant_id || incarnation < 0 || backup_set_id < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND incarnation = %ld "
                                      "AND backup_set_id = %ld AND status = 'FINISH'",
                   OB_ALL_TENANT_PG_BACKUP_TASK_TNAME,
                   tenant_id,
                   incarnation,
                   backup_set_id))) {
      LOG_WARN("failed to assign sql", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
    } else if (OB_FAIL(get_pg_backup_task(tenant_id, sql, items, sql_proxy))) {
      LOG_WARN("failed to get pg backup task", K(ret), K(tenant_id), K(sql));
    }
  }
  return ret;
}

int ObPGBackupTaskOperator::get_pg_backup_task(
    const uint64_t tenant_id, const ObSqlString& sql, ObIArray<ObPGBackupTaskItem>& items, ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;

    if (OB_UNLIKELY(!sql.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        ObPGBackupTaskItem item;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else if (OB_FAIL(extract_pg_task(result, item))) {
          LOG_WARN("failed to extract pg task", K(ret), KP(result));
        } else if (OB_FAIL(items.push_back(item))) {
          LOG_WARN("failed to push pg backup task item into array", K(ret), K(item));
        }
      }
    }
  }
  return ret;
}

int ObPGBackupTaskOperator::batch_report_task(
    const common::ObIArray<ObPGBackupTaskItem>& items, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer dml;
  const int64_t BATCH_CNT = 500;
  if (OB_UNLIKELY(items.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(items));
  } else {
    const uint64_t tenant_id = extract_tenant_id(items.at(0).table_id_);
    int64_t report_idx = 0;
    while (OB_SUCC(ret) && report_idx < items.count()) {
      sql.reuse();
      columns.reuse();
      const int64_t remain_cnt = items.count() - report_idx;
      int64_t cur_batch_cnt = remain_cnt < BATCH_CNT ? remain_cnt : BATCH_CNT;
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_batch_cnt; ++i) {
        const ObPGBackupTaskItem& item = items.at(report_idx + i);
        dml.reuse();
        if (OB_FAIL(fill_one_item(item, dml))) {
          LOG_WARN("fail to fill one item", K(ret), K(item));
        } else {
          if (0 == i) {
            if (OB_FAIL(dml.splice_column_names(columns))) {
              LOG_WARN("fail to splice column names", K(ret));
            } else if (OB_FAIL(sql.assign_fmt("INSERT /*+ use_plan_cache(none) */ INTO %s (%s) VALUES",
                           OB_ALL_TENANT_PG_BACKUP_TASK_TNAME,
                           columns.ptr()))) {
              LOG_WARN("fail to assign sql string", K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            values.reset();
            if (OB_FAIL(dml.splice_values(values))) {
              LOG_WARN("fail to splice values", K(ret));
            } else if (OB_FAIL(sql.append_fmt("%s(%s)", 0 == i ? " " : " , ", values.ptr()))) {
              LOG_WARN("fail to assign sql string", K(ret));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append(" ON DUPLICATE KEY UPDATE "))) {
          LOG_WARN("fail to append sql string", K(ret));
        } else if (OB_FAIL(sql.append(" backup_type = values(backup_type)")) ||
                   OB_FAIL(sql.append(", snapshot_version = values(snapshot_version)")) ||
                   OB_FAIL(sql.append(", partition_count = values(partition_count)")) ||
                   OB_FAIL(sql.append(", macro_block_count = values(macro_block_count)")) ||
                   OB_FAIL(sql.append(", finish_partition_count = values(finish_partition_count)")) ||
                   OB_FAIL(sql.append(", finish_macro_block_count = values(finish_macro_block_count)")) ||
                   OB_FAIL(sql.append(", input_bytes = values(input_bytes)")) ||
                   OB_FAIL(sql.append(", output_bytes = values(output_bytes)")) ||
                   OB_FAIL(sql.append(", start_time = values(start_time)")) ||
                   OB_FAIL(sql.append(", end_time = values(end_time)")) ||
                   OB_FAIL(sql.append(", retry_count = values(retry_count)")) ||
                   OB_FAIL(sql.append(", replica_role = values(replica_role)")) ||
                   OB_FAIL(sql.append(", replica_type = values(replica_type)")) ||
                   OB_FAIL(sql.append(", svr_ip = values(svr_ip)")) ||
                   OB_FAIL(sql.append(", svr_port = values(svr_port)")) ||
                   OB_FAIL(sql.append(", status = values(status)")) ||
                   OB_FAIL(sql.append(", result = values(result)"))) {
          LOG_WARN("fail to append sql string", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t affected_rows = 0;
        if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(ret));
        } else if (OB_UNLIKELY(affected_rows > 2 * cur_batch_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows), K(cur_batch_cnt));
        } else {
          report_idx += cur_batch_cnt;
          LOG_INFO("batch report pg task", K(sql));
        }
      }
    }
  }
  return ret;
}

int ObPGBackupTaskOperator::batch_remove_task(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, const int64_t max_delete_rows, common::ObISQLClient& proxy, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  affected_rows = 0;

  if (OB_INVALID_ID == tenant_id || incarnation < 0 || backup_set_id < 0 || max_delete_rows <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(incarnation), K(backup_set_id), K(max_delete_rows));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND incarnation = %ld AND "
                                    "backup_set_id = %ld limit %ld",
                 OB_ALL_TENANT_PG_BACKUP_TASK_TNAME,
                 tenant_id,
                 incarnation,
                 backup_set_id,
                 max_delete_rows))) {
    LOG_WARN("failed to assign sql", K(ret), K(tenant_id), K(incarnation), K(backup_set_id), K(max_delete_rows));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret));
  }
  return ret;
}

int ObPGBackupTaskOperator::remove_one_item(const common::ObPGKey& pg_key, ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pg_key));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", pg_key.get_tenant_id())) ||
             OB_FAIL(dml.add_pk_column("table_id", pg_key.get_table_id())) ||
             OB_FAIL(dml.add_pk_column("partition_id", pg_key.get_partition_id()))) {
    LOG_WARN("fail to add column", K(ret));
  } else {
    ObDMLExecHelper exec(sql_proxy, pg_key.get_tenant_id());
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_delete(OB_ALL_TENANT_PG_BACKUP_TASK_TNAME, dml, affected_rows))) {
      LOG_WARN("fail to exec delete", K(ret));
    }
  }
  return ret;
}

int ObPGBackupTaskOperator::fill_one_item(const ObPGBackupTaskItem& item, share::ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  char ip[common::OB_MAX_SERVER_ADDR_SIZE] = "";
  char trace_id[common::OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else if (!item.server_.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to convert ip to string", K(ret));
  } else if (OB_FAIL(item.get_trace_id(trace_id, OB_MAX_TRACE_ID_BUFFER_SIZE))) {
    LOG_WARN("failed to get trace id", K(ret), K(item));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", item.tenant_id_)) ||
             OB_FAIL(dml.add_pk_column("table_id", item.table_id_)) ||
             OB_FAIL(dml.add_pk_column("partition_id", item.partition_id_)) ||
             OB_FAIL(dml.add_pk_column("incarnation", item.incarnation_)) ||
             OB_FAIL(dml.add_pk_column("backup_set_id", item.backup_set_id_)) ||
             OB_FAIL(dml.add_column("backup_type", item.backup_type_.get_backup_type_str())) ||
             OB_FAIL(dml.add_column("snapshot_version", item.snapshot_version_)) ||
             OB_FAIL(dml.add_column("partition_count", item.partition_count_)) ||
             OB_FAIL(dml.add_column("macro_block_count", item.macro_block_count_)) ||
             OB_FAIL(dml.add_column("finish_partition_count", item.finish_partition_count_)) ||
             OB_FAIL(dml.add_column("finish_macro_block_count", item.finish_macro_block_count_)) ||
             OB_FAIL(dml.add_column("input_bytes", item.input_bytes_)) ||
             OB_FAIL(dml.add_column("output_bytes", item.output_bytes_)) ||
             OB_FAIL(dml.add_time_column("start_time", item.start_time_)) ||
             OB_FAIL(dml.add_time_column("end_time", item.end_time_)) ||
             OB_FAIL(dml.add_column("retry_count", item.retry_count_)) ||
             OB_FAIL(dml.add_column("replica_role", static_cast<int32_t>(item.role_))) ||
             OB_FAIL(dml.add_column("replica_type", static_cast<int32_t>(item.replica_type_))) ||
             OB_FAIL(dml.add_column("svr_ip", ip)) || OB_FAIL(dml.add_column("svr_port", item.server_.get_port())) ||
             OB_FAIL(dml.add_column("status", item.get_backup_task_status_str())) ||
             OB_FAIL(dml.add_column("result", item.result_)) || OB_FAIL(dml.add_column("task_id", item.task_id_)) ||
             OB_FAIL(dml.add_column("trace_id", trace_id))) {
    LOG_WARN("fail to fill backup task info", K(ret));
  }
  return ret;
}

int ObPGBackupTaskOperator::extract_pg_task(common::sqlclient::ObMySQLResult* result, ObPGBackupTaskItem& item)
{
  int ret = OB_SUCCESS;
  item.reset();
  int64_t tmp_real_str_len = 0;
  char ip[common::OB_MAX_SERVER_ADDR_SIZE] = "";
  int port = 0;
  char backup_type[common::OB_INNER_TABLE_BACKUP_TYPE_LENTH + 1] = "";
  char backup_status[common::OB_DEFAULT_STATUS_LENTH] = "";
  char trace_id[common::OB_MAX_TRACE_ID_BUFFER_SIZE] = "";

  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extract pg task get invalid argument", K(ret), KP(result));
  } else {
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", item.tenant_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "table_id", item.table_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "partition_id", item.partition_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "incarnation", item.incarnation_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "backup_set_id", item.backup_set_id_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(
        *result, "backup_type", backup_type, OB_INNER_TABLE_BACKUP_TYPE_LENTH + 1, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(*result, "snapshot_version", item.snapshot_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "partition_count", item.partition_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "macro_block_count", item.macro_block_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_partition_count", item.finish_partition_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_macro_block_count", item.finish_macro_block_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "input_bytes", item.input_bytes_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "output_bytes", item.output_bytes_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "retry_count", item.retry_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "replica_role", item.role_, ObRole);
    EXTRACT_INT_FIELD_MYSQL(*result, "replica_type", item.replica_type_, ObReplicaType);
    EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", port, int);
    EXTRACT_INT_FIELD_MYSQL(*result, "task_id", item.task_id_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "svr_ip", ip, OB_MAX_SERVER_ADDR_SIZE, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "status", backup_status, OB_DEFAULT_STATUS_LENTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "trace_id", trace_id, OB_MAX_TRACE_ID_BUFFER_SIZE, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(*result, "result", item.result_, int32_t);
    UNUSED(tmp_real_str_len);
    if (OB_FAIL(result->get_timestamp("start_time", NULL, item.start_time_))) {
      LOG_WARN("failed to get start time", K(ret));
    } else if (OB_FAIL(result->get_timestamp("end_time", NULL, item.end_time_))) {
      LOG_WARN("failed to get end time", K(ret));
    } else if (port > 0 && !item.server_.set_ip_addr(ip, port)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, fail to set server addr", K(ret), K(ip), K(port));
    } else if (OB_FAIL(item.backup_type_.set_backup_type(backup_type))) {
      LOG_WARN("failed to set backup type", K(ret), K(item));
    } else if (OB_FAIL(item.set_backup_task_status(backup_status))) {
      LOG_WARN("failed to set pg task backup task status", K(ret), K(backup_status));
    } else if (OB_FAIL(item.set_trace_id(trace_id, OB_MAX_TRACE_ID_BUFFER_SIZE))) {
      LOG_WARN("failed to set trace id", K(ret), K(trace_id));
    }
  }
  return ret;
}

int ObPGBackupTaskOperator::get_latest_backup_task(
    common::ObISQLClient& sql_proxy, const uint64_t tenant_id, ObPGBackupTaskItem& item)
{
  int ret = OB_SUCCESS;
  item.reset();
  ObArray<ObPGBackupTaskItem> items;
  ObSqlString sql;

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get latest backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                                    "ORDER BY table_id DESC, partition_id DESC LIMIT 1",
                 OB_ALL_TENANT_PG_BACKUP_TASK_TNAME))) {
    LOG_WARN("failed to assign sql", K(ret), K(sql));
  } else if (OB_FAIL(get_pg_backup_task(tenant_id, sql, items, sql_proxy))) {
    LOG_WARN("failed to get pg backup task", K(ret), K(sql), K(tenant_id));
  } else if (items.empty()) {
    // do noting
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get latest pg backup task count unexpected", K(ret), K(items), K(sql));
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObPGBackupTaskOperator::get_total_pg_task_count(
    common::ObISQLClient& sql_proxy, const uint64_t tenant_id, int64_t& task_count)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;

    if (OB_INVALID_ID == tenant_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get latest backup task get invalid argument", K(ret), K(tenant_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT COUNT(1) FROM %s ", OB_ALL_TENANT_PG_BACKUP_TASK_TNAME))) {
      LOG_WARN("failed to assign sql", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("failed to get next result", K(ret), K(sql));
    } else if (OB_FAIL(result->get_int("COUNT(1)", task_count))) {
      LOG_WARN("failed to get count(1)", K(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END != ret) {
        OB_LOG(WARN, "failed to get next result", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObPGBackupTaskOperator::update_pg_task_info(common::ObISQLClient& sql_proxy, const common::ObAddr& addr,
    const common::ObReplicaType& replica_type, const ObTaskId& trace_id, const ObPGBackupTaskInfo::BackupStatus& status,
    const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  char ip[common::OB_MAX_SERVER_ADDR_SIZE] = "";
  const uint64_t tenant_id = pkey.get_tenant_id();
  char trace_id_str[common::OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  int64_t affected_rows = -1;
  const uint64_t* value = trace_id.get();

  if (!pkey.is_valid() || !addr.is_valid() || status >= ObPGBackupTaskInfo::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update addr get invalid argument", K(ret), K(addr), K(status), K(pkey));
  } else if (!addr.ip_to_string(ip, common::OB_MAX_SERVER_ADDR_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to change ip to string", K(ret), K(addr));
  } else if (OB_FAIL(ObPGBackupTaskInfo::get_trace_id(trace_id, OB_MAX_TRACE_ID_BUFFER_SIZE, trace_id_str))) {
    LOG_WARN("failed to get trace id", K(ret), K(trace_id));
  } else if (OB_FAIL(sql.append_fmt(
                 "UPDATE %s SET svr_ip = '%s', svr_port = %d, "
                 "status = '%s', replica_type = %ld, trace_id = '%s', end_time = now(6) WHERE tenant_id = %lu AND "
                 "table_id = %ld AND partition_id = %ld",
                 OB_ALL_TENANT_PG_BACKUP_TASK_TNAME,
                 ip,
                 addr.get_port(),
                 ObPGBackupTaskInfo::get_status_str(status),
                 static_cast<int64_t>(replica_type),
                 trace_id_str,
                 tenant_id,
                 pkey.get_table_id(),
                 pkey.get_partition_id()))) {
    LOG_WARN("failed to append sql", K(ret), K(sql));
  } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected rows unexpected", K(ret), K(affected_rows));
  }
  return ret;
}

int ObPGBackupTaskOperator::update_pg_backup_task_status(
    common::ObISQLClient& sql_proxy, const ObPGBackupTaskInfo::BackupStatus& status, const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t tenant_id = pkey.get_tenant_id();
  int64_t affected_rows = -1;

  if (!pkey.is_valid() || status >= ObPGBackupTaskInfo::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update addr get invalid argument", K(ret), K(status), K(pkey));
  } else if (OB_FAIL(sql.append_fmt("UPDATE %s SET status = '%s', end_time = now(6) "
                                    "WHERE tenant_id = %lu AND "
                                    "table_id = %ld AND partition_id = %ld",
                 OB_ALL_TENANT_PG_BACKUP_TASK_TNAME,
                 ObPGBackupTaskInfo::get_status_str(status),
                 tenant_id,
                 pkey.get_table_id(),
                 pkey.get_partition_id()))) {
    LOG_WARN("failed to append sql", K(ret), K(sql));
  } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected rows unexpected", K(ret), K(affected_rows));
  }
  return ret;
}

int ObPGBackupTaskOperator::update_result_and_status(common::ObISQLClient& sql_proxy,
    const ObPGBackupTaskInfo::BackupStatus& status, const int32_t result, const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t tenant_id = pkey.get_tenant_id();
  int64_t affected_rows = -1;

  if (!pkey.is_valid() || status >= ObPGBackupTaskInfo::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update addr get invalid argument", K(ret), K(status), K(pkey));
  } else if (OB_FAIL(sql.append_fmt("UPDATE %s SET result = %d, "
                                    "status = '%s', end_time = now(6) WHERE tenant_id = %lu AND "
                                    "table_id = %ld AND partition_id = %ld",
                 OB_ALL_TENANT_PG_BACKUP_TASK_TNAME,
                 result,
                 ObPGBackupTaskInfo::get_status_str(status),
                 tenant_id,
                 pkey.get_table_id(),
                 pkey.get_partition_id()))) {
    LOG_WARN("failed to append sql", K(ret), K(sql));
  } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected rows unexpected", K(ret), K(affected_rows));
  }
  return ret;
}

int ObPGBackupTaskOperator::get_one_doing_pg_task(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, common::ObISQLClient& sql_proxy, common::ObIArray<ObPGBackupTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_GET_NUM = 1024;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;

    if (OB_INVALID_ID == tenant_id || incarnation < 0 || backup_set_id < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND incarnation = %ld "
                                      "AND backup_set_id = %ld AND status = 'DOING' group by task_id limit %ld",
                   OB_ALL_TENANT_PG_BACKUP_TASK_TNAME,
                   tenant_id,
                   incarnation,
                   backup_set_id,
                   MAX_GET_NUM))) {
      LOG_WARN("failed to assign sql", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
    } else if (OB_FAIL(get_pg_backup_task(tenant_id, sql, pg_task_infos, sql_proxy))) {
      LOG_WARN("failed to get pg backup task", K(ret), K(tenant_id), K(sql));
    }
  }
  return ret;
}

int ObPGBackupTaskOperator::get_pg_backup_task(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, const int64_t backup_task_id, common::ObIArray<ObPGBackupTaskItem>& items,
    common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const int64_t BATCH_CNT = 1024;
  ObArray<ObPGBackupTaskItem> tmp_items;

  if (OB_INVALID_ID == tenant_id || incarnation < 0 || backup_set_id < 0 || backup_task_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(incarnation), K(backup_set_id), K(backup_task_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND incarnation = %ld AND "
                                    "backup_set_id = %ld and task_id  = %ld",
                 OB_ALL_TENANT_PG_BACKUP_TASK_TNAME,
                 tenant_id,
                 incarnation,
                 backup_set_id,
                 backup_task_id))) {
    LOG_WARN("fail to assign sql", K(ret));
  } else if (OB_FAIL(get_pg_backup_task(tenant_id, sql, items, sql_proxy))) {
    LOG_WARN("failed to get pg backup task", K(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObPGBackupTaskOperator::get_one_pg_task(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, common::ObISQLClient& sql_proxy, ObPGBackupTaskInfo& pg_task_info)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;

    if (OB_INVALID_ID == tenant_id || incarnation < 0 || backup_set_id < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND incarnation = %ld "
                                      "AND backup_set_id = %ld limit 1",
                   OB_ALL_TENANT_PG_BACKUP_TASK_TNAME,
                   tenant_id,
                   incarnation,
                   backup_set_id))) {
      LOG_WARN("failed to assign sql", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else {
      int next_ret = result->next();
      if (OB_SUCCESS != next_ret) {
        ret = next_ret;
      } else if (OB_FAIL(extract_pg_task(result, pg_task_info))) {
        LOG_WARN("failed to extract pg task", K(ret), K(result));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          LOG_ERROR("check has not finish task get unexpected result", K(ret), K(sql));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

// TODO() change get_pending stream
int ObPGBackupTaskOperator::get_pending_pg_task(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, common::ObISQLClient& sql_proxy, common::ObIArray<ObPGBackupTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;

    if (OB_INVALID_ID == tenant_id || incarnation < 0 || backup_set_id < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND incarnation = %ld "
                                      "AND backup_set_id = %ld AND status = 'PENDING'",
                   OB_ALL_TENANT_PG_BACKUP_TASK_TNAME,
                   tenant_id,
                   incarnation,
                   backup_set_id))) {
      LOG_WARN("failed to assign sql", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
    } else if (OB_FAIL(get_pg_backup_task(tenant_id, sql, pg_task_infos, sql_proxy))) {
      LOG_WARN("failed to get pg backup task", K(ret), K(tenant_id), K(sql));
    }
  }
  return ret;
}

int ObPGBackupTaskOperator::update_result_and_status_and_statics(
    common::ObISQLClient& sql_proxy, const ObPGBackupTaskInfo& pg_task_info, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t tenant_id = pg_task_info.tenant_id_;
  affected_rows = 0;
  const ObPGBackupTaskItem::BackupStatus status = ObPGBackupTaskItem::DOING;
  if (!pg_task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update addr get invalid argument", K(ret), K(pg_task_info));
  } else if (OB_FAIL(sql.append_fmt(
                 "UPDATE %s SET result = %d, "
                 "status = '%s', end_time = now(6), partition_count = %ld, macro_block_count = %ld, "
                 "finish_partition_count = %ld, finish_macro_block_count = %ld, input_bytes = %ld, output_bytes = %ld "
                 "WHERE tenant_id = %lu AND table_id = %ld AND partition_id = %ld "
                 "and incarnation = %ld and backup_set_id = %ld and status = '%s'",
                 OB_ALL_TENANT_PG_BACKUP_TASK_TNAME,
                 pg_task_info.result_,
                 ObPGBackupTaskInfo::get_status_str(pg_task_info.status_),
                 pg_task_info.partition_count_,
                 pg_task_info.macro_block_count_,
                 pg_task_info.finish_partition_count_,
                 pg_task_info.finish_macro_block_count_,
                 pg_task_info.input_bytes_,
                 pg_task_info.output_bytes_,
                 tenant_id,
                 pg_task_info.table_id_,
                 pg_task_info.partition_id_,
                 pg_task_info.incarnation_,
                 pg_task_info.backup_set_id_,
                 ObPGBackupTaskInfo::get_status_str(status)))) {
    LOG_WARN("failed to append sql", K(ret), K(sql));
  } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret));
  }
  return ret;
}

int ObPGBackupTaskOperator::cancel_pending_tasks(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, const int64_t limit_num, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  const int32_t result = OB_CANCELED;

  if (OB_INVALID_ID == tenant_id || incarnation < 0 || backup_set_id < 0 || limit_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update addr get invalid argument", K(ret), K(tenant_id), K(incarnation), K(backup_set_id), K(limit_num));
  } else if (OB_FAIL(sql.append_fmt(
                 "UPDATE %s SET status = 'FINISH', result ='%d', end_time = now(6) "
                 "WHERE tenant_id = %lu AND incarnation = %ld and backup_set_id = %ld and status = 'PENDING' LIMIT %ld",
                 OB_ALL_TENANT_PG_BACKUP_TASK_TNAME,
                 result,
                 tenant_id,
                 incarnation,
                 backup_set_id,
                 limit_num))) {
    LOG_WARN("failed to append sql", K(ret), K(sql));
  } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret));
  } else if (affected_rows < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected rows unexpected", K(ret), K(affected_rows));
  }
  return ret;
}

template <typename T>
int ObTenantBackupInfoOperation::set_info_item(const char* name, const char* info_str, T& info)
{
  int ret = OB_SUCCESS;
  // %value and %info_str can be arbitrary values
  if (NULL == name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid name", K(ret));
  } else {
    ObBackupInfoItem* it = info.list_.get_first();
    while (OB_SUCCESS == ret && it != info.list_.get_header()) {
      if (NULL == it) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null iter", K(ret));
      } else {
        if (strncasecmp(it->name_, name, OB_MAX_COLUMN_NAME_LENGTH) == 0) {
          it->value_ = info_str;
          break;
        }
        it = it->get_next();
      }
    }
    if (OB_SUCC(ret)) {
      // ignore unknown item
      if (it == info.list_.get_header()) {
        LOG_WARN("unknown item", K(name), "value", info_str);
      }
    }
  }
  return ret;
}

template <typename T>
int ObTenantBackupInfoOperation::load_info(common::ObISQLClient& sql_client, T& info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObBackupUtils::get_backup_info_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.assign_fmt("SELECT name, value FROM %s WHERE tenant_id = %lu FOR UPDATE",
                   OB_ALL_TENANT_BACKUP_INFO_TNAME,
                   info.tenant_id_))) {
      LOG_WARN("append sql failed", K(ret));
    } else if (OB_FAIL(sql_client.read(res, info.tenant_id_, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get sql result", K(ret));
    } else {
      int64_t tmp_real_str_len = 0;
      char name[OB_INNER_TABLE_DEFAULT_KEY_LENTH] = "";
      char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH + 1] = "";
      while (OB_SUCCESS == ret && OB_SUCCESS == (ret = result->next())) {
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name", name, static_cast<int64_t>(sizeof(name)), tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(
            *result, "value", value_str, static_cast<int64_t>(sizeof(value_str)), tmp_real_str_len);
        (void)tmp_real_str_len;  // make compiler happy
        if (OB_SUCC(ret)) {
          if (OB_FAIL(set_info_item(name, value_str, info))) {
            LOG_WARN("set info item failed", K(ret), K(name), K(value_str));
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get result failed", K(ret), K(sql));
      }
    }
  }
  return ret;
}

int ObTenantBackupInfoOperation::load_base_backup_info(ObISQLClient& sql_client, ObBaseBackupInfo& info)
{
  return load_info(sql_client, info);
}

template <typename T>
int ObTenantBackupInfoOperation::insert_info(ObISQLClient& sql_client, T& info)
{
  int ret = OB_SUCCESS;
  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info));
  } else {
    DLIST_FOREACH(it, info.list_)
    {
      if (OB_FAIL(update_info_item(sql_client, info.tenant_id_, *it))) {
        LOG_WARN("insert item failed", K(ret), "tenant_id", info.tenant_id_, "item", *it);
        break;
      }
    }
  }
  return ret;
}

int ObTenantBackupInfoOperation::insert_base_backup_info(ObISQLClient& sql_client, ObBaseBackupInfo& info)
{
  int ret = OB_SUCCESS;
  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info));
  } else if (OB_FAIL(insert_info(sql_client, info))) {
    LOG_WARN("insert info failed", K(ret), K(info));
  }
  return ret;
}

int ObTenantBackupInfoOperation::load_info_item(
    common::ObISQLClient& sql_client, const uint64_t tenant_id, ObBackupInfoItem& item, const bool need_lock)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;

    if (OB_INVALID_ID == tenant_id || !item.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(item), K(tenant_id));
    } else if (OB_FAIL(ObBackupUtils::get_backup_info_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.assign_fmt("SELECT name, value FROM %s WHERE name = '%s' AND tenant_id = %lu",
                   OB_ALL_TENANT_BACKUP_INFO_TNAME,
                   item.name_,
                   tenant_id))) {
      LOG_WARN("assign sql failed", K(ret));
    } else if (need_lock && OB_FAIL(sql.append(" FOR UPDATE"))) {
      LOG_WARN("failed to append lock for sql", K(ret), K(sql));
    } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get sql result", K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_BACKUP_INFO_NOT_EXIST;
        LOG_WARN("backup info is not exist yet, wait later", K(ret), K(tenant_id), K(sql));
      } else {
        LOG_WARN("failed to get next", K(ret));
      }
    } else {
      int64_t tmp_real_str_len = 0;
      char name[OB_INNER_TABLE_DEFAULT_KEY_LENTH] = "";
      char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = "";
      EXTRACT_STRBUF_FIELD_MYSQL(*result, "name", name, static_cast<int64_t>(sizeof(name)), tmp_real_str_len);
      EXTRACT_STRBUF_FIELD_MYSQL(
          *result, "value", value_str, static_cast<int64_t>(sizeof(value_str)), tmp_real_str_len);
      (void)tmp_real_str_len;  // make compiler happy
      if (OB_SUCC(ret)) {
        if (0 != strcmp(name, item.name_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to select item", K(ret), K(item), K(name));
        } else {
          MEMCPY(item.value_.ptr(), value_str, sizeof(value_str));
        }
      }
    }
  }
  return ret;
}

int ObTenantBackupInfoOperation::get_backup_snapshot_version(
    common::ObISQLClient& sql_proxy, const uint64_t tenant_id, int64_t& backup_snapshot_version)
{
  int ret = OB_SUCCESS;
  const bool need_lock = false;
  ObBackupInfoItem item;
  item.name_ = "backup_snapshot_version";
  backup_snapshot_version = 0;

  if (OB_FAIL(load_info_item(sql_proxy, tenant_id, item, need_lock))) {
    if (OB_BACKUP_INFO_NOT_EXIST == ret) {
      backup_snapshot_version = 0;
      LOG_WARN("tenant backup info not exist", K(ret), K(backup_snapshot_version));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get backup snapshot_version", K(ret));
    }
  } else if (OB_FAIL(item.get_int_value(backup_snapshot_version))) {
    LOG_WARN("failed to get int value", K(ret), K(item));
  }

  return ret;
}

int ObTenantBackupInfoOperation::get_backup_schema_version(
    common::ObISQLClient& sql_proxy, const uint64_t tenant_id, int64_t& backup_schema_version)
{
  int ret = OB_SUCCESS;
  const bool need_lock = false;
  ObBackupInfoItem item;
  item.name_ = "backup_schema_version";
  backup_schema_version = 0;

  if (OB_FAIL(load_info_item(sql_proxy, tenant_id, item, need_lock))) {
    if (OB_BACKUP_INFO_NOT_EXIST == ret) {
      backup_schema_version = 0;
      LOG_WARN("tenant backup info not exist", K(ret), K(backup_schema_version));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get backup schema version", K(ret));
    }
  } else if (OB_FAIL(item.get_int_value(backup_schema_version))) {
    LOG_WARN("failed to get int value", K(ret), K(item));
  }

  return ret;
}

int ObTenantBackupInfoOperation::get_tenant_name_backup_schema_version(
    common::ObISQLClient& sql_proxy, int64_t& backup_schema_version)
{
  int ret = OB_SUCCESS;
  const bool need_lock = false;
  ObBackupInfoItem item;
  item.name_ = OB_STR_TENANT_NAME_BACKUP_SCHEMA_VERSION;
  backup_schema_version = 0;

  if (OB_FAIL(load_info_item(sql_proxy, OB_SYS_TENANT_ID, item, need_lock))) {
    if (OB_BACKUP_INFO_NOT_EXIST == ret) {
      backup_schema_version = 0;
      LOG_WARN("tenant backup info not exist", K(ret), K(backup_schema_version));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tenant_name_backup_schema_version", K(ret));
    }
  } else if (OB_FAIL(item.get_int_value(backup_schema_version))) {
    LOG_WARN("failed to get int value", K(ret), K(item));
  }

  return ret;
}

int ObTenantBackupInfoOperation::update_tenant_name_backup_schema_version(
    common::ObISQLClient& sql_client, const int64_t backup_schema_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  // %zone can be empty
  if (backup_schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(backup_schema_version));
  } else if (OB_FAIL(sql.assign_fmt("replace into %s(tenant_id, name, value) values(%lu, '%s', %ld)",
                 OB_ALL_TENANT_BACKUP_INFO_TNAME,
                 OB_SYS_TENANT_ID,
                 OB_STR_TENANT_NAME_BACKUP_SCHEMA_VERSION,
                 backup_schema_version))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(sql_client.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(sql));
  } else {
    LOG_INFO("succeed to update_tenant_name_backup_schema_version", K(backup_schema_version));
  }
  return ret;
}

int ObTenantBackupInfoOperation::clean_backup_scheduler_leader(
    common::ObISQLClient& sql_client, const uint64_t tenant_id, const common::ObAddr& scheduler_leader)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char scheduler_leader_str[MAX_IP_PORT_LENGTH] = "";

  if (tenant_id != OB_SYS_TENANT_ID || !scheduler_leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(scheduler_leader));
  } else if (OB_FAIL(scheduler_leader.ip_port_to_string(scheduler_leader_str, MAX_IP_PORT_LENGTH))) {
    LOG_WARN("failed to add addr to buf", K(ret), K(scheduler_leader));
  } else if (OB_FAIL(sql.assign_fmt("update %s set value = '' where name = '%s' and value='%s'",
                 OB_ALL_TENANT_BACKUP_INFO_TNAME,
                 OB_STR_BACKUP_SCHEDULER_LEADER,
                 scheduler_leader_str))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(sql));
  } else if (0 != affected_rows) {
    FLOG_INFO("succeed to clean_backup_scheduler_leader", K(scheduler_leader_str), K(sql), K(affected_rows));
  }
  return ret;
}

int ObTenantBackupInfoOperation::update_info_item(
    common::ObISQLClient& sql_client, const uint64_t tenant_id, const ObBackupInfoItem& item)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  // %zone can be empty
  if (OB_INVALID_ID == tenant_id || !item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET value = '%s', gmt_modified = now(6) "
                                    "WHERE tenant_id = %lu AND name = '%s'",
                 OB_ALL_TENANT_BACKUP_INFO_TNAME,
                 item.value_.ptr(),
                 tenant_id,
                 item.name_))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(sql));
  } else if (!(is_single_row(affected_rows) || is_zero_row(affected_rows))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
  } else {
    LOG_INFO("execute sql success", K(sql));
  }
  return ret;
}

int ObTenantBackupInfoOperation::remove_base_backup_info(ObISQLClient& sql_client, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t item_cnt = 0;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(
                 sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu", OB_ALL_TENANT_BACKUP_INFO_TNAME, tenant_id))) {
    LOG_WARN("sql assign_fmt failed", K(ret));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(sql), K(ret));
  } else if (OB_FAIL(get_backup_info_item_count(item_cnt))) {
    LOG_WARN("get zone item count failed", K(ret));
  } else if (item_cnt != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows not right", "expected affected_rows", item_cnt, K(affected_rows), K(ret));
  }
  return ret;
}

int ObTenantBackupInfoOperation::get_backup_info_item_count(int64_t& cnt)
{
  int ret = OB_SUCCESS;
  ObMalloc alloc(ObModIds::OB_TEMP_VARIABLES);
  ObPtrGuard<ObBaseBackupInfo> base_backup_info_guard(alloc);
  if (OB_FAIL(base_backup_info_guard.init())) {
    LOG_WARN("init temporary variable failed", K(ret));
  } else if (NULL == base_backup_info_guard.ptr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null zone info ptr", K(ret));
  } else {
    cnt = base_backup_info_guard.ptr()->get_item_count();
  }
  return ret;
}

int ObTenantBackupInfoOperation::insert_info_item(
    common::ObISQLClient& sql_client, const uint64_t tenant_id, const ObBackupInfoItem& item)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_INVALID_ID == tenant_id || !item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (tenant_id, name, value) VALUES(%lu, '%s', '%s')",
                 OB_ALL_TENANT_BACKUP_INFO_TNAME,
                 tenant_id,
                 item.name_,
                 item.value_.ptr()))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
  } else {
    LOG_INFO("execute sql success", K(sql));
  }
  return ret;
}

int ObTenantBackupInfoOperation::remove_info_item(
    common::ObISQLClient& sql_client, const uint64_t tenant_id, const char* name)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  if (OB_INVALID_ID == tenant_id || OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), KP(name));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE name = '%s'", OB_ALL_TENANT_BACKUP_INFO_TNAME, name))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(sql));
  }
  return ret;
}

/*not use anymore*/
int ObBackupTaskHistoryOperator::get_tenant_backup_task(const uint64_t tenant_id, const int64_t backup_set_id,
    const int64_t incarnation, common::ObISQLClient& proxy, ObTenantBackupTaskItem& item)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  item.reset();
  ObArray<ObTenantBackupTaskItem> items;

  if (OB_INVALID_ID == tenant_id || backup_set_id < 0 || incarnation < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task get invalid argument", K(ret), K(tenant_id), K(backup_set_id), K(incarnation));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                                    "WHERE tenant_id = %lu AND backup_set_id = %ld AND incarnation = %ld FOR UPDATE",
                 OB_ALL_BACKUP_TASK_HISTORY_TNAME,
                 tenant_id,
                 backup_set_id,
                 incarnation))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id), K(backup_set_id), K(incarnation));
  } else if (OB_FAIL(
                 ObITenantBackupTaskOperator::get_tenant_backup_history_task(OB_SYS_TENANT_ID, sql, items, proxy))) {
    LOG_WARN("fail to get pg backup task", K(ret), K(sql), K(tenant_id), K(backup_set_id), K(incarnation));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", K(ret), K(items.count()));
  } else if (0 == items.count()) {
    // do nothing
    ret = OB_INVALID_BACKUP_SET_ID;
    LOG_WARN("backup set do not exist", K(ret), K(tenant_id), K(backup_set_id), K(incarnation), K(sql));
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObBackupTaskHistoryOperator::get_need_mark_deleted_backup_tasks(const uint64_t tenant_id,
    const int64_t backup_set_id, const int64_t incarnation, const ObBackupDest& backup_dest, const bool for_update,
    common::ObISQLClient& proxy, common::ObIArray<ObTenantBackupTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  items.reset();
  char backup_dest_str[share::OB_MAX_BACKUP_DEST_LENGTH] = "";

  if (OB_INVALID_ID == tenant_id || backup_set_id < 0 || incarnation < 0 || !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get need mark deleted task get invalid argument",
        K(ret),
        K(tenant_id),
        K(backup_set_id),
        K(incarnation),
        K(backup_dest));
  } else if (OB_FAIL(backup_dest.get_backup_dest_str(backup_dest_str, share::OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", K(ret), K(backup_dest));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                                    "WHERE tenant_id = %lu AND incarnation = %ld  AND (backup_set_id = %ld "
                                    "OR prev_full_backup_set_id = %ld) AND backup_dest = '%s'",
                 OB_ALL_BACKUP_TASK_HISTORY_TNAME,
                 tenant_id,
                 incarnation,
                 backup_set_id,
                 backup_set_id,
                 backup_dest_str))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id), K(backup_set_id), K(incarnation));
  } else if (for_update && OB_FAIL(sql.append(" FOR UPDATE"))) {
    LOG_WARN("failed to append update sql", K(ret), K(sql));
  } else if (OB_FAIL(
                 ObITenantBackupTaskOperator::get_tenant_backup_history_task(OB_SYS_TENANT_ID, sql, items, proxy))) {
    LOG_WARN("fail to get pg backup task", K(ret), K(sql), K(tenant_id), K(backup_set_id), K(incarnation));
  }
  return ret;
}

int ObBackupTaskHistoryOperator::mark_backup_task_deleted(
    const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;

  if (OB_INVALID_ID == tenant_id || backup_set_id < 0 || incarnation < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mark backup task deleted get invalid argument", K(ret), K(tenant_id), K(backup_set_id), K(incarnation));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET is_mark_deleted = true "
                                    "WHERE tenant_id = %lu AND incarnation = %ld AND backup_set_id = %ld ",
                 OB_ALL_BACKUP_TASK_HISTORY_TNAME,
                 tenant_id,
                 incarnation,
                 backup_set_id))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id), K(backup_set_id), K(incarnation));
  } else if (OB_FAIL(proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else if (1 != affected_rows && 0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", K(ret), K(tenant_id), K(backup_set_id), K(affected_rows));
  }
  return ret;
}

int ObBackupTaskHistoryOperator::delete_marked_task(const uint64_t tenant_id, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete mark backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND is_mark_deleted = true",
                 OB_ALL_BACKUP_TASK_HISTORY_TNAME,
                 tenant_id))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else if (OB_FAIL(proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql), K(tenant_id));
  }
  return ret;
}

int ObBackupTaskHistoryOperator::insert_task(const ObTenantBackupTaskItem& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const bool need_fill_mark_deleted_item = true;

  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObITenantBackupTaskOperator::fill_task_history_item(item, need_fill_mark_deleted_item, dml))) {
      LOG_WARN("failed to fill on item", K(ret), K(item));
    } else if (OB_FAIL(dml.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", K(ret));
    } else if (OB_FAIL(dml.splice_insert_update_sql(OB_ALL_BACKUP_TASK_HISTORY_TNAME, sql))) {
      LOG_WARN("failed to splice insert update sql", K(ret), K(item));
    } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else {
      LOG_INFO("batch report tenant backup task history", K(sql), K(affected_rows));
    }
  }
  return ret;
}

int ObBackupTaskHistoryOperator::remove_task(
    const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id, ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (tenant_id == OB_INVALID_ARGUMENT || backup_set_id < 0 || incarnation < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("remove task get invalid argument", K(ret), K(tenant_id), K(backup_set_id), K(incarnation));
  } else if (OB_FAIL(remove_one_item(tenant_id, incarnation, backup_set_id, sql_proxy))) {
    LOG_WARN("failed to remove on item", K(ret), K(tenant_id), K(backup_set_id));
  }
  return ret;
}

int ObBackupTaskHistoryOperator::remove_one_item(
    const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id)) || OB_FAIL(dml.add_pk_column("incarnation", incarnation)) ||
      OB_FAIL(dml.add_pk_column("backup_set_id", backup_set_id))) {
    LOG_WARN("fail to add column", K(ret), K(tenant_id), K(backup_set_id), K(incarnation));
  } else {
    ObDMLExecHelper exec(sql_proxy, OB_SYS_TENANT_ID);
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_delete(OB_ALL_BACKUP_TASK_HISTORY_TNAME, dml, affected_rows))) {
      LOG_WARN("fail to exec delete", K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(tenant_id), K(backup_set_id), K(affected_rows));
    }
  }
  return ret;
}

int ObBackupTaskHistoryOperator::get_tenant_backup_tasks(
    const uint64_t tenant_id, common::ObISQLClient& proxy, common::ObIArray<ObTenantBackupTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  items.reset();

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                                    "WHERE tenant_id = %lu",
                 OB_ALL_BACKUP_TASK_HISTORY_TNAME,
                 tenant_id))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else if (OB_FAIL(
                 ObITenantBackupTaskOperator::get_tenant_backup_history_task(OB_SYS_TENANT_ID, sql, items, proxy))) {
    LOG_WARN("fail to get pg backup task", K(ret), K(sql), K(tenant_id));
  }
  return ret;
}

int ObBackupTaskHistoryOperator::get_delete_backup_set_tasks(const uint64_t tenant_id, const int64_t backup_set_id,
    common::ObISQLClient& proxy, common::ObIArray<ObTenantBackupTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  items.reset();

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                                    "WHERE tenant_id = %lu and backup_set_id = %ld and backup_type = 'D' FOR UPDATE",
                 OB_ALL_BACKUP_TASK_HISTORY_TNAME,
                 tenant_id,
                 backup_set_id))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else if (OB_FAIL(
                 ObITenantBackupTaskOperator::get_tenant_backup_history_task(OB_SYS_TENANT_ID, sql, items, proxy))) {
    LOG_WARN("fail to get pg backup task", K(ret), K(sql), K(tenant_id));
  }
  return ret;
}

int ObBackupTaskHistoryOperator::get_expired_backup_tasks(const uint64_t tenant_id, const int64_t expired_time,
    common::ObISQLClient& proxy, common::ObIArray<ObTenantBackupTaskItem>& items)
{
  int ret = OB_SUCCESS;
  UNUSED(expired_time);
  ObSqlString sql;
  items.reset();

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                                    "WHERE tenant_id = %lu and backup_type = 'D' FOR UPDATE",
                 OB_ALL_BACKUP_TASK_HISTORY_TNAME,
                 tenant_id))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else if (OB_FAIL(
                 ObITenantBackupTaskOperator::get_tenant_backup_history_task(OB_SYS_TENANT_ID, sql, items, proxy))) {
    LOG_WARN("fail to get pg backup task", K(ret), K(sql), K(tenant_id));
  }
  return ret;
}

int ObBackupTaskHistoryOperator::get_mark_deleted_backup_tasks(
    const uint64_t tenant_id, common::ObISQLClient& proxy, common::ObIArray<ObTenantBackupTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  items.reset();

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get need mark deleted task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                                    "WHERE tenant_id = %lu AND is_mark_deleted = true",
                 OB_ALL_BACKUP_TASK_HISTORY_TNAME,
                 tenant_id))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else if (OB_FAIL(
                 ObITenantBackupTaskOperator::get_tenant_backup_history_task(OB_SYS_TENANT_ID, sql, items, proxy))) {
    LOG_WARN("fail to get pg backup task", K(ret), K(sql), K(tenant_id));
  }
  return ret;
}

int ObBackupTaskHistoryOperator::get_all_tenant_backup_set_matches(const int64_t backup_set_id,
    const bool is_smaller_equal,  // use equal or user smaller equal
    common::ObISQLClient& sql_proxy, common::ObIArray<uint64_t>& tenant_list)
{
  int ret = OB_SUCCESS;
  tenant_list.reset();
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (is_smaller_equal &&
        OB_FAIL(sql.assign_fmt("select tenant_id from %s where backup_set_id <= %ld group by tenant_id",
            OB_ALL_BACKUP_TASK_HISTORY_TNAME,
            backup_set_id))) {
      LOG_WARN("failed to init backup info sql", KR(ret));
    } else if (!is_smaller_equal && OB_FAIL(sql.assign_fmt("select tenant_id from %s where backup_set_id = %ld",
                                        OB_ALL_BACKUP_TASK_HISTORY_TNAME,
                                        backup_set_id))) {
      LOG_WARN("failed to init backup info sql", KR(ret));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is NULL", KR(ret), K(sql));
    } else {
      while (OB_SUCC(ret)) {
        uint64_t tenant_id = OB_INVALID_ID;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get next row", KR(ret));
          }
        } else {
          EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, int64_t);
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(tenant_list.push_back(tenant_id))) {
            LOG_WARN("failed to push back", KR(ret), K(tenant_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupTaskHistoryOperator::get_task_in_time_range(const int64_t start_time, const int64_t end_time,
    common::ObISQLClient& proxy, common::ObIArray<ObTenantBackupTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  items.reset();

  if (start_time < 0 || end_time < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get task in time range get invalid argument", K(ret), K(start_time), K(end_time));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT * FROM %s WHERE time_to_usec(start_time) > %ld AND time_to_usec(end_time) < %ld",
                 OB_ALL_BACKUP_TASK_HISTORY_TNAME,
                 start_time,
                 end_time))) {
    LOG_WARN("failed to assign sql", K(ret), K(start_time), K(end_time));
  } else if (OB_FAIL(
                 ObITenantBackupTaskOperator::get_tenant_backup_history_task(OB_SYS_TENANT_ID, sql, items, proxy))) {
    LOG_WARN("failed to get tenant backup task in time range", K(ret), K(sql));
  }
  return ret;
}

int ObBackupTaskHistoryOperator::get_tenant_backup_task(const uint64_t tenant_id, const int64_t backup_set_id,
    const bool for_update, common::ObISQLClient& proxy, ObTenantBackupTaskItem& item)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  item.reset();
  ObArray<ObTenantBackupTaskItem> items;

  if (OB_INVALID_ID == tenant_id || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task get invalid argument", K(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                                    "WHERE tenant_id = %lu AND backup_set_id = %ld",
                 OB_ALL_BACKUP_TASK_HISTORY_TNAME,
                 tenant_id,
                 backup_set_id))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id), K(backup_set_id));
  } else if (for_update && OB_FAIL(sql.append(" FOR UPDATE"))) {
    LOG_WARN("failed to apend update sql", K(ret));
  } else if (OB_FAIL(
                 ObITenantBackupTaskOperator::get_tenant_backup_history_task(OB_SYS_TENANT_ID, sql, items, proxy))) {
    LOG_WARN("fail to get pg backup task", K(ret), K(sql), K(tenant_id), K(backup_set_id));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", K(ret), K(items.count()));
  } else if (0 == items.count()) {
    // do nothing
    ret = OB_INVALID_BACKUP_SET_ID;
    LOG_WARN("backup set do not exist", K(ret), K(tenant_id), K(backup_set_id));
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObBackupTaskHistoryOperator::get_tenant_max_succeed_backup_task(
    const uint64_t tenant_id, common::ObISQLClient& proxy, ObTenantBackupTaskItem& item)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  item.reset();
  ObArray<ObTenantBackupTaskItem> items;

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                                    "WHERE tenant_id = %lu and result = 0 ORDER BY gmt_create DESC LIMIT 1",
                 OB_ALL_BACKUP_TASK_HISTORY_TNAME,
                 tenant_id))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObITenantBackupTaskOperator::get_tenant_backup_task(OB_SYS_TENANT_ID, sql, items, proxy))) {
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

int ObBackupTaskHistoryOperator::get_all_tenant_backup_tasks(
    common::ObISQLClient& proxy, common::ObIArray<ObTenantBackupTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  items.reset();
  if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s ", OB_ALL_BACKUP_TASK_HISTORY_TNAME))) {
    LOG_WARN("fail to assign sql", K(ret));
  } else if (OB_FAIL(
                 ObITenantBackupTaskOperator::get_tenant_backup_history_task(OB_SYS_TENANT_ID, sql, items, proxy))) {
    LOG_WARN("fail to get pg backup task", K(ret), K(sql));
  }
  return ret;
}

int ObBackupTaskHistoryOperator::get_tenant_ids_with_backup_set_id(
    const int64_t backup_set_id, common::ObISQLClient& sql_proxy, common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  ObSqlString sql;
  const int64_t tenant_id = OB_SYS_TENANT_ID;
  if (backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant ids with backup set id get invalid argument", K(ret), K(backup_set_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT DISTINCT(tenant_id) FROM %s WHERE backup_set_id = %ld",
                 OB_ALL_BACKUP_TASK_HISTORY_TNAME,
                 backup_set_id))) {
    LOG_WARN("fail to assign sql", K(ret), K(backup_set_id));
  } else if (OB_FAIL(ObITenantBackupTaskOperator::get_tenant_ids(tenant_id, sql, tenant_ids, sql_proxy))) {
    LOG_WARN("failed to get tenant ids", K(ret), K(sql));
  }
  return ret;
}

int ObBackupTaskHistoryOperator::get_tenant_ids_with_snapshot_version(
    const int64_t snapshot_version, common::ObISQLClient& sql_proxy, common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  ObSqlString sql;
  const int64_t tenant_id = OB_SYS_TENANT_ID;
  if (snapshot_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant ids with backup set id get invalid argument", K(ret), K(snapshot_version));
  } else if (OB_FAIL(sql.assign_fmt("SELECT DISTINCT(tenant_id) FROM %s WHERE snapshot_version <= %ld",
                 OB_ALL_BACKUP_TASK_HISTORY_TNAME,
                 snapshot_version))) {
    LOG_WARN("fail to assign sql", K(ret), K(snapshot_version));
  } else if (OB_FAIL(ObITenantBackupTaskOperator::get_tenant_ids(tenant_id, sql, tenant_ids, sql_proxy))) {
    LOG_WARN("failed to get tenant ids", K(ret), K(sql));
  }

  return ret;
}

int ObBackupTaskHistoryOperator::update_backup_task_info(
    const ObTenantBackupTaskInfo& backup_task_info, const bool fill_mark_delete_item, common::ObISQLClient& sql_proxy)
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
  } else if (OB_FAIL(
                 ObITenantBackupTaskOperator::fill_task_history_item(backup_task_info, fill_mark_delete_item, dml))) {
    LOG_WARN("failed to fill task history item", K(ret), K(backup_task_info));
  } else if (OB_FAIL(dml.splice_column_names(columns))) {
    LOG_WARN("failed to splice column names", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_TASK_HISTORY_TNAME, sql))) {
    LOG_WARN("failed to splice insert sql", KR(ret), K(backup_task_info));
  } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(backup_task_info));
  } else if (1 != affected_rows && 0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows));
  } else {
    LOG_INFO("update tenant backup task info success", K(sql));
  }
  return ret;
}

int ObTenantBackupCleanInfoOperator::fill_one_item(const ObBackupCleanInfo& clean_info, share::ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  int64_t parameter = 0;
  char parameter_str[common::OB_INNER_TABLE_BACKUP_CLEAN_PARAMETER_LENGTH] = "";
  if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(clean_info));
  } else if (OB_FAIL(clean_info.get_clean_parameter(parameter))) {
    LOG_WARN("failed to get clean parameter", K(ret), K(clean_info));
  } else if (OB_FAIL(databuff_printf(parameter_str, OB_INNER_TABLE_BACKUP_CLEAN_PARAMETER_LENGTH, "%ld", parameter))) {
    LOG_WARN("failed to set parameter", K(ret), K(parameter));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", clean_info.tenant_id_)) ||
             OB_FAIL(dml.add_column("job_id", clean_info.job_id_)) ||
             OB_FAIL(dml.add_time_column("start_time", clean_info.start_time_)) ||
             OB_FAIL(dml.add_time_column("end_time", clean_info.end_time_)) ||
             OB_FAIL(dml.add_column("incarnation", clean_info.incarnation_)) ||
             OB_FAIL(dml.add_column("type", ObBackupCleanType::get_str(clean_info.type_))) ||
             OB_FAIL(dml.add_column("status", ObBackupCleanInfoStatus::get_str(clean_info.status_))) ||
             OB_FAIL(dml.add_column("parameter", parameter_str)) ||
             OB_FAIL(dml.add_column("error_msg", clean_info.error_msg_.ptr())) ||
             OB_FAIL(dml.add_column("comment", clean_info.comment_.ptr())) ||
             OB_FAIL(dml.add_column("clog_gc_snapshot", clean_info.clog_gc_snapshot_)) ||
             OB_FAIL(dml.add_column("result", clean_info.result_)) ||
             OB_FAIL(dml.add_column("copy_id", clean_info.copy_id_))) {
    LOG_WARN("fail to fill backup clean info", K(ret), K(clean_info));
  }
  return ret;
}

int ObTenantBackupCleanInfoOperator::get_tenant_clean_info(const uint64_t tenant_id, const common::ObSqlString& sql,
    common::ObIArray<ObBackupCleanInfo>& clean_infos, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy::MySQLResult res;
  sqlclient::ObMySQLResult* result = NULL;

  if (OB_UNLIKELY(!sql.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(sql));
  } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, query result must not be NULL", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      ObBackupCleanInfo clean_info;
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next row", K(ret));
        }
      } else if (OB_FAIL(extract_backup_clean_info(result, clean_info))) {
        LOG_WARN("failed to extract backup clean info", K(ret));
      } else if (OB_FAIL(clean_infos.push_back(clean_info))) {
        LOG_WARN("failed to push clean info into array", K(ret), K(clean_info));
      }
    }
  }
  return ret;
}

int ObTenantBackupCleanInfoOperator::get_tenant_clean_info(
    const uint64_t tenant_id, const bool for_update, ObBackupCleanInfo& clean_info, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArray<ObBackupCleanInfo> clean_infos;

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                                    "WHERE tenant_id = %lu",
                 OB_ALL_TENANT_BACKUP_CLEAN_INFO_TNAME,
                 clean_info.tenant_id_))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else if (for_update && OB_FAIL(sql.append(" FOR UPDATE"))) {
    LOG_WARN("failed to apend for update", K(ret));
  } else if (OB_FAIL(get_tenant_clean_info(tenant_id, sql, clean_infos, proxy))) {
    LOG_WARN("fail to get tenant clean info", K(ret), K(sql), K(tenant_id));
  } else if (clean_infos.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", K(ret), K(clean_infos.count()));
  } else if (0 == clean_infos.count()) {
    // do nothing
    ret = OB_BACKUP_CLEAN_INFO_NOT_EXIST;
  } else if (clean_info.tenant_id_ != clean_infos.at(0).tenant_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get tenant clean info", K(ret), K(clean_info), K(clean_infos));
  } else {
    clean_info = clean_infos.at(0);
  }
  return ret;
}

int ObTenantBackupCleanInfoOperator::insert_clean_info(
    const uint64_t tenant_id, const ObBackupCleanInfo& clean_info, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;

  if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(clean_info));
  } else if (OB_FAIL(fill_one_item(clean_info, dml))) {
    LOG_WARN("failed to fill on item", K(ret), K(clean_info));
  } else if (OB_FAIL(dml.splice_column_names(columns))) {
    LOG_WARN("failed to splice column names", K(ret));
  } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_TENANT_BACKUP_CLEAN_INFO_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret), K(clean_info));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows));
  } else {
    LOG_INFO("insert tenant backup clean info", K(sql));
  }
  return ret;
}

int ObTenantBackupCleanInfoOperator::update_clean_info(
    const uint64_t tenant_id, const ObBackupCleanInfo& clean_info, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;

  if (!clean_info.is_valid() || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(clean_info), K(tenant_id));
  } else if (OB_FAIL(fill_one_item(clean_info, dml))) {
    LOG_WARN("failed to fill on item", K(ret), K(clean_info));
  } else if (OB_FAIL(dml.splice_column_names(columns))) {
    LOG_WARN("failed to splice column names", K(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_TENANT_BACKUP_CLEAN_INFO_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret), K(clean_info));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret));
  } else if (1 != affected_rows && 0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows));
  } else {
    LOG_INFO("update tenant backup clean info", K(sql));
  }
  return ret;
}

int ObTenantBackupCleanInfoOperator::remove_clean_info(
    const uint64_t tenant_id, const ObBackupCleanInfo& clean_info, ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (tenant_id == OB_INVALID_ID || !clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("remove task get invalid argument", K(ret), K(tenant_id), K(clean_info));
  } else if (OB_FAIL(remove_one_item(tenant_id, clean_info, sql_proxy))) {
    LOG_WARN("failed to remove on item", K(ret), K(tenant_id), K(clean_info));
  }
  return ret;
}

int ObTenantBackupCleanInfoOperator::remove_one_item(
    const uint64_t tenant_id, const ObBackupCleanInfo& clean_info, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", clean_info.tenant_id_))) {
    LOG_WARN("fail to add column", K(ret), K(tenant_id), K(clean_info));
  } else {
    ObDMLExecHelper exec(sql_proxy, tenant_id);
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_delete(OB_ALL_TENANT_BACKUP_CLEAN_INFO_TNAME, dml, affected_rows))) {
      LOG_WARN("fail to exec delete", K(ret));
    }
  }
  return ret;
}

int ObTenantBackupCleanInfoOperator::get_clean_info_status(
    const uint64_t tenant_id, common::ObISQLClient& proxy, ObBackupCleanInfoStatus::STATUS& status)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::MySQLResult res;
  sqlclient::ObMySQLResult* result = NULL;
  char status_str[common::OB_DEFAULT_STATUS_LENTH] = "";

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT status FROM %s "
                                    "WHERE tenant_id = %lu",
                 OB_ALL_TENANT_BACKUP_CLEAN_INFO_TNAME,
                 tenant_id))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else if (OB_FAIL(proxy.read(res, tenant_id, sql.ptr()))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, query result must not be NULL", K(ret));
  } else if (OB_FAIL(result->next())) {
    LOG_WARN("fail to get next row", K(ret));
  } else {
    int64_t tmp_real_str_len = 0;
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "status", status_str, OB_DEFAULT_STATUS_LENTH, tmp_real_str_len);
    UNUSED(tmp_real_str_len);
    status = ObBackupCleanInfoStatus::get_status(status_str);
    if (OB_ITER_END != result->next()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get backup clean info status has multi value", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObTenantBackupCleanInfoOperator::get_deleted_tenant_clean_infos(
    common::ObISQLClient& proxy, common::ObIArray<ObBackupCleanInfo>& deleted_tenant_clean_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  deleted_tenant_clean_infos.reset();
  const uint64_t tenant_id = OB_SYS_TENANT_ID;

  if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                             "WHERE tenant_id != %lu",
          OB_ALL_TENANT_BACKUP_CLEAN_INFO_TNAME,
          tenant_id))) {
    LOG_WARN("fail to assign sql", K(ret));
  } else if (OB_FAIL(get_tenant_clean_info(tenant_id, sql, deleted_tenant_clean_infos, proxy))) {
    LOG_WARN("fail to get tenant clean info", K(ret), K(sql), K(tenant_id));
  }
  return ret;
}

int ObTenantBackupCleanInfoOperator::extract_backup_clean_info(
    sqlclient::ObMySQLResult* result, ObBackupCleanInfo& clean_info)
{
  int ret = OB_SUCCESS;
  int64_t parameter = 0;
  char type[common::OB_INNER_TABLE_BACKUP_CLEAN_TYPE_LENGTH] = "";
  char status[common::OB_DEFAULT_STATUS_LENTH] = "";
  char parameter_str[common::OB_INNER_TABLE_BACKUP_CLEAN_PARAMETER_LENGTH] = "";
  char error_msg[common::OB_MAX_ERROR_MSG_LEN] = "";
  char comment[common::MAX_TABLE_COMMENT_LENGTH] = "";

  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extract backup clean info get invalid argument", K(ret), KP(result));
  } else {
    int64_t tmp_real_str_len = 0;
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", clean_info.tenant_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "incarnation", clean_info.incarnation_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "job_id", clean_info.job_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "clog_gc_snapshot", clean_info.clog_gc_snapshot_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "result", clean_info.result_, int32_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "copy_id", clean_info.copy_id_, int64_t);

    EXTRACT_STRBUF_FIELD_MYSQL(*result, "type", type, OB_INNER_TABLE_BACKUP_CLEAN_TYPE_LENGTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "status", status, OB_DEFAULT_STATUS_LENTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(
        *result, "parameter", parameter_str, OB_INNER_TABLE_BACKUP_CLEAN_PARAMETER_LENGTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "error_msg", error_msg, OB_MAX_ERROR_MSG_LEN, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "comment", comment, MAX_TABLE_COMMENT_LENGTH, tmp_real_str_len);
    UNUSED(tmp_real_str_len);
    clean_info.type_ = ObBackupCleanType::get_type(type);
    clean_info.status_ = ObBackupCleanInfoStatus::get_status(status);
    int64_t tmp = 0;
    char* endptr = NULL;
    tmp = strtoll(parameter_str, &endptr, 0);
    if ('\0' != *endptr) {
      ret = OB_INVALID_DATA;
      LOG_ERROR("invalid data, is not int value", K(ret), K(tmp), K(parameter_str), K(endptr));
    } else {
      parameter = tmp;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(result->get_timestamp("start_time", NULL, clean_info.start_time_))) {
      LOG_WARN("failed to get start time", K(ret));
    } else if (OB_FAIL(result->get_timestamp("end_time", NULL, clean_info.end_time_))) {
      LOG_WARN("failed to get end time", K(ret));
    } else if (OB_FAIL(clean_info.set_clean_parameter(parameter))) {
      LOG_WARN("failed to set clean parameter", K(ret), K(parameter), K(clean_info));
    } else if (OB_FAIL(clean_info.error_msg_.assign(error_msg))) {
      LOG_WARN("failed to assign error msg", K(ret), K(clean_info), K(error_msg));
    } else if (OB_FAIL(clean_info.comment_.assign(comment))) {
      LOG_WARN("failed to assign comment", K(ret), K(clean_info), K(comment));
    }
  }
  return ret;
}

int ObBackupCleanInfoHistoryOperator::fill_one_item(const ObBackupCleanInfo& clean_info, share::ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  int64_t parameter = 0;
  char parameter_str[common::OB_INNER_TABLE_BACKUP_CLEAN_PARAMETER_LENGTH] = "";
  if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(clean_info));
  } else if (OB_FAIL(clean_info.get_clean_parameter(parameter))) {
    LOG_WARN("failed to get clean parameter", K(ret), K(clean_info));
  } else if (OB_FAIL(databuff_printf(parameter_str, OB_INNER_TABLE_BACKUP_CLEAN_PARAMETER_LENGTH, "%ld", parameter))) {
    LOG_WARN("failed to set parameter", K(ret), K(parameter));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", clean_info.tenant_id_)) ||
             OB_FAIL(dml.add_pk_column("job_id", clean_info.job_id_)) ||
             OB_FAIL(dml.add_time_column("start_time", clean_info.start_time_)) ||
             OB_FAIL(dml.add_time_column("end_time", clean_info.end_time_)) ||
             OB_FAIL(dml.add_column("incarnation", clean_info.incarnation_)) ||
             OB_FAIL(dml.add_column("type", ObBackupCleanType::get_str(clean_info.type_))) ||
             OB_FAIL(dml.add_column("status", ObBackupCleanInfoStatus::get_str(clean_info.status_))) ||
             OB_FAIL(dml.add_column("parameter", parameter_str)) ||
             OB_FAIL(dml.add_column("error_msg", clean_info.error_msg_.ptr())) ||
             OB_FAIL(dml.add_column("comment", clean_info.comment_.ptr())) ||
             OB_FAIL(dml.add_column("clog_gc_snapshot", clean_info.clog_gc_snapshot_)) ||
             OB_FAIL(dml.add_column("result", clean_info.result_)) ||
             OB_FAIL(dml.add_column("copy_id", clean_info.copy_id_))) {
    LOG_WARN("fail to fill backup clean info", K(ret), K(clean_info));
  }
  return ret;
}

int ObBackupCleanInfoHistoryOperator::insert_clean_info(
    const ObBackupCleanInfo& clean_info, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;

  if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(clean_info));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(fill_one_item(clean_info, dml))) {
      LOG_WARN("failed to fill on item", K(ret), K(clean_info));
    } else if (OB_FAIL(dml.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", K(ret));
    } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_BACKUP_CLEAN_INFO_HISTORY_TNAME, sql))) {
      LOG_WARN("failed to splice insert update sql", K(ret), K(clean_info));
    } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows));
    } else {
      LOG_INFO("insert tenant backup clean info", K(sql));
    }
  }
  return ret;
}
int ObBackupCleanInfoHistoryOperator::remove_tenant_clean_info(const uint64_t tenant_id, ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (tenant_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("remove task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(remove_one_item(tenant_id, sql_proxy))) {
    LOG_WARN("failed to remove on item", K(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupCleanInfoHistoryOperator::remove_one_item(const uint64_t tenant_id, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))) {
    LOG_WARN("fail to add column", K(ret), K(tenant_id));
  } else {
    ObDMLExecHelper exec(sql_proxy, tenant_id);
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_delete(OB_ALL_BACKUP_CLEAN_INFO_HISTORY_TNAME, dml, affected_rows))) {
      LOG_WARN("fail to exec delete", K(ret));
    }
  }
  return ret;
}

int ObBackupTaskCleanHistoryOpertor::fill_one_item(
    const int64_t job_id, const int64_t copy_id, const ObTenantBackupTaskInfo& tenant_backup_task, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (!tenant_backup_task.is_valid() || job_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_backup_task), K(job_id));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
    LOG_WARN("failed to add job id", K(ret), K(job_id));
  } else if (OB_FAIL(ObITenantBackupTaskOperator::fill_task_clean_history(copy_id, tenant_backup_task, dml))) {
    LOG_WARN("failed to fill one item", K(ret), K(tenant_backup_task));
  }
  return ret;
}

int ObBackupTaskCleanHistoryOpertor::insert_task_info(const int64_t job_id, const int64_t copy_id,
    const ObTenantBackupTaskInfo& tenant_backup_task, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;

  if (!tenant_backup_task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_backup_task));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(fill_one_item(job_id, copy_id, tenant_backup_task, dml))) {
      LOG_WARN("failed to fill on item", K(ret), K(tenant_backup_task));
    } else if (OB_FAIL(dml.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", K(ret));
    } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_BACKUP_TASK_CLEAN_HISTORY_TNAME, sql))) {
      LOG_WARN("failed to splice insert update sql", K(ret), K(tenant_backup_task));
    } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows));
    } else {
      LOG_INFO("insert tenant backup task history", K(sql));
    }
  }
  return ret;
}

int ObBackupTaskCleanHistoryOpertor::remove_task_info(
    const uint64_t tenant_id, const int64_t job_id, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (tenant_id == OB_INVALID_ID || job_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("remove task get invalid argument", K(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(remove_one_item(tenant_id, job_id, sql_proxy))) {
    LOG_WARN("failed to remove on item", K(ret), K(tenant_id), K(job_id));
  }
  return ret;
}

int ObBackupTaskCleanHistoryOpertor::remove_one_item(
    const uint64_t tenant_id, const int64_t job_id, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))) {
    LOG_WARN("fail to add column", K(ret), K(tenant_id));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
    LOG_WARN("fail to add column", K(ret), K(job_id));
  } else {
    ObDMLExecHelper exec(sql_proxy, tenant_id);
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_delete(OB_ALL_BACKUP_TASK_CLEAN_HISTORY_TNAME, dml, affected_rows))) {
      LOG_WARN("fail to exec delete", K(ret));
    }
  }
  return ret;
}

int ObBackupTaskHistoryOperator::get_tenant_full_backup_tasks(
    const uint64_t tenant_id, common::ObISQLClient& proxy, common::ObIArray<ObTenantBackupTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  items.reset();

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                                    "WHERE tenant_id = %lu and backup_type = 'D' FOR UPDATE",
                 OB_ALL_BACKUP_TASK_HISTORY_TNAME,
                 tenant_id))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObITenantBackupTaskOperator::get_tenant_backup_task(OB_SYS_TENANT_ID, sql, items, proxy))) {
    LOG_WARN("fail to get pg backup task", K(ret), K(sql), K(tenant_id));
  }
  return ret;
}

int ObBackupSetFilesOperator::fill_one_item_(const ObBackupSetFileInfo& item, share::ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  char cluster_version_display[share::OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH] = "";
  const int64_t pos = ObClusterVersion::get_instance().print_vsn(
      cluster_version_display, OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH, item.cluster_version_);

  if (!item.is_valid() || pos <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item), K(pos));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", item.tenant_id_)) ||
             OB_FAIL(dml.add_pk_column("backup_set_id", item.backup_set_id_)) ||
             OB_FAIL(dml.add_pk_column("incarnation", item.incarnation_)) ||
             OB_FAIL(dml.add_pk_column("copy_id", item.copy_id_)) ||
             OB_FAIL(dml.add_column("backup_type", item.backup_type_.get_backup_type_str())) ||
             OB_FAIL(dml.add_column("snapshot_version", item.snapshot_version_)) ||
             OB_FAIL(dml.add_column("prev_full_backup_set_id", item.prev_full_backup_set_id_)) ||
             OB_FAIL(dml.add_column("prev_inc_backup_set_id", item.prev_inc_backup_set_id_)) ||
             OB_FAIL(dml.add_column("prev_backup_data_version", item.prev_backup_data_version_)) ||
             OB_FAIL(dml.add_column("pg_count", item.pg_count_)) ||
             OB_FAIL(dml.add_column("macro_block_count", item.macro_block_count_)) ||
             OB_FAIL(dml.add_column("finish_pg_count", item.finish_pg_count_)) ||
             OB_FAIL(dml.add_column("finish_macro_block_count", item.finish_macro_block_count_)) ||
             OB_FAIL(dml.add_column("input_bytes", item.input_bytes_)) ||
             OB_FAIL(dml.add_column("output_bytes", item.output_bytes_)) ||
             OB_FAIL(dml.add_column("start_time", item.start_time_)) ||
             OB_FAIL(dml.add_column("end_time", item.end_time_)) ||
             OB_FAIL(dml.add_column("compatible", item.compatible_)) ||
             OB_FAIL(dml.add_column("cluster_version", item.cluster_version_)) ||
             OB_FAIL(dml.add_column("status", item.get_backup_set_status_str())) ||
             OB_FAIL(dml.add_column("result", item.result_)) ||
             OB_FAIL(dml.add_column("cluster_id", item.cluster_id_)) ||
             OB_FAIL(dml.add_column("backup_dest", item.backup_dest_.ptr())) ||
             OB_FAIL(dml.add_column("backup_data_version", item.backup_data_version_)) ||
             OB_FAIL(dml.add_column("backup_schema_version", item.backup_schema_version_)) ||
             OB_FAIL(dml.add_column("cluster_version_display", cluster_version_display)) ||
             OB_FAIL(dml.add_column("partition_count", item.partition_count_)) ||
             OB_FAIL(dml.add_column("finish_partition_count", item.finish_partition_count_)) ||
             OB_FAIL(dml.add_column("file_status", ObBackupFileStatus::get_str(item.file_status_))) ||
             OB_FAIL(dml.add_column("start_replay_log_ts", item.start_replay_log_ts_)) ||
             OB_FAIL(dml.add_column(
                 OB_STR_BACKUP_ENCRYPTION_MODE, ObBackupEncryptionMode::to_str(item.encryption_mode_))) ||
             OB_FAIL(dml.add_column(OB_STR_BACKUP_PASSWD, item.passwd_.ptr())) ||
             OB_FAIL(dml.add_column("date", item.date_))) {
    LOG_WARN("fail to fill backup task info", K(ret), K(item));
  }
  return ret;
}

int ObBackupSetFilesOperator::extract_backup_set_info_(sqlclient::ObMySQLResult* result, ObBackupSetFileInfo& item)

{
  int ret = OB_SUCCESS;
  char backup_type[common::OB_INNER_TABLE_BACKUP_TYPE_LENTH + 1] = "";
  char status[common::OB_DEFAULT_STATUS_LENTH] = "";
  char backup_dest[share::OB_MAX_BACKUP_DEST_LENGTH] = "";
  char encryption_mode[OB_MAX_ENCRYPTION_MODE_LENGTH] = "";
  char passwd[OB_MAX_PASSWORD_LENGTH] = "";
  char file_status[common::OB_DEFAULT_STATUS_LENTH] = "";
  int64_t tmp_real_str_len = 0;
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extract tenant backup task get invlid argument", K(ret), KP(result));
  } else {
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", item.tenant_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "backup_set_id", item.backup_set_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "incarnation", item.incarnation_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "copy_id", item.copy_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "snapshot_version", item.snapshot_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "prev_full_backup_set_id", item.prev_full_backup_set_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "prev_inc_backup_set_id", item.prev_inc_backup_set_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "prev_backup_data_version", item.prev_backup_data_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "pg_count", item.pg_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "macro_block_count", item.macro_block_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_pg_count", item.finish_pg_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_macro_block_count", item.finish_macro_block_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "input_bytes", item.input_bytes_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "output_bytes", item.output_bytes_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "result", item.result_, int32_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "compatible", item.compatible_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "cluster_version", item.cluster_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "cluster_id", item.cluster_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "backup_data_version", item.backup_data_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "backup_schema_version", item.backup_schema_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "partition_count", item.partition_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_partition_count", item.finish_partition_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "start_time", item.start_time_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "end_time", item.end_time_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "start_replay_log_ts", item.start_replay_log_ts_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "date", item.date_, int64_t);

    EXTRACT_STRBUF_FIELD_MYSQL(
        *result, "backup_type", backup_type, OB_INNER_TABLE_BACKUP_TYPE_LENTH + 1, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "status", status, OB_DEFAULT_STATUS_LENTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "backup_dest", backup_dest, OB_MAX_BACKUP_DEST_LENGTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(
        *result, OB_STR_BACKUP_ENCRYPTION_MODE, encryption_mode, OB_MAX_ENCRYPTION_MODE_LENGTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_BACKUP_PASSWD, passwd, OB_MAX_PASSWORD_LENGTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "file_status", file_status, OB_DEFAULT_STATUS_LENTH, tmp_real_str_len);

    UNUSED(tmp_real_str_len);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(item.backup_type_.set_backup_type(backup_type))) {
      LOG_WARN("failed to set backup type", K(ret), K(backup_type), K(OB_INNER_TABLE_BACKUP_TYPE_LENTH));
    } else if (OB_FAIL(item.set_backup_set_status(status))) {
      LOG_WARN("failed to set backup status", K(ret), K(status), K(OB_DEFAULT_STATUS_LENTH));
    } else if (OB_FAIL(item.backup_dest_.assign(backup_dest))) {
      LOG_WARN("failed to set backup dest", K(ret), K(backup_dest), K(OB_MAX_BACKUP_DEST_LENGTH));
    } else if (FALSE_IT(item.encryption_mode_ = ObBackupEncryptionMode::parse_str(encryption_mode))) {
    } else if (!ObBackupEncryptionMode::is_valid(item.encryption_mode_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid encryption_mode", K(ret), K(encryption_mode), "mode", item.encryption_mode_);
    } else if (OB_FAIL(item.passwd_.assign(passwd))) {
      LOG_WARN("failed to assign passwd", K(ret));
    } else if (FALSE_IT(item.file_status_ = ObBackupFileStatus::get_status(file_status))) {
    }
  }
  return ret;
}

// TODO(muwei.ym) this check function need replace by root_backup
int ObBackupSetFilesOperator::check_can_update_backup_set_file_info(
    const ObBackupSetFileInfo::BackupSetStatus& src_status, const ObBackupSetFileInfo::BackupSetStatus& dest_status,
    const ObBackupFileStatus::STATUS& src_file_status, const ObBackupFileStatus::STATUS& dest_file_status)
{
  int ret = OB_SUCCESS;
  if (src_status < ObBackupSetFileInfo::DOING || src_status >= ObBackupSetFileInfo::MAX ||
      dest_status < ObBackupSetFileInfo::DOING || dest_status >= ObBackupSetFileInfo::MAX ||
      !ObBackupFileStatus::is_valid(src_file_status) || !ObBackupFileStatus::is_valid(dest_file_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check can update backup set file info get invlaid argument",
        K(ret),
        K(src_status),
        K(dest_status),
        K(src_file_status),
        K(dest_file_status));
  } else {
    switch (src_status) {
      case ObBackupSetFileInfo::DOING: {
        if (ObBackupSetFileInfo::DOING != dest_status && ObBackupSetFileInfo::SUCCESS != dest_status &&
            ObBackupSetFileInfo::FAILED != dest_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup set file info", K(ret), K(src_status), K(dest_status));
        }
        break;
      }
      case ObBackupSetFileInfo::SUCCESS: {
        if (ObBackupSetFileInfo::SUCCESS != dest_status &&
            ObBackupSetFileInfo::FAILED != dest_status /*for sys backup backup*/) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup set file info", K(ret), K(src_status), K(dest_status));
        }
        break;
      }
      case ObBackupSetFileInfo::FAILED: {
        if (ObBackupSetFileInfo::FAILED != dest_status && ObBackupSetFileInfo::DOING != dest_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup set file info", K(ret), K(src_status), K(dest_status));
        }
        break;
      }
      case ObBackupSetFileInfo::MAX: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not update backup set file info", K(ret), K(src_status), K(dest_status));
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown status", K(ret), K(src_status), K(dest_status));
        break;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObBackupFileStatus::check_can_change_status(src_file_status, dest_file_status))) {
        LOG_WARN("failed to check can change stauts", K(ret), K(src_file_status), K(dest_file_status));
      }
    }
  }
  return ret;
}

int ObBackupSetFilesOperator::get_tenant_backup_set_file_info(const uint64_t tenant_id, const int64_t backup_set_id,
    const int64_t incarnation, const int64_t copy_id, const bool for_update, common::ObISQLClient& sql_proxy,
    ObBackupSetFileInfo& backup_set_file_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  backup_set_file_info.reset();
  ObArray<ObBackupSetFileInfo> backup_set_file_infos;

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(
                 sql.assign_fmt("SELECT * FROM %s "
                                "WHERE tenant_id = %lu and backup_set_id = %ld and incarnation = %ld and copy_id = %ld",
                     OB_ALL_BACKUP_SET_FILES_TNAME,
                     tenant_id,
                     backup_set_id,
                     incarnation,
                     copy_id))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else if (for_update) {
    if (OB_FAIL(sql.append(" FOR UPDATE"))) {
      LOG_WARN("failed to apend sql", K(ret), K(sql));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_backup_set_file_infos_(sql, backup_set_file_infos, sql_proxy))) {
    LOG_WARN("failed to get backup set file infos", K(ret), K(sql));
  } else if (backup_set_file_infos.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("failed to get backup set file info", K(ret), K(tenant_id), K(backup_set_id), K(incarnation), K(copy_id));
  } else if (backup_set_file_infos.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get more backup set file infos", K(ret), K(tenant_id), K(backup_set_id), K(incarnation), K(copy_id));
  } else {
    backup_set_file_info = backup_set_file_infos.at(0);
  }

  return ret;
}

int ObBackupSetFilesOperator::get_backup_set_info_with_file_status(const uint64_t tenant_id, const int64_t incarnation,
    const bool for_update, const ObBackupFileStatus::STATUS& file_status, common::ObISQLClient& sql_proxy,
    common::ObIArray<ObBackupSetFileInfo>& backup_set_file_info_array)
{
  int ret = OB_SUCCESS;
  backup_set_file_info_array.reset();
  ObSqlString sql;

  if (OB_INVALID_ID == tenant_id || incarnation <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup set file infos get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(
                 sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu and incarnation = %ld and file_status = '%s'",
                     OB_ALL_BACKUP_SET_FILES_TNAME,
                     tenant_id,
                     incarnation,
                     ObBackupFileStatus::get_str(file_status)))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else if (for_update && OB_FAIL(sql.append(" FOR UPDATE"))) {
    LOG_WARN("failed to apend update sql", K(ret), K(sql));
  } else if (OB_FAIL(get_backup_set_file_infos_(sql, backup_set_file_info_array, sql_proxy))) {
    LOG_WARN("failed to get backup set file infos", K(ret), K(sql));
  }
  return ret;
}

int ObBackupSetFilesOperator::update_tenant_backup_set_file(const ObBackupSetFileInfo& src_backup_set_file_info,
    const ObBackupSetFileInfo& dest_backup_set_file_info, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  int64_t affected_rows = 0;

  if (!src_backup_set_file_info.is_valid() || !dest_backup_set_file_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(src_backup_set_file_info), K(dest_backup_set_file_info));
  } else if (OB_FAIL(check_can_update_backup_set_file_info(src_backup_set_file_info.status_,
                 dest_backup_set_file_info.status_,
                 src_backup_set_file_info.file_status_,
                 dest_backup_set_file_info.file_status_))) {
    LOG_WARN("failed to check can update backup set file info",
        K(ret),
        K(src_backup_set_file_info),
        K(dest_backup_set_file_info));
  } else if (OB_FAIL(fill_one_item_(dest_backup_set_file_info, dml))) {
    LOG_WARN("failed to fill on item", K(ret), K(dest_backup_set_file_info));
  } else if (OB_FAIL(dml.splice_column_names(columns))) {
    LOG_WARN("failed to splice column names", K(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_SET_FILES_TNAME, sql))) {
    LOG_WARN("failed to splice update sql", K(ret), K(dest_backup_set_file_info));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else if (0 != affected_rows && 1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to update backup set files", K(sql), K(affected_rows));
  } else {
    LOG_INFO("update backup set files", K(sql), K(affected_rows));
  }
  return ret;
}

int ObBackupSetFilesOperator::insert_tenant_backup_set_file_info(
    const ObBackupSetFileInfo& backup_set_file_info, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  int64_t affected_rows = 0;

  if (!backup_set_file_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert tenant backup set file info get invalid argument", K(ret), K(backup_set_file_info));
  } else if (OB_FAIL(fill_one_item_(backup_set_file_info, dml))) {
    LOG_WARN("failed to fill on item", K(ret), K(backup_set_file_info));
  } else if (OB_FAIL(dml.splice_column_names(columns))) {
    LOG_WARN("failed to splice column names", K(ret));
  } else if (OB_FAIL(dml.splice_insert_update_sql(OB_ALL_BACKUP_SET_FILES_TNAME, sql))) {
    LOG_WARN("failed to splice update sql", K(ret), K(backup_set_file_info));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else {
    LOG_INFO("update backup set files", K(sql), K(affected_rows));
  }
  return ret;
}

int ObBackupSetFilesOperator::get_backup_set_file_infos_(
    const common::ObSqlString& sql, common::ObIArray<ObBackupSetFileInfo>& items, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = OB_SYS_TENANT_ID;

  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_UNLIKELY(!sql.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        ObBackupSetFileInfo item;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else if (OB_FAIL(extract_backup_set_info_(result, item))) {
          LOG_WARN("failed to extract tenant backup task", K(ret), K(item));
        } else if (OB_FAIL(items.push_back(item))) {
          LOG_WARN("fail to push back item", K(ret), K(item));
        }
      }
    }
  }
  return ret;
}

int ObBackupSetFilesOperator::get_tenant_backup_set_file_infos(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t full_backup_set_id, const int64_t copy_id, const bool for_update, common::ObISQLClient& sql_proxy,
    common::ObIArray<ObBackupSetFileInfo>& backup_set_file_info_array)
{
  int ret = OB_SUCCESS;
  backup_set_file_info_array.reset();
  ObSqlString sql;

  if (OB_INVALID_ID == tenant_id || incarnation <= 0 || full_backup_set_id <= 0 || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup set file infos get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                                    "WHERE tenant_id = %lu and (backup_set_id = %ld or prev_full_backup_set_id = %ld) "
                                    "and incarnation = %ld and copy_id = %ld",
                 OB_ALL_BACKUP_SET_FILES_TNAME,
                 tenant_id,
                 full_backup_set_id,
                 full_backup_set_id,
                 incarnation,
                 copy_id))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else if (for_update && OB_FAIL(sql.append(" FOR UPDATE"))) {
    LOG_WARN("failed to apend update sql", K(ret), K(sql));
  } else if (OB_FAIL(get_backup_set_file_infos_(sql, backup_set_file_info_array, sql_proxy))) {
    LOG_WARN("failed to get backup set file infos", K(ret), K(sql));
  }
  return ret;
}

///////////////////// functions for ObBackupInfoOperator //////////////////////////
int ObBackupInfoOperator::get_inner_table_version(common::ObISQLClient& sql_proxy, ObBackupInnerTableVersion& version)
{
  int ret = OB_SUCCESS;
  int64_t value = -1;

  if (OB_FAIL(get_int_value_(sql_proxy, false /*for update*/, OB_STR_BACKUP_INNER_TABLE_VERSION, value))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("Failed to get inner table version", K(ret));
    } else {
      version = OB_BACKUP_INNER_TABLE_V1;
      ret = OB_SUCCESS;
      LOG_INFO("inner table version not exist, set as V1", K(ret), K(version));
    }
  } else {
    version = static_cast<ObBackupInnerTableVersion>(value);
    if (!is_valid_backup_inner_table_version(version)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid version", K(ret), K(version));
    }
  }

  return ret;
}

int ObBackupInfoOperator::set_inner_table_version(
    common::ObISQLClient& sql_proxy, const ObBackupInnerTableVersion& version)
{
  int ret = OB_SUCCESS;

  if (!is_valid_backup_inner_table_version(version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(version));
  } else if (OB_FAIL(
                 set_int_value_(sql_proxy, OB_STR_BACKUP_INNER_TABLE_VERSION, static_cast<const int64_t>(version)))) {
    LOG_WARN("failed to set int value", K(ret), K(version));
  }

  return ret;
}

int ObBackupInfoOperator::get_max_piece_id(
    common::ObISQLClient& sql_proxy, const bool for_update, int64_t& max_piece_id)
{
  int ret = OB_SUCCESS;
  max_piece_id = -1;

  if (OB_FAIL(get_int_value_(sql_proxy, for_update, OB_STR_MAX_BACKUP_PIECE_ID, max_piece_id))) {
    LOG_WARN("Failed to get max piece id", K(ret));
  }

  return ret;
}

int ObBackupInfoOperator::set_max_piece_id(common::ObISQLClient& sql_proxy, const int64_t max_piece_id)
{
  int ret = OB_SUCCESS;
  const bool for_update = true;
  int64_t cur_max_piece_id = 0;

  if (max_piece_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid args", K(ret), K(max_piece_id));
  } else if (OB_FAIL(get_int_value_(sql_proxy, for_update, OB_STR_MAX_BACKUP_PIECE_ID, cur_max_piece_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("Failed to get max piece id", K(ret));
    }
  } else if (cur_max_piece_id > max_piece_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("new max piece id must not less than cur max piece id", K(ret), K(max_piece_id), K(cur_max_piece_id));
  }

  if (FAILEDx(set_int_value_(sql_proxy, OB_STR_MAX_BACKUP_PIECE_ID, max_piece_id))) {
    LOG_WARN("failed to set int value", K(ret), K(max_piece_id));
  }

  return ret;
}

int ObBackupInfoOperator::get_max_piece_create_date(
    common::ObISQLClient& sql_proxy, const bool for_update, int64_t& max_piece_create_date)
{
  int ret = OB_SUCCESS;
  max_piece_create_date = -1;

  if (OB_FAIL(get_int_value_(sql_proxy, for_update, OB_STR_MAX_BACKUP_PIECE_CREATE_DATE, max_piece_create_date))) {
    LOG_WARN("Failed to get max_piece_create_date", K(ret));
  }

  return ret;
}

int ObBackupInfoOperator::set_max_piece_create_date(
    common::ObISQLClient& sql_proxy, const int64_t max_piece_create_date)
{
  int ret = OB_SUCCESS;

  if (max_piece_create_date < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(max_piece_create_date));
  } else if (OB_FAIL(set_int_value_(sql_proxy, OB_STR_MAX_BACKUP_PIECE_CREATE_DATE, max_piece_create_date))) {
    LOG_WARN("failed to set max_piece_create_date", K(ret), K(max_piece_create_date));
  }

  return ret;
}

int ObBackupInfoOperator::get_tenant_name_backup_schema_version(
    common::ObISQLClient& sql_proxy, int64_t& backup_schema_version)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(get_int_value_(
          sql_proxy, false /*for update*/, OB_STR_TENANT_NAME_BACKUP_SCHEMA_VERSION, backup_schema_version))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("Failed to get inner table version", K(ret));
    } else {
      backup_schema_version = 0;
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObBackupInfoOperator::set_tenant_name_backup_schema_version(
    common::ObISQLClient& sql_proxy, const int64_t backup_schema_version)
{
  int ret = OB_SUCCESS;

  if (backup_schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(backup_schema_version));
  } else if (OB_FAIL(set_int_value_(sql_proxy, OB_STR_TENANT_NAME_BACKUP_SCHEMA_VERSION, backup_schema_version))) {
    LOG_WARN("failed to set int value", K(ret), K(OB_STR_TENANT_NAME_BACKUP_SCHEMA_VERSION));
  }

  return ret;
}

int ObBackupInfoOperator::get_backup_leader(common::ObISQLClient& sql_proxy, common::ObAddr& leader, bool& has_leader)
{
  int ret = OB_SUCCESS;
  char buf[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};
  leader.reset();
  has_leader = false;

  if (OB_FAIL(get_string_value_(sql_proxy, false /*for update*/, OB_STR_BACKUP_SCHEDULER_LEADER, buf, sizeof(buf)))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get scheduler leader", K(ret));
    }
  } else if (OB_FAIL(leader.parse_from_cstring(buf))) {
    LOG_WARN("failed to parse addr", K(ret), K(buf));
  } else {
    has_leader = true;
  }

  return ret;
}

int ObBackupInfoOperator::set_backup_leader(common::ObISQLClient& sql_proxy, const common::ObAddr& leader)
{
  int ret = OB_SUCCESS;
  char buf[MAX_IP_PORT_LENGTH] = "";

  if (!leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(leader));
  } else if (OB_FAIL(leader.ip_port_to_string(buf, sizeof(buf)))) {
    LOG_WARN("Failed to ip_port_to_string", K(ret), K(leader));
  } else if (OB_FAIL(set_string_value_(sql_proxy, OB_STR_BACKUP_SCHEDULER_LEADER, buf))) {
    LOG_WARN("Failed to set backup leader", K(ret), K(leader));
  }
  return ret;
}

int ObBackupInfoOperator::clean_backup_leader(common::ObISQLClient& sql_proxy, const common::ObAddr& leader)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char buf[MAX_IP_PORT_LENGTH] = "";

  if (!leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(leader));
  } else if (OB_FAIL(leader.ip_port_to_string(buf, sizeof(buf)))) {
    LOG_WARN("Failed to ip_port_to_string", K(ret), K(leader));
  } else if (OB_FAIL(sql.assign_fmt("update %s set value = '' where name = '%s' and value = '%s'",
                 OB_ALL_BACKUP_INFO_TNAME,
                 OB_STR_BACKUP_SCHEDULER_LEADER,
                 buf))) {
    LOG_WARN("Failed to clean backup leader", K(ret));
  } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  } else if (0 != affected_rows) {
    FLOG_INFO("succeed to clean backup leader", K(ret), "old_leader", leader);
  }
  return ret;
}

int ObBackupInfoOperator::get_int_value_(
    common::ObISQLClient& sql_proxy, const bool for_update, const char* name, int64_t& value)
{
  int ret = OB_SUCCESS;
  char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};
  value = -1;

  if (OB_FAIL(get_string_value_(sql_proxy, for_update, name, value_str, sizeof(value_str)))) {
    LOG_WARN("failed to get string value", K(ret), K(name));
  } else if (OB_FAIL(ob_atoll(value_str, value))) {
    LOG_WARN("failed to parse int", K(ret), K(value_str));
  }

  return ret;
}

int ObBackupInfoOperator::set_int_value_(common::ObISQLClient& sql_proxy, const char* name, const int64_t& value)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml_splicer;

  if (OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(name));
  } else if (OB_FAIL(dml_splicer.add_pk_column("name", name))) {
    LOG_WARN("failed to add column", K(ret), K(name));
  } else if (OB_FAIL(dml_splicer.add_column("value", value))) {
    LOG_WARN("failed to add column", K(ret), K(value));
  } else if (OB_FAIL(dml_splicer.splice_insert_update_sql(OB_ALL_BACKUP_INFO_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret));
  } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql), K(affected_rows));
  } else {
    LOG_INFO("succeed to set int value", K(ret), K(name), K(value), K(affected_rows), K(sql));
  }

  return ret;
}

int ObBackupInfoOperator::get_string_value_(
    common::ObISQLClient& sql_proxy, const bool for_update, const char* name, char* buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  if (OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(name));
  } else {
    buf[0] = '\0';
  }

  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    int real_length = 0;

    if (OB_FAIL(sql.assign_fmt("select value from %s where name = '%s'", OB_ALL_BACKUP_INFO_TNAME, name))) {
      LOG_WARN("failed to init sql", K(ret));
    } else if (for_update && OB_FAIL(sql.append(" for update"))) {
      LOG_WARN("failed to add for update", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("failed to read sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        LOG_WARN("failed to get next", K(ret), K(sql));
      }
    } else {
      EXTRACT_STRBUF_FIELD_MYSQL(*result, "value", buf, buf_len, real_length);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to extract field", K(ret), K(name), K(sql));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("get backup info string value", K(sql), K(name), K(real_length), K(buf));
      int tmp_ret = result->next();
      if (OB_ITER_END != tmp_ret) {
        char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};

        EXTRACT_STRBUF_FIELD_MYSQL(*result, name, value_str, sizeof(value_str), real_length);
        ret = OB_SUCCESS == tmp_ret ? OB_ERR_UNEXPECTED : tmp_ret;
        LOG_WARN("got more than one row", K(ret), K(tmp_ret), K(name), K(value_str), K(sql));
      }
    }
  }

  return ret;
}

int ObBackupInfoOperator::set_string_value_(common::ObISQLClient& sql_proxy, const char* name, const char* value)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml_splicer;

  if (OB_ISNULL(name) || OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(name), KP(value));
  } else if (OB_FAIL(dml_splicer.add_pk_column("name", name))) {
    LOG_WARN("failed to add column", K(ret), K(name));
  } else if (OB_FAIL(dml_splicer.add_column("value", value))) {
    LOG_WARN("failed to add column", K(ret), K(value));
  } else if (OB_FAIL(dml_splicer.splice_insert_update_sql(OB_ALL_BACKUP_INFO_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret));
  } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql), K(affected_rows));
  } else {
    LOG_INFO("succeed to set string value", K(ret), K(name), K(value), K(affected_rows), K(sql));
  }

  return ret;
}

int ObBackupSetFilesOperator::get_tenant_backup_set_file_info(const uint64_t tenant_id, const int64_t backup_set_id,
    const int64_t copy_id, const bool for_update, common::ObISQLClient& sql_proxy,
    ObBackupSetFileInfo& backup_set_file_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  backup_set_file_info.reset();
  ObArray<ObBackupSetFileInfo> backup_set_file_infos;

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                                    "WHERE tenant_id = %lu and backup_set_id = %ld and copy_id = %ld",
                 OB_ALL_BACKUP_SET_FILES_TNAME,
                 tenant_id,
                 backup_set_id,
                 copy_id))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else if (for_update && OB_FAIL(sql.append(" FOR UPDATE"))) {
    LOG_WARN("failed to apend sql", K(ret), K(sql));
  } else if (OB_FAIL(get_backup_set_file_infos_(sql, backup_set_file_infos, sql_proxy))) {
    LOG_WARN("failed to get backup set file infos", K(ret), K(sql));
  } else if (backup_set_file_infos.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("failed to get backup set file info", K(ret), K(tenant_id), K(backup_set_id), K(copy_id));
  } else if (backup_set_file_infos.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get more backup set file infos", K(ret), K(tenant_id), K(backup_set_id), K(copy_id));
  } else {
    backup_set_file_info = backup_set_file_infos.at(0);
  }

  return ret;
}

int ObBackupSetFilesOperator::get_backup_set_file_info_copies(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, common::ObISQLClient& proxy,
    common::ObIArray<ObBackupSetFileInfo>& backup_set_file_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_INVALID_ID == tenant_id || incarnation <= 0 || backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(tenant_id), K(incarnation), K(backup_set_id));
  } else if (OB_FAIL(
                 sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND incarnation = %ld AND backup_set_id = %ld",
                     OB_ALL_BACKUP_SET_FILES_TNAME,
                     tenant_id,
                     incarnation,
                     backup_set_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sql));
  } else if (OB_FAIL(get_backup_set_file_infos_(sql, backup_set_file_infos, proxy))) {
    LOG_WARN("failed to get backup set file infos", K(ret), K(sql));
  }
  return ret;
}

// cluster_level_gconf_dest [1,     10001)
// cluster_level_user_dest [10001, 20001)
// tenant_level_gconf_dest [20001, 30001)
// tenant_level_user_dest [30001, .....]
int ObBackupSetFilesOperator::get_backup_set_file_info_copy_list(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, const ObBackupBackupCopyIdLevel copy_id_level, common::ObISQLClient& proxy,
    common::ObIArray<ObBackupSetFileInfo>& backup_set_file_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t left_copy_id = 0;
  int64_t right_copy_id = 0;
  if (copy_id_level < OB_BB_COPY_ID_LEVEL_CLUSTER_GCONF_DEST || copy_id_level >= OB_BB_COPY_ID_LEVEL_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("copy id level unexpected", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(incarnation), K(backup_set_id));
  } else if (OB_FAIL(share::get_backup_copy_id_range_by_copy_level(copy_id_level, left_copy_id, right_copy_id))) {
    LOG_WARN("failed to get backup copy id range by copy level", KR(ret), K(copy_id_level));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE incarnation = %ld AND tenant_id = %lu "
                                    " AND backup_set_id = %ld AND file_status <> 'BROKEN'"
                                    " AND copy_id >= %ld AND copy_id < %ld",
                 OB_ALL_BACKUP_SET_FILES_TNAME,
                 incarnation,
                 tenant_id,
                 backup_set_id,
                 left_copy_id,
                 right_copy_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(incarnation), K(tenant_id), K(copy_id_level));
  } else if (OB_FAIL(get_backup_set_file_infos_(sql, backup_set_file_infos, proxy))) {
    LOG_WARN("failed to get backup piece list", KR(ret), K(sql));
  }
  return ret;
}

int ObBackupSetFilesOperator::get_all_cluster_level_backup_set_copy_count(const uint64_t tenant_id,
    const int64_t incarnation, const int64_t backup_set_id, common::ObISQLClient& proxy, int64_t& copy_count)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  copy_count = 0;
  sqlclient::ObMySQLResult* result = NULL;
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    if (OB_FAIL(sql.assign_fmt("select count(*) as count from %s"
                               " where incarnation = %ld and tenant_id = %lu and backup_set_id = %ld"
                               " AND file_status <> 'INCOMPLETE' AND file_status <> 'BROKEN'"
                               " AND copy_id < %ld",
            OB_ALL_BACKUP_SET_FILES_TNAME,
            incarnation,
            tenant_id,
            backup_set_id,
            OB_TENANT_GCONF_DEST_START_COPY_ID))) {
      LOG_WARN("failed to init count sql", KR(ret));
    } else if (OB_FAIL(proxy.read(res, sql.ptr()))) {
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
      EXTRACT_INT_FIELD_MYSQL(*result, "count", copy_count, int64_t);
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
