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

#define USING_LOG_PREFIX SHARE
#include "share/backup/ob_validate_operator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "common/ob_smart_var.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace share {

int ObIBackupValidateOperator::fill_one_item(const ObBackupValidateTaskItem& item, share::ObDMLSqlSplicer& splicer)
{
  int ret = OB_SUCCESS;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item));
  } else if (OB_FAIL(splicer.add_pk_column("job_id", item.job_id_)) ||
             OB_FAIL(splicer.add_pk_column("tenant_id", item.tenant_id_)) ||
             OB_FAIL(splicer.add_pk_column("incarnation", item.incarnation_)) ||
             OB_FAIL(splicer.add_pk_column("backup_set_id", item.backup_set_id_)) ||
             OB_FAIL(splicer.add_column("tenant_name", item.tenant_name_.ptr())) ||
             OB_FAIL(splicer.add_column("progress_percent", item.progress_percent_)) ||
             OB_FAIL(splicer.add_column("status", ObBackupValidateTaskInfo::get_status_str(item.status_)))) {
    LOG_WARN("failed to fill backup validate task item", K(ret), K(item));
  }
  return ret;
}

int ObIBackupValidateOperator::get_backup_validate_task(
    const common::ObSqlString& sql, common::ObISQLClient& sql_proxy, common::ObIArray<ObBackupValidateTaskItem>& items)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_UNLIKELY(!sql.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid sql", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("failed to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      // TODO : should we return error
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        ObBackupValidateTaskItem item;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get next row", K(ret));
          }
        } else {
          if (OB_FAIL(extract_validate_task(result, item))) {
            LOG_WARN("failed to extract validate task", K(ret));
          } else if (OB_FAIL(items.push_back(item))) {
            LOG_WARN("failed to push back item", K(ret), K(item));
          }
        }
      }
    }
  }
  return ret;
}

int ObIBackupValidateOperator::extract_validate_task(
    common::sqlclient::ObMySQLResult* result, ObBackupValidateTaskItem& item)
{
  int ret = OB_SUCCESS;
  char status[common::OB_DEFAULT_STATUS_LENTH] = "";
  char tenant_name[common::OB_MAX_TENANT_NAME_LENGTH] = "";
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extract backup validate task get invalid arguments", K(ret), KP(result));
  } else {
    int64_t tmp_real_str_len = 0;
    EXTRACT_INT_FIELD_MYSQL(*result, "job_id", item.job_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", item.tenant_id_, uint64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "tenant_name", tenant_name, OB_MAX_TENANT_NAME_LENGTH, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(*result, "incarnation", item.incarnation_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "backup_set_id", item.backup_set_id_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "status", status, OB_DEFAULT_STATUS_LENTH, tmp_real_str_len);
    UNUSED(tmp_real_str_len);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(item.set_status(status))) {
        LOG_WARN("failed to set validate status", K(ret));
      } else if (OB_FAIL(item.tenant_name_.assign(tenant_name))) {
        LOG_WARN("failed to assign tenant_name", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupValidateOperator::insert_task(const ObBackupValidateTaskItem& item, common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObDMLSqlSplicer splicer;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObIBackupValidateOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", K(ret));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column name", K(ret));
    } else if (OB_FAIL(splicer.splice_insert_update_sql(OB_ALL_BACKUP_VALIDATION_JOB_TNAME, sql))) {
      LOG_WARN("failed to splice insert update sql", K(ret), K(item));
    } else if (OB_FAIL(sql_client.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", K(ret), K(sql));
    }
  }
  return ret;
}

int ObBackupValidateOperator::remove_task(const int64_t job_id, common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer splicer;
  ObDMLExecHelper exec(sql_client, OB_SYS_TENANT_ID);
  if (OB_FAIL(splicer.add_pk_column("job_id", job_id))) {
    LOG_WARN("failed to add pk column", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_delete(OB_ALL_BACKUP_VALIDATION_JOB_TNAME, splicer, affected_rows))) {
      LOG_WARN("failed to exec delete", K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(job_id));
    }
  }
  return ret;
}

int ObBackupValidateOperator::get_task(
    const int64_t job_id, common::ObISQLClient& sql_client, ObBackupValidateTaskItem& item)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArray<ObBackupValidateTaskItem> items;
  if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT * FROM %s WHERE job_id = %ld FOR UPDATE", OB_ALL_BACKUP_VALIDATION_JOB_TNAME, job_id))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(ObIBackupValidateOperator::get_backup_validate_task(sql, sql_client, items))) {
    LOG_WARN("failed to get backup validate task", K(ret), K(sql));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", K(ret), K(items.count()));
  } else if (0 == items.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    // do nothing
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObBackupValidateOperator::get_task(const int64_t job_id, const uint64_t tenant_id, const int64_t backup_set_id,
    common::ObISQLClient& sql_client, ObBackupValidateTaskItem& item)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArray<ObBackupValidateTaskItem> items;
  if (OB_INVALID_ID == tenant_id || job_id < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(job_id), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu AND backup_set_id = %ld FOR UPDATE",
                 OB_ALL_BACKUP_VALIDATION_JOB_TNAME,
                 job_id,
                 tenant_id,
                 backup_set_id))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(ObIBackupValidateOperator::get_backup_validate_task(sql, sql_client, items))) {
    LOG_WARN("failed to get backup validate task", K(ret), K(sql));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", K(ret), K(items.count()));
  } else if (0 == items.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    // do nothing
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObBackupValidateOperator::get_not_finished_tasks(
    common::ObISQLClient& sql_client, common::ObIArray<ObBackupValidateTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE status <> 'FINISHED'", OB_ALL_BACKUP_VALIDATION_JOB_TNAME))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(ObIBackupValidateOperator::get_backup_validate_task(sql, sql_client, items))) {
    LOG_WARN("failed to get backup validate task", K(ret), K(sql));
  } else {
    LOG_INFO("execute get not finished tasks sql success", K(ret), K(sql));
  }
  return ret;
}

int ObBackupValidateOperator::get_tasks(const int64_t job_id, const uint64_t tenant_id,
    common::ObISQLClient& sql_client, common::ObIArray<ObBackupValidateTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu",
                 OB_ALL_BACKUP_VALIDATION_JOB_TNAME,
                 job_id,
                 tenant_id))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(ObIBackupValidateOperator::get_backup_validate_task(sql, sql_client, items))) {
    LOG_WARN("failed to get backup validate task", K(ret), K(sql));
  }
  return ret;
}

int ObBackupValidateOperator::get_all_tasks(
    common::ObISQLClient& sql_client, common::ObIArray<ObBackupValidateTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s", OB_ALL_BACKUP_VALIDATION_JOB_TNAME))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(ObIBackupValidateOperator::get_backup_validate_task(sql, sql_client, items))) {
    LOG_WARN("failed to get backup validate task", K(ret), K(sql));
  }
  return ret;
}

int ObBackupValidateOperator::report_task(const ObBackupValidateTaskItem& item, common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObDMLSqlSplicer splicer;

  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObIBackupValidateOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", K(ret));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", K(ret));
    } else if (OB_FAIL(splicer.splice_update_sql(OB_ALL_BACKUP_VALIDATION_JOB_TNAME, sql))) {
      LOG_WARN("failed to splice update sql", K(ret));
    } else if (OB_FAIL(sql_client.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to exec sql", K(ret), K(sql));
    } else if (0 != affected_rows && 1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("err unexpected", K(ret));
    } else {
      LOG_INFO("report task success", K(ret));
    }
  }
  return ret;
}

int ObBackupValidateHistoryOperator::insert_task(const ObBackupValidateTaskItem& item, common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObDMLSqlSplicer splicer;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObIBackupValidateOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", K(ret));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column name", K(ret));
    } else if (OB_FAIL(splicer.splice_insert_update_sql(OB_ALL_BACKUP_VALIDATION_JOB_HISTORY_TNAME, sql))) {
      LOG_WARN("failed to splice insert update sql", K(ret), K(item));
    } else if (OB_FAIL(sql_client.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", K(ret), K(sql));
    }
  }
  return ret;
}

int ObBackupValidateHistoryOperator::remove_task(const int64_t job_id, common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer splicer;
  ObDMLExecHelper exec(sql_client, OB_SYS_TENANT_ID);
  if (OB_FAIL(splicer.add_pk_column("job_id", job_id))) {
    LOG_WARN("failed to add pk column", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_delete(OB_ALL_BACKUP_VALIDATION_JOB_HISTORY_TNAME, splicer, affected_rows))) {
      LOG_WARN("failed to exec delete", K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(job_id));
    }
  }
  return ret;
}

int ObBackupValidateHistoryOperator::get_task(const int64_t job_id, const uint64_t tenant_id,
    const int64_t backup_set_id, common::ObISQLClient& sql_client, ObBackupValidateTaskItem& item)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArray<ObBackupValidateTaskItem> items;
  if (OB_INVALID_ID == tenant_id || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu AND backup_set_id = %ld",
                 OB_ALL_BACKUP_VALIDATION_JOB_HISTORY_TNAME,
                 job_id,
                 tenant_id,
                 backup_set_id))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(ObIBackupValidateOperator::get_backup_validate_task(sql, sql_client, items))) {
    LOG_WARN("failed to get backup validate task", K(ret), K(sql));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", K(ret), K(items.count()));
  } else if (0 == items.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    // do nothing
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObBackupValidateHistoryOperator::get_tasks(const int64_t job_id, const uint64_t tenant_id,
    common::ObISQLClient& sql_client, common::ObIArray<ObBackupValidateTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu",
                 OB_ALL_BACKUP_VALIDATION_JOB_HISTORY_TNAME,
                 job_id,
                 tenant_id))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(ObIBackupValidateOperator::get_backup_validate_task(sql, sql_client, items))) {
    LOG_WARN("failed to get backup validate task", K(ret), K(sql));
  }
  return ret;
}

int ObITenantValidateTaskOperator::fill_one_item(const ObTenantValidateTaskItem& item, share::ObDMLSqlSplicer& splicer)
{
  int ret = OB_SUCCESS;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else if (OB_FAIL(splicer.add_pk_column("tenant_id", item.tenant_id_)) ||
             OB_FAIL(splicer.add_pk_column("job_id", item.job_id_)) ||
             OB_FAIL(splicer.add_pk_column("task_id", item.task_id_)) ||
             OB_FAIL(splicer.add_pk_column("incarnation", item.incarnation_)) ||
             OB_FAIL(splicer.add_pk_column("backup_set_id", item.backup_set_id_)) ||
             OB_FAIL(splicer.add_column("status", ObTenantValidateTaskItem::get_status_str(item.status_))) ||
             OB_FAIL(splicer.add_column("backup_dest", item.backup_dest_)) ||
             OB_FAIL(splicer.add_time_column("start_time", item.start_time_)) ||
             OB_FAIL(splicer.add_time_column("end_time", item.end_time_)) ||
             OB_FAIL(splicer.add_column("total_pg_count", item.total_pg_count_)) ||
             OB_FAIL(splicer.add_column("finish_pg_count", item.finish_pg_count_)) ||
             OB_FAIL(splicer.add_column("total_partition_count", item.total_partition_count_)) ||
             OB_FAIL(splicer.add_column("finish_partition_count", item.finish_partition_count_)) ||
             OB_FAIL(splicer.add_column("total_macro_block_count", item.total_macro_block_count_)) ||
             OB_FAIL(splicer.add_column("finish_macro_block_count", item.finish_macro_block_count_)) ||
             OB_FAIL(splicer.add_column("log_size", item.log_size_)) ||
             OB_FAIL(splicer.add_column("result", item.result_)) ||
             OB_FAIL(splicer.add_column("comment", item.comment_.ptr()))) {
    LOG_WARN("failed to fill validate task item", K(ret), K(item));
  }
  return ret;
}

int ObITenantValidateTaskOperator::get_tenant_validate_task(const uint64_t tenant_id, const common::ObSqlString& sql,
    common::ObISQLClient& sql_proxy, common::ObIArray<ObTenantValidateTaskItem>& items)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_UNLIKELY(!sql.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("failed to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        ObTenantValidateTaskItem item;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get next row", K(ret));
          }
        } else {
          if (OB_FAIL(extract_tenant_task(result, item))) {
            LOG_WARN("failed to extract tenant task", K(ret), K(item));
          } else if (OB_FAIL(items.push_back(item))) {
            LOG_WARN("failed to push back item", K(ret), K(item));
          }
        }
      }
    }
  }
  return ret;
}

int ObITenantValidateTaskOperator::extract_tenant_task(
    common::sqlclient::ObMySQLResult* result, ObTenantValidateTaskItem& item)
{
  int ret = OB_SUCCESS;
  char status[common::OB_DEFAULT_STATUS_LENTH] = "";
  char comment[common::MAX_TABLE_COMMENT_LENGTH] = "";
  char backup_dest[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extract tenant task get invalid arguments", K(ret), KP(result));
  } else {
    int64_t tmp_real_str_len = 0;
    EXTRACT_INT_FIELD_MYSQL(*result, "job_id", item.job_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "task_id", item.task_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", item.tenant_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "incarnation", item.incarnation_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "backup_set_id", item.backup_set_id_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "status", status, OB_DEFAULT_STATUS_LENTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "backup_dest", backup_dest, OB_MAX_BACKUP_DEST_LENGTH, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(*result, "total_pg_count", item.total_pg_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_pg_count", item.finish_pg_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "total_partition_count", item.total_partition_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_partition_count", item.finish_partition_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "total_macro_block_count", item.total_macro_block_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_macro_block_count", item.finish_macro_block_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "log_size", item.log_size_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "result", item.result_, int32_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "comment", comment, MAX_TABLE_COMMENT_LENGTH, tmp_real_str_len);
    UNUSED(tmp_real_str_len);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(result->get_timestamp("start_time", NULL, item.start_time_))) {
        LOG_WARN("failed to get start time", K(ret));
      } else if (OB_FAIL(result->get_timestamp("end_time", NULL, item.end_time_))) {
        LOG_WARN("failed to get end time", K(ret));
      } else if (FALSE_IT(STRNCPY(item.backup_dest_, backup_dest, OB_MAX_BACKUP_DEST_LENGTH))) {
        // do nothing
      } else if (OB_FAIL(item.set_status(status))) {
        LOG_WARN("failed to get status", K(ret), K(status));
      }
    }
  }
  return ret;
}

int ObTenantValidateTaskOperator::insert_task(
    const uint64_t tenant_id, const ObTenantValidateTaskItem& item, common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer splicer;

  if (OB_INVALID_ID == tenant_id || !item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObITenantValidateTaskOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", K(ret), K(item));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", K(ret));
    } else if (OB_FAIL(splicer.splice_insert_sql(OB_ALL_TENANT_BACKUP_VALIDATION_TASK_TNAME, sql))) {
      LOG_WARN("failed to splice insert sql", K(ret), K(item));
    } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", K(ret), K(sql));
    } else if (OB_FAIL(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows));
    } else {
      LOG_INFO("insert tenant validate task", K(sql));
    }
  }
  return ret;
}

int ObTenantValidateTaskOperator::remove_task(const uint64_t tenant_id, const int64_t job_id,
    const uint64_t task_tenant_id, const int64_t incarnation, common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer splicer;
  ObDMLExecHelper exec(sql_client, tenant_id);
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == task_tenant_id || incarnation < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_tenant_id));
  } else if (OB_FAIL(splicer.add_pk_column("job_id", job_id)) ||
             OB_FAIL(splicer.add_pk_column("tenant_id", task_tenant_id)) ||
             OB_FAIL(splicer.add_pk_column("incarnation", incarnation))) {
    LOG_WARN("failed to add column", K(ret), K(tenant_id));
  } else if (OB_FAIL(exec.exec_delete(OB_ALL_TENANT_BACKUP_VALIDATION_TASK_TNAME, splicer, affected_rows))) {
    LOG_WARN("failed to exec delete", K(ret));
  }
  return ret;
}

int ObTenantValidateTaskOperator::get_task(const uint64_t tenant_id, const int64_t job_id,
    const uint64_t task_tenant_id, const int64_t incarnation, const int64_t backup_set_id,
    common::ObISQLClient& sql_client, ObTenantValidateTaskItem& item)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  item.reset();
  ObArray<ObTenantValidateTaskItem> items;

  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == task_tenant_id || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu AND incarnation = %ld AND "
                                    "backup_set_id = %ld",
                 OB_ALL_TENANT_BACKUP_VALIDATION_TASK_TNAME,
                 job_id,
                 task_tenant_id,
                 incarnation,
                 backup_set_id))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(ObITenantValidateTaskOperator::get_tenant_validate_task(tenant_id, sql, sql_client, items))) {
    LOG_WARN("failed to get tenant validate task", K(ret), K(sql));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", K(ret), K(items.count()));
  } else if (0 == items.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    // do nothing
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObTenantValidateTaskOperator::get_task(const uint64_t tenant_id, const int64_t job_id,
    const uint64_t task_tenant_id, const int64_t incarnation, common::ObISQLClient& sql_client,
    ObTenantValidateTaskItem& item)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  item.reset();
  ObArray<ObTenantValidateTaskItem> items;

  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == task_tenant_id || incarnation < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu AND incarnation = %ld",
                 OB_ALL_TENANT_BACKUP_VALIDATION_TASK_TNAME,
                 job_id,
                 task_tenant_id,
                 incarnation))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(ObITenantValidateTaskOperator::get_tenant_validate_task(tenant_id, sql, sql_client, items))) {
    LOG_WARN("failed to get tenant validate task", K(ret), K(sql));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", K(ret), K(items.count()));
  } else if (0 == items.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("task do not exist", K(ret));
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObTenantValidateTaskOperator::get_tasks(const uint64_t tenant_id, const int64_t job_id,
    const uint64_t task_tenant_id, const int64_t incarnation, common::ObISQLClient& sql_client,
    common::ObIArray<ObTenantValidateTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  items.reset();

  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == task_tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu AND incarnation = %ld",
                 OB_ALL_TENANT_BACKUP_VALIDATION_TASK_TNAME,
                 job_id,
                 task_tenant_id,
                 incarnation))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(ObITenantValidateTaskOperator::get_tenant_validate_task(tenant_id, sql, sql_client, items))) {
    LOG_WARN("failed to get tenant validate task", K(ret), K(tenant_id));
  }
  return ret;
}

int ObTenantValidateTaskOperator::get_tasks(const uint64_t task_tenant_id, const int64_t job_id,
    const uint64_t tenant_id, common::ObISQLClient& sql_client, common::ObIArray<ObTenantValidateTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  items.reset();

  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == task_tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu",
                 OB_ALL_TENANT_BACKUP_VALIDATION_TASK_TNAME,
                 job_id,
                 task_tenant_id))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(ObITenantValidateTaskOperator::get_tenant_validate_task(tenant_id, sql, sql_client, items))) {
    LOG_WARN("failed to get tenant validate task", K(ret), K(tenant_id));
  }
  return ret;
}

int ObTenantValidateTaskOperator::get_not_finished_tasks(const uint64_t tenant_id, const uint64_t task_tenant_id,
    common::ObISQLClient& sql_client, common::ObIArray<ObTenantValidateTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  items.reset();

  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == task_tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND status <> 'FINISHED'",
                 OB_ALL_TENANT_BACKUP_VALIDATION_TASK_TNAME,
                 task_tenant_id))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(ObITenantValidateTaskOperator::get_tenant_validate_task(tenant_id, sql, sql_client, items))) {
    LOG_WARN("failed to get tenant validate task", K(ret), K(tenant_id));
  }
  return ret;
}

int ObTenantValidateTaskOperator::get_finished_task(const uint64_t tenant_id, const int64_t job_id,
    const uint64_t task_tenant_id, common::ObISQLClient& sql_client, ObTenantValidateTaskItem& item)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  item.reset();
  ObArray<ObTenantValidateTaskItem> items;

  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == task_tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu AND status = 'FINISHED'",
                 OB_ALL_TENANT_BACKUP_VALIDATION_TASK_TNAME,
                 job_id,
                 task_tenant_id))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(ObITenantValidateTaskOperator::get_tenant_validate_task(tenant_id, sql, sql_client, items))) {
    LOG_WARN("failed to get tenant validate task", K(ret), K(tenant_id));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", K(ret), K(sql), K(tenant_id), K(task_tenant_id), K(job_id));
  } else if (0 == items.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    // do nothing
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObTenantValidateTaskOperator::report_task(
    const uint64_t tenant_id, const ObTenantValidateTaskItem& item, common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObDMLSqlSplicer splicer;

  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObITenantValidateTaskOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", K(ret), K(item));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", K(ret));
    } else if (OB_FAIL(splicer.splice_update_sql(OB_ALL_TENANT_BACKUP_VALIDATION_TASK_TNAME, sql))) {
      LOG_WARN("failed to splice update sql", K(ret), K(item));
    } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", K(ret));
    } else if (affected_rows != 0 && affected_rows != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows));
    } else {
      LOG_INFO("report tenant validate task", K(sql));
    }
  }
  return ret;
}

int ObTenantValidateHistoryOperator::insert_task(const ObTenantValidateTaskItem& item, common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObDMLSqlSplicer splicer;

  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObITenantValidateTaskOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", K(ret), K(item));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", K(ret));
    } else if (OB_FAIL(splicer.splice_insert_update_sql(OB_ALL_BACKUP_VALIDATION_TASK_HISTORY_TNAME, sql))) {
      LOG_WARN("failed to splice insert update sql", K(ret), K(item));
    } else if (OB_FAIL(sql_client.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", K(ret), K(sql));
    } else {
      LOG_INFO("insert tenant validate history task", K(sql), K(affected_rows));
    }
  }
  return ret;
}

int ObTenantValidateHistoryOperator::remove_task(const int64_t job_id, const uint64_t tenant_id,
    const int64_t incarnation, const int64_t backup_set_id, common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer splicer;
  ObDMLExecHelper exec(sql_client, OB_SYS_TENANT_ID);

  if (OB_INVALID_ID == tenant_id || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(splicer.add_pk_column("job_id", job_id)) ||
             OB_FAIL(splicer.add_pk_column("tenant_id", tenant_id)) ||
             OB_FAIL(splicer.add_pk_column("incarnation", incarnation)) ||
             OB_FAIL(splicer.add_pk_column("backup_set_id", backup_set_id))) {
    LOG_WARN("failed to add pk column", K(ret), K(tenant_id), K(backup_set_id));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_delete(OB_ALL_BACKUP_VALIDATION_TASK_HISTORY_TNAME, splicer, affected_rows))) {
      LOG_WARN("failed to exec delete", K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(tenant_id), K(backup_set_id), K(affected_rows));
    }
  }
  return ret;
}

int ObTenantValidateHistoryOperator::get_task(const int64_t job_id, const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, common::ObISQLClient& sql_client, ObTenantValidateTaskItem& item)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  item.reset();
  ObArray<ObTenantValidateTaskItem> items;

  if (OB_INVALID_ID == tenant_id || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu AND incarnation = %ld AND "
                                    "backup_set_id = %ld",
                 OB_ALL_BACKUP_VALIDATION_TASK_HISTORY_TNAME,
                 job_id,
                 tenant_id,
                 incarnation,
                 backup_set_id))) {
    LOG_WARN("failed to assign sql", K(ret), K(backup_set_id));
  } else if (OB_FAIL(
                 ObITenantValidateTaskOperator::get_tenant_validate_task(OB_SYS_TENANT_ID, sql, sql_client, items))) {
    LOG_WARN("failed to get tenant validate task", K(ret), K(tenant_id), K(backup_set_id));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", K(ret), K(sql), K(tenant_id), K(backup_set_id));
  } else if (0 == items.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    // do nothing
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObTenantValidateHistoryOperator::get_tasks(const int64_t job_id, const uint64_t tenant_id,
    common::ObISQLClient& sql_client, common::ObIArray<ObTenantValidateTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu",
                 OB_ALL_BACKUP_VALIDATION_TASK_HISTORY_TNAME,
                 job_id,
                 tenant_id))) {
    LOG_WARN("failed to assign sql", K(ret), K(job_id), K(tenant_id));
  } else if (OB_FAIL(
                 ObITenantValidateTaskOperator::get_tenant_validate_task(OB_SYS_TENANT_ID, sql, sql_client, items))) {
    LOG_WARN("failed to get tenant validate history task", K(ret), K(sql), K(tenant_id));
  }
  return ret;
}

int ObPGValidateTaskOperator::batch_report_pg_task(
    const uint64_t tenant_id, const common::ObIArray<ObPGValidateTaskItem>& items, common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer splicer;
  const int64_t BATCH_CNT = 64;
  // TODO : need to do in trans
  if (OB_UNLIKELY(items.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(items));
  } else {
    int64_t report_idx = 0;
    while (OB_SUCC(ret) && report_idx < items.count()) {
      const int64_t remain_cnt = items.count() - report_idx;
      int64_t cur_batch_cnt = remain_cnt < BATCH_CNT ? remain_cnt : BATCH_CNT;
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_batch_cnt; ++i) {
        sql.reuse();
        columns.reuse();
        splicer.reuse();
        const ObPGValidateTaskItem& item = items.at(report_idx + i);
        if (OB_FAIL(fill_one_item(item, splicer))) {
          LOG_WARN("failed to fill one item", K(ret), K(item));
        } else {
          if (OB_FAIL(splicer.splice_column_names(columns))) {
            LOG_WARN("failed to splice column names", K(ret));
          } else if (OB_FAIL(sql.assign_fmt("INSERT /*+ use_plan_cache(none) */ INTO %s (%s) VALUES",
                         OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TNAME,
                         columns.ptr()))) {
            LOG_WARN("failed to assign sql string", K(ret));
          }
          if (OB_SUCC(ret)) {
            values.reset();
            if (OB_FAIL(splicer.splice_values(values))) {
              LOG_WARN("failed to splice values", K(ret));
            } else if (OB_FAIL(sql.append_fmt("(%s)", values.ptr()))) {
              LOG_WARN("failed to assign sql string", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(sql.append(" ON DUPLICATE KEY UPDATE "))) {
              LOG_WARN("fail to append sql string", K(ret));
            } else if (OB_FAIL(sql.append("archive_round = values(archive_round)")) ||
                       OB_FAIL(sql.append(", status = values(status)")) ||
                       OB_FAIL(sql.append(", trace_id = values(trace_id)")) ||
                       OB_FAIL(sql.append(", svr_ip = values(svr_ip)")) ||
                       OB_FAIL(sql.append(", svr_port = values(svr_port)")) ||
                       OB_FAIL(sql.append(", total_partition_count = values(total_partition_count)")) ||
                       OB_FAIL(sql.append(", finish_partition_count = values(finish_partition_count)")) ||
                       OB_FAIL(sql.append(", total_macro_block_count = values(total_macro_block_count)")) ||
                       OB_FAIL(sql.append(", finish_macro_block_count = values(finish_macro_block_count)")) ||
                       OB_FAIL(sql.append(", log_info = values(log_info)")) ||
                       OB_FAIL(sql.append(", log_size = values(log_size)")) ||
                       OB_FAIL(sql.append(", result = values(result)")) ||
                       OB_FAIL(sql.append(", comment = values(comment)"))) {
              LOG_WARN("fail to append sql string", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            int64_t affected_rows = 0;
            if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
              LOG_WARN("failed to execute sql", K(ret), K(sql));
            } else {
              LOG_INFO("insert pg task success", K(ret), K(affected_rows), K(sql));
            }
          }
        }
      }
      report_idx += cur_batch_cnt;
    }
  }
  return ret;
}

int ObPGValidateTaskOperator::batch_remove_pg_task(const uint64_t tenant_id, const int64_t job_id,
    const uint64_t task_tenant_id, const int64_t max_delete_rows, common::ObISQLClient& sql_client,
    int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == task_tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE job_id = %ld AND tenant_id = %lu LIMIT %ld",
                 OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TNAME,
                 job_id,
                 task_tenant_id,
                 max_delete_rows))) {
    LOG_WARN("failed to assign sql", K(ret), K(job_id), K(task_tenant_id));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", K(ret));
  } else if (affected_rows > max_delete_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("err unexpected", K(ret), K(affected_rows), K(max_delete_rows));
  }
  return ret;
}

int ObPGValidateTaskOperator::get_pg_task(const uint64_t tenant_id, const int64_t job_id, const uint64_t task_tenant_id,
    const int64_t incarnation, const int64_t backup_set_id, const common::ObPartitionKey& pkey,
    common::ObISQLClient& sql_client, ObPGValidateTaskItem& item)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  common::ObArray<ObPGValidateTaskItem> items;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == task_tenant_id || incarnation < 0 || backup_set_id < 0 ||
      !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_tenant_id), K(incarnation), K(backup_set_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu AND incarnation = %ld "
                                    "AND backup_set_id = %ld AND table_id = %lu AND partition_id = %ld",
                 OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TNAME,
                 job_id,
                 task_tenant_id,
                 incarnation,
                 backup_set_id,
                 pkey.get_table_id(),
                 pkey.get_partition_id()))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(get_pg_validate_task(tenant_id, sql, sql_client, items))) {
    LOG_WARN("failed to get pg validate task", K(ret), K(tenant_id), K(sql));
  } else if (1 != items.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("items count is not expected, should be 1",
        K(ret),
        K(job_id),
        K(tenant_id),
        K(task_tenant_id),
        K(incarnation),
        K(backup_set_id),
        K(pkey));
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObPGValidateTaskOperator::get_pg_tasks(const uint64_t tenant_id, const int64_t job_id,
    const uint64_t task_tenant_id, const int64_t incarnation, const int64_t backup_set_id,
    common::ObISQLClient& sql_client, common::ObIArray<ObPGValidateTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == task_tenant_id || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_tenant_id), K(incarnation), K(backup_set_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu AND incarnation = %ld AND "
                                    "backup_set_id = %ld",
                 OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TNAME,
                 job_id,
                 task_tenant_id,
                 incarnation,
                 backup_set_id))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(get_pg_validate_task(tenant_id, sql, sql_client, items))) {
    LOG_WARN("failed to get pg validate task", K(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObPGValidateTaskOperator::get_doing_task(const uint64_t tenant_id, const int64_t job_id,
    const uint64_t task_tenant_id, const int64_t incarnation, const int64_t backup_set_id,
    common::ObISQLClient& sql_client, common::ObIArray<ObPGValidateTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == task_tenant_id || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu AND incarnation = %ld"
                                    " AND backup_set_id = %ld AND status = 'DOING'",
                 OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TNAME,
                 job_id,
                 task_tenant_id,
                 incarnation,
                 backup_set_id))) {
    LOG_WARN("failed to assign sql", K(ret), K(tenant_id), K(task_tenant_id), K(incarnation), K(backup_set_id));
  } else if (OB_FAIL(get_pg_validate_task(tenant_id, sql, sql_client, items))) {
    LOG_WARN("failed to get doing task", K(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObPGValidateTaskOperator::get_pending_task(const uint64_t tenant_id, const int64_t job_id,
    const uint64_t task_tenant_id, const int64_t incarnation, common::ObISQLClient& sql_client,
    common::ObIArray<ObPGValidateTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == task_tenant_id || incarnation < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu AND"
                                    " incarnation = %ld AND status = 'PENDING'",
                 OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TNAME,
                 job_id,
                 task_tenant_id,
                 incarnation))) {
    LOG_WARN("failed to assign sql", K(ret), K(task_tenant_id));
  } else if (OB_FAIL(get_pg_validate_task(tenant_id, sql, sql_client, items))) {
    LOG_WARN("failed to get pending task", K(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObPGValidateTaskOperator::get_pending_task(const uint64_t tenant_id, const int64_t job_id,
    const uint64_t task_tenant_id, const int64_t incarnation, const int64_t backup_set_id,
    common::ObISQLClient& sql_client, common::ObIArray<ObPGValidateTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == task_tenant_id || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_tenant_id), K(incarnation), K(backup_set_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu AND"
                                    " incarnation = %ld AND backup_set_id = %ld AND status = 'PENDING'",
                 OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TNAME,
                 job_id,
                 task_tenant_id,
                 incarnation,
                 backup_set_id))) {
    LOG_WARN("failed to assign sql", K(ret), K(task_tenant_id), K(backup_set_id));
  } else if (OB_FAIL(get_pg_validate_task(tenant_id, sql, sql_client, items))) {
    LOG_WARN("failed to get pending task", K(ret), K(tenant_id), K(sql));
  }
  return ret;
}

// TODO : task_id
int ObPGValidateTaskOperator::get_finished_task(const uint64_t tenant_id, const int64_t job_id,
    const uint64_t task_tenant_id, const int64_t incarnation, common::ObISQLClient& sql_client,
    common::ObIArray<ObPGValidateTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == task_tenant_id || incarnation < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu AND incarnation = %ld AND "
                                    "status = 'FINISHED'",
                 OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TNAME,
                 job_id,
                 task_tenant_id,
                 incarnation))) {
    LOG_WARN("failed to assign sql", K(ret), K(task_tenant_id), K(incarnation));
  } else if (OB_FAIL(get_pg_validate_task(tenant_id, sql, sql_client, items))) {
    LOG_WARN("failed to get doing task", K(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObPGValidateTaskOperator::get_one_doing_pg_tasks(const uint64_t tenant_id, const ObPGValidateTaskRowKey& row_key,
    common::ObISQLClient& sql_client, common::ObIArray<ObPGValidateTaskInfo>& items)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_PG_NUM = 1024;
  ObSqlString sql;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(row_key));
  } else if (OB_FAIL(
                 sql.assign_fmt("SELECT * FROM %s WHERE "
                                "(tenant_id, job_id, task_id, incarnation, backup_set_id, table_id, partition_id) > "
                                "(%lu, %ld, %ld, %ld, %ld, %ld, %ld) AND status = 'DOING' GROUP BY trace_id LIMIT %ld",
                     OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TNAME,
                     row_key.tenant_id_,
                     row_key.job_id_,
                     row_key.task_id_,
                     row_key.incarnation_,
                     row_key.backup_set_id_,
                     row_key.table_id_,
                     row_key.partition_id_,
                     MAX_PG_NUM))) {
    LOG_WARN("failed to assign sql", K(ret), K(tenant_id), K(row_key));
  } else if (OB_FAIL(get_pg_validate_task(tenant_id, sql, sql_client, items))) {
    LOG_WARN("failed to get doing task", K(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObPGValidateTaskOperator::get_finished_task(const uint64_t tenant_id, const int64_t job_id,
    const uint64_t task_tenant_id, const int64_t incarnation, const int64_t backup_set_id,
    common::ObISQLClient& sql_client, common::ObIArray<ObPGValidateTaskItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == task_tenant_id || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu AND incarnation = %ld AND "
                                    "backup_set_id = %ld AND status = 'FINISHED'",
                 OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TNAME,
                 job_id,
                 task_tenant_id,
                 incarnation,
                 backup_set_id))) {
    LOG_WARN("failed to assign sql", K(ret), K(task_tenant_id), K(incarnation), K(backup_set_id));
  } else if (OB_FAIL(get_pg_validate_task(tenant_id, sql, sql_client, items))) {
    LOG_WARN("failed to get doing task", K(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObPGValidateTaskOperator::update_pg_task_info(const uint64_t tenant_id, common::ObISQLClient& sql_client,
    // const ObTaskId &trace_id,
    const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  const uint64_t task_tenant_id = pkey.get_tenant_id();

  if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pkey));
  } else if (OB_FAIL(sql.assign_fmt(
                 "UPDATE %s SET end_time = now(6) WHERE tenant_id = %lu AND table_id = %ld AND partition_id = %ld",
                 OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TNAME,
                 task_tenant_id,
                 pkey.get_table_id(),
                 pkey.get_partition_id()))) {
    LOG_WARN("failed to append sql", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", K(ret));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected rows unexpected", K(ret), K(affected_rows));
  }
  return ret;
}

int ObPGValidateTaskOperator::update_pg_task_status(const uint64_t tenant_id, common::ObISQLClient& sql_client,
    const common::ObPartitionKey& pkey, const ObPGValidateTaskInfo::ValidateStatus& status)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t task_tenant_id = pkey.get_tenant_id();
  int64_t affected_rows = -1;
  const char* status_str = "PENDING";

  if (!pkey.is_valid() || status >= ObPGValidateTaskInfo::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(status), K(pkey));
  } else if (OB_FAIL(sql.append_fmt("UPDATE %s SET status = '%s', end_time = now(6) WHERE tenant_id = %lu AND table_id "
                                    "= %ld AND partition_id = %ld",
                 OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TNAME,
                 status_str,
                 task_tenant_id,
                 pkey.get_table_id(),
                 pkey.get_partition_id()))) {
    LOG_WARN("failed to append sql", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected rows unexpected", K(ret), K(affected_rows));
  } else {
    LOG_INFO("update pg task status", K(ret), K(sql), K(affected_rows));
  }
  return ret;
}

int ObPGValidateTaskOperator::update_pg_task_result_and_status(const uint64_t tenant_id, const int64_t backup_set_id,
    common::ObISQLClient& sql_client, const common::ObPartitionKey& pkey,
    const ObPGValidateTaskInfo::ValidateStatus& status, const int32_t result)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  const uint64_t task_tenant_id = pkey.get_tenant_id();
  const char* status_str = ObPGValidateTaskInfo::get_status_str(status);

  if (!pkey.is_valid() || status >= ObPGValidateTaskInfo::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(status), K(pkey));
  } else if (OB_FAIL(sql.append_fmt("UPDATE %s SET result = %d, status = '%s' WHERE tenant_id = %lu AND "
                                    " backup_set_id = %ld AND table_id = %ld AND partition_id = %ld",
                 OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TNAME,
                 result,
                 status_str,
                 task_tenant_id,
                 backup_set_id,
                 pkey.get_table_id(),
                 pkey.get_partition_id()))) {
    LOG_WARN("failed to append sql", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", K(ret));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected rows unexpected", K(ret), K(affected_rows));
  }
  return ret;
}

int ObPGValidateTaskOperator::fill_one_item(const ObPGValidateTaskItem& item, share::ObDMLSqlSplicer& splicer)
{
  int ret = OB_SUCCESS;
  char ip[common::MAX_IP_ADDR_LENGTH] = "";
  char trace_id_str[common::OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  char log_info[64] = "";
  const share::ObTaskId& trace_id = item.trace_id_;
  const uint64_t* value = NULL;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else if (!item.server_.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to convert ip to string", K(ret));
  } else if (FALSE_IT(value = trace_id.get())) {
  } else if (OB_FAIL(databuff_printf(trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE, "%lu_%lu", value[0], value[1]))) {
    LOG_WARN("failed to print trace id", K(ret));
  } else if (OB_FAIL(splicer.add_pk_column("tenant_id", item.tenant_id_)) ||
             OB_FAIL(splicer.add_pk_column("job_id", item.job_id_)) ||
             OB_FAIL(splicer.add_pk_column("task_id", item.task_id_)) ||
             OB_FAIL(splicer.add_pk_column("incarnation", item.incarnation_)) ||
             OB_FAIL(splicer.add_pk_column("backup_set_id", item.backup_set_id_)) ||
             OB_FAIL(splicer.add_pk_column("table_id", item.table_id_)) ||
             OB_FAIL(splicer.add_pk_column("partition_id", item.partition_id_)) ||
             OB_FAIL(splicer.add_column("archive_round", item.archive_round_)) ||
             OB_FAIL(splicer.add_column("status", ObPGValidateTaskInfo::get_status_str(item.status_))) ||
             OB_FAIL(splicer.add_column("trace_id", trace_id_str)) || OB_FAIL(splicer.add_column("svr_ip", ip)) ||
             OB_FAIL(splicer.add_column("svr_port", item.server_.get_port())) ||
             OB_FAIL(splicer.add_column("total_partition_count", item.total_partition_count_)) ||
             OB_FAIL(splicer.add_column("finish_partition_count", item.finish_partition_count_)) ||
             OB_FAIL(splicer.add_column("total_macro_block_count", item.total_macro_block_count_)) ||
             OB_FAIL(splicer.add_column("finish_macro_block_count", item.finish_macro_block_count_)) ||
             OB_FAIL(splicer.add_column("log_info", log_info)) ||
             OB_FAIL(splicer.add_column("log_size", item.log_size_)) ||
             OB_FAIL(splicer.add_column("result", item.result_)) ||
             OB_FAIL(splicer.add_column("comment", item.comment_.ptr()))) {
    LOG_WARN("failed to fill pg validate task item", K(ret));
  }
  return ret;
}

int ObPGValidateTaskOperator::get_pg_validate_task(const uint64_t tenant_id, const common::ObSqlString& sql,
    common::ObISQLClient& sql_proxy, common::ObIArray<ObPGValidateTaskItem>& items)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_UNLIKELY(!sql.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("failed to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        ObPGValidateTaskItem item;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get next row", K(ret));
          }
        } else {
          if (OB_FAIL(extract_pg_task(result, item))) {
            LOG_WARN("failed to extract pg task", K(ret), KP(result));
          } else if (OB_FAIL(items.push_back(item))) {
            LOG_WARN("failed to push pg validate task item into array", K(ret), K(item));
          }
        }
      }
    }
  }
  return ret;
}

int ObPGValidateTaskOperator::extract_pg_task(common::sqlclient::ObMySQLResult* result, ObPGValidateTaskItem& item)
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0;
  char ip[common::OB_MAX_SERVER_ADDR_SIZE] = "";
  int port = 0;
  char validate_status[common::OB_DEFAULT_STATUS_LENTH] = "";
  char comment[common::MAX_TABLE_COMMENT_LENGTH] = "";
  char log_info[64];
  char trace_id[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  uint64_t value[2];

  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extract pg task get invalid arguments", K(ret), KP(result));
  } else {
    EXTRACT_INT_FIELD_MYSQL(*result, "job_id", item.job_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "task_id", item.task_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "table_id", item.table_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", item.tenant_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "incarnation", item.incarnation_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "archive_round", item.archive_round_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "backup_set_id", item.backup_set_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "partition_id", item.partition_id_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "status", validate_status, OB_DEFAULT_STATUS_LENTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "trace_id", trace_id, OB_MAX_TRACE_ID_BUFFER_SIZE, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "svr_ip", ip, OB_MAX_SERVER_ADDR_SIZE, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", port, int);
    EXTRACT_INT_FIELD_MYSQL(*result, "total_partition_count", item.total_partition_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_partition_count", item.finish_partition_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "total_macro_block_count", item.total_macro_block_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "finish_macro_block_count", item.finish_macro_block_count_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "log_info", log_info, 64, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(*result, "log_size", item.log_size_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "result", item.result_, int32_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "comment", comment, MAX_TABLE_COMMENT_LENGTH, tmp_real_str_len);
    UNUSED(tmp_real_str_len);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(item.set_status(validate_status))) {
        LOG_WARN("failed to set validate status", K(ret));
      } else if (FALSE_IT(sscanf(trace_id, "%lu_%lu", &value[0], &value[1]))) {
        // do nothing
      } else if (OB_FAIL(item.trace_id_.set(value))) {
        LOG_WARN("failed to set trace id", K(ret));
      } else if (port > 0 && !item.server_.set_ip_addr(ip, port)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to set server ip addr", K(ret));
      }
    }
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
