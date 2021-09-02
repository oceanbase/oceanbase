// Copyright 2021 OceanBase Inc. All Rights Reserved
// Author:
//     yanfeng <yangyi.yyy@alibaba-inc.com>
// Normalizer:
//     yanfeng <yangyi.yyy@alibaba-inc.com

#define USING_LOG_PREFIX SHARE
#include "share/backup/ob_backup_backuppiece_operator.h"

using namespace oceanbase::share;

namespace oceanbase {
namespace share {

int ObIBackupBackupPieceJobOperator::fill_one_item(const ObBackupBackupPieceJobInfo& item, ObDMLSqlSplicer& splicer)
{
  int ret = OB_SUCCESS;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(item));
  } else if (OB_FAIL(item.backup_dest_.get_backup_dest_str(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(item));
  } else if (OB_FAIL(splicer.add_pk_column("job_id", item.job_id_)) ||
             OB_FAIL(splicer.add_pk_column("tenant_id", item.tenant_id_)) ||
             OB_FAIL(splicer.add_pk_column("incarnation", item.incarnation_)) ||
             OB_FAIL(splicer.add_pk_column("backup_piece_id", item.piece_id_)) ||
             OB_FAIL(splicer.add_column("result", item.result_)) ||
             OB_FAIL(splicer.add_column("max_backup_times", item.max_backup_times_)) ||
             OB_FAIL(splicer.add_column("status", item.get_status_str())) ||
             OB_FAIL(splicer.add_column("backup_dest", backup_dest_str)) ||
             OB_FAIL(splicer.add_column("comment", item.comment_.ptr())) ||
             OB_FAIL(splicer.add_column("type", item.type_))) {
    LOG_WARN("failed to fill backup backupset job item", KR(ret), K(item));
  }
  return ret;
}

int ObIBackupBackupPieceJobOperator::extract_one_item(
    sqlclient::ObMySQLResult* result, ObBackupBackupPieceJobInfo& item)
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0;
  char status_str[OB_DEFAULT_STATUS_LENTH] = "";
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  char comment_str[MAX_TABLE_COMMENT_LENGTH] = "";
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("result should not be null", KR(ret), KP(result));
  } else {
    EXTRACT_INT_FIELD_MYSQL(*result, "job_id", item.job_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", item.tenant_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "incarnation", item.incarnation_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "backup_piece_id", item.piece_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "result", item.result_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "max_backup_times", item.max_backup_times_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "status", status_str, OB_DEFAULT_STATUS_LENTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "backup_dest", backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "comment", comment_str, MAX_TABLE_COMMENT_LENGTH, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(*result, "type", item.type_, int64_t);
    UNUSED(tmp_real_str_len);

    if (FAILEDx(item.backup_dest_.set(backup_dest_str))) {
      LOG_WARN("failed to set backup dest", KR(ret), K(backup_dest_str));
    } else if (OB_FAIL(item.set_status(status_str))) {
      LOG_WARN("failed to set status", KR(ret), K(status_str));
    } else if (OB_FAIL(item.comment_.assign(comment_str))) {
      LOG_WARN("failed to assign comment", KR(ret), K(comment_str));
    }
  }
  return ret;
}

int ObIBackupBackupPieceJobOperator::get_item_list(const common::ObSqlString& sql, common::ObISQLClient& proxy,
    common::ObIArray<ObBackupBackupPieceJobInfo>& item_list)
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
        ObBackupBackupPieceJobInfo item;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get next row", KR(ret));
          }
        } else if (OB_FAIL(extract_one_item(result, item))) {
          LOG_WARN("failed to extract backup backuppiece job", KR(ret), K(item));
        } else if (OB_FAIL(item_list.push_back(item))) {
          LOG_WARN("failed to push back item", KR(ret), K(item));
        }
      }
    }
  }
  return ret;
}

int ObBackupBackupPieceJobOperator::insert_job_item(const ObBackupBackupPieceJobInfo& item, common::ObISQLClient& proxy)
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
    if (OB_FAIL(ObIBackupBackupPieceJobOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", KR(ret), K(item));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column name", KR(ret));
    } else if (OB_FAIL(splicer.splice_insert_sql(OB_ALL_BACKUP_BACKUPPIECE_JOB_TNAME, sql))) {
      LOG_WARN("failed to splice insert update sql", KR(ret), K(item));
    } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObBackupBackupPieceJobOperator::get_all_job_items(
    common::ObISQLClient& proxy, common::ObIArray<ObBackupBackupPieceJobInfo>& items)
{
  int ret = OB_SUCCESS;
  items.reset();
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s", OB_ALL_BACKUP_BACKUPPIECE_JOB_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else if (OB_FAIL(ObIBackupBackupPieceJobOperator::get_item_list(sql, proxy, items))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql));
  }
  return ret;
}

int ObBackupBackupPieceJobOperator::get_one_job(
    common::ObISQLClient& proxy, common::ObIArray<ObBackupBackupPieceJobInfo>& items)
{
  int ret = OB_SUCCESS;
  items.reset();
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s LIMIT 1", OB_ALL_BACKUP_BACKUPPIECE_JOB_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else if (OB_FAIL(ObIBackupBackupPieceJobOperator::get_item_list(sql, proxy, items))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql));
  }
  return ret;
}

int ObBackupBackupPieceJobOperator::get_job_item(
    const int64_t job_id, common::ObISQLClient& proxy, ObBackupBackupPieceJobInfo& item)
{
  int ret = OB_SUCCESS;
  item.reset();
  ObSqlString sql;
  ObArray<ObBackupBackupPieceJobInfo> items;

  if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup backuppiece job get invalid argument", KR(ret), K(job_id));
  } else if (OB_FAIL(
                 sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld", OB_ALL_BACKUP_BACKUPPIECE_JOB_TNAME, job_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(job_id));
  } else if (OB_FAIL(ObIBackupBackupPieceJobOperator::get_item_list(sql, proxy, items))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", KR(ret), K(items.count()));
  } else if (0 == items.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("backup backuppiece job do not exist", KR(ret), K(job_id));
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObBackupBackupPieceJobOperator::report_job_item(const ObBackupBackupPieceJobInfo& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer splicer;

  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(item));
  } else {
    if (OB_FAIL(ObIBackupBackupPieceJobOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", KR(ret), K(item));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", KR(ret));
    } else if (OB_FAIL(splicer.splice_update_sql(OB_ALL_BACKUP_BACKUPPIECE_JOB_TNAME, sql))) {
      LOG_WARN("failed to splice insert sql", KR(ret), K(item));
    } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret));
    } else if (1 != affected_rows && 0 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows));
    } else {
      LOG_INFO("report backup backupiece job item", K(sql));
    }
  }
  return ret;
}

int ObBackupBackupPieceJobOperator::remove_job_item(const int64_t job_id, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer splicer;
  ObDMLExecHelper exec(proxy, OB_SYS_TENANT_ID);
  if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(job_id));
  } else if (OB_FAIL(splicer.add_pk_column("job_id", job_id))) {
    LOG_WARN("failed to add pk column", KR(ret), K(job_id));
  } else if (OB_FAIL(exec.exec_delete(OB_ALL_BACKUP_BACKUPPIECE_JOB_TNAME, splicer, affected_rows))) {
    LOG_WARN("failed to exec delete", KR(ret));
  }
  return ret;
}

int ObBackupBackupPieceJobHistoryOperator::insert_job_item(
    const ObBackupBackupPieceJobInfo& item, common::ObISQLClient& proxy)
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
    if (OB_FAIL(ObIBackupBackupPieceJobOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", KR(ret), K(item));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column name", KR(ret));
    } else if (OB_FAIL(splicer.splice_insert_sql(OB_ALL_BACKUP_BACKUPPIECE_JOB_HISTORY_TNAME, sql))) {
      LOG_WARN("failed to splice insert update sql", KR(ret), K(item));
    } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObBackupBackupPieceJobHistoryOperator::get_job_item(
    const int64_t job_id, common::ObISQLClient& proxy, ObBackupBackupPieceJobInfo& item)
{
  int ret = OB_SUCCESS;
  item.reset();
  ObSqlString sql;
  ObArray<ObBackupBackupPieceJobInfo> items;

  if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup backuppiece job get invalid argument", KR(ret), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT * FROM %s WHERE job_id = %ld", OB_ALL_BACKUP_BACKUPPIECE_JOB_HISTORY_TNAME, job_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(job_id));
  } else if (OB_FAIL(ObIBackupBackupPieceJobOperator::get_item_list(sql, proxy, items))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql));
  } else if (items.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", KR(ret), K(items.count()));
  } else if (0 == items.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("backup backuppiece job do not exist", KR(ret), K(job_id));
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObBackupBackupPieceJobHistoryOperator::report_job_item(
    const ObBackupBackupPieceJobInfo& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer splicer;

  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(item));
  } else {
    if (OB_FAIL(ObIBackupBackupPieceJobOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", KR(ret), K(item));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", KR(ret));
    } else if (OB_FAIL(splicer.splice_insert_update_sql(OB_ALL_BACKUP_BACKUPPIECE_JOB_HISTORY_TNAME, sql))) {
      LOG_WARN("failed to splice insert sql", KR(ret), K(item));
    } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret));
    } else {
      LOG_INFO("report backup backupiece job history item", K(sql));
    }
  }
  return ret;
}

int ObBackupBackupPieceJobHistoryOperator::remove_job_item(const int64_t job_id, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer splicer;
  ObDMLExecHelper exec(proxy, OB_SYS_TENANT_ID);
  if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(job_id));
  } else if (OB_FAIL(splicer.add_pk_column("job_id", job_id))) {
    LOG_WARN("failed to add pk column", KR(ret), K(job_id));
  } else if (OB_FAIL(exec.exec_delete(OB_ALL_BACKUP_BACKUPPIECE_JOB_HISTORY_TNAME, splicer, affected_rows))) {
    LOG_WARN("failed to exec delete", KR(ret));
  }
  return ret;
}

int ObIBackupBackupPieceTaskOperator::fill_one_item(const ObBackupBackupPieceTaskInfo& item, ObDMLSqlSplicer& splicer)
{
  int ret = OB_SUCCESS;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(item));
  } else if (OB_FAIL(item.backup_dest_.get_backup_dest_str(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(item));
  } else if (OB_FAIL(splicer.add_pk_column("job_id", item.job_id_)) ||
             OB_FAIL(splicer.add_pk_column("incarnation", item.incarnation_)) ||
             OB_FAIL(splicer.add_pk_column("tenant_id", item.tenant_id_)) ||
             OB_FAIL(splicer.add_pk_column("round_id", item.round_id_)) ||
             OB_FAIL(splicer.add_pk_column("backup_piece_id", item.piece_id_)) ||
             OB_FAIL(splicer.add_pk_column("copy_id", item.copy_id_)) ||
             OB_FAIL(splicer.add_time_column("start_time", item.start_ts_)) ||
             OB_FAIL(splicer.add_time_column("end_time", item.end_ts_)) ||
             OB_FAIL(splicer.add_column("status", item.get_status_str())) ||
             OB_FAIL(splicer.add_column("backup_dest", backup_dest_str)) ||
             OB_FAIL(splicer.add_column("result", item.result_)) || OB_FAIL(splicer.add_column("comment", ""))) {
    LOG_WARN("failed to fill backup backupset task item", KR(ret), K(item));
  }

  return ret;
}

int ObIBackupBackupPieceTaskOperator::extract_one_item(
    sqlclient::ObMySQLResult* result, ObBackupBackupPieceTaskInfo& item)
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0;
  char status_str[OB_DEFAULT_STATUS_LENTH] = "";
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  char comment_str[MAX_TABLE_COMMENT_LENGTH] = "";
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("result should not be null", KR(ret), KP(result));
  } else {
    EXTRACT_INT_FIELD_MYSQL(*result, "job_id", item.job_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "incarnation", item.incarnation_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", item.tenant_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "round_id", item.round_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "backup_piece_id", item.piece_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "copy_id", item.copy_id_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "status", status_str, OB_DEFAULT_STATUS_LENTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "backup_dest", backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(*result, "result", item.result_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "comment", comment_str, MAX_TABLE_COMMENT_LENGTH, tmp_real_str_len);
    UNUSED(tmp_real_str_len);

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(result->get_timestamp("start_time", NULL, item.start_ts_))) {
      LOG_WARN("failed to get start timestamp", KR(ret));
    } else if (OB_FAIL(result->get_timestamp("end_time", NULL, item.end_ts_))) {
      LOG_WARN("failed to get end timestamp", KR(ret));
    } else if (OB_FAIL(item.set_status(status_str))) {
      LOG_WARN("failed to set status", KR(ret), K(status_str));
    } else if (OB_FAIL(item.backup_dest_.set(backup_dest_str))) {
      LOG_WARN("failed to set backup dest", KR(ret), K(backup_dest_str));
    }
  }
  return ret;
}

int ObIBackupBackupPieceTaskOperator::get_item_list(const common::ObSqlString& sql, common::ObISQLClient& proxy,
    common::ObIArray<ObBackupBackupPieceTaskInfo>& item_list)
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
        ObBackupBackupPieceTaskInfo item;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get next row", KR(ret));
          }
        } else if (OB_FAIL(extract_one_item(result, item))) {
          LOG_WARN("failed to extract backup backuppiece job", KR(ret), K(item));
        } else if (OB_FAIL(item_list.push_back(item))) {
          LOG_WARN("failed to push back item", KR(ret), K(item));
        }
      }
    }
  }
  return ret;
}

int ObBackupBackupPieceTaskOperator::get_job_task_count(
    const ObBackupBackupPieceJobInfo& job_info, common::ObISQLClient& sql_proxy, int64_t& task_count)
{
  int ret = OB_SUCCESS;
  const int64_t job_id = job_info.job_id_;
  task_count = 0;
  ObSqlString sql;
  sqlclient::ObMySQLResult* result = NULL;
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    if (OB_FAIL(sql.assign_fmt(
            "select count(*) as count from %s where job_id = %ld", OB_ALL_BACKUP_BACKUPPIECE_TASK_TNAME, job_id))) {
      LOG_WARN("failed to init backup info sql", KR(ret));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
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

int ObBackupBackupPieceTaskOperator::insert_task_item(
    const ObBackupBackupPieceTaskInfo& item, common::ObISQLClient& proxy)
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
    if (OB_FAIL(ObIBackupBackupPieceTaskOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", KR(ret), K(item));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column name", KR(ret));
    } else if (OB_FAIL(splicer.splice_insert_update_sql(OB_ALL_BACKUP_BACKUPPIECE_TASK_TNAME, sql))) {
      LOG_WARN("failed to splice insert update sql", KR(ret), K(item));
    } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObBackupBackupPieceTaskOperator::get_all_task_items(
    const int64_t job_id, common::ObISQLClient& proxy, ObIArray<ObBackupBackupPieceTaskInfo>& items)
{
  int ret = OB_SUCCESS;
  items.reset();
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s", OB_ALL_BACKUP_BACKUPPIECE_TASK_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret), K(job_id));
  } else if (OB_FAIL(ObIBackupBackupPieceTaskOperator::get_item_list(sql, proxy, items))) {
    LOG_WARN("fail to get tenant backup backupiece task item", KR(ret), K(sql));
  }
  return ret;
}

int ObBackupBackupPieceTaskOperator::get_doing_task_items(const int64_t job_id, const uint64_t tenant_id,
    common::ObISQLClient& proxy, common::ObIArray<ObBackupBackupPieceTaskInfo>& items)
{
  int ret = OB_SUCCESS;
  items.reset();
  ObSqlString sql;
  if (job_id < 0 || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup backuppiece task get invalid argument", KR(ret), K(job_id), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu",
                 OB_ALL_BACKUP_BACKUPPIECE_TASK_TNAME,
                 job_id,
                 tenant_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(job_id), K(tenant_id));
  } else if (OB_FAIL(ObIBackupBackupPieceTaskOperator::get_item_list(sql, proxy, items))) {
    LOG_WARN("fail to get tenant backup backuppiece task item", KR(ret), K(sql));
  }
  return ret;
}

int ObBackupBackupPieceTaskOperator::get_task_item(const int64_t job_id, const int64_t tenant_id,
    const int64_t piece_id, common::ObISQLClient& proxy, ObBackupBackupPieceTaskInfo& item)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupBackupPieceTaskInfo> item_list;
  ObSqlString sql;
  if (job_id < 0 || OB_INVALID_ID == tenant_id || piece_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup backuppiece task get invalid argument", KR(ret), K(job_id), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu AND backup_piece_id = %ld",
                 OB_ALL_BACKUP_BACKUPPIECE_TASK_TNAME,
                 job_id,
                 tenant_id,
                 piece_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(job_id), K(tenant_id), K(piece_id));
  } else if (OB_FAIL(ObIBackupBackupPieceTaskOperator::get_item_list(sql, proxy, item_list))) {
    LOG_WARN("fail to get tenant backup backuppiece task item", KR(ret), K(sql));
  } else if (item_list.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no doing task for tenant", KR(ret), K(job_id), K(tenant_id));
  } else if (item_list.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item list count should be 1", KR(ret), K(item_list.count()));
  } else {
    item = item_list.at(0);
  }
  return ret;
}

int ObBackupBackupPieceTaskOperator::get_smallest_doing_task(
    const int64_t job_id, const uint64_t tenant_id, common::ObISQLClient& proxy, ObBackupBackupPieceTaskInfo& item)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupBackupPieceTaskInfo> item_list;
  ObSqlString sql;
  if (job_id < 0 || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup backuppiece task get invalid argument", KR(ret), K(job_id), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld AND tenant_id = %lu AND status = 'DOING' "
                                    "ORDER BY backup_piece_id LIMIT 1",
                 OB_ALL_BACKUP_BACKUPPIECE_TASK_TNAME,
                 job_id,
                 tenant_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(job_id), K(tenant_id));
  } else if (OB_FAIL(ObIBackupBackupPieceTaskOperator::get_item_list(sql, proxy, item_list))) {
    LOG_WARN("fail to get tenant backup backuppiece task item", KR(ret), K(sql));
  } else if (item_list.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no doing task for tenant", KR(ret), K(job_id), K(tenant_id));
  } else if (item_list.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item list count should be 1", KR(ret), K(item_list.count()));
  } else {
    item = item_list.at(0);
  }
  return ret;
}

int ObBackupBackupPieceTaskOperator::update_task_finish(
    const uint64_t tenant_id, const int64_t job_id, const int64_t piece_id, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (job_id < 0 || piece_id < 0 || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update backup backuppiece task get invalid argument", KR(ret), K(job_id), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET status = '%s' WHERE"
                                    " job_id = %ld AND tenant_id = %lu AND backup_piece_id = %ld",
                 OB_ALL_BACKUP_BACKUPPIECE_TASK_TNAME,
                 "FINISH",
                 job_id,
                 tenant_id,
                 piece_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(job_id), K(tenant_id), K(piece_id));
  } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(sql));
  } else if (1 != affected_rows && 0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObBackupBackupPieceTaskOperator::report_task_item(
    const ObBackupBackupPieceTaskInfo& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer splicer;

  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(item));
  } else {
    if (OB_FAIL(ObIBackupBackupPieceTaskOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", KR(ret), K(item));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", KR(ret));
    } else if (OB_FAIL(splicer.splice_update_sql(OB_ALL_BACKUP_BACKUPPIECE_TASK_TNAME, sql))) {
      LOG_WARN("failed to splice insert sql", KR(ret), K(item));
    } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret));
    } else if (1 != affected_rows && 0 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows));
    } else {
      LOG_INFO("report backup backupiece task item", K(sql));
    }
  }
  return ret;
}

int ObBackupBackupPieceTaskOperator::remove_task_items(const int64_t job_id, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer splicer;
  ObDMLExecHelper exec(proxy, OB_SYS_TENANT_ID);
  if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(job_id));
  } else if (OB_FAIL(splicer.add_pk_column("job_id", job_id))) {
    LOG_WARN("failed to add pk column", KR(ret), K(job_id));
  } else if (OB_FAIL(exec.exec_delete(OB_ALL_BACKUP_BACKUPPIECE_TASK_TNAME, splicer, affected_rows))) {
    LOG_WARN("failed to exec delete", KR(ret));
  }
  return ret;
}

int ObBackupBackupPieceTaskHistoryOperator::insert_task_item(
    const ObBackupBackupPieceTaskInfo& item, common::ObISQLClient& proxy)
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
    if (OB_FAIL(ObIBackupBackupPieceTaskOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", KR(ret), K(item));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column name", KR(ret));
    } else if (OB_FAIL(splicer.splice_insert_sql(OB_ALL_BACKUP_BACKUPPIECE_TASK_HISTORY_TNAME, sql))) {
      LOG_WARN("failed to splice insert update sql", KR(ret), K(item));
    } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObBackupBackupPieceTaskHistoryOperator::get_task_items(
    const int64_t job_id, common::ObISQLClient& proxy, common::ObIArray<ObBackupBackupPieceTaskInfo>& items)
{
  int ret = OB_SUCCESS;
  items.reset();
  ObSqlString sql;
  if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup backuppiece job get invalid argument", KR(ret), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT * FROM %s WHERE job_id = %ld", OB_ALL_BACKUP_BACKUPPIECE_TASK_HISTORY_TNAME, job_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(job_id));
  } else if (OB_FAIL(ObIBackupBackupPieceTaskOperator::get_item_list(sql, proxy, items))) {
    LOG_WARN("fail to get tenant backup backupset task item", KR(ret), K(sql));
  }
  return ret;
}

int ObBackupBackupPieceTaskHistoryOperator::report_task_item(
    const ObBackupBackupPieceTaskInfo& item, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer splicer;

  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(item));
  } else {
    if (OB_FAIL(ObIBackupBackupPieceTaskOperator::fill_one_item(item, splicer))) {
      LOG_WARN("failed to fill one item", KR(ret), K(item));
    } else if (OB_FAIL(splicer.splice_column_names(columns))) {
      LOG_WARN("failed to splice column names", KR(ret));
    } else if (OB_FAIL(splicer.splice_insert_update_sql(OB_ALL_BACKUP_BACKUPPIECE_TASK_HISTORY_TNAME, sql))) {
      LOG_WARN("failed to splice insert sql", KR(ret), K(item));
    } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret));
    } else {
      LOG_INFO("report backup backupiece task history item", K(sql));
    }
  }
  return ret;
}

int ObBackupBackupPieceTaskHistoryOperator::report_task_items(
    const common::ObIArray<ObBackupBackupPieceTaskInfo>& items, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer splicer;
  const int64_t BATCH_CNT = 512;
  int64_t report_idx = 0;
  while (OB_SUCC(ret) && report_idx < items.count()) {
    sql.reuse();
    columns.reuse();
    const int64_t remain_cnt = items.count() - report_idx;
    int64_t cur_batch_cnt = remain_cnt < BATCH_CNT ? remain_cnt : BATCH_CNT;
    for (int64_t i = 0; OB_SUCC(ret) && i < cur_batch_cnt; ++i) {
      const ObBackupBackupPieceTaskInfo& item = items.at(report_idx + i);
      splicer.reuse();
      if (OB_FAIL(ObIBackupBackupPieceTaskOperator::fill_one_item(item, splicer))) {
        LOG_WARN("failed to fill one item", KR(ret), K(item));
      } else {
        if (0 == i) {
          if (OB_FAIL(splicer.splice_column_names(columns))) {
            LOG_WARN("failed to splice column names", KR(ret));
          } else if (OB_FAIL(sql.assign_fmt("INSERT /*+ use_plan_cache(none) */ INTO %s (%s) VALUES",
                         OB_ALL_BACKUP_BACKUPPIECE_TASK_HISTORY_TNAME,
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
      } else if (OB_FAIL(sql.append("  result = values(result)")) || OB_FAIL(sql.append(", status = values(status)")) ||
                 OB_FAIL(sql.append(", backup_dest = values(backup_dest)")) ||
                 OB_FAIL(sql.append(", comment = values(comment)"))) {
        LOG_WARN("failed to append to sql", KR(ret), K(sql));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
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
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
