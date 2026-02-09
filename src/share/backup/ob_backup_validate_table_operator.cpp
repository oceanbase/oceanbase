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
#include "ob_backup_validate_table_operator.h"
#include "src/share/inner_table/ob_inner_table_schema_constants.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
namespace share
{

uint64_t ObBackupValidateBaseTableOperator::get_exec_tenant_id(uint64_t tenant_id)
{
  return gen_meta_tenant_id(tenant_id);
}

//TODO(xingzhi): check if transaction is needed
int ObBackupValidateJobOperator::insert_job(common::ObISQLClient &proxy, const ObBackupValidateJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  if (!job_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(job_attr));
  } else if (OB_FAIL(fill_dml_with_job_(job_attr, dml))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to fill backup job", KR(ret), K(job_attr));
  } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_BACKUP_VALIDATE_JOB_TNAME, sql))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to splice insert update sql", KR(ret), K(job_attr));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(job_attr.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("[BACKUP_VALIDATE]fail to exec sql", KR(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]error unexpected, invalid affected rows", KR(ret), K(affected_rows), K(sql), K(job_attr));
  } else {
    LOG_INFO("[BACKUP_VALIDATE]success insert one backup validate job", K(job_attr), K(sql));
  }
  return ret;
}

int ObBackupValidateJobOperator::append_comment(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const int64_t job_id,
    const char *comment)
{
  int ret = OB_SUCCESS;
  if (tenant_id == OB_INVALID_ID || job_id <= 0 || OB_ISNULL(comment)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(tenant_id), K(job_id), KP(comment));
  } else {
    char comment_str[share::OB_COMMENT_LENGTH] = { 0 };
    int job_result = OB_SUCCESS;
    bool skip_update = false;
    {
      HEAP_VAR(ObMySQLProxy::ReadResult, res) {
        ObMySQLResult *result = NULL;
        ObSqlString sql;
        int64_t real_length = 0;
        if (OB_FAIL(sql.assign_fmt("SELECT %s, %s FROM %s WHERE %s=%lu AND %s=%ld FOR UPDATE",
                OB_STR_COMMENT,
                OB_STR_RESULT,
                OB_ALL_BACKUP_VALIDATE_JOB_TNAME,
                OB_STR_TENANT_ID, tenant_id,
                OB_STR_JOB_ID, job_id))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to build select sql", KR(ret));
        } else if (OB_FAIL(trans.read(res, get_exec_tenant_id(tenant_id), sql.ptr()))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to exec select", KR(ret), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[BACKUP_VALIDATE]result is null", KR(ret));
        } else if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_ENTRY_NOT_EXIST;
            LOG_WARN("[BACKUP_VALIDATE]job not exist", KR(ret), K(tenant_id), K(job_id));
          } else {
            LOG_WARN("[BACKUP_VALIDATE]failed to get next row", KR(ret));
          }
        } else {
          EXTRACT_STRBUF_FIELD_MYSQL((*result), OB_STR_COMMENT, comment_str, share::OB_COMMENT_LENGTH, real_length);
          EXTRACT_INT_FIELD_MYSQL((*result), OB_STR_RESULT, job_result, int);
          if (OB_SUCC(ret) && OB_SUCCESS != job_result) {
            skip_update = true;
            LOG_INFO("[BACKUP_VALIDATE]skip update comment because job not success",
                K(tenant_id), K(job_id), K(job_result));
          }
        }
      }
    }
    if (OB_SUCC(ret) && !skip_update) {
      int64_t pos = strlen(comment_str);
      if (OB_FAIL(databuff_printf(comment_str, share::OB_COMMENT_LENGTH, pos, "%s", comment))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to append comment", KR(ret), K(comment));
      } else if (OB_FAIL(update_comment(trans, tenant_id, job_id, comment_str))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to update comment", KR(ret), K(tenant_id), K(job_id), K(comment_str));
      }
    }
  }
  return ret;
}

int ObBackupValidateJobOperator::update_comment(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const int64_t job_id,
    const char *comment)
{
  int ret = OB_SUCCESS;
  if (tenant_id == OB_INVALID_ID || job_id <= 0 || OB_ISNULL(comment)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(tenant_id), K(job_id), KP(comment));
  } else {
    ObSqlString sql;
    int64_t affected_rows = -1;
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OB_STR_JOB_ID, job_id))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret), K(job_id));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret), K(tenant_id));
    } else if (OB_FAIL(dml.add_column(OB_STR_COMMENT, comment))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret), K(comment));
    } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_VALIDATE_JOB_TNAME, sql))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to splice_update_sql", KR(ret), K(sql));
    } else if (OB_FAIL(proxy.write(get_exec_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]invalid affected_rows", KR(ret), K(affected_rows), K(sql));
    } else {
      LOG_INFO("[BACKUP_VALIDATE]update comment", K(tenant_id), K(job_id), K(comment));
    }
  }
  return ret;
}

int ObBackupValidateJobOperator::fill_dml_with_job_(const ObBackupValidateJobAttr &job_attr, ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  int64_t parameter = 0;

  if (OB_FAIL(dml.add_pk_column(OB_STR_JOB_ID, job_attr.job_id_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, job_attr.tenant_id_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_INCARNATION, job_attr.incarnation_id_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_INITIATOR_TENANT_ID, job_attr.initiator_tenant_id_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_INITIATOR_JOB_ID, job_attr.initiator_job_id_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_VALIDATE_TYPE, job_attr.type_.get_str()))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_PATH, job_attr.validate_path_.ptr()))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (job_attr.path_type_.is_valid() && OB_FAIL(dml.add_column(OB_STR_PATH_TYPE, job_attr.path_type_.get_str()))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_VALIDATE_LEVEL, job_attr.level_.get_str()))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_DESCRIPTION, job_attr.description_.ptr()))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_START_TS, job_attr.start_ts_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_END_TS, job_attr.end_ts_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, job_attr.status_.get_str()))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_RESULT, job_attr.result_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(job_attr.get_executor_tenant_id_str(dml))) {
    LOG_WARN("fail to get backup tenant id str", KR(ret), K(job_attr));
  } else if (OB_FAIL(job_attr.get_validate_ids_str(dml))) {
    LOG_WARN("fail to get validate ids str", KR(ret), K(job_attr));
  }
  return ret;
}

int ObBackupValidateJobOperator::get_jobs(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    ObIArray<ObBackupValidateJobAttr> &job_attrs)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  HEAP_VAR(ObMySQLProxy::ReadResult, res) {
    ObMySQLResult *result = NULL;
    if (OB_FAIL(fill_select_job_sql_(sql))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to fill select job sql", KR(ret));
    } else if (OB_FAIL(sql.append_fmt(" order by job_id"))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to append sql", KR(ret));
    } else if (OB_FAIL(proxy.read(res, get_exec_tenant_id(tenant_id), sql.ptr()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]result is null", KR(ret), K(sql));
    } else if (OB_FAIL(parse_job_result_(*result, job_attrs))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to fill job attr", KR(ret), K(sql));
    } else {
      LOG_INFO("[BACKUP_VALIDATE]success to get jobs", K(job_attrs));
    }
  }
  return ret;
}

int ObBackupValidateJobOperator::update_task_count(
    common::ObISQLClient &proxy,
    const ObBackupValidateJobAttr &job_attr,
    const bool is_total)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;

  if (!job_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(job_attr));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OB_STR_JOB_ID, job_attr.job_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(job_attr));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, job_attr.tenant_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(job_attr));
    } else if (is_total && OB_FAIL(dml.add_column(OB_STR_TASK_COUNT, job_attr.task_count_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret), K(job_attr));
    } else if (!is_total && OB_FAIL(dml.add_column(OB_STR_SUCCESS_TASK_COUNT, job_attr.success_task_count_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret), K(job_attr));
    } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_VALIDATE_JOB_TNAME, sql))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to splice update sql", KR(ret));
    } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(job_attr.tenant_id_), sql.ptr(), affected_rows))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
    } else if (1 != affected_rows && 0 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]invalid affected_rows", KR(ret), K(affected_rows), K(sql));
    } else {
      FLOG_INFO("[BACKUP_VALIDATE]success update task count", K(job_attr), K(sql));
    }
  }
  return ret;
}

int ObBackupValidateJobOperator::update_retry_count(
    common::ObISQLClient &proxy,
    const ObBackupValidateJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (!job_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(job_attr));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, job_attr.tenant_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(job_attr));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_JOB_ID, job_attr.job_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(job_attr));
    } else if (OB_FAIL(dml.add_column(OB_STR_RETRY_COUNT, job_attr.retry_count_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret), K(job_attr));
    } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_VALIDATE_JOB_TNAME, sql))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to splice update sql", KR(ret));
    } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(job_attr.tenant_id_), sql.ptr(), affected_rows))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]invalid affected_rows", KR(ret), K(affected_rows), K(sql), K(job_attr));
    }
  }
  return ret;
}

int ObBackupValidateJobOperator::get_job(
    common::ObISQLClient &proxy,
    bool need_lock,
    const uint64_t tenant_id,
    const int64_t job_id,
    const bool is_initiator,
    ObBackupValidateJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!is_valid_tenant_id(tenant_id) || job_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", K(tenant_id), K(job_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(fill_select_job_sql_(sql))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to fill select backup validate job sql", KR(ret));
      } else if (OB_FAIL(sql.append_fmt(" where %s=%lu", OB_STR_TENANT_ID, tenant_id))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to fill select backup job sql", KR(ret), K(tenant_id));
      } else if (!is_initiator && OB_FAIL(sql.append_fmt(" and %s=%ld", OB_STR_JOB_ID, job_id))) {
        LOG_WARN("fail to append sql", KR(ret));
      } else if (is_initiator && OB_FAIL(sql.append_fmt(" and %s=%ld", OB_STR_INITIATOR_JOB_ID, job_id))) {
        LOG_WARN("fail to append sql", KR(ret));
      } else if (need_lock && OB_FAIL(sql.append_fmt(" for update"))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to append sql", KR(ret));
      } else if (OB_FAIL(proxy.read(res, get_exec_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql), K(tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[BACKUP_VALIDATE]result is null", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
        LOG_WARN("[BACKUP_VALIDATE]failed to get next row", KR(ret));
      } else if (OB_FAIL(do_parse_job_result_(*result, job_attr))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to parse job result", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupValidateJobOperator::fill_select_job_sql_(ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.assign_fmt("select %s", OB_STR_JOB_ID))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to assign fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_TENANT_ID))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_INCARNATION))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_INITIATOR_TENANT_ID))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_INITIATOR_JOB_ID))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_VALIDATE_TYPE))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_PATH))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_PATH_TYPE))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_ID))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_VALIDATE_LEVEL))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_START_TS))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_END_TS))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_STATUS))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_RESULT))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_RETRY_COUNT))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_EXECUTOR_TENANT_ID))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_DESCRIPTION))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(" from %s", OB_ALL_BACKUP_VALIDATE_JOB_TNAME))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  }
  return ret;
}

int ObBackupValidateJobOperator::parse_job_result_(
    sqlclient::ObMySQLResult &result,
    ObIArray<ObBackupValidateJobAttr> &job_attrs)
{
  int ret = OB_SUCCESS;
  // traverse each returned row
  while (OB_SUCC(ret)) {
    ObBackupValidateJobAttr job;
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("[BACKUP_VALIDATE]failed to get next row", KR(ret));
      }
    } else if (OB_FAIL(do_parse_job_result_(result, job))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to parse job result", KR(ret));
    } else if (OB_FAIL(job_attrs.push_back(job))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to push back job", KR(ret), K(job));
    }
  }
  return ret;
}

int ObBackupValidateJobOperator::do_parse_job_result_(sqlclient::ObMySQLResult &result, ObBackupValidateJobAttr &job)
{
  int ret = OB_SUCCESS;
  int64_t real_length = 0;
  int64_t pos = 0;

  HEAP_VARS_4((char[OB_MAX_BACKUP_DEST_LENGTH], validate_path),
              (char[OB_SYS_TASK_TYPE_LENGTH], validate_path_type),
              (char[OB_DEFAULT_STATUS_LENTH], status_str),
              (char[OB_SYS_TASK_TYPE_LENGTH], validate_type_str)) {
    HEAP_VARS_4((char[OB_SYS_TASK_TYPE_LENGTH], validate_level),
                (char[OB_INNER_TABLE_DEFAULT_VALUE_LENTH], executor_tenant_id_str),
                (char[OB_INNER_TABLE_DEFAULT_VALUE_LENTH], validate_ids),
                (char[OB_INNER_TABLE_DEFAULT_VALUE_LENTH], desc_str)) {
      MEMSET(validate_path, 0, OB_MAX_BACKUP_DEST_LENGTH);
      MEMSET(validate_path_type, 0, OB_SYS_TASK_TYPE_LENGTH);
      MEMSET(status_str, 0, OB_DEFAULT_STATUS_LENTH);
      MEMSET(validate_type_str, 0, OB_SYS_TASK_TYPE_LENGTH);
      MEMSET(validate_level, 0, OB_SYS_TASK_TYPE_LENGTH);
      MEMSET(executor_tenant_id_str, 0, OB_INNER_TABLE_DEFAULT_VALUE_LENTH);
      MEMSET(validate_ids, 0, OB_INNER_TABLE_DEFAULT_VALUE_LENTH);
      MEMSET(desc_str, 0, OB_INNER_TABLE_DEFAULT_VALUE_LENTH);

      EXTRACT_INT_FIELD_MYSQL(result, OB_STR_JOB_ID, job.job_id_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, job.tenant_id_, uint64_t);
      EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INCARNATION, job.incarnation_id_, uint64_t);
      EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INITIATOR_TENANT_ID, job.initiator_tenant_id_, uint64_t);
      EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INITIATOR_JOB_ID, job.initiator_job_id_, int64_t);
      EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_EXECUTOR_TENANT_ID, executor_tenant_id_str,
                                    OB_INNER_TABLE_DEFAULT_VALUE_LENTH, real_length);
      EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_VALIDATE_TYPE, validate_type_str, OB_SYS_TASK_TYPE_LENGTH, real_length);
      EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_PATH, validate_path, OB_MAX_BACKUP_DEST_LENGTH, real_length);
      EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_PATH_TYPE, validate_path_type, OB_SYS_TASK_TYPE_LENGTH, real_length);
      EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_VALIDATE_LEVEL, validate_level, OB_SYS_TASK_TYPE_LENGTH, real_length);
      EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_ID, validate_ids, OB_INNER_TABLE_DEFAULT_VALUE_LENTH, real_length);
      EXTRACT_INT_FIELD_MYSQL(result, OB_STR_START_TS, job.start_ts_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(result, OB_STR_END_TS, job.end_ts_, int64_t);
      EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STATUS, status_str, OB_DEFAULT_STATUS_LENTH, real_length);
      EXTRACT_INT_FIELD_MYSQL(result, OB_STR_RESULT, job.result_, int);
      EXTRACT_INT_FIELD_MYSQL(result, OB_STR_RETRY_COUNT, job.retry_count_, int);
      EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_DESCRIPTION, desc_str, OB_INNER_TABLE_DEFAULT_VALUE_LENTH, real_length);

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(job.set_executor_tenant_id(executor_tenant_id_str))) {
        LOG_WARN("[BACKUP_VALIDATE]fail to set executor tenant id", KR(ret), K(executor_tenant_id_str));
      } else if (OB_FAIL(job.type_.set(validate_type_str))) {
        LOG_WARN("[BACKUP_VALIDATE]fail to set validate type", KR(ret), K(validate_type_str));
      } else if (OB_FAIL(job.validate_path_.assign(validate_path))) {
        LOG_WARN("[BACKUP_VALIDATE]fail to assign validate path", KR(ret), K(validate_path));
      } else if (0 != strlen(validate_path_type) && OB_FAIL(job.path_type_.set(validate_path_type))) {
        LOG_WARN("[BACKUP_VALIDATE]fail to set path type", KR(ret), K(validate_path_type));
      } else if (OB_FAIL(job.level_.set(validate_level))) {
        LOG_WARN("[BACKUP_VALIDATE]fail to set validate level", KR(ret), K(validate_level));
      } else if (validate_ids[0] != '\0' && OB_FAIL(job.set_validate_ids(validate_ids))) {
        LOG_WARN("[BACKUP_VALIDATE]fail to set validate ids", KR(ret), K(validate_ids));
      } else if (OB_FAIL(job.status_.set_staust(status_str))) {
        LOG_WARN("[BACKUP_VALIDATE]fail to set status", KR(ret), K(status_str));
      } else if (OB_FAIL(job.description_.assign(desc_str))) {
        LOG_WARN("[BACKUP_VALIDATE]fail to set description", KR(ret), K(desc_str));
      }
    } // end of HEAP_VARS_4 (validate_level, executor_tenant_id_str, validate_ids, desc_str)
  } // end of HEAP_VARS_4 (validate_path, validate_path_type, status_str, validate_type_str)

  return ret;
}

int ObBackupValidateJobOperator::cnt_jobs(common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const uint64_t initiator_tenant_id,
    const int64_t initiator_job_id,
    int64_t &cnt)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArray<ObBackupValidateJobAttr> job_attrs;
  if (tenant_id == OB_INVALID_TENANT_ID || initiator_tenant_id == OB_INVALID_TENANT_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(fill_select_job_sql_(sql))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to fill select backup validate job sql", KR(ret));
      } else if (OB_FAIL(sql.append_fmt(" where %s=%lu and %s=%lu and %s=%lu",
          OB_STR_TENANT_ID, tenant_id, OB_STR_INITIATOR_TENANT_ID,
          initiator_tenant_id, OB_STR_INITIATOR_JOB_ID, initiator_job_id))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
      } else if (OB_FAIL(proxy.read(res, get_exec_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql), K(tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[BACKUP_VALIDATE]result is null", KR(ret), K(sql));
      } else if (OB_FAIL(parse_job_result_(*result, job_attrs))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to parse result", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      cnt = job_attrs.count();
    }
  }
  return ret;
}

int ObBackupValidateJobOperator::advance_job_status(
    common::ObISQLClient &proxy,
    const ObBackupValidateJobAttr &job_attr,
    const ObBackupValidateStatus &next_status,
    const int result,
    const int64_t end_ts)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml;
  if (!next_status.is_valid() || !job_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(next_status), K(job_attr));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_JOB_ID, job_attr.job_id_))) {
    LOG_WARN("[BACKUP_VALIDATE]add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, job_attr.tenant_id_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, next_status.get_str()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_END_TS, end_ts))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_RESULT, result))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_VALIDATE_JOB_TNAME, sql))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to splice_update_sql", KR(ret));
  } else if (ObBackupValidateStatus::Status::CANCELING != next_status.status_
    && OB_FAIL(sql.append_fmt(" and %s='%s'", OB_STR_STATUS, job_attr.status_.get_str()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append sql", KR(ret), K(sql));
  } else if (OB_FAIL(proxy.write(get_exec_tenant_id(job_attr.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]invalid affected_rows", KR(ret), K(affected_rows), K(sql), K(next_status));
  } else {
    LOG_INFO("[BACKUP_VALIDATE]advance status", K(job_attr), K(next_status));
  }
  return ret;
}

int ObBackupValidateJobOperator::move_to_history(
  common::ObISQLClient &proxy,
  const uint64_t tenant_id,
  const int64_t job_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (tenant_id == OB_INVALID_TENANT_ID || job_id <=0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt(
                        "insert into %s select * from %s where %s=%lu and %s=%lu",
                        OB_ALL_BACKUP_VALIDATE_JOB_HISTORY_TNAME, OB_ALL_BACKUP_VALIDATE_JOB_TNAME,
                        OB_STR_JOB_ID, job_id, OB_STR_TENANT_ID, tenant_id))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to init sql", KR(ret));
  } else if (OB_FAIL(proxy.write(get_exec_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql), K(tenant_id));
  } else if (OB_FALSE_IT(sql.reset())) {
  } else if (OB_FAIL(sql.assign_fmt("delete from %s where %s=%lu and %s=%lu",
                        OB_ALL_BACKUP_VALIDATE_JOB_TNAME, OB_STR_JOB_ID, job_id, OB_STR_TENANT_ID, tenant_id))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to init sql", KR(ret));
  } else if (OB_FAIL(proxy.write(get_exec_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql), K(tenant_id));
  } else {
    LOG_INFO("[BACKUP_VALIDATE]succeed move backup job to history table", K(tenant_id), K(job_id));
  }
  return ret;
}

int ObBackupValidateJobOperator::report_failed_to_sys_tenant(
  common::ObISQLClient &proxy,
  const ObBackupValidateJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  share::ObDMLSqlSplicer dml;
  int64_t affected_rows = -1;
  if (OB_SUCCESS == job_attr.result_) {
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, OB_SYS_TENANT_ID))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_JOB_ID, job_attr.initiator_job_id_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_RESULT, job_attr.result_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_VALIDATE_JOB_TNAME, sql))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to splice update sql", KR(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(OB_SYS_TENANT_ID), sql.ptr(), affected_rows))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to write sql", KR(ret), K(sql));
  }
  return ret;
}

int ObBackupValidateJobOperator::cancel_jobs(common::ObISQLClient &proxy, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml;
  ObBackupValidateStatus status(ObBackupValidateStatus::Status::CANCELING);
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, status.get_str()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_VALIDATE_JOB_TNAME, sql))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to splice_update_sql", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(" and (%s='INIT' or %s='DOING')", OB_STR_STATUS, OB_STR_STATUS))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append sql", KR(ret), K(sql));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
  } else {
    LOG_INFO("success cancel the validate jobs of tenant", KR(ret), K(tenant_id));
  }
  return ret;
}

/*
*----------------------ObBackupValidateTaskOperator-------------------------
*/
int ObBackupValidateTaskOperator::insert_task(common::ObISQLClient &proxy, const ObBackupValidateTaskAttr &task_attr)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  if (!task_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(task_attr));
  } else if (OB_FAIL(fill_dml_with_task_(task_attr, dml))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to fill backup task", KR(ret), K(task_attr));
  } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_BACKUP_VALIDATE_TASK_TNAME, sql))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to splice insert update sql", KR(ret), K(task_attr));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(task_attr.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("[BACKUP_VALIDATE]fail to exec sql", KR(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]error unexpected, invalid affected rows", KR(ret), K(affected_rows), K(sql), K(task_attr));
  } else {
    LOG_INFO("[BACKUP_VALIDATE]success insert one backup validate task", K(task_attr), K(sql));
  }
  return ret;
}

int ObBackupValidateTaskOperator::fill_dml_with_task_(const ObBackupValidateTaskAttr &task_attr, ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, task_attr.tenant_id_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, task_attr.task_id_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_INCARNATION, task_attr.incarnation_id_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_JOB_ID, task_attr.job_id_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_TASK_TYPE, task_attr.type_.get_str()))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_PATH, task_attr.validate_path_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (task_attr.path_type_.is_valid() && OB_FAIL(dml.add_column(OB_STR_PATH_TYPE, task_attr.path_type_.get_str()))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_DEST_ID, task_attr.dest_id_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_BACKUP_PLUS_ARCHIVELOG, task_attr.get_plus_archivelog_str()))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_INITIATOR_TASK_ID, task_attr.initiator_task_id_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_ID, task_attr.validate_id_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_VALIDATE_LEVEL, task_attr.validate_level_.get_str()))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_ROUND_ID, task_attr.round_id_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_START_TS, task_attr.start_ts_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_END_TS, task_attr.end_ts_))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, task_attr.status_.get_str()))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_RESULT, task_attr.result_))) {
    LOG_WARN("failed to add column", KR(ret));
  }
  return ret;
}

int ObBackupValidateTaskOperator::get_backup_validate_task(
    common::ObISQLClient &proxy,
    const int64_t task_id,
    const uint64_t tenant_id,
    ObBackupValidateTaskAttr &task_attr)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (task_id <= 0 || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(task_id), K(tenant_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("select * from %s where %s=%ld and %s=%lu",
          OB_ALL_BACKUP_VALIDATE_TASK_TNAME, OB_STR_TENANT_ID, tenant_id, OB_STR_TASK_ID, task_id))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
      } else if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[BACKUP_VALIDATE]failed to get result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("[BACKUP_VALIDATE]entry not exist", KR(ret), K(sql));
        } else {
          LOG_WARN("[BACKUP_VALIDATE]failed to get next", KR(ret), K(sql));
        }
      } else if (OB_FAIL(do_parse_task_result_(*result, task_attr))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to parse result", KR(ret), K(task_id));
      } else {
        FLOG_INFO("[BACKUP_VALIDATE]success get backup validate task", K(sql), K(task_attr));
      }
    }
  }
  return ret;
}

int ObBackupValidateTaskOperator::get_tasks(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t job_id,
    ObIArray<ObBackupValidateTaskAttr> &task_attrs)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  HEAP_VAR(ObMySQLProxy::ReadResult, res) {
    ObMySQLResult *result = NULL;
    if (OB_FAIL(fill_select_task_sql_(sql))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to fill select task sql", KR(ret));
    } else if (OB_FAIL(sql.append_fmt(" where %s=%lu and %s=%ld order by task_id",
                                          OB_STR_TENANT_ID, tenant_id, OB_STR_JOB_ID, job_id))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to append sql", KR(ret));
    } else if (OB_FAIL(proxy.read(res, get_exec_tenant_id(tenant_id), sql.ptr()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]result is null", KR(ret), K(sql));
    } else if (OB_FAIL(parse_task_result_(*result, task_attrs))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to fill task attr", KR(ret), K(sql));
    } else {
      LOG_INFO("[BACKUP_VALIDATE]success to get tasks", K(task_attrs));
    }
  }
  return ret;
}

int ObBackupValidateTaskOperator::fill_select_task_sql_(ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.assign_fmt("select %s", OB_STR_TASK_ID))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to assign fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_TENANT_ID))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_INCARNATION))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_JOB_ID))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_TASK_TYPE))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_PATH))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_PATH_TYPE))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_DEST_ID))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_BACKUP_PLUS_ARCHIVELOG))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_INITIATOR_TASK_ID))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_ID))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_VALIDATE_LEVEL))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_ROUND_ID))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_START_TS))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_END_TS))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_STATUS))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_TOTAL_LS_COUNT))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_FINISH_LS_COUNT))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_TOTAL_BYTES))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_VALIDATED_BYTES))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_RESULT))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_COMMENT))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(" from %s", OB_ALL_BACKUP_VALIDATE_TASK_TNAME))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
  }
  return ret;
}

int ObBackupValidateTaskOperator::parse_task_result_(
    sqlclient::ObMySQLResult &result,
    ObIArray<ObBackupValidateTaskAttr> &task_attrs)
{
  int ret = OB_SUCCESS;
  // traverse each returned row
  while (OB_SUCC(ret)) {
    ObBackupValidateTaskAttr task;
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("[BACKUP_VALIDATE]failed to get next row", KR(ret));
      }
    } else if (OB_FAIL(do_parse_task_result_(result, task))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to parse task result", KR(ret));
    } else if (OB_FAIL(task_attrs.push_back(task))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to push back task", KR(ret), K(task));
    }
  }
  return ret;
}

int ObBackupValidateTaskOperator::do_parse_task_result_(sqlclient::ObMySQLResult &result, ObBackupValidateTaskAttr &task)
{
  int ret = OB_SUCCESS;
  int64_t real_length = 0;
  int64_t pos = 0;
  char validate_path[OB_MAX_BACKUP_DEST_LENGTH] = "";
  char validate_path_type[OB_SYS_TASK_TYPE_LENGTH] = "";
  char status_str[OB_DEFAULT_STATUS_LENTH] = "";
  char validate_type_str[OB_SYS_TASK_TYPE_LENGTH] = "";
  char validate_level[OB_SYS_TASK_TYPE_LENGTH] = "";
  char plus_archivelog_str[OB_SYS_TASK_TYPE_LENGTH] = "";
  char comment_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = "";

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TASK_ID, task.task_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, task.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INCARNATION, task.incarnation_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_JOB_ID, task.job_id_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_TASK_TYPE, validate_type_str, OB_SYS_TASK_TYPE_LENGTH, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_PATH, validate_path, OB_MAX_BACKUP_DEST_LENGTH, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_PATH_TYPE, validate_path_type, OB_SYS_TASK_TYPE_LENGTH, real_length);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DEST_ID, task.dest_id_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_BACKUP_PLUS_ARCHIVELOG, plus_archivelog_str, OB_SYS_TASK_TYPE_LENGTH, real_length);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INITIATOR_TASK_ID, task.initiator_task_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ID, task.validate_id_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_VALIDATE_LEVEL, validate_level, OB_SYS_TASK_TYPE_LENGTH, real_length);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ROUND_ID, task.round_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_START_TS, task.start_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_END_TS, task.end_ts_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STATUS, status_str, OB_DEFAULT_STATUS_LENTH, real_length);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TOTAL_LS_COUNT, task.total_ls_count_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_FINISH_LS_COUNT, task.finish_ls_count_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TOTAL_BYTES, task.total_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_VALIDATED_BYTES, task.validate_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_RESULT, task.result_, int);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_COMMENT, comment_str, OB_INNER_TABLE_DEFAULT_VALUE_LENTH, real_length);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(task.type_.set(validate_type_str))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to set validate type", KR(ret), K(validate_type_str));
    } else if (OB_FAIL(task.validate_path_.assign(validate_path))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to assign validate path", KR(ret), K(validate_path));
    } else if (OB_FAIL(task.path_type_.set(validate_path_type))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to set path type", KR(ret), K(validate_path_type));
    } else if (OB_FAIL(task.validate_level_.set(validate_level))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to set validate level", KR(ret), K(validate_level));
    } else if (OB_FAIL(task.status_.set_staust(status_str))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to set status", KR(ret), K(status_str));
    } else if (OB_FAIL(task.comment_.assign(comment_str))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to assign comment", KR(ret), K(comment_str));
    } else {
      // set plus_archivelog
      if (0 == STRCMP(plus_archivelog_str, "ON")) {
        task.plus_archivelog_ = true;
      } else {
        task.plus_archivelog_ = false;
      }
    }
  }
  return ret;
}

int ObBackupValidateTaskOperator::advance_task_status(
    common::ObMySQLTransaction &trans,
    const ObBackupValidateTaskAttr &task_attr,
    const ObBackupValidateStatus &next_status,
    const int result,
    const int64_t end_ts)
{
  int ret = OB_SUCCESS;
  const char *comment_to_append = OB_SUCCESS == result ? "" : common::ob_strerror(result);
  if (!next_status.is_valid() || !task_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid next_status or invalid task", KR(ret), K(task_attr));
  } else {
    char status_str[OB_DEFAULT_STATUS_LENTH] = { 0 };
    char comment_str[share::OB_COMMENT_LENGTH] = { 0 };
    bool skip_update = false;
    {
      HEAP_VAR(ObMySQLProxy::ReadResult, res) {
        ObMySQLResult *mysql_result = NULL;
        ObSqlString sql;
        int64_t real_length = 0;
        if (OB_FAIL(sql.assign_fmt("SELECT %s, %s FROM %s WHERE %s=%lu AND %s=%ld FOR UPDATE",
                OB_STR_STATUS,
                OB_STR_COMMENT,
                OB_ALL_BACKUP_VALIDATE_TASK_TNAME,
                OB_STR_TENANT_ID, task_attr.tenant_id_,
                OB_STR_TASK_ID, task_attr.task_id_))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to build select sql", KR(ret), K(task_attr));
        } else if (OB_FAIL(trans.read(res, get_exec_tenant_id(task_attr.tenant_id_), sql.ptr()))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to exec select", KR(ret), K(sql));
        } else if (OB_ISNULL(mysql_result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[BACKUP_VALIDATE]result is null", KR(ret));
        } else if (OB_FAIL(mysql_result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_ENTRY_NOT_EXIST;
          }
          LOG_WARN("[BACKUP_VALIDATE]failed to get next row", KR(ret), K(sql));
        } else {
          EXTRACT_STRBUF_FIELD_MYSQL((*mysql_result), OB_STR_STATUS, status_str,
              OB_DEFAULT_STATUS_LENTH, real_length);
          if (OB_SUCC(ret)) {
            EXTRACT_STRBUF_FIELD_MYSQL((*mysql_result), OB_STR_COMMENT, comment_str,
                share::OB_COMMENT_LENGTH, real_length);
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (0 != STRCMP(status_str, task_attr.status_.get_str())) {
      skip_update = true;
    } else if (NULL != comment_to_append && '\0' != comment_to_append[0]) {
      int64_t pos = strlen(comment_str);
      if (OB_FAIL(databuff_printf(comment_str, share::OB_COMMENT_LENGTH, pos, " "))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to append comment", KR(ret), K(comment_to_append));
      } else if (OB_FAIL(databuff_printf(comment_str, share::OB_COMMENT_LENGTH, pos, "%s", comment_to_append))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to append comment", KR(ret), K(comment_to_append));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!skip_update && OB_FAIL(update_task_status_row_(trans, task_attr, next_status, result, end_ts, comment_str))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to update task row", KR(ret), K(task_attr));
    }
  }
  return ret;
}



int ObBackupValidateTaskOperator::update_task_status_row_(
    common::ObMySQLTransaction &trans,
    const ObBackupValidateTaskAttr &task_attr,
    const ObBackupValidateStatus &next_status,
    const int result,
    const int64_t end_ts,
    const char *comment)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (!task_attr.is_valid() || !next_status.is_valid() || OB_ISNULL(comment)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(task_attr), K(next_status), KP(comment));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, task_attr.task_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(task_attr));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, task_attr.tenant_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(task_attr));
    } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, next_status.get_str()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.add_column(OB_STR_RESULT, result))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.add_column(OB_STR_COMMENT, comment))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.add_column(OB_STR_END_TS, end_ts))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_VALIDATE_TASK_TNAME, sql))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to splice update sql", KR(ret));
    } else if (OB_FAIL(trans.write(get_exec_tenant_id(task_attr.tenant_id_), sql.ptr(), affected_rows))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]invalid affected_rows", KR(ret), K(affected_rows), K(sql));
    } else {
      FLOG_INFO("[BACKUP_VALIDATE]success advance task status", K(task_attr), K(sql));
    }
  }
  return ret;
}

int ObBackupValidateTaskOperator::add_comment(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const int64_t task_id,
    const char *comment)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  if (!is_valid_tenant_id(tenant_id) || task_id <= 0 || OB_ISNULL(comment)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(tenant_id), K(task_id), KP(comment));
  } else if (OB_FAIL(trans.start(&proxy, get_exec_tenant_id(tenant_id)))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to start trans", KR(ret), K(tenant_id));
  } else {
    char comment_str[share::OB_COMMENT_LENGTH] = { 0 };
    bool skip_update = false;
    {
      HEAP_VAR(ObMySQLProxy::ReadResult, res) {
        ObMySQLResult *mysql_result = NULL;
        ObSqlString sql;
        int64_t real_length = 0;
        if (OB_FAIL(sql.assign_fmt("SELECT %s FROM %s WHERE %s=%lu AND %s=%ld FOR UPDATE",
                OB_STR_COMMENT,
                OB_ALL_BACKUP_VALIDATE_TASK_TNAME,
                OB_STR_TENANT_ID, tenant_id,
                OB_STR_TASK_ID, task_id))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to build select sql", KR(ret));
        } else if (OB_FAIL(trans.read(res, get_exec_tenant_id(tenant_id), sql.ptr()))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to exec select", KR(ret), K(sql));
        } else if (OB_ISNULL(mysql_result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[BACKUP_VALIDATE]result is null", KR(ret));
        } else if (OB_FAIL(mysql_result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_ENTRY_NOT_EXIST;
          }
          LOG_WARN("[BACKUP_VALIDATE]failed to get next row", KR(ret), K(sql));
        } else {
          EXTRACT_STRBUF_FIELD_MYSQL((*mysql_result), OB_STR_COMMENT, comment_str,
              share::OB_COMMENT_LENGTH, real_length);
          if (OB_SUCC(ret) && '\0' != comment_str[0]) {
            skip_update = true;
            LOG_INFO("[BACKUP_VALIDATE]skip add comment because exists", K(tenant_id), K(task_id));
          }
        }
      }
    }
    if (OB_SUCC(ret) && !skip_update) {
      int64_t pos = strlen(comment_str);
      if (OB_FAIL(databuff_printf(comment_str, share::OB_COMMENT_LENGTH, pos, "%s", comment))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to copy comment", KR(ret), KP(comment));
      } else if (OB_FAIL(update_task_comment_row_(trans, tenant_id, task_id, comment_str))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to update comment row", KR(ret), K(tenant_id), K(task_id));
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to end trans", KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObBackupValidateTaskOperator::update_task_comment_row_(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const int64_t task_id,
    const char *comment)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (!is_valid_tenant_id(tenant_id) || task_id <= 0 || OB_ISNULL(comment)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(tenant_id), K(task_id), KP(comment));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, task_id))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(task_id));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(tenant_id));
    } else if (OB_FAIL(dml.add_column(OB_STR_COMMENT, comment))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_VALIDATE_TASK_TNAME, sql))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to splice update sql", KR(ret));
    } else if (OB_FAIL(trans.write(get_exec_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]invalid affected_rows", KR(ret), K(affected_rows), K(sql));
    } else {
      FLOG_INFO("[BACKUP_VALIDATE]success add comment", K(tenant_id), K(task_id));
    }
  }
  return ret;
}

int ObBackupValidateTaskOperator::add_finish_ls_count(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const int64_t task_id)
{
  int ret = OB_SUCCESS;
  int64_t finish_ls_count = 0;
  if (!is_valid_tenant_id(tenant_id) || task_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(tenant_id), K(task_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *mysql_result = NULL;
      ObSqlString sql;
      if (OB_FAIL(sql.assign_fmt("SELECT %s FROM %s WHERE %s=%lu AND %s=%ld FOR UPDATE",
              OB_STR_FINISH_LS_COUNT,
              OB_ALL_BACKUP_VALIDATE_TASK_TNAME,
              OB_STR_TENANT_ID, tenant_id,
              OB_STR_TASK_ID, task_id))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to build select sql", KR(ret), K(tenant_id), K(task_id));
      } else if (OB_FAIL(trans.read(res, get_exec_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to exec select", KR(ret), K(sql));
      } else if (OB_ISNULL(mysql_result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[BACKUP_VALIDATE]result is null", KR(ret));
      } else if (OB_FAIL(mysql_result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
        LOG_WARN("[BACKUP_VALIDATE]failed to get next row", KR(ret), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL((*mysql_result), OB_STR_FINISH_LS_COUNT, finish_ls_count, int64_t);
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    finish_ls_count++;
    if (OB_FAIL(update_finish_ls_count_(trans, tenant_id, task_id, finish_ls_count))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to update finish ls count", KR(ret), K(tenant_id), K(task_id));
    }
  }
  return ret;
}

int ObBackupValidateTaskOperator::update_finish_ls_count_(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const int64_t task_id,
    const int64_t finish_ls_count)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (!is_valid_tenant_id(tenant_id) || task_id <= 0 || finish_ls_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(tenant_id), K(task_id), K(finish_ls_count));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, task_id))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(task_id));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(tenant_id));
  } else if (OB_FAIL(dml.add_column(OB_STR_FINISH_LS_COUNT, finish_ls_count))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret), K(finish_ls_count));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_VALIDATE_TASK_TNAME, sql))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to splice update sql", KR(ret));
  } else if (OB_FAIL(trans.write(get_exec_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]invalid affected_rows", KR(ret), K(affected_rows), K(sql));
  } else {
    FLOG_INFO("[BACKUP_VALIDATE]success add finish ls count", K(tenant_id), K(task_id), K(finish_ls_count));
  }
  return ret;
}

int ObBackupValidateTaskOperator::update_ls_count_and_bytes(
    common::ObISQLClient &proxy,
    const ObBackupValidateTaskAttr &task_attr,
    const bool is_total)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;

  if (!task_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(task_attr));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, task_attr.task_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(task_attr));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, task_attr.tenant_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(task_attr));
    } else if (is_total && OB_FAIL(dml.add_column(OB_STR_TOTAL_LS_COUNT, task_attr.total_ls_count_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret), K(task_attr));
    } else if (is_total && OB_FAIL(dml.add_column(OB_STR_TOTAL_BYTES, task_attr.total_bytes_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret), K(task_attr));
    } else if (!is_total && OB_FAIL(dml.add_column(OB_STR_FINISH_LS_COUNT, task_attr.finish_ls_count_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret), K(task_attr));
    } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_VALIDATE_TASK_TNAME, sql))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to splice update sql", KR(ret));
    } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(task_attr.tenant_id_), sql.ptr(), affected_rows))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
    } else if (1 != affected_rows && 0 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]invalid affected_rows", KR(ret), K(affected_rows), K(sql));
    } else {
      FLOG_INFO("[BACKUP_VALIDATE]success update ls count", K(task_attr), K(sql));
    }
  }
  return ret;
}

int ObBackupValidateTaskOperator::update_validated_bytes(
    common::ObMySQLTransaction &trans,
    const int64_t task_id,
    const uint64_t tenant_id,
    const int64_t validated_bytes)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t current_validated_bytes = 0;
  int64_t total_validated_bytes = 0;
  if (task_id <= 0 || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(task_id), K(tenant_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *mysql_result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT %s FROM %s WHERE %s=%lu AND %s=%ld FOR UPDATE",
                      OB_STR_VALIDATED_BYTES, OB_ALL_BACKUP_VALIDATE_TASK_TNAME,
                      OB_STR_TENANT_ID, tenant_id, OB_STR_TASK_ID, task_id))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to build select sql", KR(ret), K(tenant_id), K(task_id));
      } else if (OB_FAIL(trans.read(res, get_exec_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to exec select", KR(ret), K(sql));
      } else if (OB_ISNULL(mysql_result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[BACKUP_VALIDATE]result is null", KR(ret), K(sql));
      } else if (OB_FAIL(mysql_result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
        LOG_WARN("[BACKUP_VALIDATE]failed to get next row", KR(ret), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL((*mysql_result), OB_STR_VALIDATED_BYTES, current_validated_bytes, int64_t);
        total_validated_bytes = current_validated_bytes + validated_bytes;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(update_validated_bytes_(trans, task_id, tenant_id, total_validated_bytes))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to update validated bytes", KR(ret), K(tenant_id), K(task_id),
                K(current_validated_bytes), K(validated_bytes), K(total_validated_bytes));
  }
  return ret;
}

int ObBackupValidateTaskOperator::update_validated_bytes_(
    common::ObMySQLTransaction &trans,
    const int64_t task_id,
    const uint64_t tenant_id,
    const int64_t validated_bytes)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (!is_valid_tenant_id(tenant_id) || task_id <= 0 || validated_bytes < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(tenant_id), K(task_id), K(validated_bytes));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, task_id))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(task_id));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(tenant_id));
  } else if (OB_FAIL(dml.add_column(OB_STR_VALIDATED_BYTES, validated_bytes))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret), K(validated_bytes));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_VALIDATE_TASK_TNAME, sql))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to splice update sql", KR(ret));
  } else if (OB_FAIL(trans.write(get_exec_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
  } else if (1 != affected_rows && 0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]invalid affected_rows", KR(ret), K(affected_rows), K(sql));
  } else {
    FLOG_INFO("[BACKUP_VALIDATE]success add validated bytes", K(tenant_id), K(task_id), K(validated_bytes));
  }
  return ret;
}

int ObBackupValidateTaskOperator::move_task_to_history(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const int64_t task_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (!is_valid_tenant_id(tenant_id) || task_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(tenant_id), K(task_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "insert into %s select * from %s where %s=%lu and %s=%ld",
      OB_ALL_BACKUP_VALIDATE_TASK_HISTORY_TNAME, OB_ALL_BACKUP_VALIDATE_TASK_TNAME,
      OB_STR_TENANT_ID, tenant_id, OB_STR_TASK_ID, task_id))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to init sql", KR(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
  } else if (OB_FALSE_IT(sql.reset())) {
  } else if (OB_FAIL(sql.assign_fmt("delete from %s where %s=%lu and %s=%ld",
      OB_ALL_BACKUP_VALIDATE_TASK_TNAME, OB_STR_TENANT_ID, tenant_id, OB_STR_TASK_ID, task_id))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to init sql", KR(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
  } else {
    FLOG_INFO("[BACKUP_VALIDATE]succeed move backup validate task to history table", K(tenant_id), K(task_id));
  }
  return ret;
}

int ObBackupValidateLSTaskOperator::advance_ls_task_status(
    common::ObISQLClient &proxy,
    const ObBackupValidateLSTaskAttr &ls_attr,
    const ObBackupTaskStatus &next_status,
    const int result,
    const int64_t end_ts)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  const char *comment = (OB_SUCCESS == result) ? "" : common::ob_strerror(result);
  if (!next_status.is_valid() || !ls_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid status", KR(ret), K(ls_attr), K(next_status));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, ls_attr.task_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(ls_attr.task_id_));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, ls_attr.tenant_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(ls_attr.tenant_id_));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_LS_ID, ls_attr.ls_id_.id()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(ls_attr.ls_id_));
    } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, next_status.get_str()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.add_column(OB_STR_RESULT, result))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.add_column(OB_STR_END_TS, end_ts))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (!ls_attr.comment_.is_empty() && OB_FAIL(dml.add_column(OB_STR_COMMENT, ls_attr.comment_.ptr()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret), K(ls_attr));
    } else if (ls_attr.comment_.is_empty() && OB_FAIL(dml.add_column(OB_STR_COMMENT, comment))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret), K(comment));
    } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_VALIDATE_LS_TASK_TNAME, sql))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to splice update sql", KR(ret));
    } else if (OB_FAIL(sql.append_fmt(" AND %s='%s'", OB_STR_STATUS, ls_attr.status_.get_str()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to append extra condition", KR(ret));
    } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(ls_attr.tenant_id_), sql.ptr(), affected_rows))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]invalid affected_rows", KR(ret), K(affected_rows), K(sql), K(next_status));
    } else {
      FLOG_INFO("[BACKUP_VALIDATE]advance ls task status", K(ls_attr), K(next_status), K(result), K(sql));
    }
  }
  return ret;
}

int ObBackupValidateLSTaskOperator::update_dst_and_status(
    common::ObISQLClient &proxy,
    const int64_t task_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    share::ObTaskId task_trace_id,
    common::ObAddr &dst)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  char server_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char trace_id_str[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  task_trace_id.to_string(trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE);
  if (!ls_id.is_valid() || !dst.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(dst), K(ls_id));
  } else if (!dst.ip_to_string(server_ip, OB_MAX_SERVER_ADDR_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]failed to change ip to string", KR(ret), K(dst));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, task_id))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(task_id));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(tenant_id));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_LS_ID, ls_id.id()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(ls_id));
    } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, "DOING"))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.add_column(OB_STR_SEVER_IP, server_ip))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.add_column(OB_STR_SERVER_PORT, dst.get_port()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.add_column(OB_STR_TASK_TRACE_ID, trace_id_str))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_VALIDATE_LS_TASK_TNAME, sql))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to splice update sql", KR(ret));
    } else if (OB_FAIL(sql.append_fmt(" AND %s='PENDING'", OB_STR_STATUS))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to append extra condition", KR(ret));
    } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]invalid affected_rows", KR(ret), K(affected_rows), K(sql));
    } else {
      FLOG_INFO("[BACKUP_VALIDATE]success update dst and status", K(sql));
    }
  }
  return ret;
}

int ObBackupValidateLSTaskOperator::move_ls_task_to_history(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const int64_t task_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (!is_valid_tenant_id(tenant_id) || task_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(tenant_id), K(task_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "insert into %s select * from %s where %s=%lu and %s=%ld",
      OB_ALL_BACKUP_VALIDATE_LS_TASK_HISTORY_TNAME, OB_ALL_BACKUP_VALIDATE_LS_TASK_TNAME,
      OB_STR_TENANT_ID, tenant_id, OB_STR_TASK_ID, task_id))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to init sql", KR(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
  } else if (OB_FALSE_IT(sql.reset())) {
  } else if (OB_FAIL(sql.assign_fmt(
      "delete from %s where %s=%lu and %s=%ld",
      OB_ALL_BACKUP_VALIDATE_LS_TASK_TNAME, OB_STR_TENANT_ID, tenant_id, OB_STR_TASK_ID, task_id))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to init sql", KR(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
  } else {
    FLOG_INFO("[BACKUP_VALIDATE]succeed move backup validate ls task to history table", K(tenant_id), K(task_id));
  }
  return ret;
}

int ObBackupValidateLSTaskOperator::init_stats(
    common::ObISQLClient &proxy,
    const int64_t task_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const int64_t total_object)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (task_id <= 0 || !is_valid_tenant_id(tenant_id) || !ls_id.is_valid() || total_object <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(task_id), K(tenant_id), K(ls_id), K(total_object));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, task_id))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(task_id));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(tenant_id));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_LS_ID, ls_id.id()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(ls_id));
    } else if (OB_FAIL(dml.add_column(OB_STR_TOTAL_OBJECT_COUNT, total_object))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_VALIDATE_LS_TASK_TNAME, sql))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to splice update sql", KR(ret));
    } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
    } else if (affected_rows != 1 && affected_rows != 0) {
      // stats may has been init when redo ls task, so affected_rows may be 0
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]invalid affected_rows", KR(ret), K(affected_rows), K(sql));
    } else {
      FLOG_INFO("[BACKUP_VALIDATE]success update stats", K(sql));
    }
  }
  return ret;
}

int ObBackupValidateLSTaskOperator::update_stats(common::ObISQLClient &proxy,
  const int64_t task_id, const uint64_t tenant_id,
  const ObLSID &ls_id, const int64_t validated_bytes,
  const int64_t finish_object_count)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (task_id <= 0 || !is_valid_tenant_id(tenant_id) || !ls_id.is_valid() || finish_object_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(task_id), K(tenant_id),
                K(ls_id), K(finish_object_count));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, task_id))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(task_id));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(tenant_id));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_LS_ID, ls_id.id()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(ls_id));
    } else if (OB_FAIL(dml.add_column(OB_STR_VALIDATED_BYTES, validated_bytes))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.add_column(OB_STR_FINISH_OBJECT_COUNT, finish_object_count))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_VALIDATE_LS_TASK_TNAME, sql))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to splice update sql", KR(ret));
    } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
    } else if (affected_rows != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]invalid affected_rows", KR(ret), K(sql));
    } else {
      FLOG_INFO("[BACKUP_VALIDATE]success update stats", K(sql));
    }
  }
  return ret;
}

int ObBackupValidateLSTaskOperator::get_validated_stats(
    common::ObISQLClient &proxy,
    const int64_t task_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    int64_t &finish_object_count,
    int64_t &validated_bytes)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  finish_object_count = 0;
  validated_bytes = 0;
  if (task_id <= 0 || !is_valid_tenant_id(tenant_id) || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(task_id), K(tenant_id), K(ls_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("select %s, %s from %s where %s=%ld and %s=%lu and %s=%ld",
                                    OB_STR_FINISH_OBJECT_COUNT, OB_STR_VALIDATED_BYTES,
                                    OB_ALL_BACKUP_VALIDATE_LS_TASK_TNAME, OB_STR_TASK_ID,
                                    task_id, OB_STR_TENANT_ID, tenant_id, OB_STR_LS_ID, ls_id.id()))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
      } else if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[BACKUP_VALIDATE]result is null", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
        LOG_WARN("[BACKUP_VALIDATE]failed to get next row", KR(ret), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL((*result), OB_STR_FINISH_OBJECT_COUNT, finish_object_count, int64_t);
        EXTRACT_INT_FIELD_MYSQL((*result), OB_STR_VALIDATED_BYTES, validated_bytes, int64_t);
      }
    }
  }
  return ret;
}

int ObBackupValidateLSTaskOperator::redo_ls_task(
    common::ObISQLClient &proxy,
    const ObBackupValidateLSTaskAttr &ls_attr,
    const int64_t retry_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  const char *empty_str =  "";
  if (!ls_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(ls_attr));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, ls_attr.task_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(ls_attr.task_id_));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, ls_attr.tenant_id_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(ls_attr.tenant_id_));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_LS_ID, ls_attr.ls_id_.id()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add pk column", KR(ret), K(ls_attr.ls_id_));
    } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, "INIT"))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.add_column(OB_STR_RESULT, OB_SUCCESS))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.add_column(OB_STR_SEVER_IP, empty_str))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.add_column(OB_STR_SERVER_PORT, 0))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.add_column(OB_STR_TASK_TRACE_ID, empty_str))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.add_column(OB_STR_COMMENT, empty_str))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.add_column(OB_STR_RETRY_ID, retry_id))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
    } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_VALIDATE_LS_TASK_TNAME, sql))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to splice update sql", KR(ret));
    } else if (OB_FAIL(sql.append_fmt(" AND %s='%s'", OB_STR_STATUS, ls_attr.status_.get_str()))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to append extra condition", KR(ret));
    } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(ls_attr.tenant_id_), sql.ptr(), affected_rows))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_VALIDATE]invalid affected_rows", KR(ret), K(affected_rows), K(sql), K(ls_attr));
    } else {
      LOG_INFO("[BACKUP_VALIDATE]success redo ls status", K(ls_attr), K(sql));
    }
  }
  return ret;
}

int ObBackupValidateLSTaskOperator::get_ls_tasks_from_task_id(
    common::ObISQLClient &proxy,
    const int64_t task_id,
    const uint64_t tenant_id,
    bool need_lock,
    ObIArray<ObBackupValidateLSTaskAttr> &ls_attrs)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (task_id <= 0 || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(tenant_id), K(task_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("select * from %s where %s=%lu and %s=%ld order by end_ts",
          OB_ALL_BACKUP_VALIDATE_LS_TASK_TNAME, OB_STR_TENANT_ID, tenant_id, OB_STR_TASK_ID, task_id))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to append fmt", KR(ret));
      } else if (need_lock && OB_FAIL(sql.append_fmt(" for update"))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to append sql", KR(ret));
      } else if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[BACKUP_VALIDATE]result is null", KR(ret), K(sql));
      } else if (OB_FAIL(parse_ls_task_result_(*result, ls_attrs))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to parse result", KR(ret));
      } else {
        LOG_INFO("[BACKUP_VALIDATE]success get ls tasks from task id", KR(ret), K(ls_attrs));
      }
    }
  }
  return ret;
}

int ObBackupValidateLSTaskOperator::insert_ls_task(
    common::ObISQLClient &proxy,
    const ObBackupValidateLSTaskAttr &ls_attr)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  if (!ls_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", K(ls_attr));
  } else if (OB_FAIL(fill_dml_with_ls_task_(ls_attr, dml))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to fill backup ls task", KR(ret), K(ls_attr));
  } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_BACKUP_VALIDATE_LS_TASK_TNAME, sql))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to splice insert update sql", KR(ret), K(ls_attr));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(ls_attr.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("[BACKUP_VALIDATE]fail to exec sql", KR(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]error unexpected, invalid affected rows", KR(ret), K(affected_rows), K(sql));
  } else {
    LOG_INFO("[BACKUP_VALIDATE]insert one ls task", K(ls_attr), K(sql));
  }
  return ret;
}

int ObBackupValidateLSTaskOperator::fill_dml_with_ls_task_(
    const ObBackupValidateLSTaskAttr &ls_attr,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, ls_attr.task_id_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, ls_attr.tenant_id_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_LS_ID, ls_attr.ls_id_.id()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_JOB_ID, ls_attr.job_id_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_TASK_TYPE, ls_attr.task_type_.get_str()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_PATH, ls_attr.validate_path_.ptr()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_DEST_ID, ls_attr.dest_id_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_VALIDATE_LEVEL, ls_attr.validate_level_.get_str()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, ls_attr.status_.get_str()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_START_TS, ls_attr.start_ts_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_END_TS, ls_attr.end_ts_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_TASK_TRACE_ID, ""))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_RESULT, ls_attr.result_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_ROUND_ID, ls_attr.round_id_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_ID, ls_attr.validate_id_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_RETRY_ID, ls_attr.retry_id_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_VALIDATED_BYTES, ls_attr.stats_.validated_bytes_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_TOTAL_OBJECT_COUNT, ls_attr.stats_.total_object_count_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_FINISH_OBJECT_COUNT, ls_attr.stats_.finish_object_count_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to add column", KR(ret));
  }
  return ret;
}

int ObBackupValidateLSTaskOperator::parse_ls_task_result_(
    sqlclient::ObMySQLResult &result,
    ObIArray<ObBackupValidateLSTaskAttr> &ls_attrs)
{
  int ret = OB_SUCCESS;
  // traverse each returned row
  ObBackupValidateLSTaskAttr ls_attr;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("[BACKUP_VALIDATE]failed to get next row", KR(ret));
      }
    } else if (OB_FAIL(do_parse_ls_task_result_(result, ls_attr))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to parse ls result", KR(ret));
    } else if (OB_FAIL(ls_attrs.push_back(ls_attr))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to push back ls", KR(ret));
    } else {
      LOG_INFO("[BACKUP_VALIDATE]success parse ls result", KR(ret), K(ls_attrs), K(ls_attr));
    }
  }

  return ret;
}

int ObBackupValidateLSTaskOperator::do_parse_ls_task_result_(
    sqlclient::ObMySQLResult &result,
    ObBackupValidateLSTaskAttr &ls_attr)
{
  int ret = OB_SUCCESS;
  ls_attr.reset();
  int64_t real_length = 0;
  char status_str[OB_DEFAULT_STATUS_LENTH] = "";
  char task_type_str[OB_DEFAULT_STATUS_LENTH] = "";
  char validate_path_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  char validate_level_str[OB_DEFAULT_STATUS_LENTH] = "";
  char trace_id_str[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  char server_str[OB_MAX_SERVER_ADDR_SIZE] = { 0 };
  char comment_str[MAX_TABLE_COMMENT_LENGTH] = "";
  int64_t port = 0;
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TASK_ID, ls_attr.task_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_JOB_ID, ls_attr.job_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, ls_attr.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_LS_ID, ls_attr.ls_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ROUND_ID, ls_attr.round_id_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_PATH, validate_path_str, OB_MAX_BACKUP_DEST_LENGTH, real_length);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DEST_ID, ls_attr.dest_id_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_VALIDATE_LEVEL, validate_level_str, OB_DEFAULT_STATUS_LENTH, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_TASK_TYPE, task_type_str, OB_DEFAULT_STATUS_LENTH, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STATUS, status_str, OB_DEFAULT_STATUS_LENTH, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_TASK_TYPE, task_type_str, OB_DEFAULT_STATUS_LENTH, real_length);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_START_TS, ls_attr.start_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_END_TS, ls_attr.end_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_RESULT, ls_attr.result_, int);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_RETRY_ID, ls_attr.retry_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ID, ls_attr.validate_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_VALIDATED_BYTES, ls_attr.stats_.validated_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TOTAL_OBJECT_COUNT, ls_attr.stats_.total_object_count_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_FINISH_OBJECT_COUNT, ls_attr.stats_.finish_object_count_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_TASK_TRACE_ID, trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_COMMENT, comment_str, MAX_TABLE_COMMENT_LENGTH, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_SEVER_IP, server_str, OB_MAX_SERVER_ADDR_SIZE, real_length);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_SERVER_PORT, port, int64_t);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ls_attr.status_.set_status(status_str))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to set status", KR(ret), K(status_str));
    } else if (OB_FAIL(ls_attr.task_type_.set(task_type_str))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to set task type", KR(ret), K(task_type_str));
    } else if (OB_FAIL(ls_attr.validate_path_.assign(validate_path_str))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to set validate path", KR(ret), K(validate_path_str));
    } else if (OB_FAIL(ls_attr.validate_level_.set(validate_level_str))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to set validate level", KR(ret), K(validate_level_str));
    } else if (strcmp(trace_id_str, "") != 0 && OB_FAIL(ls_attr.task_trace_id_.set(trace_id_str))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to set trace id", KR(ret), K(trace_id_str));
    } else if (OB_FAIL(ls_attr.comment_.assign(comment_str))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to assign comment", KR(ret), K(comment_str));
    } else if (!ls_attr.dst_.set_ip_addr(server_str, static_cast<int32_t>(port))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[BACKUP_VALIDATE]failed to set dst", KR(ret), K(server_str), K(port));
    } else {
      LOG_INFO("[BACKUP_VALIDATE]success parse ls task result", K(ls_attr));
    }
  }
  return ret;
}

int ObBackupValidateLSTaskOperator::get_ls_task(
    common::ObISQLClient &proxy,
    bool need_lock,
    const int64_t task_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObBackupValidateLSTaskAttr &ls_attr)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (task_id <= 0 || !is_valid_tenant_id(tenant_id) || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(task_id), K(tenant_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("select * from %s where %s=%ld and %s=%ld and %s=%ld",
          OB_ALL_BACKUP_VALIDATE_LS_TASK_TNAME, OB_STR_TASK_ID, task_id, OB_STR_LS_ID, ls_id.id(),
          OB_STR_TENANT_ID, tenant_id))) {
        LOG_WARN("[BACKUP_VALIDATE]failed append sql", KR(ret),K(sql));
      } else if (need_lock && OB_FAIL(sql.append_fmt(" for update"))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to append sql", KR(ret));
      } else if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[BACKUP_VALIDATE]result is null", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("[BACKUP_VALIDATE]entry not exit", KR(ret), K(sql));
        } else {
          LOG_WARN("[BACKUP_VALIDATE]failed to get next", KR(ret), K(sql));
        }
      } else if (OB_FAIL(do_parse_ls_task_result_(*result, ls_attr))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to do parse ls result", KR(ret));
      }
    }
  }
  return ret;
}

/*
 *---------------------- LSTaskIterator ----------------------
 */
int ObBackupValidateLSTaskOperator::LSTaskIterator::init(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t task_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const ObBackupTaskStatus pending_status(ObBackupTaskStatus::Status::PENDING);
  const ObBackupTaskStatus doing_status(ObBackupTaskStatus::Status::DOING);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[BACKUP_VALIDATE]iterator already inited", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s", OB_ALL_BACKUP_VALIDATE_LS_TASK_TNAME))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to fill select ls task sql", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" WHERE %s=%ld", OB_STR_TASK_ID, task_id))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append where clause", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" AND (%s='%s' OR %s='%s')",
                                            OB_STR_STATUS, pending_status.get_str(),
                                            OB_STR_STATUS, doing_status.get_str()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to append status condition", K(ret));
  } else if (OB_FAIL(sql_proxy.read(res_, get_exec_tenant_id(tenant_id), sql.ptr()))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to exec sql", K(ret), K(sql), K(tenant_id));
  } else if (OB_ISNULL(result_ = res_.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]result is null", K(ret), K(sql));
  } else {
    sql_proxy_ = &sql_proxy;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupValidateLSTaskOperator::LSTaskIterator::next(ObBackupValidateLSTaskAttr &ls_task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_VALIDATE]iterator not inited", K(ret));
  } else if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]result is null", K(ret));
  } else if (OB_FAIL(result_->next())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get next row", K(ret));
    }
  } else if (OB_FAIL(do_parse_ls_task_result_(*result_, ls_task))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to parse ls task", K(ret));
  }

  return ret;
}

void ObBackupValidateLSTaskOperator::LSTaskIterator::reset()
{
  sql_proxy_ = nullptr;
  tenant_id_ = OB_INVALID_TENANT_ID;
  result_ = nullptr;
  is_inited_ = false;
  res_.reset();
}

} //share
} // oceanbase