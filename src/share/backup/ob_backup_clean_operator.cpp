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
#include "ob_backup_clean_operator.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "observer/ob_sql_client_decorator.h"
#include "lib/string/ob_sql_string.h"
#include "common/ob_smart_var.h"
#include "share/config/ob_server_config.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "ob_backup_clean_struct.h"
namespace oceanbase
{ 
using namespace common;
using namespace common::sqlclient;
namespace share
{
/*
 *------------------------------__all_backup_job----------------------------
 */
int ObBackupCleanJobOperator::insert_job(
    common::ObISQLClient &proxy, 
    const ObBackupCleanJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  if (!job_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(job_attr));
  } else if (OB_FAIL(fill_dml_with_job_(job_attr, dml))) {
    LOG_WARN("failed to fill backup job", K(ret), K(job_attr));
  } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_BACKUP_DELETE_JOB_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret), K(job_attr));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(job_attr.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows), K(sql), K(job_attr));
  } else {
    LOG_INFO("[BACKUP_CLEAN]success insert one backup clean job", K(job_attr), K(sql));
  }
  return ret;
}

int ObBackupCleanJobOperator::fill_dml_with_job_(const ObBackupCleanJobAttr &job_attr, ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  int64_t parameter = 0;
  char parameter_str[common::OB_INNER_TABLE_BACKUP_CLEAN_PARAMETER_LENGTH] = { 0 };
  if (OB_FAIL(job_attr.get_clean_parameter(parameter))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(databuff_printf(parameter_str, OB_INNER_TABLE_BACKUP_CLEAN_PARAMETER_LENGTH, "%ld", parameter))) {
    LOG_WARN("failed to set parameter", K(ret), K(parameter));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_JOB_ID, job_attr.job_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, job_attr.tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_INCARNATION, job_attr.incarnation_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_INITIATOR_TENANT_ID, job_attr.initiator_tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_INITIATOR_JOB_ID, job_attr.initiator_job_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_CLEAN_TYPE, ObNewBackupCleanType::get_str(job_attr.clean_type_)))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_CLEAN_PARAMETER, parameter_str))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_DESCRIPTION, job_attr.description_.ptr()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_START_TS, job_attr.start_ts_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_END_TS, job_attr.end_ts_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, job_attr.status_.get_str()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_RESULT, job_attr.result_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_BACKUP_STR_JOB_LEVEL, job_attr.job_level_.get_str()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(job_attr.get_executor_tenant_id_str(dml))) {
    LOG_WARN("fail to get backup tenant id str", K(ret), K(job_attr));
  } 
  return ret;
}

int ObBackupCleanJobOperator::get_jobs(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id, 
    bool need_lock, 
    ObIArray<ObBackupCleanJobAttr> &job_attrs)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(fill_select_job_sql_(sql))) {
        LOG_WARN("failed to fill select job sql", K(ret));
      } else if (need_lock && OB_FAIL(sql.append_fmt(" for update"))) {
        LOG_WARN("failed to append sql", K(ret));
      } else if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_job_result_(*result, job_attrs))) {
        LOG_WARN("failed to parse result", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupCleanJobOperator::get_job(
    common::ObISQLClient &proxy, 
    bool need_lock, 
    const uint64_t tenant_id,
    const int64_t job_id,
    const bool is_initiator,
    ObBackupCleanJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!is_valid_tenant_id(tenant_id) || job_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(job_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(fill_select_job_sql_(sql))) {
        LOG_WARN("failed to fill select backup job sql", K(ret));
      } else if (OB_FAIL(sql.append_fmt(" where %s=%lu", OB_STR_TENANT_ID, tenant_id))) {
        LOG_WARN("failed to fill select backup job sql", K(ret), K(tenant_id));
      } else if (!is_initiator && OB_FAIL(sql.append_fmt(" and %s=%ld", OB_STR_JOB_ID, job_id))) {
        LOG_WARN("fail to append sql", K(ret));
      } else if (is_initiator && OB_FAIL(sql.append_fmt(" and %s=%ld", OB_STR_INITIATOR_JOB_ID, job_id))) {
        LOG_WARN("fail to append sql", K(ret));
      } else if (need_lock && OB_FAIL(sql.append_fmt(" for update"))) {
        LOG_WARN("failed to append sql", K(ret));
      } else if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("entry not exist", K(ret), K(sql));
        } else {
          LOG_WARN("failed to get next", K(ret), K(sql));
        }
      } else if (OB_FAIL(do_parse_job_result_(*result, job_attr))) {
        LOG_WARN("failed to parse job result", K(ret));
      }
    }
  }
  return ret;
}
int ObBackupCleanJobOperator::cnt_jobs(
    common::ObISQLClient &proxy, 
    const uint64_t tenant_id, 
    const uint64_t initiator_tenant_id, 
    const int64_t initiator_job_id, 
    int64_t &cnt)
{
  int ret = OB_SUCCESS;
  cnt = 0;
  ObSqlString sql;
  ObArray<ObBackupCleanJobAttr> job_attrs;
  if (!is_valid_tenant_id(tenant_id) || !is_valid_tenant_id(initiator_tenant_id) || initiator_job_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(initiator_tenant_id), K(initiator_job_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(fill_select_job_sql_(sql))) {
        LOG_WARN("failed to fill select backup job sql", K(ret));
      } else if (OB_FAIL(sql.append_fmt(" where %s=%lu and %s=%lu and %s=%ld", OB_STR_TENANT_ID, tenant_id,
          OB_STR_INITIATOR_TENANT_ID, initiator_tenant_id, OB_STR_INITIATOR_JOB_ID, initiator_job_id))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_job_result_(*result, job_attrs))) {
        LOG_WARN("failed to parse result", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      cnt = job_attrs.count();
    }
  }
  return ret;
}

int ObBackupCleanJobOperator::fill_select_job_sql_(ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.assign_fmt("select %s", OB_STR_JOB_ID))) {
    LOG_WARN("failed to assign fmt", K(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_TENANT_ID))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_INCARNATION))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_INITIATOR_TENANT_ID))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_INITIATOR_JOB_ID))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_EXECUTOR_TENANT_ID))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_CLEAN_TYPE))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_CLEAN_PARAMETER))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_START_TS))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_END_TS))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_STATUS))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_RESULT))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_RETRY_COUNT))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_STR_COMMENT))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(sql.append_fmt(", %s", OB_BACKUP_STR_JOB_LEVEL))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" from %s", OB_ALL_BACKUP_DELETE_JOB_TNAME))) {
    LOG_WARN("failed to append fmt", K(ret));
  } 
  return ret;
}

int ObBackupCleanJobOperator::parse_job_result_(
    sqlclient::ObMySQLResult &result, 
    common::ObIArray<ObBackupCleanJobAttr> &jobs)
{
  int ret = OB_SUCCESS;
  // traverse each returned row
  while (OB_SUCC(ret)) {
    ObBackupCleanJobAttr job;
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (OB_FAIL(do_parse_job_result_(result, job))) {
      LOG_WARN("failed to parse job result", K(ret));
    } else if (OB_FAIL(jobs.push_back(job))) {
      LOG_WARN("failed to push back job", K(ret), K(job));
    }
  }
  return ret;
}

int ObBackupCleanJobOperator::parse_int_(const char *str, int64_t &val)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  val = 0;
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(str));
  } else {
    char *p_end = NULL;
    if ('\0' == str[0]) {
      is_valid = false;
    } else if (OB_FAIL(ob_strtoll(str, p_end, val))) {
      LOG_WARN("failed to get value from string", K(ret), K(str));
    } else if ('\0' == *p_end) {
      is_valid = true;
    } else {
      is_valid = false;
      LOG_WARN("set int error", K(str), K(is_valid));
    }

    if (!is_valid) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid int str", K(ret), K(str));
    }
  }

  return ret;
}

int ObBackupCleanJobOperator::do_parse_job_result_(
    ObMySQLResult &result, 
    ObBackupCleanJobAttr &job)
{
  int ret = OB_SUCCESS;
  int64_t real_length = 0;
  char status_str[OB_DEFAULT_STATUS_LENTH] = "";
  char clean_type_str[OB_DEFAULT_STATUS_LENTH] = "";
  char job_level[OB_SYS_TASK_TYPE_LENGTH] = "";
  char parameter_str[common::OB_INNER_TABLE_BACKUP_CLEAN_PARAMETER_LENGTH] = { 0 };
  char executor_tenant_id_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = "";
  int64_t parameter = 0;
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_JOB_ID, job.job_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, job.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INCARNATION, job.incarnation_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INITIATOR_TENANT_ID, job.initiator_tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INITIATOR_JOB_ID, job.initiator_job_id_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_CLEAN_TYPE, clean_type_str, OB_SYS_TASK_TYPE_LENGTH, real_length);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_START_TS, job.start_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_END_TS, job.end_ts_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STATUS, status_str, OB_DEFAULT_STATUS_LENTH, real_length);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_RESULT, job.result_, int);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_RETRY_COUNT, job.retry_count_, int);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_CLEAN_PARAMETER, parameter_str, common::OB_INNER_TABLE_BACKUP_CLEAN_PARAMETER_LENGTH, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_BACKUP_STR_JOB_LEVEL, job_level, OB_SYS_TASK_TYPE_LENGTH, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_EXECUTOR_TENANT_ID, executor_tenant_id_str, OB_INNER_TABLE_DEFAULT_VALUE_LENTH, real_length);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(parse_int_(parameter_str, parameter))) {
    LOG_WARN("failed to parse parameter", K(ret), K(parameter_str));
  } else if (OB_FAIL(job.status_.set_status(status_str))) {
    LOG_WARN("failed to set status", K(ret), K(status_str));
  } else if (FALSE_IT(job.clean_type_ = ObNewBackupCleanType::get_type(clean_type_str))) {
  } else if (OB_FAIL(job.job_level_.set_level(job_level))) {
    LOG_WARN("failed to set backup level", K(ret), K(job_level));
  } else if (OB_FAIL(job.set_clean_parameter(parameter))) {
    LOG_WARN("failed to set clean parameter", K(ret), K(parameter));
  } else if (OB_FAIL(job.set_executor_tenant_id(ObString(executor_tenant_id_str)))) {
    LOG_WARN("set backup tenant id failed", K(ret), K(executor_tenant_id_str));
  }

  return ret;
}

int ObBackupCleanJobOperator::advance_job_status(
    common::ObISQLClient &proxy, 
    const ObBackupCleanJobAttr &job_attr,
    const ObBackupCleanStatus &next_status, 
    const int result,
    const int64_t end_ts)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  const char *comment = OB_SUCCESS == result ? "" : common::ob_strerror(result);
  if (!next_status.is_valid() || !job_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status", K(ret), K(job_attr));
  } else if (OB_FAIL(sql.assign_fmt(
      "update %s set %s='%s',%s=%ld,%s=%d,%s='%s' where %s=%lu and %s=%lu", 
      OB_ALL_BACKUP_DELETE_JOB_TNAME, OB_STR_STATUS, next_status.get_str(),
      OB_STR_END_TS, end_ts, OB_STR_RESULT, result, OB_STR_COMMENT, comment,
      OB_STR_JOB_ID, job_attr.job_id_, OB_STR_TENANT_ID, job_attr.tenant_id_))) {
    LOG_WARN("failed to assign sql", K(ret), K(next_status));
  } else if (ObBackupCleanStatus::Status::CANCELING != next_status.status_ 
    && OB_FAIL(sql.append_fmt(" and %s='%s'", OB_STR_STATUS, job_attr.status_.get_str()))) {
    LOG_WARN("failed to append sql", K(ret), K(sql));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(job_attr.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid affected_rows", K(ret), K(affected_rows), K(sql), K(next_status));
  } else {
    FLOG_INFO("[BACKUP_CLEAN]success advance job status", K(job_attr), K(next_status));
  }
  return ret;
}

int ObBackupCleanJobOperator::update_task_count(
    common::ObISQLClient &proxy, 
    const ObBackupCleanJobAttr &job_attr,
    const bool is_total)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (!job_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task attr", K(ret), K(job_attr));
  } else if (is_total && OB_FAIL(sql.assign_fmt(
      "update %s set %s=%ld where %s=%ld and %s=%lu", 
      OB_ALL_BACKUP_DELETE_JOB_TNAME, OB_STR_TASK_COUNT, job_attr.task_count_,
      OB_STR_JOB_ID, job_attr.job_id_, OB_STR_TENANT_ID, job_attr.tenant_id_))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (!is_total && OB_FAIL(sql.assign_fmt(
      "update %s set %s=%ld where %s=%ld and %s=%lu", 
      OB_ALL_BACKUP_DELETE_JOB_TNAME, OB_STR_SUCCESS_TASK_COUNT, job_attr.success_task_count_,
      OB_STR_JOB_ID, job_attr.job_id_, OB_STR_TENANT_ID, job_attr.tenant_id_))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(job_attr.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else if (1 != affected_rows && 0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid affected_rows", K(ret), K(affected_rows), K(sql));
  } else {
    FLOG_INFO("[BACKUP_CLEAN]success update task count", K(job_attr), K(sql));
  }
  return ret;
}

int ObBackupCleanJobOperator::move_job_to_his(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const int64_t job_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (job_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),  K(job_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "insert into %s select * from %s where %s=%lu ", 
      OB_ALL_BACKUP_DELETE_JOB_HISTORY_TNAME, OB_ALL_BACKUP_DELETE_JOB_TNAME,
      OB_STR_JOB_ID, job_id))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else if (OB_FALSE_IT(sql.reset())) {
  } else if (OB_FAIL(sql.assign_fmt(
      "delete from %s where %s=%lu", 
      OB_ALL_BACKUP_DELETE_JOB_TNAME, OB_STR_JOB_ID, job_id))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else {
    FLOG_INFO("[BACKUP_CLEAN]succeed move backup job to history table", K(job_id), K(tenant_id));
  }
  return ret;
}

int ObBackupCleanJobOperator::check_same_tenant_and_clean_type_job_exist(
    common::ObISQLClient &proxy,
    const ObBackupCleanJobAttr &job_attr,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  is_exist = true;
  int64_t parameter = 0;
  int64_t affected_rows = -1;

  if (!job_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(job_attr));
  } else if (OB_FAIL(job_attr.get_clean_parameter(parameter))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt(
        "select * from %s where %s=%lu and type='%s' and parameter=%lu", 
        OB_ALL_BACKUP_DELETE_JOB_TNAME, OB_STR_TENANT_ID, job_attr.tenant_id_,
        ObNewBackupCleanType::get_str(job_attr.clean_type_), parameter))) {
        LOG_WARN("failed to assign sql", K(ret), K(sql), K(ObNewBackupCleanType::get_str(job_attr.clean_type_)));  
      } else if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(job_attr.tenant_id_), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql)); 
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          is_exist = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get next row", K(ret), K(sql));
        }
      }
    }
  }
  return ret;
}

int ObBackupCleanJobOperator::report_failed_to_sys_tenant(
  common::ObISQLClient &proxy,
  const ObBackupCleanJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  share::ObDMLSqlSplicer dml;
  int64_t affected_rows = -1;
  if (OB_SUCCESS == job_attr.result_) {
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to add pk column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_JOB_ID, job_attr.initiator_job_id_))) {
    LOG_WARN("failed to add pk column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_RESULT, job_attr.result_))) {
    LOG_WARN("failed to add pk column", K(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_DELETE_JOB_TNAME, sql))) {
    LOG_WARN("failed to splice_update_sql", K(ret));
  } else if (OB_FAIL(proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else if (1 < affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid affected_rows, more than one job be updated is invalid", K(ret), K(affected_rows), K(sql));
  } else {
    FLOG_INFO("succeed to report report_backup_result_to_initiator");
  }
  return ret;
}

int ObBackupCleanJobOperator::update_retry_count(
    common::ObISQLClient &proxy, 
    const ObBackupCleanJobAttr &job_attr)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (!job_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(job_attr));
  } else if (OB_FAIL(sql.assign_fmt(
      "update %s set %s=%ld where %s=%lu and %s=%ld", 
      OB_ALL_BACKUP_DELETE_JOB_TNAME, OB_STR_RETRY_COUNT, job_attr.retry_count_,
      OB_STR_TENANT_ID, job_attr.tenant_id_, OB_STR_JOB_ID, job_attr.job_id_))) {
    LOG_WARN("failed to init sql", K(ret), K(job_attr));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(job_attr.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid affected_rows", K(ret), K(affected_rows), K(sql), K(job_attr));
  }
  return ret;
}
//----------------------------__all_backup_task----------------------------
int ObBackupCleanTaskOperator::insert_backup_clean_task(
    common::ObISQLClient &proxy, 
    const ObBackupCleanTaskAttr &task_attr)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  if (!task_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_attr));
  } else if (OB_FAIL(fill_dml_with_backup_clean_task_(task_attr, dml))) {
    LOG_WARN("failed to fill backup set task", K(ret), K(task_attr));
  } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_BACKUP_DELETE_TASK_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret), K(task_attr));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(task_attr.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows), K(sql));
  } else {
    FLOG_INFO("[BACKUP_CLEAN]insert one backup clean task", K(task_attr), K(sql));
  }
  return ret;
}

int ObBackupCleanTaskOperator::fill_dml_with_backup_clean_task_(
    const ObBackupCleanTaskAttr &task_attr, 
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  int64_t id = 0;
  if (OB_FAIL(task_attr.get_backup_clean_id(id))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, task_attr.task_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, task_attr.tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_INCARNATION, task_attr.incarnation_id_))) {
    LOG_WARN("failed to add column", K(ret)); 
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_JOB_ID, task_attr.job_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_TYPE, ObBackupCleanTaskType::get_str(task_attr.task_type_)))) {
    LOG_WARN("failed to add column", K(ret)); 
  } else if (OB_FAIL(dml.add_column(OB_STR_ID, id))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_ROUND_ID, task_attr.round_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_DEST_ID, task_attr.dest_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, task_attr.status_.get_str()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_BACKUP_PATH, task_attr.backup_path_.ptr()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_RESULT, task_attr.result_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_START_TS, task_attr.start_ts_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_END_TS, task_attr.end_ts_))) {
    LOG_WARN("failed to add column", K(ret));
  }
  return ret;
}

int ObBackupCleanTaskOperator::get_backup_clean_tasks(
    common::ObISQLClient &proxy, 
    const int64_t job_id, 
    const uint64_t tenant_id,
    bool need_lock,  
    ObArray<ObBackupCleanTaskAttr> &task_attrs)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (job_id <= 0 || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(job_id), K(tenant_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("select * from %s where %s=%ld and %s=%lu order by end_ts",
          OB_ALL_BACKUP_DELETE_TASK_TNAME, OB_STR_TENANT_ID, tenant_id, OB_STR_JOB_ID, job_id))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (need_lock && OB_FAIL(sql.append_fmt(" for update"))) {
        LOG_WARN("failed to append sql", K(ret), K(job_id));
      } else if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql), K(tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get result", K(ret));
      } else if (OB_FAIL(parse_task_result_(*result, task_attrs))) {
        LOG_WARN("failed to parse result", K(ret), K(job_id));
      } else {
        FLOG_INFO("[BACKUP_CLEAN]success get backup clean tasks", K(sql), K(task_attrs));
      }
    }
  }
  return ret;
}

int ObBackupCleanTaskOperator::get_backup_clean_task(
    common::ObISQLClient &proxy, 
    const int64_t task_id, 
    const uint64_t tenant_id,
    bool need_lock,  
    ObBackupCleanTaskAttr &task_attr)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (task_id <= 0 || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id), K(tenant_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("select * from %s where %s=%ld and %s=%lu",
          OB_ALL_BACKUP_DELETE_TASK_TNAME, OB_STR_TENANT_ID, tenant_id, OB_STR_TASK_ID, task_id))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (need_lock && OB_FAIL(sql.append_fmt(" for update"))) {
        LOG_WARN("failed to append sql", K(ret), K(task_id));
      } else if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get result", K(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("entry not exit", K(ret), K(sql));
        } else {
          LOG_WARN("failed to get next", K(ret), K(sql));
        }
      } else if (OB_FAIL(do_parse_task_result_(*result, task_attr))) {
        LOG_WARN("failed to parse result", K(ret), K(task_id));
      } else {
        FLOG_INFO("[BACKUP_CLEAN]success get backup clean task", K(sql), K(task_attr));
      }
    }
  }
  return ret;
}

int ObBackupCleanTaskOperator::check_backup_clean_task_exist(
    common::ObISQLClient &proxy, 
    const uint64_t tenant_id,
    const ObBackupCleanTaskType::TYPE task_type,
    const int64_t id,
    const int64_t dest_id,
    bool need_lock,  
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  is_exist = true;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("select * from %s where %s=%ld and %s='%s' and %s=%ld and %s=%ld",
          OB_ALL_BACKUP_DELETE_TASK_TNAME, OB_STR_TENANT_ID, tenant_id, OB_STR_TASK_TYPE, ObBackupCleanTaskType::get_str(task_type),
          OB_STR_ID, id, OB_STR_DEST_ID, dest_id))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (need_lock && OB_FAIL(sql.append_fmt(" for update"))) {
        LOG_WARN("failed to append sql", K(ret));
      } else if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get result", K(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          is_exist = false;
          ret = OB_SUCCESS;
          LOG_INFO("[BACKUP_CLEAN]task is not exist", K(ret), K(sql));
        } else {
          LOG_WARN("failed to iterate result", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObBackupCleanTaskOperator::parse_task_result_(
    sqlclient::ObMySQLResult &result, 
    common::ObIArray<ObBackupCleanTaskAttr> &task_attrs)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObBackupCleanTaskAttr task_attr;
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (OB_FAIL(do_parse_task_result_(result, task_attr))) {
      LOG_WARN("failed to parse job result", K(ret));
    } else if (OB_FAIL(task_attrs.push_back(task_attr))) {
      LOG_WARN("failed to push back job", K(ret), K(task_attr));
    }
  }
  return ret;
}

int ObBackupCleanTaskOperator::do_parse_task_result_(
    ObMySQLResult &result, 
    ObBackupCleanTaskAttr &task_attr)
{
  int ret = OB_SUCCESS;
  int64_t real_length = 0;
  char backup_path[OB_MAX_BACKUP_DEST_LENGTH] = "";
  char status_str[OB_DEFAULT_STATUS_LENTH] = "";
  char task_type_str[OB_DEFAULT_STATUS_LENTH] = "";
  int64_t id = 0;
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, task_attr.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TASK_ID, task_attr.task_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INCARNATION, task_attr.incarnation_id_, int64_t); 
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_JOB_ID, task_attr.job_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ID, id, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ROUND_ID, task_attr.round_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DEST_ID, task_attr.dest_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_START_TS, task_attr.start_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_END_TS, task_attr.end_ts_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STATUS, status_str, OB_DEFAULT_STATUS_LENTH, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_TASK_TYPE, task_type_str, OB_DEFAULT_STATUS_LENTH, real_length);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_RESULT, task_attr.result_, int);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TOTAL_LS_COUNT, task_attr.total_ls_count_, int);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_FINISH_LS_COUNT, task_attr.finish_ls_count_, int);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_BACKUP_PATH, backup_path, OB_MAX_BACKUP_DEST_LENGTH, real_length);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(task_attr.backup_path_.assign(backup_path))) {
    LOG_WARN("failed to set backup path", K(ret), K(backup_path));
  } else if (OB_FAIL(task_attr.status_.set_status(status_str))) {
    LOG_WARN("failed to set status", K(ret), K(status_str));
  } else if (FALSE_IT(task_attr.task_type_ = ObBackupCleanTaskType::get_type(task_type_str))) {
  } else if (OB_FAIL(task_attr.set_backup_clean_id(id))) {
    LOG_WARN("failed to set backup clean id", K(ret), K(status_str)); 
  }
  return ret;
}

int ObBackupCleanTaskOperator::update_ls_count(
    common::ObISQLClient &proxy, 
    const ObBackupCleanTaskAttr &task_attr,
    const bool is_total)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (!task_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task attr", K(ret), K(task_attr));
  } else if (is_total && OB_FAIL(sql.assign_fmt(
      "update %s set %s=%ld where %s=%lu and %s=%lu", 
      OB_ALL_BACKUP_DELETE_TASK_TNAME, OB_STR_TOTAL_LS_COUNT, task_attr.total_ls_count_,
      OB_STR_TASK_ID, task_attr.task_id_, OB_STR_TENANT_ID, task_attr.tenant_id_))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (!is_total && OB_FAIL(sql.assign_fmt(
      "update %s set %s=%ld where %s=%lu and %s=%lu", 
      OB_ALL_BACKUP_DELETE_TASK_TNAME, OB_STR_FINISH_LS_COUNT, task_attr.finish_ls_count_,
      OB_STR_TASK_ID, task_attr.task_id_, OB_STR_TENANT_ID, task_attr.tenant_id_))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(task_attr.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else if (1 != affected_rows && 0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid affected_rows", K(ret), K(affected_rows), K(sql));
  } else {
    FLOG_INFO("[BACKUP_CLEAN]success update ls task count", K(task_attr), K(sql));
  }
  return ret;
}

int ObBackupCleanTaskOperator::advance_task_status(
    common::ObISQLClient &proxy, 
    const ObBackupCleanTaskAttr &task_attr,
    const ObBackupCleanStatus &next_status, 
    const int result,
    const int64_t end_ts)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  const char *comment = OB_SUCCESS == result ? "" : common::ob_strerror(result);
  if (!next_status.is_valid() || !task_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid next_status or invalid task", K(ret), K(task_attr));
  } else if (OB_FAIL(sql.assign_fmt(
      "update %s set %s='%s',%s=%d,%s='%s',%s=%ld where %s=%lu and %s=%lu", 
      OB_ALL_BACKUP_DELETE_TASK_TNAME, OB_STR_STATUS, next_status.get_str(),
      OB_STR_RESULT, result, OB_STR_COMMENT, comment, OB_STR_END_TS, end_ts,
      OB_STR_TASK_ID, task_attr.task_id_, OB_STR_TENANT_ID, task_attr.tenant_id_))) {
    LOG_WARN("failed to init sql", K(ret), K(next_status));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(task_attr.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid affected_rows", K(ret), K(affected_rows), K(sql));
  } else {
    FLOG_INFO("[BACKUP_CLEAN]success advance task status", K(task_attr),  K(sql));
  }
  return ret;
}

int ObBackupCleanTaskOperator::move_task_to_history(
    common::ObISQLClient &proxy, 
    const uint64_t tenant_id, 
    const int64_t task_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (!is_valid_tenant_id(tenant_id) || task_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(task_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "insert into %s select * from %s where %s=%lu and %s=%lu", 
      OB_ALL_BACKUP_DELETE_TASK_HISTORY_TNAME, OB_ALL_BACKUP_DELETE_TASK_TNAME,
      OB_STR_TASK_ID, task_id, OB_STR_TENANT_ID, tenant_id))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else if (OB_FALSE_IT(sql.reset())) {
  } else if (OB_FAIL(sql.assign_fmt("delete from %s where %s=%lu and %s=%lu", 
      OB_ALL_BACKUP_DELETE_TASK_TNAME, OB_STR_TASK_ID, task_id, OB_STR_TENANT_ID, tenant_id))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else {
    FLOG_INFO("[BACKUP_CLEAN]succeed move backup task to history table", K(tenant_id), K(task_id));
  }
  return ret;
}
 
int ObBackupCleanTaskOperator::update_stats(
    common::ObISQLClient &proxy, 
    const int64_t task_id, 
    const uint64_t tenant_id, 
    const ObBackupCleanStats &stats)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (task_id <= 0 || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id), K(tenant_id), K(stats));
  } else if (OB_FAIL(sql.assign_fmt(
      "update %s set %s=%ld, %s=%ld, %s=%ld, %s=%ld where %s=%ld and %s=%lu", 
      OB_ALL_BACKUP_DELETE_TASK_TNAME, OB_STR_TOTAL_BYTES, stats.total_bytes_, OB_STR_DELETE_BYTES, stats.delete_bytes_,
      OB_STR_TOTAL_FILES_COUNT, stats.total_files_count_, OB_STR_DELETE_FILES_COUNT, stats.delete_files_count_,
      OB_STR_TASK_ID, task_id, OB_STR_TENANT_ID, tenant_id))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else if (affected_rows > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid affected_rows", K(ret), K(sql));
  } else {
    FLOG_INFO("[BACKUP_CLEAN]success update stats", K(sql));
  }
  return ret;
}

int ObBackupCleanTaskOperator::check_current_task_exist(common::ObISQLClient &proxy, const uint64_t tenant_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  is_exist = true;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("select * from %s where %s=%ld",
          OB_ALL_BACKUP_DELETE_TASK_TNAME, OB_STR_TENANT_ID, tenant_id))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get result", K(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          is_exist = false;
          ret = OB_SUCCESS;
          LOG_INFO("[BACKUP_CLEAN]current task is not exist", K(ret), K(sql));
        } else {
          LOG_WARN("failed to iterate result", K(ret));
        }
      }
    }
  }
  return ret; 
}

/*
 *--------------------------__all_backup_ls_task---------------------------------
 */
int ObBackupCleanLSTaskOperator::update_stats_(
    common::ObISQLClient &proxy, 
    const int64_t task_id, 
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObBackupCleanStats &stats) 
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (task_id <= 0 || !is_valid_tenant_id(tenant_id) || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id), K(tenant_id), K(ls_id), K(stats));
  } else if (OB_FAIL(sql.assign_fmt(
      "update %s set %s=%ld, %s=%ld, %s=%ld, %s=%ld where %s=%ld and %s=%lu and %s=%ld", 
      OB_ALL_BACKUP_DELETE_LS_TASK_TNAME, OB_STR_TOTAL_BYTES, stats.total_bytes_, OB_STR_DELETE_BYTES, stats.delete_bytes_,
      OB_STR_TOTAL_FILES_COUNT, stats.total_files_count_, OB_STR_DELETE_FILES_COUNT, stats.delete_files_count_,
      OB_STR_TASK_ID, task_id, OB_STR_TENANT_ID, tenant_id, OB_STR_LS_ID, ls_id.id()))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else if (affected_rows != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid affected_rows", K(ret), K(sql));
  } else {
    FLOG_INFO("[BACKUP_CLEAN]success update stats", K(sql));
  }
  return ret;
}

int ObBackupCleanLSTaskOperator::insert_ls_task(
    common::ObISQLClient &proxy,
    const ObBackupCleanLSTaskAttr &ls_attr)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  if (!ls_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ls_attr));
  } else if (OB_FAIL(fill_dml_with_ls_task_(ls_attr, dml))) {
    LOG_WARN("failed to fill backup job", K(ret), K(ls_attr));
  } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_BACKUP_DELETE_LS_TASK_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret), K(ls_attr));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(ls_attr.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows), K(sql));
  } else {
    LOG_INFO("[BACKUP_CLEAN]insert one ls task", K(ls_attr), K(sql));
  }
  return ret;
}

int ObBackupCleanLSTaskOperator::fill_dml_with_ls_task_(
    const ObBackupCleanLSTaskAttr &ls_attr, 
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, ls_attr.task_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, ls_attr.tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_LS_ID, ls_attr.ls_id_.id()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_JOB_ID, ls_attr.job_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_TASK_TYPE, ObBackupCleanTaskType::get_str(ls_attr.task_type_)))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, ls_attr.status_.get_str()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_START_TS, ls_attr.start_ts_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_END_TS, ls_attr.end_ts_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_TASK_TRACE_ID, ""))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_RESULT, ls_attr.result_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_ROUND_ID, ls_attr.round_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else {
    if (ls_attr.is_delete_backup_set_task()) {
      if (OB_FAIL(dml.add_column(OB_STR_ID, ls_attr.backup_set_id_))) {
        LOG_WARN("failed to add column", K(ret));
      }
    } else if (ls_attr.is_delete_backup_piece_task()) {
      if (OB_FAIL(dml.add_column(OB_STR_ID, ls_attr.backup_piece_id_))) {
        LOG_WARN("failed to add column", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to add column", K(ret)); 
    }
  }
  return ret;
}

int ObBackupCleanLSTaskOperator::get_ls_tasks_from_task_id(
    common::ObISQLClient &proxy, 
    const int64_t task_id, 
    const uint64_t tenant_id,
    bool need_lock, 
    ObIArray<ObBackupCleanLSTaskAttr> &ls_attrs)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (task_id <= 0 || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(tenant_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("select * from %s where %s=%ld order by end_ts",
          OB_ALL_BACKUP_DELETE_LS_TASK_TNAME, OB_STR_TASK_ID, task_id))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (need_lock && OB_FAIL(sql.append_fmt(" for update"))) {
        LOG_WARN("failed to append sql", K(ret));
      } else if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_ls_result_(*result, ls_attrs))) {
        LOG_WARN("failed to parse result", K(ret));
      } else {
        LOG_INFO("[BACKUP_CLEAN]success get ls tasks from task id", K(ret), K(ls_attrs)); 
      }
    }
  }
  return ret;
}

int ObBackupCleanLSTaskOperator::get_ls_task(
    common::ObISQLClient &proxy, 
    bool need_lock, 
    const int64_t task_id,
    const uint64_t tenant_id, 
    const ObLSID &ls_id, 
    ObBackupCleanLSTaskAttr &ls_attr)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (task_id <= 0 || !is_valid_tenant_id(tenant_id) || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id), K(tenant_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("select * from %s where %s=%ld and %s=%ld and %s=%ld",
          OB_ALL_BACKUP_DELETE_LS_TASK_TNAME, OB_STR_TASK_ID, task_id, OB_STR_LS_ID, ls_id.id(), OB_STR_TENANT_ID, tenant_id))) {
        LOG_WARN("failed append sql", K(ret),K(sql));
      } else if (need_lock && OB_FAIL(sql.append_fmt(" for update"))) {
        LOG_WARN("failed to append sql", K(ret));
      } else if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("entry not exit", K(ret), K(sql));
        } else {
          LOG_WARN("failed to get next", K(ret), K(sql));
        }
      } else if (OB_FAIL(do_parse_ls_result_(*result, ls_attr))) {
        LOG_WARN("failed to do parse ls result", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupCleanLSTaskOperator::parse_ls_result_(
    sqlclient::ObMySQLResult &result, 
    ObIArray<ObBackupCleanLSTaskAttr> &ls_attrs)
{
  int ret = OB_SUCCESS;
  // traverse each returned row
  ObBackupCleanLSTaskAttr ls_attr;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (OB_FAIL(do_parse_ls_result_(result, ls_attr))) {
      LOG_WARN("failed to parse ls result", K(ret));
    } else if (OB_FAIL(ls_attrs.push_back(ls_attr))) {
      LOG_WARN("failed to push back ls", K(ret));
    } else {
      LOG_INFO("[BACKUP_CLEAN]success parse ls result", K(ret), K(ls_attrs), K(ls_attr));   
    }
  }
 
  return ret;
}

int ObBackupCleanLSTaskOperator::do_parse_ls_result_(ObMySQLResult &result, ObBackupCleanLSTaskAttr &ls_attr)
{
  int ret = OB_SUCCESS;
  ls_attr.reset();
  int64_t real_length = 0;
  char status_str[OB_DEFAULT_STATUS_LENTH] = "";
  char task_type_str[64] = "";
  char trace_id_str[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  char server_str[OB_MAX_SERVER_ADDR_SIZE] = { 0 };
  int64_t port = 0;
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TASK_ID, ls_attr.task_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_JOB_ID, ls_attr.job_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, ls_attr.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_LS_ID, ls_attr.ls_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ROUND_ID, ls_attr.round_id_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STATUS, status_str, OB_DEFAULT_STATUS_LENTH, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_TASK_TYPE, task_type_str, 64, real_length);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_START_TS, ls_attr.start_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_END_TS, ls_attr.end_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_RESULT, ls_attr.result_, int);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_RETRY_ID, ls_attr.retry_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TOTAL_BYTES, ls_attr.stats_.total_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DELETE_BYTES, ls_attr.stats_.delete_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TOTAL_FILES_COUNT, ls_attr.stats_.total_files_count_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DELETE_FILES_COUNT, ls_attr.stats_.delete_files_count_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_TASK_TRACE_ID, trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_SEVER_IP, server_str, OB_MAX_SERVER_ADDR_SIZE, real_length);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_SERVER_PORT, port, int64_t);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ls_attr.status_.set_status(status_str))) {
    LOG_WARN("failed to set status", K(ret), K(status_str));
  } else if (FALSE_IT(ls_attr.task_type_ = ObBackupCleanTaskType::get_type(task_type_str))) {
  } else if (strcmp(trace_id_str, "") != 0 && OB_FAIL(ls_attr.task_trace_id_.set(trace_id_str))) {
    LOG_WARN("failed to set ls task trace id", K(ret), K(trace_id_str));
  } else if (!ls_attr.dst_.set_ip_addr(server_str, static_cast<int32_t>(port))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to set server ip and port", K(ret), K(server_str), K(port));
  } else {
    if (ls_attr.is_delete_backup_set_task()) {
      EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ID, ls_attr.backup_set_id_, int64_t);
    } else if (ls_attr.is_delete_backup_piece_task()) {
      EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ID, ls_attr.backup_piece_id_, int64_t);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task type error", K(ret), K(ls_attr));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("[BACKUP_CLEAN]success to read ls attr", K(ls_attr));
  }
  return ret;
}

int ObBackupCleanLSTaskOperator::advance_ls_task_status(
    common::ObISQLClient &proxy, 
    const ObBackupCleanLSTaskAttr &ls_attr,
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
    LOG_WARN("invalid status", K(ret), K(ls_attr), K(next_status));
  } else if (OB_FAIL(sql.assign_fmt(
      "update %s set %s='%s', %s=%d, %s=%ld, %s='%s' where %s=%lu and %s=%lu and %s=%ld and %s='%s'", 
      OB_ALL_BACKUP_DELETE_LS_TASK_TNAME, OB_STR_STATUS, next_status.get_str(),
      OB_STR_RESULT, result, OB_STR_END_TS, end_ts, OB_STR_COMMENT, comment,
      OB_STR_TASK_ID, ls_attr.task_id_, OB_STR_TENANT_ID, ls_attr.tenant_id_,
      OB_STR_LS_ID, ls_attr.ls_id_.id(), OB_STR_STATUS, ls_attr.status_.get_str()))) {
    LOG_WARN("failed to init sql", K(ret), K(ls_attr), K(next_status));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(ls_attr.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid affected_rows", K(ret), K(affected_rows), K(sql), K(next_status));
  } else {
    FLOG_INFO("[BACKUP_CLEAN]advance ls task status", K(ls_attr), K(next_status), K(result), K(sql));
  }
  return ret;
}

int ObBackupCleanLSTaskOperator::redo_ls_task(
    common::ObISQLClient &proxy, 
    const ObBackupCleanLSTaskAttr &ls_attr,
    const int64_t retry_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  const char *empty_str =  "";
  if (!ls_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_attr));
  } else if (OB_FAIL(sql.assign_fmt(
      "update %s set %s='INIT',%s=%d,%s='%s',%s=0,%s='%s',%s='%s', %s=%ld where %s=%lu and %s=%lu and %s=%ld and %s='%s'", 
      OB_ALL_BACKUP_DELETE_LS_TASK_TNAME, OB_STR_STATUS,
      OB_STR_RESULT, OB_SUCCESS, OB_STR_SEVER_IP, empty_str, OB_STR_SERVER_PORT,
      OB_STR_TASK_TRACE_ID, empty_str, OB_STR_COMMENT, empty_str, OB_STR_RETRY_ID, retry_id,
      OB_STR_TASK_ID, ls_attr.task_id_, OB_STR_TENANT_ID, ls_attr.tenant_id_,
      OB_STR_LS_ID, ls_attr.ls_id_.id(), OB_STR_STATUS, ls_attr.status_.get_str()))) {
    LOG_WARN("failed to init sql", K(ret), K(ls_attr));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(ls_attr.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid affected_rows", K(ret), K(affected_rows), K(sql), K(ls_attr));
  } else {
    LOG_INFO("[BACKUP_CLEAN]success redo ls status", K(ls_attr), K(sql));
  }
  return ret;
}

int ObBackupCleanLSTaskOperator::update_dst_and_status(
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
    LOG_WARN("invalid argument", K(ret), K(dst), K(ls_id));
  } else if (!dst.ip_to_string(server_ip, OB_MAX_SERVER_ADDR_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to change ip to string", K(ret), K(dst));
  } else if (OB_FAIL(sql.assign_fmt(
      "update %s set %s='DOING', %s='%s', %s=%d, %s='%s' where %s=%ld and %s=%lu and %s=%ld and %s='PENDING'", 
      OB_ALL_BACKUP_DELETE_LS_TASK_TNAME, OB_STR_STATUS, OB_STR_SEVER_IP, server_ip, 
      OB_STR_SERVER_PORT, dst.get_port(), OB_STR_TASK_TRACE_ID, trace_id_str, OB_STR_TASK_ID, task_id, OB_STR_TENANT_ID, tenant_id,
      OB_STR_LS_ID, ls_id.id(), OB_STR_STATUS))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid affected_rows", K(ret), K(affected_rows), K(sql));
  } else {
    FLOG_INFO("[BACKUP_CLEAN]success update dst and status", K(sql));
  }
  return ret;
}

int ObBackupCleanLSTaskOperator::move_ls_to_his(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t task_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (!is_valid_tenant_id(tenant_id) || task_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(task_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "insert into %s select * from %s where %s=%lu and %s=%ld", 
      OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_TNAME, OB_ALL_BACKUP_DELETE_LS_TASK_TNAME,
      OB_STR_TENANT_ID, tenant_id, OB_STR_TASK_ID, task_id))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else if (OB_FALSE_IT(sql.reset())) {
  } else if (OB_FAIL(sql.assign_fmt("delete from %s where %s=%lu and %s=%ld", 
      OB_ALL_BACKUP_DELETE_LS_TASK_TNAME, OB_STR_TENANT_ID, tenant_id, OB_STR_TASK_ID, task_id))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else {
    FLOG_INFO("[BACKUP_CLEAN]succeed move backup clean ls task to history table", K(tenant_id), K(task_id), K(affected_rows));
  }
  return ret;
}

int ObDeletePolicyOperator::fill_dml_with_delete_policy_(const ObDeletePolicyAttr &delete_policy, ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, delete_policy.tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_POLICY_NAME, delete_policy.policy_name_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_RECOVERY_WINDOW, delete_policy.recovery_window_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_REDUNDANACY, delete_policy.redundancy_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_BACKUP_COPIES, delete_policy.backup_copies_))) {
    LOG_WARN("failed to add column", K(ret));
  } 
  return ret;
}

int ObDeletePolicyOperator::insert_delete_policy(common::ObISQLClient &proxy, const ObDeletePolicyAttr &delete_policy)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  if (!delete_policy.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(delete_policy));
  } else if (OB_FAIL(fill_dml_with_delete_policy_(delete_policy, dml))) {
    LOG_WARN("failed to fill backup job", K(ret), K(delete_policy));
  } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_BACKUP_DELETE_POLICY_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret), K(delete_policy));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(delete_policy.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows), K(sql), K(delete_policy));
  } else {
    LOG_INFO("[BACKUP_CLEAN]success insert one backup delete policy", K(delete_policy), K(sql));
  }
  return ret;
}

int ObDeletePolicyOperator::drop_delete_policy(common::ObISQLClient &proxy, const ObDeletePolicyAttr &delete_policy)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("delete from %s where %s=%lu and %s='%s'", 
      OB_ALL_BACKUP_DELETE_POLICY_TNAME, OB_STR_TENANT_ID, delete_policy.tenant_id_, OB_STR_POLICY_NAME, delete_policy.policy_name_))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(delete_policy.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  }
  return ret;
}

int ObDeletePolicyOperator::get_default_delete_policy(common::ObISQLClient &proxy, const uint64_t tenant_id, ObDeletePolicyAttr &delete_policy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("select * from %s where %s=%lu and %s='default'",
          OB_ALL_BACKUP_DELETE_POLICY_TNAME, OB_STR_TENANT_ID, tenant_id, OB_STR_POLICY_NAME))) {
        LOG_WARN("failed append sql", K(ret),K(sql));
      } else if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("entry not exit", K(ret), K(sql));
        } else {
          LOG_WARN("failed to get next", K(ret), K(sql));
        }
      } else {
        int64_t real_length = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_TENANT_ID, delete_policy.tenant_id_, uint64_t);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_RECOVERY_WINDOW, delete_policy.recovery_window_, sizeof(delete_policy.recovery_window_), real_length);
        LOG_INFO("get value", K(ret), K(delete_policy));
      }
    }
  }
  return ret;
}

}
}