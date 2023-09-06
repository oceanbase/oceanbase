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

#include "ob_recover_table_persist_helper.h"
#include "ob_import_table_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

using namespace oceanbase;
using namespace share;
ObRecoverTablePersistHelper::ObRecoverTablePersistHelper()
  : is_inited_(false), tenant_id_()
{
}

int ObRecoverTablePersistHelper::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if(!is_sys_tenant(tenant_id) && !is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(table_op_.init(OB_ALL_RECOVER_TABLE_JOB_TNAME, *this))) {
    LOG_WARN("failed to init table op", K(ret));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObRecoverTablePersistHelper::insert_recover_table_job(
    common::ObISQLClient &proxy, const ObRecoverTableJob &job) const
{
  int ret = OB_SUCCESS;
  int64_t affect_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoverTablePersistHelper not init", K(ret));
  } else if (OB_FAIL(table_op_.insert_or_update_row(proxy, job, affect_rows))) {
    LOG_WARN("failed to insert or update row", K(ret), K(job));
  }
  return ret;
}

int ObRecoverTablePersistHelper::get_recover_table_job(
    common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t job_id,
    ObRecoverTableJob &job) const
{
  int ret = OB_SUCCESS;
  ObRecoverTableJob::Key key;
  key.tenant_id_ = tenant_id;
  key.job_id_ = job_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoverTablePersistHelper not init", K(ret));
  } else if (OB_FAIL(table_op_.get_row(proxy, false, key, job))) {
    LOG_WARN("failed to get row", KR(ret), K(key));
  }
  return ret;
}

int ObRecoverTablePersistHelper::is_recover_table_job_exist(
    common::ObISQLClient &proxy,
    const uint64_t target_tenant_id,
    bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = get_exec_tenant_id();
  if (OB_FAIL(sql.assign_fmt("select count(*) cnt from %s where target_tenant_id=%lu", OB_ALL_RECOVER_TABLE_JOB_TNAME, target_tenant_id))) {
    LOG_WARN("fail to assign sql", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to get next row", K(ret));
      } else {
        int64_t cnt = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, "cnt", cnt, int64_t);
        if (OB_FAIL(ret)) {
        } else {
          is_exist = 0 != cnt;
        }
      }
    }
  }

  return ret;
}

int ObRecoverTablePersistHelper::advance_status(
    common::ObISQLClient &proxy, const ObRecoverTableJob &job, const ObRecoverTableStatus &next_status) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoverTablePersistHelper not init", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_JOB_ID, job.get_job_id()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, job.get_tenant_id()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (next_status.is_finish() && OB_FAIL(dml.add_column(OB_STR_RESULT, job.get_result().get_result_str()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (next_status.is_finish() && OB_FAIL(dml.add_column(OB_STR_COMMENT, job.get_result().get_comment()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (next_status.is_finish() && OB_FAIL(dml.add_column(OB_STR_END_TS, job.get_end_ts()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, next_status.get_str()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_RECOVER_TABLE_JOB_TNAME, sql))) {
    LOG_WARN("failed to splice update sql", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" and %s='%s'", OB_STR_STATUS, job.get_status().get_str()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(proxy.write(get_exec_tenant_id(), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write", K(ret), K(sql));
  }
  return ret;
}

int ObRecoverTablePersistHelper::force_cancel_recover_job(common::ObISQLClient &proxy) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml;
  ObRecoverTableStatus status(ObRecoverTableStatus::CANCELING);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoverTablePersistHelper not init", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, status.get_str()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_RECOVER_TABLE_JOB_TNAME, sql))) {
    LOG_WARN("failed to splice update sql", K(ret));
  } else if (OB_FAIL(proxy.write(get_exec_tenant_id(), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else {
    LOG_INFO("success cancel recover job", K(tenant_id_));
  }
  return ret;
}

int ObRecoverTablePersistHelper::get_all_recover_table_job(
    common::ObISQLClient &proxy,  common::ObIArray<ObRecoverTableJob> &jobs) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = get_exec_tenant_id();
  jobs.reset();
  if (OB_FAIL(sql.assign_fmt("select * from %s", OB_ALL_RECOVER_TABLE_JOB_TNAME))) {
    LOG_WARN("fail to assign sql", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql), K(exec_tenant_id));
      } else {
        while (OB_SUCC(ret)) {
          ObRecoverTableJob job;
          job.reset();
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("failed to get next row", K(ret));
            }
          } else if (OB_FAIL(job.parse_from(*result))) {
            LOG_WARN("failed to parse job result", K(ret));
          } else if (OB_FAIL(jobs.push_back(job))) {
            LOG_WARN("failed to push back job", K(ret), K(job));
          }
        }
      }
    }
  }
  LOG_INFO("get targets", K(ret), K(jobs), K(exec_tenant_id), K(sql));
  return ret;
}

int ObRecoverTablePersistHelper::get_recover_table_job_by_initiator(common::ObISQLClient &proxy,
    const ObRecoverTableJob &initiator_job, ObRecoverTableJob &target_job) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  uint64_t initiator_tenant_id = initiator_job.get_tenant_id();
  int64_t initiator_job_id = initiator_job.get_job_id();
  const uint64_t exec_tenant_id = get_exec_tenant_id();
  if (OB_FAIL(sql.assign_fmt("select * from %s", OB_ALL_RECOVER_TABLE_JOB_TNAME))) {
    LOG_WARN("fail to assign sql", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" where initiator_tenant_id =%lu and initiator_job_id=%ld", initiator_tenant_id, initiator_job_id))) {
    LOG_WARN("failed to append sql", K(ret), K(sql), K(initiator_tenant_id), K(initiator_job_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
        LOG_WARN("no row exist", K(ret));
      } else if (OB_FAIL(target_job.parse_from(*result))) {
        LOG_WARN("failed to parse row", K(ret));
      } else if (OB_ITER_END != result->next()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("multi value exist", K(ret), K(sql), K(exec_tenant_id));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  LOG_INFO("get recover table job", K(ret), K(target_job), K(sql));
  return ret;
}

int ObRecoverTablePersistHelper::delete_recover_table_job(
    common::ObISQLClient &proxy, const ObRecoverTableJob &job) const
{
  int ret = OB_SUCCESS;
  int64_t affect_rows = 0;
  ObRecoverTableJob::Key key;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoverTablePersistHelper not init", K(ret));
  } else if (OB_FAIL(job.get_pkey(key))) {
    LOG_WARN("failed to get pkey", K(ret));
  } else if (OB_FAIL(table_op_.delete_row(proxy, key, affect_rows))) {
    LOG_WARN("failed to delete row", K(ret), K(job));
  }
  return ret;
}

int ObRecoverTablePersistHelper::insert_recover_table_job_history(
    common::ObISQLClient &proxy, const ObRecoverTableJob &job) const
{
  int ret = OB_SUCCESS;
  int64_t affect_rows = 0;
  ObInnerTableOperator table_op;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoverTablePersistHelper not init", K(ret));
  } else if (OB_FAIL(table_op.init(OB_ALL_RECOVER_TABLE_JOB_HISTORY_TNAME, *this))) {
    LOG_WARN("failed to init table op", K(ret));
  } else if (OB_FAIL(table_op.insert_or_update_row(proxy, job, affect_rows))) {
    LOG_WARN("failed to insert or update row", K(ret), K(job));
  }
  return ret;
}

int ObRecoverTablePersistHelper::get_recover_table_job_history_by_initiator(common::ObISQLClient &proxy,
    const ObRecoverTableJob &initiator_job, ObRecoverTableJob &target_job) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  uint64_t initiator_tenant_id = initiator_job.get_tenant_id();
  int64_t initiator_job_id = initiator_job.get_job_id();
  const uint64_t exec_tenant_id = get_exec_tenant_id();
  if (OB_FAIL(sql.assign_fmt("select * from %s", OB_ALL_RECOVER_TABLE_JOB_HISTORY_TNAME))) {
    LOG_WARN("fail to assign sql", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" where initiator_tenant_id =%lu and initiator_job_id=%ld", initiator_tenant_id, initiator_job_id))) {
    LOG_WARN("failed to append sql", K(ret), K(sql), K(initiator_tenant_id), K(initiator_job_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
        LOG_WARN("no row exist", K(ret));
      } else if (OB_FAIL(target_job.parse_from(*result))) {
        LOG_WARN("failed to parse row", K(ret));
      } else if (OB_ITER_END != result->next()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("multi value exist", K(ret), K(sql), K(exec_tenant_id));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  LOG_INFO("get recover table job history", K(ret), K(target_job), K(sql));
  return ret;
}