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

#include "ob_import_table_persist_helper.h"
#include "ob_import_table_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

using namespace oceanbase;
using namespace share;
ObImportTableJobPersistHelper::ObImportTableJobPersistHelper()
  : is_inited_(false), tenant_id_(), table_op_()
{
}

int ObImportTableJobPersistHelper::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if(!is_sys_tenant(tenant_id) && !is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(table_op_.init(OB_ALL_IMPORT_TABLE_JOB_TNAME, *this))) {
    LOG_WARN("failed to init table op", K(ret));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}


int ObImportTableJobPersistHelper::insert_import_table_job(
    common::ObISQLClient &proxy, const ObImportTableJob &job) const
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

int ObImportTableJobPersistHelper::get_import_table_job(
    common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t job_id, ObImportTableJob &job) const
{
  int ret = OB_SUCCESS;
  ObImportTableJob::Key key;
  key.tenant_id_ = tenant_id;
  key.job_id_ = job_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObImportTableJobPersistHelper not init", K(ret));
  } else if (OB_FAIL(table_op_.get_row(proxy, false, key, job))) {
    LOG_WARN("failed to get row", KR(ret), K(key));
  }
  return ret;
}

int ObImportTableJobPersistHelper::get_all_import_table_jobs(
    common::ObISQLClient &proxy,  common::ObIArray<ObImportTableJob> &jobs) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = get_exec_tenant_id();
  jobs.reset();
  if (OB_FAIL(sql.assign_fmt("select * from %s", OB_ALL_IMPORT_TABLE_JOB_TNAME))) {
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
          ObImportTableJob job;
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

int ObImportTableJobPersistHelper::advance_status(
    common::ObISQLClient &proxy, const ObImportTableJob &job, const ObImportTableJobStatus &next_status) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObImportTableJobPersistHelper not init", K(ret));
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
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_IMPORT_TABLE_JOB_TNAME, sql))) {
    LOG_WARN("failed to splice update sql", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" and %s='%s'", OB_STR_STATUS, job.get_status().get_str()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(proxy.write(get_exec_tenant_id(), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write", K(ret), K(sql));
  }
  return ret;
}

int ObImportTableJobPersistHelper::force_cancel_import_job(common::ObISQLClient &proxy) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml;
  ObRecoverTableStatus status(ObRecoverTableStatus::CANCELING);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObImportTableJobPersistHelper not init", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, status.get_str()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_IMPORT_TABLE_JOB_TNAME, sql))) {
    LOG_WARN("failed to splice update sql", K(ret));
  } else if (OB_FAIL(proxy.write(get_exec_tenant_id(), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else {
    LOG_INFO("success cancel import job", K(tenant_id_));
  }
  return ret;
}

int ObImportTableJobPersistHelper::move_import_job_to_history(
    common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t job_id) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (tenant_id == OB_INVALID_TENANT_ID || job_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "insert into %s select * from %s where %s=%lu",
      OB_ALL_IMPORT_TABLE_JOB_HISTORY_TNAME, OB_ALL_IMPORT_TABLE_JOB_TNAME,
      OB_STR_JOB_ID, job_id))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else if (OB_FALSE_IT(sql.reset())) {
  } else if (OB_FAIL(sql.assign_fmt("delete from %s where %s=%lu",
      OB_ALL_IMPORT_TABLE_JOB_TNAME, OB_STR_JOB_ID, job_id))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else {
    LOG_INFO("succeed move import job to history table", K(tenant_id), K(job_id));
  }
  return ret;
}

int ObImportTableJobPersistHelper::get_import_table_job_history_by_initiator(common::ObISQLClient &proxy,
    const uint64_t initiator_tenant_id, const uint64_t initiator_job_id, ObImportTableJob &job) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = get_exec_tenant_id();
  if (OB_FAIL(sql.assign_fmt("select * from %s", OB_ALL_IMPORT_TABLE_JOB_HISTORY_TNAME))) {
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
        } else {
          LOG_WARN("failed to get next", K(ret));
        }
      } else if (OB_FAIL(job.parse_from(*result))) {
        LOG_WARN("failed to parse row", K(ret));
      } else if (OB_ITER_END != result->next()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("multi value exist", K(ret), K(sql), K(exec_tenant_id));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  LOG_INFO("get import table job history", K(ret), K(job), K(sql));
  return ret;
}

int ObImportTableJobPersistHelper::get_import_table_job_by_initiator(common::ObISQLClient &proxy,
    const uint64_t initiator_tenant_id, const uint64_t initiator_job_id, ObImportTableJob &job) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = get_exec_tenant_id();
  if (OB_FAIL(sql.assign_fmt("select * from %s", OB_ALL_IMPORT_TABLE_JOB_TNAME))) {
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
        } else {
          LOG_WARN("failed to get next", K(ret));
        }
      } else if (OB_FAIL(job.parse_from(*result))) {
        LOG_WARN("failed to parse row", K(ret));
      } else if (OB_ITER_END != result->next()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("multi value exist", K(ret), K(sql), K(exec_tenant_id));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  LOG_INFO("get import table job", K(ret), K(job), K(sql));
  return ret;
}

int ObImportTableJobPersistHelper::report_import_job_statistics(
    common::ObISQLClient &proxy, const ObImportTableJob &job) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObImportTableJobPersistHelper not init", K(ret));
  } else if (OB_FAIL(table_op_.update_row(proxy, job, affected_rows))) {
    LOG_WARN("failed to compare and swap status", K(ret), K(job));
  }
  return ret;
}

int ObImportTableJobPersistHelper::report_statistics(common::ObISQLClient &proxy, const ObImportTableJob &job) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObImportTableTaskPersistHelper not init", K(ret));
  } else if (OB_FAIL(table_op_.update_row(proxy, job, affected_rows))) {
    LOG_WARN("failed to update row", K(ret), K(job));
  }
  return ret;
}

ObImportTableTaskPersistHelper::ObImportTableTaskPersistHelper()
  : is_inited_(false), tenant_id_(), table_op_()
{
}

int ObImportTableTaskPersistHelper::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if(!is_sys_tenant(tenant_id) && !is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(table_op_.init(OB_ALL_IMPORT_TABLE_TASK_TNAME, *this))) {
    LOG_WARN("failed to init table op", K(ret));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}


int ObImportTableTaskPersistHelper::insert_import_table_task(
    common::ObISQLClient &proxy, const ObImportTableTask &task) const
{
  int ret = OB_SUCCESS;
  int64_t affect_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoverTablePersistHelper not init", K(ret));
  } else if (OB_FAIL(table_op_.insert_or_update_row(proxy, task, affect_rows))) {
    LOG_WARN("failed to insert or update row", K(ret), K(task));
  }
  return ret;
}

int ObImportTableTaskPersistHelper::get_recover_table_task(
    common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t task_id, ObImportTableTask &task) const
{
  int ret = OB_SUCCESS;
  ObImportTableTask::Key key;
  key.tenant_id_ = tenant_id;
  key.task_id_ = task_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObImportTableTaskPersistHelper not init", K(ret));
  } else if (OB_FAIL(table_op_.get_row(proxy, false, key, task))) {
    LOG_WARN("failed to get row", KR(ret), K(key));
  }
  return ret;
}

int ObImportTableTaskPersistHelper::advance_status(
    common::ObISQLClient &proxy, const ObImportTableTask &task, const ObImportTableTaskStatus &next_status) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObImportTableTaskPersistHelper not init", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, task.get_task_id()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, task.get_tenant_id()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (next_status.is_finish() && OB_FAIL(dml.add_column(OB_STR_RESULT, task.get_result().get_result_str()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (next_status.is_finish() && OB_FAIL(dml.add_column(OB_STR_COMMENT, task.get_result().get_comment()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, next_status.get_str()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_IMPORT_TABLE_TASK_TNAME, sql))) {
    LOG_WARN("failed to splice update sql", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" and %s='%s'", OB_STR_STATUS, task.get_status().get_str()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(proxy.write(get_exec_tenant_id(), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write", K(ret), K(sql));
  }
  return ret;
}

int ObImportTableTaskPersistHelper::get_all_import_table_tasks_by_initiator(common::ObISQLClient &proxy,
    const ObImportTableJob &job, common::ObIArray<ObImportTableTask> &tasks) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = get_exec_tenant_id();
  tasks.reset();
  const int64_t job_id = job.get_job_id();
  if (OB_FAIL(sql.assign_fmt("select * from %s", OB_ALL_IMPORT_TABLE_TASK_TNAME))) {
    LOG_WARN("fail to assign sql", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" where job_id=%ld", job_id))) {
    LOG_WARN("failed to append sql", K(ret), K(sql), K(job_id));
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
          ObImportTableTask task;
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("failed to get next row", K(ret));
            }
          } else if (OB_FAIL(task.parse_from(*result))) {
            LOG_WARN("failed to parse job result", K(ret));
          } else if (OB_FAIL(tasks.push_back(task))) {
            LOG_WARN("failed to push back job", K(ret), K(task));
          }
        }
      }
    }
  }
  LOG_INFO("get import table tasks", K(ret), K(tasks));
  return ret;
}

int ObImportTableTaskPersistHelper::get_one_not_finish_task_by_initiator(
    common::ObISQLClient &proxy, const ObImportTableJob &job, bool &all_finish, ObImportTableTask &task) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = get_exec_tenant_id();
  task.reset();
  all_finish = false;
  const int64_t job_id = job.get_job_id();
  if (OB_FAIL(sql.assign_fmt("select * from %s", OB_ALL_IMPORT_TABLE_TASK_TNAME))) {
    LOG_WARN("fail to assign sql", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" where job_id=%ld and status != 'FINISH' order by task_id limit 1", job_id))) {
    LOG_WARN("failed to append sql", K(ret), K(sql), K(job_id));
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
          all_finish = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get next", K(ret));
        }
      } else if (OB_FAIL(task.parse_from(*result))) {
        LOG_WARN("failed to parse from result", K(ret));
      }
    }
  }
  LOG_INFO("get import table tasks", K(ret), K(task));
  return ret;
}

int ObImportTableTaskPersistHelper::move_import_task_to_history(
    common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t job_id) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (tenant_id == OB_INVALID_TENANT_ID || job_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "insert into %s select * from %s where %s=%lu",
      OB_ALL_IMPORT_TABLE_TASK_HISTORY_TNAME, OB_ALL_IMPORT_TABLE_TASK_TNAME,
      OB_STR_JOB_ID, job_id))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else if (OB_FALSE_IT(sql.reset())) {
  } else if (OB_FAIL(sql.assign_fmt("delete from %s where %s=%lu",
      OB_ALL_IMPORT_TABLE_TASK_TNAME, OB_STR_JOB_ID, job_id))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else {
    LOG_INFO("succeed move import task to history table", K(tenant_id), K(job_id));
  }
  return ret;
}

int ObImportTableTaskPersistHelper::report_import_task_statistics(
    common::ObISQLClient &proxy, const ObImportTableTask &task) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObImportTableTaskPersistHelper not init", K(ret));
  } else if (OB_FAIL(table_op_.update_row(proxy, task, affected_rows))) {
    LOG_WARN("failed to compare and swap status", K(ret), K(task));
  }
  return ret;
}