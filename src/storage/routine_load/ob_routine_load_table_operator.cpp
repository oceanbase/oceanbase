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

#include "share/ob_dml_sql_splicer.h"
#include "storage/routine_load/ob_routine_load_table_operator.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{

int ObRoutineLoadJob::init(
    const uint64_t tenant_id,
    const int64_t job_id,
    const ObString &job_name,
    const int64_t create_time,
    const int64_t pause_time,
    const int64_t end_time,
    const uint64_t database_id,
    const uint64_t table_id,
    const ObRoutineLoadJobState state,
    const ObString &job_properties,
    const ObRLoadDynamicFields &dynamic_fields,
    const ObString &err_infos,
    const common::ObCurTraceId::TraceId &trace_id,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  job_id_ = job_id;
  job_name_ = job_name;
  create_time_ = create_time;
  pause_time_ = pause_time;
  end_time_ = end_time;
  database_id_ = database_id;
  table_id_ = table_id;
  state_ = state;
  job_properties_ = job_properties;
  dynamic_fields_ = dynamic_fields;
  err_infos_ = err_infos;
  trace_id_ = trace_id;
  ret_code_ = ret_code;
  return ret;
}

bool ObRoutineLoadJob::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && OB_INVALID_ID != job_id_
         && !job_name_.empty()
         && OB_INVALID_TIMESTAMP != create_time_
         && OB_INVALID_ID != database_id_
         && OB_INVALID_ID != table_id_
         && ObRoutineLoadJobState::MAX > state_
         && !job_properties_.empty();
}

ObRoutineLoadTableOperator::ObRoutineLoadTableOperator()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    proxy_(nullptr)
{
}

const char* ObRoutineLoadTableOperator::JOB_STATE_ARRAY[] =
{
  "RUNNING",
  "PAUSED",
  "STOPPED",
  "CANCELLED",
};

ObRoutineLoadJobState ObRoutineLoadTableOperator::str_to_job_state(const ObString &state_str)
{
  ObRoutineLoadJobState ret_state = ObRoutineLoadJobState::MAX;
  if (state_str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(JOB_STATE_ARRAY); i++) {
      if (0 == state_str.case_compare(JOB_STATE_ARRAY[i])) {
        ret_state = static_cast<ObRoutineLoadJobState>(i);
        break;
      }
    }
  }
  return ret_state;
}

const char* ObRoutineLoadTableOperator::job_state_to_str(const ObRoutineLoadJobState &state)
{
  STATIC_ASSERT(ARRAYSIZEOF(JOB_STATE_ARRAY) == static_cast<int64_t>(ObRoutineLoadJobState::MAX),
                "state string array size mismatch with enum routine load job state count");
  const char* str = INVALID_STR;
  if (state >= ObRoutineLoadJobState::RUNNING && state < ObRoutineLoadJobState::MAX) {
    str = JOB_STATE_ARRAY[static_cast<int64_t>(state)];
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid routine load job state", K(state));
  }
  return str;
}

int ObRoutineLoadTableOperator::init(const uint64_t tenant_id, ObISQLClient *proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("clone table operator init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_sys_tenant(tenant_id) && !is_user_tenant(tenant_id)
                         || OB_ISNULL(proxy))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(proxy));
  } else {
    tenant_id_ = tenant_id;
    proxy_ = proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObRoutineLoadTableOperator::insert_routine_load_job(const ObRoutineLoadJob &job)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  const int64_t start_time = 0; // in add_time_column(), "0" means now(6)

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clone table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job));
  } else if (OB_FAIL(build_insert_dml_(job, dml))) {
    LOG_WARN("fail to build insert dml", KR(ret), K(job));
  } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_ROUTINE_LOAD_JOB_TNAME, sql))) {
    LOG_WARN("splice insert sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(tenant_id_), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }

  return ret;
}

int ObRoutineLoadTableOperator::build_insert_dml_(
    const ObRoutineLoadJob &job,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  char trace_id_buf[OB_MAX_TRACE_ID_BUFFER_SIZE] = {'\0'};
  if (OB_FAIL(dml.add_pk_column("job_id", job.get_job_id()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("job_name", job.get_job_name()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_time_column("create_time", job.get_create_time()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_INVALID_TIMESTAMP != job.get_pause_time()
             && OB_FAIL(dml.add_time_column("pause_time", job.get_pause_time()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_INVALID_TIMESTAMP != job.get_end_time()
             && OB_FAIL(dml.add_time_column("end_time", job.get_end_time()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("database_id", job.get_database_id()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("table_id", job.get_table_id()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("state", job_state_to_str(job.get_state())))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("job_properties", ObHexEscapeSqlStr(job.get_job_properties())))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("progress", job.get_dynamic_fields().progress_))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("lag", job.get_dynamic_fields().lag_))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("tmp_progress", job.get_dynamic_fields().tmp_progress_))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("tmp_lag", job.get_dynamic_fields().tmp_lag_))) {
    LOG_WARN("add column failed", KR(ret));
  // } else if (OB_FAIL(dml.add_column("err_infos", job.get_err_infos()))) {
  //   LOG_WARN("add column failed", KR(ret));
  } else if (FALSE_IT(job.get_trace_id().to_string(trace_id_buf, sizeof(trace_id_buf)))) {
  } else if (OB_FAIL(dml.add_column("last_trace_id", trace_id_buf))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("last_ret_code", job.get_ret_code()))) {
    LOG_WARN("add column failed", KR(ret));
  }
  return ret;
}

int ObRoutineLoadTableOperator::get_dynamic_fields(
    const int64_t job_id,
    const bool need_lock,
    common::ObIAllocator &allocator,
    ObRLoadDynamicFields &dynamic_fields)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_ID == job_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT progress, lag, tmp_progress, tmp_lag "
                                    "FROM %s WHERE job_id = %ld",
                                OB_ALL_ROUTINE_LOAD_JOB_TNAME, job_id))) {
    LOG_WARN("assign sql failed", KR(ret), K(tenant_id_), K(job_id));
  } else if (need_lock && OB_FAIL(sql.append_fmt(" FOR UPDATE "))) {  /*lock row*/
    LOG_WARN("assign sql failed", KR(ret));
  } else {
    ObString progress;
    ObString lag;
    ObString tmp_progress;
    ObString tmp_lag;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy_->read(res, tenant_id_, sql.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(tenant_id_), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
        LOG_WARN("fail to get next", KR(ret));
      } else {
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "progress", progress);
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "lag", lag);
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "tmp_progress", tmp_progress);
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "tmp_lag", tmp_lag);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ob_write_string(allocator, progress, dynamic_fields.progress_))) {
            LOG_WARN("fail to write progress", KR(ret), K(progress));
          } else if (OB_FAIL(ob_write_string(allocator, lag, dynamic_fields.lag_))) {
            LOG_WARN("fail to write lag", KR(ret), K(lag));
          } else if (OB_FAIL(ob_write_string(allocator, tmp_progress, dynamic_fields.tmp_progress_))) {
            LOG_WARN("fail to write tmp_progress", KR(ret), K(tmp_progress));
          } else if (OB_FAIL(ob_write_string(allocator, tmp_lag, dynamic_fields.tmp_lag_))) {
            LOG_WARN("fail to write tmp_lag", KR(ret), K(tmp_lag));
          }
        }
      }
    }
  }
  return ret;
}

int ObRoutineLoadTableOperator::update_offsets_from_tmp_results(
    const int64_t job_id,
    const ObRLoadDynamicFields &dynamic_fields,
    const ObRLoadDynamicFields &origin_dynamic_fields)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_ID == job_id
                  || dynamic_fields.tmp_progress_.empty()
                  || dynamic_fields.tmp_lag_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id), K(dynamic_fields));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("progress", dynamic_fields.tmp_progress_))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("lag", dynamic_fields.tmp_lag_))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("tmp_progress", ObString()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("tmp_lag", ObString()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (origin_dynamic_fields.progress_.empty() && origin_dynamic_fields.lag_.empty()) {
    if (OB_FAIL(dml.get_extra_condition().assign_fmt("%s IS NULL AND %s IS NULL", "progress", "lag"))) {
      LOG_WARN("add extra_condition failed", KR(ret), K(origin_dynamic_fields));
    }
  } else if (OB_FAIL(dml.get_extra_condition().assign_fmt("%s = '%.*s' AND %s = '%.*s'",
                                  "progress", origin_dynamic_fields.progress_.length(), origin_dynamic_fields.progress_.ptr(),
                                  "lag", origin_dynamic_fields.lag_.length(), origin_dynamic_fields.lag_.ptr()))) {
    LOG_WARN("add extra_condition failed", KR(ret), K(origin_dynamic_fields));
  }
  if (FAILEDx(dml.splice_update_sql(OB_ALL_ROUTINE_LOAD_JOB_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(tenant_id_), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObRoutineLoadTableOperator::update_tmp_results(
    const int64_t job_id,
    const ObString &tmp_progress,
    const ObString &tmp_lag)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_ID == job_id
                  || tmp_progress.empty()
                  || tmp_lag.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id), K(tmp_progress), K(tmp_lag));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("tmp_progress", tmp_progress))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("tmp_lag", tmp_lag))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_ROUTINE_LOAD_JOB_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(tenant_id_), K(sql));
  } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObRoutineLoadTableOperator::reset_tmp_results(const int64_t job_id)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_ID == job_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("tmp_progress", ObString()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("tmp_lag", ObString()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_ROUTINE_LOAD_JOB_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(tenant_id_), K(sql));
  } else if (OB_UNLIKELY(!is_single_row(affected_rows) && !is_zero_row(affected_rows))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObRoutineLoadTableOperator::update_info_when_failed(
    const int64_t job_id,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_ID == job_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
    LOG_WARN("add column failed", KR(ret));
  // } else if (OB_FAIL(dml.add_column("tmp_progress", ObString()))) {
  //   LOG_WARN("add column failed", KR(ret));
  // } else if (OB_FAIL(dml.add_column("tmp_lag", ObString()))) {
  //   LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("last_ret_code", ret_code))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("state", job_state_to_str(ObRoutineLoadJobState::CANCELLED)))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_ROUTINE_LOAD_JOB_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(tenant_id_), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObRoutineLoadTableOperator::update_trace_id(
    const int64_t job_id,
    const common::ObCurTraceId::TraceId &trace_id)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  char trace_id_buf[OB_MAX_TRACE_ID_BUFFER_SIZE] = {'\0'};
  if (OB_UNLIKELY(OB_INVALID_ID == job_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (FALSE_IT(trace_id.to_string(trace_id_buf, sizeof(trace_id_buf)))) {
  } else if (OB_FAIL(dml.add_column("last_trace_id", trace_id_buf))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_ROUTINE_LOAD_JOB_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(tenant_id_), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObRoutineLoadTableOperator::update_status(
    const int64_t job_id,
    const ObRoutineLoadJobState &old_status,
    const ObRoutineLoadJobState &new_status)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_ID == job_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("state", job_state_to_str(new_status)))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.get_extra_condition().assign_fmt("%s = '%s'",
                                  "state", job_state_to_str(old_status)))) {
    LOG_WARN("add extra_condition failed", KR(ret), K(old_status));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_ROUTINE_LOAD_JOB_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(tenant_id_), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObRoutineLoadTableOperator::update_pause_time(
    const int64_t job_id,
    const int64_t pause_time)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_ID == job_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id), K(pause_time));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_INVALID_TIMESTAMP != pause_time
              && OB_FAIL(dml.add_time_column("pause_time", pause_time))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_INVALID_TIMESTAMP == pause_time
              && OB_FAIL(dml.add_column(true, "pause_time"))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_ROUTINE_LOAD_JOB_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(tenant_id_), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObRoutineLoadTableOperator::update_job_name_and_stop_time(
    const int64_t job_id,
    const ObString &job_name,
    const int64_t end_time)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  ObSqlString new_job_name;
  if (OB_UNLIKELY(OB_INVALID_ID == job_id || end_time < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id), K(end_time));
  } else if (OB_FAIL(new_job_name.assign_fmt("%.*s_delete_%ld",
                                              job_name.length(), job_name.ptr(),
                                              end_time))) {
    LOG_WARN("fail to assign new job name", KR(ret), K(job_name), K(end_time));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("job_name", new_job_name.string()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_time_column("end_time", end_time))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_ROUTINE_LOAD_JOB_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(tenant_id_), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObRoutineLoadTableOperator::get_status(
    const ObString &job_name,
    ObRoutineLoadJobState &status)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(job_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_name));
  } else if (OB_FAIL(sql.assign_fmt("SELECT state FROM %s WHERE job_name = '%.*s'",
                                  OB_ALL_ROUTINE_LOAD_JOB_TNAME, job_name.length(), job_name.ptr()))) {
    LOG_WARN("assign sql failed", KR(ret), K(job_name));
  } else {
    ObString state_str;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy_->read(res, tenant_id_, sql.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(tenant_id_), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
        LOG_WARN("fail to get next", KR(ret));
      } else {
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "state", state_str);
        if (OB_SUCC(ret)) {
          status = str_to_job_state(state_str);
        }
      }
    }
  }
  return ret;
}

int ObRoutineLoadTableOperator::get_job_id_by_name(
    const ObString &job_name,
    int64_t &job_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(job_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_name));
  } else if (OB_FAIL(sql.assign_fmt("SELECT job_id FROM %s WHERE job_name = '%.*s'",
                                  OB_ALL_ROUTINE_LOAD_JOB_TNAME, job_name.length(), job_name.ptr()))) {
    LOG_WARN("assign sql failed", KR(ret), K(job_name));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy_->read(res, tenant_id_, sql.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(tenant_id_), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
        LOG_WARN("fail to get next", KR(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "job_id", job_id, int64_t);
      }
    }
  }
  return ret;
}

int ObRoutineLoadTableOperator::update_error_msg(
    const int64_t job_id,
    const ObString &error_msg)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_ID == job_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("last_error_msg", ObHexEscapeSqlStr(error_msg)))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.get_extra_condition().assign_fmt("%s IS NULL OR %s = ''", "last_error_msg", "last_error_msg"))) {
    LOG_WARN("add extra_condition failed", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_ROUTINE_LOAD_JOB_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(tenant_id_), K(sql));
  } else if (!is_single_row(affected_rows) && !is_zero_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

} // storage
} // oceanbase