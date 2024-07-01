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

#define USING_LOG_PREFIX RS

#include "ob_dbms_sched_table_operator.h"

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "lib/worker.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_schema_utils.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/ob_server_struct.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "share/schema/ob_multi_version_schema_service.h"


namespace oceanbase
{

using namespace common;
using namespace share;
using namespace share::schema;
using namespace sqlclient;
using namespace storage;

namespace dbms_scheduler
{

int ObDBMSSchedTableOperator::update_next_date(
  uint64_t tenant_id, ObDBMSSchedJobInfo &job_info, int64_t next_date)
{
  int ret = OB_SUCCESS;

  ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const int64_t now = ObTimeUtility::current_time();

  CK (OB_NOT_NULL(sql_proxy_));
  CK (OB_LIKELY(tenant_id != OB_INVALID_ID));
  CK (OB_LIKELY(job_info.job_ != OB_INVALID_ID));

  OZ (dml.add_gmt_modified(now));
  OZ (dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
  OZ (dml.add_pk_column("job", job_info.job_));
  OZ (dml.add_pk_column("job_name", job_info.job_name_));
  OZ (dml.add_time_column("next_date", next_date));
  OZ (dml.splice_update_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql));
  OZ (sql_proxy_->write(tenant_id, sql.ptr(), affected_rows));
  return ret;
}


int ObDBMSSchedTableOperator::update_for_start(
  uint64_t tenant_id, ObDBMSSchedJobInfo &job_info, int64_t next_date)
{
  int ret = OB_SUCCESS;

  ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const int64_t now = ObTimeUtility::current_time();

  CK (OB_NOT_NULL(sql_proxy_));
  CK (OB_LIKELY(tenant_id != OB_INVALID_ID));
  CK (OB_LIKELY(job_info.job_ != OB_INVALID_ID));

  OX (job_info.this_date_ = now);
  OZ (dml.add_gmt_modified(now));
  OZ (dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
  OZ (dml.add_pk_column("job", job_info.job_));
  OZ (dml.add_pk_column("job_name", job_info.job_name_));
  OZ (dml.add_time_column("this_date", job_info.this_date_));
  OZ (dml.add_time_column("next_date", next_date));
  OZ (dml.add_column("state", "SCHEDULED"));
  OZ (dml.splice_update_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql));
  OZ (sql.append_fmt(" and this_date is null"));
  OZ (sql_proxy_->write(tenant_id, sql.ptr(), affected_rows));
  CK (affected_rows == 1);
  return ret;
}

int ObDBMSSchedTableOperator::_build_job_drop_dml(int64_t now, ObDBMSSchedJobInfo &job_info, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t tenant_id = job_info.tenant_id_;
  OZ (dml.add_gmt_modified(now));
  OZ (dml.add_pk_column("tenant_id",
        ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
  OZ (dml.add_pk_column("job_name", job_info.job_name_));
  OZ (dml.splice_delete_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql));
  return ret;
}

int ObDBMSSchedTableOperator::_build_job_finished_dml(int64_t now, ObDBMSSchedJobInfo &job_info, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t tenant_id = job_info.tenant_id_;
  OZ (dml.add_gmt_modified(now));
  OZ (dml.add_pk_column("tenant_id",
        ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
  OZ (dml.add_pk_column("job", job_info.job_));
  OZ (dml.add_pk_column("job_name", job_info.job_name_));
  OZ (dml.add_column("state", job_info.state_));
  if (0 == job_info.state_.case_compare("COMPLETED")) {
    OZ (dml.add_column("enabled", false));
  } else if (0 == job_info.state_.case_compare("BROKEN")) {
    OZ (dml.add_time_column("next_date", 64060560000000000));
  }
  OZ (dml.add_column(true, "this_date"));
  OZ (dml.add_time_column("last_date", job_info.this_date_));
  OZ (dml.add_column("failures", job_info.failures_));
  OZ (dml.add_column("flag", job_info.flag_));
  OZ (dml.add_column("total", job_info.total_));
  OZ (dml.get_extra_condition().assign_fmt("state!='BROKEN' AND (last_date is null OR last_date<=usec_to_time(%ld))", job_info.last_date_));
  OZ (dml.splice_update_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql));
  return ret;
}


int ObDBMSSchedTableOperator::_build_job_rollback_start_dml(ObDBMSSchedJobInfo &job_info, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t tenant_id = job_info.tenant_id_;
  OZ (dml.add_pk_column("tenant_id",
        ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
  OZ (dml.add_pk_column("job", job_info.job_));
  OZ (dml.add_pk_column("job_name", job_info.job_name_));
  OZ (dml.add_column(true, "this_date"));
  OZ (dml.add_time_column("next_date", job_info.next_date_));// roll back to old next date
  OZ (dml.splice_update_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql));
  return ret;
}

int ObDBMSSchedTableOperator::_build_job_log_dml(
  int64_t now, ObDBMSSchedJobInfo &job_info, int err, const ObString &errmsg, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t tenant_id = job_info.tenant_id_;
  uint64_t data_version = 0;
  OZ (dml.add_gmt_create(now));
  OZ (dml.add_gmt_modified(now));
  OZ (dml.add_pk_column("job", job_info.get_job_id()));
  OZ (dml.add_time_column("time", now));
  OZ (dml.add_column("code", err));
  OZ (dml.add_column("message", ObHexEscapeSqlStr(errmsg.empty() ? ObString("SUCCESS") : errmsg)));
  OZ (dml.add_column("job_class", job_info.job_class_));
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (DATA_VERSION_SUPPORT_RUN_DETAIL_V2(data_version)) {
    OZ (dml.add_pk_column("job_name", job_info.job_name_));
    OZ (dml.splice_insert_sql(OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_TNAME, sql));
  } else {
    OZ (dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
    OZ (dml.splice_insert_sql(OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_TNAME, sql));
  }
  return ret;
}

int ObDBMSSchedTableOperator::_check_need_record(ObDBMSSchedJobInfo &job_info, bool &need_record, bool err_state)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  int64_t tenant_id = job_info.tenant_id_;
  need_record = true;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (DATA_VERSION_SUPPORT_JOB_CLASS(data_version)) {
    ObDBMSSchedJobClassInfo job_class_info;
    ObArenaAllocator allocator("DBMSSchedTmp");
    CK (OB_LIKELY(!job_info.job_class_.empty()));
    if (OB_SUCC(ret)) {
      if (0 == job_info.job_class_.case_compare("DEFAULT_JOB_CLASS")) { // DEFAULT_JOB_CLASS need record unconditionally
        need_record = true;
      } else {
        OZ (get_dbms_sched_job_class_info(tenant_id, job_info.is_oracle_tenant(), job_info.get_job_class(), allocator, job_class_info));
        if (OB_SUCC(ret)) {
          ObString logging_level = job_class_info.get_logging_level();
          if (logging_level.empty()) {
            LOG_WARN("logging_level may not assigned");
          } else if (0 == logging_level.case_compare("OFF")) {
            need_record = false;
          } else if (0 == logging_level.case_compare("RUNS")) {
            need_record = true;
          } else if (0 == logging_level.case_compare("FAILED RUNS") && !err_state) {
            need_record = false;
          }
        }

      }
    }
  }
  return ret;
}

int ObDBMSSchedTableOperator::update_for_missed(ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const int64_t now = ObTimeUtility::current_time();
  bool need_record = true;
  int64_t tenant_id = job_info.tenant_id_;
  CK (OB_NOT_NULL(sql_proxy_));
  CK (OB_LIKELY(tenant_id != OB_INVALID_ID));
  CK (OB_LIKELY(job_info.job_ != OB_INVALID_ID));
  OZ (_check_need_record(job_info, need_record));

  if (OB_SUCC(ret) && need_record) {
    OZ (_build_job_log_dml(now, job_info, 0, "check job missed", sql));
    OZ (trans.start(sql_proxy_, tenant_id, true));
    OZ (trans.write(tenant_id, sql.ptr(), affected_rows));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObDBMSSchedTableOperator::update_for_rollback(ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObSqlString sql1;
  ObSqlString sql2;
  int64_t affected_rows = 0;
  const int64_t now = ObTimeUtility::current_time();
  bool need_record = true;
  int64_t tenant_id = job_info.tenant_id_;
  CK (OB_NOT_NULL(sql_proxy_));
  CK (OB_LIKELY(tenant_id != OB_INVALID_ID));
  CK (OB_LIKELY(job_info.job_ != OB_INVALID_ID));
  OZ (_check_need_record(job_info, need_record));

  OZ (_build_job_rollback_start_dml(job_info, sql1));
  if (OB_SUCC(ret) && need_record) {
    OZ (_build_job_log_dml(now, job_info, 0, "send job rpc failed", sql2));
  }

  OZ (trans.start(sql_proxy_, job_info.tenant_id_, true));
  OZ (trans.write(job_info.tenant_id_, sql1.ptr(), affected_rows));
  if (OB_SUCC(ret) && need_record) {
    OZ (trans.write(job_info.tenant_id_, sql2.ptr(), affected_rows));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  return ret;
}

int ObDBMSSchedTableOperator::update_for_enddate(ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  OZ (update_for_end(job_info, 0, "check job enddate"));
  return ret;
}

int ObDBMSSchedTableOperator::update_for_timeout(ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  OZ (update_for_end(job_info, -4012, "check job timeout"));
  return ret;
}

int ObDBMSSchedTableOperator::update_for_end(ObDBMSSchedJobInfo &job_info, int err, const ObString &errmsg)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObSqlString sql1;
  ObSqlString sql2;
  int64_t affected_rows = 0;
  const int64_t now = ObTimeUtility::current_time();
  bool need_record = true;
  int64_t tenant_id = job_info.tenant_id_;
  CK (OB_NOT_NULL(sql_proxy_));
  CK (OB_LIKELY(tenant_id != OB_INVALID_ID));
  CK (OB_LIKELY(job_info.job_ != OB_INVALID_ID));
  OZ (_check_need_record(job_info, need_record, err == 0 ? false : true));

  if (OB_FAIL(ret)) {
  } else if (job_info.is_date_expression_job_class() && now >= job_info.end_date_ && true == job_info.auto_drop_) {
    OZ (_build_job_drop_dml(now, job_info, sql1));
  } else if ((now >= job_info.end_date_ || job_info.get_interval_ts() == 0) && (true == job_info.auto_drop_)) {
    OZ (_build_job_drop_dml(now, job_info, sql1));
  } else {
    OX (job_info.failures_ = (err == 0) ? 0 : (job_info.failures_ + 1));
    OX (job_info.flag_ = job_info.failures_ > 15 ? (job_info.flag_ | 0x1) : (job_info.flag_ & 0xfffffffffffffffE));
    OX (job_info.total_ += (job_info.this_date_ > 0 ? now - job_info.this_date_ : 0));
    if (OB_SUCC(ret) && ((job_info.flag_ & 0x1) != 0)) {
      // when if failures > 16 then set broken state.
      job_info.state_ = ObString("BROKEN");
    } else if (job_info.is_date_expression_job_class()) {
      if (now >= job_info.end_date_) {
        job_info.state_ = ObString("COMPLETED");
      }
    } else if (now >= job_info.end_date_ || job_info.get_interval_ts() == 0) {
      // when end_date is reach and auto_drop is set false, disable set completed state.
      // for once job, not wait until end date, set completed state when running end
      job_info.state_ = ObString("COMPLETED");
    }
    OZ (_build_job_finished_dml(now, job_info, sql1));
  }

  if (OB_SUCC(ret) && need_record) {
    OZ (_build_job_log_dml(now, job_info, err, errmsg, sql2));
  }

  OZ (trans.start(sql_proxy_, tenant_id, true));
  OZ (trans.write(tenant_id, sql1.ptr(), affected_rows));
  if (OB_SUCC(ret) && need_record) {
    OZ (trans.write(tenant_id, sql2.ptr(), affected_rows));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObDBMSSchedTableOperator::check_job_can_running(int64_t tenant_id, int64_t alive_job_count, bool &can_running)
{
  int ret = OB_SUCCESS;
  uint64_t job_queue_processor = 0;
  uint64_t job_running_cnt = 0;
  ObSqlString sql;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  share::schema::ObSchemaGetterGuard guard;
  bool is_restore = false;
  OX (can_running = false);
  CK (tenant_config.is_valid());
  OX (job_queue_processor = tenant_config->job_queue_processes);
  // found current running job count
  if (OB_FAIL(ret)) {
  } else if (alive_job_count <= job_queue_processor) {
    can_running = true;
  } else {
    OZ (sql.append_fmt("select count(*) from %s where this_date is not null", OB_ALL_TENANT_SCHEDULER_JOB_TNAME));

    CK (OB_NOT_NULL(GCTX.schema_service_));
    OZ (GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard));
    OZ (guard.check_tenant_is_restore(tenant_id, is_restore));

    // job can not run in standy cluster and restore.
    if (OB_SUCC(ret) && job_queue_processor > 0
        && !is_restore) {
      SMART_VAR(ObMySQLProxy::MySQLResult, result) {
        if (OB_FAIL(sql_proxy_->read(result, tenant_id, sql.ptr()))) {
          LOG_WARN("execute query failed", K(ret), K(sql), K(tenant_id));
        } else if (OB_ISNULL(result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get result failed", K(ret), K(sql), K(tenant_id));
        } else {
          if (OB_SUCCESS == (ret = result.get_result()->next())) {
            int64_t int_value = 0;
            if (OB_FAIL(result.get_result()->get_int(static_cast<const int64_t>(0), int_value))) {
              LOG_WARN("failed to get column in row. ", K(ret));
            } else {
              job_running_cnt = static_cast<uint64_t>(int_value);
            }
          } else {
            LOG_WARN("failed to calc all running job, no row return", K(ret));
          }
        }
      }
      OX (can_running = (job_queue_processor > job_running_cnt));
    }
  }
  return ret;
}

int ObDBMSSchedTableOperator::extract_info(
  sqlclient::ObMySQLResult &result, int64_t tenant_id, bool is_oracle_tenant,
  ObIAllocator &allocator, ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  ObDBMSSchedJobInfo job_info_local;

  job_info_local.tenant_id_ = tenant_id;
  job_info_local.is_oracle_tenant_ = is_oracle_tenant;
  EXTRACT_INT_FIELD_MYSQL(result, "job", job_info_local.job_, uint64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "lowner", job_info_local.lowner_);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "powner", job_info_local.powner_);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "cowner", job_info_local.cowner_);

#define EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(result, col_name, v)   \
do {                                                                  \
  ObObj obj;                                                          \
  OZ ((result).get_obj(col_name, obj));                               \
  if (OB_SUCC(ret)) {                                                 \
    if (obj.is_null()) {                                              \
      v = static_cast<int64_t>(0);                                    \
    } else {                                                          \
      OZ (obj.get_timestamp(v));                                      \
    }                                                                 \
  } else if (OB_ERR_COLUMN_NOT_FOUND == ret) {                        \
    ret = OB_SUCCESS;                                                 \
    v = static_cast<int64_t>(0);                                      \
  }                                                                   \
} while (false)

#define EXTRACT_NUMBER_FIELD_MYSQL_SKIP_RET(result, col_name, v)      \
do {                                                                  \
  common::number::ObNumber nmb_val;                                   \
  OZ ((result).get_number(col_name, nmb_val));                        \
  if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) {   \
    ret = OB_SUCCESS;                                                 \
    v = static_cast<int64_t>(0);                                     \
  } else if (OB_SUCCESS == ret) {                                     \
    OZ (nmb_val.extract_valid_int64_with_trunc(v));                  \
  }                                                                   \
} while (false)

  EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(result, "gmt_modified", job_info_local.last_modify_);
  //lowner not used
  //powner not used
  //cowner not used
  //last_modify not used
  EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(result, "last_date", job_info_local.last_date_);
  EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(result, "this_date", job_info_local.this_date_);
  EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(result, "next_date", job_info_local.next_date_);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "total", job_info_local.total_, uint64_t);
  EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(result, "start_date", job_info_local.start_date_);
  EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(result, "end_date", job_info_local.end_date_);

#undef EXTRACT_NUMBER_FIELD_MYSQL_SKIP_RET
#undef EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET

  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "interval#", job_info_local.interval_);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "failures", job_info_local.failures_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "flag", job_info_local.flag_, uint64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "what", job_info_local.what_);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "nlsenv", job_info_local.nlsenv_);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "charenv", job_info_local.charenv_);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "field1", job_info_local.field1_);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "scheduler_flags", job_info_local.scheduler_flags_, uint64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "exec_env", job_info_local.exec_env_);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "job_name", job_info_local.job_name_);
  //job_style not used
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "job_class", job_info_local.job_class_);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "program_name", job_info_local.program_name_);
  //job_type not used
  //job_action not used
  //number_of_argument not used
  //repeat_interval not used
  EXTRACT_BOOL_FIELD_MYSQL_SKIP_RET(result, "enabled", job_info_local.enabled_);
  EXTRACT_BOOL_FIELD_MYSQL_SKIP_RET(result, "auto_drop", job_info_local.auto_drop_);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "state", job_info_local.state_);
  //run_count not used
  //retry_count not used
  //last_run_duration not used
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "interval_ts", job_info_local.interval_ts_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "max_run_duration", job_info_local.max_run_duration_, uint64_t);
  //comments not used
  //credential_name not used
  //destination_name not used

  OZ (job_info.deep_copy(allocator, job_info_local));

  return ret;
}

int ObDBMSSchedTableOperator::get_dbms_sched_job_info(
  uint64_t tenant_id, bool is_oracle_tenant, uint64_t job_id, const common::ObString &job_name,
  ObIAllocator &allocator, ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  CK (OB_NOT_NULL(sql_proxy_));
  CK (OB_LIKELY(tenant_id != OB_INVALID_ID));
  CK (OB_LIKELY(job_id != OB_INVALID_ID));

  if (!job_name.empty()) {
    OZ (sql.append_fmt("select * from %s where tenant_id = %lu and job_name = \'%.*s\' and job = %ld",
        OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
        ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
        job_name.length(),
        job_name.ptr(),
        job_id));
  } else {
    OZ (sql.append_fmt("select * from %s where tenant_id = %lu and job = %ld",
        OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
        ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
        job_id));
  }


  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(sql_proxy_->read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute query failed", K(ret), K(sql), K(tenant_id), K(job_id));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get result", K(ret), K(tenant_id), K(job_id));
      } else {
        if (OB_SUCCESS == (ret = result.get_result()->next())) {
          OZ (extract_info(*(result.get_result()), tenant_id, is_oracle_tenant, allocator, job_info));
          if (OB_SUCC(ret)) {
            int tmp_ret = result.get_result()->next();
            if (OB_SUCCESS == tmp_ret) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("got more than one row for dbms sched job!", K(ret), K(tenant_id), K(job_id));
            } else if (tmp_ret != OB_ITER_END) {
              ret = tmp_ret;
              LOG_ERROR("got next row for dbms sched job failed", K(ret), K(tenant_id), K(job_id));
            }
          }
        } else if (OB_ITER_END == ret) {
          LOG_WARN("job not exists, may delete alreay!", K(ret), K(tenant_id), K(job_id));
        } else {
          LOG_WARN("failed to get next", K(ret), K(tenant_id), K(job_id));
        }
      }
    }
  }
  return ret;
}

int ObDBMSSchedTableOperator::get_dbms_sched_job_infos_in_tenant(
  uint64_t tenant_id, bool is_oracle_tenant, ObIAllocator &allocator, ObIArray<ObDBMSSchedJobInfo> &job_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  CK (OB_NOT_NULL(sql_proxy_));
  CK (OB_LIKELY(tenant_id != OB_INVALID_ID));

  OZ (sql.append_fmt("select * from %s where job > 0 and job_name != \'%s\' and (state is NULL or state != \'%s\')",
      OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
      "__dummy_guard",
      "COMPLETED"));

  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(sql_proxy_->read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute query failed", K(ret), K(sql), K(tenant_id));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get result failed", K(ret), K(sql), K(tenant_id));
      } else {
        do {
          if (OB_FAIL(result.get_result()->next())) {
            LOG_INFO("failed to get result", K(ret));
          } else {
            ObDBMSSchedJobInfo job_info;
            OZ (extract_info(*(result.get_result()), tenant_id, is_oracle_tenant, allocator, job_info));
            OZ (job_infos.push_back(job_info));
          }
        } while (OB_SUCC(ret));
        ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
      }
    }
  }

  return ret;
}

int ObDBMSSchedTableOperator::extract_job_class_info(
  sqlclient::ObMySQLResult &result, int64_t tenant_id, bool is_oracle_tenant,
  ObIAllocator &allocator, ObDBMSSchedJobClassInfo &job_class_info)
{
  int ret = OB_SUCCESS;
  ObDBMSSchedJobClassInfo job_class_info_local;

  job_class_info_local.tenant_id_ = tenant_id;
  job_class_info_local.is_oracle_tenant_ = is_oracle_tenant;
  // EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "log_history", job_class_info_local.log_history_, uint64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "job_class_name", job_class_info_local.job_class_name_);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "resource_consumer_group", job_class_info_local.resource_consumer_group_);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "service", job_class_info_local.service_);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "logging_level", job_class_info_local.logging_level_);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "comments", job_class_info_local.comments_);

  OZ (job_class_info.deep_copy(allocator, job_class_info_local));

  return ret;
}

int ObDBMSSchedTableOperator::get_dbms_sched_job_class_info(
  uint64_t tenant_id, bool is_oracle_tenant, const common::ObString job_class_name,
  common::ObIAllocator &allocator, ObDBMSSchedJobClassInfo &job_class_info) {
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  CK (OB_NOT_NULL(sql_proxy_));
  CK (OB_LIKELY(tenant_id != OB_INVALID_ID));
  CK (OB_LIKELY(!job_class_name.empty()));
  OZ (sql.append_fmt("select * from %s where tenant_id = %lu and job_class_name = \'%.*s\'",
      OB_ALL_TENANT_SCHEDULER_JOB_CLASS_TNAME, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id), job_class_name.length(), job_class_name.ptr()));
  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(sql_proxy_->read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute query failed", K(ret), K(sql), K(tenant_id), K(job_class_name));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get result failed", K(ret), K(sql), K(tenant_id), K(job_class_name));
      } else {
        if (OB_SUCCESS == (ret = result.get_result()->next())) {
          OZ (extract_job_class_info(*(result.get_result()), tenant_id, is_oracle_tenant, allocator, job_class_info));
          if (OB_SUCC(ret)) {
            int tmp_ret = result.get_result()->next();
            if (OB_SUCCESS == tmp_ret) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("got more than one row for dbms sched job class!", K(ret), K(tenant_id), K(job_class_name));
            } else if (tmp_ret != OB_ITER_END) {
              ret = tmp_ret;
              LOG_ERROR("got next row for dbms sched job class failed", K(ret), K(tenant_id), K(job_class_name));
            }
          }
        } else if (OB_ITER_END == ret) {
          LOG_INFO("job_class_name not exists, may delete alreay!", K(ret), K(tenant_id), K(job_class_name));
          ret = OB_SUCCESS; // job not exist, do nothing ...
        } else {
          LOG_WARN("failed to get next", K(ret), K(tenant_id), K(job_class_name));
        }
      }
    }
  }
  return ret;
}

int ObDBMSSchedTableOperator::purge_run_detail_histroy(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t DAY_INTERVAL_USEC = 24 * 60 * 60 * 1000000LL;
  const int64_t now = ObTimeUtility::current_time();

  ObMySQLTransaction trans;
  ObSqlString sql;
  ObSqlString sql0;
  bool need_purge_v2_table = false;
  int64_t affected_rows = 0;
  uint64_t data_version = 0;

  CK (OB_NOT_NULL(sql_proxy_));
  CK (OB_LIKELY(tenant_id != OB_INVALID_ID));

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (DATA_VERSION_SUPPORT_RUN_DETAIL_V2(data_version)) {
    OZ (sql0.append_fmt("delete from %s where time < usec_to_time(%ld - "
        "NVL((select log_history from %s where %s.job_class = %s.job_class_name),30) * %ld)",
        OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_TNAME,
        now,
        OB_ALL_TENANT_SCHEDULER_JOB_CLASS_TNAME,
        OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_TNAME,
        OB_ALL_TENANT_SCHEDULER_JOB_CLASS_TNAME,
        DAY_INTERVAL_USEC
        ));
    OX (need_purge_v2_table = true);
  }

  OZ (sql.append_fmt("delete from %s where tenant_id = %ld and time < usec_to_time(%ld - "
      "NVL((select log_history from %s where %s.job_class = %s.job_class_name),30) * %ld)", // log_history is 30 days by default which is consistend with oracle
      OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_TNAME,
      share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
      now,
      OB_ALL_TENANT_SCHEDULER_JOB_CLASS_TNAME,
      OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_TNAME,
      OB_ALL_TENANT_SCHEDULER_JOB_CLASS_TNAME,
      DAY_INTERVAL_USEC
      ));
  OZ (trans.start(sql_proxy_, tenant_id, true));
  OZ (trans.write(tenant_id, sql.ptr(), affected_rows));
  if (need_purge_v2_table) {
    OZ (trans.write(tenant_id, sql0.ptr(), affected_rows));
  }
  int tmp_ret = OB_SUCCESS;
  if (trans.is_started()) {
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  LOG_INFO("purge run detail history", K(ret), K(tenant_id), K(sql.ptr()));
  if (need_purge_v2_table) {
    LOG_INFO("purge run detail v2 history", K(ret), K(tenant_id), K(sql.ptr()));
  }
  return ret;
}

} // end for namespace dbms_scheduler
} // end for namespace oceanbase
