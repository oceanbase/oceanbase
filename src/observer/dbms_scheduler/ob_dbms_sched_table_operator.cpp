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


#define TO_TS(second) 1000000L * second

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace share::schema;
using namespace sqlclient;

namespace dbms_scheduler
{

int ObDBMSSchedTableOperator::update_for_start(
  uint64_t tenant_id, ObDBMSSchedJobInfo &job_info, bool update_nextdate)
{
  int ret = OB_SUCCESS;

  ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const int64_t now = ObTimeUtility::current_time();
  int64_t delay = 0;
  int64_t dummy_execute_at = 0;

  CK (OB_NOT_NULL(sql_proxy_));
  CK (OB_LIKELY(tenant_id != OB_INVALID_ID));
  CK (OB_LIKELY(job_info.job_ != OB_INVALID_ID));

  OZ (calc_execute_at(
    job_info, (update_nextdate ? job_info.next_date_ : dummy_execute_at), delay, true));

  OX (job_info.this_date_ = now);
  OZ (dml.add_gmt_modified(now));
  OZ (dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
  OZ (dml.add_pk_column("job", job_info.job_));
  OZ (dml.add_time_column("this_date", job_info.this_date_));
  OZ (dml.add_column("state", "SCHEDULED"));
  OZ (dml.splice_update_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql));
  OZ (sql_proxy_->write(tenant_id, sql.ptr(), affected_rows));

  return ret;
}

int ObDBMSSchedTableOperator::update_nextdate(
  uint64_t tenant_id, ObDBMSSchedJobInfo &job_info)
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
  OZ (dml.add_time_column("next_date", job_info.next_date_));
  OZ (dml.splice_update_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql));
  OZ (sql_proxy_->write(tenant_id, sql.ptr(), affected_rows));

  return ret;
}


int ObDBMSSchedTableOperator::update_for_end(
  uint64_t tenant_id, ObDBMSSchedJobInfo &job_info, int err, const ObString &errmsg)
{
  int ret = OB_SUCCESS;

  ObMySQLTransaction trans;
  ObDMLSqlSplicer dml1;
  ObSqlString sql1;
  ObDMLSqlSplicer dml2;
  ObSqlString sql2;
  int64_t affected_rows = 0;
  const int64_t now = ObTimeUtility::current_time();
  int64_t next_date;
  int64_t delay;

  UNUSED(errmsg);

  CK (OB_NOT_NULL(sql_proxy_));
  CK (OB_LIKELY(tenant_id != OB_INVALID_ID));
  CK (OB_LIKELY(job_info.job_ != OB_INVALID_ID));
  // when if failures > 16 then set broken flag.
  OX (job_info.failures_ = errmsg.empty() ? 0 : (job_info.failures_ + 1));
  OX (job_info.flag_ = job_info.failures_ > 15 ? (job_info.flag_ | 0x1) : (job_info.flag_ & 0xfffffffffffffffE));
  if ((now >= job_info.end_date_) && (true == job_info.auto_drop_)) {
    // when end_date is reach and auto_drop is set true, drop job.
    OZ (dml1.add_gmt_modified(now));
    OZ (dml1.add_pk_column("tenant_id",
          ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
    OZ (dml1.add_pk_column("job", job_info.job_));
    OZ (dml1.splice_delete_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql1));
  } else {
    if (OB_SUCC(ret) && ((job_info.flag_ & 0x1) != 0)) {
      // when if failures > 16 then set broken state.
      job_info.next_date_ = 64060560000000000; // 4000-01-01
      OZ (dml1.add_column("state", "BROKEN"));
    } else if (now >= job_info.end_date_) {
      // when end_date is reach and auto_drop is set false, disable set completed state.
      job_info.enabled_ = false;
      OZ (dml1.add_column("state", "COMPLETED"));
      OZ (dml1.add_column("enabled", job_info.enabled_));
    }
    CK (job_info.this_date_ > 0);
    OX (job_info.total_ += (now - job_info.this_date_));
    OZ (dml1.add_gmt_modified(now));
    OZ (dml1.add_pk_column("tenant_id",
          ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
    OZ (dml1.add_pk_column("job", job_info.job_));
    OZ (dml1.add_column(true, "this_date"));
    OZ (dml1.add_time_column("last_date", job_info.this_date_));
    OZ (dml1.add_time_column("next_date", job_info.next_date_));
    OZ (dml1.add_column("failures", job_info.failures_));
    OZ (dml1.add_column("flag", job_info.failures_ > 16 ? 1 : job_info.flag_));
    OZ (dml1.add_column("total", job_info.total_));
    OZ (dml1.splice_update_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql1));
  }
  OZ (dml2.add_gmt_create(now));
  OZ (dml2.add_gmt_modified(now));
  OZ (dml2.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
  OZ (dml2.add_pk_column("job", job_info.job_));
  OZ (dml2.add_time_column("time", now));
  OZ (dml2.add_column("code", err));
  OZ (dml2.add_column(
    "message", ObHexEscapeSqlStr(errmsg.empty() ? ObString("SUCCESS") : errmsg)));
  OZ (dml2.splice_insert_sql(OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_TNAME, sql2));

  OZ (trans.start(sql_proxy_, tenant_id, true));

  OZ (trans.write(tenant_id, sql1.ptr(), affected_rows));
  OZ (trans.write(tenant_id, sql2.ptr(), affected_rows));

  if (trans.is_started()) {
    trans.end(true);
  }

  return ret;
}

int ObDBMSSchedTableOperator::check_job_timeout(ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  if ((!job_info.is_running()) || (job_info.get_max_run_duration() == 0)) {
    //not running or not set timeout
  } else if (ObTimeUtility::current_time() > (job_info.get_this_date() + TO_TS(job_info.get_max_run_duration()))) {
    OZ(update_for_end(job_info.get_tenant_id(), job_info, 0, NULL));
    LOG_WARN("job is timeout, force update for end", K(job_info), K(ObTimeUtility::current_time()));
  } else {
    LOG_DEBUG("job is still running, not timeout", K(job_info));
  }
  return ret;
}

int ObDBMSSchedTableOperator::check_auto_drop(ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  if (job_info.is_running()) {
    // running job not check
  } else if (ObTimeUtility::current_time() > (job_info.end_date_) &&
             (true == job_info.auto_drop_)) {
    OZ(update_for_end(job_info.get_tenant_id(), job_info, 0, NULL));
    LOG_WARN("auto drop miss out job", K(job_info), K(ObTimeUtility::current_time()));
  } else {
    LOG_DEBUG("job no need to drop", K(job_info));
  }
  return ret;
}

int ObDBMSSchedTableOperator::check_job_can_running(int64_t tenant_id, bool &can_running)
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
  OZ (sql.append_fmt("select count(*) from %s where this_date is not null", OB_ALL_TENANT_SCHEDULER_JOB_TNAME));

  CK (OB_NOT_NULL(GCTX.schema_service_));
  OZ (GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard));
  OZ (guard.check_tenant_is_restore(tenant_id, is_restore));

  // job can not run in standy cluster and restore.
  if (OB_SUCC(ret) && job_queue_processor > 0
      && !GCTX.is_standby_cluster()
      && !is_restore) {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(sql_proxy_->read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute query failed", K(ret), K(sql), K(tenant_id));
      } else if (OB_NOT_NULL(result.get_result())) {
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
  EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(result, "last_date", job_info_local.last_date_);
  EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(result, "this_date", job_info_local.this_date_);
  EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(result, "next_date", job_info_local.next_date_);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "total", job_info_local.total_, uint64_t);
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
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "program_name", job_info_local.program_name_);
  EXTRACT_BOOL_FIELD_MYSQL_SKIP_RET(result, "enabled", job_info_local.enabled_);
  EXTRACT_BOOL_FIELD_MYSQL_SKIP_RET(result, "auto_drop", job_info_local.auto_drop_);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "interval_ts", job_info_local.interval_ts_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "max_run_duration", job_info_local.max_run_duration_, uint64_t);

  OZ (job_info.deep_copy(allocator, job_info_local));

  return ret;
}

int ObDBMSSchedTableOperator::get_dbms_sched_job_info(
  uint64_t tenant_id, bool is_oracle_tenant, uint64_t job_id,
  ObIAllocator &allocator, ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  CK (OB_NOT_NULL(sql_proxy_));
  CK (OB_LIKELY(tenant_id != OB_INVALID_ID));
  CK (OB_LIKELY(job_id != OB_INVALID_ID));

  OZ (sql.append_fmt("select * from %s where tenant_id = %lu and job = %ld",
      OB_ALL_TENANT_SCHEDULER_JOB_TNAME, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id), job_id));

  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(sql_proxy_->read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute query failed", K(ret), K(sql), K(tenant_id), K(job_id));
      } else if (OB_NOT_NULL(result.get_result())) {
        if (OB_SUCCESS == (ret = result.get_result()->next())) {
          OZ (extract_info(*(result.get_result()), tenant_id, is_oracle_tenant, allocator, job_info));
          if (OB_SUCC(ret) && (result.get_result()->next()) != OB_ITER_END) {
            LOG_ERROR("got more than one row for dbms sched job!", K(ret), K(tenant_id), K(job_id));
            ret = OB_ERR_UNEXPECTED;
          }
        } else if (OB_ITER_END == ret) {
          LOG_INFO("job not exists, may delete alreay!", K(ret), K(tenant_id), K(job_id));
          ret = OB_SUCCESS; // job not exist, do nothing ...
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

  OZ (sql.append_fmt("select * from %s where job_name != \'%s\' and (state is NULL or state != \'%s\')",
      OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
      "__dummy_guard",
      "COMPLETED"));

  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(sql_proxy_->read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute query failed", K(ret), K(sql), K(tenant_id));
      } else if (OB_NOT_NULL(result.get_result())) {
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

int ObDBMSSchedTableOperator::calc_execute_at(
  ObDBMSSchedJobInfo &job_info, int64_t &execute_at, int64_t &delay, bool ignore_nextdate)
{
  int ret = OB_SUCCESS;

  ObString &interval = job_info.get_interval();

  const int64_t now = ObTimeUtility::current_time();
  int64_t last_sub_next =
    (job_info.get_last_modify() / 1000 / 1000) - (job_info.get_next_date() / 1000/ 1000);
  if (job_info.get_next_date() != 0 && (!ignore_nextdate || job_info.get_next_date() != execute_at)) {
    if (job_info.get_next_date() > now) {
      execute_at = job_info.get_next_date();
      delay = job_info.get_next_date() - now;
    } else if (last_sub_next < 5 && last_sub_next >= -5) {
      execute_at = now;
      delay = 0;
    } else {
      delay = -1;
    }
  } else {
    delay = -1;
  }

  if (delay < 0 && !interval.empty()) {
    ObSqlString sql;
    common::ObISQLClient *sql_proxy = sql_proxy_;
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(sql_proxy_)));
    // NOTE: we need utc timestamp.
    if (lib::is_mysql_mode()) {
      OZ (sql.append_fmt("select utc_timestamp() from dual;"));
    } else {
      OZ (sql.append_fmt(
        "select cast(to_timestamp(sys_extract_utc(to_timestamp(sysdate))) as date) from dual;"));
      sql_proxy = &oracle_proxy;
    }
    
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(sql_proxy->read(result, job_info.get_tenant_id(), sql.ptr()))) {
        LOG_WARN("execute query failed", K(ret), K(sql), K(job_info));
      } else if (OB_NOT_NULL(result.get_result())) {
        if (OB_FAIL(result.get_result()->next())) {
          LOG_WARN("failed to get result", K(ret));
        } else {
          int64_t sysdate = 0;
          int64_t col_idx = 0;
          OZ (result.get_result()->get_datetime(col_idx, sysdate));
          if (OB_SUCC(ret)) {
            execute_at = sysdate + job_info.get_interval_ts();
          }
          LOG_INFO("interval date is", K(sysdate), K(execute_at));
          if (OB_FAIL(ret)) {
          } else if (job_info.get_next_date() > execute_at) {
            execute_at = job_info.get_next_date();
            delay = execute_at - sysdate;
          } else {
            delay = execute_at - sysdate;
          }
          if (OB_SUCC(ret)) {
            OX (job_info.next_date_ = execute_at);
            OZ (update_nextdate(job_info.get_tenant_id(), job_info));
          }
        }
      }
    }
  }

  return ret;
}

} // end for namespace dbms_scheduler
} // end for namespace oceanbase
