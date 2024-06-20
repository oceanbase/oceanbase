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

#define USING_LOG_PREFIX PL

#include "share/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "observer/ob_inner_sql_connection.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_schema_utils.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "observer/dbms_scheduler/ob_dbms_sched_table_operator.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_executor.h"
#include "ob_dbms_scheduler_mysql.h"
#include "share/stat/ob_dbms_stats_maintenance_window.h"
#include "storage/ob_common_id_utils.h"
namespace oceanbase
{

using namespace common;
using namespace share;
using namespace observer;
using namespace sqlclient;
using namespace dbms_scheduler;
namespace pl
{

int ObDBMSSchedulerMysql::execute_sql(sql::ObExecContext &ctx, ObSqlString &sql, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnectionPool *pool = NULL;
  ObInnerSQLConnection *conn = NULL;
  sql::ObSQLSessionInfo *session = NULL;
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_NOT_NULL(session = ctx.get_my_session()));

  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  CK (OB_NOT_NULL(
    pool = static_cast<ObInnerSQLConnectionPool *>(ctx.get_sql_proxy()->get_pool())));
  OZ (pool->acquire_spi_conn(session, conn));
  OZ (conn->execute_write(session->get_effective_tenant_id(), sql.ptr(), affected_rows));
  if (OB_NOT_NULL(conn)) {
    ctx.get_sql_proxy()->close(conn, ret);
  }
  return ret;
}

int ObDBMSSchedulerMysql::create_job(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  int64_t affected_rows = 0;
  int64_t job_id = OB_INVALID_ID;
  int64_t tenant_id = ctx.get_my_session()->get_effective_tenant_id();

  UNUSED(result);
  if (0 == (params.at(10).get_string()).case_compare("__dummy_guard")) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("forbidden to create a job named '__dummy_guard'", K(ret), K(params.at(10).get_string()));
  } else {
    ObSqlString sql;
    ObSqlString sql0;
    CK (OB_LIKELY(28 == params.count()));
    CK (OB_NOT_NULL(ctx.get_my_session()));
    CK (OB_NOT_NULL(ctx.get_sql_proxy()));
    OZ (_generate_job_id(tenant_id, job_id));
    OZ (_splice_insert_sql(ctx, params, tenant_id, job_id, sql));
    OZ (_splice_insert_sql(ctx, params, tenant_id, 0, sql0));
    OZ (trans.start(ctx.get_sql_proxy(), tenant_id, true));
    OZ (trans.write(tenant_id, sql.ptr(), affected_rows));
    OZ (trans.write(tenant_id, sql0.ptr(), affected_rows));
    CK (OB_LIKELY(1 == affected_rows));
    if (trans.is_started()) {
      trans.end(OB_SUCCESS == ret);
    }
    LOG_INFO("create job", K(ret), K(tenant_id), K(job_id), K(ObHexEscapeSqlStr(params.at(10).get_string())), K(tenant_id));
  }
  return ret;
}

int ObDBMSSchedulerMysql::disable(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t tenant_id = OB_INVALID_ID;
  const int64_t now = ObTimeUtility::current_time();
  CK (OB_LIKELY(3 == params.count()));
  OZ (dml.add_gmt_modified(now));
  OX (tenant_id = ctx.get_my_session()->get_effective_tenant_id());
  OZ (dml.add_pk_column("tenant_id",
    share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
  OZ (dml.add_pk_column("job_name", ObHexEscapeSqlStr(params.at(0).get_string())));
  OZ (dml.add_column("enabled", false));
  OZ (dml.splice_update_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql));
  OZ (execute_sql(ctx, sql, affected_rows));
  CK (OB_LIKELY(1 == affected_rows || 2 == affected_rows));
  return ret;
}

int ObDBMSSchedulerMysql::enable(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObString zone;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t tenant_id = OB_INVALID_ID;
  const int64_t now = ObTimeUtility::current_time();
  CK (OB_LIKELY(1 == params.count()));
  OZ (dml.add_gmt_modified(now));
  OX (tenant_id = ctx.get_my_session()->get_effective_tenant_id());
  OZ (dml.add_pk_column("tenant_id",
    share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
  OZ (dml.add_pk_column("job_name", ObHexEscapeSqlStr(params.at(0).get_string())));
  OZ (dml.add_column("enabled", true));
  OZ (dml.splice_update_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql));
  OZ (execute_sql(ctx, sql, affected_rows));
  CK (OB_LIKELY(1 == affected_rows || 2 == affected_rows));
  return ret;
}

int ObDBMSSchedulerMysql::set_attribute(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObString attr_name;
  ObString attr_val;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t tenant_id = OB_INVALID_ID;
  bool is_stat_window_attr = false;
  const int64_t now = ObTimeUtility::current_time();
  CK (OB_LIKELY(3 == params.count()));
  OZ (dml.add_gmt_modified(now));
  OX (tenant_id = ctx.get_my_session()->get_effective_tenant_id());
  OZ (dml.add_pk_column("tenant_id",
    share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
  OZ (dml.add_pk_column("job_name", ObHexEscapeSqlStr(params.at(0).get_string())));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDbmsStatsMaintenanceWindow::is_stats_maintenance_window_attr(
                                                                          ctx.get_my_session(),
                                                                          params.at(0).get_string(),
                                                                          params.at(1).get_string(),
                                                                          params.at(2).get_string(),
                                                                          is_stat_window_attr,
                                                                          dml))) {
      LOG_WARN("failed to is stats maintenance window attr", K(ret), K(params.at(0).get_string()),
                                        K(params.at(1).get_string()), K(params.at(2).get_string()));
    } else if (is_stat_window_attr) {
      OZ (dml.splice_update_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql));
      OZ (execute_sql(ctx, sql, affected_rows));
      CK (1 == affected_rows || 2 == affected_rows);
    } else {
      OZ (params.at(1).get_varchar(attr_name));
      OZ (params.at(2).get_varchar(attr_val));
      if (attr_name.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "attr_name empty");
        LOG_WARN("attr_name empty", K(ret), K(params.at(0).get_string()),
                                                      K(params.at(1).get_string()),
                                                      K(params.at(2).get_string()));
      } else if (attr_val.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "attr_val empty");
        LOG_WARN("attr_val empty", K(ret), K(params.at(0).get_string()),
                                                      K(params.at(1).get_string()),
                                                      K(params.at(2).get_string()));
      } else if (0 == attr_name.case_compare("max_run_duration")) { // set max run duration
        OZ (dml.add_column("max_run_duration", atoi(attr_val.ptr())));
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "the job set attribute");
        LOG_WARN("not support the job set attribute", K(ret), K(params.at(0).get_string()),
                                                      K(params.at(1).get_string()),
                                                      K(params.at(2).get_string()));
      }
    }
  }
  return ret;
}

int ObDBMSSchedulerMysql::get_and_increase_job_id(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  UNUSED(params);
  int ret = OB_SUCCESS;
  int64_t tenant_id = ctx.get_my_session()->get_effective_tenant_id();
  int64_t job_id = 0;
  OZ (_generate_job_id(tenant_id, job_id));
  OX (result.set_int(job_id));
  LOG_INFO("get and increase job id", K(ret), K(job_id));
  return ret;
}

int ObDBMSSchedulerMysql::_generate_job_id(int64_t tenant_id, int64_t &max_job_id)
{
  int ret = OB_SUCCESS;
  ObCommonID raw_id;
  if (OB_FAIL(storage::ObCommonIDUtils::gen_unique_id(tenant_id, raw_id))) {
    LOG_WARN("gen unique id failed", K(ret), K(tenant_id));
  } else {
    max_job_id = raw_id.id() + ObDBMSSchedTableOperator::JOB_ID_OFFSET;
  }
  return ret;
}

int ObDBMSSchedulerMysql::_splice_insert_sql(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    int64_t tenant_id,
    int64_t job_id,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  char buf[OB_MAX_PROC_ENV_LENGTH];
  const int64_t now = ObTimeUtility::current_time();
  int64_t pos = 0;

  OZ (dml.add_gmt_create(now));
  OZ (dml.add_gmt_modified(now));
  OZ (dml.add_pk_column("tenant_id",
    share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
  OZ (dml.add_column("lowner", ObHexEscapeSqlStr(params.at(1).get_string())));
  OZ (dml.add_column("powner", ObHexEscapeSqlStr(params.at(2).get_string())));
  OZ (dml.add_column("cowner", ObHexEscapeSqlStr(params.at(3).get_string())));
  OZ (dml.add_raw_time_column("next_date", params.at(4).get_int()));
  OZ (dml.add_column("total", 0));
  OZ (dml.add_column("`interval#`",
    ObHexEscapeSqlStr(params.at(5).is_null() ? ObString("null") : params.at(5).get_string())));
  OZ (dml.add_column("flag", params.at(6).get_int32()));
  OZ (dml.add_column("what", ObHexEscapeSqlStr(params.at(7).get_string())));
  OZ (dml.add_column("nlsenv", ObHexEscapeSqlStr(params.at(8).get_string())));
  OZ (sql::ObExecEnv::gen_exec_env(*(ctx.get_my_session()), buf, OB_MAX_PROC_ENV_LENGTH, pos));
  OZ (dml.add_column("exec_env", ObHexEscapeSqlStr(ObString(pos, buf))));
  OZ (dml.add_column("job_name", ObHexEscapeSqlStr(params.at(10).get_string())));
  OZ (dml.add_pk_column("job", job_id));
  OZ (dml.add_column("job_type", ObHexEscapeSqlStr(params.at(11).get_string())));
  OZ (dml.add_column("job_action", ObHexEscapeSqlStr(params.at(12).get_string())));
  OZ (dml.add_column("number_of_argument", params.at(13).get_int()));
  OZ (dml.add_raw_time_column("start_date", params.at(14).get_int()));
  OZ (dml.add_column("repeat_interval", ObHexEscapeSqlStr(params.at(15).get_string())));
  OZ (dml.add_raw_time_column("end_date", params.at(16).get_int()));
  OZ (dml.add_column("job_class", ObHexEscapeSqlStr(params.at(17).get_string())));
  OZ (dml.add_column("enabled", params.at(18).get_bool()));
  OZ (dml.add_column("auto_drop", params.at(19).get_bool()));
  OZ (dml.add_column("comments", ObHexEscapeSqlStr(params.at(20).get_string())));
  OZ (dml.add_column("credential_name", ObHexEscapeSqlStr(params.at(21).get_string())));
  OZ (dml.add_column("destination_name", ObHexEscapeSqlStr(params.at(22).get_string())));
  OZ (dml.add_column("field1", ObHexEscapeSqlStr(params.at(23).get_string())));
  OZ (dml.add_column("program_name", ObHexEscapeSqlStr(params.at(24).get_string())));
  OZ (dml.add_column("job_style", ObHexEscapeSqlStr(params.at(25).get_string())));
  OZ (dml.add_column("interval_ts", params.at(26).get_int() * 1000000));
  OZ (dml.add_column("max_run_duration", params.at(27).get_int()));
  if (0 == params.at(17).get_string().compare("DATE_EXPRESSION_JOB_CLASS")) {
    int64_t scheduler_flags = ObDBMSSchedJobInfo::JOB_SCHEDULER_FLAG_DATE_EXPRESSION_JOB_CLASS;
    dml.add_column("scheduler_flags", scheduler_flags);
  }
  OZ (dml.splice_insert_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql));
  return ret;
}

} // end of pl
} // end oceanbase
