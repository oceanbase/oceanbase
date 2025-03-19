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
#include "share/balance/ob_scheduled_trigger_partition_balance.h" // ObScheduledTriggerPartitionBalance

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
  OZ (ObScheduledTriggerPartitionBalance::check_disable_trigger_job(tenant_id, params.at(0).get_string()));
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
  OZ (ObScheduledTriggerPartitionBalance::check_enable_trigger_job(tenant_id, params.at(0).get_string()));
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
  bool is_trigger_part_balance_attr = false;
  const int64_t now = ObTimeUtility::current_time();
  CK (OB_LIKELY(3 == params.count()));
  OZ (dml.add_gmt_modified(now));
  OX (tenant_id = ctx.get_my_session()->get_effective_tenant_id());
  OZ (dml.add_pk_column("tenant_id",
    share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
  OZ (dml.add_pk_column("job_name", ObHexEscapeSqlStr(params.at(0).get_string())));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDbmsStatsMaintenanceWindow::is_stats_maintenance_window_attr(
                                                                          ctx,
                                                                          params.at(0).get_string(),
                                                                          params.at(1).get_string(),
                                                                          params.at(2).get_string(),
                                                                          is_stat_window_attr,
                                                                          dml))) {
      LOG_WARN("failed to is stats maintenance window attr", K(ret), K(params.at(0).get_string()),
                                        K(params.at(1).get_string()), K(params.at(2).get_string()));
    } else if (OB_FAIL(ObScheduledTriggerPartitionBalance::set_attr_for_trigger_part_balance(
        ctx.get_my_session(),
        params.at(0).get_string(),
        params.at(1).get_string(),
        params.at(2).get_string(),
        is_trigger_part_balance_attr,
        dml))) {
      LOG_WARN("is scheduled trigger partition balance attr failed", KR(ret), "job_name", params.at(0).get_string(),
          "attr_name", params.at(1).get_string(), "val_str", params.at(2).get_string());
    } else if (is_stat_window_attr || is_trigger_part_balance_attr) {
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


} // end of pl
} // end oceanbase
