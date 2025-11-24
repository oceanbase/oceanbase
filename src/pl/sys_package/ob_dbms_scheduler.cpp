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

#include "observer/ob_inner_sql_connection_pool.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_executor.h"
#include "ob_dbms_scheduler.h"
#include "share/stat/ob_dbms_stats_maintenance_window.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_rpc_proxy.h"
#include "share/ob_scheduled_manage_dynamic_partition.h"
#include "share/balance/ob_scheduled_trigger_partition_balance.h" // ObScheduledTriggerPartitionBalance
#include "observer/dbms_scheduler/ob_dbms_sched_time_utils.h"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace observer;
using namespace sqlclient;
using namespace dbms_scheduler;
namespace pl
{

int ObDBMSScheduler::execute_sql(sql::ObExecContext &ctx, ObSqlString &sql, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObInnerSQLConnectionPool *pool = NULL;
  ObInnerSQLConnection *conn = NULL;
  sql::ObSQLSessionInfo *session = NULL;
  bool need_reset_mode = false;
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_NOT_NULL(session = ctx.get_my_session()));
  // we need to update __all_job table in mysql mode.
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ctx.get_sql_proxy()->write(session->get_effective_tenant_id(), sql.ptr(), affected_rows))) {
      LOG_WARN("fail to exec sql", K(sql), K(ret));
    }
  }
  return ret;
}

int ObDBMSScheduler::get_and_increase_job_id(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  UNUSED(params);
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ctx.get_my_session()));
  if (OB_SUCC(ret)) {
    int64_t tenant_id = ctx.get_my_session()->get_effective_tenant_id();
    int64_t job_id = 0;
    char job_id_str[VALUE_BUFSIZE];
    OZ (ObDBMSSchedJobUtils::generate_job_id(tenant_id, job_id));
    snprintf(job_id_str, VALUE_BUFSIZE, "%ld", job_id);
    OX (result.set_varchar(job_id_str));
    OZ (deep_copy_obj(ctx.get_allocator(), result, result));
    LOG_INFO("get and increase job id", K(ret), K(job_id), K(job_id_str));
  }
  return ret;
}

/*
  参数说明和用途
   0 ==> JOB_NAME              VARCHAR2
   1 ==> JOB_STYLE             VARCHAR2
     REGULAR (LIGHTWEIGHT IN_MEMORY_RUNTIME IN_MEMORY_FULL)不支持
   2 ==> PROGRAM_TYPE          VARCHAR2
     PLSQL_BLOCK STORED_PROCEDURE (EXECUTABLE CHAIN SQL_SCRIPT BACKUP_SCRIPT EXTERNAL_SCRIPT) 不支持
     NAME (program_name)
   3 ==> PROGRAM_ACTION        VARCHAR2
   4 ==> NUMBER_OF_ARGUMENTS   PLS_INTEGER
   5 ==> SCHEDULE_TYPE         VARCHAR2
     IMMEDIATE - Start date and repeat interval are NULL
     ONCE - Repeat interval is NULL
     PLSQL - PL/SQL expression used as schedule
     CALENDAR - Oracle calendaring expression used as schedule
     EVENT - Event schedule
     NAMED - Named schedule
     WINDOW - Window used as schedule
     WINDOW_GROUP - Window group used as schedule
   6 ==> SCHEDULE_EXPR         VARCHAR2
   7 ==> QUEUE_SPEC            VARCHAR2     未使用
   8 ==> START_DATE            TIMESTAMP
   9 ==> END_DATE              TIMESTAMP
  10 ==> JOB_CLASS             VARCHAR2
  11 ==> COMMENTS              VARCHAR2
  12 ==> ENABLED               BOOLEAN
  13 ==> AUTO_DROP             BOOLEAN
  14 ==> INVOKER               VARCHAR2     当前用户 未使用
  15 ==> SYS_PRIVS             PLS_INTEGER  未使用
  16 ==> AQ_JOB                BOOLEAN      未使用
  17 ==> DESTINATION_NAME      VARCHAR2     未使用
  18 ==> CREDENTIAL            VARCHAR2     未使用
  19 ==> max_run_duration
*/
int ObDBMSScheduler::create_job(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  uint64_t data_version = 0;
  if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret));
  } else if (OB_ISNULL(ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    int64_t tenant_id = ctx.get_my_session()->get_effective_tenant_id();
    char buf[OB_MAX_PROC_ENV_LENGTH];
    int64_t pos = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("fail to get tenant data version", KR(ret), K(tenant_id), K(data_version));
    } else if (DATA_VERSION_4_2_1_1 > data_version) {
      // not allowed to create job when version < 4211
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not allowed to create job before upgrade finished", K(ret), K(tenant_id), K(data_version));
    } else if (OB_FAIL(sql::ObExecEnv::gen_exec_env(*(ctx.get_my_session()), buf, OB_MAX_PROC_ENV_LENGTH, pos))) {
      LOG_WARN("gen exec env failed", K(ret));
    } else {
      HEAP_VAR(dbms_scheduler::ObDBMSSchedJobInfo, job_info) {
        job_info.tenant_id_ = ctx.get_my_session()->get_effective_tenant_id();
        job_info.user_id_ = ctx.get_my_session()->get_user_id();
        job_info.database_id_ = ctx.get_my_session()->get_database_id(); //oracle database_id = oceanbase_id
        job_info.powner_ = ctx.get_my_session()->get_user_name();
        job_info.lowner_ = ctx.get_my_session()->get_user_name(); //oracle 模式 database_name = user_name
        job_info.cowner_ = ctx.get_my_session()->get_user_name(); //oracle 模式 database_name = user_name
        job_info.exec_env_ = ObString(pos, buf);

        //check job name
        if (OB_SUCC(ret)) {
          ObString job_name;
          if (OB_FAIL(ObCharset::charset_convert(ctx.get_allocator(), params.at(0).get_string(), params.at(0).get_collation_type(), ObCharset::get_system_collation(), job_name))) {
            LOG_WARN("charset convert failed", K(ret), K(tenant_id));
          } else if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_name(job_name))) {
            ret = OB_SP_RAISE_APPLICATION_ERROR;
            ObString err_info("job name is an invalid name for a database object.");
            LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
          } else {
            job_info.job_name_ = job_name;
          }
        }

        //check job style
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_job_style(params.at(1).get_string()))) {
            ret = OB_SP_RAISE_APPLICATION_ERROR;
            ObString err_info("no support job style");
            LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27463L, err_info.length(), err_info.ptr());
          } else {
            job_info.job_style_ = params.at(1).get_string();
          }
        }

        if (OB_SUCC(ret)) {
          if (0 == params.at(2).get_string().case_compare("NAMED")) {
            job_info.job_type_ = "STORED_PROCEDURE";
            //check program_name
            ObString program_name;
            if (OB_FAIL(ObCharset::charset_convert(ctx.get_allocator(), params.at(3).get_string(), params.at(3).get_collation_type(), ObCharset::get_system_collation(), program_name))) {
              LOG_WARN("charset convert failed", K(ret), K(tenant_id));
            } else if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_name(program_name))) {
              ret = OB_SP_RAISE_APPLICATION_ERROR;
              ObString err_info("program name is an invalid name for a database object.");
              LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
            } else {
              job_info.program_name_ = program_name;
            }
          } else {
            //check job type
            if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_job_type(params.at(2).get_string()))) {
              ret = OB_SP_RAISE_APPLICATION_ERROR;
              ObString err_info("no support job type");
              LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27463L, err_info.length(), err_info.ptr());
            } else {
              job_info.job_type_ = params.at(2).get_string();
              OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(3).get_string(), params.at(3).get_collation_type(), ObCharset::get_system_collation(), job_info.job_action_));
            }
          }
        }

        //check job argument
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_argument_num(params.at(4).get_int()))) {
            ret = OB_SP_RAISE_APPLICATION_ERROR;
            ObString err_info("no support job argument");
            LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27418L, err_info.length(), err_info.ptr());
          } else {
            job_info.number_of_argument_ = params.at(4).get_int();
          }
        }

        // job_info.schedule_type = params.at(5).get_string();

        //check repeat interval
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_repeat_interval(tenant_id, params.at(6).get_string(), true))) {
            if (OB_INVALID_ARGUMENT_NUM == ret) {
              ObString err_info("calendar expression restriction MAX_FREQUENCY_INTERVAL encountered");
              LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27418L, err_info.length(), err_info.ptr());
            } else {
              ObString err_info("syntax error in repeat interval or calendar");
              LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27418L, err_info.length(), err_info.ptr());
            }
            ret = OB_SP_RAISE_APPLICATION_ERROR;
          } else {
            job_info.repeat_interval_ = params.at(6).get_string();
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_end_date(params.at(8).get_int(), params.at(9).get_int()))) {
            ret = OB_SP_RAISE_APPLICATION_ERROR;
            ObString err_info("has an invalid END_DATE");
            LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27483L, err_info.length(), err_info.ptr());
          } else {
            job_info.start_date_ = params.at(8).get_int();
            job_info.end_date_ = params.at(9).get_int();
          }
        }

        if (OB_SUCC(ret)) {
          ObString job_class;
          if (OB_FAIL(ObCharset::charset_convert(ctx.get_allocator(), params.at(10).get_string(), params.at(10).get_collation_type(), ObCharset::get_system_collation(), job_class))) {
            LOG_WARN("charset convert failed", K(ret), K(tenant_id));
          } else if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_name(job_class))) {
            ret = OB_SP_RAISE_APPLICATION_ERROR;
            ObString err_info("job class is an invalid name for a database object.");
            LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
          } else if (OB_FAIL(ObDBMSSchedJobUtils::check_not_buildin_job_class(job_class))) {
            ret = OB_SP_RAISE_APPLICATION_ERROR;
            ObString err_info("job class same with buildin job class is not allowed.");
            LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
          } else if ((0 != job_class.case_compare("DEFAULT_JOB_CLASS"))
            && OB_FAIL(ObDBMSSchedJobUtils::job_class_check_impl(tenant_id, job_class))) {
            ret = OB_SP_RAISE_APPLICATION_ERROR;
            ObString err_info("job class is not exist.");
            LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
          } else {
            job_info.job_class_ = job_class;
          }
        }

        OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(11).get_string(), params.at(11).get_collation_type(), ObCharset::get_system_collation(), job_info.comments_));
        job_info.enabled_ = params.at(12).get_bool();
        job_info.auto_drop_ = params.at(13).get_bool();

        if (OB_SUCC(ret)) {
          if (!params.at(17).get_string().empty() ||  !params.at(18).get_string().empty()) {
            ret = OB_SP_RAISE_APPLICATION_ERROR;
            ObString err_info("not support destination_name or credential_name");
            LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27418L, err_info.length(), err_info.ptr());
          } else {
            // job_info.destination_name_ = params.at(17).get_string(); //不使用，不让设
            // job_info.credential_name_ = params.at(18).get_string(); //不使用，不让设
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_max_run_duration(params.at(19).get_int()))) {
            ret = OB_SP_RAISE_APPLICATION_ERROR;
            ObString err_info("max_run_duration is invalid");
            LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 1867L, err_info.length(), err_info.ptr());
          } else {
            job_info.max_run_duration_ = params.at(19).get_int();
          }
        }

        if (OB_SUCC(ret)) {
          int64_t job_id = 0;
          job_info.func_type_ = ObDBMSSchedFuncType::USER_JOB;
          if (OB_FAIL(ObDBMSSchedJobUtils::generate_job_id(tenant_id, job_id))) {
            LOG_WARN("faild generate job id", K(ret), K(tenant_id), K(job_info));
          } else if (OB_FAIL(ObDBMSSchedJobUtils::create_dbms_sched_job(
                *ctx.get_sql_proxy(), tenant_id, job_id, job_info))) {
            LOG_WARN("failed to create dbms scheduler job", KR(ret), K(job_info));
          } else {
            LOG_INFO("success create job", K(ret), K(tenant_id), K(job_info.job_name_));
          }
        }
      }
    }
  }
  return ret;
}

int ObDBMSScheduler::drop_job(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString job_name;
  ObArenaAllocator allocator;
  ObDBMSSchedJobInfo job_info;
  UNUSED(result);
  CK (OB_LIKELY(4 == params.count()));
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(0).get_string(), params.at(0).get_collation_type(), ObCharset::get_system_collation(), job_name));
  OZ (job_priv_check_impl(ctx, job_name, allocator, job_info));
  if (OB_SUCC(ret)) {
    if (!job_info.is_user_job()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("not allowed to drop inner job", K(ret), K(job_name), K(job_info));
    } else if (OB_FAIL(ObDBMSSchedJobUtils::remove_dbms_sched_job(*ctx.get_sql_proxy(),
                                                                  job_info.get_tenant_id(),
                                                                  job_name))) {
      LOG_WARN("drop job failed", K(ret));
    }
  }
  LOG_INFO("drop job", K(ret), K(ObHexEscapeSqlStr(job_name)));
  return ret;
}

int ObDBMSScheduler::run_job(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObString job_name;
  bool force = false;
  ObArenaAllocator allocator;
  ObDBMSSchedJobInfo job_info;
  CK (OB_LIKELY(2 == params.count()));
  OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(0).get_string(), params.at(0).get_collation_type(), ObCharset::get_system_collation(), job_name));
  OX (force = params.at(1).is_null() ? false : params.at(1).get_bool());
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  OZ (job_priv_check_impl(ctx, job_name, allocator, job_info));
  if (OB_SUCC(ret)) {
    if (!job_info.is_running() || force) {
      ObDBMSSchedJobExecutor executor;
      if (!force && !job_info.get_zone().empty() && !(0 == job_info.get_zone().compare("RANDOM"))) {
        OZ (ObDBMSSchedJobUtils::zone_check_impl(job_info.get_tenant_id(), job_info.get_zone()));
      }
      OZ (executor.init(ctx.get_sql_proxy(),
          &share::schema::ObMultiVersionSchemaService::get_instance()));
      OZ (executor.run_dbms_sched_job(job_info.get_tenant_id(), job_info.is_oracle_tenant(), job_info.get_job_id(), job_info.get_job_name()));
    } else {
      // not force, and job is running, do nothing ...
    }
  }
  LOG_INFO("run job", K(ret), K(job_name));
  return ret;
}

int ObDBMSScheduler::stop_job(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  ObString job_name;
  ObSqlString sql;
  ObArenaAllocator allocator;
  ObDBMSSchedJobInfo job_info;
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_NOT_NULL(ctx.get_my_session()));
  OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(0).get_string(), params.at(0).get_collation_type(), ObCharset::get_system_collation(), job_name));
  OZ (job_priv_check_impl(ctx, job_name, allocator, job_info));
  OZ (ObDBMSSchedJobUtils::stop_dbms_sched_job(*ctx.get_sql_proxy(), job_info, false));
  LOG_INFO("stop job", K(ret), K(job_name));
  return ret;
}

int ObDBMSScheduler::generate_job_name(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  UNUSED(ctx);
  UNUSED(params);
  UNUSED(result);
  int ret = OB_SUCCESS;
  return ret;
}

int ObDBMSScheduler::job_priv_check_impl(sql::ObExecContext &ctx, ObString &job_name, ObArenaAllocator &allocator, ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = OB_INVALID_ID;
  const ObUserInfo *user_info = nullptr;
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_NOT_NULL(ctx.get_virtual_table_ctx().schema_guard_));
  OX (tenant_id = ctx.get_my_session()->get_effective_tenant_id());
  if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_name(job_name))) {
    ret = OB_SP_RAISE_APPLICATION_ERROR;
    ObString err_info("job name is an invalid name for a database object.");
    LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
  } else if (OB_FAIL(ObDBMSSchedJobUtils::get_dbms_sched_job_info(*ctx.get_sql_proxy(),
                                                                  tenant_id,
                                                                  true, // is_oracle_tenant
                                                                  job_name,
                                                                  allocator,
                                                                  job_info))) {
    LOG_WARN("get job failed", K(ret));
    if (ret == OB_ENTRY_NOT_EXIST) {
      ret = OB_SP_RAISE_APPLICATION_ERROR;
      ObString err_info("job not exist");
      LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 23421L, err_info.length(), err_info.ptr());
    }
  } else if(OB_FAIL(ctx.get_virtual_table_ctx().schema_guard_->get_user_info(tenant_id, ctx.get_my_session()->get_user_id(), user_info))) {
    LOG_WARN("fail to get user id", KR(ret), K(tenant_id), K(ctx.get_my_session()->get_user_id()));
  } else if (OB_ISNULL(user_info)) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("user not exist", KR(ret), K(tenant_id), K(ctx.get_my_session()->get_user_id()));
  } else if (OB_FAIL(ObDBMSSchedJobUtils::check_dbms_sched_job_priv(user_info, job_info))) {
    ret = OB_SP_RAISE_APPLICATION_ERROR;
    ObString err_info("current user does not have permission.");
    LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 23421L, err_info.length(), err_info.ptr());
  }
  return ret;
}

int ObDBMSScheduler::set_job_attribute(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObString attr_name;
  ObString attr_val;
  ObString job_name;
  ObArenaAllocator allocator;
  ObDBMSSchedJobInfo job_info;
  uint64_t tenant_id = OB_INVALID_ID;

  CK (OB_LIKELY(3 == params.count()));
  OX (tenant_id = ctx.get_my_session()->get_effective_tenant_id());
  OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(0).get_string(), params.at(0).get_collation_type(), ObCharset::get_system_collation(), job_name));
  OZ (job_priv_check_impl(ctx, job_name, allocator, job_info));
  OZ (params.at(1).get_varchar(attr_name));
  OZ (params.at(2).get_varchar(attr_val));
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (attr_name.empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                  "OBE-23428: job associated attr_name is null");
  } else if (attr_val.empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                  "OBE-23428: job associated attr_val is null");
  } else {
    bool is_stat_window_attr = false;
    bool is_trigger_balance_attr = false;
    bool is_dynamic_partition_attr = false;
    ObDMLSqlSplicer dml;
    const int64_t now = ObTimeUtility::current_time();
    if (job_info.is_stats_maintenance_job()) {
      OZ (dml.add_gmt_modified(now));
      OZ (dml.add_pk_column("tenant_id",
        share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
      OZ (dml.add_pk_column("job_name", ObHexEscapeSqlStr(job_name)));
      if (OB_FAIL(ObDbmsStatsMaintenanceWindow::is_stats_maintenance_window_attr(
                                                                            ctx,
                                                                            job_name,
                                                                            attr_name,
                                                                            attr_val,
                                                                            is_stat_window_attr,
                                                                            dml))) {
        LOG_WARN("failed to is stats maintenance window attr", K(ret), K(tenant_id), K(job_name), K(attr_name), K(attr_val));
      } else if (is_stat_window_attr) {
        ObSqlString sql;
        int64_t affected_rows = 0;
        OZ (dml.splice_update_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql));
        OZ (execute_sql(ctx, sql, affected_rows));
        CK (1 == affected_rows || 2 == affected_rows);
      }
    } else if (job_info.is_trigger_part_balance_job()) {
      OZ (dml.add_gmt_modified(now));
      OZ (dml.add_pk_column("tenant_id",
        share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
      OZ (dml.add_pk_column("job_name", ObHexEscapeSqlStr(job_name)));
      if (FAILEDx(ObScheduledTriggerPartitionBalance::set_attr_for_trigger_part_balance(
          ctx.get_my_session(),
          params.at(0).get_string(),
          params.at(1).get_string(),
          params.at(2).get_string(),
          is_trigger_balance_attr,
          dml))) {
        LOG_WARN("is scheduled trigger partition balance attr failed", KR(ret), "job_name", params.at(0).get_string(),
            "attr_name", params.at(1).get_string(), "val_str", params.at(2).get_string());
      } else if (is_trigger_balance_attr) {
        ObSqlString sql;
        int64_t affected_rows = 0;
        OZ (dml.splice_update_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql));
        OZ (execute_sql(ctx, sql, affected_rows));
        CK (1 == affected_rows || 2 == affected_rows);
      }
    } else if (job_info.is_dynamic_partition_job()) {
      OZ (dml.add_gmt_modified(now));
      OZ (dml.add_pk_column("tenant_id",
        share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
      OZ (dml.add_pk_column("job_name", ObHexEscapeSqlStr(job_name)));
      if (OB_FAIL(ObScheduledManageDynamicPartition::set_attribute(ctx.get_my_session(),
                                                                   job_name,
                                                                   attr_name,
                                                                   attr_val,
                                                                   is_dynamic_partition_attr,
                                                                   dml))) {
        LOG_WARN("failed to set attribute for scheduled manage dynamic partition", KR(ret),
          K(tenant_id), K(job_name), K(attr_name), K(attr_val));
      } else if (is_dynamic_partition_attr) {
        ObSqlString sql;
        int64_t affected_rows = 0;
        OZ (dml.splice_update_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql));
        OZ (execute_sql(ctx, sql, affected_rows));
        CK (1 == affected_rows || 2 == affected_rows);
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (is_stat_window_attr || is_trigger_balance_attr || is_dynamic_partition_attr) {
      // do nothing
    } else {
      ObObj attr_val_obj;
      attr_val_obj.set_string(ObVarcharType, attr_val);
      if (OB_FAIL(ObDBMSSchedJobUtils::update_dbms_sched_job_info(*ctx.get_sql_proxy(), job_info, attr_name, attr_val_obj, true))) {
        LOG_WARN("failed to update dbms sched job info", K(ret), K(tenant_id), K(job_info), K(attr_name), K(attr_val_obj));
      }
    }
  }
  LOG_INFO("set job attribute", K(ret), K(attr_name), K(attr_val), K(tenant_id));
  return ret;
}

int ObDBMSScheduler::disable(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObString job_name;
  ObSqlString sql;
  ObArenaAllocator allocator;
  ObDBMSSchedJobInfo job_info;
  ObObj obj;
  obj.set_bool(false);
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_LIKELY(3 == params.count()));
  OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(0).get_string(), params.at(0).get_collation_type(), ObCharset::get_system_collation(), job_name));
  OZ (job_priv_check_impl(ctx, job_name, allocator, job_info));
  OZ (ObScheduledTriggerPartitionBalance::check_disable_trigger_job(job_info.get_tenant_id(), job_name));
  OZ (ObDBMSSchedJobUtils::update_dbms_sched_job_info(*ctx.get_sql_proxy(), job_info, ObString("enabled"), obj));
  LOG_INFO("disable job", K(ret), K(ObHexEscapeSqlStr(job_name)), K(job_info.get_tenant_id()));
  return ret;
}

int ObDBMSScheduler::create_program(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const int64_t now = ObTimeUtility::current_time();
  uint64_t tenant_id = OB_INVALID_ID;
  ObString program_name;
  ObString program_type;
  ObString program_action;
  ObString comments;
  UNUSED(result);
  CK (OB_LIKELY(6 == params.count()));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCharset::charset_convert(ctx.get_allocator(), params.at(0).get_string(), params.at(0).get_collation_type(), ObCharset::get_system_collation(), program_name))) {
      LOG_WARN("charset convert failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_name(program_name))) {
      ret = OB_SP_RAISE_APPLICATION_ERROR;
      ObString err_info("program is an invalid name for a database object.");
      LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
    }
  }
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  OZ (dml.add_gmt_modified(now));
  OZ (dml.add_pk_column("tenant_id",
    share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
  OZ (dml.add_pk_column("program_name", ObHexEscapeSqlStr(program_name)));
  OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(1).get_string(), params.at(1).get_collation_type(), ObCharset::get_system_collation(), program_type));
  OZ (dml.add_column("program_type", ObHexEscapeSqlStr(program_type)));
  OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(2).get_string(), params.at(2).get_collation_type(), ObCharset::get_system_collation(), program_action));
  OZ (dml.add_column("program_action", ObHexEscapeSqlStr(program_action)));
  OZ (dml.add_column("number_of_argument", params.at(3).get_int()));
  OZ (dml.add_column("enabled", params.at(4).get_bool()));
  OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(5).get_string(), params.at(5).get_collation_type(), ObCharset::get_system_collation(), comments));
  OZ (dml.add_column("comments", ObHexEscapeSqlStr(comments)));
  OZ (dml.add_column("lowner", ObHexEscapeSqlStr(ctx.get_my_session()->get_user_name())));
  OZ (dml.add_column("powner", ObHexEscapeSqlStr(ctx.get_my_session()->get_user_name())));
  OZ (dml.add_column("cowner", ObHexEscapeSqlStr(ctx.get_my_session()->get_user_name())));
  OZ (dml.splice_insert_sql(OB_ALL_TENANT_SCHEDULER_PROGRAM_TNAME, sql));
  OZ (execute_sql(ctx, sql, affected_rows));
  CK (OB_LIKELY(1 == affected_rows));
  return ret;
}

int ObDBMSScheduler::define_program_argument(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString job_name;
  ObString program_name;
  ObString argument_name;
  ObString argument_type;
  ObString default_value;
  ObArenaAllocator allocator;
  ObDBMSSchedJobInfo job_info;
  bool is_for_default = true;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const int64_t now = ObTimeUtility::current_time();
  uint64_t tenant_id = OB_INVALID_ID;
  UNUSED(result);
  CK (OB_LIKELY(8 == params.count()));
  OX (is_for_default = params.at(7).get_bool());
  OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(6).get_string(), params.at(6).get_collation_type(), ObCharset::get_system_collation(), job_name));
  if (is_for_default) {
    OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(0).get_string(), params.at(0).get_collation_type(), ObCharset::get_system_collation(), program_name));
  } else {
    OZ (job_priv_check_impl(ctx, job_name, allocator, job_info));
    if (OB_SUCC(ret)) {
      program_name = job_info.get_program_name();
      if (program_name.empty()) {
        ret = OB_SP_RAISE_APPLICATION_ERROR;
        ObString err_info("no program has found with job");
        LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 23422L, err_info.length(), err_info.ptr());
      } else if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_name(params.at(2).get_string()))) {
        ret = OB_SP_RAISE_APPLICATION_ERROR;
        ObString err_info("argument name is an invalid name for a database object.");
        LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
      }
    }
  }
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  OZ (dml.add_gmt_modified(now));
  OZ (dml.add_pk_column("tenant_id",
    share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
  OZ (dml.add_pk_column("program_name", ObHexEscapeSqlStr(program_name)));
  OZ (dml.add_column("argument_position", params.at(1).get_int()));
  OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(2).get_string(), params.at(2).get_collation_type(), ObCharset::get_system_collation(), argument_name));
  OZ (dml.add_column("argument_name", ObHexEscapeSqlStr(argument_name)));
  OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(3).get_string(), params.at(3).get_collation_type(), ObCharset::get_system_collation(), argument_type));
  OZ (dml.add_column("argument_type", ObHexEscapeSqlStr(argument_type)));
  OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(4).get_string(), params.at(4).get_collation_type(), ObCharset::get_system_collation(), default_value));
  OZ (dml.add_column("default_value", ObHexEscapeSqlStr(default_value)));
  OZ (dml.add_column("out_argument", params.at(5).get_bool()));
  OZ (dml.add_column("job_name", ObHexEscapeSqlStr(job_name)));
  OZ (dml.add_column("is_for_default", params.at(7).get_bool()));
  OZ (dml.splice_insert_sql(OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_TNAME, sql));
  OZ (execute_sql(ctx, sql, affected_rows));
  CK (OB_LIKELY(1 == affected_rows));
  return ret;
}

int ObDBMSScheduler::enable(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObString job_name;
  ObArenaAllocator allocator;
  ObDBMSSchedJobInfo job_info;
  ObObj obj;
  obj.set_bool(true);
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_LIKELY(1 == params.count()));
  OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(0).get_string(), params.at(0).get_collation_type(), ObCharset::get_system_collation(), job_name));
  OZ (job_priv_check_impl(ctx, job_name, allocator, job_info));
  OZ (ObScheduledTriggerPartitionBalance::check_enable_trigger_job(job_info.get_tenant_id(), job_name));
  OZ (ObDBMSSchedJobUtils::update_dbms_sched_job_info(*ctx.get_sql_proxy(), job_info, ObString("enabled"), obj));
  return ret;
}

int ObDBMSScheduler::drop_program(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t tenant_id = OB_INVALID_ID;
  const int64_t now = ObTimeUtility::current_time();
  ObString program_name;
  UNUSED(result);
  CK (OB_LIKELY(2 == params.count()));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCharset::charset_convert(ctx.get_allocator(), params.at(0).get_string(), params.at(0).get_collation_type(), ObCharset::get_system_collation(), program_name))) {
      LOG_WARN("charset convert failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_name(program_name))) {
      ret = OB_SP_RAISE_APPLICATION_ERROR;
      ObString err_info("program is an invalid name for a database object.");
      LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
    }
  }
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  OZ (dml.add_gmt_modified(now));
  OX (tenant_id = ctx.get_my_session()->get_effective_tenant_id());
  OZ (dml.add_pk_column("tenant_id",
    share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
  OZ (dml.add_pk_column("program_name", ObHexEscapeSqlStr(program_name)));
  OZ (dml.splice_delete_sql(OB_ALL_TENANT_SCHEDULER_PROGRAM_TNAME, sql));
  OZ (execute_sql(ctx, sql, affected_rows));
  CK (1 == affected_rows);
  OZ (dml.splice_delete_sql(OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_TNAME, sql));
  OZ (execute_sql(ctx, sql, affected_rows));
  if (0 == affected_rows) {
    LOG_WARN("program argument may not assigned", K(affected_rows));
  }
  return ret;
}

int ObDBMSScheduler::create_job_class(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t tenant_id = OB_INVALID_ID;
  const int64_t now = ObTimeUtility::current_time();
  ObString job_class_name;
  ObString comments;

  UNUSED(result);
  CK (OB_LIKELY(6 == params.count()));
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  OX (tenant_id = ctx.get_my_session()->get_effective_tenant_id());

  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (!DATA_VERSION_SUPPORT_JOB_CLASS(data_version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                    "OBE-23428: not support create job_class");
  }

  ObString logging_level;
  OZ(params.at(3).get_varchar(logging_level));
  //check logging_level and log_history
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCharset::charset_convert(ctx.get_allocator(), params.at(0).get_string(), params.at(0).get_collation_type(), ObCharset::get_system_collation(), job_class_name))) {
      LOG_WARN("charset convert failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_name(job_class_name))) {
      ret = OB_SP_RAISE_APPLICATION_ERROR;
      ObString err_info("job class is an invalid name for a database object.");
      LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
    } else if (0 == job_class_name.case_compare("DEFAULT_JOB_CLASS")) {
      ret = OB_SP_RAISE_APPLICATION_ERROR;
      ObString err_info("not allowed to create default job class.");
      LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
    } else if (OB_FAIL(ObDBMSSchedJobUtils::check_not_buildin_job_class(params.at(0).get_string()))) {
      ret = OB_SP_RAISE_APPLICATION_ERROR;
      ObString err_info("not allowed to create build-in job class.");
      LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
    } else if (!is_ora_sys_user(ctx.get_my_session()->get_user_id())) {
      ret = OB_SP_RAISE_APPLICATION_ERROR;
      ObString err_info("current user does not have permission.");
      LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
    } else if (0 != logging_level.case_compare("OFF") &&
      0 != logging_level.case_compare("RUNS") &&
      0 != logging_level.case_compare("FAILED RUNS")) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                  "OBE-23428: job_class logging_level attr_value is not supported");
    } else if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_log_history(params.at(4).get_int()))) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                  "OBE-23428: job_class log_history attr_value is not supported");
    }
  }

  if (OB_SUCC(ret)) {
    OZ (dml.add_gmt_create(now));
    OZ (dml.add_gmt_modified(now));
    OZ (dml.add_pk_column("tenant_id",
      share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
    OZ (dml.add_pk_column("job_class_name", ObHexEscapeSqlStr(job_class_name)));
    OZ (dml.add_column("resource_consumer_group", ObHexEscapeSqlStr(params.at(1).get_string())));
    OZ (dml.add_column("service", ObHexEscapeSqlStr(params.at(2).get_string())));
    OZ (dml.add_column("logging_level", params.at(3).get_string()));
    OZ (dml.add_column("log_history", params.at(4).get_int()));
    OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(5).get_string(), params.at(5).get_collation_type(), ObCharset::get_system_collation(), comments));
    OZ (dml.add_column("comments", ObHexEscapeSqlStr(comments)));
    OZ (dml.splice_insert_sql(OB_ALL_TENANT_SCHEDULER_JOB_CLASS_TNAME, sql));
    OZ (execute_sql(ctx, sql, affected_rows));
    CK (OB_LIKELY(1 == affected_rows));
  }
  LOG_INFO("create job class", K(ret), K(ObHexEscapeSqlStr(job_class_name)), K(tenant_id));
  return ret;
}

int ObDBMSScheduler::drop_job_class(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t tenant_id = OB_INVALID_ID;
  ObString job_class_name;
  const int64_t now = ObTimeUtility::current_time();


  UNUSED(result);
  CK (OB_LIKELY(2 == params.count()));
  CK (OB_NOT_NULL(ctx.get_my_session()));
  OX (tenant_id = ctx.get_my_session()->get_effective_tenant_id());
  OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(0).get_string(), params.at(0).get_collation_type(), ObCharset::get_system_collation(), job_class_name));
  uint64_t data_version = 0;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
    } else if (!DATA_VERSION_SUPPORT_JOB_CLASS(data_version)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                      "OBE-23428: not support drop job_class");
    } else if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_name(job_class_name))) {
      ret = OB_SP_RAISE_APPLICATION_ERROR;
      ObString err_info("job class is an invalid name for a database object.");
      LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
    } else if (!is_ora_sys_user(ctx.get_my_session()->get_user_id())) {
      ret = OB_SP_RAISE_APPLICATION_ERROR;
      ObString err_info("current user does not have permission.");
      LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
    }
  }

  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  OZ (sql.append_fmt("delete from %s where tenant_id = %ld and job_class_name = \'%.*s\'",
      OB_ALL_TENANT_SCHEDULER_JOB_CLASS_TNAME,
      share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
      job_class_name.length(),
      job_class_name.ptr()));
  OZ (sql.append_fmt(" and not exists (select 1 from %s where job_class = \'%.*s\')",
      OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
      job_class_name.length(),
      job_class_name.ptr()));
  OZ (execute_sql(ctx, sql, affected_rows));
  if (OB_SUCC(ret) && 1 != affected_rows) {
    ret = OB_SP_RAISE_APPLICATION_ERROR;
    ObString err_info("job class maybe not exist or has referenced by job");
    LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
    LOG_WARN("job class maybe not exist or has referenced by job", K(ret), K(affected_rows), K(tenant_id));
  }
  LOG_INFO("drop job class", K(ret), K(ObHexEscapeSqlStr(params.at(0).get_string())), K(tenant_id));
  return ret;
}

int ObDBMSScheduler::set_job_class_attribute(
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
  const int64_t now = ObTimeUtility::current_time();

  CK (OB_LIKELY(3 == params.count()));
  OX (tenant_id = ctx.get_my_session()->get_effective_tenant_id());

  uint64_t data_version = 0;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
    } else if (!DATA_VERSION_SUPPORT_JOB_CLASS(data_version)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                      "OBE-23428: not support set job_class_attribute");
    } else if (!is_ora_sys_user(ctx.get_my_session()->get_user_id())) {
      ret = OB_SP_RAISE_APPLICATION_ERROR;
      ObString err_info("current user does not have permission.");
      LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
    }
  }

  ObString job_class_name;
  OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(0).get_string(), params.at(0).get_collation_type(), ObCharset::get_system_collation(), job_class_name));
  OZ (ObDBMSSchedJobUtils::job_class_check_impl(tenant_id, job_class_name));
  OZ (dml.add_pk_column("tenant_id",
    share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
  OZ (dml.add_pk_column("job_class_name", ObHexEscapeSqlStr(params.at(0).get_string())));
  if (OB_SUCC(ret)) {
    OZ (params.at(1).get_varchar(attr_name));
    OZ (params.at(2).get_varchar(attr_val));
    if (OB_SUCC(ret)) {
      if (attr_name.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
                      "OBE-23428: job_class associated attr_name is null");
      } else if (attr_val.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
                      "OBE-23428: job_class associated attr_val is null");
      } else if (0 == attr_name.case_compare("logging_level")) { // set logging_level
        if (0 == attr_val.case_compare("OFF") ||
          0 == attr_val.case_compare("RUNS") ||
          0 == attr_val.case_compare("FAILED RUNS")) {
          OZ (dml.add_column("logging_level", params.at(2).get_string()));
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
                      "OBE-23428: job_class logging_level attr_value is not supported");
        }
      } else if (0 == attr_name.case_compare("log_history")) { // set log_history
        const int MAX_LOG_HISTORY_LEN = 16;
        char log_history_buf[MAX_LOG_HISTORY_LEN];
        int64_t pos = attr_val.to_string(log_history_buf, MAX_LOG_HISTORY_LEN);
        int64_t log_history = atoll(log_history_buf);
        if (OB_SUCC(ObDBMSSchedJobUtils::check_is_valid_log_history(log_history))) {
          OZ (dml.add_column("log_history", log_history));
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
                      "OBE-23428: job_class log_history attr_value is not supported");
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
                      "OBE-23428: job_class associated attr name is not supported");
      }
    }
    if (OB_SUCC(ret)) {
      OZ (dml.splice_update_sql(OB_ALL_TENANT_SCHEDULER_JOB_CLASS_TNAME, sql));
      OZ (execute_sql(ctx, sql, affected_rows));
      CK (1 == affected_rows || 0 == affected_rows);
    }
  }
  LOG_INFO("set job class attribute", K(ret), K(attr_name), K(tenant_id));
  return ret;
}

int ObDBMSScheduler::_check_session_alive(const ObSQLSessionInfo *session) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(session->is_query_killed())) {
    ret = OB_ERR_QUERY_INTERRUPTED;
    LOG_WARN("query is killed", K(ret));
  } else if (OB_UNLIKELY(session->is_zombie())) {
    ret = OB_ERR_SESSION_INTERRUPTED;
    LOG_WARN("session is killed", K(ret));
  }
  return ret;
}

int ObDBMSScheduler::_do_purge_log(sql::ObExecContext &ctx,
                                   int64_t tenant_id,
                                   ObString &job_name,
                                   const int64_t job_id,
                                   const int64_t log_history,
                                   const char* table_name,
                                   bool is_v2_table)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const int64_t now = ObTimeUtility::current_time();
  int64_t day_duration = 24L * 60L * 60L * 1000L * 1000L;
  OZ (sql.append_fmt("delete from %s", table_name));
  if (!is_v2_table) {
    OZ (sql.append_fmt(" where tenant_id = %ld",
      share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)));
    if (0 < job_id) {
      OZ (sql.append_fmt(" and job = %ld", job_id));
    }
    if (OB_SUCC(ObDBMSSchedJobUtils::check_is_valid_log_history(log_history))) {
      int64_t delete_timestap = max(0, now - log_history * day_duration);
      OZ (sql.append_fmt(" and time < usec_to_time(%ld)", delete_timestap));
    }
  } else {
    if (!job_name.empty()) {
      OZ (sql.append_fmt(" where job_name = \'%.*s\'", job_name.length(), job_name.ptr()));
      if (OB_SUCC(ObDBMSSchedJobUtils::check_is_valid_log_history(log_history))) {
        int64_t delete_timestap = max(0, now - log_history * day_duration);
        OZ (sql.append_fmt(" and time < usec_to_time(%ld)", delete_timestap));
      }
    } else if (OB_SUCC(ObDBMSSchedJobUtils::check_is_valid_log_history(log_history))) {
      int64_t delete_timestap = max(0, now - log_history * day_duration);
      OZ (sql.append_fmt(" where time < usec_to_time(%ld)", delete_timestap));
    }
  }
  OZ (sql.append_fmt(" order by time limit %ld", BATCH_COUNT));
  do {
    OZ (execute_sql(ctx, sql, affected_rows));
    OZ (_check_session_alive(ctx.get_my_session()));
  } while (OB_SUCC(ret) && affected_rows == BATCH_COUNT);
  LOG_INFO("do purge log", K(ret), K(tenant_id), K(table_name), K(sql));
  return ret;
}

int ObDBMSScheduler::purge_log(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  int64_t log_history = 0;
  ObString job_name;
  ObArenaAllocator allocator;
  ObDBMSSchedJobInfo job_info;
  uint64_t tenant_id = OB_INVALID_ID;
  CK (OB_LIKELY(3 == params.count()));
  OX (log_history = params.at(0).get_int());
  OZ (ObCharset::charset_convert(ctx.get_allocator(), params.at(2).get_string(), params.at(2).get_collation_type(), ObCharset::get_system_collation(), job_name));
  OX (tenant_id = ctx.get_my_session()->get_effective_tenant_id());
  if (job_name.empty()) {
    if (!is_ora_sys_user(ctx.get_my_session()->get_user_id())) {
      ret = OB_SP_RAISE_APPLICATION_ERROR;
      ObString err_info("current user does not have permission.");
      LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
    }
  } else {
    OZ (job_priv_check_impl(ctx, job_name, allocator, job_info));
  }
  uint64_t data_version = 0;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (DATA_VERSION_SUPPORT_RUN_DETAIL_V2(data_version)) {
    OZ (_do_purge_log(ctx, tenant_id, job_name, job_info.get_job_id(), log_history, OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_TNAME, true));
  }
  OZ (_do_purge_log(ctx, tenant_id, job_name, job_info.get_job_id(), log_history, OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_TNAME, false));
  LOG_INFO("purge log", K(ret), K(tenant_id));
  return ret;
}

int ObDBMSScheduler::evaluate_calendar_string(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObDBMSSchedTimeUtil util;
  ObString calendar_string;
  int64_t start_date;
  int64_t return_date_after;
  int64_t next_run_date;
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_LIKELY(4 == params.count()));
  OZ (params.at(0).get_varchar(calendar_string));
  OX (start_date = params.at(1).get_int();)
  OX (return_date_after = params.at(2).get_int();)
  OZ (util.calc_repeat_interval_next_date(calendar_string, start_date, next_run_date, start_date, return_date_after));
  OX (params.at(3).set_timestamp(next_run_date);)
  return ret;
}

} // end of pl
} // end oceanbase
