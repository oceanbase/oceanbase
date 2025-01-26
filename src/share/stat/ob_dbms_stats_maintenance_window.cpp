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

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/ob_dbms_stats_maintenance_window.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "observer/dbms_scheduler/ob_dbms_sched_table_operator.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "observer/ob_sql_client_decorator.h"

namespace oceanbase {

namespace common {

const char *windows_name[DAY_OF_WEEK] = {"MONDAY_WINDOW",
                                         "TUESDAY_WINDOW",
                                         "WEDNESDAY_WINDOW",
                                         "THURSDAY_WINDOW",
                                         "FRIDAY_WINDOW",
                                         "SATURDAY_WINDOW",
                                         "SUNDAY_WINDOW"};
const char *opt_stats_history_manager = "OPT_STATS_HISTORY_MANAGER";
const char *async_gather_stats_job_proc = "ASYNC_GATHER_STATS_JOB_PROC";
const int64_t OPT_STATS_HISTORY_MANAGER_JOB_ID = 8;
const char *spm_stats_manager = "SPM_STATS_MANAGER";

int ObDbmsStatsMaintenanceWindow::get_stats_maintenance_window_jobs_sql(const ObSysVariableSchema &sys_variable,
                                                                        const uint64_t tenant_id,
                                                                        common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  char buf[OB_MAX_PROC_ENV_LENGTH];
  int64_t current_time = ObTimeUtility::current_time();
  ObSqlString tmp_sql;
  ObSqlString job_action;
  int64_t job_id = 1;
  int64_t pos = 0;
  int32_t offset_sec = 0;
  bool is_oracle_mode = false;
  if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("failed to get oracle mode", K(ret));
  } else if (OB_FAIL(sql::ObExecEnv::gen_exec_env(sys_variable, buf, OB_MAX_PROC_ENV_LENGTH, pos))) {
    LOG_WARN("failed to gen exec env", K(ret));
  } else if (OB_FAIL(get_time_zone_offset(sys_variable, tenant_id, offset_sec))) {
    LOG_WARN("failed to get time zone offset", K(ret));
  } else {
    ObString exec_env(pos, buf);
    HEAP_VAR(dbms_scheduler::ObDBMSSchedJobInfo, job_info) {
      //current_time = current_time + offset_sec * 1000000;
      for (int64_t i = 0; i < DAY_OF_WEEK; ++i) {
        int64_t start_usec = -1;
        ObSqlString job_action;
        if (OB_FAIL(get_window_job_info(current_time, i + 1, offset_sec, start_usec, job_action))) {
          LOG_WARN("failed to get window job info", K(ret));
        } else if (OB_UNLIKELY(start_usec == -1 || job_action.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(start_usec), K(job_action));
        } else {
          if (OB_FAIL(get_stat_window_job_info(is_oracle_mode,
                                                    tenant_id,
                                                    job_id,
                                                    windows_name[i],
                                                    exec_env,
                                                    start_usec,
                                                    job_action,
                                                    job_info))) {
            LOG_WARN("failed to get stat window job info", K(ret));
          } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::create_dbms_sched_job(sql_client, tenant_id, job_id, job_info))) {
            LOG_WARN("failed to create dbms sched job", K(ret), K(job_info));
          } else {
            ++ job_id;
          }
        }
      }
      if (OB_SUCC(ret)) {
        //set stats history manager job
        if (OB_FAIL(get_stats_history_manager_job_info(is_oracle_mode, tenant_id,
                                                      job_id, exec_env, job_info))) {
          LOG_WARN("failed to get stats history manager job sql", K(ret));
        } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::create_dbms_sched_job(sql_client, tenant_id, job_id, job_info))) {
          LOG_WARN("failed to create dbms sched job", K(ret), K(job_info));
        } else {
          ++ job_id;
        }

        //set async gather stats job
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(get_async_gather_stats_job_info(is_oracle_mode, tenant_id,
                                                  job_id, exec_env, job_info))) {
          LOG_WARN("failed to get async gather stats job sql", K(ret));
        } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::create_dbms_sched_job(sql_client, tenant_id, job_id, job_info))) {
          LOG_WARN("failed to create dbms sched job", K(ret), K(job_info));
        } else {
          ++ job_id;
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(get_spm_stats_job_info(is_oracle_mode, tenant_id, job_id,
                                                offset_sec, exec_env, job_info))) {
          LOG_WARN("failed to get stats history manager job sql", K(ret));
        } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::create_dbms_sched_job(sql_client, tenant_id, job_id, job_info))) {
          LOG_WARN("failed to create dbms sched job", K(ret), K(job_info));
        } else {
          ++ job_id;
        }
      }
    }
  }
  return ret;
}

int ObDbmsStatsMaintenanceWindow::get_stat_window_job_info(const bool is_oracle_mode,
                                                          const uint64_t tenant_id,
                                                          const int64_t job_id,
                                                          const char *job_name,
                                                          const ObString &exec_env,
                                                          const int64_t start_usec,
                                                          ObSqlString &job_action,
                                                          dbms_scheduler::ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  int64_t interval_ts = DEFAULT_WEEK_INTERVAL_USEC;
  int64_t end_date = 64060560000000000;//4000-01-01 00:00:00.000000
  int64_t default_duration_sec = is_work_day(job_id) ? DEFAULT_WORKING_DAY_DURATION_SEC : DEFAULT_NON_WORKING_DAY_DURATION_SEC;
  job_info.tenant_id_ = tenant_id;
  job_info.job_name_ = ObString(job_name);
  job_info.job_ = job_id;
  job_info.job_action_ = job_action.string();
  job_info.lowner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
  job_info.powner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
  job_info.cowner_ = is_oracle_mode ? ObString("SYS") : ObString("oceanbase");
  job_info.job_style_ = ObString("regular");
  job_info.job_type_ = ObString("STORED_PROCEDURE");
  job_info.job_class_ = ObString("DEFAULT_JOB_CLASS");
  job_info.start_date_ = start_usec;
  job_info.end_date_ = end_date;
  job_info.repeat_interval_ = ObString("FREQ=WEEKLY; INTERVAL=1");
  job_info.enabled_ = true;
  job_info.auto_drop_ = false;
  job_info.max_run_duration_ = default_duration_sec;
  job_info.exec_env_ = exec_env;
  job_info.comments_ = ObString("used to auto gather table stats");
  job_info.func_type_ = dbms_scheduler::ObDBMSSchedFuncType::STAT_MAINTENANCE_JOB;
  return ret;
}

int ObDbmsStatsMaintenanceWindow::get_stats_history_manager_job_info(const bool is_oracle_mode,
                                                                    const uint64_t tenant_id,
                                                                    const int64_t job_id,
                                                                    const ObString &exec_env,
                                                                    dbms_scheduler::ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  int64_t interval_ts = DEFAULT_DAY_INTERVAL_USEC;
  int64_t end_date = 64060560000000000;//4000-01-01 00:00:00.000000
  int64_t current = ObTimeUtility::current_time() + DEFAULT_DAY_INTERVAL_USEC;
  job_info.tenant_id_ = tenant_id;
  job_info.job_name_ = ObString(opt_stats_history_manager);
  job_info.job_ = job_id;
  job_info.job_action_ = ObString("DBMS_STATS.PURGE_STATS(NULL)");
  job_info.lowner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
  job_info.powner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
  job_info.cowner_ = is_oracle_mode ? ObString("SYS") : ObString("oceanbase");
  job_info.job_style_ = ObString("regular");
  job_info.job_type_ = ObString("STORED_PROCEDURE");
  job_info.job_class_ = ObString("DEFAULT_JOB_CLASS");
  job_info.start_date_ = current;
  job_info.end_date_ = end_date;
  job_info.repeat_interval_ = ObString("FREQ=DAYLY; INTERVAL=1");
  job_info.enabled_ = true;
  job_info.auto_drop_ = false;
  job_info.max_run_duration_ = DEFAULT_HISTORY_MANAGER_DURATION_SEC;
  job_info.exec_env_ = exec_env;
  job_info.comments_ = ObString("used to stats history manager");
  job_info.func_type_ = dbms_scheduler::ObDBMSSchedFuncType::STAT_MAINTENANCE_JOB;
  return ret;
}

int ObDbmsStatsMaintenanceWindow::get_async_gather_stats_job_info(const bool is_oracle_mode,
                                                                 const uint64_t tenant_id,
                                                                 const int64_t job_id,
                                                                 const ObString &exec_env,
                                                                 dbms_scheduler::ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  int64_t interval_ts = DEFAULT_ASYNC_GATHER_STATS_INTERVAL_USEC;
  int64_t end_date = 64060560000000000;//4000-01-01 00:00:00.000000
  int64_t current = ObTimeUtility::current_time() + DEFAULT_ASYNC_GATHER_STATS_INTERVAL_USEC;
  job_info.tenant_id_ = tenant_id;
  job_info.job_name_ = ObString(async_gather_stats_job_proc);
  job_info.job_ = job_id;
  job_info.job_action_ = ObString("DBMS_STATS.ASYNC_GATHER_STATS_JOB_PROC(600000000)");
  job_info.lowner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
  job_info.powner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
  job_info.cowner_ = is_oracle_mode ? ObString("SYS") : ObString("oceanbase");
  job_info.job_style_ = ObString("regular");
  job_info.job_type_ = ObString("STORED_PROCEDURE");
  job_info.job_class_ = ObString("DEFAULT_JOB_CLASS");
  job_info.start_date_ = current;
  job_info.end_date_ = end_date;
  job_info.repeat_interval_ = ObString("FREQ=MINUTELY; INTERVAL=15");
  job_info.enabled_ = true;
  job_info.auto_drop_ = false;
  job_info.max_run_duration_ = DEFAULT_ASYNC_GATHER_STATS_DURATION_SEC;
  job_info.exec_env_ = exec_env;
  job_info.comments_ = ObString("used to async gather stats");
  job_info.func_type_ = dbms_scheduler::ObDBMSSchedFuncType::STAT_MAINTENANCE_JOB;
  return ret;
}

int ObDbmsStatsMaintenanceWindow::get_spm_stats_job_info(const bool is_oracle_mode,
                                                        const uint64_t tenant_id,
                                                        const int64_t job_id,
                                                        const int64_t offset_sec,
                                                        const ObString &exec_env,
                                                        dbms_scheduler::ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;

  ObTime ob_time;
  int64_t current_time = ObTimeUtility::current_time();
  if (OB_UNLIKELY(current_time <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(current_time));
  } else if (OB_FAIL(ObTimeConverter::usec_to_ob_time(current_time + offset_sec * 1000000, ob_time))) {
    LOG_WARN("failed to usec to ob time", K(ret), K(current_time));
  } else {
    // handle spm stats at 1:00 every day
    int64_t default_start_hour = 1;
    int64_t total_hour_with_trunc = current_time / USEC_OF_HOUR;
    int64_t current_hour = ob_time.parts_[DT_HOUR];
    int64_t offset_hour = 1 * HOUR_OF_DAY + default_start_hour - current_hour;
    int64_t start_usec = (total_hour_with_trunc + offset_hour) * USEC_OF_HOUR;
    LOG_INFO("succeed to get spm job info", K(ob_time), K(start_usec), K(offset_hour), K(current_time),
                                               K(total_hour_with_trunc), K(current_hour),
                                               K(default_start_hour));
    int64_t interval_ts = DEFAULT_DAY_INTERVAL_USEC;
    int64_t end_date = 64060560000000000;//4000-01-01 00:00:00.000000
    job_info.tenant_id_ = tenant_id;
    job_info.job_name_ = ObString(spm_stats_manager);
    job_info.job_ = job_id;
    job_info.job_action_ = ObString("DBMS_SPM.HANDLE_SPM_STATS_JOB_PROC()");
    job_info.lowner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
    job_info.powner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
    job_info.cowner_ = is_oracle_mode ? ObString("SYS") : ObString("oceanbase");
    job_info.job_style_ = ObString("regular");
    job_info.job_type_ = ObString("STORED_PROCEDURE");
    job_info.job_class_ = ObString("DEFAULT_JOB_CLASS");
    job_info.start_date_ = start_usec;
    job_info.end_date_ = end_date;
    job_info.repeat_interval_ = ObString("FREQ=DAYLY; INTERVAL=1");
    job_info.enabled_ = true;
    job_info.auto_drop_ = false;
    job_info.max_run_duration_ = 10 * 60; // 10 min
    job_info.exec_env_ = exec_env;
    job_info.comments_ = ObString("used to handle spm stats");
    job_info.func_type_ = dbms_scheduler::ObDBMSSchedFuncType::STAT_MAINTENANCE_JOB;
  }
  return ret;
}

/* like Oracle, we set 7 windows:
   *  WINDOW_NAME                   REPEAT_INTERVAL                       DURATION
   * MONDAY_WINDOW                freq=daily;byday=MON;byhour=22;          4 hours
   * TUESDAY_WINDOW               freq=daily;byday=TUE;byhour=22;          4 hours
   * WEDNESDAY_WINDOW             freq=daily;byday=WED;byhour=22;          4 hours
   * THURSDAY_WINDOW              freq=daily;byday=THU;byhour=22;          4 hours
   * FRIDAY_WINDOW                freq=daily;byday=FRI;byhour=22;          4 hours
   * SATURDAY_WINDOW              freq=daily;byday=SAT;byhour=6;           20 hours
   * SUNDAY_WINDOW                freq=daily;byday=SUN;byhour=6;           20 hours
   *
   */
int ObDbmsStatsMaintenanceWindow::get_window_job_info(const int64_t current_time,
                                                      const int64_t nth_window,
                                                      const int64_t offset_sec,
                                                      int64_t &start_usec,
                                                      ObSqlString &job_action)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  if (OB_UNLIKELY(nth_window < 1 || nth_window > DAYS_PER_WEEK || current_time <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(nth_window), K(current_time));
  } else if (OB_FAIL(ObTimeConverter::usec_to_ob_time(current_time + offset_sec * 1000000,
                                                      ob_time))) {
    LOG_WARN("failed to usec to ob time", K(ret), K(current_time), K(offset_sec));
  } else if (OB_UNLIKELY(ob_time.parts_[DT_WDAY] < 1 ||
                         ob_time.parts_[DT_WDAY] > DAYS_PER_WEEK)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(ob_time.parts_[DT_WDAY]));
  } else {
    //work day set default start time is 22:00 and non-work day set default start time is 6:00
    int64_t default_start_hour = is_work_day(nth_window) ? DEFAULT_WORKING_DAY_START_HOHR : DEFAULT_NON_WORKING_DAY_START_HOHR;
    //work day set default duration time is 4 hours and non-work day set default duration time is 20 hours
    int64_t default_duration_usec = is_work_day(nth_window) ? DEFAULT_WORKING_DAY_DURATION_USEC : DEFAULT_NON_WORKING_DAY_DURATION_USEC;
    int64_t total_hour_with_trunc = current_time / USEC_OF_HOUR;
    int64_t current_hour = ob_time.parts_[DT_HOUR];
    int64_t current_wday = ob_time.parts_[DT_WDAY];
    LOG_INFO("begin to get window job info", K(current_time), K(total_hour_with_trunc),
                                             K(current_hour), K(current_wday), K(nth_window),
                                             K(default_start_hour), K(default_duration_usec));
    if (OB_FAIL(job_action.append_fmt("DBMS_STATS.GATHER_DATABASE_STATS_JOB_PROC(%ld)",
                                      default_duration_usec))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      int64_t offset_day = nth_window - current_wday;
      if (offset_day < 0) {
        offset_day = offset_day + DAY_OF_WEEK;
      } else if (offset_day  == 0) {
        offset_day = current_hour > default_start_hour ? DAY_OF_WEEK : offset_day;
      }
      int64_t offset_hour = default_start_hour - current_hour + offset_day * HOUR_OF_DAY;
      start_usec = (total_hour_with_trunc + offset_hour) * USEC_OF_HOUR;
      LOG_INFO("succeed to get window job info", K(start_usec), K(offset_hour), K(current_time),
                                                 K(total_hour_with_trunc), K(current_hour),
                                                 K(current_wday), K(nth_window), K(offset_sec),
                                                 K(default_start_hour), K(default_duration_usec),
                                                 K(job_action), K(offset_day));
    }
  }
  return ret;
}

int ObDbmsStatsMaintenanceWindow::is_stats_maintenance_window_attr(const sql::ObSQLSessionInfo *session,
                                                                   const ObString &job_name,
                                                                   const ObString &attr_name,
                                                                   const ObString &val_name,
                                                                   bool &is_window_attr,
                                                                   share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  is_window_attr = false;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(session));
  } else if (is_stats_job(job_name)) {
    //now we just support modify job_action、start_date
    if (0 == attr_name.case_compare("job_action")) {
      const char *history_stats_job = "DBMS_STATS.PURGE_STATS(";
      const char *async_gather_stats_job = "DBMS_STATS.ASYNC_GATHER_STATS_JOB_PROC(";
      const char *maintenance_window_job = "DBMS_STATS.GATHER_DATABASE_STATS_JOB_PROC(";
      if ((0 == job_name.case_compare(opt_stats_history_manager) &&
           !val_name.empty() &&
           0 == strncasecmp(val_name.ptr(), history_stats_job, strlen(history_stats_job))) ||
          (0 == job_name.case_compare(async_gather_stats_job_proc) &&
           !val_name.empty() &&
           0 == strncasecmp(val_name.ptr(), async_gather_stats_job, strlen(async_gather_stats_job))) ||
          (0 != job_name.case_compare(opt_stats_history_manager) &&
           0 != job_name.case_compare(async_gather_stats_job_proc) &&
           !val_name.empty() &&
           0 == strncasecmp(val_name.ptr(), maintenance_window_job, strlen(maintenance_window_job)))) {
        if (OB_FAIL(dml.add_column("job_action", ObHexEscapeSqlStr(val_name)))) {
          LOG_WARN("failed to add column", K(ret));
        } else if (OB_FAIL(dml.add_column("what", ObHexEscapeSqlStr(val_name)))) {
          LOG_WARN("failed to add column", K(ret));
        } else {
          is_window_attr = true;
        }
      } else {
        ret = OB_ERR_DBMS_STATS_PL;
        LOG_WARN("the hour of interval must be between 0 and 24", K(ret));
        LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "the hour of interval must be between 0 and 24");
      }
    } else if (0 == attr_name.case_compare("next_date")) {
      ObObj time_obj;
      ObObj src_obj;
      int64_t current_time = ObTimeUtility::current_time();
      ObArenaAllocator calc_buf("DbmsStatsWindow");
      ObCastCtx cast_ctx(&calc_buf, NULL, CM_NONE, ObCharset::get_system_collation());
      cast_ctx.dtc_params_ = session->get_dtc_params();
      int64_t specify_time = -1;
      int32_t offset_sec = 0;
      src_obj.set_string(ObVarcharType, val_name);
      const ObTimeZoneInfo* tz_info = get_timezone_info(session);
      if (NULL != tz_info) {
        if (OB_FAIL(tz_info->get_timezone_offset(ObTimeUtility::current_time(), offset_sec))) {
          LOG_WARN("failed to get timezone offset", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (lib::is_oracle_mode()) {
        if (OB_FAIL(ObObjCaster::to_type(ObTimestampTZType, cast_ctx, src_obj, time_obj))) {
          LOG_WARN("failed to ObTimestampTZType type", K(ret));
        } else {
          specify_time = time_obj.get_otimestamp_value().time_us_;
        }
      } else if (lib::is_mysql_mode()) {
        if (OB_FAIL(ObObjCaster::to_type(ObDateTimeType, cast_ctx, src_obj, time_obj))) {
          LOG_WARN("failed to ObTimestampType type", K(ret));
        } else {
          specify_time = time_obj.get_datetime() - SEC_TO_USEC(offset_sec);
        }
      }
      if (OB_SUCC(ret)) {
        bool is_valid = false;
        if (OB_FAIL(check_date_validate(job_name, specify_time + SEC_TO_USEC(offset_sec),
                                        current_time + SEC_TO_USEC(offset_sec), is_valid))) {
          LOG_WARN("failed to check date valid", K(ret));
        } else if (!is_valid) {
          ret = OB_ERR_DBMS_STATS_PL;
          LOG_WARN("Invalid date", K(ret));
          LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL,
                         "The date is invalid. Please check wether they are the same day in a week, or the day is passed.");      
        } else if (OB_FAIL(dml.add_time_column("next_date", specify_time))) {
          LOG_WARN("failed to add column", K(ret));
        } else if (OB_FAIL(dml.add_time_column("start_date", specify_time))) {
          LOG_WARN("failed to add column", K(ret));
        } else {
          is_window_attr = true;
          LOG_TRACE("succeed to set next date", K(specify_time));
        }
      }
    } else if (0 == attr_name.case_compare("duration")) {
      // support set duration column.
      const int MAX_SPECIFY_TIME_LEN = 16;
      char specify_time_buf[MAX_SPECIFY_TIME_LEN];
      int64_t pos = val_name.to_string(specify_time_buf, MAX_SPECIFY_TIME_LEN);
      int64_t specify_time = atoll(specify_time_buf);
      if (specify_time < 0 || specify_time > DEFAULT_DAY_INTERVAL_USEC) {
        ret = OB_ERR_DBMS_STATS_PL;
        LOG_WARN("the hour of interval must be between 0 and 24", K(ret));
        LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "the hour of interval must be between 0 and 24");      
      } else if (OB_FAIL(dml.add_column("max_run_duration", specify_time))) {
        LOG_WARN("fail to add column", K(ret));
      } else {
        is_window_attr = true;
        LOG_TRACE("succeed to set max_run_duration", K(val_name));
      }
    } else {/*do nothing*/
      ret = OB_ERR_DBMS_STATS_PL;
      ObSqlString errmsg;
      errmsg.append_fmt("%.*s is not a valid window attribute.", attr_name.length(), attr_name.ptr());
      LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, errmsg.ptr());  
      LOG_WARN("not a valid window attribute", K(errmsg));
    }
  }
  return ret;
}

bool ObDbmsStatsMaintenanceWindow::is_stats_job(const ObString &job_name)
{
  bool is_true = false;
  for (int64_t i = 0; !is_true && i < DAY_OF_WEEK; ++i) {
    if (0 == job_name.case_compare(windows_name[i])) {
      is_true = true;
    }
  }
  if (!is_true) {
    is_true = (0 == job_name.case_compare(opt_stats_history_manager) ||
               0 == job_name.case_compare(async_gather_stats_job_proc));
  }
  return is_true;
}

int ObDbmsStatsMaintenanceWindow::get_time_zone_offset(const ObSysVariableSchema &sys_variable,
                                                       const uint64_t tenant_id,
                                                       int32_t &offset_sec)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  const ObSysVarSchema *sysvar_schema = NULL;
  if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("failed to get oracle mode", K(ret));
  } else if (OB_FAIL(sys_variable.get_sysvar_schema(share::SYS_VAR_TIME_ZONE, sysvar_schema))) {
    LOG_WARN("failed to get sysvar schema", K(ret));
  } else if (OB_ISNULL(sysvar_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(sysvar_schema));
  } else {
    ObArenaAllocator calc_buf(ObModIds::OB_SQL_PARSER);
    char *buf = NULL;
    int32_t buf_len = sysvar_schema->get_value().length();
    if (OB_ISNULL(buf = static_cast<char*>(calc_buf.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(buf_len));
    } else {
      MEMCPY(buf, sysvar_schema->get_value().ptr(), buf_len);
      ObString trimed_tz_str(buf_len, buf);
      trimed_tz_str = trimed_tz_str.trim();
      int ret_more = OB_SUCCESS;
      if (OB_FAIL(ObTimeConverter::str_to_offset(trimed_tz_str, offset_sec, ret_more,
                                                 is_oracle_mode, true))) {
        if (ret != OB_ERR_UNKNOWN_TIME_ZONE) {
          LOG_WARN("fail to convert str_to_offset", K(trimed_tz_str), K(ret));
        } else if (ret_more != OB_SUCCESS) {
          ret = ret_more;
          LOG_WARN("invalid time zone hour or minute", K(trimed_tz_str), K(ret));
        }
      }
      if (OB_ERR_UNKNOWN_TIME_ZONE == ret) {
        ObTimeZoneInfoPos tz_info;
        ObTZMapWrap tz_map_wrap;
        ObTimeZoneInfoManager *tz_info_mgr = NULL;
        if (OB_FAIL(OTTZ_MGR.get_tenant_timezone(tenant_id, tz_map_wrap, tz_info_mgr))) {
          LOG_WARN("get tenant timezone failed", K(ret));
        } else if (OB_ISNULL(tz_info_mgr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tz info mgr is null", K(ret));
        } else if (OB_FAIL(tz_info_mgr->find_time_zone_info(trimed_tz_str, tz_info))) {
          LOG_WARN("fail to find time zone", K(trimed_tz_str), K(ret));
        } else if (OB_FAIL(tz_info.get_timezone_offset(ObTimeUtility::current_time(), offset_sec))) {
          LOG_WARN("failed to get timezone offset", K(ret));
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

int ObDbmsStatsMaintenanceWindow::check_date_validate(const ObString &job_name,
                                                      const int64_t specify_time,
                                                      const int64_t current_time,
                                                      bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObTime ob_time;
  if (specify_time <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(specify_time));
  } else if (current_time > specify_time) {
    is_valid = false;
  } else if (0 == job_name.case_compare(opt_stats_history_manager) ||
             0 == job_name.case_compare(async_gather_stats_job_proc)) {
    is_valid = true;
  } else if (OB_FAIL(ObTimeConverter::usec_to_ob_time(specify_time, ob_time))) {
    LOG_WARN("failed to usec to ob time", K(ret), K(specify_time));
  } else if (OB_UNLIKELY(ob_time.parts_[DT_WDAY] < 1 ||
                         ob_time.parts_[DT_WDAY] > DAYS_PER_WEEK)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(ob_time.parts_[DT_WDAY]));
  } else  {
    if (0 == job_name.case_compare(windows_name[ob_time.parts_[DT_WDAY]-1])) {
      is_valid = true;
    }
  }

  return ret;
}

int ObDbmsStatsMaintenanceWindow::get_async_gather_stats_job_for_upgrade(common::ObMySQLProxy *sql_proxy,
                                                                         const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  int64_t job_id = 0;
  ObString exec_env;
  ObSqlString values_list;
  bool is_join_exists = false;
  //bug:
  ObArenaAllocator allocator("AsyncStatsJob");
  if (OB_FAIL(check_job_exists(sql_proxy, tenant_id, async_gather_stats_job_proc, is_join_exists))) {
    LOG_WARN("failed to check async gather job exists", K(ret));
  } else if (is_join_exists) {
    //do nothing
  } else if (OB_FAIL(get_next_job_id_and_exec_env(sql_proxy, allocator, tenant_id, job_id, exec_env))) {
    LOG_WARN("failed to get async gather stats job id and exec env", K(ret));
  } else if (OB_UNLIKELY(job_id > dbms_scheduler::ObDBMSSchedTableOperator::JOB_ID_OFFSET ||
                         exec_env.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(job_id), K(exec_env));
  } else if (OB_FAIL(share::ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
    LOG_WARN("failed to get tenant compat mode", KR(ret), K(tenant_id));
  } else {
    HEAP_VAR(dbms_scheduler::ObDBMSSchedJobInfo, job_info) {
      if (OB_FAIL(get_async_gather_stats_job_info(lib::Worker::CompatMode::ORACLE == compat_mode,
                                                        tenant_id, job_id, exec_env, job_info))) {
        LOG_WARN("failed to get async gather stats job info", K(ret), K(job_info));
      } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::create_dbms_sched_job(*sql_proxy, tenant_id, job_id, job_info))) {
        LOG_WARN("failed to create dbms sched job", K(ret), K(job_info));
      }
    }
  }
  return ret;
}

int ObDbmsStatsMaintenanceWindow::get_next_job_id_and_exec_env(common::ObMySQLProxy *sql_proxy,
                                                               ObIAllocator &allocator,
                                                               const uint64_t tenant_id,
                                                               int64_t &job_id,
                                                               ObString &exec_env)
{
  int ret = OB_SUCCESS;
  ObSqlString select_sql;
  if (OB_FAIL(select_sql.append_fmt("SELECT tt.job, t.exec_env FROM"\
                                    " %s t, (SELECT max(job) + 1 AS job FROM %s"\
                                             " WHERE tenant_id = %ld and job <= %ld AND job > 0) tt"\
                                    " WHERE t.tenant_id = %ld and t.job_name = '%s' AND t.job = %ld;",
                                    share::OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
                                    share::OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
                                    share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                    dbms_scheduler::ObDBMSSchedTableOperator::JOB_ID_OFFSET,
                                    share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                    opt_stats_history_manager,
                                    OPT_STATS_HISTORY_MANAGER_JOB_ID))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy);
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, tenant_id, select_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(select_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        int64_t get_rows = 0;
        //expected only get one row.
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          int64_t fisrt_col = 0;
          int64_t second_col = 1;
          ObObj obj;
          ObString tmp_exec_env;
          if (get_rows > 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error, expected only one row", K(ret));
          } else if (OB_FAIL(client_result->get_obj(fisrt_col, obj))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(obj.get_int(job_id))) {
            LOG_WARN("failed to get int", K(ret), K(obj));
          } else if (OB_FAIL(client_result->get_obj(second_col, obj))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(obj.get_varchar(tmp_exec_env))) {
            LOG_WARN("failed to get int", K(ret), K(obj));
          } else if (OB_FAIL(ob_write_string(allocator, tmp_exec_env, exec_env))) {
            LOG_WARN("failed to ob write string", K(ret));
          } else {
            ++ get_rows;
          }
        }
        ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
      }
      int tmp_ret = OB_SUCCESS;
      if (NULL != client_result) {
        if (OB_SUCCESS != (tmp_ret = client_result->close())) {
          LOG_WARN("close result set failed", K(ret), K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
    LOG_INFO("succeed to get next job id and exec env", K(ret), K(select_sql), K(job_id), K(exec_env));
  }
  return ret;
}

int ObDbmsStatsMaintenanceWindow::check_job_exists(common::ObMySQLProxy *sql_proxy,
                                                   const uint64_t tenant_id,
                                                   const char* job_name,
                                                   bool &is_join_exists)
{
  int ret = OB_SUCCESS;
  is_join_exists = false;
  ObSqlString select_sql;
  int64_t row_count = 0;
  if (OB_FAIL(select_sql.append_fmt("SELECT count(*) FROM %s WHERE tenant_id = %ld and job_name = '%s';",
                                    share::OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
                                    share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                    job_name))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy);
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, tenant_id, select_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(select_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        //expected only get one row.
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          int64_t idx = 0;
          ObObj obj;
          if (OB_FAIL(client_result->get_obj(idx, obj))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(obj.get_int(row_count))) {
            LOG_WARN("failed to get int", K(ret), K(obj));
          } else if (OB_UNLIKELY(row_count != 2 && row_count != 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(ret), K(row_count));
          } else {
            is_join_exists = row_count > 0;
          }
        }
        ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
      }
      int tmp_ret = OB_SUCCESS;
      if (NULL != client_result) {
        if (OB_SUCCESS != (tmp_ret = client_result->close())) {
          LOG_WARN("close result set failed", K(ret), K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
    LOG_INFO("succeed to check job exists", K(ret), K(select_sql), K(is_join_exists), K(row_count));
  }
  return ret;
}

int ObDbmsStatsMaintenanceWindow::get_spm_stats_upgrade_jobs_sql(common::ObMySQLProxy *sql_proxy,
                                                                 const ObSysVariableSchema &sys_variable,
                                                                 const uint64_t tenant_id,
                                                                 const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  char buf[OB_MAX_PROC_ENV_LENGTH];
  int64_t pos = 0;
  int32_t offset_sec = 0;
  int64_t job_id = 0;
  ObString tmp_exec_env;
  ObArenaAllocator allocator("SpmSchedularJob");
  if (OB_FAIL(get_next_job_id_and_exec_env(sql_proxy, allocator, tenant_id, job_id, tmp_exec_env))) {
    LOG_WARN("failed to get async gather stats job id and exec env", K(ret));
  } else if (OB_UNLIKELY(job_id > dbms_scheduler::ObDBMSSchedTableOperator::JOB_ID_OFFSET ||
                         tmp_exec_env.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(job_id), K(tmp_exec_env));
  } else if (OB_FAIL(sql::ObExecEnv::gen_exec_env(sys_variable, buf, OB_MAX_PROC_ENV_LENGTH, pos))) {
    LOG_WARN("failed to gen exec env", K(ret));
  } else if (OB_FAIL(get_time_zone_offset(sys_variable, tenant_id, offset_sec))) {
    LOG_WARN("failed to get time zone offset", K(ret));
  } else {
    HEAP_VAR(dbms_scheduler::ObDBMSSchedJobInfo, job_info) {
      ObString exec_env(pos, buf);
      if (OB_FAIL(get_spm_stats_job_info(is_oracle_mode, tenant_id, job_id,
                                        offset_sec, exec_env, job_info))) {
        LOG_WARN("failed to get stats history manager job sql", K(ret));
      } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::create_dbms_sched_job(*sql_proxy, tenant_id, job_id, job_info))) {
        LOG_WARN("failed to create dbms sched job", K(ret), K(job_info));
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase

