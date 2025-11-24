/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include "share/ob_scheduled_manage_dynamic_partition.h"

#include "share/stat/ob_dbms_stats_maintenance_window.h"

namespace oceanbase
{
namespace share
{

int ObScheduledManageDynamicPartition::create_jobs(
  const schema::ObSysVariableSchema &sys_variable,
  const uint64_t tenant_id,
  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_daily_job_(sys_variable,
                                tenant_id,
                                trans))) {
    LOG_WARN("fail to create scheduled manage dynamic partition daily job", KR(ret), K(tenant_id));
  } else if (OB_FAIL(create_hourly_job_(sys_variable,
                                        tenant_id,
                                        trans))) {
    LOG_WARN("fail to create scheduled manage dynamic partition hourly job", KR(ret), K(tenant_id));
  }

  return ret;
}

int ObScheduledManageDynamicPartition::create_jobs_for_upgrade(
  common::ObMySQLProxy *sql_proxy,
  const schema::ObSysVariableSchema &sys_variable,
  const uint64_t tenant_id,
  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    bool daily_job_exists = false;
    bool hourly_job_exists = false;
    if (OB_FAIL(ObDbmsStatsMaintenanceWindow::check_job_exists(
                sql_proxy,
                tenant_id,
                SCHEDULED_MANAGE_DYNAMIC_PARTITION_DAILY_JOB_NAME,
                daily_job_exists))) {
      LOG_WARN("fail to check daily job exists", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ObDbmsStatsMaintenanceWindow::check_job_exists(
                       sql_proxy,
                       tenant_id,
                       SCHEDULED_MANAGE_DYNAMIC_PARTITION_HOURLY_JOB_NAME,
                       hourly_job_exists))) {
      LOG_WARN("fail to check hourly job exists", KR(ret), K(tenant_id));
    } else if (!daily_job_exists && OB_FAIL(create_daily_job_(sys_variable,
                                                              tenant_id,
                                                              trans))) {
      LOG_WARN("fail to create scheduled manage dynamic partition daily job", KR(ret), K(tenant_id));
    } else if (!hourly_job_exists && OB_FAIL(create_hourly_job_(sys_variable,
                                                                tenant_id,
                                                                trans))) {
      LOG_WARN("fail to create scheduled manage dynamic partition hourly job", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObScheduledManageDynamicPartition::set_attribute(
  const ObSQLSessionInfo *session,
  const ObString &job_name,
  const ObString &attr_name,
  const ObString &attr_val_str,
  bool &is_scheduled_manage_dynamic_partition_daily_attr,
  ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  is_scheduled_manage_dynamic_partition_daily_attr = false;
  if (OB_ISNULL(session) || job_name.empty() || attr_name.empty() || attr_val_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(session), K(job_name), K(attr_name), K(attr_val_str));
  } else if (FALSE_IT(tenant_id = session->get_effective_tenant_id())) {
  } else if (!is_daily_job_(job_name)) {
    is_scheduled_manage_dynamic_partition_daily_attr = false;
  } else if (!is_user_tenant(tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not user tenant, can't set attribute for SCHEDULED_MANAGE_DYNAMIC_PARTITION_DAILY job", KR(ret), K(tenant_id));
  } else if (0 == attr_name.case_compare("start_date")) {
    int64_t next_date_ts = 0;
    if (OB_FAIL(parse_next_date_(session, attr_val_str, next_date_ts))) {
      LOG_WARN("parse trigger time failed", KR(ret), K(attr_val_str));
    } else if (ObTimeUtility::current_time() > next_date_ts) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid next_date_ts", KR(ret), K(attr_val_str), K(next_date_ts));
    } else if (OB_FAIL(dml.add_time_column("next_date", next_date_ts))) {
      LOG_WARN("failed to add column", KR(ret));
    } else if (OB_FAIL(dml.add_time_column("start_date", next_date_ts))) {
      LOG_WARN("failed to add column", KR(ret));
    } else {
      is_scheduled_manage_dynamic_partition_daily_attr = true;
      LOG_INFO("succeed to set next date", K(attr_val_str), K(next_date_ts));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    ObSqlString errmsg;
    (void)errmsg.assign_fmt("%.*s. Not a valid attribute for SCHEDULED_MANAGE_DYNAMIC_PARTITION_DAILY.",
        attr_name.length(),
        attr_name.ptr());
    LOG_WARN("not a valid SCHEDULED_MANAGE_DYNAMIC_PARTITION_DAILY attribute", KR(ret), K(errmsg), K(attr_name));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, errmsg.ptr());
  }

  return ret;
}

int ObScheduledManageDynamicPartition::create_daily_job_(
  const schema::ObSysVariableSchema &sys_variable,
  const uint64_t tenant_id,
  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t timestamp = 0;
  int32_t offset_sec = 0;
  int64_t current_time = ObTimeUtility::current_time();
  if (OB_FAIL(ObDbmsStatsMaintenanceWindow::get_time_zone_offset(sys_variable,
                                                                 tenant_id,
                                                                 offset_sec))) {
    LOG_WARN("fail to get time zone offset", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_today_zero_hour_timestamp_(offset_sec, timestamp))) {
    LOG_WARN("failed to get time zone offset", KR(ret), K(tenant_id));
  } else {
    const int64_t start_usec_daily = timestamp + USEC_OF_HOUR * HOURS_PER_DAY; // next day 00:00:00
    ObString job_name_daily(SCHEDULED_MANAGE_DYNAMIC_PARTITION_DAILY_JOB_NAME);
    ObString repeat_interval_daily("FREQ=DAILY;INTERVAL=1");
    ObString job_action_daily("DBMS_PARTITION.MANAGE_DYNAMIC_PARTITION(null, 'DAY,WEEK,MONTH,YEAR')");
    if (OB_FAIL(create_job_(sys_variable,
                            tenant_id,
                            start_usec_daily,
                            job_name_daily,
                            repeat_interval_daily,
                            job_action_daily,
                            trans))) {
      LOG_WARN("fail to create scheduled manage dynamic partition job", KR(ret), K(tenant_id), K(job_name_daily));
    }
  }
  return ret;
}

int ObScheduledManageDynamicPartition::create_hourly_job_(
  const schema::ObSysVariableSchema &sys_variable,
  const uint64_t tenant_id,
  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t timestamp = 0;
  int32_t offset_sec = 0;
  int64_t current_time = ObTimeUtility::current_time();
  if (OB_FAIL(ObDbmsStatsMaintenanceWindow::get_time_zone_offset(sys_variable,
                                                                 tenant_id,
                                                                 offset_sec))) {
    LOG_WARN("fail to get time zone offset", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_today_zero_hour_timestamp_(offset_sec, timestamp))) {
    LOG_WARN("failed to get time zone offset", KR(ret), K(tenant_id));
  } else {
    const int64_t start_usec_hourly = (current_time / USEC_OF_HOUR + 1) * USEC_OF_HOUR; // next hour 00:00
    ObString job_name_hourly(SCHEDULED_MANAGE_DYNAMIC_PARTITION_HOURLY_JOB_NAME);
    ObString repeat_interval_hourly("FREQ=HOURLY;INTERVAL=1");
    ObString job_action_hourly("DBMS_PARTITION.MANAGE_DYNAMIC_PARTITION(null, 'HOUR')");
    if (OB_FAIL(create_job_(sys_variable,
                            tenant_id,
                            start_usec_hourly,
                            job_name_hourly,
                            repeat_interval_hourly,
                            job_action_hourly,
                            trans))) {
      LOG_WARN("fail to create scheduled manage dynamic partition job", KR(ret), K(tenant_id), K(job_name_hourly));
    }
  }
  return ret;
}

int ObScheduledManageDynamicPartition::create_job_(
  const schema::ObSysVariableSchema &sys_variable,
  const uint64_t tenant_id,
  const int64_t start_usec,
  const ObString &job_name,
  const ObString &repeat_interval,
  const ObString &job_action,
  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  char buf[OB_MAX_PROC_ENV_LENGTH] = {0};
  int64_t pos = 0;
  int64_t job_id = OB_INVALID_ID;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must be user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("failed to get oracle mode", KR(ret));
  } else if (OB_FAIL(sql::ObExecEnv::gen_exec_env(sys_variable, buf, OB_MAX_PROC_ENV_LENGTH, pos))) {
    LOG_WARN("failed to gen exec env", KR(ret));
  } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::generate_job_id(tenant_id, job_id))) {
    LOG_WARN("generate_job_id failed", KR(ret), K(tenant_id));
  } else {
    ObString exec_env(pos, buf);

    const int64_t default_duration_sec = 3600; // 1hour
    const int64_t end_date = 64060560000000000; // 4000-01-01 00:00:00.000000 (same as maintenance_window)

    HEAP_VAR(dbms_scheduler::ObDBMSSchedJobInfo, job_info) {
      job_info.tenant_id_ = tenant_id;
      job_info.job_ = job_id;
      job_info.job_name_ = job_name;
      job_info.job_action_ = job_action;
      job_info.lowner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
      job_info.powner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
      job_info.cowner_ = is_oracle_mode ? ObString("SYS") :  ObString("oceanbase");
      job_info.job_style_ = ObString("REGULAR");
      job_info.job_type_ = ObString("STORED_PROCEDURE");
      job_info.job_class_ = ObString("DEFAULT_JOB_CLASS");
      job_info.start_date_ = start_usec;
      job_info.end_date_ = end_date;
      job_info.repeat_interval_ = repeat_interval;
      job_info.enabled_ = true;
      job_info.auto_drop_ = false;
      job_info.max_run_duration_ = default_duration_sec;
      job_info.exec_env_ = exec_env;
      job_info.comments_ = ObString("used to perform manage dynamic partition periodically");
      job_info.func_type_ = dbms_scheduler::ObDBMSSchedFuncType::DYNAMIC_PARTITION_MANAGE_JOB;

      if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::create_dbms_sched_job(trans, tenant_id, job_id, job_info))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("finish create manage dynamic partition job, job duplicated", K(job_info));
        } else {
          LOG_WARN("failed to create manage dynamic partition job", KR(ret), K(job_info));
        }
      } else {
        LOG_INFO("finish create manage dynamic partition job", K(job_info));
      }
    } else {
      LOG_WARN("alloc dbms_schduled_job_info for manage dynamic partition failed", KR(ret));
    }
  }
  return ret;
}

int ObScheduledManageDynamicPartition::parse_next_date_(
  const sql::ObSQLSessionInfo *session,
  const common::ObString &next_date_str,
  int64_t &next_date_ts)
{
  int ret = OB_SUCCESS;
  next_date_ts = 0;
  int32_t offset_sec = 0;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null session", KR(ret), K(session), K(next_date_str));
  } else if (next_date_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid next_date", KR(ret), K(next_date_str), K(next_date_ts));
  } else {
    ObObj time_obj;
    ObObj src_obj;
    ObCastCtx cast_ctx;
    cast_ctx.dtc_params_ = session->get_dtc_params();
    src_obj.set_string(ObVarcharType, next_date_str);
    const ObTimeZoneInfo* tz_info = get_timezone_info(session);
    if (OB_ISNULL(tz_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tz info is null", KR(ret));
    } else if (FALSE_IT(offset_sec = tz_info->get_offset())) {
    } else if (lib::is_oracle_mode()) {
      if (OB_FAIL(ObObjCaster::to_type(ObTimestampTZType, cast_ctx, src_obj, time_obj))) {
        LOG_WARN("failed to ObTimestampTZType type", KR(ret), K(src_obj));
      } else {
        next_date_ts = time_obj.get_otimestamp_value().time_us_;
      }
    } else { // mysql mode
      if (OB_FAIL(ObObjCaster::to_type(ObDateTimeType, cast_ctx, src_obj, time_obj))) {
        LOG_WARN("failed to ObDateTimeType type", KR(ret), K(src_obj));
      } else {
        next_date_ts = time_obj.get_datetime() - SEC_TO_USEC(offset_sec);
      }
    }
  }
  return ret;
}

int ObScheduledManageDynamicPartition::get_today_zero_hour_timestamp_(
  const int32_t offset_sec,
  int64_t &timestamp)
{
  int ret = OB_SUCCESS;
  int64_t current_time = ObTimeUtility::current_time();
  ObTime ob_time;

  if (OB_FAIL(ObTimeConverter::usec_to_ob_time(
                     current_time + offset_sec * USECS_PER_SEC,
                     ob_time))) {
    LOG_WARN("failed to usec to ob time", KR(ret), K(current_time), K(offset_sec));
  } else {
    int64_t current_hour = ob_time.parts_[DT_HOUR];
    timestamp = (current_time / USEC_OF_HOUR - current_hour) * USEC_OF_HOUR; // today day 00:00:00
  }

  return ret;
}

bool ObScheduledManageDynamicPartition::is_daily_job_(
  const common::ObString &job_name)
{
  return 0 == job_name.case_compare(SCHEDULED_MANAGE_DYNAMIC_PARTITION_DAILY_JOB_NAME);
}

} // end namespace share
} // end namespace oceanbase
