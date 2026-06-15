/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/ob_scheduled_inspection.h"
#include "share/ob_scheduled_manage_dynamic_partition.h"

#include "share/stat/ob_dbms_stats_maintenance_window.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"

namespace oceanbase
{
namespace share
{

using namespace common;
using namespace dbms_scheduler;

ObString ObScheduledInspection::get_purge_recyclebin_job_action_(const bool is_oracle_mode)
{
  return is_oracle_mode
    ? ObString("EXECUTE IMMEDIATE 'PURGE RECYCLEBIN'")
    : ObString("PURGE RECYCLEBIN");
}

int ObScheduledInspection::create_job_(
  const schema::ObSysVariableSchema &sys_variable,
  const uint64_t tenant_id,
  const ObString &job_name,
  const ObString &job_action,
  const ObString &repeat_interval,
  const ObString &job_type,
  const ObDBMSSchedFuncType func_type,
  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  char buf[OB_MAX_PROC_ENV_LENGTH] = {0};
  int64_t pos = 0;
  int64_t job_id = OB_INVALID_ID;
  const int64_t start_usec = ObTimeUtility::current_time();
  const int64_t end_date = 64060560000000000; // 4000-01-01 00:00:00.000000
  const int64_t default_duration_sec = 3600; // 1 hour

  if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("failed to get oracle mode", KR(ret));
  } else if (OB_FAIL(sql::ObExecEnv::gen_exec_env(sys_variable, buf, OB_MAX_PROC_ENV_LENGTH, pos))) {
    LOG_WARN("failed to gen exec env", KR(ret));
  } else if (OB_FAIL(ObDBMSSchedJobUtils::generate_job_id(tenant_id, job_id))) {
    LOG_WARN("generate_job_id failed", KR(ret), K(tenant_id));
  } else {
    ObString exec_env(pos, buf);
    HEAP_VAR(ObDBMSSchedJobInfo, job_info) {
      job_info.tenant_id_ = tenant_id;
      job_info.job_ = job_id;
      job_info.job_name_ = job_name;
      job_info.job_action_ = job_action;
      job_info.lowner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
      job_info.powner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
      job_info.cowner_ = is_oracle_mode ? ObString("SYS") : ObString("oceanbase");
      job_info.job_style_ = ObString("REGULAR");
      job_info.job_type_ = job_type;
      job_info.job_class_ = ObString("DEFAULT_JOB_CLASS");
      job_info.start_date_ = start_usec;
      job_info.end_date_ = end_date;
      job_info.repeat_interval_ = repeat_interval;
      job_info.enabled_ = true;
      job_info.auto_drop_ = false;
      job_info.max_run_duration_ = default_duration_sec;
      job_info.exec_env_ = exec_env;
      job_info.comments_ = ObString("used to run inspection related scheduled tasks");
      job_info.func_type_ = func_type;

      if (OB_FAIL(ObDBMSSchedJobUtils::create_dbms_sched_job(trans, tenant_id, job_id, job_info))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("finish create scheduled job, job duplicated", K(job_info));
        } else {
          LOG_WARN("failed to create scheduled job", KR(ret), K(job_info));
        }
      } else {
        LOG_INFO("finish create scheduled job", K(job_info));
      }
    }
  }
  return ret;
}

int ObScheduledInspection::create_jobs(
  const schema::ObSysVariableSchema &sys_variable,
  const uint64_t tenant_id,
  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("failed to get oracle mode", KR(ret));
  } else if (OB_FAIL(create_job_(sys_variable,
                                 tenant_id,
                                 ObString(SCHEDULED_RUN_INSPECTION_JOB_NAME),
                                 ObString("DBMS_SCHEMA.RUN_INSPECTION()"),
                                 ObString("FREQ=SECONDLY; INTERVAL=1500"),
                                 ObString("STORED_PROCEDURE"),
                                 ObDBMSSchedFuncType::RUN_INSPECTION_JOB,
                                 trans))) {
    LOG_WARN("fail to create scheduled run inspection job", KR(ret), K(tenant_id));
  } else if (!is_meta_tenant(tenant_id)
             && OB_FAIL(create_job_(sys_variable,
                                    tenant_id,
                                    ObString(SCHEDULED_PURGE_RECYCLEBIN_JOB_NAME),
                                    get_purge_recyclebin_job_action_(is_oracle_mode),
                                    ObString("FREQ=MINUTELY; INTERVAL=10"),
                                    ObString("PLSQL_BLOCK"),
                                    ObDBMSSchedFuncType::PURGE_RECYCLEBIN_JOB,
                                    trans))) {
    LOG_WARN("fail to create scheduled purge recyclebin job", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObScheduledInspection::create_jobs_for_upgrade(
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
    bool run_inspection_exists = false;
    bool purge_recyclebin_exists = false;
    bool is_oracle_mode = false;
    if (OB_FAIL(ObDbmsStatsMaintenanceWindow::check_job_exists(
                sql_proxy,
                tenant_id,
                SCHEDULED_RUN_INSPECTION_JOB_NAME,
                run_inspection_exists))) {
      LOG_WARN("fail to check run inspection job exists", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ObDbmsStatsMaintenanceWindow::check_job_exists(
                       sql_proxy,
                       tenant_id,
                       SCHEDULED_PURGE_RECYCLEBIN_JOB_NAME,
                       purge_recyclebin_exists))) {
      LOG_WARN("fail to check purge recyclebin job exists", KR(ret), K(tenant_id));
    } else if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
      LOG_WARN("failed to get oracle mode", KR(ret));
    } else if (!run_inspection_exists
               && OB_FAIL(create_job_(sys_variable,
                                      tenant_id,
                                      ObString(SCHEDULED_RUN_INSPECTION_JOB_NAME),
                                      ObString("DBMS_SCHEMA.RUN_INSPECTION()"),
                                      ObString("FREQ=SECONDLY; INTERVAL=1500"),
                                      ObString("STORED_PROCEDURE"),
                                      ObDBMSSchedFuncType::RUN_INSPECTION_JOB,
                                      trans))) {
      LOG_WARN("fail to create scheduled run inspection job", KR(ret), K(tenant_id));
    } else if (!purge_recyclebin_exists
               && !is_meta_tenant(tenant_id)
               && OB_FAIL(create_job_(sys_variable,
                                      tenant_id,
                                      ObString(SCHEDULED_PURGE_RECYCLEBIN_JOB_NAME),
                                      get_purge_recyclebin_job_action_(is_oracle_mode),
                                      ObString("FREQ=MINUTELY; INTERVAL=10"),
                                      ObString("PLSQL_BLOCK"),
                                      ObDBMSSchedFuncType::PURGE_RECYCLEBIN_JOB,
                                      trans))) {
      LOG_WARN("fail to create scheduled purge recyclebin job", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObScheduledInspection::set_attribute(
  common::ObISQLClient &sql_client,
  sql::ObSQLSessionInfo *session,
  const common::ObString &attr_name,
  const common::ObString &attr_val,
  const dbms_scheduler::ObDBMSSchedJobInfo &job_info,
  bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (job_info.is_inspection_job() && 0 != attr_name.case_compare("repeat_interval")) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT,
      "SET_ATTRIBUTE for SCHEDULED_RUN_INSPECTION job. Only repeat_interval is supported");
    LOG_WARN("attribute not supported for SCHEDULED_RUN_INSPECTION", KR(ret), K(attr_name));
  } else if (0 == attr_name.case_compare("repeat_interval")) {
    ObObj attr_val_obj;
    attr_val_obj.set_string(ObVarcharType, attr_val);
    if (OB_FAIL(ObDBMSSchedJobUtils::update_dbms_sched_job_info(sql_client, job_info, attr_name, attr_val_obj, false))) {
      LOG_WARN("failed to update job info", KR(ret), K(attr_name), K(attr_val));
    }
  } else if (0 == attr_name.case_compare("start_date")) {
    int64_t start_date_ts = OB_INVALID_TIMESTAMP;
    if (OB_FAIL(ObScheduledManageDynamicPartition::parse_next_date(session, attr_val, start_date_ts))) {
      LOG_WARN("failed to parse start_date", KR(ret), K(attr_val));
    } else if (ObTimeUtility::current_time() > start_date_ts) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "start_date: cannot be in the past");
      LOG_WARN("start_date cannot be in the past", KR(ret), K(attr_val), K(start_date_ts));
    } else {
      ObObj attr_val_obj;
      attr_val_obj.set_time(start_date_ts);
      if (OB_FAIL(ObDBMSSchedJobUtils::update_dbms_sched_job_info(sql_client, job_info, attr_name, attr_val_obj, false))) {
        LOG_WARN("failed to update job info", KR(ret), K(attr_name), K(attr_val));
      }
    }
  } else if (0 == attr_name.case_compare("max_run_duration")) {
    ObObj attr_val_obj;
    attr_val_obj.set_string(ObVarcharType, attr_val);
    if (OB_FAIL(ObDBMSSchedJobUtils::update_dbms_sched_job_info(sql_client, job_info, attr_name, attr_val_obj, false))) {
      LOG_WARN("failed to update job info", KR(ret), K(attr_name), K(attr_val));
    }
  } else {
    is_valid = false;
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT,
      "SET_ATTRIBUTE for SCHEDULED_PURGE_RECYCLEBIN job. Supported attributes: repeat_interval, start_date, max_run_duration");
    LOG_WARN("unsupported attribute", KR(ret), K(attr_name));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
