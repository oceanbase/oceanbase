/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/ob_scheduled_recycle_schema_history.h"

#include "share/config/ob_server_config.h"
#include "share/stat/ob_dbms_stats_maintenance_window.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"

namespace oceanbase
{
namespace share
{

using namespace common;
using namespace dbms_scheduler;

int ObScheduledRecycleSchemaHistory::create_job(
  const schema::ObSysVariableSchema &sys_variable,
  const uint64_t tenant_id,
  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  char buf[OB_MAX_PROC_ENV_LENGTH] = {0};
  int64_t pos = 0;
  int64_t job_id = OB_INVALID_ID;

  ObSqlString repeat_interval;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must be user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("failed to get oracle mode", KR(ret));
  } else if (OB_FAIL(sql::ObExecEnv::gen_exec_env(sys_variable, buf, OB_MAX_PROC_ENV_LENGTH, pos))) {
    LOG_WARN("failed to gen exec env", KR(ret));
  } else if (OB_FAIL(ObDBMSSchedJobUtils::generate_job_id(tenant_id, job_id))) {
    LOG_WARN("generate_job_id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(repeat_interval.assign("FREQ=MINUTELY; INTERVAL=10"))) {
    LOG_WARN("fail to build repeat interval", KR(ret), K(tenant_id));
  } else {
    ObString exec_env(pos, buf);
    ObString job_name(SCHEDULED_RECYCLE_SCHEMA_HISTORY_JOB_NAME);
    ObString job_action("DBMS_SCHEMA.RECYCLE_SCHEMA_HISTORY()");
    const int64_t start_usec = ObTimeUtility::current_time();
    const int64_t end_date = 64060560000000000; // 4000-01-01 00:00:00.000000
    const int64_t default_duration_sec = 3600; // 1 hour

    HEAP_VAR(ObDBMSSchedJobInfo, job_info) {
      job_info.tenant_id_ = tenant_id;
      job_info.job_ = job_id;
      job_info.job_name_ = job_name;
      job_info.job_action_ = job_action;
      job_info.lowner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
      job_info.powner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
      job_info.cowner_ = is_oracle_mode ? ObString("SYS") : ObString("oceanbase");
      job_info.job_style_ = ObString("REGULAR");
      job_info.job_type_ = ObString("STORED_PROCEDURE");
      job_info.job_class_ = ObString("DEFAULT_JOB_CLASS");
      job_info.start_date_ = start_usec;
      job_info.end_date_ = end_date;
      job_info.repeat_interval_ = repeat_interval.string();
      job_info.enabled_ = true;
      job_info.auto_drop_ = false;
      job_info.max_run_duration_ = default_duration_sec;
      job_info.exec_env_ = exec_env;
      job_info.comments_ = ObString("used to recycle schema history periodically");
      job_info.func_type_ = ObDBMSSchedFuncType::SCHEMA_HISTORY_RECYCLE_JOB;

      if (OB_FAIL(ObDBMSSchedJobUtils::create_dbms_sched_job(trans, tenant_id, job_id, job_info))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("finish create schema history recycle job, job duplicated", K(job_info));
        } else {
          LOG_WARN("failed to create schema history recycle job", KR(ret), K(job_info));
        }
      } else {
        LOG_INFO("finish create schema history recycle job", K(job_info));
      }
    }
  }

  return ret;
}

int ObScheduledRecycleSchemaHistory::create_job_for_upgrade(
  common::ObMySQLProxy *sql_proxy,
  const schema::ObSysVariableSchema &sys_variable,
  const uint64_t tenant_id,
  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must be user tenant", KR(ret), K(tenant_id));
  } else {
    bool job_exists = false;
    if (OB_FAIL(ObDbmsStatsMaintenanceWindow::check_job_exists(
        sql_proxy,
        tenant_id,
        SCHEDULED_RECYCLE_SCHEMA_HISTORY_JOB_NAME,
        job_exists))) {
      LOG_WARN("fail to check schema history recycle job exists", KR(ret), K(tenant_id));
    } else if (job_exists) {
      // skip
      LOG_INFO("schema history recycle job already exists, skip", K(tenant_id));
    } else if (OB_FAIL(create_job(sys_variable, tenant_id, trans))) {
      LOG_WARN("fail to create schema history recycle job", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObScheduledRecycleSchemaHistory::set_attribute(
  common::ObISQLClient &sql_client,
  const uint64_t tenant_id,
  const common::ObString &attr_name,
  const common::ObString &attr_val,
  const dbms_scheduler::ObDBMSSchedJobInfo &job_info,
  bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (!is_user_tenant(tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not user tenant, can't set attribute for SCHEDULED_RECYCLE_SCHEMA_HISTORY job", KR(ret), K(tenant_id));
  } else if (0 != attr_name.case_compare("repeat_interval")) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "only repeat_interval is supported for SCHEDULED_RECYCLE_SCHEMA_HISTORY");
    LOG_WARN("only repeat_interval is supported", KR(ret), K(attr_name));
  } else {
    is_valid = true;
    ObObj attr_val_obj;
    attr_val_obj.set_string(ObVarcharType, attr_val);
    if (OB_FAIL(ObDBMSSchedJobUtils::update_dbms_sched_job_info(sql_client, job_info, attr_name, attr_val_obj, false))) {
      LOG_WARN("failed to update job info", KR(ret), K(tenant_id), K(attr_name), K(attr_val));
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
