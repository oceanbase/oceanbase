/**
 * Copyright (c) 2025 OceanBase
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

#include "share/ob_lob_check_job_scheduler.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_dbms_stats_maintenance_window.h"

namespace oceanbase
{
namespace share
{

int ObLobCheckJobScheduler::create_lob_check_job(
  const schema::ObSysVariableSchema &sys_variable,
  const uint64_t tenant_id,
  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_job_(sys_variable, tenant_id, trans))) {
    LOG_WARN("fail to create LOB check job", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObLobCheckJobScheduler::create_job_(
  const schema::ObSysVariableSchema &sys_variable,
  const uint64_t tenant_id,
  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  char buf[OB_MAX_PROC_ENV_LENGTH] = {0};
  int64_t pos = 0;
  int64_t job_id = OB_INVALID_ID;
  ObTime ob_time;
  int32_t offset_sec = 0;
  uint64_t current_time = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must be user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("failed to get oracle mode", KR(ret));
  } else if (OB_FAIL(sql::ObExecEnv::gen_exec_env(sys_variable, buf, OB_MAX_PROC_ENV_LENGTH, pos))) {
    LOG_WARN("failed to gen exec env", KR(ret));
  } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::generate_job_id(tenant_id, job_id))) {
    LOG_WARN("generate_job_id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObDbmsStatsMaintenanceWindow::get_time_zone_offset(
      sys_variable,
      tenant_id,
      offset_sec))) {
    LOG_WARN("failed to get time zone offset", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObTimeConverter::usec_to_ob_time(
      current_time + offset_sec * USECS_PER_SEC,
      ob_time))) {
    LOG_WARN("failed to usec to ob time", KR(ret), K(current_time), K(offset_sec));
  }  else {
    ObString exec_env(pos, buf);
    int64_t current_hour = ob_time.parts_[DT_HOUR];
    int64_t hours_to_next_day = HOURS_PER_DAY - current_hour;
    const int64_t start_usec = (current_time / USEC_OF_HOUR + hours_to_next_day) * USEC_OF_HOUR + 4 * USEC_OF_HOUR; // next day 04:00:00
    const int64_t end_date = 64060560000000000; // 4000-01-01 00:00:00.000000
    const int64_t max_run_duration_sec = 7200; // 2 hours (7200 seconds)

    ObString job_name(LOB_CHECK_JOB_NAME);
    ObString repeat_interval("FREQ=WEEKLY; INTERVAL=1");
    ObString job_action;
    job_action = ObString("DBMS_LOB_MANAGER.CHECK_LOB_INNER()");

    HEAP_VAR(dbms_scheduler::ObDBMSSchedJobInfo, job_info) {
      job_info.tenant_id_ = tenant_id;
      job_info.job_ = job_id;
      job_info.job_name_ = job_name;
      job_info.job_action_ = job_action;
      job_info.lowner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
      job_info.powner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
      job_info.cowner_ = is_oracle_mode ? ObString("SYS") : ObString("oceanbase");
      job_info.job_style_ = ObString("REGULAR");
      job_info.job_type_ = ObString("PLSQL_BLOCK");
      job_info.job_class_ = ObString("DEFAULT_JOB_CLASS");
      job_info.start_date_ = start_usec;
      job_info.end_date_ = end_date;
      job_info.repeat_interval_ = repeat_interval;
      job_info.enabled_ = false; // disabled by default, user can enable it manually
      job_info.auto_drop_ = false;
      job_info.max_run_duration_ = max_run_duration_sec;
      job_info.exec_env_ = exec_env;
      job_info.comments_ = ObString("LOB consistency check job, runs weekly to check LOB data consistency");
      job_info.func_type_ = dbms_scheduler::ObDBMSSchedFuncType::LOB_CHECK_JOB;

      if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::create_dbms_sched_job(trans, tenant_id, job_id, job_info))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("finish create LOB check job, job already exists (duplicated)", K(job_info));
        } else {
          LOG_WARN("failed to create LOB check job", KR(ret), K(job_info));
        }
      } else {
        LOG_INFO("successfully created LOB check job", K(job_info));
      }
    }
  }
  return ret;
}

} // end of share
} // end of oceanbase
