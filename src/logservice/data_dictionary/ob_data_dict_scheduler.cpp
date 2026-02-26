/**
* Copyright (c) 2022 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*
* Define DataDictionaryService
*/

#define USING_LOG_PREFIX DATA_DICT

#include "ob_data_dict_scheduler.h"
#include "ob_data_dict_utils.h"
#include "share/stat/ob_dbms_stats_maintenance_window.h"          // ObDbmsStatsMaintenanceWindow
#include "observer/dbms_scheduler/ob_dbms_sched_table_operator.h" // ObDBMSSchedTableOperator

namespace oceanbase
{
namespace datadict
{

using namespace dbms_scheduler;
int ObDataDictScheduler::create_scheduled_trigger_dump_data_dict_job(const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    const bool is_enabled,
    const bool schedule_at_once,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObDMLExecHelper exec(trans, tenant_id);
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t current_time = ObTimeUtility::current_time();
  bool is_oracle_mode = false;
  char buf[OB_MAX_PROC_ENV_LENGTH] = {0};
  int64_t pos = 0;
  int32_t offset_sec = 0;
  ObTime ob_time;
  int64_t job_id = OB_INVALID_INDEX;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must be user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("failed to get oracle mode", KR(ret));
  } else if (OB_FAIL(sql::ObExecEnv::gen_exec_env(sys_variable, buf, OB_MAX_PROC_ENV_LENGTH, pos))) {
    LOG_WARN("failed to gen exec env", KR(ret));
  } else if (OB_FAIL(ObDBMSSchedJobUtils::generate_job_id(tenant_id, job_id))) {
    LOG_WARN("generate_job_id failed", KR(ret), K(tenant_id));
  } else {
    ObString exec_env(pos, buf);
     // if schedule_at_once == true, execute dump data_dictionary immediately(one minute after current_time),
     // otherwise execute dump data_dictionary 120 minutes after current_time
    const int64_t start_usec = schedule_at_once ? (current_time/_MIN_ + 1) * _MIN_ : (current_time/_MIN_ + 120) * _MIN_;
    ObString job_name(SCHEDULED_TRIGGER_DUMP_DATA_DICT_JOB_NAME);
    ObString job_action("DBMS_DATA_DICT.TRIGGER_DUMP()");
    ObString repeat_interval("FREQ=MINUTELY; INTERVAL=120");
    const int64_t default_duration_sec = 7200; // 2 hours
    const int64_t end_date = 64060560000000000; // 4000-01-01 00:00:00.000000 (same as maintenance_window)

    HEAP_VAR(ObDBMSSchedJobInfo, job_info) {
      job_info.tenant_id_ = tenant_id;
      job_info.job_ = job_id;
      job_info.job_name_ = job_name;
      job_info.job_action_ = job_action;
      job_info.lowner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
      job_info.powner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
      job_info.cowner_ = is_oracle_mode ? ObString("SYS") :  ObString("oceanbase");
      job_info.job_style_ = ObString("REGULAR");
      job_info.job_type_ = ObString("PLSQL_BLOCK");
      job_info.job_class_ = ObString("DBMS_DATA_DICT_JOB_CLASS");
      job_info.func_type_ = ObDBMSSchedFuncType::DATA_DICT_DUMP_JOB;
      job_info.start_date_ = start_usec;
      job_info.end_date_ = 64060560000000000; // 4000-01-01 00:00:00.000000
      job_info.repeat_interval_ = repeat_interval;
      job_info.enabled_ = is_enabled;
      job_info.auto_drop_ = false;
      job_info.max_run_duration_ = default_duration_sec;
      job_info.exec_env_ = exec_env;
      job_info.comments_ = ObString("used to dump data_dictionary");

      if (OB_FAIL(ObDBMSSchedJobUtils::create_dbms_sched_job(trans, tenant_id, job_id, job_info))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("finish create data_dict dump job, job duplicated", K(job_info));
        } else {
          LOG_WARN("failed to create data_dict dump job", KR(ret), K(job_info));
        }
      } else {
        LOG_INFO("finish create data_dict dump job", K(job_info));
      }
    } else {
      LOG_WARN("alloc dbms_schduled_job_info for data_dict failed", KR(ret));
    }
  }
  return ret;
}

int ObDataDictScheduler::set_attr_for_trigger_dump_data_dict(
    const sql::ObSQLSessionInfo *session,
    const common::ObString &job_name,
    const common::ObString &attr_name,
    const common::ObString &attr_val_str,
    bool &is_balance_attr,
    share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  return ret;
}

bool ObDataDictScheduler::is_trigger_dump_data_dict_job(const common::ObString &job_name)
{
  return 0 == job_name.case_compare(SCHEDULED_TRIGGER_DUMP_DATA_DICT_JOB_NAME);
}

} // namespace datadict
} // namespace oceanbase
