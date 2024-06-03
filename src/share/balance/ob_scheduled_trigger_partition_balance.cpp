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

#include "ob_scheduled_trigger_partition_balance.h"
#include "share/stat/ob_dbms_stats_maintenance_window.h" // ObDbmsStatsMaintenanceWindow
#include "observer/dbms_scheduler/ob_dbms_sched_table_operator.h" // ObDBMSSchedTableOperator

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace share
{
int ObScheduledTriggerPartitionBalance::create_scheduled_trigger_partition_balance_job(
    const ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    const bool is_enabled,
    ObMySQLTransaction &trans)
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
  } else if (OB_FAIL(ObDbmsStatsMaintenanceWindow::get_time_zone_offset(
      sys_variable,
      tenant_id,
      offset_sec))) {
    LOG_WARN("failed to get time zone offset", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObTimeConverter::usec_to_ob_time(
      current_time + offset_sec * USECS_PER_SEC,
      ob_time))) {
    LOG_WARN("failed to usec to ob time", KR(ret), K(current_time), K(offset_sec));
  } else if (OB_FAIL(get_new_job_id_(tenant_id, trans, job_id))) {
    LOG_WARN("get new job_id failed", KR(ret), K(tenant_id));
  } else {
    ObString exec_env(pos, buf);
    int64_t current_hour = ob_time.parts_[DT_HOUR];
    int64_t hours_to_next_day = HOURS_PER_DAY - current_hour;
    const int64_t start_usec = (current_time / USEC_OF_HOUR + hours_to_next_day) * USEC_OF_HOUR; // next day 00:00:00
    // Because of the design flaw in dbms_scheduler, two jobs need to be inserted to avoid duplication
    // TODO: remove the redundant job after dbms_schduler optimization
    if (OB_FAIL(insert_ignore_new_dbms_scheduler_job_(
        tenant_id,
        is_oracle_mode,
        is_enabled,
        0/*job_id*/,
        exec_env,
        start_usec,
        trans))) {
      if (OB_ENTRY_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("job exist, skip insert job with valid job_id", KR(ret), K(tenant_id),
            K(is_oracle_mode), K(is_enabled), K(job_id), K(exec_env), K(start_usec));
      } else {
        LOG_WARN("insert_new_dbms_scheduler_job failed", KR(ret), K(tenant_id),
            K(is_oracle_mode), K(is_enabled), K(job_id), K(exec_env), K(start_usec));
      }
    } else if (OB_FAIL(insert_ignore_new_dbms_scheduler_job_(
        tenant_id,
        is_oracle_mode,
        is_enabled,
        job_id,
        exec_env,
        start_usec,
        trans))) {
      LOG_WARN("insert_new_dbms_scheduler_job failed", KR(ret), K(tenant_id),
          K(is_oracle_mode), K(is_enabled), K(job_id), K(exec_env), K(start_usec));
    } else {
      LOG_INFO("finish create trigger partition balance job", K(tenant_id), K(is_enabled),
          K(offset_sec), K(current_hour), K(hours_to_next_day), K(job_id), K(exec_env),
          K(start_usec), K(affected_rows));
    }
  }
  return ret;
}

int ObScheduledTriggerPartitionBalance::insert_ignore_new_dbms_scheduler_job_(
    const uint64_t tenant_id,
    const bool is_oracle_mode,
    const bool is_enabled,
    const int64_t job_id,
    const ObString &exec_env,
    const int64_t start_usec,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObDMLExecHelper exec(trans, tenant_id);
  ObSqlString sql;
  int64_t affected_rows = 0;

  ObString job_name(SCHEDULED_TRIGGER_PARTITION_BALANCE_JOB_NAME);
  ObString job_action("DBMS_BALANCE.TRIGGER_PARTITION_BALANCE()");
  ObString repeat_interval("FREQ=DAILY; INTERVAL=1");
  const int64_t interval_ts = USECS_PER_DAY;
  const int64_t default_duration_sec = SECS_PER_HOUR * 2;
  const int64_t end_date = 64060560000000000; // 4000-01-01 00:00:00.000000 (same as maintenance_window)

  CK (is_user_tenant(tenant_id));
  CK (job_id >= 0);
  CK (!exec_env.empty());
  CK (start_usec > 0);
  OZ (dml.add_pk_column("tenant_id", schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id))); // 0
  OZ (dml.add_pk_column("job_name", ObHexEscapeSqlStr(job_name)));
  OZ (dml.add_pk_column("job", job_id));
  OZ (dml.add_column("lowner", is_oracle_mode ? ObHexEscapeSqlStr("SYS") : ObHexEscapeSqlStr("root")));
  OZ (dml.add_column("powner", is_oracle_mode ? ObHexEscapeSqlStr("SYS") : ObHexEscapeSqlStr("root")));
  OZ (dml.add_column("cowner", is_oracle_mode ? ObHexEscapeSqlStr("SYS") : ObHexEscapeSqlStr("oceanbase")));
  OZ (dml.add_time_column("next_date", start_usec));
  OZ (dml.add_column("total", 0));
  OZ (dml.add_column("`interval#`", ObHexEscapeSqlStr(repeat_interval)));
  OZ (dml.add_column("flag", 0));
  OZ (dml.add_column("what", ObHexEscapeSqlStr(job_action)));
  OZ (dml.add_column("nlsenv", ObHexEscapeSqlStr(ObString(""))));
  OZ (dml.add_column("field1", ObHexEscapeSqlStr(ObString(""))));
  OZ (dml.add_column("exec_env", ObHexEscapeSqlStr(exec_env)));
  OZ (dml.add_column("job_style", ObHexEscapeSqlStr(ObString("REGULER"))));
  OZ (dml.add_column("program_name", ObHexEscapeSqlStr(ObString(""))));
  OZ (dml.add_column("job_type", ObHexEscapeSqlStr(ObString("STORED_PROCEDURE"))));
  OZ (dml.add_column("job_action", ObHexEscapeSqlStr(job_action)));
  OZ (dml.add_column("number_of_argument", 0));
  OZ (dml.add_time_column("start_date", start_usec));
  OZ (dml.add_column("repeat_interval", ObHexEscapeSqlStr(repeat_interval)));
  OZ (dml.add_raw_time_column("end_date", end_date));
  OZ (dml.add_column("job_class", ObHexEscapeSqlStr(ObString("DEFAULT_JOB_CLASS"))));
  OZ (dml.add_column("enabled", is_enabled));
  OZ (dml.add_column("auto_drop", false));
  OZ (dml.add_column("comments", ObHexEscapeSqlStr(ObString("used to auto trigger partition balance"))));
  OZ (dml.add_column("credential_name", ObHexEscapeSqlStr(ObString(""))));
  OZ (dml.add_column("destination_name", ObHexEscapeSqlStr(ObString(""))));
  OZ (dml.add_column("interval_ts", interval_ts));
  OZ (dml.add_column("max_run_duration", default_duration_sec));
  OZ (dml.finish_row());
  OZ (exec.exec_insert_ignore(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, dml, affected_rows));
  if (OB_FAIL(ret)) {
  } else if (1 == affected_rows) {
    ret = OB_SUCCESS;
    LOG_INFO("insert job successfully", KR(ret), K(affected_rows), K(tenant_id),
        K(is_oracle_mode), K(is_enabled), K(job_id), K(exec_env), K(start_usec));
  } else if (0 == affected_rows) {
    ret = OB_ENTRY_EXIST;
    LOG_INFO("insert ignore job", KR(ret), K(affected_rows), K(tenant_id),
        K(is_oracle_mode), K(is_enabled), K(job_id), K(exec_env), K(start_usec));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert ignore job failed", KR(ret), K(affected_rows), K(tenant_id),
        K(is_oracle_mode), K(is_enabled), K(job_id), K(exec_env), K(start_usec));
  }
  return ret;
}

int ObScheduledTriggerPartitionBalance::set_attr_for_trigger_part_balance(
    const sql::ObSQLSessionInfo *session,
    const ObString &job_name,
    const ObString &attr_name,
    const ObString &attr_val_str,
    bool &is_balance_attr,
    share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  is_balance_attr = false;
  if (OB_ISNULL(session) || job_name.empty() || attr_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(session), K(job_name), K(attr_name), K(attr_val_str));
  } else if (!is_trigger_job(job_name)) {
    is_balance_attr = false;
  } else if (!is_user_tenant(session->get_effective_tenant_id())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not user tenant, can't set attribute for trigger part balance job",
        KR(ret), "tenant_id", session->get_effective_tenant_id());
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "not user tenant, set attribute for SCHEDULED_TRIGGER_PARTITION_BALANCE is");
  } else if (OB_UNLIKELY(attr_val_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty attr val", KR(ret), K(job_name), K(attr_name), K(attr_val_str));
    ObSqlString errmsg;
    (void)errmsg.assign_fmt("%.*s. The value can not be empty.",
        attr_name.length(),
        attr_name.ptr());
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, errmsg.ptr());
  } else if (0 == attr_name.case_compare("job_action")) {
    // not a strict check
    if (attr_val_str.prefix_match_ci("DBMS_BALANCE.TRIGGER_PARTITION_BALANCE(")) {
      if (OB_FAIL(dml.add_column("job_action", ObHexEscapeSqlStr(attr_val_str)))) {
        LOG_WARN("failed to add column", KR(ret), K(job_name), K(attr_name), K(attr_val_str));
      } else if (OB_FAIL(dml.add_column("what", ObHexEscapeSqlStr(attr_val_str)))) {
        LOG_WARN("failed to add column", KR(ret), K(job_name), K(attr_name), K(attr_val_str));
      } else {
        is_balance_attr = true;
        LOG_INFO("succeed to set job_action", K(attr_val_str));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid job_action", K(attr_val_str));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "job_action. The value should be \'DBMS_BALANCE.TRIGGER_PARTITION_BALANCE(x)\'");
    }
  } else if (0 == attr_name.case_compare("next_date")) {
    int64_t next_date_ts = OB_INVALID_TIMESTAMP;
    int32_t offset_sec = 0;
    if (OB_FAIL(ObDbmsStatsMaintenanceWindow::parse_next_date(session, attr_val_str, offset_sec, next_date_ts))) {
      LOG_WARN("parse next date failed", KR(ret), K(attr_val_str));
    } else if (ObTimeUtility::current_time() > next_date_ts) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid next_date", KR(ret), K(attr_val_str), K(next_date_ts));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "NEXT_DATE. Can not be smaller than current time.");
    } else if (OB_FAIL(dml.add_time_column("next_date", next_date_ts))) {
      LOG_WARN("failed to add column", KR(ret));
    } else if (OB_FAIL(dml.add_time_column("start_date", next_date_ts))) {
      LOG_WARN("failed to add column", KR(ret));
    } else {
      is_balance_attr = true;
      LOG_INFO("succeed to set next date", K(attr_val_str), K(next_date_ts));
    }
  } else if (0 == attr_name.case_compare("max_run_duration")) {
    ObSqlString tmp_attr_val;
    int64_t duration = 0;
    // ObString is not safe, convert to ObSqlString
    if (OB_FAIL(tmp_attr_val.assign(attr_val_str))) {
      LOG_WARN("assgin failed", KR(ret), K(attr_val_str));
    } else if (FALSE_IT(duration = atol(tmp_attr_val.ptr()))) {
    } else if (duration < 0 || duration > DEFAULT_DAY_INTERVAL_USEC) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("the hour of interval must be between 0 and 24", KR(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "max_run_duration. The hour of duration must be between 0 and 24");
    } else if (OB_FAIL(dml.add_column("max_run_duration", duration))) {
      LOG_WARN("fail to add column", KR(ret));
    } else {
      is_balance_attr = true;
      LOG_INFO("succeed to set max_run_duration", K(attr_val_str), K(duration));
    }
  } else if (0 == attr_name.case_compare("repeat_interval")) {
    int64_t interval_ts = 0;
    if (OB_FAIL(parse_repeat_interval(attr_val_str, interval_ts))) {
      LOG_WARN("parse repeat interval", KR(ret), K(attr_val_str));
    } else if (OB_FAIL(dml.add_column("interval_ts", interval_ts))) {
      LOG_WARN("add column failed", KR(ret), K(interval_ts));
    } else if (OB_FAIL(dml.add_column("repeat_interval", attr_val_str))) {
      LOG_WARN("add column failed", KR(ret), K(attr_val_str));
    } else if (OB_FAIL(dml.add_column("`interval#`", attr_val_str))) {
      LOG_WARN("add column failed", KR(ret), K(attr_val_str));
    } else {
      is_balance_attr = true;
      LOG_INFO("set repeat_interval successfully", K(attr_val_str), K(interval_ts));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    ObSqlString errmsg;
    (void)errmsg.assign_fmt("%.*s. Not a valid attribute for SCHEDULED_TRIGGER_PARTITION_BALANCE.",
        attr_name.length(),
        attr_name.ptr());
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, errmsg.ptr());
    LOG_WARN("not a valid scheduled trigger partition balance attribute", K(errmsg), K(attr_name));
  }
  return ret;
}

// repeat_interval_str should like "FREQ=x; INTERVAL=x"
int ObScheduledTriggerPartitionBalance::parse_repeat_interval(
    const ObString &repeat_interval_str,
    int64_t &interval_ts)
{
  int ret = OB_SUCCESS;
  interval_ts = 0; // consistent with dbms_scheduler
  ObArenaAllocator allocator;
  ObString tmp_str;
  if (OB_UNLIKELY(repeat_interval_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid repeat interval", KR(ret), K(repeat_interval_str));
  } else if (OB_FAIL(ob_strip_space(allocator, repeat_interval_str, tmp_str))) {
    LOG_WARN("ob_strip_space failed", KR(ret), K(repeat_interval_str));
  } else {
    char *freq_str = NULL;
    char *interval_str = NULL;
    int64_t freq_ts = 0;
    freq_str = strtok_r((NULL == freq_str ? tmp_str.ptr() : NULL), ";", &interval_str);
    if (OB_ISNULL(freq_str) || OB_ISNULL(interval_str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid repeat interval", KR(ret), K(repeat_interval_str));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "REPEAT_INTERVAL. The format should be \'FREQ=x; INTERVAL=x\'");
    } else {
      char freq_val[DEFAULT_BUF_LENGTH] = {0};
      int32_t interval_val = 0;
      errno = 0;
      if (OB_UNLIKELY(1 != sscanf(freq_str, "FREQ=%s", freq_val))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid FREQ str", KR(ret), K(freq_str), K(errno), KERRMSG);
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "FREQ. The format should be \'FREQ=x; INTERVAL=x\'");
      } else if (OB_UNLIKELY(1 != sscanf(interval_str, "INTERVAL=%d", &interval_val))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid INTERVAL str", KR(ret), K(interval_val), K(errno), KERRMSG);
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "INTERVAL. The format should be \'FREQ=x; INTERVAL=x\'");
      } else if (OB_UNLIKELY(interval_val <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid INTERVAL", KR(ret), K(interval_val));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "INTERVAL. The value can not be 0 and should be less than INT32_MAX");
      } else {
        ObString frequency(freq_val);
        if (0 == frequency.case_compare("MINUTELY")) {
          freq_ts = SECS_PER_MIN;
        } else if (0 == frequency.case_compare("HOURLY")) {
          freq_ts = SECS_PER_HOUR;
        } else if (0 == frequency.case_compare("DAILY")) {
          freq_ts = SECS_PER_DAY;
        } else if (0 == frequency.case_compare("WEEKLY")) {
          freq_ts = SECS_PER_DAY * DAYS_PER_WEEK;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("not valid frequency", KR(ret), K(frequency));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "FREQ. FREQ should be MINUTELY/HOURLY/DAILY/WEEKLY");
        }
        if (OB_SUCC(ret)) {
          interval_ts = interval_val * freq_ts * USECS_PER_SEC;
          LOG_INFO("parse_repeat_interval finished", KR(ret), K(interval_ts),
              K(interval_val), K(freq_ts), K(repeat_interval_str));
        }
      }
    }
  }
  return ret;
}

// ID of user job created concurrently during the upgrade is greater than JOB_ID_OFFSET.
// Therefore, it is safe to use the max value + 1 smaller than JOB_ID_OFFSET.
int ObScheduledTriggerPartitionBalance::get_new_job_id_(
    const uint64_t tenant_id,
    ObMySQLTransaction &trans,
    int64_t &job_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  job_id = OB_INVALID_INDEX;
  if (OB_FAIL(sql.assign_fmt("select max(job) + 1 as new_job_id from %s where job <= %ld",
      OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
      dbms_scheduler::ObDBMSSchedTableOperator::JOB_ID_OFFSET))) {
    LOG_WARN("assign fmt failed", KR(ret), K(tenant_id));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(trans.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to get job", KR(ret), K(sql));
      } else if (OB_FAIL(result->get_int("new_job_id", job_id))) {
        LOG_WARN("get int failed", KR(ret), K(tenant_id), K(sql));
      } else {
         LOG_INFO("get new job_id successfully", KR(ret), K(tenant_id), K(job_id));
      }
    }
  }
  return ret;
}

int ObScheduledTriggerPartitionBalance::check_modify_schedule_interval(
    const uint64_t tenant_id,
    const int64_t interval,
    bool &is_passed)
{
  int ret = OB_SUCCESS;
  is_passed = false;
  uint64_t data_version = 0;
  bool job_enabled = false;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || interval < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(interval));
  } else if (0 == interval) {
    is_passed = true;
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("get min data_version failed", KR(ret), K(tenant_id), K(data_version));
  } else if (data_version < DATA_VERSION_4_2_4_0) {
    is_passed = true;
  } else if (OB_FAIL(check_if_scheduled_trigger_pb_enabled_(tenant_id, job_enabled))) {
    LOG_WARN("check enabled failed", KR(ret), K(tenant_id));
  } else if (job_enabled) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not allowed to alter schedule_interval when scheduled job is enabled", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "DBMS_SCHEDULER job \'SCHEDULED_TRIGGER_PARTITION_BALANCE\' is enabled. Operation is");
  }
  return ret;
}

int ObScheduledTriggerPartitionBalance::check_if_scheduled_trigger_pb_enabled_(
    const uint64_t tenant_id,
    bool &enabled)
{
  int ret = OB_SUCCESS;
  enabled = false;
  ObSqlString sql;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "select enabled from %s where job_name = '%s' and job = 0",
      OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
      SCHEDULED_TRIGGER_PARTITION_BALANCE_JOB_NAME))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sql));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(GCTX.sql_proxy_->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          enabled = false;
          LOG_INFO("scheduled_trigger_partition_balance job not found", K(enabled), K(sql));
        } else {
          LOG_WARN("failed to get balance task", KR(ret), K(sql));
        }
      } else if (OB_FAIL(result->get_bool("enabled", enabled))) {
        LOG_WARN("get bool failed", KR(ret), K(sql));
      }
    }
  }
  return ret;
}

int ObScheduledTriggerPartitionBalance::check_enable_trigger_job(
    const uint64_t tenant_id,
    const common::ObString &job_name)
{
  int ret = OB_SUCCESS;
  if (!is_trigger_job(job_name)) {
    // do nothing
  } else if (!is_user_tenant(tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not user tenant", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "not user tenant, enable SCHEDULED_TRIGGER_PARTITION_BALANCE is");
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant config is invalid", KR(ret), K(tenant_id));
    } else {
      const int64_t interval = tenant_config->partition_balance_schedule_interval;
      if (interval > 0) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("not allowed to enable job when schedule_interval is not 0",
            KR(ret), K(tenant_id), K(interval));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "partition_balance_schedule_interval is not 0. Operation is");
      }
    }
  }
  return ret;
}

int ObScheduledTriggerPartitionBalance::check_disable_trigger_job(
    const uint64_t tenant_id,
    const common::ObString &job_name)
{
  int ret = OB_SUCCESS;
  if (!is_trigger_job(job_name)) {
    // do nothing
  } else if (!is_user_tenant(tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not user tenant", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "not user tenant, disable SCHEDULED_TRIGGER_PARTITION_BALANCE is");
  }
  return ret;
}

bool ObScheduledTriggerPartitionBalance::is_trigger_job(const common::ObString &job_name)
{
  return 0 == job_name.case_compare(SCHEDULED_TRIGGER_PARTITION_BALANCE_JOB_NAME);
}

} // end namespace share
} // end namespace oceanbase