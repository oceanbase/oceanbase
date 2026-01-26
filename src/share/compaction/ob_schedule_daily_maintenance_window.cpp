/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include "share/compaction/ob_schedule_daily_maintenance_window.h"
#include "share/stat/ob_dbms_stats_maintenance_window.h"
#include "share/ob_scheduled_manage_dynamic_partition.h"
#include "observer/ob_sql_client_decorator.h"
#include "rootserver/freeze/window/ob_window_compaction_helper.h"


namespace oceanbase
{
namespace share
{

/* ---------------------------------- ObDailyWindowJobConfig ---------------------------------- */
const char *ObDailyWindowJobConfig::ObJobConfigKeyStr[] = {
  "version",
  "thread_cnt",
  "prev_plan",
  "prev_group"
};

const char *ObDailyWindowJobConfig::DEFAULT_JOB_CONFIG = "{\"version\":1, \"thread_cnt\":0}";

ObDailyWindowJobConfig::ObDailyWindowJobConfig(ObArenaAllocator &allocator)
  : allocator_(allocator),
    info_(0),
    prev_plan_(),
    prev_group_()
{
  version_ = JOB_CONFIG_VERSION;
  thread_cnt_ = MIN_THREAD_CNT;
}

ObDailyWindowJobConfig::~ObDailyWindowJobConfig()
{
  reset();
}

void ObDailyWindowJobConfig::reset()
{
  prev_plan_.reset();
  prev_group_.reset();
  version_ = JOB_CONFIG_VERSION;
  thread_cnt_ = MIN_THREAD_CNT;
}

void ObDailyWindowJobConfig::reset_prev_info()
{
  prev_plan_.reset();
  prev_group_.reset();
}

bool ObDailyWindowJobConfig::is_valid() const
{
  return (version_ >= JOB_CONFIG_VERSION && version_ <= JOB_CONFIG_VERSION_MAX)
      && (thread_cnt_ >= MIN_THREAD_CNT && thread_cnt_ <= MAX_THREAD_CNT)
      && (prev_plan_.empty() || prev_plan_.length() <= OB_MAX_RESOURCE_PLAN_NAME_LENGTH)
      && (prev_group_.empty() || prev_group_.length() <= OB_MAX_RESOURCE_PLAN_NAME_LENGTH);
}

#define GET_NUMBER_WITH_CHECKER(value, label, it, left_bound, right_bound) \
  do { \
    if (json::JT_NUMBER != it->value_->get_type()) { \
      ret = OB_INVALID_ARGUMENT; \
      LOG_WARN(#label " is not a number", KR(ret), #label, it->value_->get_type()); \
    } else if (it->value_->get_number() < left_bound || it->value_->get_number() > right_bound) { \
      ret = OB_INVALID_ARGUMENT; \
      LOG_WARN(#label " is out of range", KR(ret), #label, it->value_->get_number()); \
    } else { \
      value = it->value_->get_number(); \
    } \
  } while (0)
#define GET_STRING_WITH_CHECKER(value, label, it) \
  do { \
    if (json::JT_STRING != it->value_->get_type()) { \
      ret = OB_INVALID_ARGUMENT; \
      LOG_WARN(#label " is not a string", KR(ret), #label, it->value_->get_type()); \
    } else if (OB_FAIL(value.assign(it->value_->get_string()))) { \
      LOG_WARN("failed to assign string", KR(ret), #label, it->value_->get_string()); \
    } \
  } while (0)

int ObDailyWindowJobConfig::parse_from_string(const common::ObString &job_config)
{
  int ret = OB_SUCCESS;
  reset();
  json::Parser parser;
  json::Value *root = nullptr;
  if (OB_FAIL(parser.init(&allocator_))) {
    LOG_WARN("failed to init parser", KR(ret));
  } else if (OB_FAIL(parser.parse(job_config.ptr(), job_config.length(), root))) {
    LOG_WARN("failed to parse job config", KR(ret), K(job_config));
  } else if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root is null", KR(ret));
  } else if (json::JT_OBJECT != root->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job config is not a json object", KR(ret), K(job_config), "type", root->get_type());
  } else {
    DLIST_FOREACH_X(it, root->get_object(), OB_SUCC(ret))
    {
      if (it->name_.case_compare(ObJobConfigKeyStr[VERSION]) == 0) {
        GET_NUMBER_WITH_CHECKER(version_, version, it, JOB_CONFIG_VERSION, JOB_CONFIG_VERSION_MAX);
      } else if (it->name_.case_compare(ObJobConfigKeyStr[THREAD_CNT]) == 0) {
        GET_NUMBER_WITH_CHECKER(thread_cnt_, thread_cnt, it, MIN_THREAD_CNT, MAX_THREAD_CNT);
      } else if (it->name_.case_compare(ObJobConfigKeyStr[PREV_PLAN]) == 0) {
        GET_STRING_WITH_CHECKER(prev_plan_, prev_plan, it);
      } else if (it->name_.case_compare(ObJobConfigKeyStr[PREV_GROUP]) == 0) {
        GET_STRING_WITH_CHECKER(prev_group_, prev_group, it);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid job config", KR(ret), KPC(this));
    } else {
      LOG_INFO("succeed to parse job config", KPC(this));
    }
  }
  return ret;
}

#undef GET_STRING_WITH_CHECKER
#undef GET_NUMBER_WITH_CHECKER

int ObDailyWindowJobConfig::encode_to_string(ObSqlString &job_config) const
{
  int ret = OB_SUCCESS;
  job_config.reset();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid job config", KR(ret), KPC(this));
  } else if (OB_FAIL(job_config.append_fmt("{\"%s\":%ld, \"%s\":%ld", ObJobConfigKeyStr[VERSION], JOB_CONFIG_VERSION, ObJobConfigKeyStr[THREAD_CNT], thread_cnt_))) {
    LOG_WARN("failed to append version and thread_cnt", KR(ret));
  } else if (OB_NOT_NULL(prev_plan_.ptr()) && OB_FAIL(job_config.append_fmt(", \"%s\":\"%s\"", ObJobConfigKeyStr[PREV_PLAN], prev_plan_.ptr()))) {
    LOG_WARN("failed to append prev_plan", KR(ret));
  } else if (OB_NOT_NULL(prev_group_.ptr()) && OB_FAIL(job_config.append_fmt(", \"%s\":\"%s\"", ObJobConfigKeyStr[PREV_GROUP], prev_group_.ptr()))) {
    LOG_WARN("failed to append prev_group", KR(ret));
  } else if (OB_FAIL(job_config.append("}"))) {
    LOG_WARN("failed to append }", KR(ret));
  } else if (job_config.length() > OB_MAX_JOB_INFO_JOB_CONFIG_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("job config is too long", KR(ret), K(job_config));
  } else {
    LOG_INFO("succeed to encode job config", K(job_config));
  }
  return ret;
}

int ObDailyWindowJobConfig::set_prev_plan(const common::ObString &prev_plan)
{
  int ret = OB_SUCCESS;
  if (is_prev_plan_submitted()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prev_plan is already set", KR(ret), K(prev_plan));
  } else if (OB_FAIL(prev_plan_.assign(prev_plan))) {
    LOG_WARN("failed to assign prev_plan", KR(ret), K(prev_plan));
  }
  return ret;
}

int ObDailyWindowJobConfig::set_prev_group(const common::ObString &prev_group)
{
  int ret = OB_SUCCESS;
  if (is_prev_group_submitted()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prev_group is already set", KR(ret), K(prev_group));
  } else if (OB_FAIL(prev_group_.assign(prev_group))) {
    LOG_WARN("failed to assign prev_group", KR(ret), K(prev_group));
  }
  return ret;
}

/* ---------------------------------- ObScheduleDailyMaintenanceWindow ---------------------------------- */
int ObScheduleDailyMaintenanceWindow::create_jobs_for_tenant_creation(
  const schema::ObSysVariableSchema &sys_variable,
  const uint64_t tenant_id,
  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_daily_job_(sys_variable, tenant_id, trans))) {
    LOG_WARN("failed to create daily maintenance window job", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::create_jobs_for_upgrade(
  common::ObMySQLProxy *sql_proxy,
  const schema::ObSysVariableSchema &sys_variable,
  const uint64_t tenant_id,
  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool job_exists = false;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObDbmsStatsMaintenanceWindow::check_job_exists(
                sql_proxy,
                tenant_id,
                DAILY_MAINTENANCE_WINDOW_JOB_NAME,
                job_exists))) {
    LOG_WARN("failed to check daily job exists", KR(ret), K(tenant_id));
  } else if (job_exists) {
    LOG_INFO("daily maintenance window job already exists", K(tenant_id));
  } else if (OB_FAIL(create_daily_job_(sys_variable, tenant_id, trans))) {
    LOG_WARN("failed to create daily maintenance window job", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::set_attribute(
    sql::ObExecContext &ctx,
    const common::ObString &job_name,
    const common::ObString &attr_name,
    const common::ObString &attr_val_str,
    bool &is_daily_maintenance_window_attr,
    share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  uint64_t tenant_id = OB_INVALID_ID;
  is_daily_maintenance_window_attr = false;
  sql::ObSQLSessionInfo *session = ctx.get_my_session();
  if (OB_ISNULL(session) || job_name.empty() || attr_name.empty() || attr_val_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(session), K(job_name), K(attr_name), K(attr_val_str));
  } else if (FALSE_IT(tenant_id = session->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get min data version", KR(ret));
  } else if (data_version < DATA_VERSION_4_5_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("DAILY_MAINTENANCE_WINDOW is not supported in data version less than 4.5.1.0", KR(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "DAILY_MAINTENANCE_WINDOW in data version less than 4.5.1.0");
  } else if (!is_user_tenant(tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not user tenant, can't set attribute for daily maintenance window job", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set attribute for daily maintenance window non-user tenant");
  } else if (!is_daily_maintenance_job_(job_name)) {
    is_daily_maintenance_window_attr = false;
  } else if (0 == attr_name.case_compare("next_date")) {
    if (OB_FAIL(construct_set_next_date_dml_sql(*session, attr_val_str, is_daily_maintenance_window_attr, dml))) {
      LOG_WARN("failed to construct set next date dml sql", KR(ret));
    }
  } else if (0 == attr_name.case_compare("duration")) {
    if (OB_FAIL(construct_set_duration_dml_sql(ctx, attr_val_str, is_daily_maintenance_window_attr, dml))) {
      LOG_WARN("failed to construct set duration dml sql", KR(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid attribute for DAILY_MAINTENANCE_WINDOW", KR(ret), K(attr_name));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "attribute for DAILY_MAINTENANCE_WINDOW"); // prefix: Incorrect arguments to
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::construct_set_next_date_dml_sql(
    sql::ObSQLSessionInfo &session,
    const common::ObString &attr_val_str,
    bool &is_daily_maintenance_window_attr,
    share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  int64_t next_date_ts = 0;
  if (OB_FAIL(ObScheduledManageDynamicPartition::parse_next_date(&session, attr_val_str, next_date_ts))) {
    LOG_WARN("parse trigger time failed", KR(ret), K(attr_val_str));
  } else if (ObTimeUtility::current_time() > next_date_ts) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid next_date_ts", KR(ret), K(attr_val_str), K(next_date_ts));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "set next_date, it is previous date"); // prefix: Incorrect arguments to
  } else if (OB_FAIL(dml.add_time_column("next_date", next_date_ts))) {
    LOG_WARN("failed to add column", KR(ret));
  } else if (OB_FAIL(dml.add_time_column("start_date", next_date_ts))) {
    LOG_WARN("failed to add column", KR(ret));
  } else {
    is_daily_maintenance_window_attr = true;
    LOG_INFO("succeed to set next date for daily maintenance window job", K(attr_val_str), K(next_date_ts));
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::construct_set_duration_dml_sql(
    sql::ObExecContext &ctx,
    const common::ObString &attr_val_str,
    bool &is_daily_maintenance_window_attr,
    share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  char *cname = nullptr;
  int64_t duration_sec = 0;
  if (OB_FAIL(ob_dup_cstring(ctx.get_allocator(), attr_val_str, cname))) {
    LOG_WARN("failed to dup cstring", KR(ret));
  } else if (OB_FAIL(common::ob_atoll(cname, duration_sec))) {
    LOG_WARN("failed to atoll", KR(ret));
  } else if (duration_sec < MIN_MAX_RUN_DURATION_SEC || duration_sec > DEFAULT_FULL_DAY_SEC) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid duration", KR(ret), K(attr_val_str), K(duration_sec));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "set duration, should be between 60 to 86400 seconds"); // prefix: Incorrect arguments to
  } else if (OB_FAIL(dml.add_column("max_run_duration", duration_sec))) {
    LOG_WARN("failed to add column", KR(ret));
  } else {
    is_daily_maintenance_window_attr = true;
    LOG_INFO("succeed to set duration for daily maintenance window job", K(attr_val_str), K(duration_sec));
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::get_daily_maintenance_window_job_info(
    ObExecContext &ctx,
    dbms_scheduler::ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  sql::ObSQLSessionInfo *session = ctx.get_my_session();
  common::ObISQLClient *sql_client = ctx.get_sql_proxy();
  if (OB_UNLIKELY(!ctx.is_valid() || OB_ISNULL(session) || OB_ISNULL(sql_client))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ctx), K(session), K(sql_client));
  } else if (FALSE_IT(tenant_id = session->get_effective_tenant_id())) {
  } else if (OB_FAIL(get_daily_maintenance_window_job_info(*sql_client, tenant_id, ctx.get_allocator(), job_info))) {
    LOG_WARN("failed to get dbms sched job info", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::get_daily_maintenance_window_job_info(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObIAllocator &allocator,
    dbms_scheduler::ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  const bool is_oracle_tenant = lib::is_oracle_mode();
  if (OB_FAIL(DBMSSchedJobUtils::get_dbms_sched_job_info(sql_client,
                                                         tenant_id,
                                                         is_oracle_tenant,
                                                         DAILY_MAINTENANCE_WINDOW_JOB_NAME,
                                                         allocator,
                                                         job_info))) {
    LOG_WARN("failed to get dbms sched job info", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!job_info.valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid job info", KR(ret), K(tenant_id), K(job_info));
  } else {
    LOG_TRACE("succeed to get daily maintenance window job info", KR(ret), K(tenant_id), K(job_info)); // will called
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::get_daily_maintenance_window_job_info(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    ObIAllocator &allocator,
    dbms_scheduler::ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  ObSQLClientRetryWeak sql_client_retry_weak(&trans, tenant_id, OB_ALL_TENANT_SCHEDULER_JOB_TID);
  if (OB_FAIL(get_daily_maintenance_window_job_info(sql_client_retry_weak, tenant_id, allocator, job_info))) {
    LOG_WARN("failed to get daily maintenance window job info", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::set_thread_count(
    ObExecContext &ctx,
    const uint64_t tenant_id,
    const int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  common::ObISQLClient *sql_client = ctx.get_sql_proxy();
  ObMySQLTransaction trans; // use existing sql proxy
  dbms_scheduler::ObDBMSSchedJobInfo job_info;
  ObArenaAllocator allocator(ctx.get_allocator());
  ObDailyWindowJobConfig job_cfg(allocator);
  ObSqlString new_job_config;

  if (OB_UNLIKELY(!ctx.is_valid() || OB_ISNULL(sql_client))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ctx), K(sql_client));
  } else if (OB_FAIL(trans.start(sql_client, tenant_id))) {
    LOG_WARN("failed to start transaction", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_daily_maintenance_window_job_info(trans, tenant_id, ctx.get_allocator(), job_info))) {
    LOG_WARN("failed to get daily maintenance window job info", KR(ret), K(tenant_id));
  } else if (OB_FAIL(job_cfg.parse_from_string(job_info.job_config_))) {
    LOG_WARN("failed to parse job config", KR(ret), "job_config", job_info.job_config_);
  } else if (job_cfg.thread_cnt_ == thread_cnt) {
    ret = OB_ENTRY_EXIST;
    LOG_WARN("current thread cnt is the same as the current thread cnt, don't need to set", KR(ret), K(thread_cnt));
    LOG_USER_ERROR(OB_ENTRY_EXIST, "new value is the same with the current value");
  } else if (FALSE_IT(job_cfg.thread_cnt_ = thread_cnt)) {
  } else if (OB_FAIL(job_cfg.encode_to_string(new_job_config))) {
    LOG_WARN("failed to encode job config", K(ret), K(job_cfg));
  } else if (OB_FAIL(set_job_config(trans, allocator, tenant_id, new_job_config.string()))) {
    LOG_WARN("failed to set job config", KR(ret), K(new_job_config));
  }

  if (trans.is_started()) {
    int trans_ret = trans.end(OB_SUCC(ret));
    if (OB_SUCCESS != trans_ret) {
      LOG_WARN_RET(trans_ret, "failed to commit trans", KR(ret), K(trans_ret));
      ret = COVER_SUCC(trans_ret);
    }
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::set_job_config(
    ObMySQLTransaction &trans,
    ObIAllocator &allocator,
    const uint64_t tenant_id,
    const ObDailyWindowJobConfig &job_cfg)
{
  int ret = OB_SUCCESS;
  ObSqlString new_job_config;
  if (OB_FAIL(job_cfg.encode_to_string(new_job_config))) {
    LOG_WARN("failed to encode job config", K(ret), K(job_cfg));
  } else if (OB_FAIL(set_job_config(trans, allocator, tenant_id, new_job_config.string()))) {
    LOG_WARN("failed to set job config", K(ret), K(new_job_config));
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::set_job_config(
    ObMySQLTransaction &trans,
    ObIAllocator &allocator,
    const uint64_t tenant_id,
    const common::ObString &new_job_config)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const uint64_t sql_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(dml.add_gmt_modified(now))) {
    LOG_WARN("failed to add gmt modified", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", sql_tenant_id))) {
    LOG_WARN("failed to add tenant it", KR(ret), K(sql_tenant_id));
  } else if (OB_FAIL(dml.add_pk_column("job_name", ObHexEscapeSqlStr(DAILY_MAINTENANCE_WINDOW_JOB_NAME)))) {
    LOG_WARN("failed to add job name", KR(ret));
  } else if (OB_FAIL(dml.add_column("job_config", new_job_config))) {
    LOG_WARN("failed to add job config", KR(ret));
  } else if (OB_FAIL(dml.get_extra_condition().assign("job > 0"))) {
    LOG_WARN("failed to add extra condition", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql))) {
    LOG_WARN("failed to splice update sql", KR(ret));
  } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", KR(ret));
  } else if (OB_UNLIKELY(1 != affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to update job config", KR(ret), K(affected_rows));
  }
  LOG_INFO("[WIN-COMPACTION] Finish set job config for daily maintenance window job", KR(ret), K(tenant_id), K(sql_tenant_id), K(new_job_config), K(sql));
  return ret;
}

int ObScheduleDailyMaintenanceWindow::check_supported(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  uint64_t min_data_version = 0;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("window compaction is only supported for user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, min_data_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (min_data_version < DATA_VERSION_4_5_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("window compaction is not supported now, please upgrade observer first", KR(ret), K(tenant_id), K(min_data_version));
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::check_for_delete_(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const common::ObString &entity,
    const EntityType entity_type)
{
  int ret = OB_SUCCESS;
  bool is_window_compaction_active = false;
  if (OB_UNLIKELY(entity_type >= ENTITY_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entity type", KR(ret), K(entity_type));
  } else if (OB_FAIL(rootserver::ObWindowCompactionHelper::check_window_compaction_global_active(tenant_id, is_window_compaction_active))) {
    LOG_WARN("failed to check window compaction global active", KR(ret), K(tenant_id));
  } else if (!is_window_compaction_active) {
  } else if ((ENTITY_PLAN   == entity_type && is_window_plan(entity))
          || (ENTITY_CGROUP == entity_type && is_window_consumer_group(entity))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot delete entity during window compaction", KR(ret), K(entity), K(entity_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "please stop window compaction firstly, "
                   "delete INNER_DAILY_WINDOW_PLAN/INNER_DAILY_WINDOW_COMPACTION_LOW_GROUP during window compaction");
  } else {
    ObArenaAllocator allocator(ObMemAttr(tenant_id, "CheckDelE"));
    ObDailyWindowJobConfig job_cfg(allocator);
    dbms_scheduler::ObDBMSSchedJobInfo job_info;
    if (OB_FAIL(get_daily_maintenance_window_job_info(trans, tenant_id, allocator, job_info))) {
      LOG_WARN("failed to get daily maintenance window job info", KR(ret), K(tenant_id));
    } else if (OB_FAIL(job_cfg.parse_from_string(job_info.job_config_))) {
      LOG_WARN("failed to parse job config", KR(ret), "job_config", job_info.job_config_);
    } else if ((ENTITY_PLAN   == entity_type && job_cfg.is_prev_plan_submitted()  && 0 == job_cfg.prev_plan_.string().case_compare(entity))
            || (ENTITY_CGROUP == entity_type && job_cfg.is_prev_group_submitted() && 0 == job_cfg.prev_group_.string().case_compare(entity))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("cannot delete prev plan or prev group during window compaction", KR(ret), K(entity), K(job_cfg));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "please stop window compaction firstly, delete prev plan or prev group during window compaction");
    }
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::check_for_delete_plan(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const common::ObString &plan)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_for_delete_(trans, tenant_id, plan, ENTITY_PLAN))) {
    LOG_WARN("failed to check for delete plan", KR(ret), K(tenant_id), K(plan));
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::check_for_delete_consumer_group(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const common::ObString &consumer_group)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_for_delete_(trans, tenant_id, consumer_group, ENTITY_CGROUP))) {
    LOG_WARN("failed to check for delete consumer group", KR(ret), K(tenant_id), K(consumer_group));
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::check_for_delete_plan_directive(
  ObMySQLTransaction &trans,
  const uint64_t tenant_id,
  const common::ObString &plan,
  const common::ObString &consumer_group)
{
  int ret = OB_SUCCESS;
  bool is_window_compaction_active = false;
  if (!is_window_plan(plan) || !is_window_consumer_group(consumer_group)) {
  } else if (OB_FAIL(rootserver::ObWindowCompactionHelper::check_window_compaction_global_active(tenant_id, is_window_compaction_active))) {
    LOG_WARN("failed to check window compaction global active", KR(ret), K(tenant_id));
  } else if (!is_window_compaction_active) {
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot delete plan directive during window compaction", KR(ret), K(plan), K(consumer_group));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "please stop window compaction firstly, "
                   "delete directive of INNER_DAILY_WINDOW_COMPACTION_LOW_GROUP in INNER_DAILY_WINDOW_PLAN during window compaction");
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::check_for_set_consumer_group_mapping(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const common::ObString &attribute,
    const common::ObString &value,
    const bool is_window_compaction_switch)
{
  int ret = OB_SUCCESS;
  bool is_window_compaction_active = false;
  if (0 != attribute.case_compare("function") || 0 != value.case_compare("COMPACTION_LOW")) {
  } else if (OB_FAIL(rootserver::ObWindowCompactionHelper::check_window_compaction_global_active(tenant_id, is_window_compaction_active))) {
    LOG_WARN("failed to check window compaction global active", KR(ret), K(tenant_id));
  } else if (!is_window_compaction_active || is_window_compaction_switch) {
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot set consumer group mapping during window compaction", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "please stop window compaction firstly, "
                   "set consumer group mapping for function COMPACTION_LOW during window compaction");
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::get_next_default_start_timestamp_(
  const int32_t offset_sec,
  int64_t &timestamp)
{
  int ret = OB_SUCCESS;
  int64_t current_time = ObTimeUtility::current_time();
  ObTime ob_time;
  if (OB_FAIL(ObTimeConverter::usec_to_ob_time(current_time + offset_sec * USECS_PER_SEC, ob_time))) {
    LOG_WARN("failed to usec to ob_time", KR(ret), K(current_time), K(offset_sec));
  } else {
    const int64_t current_hour = ob_time.parts_[DT_HOUR];
    const int64_t today_zero_hour = (current_time / USEC_OF_HOUR - current_hour) * USEC_OF_HOUR; // get the timestamp of today's 00:00 a.m.
    const int64_t today_2am = today_zero_hour + 2 * USEC_OF_HOUR; // get the timestamp of today's 02:00 a.m.
    if (current_time < today_2am) {
      timestamp = today_2am;
    } else {
      timestamp = today_2am + USEC_OF_HOUR * HOURS_PER_DAY;
    }
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::create_daily_job_(
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  char buf[OB_MAX_PROC_ENV_LENGTH] = {0};
  int64_t pos = 0;
  int64_t job_id = OB_INVALID_ID;
  int64_t next_start_usec = 0;
  int32_t offset_sec = 0;
  int64_t current_time = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only user tenant need window compaction", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("failed to get oracle mode", KR(ret));
  } else if (OB_FAIL(sql::ObExecEnv::gen_exec_env(sys_variable, buf, OB_MAX_PROC_ENV_LENGTH, pos))) {
    LOG_WARN("failed to gen exec env", KR(ret));
  } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::generate_job_id(tenant_id, job_id))) {
    LOG_WARN("generate_job_id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObDbmsStatsMaintenanceWindow::get_time_zone_offset(sys_variable, tenant_id, offset_sec))) {
    LOG_WARN("fail to get time zone offset", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_next_default_start_timestamp_(offset_sec, next_start_usec))) {
    LOG_WARN("failed to get time zone offset", KR(ret), K(tenant_id));
  } else {
    HEAP_VAR(dbms_scheduler::ObDBMSSchedJobInfo, job_info) {
      job_info.tenant_id_ = tenant_id;
      job_info.job_ = job_id;
      job_info.job_name_ = ObString(DAILY_MAINTENANCE_WINDOW_JOB_NAME);
      job_info.job_action_ = ObString("DBMS_DAILY_MAINTENANCE.TRIGGER_WINDOW_COMPACTION_PROC(true)");
      job_info.lowner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
      job_info.powner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
      job_info.cowner_ = is_oracle_mode ? ObString("SYS") :  ObString("oceanbase");
      job_info.job_style_ = ObString("REGULAR");
      job_info.job_type_ = ObString("STORED_PROCEDURE");
      job_info.job_class_ = ObString("DEFAULT_JOB_CLASS");
      job_info.start_date_ = next_start_usec;
      job_info.end_date_ = DEFAULT_END_DATE;
      job_info.repeat_interval_ = ObString("FREQ=DAILY;INTERVAL=1");
      job_info.enabled_ = true;
      job_info.auto_drop_ = false;
      job_info.max_run_duration_ = DEFAULT_DURATION_SEC; // 6h
      job_info.exec_env_ = ObString(pos, buf);
      job_info.comments_ = ObString("used to do window compaction daily");
      job_info.func_type_ = dbms_scheduler::ObDBMSSchedFuncType::DAILY_MAINTENANCE_JOB;
      job_info.job_config_ = ObString(ObDailyWindowJobConfig::DEFAULT_JOB_CONFIG);

      if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::create_dbms_sched_job(trans, tenant_id, job_id, job_info))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("finish create daily maintenance window job, job duplicated", K(job_info));
        } else {
          LOG_WARN("failed to create daily maintenance window job", KR(ret), K(job_info));
        }
      } else {
        LOG_INFO("succeed to create daily maintenance window job", K(job_info));
      }
    } else {
      LOG_WARN("alloc dbms_schduled_job_info for daily maintenance window failed", KR(ret));
    }
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::check_enable_trigger_job(
    const uint64_t tenant_id,
    const common::ObString &job_name)
{
  int ret = OB_SUCCESS;
  if (!is_daily_maintenance_job_(job_name)) {
    // do nothing
  } else if (OB_FAIL(check_supported(tenant_id))) {
    if (OB_NOT_SUPPORTED == ret) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "enable window compaction in non-user tenant or data version < 4.5.1 is");
    } else {
      LOG_WARN("failed to check supported", KR(ret), K(tenant_id));
    }
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tenant config", KR(ret), K(tenant_id));
    } else if (!tenant_config->enable_window_compaction) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("not allowed to enable when enable_window_compaction is false", KR(ret), K(tenant_id));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "parameter enable_window_compaction is false, enable DAILY_MAINTENANCE_WINDOW is");
    }
  }
  return ret;
}

int ObScheduleDailyMaintenanceWindow::check_disable_trigger_job(
    const uint64_t tenant_id,
    const common::ObString &job_name)
{
  int ret = OB_SUCCESS;
  if (!is_daily_maintenance_job_(job_name)) {
    // do nothing
  } else if (OB_FAIL(check_supported(tenant_id))) {
    if (OB_NOT_SUPPORTED == ret) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "disable window compaction in non-user tenant or data version < 4.5.1 is");
    } else {
      LOG_WARN("failed to check supported", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

bool ObScheduleDailyMaintenanceWindow::is_daily_maintenance_job_(const common::ObString &job_name)
{
  return 0 == job_name.case_compare(DAILY_MAINTENANCE_WINDOW_JOB_NAME);
}

} // end of share
} // end of oceanbase