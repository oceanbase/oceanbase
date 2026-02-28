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

#ifndef OCEANBASE_SHARE_OB_SCHEDULE_DAILY_MAINTENANCE_WINDOW_H_
#define OCEANBASE_SHARE_OB_SCHEDULE_DAILY_MAINTENANCE_WINDOW_H_

#include "lib/json/ob_json.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"

namespace oceanbase
{
namespace share
{

/**
 * The job_config is a json-like string, which is used to store the job config and job runtime state for daily maintenance window.
 * The format is:
 * {
 *   "version": 1,        // used for compatibility for newly added fields.
 *   "thread_cnt": 0,     // the thread count for compaction low function (major/medium).
 *                        // default value is 0, means the maximum cpu count allowed for compaction low in current tenant.
 *   "prev_plan": "xxx",  // optional, the previous resource manager plan before switching to INNER_DAILY_WINDOW_PLAN.
 *   "prev_group": "xxx", // optional, the previous consumer group before switching to INNER_DAILY_WINDOW_COMPACTION_LOW_GROUP.
 * }
 *
 * The job_config has only 2 modes:
 * - 1: {"version": x, "thread_cnt": x}
 *      The default mode, means resource manager plan and consumer group are not switched to window plan and consumer group yet, prev_plan and prev_group are both null (not in job_config).
 *
 * - 2: {"version": x, "thread_cnt": x, "prev_plan": "xxx", "prev_group": "xxx"}
 *      READY or FINISHED to switch to window plan and consumer group.
 *      If there is no active resource manager plan and consumer group before switching, prev_plan and prev_group are both "" (empty string, not null).
 *
 * Example:
 * 1. Initial state (no window compaction):
 *    {"version":1, "thread_cnt":0}
 *
 * 2. User set thread count to 20:
 *    {"version":1, "thread_cnt":20}
 *
 * 3. Window compaction started, saved previous state:
 *    {"version":1, "thread_cnt":20, "prev_plan":"GLOBAL_PLAN", "prev_group":"BACKGROUND_GROUP"}
 *
 * 4. Window compaction started, but no previous plan and previous consumer group were set:
 *    {"version":1, "thread_cnt":20, "prev_plan":"", "prev_group":""}
 *    Attention: empty strings ("") mean "explicitly no plan", different from missing fields
 *
 * 5. Window compaction started, but no previous plan was set:
 *    {"version":1, "thread_cnt":20, "prev_plan":"", "prev_group":"BACKGROUND_GROUP"}
 *
 * 6. Window compaction started, but no previous consumer group was set:
 *    {"version":1, "thread_cnt":20, "prev_plan":"GLOBAL_PLAN", "prev_group":""}
 **/
struct ObDailyWindowJobConfig
{
public:
  enum ObJobConfigKey
  {
    VERSION = 0,
    THREAD_CNT = 1,
    PREV_PLAN = 2,
    PREV_GROUP = 3,
    CONFIG_KEY_MAX
  };
  static const char *ObJobConfigKeyStr[CONFIG_KEY_MAX];
  static const char *DEFAULT_JOB_CONFIG;
public:
  ObDailyWindowJobConfig(ObArenaAllocator &allocator);
  ~ObDailyWindowJobConfig();
  void reset();
  void reset_prev_info();
  bool is_valid() const;
  int parse_from_string(const common::ObString &job_config);
  int encode_to_string(ObSqlString &job_config) const;
  int set_prev_plan(const common::ObString &prev_plan);
  int set_prev_group(const common::ObString &prev_group);
  // prev_plan.ptr() is null means no prev plan is set in job_config. Default value of resource_manager_plan is "" (empty string, not null).
  // prev_group.ptr() is null means no prev group is set in job_config. If COMPACTION_LOW function doesn't not map to any consumer group, take it as ""(empty string, not null).
  OB_INLINE bool is_prev_plan_submitted() const { return OB_NOT_NULL(prev_plan_.ptr()); };
  OB_INLINE bool is_prev_group_submitted() const { return OB_NOT_NULL(prev_group_.ptr()); };
  OB_INLINE bool is_prev_info_empty() const { return OB_ISNULL(prev_plan_.ptr()) && OB_ISNULL(prev_group_.ptr()); }
  OB_INLINE bool is_prev_info_submitted() const { return is_prev_plan_submitted() && is_prev_group_submitted(); }
  TO_STRING_KV(K_(version), K_(thread_cnt), K_(prev_plan), K_(prev_group));
public:
  static constexpr int64_t MIN_THREAD_CNT = 0;
  static constexpr int64_t MAX_THREAD_CNT = 100;
  static constexpr int64_t JOB_CONFIG_VERSION = 1; // when add new version, please update DEFAULT_JOB_CONFIG
  static constexpr int64_t JOB_CONFIG_VERSION_MAX = 1;
  static constexpr int64_t DWSC_VERSION_ONE_BYTE = 8;
  static constexpr int64_t DWSC_RESERVED_BITS = 48;
public:
  ObArenaAllocator &allocator_;
  union {
    int64_t info_;
    struct {
      uint64_t version_    : DWSC_VERSION_ONE_BYTE;
      uint64_t thread_cnt_ : DWSC_VERSION_ONE_BYTE;
      uint64_t reserved_   : DWSC_RESERVED_BITS;
    };
  };
  ObSqlString prev_plan_; // ObString does not contain \0, it's unsafe to use ptr()
  ObSqlString prev_group_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDailyWindowJobConfig);
};


class ObScheduleDailyMaintenanceWindow
{
public:
  enum EntityType
  {
    ENTITY_PLAN = 0,
    ENTITY_CGROUP = 1,
    ENTITY_MAX
  };
public:
  // create daily maintenance window job when create tenant
  static int create_jobs_for_tenant_creation(
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    ObMySQLTransaction &trans);
  // create daily maintenance window job when upgrade. if job already exists, skip
  static int create_jobs_for_upgrade(
    common::ObMySQLProxy *sql_proxy,
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    ObMySQLTransaction &trans);
  static int set_attribute(
    ObExecContext &ctx,
    const common::ObString &job_name,
    const common::ObString &attr_name,
    const common::ObString &attr_val_str,
    bool &is_daily_maintenance_window_attr,
    share::ObDMLSqlSplicer &dml);
  static int get_daily_maintenance_window_job_info(
    ObExecContext &ctx,
    dbms_scheduler::ObDBMSSchedJobInfo &job_info);
  static int get_daily_maintenance_window_job_info(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObIAllocator &allocator,
    dbms_scheduler::ObDBMSSchedJobInfo &job_info);
  static int get_daily_maintenance_window_job_info(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    ObIAllocator &allocator,
    dbms_scheduler::ObDBMSSchedJobInfo &job_info);
  static int set_thread_count(
    ObExecContext &ctx,
    const uint64_t tenant_id,
    const int64_t thread_cnt);
  static int set_job_config(
    ObMySQLTransaction &trans,
    ObIAllocator &allocator,
    const uint64_t tenant_id,
    const ObDailyWindowJobConfig &job_cfg);
  static int set_job_config(
    ObMySQLTransaction &trans,
    ObIAllocator &allocator,
    const uint64_t tenant_id,
    const common::ObString &new_job_config);
  static int check_supported(const uint64_t tenant_id);
  static int check_for_delete_plan(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const common::ObString &plan);
  static int check_for_delete_consumer_group(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const common::ObString &consumer_group);
  static int check_for_delete_plan_directive(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const common::ObString &plan,
    const common::ObString &consumer_group);
  static int check_for_set_consumer_group_mapping(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const common::ObString &attribute,
    const common::ObString &value,
    const bool is_window_compaction_switch);
  static int check_enable_trigger_job(
    const uint64_t tenant_id,
    const common::ObString &job_name);
  static int check_disable_trigger_job(
    const uint64_t tenant_id,
    const common::ObString &job_name);
public:
  static bool is_window_plan(const common::ObString &plan) { return 0 == plan.case_compare(INNER_DAILY_WINDOW_PLAN_NAME); }
  static bool is_window_consumer_group(const common::ObString &consumer_group) { return 0 == consumer_group.case_compare(INNER_DAILY_WINDOW_COMPACTION_LOW_GROUP_NAME); }
private:
  using DBMSSchedJobUtils = dbms_scheduler::ObDBMSSchedJobUtils;
  static constexpr int64_t SEC_PER_HOUR = 60 * 60;
  static constexpr int64_t DEFAULT_DURATION_SEC = 6 * SEC_PER_HOUR; // 6h
  static constexpr int64_t DEFAULT_FULL_DAY_SEC = 24 * SEC_PER_HOUR;  // 24h
  static constexpr int64_t MIN_MAX_RUN_DURATION_SEC = 60; // 1 minute
  static constexpr int64_t DEFAULT_END_DATE = 64060560000000000; // 4000-01-01 00:00:00.000000 (the same as dbms_stats_maintenance_window)
private:
  static int get_next_default_start_timestamp_(
      const int32_t offset_sec,
      int64_t &timestamp);
  static int create_daily_job_(
      const schema::ObSysVariableSchema &sys_variable,
      const uint64_t tenant_id,
      ObMySQLTransaction &trans);
  static bool is_daily_maintenance_job_(const common::ObString &job_name);
  static int construct_set_next_date_dml_sql(
      sql::ObSQLSessionInfo &session,
      const common::ObString &attr_val_str,
      bool &is_daily_maintenance_window_attr,
      share::ObDMLSqlSplicer &dml);
  static int construct_set_duration_dml_sql(
      sql::ObExecContext &ctx,
      const common::ObString &attr_val_str,
      bool &is_daily_maintenance_window_attr,
      share::ObDMLSqlSplicer &dml);
  static int check_for_delete_(
      ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const common::ObString &entity,
      const EntityType entity_type);
};

} // end of share
} // end of oceanbase

#endif // OCEANBASE_SHARE_OB_DAILY_MAINTENANCE_WINDOW_H_