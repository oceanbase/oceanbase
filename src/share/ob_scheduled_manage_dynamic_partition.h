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

#ifndef OCEANBASE_SHARE_OB_SCHEDULED_MANAGE_DYNAMIC_PARTITION_H_
#define OCEANBASE_SHARE_OB_SCHEDULED_MANAGE_DYNAMIC_PARTITION_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace share
{
static const char *SCHEDULED_MANAGE_DYNAMIC_PARTITION_DAILY_JOB_NAME = "SCHEDULED_MANAGE_DYNAMIC_PARTITION_DAILY";
static const char *SCHEDULED_MANAGE_DYNAMIC_PARTITION_HOURLY_JOB_NAME = "SCHEDULED_MANAGE_DYNAMIC_PARTITION_HOURLY";

class ObScheduledManageDynamicPartition
{
public:
  // create scheduled manage dynamic partition daily and hourly jobs
  static int create_jobs(
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    ObMySQLTransaction &trans);

  // create scheduled manage dynamic partition daily and hourly jobs
  // if jobs already exist, skip
  static int create_jobs_for_upgrade(
    common::ObMySQLProxy *sql_proxy,
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    ObMySQLTransaction &trans);

  static int set_attribute(
    const sql::ObSQLSessionInfo *session,
    const common::ObString &job_name,
    const common::ObString &attr_name,
    const common::ObString &attr_val_str,
    bool &is_scheduled_manage_dynamic_partition_daily_attr,
    share::ObDMLSqlSplicer &dml);

private:
  static int create_daily_job_(
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    ObMySQLTransaction &trans);

  static int create_hourly_job_(
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    ObMySQLTransaction &trans);

  static int create_job_(
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    const int64_t start_usec,
    const ObString &job_name,
    const ObString &repeat_interval,
    const ObString &job_action,
    ObMySQLTransaction &trans);
  static int parse_next_date_(
    const sql::ObSQLSessionInfo *session,
    const common::ObString &next_date_str,
    int64_t &next_date_ts);
  static int get_today_zero_hour_timestamp_(const int32_t offset_sec, int64_t &timestamp);
  static bool is_daily_job_(const common::ObString &job_name);
};

} // end of share
} // end of oceanbase

#endif // OCEANBASE_SHARE_OB_SCHEDULED_MANAGE_DYNAMIC_PARTITION_H_
