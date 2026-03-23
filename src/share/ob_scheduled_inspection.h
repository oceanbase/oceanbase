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

#ifndef OCEANBASE_SHARE_OB_SCHEDULED_INSPECTION_H_
#define OCEANBASE_SHARE_OB_SCHEDULED_INSPECTION_H_

#include "sql/engine/ob_exec_context.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h" // ObDBMSSchedJobInfo

namespace oceanbase
{
namespace share
{

static const char *SCHEDULED_RUN_INSPECTION_JOB_NAME = "SCHEDULED_RUN_INSPECTION";
static const char *SCHEDULED_PURGE_RECYCLEBIN_JOB_NAME = "SCHEDULED_PURGE_RECYCLEBIN";

class ObScheduledInspection
{
public:
  // Create run_inspection and purge_recyclebin jobs for a tenant
  static int create_jobs(
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    ObMySQLTransaction &trans);

  // Create run_inspection and purge_recyclebin jobs for a tenant during upgrade
  // If job already exists, skip.
  static int create_jobs_for_upgrade(
    common::ObMySQLProxy *sql_proxy,
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    ObMySQLTransaction &trans);

  // Set attribute for scheduled inspection jobs (SCHEDULED_RUN_INSPECTION, SCHEDULED_PURGE_RECYCLEBIN).
  // Only repeat_interval is allowed
  static int set_attribute(
    common::ObISQLClient &sql_client,
    const common::ObString &attr_name,
    const common::ObString &attr_val,
    const dbms_scheduler::ObDBMSSchedJobInfo &job_info,
    bool &is_valid);

private:
  static common::ObString get_purge_recyclebin_job_action_(const bool is_oracle_mode);

  static int create_job_(
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    const common::ObString &job_name,
    const common::ObString &job_action,
    const common::ObString &repeat_interval,
    const common::ObString &job_type,
    const dbms_scheduler::ObDBMSSchedFuncType func_type,
    ObMySQLTransaction &trans);
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_SCHEDULED_INSPECTION_H_
