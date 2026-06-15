/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_SCHEDULED_RECYCLE_SCHEMA_HISTORY_H_
#define OCEANBASE_SHARE_OB_SCHEDULED_RECYCLE_SCHEMA_HISTORY_H_

#include "sql/engine/ob_exec_context.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h" // ObDBMSSchedJobInfo

namespace oceanbase
{
namespace share
{

static const char *SCHEDULED_RECYCLE_SCHEMA_HISTORY_JOB_NAME = "SCHEDULED_RECYCLE_SCHEMA_HISTORY";

class ObScheduledRecycleSchemaHistory
{
public:
  // Create schema history recycle job for a tenant (user tenant only).
  static int create_job(
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    ObMySQLTransaction &trans);

  // Create schema history recycle job for a tenant during upgrade (user tenant only).
  // If job already exists, skip.
  static int create_job_for_upgrade(
    common::ObMySQLProxy *sql_proxy,
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    ObMySQLTransaction &trans);

  // Set attribute for SCHEDULED_RECYCLE_SCHEMA_HISTORY job.
  // Only repeat_interval is allowed, and only for user tenant.
  static int set_attribute(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const common::ObString &attr_name,
    const common::ObString &attr_val,
    const dbms_scheduler::ObDBMSSchedJobInfo &job_info,
    bool &is_valid);

};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_SCHEDULED_RECYCLE_SCHEMA_HISTORY_H_
