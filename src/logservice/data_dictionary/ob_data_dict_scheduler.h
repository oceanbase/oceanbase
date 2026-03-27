/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_DATA_DICTIONARY_SCHEDULER_
#define OCEANBASE_DATA_DICTIONARY_SCHEDULER_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace datadict
{
static const char *SCHEDULED_TRIGGER_DUMP_DATA_DICT_JOB_NAME = "SCHEDULED_TRIGGER_DUMP_DATA_DICT";
class ObDataDictScheduler
{
public:
  // create job after tenant create or cluster upgrade
  static int create_scheduled_trigger_dump_data_dict_job(const schema::ObSysVariableSchema &sys_variable,
      const uint64_t tenant_id,
      const bool is_enabled,
      const bool schedule_at_once,
      common::ObMySQLTransaction &trans);
  static int set_attr_for_trigger_dump_data_dict(
      const sql::ObSQLSessionInfo *session,
      const common::ObString &job_name,
      const common::ObString &attr_name,
      const common::ObString &attr_val_str,
      bool &is_balance_attr,
      share::ObDMLSqlSplicer &dml);
  static bool is_trigger_dump_data_dict_job(const common::ObString &job_name);
};

} // namespace datadict
} // namespace oceanbase
#endif