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

#ifndef OCEANBASE_SHARE_OB_SCHEDULED_TRIGGER_PARTITION_BALANCE_H_
#define OCEANBASE_SHARE_OB_SCHEDULED_TRIGGER_PARTITION_BALANCE_H_

#include "sql/engine/ob_exec_context.h"
//#include "lib/mysqlclient/ob_mysql_transaction.h"

namespace oceanbase
{
namespace share
{
static const char *SCHEDULED_TRIGGER_PARTITION_BALANCE_JOB_NAME = "SCHEDULED_TRIGGER_PARTITION_BALANCE";

class ObScheduledTriggerPartitionBalance
{
public:
  static int create_scheduled_trigger_partition_balance_job(
      const schema::ObSysVariableSchema &sys_variable,
      const uint64_t tenant_id,
      const bool is_enabled,
      common::ObMySQLTransaction &trans);
  static int set_attr_for_trigger_part_balance(
      const sql::ObSQLSessionInfo *session,
      const common::ObString &job_name,
      const common::ObString &attr_name,
      const common::ObString &attr_val_str,
      bool &is_balance_attr,
      share::ObDMLSqlSplicer &dml);
  static int parse_repeat_interval(const common::ObString &repeat_interval_str, int64_t &interval_ts);
  static int check_modify_schedule_interval(
      const uint64_t tenant_id,
      const int64_t interval,
      bool &is_passed);
  static int check_enable_trigger_job(
      const uint64_t tenant_id,
      const common::ObString &job_name);
  static int check_disable_trigger_job(
      const uint64_t tenant_id,
      const common::ObString &job_name);
  static bool is_trigger_job(const common::ObString &job_name);
private:
  static int insert_ignore_new_dbms_scheduler_job_(
      const uint64_t tenant_id,
      const bool is_oracle_mode,
      const bool is_enabled,
      const int64_t job_id,
      const ObString &exec_env,
      const int64_t start_usec,
      ObMySQLTransaction &trans);
  static int get_new_job_id_(const uint64_t tenant_id, common::ObMySQLTransaction &trans, int64_t &job_id);
  static int check_if_scheduled_trigger_pb_enabled_(
      const uint64_t tenant_id,
      bool &enabled);
};
} // end of share
} // end of oceanbase
#endif // OCEANBASE_SHARE_OB_SCHEDULED_TRIGGER_PARTITION_BALANCE_H_