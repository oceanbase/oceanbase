/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_SCHEDULER_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_SCHEDULER_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSScheduler
{
public:
  ObDBMSScheduler() {}
  virtual ~ObDBMSScheduler() {}

public:
 static const int64_t JOB_ID_OFFSET = (1LL<<50);
 static constexpr int64_t VALUE_BUFSIZE = 128;
 static const int64_t BATCH_COUNT = 1024;
static const int64_t CREATE_JOB_PARAMETER_COUNT = 28;
#define DECLARE_FUNC(func) \
  static int func(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

  DECLARE_FUNC(create_job);
  DECLARE_FUNC(drop_job);
  DECLARE_FUNC(run_job);
  DECLARE_FUNC(stop_job);
  DECLARE_FUNC(generate_job_name);
  DECLARE_FUNC(set_job_attribute);
  DECLARE_FUNC(disable);

  DECLARE_FUNC(create_program);
  DECLARE_FUNC(define_program_argument);
  DECLARE_FUNC(enable);
  DECLARE_FUNC(drop_program);
  DECLARE_FUNC(create_job_class);
  DECLARE_FUNC(drop_job_class);
  DECLARE_FUNC(set_job_class_attribute);
  DECLARE_FUNC(purge_log);
  DECLARE_FUNC(get_and_increase_job_id);

#undef DECLARE_FUNC

private:
  static int execute_sql(sql::ObExecContext &ctx, ObSqlString &sql, int64_t &affected_rows);
  static int job_priv_check_impl(sql::ObExecContext &ctx, ObString &job_name, ObArenaAllocator &allocator, dbms_scheduler::ObDBMSSchedJobInfo &job_info);
  static int _check_session_alive(const ObSQLSessionInfo *session);
  static int _do_purge_log(sql::ObExecContext &ctx, int64_t tenant_id, ObString &job_name, const int64_t job_id, const int64_t log_history, const char* table_name, bool is_v2_table);
};

} // end of pl
} // end of oceanbase

#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_SCHEDULER_H_ */