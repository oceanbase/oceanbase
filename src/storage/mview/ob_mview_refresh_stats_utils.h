/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "share/schema/ob_schema_struct.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace share
{
struct ObScnRange;
} // namespace share
namespace sql
{
class ObExecContext;
class ObSQLSessionInfo;
} // namespace sql
namespace storage
{
class ObMViewRefreshArg;
class ObMViewTransaction;
struct ObMViewRefreshParam;

struct ObMViewDetailTableChangeStats
{
  explicit ObMViewDetailTableChangeStats(const uint64_t detail_table_id)
      : detail_table_id_(detail_table_id), num_rows_ins_(0), num_rows_upd_(0),
        num_rows_del_(0), num_rows_(0) {}

  TO_STRING_KV(K_(detail_table_id), K_(num_rows_ins), K_(num_rows_upd), K_(num_rows_del), K_(num_rows));

  const uint64_t detail_table_id_;
  int64_t num_rows_ins_;
  int64_t num_rows_upd_;
  int64_t num_rows_del_;
  int64_t num_rows_;
};

// Caller-owned columns of __all_mview_refresh_run_stats. Lets the async
// schedule path build the payload without an ObMViewRefreshArg / ObExecContext.
struct ObMViewRefreshRunStatsParam
{
public:
  ObMViewRefreshRunStatsParam()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      run_user_id_(common::OB_INVALID_ID),
      mview_id_(common::OB_INVALID_ID),
      refresh_id_(common::OB_INVALID_ID),
      start_time_(0),
      data_target_scn_(0),
      parallelism_(0),
      nested_(false)
  {
  }
  bool is_valid() const
  {
    return common::OB_INVALID_TENANT_ID != tenant_id_ &&
           common::OB_INVALID_ID != mview_id_ &&
           common::OB_INVALID_ID != refresh_id_;
  }
  TO_STRING_KV(K_(tenant_id), K_(run_user_id), K_(mview_id), K_(refresh_id),
               K_(start_time), K_(data_target_scn), K_(method),
               K_(parallelism), K_(nested));

  uint64_t tenant_id_;
  uint64_t run_user_id_;
  uint64_t mview_id_;
  int64_t refresh_id_;
  int64_t start_time_;
  uint64_t data_target_scn_;
  common::ObString method_;
  int64_t parallelism_;
  bool nested_;
};

// Snapshot of execution context needed to capture the plan of a single
// mview-refresh SQL statement.  Populated by the caller right after execution.
struct ObMViewStmtPlanCaptureInfo
{
  char sql_id_buf_[OB_MAX_SQL_ID_LENGTH + 1];
  char trace_id_buf_[OB_MAX_TRACE_ID_BUFFER_SIZE];
  char svr_ip_buf_[OB_IP_STR_BUFF];
  int64_t svr_port_;
  uint64_t plan_id_;
  uint64_t plan_hash_;
  bool should_capture_;
  ObMViewStmtPlanCaptureInfo()
    : svr_port_(0), plan_id_(0), plan_hash_(0), should_capture_(false)
  {
    sql_id_buf_[0] = '\0';
    trace_id_buf_[0] = '\0';
    svr_ip_buf_[0] = '\0';
  }
  common::ObString sql_id() const { return common::ObString(sql_id_buf_); }
  common::ObString trace_id() const { return common::ObString(trace_id_buf_); }
  common::ObString svr_ip() const { return common::ObString(svr_ip_buf_); }
  TO_STRING_KV("sql_id", sql_id_buf_, "trace_id", trace_id_buf_,
               "svr_ip", svr_ip_buf_, K_(svr_port), K_(plan_id),
               K_(plan_hash), K_(should_capture));
};

class ObMViewRefreshStatsUtils
{
public:
  static int write_run_start(common::ObMySQLProxy *sql_proxy,
                             const ObMViewRefreshRunStatsParam &run_params);
  static int write_mv_start(sql::ObExecContext &ctx,
                            ObMViewTransaction &trans,
                            const ObMViewRefreshParam &refresh_param,
                            const share::schema::ObMVRefreshStatsCollectionLevel collection_level,
                            const share::schema::ObMVRefreshType refresh_type,
                            const int64_t start_time,
                            const int64_t num_steps);
  static int write_stmt_batch(sql::ObExecContext &ctx,
                              sql::ObSQLSessionInfo *session_info,
                              const ObMViewRefreshParam &refresh_param,
                              const common::ObIArray<common::ObString> &refresh_sqls);
  static int write_detail_table_change(sql::ObExecContext &ctx,
                                       const ObMViewRefreshParam &refresh_param,
                                       const ObMViewDetailTableChangeStats &data);
  static int write_stmt_before_step(sql::ObExecContext &ctx,
                                    const ObMViewRefreshParam &refresh_param,
                                    int64_t step,
                                    int64_t start_time);
  static int write_stmt_after_step(sql::ObExecContext &ctx,
                                   const ObMViewRefreshParam &refresh_param,
                                   int64_t step,
                                   int64_t execution_time,
                                   int result,
                                   const ObMViewStmtPlanCaptureInfo &capture_info,
                                   int64_t actual_parallelism);
  static int write_mv_end(sql::ObExecContext &ctx,
                          ObMViewTransaction &trans,
                          const ObMViewRefreshParam &refresh_param,
                          const share::schema::ObMVRefreshStatsCollectionLevel collection_level,
                          const int64_t end_time,
                          const int64_t elapsed_time,
                          const int result,
                          const share::ObScnRange &mview_refresh_scn_range);
  static int write_run_end(common::ObMySQLProxy *sql_proxy,
                           const uint64_t tenant_id,
                           const int64_t refresh_id,
                           const int64_t end_time,
                           const int64_t log_purge_time,
                           const int result,
                           const common::ObString &error_message,
                           const int64_t num_failures);

  static int purge_refresh_stats(common::ObMySQLProxy *sql_proxy,
                                 uint64_t tenant_id,
                                 int64_t retention_period,
                                 int64_t &affected_rows);

private:
  static int execute_trans_write(common::ObMySQLProxy *sql_proxy,
                                 uint64_t tenant_id,
                                 const common::ObSqlString &sql,
                                 int64_t &affected_rows);
};

} // namespace storage
} // namespace oceanbase
