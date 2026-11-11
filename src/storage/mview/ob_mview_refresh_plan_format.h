/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_MVIEW_OB_MVIEW_REFRESH_PLAN_FORMAT_
#define OCEANBASE_STORAGE_MVIEW_OB_MVIEW_REFRESH_PLAN_FORMAT_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/monitor/ob_plan_info_manager.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
} // namespace common
namespace storage
{

// Number of OTHERSTAT slots per operator in __ALL_VIRTUAL_SQL_PLAN_MONITOR
static const int64_t MVIEW_PLAN_OTHERSTAT_SLOT_COUNT = 10;

// Per-operator monitor data (timestamps, DOP, skew metrics) from plan monitor,
// stored alongside ObSqlPlanItem* in a parallel array since ObSqlPlanItem
// has no such fields.
struct MViewPlanMonitorTime
{
  int64_t open_time_;
  int64_t close_time_;
  int64_t dop_;
  int64_t max_rows_;
  int64_t sum_db_time_;
  int64_t starts_;
  int64_t max_wa_mem_;
  common::ObString server_;

  MViewPlanMonitorTime()
      : open_time_(0),
        close_time_(0),
        dop_(0),
        max_rows_(0),
        sum_db_time_(0),
        starts_(0),
        max_wa_mem_(0),
        server_()
  {}

  TO_STRING_KV(K_(open_time),
               K_(close_time),
               K_(dop),
               K_(max_rows),
               K_(sum_db_time),
               K_(starts),
               K_(max_wa_mem),
               K_(server));
};

// Per-operator OTHERSTAT id/value pairs captured from plan monitor.
struct MViewPlanOtherStat
{
  int64_t ids_[MVIEW_PLAN_OTHERSTAT_SLOT_COUNT];
  int64_t values_[MVIEW_PLAN_OTHERSTAT_SLOT_COUNT];

  MViewPlanOtherStat()
  {
    MEMSET(ids_, 0, sizeof(ids_));
    MEMSET(values_, 0, sizeof(values_));
  }

  TO_STRING_KV("slot_count", static_cast<int64_t>(MVIEW_PLAN_OTHERSTAT_SLOT_COUNT));
};

// Plan contents shared by persisted-plan parsing and live-plan capture.
struct MViewStmtPlanInfo
{
  explicit MViewStmtPlanInfo(common::ObIAllocator &allocator)
    : plan_hash_(0),
      plan_infos_(common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                  common::ModulePageAllocator(allocator, "MvPlanPI")),
      monitor_times_(common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                     common::ModulePageAllocator(allocator, "MvPlanMT")),
      other_stats_(common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                   common::ModulePageAllocator(allocator, "MvPlanOS")),
      outline_data_(),
      optimization_info_()
  {}

  uint64_t plan_hash_;
  common::ObArray<sql::ObSqlPlanItem *> plan_infos_;
  common::ObArray<MViewPlanMonitorTime> monitor_times_;
  common::ObArray<MViewPlanOtherStat> other_stats_;
  common::ObString outline_data_;
  common::ObString optimization_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(MViewStmtPlanInfo);
};

/**
 * Query plan-monitor virtual tables and produce a JSON representation of
 * the complete plan tree (plan structure + runtime monitor + OTHERSTAT),
 * plus the statement's resource usage read from its audit record.
 * The JSON is designed to be persisted in __all_mview_refresh_stmt_stats.execution_plan;
 * report-side consumers can parse it with parse_mview_plan(), render the
 * contents with render_mview_plan_text(), and read resource metrics with
 * parse_mview_plan_resources().
 *
 * @param tenant_id     tenant id for querying virtual tables
 * @param sql_id        sql_id of the refresh SQL (pre-computed by caller)
 * @param plan_id       plan cache id of the executed plan
 * @param plan_hash     plan hash value
 * @param cpu_time      statement cpu time, from the session's audit record
 * @param io_wait_time  statement user io wait time, from the audit record
 * @param disk_reads    statement disk read count, from the audit record
 * @param memory_used   statement peak memory usage, from the audit record
 * @param result_json   [out] plan tree serialized as JSON string
 */
int get_mview_stmt_execution_plan(uint64_t tenant_id,
                                  const common::ObString &sql_id,
                                  const common::ObString &trace_id,
                                  const common::ObString &svr_ip,
                                  int64_t svr_port,
                                  uint64_t plan_id,
                                  uint64_t plan_hash,
                                  int64_t cpu_time,
                                  int64_t io_wait_time,
                                  int64_t disk_reads,
                                  int64_t memory_used,
                                  common::ObSqlString &result_json);

/**
 * Build the minimal execution_plan JSON used when only plan_hash is available.
 * Resource usage keys are always written, independent of plan capture mode.
 */
int build_mview_plan_hash_json(common::ObIAllocator &allocator,
                               uint64_t plan_hash,
                               int64_t cpu_time,
                               int64_t io_wait_time,
                               int64_t disk_reads,
                               int64_t memory_used,
                               common::ObSqlString &result_json);

// Render a parsed or captured statement plan as text.
int render_mview_plan_text(common::ObIAllocator &allocator,
                           const MViewStmtPlanInfo &plan,
                           common::ObSqlString &result_text);

/**
 * Parse stmt-level resource fields from plan JSON top-level keys
 * (cpu_time / io_wait_time / disk_reads / memory_used).
 * Written at capture time; independent of whether the operator tree is present.
 * Missing keys leave the corresponding output at 0.
 */
int parse_mview_plan_resources(common::ObIAllocator &allocator,
                               const common::ObString &plan_json,
                               int64_t &cpu_time,
                               int64_t &io_wait_time,
                               int64_t &disk_reads,
                               int64_t &memory_used);

// for live plan: Capture the execution plan and runtime metrics of a running refresh statement.
int capture_mview_live_plan(common::ObMySQLProxy &sql_proxy,
                            const uint64_t tenant_id,
                            const common::ObString &trace_id,
                            const common::ObString &sql_id,
                            const common::ObString &svr_ip,
                            const int64_t svr_port,
                            const uint64_t plan_id,
                            common::ObIAllocator &allocator,
                            MViewStmtPlanInfo &plan);

// Serialize statement plan contents and resource usage for storage or reports.
int serialize_mview_plan(common::ObIAllocator &allocator,
                         const MViewStmtPlanInfo &plan,
                         int64_t cpu_time,
                         int64_t io_wait_time,
                         int64_t disk_reads,
                         int64_t memory_used,
                         common::ObSqlString &result_json);

// Capture plan operators and runtime statistics from plan-monitor virtual tables.
int query_mview_plan(common::ObMySQLProxy &sql_proxy,
                     const uint64_t tenant_id,
                     const common::ObString &trace_id,
                     const common::ObString &sql_id,
                     const common::ObString &svr_ip,
                     const int64_t svr_port,
                     const uint64_t plan_id,
                     const bool filter_plan_hash,
                     const uint64_t plan_hash,
                     const bool only_realtime,
                     common::ObIAllocator &allocator,
                     MViewStmtPlanInfo &plan);

// Render the plan tree, operator statistics, and predicate information as text.
int render_mview_plan_body(common::ObIAllocator &allocator,
                            const MViewStmtPlanInfo &plan,
                            common::ObSqlString &result_text);

// Parse persisted statement plan contents using allocator-owned strings.
int parse_mview_plan(common::ObIAllocator &allocator,
                     const common::ObString &plan_json,
                     MViewStmtPlanInfo &plan);

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MVIEW_OB_MVIEW_REFRESH_PLAN_FORMAT_
