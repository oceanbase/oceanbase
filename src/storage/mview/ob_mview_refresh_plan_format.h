/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_MVIEW_OB_MVIEW_REFRESH_PLAN_FORMAT_
#define OCEANBASE_STORAGE_MVIEW_OB_MVIEW_REFRESH_PLAN_FORMAT_

#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
} // namespace sql
namespace storage
{

/**
 * Query plan-monitor virtual tables and produce a JSON representation of
 * the complete plan tree (plan structure + runtime monitor + OTHERSTAT).
 * The JSON is designed to be persisted in __all_mview_refresh_stmt_stats.execution_plan;
 * report-side consumers can render it back to ASCII via render_mview_plan_text()
 * and aggregate resource metrics via aggregate_mview_plan_resources().
 *
 * @param ctx           execution context (provides sql_proxy)
 * @param tenant_id     tenant id for querying virtual tables
 * @param sql_id        sql_id of the refresh SQL (pre-computed by caller)
 * @param plan_id       plan cache id of the executed plan
 * @param plan_hash     plan hash value
 * @param result_json   [out] plan tree serialized as JSON string
 */
int get_mview_stmt_execution_plan(sql::ObExecContext &ctx,
                                  uint64_t tenant_id,
                                  const common::ObString &sql_id,
                                  const common::ObString &trace_id,
                                  const common::ObString &svr_ip,
                                  int64_t svr_port,
                                  uint64_t plan_id,
                                  uint64_t plan_hash,
                                  common::ObSqlString &result_json);

/**
 * Build the minimal execution_plan JSON used when only plan_hash is available.
 */
int build_mview_plan_hash_json(common::ObIAllocator &allocator,
                               uint64_t plan_hash,
                               common::ObSqlString &result_json);

/**
 * Parse a plan JSON produced by get_mview_stmt_execution_plan() and render
 * it back as the ASCII plan table + operator detail block used in refresh
 * reports.  Allocator is used for string copies during rendering and is
 * expected to outlive result_text.
 */
int render_mview_plan_text(common::ObIAllocator &allocator,
                           const common::ObString &plan_json,
                           uint64_t &plan_hash,
                           common::ObSqlString &result_text);

/**
 * Parse a plan JSON and compute step-level resource aggregates derived from
 * per-operator monitor data:
 *   cpu_time      = SUM(sum_db_time) across operators (total DB time)
 *   io_wait_time  = SUM(io_cost) across operators (total user IO wait)
 *   memory_used   = MAX(max_wa_mem) across operators
 *   disk_reads    = SUM(OTHERSTAT[*].value) where id == IO_READ_BYTES
 *   affected_rows = MAX(real_rows) over DML operators (INSERT/UPDATE/DELETE/MERGE)
 *
 * On malformed JSON or missing keys, the affected output is left at 0.
 * Returns OB_INVALID_ARGUMENT if the top-level JSON structure is wrong.
 */
int aggregate_mview_plan_resources(common::ObIAllocator &allocator,
                                   const common::ObString &plan_json,
                                   int64_t &cpu_time,
                                   int64_t &io_wait_time,
                                   int64_t &disk_reads,
                                   int64_t &memory_used);

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MVIEW_OB_MVIEW_REFRESH_PLAN_FORMAT_
