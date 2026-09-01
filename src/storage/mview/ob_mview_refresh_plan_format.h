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
 * the complete plan tree (plan structure + runtime monitor + OTHERSTAT),
 * plus the statement's resource usage read from its audit record.
 * The JSON is designed to be persisted in __all_mview_refresh_stmt_stats.execution_plan;
 * report-side consumers can render it back to ASCII via render_mview_plan_text()
 * and read resource metrics via parse_mview_plan_resources().
 *
 * @param ctx           execution context (provides sql_proxy)
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
int get_mview_stmt_execution_plan(sql::ObExecContext &ctx,
                                  uint64_t tenant_id,
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

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MVIEW_OB_MVIEW_REFRESH_PLAN_FORMAT_
