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

#ifndef OCEANBASE_STORAGE_MVIEW_OB_MVIEW_REFRESH_REPORT_H_
#define OCEANBASE_STORAGE_MVIEW_OB_MVIEW_REFRESH_REPORT_H_

#include "lib/string/ob_sql_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_allocator.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace common
{
class ObTimeZoneInfo;
}  // namespace common
namespace storage
{

// Forward declarations — full definitions in ob_mview_refresh_report_executor.h
struct MViewReportRunData;
struct MViewReportMVData;
struct MViewReportChangeData;
struct MViewReportStmtData;
struct MViewReportContext;

// ====== Table layout descriptor ======
// Single-source column definition: label + width drive separators,
// headers, and data-row format strings.
struct ColDescriptor
{
  const char *label;
  int width;
};

struct MViewReportIndexRange
{
  MViewReportIndexRange() : first_(-1), last_(-1) {}
  explicit MViewReportIndexRange(const int64_t idx) : first_(idx), last_(idx) {}
  bool is_valid() const { return first_ >= 0 && last_ >= first_; }
  int64_t count() const { return is_valid() ? last_ - first_ + 1 : 0; }

  int64_t first_;
  int64_t last_;
};

// ====== Per-MV group view ======
// One entry per logical MV. It stores stable ranges into sorted raw report
// arrays and a small set of scan-time aggregates for the final attempt.
struct MViewReportMVGroup
{
  MViewReportMVGroup()
    : changes_(0), cpu_time_us_(0), io_wait_time_us_(0),
      disk_reads_(0), max_memory_bytes_(0), slowest_stmt_idx_(-1)
  {}
  int64_t num_retries() const { return attempt_range_.count() > 0 ? attempt_range_.count() - 1 : 0; }
  bool has_changes() const { return final_change_range_.is_valid(); }
  bool has_stmts() const { return final_stmt_range_.is_valid(); }

  // mv_array_ — all retry attempts sharing this mview_id; last_ is the final attempt.
  MViewReportIndexRange attempt_range_;
  // change_array_ — base-table DML stats of the final attempt only.
  MViewReportIndexRange final_change_range_;
  // stmt_array_ — SQL step stats of the final attempt only.
  MViewReportIndexRange final_stmt_range_;

  int64_t changes_;               // total DML changes (ins+upd+del) on base tables
  int64_t cpu_time_us_;           // sum of cpu_time_ over all steps of final attempt
  int64_t io_wait_time_us_;       // sum of io_wait_time_ over all steps of final attempt
  int64_t disk_reads_;            // sum of disk_reads_ over all steps of final attempt
  int64_t max_memory_bytes_;      // peak memory_used_ across all steps of final attempt
  int64_t slowest_stmt_idx_;      // stmt_array_ index of the slowest step; -1 = N/A

  TO_STRING_KV(K_(changes), K_(cpu_time_us), K_(io_wait_time_us));
};

// ====== Summary section ======
struct MViewReportSummaryInfo
{
  MViewReportSummaryInfo()
    : run_(NULL),
      total_execute_time_us_(0),
      total_sched_overhead_us_(0),
      total_retry_overhead_us_(0),
      num_failures_(0),
      total_retries_(0),
      num_distinct_mvs_(0),
      slowest_grp_(0),
      method_display_(NULL),
      status_str_(NULL),
      elapsed_us_(0)
  {}
  const MViewReportRunData *run_;
  int64_t total_execute_time_us_;
  int64_t total_sched_overhead_us_;
  int64_t total_retry_overhead_us_;
  int64_t num_failures_;
  int64_t total_retries_;
  int64_t num_distinct_mvs_;
  int64_t slowest_grp_;
  const char *method_display_;
  const char *status_str_;
  int64_t elapsed_us_;
};

// ====== Resource overview section ======
struct MViewReportResourceOverview
{
  int64_t total_steps_;
  int64_t total_cpu_us_;
  int64_t total_io_wait_us_;
  int64_t total_disk_reads_;
  int64_t max_memory_bytes_;

  MViewReportResourceOverview()
    : total_steps_(0),
      total_cpu_us_(0),
      total_io_wait_us_(0),
      total_disk_reads_(0),
      max_memory_bytes_(0)
  {}
};

// ====== Top-level report object ======
// Allocated on the heap (exceeds 256-byte stack limit).
// build() computes topology + aggregates inline, copies results
// into sub-object fields.  append_text/append_json only read report fields.
struct MViewRefreshReport
{
  MViewReportSummaryInfo summary_;
  MViewReportResourceOverview resources_;
  common::ObSEArray<MViewReportMVGroup, 4> mv_groups_;

  // Raw data refs — set by build() from fetcher output.
  const MViewReportRunData *run_data_;
  const common::ObIArray<MViewReportMVData> *mv_array_;
  const common::ObIArray<MViewReportChangeData> *change_array_;
  const common::ObIArray<MViewReportStmtData> *stmt_array_;

  // Allocator for transient buffers used during rendering (set by build()).
  common::ObIAllocator *allocator_;

  explicit MViewRefreshReport(common::ObIAllocator &allocator)
    : mv_groups_(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator, "MvRptGrp")),
      run_data_(NULL),
      mv_array_(NULL),
      change_array_(NULL),
      stmt_array_(NULL),
      allocator_(&allocator)
  {}

  int build(MViewReportContext &ctx);
  int append_text(common::ObSqlString &out, const common::ObTimeZoneInfo *tz) const;
  int append_json(common::ObSqlString &out) const;
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_MVIEW_OB_MVIEW_REFRESH_REPORT_H_
