/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

// ====== Shared computed values (per-MV dimension) ======
// Both MVSummaryRow and PerMVDetail reference this structure via index.
// Negative sentinel values (-1 / -1.0) denote "not applicable" (N/A).
struct MViewReportPerMVComputed
{
  MViewReportPerMVComputed()
    : mv_(NULL), display_num_(0), role_(NULL), type_(NULL), type_short_(NULL),
      changes_(0), elapsed_pct_(0.0), throughput_per_sec_(-1.0), sched_delay_us_(-1),
      result_(0), first_idx_(0), last_idx_(0),
      change_group_first_(-1), change_group_last_(-1),
      stmt_group_first_(-1), stmt_group_last_(-1),
      slowest_stmt_idx_(-1), cpu_time_us_(0), io_wait_time_us_(0),
      disk_reads_(0), max_memory_bytes_(0),
      slowest_(false)
  {}
  const MViewReportMVData *mv_;
  int64_t display_num_;
  const char *role_;              // "TGT" / "DEP"
  const char *type_;              // "FAST" / "COMPLETE"
  const char *type_short_;        // "FAST" / "COMPL"
  int64_t changes_;
  double elapsed_pct_;
  double throughput_per_sec_;     // -1.0 = N/A
  int64_t sched_delay_us_;        // -1 = N/A
  int64_t result_;
  int64_t first_idx_;
  int64_t last_idx_;
  int64_t change_group_first_;
  int64_t change_group_last_;
  int64_t stmt_group_first_;
  int64_t stmt_group_last_;
  int64_t slowest_stmt_idx_;
  int64_t cpu_time_us_;
  int64_t io_wait_time_us_;
  int64_t disk_reads_;
  int64_t max_memory_bytes_;
  bool slowest_;
  TO_STRING_KV(K_(display_num), K_(changes), K_(elapsed_pct));
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
  common::ObSEArray<MViewReportPerMVComputed, 4> per_mv_computed_;

  // Raw data refs — set by build() from fetcher output.
  const MViewReportRunData *run_data_;
  const common::ObIArray<MViewReportMVData> *mv_array_;
  const common::ObIArray<MViewReportChangeData> *change_array_;
  const common::ObIArray<MViewReportStmtData> *stmt_array_;

  // Allocator for transient buffers used during rendering (set by build()).
  common::ObIAllocator *allocator_;

  explicit MViewRefreshReport(common::ObIAllocator &allocator)
    : per_mv_computed_(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator, "MvRptPMC")),
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
