/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/mview/cmd/ob_mview_refresh_report.h"

#include "lib/container/ob_se_array.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_errno.h"
#include "share/scn.h"
#include "storage/mview/cmd/ob_mview_refresh_report_executor.h"
#include "storage/mview/cmd/ob_mview_refresh_report_formatter.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

// ====== Topology helpers (moved from aggregator) ======

struct MViewTopoVertex
{
  int64_t indeg_;
  int64_t depth_;
  int64_t ready_time_;
  int64_t max_end_time_;
  int64_t first_child_;
};

struct MViewTopoEdge
{
  int64_t to_;
  int64_t next_;
};

bool is_target_mv_missing(const MViewReportRunData &run_data, const ObIArray<MViewReportMVData> &mv_array)
{
  bool target_mv_missing = false;
  if (run_data.has_nested_target()) {
    target_mv_missing = true;
    for (int64_t i = 0; target_mv_missing && i < mv_array.count(); ++i) {
      if (mv_array.at(i).mview_id_ == run_data.mview_id_) {
        target_mv_missing = false;
      }
    }
  }
  return target_mv_missing;
}

int resolve_method_display(const MViewReportRunData &run_data,
                           ObIAllocator &allocator,
                           const ObIArray<MViewReportMVData> &mv_array,
                           const char *&method_display)
{
  int ret = OB_SUCCESS;
  static constexpr int64_t METHOD_DISPLAY_BUF_LEN = 64;
  char *method_buf = static_cast<char *>(allocator.alloc(METHOD_DISPLAY_BUF_LEN));
  method_display = NULL;
  if (OB_ISNULL(method_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc method_display", KR(ret));
  } else if (!run_data.method_.empty()) {
    int64_t pos = 0;
    bool first = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < run_data.method_.length(); ++i) {
      const char *name = NULL;
      const char ch = run_data.method_.ptr()[i];
      if ('f' == ch || 'F' == ch) {
        name = "FAST";
      } else if ('c' == ch || 'C' == ch) {
        name = "COMPLETE";
      } else if ('?' == ch) {
        name = "FORCE";
      } else if ('p' == ch || 'P' == ch) {
        name = "PCT";
      }
      if (NULL != name) {
        if (OB_FAIL(databuff_printf(method_buf, METHOD_DISPLAY_BUF_LEN, pos, "%s%s", first ? "" : " ", name))) {
          LOG_WARN("method_buf overflow", KR(ret));
        } else {
          first = false;
        }
      }
    }
    if (OB_SUCC(ret) && first) {
      if (OB_FAIL(databuff_printf(method_buf, METHOD_DISPLAY_BUF_LEN, "AUTO"))) {
        LOG_WARN("method_buf overflow", KR(ret));
      }
    }
  } else if (mv_array.count() > 0) {
    if (OB_FAIL(databuff_printf(method_buf,
                                METHOD_DISPLAY_BUF_LEN,
                                "AUTO (executed %s)",
                                mv_array.at(0).type_name()))) {
      LOG_WARN("method_buf overflow", KR(ret));
    }
  } else if (OB_FAIL(databuff_printf(method_buf, METHOD_DISPLAY_BUF_LEN, "AUTO"))) {
    LOG_WARN("method_buf overflow", KR(ret));
  }
  if (OB_SUCC(ret)) {
    method_display = method_buf;
  }
  return ret;
}

static int find_mv_id_index(const ObIArray<int64_t> &mv_ids, const int64_t mv_id, int64_t &idx)
{
  int ret = OB_ENTRY_NOT_EXIST;
  int64_t left = 0;
  int64_t right = mv_ids.count() - 1;
  idx = -1;
  while (OB_ENTRY_NOT_EXIST == ret && left <= right) {
    const int64_t mid = (left + right) / 2;
    if (mv_ids.at(mid) == mv_id) {
      idx = mid;
      ret = OB_SUCCESS;
    } else if (mv_ids.at(mid) < mv_id) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return ret;
}

// ====== Aggregation helpers (moved from aggregator) ======

static bool change_is_earlier(const MViewReportChangeData &change_data,
                              const int64_t mview_id, const int64_t retry_id)
{
  bool before = false;
  if (change_data.mview_id_ < mview_id) {
    before = true;
  } else if (change_data.mview_id_ == mview_id && change_data.retry_id_ < retry_id) {
    before = true;
  }
  return before;
}

static bool stmt_is_earlier(const MViewReportStmtData &stmt_data,
                            const int64_t mview_id, const int64_t retry_id)
{
  bool before = false;
  if (stmt_data.mview_id_ < mview_id) {
    before = true;
  } else if (stmt_data.mview_id_ == mview_id && stmt_data.retry_id_ < retry_id) {
    before = true;
  }
  return before;
}

// ====== Phase 1: Topology ======

static int build_mv_topology(MViewReportContext &ctx)
{
  int ret = OB_SUCCESS;
  MViewReportData &data = *ctx.data_;
  if (OB_ISNULL(data.run_data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("run_data is null", KR(ret));
  } else if (!has_distinct_mviews(data.mv_array_) || 0 == data.dep_edges_.count()) {
    for (int64_t i = 0; i < data.mv_array_.count(); ++i) {
      data.mv_array_.at(i).topo_order_ = 1;
      data.mv_array_.at(i).deps_ready_time_ = data.run_data_->start_time_;
    }
  } else {
    int64_t vertex_count = data.mv_ids_.count();
    int64_t edge_count = data.dep_edges_.count();
    MViewTopoVertex *vertices = NULL;
    MViewTopoEdge *edges = NULL;
    int64_t *queue = NULL;
    int64_t queue_tail = 0;
    int64_t queue_head = 0;

    if (OB_UNLIKELY(vertex_count <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mv ids is empty for multi-mview refresh", KR(ret), K(vertex_count), K(data.mv_array_.count()));
    } else if (OB_ISNULL(vertices = static_cast<MViewTopoVertex *>(
                               ctx.allocator_->alloc(sizeof(MViewTopoVertex) * vertex_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc topology vertices", KR(ret), K(vertex_count));
    } else if (OB_ISNULL(queue = static_cast<int64_t *>(
                               ctx.allocator_->alloc(sizeof(int64_t) * vertex_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc topology queue", KR(ret), K(vertex_count));
    } else if (edge_count > 0
               && OB_ISNULL(edges = static_cast<MViewTopoEdge *>(
                              ctx.allocator_->alloc(sizeof(MViewTopoEdge) * edge_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc topology edges", KR(ret), K(edge_count));
    } else {
      for (int64_t i = 0; i < vertex_count; ++i) {
        vertices[i].indeg_ = 0;
        vertices[i].depth_ = 0;
        vertices[i].ready_time_ = data.run_data_->start_time_;
        vertices[i].max_end_time_ = 0;
        vertices[i].first_child_ = -1;
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < data.mv_array_.count(); ++i) {
        int64_t mv_idx = -1;
        if (OB_FAIL(find_mv_id_index(data.mv_ids_, data.mv_array_.at(i).mview_id_, mv_idx))) {
          LOG_WARN("fail to find mv index", KR(ret), K(data.mv_array_.at(i).mview_id_));
        } else if (data.mv_array_.at(i).end_time_ > vertices[mv_idx].max_end_time_) {
          vertices[mv_idx].max_end_time_ = data.mv_array_.at(i).end_time_;
        }
      }

      int64_t valid_edge_count = 0;
      for (int64_t edge_idx = 0; OB_SUCC(ret) && edge_idx < edge_count; ++edge_idx) {
        int64_t child_idx = -1;
        int64_t parent_idx = -1;
        if (OB_FAIL(find_mv_id_index(data.mv_ids_, data.dep_edges_.at(edge_idx).child_id_, child_idx))) {
          LOG_WARN("fail to find child mv index", KR(ret), K(data.dep_edges_.at(edge_idx)));
        } else if (OB_FAIL(find_mv_id_index(data.mv_ids_, data.dep_edges_.at(edge_idx).parent_id_, parent_idx))) {
          LOG_WARN("fail to find parent mv index", KR(ret), K(data.dep_edges_.at(edge_idx)));
        } else if (child_idx == parent_idx) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("self-loop detected in mview dependency edge", KR(ret), K(child_idx), K(data.dep_edges_.at(edge_idx)));
        } else {
          ++vertices[child_idx].indeg_;
          if (vertices[parent_idx].max_end_time_ > vertices[child_idx].ready_time_) {
            vertices[child_idx].ready_time_ = vertices[parent_idx].max_end_time_;
          }
          edges[valid_edge_count].to_ = child_idx;
          edges[valid_edge_count].next_ = vertices[parent_idx].first_child_;
          vertices[parent_idx].first_child_ = valid_edge_count;
          ++valid_edge_count;
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < vertex_count; ++i) {
        if (0 == vertices[i].indeg_) {
          vertices[i].depth_ = 1;
          queue[queue_tail++] = i;
        }
      }
      while (OB_SUCC(ret) && queue_head < queue_tail) {
        int64_t current_vertex = queue[queue_head++];
        for (int64_t edge_idx = vertices[current_vertex].first_child_; edge_idx >= 0;
             edge_idx = edges[edge_idx].next_) {
          int64_t child_idx = edges[edge_idx].to_;
          if (vertices[current_vertex].depth_ + 1 > vertices[child_idx].depth_) {
            vertices[child_idx].depth_ = vertices[current_vertex].depth_ + 1;
          }
          --vertices[child_idx].indeg_;
          if (0 == vertices[child_idx].indeg_) {
            queue[queue_tail++] = child_idx;
          }
        }
      }
      if (OB_SUCC(ret) && OB_UNLIKELY(queue_tail != vertex_count)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid mview dependency graph",
                 KR(ret), K(queue_tail), K(vertex_count),
                 K(data.dep_edges_.count()), K(data.mv_ids_.count()));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < data.mv_array_.count(); ++i) {
        int64_t mv_id = data.mv_array_.at(i).mview_id_;
        int64_t mv_idx = -1;
        if (OB_FAIL(find_mv_id_index(data.mv_ids_, mv_id, mv_idx))) {
          LOG_WARN("fail to find mv index", KR(ret), K(mv_id));
        } else {
          data.mv_array_.at(i).topo_order_ = vertices[mv_idx].depth_;
          data.mv_array_.at(i).deps_ready_time_ = vertices[mv_idx].ready_time_;
        }
      }
    }
  }
  return ret;
}

// ====== Phase 2: Group identification + per-MV computed skeleton ======
// Single pass over mv_array_:
//   - sums total_execute_time
//   - identifies retry-attempt ranges on mv_groups_
//   - tracks slowest group, num_failures, retries, retry overhead, sched overhead

static int build_mv_groups(MViewReportContext &ctx, MViewRefreshReport &report)
{
  int ret = OB_SUCCESS;
  const MViewReportData &data = *ctx.data_;
  const ObIArray<MViewReportMVData> &mv_array = data.mv_array_;
  MViewReportSummaryInfo &summary = report.summary_;
  ObIArray<MViewReportMVGroup> &mv_groups = report.mv_groups_;

  int64_t total_execute_time = 0;
  int64_t prev_mv_id = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_array.count(); ++i) {
    total_execute_time += mv_array.at(i).elapsed_time_;
    if (mv_array.at(i).mview_id_ != prev_mv_id) {
      if (OB_INVALID_ID != prev_mv_id) {
        mv_groups.at(mv_groups.count() - 1).attempt_range_.last_ = i - 1;
      }
      MViewReportMVGroup group;
      group.attempt_range_ = MViewReportIndexRange(i);
      if (OB_FAIL(mv_groups.push_back(group))) {
        LOG_WARN("fail to push mv group", KR(ret));
      } else {
        prev_mv_id = mv_array.at(i).mview_id_;
      }
    }
  }
  if (OB_SUCC(ret) && mv_array.count() > 0 && mv_groups.count() > 0) {
    mv_groups.at(mv_groups.count() - 1).attempt_range_.last_ = mv_array.count() - 1;
  }
  summary.total_execute_time_us_ = total_execute_time;
  summary.num_distinct_mvs_ = mv_groups.count();

  int64_t total_retries = 0;
  int64_t total_retry_overhead = 0;
  int64_t num_failures = 0;
  int64_t total_sched_overhead = 0;
  int64_t slowest_grp = 0;
  for (int64_t group_idx = 0; OB_SUCC(ret) && group_idx < mv_groups.count(); ++group_idx) {
    const int64_t last = mv_groups.at(group_idx).attempt_range_.last_;
    const int64_t first = mv_groups.at(group_idx).attempt_range_.first_;
    const int64_t slow_last = mv_groups.at(slowest_grp).attempt_range_.last_;
    if (mv_array.at(last).elapsed_time_ > mv_array.at(slow_last).elapsed_time_) {
      slowest_grp = group_idx;
    }
    total_retries += last - first;
    if (last != first) {
      const int64_t overhead_end = mv_array.at(last).is_failed() ? last : (last - 1);
      for (int64_t i = first; i <= overhead_end; ++i) {
        total_retry_overhead += mv_array.at(i).elapsed_time_;
      }
    }
    if (mv_array.at(last).is_failed()) {
      ++num_failures;
    }
    const MViewReportMVData &mv = mv_array.at(last);
    const int64_t delay = mv.sched_delay_us();
    if (delay >= 0) {
      total_sched_overhead += delay;
    }
  }
  summary.total_retries_ = total_retries;
  summary.total_retry_overhead_us_ = total_retry_overhead;
  summary.num_failures_ = num_failures;
  summary.total_sched_overhead_us_ = total_sched_overhead;
  summary.slowest_grp_ = slowest_grp;
  return ret;
}

// ====== Phase 3: Resource totals ======

static void aggregate_mv_resources(const ObIArray<MViewReportMVGroup> &mv_groups,
                                   MViewReportResourceOverview &resources)
{
  int64_t total_steps = 0;
  int64_t total_cpu = 0;
  int64_t total_io_wait = 0;
  int64_t total_disk_reads = 0;
  int64_t max_memory = 0;
  for (int64_t i = 0; i < mv_groups.count(); ++i) {
    const MViewReportMVGroup &group = mv_groups.at(i);
    total_steps += group.final_stmt_range_.count();
    total_cpu += group.cpu_time_us_;
    total_io_wait += group.io_wait_time_us_;
    total_disk_reads += group.disk_reads_;
    if (group.max_memory_bytes_ > max_memory) {
      max_memory = group.max_memory_bytes_;
    }
  }
  resources.total_steps_ = total_steps;
  resources.total_cpu_us_ = total_cpu;
  resources.total_io_wait_us_ = total_io_wait;
  resources.total_disk_reads_ = total_disk_reads;
  resources.max_memory_bytes_ = max_memory;
}

// ====== Phase 4: Per-MV change/stmt range indexing ======

static int index_mv_group_ranges(const MViewReportData &data,
                                 ObIArray<MViewReportMVGroup> &mv_groups)
{
  int ret = OB_SUCCESS;
  int64_t change_cursor = 0;
  int64_t stmt_cursor = 0;
  for (int64_t group_idx = 0; OB_SUCC(ret) && group_idx < mv_groups.count(); ++group_idx) {
    MViewReportMVGroup &group = mv_groups.at(group_idx);
    const int64_t mv_idx = group.attempt_range_.last_;
    const MViewReportMVData &mv = data.mv_array_.at(mv_idx);
    const int64_t mview_id = mv.mview_id_;
    const int64_t retry_id = mv.retry_id_;

    int64_t slowest_stmt_time = 0;
    while (change_cursor < data.change_array_.count()
           && change_is_earlier(data.change_array_.at(change_cursor), mview_id, retry_id)) {
      ++change_cursor;
    }
    while (change_cursor < data.change_array_.count()
           && data.change_array_.at(change_cursor).mview_id_ == mview_id
           && data.change_array_.at(change_cursor).retry_id_ == retry_id) {
      const MViewReportChangeData &change = data.change_array_.at(change_cursor);
      if (!group.final_change_range_.is_valid()) {
        group.final_change_range_.first_ = change_cursor;
      }
      group.final_change_range_.last_ = change_cursor;
      group.changes_ += change.total_changes();
      ++change_cursor;
    }

    while (stmt_cursor < data.stmt_array_.count()
           && stmt_is_earlier(data.stmt_array_.at(stmt_cursor), mview_id, retry_id)) {
      ++stmt_cursor;
    }
    while (stmt_cursor < data.stmt_array_.count()
           && data.stmt_array_.at(stmt_cursor).mview_id_ == mview_id
           && data.stmt_array_.at(stmt_cursor).retry_id_ == retry_id) {
      const MViewReportStmtData &stmt_stat = data.stmt_array_.at(stmt_cursor);
      if (!group.final_stmt_range_.is_valid()) {
        group.final_stmt_range_.first_ = stmt_cursor;
      }
      group.final_stmt_range_.last_ = stmt_cursor;
      group.cpu_time_us_ += stmt_stat.cpu_time_;
      group.io_wait_time_us_ += stmt_stat.io_wait_time_;
      group.disk_reads_ += stmt_stat.disk_reads_;
      if (stmt_stat.memory_used_ > group.max_memory_bytes_) {
        group.max_memory_bytes_ = stmt_stat.memory_used_;
      }
      if (stmt_stat.execution_time_ > slowest_stmt_time) {
        slowest_stmt_time = stmt_stat.execution_time_;
        group.slowest_stmt_idx_ = stmt_cursor;
      }
      ++stmt_cursor;
    }
  }
  return ret;
}

// ====== Main build() ======

int MViewRefreshReport::build(MViewReportContext &ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ctx.allocator_) || OB_ISNULL(ctx.data_) || OB_ISNULL(ctx.data_->run_data_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid context", KR(ret));
  } else {
    const MViewReportData &data = *ctx.data_;
    const MViewReportRunData &run_data = *data.run_data_;
    const ObIArray<MViewReportMVData> &mv_array = data.mv_array_;

    run_data_ = &run_data;
    mv_array_ = &data.mv_array_;
    change_array_ = &data.change_array_;
    stmt_array_ = &data.stmt_array_;
    summary_.run_ = &run_data;

    if (OB_FAIL(build_mv_topology(ctx))) {
      LOG_WARN("fail to compute topology", KR(ret));
    } else if (OB_FAIL(build_mv_groups(ctx, *this))) {
      LOG_WARN("fail to compute groups", KR(ret));
    } else if (OB_FAIL(index_mv_group_ranges(data, mv_groups_))) {
      LOG_WARN("fail to index mv group ranges", KR(ret));
    } else {
      aggregate_mv_resources(mv_groups_, resources_);

      summary_.elapsed_us_ = run_data.elapsed_us();
      summary_.status_str_ = run_data.summary_status();
      if (OB_FAIL(resolve_method_display(run_data, *ctx.allocator_, mv_array, summary_.method_display_))) {
        LOG_WARN("fail to resolve method display", KR(ret));
      }
    }
  }

  return ret;
}

int MViewRefreshReport::append_text(ObSqlString &out, const ObTimeZoneInfo *tz) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(run_data_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("report not built, build() must be called first", KR(ret));
  } else if (OB_FAIL(ob_mview_refresh_report_format_text_report(*this, tz, out))) {
    LOG_WARN("fail to format text report", KR(ret));
  }
  return ret;
}

int MViewRefreshReport::append_json(ObSqlString &out) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(run_data_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("report not built, build() must be called first", KR(ret));
  } else if (OB_FAIL(ob_mview_refresh_report_format_json_report(*this, NULL, out))) {
    LOG_WARN("fail to format json report", KR(ret));
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
