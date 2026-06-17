/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/mview/cmd/ob_mview_refresh_report.h"

#include "lib/container/ob_se_array.h"
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

static bool has_multiple_mviews(const ObIArray<MViewReportMVData> &mv_array)
{
  return mv_array.count() > 1
         && mv_array.at(0).mview_id_ != mv_array.at(mv_array.count() - 1).mview_id_;
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

static int build_phase1_topology(MViewReportContext &ctx)
{
  int ret = OB_SUCCESS;
  MViewReportData &data = *ctx.data_;
  if (OB_ISNULL(data.run_data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("run_data is null", KR(ret));
  } else if (!has_multiple_mviews(data.mv_array_) || 0 == data.dep_edges_.count()) {
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
//   - identifies group boundaries (first_idx_ / last_idx_ on per_mv_computed_)
//   - tracks slowest group, num_failures, retries, retry overhead, sched overhead

static int build_phase2_groups(MViewReportContext &ctx, MViewRefreshReport &report)
{
  int ret = OB_SUCCESS;
  const MViewReportData &data = *ctx.data_;
  const ObIArray<MViewReportMVData> &mv_array = data.mv_array_;
  MViewReportSummaryInfo &summary = report.summary_;
  ObIArray<MViewReportPerMVComputed> &per_mv_computed = report.per_mv_computed_;

  int64_t total_execute_time = 0;
  int64_t prev_mv_id = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_array.count(); ++i) {
    total_execute_time += mv_array.at(i).elapsed_time_;
    if (mv_array.at(i).mview_id_ != prev_mv_id) {
      if (OB_INVALID_ID != prev_mv_id) {
        per_mv_computed.at(per_mv_computed.count() - 1).last_idx_ = i - 1;
      }
      MViewReportPerMVComputed comp;
      comp.first_idx_ = i;
      comp.last_idx_ = i;
      if (OB_FAIL(per_mv_computed.push_back(comp))) {
        LOG_WARN("fail to push per_mv_computed", KR(ret));
      } else {
        prev_mv_id = mv_array.at(i).mview_id_;
      }
    }
  }
  if (OB_SUCC(ret) && mv_array.count() > 0 && per_mv_computed.count() > 0) {
    per_mv_computed.at(per_mv_computed.count() - 1).last_idx_ = mv_array.count() - 1;
  }
  summary.total_execute_time_us_ = total_execute_time;
  summary.num_distinct_mvs_ = per_mv_computed.count();

  int64_t total_retries = 0;
  int64_t total_retry_overhead = 0;
  int64_t num_failures = 0;
  int64_t total_sched_overhead = 0;
  int64_t slowest_grp = 0;
  for (int64_t group_idx = 0; OB_SUCC(ret) && group_idx < per_mv_computed.count(); ++group_idx) {
    const int64_t last = per_mv_computed.at(group_idx).last_idx_;
    const int64_t first = per_mv_computed.at(group_idx).first_idx_;
    const int64_t slow_last = per_mv_computed.at(slowest_grp).last_idx_;
    if (mv_array.at(last).elapsed_time_ > mv_array.at(slow_last).elapsed_time_) {
      slowest_grp = group_idx;
    }
    total_retries += last - first;
    if (last != first) {
      const int64_t overhead_end = (0 != mv_array.at(last).result_) ? last : (last - 1);
      for (int64_t i = first; i <= overhead_end; ++i) {
        total_retry_overhead += mv_array.at(i).elapsed_time_;
      }
    }
    if (0 != mv_array.at(last).result_) {
      ++num_failures;
    }
    const MViewReportMVData &mv = mv_array.at(last);
    if (mv.deps_ready_time_ > 0 && mv.start_time_ > mv.deps_ready_time_) {
      total_sched_overhead += mv.start_time_ - mv.deps_ready_time_;
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

static void build_phase3_resources(const MViewReportData &data,
                                   const ObIArray<MViewReportPerMVComputed> &per_mv_computed,
                                   MViewReportResourceOverview &resources)
{
  int64_t total_cpu = 0;
  int64_t total_io_wait = 0;
  int64_t total_disk_reads = 0;
  int64_t max_memory = 0;
  for (int64_t i = 0; i < per_mv_computed.count(); ++i) {
    total_cpu += per_mv_computed.at(i).cpu_time_us_;
    total_io_wait += per_mv_computed.at(i).io_wait_time_us_;
    total_disk_reads += per_mv_computed.at(i).disk_reads_;
    if (per_mv_computed.at(i).max_memory_bytes_ > max_memory) {
      max_memory = per_mv_computed.at(i).max_memory_bytes_;
    }
  }
  resources.total_steps_ = data.stmt_array_.count();
  resources.total_cpu_us_ = total_cpu;
  resources.total_io_wait_us_ = total_io_wait;
  resources.total_disk_reads_ = total_disk_reads;
  resources.max_memory_bytes_ = max_memory;
}

// ====== Phase 4: Per-MV change/stmt cursor scan ======

static int build_phase4_per_mv_cursors(const MViewReportData &data,
                                       ObIArray<MViewReportPerMVComputed> &per_mv_computed)
{
  int ret = OB_SUCCESS;
  int64_t change_cursor = 0;
  int64_t stmt_cursor = 0;
  for (int64_t group_idx = 0; OB_SUCC(ret) && group_idx < per_mv_computed.count(); ++group_idx) {
    MViewReportPerMVComputed &comp = per_mv_computed.at(group_idx);
    const int64_t mv_idx = comp.last_idx_;
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
      if (comp.change_group_first_ < 0) {
        comp.change_group_first_ = change_cursor;
      }
      comp.change_group_last_ = change_cursor;
      comp.changes_ += change.num_rows_ins_ + change.num_rows_upd_ + change.num_rows_del_;
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
      if (comp.stmt_group_first_ < 0) {
        comp.stmt_group_first_ = stmt_cursor;
      }
      comp.stmt_group_last_ = stmt_cursor;
      comp.cpu_time_us_ += stmt_stat.cpu_time_;
      comp.io_wait_time_us_ += stmt_stat.io_wait_time_;
      comp.disk_reads_ += stmt_stat.disk_reads_;
      if (stmt_stat.memory_used_ > comp.max_memory_bytes_) {
        comp.max_memory_bytes_ = stmt_stat.memory_used_;
      }
      if (stmt_stat.execution_time_ > slowest_stmt_time) {
        slowest_stmt_time = stmt_stat.execution_time_;
        comp.slowest_stmt_idx_ = stmt_cursor;
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

    if (OB_FAIL(build_phase1_topology(ctx))) {
      LOG_WARN("fail to compute topology", KR(ret));
    } else if (OB_FAIL(build_phase2_groups(ctx, *this))) {
      LOG_WARN("fail to compute groups", KR(ret));
    } else if (OB_FAIL(build_phase4_per_mv_cursors(data, per_mv_computed_))) {
      LOG_WARN("fail to scan per-mv cursors", KR(ret));
    } else {
      build_phase3_resources(data, per_mv_computed_, resources_);

      const int64_t total_elapsed = run_data.end_time_ - run_data.start_time_;
      summary_.elapsed_us_ = total_elapsed;
      summary_.status_str_ = (0 == run_data.number_of_failures_) ? "SUCCESS" : "FAILED";

      // Resolve method display name
      static constexpr int64_t METHOD_DISPLAY_BUF_LEN = 64;
      char *method_buf = static_cast<char *>(ctx.allocator_->alloc(METHOD_DISPLAY_BUF_LEN));
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
            if (OB_FAIL(databuff_printf(method_buf, METHOD_DISPLAY_BUF_LEN, pos,
                                        "%s%s", first ? "" : " ", name))) {
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
        if (OB_FAIL(databuff_printf(method_buf, METHOD_DISPLAY_BUF_LEN, "%s",
                 (0 == mv_array.at(0).refresh_type_) ? "AUTO (executed COMPLETE)" : "AUTO (executed FAST)"))) {
          LOG_WARN("method_buf overflow", KR(ret));
        }
      } else {
        if (OB_FAIL(databuff_printf(method_buf, METHOD_DISPLAY_BUF_LEN, "AUTO"))) {
          LOG_WARN("method_buf overflow", KR(ret));
        }
      }
      summary_.method_display_ = method_buf;

      // Per-MV derived fields (role, type, throughput, elapsed_pct, sched_delay)
      const int64_t num_mvs = per_mv_computed_.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < num_mvs; ++i) {
        MViewReportPerMVComputed &comp = per_mv_computed_.at(i);
        const MViewReportMVData &mv = mv_array.at(comp.last_idx_);
        comp.mv_ = &mv;
        comp.display_num_ = i + 1;
        comp.slowest_ = (i == summary_.slowest_grp_ && num_mvs > 1);
        comp.result_ = mv.result_;
        comp.type_ = (0 == mv.refresh_type_) ? "COMPLETE" : "FAST";
        comp.type_short_ = (0 == mv.refresh_type_) ? "COMPL" : "FAST";
        if (!run_data.nested_ || mv.mview_id_ == run_data.mview_id_) {
          comp.role_ = "TGT";
        } else {
          comp.role_ = "DEP";
        }
        comp.elapsed_pct_ = (total_elapsed > 0)
          ? (static_cast<double>(mv.elapsed_time_) / static_cast<double>(total_elapsed) * 100.0)
          : 0.0;
        const double elapsed_s = static_cast<double>(mv.elapsed_time_) / 1000000.0;
        if (elapsed_s > 0.0 && comp.changes_ > 0) {
          comp.throughput_per_sec_ = static_cast<double>(comp.changes_) / elapsed_s;
        } else {
          comp.throughput_per_sec_ = -1.0;
        }
        if (mv.deps_ready_time_ > 0 && mv.start_time_ > mv.deps_ready_time_) {
          comp.sched_delay_us_ = mv.start_time_ - mv.deps_ready_time_;
        } else {
          comp.sched_delay_us_ = -1;
        }
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
