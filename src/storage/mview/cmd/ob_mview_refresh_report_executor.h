/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "share/ob_define.h"
#include "share/scn.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObSQLSessionInfo;
} // namespace sql
namespace storage
{

inline bool scn_is_unknown(const uint64_t scn)
{
  return 0 == scn || share::OB_INVALID_SCN_VAL == scn;
}

// Arguments for DBMS_MVIEW.REFRESH_REPORT.
// Either refresh_id or mv_name must be specified.
struct ObMViewRefreshReportArg
{
public:
  ObMViewRefreshReportArg()
    : refresh_id_(common::OB_INVALID_ID),
      tenant_id_(common::OB_INVALID_TENANT_ID),
      has_refresh_id_(false)
  {}
  bool is_valid() const
  {
    return tenant_id_ != common::OB_INVALID_TENANT_ID
           && (has_refresh_id_ || !mv_name_.empty());
  }
  TO_STRING_KV(K_(refresh_id), K_(mv_name), K_(tenant_id), K_(format), K_(has_refresh_id));

public:
  int64_t refresh_id_;
  common::ObString mv_name_;
  uint64_t tenant_id_;
  common::ObString format_;
  bool has_refresh_id_;
};

// Edge parent->child: child MV cannot refresh before parent MV finishes.
struct MViewDepEdge
{
  int64_t child_id_;
  int64_t parent_id_;
  MViewDepEdge() : child_id_(0), parent_id_(0) {}
  MViewDepEdge(int64_t child, int64_t parent) : child_id_(child), parent_id_(parent) {}
  TO_STRING_KV(K_(child_id), K_(parent_id));
};

struct MViewReportMVData;

// Top-level metadata of one refresh, from __all_virtual_mview_refresh_run_stats.
struct MViewReportRunData
{
  MViewReportRunData() { reset(); }
  void reset()
  {
    refresh_id_ = common::OB_INVALID_ID;
    run_user_id_ = 0;
    parallelism_ = 0;
    nested_ = false;
    start_time_ = 0;
    end_time_ = 0;
    elapsed_time_ = 0;
    mview_id_ = 0;
    data_target_scn_ = 0;
    is_valid_ = false;
    result_ = 0;
    run_owner_.reset();
    method_.reset();
    trace_id_.reset();
    error_message_.reset();
    mview_name_.reset();
    mview_display_name_.reset();
  }
  bool is_success() const { return 0 == result_; }
  bool is_running() const { return 1 == result_; }
  bool is_failed() const { return !is_success() && !is_running(); }
  bool needs_live_elapsed() const
  {
    return is_running() && start_time_ > 0 && 0 == end_time_;
  }
  bool is_default_parallelism() const { return parallelism_ <= 0; }
  bool has_target_mview() const { return mview_id_ != 0; }
  const common::ObString &display_mview_name() const
  {
    return mview_display_name_.empty() ? mview_name_ : mview_display_name_;
  }
  bool has_nested_target() const { return nested_ && mview_id_ > 0; }
  bool is_target_mv(const int64_t candidate_mview_id) const
  {
    return !nested_ || mview_id_ == candidate_mview_id;
  }
  const char *mv_role_label(const int64_t candidate_mview_id) const
  {
    return is_target_mv(candidate_mview_id) ? "TGT" : "DEP";
  }
  const char *nested_role_suffix(const int64_t candidate_mview_id) const
  {
    const char *result = "";
    if (nested_) {
      if (is_target_mv(candidate_mview_id)) {
        result = " (TGT)";
      } else {
        result = " (DEP)";
      }
    }
    return result;
  }
  const char *summary_status() const
  {
    const char *result = "FAILED";
    if (is_running()) {
      result = "RUNNING";
    } else if (is_success()) {
      result = "SUCCESS";
    }
    return result;
  }
  int64_t elapsed_us() const { return elapsed_time_; }
  bool data_target_scn_known() const
  {
    return !scn_is_unknown(data_target_scn_);
  }
  TO_STRING_KV(K_(refresh_id), K_(is_valid));

  int64_t refresh_id_;
  int64_t run_user_id_;
  common::ObString run_owner_;
  common::ObString method_;
  int64_t parallelism_;
  bool nested_;
  int64_t start_time_;
  int64_t end_time_;
  int64_t elapsed_time_;
  common::ObString trace_id_;
  int64_t mview_id_;
  uint64_t data_target_scn_;
  common::ObString mview_name_;
  common::ObString mview_display_name_;
  common::ObString error_message_;
  bool is_valid_;
  int64_t result_;
};

// Per-MV refresh status, timing, and SCN range, from __all_virtual_mview_refresh_stats.
struct MViewReportMVData
{
  MViewReportMVData() { reset(); }
  void reset()
  {
    mview_id_ = 0;
    retry_id_ = 0;
    refresh_type_ = 0;
    start_time_ = 0;
    end_time_ = 0;
    elapsed_time_ = 0;
    initial_num_rows_ = 0;
    final_num_rows_ = 0;
    result_ = 0;
    topo_order_ = 0;
    deps_ready_time_ = 0;
    mv_refresh_start_scn_ = 0;
    mv_refresh_end_scn_ = 0;
    base_table_start_scn_ = 0;
    svr_port_ = 0;
    mv_name_.reset();
    mv_display_name_.reset();
    svr_ip_.reset();
  }
  bool is_success() const { return 0 == result_; }
  bool is_running() const { return 1 == result_; }
  bool is_failed() const { return !is_success() && !is_running(); }
  bool is_complete_refresh() const { return 0 == refresh_type_; }
  bool needs_live_elapsed() const
  {
    return is_running() && start_time_ > 0 && 0 == end_time_;
  }
  int64_t sched_delay_us() const
  {
    int64_t delay = -1;
    if (deps_ready_time_ > 0 && start_time_ > deps_ready_time_) {
      delay = start_time_ - deps_ready_time_;
    }
    return delay;
  }
  bool has_deps_ready_time() const { return deps_ready_time_ > 0; }
  bool has_server() const { return !svr_ip_.empty(); }
  const char *type_name() const { return is_complete_refresh() ? "COMPLETE" : "FAST"; }
  const char *type_short_name() const { return is_complete_refresh() ? "COMPL" : "FAST"; }
  const common::ObString &display_name() const
  {
    return mv_display_name_.empty() ? mv_name_ : mv_display_name_;
  }
  bool base_table_start_scn_known() const
  {
    return !scn_is_unknown(base_table_start_scn_);
  }
  bool mv_refresh_start_scn_known() const
  {
    return !scn_is_unknown(mv_refresh_start_scn_);
  }
  bool mv_refresh_end_scn_known() const
  {
    return !scn_is_unknown(mv_refresh_end_scn_);
  }
  TO_STRING_KV(K_(mview_id), K_(retry_id), K_(result), K_(topo_order));

  int64_t mview_id_;
  int64_t retry_id_;
  int64_t refresh_type_;
  int64_t start_time_;
  int64_t end_time_;
  int64_t elapsed_time_;
  int64_t initial_num_rows_;
  int64_t final_num_rows_;
  int64_t result_;
  int64_t topo_order_;
  int64_t deps_ready_time_;
  uint64_t mv_refresh_start_scn_;
  uint64_t mv_refresh_end_scn_;
  uint64_t base_table_start_scn_;
  common::ObString mv_name_;
  common::ObString mv_display_name_;
  common::ObString svr_ip_;
  int64_t svr_port_;
};

// Per-base-table DML change counts, from __all_virtual_mview_refresh_change_stats.
struct MViewReportChangeData
{
  MViewReportChangeData() { reset(); }
  void reset()
  {
    mview_id_ = 0;
    retry_id_ = 0;
    detail_table_id_ = 0;
    num_rows_ins_ = 0;
    num_rows_upd_ = 0;
    num_rows_del_ = 0;
    num_rows_ = 0;
    tbl_name_.reset();
    tbl_display_name_.reset();
  }
  int64_t total_changes() const
  {
    return num_rows_ins_ + num_rows_upd_ + num_rows_del_;
  }
  const common::ObString &display_name() const
  {
    return tbl_display_name_.empty() ? tbl_name_ : tbl_display_name_;
  }
  TO_STRING_KV(K_(mview_id), K_(detail_table_id));

  int64_t mview_id_;
  int64_t retry_id_;
  int64_t detail_table_id_;
  int64_t num_rows_ins_;
  int64_t num_rows_upd_;
  int64_t num_rows_del_;
  int64_t num_rows_;
  common::ObString tbl_name_;
  common::ObString tbl_display_name_;
};

// Per-step SQL stats and execution plan, from __all_virtual_mview_refresh_stmt_stats.
struct MViewReportStmtData
{
  MViewReportStmtData() { reset(); }
  void reset()
  {
    mview_id_ = 0;
    retry_id_ = 0;
    step_ = 0;
    execution_time_ = 0;
    result_ = 0;
    start_time_ = 0;
    cpu_time_ = 0;
    io_wait_time_ = 0;
    disk_reads_ = 0;
    memory_used_ = 0;
    sqlid_.reset();
    execution_plan_.reset();
  }
  bool is_success() const { return 0 == result_; }
  bool is_running() const { return 1 == result_; }
  bool is_failed() const { return !is_success() && !is_running(); }
  TO_STRING_KV(K_(mview_id), K_(step), K_(result));

  int64_t mview_id_;
  int64_t retry_id_;
  int64_t step_;
  int64_t execution_time_;
  int64_t result_;
  int64_t start_time_;
  int64_t cpu_time_;
  int64_t io_wait_time_;
  int64_t disk_reads_;
  int64_t memory_used_;
  common::ObString sqlid_;
  common::ObString execution_plan_;
};

// All raw data fetched for one refresh run.
struct MViewReportData
{
  MViewReportRunData *run_data_;
  common::ObSEArray<MViewReportMVData, 4> mv_array_;
  common::ObSEArray<MViewReportChangeData, 8> change_array_;
  common::ObSEArray<MViewReportStmtData, 16> stmt_array_;
  common::ObSEArray<MViewDepEdge, 16> dep_edges_;
  common::ObSEArray<int64_t, 8> mv_ids_;

  MViewReportData() : run_data_(NULL) {}
  explicit MViewReportData(common::ObIAllocator &allocator)
    : run_data_(NULL),
      mv_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator, "MvRptMV")),
      change_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator, "MvRptCH")),
      stmt_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator, "MvRptST")),
      dep_edges_(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator, "MvRptDE")),
      mv_ids_(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator, "MvRptID"))
  {}
};

// Context bundling allocator and raw data fetched for one refresh run.
struct MViewReportContext
{
  common::ObIAllocator *allocator_;
  MViewReportData *data_;

  MViewReportContext() : allocator_(NULL), data_(NULL) {}
};

inline bool has_distinct_mviews(const common::ObIArray<MViewReportMVData> &mv_array)
{
  return mv_array.count() > 1 && mv_array.at(0).mview_id_ != mv_array.at(mv_array.count() - 1).mview_id_;
}

inline bool should_skip_per_mv_detail(const int64_t num_distinct_mvs,
                                      const common::ObIArray<MViewReportMVData> &mv_array)
{
  return 0 == num_distinct_mvs || (mv_array.count() > 0 && mv_array.at(0).is_complete_refresh());
}

bool is_target_mv_missing(const MViewReportRunData &run_data, const common::ObIArray<MViewReportMVData> &mv_array);

int resolve_method_display(const MViewReportRunData &run_data,
                           common::ObIAllocator &allocator,
                           const common::ObIArray<MViewReportMVData> &mv_array,
                           const char *&method_display);

class ObMViewRefreshReportExecutor
{
public:
  ObMViewRefreshReportExecutor();
  ~ObMViewRefreshReportExecutor();
  DISABLE_COPY_ASSIGN(ObMViewRefreshReportExecutor);

  int execute(sql::ObExecContext &ctx,
              const ObMViewRefreshReportArg &arg,
              common::ObSqlString &report_text);

private:
  int resolve_refresh_id_(sql::ObExecContext &ctx,
                          const ObMViewRefreshReportArg &arg,
                          int64_t &refresh_id);

  static int resolve_mv_name_to_schema(sql::ObSQLSessionInfo &session,
                                       share::schema::ObSchemaGetterGuard &schema_guard,
                                       uint64_t tenant_id,
                                       const common::ObString &mv_name_input,
                                       const share::schema::ObTableSchema *&mv_schema,
                                       common::ObString &out_db_name,
                                       common::ObString &out_mv_name);
  static int resolve_mv_with_guard_(sql::ObExecContext &ctx,
                                    uint64_t target_tenant_id,
                                    const common::ObString &mv_name,
                                    share::schema::ObSchemaGetterGuard &local_guard,
                                    const share::schema::ObTableSchema *&mv_schema,
                                    common::ObString &out_db_name,
                                    common::ObString &out_mv_name);
};

} // namespace storage
} // namespace oceanbase
