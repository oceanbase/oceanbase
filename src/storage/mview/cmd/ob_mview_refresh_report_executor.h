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

// Top-level metadata of one refresh, from __all_virtual_mview_refresh_run_stats.
struct MViewReportRunData
{
  MViewReportRunData() { reset(); }
  void reset()
  {
    refresh_id_ = common::OB_INVALID_ID;
    run_user_id_ = 0;
    push_deferred_rpc_ = false;
    refresh_after_errors_ = false;
    purge_option_ = 0;
    parallelism_ = 0;
    heap_size_ = 0;
    atomic_refresh_ = false;
    nested_ = false;
    out_of_place_ = false;
    number_of_failures_ = 0;
    start_time_ = 0;
    end_time_ = 0;
    elapsed_time_ = 0;
    log_purge_time_ = 0;
    complete_stats_available_ = false;
    mview_id_ = 0;
    data_target_scn_ = 0;
    is_valid_ = false;
    run_owner_.reset();
    method_.reset();
    trace_id_.reset();
    mview_name_.reset();
  }
  TO_STRING_KV(K_(refresh_id), K_(is_valid));

  int64_t refresh_id_;
  int64_t run_user_id_;
  common::ObString run_owner_;
  common::ObString method_;
  bool push_deferred_rpc_;
  bool refresh_after_errors_;
  int64_t purge_option_;
  int64_t parallelism_;
  int64_t heap_size_;
  bool atomic_refresh_;
  bool nested_;
  bool out_of_place_;
  int64_t number_of_failures_;
  int64_t start_time_;
  int64_t end_time_;
  int64_t elapsed_time_;
  int64_t log_purge_time_;
  bool complete_stats_available_;
  common::ObString trace_id_;
  int64_t mview_id_;
  uint64_t data_target_scn_;
  common::ObString mview_name_;
  bool is_valid_;
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
    num_steps_ = 0;
    result_ = 0;
    topo_order_ = 0;
    deps_ready_time_ = 0;
    mv_refresh_start_scn_ = 0;
    mv_refresh_end_scn_ = 0;
    base_table_start_scn_ = 0;
    svr_port_ = 0;
    mv_owner_.reset();
    mv_name_.reset();
    svr_ip_.reset();
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
  int64_t num_steps_;
  int64_t result_;
  int64_t topo_order_;
  int64_t deps_ready_time_;
  uint64_t mv_refresh_start_scn_;
  uint64_t mv_refresh_end_scn_;
  uint64_t base_table_start_scn_;
  common::ObString mv_owner_;
  common::ObString mv_name_;
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
    tbl_owner_.reset();
    tbl_name_.reset();
  }
  TO_STRING_KV(K_(mview_id), K_(detail_table_id));

  int64_t mview_id_;
  int64_t retry_id_;
  int64_t detail_table_id_;
  int64_t num_rows_ins_;
  int64_t num_rows_upd_;
  int64_t num_rows_del_;
  int64_t num_rows_;
  common::ObString tbl_owner_;
  common::ObString tbl_name_;
};

// Per-step SQL stats, statement text, and execution plan, from __all_virtual_mview_refresh_stmt_stats.
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
    affected_rows_ = 0;
    memory_used_ = 0;
    sqlid_.reset();
    stmt_.reset();
    execution_plan_.reset();
  }
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
  int64_t affected_rows_;
  int64_t memory_used_;
  common::ObString sqlid_;
  common::ObString stmt_;
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
  int resolve_names(sql::ObExecContext &ctx,
                     uint64_t tenant_id,
                     MViewReportContext &context);

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
