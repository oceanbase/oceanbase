/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_ODPS_FILE_PRUNER_H
#define _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_ODPS_FILE_PRUNER_H

#include "ob_i_lake_table_file_pruner.h"
#include "ob_lake_table_fwd.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "sql/das/ob_das_define.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObRawExpr;
class ObSqlSchemaGuard;

/// One filtered/projected ODPS column as seen by the optimizer-time pushdown
/// predicate printer: ties the OB schema column id — which is what the
/// re-derived pushdown filter tree carries in its white filter nodes — to the
/// pseudo column the column is generated from (pseudo expr type + 1-based odps
/// column index, matching the runtime ObExpr::extra_).
struct ObOdpsPredColumnInfo
{
  ObOdpsPredColumnInfo() : column_id_(common::OB_INVALID_ID), pseudo_type_(T_INVALID), column_idx_(0) {}
  ObOdpsPredColumnInfo(uint64_t column_id, ObItemType pseudo_type, int64_t column_idx)
      : column_id_(column_id), pseudo_type_(pseudo_type), column_idx_(column_idx) {}
  uint64_t column_id_;
  ObItemType pseudo_type_;
  int64_t column_idx_;
  TO_STRING_KV(K_(column_id), K_(pseudo_type), K_(column_idx));
};

/// One server slot's scan units. Slot i is the i-th alive server; placement
/// onto candidate tablet locs happens in
/// ObLakeTablePartitionInfo::select_location_for_odps.
struct ObOdpsSlotFiles
{
  ObOdpsSlotFiles() : files_() {}
  common::ObIArray<ObOptOdpsFile *> &get_files() { return files_; }
  const common::ObIArray<ObOptOdpsFile *> &get_files() const { return files_; }
  TO_STRING_KV(K_(files));
  common::ObSEArray<ObOptOdpsFile *, 4> files_;
};

/// Optimizer-time partition pruner for ODPS (MaxCompute) tables read through the
/// lake access path.
///
/// An ODPS table is materialized in the OB schema as a one-level LIST table
/// (PARTITION_LEVEL_ONE only; no subpartition). Multiple ODPS partition keys
/// (e.g. ds, hh) are LIST COLUMNS cells of that single level, not part+subpart.
/// One schema partition per ODPS partition, whose list row holds one cell per
/// partition key column (the `metadata$external_partition[i]` pseudo columns).
/// The ODPS partition spec string that the row iterators consume is rebuilt
/// from that list row with ObOdpsCatalogUtils::construct_partition_values (the
/// same formatter the stats path uses) — never read from external_location,
/// which is not reliably the spec (for CREATE EXTERNAL TABLE schemas it only
/// holds the auto-generated partition name).
/// Non-partitioned ODPS tables have no partition key columns and are scanned
/// as a single implicit partition with an empty spec.
///
/// Pruning strategy (see prune_partitions for the parity discussion): identical
/// to ObHiveFilePruner — per partition key column query ranges are extracted from
/// the filters (ObPreRangeGraph) and every partition's list row is checked
/// against those bounds plus the pushdown filter (ObLakePartRowPushDownFilter,
/// driven purely by part_column_ids + the list row).
///
/// The call chain:
///   init -> prune_partitions -> (filter_files_by_sample) -> plan_files
///        -> ObLakeTablePartitionInfo::select_location_for_odps
class ObODPSFilePruner : public ObILakeTableFilePruner
{
public:
  OB_UNIS_VERSION(1);

public:
  explicit ObODPSFilePruner(common::ObIAllocator &allocator);
  virtual ~ObODPSFilePruner()
  {
    reset();
  }

  void reset();
  virtual int assign(const ObILakeTableFilePruner &o) override;
  virtual int clone(common::ObIAllocator &allocator, ObILakeTableFilePruner *&pruner) const override;

  int init(ObSqlSchemaGuard &sql_schema_guard,
           const ObDMLStmt &stmt,
           ObExecContext *exec_ctx,
           const uint64_t table_id,
           const uint64_t ref_table_id,
           const common::ObIArray<ObRawExpr *> &filter_exprs);

  /// Select the surviving first-level ODPS partitions and emit one
  /// ObOptOdpsFile per partition (possibly empty). No ODPS API is
  /// called here — the size / row count / download session fetch happens right
  /// after, in plan_files.
  int prune_partitions(ObExecContext &exec_ctx, common::ObIArray<ObOptOdpsFile *> &files);

  /// Plans the final scan units at optimizer time. Takes the first-level
  /// partition units produced by prune_partitions (already sample-filtered)
  /// and replaces them with the final scan units (fetch + split + slot
  /// assignment in assign_odps_file_to_sqcs_). Every unit also reports the
  /// server slot (0..server_count-1) the assignment heuristic picked for it;
  /// the actual server placement (candidate tablet location per alive server)
  /// happens in ObLakeTablePartitionInfo::select_location_for_odps. When no
  /// real unit is produced (all partitions pruned / 0-row table) one dummy
  /// unit is emitted so the lake file map always has an entry.
  int plan_files(ObExecContext &exec_ctx,
                 const ObDMLStmt &stmt,
                 common::ObIArray<ObOptOdpsFile *> &files,
                 common::ObIArray<int64_t> &slot_idxs,
                 const int64_t estimated_parallel = 0);

  /// ODPS api mode parsed from the schema's external file format; decides how
  /// plan_files turns the selected partitions into scan units.
  ObODPSGeneralFormat::ApiMode get_api_mode() const { return api_mode_; }
  bool is_tunnel_api_mode() const
  {
    return ObODPSGeneralFormat::ApiMode::TUNNEL_API == api_mode_;
  }

  // The schema's external file format / properties string; carries the whole
  // ODPS connection (endpoint/credentials/project/table/api mode). The stats
  // path reuses it to talk to ODPS for CREATE EXTERNAL TABLE, which has no
  // catalog entity of its own.
  const common::ObString &get_format_str() const { return format_str_; }

  // Estimated row count of the selected partitions before pushed row filters.
  // A negative value means that no plan-time row count was available.
  bool get_planned_row_count(int64_t &row_count) const
  {
    bool valid = estimated_row_count_ >= 0;
    if (valid) {
      row_count = estimated_row_count_;
    }
    return valid;
  }

  // ---------------------------------------------------------------------
  // Load-aware ODPS assignment heuristic (covered directly by
  // unittest/share/catalog/odps/test_split_task.cpp).
  // ---------------------------------------------------------------------
  struct FileInfoWithIdx {
    int64_t file_size_;
    int64_t file_idx_;
    int64_t sqc_idx_;
    int64_t remain_file_size_;
    bool should_split_; // 是否需要分块
    int64_t start_permyriad_;
    int64_t end_permyriad_;
    int64_t process_size_;
    FileInfoWithIdx() : file_size_(0), file_idx_(-1), sqc_idx_(-1), remain_file_size_(0), should_split_(false), start_permyriad_(0), end_permyriad_(100), process_size_(0) {}
    FileInfoWithIdx(int64_t file_size, int64_t file_idx, int64_t sqc_idx, int64_t remain_file_size, bool should_split, int64_t start_permyriad, int64_t end_permyriad)
      : file_size_(file_size), file_idx_(file_idx), sqc_idx_(sqc_idx), remain_file_size_(remain_file_size), should_split_(should_split), start_permyriad_(start_permyriad), end_permyriad_(end_permyriad), process_size_(0) {}
    TO_STRING_KV(K(file_size_), K(file_idx_), K(sqc_idx_), K(remain_file_size_), K(should_split_), K(process_size_));
  };

    // SQC文件集合结构，用于跟踪每个SQC的负载情况
  struct SqcFileSet {
    int64_t total_file_size_;
    int64_t total_file_count_;
    int64_t total_processing_time_ms_; // 总处理时间（毫秒）
    int64_t sqc_idx_;
    SqcFileSet() : total_file_size_(0), total_file_count_(0), total_processing_time_ms_(0), sqc_idx_(-1) {}
    // 计算添加文件后的总处理时间
    int64_t calc_total_time_with_file(int64_t file_size, int64_t file_count = 1) const {
      int64_t processing_time = file_size / (10 * 1024 * 1024); // 处理时间（毫秒），10MB/s
      int64_t open_cost = file_count * 1200; // 文件打开成本，1200ms
      return total_processing_time_ms_ + processing_time + open_cost;
    }
    // 添加文件到当前SQC
    int add_file(int64_t file_size, int64_t file_count = 1) {
      int ret = OB_SUCCESS;
      total_file_size_ += file_size;
      total_file_count_ += file_count;
      total_processing_time_ms_ = calc_total_time_with_file(total_file_size_, total_file_count_); // 重新计算总时间
      if (total_processing_time_ms_ < 0) {
        ret = OB_ERROR_OUT_OF_RANGE;
      }
      return ret;
    }
    TO_STRING_KV(K(total_file_size_), K(total_file_count_), K(total_processing_time_ms_), K(sqc_idx_));
  };

  static int calc_assigned_odps_files_to_sqcs_optimized(
      const common::ObIArray<int64_t> &file_sizes,
      common::ObSEArray<FileInfoWithIdx, 20> &assigned_idx,
      int64_t sqc_count);

public:
  TO_STRING_KV(K_(loc_meta), K_(is_partitioned), K_(need_all), K_(all_partitions_selected),
               K_(api_mode));

private:
  int create_odps_file_(const common::ObString &partition_spec,
                        const int64_t part_id,
                        ObOptOdpsFile *&file);
  int fill_partition_values_(const share::schema::ObTableSchema &table_schema,
                             const common::ObIArray<const share::schema::ObPartition *> &parts);
  /// Resolve the authoritative ODPS partition spec per selected partition.
  /// Catalog (mocked) tables carry the driver-reported spec in the mocked
  /// partition's external_location (written by the resolver at resolve time,
  /// never persisted); CREATE EXTERNAL TABLE tables read it from the
  /// __all_external_table_file mirror through the file-manager KV cache. A
  /// partition with no authoritative spec (empty mirror row) yields an empty
  /// string and is skipped by the caller.
  int resolve_partition_specs_(ObExecContext &exec_ctx,
                               const common::ObIArray<const share::schema::ObPartition *> &parts,
                               common::ObIArray<common::ObString> &part_specs);
  /// Walks the statement's column items of this table and collects the projected
  /// ODPS column indexes from the pseudo column exprs hidden inside the (stored
  /// generated) column expressions — the optimizer-time twin of
  /// ObLogTableScan::extract_file_column_exprs_recursively.
  int collect_projected_column_idxs_(const ObDMLStmt &stmt);
  int collect_pseudo_col_idx_recursively_(const ObRawExpr *expr);
  /// Schema partition ids selected by an explicit PARTITION(p0, ...) clause
  /// (empty when the clause is absent), from TableItem::part_ids_.
  int collect_clause_part_ids_(const ObDMLStmt &stmt);
  /// Number of alive compute servers eligible for the scan (location service
  /// external_table_get, self as the single-node fallback).
  int get_server_count_(ObExecContext &exec_ctx, int64_t &server_count);

  // ---------------------------------------------------------------------
  // The assignment (assign_odps_file_to_sqcs_ and the per-partition size
  // fetch; the split_* helpers they drive are file-local in the .cpp). The
  // inputs come from the pruner itself and the output lands in per-slot lists.
  // ---------------------------------------------------------------------
  int assign_odps_file_to_sqcs_(ObExecContext &exec_ctx,
                                common::ObIArray<ObOptOdpsFile *> &files,
                                int64_t parallel,
                                common::ObIArray<ObOdpsSlotFiles> &slot_lists);
  int fetch_odps_all_partitions_info_for_task_assign_(ObExecContext &exec_ctx,
                                                      common::ObIArray<ObOptOdpsFile *> &files,
                                                      bool &one_partition_per_thread);

private:
  DISABLE_COPY_ASSIGN(ObODPSFilePruner);
  ObSqlSchemaGuard *sql_schema_guard_;
  // one entry per partition key column, in partition key order
  common::ObFixedArray<ObLakePartFieldBound *, common::ObIAllocator> odps_part_bounds_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> part_column_ids_;
  ObODPSGeneralFormat::ApiMode api_mode_;
  common::ObString format_str_;
  common::ObSEArray<int64_t, 8> nonpart_col_idxs_;
  common::ObSEArray<int64_t, 8> part_col_idxs_;
  // (OB column id -> pseudo column type/idx) of the projected columns,
  // collected together with the idx arrays above; the optimizer-time predicate
  // printer resolves the white-filter column ids against it.
  common::ObSEArray<ObOdpsPredColumnInfo, 8> pred_col_infos_;
  // schema partition ids from an explicit PARTITION(p0, ...) clause
  common::ObSEArray<common::ObObjectID, 8> clause_part_ids_;
  // Selected-partition row count before row filters; -1 means unavailable.
  int64_t estimated_row_count_;
};

} // namespace sql
} // namespace oceanbase
#endif
