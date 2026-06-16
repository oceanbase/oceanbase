/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_HIVE_FILE_PRUNER_H
#define _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_HIVE_FILE_PRUNER_H

#include "ob_i_lake_table_file_pruner.h"
#include "ob_opt_hive_define_fwd.h"
#include "sql/das/ob_das_define.h"
#include "sql/engine/table/ob_external_table_pushdown_filter.h"
#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObRawExpr;
class ObSqlSchemaGuard;
struct HiveTableFileCache;

struct ObHivePartFieldBound
{
public:
  OB_UNIS_VERSION(1);

public:
  ObHivePartFieldBound(common::ObIAllocator &allocator);
  void reset();
  int assign(const ObHivePartFieldBound &other);
  int deep_copy(ObHivePartFieldBound &src);
  TO_STRING_KV(K_(column_id), K_(is_whole_range), K_(is_always_false), K_(bounds), K_(range_exprs));

  common::ObIAllocator &allocator_;
  uint64_t column_id_;
  bool is_whole_range_;
  bool is_always_false_;
  ObFixedArray<ObFieldBound *, ObIAllocator> bounds_;
  ObFixedArray<ObRawExpr*, ObIAllocator> range_exprs_;

private:
  DISABLE_COPY_ASSIGN(ObHivePartFieldBound);
};

class ObHiveFilePruner : public ObILakeTableFilePruner
{
public:
  OB_UNIS_VERSION(1);

public:
  explicit ObHiveFilePruner(common::ObIAllocator &allocator);
  virtual ~ObHiveFilePruner()
  {
    reset();
  }

  void reset();
  virtual int assign(const ObILakeTableFilePruner &o);
  int init(ObSqlSchemaGuard &sql_schema_guard,
           const ObDMLStmt &stmt,
           ObExecContext *exec_ctx,
           const uint64_t table_id,
           const uint64_t ref_table_id,
           const ObIArray<ObRawExpr *> &filter_exprs);

  int prunner_files(ObExecContext &exec_ctx,
                    ObIArray<ObHiveFileDesc> &filtered_files);
  int get_part_id_and_range_exprs(ObIArray<uint64_t> &part_column_ids,
                                  ObIArray<ObRawExpr*> &range_exprs);
private:
  int prune_partition_by_hms(ObExecContext &exec_ctx,
                             ObIArray<ObHiveFileDesc> &filtered_files);

  int generate_partition_bound(const ObDMLStmt &stmt,
                               ObExecContext *exec_ctx,
                               const ObTableSchema *table_schema,
                               const ObIArray<ObRawExpr *> &filter_exprs);

  int build_field_bound_from_ranges(ObIArray<ObNewRange *> &ranges,
                                    ObHivePartFieldBound &part_field_bound);

  int construct_partition_values(common::ObIAllocator &allocator,
                                 const common::ObIArray<common::ObString> &partition_column_names,
                                 const common::ObIArrayWrap<common::ObString> &partition_values,
                                 common::ObString &partition_value_str);

private:
  DISABLE_COPY_ASSIGN(ObHiveFilePruner);
  bool check_one_row_part_column(ObNewRow ob_part_row);
  bool check_one_part(ObObj &part_val, ObHivePartFieldBound &field_bounds);
  bool is_hive_default_partition_obj_(const ObObj &obj) const;
  bool is_hive_default_point_bound_(const ObFieldBound &bound) const;

  // DBMS_STATS + WHERE IN fast path: small (M bounds) drives big (N partitions)
  int filter_partitions_by_str_hash_(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      HiveTableFileCache &cache_info,
      ObIArray<HivePartitionInfo *> &partition_infos,
      ObIAllocator &tmp_allocator,
      ObIArray<int64_t> &selected_part_idxs,
      ObIArray<int64_t> &tmp_part_id,
      ObIArray<common::ObString> &tmp_part_path);

  int register_matched_partition_(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      HiveTableFileCache &cache_info,
      const HivePartitionInfo &partition_info,
      int64_t part_info_idx,
      ObNewRow &ob_part_row,
      ObIArray<int64_t> &selected_part_idxs,
      ObIArray<int64_t> &tmp_part_id,
      ObIArray<common::ObString> &tmp_part_path);

  static int point_bound_to_hive_string_(
      const common::ObObj &obj,
      common::ObIAllocator &allocator,
      common::ObString &result);

public:
  TO_STRING_KV(K_(loc_meta), K_(is_partitioned), K_(need_all), K_(use_fast_path));

private:
  ObSqlSchemaGuard *sql_schema_guard_;
  ObFixedArray<ObHivePartFieldBound *, common::ObIAllocator> hive_part_bounds_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> part_column_ids_;
  bool use_fast_path_;
};

class ObHivePushDownFilter : public ObLakeTablePushDownFilter
{
public:
  ObHivePushDownFilter(ObExecContext &exec_ctx,
                       ObLakeTablePushDownFilterSpec &file_filter_spec,
                       common::ObFixedArray<uint64_t, common::ObIAllocator> *part_column_ids)
      : ObLakeTablePushDownFilter(exec_ctx, file_filter_spec), part_column_ids_(part_column_ids)
  {
  }

  virtual ~ObHivePushDownFilter()
  {
  }

private:
  class HivePartitionFilterParamBuilder : public MinMaxFilterParamBuilder
  {
  public:
    explicit HivePartitionFilterParamBuilder(
        ObNewRow &row,
        common::ObFixedArray<uint64_t, common::ObIAllocator> &part_column_ids)
        : row_(row), part_column_ids_(part_column_ids)
    {
    }
    virtual ~HivePartitionFilterParamBuilder()
    {
    }
    int build(const int32_t ext_tbl_col_id,
              const ObColumnMeta &column_meta,
              blocksstable::ObMinMaxFilterParam &param) override;
    int next_range(const int64_t column_id, int64_t &offset, int64_t &rows)
    {
      return OB_NOT_SUPPORTED;
    }

  private:
    ObNewRow &row_;
    common::ObFixedArray<uint64_t, common::ObIAllocator> &part_column_ids_;
  };

public:
  int filter(ObNewRow row, bool &is_filtered);

private:
  common::ObFixedArray<uint64_t, common::ObIAllocator> *part_column_ids_;
};

} // namespace sql
} // namespace oceanbase
#endif
