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
  virtual int clone(common::ObIAllocator &allocator, ObILakeTableFilePruner *&pruner) const override;
  int init(ObSqlSchemaGuard &sql_schema_guard,
           const ObDMLStmt &stmt,
           ObExecContext *exec_ctx,
           const uint64_t table_id,
           const uint64_t ref_table_id,
           const ObIArray<ObRawExpr *> &filter_exprs);

  int prune_files(ObExecContext &exec_ctx,
                  ObIArray<ObHiveFileDesc> &filtered_files);
  int get_part_id_and_range_exprs(ObIArray<uint64_t> &part_column_ids,
                                  ObIArray<ObRawExpr*> &range_exprs) override;
private:
  int prune_partition_by_hms(ObExecContext &exec_ctx,
                             ObIArray<ObHiveFileDesc> &filtered_files);

  int construct_partition_values(common::ObIAllocator &allocator,
                                 const common::ObIArray<common::ObString> &partition_column_names,
                                 const common::ObIArrayWrap<common::ObString> &partition_values,
                                 common::ObString &partition_value_str);

private:
  DISABLE_COPY_ASSIGN(ObHiveFilePruner);
  // Extends the base check with the hive default partition handling: the
  // "__HIVE_DEFAULT_PARTITION__" string maps to a NULL partition value in the
  // partition row, so a NULL cell also matches a default-partition point bound.
  virtual bool check_one_part(const common::ObObj &part_val,
                              const ObLakePartFieldBound &field_bounds) override;
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
  ObFixedArray<ObLakePartFieldBound *, common::ObIAllocator> hive_part_bounds_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> part_column_ids_;
  bool use_fast_path_;
};

} // namespace sql
} // namespace oceanbase
#endif
