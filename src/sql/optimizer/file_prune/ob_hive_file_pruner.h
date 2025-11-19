/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_HIVE_FILE_PRUNER_H
#define _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_HIVE_FILE_PRUNER_H

#include "ob_i_lake_table_file_pruner.h"
#include "sql/das/ob_das_define.h"
#include "sql/engine/table/ob_external_table_pushdown_filter.h"
#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObRawExpr;
class ObSqlSchemaGuard;

struct ObHiveFileDesc
{
  ObHiveFileDesc() : part_id_(OB_INVALID_PARTITION_ID), file_size_(0), modify_ts_(0)
  {
  }
  TO_STRING_KV(K(part_id_), K(file_size_), K(modify_ts_), K(file_path_));
  int64_t part_id_;
  int64_t file_size_;
  int64_t modify_ts_;
  ObString file_path_;
};

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

public:
  TO_STRING_KV(K_(loc_meta), K_(is_partitioned), K_(need_all));

private:
  ObSqlSchemaGuard *sql_schema_guard_;
  ObFixedArray<ObHivePartFieldBound *, common::ObIAllocator> hive_part_bounds_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> part_column_ids_;
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
