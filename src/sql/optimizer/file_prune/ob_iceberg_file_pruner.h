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

#ifndef _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_ICEBERG_FILE_PRUNER_H
#define _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_ICEBERG_FILE_PRUNER_H

#include "lib/hash/ob_pointer_hashmap.h"
#include "ob_i_lake_table_file_pruner.h"
#include "ob_iceberg_file_filter.h"
#include "ob_lake_table_fwd.h"
#include "sql/engine/table/ob_external_table_pushdown_filter.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/table_format/iceberg/scan/task.h"
#include "sql/table_format/iceberg/spec/manifest.h"
#include "sql/table_format/iceberg/spec/manifest_list.h"
#include "sql/table_format/iceberg/spec/partition.h"
#include "sql/table_format/iceberg/spec/table_metadata.h"

namespace oceanbase
{
namespace sql
{
class ObRawExpr;
class ObSqlSchemaGuard;


struct ObPartFieldBound
{
public:
  OB_UNIS_VERSION(1);
public:
  ObPartFieldBound(common::ObIAllocator &allocator);
  void reset();
  int assign(const ObPartFieldBound &other);
  int deep_copy(ObPartFieldBound &src);
  TO_STRING_KV(K_(column_id), K_(transform_type), K_(is_whole_range), K_(is_always_false), K_(bounds));

  common::ObIAllocator &allocator_;
  uint64_t column_id_;
  iceberg::TransformType transform_type_;
  bool is_whole_range_;
  bool is_always_false_;
  ObFixedArray<ObFieldBound*, ObIAllocator> bounds_;
private:
  DISABLE_COPY_ASSIGN(ObPartFieldBound);
};

struct ObIcebergPartBound
{
public:
  OB_UNIS_VERSION(1);
public:
  ObIcebergPartBound(common::ObIAllocator &allocator);
  void reset();
  int assign(const ObIcebergPartBound &other);
  int deep_copy(ObIcebergPartBound &src);
  TO_STRING_KV(K_(part_field_bounds));

  common::ObIAllocator &allocator_;
  ObFixedArray<ObPartFieldBound*, ObIAllocator> part_field_bounds_;
private:
  DISABLE_COPY_ASSIGN(ObIcebergPartBound);
};

struct ObIcebergFileDesc
{
  ObIcebergFileDesc(ObIAllocator &allocator)
      : part_idx_(OB_INVALID_ID), entry_(nullptr),
        delete_files_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator))
  {
  }
  TO_STRING_KV(K_(part_idx));
  uint64_t part_idx_;
  iceberg::ManifestEntry *entry_;
  ObArray<const iceberg::ManifestEntry *> delete_files_;
};

class ObIcebergFilePrunner : public ObILakeTableFilePruner
{
public:
  OB_UNIS_VERSION(1);
public:
  explicit ObIcebergFilePrunner(common::ObIAllocator &allocator);
  virtual ~ObIcebergFilePrunner() { reset(); }

  void reset();
  virtual int assign(const ObILakeTableFilePruner &other);
  int init(ObSqlSchemaGuard *schema_guard,
           const ObDMLStmt &stmt,
           ObExecContext *exec_ctx,
           const uint64_t table_id,
           const uint64_t ref_table_id,
           const ObIArray<iceberg::PartitionSpec *> &partition_specs,
           const ObIArray<ObRawExpr*> &filter_exprs);

  int prune_manifest_files(ObIArray<iceberg::ManifestFile*> &manifest_list,
                           ObIArray<iceberg::ManifestFile*> &valid_manifest_list);
  int prune_data_files(ObExecContext &exec_ctx,
                       ObIArray<iceberg::ManifestEntry*> &manifest_entries,
                       hash::ObHashMap<ObLakeTablePartKey, uint64_t> &part_key_map,
                       ObIArray<ObIcebergFileDesc*> &file_descs);

  static int transform_bucket_range(ObNewRange &range, const int64_t N);
private:
  int genearte_partition_bound(const ObDMLStmt &stmt,
                               ObExecContext *exec_ctx,
                               const ObTableSchema *table_schema,
                               const ObIArray<iceberg::PartitionSpec*> &partition_specs,
                               const ObIArray<ObRawExpr*> &filter_exprs);
  int build_field_bound_from_ranges(ObIArray<ObNewRange*> &ranges,
                                    ObPartFieldBound &part_field_bound);

  int transform_bound_by_part_type(iceberg::Transform &transform,
                                   ColumnItem &column_item,
                                   ObIArray<ObNewRange*> &part_bounds,
                                   ObExecContext *exec_ctx,
                                   const ObTableSchema *table_schema);

  static int transform_truncate_range(ObNewRange &range, const int64_t W, ObIAllocator &allocator);
  static int transform_time_range(ObNewRange &range,
                           iceberg::TransformType type,
                           ObSQLSessionInfo *session_info);
  static int transform_void_range(ObNewRange &range);
  static int obj_to_ob_time(ObObj &val, ObTime &ob_time, ObSQLSessionInfo *session_info);
  int from_partition_field_summary(ObIAllocator &allocator,
                                   ObFieldBound &field_bound,
                                   iceberg::TransformType transform_type,
                                   ObColumnMeta &column_meta,
                                   iceberg::PartitionFieldSummary &summary);

  int check_manifest_file_in_bound(ObIAllocator &allocator,
                                   iceberg::ManifestFile& manifest_file,
                                   ObIcebergPartBound& part_bound,
                                   bool &in_bound);

  ObIcebergPartBound* get_part_bound_by_spec_id(int64_t spec_id);

  int check_manifest_entry_in_bound(iceberg::ManifestEntry& manifest_entry,
                                    ObIcebergPartBound& part_bound,
                                    bool &in_bound);

private:
  DISABLE_COPY_ASSIGN(ObIcebergFilePrunner);
public:
  TO_STRING_KV(K_(loc_meta), K_(is_partitioned), K_(need_all));
private:
  common::ObFixedArray<PartColDesc, common::ObIAllocator> part_column_descs_;
  common::ObFixedArray<std::pair<int32_t, ObIcebergPartBound*>, common::ObIAllocator> part_bound_;
};

}
}
#endif
