/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_LAKE_TABLE_PARTITION_INFO_
#define OCEANBASE_SQL_OB_LAKE_TABLE_PARTITION_INFO_

#include "lib/hash/ob_hashmap.h"
#include "sql/optimizer/file_prune/ob_ext_file_pruner.h"
#include "sql/optimizer/file_prune/ob_hive_file_pruner.h"
#include "sql/optimizer/file_prune/ob_iceberg_file_pruner.h"
#include "sql/optimizer/file_prune/ob_lake_table_fwd.h"
#include "sql/optimizer/ob_table_partition_info.h"

namespace oceanbase
{
namespace share {
class ObExternalTablePartInfoArray;
namespace schema {
class ObIcebergTableSchema;
}
}
namespace sql
{

class ObRawExprResType;

// Map 从少量 bucket 起步并按 2 倍扩容，避免唯一分区数远小于文件数时过度预分配。
typedef common::hash::ObHashMap<
    ObLakeTablePartKey,
    uint64_t,
    common::hash::NoPthreadDefendMode,
    common::hash::hash_func<ObLakeTablePartKey>,
    common::hash::equal_to<ObLakeTablePartKey>,
    common::hash::SimpleAllocer<
        typename common::hash::HashMapTypes<ObLakeTablePartKey, uint64_t>::AllocType>,
    common::hash::NormalPointer,
    common::ObMalloc,
    2 /* EXTEND_RATIO */>
    ObLakeTablePartKeyMap;

class ObLakeTablePartitionInfo : public ObTablePartitionInfo
{
public:
  ObLakeTablePartitionInfo(common::ObIAllocator &allocator)
      : ObTablePartitionInfo(allocator), allocator_(allocator), is_hash_aggregate_(false),
        hash_count_(0), first_bucket_partition_value_offset_(-1), file_pruner_(NULL),
        iceberg_file_descs_(allocator)
  {}
  virtual ~ObLakeTablePartitionInfo() override;

  virtual int assign(const ObTablePartitionInfo &other) override;
  virtual uint64_t get_table_id() const override
  {
    return file_pruner_->get_table_id();
  }
  virtual uint64_t get_ref_table_id() const override
  {
    return file_pruner_->get_ref_table_id();
  }
  virtual share::schema::ObPartitionLevel get_part_level() const override
  {
    return file_pruner_->is_partitioned() ? share::schema::ObPartitionLevel::PARTITION_LEVEL_ONE
                                           : share::schema::ObPartitionLevel::PARTITION_LEVEL_ZERO;
  }
  virtual PartitionInfoType get_partition_info_type() const override
  {
    return PartitionInfoType::LAKE_TABLE;
  }
  virtual const ObDASTableLocMeta &get_loc_meta() const override
  {
    return file_pruner_->get_loc_meta();
  }

  virtual ObDASTableLocMeta &get_loc_meta() override
  {
    return file_pruner_->get_loc_meta();
  }
  virtual int replace_final_location_key(ObExecContext &exec_ctx, uint64_t ref_table_id, bool is_local_index) override
  {
    return OB_SUCCESS;
  }

  void set_is_hash_aggregate(bool v) { is_hash_aggregate_ = v; }
  bool is_hash_aggregate() const { return is_hash_aggregate_; }
  void set_hash_count(int64_t v) { hash_count_ = v; }
  int64_t get_hash_count() const { return hash_count_; }
  const ObILakeTableFilePruner *get_file_pruner() const { return file_pruner_; }
  ObILakeTableFilePruner *get_file_pruner() { return file_pruner_; }

  ObIArray<ObIcebergFileDesc*>& get_file_descs() { return iceberg_file_descs_; }

  int prune_file_and_select_location(ObSqlSchemaGuard &sql_schema_guard,
                                     const ObDMLStmt &stmt,
                                     ObExecContext *exec_ctx,
                                     const uint64_t table_id,
                                     const uint64_t ref_table_id,
                                     int64_t lake_table_snapshot_id,
                                     const ObIArray<ObRawExpr*> &filter_exprs);

  // 为保留文件选择执行节点；bucket 表复用预先生成的稠密分区 ID。
  int select_location_for_iceberg(ObExecContext *exec_ctx,
                                  ObLakeTablePartKeyMap &part_key_map,
                                  ObIArray<ObIcebergFileDesc *> &file_descs);
  // 为实际访问的 source field 构造 part_id 到 Identity 值的共享映射。
  int prepare_iceberg_partition_infos(ObSqlSchemaGuard &sql_schema_guard,
                                      const common::ObIArray<ObRawExpr *> &file_column_exprs,
                                      common::ObIAllocator &partition_info_allocator,
                                      share::ObExternalTablePartInfoArray &partition_infos);

  int select_location_for_hive(ObExecContext *exec_ctx, ObIArray<ObHiveFileDesc> &file_descs);
  int select_location_for_plugin(ObExecContext *exec_ctx,
                                 ObIArray<ObPluginSplitDesc *> &plugin_splits,
                                 ObExtTableDispatchMode dispatch_mode);
  int get_partition_values(ObIArray<ObString> &partition_values) const;
private:
  // 按 (spec_id, partition tuple) 去重并生成查询内稠密分区 ID。
  int build_iceberg_part_ids(ObLakeTablePartKeyMap &part_key_map,
                             ObIArray<ObIcebergFileDesc *> &file_descs);
  // 为一个唯一分区物化紧凑的 (source_id, value) 行。
  int build_iceberg_partition_row(const iceberg::PartitionSpec &partition_spec,
                                  const iceberg::ManifestEntry &manifest_entry,
                                  const common::ObIArray<int32_t> &source_field_ids,
                                  const common::ObIArray<ObRawExpr *> &file_column_exprs,
                                  common::ObIAllocator &cast_allocator,
                                  common::ObIAllocator &partition_info_allocator,
                                  common::ObNewRow &partition_row);
  static int cast_iceberg_partition_value(const common::ObObj &source_value,
                                          const ObRawExprResType &target_type,
                                          common::ObIAllocator &allocator,
                                          common::ObObj &target_value);
  // 将 file desc 上的分区 ID 回填到优化器文件对象。
  int update_iceberg_file_part_ids();
  int check_iceberg_use_hash_part(const ObIArray<iceberg::PartitionSpec*> &partition_specs, int64_t &offset);
  int get_bucket_idx(const ObLakeTablePartKey &part_key, const int64_t offset, int32_t &bucket_idx);
  int init_tablet_loc_by_addr(ObCandiTabletLoc &tablet_loc, const ObAddr &addr, const uint64_t part_id);
  int add_table_file(ObCandiTabletLoc &tablet_loc,
                     ObIcebergFileDesc *file_desc,
                     const int64_t file_desc_idx);
  int add_table_file_for_hive(ObCandiTabletLoc &tablet_loc, ObHiveFileDesc &file_desc);
  int select_location_for_plugin_round_robin(ObExecContext *exec_ctx,
                                             ObIArray<ObPluginSplitDesc *> &plugin_splits);
  int add_table_file_for_plugin(ObCandiTabletLoc &tablet_loc,
                                ObPluginSplitDesc *split_desc);
  template <typename T>
  int filter_files_by_sample(const ObDMLStmt &stmt, const uint64_t table_id, common::ObIArray<T> &files);

private:
  ObIAllocator &allocator_;
  // 外表的分区是否是按照hash聚合的
  bool is_hash_aggregate_;
  // hash聚合的外表的总分区数
  int64_t hash_count_;
  // 第一个使用 bucket 的 partition value 的 offset
  int64_t first_bucket_partition_value_offset_ = -1;
  ObILakeTableFilePruner *file_pruner_;
  ObSqlArray<ObIcebergFileDesc*> iceberg_file_descs_;
};


}
}
#endif // OCEANBASE_SQL_OB_LAKE_TABLE_PARTITION_INFO_
