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

#ifndef OCEANBASE_SQL_OB_LAKE_TABLE_PARTITION_INFO_
#define OCEANBASE_SQL_OB_LAKE_TABLE_PARTITION_INFO_

#include "sql/optimizer/file_prune/ob_iceberg_file_pruner.h"
#include "sql/optimizer/file_prune/ob_hive_file_pruner.h"
#include "sql/optimizer/ob_table_partition_info.h"

namespace oceanbase
{
namespace share {
namespace schema {
class ObIcebergTableSchema;
}
}
namespace sql
{

class ObLakeTablePartitionInfo : public ObTablePartitionInfo
{
public:
  ObLakeTablePartitionInfo()
  : ObTablePartitionInfo(PartitionInfoType::LAKE_TABLE),
    is_hash_aggregate_(false),
    hash_count_(0),
    first_bucket_partition_value_offset_(-1),
    file_pruner_(NULL),
    iceberg_file_descs_()
  {}

  ObLakeTablePartitionInfo(common::ObIAllocator &allocator)
  : ObTablePartitionInfo(allocator, PartitionInfoType::LAKE_TABLE),
    is_hash_aggregate_(false),
    hash_count_(0),
    first_bucket_partition_value_offset_(-1),
    file_pruner_(NULL),
    iceberg_file_descs_()
  {}
  virtual ~ObLakeTablePartitionInfo() {}

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

  ObIArray<ObIcebergFileDesc*>& get_file_descs() { return iceberg_file_descs_; }

  int prune_file_and_select_location(ObSqlSchemaGuard &sql_schema_guard,
                                     const ObDMLStmt &stmt,
                                     ObExecContext *exec_ctx,
                                     const uint64_t table_id,
                                     const uint64_t ref_table_id,
                                     const ObIArray<ObRawExpr*> &filter_exprs);

  int select_location_for_iceberg(ObExecContext *exec_ctx,
                                  hash::ObHashMap<ObLakeTablePartKey, uint64_t> &part_key_map,
                                  ObIArray<ObIcebergFileDesc*> &file_descs);

  int select_location_for_hive(ObExecContext *exec_ctx, ObIArray<ObHiveFileDesc> &file_descs);
  int get_partition_values(ObIArray<ObString> &partition_values) const;
private:
  int check_iceberg_use_hash_part(const ObIArray<iceberg::PartitionSpec*> &partition_specs, int64_t &offset);
  int get_bucket_idx(const ObLakeTablePartKey &part_key, const int64_t offset, int32_t &bucket_idx);
  int init_tablet_loc_by_addr(ObCandiTabletLoc &tablet_loc, const ObAddr &addr, const uint64_t part_id);
  int add_table_file(ObCandiTabletLoc &tablet_loc, ObIcebergFileDesc *file_desc);
  int add_table_file_for_hive(ObCandiTabletLoc &tablet_loc, ObHiveFileDesc &file_desc);

private:
  // 外表的分区是否是按照hash聚合的
  bool is_hash_aggregate_;
  // hash聚合的外表的总分区数
  int64_t hash_count_;
  // 第一个使用 bucket 的 partition value 的 offset
  int64_t first_bucket_partition_value_offset_ = -1;
  ObILakeTableFilePruner *file_pruner_;
  ObSEArray<ObIcebergFileDesc*, 16, common::ModulePageAllocator, true> iceberg_file_descs_;
};


}
}
#endif // OCEANBASE_SQL_OB_LAKE_TABLE_PARTITION_INFO_
