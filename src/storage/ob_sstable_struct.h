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

#ifndef OB_SSTABLE_STRUCT_H_
#define OB_SSTABLE_STRUCT_H_

#include "blocksstable/ob_block_sstable_struct.h"
#include "ob_storage_struct.h"
#include "ob_i_table.h"

namespace oceanbase {
namespace storage {

const int64_t MAX_MERGE_THREAD = 64;
const int64_t MACRO_BLOCK_CNT_PER_THREAD = 128;

enum ObSSTableMergeInfoStatus { MERGE_START = 0, MERGE_RUNNING = 1, MERGE_FINISH = 2, MERGE_STATUS_MAX };

struct ObMultiVersionSSTableMergeInfo final {
public:
  ObMultiVersionSSTableMergeInfo();
  ~ObMultiVersionSSTableMergeInfo() = default;
  int add(const ObMultiVersionSSTableMergeInfo& info);
  void reset();
  TO_STRING_KV(K_(delete_logic_row_count), K_(update_logic_row_count), K_(insert_logic_row_count),
      K_(empty_delete_logic_row_count));

public:
  uint64_t delete_logic_row_count_;
  uint64_t update_logic_row_count_;
  uint64_t insert_logic_row_count_;
  uint64_t empty_delete_logic_row_count_;
};

struct ObSSTableMergeInfo final {
public:
  ObSSTableMergeInfo();
  ~ObSSTableMergeInfo() = default;
  int add(const ObSSTableMergeInfo& other);
  OB_INLINE bool is_major_merge() const
  {
    return storage::is_major_merge(merge_type_);
  }
  void dump_info(const char* msg);
  void reset();
  TO_STRING_KV("tenant_id", extract_tenant_id(table_id_), K_(table_id), K_(partition_id), K_(version), K_(table_type),
      K_(major_table_id), K_(table_count), K_(merge_start_time), K_(merge_finish_time), K_(merge_cost_time),
      K_(estimate_cost_time), K_(occupy_size), K_(macro_block_count), K_(use_old_macro_block_count),
      K_(total_row_count), K_(delete_row_count), K_(insert_row_count), K_(update_row_count), K_(base_row_count),
      K_(use_base_row_count), K_(memtable_row_count), K_(output_row_count), K_(purged_row_count), K_(merge_level),
      K_(rewrite_macro_old_micro_block_count), K_(rewrite_macro_total_micro_block_count), K_(error_code),
      K_(total_child_task), K_(finish_child_task), K_(is_complement), K_(merge_type), K_(step_merge_start_version),
      K_(step_merge_end_version), K_(macro_bloomfilter_count));

public:
  uint64_t table_id_;
  int64_t partition_id_;
  common::ObVersion version_;
  int32_t table_type_;
  uint64_t major_table_id_;
  int64_t merge_start_time_;
  int64_t merge_finish_time_;
  int64_t merge_cost_time_;
  int64_t estimate_cost_time_;
  int64_t occupy_size_;
  int64_t macro_block_count_;
  int64_t use_old_macro_block_count_;
  int64_t total_row_count_;
  int64_t delete_row_count_;
  int64_t insert_row_count_;
  int64_t update_row_count_;
  int64_t base_row_count_;
  int64_t use_base_row_count_;
  int64_t memtable_row_count_;
  int64_t output_row_count_;
  int64_t purged_row_count_;
  ObMergeLevel merge_level_;
  int64_t rewrite_macro_old_micro_block_count_;
  int64_t rewrite_macro_total_micro_block_count_;
  int32_t error_code_;
  int64_t total_child_task_;
  int64_t finish_child_task_;
  bool is_complement_;
  ObMergeType merge_type_;
  ObSSTableMergeInfoStatus merge_status_;
  int64_t step_merge_start_version_;
  int64_t step_merge_end_version_;
  int64_t snapshot_version_;
  int64_t column_cnt_;
  int64_t table_count_;
  int64_t macro_bloomfilter_count_;
};

struct ObMergeChecksumInfo final {
public:
  ObMergeChecksumInfo();
  ~ObMergeChecksumInfo() = default;
  int64_t* column_checksums_;
  int64_t** increment_column_checksums_;
  int64_t concurrent_cnt_;
  int64_t column_count_;
};

struct ObCreateSSTableParam {
public:
  ObCreateSSTableParam()
      : table_key_(),
        schema_version_(-1),
        progressive_merge_start_version_(0),
        progressive_merge_end_version_(0),
        create_snapshot_version_(0),
        dump_memtable_timestamp_(0),
        create_index_base_version_(0),
        checksum_method_(0),
        progressive_merge_round_(0),
        progressive_merge_step_(0),
        pg_key_(),
        logical_data_version_(0),
        has_compact_row_(false)
  {}
  virtual ~ObCreateSSTableParam() = default;
  bool is_valid() const
  {
    return table_key_.is_valid() && schema_version_ >= 0 && progressive_merge_start_version_ >= 0 &&
           progressive_merge_end_version_ >= 0 && create_snapshot_version_ >= 0 && dump_memtable_timestamp_ >= 0 &&
           create_index_base_version_ >= 0 &&
           ((!ObITable::is_major_sstable(table_key_.table_type_)) ||
               ObITable::is_trans_sstable(table_key_.table_type_) ||
               (ObITable::is_major_sstable(table_key_.table_type_) &&
                   checksum_method_ >= blocksstable::CCM_TYPE_AND_VALUE &&
                   checksum_method_ <= blocksstable::CCM_VALUE_ONLY)) &&
           ((ObITable::is_major_sstable(table_key_.table_type_) && logical_data_version_ >= table_key_.version_) ||
               logical_data_version_ >= table_key_.trans_version_range_.snapshot_version_);
  }
  int assign(const ObCreateSSTableParam& other);
  TO_STRING_KV(K_(table_key), K_(schema_version), K_(progressive_merge_start_version),
      K_(progressive_merge_end_version), K_(create_snapshot_version), K_(dump_memtable_timestamp), K_(checksum_method),
      K_(progressive_merge_round), K_(progressive_merge_step), K_(logical_data_version), K_(has_compact_row));
  ObITable::TableKey table_key_;
  int64_t schema_version_;
  int64_t progressive_merge_start_version_;
  int64_t progressive_merge_end_version_;
  int64_t create_snapshot_version_;
  int64_t dump_memtable_timestamp_;
  int64_t create_index_base_version_;
  int64_t checksum_method_;
  int64_t progressive_merge_round_;
  int64_t progressive_merge_step_;
  ObPGKey pg_key_;
  int64_t logical_data_version_;
  bool has_compact_row_;
};

struct ObCreateSSTableParamWithTable final : public ObCreateSSTableParam {
public:
  ObCreateSSTableParamWithTable() : ObCreateSSTableParam(), schema_(NULL)
  {}
  ~ObCreateSSTableParamWithTable() = default;
  bool is_valid() const
  {
    return ObCreateSSTableParam::is_valid() && NULL != schema_;
  }
  TO_STRING_KV(K_(table_key), KP_(schema), K_(schema_version), K_(progressive_merge_start_version),
      K_(progressive_merge_end_version), K_(create_snapshot_version), K_(checksum_method), K_(progressive_merge_round),
      K_(progressive_merge_step), K_(logical_data_version));

public:
  const share::schema::ObTableSchema* schema_;
};

struct ObCreateSSTableParamWithPartition final : public ObCreateSSTableParam {
public:
  ObCreateSSTableParamWithPartition() : ObCreateSSTableParam(), schema_(NULL), is_inited_(false)
  {}
  ~ObCreateSSTableParamWithPartition() = default;
  int extract_from(const ObCreateSSTableParamWithTable& param, ObCreatePartitionMeta& schema);
  void set_is_inited(bool is_init)
  {
    is_inited_ = is_init;
  }
  bool is_valid() const
  {
    return is_inited_ && ObCreateSSTableParam::is_valid() && NULL != schema_ && schema_->is_valid();
  }
  TO_STRING_KV(K_(is_inited), K_(table_key), KP(schema_), K_(schema_version), K_(progressive_merge_start_version),
      K_(progressive_merge_end_version), K_(create_snapshot_version), K_(checksum_method), K_(progressive_merge_round),
      K_(progressive_merge_step), K_(logical_data_version));

public:
  const ObCreatePartitionMeta* schema_;

private:
  bool is_inited_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_SSTABLE_STRUCT_H_
