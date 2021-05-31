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

#define USING_LOG_PREFIX STORAGE

#include "ob_sstable_struct.h"
#include "share/ob_force_print_log.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;

ObMultiVersionSSTableMergeInfo::ObMultiVersionSSTableMergeInfo()
    : delete_logic_row_count_(0),
      update_logic_row_count_(0),
      insert_logic_row_count_(0),
      empty_delete_logic_row_count_(0)
{}

void ObMultiVersionSSTableMergeInfo::reset()
{
  delete_logic_row_count_ = 0;
  update_logic_row_count_ = 0;
  insert_logic_row_count_ = 0;
  empty_delete_logic_row_count_ = 0;
}

int ObMultiVersionSSTableMergeInfo::add(const ObMultiVersionSSTableMergeInfo& info)
{
  int ret = OB_SUCCESS;
  delete_logic_row_count_ += info.delete_logic_row_count_;
  update_logic_row_count_ += info.update_logic_row_count_;
  insert_logic_row_count_ += info.insert_logic_row_count_;
  empty_delete_logic_row_count_ += info.empty_delete_logic_row_count_;
  return ret;
}

ObSSTableMergeInfo::ObSSTableMergeInfo()
    : table_id_(0),
      partition_id_(0),
      version_(0),
      table_type_(0),
      major_table_id_(0),
      merge_start_time_(0),
      merge_finish_time_(0),
      merge_cost_time_(0),
      estimate_cost_time_(0),
      occupy_size_(0),
      macro_block_count_(0),
      use_old_macro_block_count_(0),
      total_row_count_(0),
      delete_row_count_(0),
      insert_row_count_(0),
      update_row_count_(0),
      base_row_count_(0),
      use_base_row_count_(0),
      memtable_row_count_(0),
      output_row_count_(0),
      purged_row_count_(0),
      merge_level_(MACRO_BLOCK_MERGE_LEVEL),
      rewrite_macro_old_micro_block_count_(0),
      rewrite_macro_total_micro_block_count_(0),
      error_code_(0),
      total_child_task_(0),
      finish_child_task_(0),
      is_complement_(false),
      merge_type_(INVALID_MERGE_TYPE),
      merge_status_(MERGE_STATUS_MAX),
      step_merge_start_version_(0),
      step_merge_end_version_(0),
      snapshot_version_(0),
      column_cnt_(0),
      table_count_(0),
      macro_bloomfilter_count_(0)
{}

int ObSSTableMergeInfo::add(const ObSSTableMergeInfo& other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  table_type_ = other.table_type_;
  major_table_id_ = other.major_table_id_;
  if (0 == merge_start_time_) {
    merge_start_time_ = other.merge_start_time_;
  } else if (merge_start_time_ > other.merge_start_time_) {
    merge_start_time_ = other.merge_start_time_;
  }
  if (merge_finish_time_ < other.merge_finish_time_) {
    merge_finish_time_ = other.merge_finish_time_;
  }
  merge_cost_time_ = merge_finish_time_ - merge_start_time_;
  estimate_cost_time_ += other.estimate_cost_time_;
  occupy_size_ += other.occupy_size_;
  macro_block_count_ += other.macro_block_count_;
  use_old_macro_block_count_ += other.use_old_macro_block_count_;
  total_row_count_ += other.total_row_count_;
  delete_row_count_ += other.delete_row_count_;
  insert_row_count_ += other.insert_row_count_;
  update_row_count_ += other.update_row_count_;
  base_row_count_ += other.base_row_count_;
  use_base_row_count_ += other.use_base_row_count_;
  memtable_row_count_ += other.memtable_row_count_;
  output_row_count_ += other.output_row_count_;
  purged_row_count_ += other.purged_row_count_;
  merge_level_ = other.merge_level_;
  rewrite_macro_old_micro_block_count_ += other.rewrite_macro_old_micro_block_count_;
  rewrite_macro_total_micro_block_count_ += other.rewrite_macro_total_micro_block_count_;
  total_child_task_ = other.total_child_task_;
  finish_child_task_ = other.finish_child_task_;
  partition_id_ = other.partition_id_;
  error_code_ = other.error_code_;
  merge_type_ = other.merge_type_;
  merge_status_ = other.merge_status_;

  if (other.version_.is_valid()) {
    version_ = other.version_.version_;
  }
  step_merge_start_version_ = other.step_merge_start_version_;
  step_merge_end_version_ = other.step_merge_end_version_;
  snapshot_version_ = other.snapshot_version_;
  table_count_ = other.table_count_;
  macro_bloomfilter_count_ += other.macro_bloomfilter_count_;
  return ret;
}

void ObSSTableMergeInfo::reset()
{
  table_id_ = 0;
  table_type_ = 0;
  major_table_id_ = 0;
  merge_start_time_ = 0;
  merge_finish_time_ = 0;
  merge_cost_time_ = 0;
  estimate_cost_time_ = 0;
  occupy_size_ = 0;
  macro_block_count_ = 0;
  use_old_macro_block_count_ = 0;
  total_row_count_ = 0;
  delete_row_count_ = 0;
  insert_row_count_ = 0;
  update_row_count_ = 0;
  base_row_count_ = 0;
  use_base_row_count_ = 0;
  memtable_row_count_ = 0;
  output_row_count_ = 0;
  purged_row_count_ = 0;
  merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  rewrite_macro_old_micro_block_count_ = 0;
  rewrite_macro_total_micro_block_count_ = 0;
  total_child_task_ = 0;
  finish_child_task_ = 0;
  partition_id_ = 0;
  error_code_ = 0;
  is_complement_ = false;
  merge_type_ = INVALID_MERGE_TYPE;
  merge_status_ = MERGE_STATUS_MAX;
  version_ = 0;
  step_merge_start_version_ = 0;
  step_merge_end_version_ = 0;
  column_cnt_ = 0;
  table_count_ = 0;
  macro_bloomfilter_count_ = 0;
}

void ObSSTableMergeInfo::dump_info(const char* msg)
{
  int64_t output_row_per_s = 0;
  int64_t new_macro_KB_per_s = 0;
  if (merge_cost_time_ != 0) {
    output_row_per_s = (output_row_count_ * 1000 * 1000) / merge_cost_time_;
    new_macro_KB_per_s = (macro_block_count_ - use_old_macro_block_count_) * 2 * 1024 * 1000 * 1000 / merge_cost_time_;
  }
  FLOG_INFO("dump merge info", K(msg), K(output_row_per_s), K(new_macro_KB_per_s), K(*this));
}

ObMergeChecksumInfo::ObMergeChecksumInfo()
    : column_checksums_(NULL), increment_column_checksums_(NULL), concurrent_cnt_(0), column_count_(0)
{}

int ObCreateSSTableParam::assign(const ObCreateSSTableParam& other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", KR(ret), K(other));
  } else {
    table_key_ = other.table_key_;
    schema_version_ = other.schema_version_;
    progressive_merge_start_version_ = other.progressive_merge_start_version_;
    progressive_merge_end_version_ = other.progressive_merge_end_version_;
    create_snapshot_version_ = other.create_snapshot_version_;
    create_index_base_version_ = other.create_index_base_version_;
    checksum_method_ = other.checksum_method_;
    progressive_merge_round_ = other.progressive_merge_round_;
    progressive_merge_step_ = other.progressive_merge_step_;
    pg_key_ = other.pg_key_;
    logical_data_version_ = other.logical_data_version_;
    has_compact_row_ = other.has_compact_row_;
    dump_memtable_timestamp_ = other.dump_memtable_timestamp_;
  }
  return ret;
}

int ObCreateSSTableParamWithPartition::extract_from(
    const ObCreateSSTableParamWithTable& param, ObCreatePartitionMeta& schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", KR(ret), K(param));
  } else if (OB_FAIL(ObCreateSSTableParam::assign(param))) {
    STORAGE_LOG(WARN, "failed to assign", KR(ret), K(param));
  } else if (OB_FAIL(schema.extract_from(*param.schema_))) {
    STORAGE_LOG(WARN, "failed to extract schema", KR(ret), K(param));
  } else {
    schema_ = &schema;
    is_inited_ = true;
  }
  return ret;
}
