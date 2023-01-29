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
{
}

void ObMultiVersionSSTableMergeInfo::reset()
{
  delete_logic_row_count_ = 0;
  update_logic_row_count_ = 0;
  insert_logic_row_count_ = 0;
  empty_delete_logic_row_count_ = 0;
}

int ObMultiVersionSSTableMergeInfo::add(const ObMultiVersionSSTableMergeInfo &info)
{
  int ret = OB_SUCCESS;
  delete_logic_row_count_ += info.delete_logic_row_count_;
  update_logic_row_count_ += info.update_logic_row_count_;
  insert_logic_row_count_ += info.insert_logic_row_count_;
  empty_delete_logic_row_count_ += info.empty_delete_logic_row_count_;
  return ret;
}

const char * ObParalleMergeInfo::para_info_type_str[] = {
    "scan_units",
    "cost_time",
    "use_old_macro_block_cnt",
    "incremental_row_count"
};

const char *ObParalleMergeInfo::get_para_info_str(const int64_t idx) const
{
  const char * ret_str = nullptr;
  STATIC_ASSERT(static_cast<int64_t>(ARRAY_IDX_MAX) == ARRAYSIZEOF(para_info_type_str), "str len is mismatch");
  if (idx < SCAN_UNITS || idx >= ARRAY_IDX_MAX) {
    ret_str = "invalid_type";
  } else {
    ret_str = para_info_type_str[idx];
  }
  return ret_str;
}

int64_t ObParalleMergeInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    for (int i = 0; i < ARRAY_IDX_MAX; ++i) {
      J_OBJ_START();
      J_KV("type", get_para_info_str(i), "info", info_[i]);
      J_OBJ_END();
      J_COMMA();
    }
    J_OBJ_END();
  }
  return pos;
}

int64_t ObParalleMergeInfo::to_paral_info_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    for (int i = 0; i < ARRAY_IDX_MAX; ++i) {
      J_OBJ_START();
      if (0 == info_[i].count_ && 0 == info_[i].sum_value_) {
        J_KV("type", get_para_info_str(i), "info", "EMPTY");
      } else {
        J_KV("type", get_para_info_str(i), "min", info_[i].min_value_, "max", info_[i].max_value_,
            "avg", info_[i].count_ > 0 ? info_[i].sum_value_ / info_[i].count_ : 0);
      }
      J_OBJ_END();
      J_COMMA();
    }
    J_OBJ_END();
  }
  return pos;
}

void ObParalleMergeInfo::reset()
{
  for (int i = 0; i < ARRAY_IDX_MAX; ++i) {
    info_[i].reset();
  }
}

ObSSTableMergeInfo::ObSSTableMergeInfo()
    : tenant_id_(0),
      ls_id_(),
      tablet_id_(),
      compaction_scn_(0),
      merge_type_(INVALID_MERGE_TYPE),
      merge_start_time_(0),
      merge_finish_time_(0),
      dag_id_(),
      occupy_size_(0),
      new_flush_occupy_size_(0),
      original_size_(0),
      compressed_size_(0),
      macro_block_count_(0),
      multiplexed_macro_block_count_(0),
      new_micro_count_in_new_macro_(0),
      multiplexed_micro_count_in_new_macro_(0),
      total_row_count_(0),
      incremental_row_count_(0),
      new_flush_data_rate_(0),
      is_full_merge_(false),
      progressive_merge_round_(0),
      progressive_merge_num_(0),
      concurrent_cnt_(0),
      macro_bloomfilter_count_(0),
      parallel_merge_info_(),
      filter_statistics_(),
      participant_table_str_("\0"),
      macro_id_list_("\0"),
      comment_("\0")
{
}

bool ObSSTableMergeInfo::is_valid() const
{
  bool bret = true;
  if (OB_UNLIKELY(tenant_id_ <= 0 || !ls_id_.is_valid() || !tablet_id_.is_valid() || compaction_scn_ <= 0)) {
    bret = false;
  }
  return bret;
}

int ObSSTableMergeInfo::add(const ObSSTableMergeInfo &other)
{
  int ret = OB_SUCCESS;
  if (merge_finish_time_ < other.merge_finish_time_) {
    merge_finish_time_ = other.merge_finish_time_;
  }
  new_flush_occupy_size_ += other.new_flush_occupy_size_;
  occupy_size_ += other.occupy_size_;
  original_size_ += other.original_size_;
  compressed_size_ += other.compressed_size_;
  macro_block_count_ += other.macro_block_count_;
  multiplexed_macro_block_count_ += other.multiplexed_macro_block_count_;
  total_row_count_ += other.total_row_count_;
  incremental_row_count_ += other.incremental_row_count_;
  multiplexed_micro_count_in_new_macro_ += other.multiplexed_micro_count_in_new_macro_;
  new_micro_count_in_new_macro_ += other.new_micro_count_in_new_macro_;

  if (1 == concurrent_cnt_) {
    // do nothing
  } else {
    parallel_merge_info_.info_[ObParalleMergeInfo::MERGE_COST_TIME].add(other.merge_finish_time_ - other.merge_start_time_);
    parallel_merge_info_.info_[ObParalleMergeInfo::USE_OLD_BLK_CNT].add(other.multiplexed_macro_block_count_);
    parallel_merge_info_.info_[ObParalleMergeInfo::INC_ROW_CNT].add(other.incremental_row_count_);
  }
  macro_bloomfilter_count_ += other.macro_bloomfilter_count_;
  filter_statistics_.add(other.filter_statistics_);
  return ret;
}

void ObSSTableMergeInfo::reset()
{
  tenant_id_ = 0;
  ls_id_.reset();
  tablet_id_.reset();
  compaction_scn_ = 0;
  merge_type_ = INVALID_MERGE_TYPE;
  merge_start_time_ = 0;
  merge_finish_time_ = 0;
  occupy_size_ = 0;
  new_flush_occupy_size_ = 0;
  original_size_ = 0;
  compressed_size_ = 0;
  macro_block_count_ = 0;
  multiplexed_macro_block_count_ = 0;
  new_micro_count_in_new_macro_ = 0;
  multiplexed_micro_count_in_new_macro_ = 0;
  total_row_count_ = 0;
  incremental_row_count_ = 0;
  new_flush_data_rate_ = 0;
  is_full_merge_ = false;
  progressive_merge_round_ = 0;
  progressive_merge_num_ = 0;
  concurrent_cnt_ = 0;
  macro_bloomfilter_count_ = 0;
  parallel_merge_info_.reset();
  filter_statistics_.reset();
  MEMSET(participant_table_str_, '\0', sizeof(participant_table_str_));
  MEMSET(macro_id_list_, '\0', sizeof(macro_id_list_));
  MEMSET(comment_, '\0', sizeof(comment_));
}

void ObSSTableMergeInfo::dump_info(const char *msg)
{
  int64_t output_row_per_s = 0;
  int64_t new_macro_KB_per_s = 0;
  if (merge_finish_time_ > merge_start_time_) {
    const int64_t merge_cost_time = merge_finish_time_ - merge_start_time_;
    output_row_per_s = (incremental_row_count_ * 1000 * 1000) / merge_cost_time;
    new_macro_KB_per_s = (macro_block_count_ - multiplexed_macro_block_count_) * 2 * 1024 * 1000 * 1000 / merge_cost_time;
  }
  FLOG_INFO("dump merge info", K(msg), K(output_row_per_s), K(new_macro_KB_per_s), K(*this));
}

