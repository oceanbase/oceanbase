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
#include "compaction/ob_compaction_diagnose.h"
#include "share/ob_force_print_log.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::compaction;

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

/*
 * PartTableInfo func
 * */
void PartTableInfo::fill_info(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (is_major_merge_) {
    compaction::ADD_COMPACTION_INFO_PARAM(buf, buf_len,
        "table_cnt", table_cnt_,
        "[MAJOR]snapshot_version", snapshot_version_);
    if (table_cnt_ > 1) {
      compaction::ADD_COMPACTION_INFO_PARAM(buf, buf_len,
          "[MINI]start_scn", start_scn_,
          "end_scn", end_scn_);
    }
  } else {
    if (table_cnt_ > 0) {
      compaction::ADD_COMPACTION_INFO_PARAM(buf, buf_len,
          "table_cnt", table_cnt_,
          "start_scn", start_scn_,
          "end_scn", end_scn_);
    }
  }
}

/*
 * ObSSTableMergeInfo func
 * */
ObSSTableMergeInfo::ObSSTableMergeInfo()
    : compaction::ObIDiagnoseInfo(),
      ls_id_(),
      tablet_id_(),
      is_fake_(false),
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
      start_cg_idx_(0),
      end_cg_idx_(0),
      suspect_add_time_(0),
      early_create_time_(0),
      dag_ret_(OB_SUCCESS),
      retry_cnt_(0),
      task_id_(),
      error_location_(),
      parallel_merge_info_(),
      filter_statistics_(),
      participant_table_info_(),
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
  is_fake_ = false;
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
  start_cg_idx_ = 0;
  end_cg_idx_ = 0;
  suspect_add_time_ = 0;
  early_create_time_ = 0;
  dag_ret_ = OB_SUCCESS;
  retry_cnt_ = 0;
  task_id_.reset();
  error_location_.reset();
  info_param_ = nullptr; // allow nullptr
  parallel_merge_info_.reset();
  filter_statistics_.reset();
  participant_table_info_.reset();
  MEMSET(macro_id_list_, '\0', sizeof(macro_id_list_));
  MEMSET(comment_, '\0', sizeof(comment_));
  kept_snapshot_info_.reset();
  merge_level_ = MERGE_LEVEL_MAX;
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

int ObSSTableMergeInfo::fill_comment(char *buf, const int64_t buf_len, const char* other_info) const
{
  int ret = OB_SUCCESS;
  compaction::ADD_COMPACTION_INFO_PARAM(buf, buf_len,
        "comment", comment_);
  if (0 != suspect_add_time_) {
    compaction::ObIDiagnoseInfoMgr::add_compaction_info_param(buf, buf_len, "[suspect info=");
    compaction::ObIDiagnoseInfoMgr::add_compaction_info_param(buf, buf_len, other_info);
    compaction::ADD_COMPACTION_INFO_PARAM(buf, buf_len,
        "add_time", suspect_add_time_);
    compaction::ObIDiagnoseInfoMgr::add_compaction_info_param(buf, buf_len, "]"); // finish add suspect info
  }
  if (0 != dag_ret_) {
    compaction::ADD_COMPACTION_INFO_PARAM(buf, buf_len,
        "[dag warning info=latest_error_code", dag_ret_,
        "early_create_time", early_create_time_,
        "latest_error_trace", task_id_,
        "retry_cnt", retry_cnt_,
        "location", error_location_);
    compaction::ObIDiagnoseInfoMgr::add_compaction_info_param(buf, buf_len, "]"); // finish add dag warning info
  }
  return ret;
}

void ObSSTableMergeInfo::update_start_time()
{
  int64_t current_time = ObTimeUtility::fast_current_time();
  (void)ATOMIC_CAS(&merge_start_time_, 0, current_time);
}

void ObSSTableMergeInfo::shallow_copy(ObIDiagnoseInfo *other)
{
  ObSSTableMergeInfo *info = nullptr;
  if (OB_NOT_NULL(other) && OB_NOT_NULL(info = dynamic_cast<ObSSTableMergeInfo *>(other))) {
    tenant_id_ = info->tenant_id_;
    priority_ = info->priority_;
    ls_id_ = info->ls_id_;
    tablet_id_ = info->tablet_id_;
    is_fake_ = info->is_fake_;
    compaction_scn_ = info->compaction_scn_;
    merge_type_ = info->merge_type_;
    merge_start_time_ = info->merge_start_time_;
    merge_finish_time_ = info->merge_finish_time_;
    dag_id_ = info->dag_id_;
    occupy_size_ = info->occupy_size_;
    new_flush_occupy_size_ = info->new_flush_occupy_size_;
    original_size_ = info->original_size_;
    compressed_size_ = info->compressed_size_;
    macro_block_count_ = info->macro_block_count_;
    multiplexed_macro_block_count_ = info->multiplexed_macro_block_count_;
    new_micro_count_in_new_macro_ = info->new_micro_count_in_new_macro_;
    multiplexed_micro_count_in_new_macro_ = info->multiplexed_micro_count_in_new_macro_;
    total_row_count_ = info->total_row_count_;
    incremental_row_count_ = info->incremental_row_count_;
    new_flush_data_rate_ = info->new_flush_data_rate_;
    is_full_merge_ = info->is_full_merge_;
    progressive_merge_round_ = info->progressive_merge_round_;
    progressive_merge_num_ = info->progressive_merge_num_;
    concurrent_cnt_ = info->concurrent_cnt_;
    macro_bloomfilter_count_ = info->macro_bloomfilter_count_;
    start_cg_idx_ = info->start_cg_idx_;
    end_cg_idx_ = info->end_cg_idx_;
    dag_ret_ = info->dag_ret_;
    task_id_ = info->task_id_;
    retry_cnt_ = info->retry_cnt_;
    suspect_add_time_ = info->suspect_add_time_;
    parallel_merge_info_ = info->parallel_merge_info_;
    filter_statistics_ = info->filter_statistics_;
    participant_table_info_ = info->participant_table_info_;
    MEMSET(macro_id_list_, '\0', sizeof(macro_id_list_));
    strncpy(macro_id_list_, info->macro_id_list_, strlen(info->macro_id_list_));
    MEMSET(comment_, '\0', sizeof(comment_));
    strncpy(comment_, info->comment_, strlen(info->comment_));
    kept_snapshot_info_ = info->kept_snapshot_info_;
    merge_level_ = info->merge_level_;
  }
}
