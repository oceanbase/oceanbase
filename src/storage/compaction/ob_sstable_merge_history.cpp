//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_sstable_merge_history.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
namespace compaction
{
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
      if (!info_[i].is_empty()) {
        J_OBJ_START();
        J_KV("type", get_para_info_str(i), "info", info_[i]);
        J_OBJ_END();
        J_COMMA();
      }
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
      if (0 != info_[i].count_ || 0 != info_[i].sum_value_) {
        J_OBJ_START();
        J_KV("type", get_para_info_str(i), "min", info_[i].min_value_, "max", info_[i].max_value_,
            "avg", info_[i].count_ > 0 ? info_[i].sum_value_ / info_[i].count_ : 0);
        J_OBJ_END();
        J_COMMA();
      }
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
      if (inc_major_cnt_ > 0) {
        compaction::ADD_COMPACTION_INFO_PARAM(buf, buf_len,
            "inc_major_cnt", inc_major_cnt_, "[INC_MAJOR]start_scn", inc_major_start_scn_, "end_scn", inc_major_end_scn_);
      }
      if (table_cnt_ - inc_major_cnt_ > 1) {
        compaction::ADD_COMPACTION_INFO_PARAM(buf, buf_len,
            "[MINI]start_scn", start_scn_,
            "end_scn", end_scn_);
      }
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

/**
 * -------------------------------------------------------------------ObMergeStaticInfo-------------------------------------------------------------------
 */
ObMergeStaticInfo::ObMergeStaticInfo()
  : ls_id_(),
    tablet_id_(),
    merge_type_(INVALID_MERGE_TYPE),
    compaction_scn_(0),
    concurrent_cnt_(0),
    progressive_merge_round_(0),
    progressive_merge_num_(0),
    kept_snapshot_info_(),
    participant_table_info_(),
    mds_filter_info_str_("\0"),
    merge_level_(MERGE_LEVEL_MAX),
    exec_mode_(ObExecMode::EXEC_MODE_MAX),
    merge_reason_(ObAdaptiveMergePolicy::NONE),
    base_major_status_(ObCOMajorSSTableStatus::INVALID_CO_MAJOR_SSTABLE_STATUS),
    co_major_merge_type_(ObCOMajorMergePolicy::INVALID_CO_MAJOR_MERGE_TYPE),
    is_full_merge_(false)
{}

bool ObMergeStaticInfo::is_valid() const
{
  return ls_id_.is_valid() &&
         ((tablet_id_.is_valid() && is_valid_merge_type(merge_type_)) ||
          BATCH_EXEC == merge_type_) &&
         compaction_scn_ > 0 && is_valid_exec_mode(exec_mode_);
}

void ObMergeStaticInfo::reset()
{
  ls_id_.reset();
  tablet_id_.reset();
  merge_type_ = INVALID_MERGE_TYPE;
  compaction_scn_ = 0;
  concurrent_cnt_ = 0;
  progressive_merge_round_ = 0;
  progressive_merge_num_ = 0;
  kept_snapshot_info_.reset();
  participant_table_info_.reset();
  merge_level_ = MERGE_LEVEL_MAX;
  exec_mode_ = ObExecMode::EXEC_MODE_MAX;
  merge_reason_ = ObAdaptiveMergePolicy::NONE;
  base_major_status_ = ObCOMajorSSTableStatus::INVALID_CO_MAJOR_SSTABLE_STATUS;
  co_major_merge_type_ = ObCOMajorMergePolicy::INVALID_CO_MAJOR_MERGE_TYPE;
  is_full_merge_ = false;
  MEMSET(mds_filter_info_str_, '\0', sizeof(mds_filter_info_str_));
}

void ObMergeStaticInfo::shallow_copy(const ObMergeStaticInfo &other)
{
  ls_id_ = other.ls_id_;
  tablet_id_ = other.tablet_id_;
  merge_type_ = other.merge_type_;
  compaction_scn_ = other.compaction_scn_;
  concurrent_cnt_ = other.concurrent_cnt_;
  progressive_merge_round_ = other.progressive_merge_round_;
  progressive_merge_num_ = other.progressive_merge_num_;
  merge_sstable_count_ = other.merge_sstable_count_;
  kept_snapshot_info_ = other.kept_snapshot_info_;
  participant_table_info_ = other.participant_table_info_;
  merge_level_ = other.merge_level_;
  exec_mode_ = other.exec_mode_;
  merge_reason_ = other.merge_reason_;
  base_major_status_ = other.base_major_status_;
  co_major_merge_type_ = other.co_major_merge_type_;
  is_full_merge_ = other.is_full_merge_;
  MEMSET(mds_filter_info_str_, '\0', sizeof(mds_filter_info_str_));
  strncpy(mds_filter_info_str_, other.mds_filter_info_str_, strlen(other.mds_filter_info_str_));
}

int64_t ObMergeStaticInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(K_(ls_id), K_(tablet_id), "merge_type", merge_type_to_str(merge_type_),
      K_(compaction_scn), K_(is_full_merge), K_(concurrent_cnt), K_(merge_sstable_count),
      "merge_level", merge_level_to_str(merge_level_),
      "exec_mode", exec_mode_to_str(exec_mode_));
    if (is_major_merge_type(merge_type_)) {
      J_COMMA();
      J_KV("merge_reason", ObAdaptiveMergePolicy::merge_reason_to_str(merge_reason_),
        "progressive_merge_round", progressive_merge_round_, "progressive_merge_num", progressive_merge_num_);
      if (ObCOMajorMergePolicy::is_valid_major_merge_type(co_major_merge_type_)) {
        J_COMMA();
        J_KV("base_major_status", co_major_sstable_status_to_str(base_major_status_),
        "co_major_merge_type", ObCOMajorMergePolicy::co_major_merge_type_to_str(co_major_merge_type_));
      }
    }
    J_COMMA();
    J_KV(K_(kept_snapshot_info), K_(participant_table_info), K_(mds_filter_info_str));
    J_OBJ_END();
  }
  return pos;
}

/**
 * -------------------------------------------------------------------ObMergeRunningInfo-------------------------------------------------------------------
 */
ObMergeRunningInfo::ObMergeRunningInfo()
  : merge_start_time_(0),
    merge_finish_time_(0),
    execute_time_(0),
    start_cg_idx_(0),
    end_cg_idx_(0),
    io_percentage_(0),
    dag_id_(),
    parallel_merge_info_(),
    comment_("\0")
{}

void ObMergeRunningInfo::reset()
{
  merge_start_time_ = 0;
  merge_finish_time_ = 0;
  execute_time_ = 0;
  start_cg_idx_ = 0;
  end_cg_idx_ = 0;
  io_percentage_ = 0;
  dag_id_.reset();
  parallel_merge_info_.reset();
  MEMSET(comment_, '\0', sizeof(comment_));
}

bool ObMergeRunningInfo::is_valid() const
{
  return merge_start_time_ > 0 && merge_finish_time_ > 0 && dag_id_.is_valid();
}

void ObMergeRunningInfo::shallow_copy(const ObMergeRunningInfo &other)
{
  merge_start_time_ = other.merge_start_time_;
  merge_finish_time_ = other.merge_finish_time_;
  execute_time_ = other.execute_time_;
  start_cg_idx_ = other.start_cg_idx_;
  end_cg_idx_ = other.end_cg_idx_;
  io_percentage_ = other.io_percentage_;
  dag_id_ = other.dag_id_;
  parallel_merge_info_ = other.parallel_merge_info_;
  MEMSET(comment_, '\0', sizeof(comment_));
  strncpy(comment_, other.comment_, strlen(other.comment_));
}
/**
 * -------------------------------------------------------------------ObMergeBlockInfo-------------------------------------------------------------------
 */
ObMergeBlockInfo::ObMergeBlockInfo()
  : occupy_size_(0),
    original_size_(0),
    compressed_size_(0),
    macro_block_count_(0),
    multiplexed_macro_block_count_(0),
    new_micro_count_in_new_macro_(0),
    multiplexed_micro_count_in_new_macro_(0),
    total_row_count_(0),
    incremental_row_count_(0),
    new_flush_data_rate_(0),
    new_micro_info_(),
    block_io_us_(0),
    macro_id_list_("\0")
{}

void ObMergeBlockInfo::reset()
{
  occupy_size_ = 0;
  original_size_ = 0;
  compressed_size_ = 0;
  macro_block_count_ = 0;
  multiplexed_macro_block_count_ = 0;
  new_micro_count_in_new_macro_ = 0;
  multiplexed_micro_count_in_new_macro_ = 0;
  total_row_count_ = 0;
  incremental_row_count_ = 0;
  new_flush_data_rate_ = 0;
  new_micro_info_.reset();
  block_io_us_ = 0;
  MEMSET(macro_id_list_, '\0', sizeof(macro_id_list_));
}

bool ObMergeBlockInfo::is_valid() const {
  return (0 == macro_block_count_ && 0 == total_row_count_) ||
         (macro_block_count_ > 0 && total_row_count_ > 0);
}

void ObMergeBlockInfo::shallow_copy(const ObMergeBlockInfo &other)
{
  occupy_size_ = other.occupy_size_;
  original_size_ = other.original_size_;
  compressed_size_ = other.compressed_size_;
  macro_block_count_ = other.macro_block_count_;
  multiplexed_macro_block_count_ = other.multiplexed_macro_block_count_;
  new_micro_count_in_new_macro_ = other.new_micro_count_in_new_macro_;
  multiplexed_micro_count_in_new_macro_ = other.multiplexed_micro_count_in_new_macro_;
  total_row_count_ = other.total_row_count_;
  incremental_row_count_ = other.incremental_row_count_;
  new_flush_data_rate_ = other.new_flush_data_rate_;
  new_micro_info_ = other.new_micro_info_;
  block_io_us_ = other.block_io_us_;
  MEMSET(macro_id_list_, '\0', sizeof(macro_id_list_));
  strncpy(macro_id_list_, other.macro_id_list_, strlen(other.macro_id_list_));
}

void ObMergeBlockInfo::add(const ObMergeBlockInfo &other)
{
  total_row_count_ += other.total_row_count_;
  incremental_row_count_ += other.incremental_row_count_;
  add_without_row_cnt(other);
}

/*
* for column store, each batch should have same row cnt, need skip when add
*/
void ObMergeBlockInfo::add_without_row_cnt(const ObMergeBlockInfo &other)
{
  occupy_size_ += other.occupy_size_;
  original_size_ += other.original_size_;
  compressed_size_ += other.compressed_size_;
  macro_block_count_ += other.macro_block_count_;
  multiplexed_macro_block_count_ += other.multiplexed_macro_block_count_;
  multiplexed_micro_count_in_new_macro_ += other.multiplexed_micro_count_in_new_macro_;
  new_micro_count_in_new_macro_ += other.new_micro_count_in_new_macro_;
  block_io_us_ += other.block_io_us_;
  new_micro_info_.add(other.new_micro_info_);
}

void ObMergeBlockInfo::add_index_block_info(const ObMergeBlockInfo &block_info)
{
  new_micro_info_.add(block_info.new_micro_info_);
}
/**
 * -------------------------------------------------------------------ObMergeDiagnoseInfo-------------------------------------------------------------------
 */

ObMergeDiagnoseInfo::ObMergeDiagnoseInfo()
  : dag_ret_(0),
    retry_cnt_(0),
    suspect_add_time_(0),
    early_create_time_(0),
    error_trace_(),
    error_location_()
{}

void ObMergeDiagnoseInfo::reset()
{
  dag_ret_ = 0;
  retry_cnt_ = 0;
  suspect_add_time_ = 0;
  early_create_time_ = 0;
  error_trace_.reset();
  error_location_.reset();
}

void ObMergeDiagnoseInfo::shallow_copy(const ObMergeDiagnoseInfo &other)
{
  dag_ret_ = other.dag_ret_;
  retry_cnt_ = other.retry_cnt_;
  suspect_add_time_ = other.suspect_add_time_;
  early_create_time_ = other.early_create_time_;
  error_trace_ = other.error_trace_;
  error_location_ = other.error_location_;
}

/**
 * -------------------------------------------------------------------ObSSTableMergeHistory-------------------------------------------------------------------
 */
ObSSTableMergeHistory::ObSSTableMergeHistory(const bool need_free_param)
  : ObIDiagnoseInfo(need_free_param),
    static_info_(),
    running_info_(),
    block_info_(),
    diagnose_info_(),
    sstable_merge_block_info_array_()
{}

void ObSSTableMergeHistory::reset()
{
  static_info_.reset();
  running_info_.reset();
  block_info_.reset();
  diagnose_info_.reset();
}

bool ObSSTableMergeHistory::is_valid() const
{
  bool bret = true;
  if (OB_UNLIKELY(!static_info_.is_valid())) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "static info is invalid", K(bret), K_(static_info));
  } else if (OB_UNLIKELY(!running_info_.is_valid())) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "running info is invalid", K(bret), K_(running_info));
  } else if (OB_UNLIKELY(!block_info_.is_valid())) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "block info is invalid", K(bret), K_(block_info));
  }
  return bret;
}

void ObSSTableMergeHistory::shallow_copy(ObIDiagnoseInfo *other)
{
  ObSSTableMergeHistory *info = nullptr;
  if (OB_NOT_NULL(other) && OB_NOT_NULL(info = static_cast<ObSSTableMergeHistory *>(other))) {
    static_info_.shallow_copy(info->static_info_);
    running_info_.shallow_copy(info->running_info_);
    block_info_.shallow_copy(info->block_info_);
    diagnose_info_.shallow_copy(info->diagnose_info_);
    sstable_merge_block_info_array_.assign(info->sstable_merge_block_info_array_);
  }
}

void ObSSTableMergeHistory::update_block_info(
  const ObMergeBlockInfo &block_info,
  const bool without_row_cnt)
{
  lib::ObMutexGuard guard(lock_);
  if (without_row_cnt) {
    block_info_.add_without_row_cnt(block_info);
  } else {
    block_info_.add(block_info);
  }
  running_info_.merge_finish_time_ = ObTimeUtility::fast_current_time();
}

void ObSSTableMergeHistory::update_block_info_with_sstable_block_info(
  const ObMergeBlockInfo &block_info,
  const bool without_row_cnt,
  ObIArray<ObSSTableMergeBlockInfo> &array)
{
  update_block_info(block_info, without_row_cnt);
  if (array.count() != sstable_merge_block_info_array_.count()) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "sstable merge block info array count is not equal", K(array.count()), K(sstable_merge_block_info_array_.count()));
  } else {
    lib::ObMutexGuard guard(lock_);
    for (int64_t i = 0; i < array.count(); ++i) {
      sstable_merge_block_info_array_.at(i).add(array.at(i));
    }
  }
}

int ObSSTableMergeHistory::fill_comment(char *buf, const int64_t buf_len, const char* other_info) const
{
  int ret = OB_SUCCESS;
  if (strlen(running_info_.comment_) > 0) {
    compaction::ObIDiagnoseInfoMgr::add_compaction_info_param(buf, buf_len, running_info_.comment_);
  }
  if (0 != diagnose_info_.suspect_add_time_) {
    compaction::ObIDiagnoseInfoMgr::add_compaction_info_param(buf, buf_len, "[suspect info=");
    compaction::ObIDiagnoseInfoMgr::add_compaction_info_param(buf, buf_len, other_info);
    compaction::ADD_COMPACTION_INFO_PARAM(buf, buf_len, "add_time", diagnose_info_.suspect_add_time_);
    compaction::ObIDiagnoseInfoMgr::add_compaction_info_param(buf, buf_len, "]"); // finish add suspect info
  }
  if (0 != diagnose_info_.dag_ret_) {
    compaction::ADD_COMPACTION_INFO_PARAM(buf, buf_len,
        "[dag warning info=latest_error_code", diagnose_info_.dag_ret_,
        "early_create_time", diagnose_info_.early_create_time_,
        "latest_error_trace", diagnose_info_.error_trace_,
        "retry_cnt", diagnose_info_.retry_cnt_,
        "location", diagnose_info_.error_location_);
    compaction::ObIDiagnoseInfoMgr::add_compaction_info_param(buf, buf_len, "]"); // finish add dag warning info
  }
  if (sstable_merge_block_info_array_.count() > 1) {
    for (int64_t i = 0; i < sstable_merge_block_info_array_.count(); ++i) {
      if (!sstable_merge_block_info_array_.at(i).is_empty()) {
        compaction::ADD_COMPACTION_INFO_PARAM(buf, buf_len, "i", i);
      }
      if (sstable_merge_block_info_array_.at(i).multiplexed_macro_block_count_ > 0) {
        compaction::ADD_COMPACTION_INFO_PARAM(buf, buf_len, "macro", sstable_merge_block_info_array_.at(i).multiplexed_macro_block_count_);
      }
      if (sstable_merge_block_info_array_.at(i).multiplexed_micro_count_in_new_macro_ > 0) {
        compaction::ADD_COMPACTION_INFO_PARAM(buf, buf_len, "micro", sstable_merge_block_info_array_.at(i).multiplexed_micro_count_in_new_macro_);
      }
    }
  }
  return ret;
}

int64_t ObSSTableMergeHistory::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(K_(static_info), K_(running_info));
    if (!block_info_.is_empty()) {
      J_COMMA();
      J_KV("block_info", block_info_);
    }
    if (!diagnose_info_.is_empty()) {
      J_COMMA();
      J_KV("diagnose_info", diagnose_info_);
    }
    if (!sstable_merge_block_info_array_.empty()) {
      J_COMMA();
      J_KV("sstable_merge_block_info_array", sstable_merge_block_info_array_);
    }
    J_OBJ_END();
  }
  return pos;
}

int ObSSTableMergeHistory::init_sstable_merge_block_info_array(
    const int64_t count,
    ObIArray<ObSSTableMergeBlockInfo> &sstable_merge_block_info_array)
{
  int ret = OB_SUCCESS;
  if (count <= 1) {
    // do nothing
  } else if (OB_FAIL(sstable_merge_block_info_array.prepare_allocate(count))) {
    LOG_WARN("failed to prepare allocate sstable_merge_block_info_array", K(ret), K(count));
  } else {
    for (int64_t i = 0; i < count; ++i) {
      sstable_merge_block_info_array.at(i).reset();
    }
  }
  return ret;
}
} // namespace compaction
} // namespace oceanbase
