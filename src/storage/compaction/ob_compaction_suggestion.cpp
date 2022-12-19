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

#include "ob_compaction_suggestion.h"
#include "ob_partition_merge_progress.h"
#include "ob_compaction_diagnose.h"
#include "ob_tablet_merge_ctx.h"
#include "storage/ob_sstable_struct.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace compaction
{

int ObCompactionSuggestionMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(array_.init(SUGGESTION_MAX_CNT))) {
    STORAGE_LOG(WARN, "failed to init ObInfoRingArray", K(ret));
  }
  return ret;
}

int ObCompactionSuggestionMgr::analyze_merge_info(
    ObTabletMergeInfo &tablet_merge_info,
    ObPartitionMergeProgress &progress)
{
  int ret = OB_SUCCESS;
  ObCompactionSuggestion suggestion;
  char * buf = suggestion.suggestion_;
  int64_t *scan_row_array = nullptr;
  const int64_t buf_len = OB_DIAGNOSE_INFO_LENGTH;
  ObSSTableMergeInfo merge_info = tablet_merge_info.get_sstable_merge_info();
  if (1 != merge_info.concurrent_cnt_ && progress.is_inited()) { // parallel compaction
    ObParalleMergeInfo &paral_info = merge_info.parallel_merge_info_;
    if (paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].max_value_
        > paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].min_value_ * SCAN_AVERAGE_PARAM) {
      if (progress.get_concurrent_count() != merge_info.concurrent_cnt_
          || OB_ISNULL(scan_row_array = progress.get_scanned_row_cnt_arr())) {
        STORAGE_LOG(WARN, "parallel degress is invalid or progress scan row count array is null",
            K(ret), K(progress));
      } else if (scan_row_array[progress.get_concurrent_count() - 1]
          == paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].max_value_) {
        // last parallel task have large scan size
        ADD_COMPACTION_INFO_PARAM(buf, buf_len,
            "uneven_scan_size_max", paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].max_value_,
            "uneven_scan_size_min", paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].min_value_,
            "suggest", "last parallel task have large scan size");
      } else {
        ADD_COMPACTION_INFO_PARAM(buf, buf_len,
            "uneven_scan_size_max", paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].max_value_,
            "uneven_scan_size_min", paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].min_value_,
            "suggest", "uneven scan size");
      }
    }
  }
  if (is_major_merge_type(merge_info.merge_type_)
      && merge_info.macro_block_count_ >= MACRO_CNT_PARAM) {
    if (1 == merge_info.concurrent_cnt_) {
      ADD_COMPACTION_INFO_PARAM(buf, buf_len,
          "macro_block_count", merge_info.macro_block_count_,
          "multiplexed_macro_count", merge_info.multiplexed_macro_block_count_,
          "suggest", "cannot be divided into parallel tasks");
    } else if (merge_info.multiplexed_macro_block_count_ < merge_info.macro_block_count_ * MACRO_MULTIPLEXED_PARAM) {
      ADD_COMPACTION_INFO_PARAM(buf, buf_len,
          "macro_block_count", merge_info.macro_block_count_,
          "multiplexed_macro_count", merge_info.multiplexed_macro_block_count_,
          "suggest", "low macro multiplexed ratio");
    }
  }
  if (merge_info.merge_finish_time_ - merge_info.merge_start_time_ > MERGE_COST_TIME_PARAM) {
    if (merge_info.incremental_row_count_ >= INC_ROW_CNT_PARAM) {
      ADD_COMPACTION_INFO_PARAM(buf, buf_len,
              "inc_row_count", merge_info.incremental_row_count_,
              "suggest", "too many incremental row");
    } else if (merge_info.macro_block_count_ >= SINGLE_PARTITION_MACRO_CNT_PARAM) {
      ADD_COMPACTION_INFO_PARAM(buf, buf_len,
              "macro_count", merge_info.macro_block_count_,
              "suggest", "large single partition");
    }
  }
  if (strlen(buf) > 0) {
    suggestion.merge_type_ = merge_info.merge_type_;
    suggestion.tenant_id_ = merge_info.tenant_id_;
    suggestion.ls_id_ = merge_info.ls_id_.id();
    suggestion.tablet_id_ = merge_info.tablet_id_.id();
    suggestion.merge_start_time_ = merge_info.merge_start_time_;
    suggestion.merge_finish_time_ = merge_info.merge_finish_time_;
    if (OB_FAIL(array_.add(suggestion))) {
      STORAGE_LOG(WARN, "failed to add suggestion", K(ret), K(suggestion));
    }
  }
  return ret;
}

int ObCompactionSuggestionIterator::open(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObCompactionSuggestionIterator has been opened", K(ret));
  } else if (!::is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    cur_idx_ = 0;
    is_opened_ = true;
  }
  return ret;
}

int ObCompactionSuggestionIterator::get_next_info(ObCompactionSuggestion &info)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ObCompactionSuggestionMgr::get_instance().get(cur_idx_++, info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_ITER_END;
        } else {
          STORAGE_LOG(WARN, "failed to get suggestion", K(ret));
        }
      } else if (OB_SYS_TENANT_ID == tenant_id_
          || tenant_id_ == info.tenant_id_) {
        break;
      }
    }
  }
  return ret;
}

}//compaction
}//oceanbase
