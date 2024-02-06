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
/*
 * ObDagSuggestionEvent implement
 * */
int ObDagSuggestionEvent::analyze_insufficient_thread(
    const int64_t priority,
    const share::ObDagType::ObDagTypeEnum dag_type,
    char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t worker_thread = 0;
  if (TOLERATE_INSUFFICIENT_THREADS_COUNT == insufficient_threads_) { // only once
    if (OB_FAIL(MTL(ObTenantDagScheduler*)->get_limit(priority, worker_thread))) {
      STORAGE_LOG(WARN, "fail to get priority thread limit", K(ret), K(priority));
    } else {
      ADD_COMPACTION_INFO_PARAM(buf, buf_len,
            "dag_type", ObIDag::get_dag_type_str(dag_type),
            "dag_priority", priority,
            "thread_limit", worker_thread,
            "suggest", ObCompactionSuggestionMgr::get_add_thread_suggestion(priority));
    }
  }
  return ret;
}

/*
 * ObCompactionSuggestionMgr implement
 * */
const char *ObCompactionSuggestionMgr::ObAddWorkerThreadSuggestion[share::ObDagPrio::DAG_PRIO_MAX] =
{
  "increase compaction_high_thread_score",
  "",
  "increase compaction_mid_thread_score",
  "",
  "increase compaction_low_thread_score",
  "",
  "",
  ""
};

const char* ObCompactionSuggestionMgr::get_add_thread_suggestion(const int64_t priority)
{
  const char *str = "";
  if (priority >= share::ObDagPrio::DAG_PRIO_MAX || priority < share::ObDagPrio::DAG_PRIO_COMPACTION_HIGH) {
    str = "invalid_priority";
  } else {
    str = ObAddWorkerThreadSuggestion[priority];
  }
  return str;
}

int ObCompactionSuggestionMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(array_.init(SUGGESTION_MAX_CNT))) {
    STORAGE_LOG(WARN, "failed to init ObInfoRingArray", K(ret));
  } else {
    MEMSET(dag_event_, 0, sizeof(dag_event_));
  }
  return ret;
}

int ObCompactionSuggestionMgr::analyze_insufficient_thread(
    const uint64_t tenant_id,
    const share::ObDagType::ObDagTypeEnum dag_type,
    const int64_t priority,
    const int64_t thread_limit,
    const int64_t added_dag_cnts,
    const int64_t scheduled_dag_cnts)
{
  int ret = OB_SUCCESS;
  ObCompactionSuggestion suggestion;
  char * buf = suggestion.suggestion_;
  const int64_t buf_len = OB_DIAGNOSE_INFO_LENGTH;
  float schedule_ratio = added_dag_cnts == 0 ? 1 : (float)scheduled_dag_cnts / (float)added_dag_cnts;
  if (ObDagSuggestionEvent::TOLERATE_SCHEDULE_DAG_RATIO >= schedule_ratio) {
    ADD_COMPACTION_INFO_PARAM(buf, buf_len,
          "dag_type", ObIDag::get_dag_type_str(dag_type),
          "dag_priority", priority,
          "thread_limit", thread_limit,
          "suggest", ObCompactionSuggestionMgr::get_add_thread_suggestion(priority));
  }
  if (strlen(buf) > 0) {
    suggestion.merge_type_ =  INVALID_MERGE_TYPE;
    suggestion.tenant_id_ = tenant_id;
    suggestion.ls_id_ = UNKNOW_LS_ID.id();
    suggestion.tablet_id_ = UNKNOW_TABLET_ID.id();
    suggestion.merge_start_time_ = common::ObTimeUtility::fast_current_time();
    suggestion.merge_finish_time_ = common::ObTimeUtility::fast_current_time();
    if (OB_FAIL(array_.add(suggestion))) {
      STORAGE_LOG(WARN, "failed to add suggestion", K(ret), K(suggestion));
    }
  }
  return ret;
}

int ObCompactionSuggestionMgr::analyze_schedule_status(
    ObTabletMergeInfo &tablet_merge_info,
    const uint64_t tenant_id,
    const share::ObDagType::ObDagTypeEnum dag_type,
    const int64_t priority,
    const int64_t schedule_wait_time)
{
  int ret = OB_SUCCESS;
  if (ObDagType::ObDagTypeEnum::DAG_TYPE_MAX <= dag_type) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(dag_type));
  } else {
    ObCompactionSuggestion suggestion;
    char * buf = suggestion.suggestion_;
    const int64_t buf_len = OB_DIAGNOSE_INFO_LENGTH;
    ObSSTableMergeInfo merge_info = tablet_merge_info.get_sstable_merge_info();
    MTL_SWITCH(tenant_id) {
      if (SCHEDULE_WAIT_LONG_TIME < schedule_wait_time) {
        ++dag_event_[dag_type].insufficient_threads_;
      } else {
        dag_event_[dag_type].insufficient_threads_ = 0;
      }
      if (OB_FAIL(dag_event_[dag_type].analyze_insufficient_thread(priority, dag_type, buf, buf_len))) {
        STORAGE_LOG(WARN, "failed to analyze insufficient thread", K(ret));
      }
    }
    if (OB_SUCC(ret) && strlen(buf) > 0) {
      suggestion.merge_type_ = merge_info.merge_type_;
      suggestion.tenant_id_ = merge_info.tenant_id_;
      suggestion.ls_id_ = merge_info.ls_id_.id();
      suggestion.tablet_id_ = merge_info.tablet_id_.id();
      suggestion.merge_start_time_ = merge_info.merge_start_time_;
      suggestion.merge_finish_time_ = common::ObTimeUtility::fast_current_time();
      if (OB_FAIL(array_.add(suggestion))) {
        STORAGE_LOG(WARN, "failed to add suggestion", K(ret), K(suggestion));
      }
    }
  }
  return ret;
}

// count can't be zero
int64_t ObCompactionSuggestionMgr::calc_variance(
    const int64_t count,
    const int64_t max_value,
    const int64_t min_value,
    const int64_t avg_value)
{
  int64_t variance = 0;
  const int64_t max_min_diff = max_value - min_value;
  const int64_t max_avg_diff = max_value - avg_value;
  const int64_t min_avg_diff = avg_value - min_value;
  variance = (max_min_diff * max_min_diff - (max_avg_diff * max_avg_diff - min_avg_diff * min_avg_diff) / 2 * count) / count;
  return variance;
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
  const int64_t cost_time = common::ObTimeUtility::fast_current_time() - merge_info.merge_start_time_;
  if (cost_time > MERGE_COST_TIME_PARAM) {
    if (merge_info.incremental_row_count_ >= INC_ROW_CNT_PARAM) {
      ADD_COMPACTION_INFO_PARAM(buf, buf_len,
              "inc_row_count", merge_info.incremental_row_count_,
              "suggest", "too many incremental row");
    } else if (merge_info.macro_block_count_ >= SINGLE_PARTITION_MACRO_CNT_PARAM) {
      ADD_COMPACTION_INFO_PARAM(buf, buf_len,
              "macro_count", merge_info.macro_block_count_,
              "suggest", "large single partition");
    }
    if (1 != merge_info.concurrent_cnt_ && progress.is_inited()) { // parallel compaction
      ObParalleMergeInfo &paral_info = merge_info.parallel_merge_info_;
      const int64_t count = paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].count_;
      if (0 < count) {
        const int64_t max_scan_row_cnt = paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].max_value_;
        const int64_t min_scan_row_cnt = paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].min_value_;
        const int64_t avg_scan_row_cnt = paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].sum_value_ / count;
        const int64_t variance = calc_variance(count, max_scan_row_cnt, min_scan_row_cnt, avg_scan_row_cnt);
        if (0 < avg_scan_row_cnt && SCAN_AVERAGE_RAITO < variance / (avg_scan_row_cnt * avg_scan_row_cnt)) {
          if (progress.get_concurrent_count() != merge_info.concurrent_cnt_
              || OB_ISNULL(scan_row_array = progress.get_scanned_row_cnt_arr())) {
            STORAGE_LOG(WARN, "parallel degress is invalid or progress scan row count array is null",
                K(ret), K(progress));
          } else if (scan_row_array[progress.get_concurrent_count() - 1]
              == paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].max_value_) {
            // last parallel task have large scan size
            ADD_COMPACTION_INFO_PARAM(buf, buf_len,
                "uneven_scan_row_cnt_max", paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].max_value_,
                "uneven_scan_row_cnt_min", paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].min_value_,
                "suggest", "last parallel task have large scan row cnt");
          } else {
            ADD_COMPACTION_INFO_PARAM(buf, buf_len,
                "uneven_scan_row_cnt_max", paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].max_value_,
                "uneven_scan_row_cnt_min", paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].min_value_,
                "suggest", "uneven scan row cnt");
          }
        }
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
  if (strlen(buf) > 0) {
    suggestion.merge_type_ = merge_info.merge_type_;
    suggestion.tenant_id_ = merge_info.tenant_id_;
    suggestion.ls_id_ = merge_info.ls_id_.id();
    suggestion.tablet_id_ = merge_info.tablet_id_.id();
    suggestion.merge_start_time_ = merge_info.merge_start_time_;
    suggestion.merge_finish_time_ = common::ObTimeUtility::fast_current_time();
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
