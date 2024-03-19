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

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace compaction
{
/*
 * ObCompactionHistogramBucketUtil implement
 * */
int64_t ObCompactionHistogramBucketUtil::get_index(const int64_t times)
{
  int64_t index = BUCKET_MAX_COUNT - 1;
  for (int64_t i = 0; i < BUCKET_MAX_COUNT; ++i) {
    if (Bucket[i] >= times) {
      index = i;
      break;
    }
  }
  return index;
}

int64_t ObCompactionHistogramBucketUtil::get_index_value(const int64_t index)
{
  int64_t value = 0;
  if (BUCKET_MAX_COUNT > index) {
    value = Bucket[index];
  }
  return value;
}

/*
 * ObCompactionHistogramStat implement
 * */
bool ObCompactionHistogramStat::is_empty() const
{
  bool bret = true;
  bret &= (0 == running_cnt_ && 0 == get_total_cnt());
  return bret;
}

int ObCompactionHistogramStat::add_value(const int64_t time, const bool failed)
{
  int ret = OB_SUCCESS;
  int64_t index = ObCompactionHistogramBucketUtil::get_index(time);
  if (ObCompactionHistogramBucketUtil::BUCKET_MAX_COUNT < index || 0 > index) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get unexpected index", K(ret), K(index));
  } else {
    ++bucket_[index].finish_cnt_;
    if (failed) {
      ++failed_cnt_;
    }
    if (bucket_[index].min_time_ > time) {
      bucket_[index].min_time_ = time;
    }
    if (bucket_[index].max_time_ < time) {
      bucket_[index].max_time_ = time;
    }
    bucket_[index].sum_time_ += time;
  }
  return ret;
}

int64_t ObCompactionHistogramStat::get_total_cnt() const
{
  int64_t total_cnt = 0;
  for (int64_t i = 0; i < ObCompactionHistogramBucketUtil::BUCKET_MAX_COUNT; ++i) {
    total_cnt += bucket_[i].finish_cnt_;
  }
  return total_cnt;
}

int64_t ObCompactionHistogramStat::get_sum_time() const
{
  int64_t sum_time = 0;
  for (int64_t i = 0; i < ObCompactionHistogramBucketUtil::BUCKET_MAX_COUNT; ++i) {
    sum_time += bucket_[i].sum_time_;
  }
  return sum_time;
}

int64_t ObCompactionHistogramStat::get_min_time() const
{
  int64_t min_time = INT64_MAX;
  for (int64_t i = 0; i < ObCompactionHistogramBucketUtil::BUCKET_MAX_COUNT; ++i) {
    min_time = MIN(min_time, bucket_[i].min_time_);
  }
  return min_time;
}

int64_t ObCompactionHistogramStat::get_max_time() const
{
  int64_t max_time = 0;
  for (int64_t i = 0; i < ObCompactionHistogramBucketUtil::BUCKET_MAX_COUNT; ++i) {
    max_time = MAX(max_time, bucket_[i].max_time_);
  }
  return max_time;
}

int64_t ObCompactionHistogramStat::get_left_time(const int64_t idx) const
{
  int64_t min_time = (idx == 0) ? 0 : ObCompactionHistogramBucketUtil::get_index_value(idx - 1);
  if (0 < bucket_[idx].finish_cnt_) {
    min_time = MAX(min_time, bucket_[idx].min_time_);
  }
  return min_time;
}

int64_t ObCompactionHistogramStat::get_right_time(const int64_t idx) const
{
  int64_t max_time = ObCompactionHistogramBucketUtil::get_index_value(idx);
  if (0 < bucket_[idx].finish_cnt_) {
    max_time = MIN(max_time, bucket_[idx].max_time_);
  }
  return max_time;
}

int64_t ObCompactionHistogramStat::get_percentile(const double p) const
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  int64_t total_cnt = get_total_cnt();
  int64_t threshold = total_cnt * (p / 100);
  int64_t current_cnt = 0;
  int64_t left_value = 0;
  int64_t right_value = 0;
  double pos_ratio = 0;
  for (int64_t i = 0; i < ObCompactionHistogramBucketUtil::BUCKET_MAX_COUNT; ++i) {
    current_cnt += bucket_[i].finish_cnt_;
    if (current_cnt >= threshold) {
      left_value = get_left_time(i);
      right_value = get_right_time(i);
      if (bucket_[i].finish_cnt_ > 0) { // defence
        pos_ratio = (threshold - (current_cnt - bucket_[i].finish_cnt_)) / static_cast<double>(bucket_[i].finish_cnt_);
      }
      value = left_value + (right_value - left_value) * pos_ratio;
      break;
    }
  }
  return value;
}

int64_t ObCompactionHistogramStat::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  #define J_TMP_STRING(value) \
    if (0 < value) { \
      J_KV(K(value)); \
      J_COMMA(); \
    }
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    int64_t finish_cnt = get_total_cnt();
    int64_t max_time = get_max_time();
    int64_t min_time = get_min_time();
    int64_t sum_time = get_sum_time();
    J_OBJ_START();
    J_TMP_STRING(running_cnt_);
    J_TMP_STRING(failed_cnt_);
    J_TMP_STRING(finish_cnt);
    if (0 < finish_cnt) {
      J_KV(K(max_time));
      J_COMMA();
      J_KV(K(min_time));
      J_COMMA();
      J_KV("avg_time", sum_time / finish_cnt);
      J_COMMA();
      int64_t p90_time = get_percentile(90);
      J_KV(K(p90_time));
    }
    J_OBJ_END();
  }
  #undef J_TMP_STRING
  return pos;
}

/*
 * ObCompactionDagStatus implement
 * */
int64_t ObCompactionDagStatus::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  #define J_TMP_STRING(type) \
    if (COMPACTION_DAG_MAX > type && !histogram_stat_[type].is_empty()) { \
      J_KV("type", OB_DAG_TYPES[type].dag_type_str_, "stat", histogram_stat_[type]); \
      J_COMMA(); \
    }
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    // mini
    J_TMP_STRING(ObDagType::DAG_TYPE_MINI_MERGE);
    J_TMP_STRING(ObDagType::DAG_TYPE_MDS_MINI_MERGE);
    J_TMP_STRING(ObDagType::DAG_TYPE_TX_TABLE_MERGE);
    // minor
    J_TMP_STRING(ObDagType::DAG_TYPE_MERGE_EXECUTE);
    // major
    J_TMP_STRING(ObDagType::DAG_TYPE_MAJOR_MERGE);
    J_TMP_STRING(ObDagType::DAG_TYPE_CO_MERGE_PREPARE);
    J_TMP_STRING(ObDagType::DAG_TYPE_CO_MERGE_SCHEDULE);
    J_TMP_STRING(ObDagType::DAG_TYPE_CO_MERGE_BATCH_EXECUTE);
    J_TMP_STRING(ObDagType::DAG_TYPE_CO_MERGE_FINISH);
    J_OBJ_END();
  }
  #undef J_TMP_STRING
  return pos;
}

void ObCompactionDagStatus::clear()
{
  for (int64_t i = 0; i < COMPACTION_DAG_MAX; ++i) {
    histogram_stat_[i].clear();
  }
}

void ObCompactionDagStatus::update_running_cnt(const ObIArray<int64_t> &dag_running_cnts)
{
  for (int64_t i = 0; i < COMPACTION_DAG_MAX && i < dag_running_cnts.count(); ++i) {
    histogram_stat_[i].running_cnt_ = dag_running_cnts.at(i);
  }
}

void ObCompactionDagStatus::update_finish_cnt(
    const share::ObDagType::ObDagTypeEnum type,
    const bool failed,
    const int64_t exe_time)
{
  if (COMPACTION_DAG_MAX > type) {
    histogram_stat_[type].add_value(exe_time, failed);
  }
}

int64_t ObCompactionDagStatus::get_cost_long_time(const int64_t prio)
{
  int64_t ret_time = INT64_MAX;
  if (0 < prio && COMPACTION_PRIORITY_MAX > prio) {
    ret_time = COST_LONG_TIME[prio];
  }
  return ret_time;
}

/*
 * ObCompactionSuggestionMgr implement
 * */
const char *ObCompactionSuggestionMgr::ObCompactionSuggestionReasonStr[] =
{
  "dag cnt is full",
  "schedule slower than add",
  "dag cost long time"
};

const char* ObCompactionSuggestionMgr::get_suggestion_reason(const int64_t reason)
{
  static_assert(sizeof(ObCompactionSuggestionReasonStr)/ sizeof(char *) == static_cast<int64_t>(ObCompactionSuggestionReason::MAX_REASON),
                "ObCompactionSuggestionReasonStr array size mismatch enum ObCompactionSuggestionReason count");
  const char *str = "";
  if (ObCompactionSuggestionReason::DAG_FULL > reason || ObCompactionSuggestionReason::MAX_REASON <= reason) {
    str = "invalid_reason";
  } else {
    str = ObCompactionSuggestionReasonStr[reason];
  }
  return str;
}

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
  static_assert(sizeof(ObAddWorkerThreadSuggestion)/ sizeof(char *) == static_cast<int64_t>(share::ObDagPrio::DAG_PRIO_MAX),
                "ObAddWorkerThreadSuggestion array size mismatch enum ObDagPrio count");
  const char *str = "";
  if (share::ObDagPrio::DAG_PRIO_MAX <= priority|| share::ObDagPrio::DAG_PRIO_COMPACTION_HIGH > priority) {
    str = "invalid_priority";
  } else {
    str = ObAddWorkerThreadSuggestion[priority];
  }
  return str;
}

int ObCompactionSuggestionMgr::mtl_init(ObCompactionSuggestionMgr *&compaction_suggestion_mgr)
{
  return compaction_suggestion_mgr->init();
}

int ObCompactionSuggestionMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(array_.init(SUGGESTION_MAX_CNT))) {
    STORAGE_LOG(WARN, "failed to init ObInfoRingArray", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObCompactionSuggestionMgr::get_suggestion_list(ObIArray<ObCompactionSuggestion> &input_array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(array_.get_list(input_array))) {
    STORAGE_LOG(WARN, "failed to get suggestion list", K(ret));
  }
  return ret;
}

int ObCompactionSuggestionMgr::update_running_cnt(const ObIArray<int64_t> &dag_running_cnts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else {
    ObMutexGuard guard(lock_);
    compaction_dag_status_.update_running_cnt(dag_running_cnts);
  }
  return ret;
}

int ObCompactionSuggestionMgr::update_finish_cnt(
    const share::ObDagType::ObDagTypeEnum type,
    const bool failed,
    const int64_t exe_time)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (ObCompactionDagStatus::COMPACTION_DAG_MAX > type) {
    ObMutexGuard guard(lock_);
    compaction_dag_status_.update_finish_cnt(type, failed, exe_time);
  }
  return ret;
}

void ObCompactionSuggestionMgr::clear_compaction_dag_status()
{
  ObMutexGuard guard(lock_);
  compaction_dag_status_.clear();
}

// suggest to add threads periodically
int ObCompactionSuggestionMgr::analyze_for_suggestion(
    const int64_t reason,
    const int64_t priority,
    const int64_t thread_limit,
    const int64_t running_task,
    ObCompactionDagStatus &dag_status)
{
  #define ADD_COMPACTION_DAG_INFO_PARAM(dag_type) \
    if (ObCompactionDagStatus::COMPACTION_DAG_MAX > dag_type && !dag_status.histogram_stat_[dag_type].is_empty()) { \
      ADD_COMPACTION_INFO_PARAM(buf, buf_len, \
        "type", ObIDag::get_dag_type_str(dag_type), \
        "stat", dag_status.histogram_stat_[dag_type]); \
    }
  int ret = OB_SUCCESS;
  // start to analyze
  ObCompactionSuggestion suggestion;
  char *buf = suggestion.suggestion_;
  const int64_t buf_len = OB_DIAGNOSE_INFO_LENGTH;

  ADD_COMPACTION_INFO_PARAM(buf, buf_len,
      "reason", get_suggestion_reason(reason),
      K(thread_limit),
      K(running_task),
      "suggest", ObCompactionSuggestionMgr::get_add_thread_suggestion(priority));
  if (share::ObDagPrio::DAG_PRIO_COMPACTION_HIGH == priority) {
    ADD_COMPACTION_DAG_INFO_PARAM(ObDagType::ObDagTypeEnum::DAG_TYPE_MINI_MERGE);
    ADD_COMPACTION_DAG_INFO_PARAM(ObDagType::ObDagTypeEnum::DAG_TYPE_MDS_MINI_MERGE);
    ADD_COMPACTION_DAG_INFO_PARAM(ObDagType::ObDagTypeEnum::DAG_TYPE_TX_TABLE_MERGE);
  } else if (share::ObDagPrio::DAG_PRIO_COMPACTION_MID == priority) {
    ADD_COMPACTION_DAG_INFO_PARAM(ObDagType::ObDagTypeEnum::DAG_TYPE_MERGE_EXECUTE);
  } else if (share::ObDagPrio::DAG_PRIO_COMPACTION_LOW == priority) {
    ADD_COMPACTION_DAG_INFO_PARAM(ObDagType::ObDagTypeEnum::DAG_TYPE_MAJOR_MERGE);
    ADD_COMPACTION_DAG_INFO_PARAM(ObDagType::ObDagTypeEnum::DAG_TYPE_CO_MERGE_PREPARE);
    ADD_COMPACTION_DAG_INFO_PARAM(ObDagType::ObDagTypeEnum::DAG_TYPE_CO_MERGE_SCHEDULE);
    ADD_COMPACTION_DAG_INFO_PARAM(ObDagType::ObDagTypeEnum::DAG_TYPE_CO_MERGE_BATCH_EXECUTE);
    ADD_COMPACTION_DAG_INFO_PARAM(ObDagType::ObDagTypeEnum::DAG_TYPE_CO_MERGE_FINISH);
  }
  if (strlen(buf) > 0) {
    suggestion.merge_type_ =  INVALID_MERGE_TYPE;
    if (ObDagPrio::DAG_PRIO_COMPACTION_HIGH == priority) {
      suggestion.merge_type_ = MINI_MERGE;
    } else if (ObDagPrio::DAG_PRIO_COMPACTION_MID == priority) {
      suggestion.merge_type_ = MINOR_MERGE;
    } else if (ObDagPrio::DAG_PRIO_COMPACTION_LOW == priority) {
      suggestion.merge_type_ = MEDIUM_MERGE;
    }
    suggestion.tenant_id_ = MTL_ID();
    suggestion.ls_id_ = UNKNOW_LS_ID.id();
    suggestion.tablet_id_ = UNKNOW_TABLET_ID.id();
    suggestion.merge_start_time_ = common::ObTimeUtility::fast_current_time();
    suggestion.merge_finish_time_ = suggestion.merge_start_time_;
    if (OB_FAIL(array_.add(suggestion))) {
      STORAGE_LOG(WARN, "failed to add suggestion", K(ret), K(suggestion));
    }
  }
  #undef ADD_COMPACTION_DAG_INFO_PARAM
  return ret;
}

int ObCompactionSuggestionMgr::diagnose_for_suggestion(
    const ObIArray<int64_t> &reasons,
    const ObIArray<int64_t> &running_cnts,
    const ObIArray<int64_t> &thread_limits,
    const ObIArray<int64_t> &dag_running_cnts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (ObIDag::MergeDagPrioCnt != reasons.count()
      || ObIDag::MergeDagPrioCnt != running_cnts.count()
      || ObIDag::MergeDagPrioCnt != thread_limits.count()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else {
    int64_t suggestion_reason;
    int64_t thread_limit;
    int64_t running_cnt;
    update_running_cnt(dag_running_cnts);
    ObCompactionDagStatus dag_status;
    {
      ObMutexGuard guard(lock_);
      dag_status = compaction_dag_status_; // copy
    }
    // force to print log
    int64_t end_time = common::ObTimeUtility::fast_current_time();
    STORAGE_LOG(INFO, "[COMPACTION DAG STATUS] ", "start_time", click_time_, K(end_time), K(dag_status));
    click_time_ = end_time;
    // ayalyze
    for (int64_t i = 0; i < ObIDag::MergeDagPrioCnt; ++i) {
      if (ObCompactionSuggestionReason::MAX_REASON <= reasons.at(i)
          || ObCompactionSuggestionReason::DAG_FULL > reasons.at(i)) {
        // do nothing
      } else if (OB_TMP_FAIL(analyze_for_suggestion(
              reasons.at(i),
              ObIDag::MergeDagPrio[i],
              thread_limits.at(i),
              running_cnts.at(i),
              dag_status))) {
        COMMON_LOG(WARN, "fail to analyze insufficient thread", K(tmp_ret));
      }
    }
    clear_compaction_dag_status();
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

// analyze success merge dag
int ObCompactionSuggestionMgr::analyze_merge_info(
    const ObSSTableMergeInfo &merge_info,
    const share::ObDagType::ObDagTypeEnum type,
    const int64_t cost_time)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else {
    ObCompactionSuggestion suggestion;
    char * buf = suggestion.suggestion_;
    int64_t *scan_row_array = nullptr;
    bool need_suggestion = false;
    const int64_t buf_len = OB_DIAGNOSE_INFO_LENGTH;
    if (ObCompactionDagStatus::get_cost_long_time(OB_DAG_TYPES[type].init_dag_prio_) <= cost_time) {
      ADD_COMPACTION_INFO_PARAM(buf, buf_len,
                "reason", get_suggestion_reason(ObCompactionSuggestionReason::DAG_COST_LONGTIME));
      if (TOO_MANY_FAILED_COUNT <= merge_info.retry_cnt_) {
        ADD_COMPACTION_INFO_PARAM(buf, buf_len,
                "too many failed count", merge_info.retry_cnt_);
      }
      if (merge_info.incremental_row_count_ >= INC_ROW_CNT_PARAM) {
        ADD_COMPACTION_INFO_PARAM(buf, buf_len,
                "too many incremental row", merge_info.incremental_row_count_);
        need_suggestion = true;
      }
      if (merge_info.macro_block_count_ >= SINGLE_PARTITION_MACRO_CNT_PARAM) {
        ADD_COMPACTION_INFO_PARAM(buf, buf_len,
                "large single partition", merge_info.macro_block_count_);
        need_suggestion = true;
      }
      if (merge_info.macro_block_count_ >= MACRO_CNT_PARAM) {
        ADD_COMPACTION_INFO_PARAM(buf, buf_len,
              "macro_block_count", merge_info.macro_block_count_,
              "multiplexed_macro_count", merge_info.multiplexed_macro_block_count_,
              "new_flush_data_rate", merge_info.new_flush_data_rate_,
              "concurrent_cnt", merge_info.concurrent_cnt_);
        need_suggestion = true;
      }
      if (1 != merge_info.concurrent_cnt_) { // parallel compaction
        const ObParalleMergeInfo &paral_info = merge_info.parallel_merge_info_;
        const int64_t count = paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].count_;
        if (0 < count) {
          const int64_t max_scan_row_cnt = paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].max_value_;
          const int64_t min_scan_row_cnt = paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].min_value_;
          const int64_t avg_scan_row_cnt = paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].sum_value_ / count;
          const int64_t variance = calc_variance(count, max_scan_row_cnt, min_scan_row_cnt, avg_scan_row_cnt);
          if (0 < avg_scan_row_cnt && SCAN_AVERAGE_RAITO < variance / (avg_scan_row_cnt * avg_scan_row_cnt)) {
            ADD_COMPACTION_INFO_PARAM(buf, buf_len,
                "uneven scan row cnt, max", paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].max_value_,
                "min", paral_info.info_[ObParalleMergeInfo::SCAN_UNITS].min_value_);
          }
        }
        // add threads is valid only when concurrent_cnt_ is not 1
        if (need_suggestion) {
          ADD_COMPACTION_INFO_PARAM(buf, buf_len,
            "suggestion", get_add_thread_suggestion(OB_DAG_TYPES[type].init_dag_prio_));
        }
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
  }
  return ret;
}

int ObCompactionSuggestionIterator::open(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  omt::TenantIdList all_tenants;
  all_tenants.set_label(ObModIds::OB_TENANT_ID_LIST);
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObCompactionSuggestionIterator has been opened", K(ret));
  } else if (!::is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else if (OB_SYS_TENANT_ID == tenant_id) { // sys tenant can get all tenants' info
    GCTX.omt_->get_tenant_ids(all_tenants);
  } else if (OB_FAIL(all_tenants.push_back(tenant_id))) {
    STORAGE_LOG(WARN, "failed to push back tenant_id", K(ret), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_tenants.size(); ++i) {
    uint64_t tenant_id = all_tenants[i];
    if (!is_virtual_tenant_id(tenant_id)) { // skip virtual tenant
      MTL_SWITCH(tenant_id) {
        if (OB_FAIL(MTL(ObCompactionSuggestionMgr *)->get_suggestion_list(suggestion_array_))) {
          STORAGE_LOG(WARN, "failed to get suggestion list", K(ret));
        }
      } else {
        if (OB_TENANT_NOT_IN_SERVER != ret) {
          STORAGE_LOG(WARN, "switch tenant failed", K(ret), K(tenant_id));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  } // end for

  if (OB_SUCC(ret)) {
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
  } else if (cur_idx_ >= suggestion_array_.count()) {
    ret = OB_ITER_END;
  } else {
    info = suggestion_array_.at(cur_idx_);
    ++cur_idx_;
  }
  return ret;
}

}//compaction
}//oceanbase
