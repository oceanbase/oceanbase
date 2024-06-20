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

#ifndef SRC_STORAGE_COMPACTION_OB_COMPACTION_SUGGESTION_H_
#define SRC_STORAGE_COMPACTION_OB_COMPACTION_SUGGESTION_H_

#include "share/scheduler/ob_dag_scheduler_config.h"
#include "storage/compaction/ob_compaction_util.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_iarray.h"
#include "storage/ob_sstable_struct.h"

namespace oceanbase
{
namespace compaction
{
class ObPartitionMergeProgress;

// 32 B
struct ObCompactionHistogramBucket final
{
  ObCompactionHistogramBucket()
  {
    clear();
  }
  ~ObCompactionHistogramBucket() {}
  void clear() {
    max_time_ = 0;
    min_time_ = INT64_MAX;
    sum_time_ = 0;
    finish_cnt_ = 0;
  }
  int64_t max_time_;
  int64_t min_time_;
  int64_t sum_time_;
  int64_t finish_cnt_;
};

struct ObCompactionHistogramBucketUtil final
{
  static int64_t get_index(const int64_t times);
  static int64_t get_index_value(const int64_t index);

  static const int64_t MAX_BUCKET_VALUE = 10000 * 1000LL * 1000LL; // 10000s
  static const int64_t MIN_BUCKET_VALUE = 1;
  static const int64_t BUCKET_MAX_COUNT = 24;
  // 1ms,2ms,5ms,10ms,20ms,40ms,80ms,160ms,320ms,640ms,1280ms,2560ms,5120ms,10000ms,20000ms,40000ms,80000ms,
  // 160000ms,320000ms,640000ms,1280000ms,2560000ms,5120000ms,10000000ms
  static constexpr int64_t Bucket[BUCKET_MAX_COUNT] = {
    MIN_BUCKET_VALUE, 2000LL, 5000LL, 10000LL, 20000LL, 40000LL, 80000LL, 160000LL,
    320000LL, 640000LL, 1280000LL, 2560000LL, 5120000LL, 10000000LL, 20000000LL, 40000000LL,
    80000000LL, 160000000LL, 320000000LL, 640000000LL, 1280000000LL, 2560000000LL, 5120000000LL, MAX_BUCKET_VALUE};
};

// 8 B+768 B=776 B
class ObCompactionHistogramStat final
{
public:
  ObCompactionHistogramStat()
  {
    clear();
  }
  ~ObCompactionHistogramStat() {}

  void clear()
  {
    running_cnt_ = 0;
    failed_cnt_ = 0;
    for (int64_t i = 0; i < ObCompactionHistogramBucketUtil::BUCKET_MAX_COUNT; ++i) {
      bucket_[i].clear();
    }
  }
  bool is_empty() const;
  int add_value(const int64_t time, const bool failed);
  int64_t get_total_cnt() const;
  int64_t get_sum_time() const;
  int64_t get_min_time() const;
  int64_t get_max_time() const;
  int64_t get_percentile(const double p) const;
  DECLARE_TO_STRING;

private:
  int64_t get_left_time(const int64_t idx) const;
  int64_t get_right_time(const int64_t idx) const;

public:
  int32_t running_cnt_; // running dag cnt now
  int32_t failed_cnt_;
  ObCompactionHistogramBucket bucket_[ObCompactionHistogramBucketUtil::BUCKET_MAX_COUNT];
};

template <typename T>
class ObInfoRingArray
{
public:
  ObInfoRingArray(ObIAllocator &allocator)
    : is_inited_(false),
      allocator_(allocator),
      lock_(common::ObLatchIds::INFO_MGR_LOCK),
      pos_(0),
      max_cnt_(0),
      array_(NULL)
  {
  }
  virtual ~ObInfoRingArray() { clear(); }

  int init(const int64_t max_cnt);
  void clear()
  {
    SpinRLockGuard guard(lock_);
    pos_ = 0;
  }
  void destroy()
  {
    clear();
    if (OB_NOT_NULL(array_)) {
      allocator_.free(array_);
      array_ = NULL;
    }
    is_inited_ = false;
  }
  int get(const int64_t pos, T &item);
  int add(const T &item);
  int add_no_lock(const T &item);
  int get_list(ObIArray<T> &input_array);
  int size() const { return pos_ > max_cnt_ ? max_cnt_ : pos_; }
  int get_last_pos() const { return (pos_ - 1 + max_cnt_) % max_cnt_; }

protected:
  bool is_inited_;
  ObIAllocator &allocator_;
  common::SpinRWLock lock_;
  int64_t pos_; // pos can fill next time
  int64_t max_cnt_;
  T *array_;
};

struct ObCompactionSuggestion
{
  ObCompactionSuggestion()
    : merge_type_(compaction::INVALID_MERGE_TYPE),
      tenant_id_(OB_INVALID_TENANT_ID),
      ls_id_(0),
      tablet_id_(0),
      merge_start_time_(0),
      merge_finish_time_(0),
      suggestion_()
    {}
  TO_STRING_KV("merge_type", merge_type_to_str(merge_type_), K_(tenant_id), K_(ls_id), K_(tablet_id), K_(merge_start_time),
      K_(merge_finish_time), K_(suggestion));

  compaction::ObMergeType merge_type_;
  uint64_t tenant_id_;
  int64_t ls_id_;
  int64_t tablet_id_;
  int64_t merge_start_time_;
  int64_t merge_finish_time_;
  char suggestion_[common::OB_DIAGNOSE_INFO_LENGTH];
};

// 7760 B
struct ObCompactionDagStatus final
{
  ObCompactionDagStatus()
  { clear(); }
  ~ObCompactionDagStatus() {}

  void clear();
  void update_running_cnt(const ObIArray<int64_t> &dag_running_cnts);
  void update_finish_cnt(
      const share::ObDagType::ObDagTypeEnum type,
      const bool failed,
      const int64_t exe_time);

  DECLARE_TO_STRING;

  // max COMPACTION mode dag is DAG_TYPE_MDS_MINI_MERGE, which is 9
  static const int64_t COMPACTION_DAG_MAX = 10;
  // max COMPACTION prio DAG_PRIO_COMPACTION_LOW = 4
  static const int64_t COMPACTION_PRIORITY_MAX = 5;
  // for mini/minor/major merge
  static constexpr int64_t COST_LONG_TIME[COMPACTION_PRIORITY_MAX] = {
    10 * 60 * 1000 * 1000L, INT64_MAX, 20 * 60 * 1000 * 1000L, INT64_MAX, 60 * 60 * 1000 * 1000L}; // 10m,30m,60m
  static int64_t get_cost_long_time(const int64_t prio);
  ObCompactionHistogramStat histogram_stat_[COMPACTION_DAG_MAX];
};

/*
 * ObCompactionSuggestionMgr
 * */
class ObCompactionSuggestionMgr {
public:
  enum ObCompactionSuggestionReason
  {
    DAG_FULL,
    SCHE_SLOW,
    DAG_COST_LONGTIME,
    MAX_REASON
  };
  static int mtl_init(ObCompactionSuggestionMgr *&compaction_suggestion_mgr);
  ObCompactionSuggestionMgr()
    : is_inited_(false),
      click_time_(ObTimeUtility::fast_current_time()),
      allocator_("CompSuggestMgr"),
      lock_(ObLatchIds::COMPACTION_DIAGNOSE_LOCK),
      compaction_dag_status_(),
      array_(allocator_)
  {
  }
  ~ObCompactionSuggestionMgr() { destroy(); }

  int init();
  void destroy()
  {
    array_.destroy();
  }
  int get_suggestion_list(ObIArray<ObCompactionSuggestion> &input_array);
  // func for compaction status
  int update_running_cnt(const ObIArray<int64_t> &dag_running_cnts);
  int update_finish_cnt(
      const share::ObDagType::ObDagTypeEnum type,
      const bool failed,
      const int64_t exe_time);
  void clear_compaction_dag_status();

  int analyze_merge_info(
    const ObSSTableMergeInfo &merge_info,
    const share::ObDagType::ObDagTypeEnum type,
    const int64_t cost_time);

  int diagnose_for_suggestion(
      const ObIArray<int64_t> &reasons,
      const ObIArray<int64_t> &running_cnts,
      const ObIArray<int64_t> &thread_limits,
      const ObIArray<int64_t> &dag_running_cnts);
public:
  static const int64_t TOO_MANY_FAILED_COUNT = 20;
  static const int64_t SCAN_AVERAGE_RAITO = 4; // 2 * 2
  static const int64_t INC_ROW_CNT_PARAM = 5 * 1000 * 1000; // 5 Million
  static const int64_t SINGLE_PARTITION_MACRO_CNT_PARAM = 256 * 1024; // single partition size 500G
  static const int64_t MACRO_CNT_PARAM = 5 * 1000; // 5 k

  static const char *ObCompactionSuggestionReasonStr[];
  static const char* get_suggestion_reason(const int64_t reason);
  static const char *ObAddWorkerThreadSuggestion[share::ObDagPrio::DAG_PRIO_MAX];
  static const char* get_add_thread_suggestion(const int64_t priority);
private:
  int64_t calc_variance(
      const int64_t count,
      const int64_t max_value,
      const int64_t min_value,
      const int64_t avg_value);
  int analyze_for_suggestion(
    const int64_t reason,
    const int64_t priority,
    const int64_t thread_limit,
    const int64_t running_task,
    ObCompactionDagStatus &dag_status);

private:
  friend class ObCompactionSuggestionIterator;
private:
  static const int64_t SUGGESTION_MAX_CNT = 200;

  bool is_inited_;
  int64_t click_time_;
  ObArenaAllocator allocator_;
  lib::ObMutex lock_;
  ObCompactionDagStatus compaction_dag_status_;
  ObInfoRingArray<ObCompactionSuggestion> array_;
};

/*
 * ObCompactionSuggestionIterator
 * */

class ObCompactionSuggestionIterator
{
public:
  ObCompactionSuggestionIterator() :
    suggestion_array_(),
    cur_idx_(0),
    is_opened_(false)
  {
  }
  virtual ~ObCompactionSuggestionIterator() { reset(); }
  int open(const int64_t tenant_id);
  int get_next_info(ObCompactionSuggestion &info);
  void reset()
  {
    suggestion_array_.reset();
    cur_idx_ = 0;
    is_opened_ = false;
  }
private:
  ObArray<ObCompactionSuggestion> suggestion_array_;
  int64_t cur_idx_;
  bool is_opened_;
};

template <typename T>
int ObInfoRingArray<T>::init(const int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "already been initiated", K(ret));
  } else if (max_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "max count is invalid", K(ret), K(max_cnt));
  } else {
    void *buf = NULL;
    if (OB_ISNULL(buf = allocator_.alloc(max_cnt * sizeof(T)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to allocate memory", K(ret), K(max_cnt),
          "alloc_size", max_cnt * sizeof(T));
    } else {
      array_ = new(buf) T[max_cnt]();
      max_cnt_ = max_cnt;
      is_inited_ = true;
    }
  }
  return ret;
}

template <typename T>
int ObInfoRingArray<T>::get(const int64_t idx, T &item)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(array_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "array is null", K(ret));
  } else if (idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid idx", K(ret), K(idx), K_(pos));
  } else {
    SpinRLockGuard guard(lock_);
    if (idx >= max_cnt_ || idx >= pos_) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      item = array_[idx];
    }
  }
  return ret;
}

template <typename T>
int ObInfoRingArray<T>::add(const T &item)
{
  SpinWLockGuard guard(lock_);
  return add_no_lock(item);
}

template <typename T>
int ObInfoRingArray<T>::add_no_lock(const T &item)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(array_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "array is null", K(ret));
  } else {
    array_[pos_ % max_cnt_] = item;
    pos_++;
  }
  return ret;
}

template <typename T>
int ObInfoRingArray<T>::get_list(ObIArray<T> &input_array)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  for (int i = 0; OB_SUCC(ret) && i < size(); ++i) {
    if (OB_FAIL(input_array.push_back(array_[i]))) {
      STORAGE_LOG(WARN, "failed to push into input array", K(ret), K(i), K(array_[i]));
    }
  }
  return ret;
}

}//compaction
}//oceanbase

#endif /* SRC_STORAGE_COMPACTION_OB_COMPACTION_SUGGESTION_H_ */
