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

#include "storage/compaction/ob_compaction_util.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace compaction
{
class ObPartitionMergeProgress;
struct ObTabletMergeInfo;

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
    : merge_type_(storage::INVALID_MERGE_TYPE),
      tenant_id_(OB_INVALID_TENANT_ID),
      ls_id_(0),
      tablet_id_(0),
      merge_start_time_(0),
      merge_finish_time_(0),
      suggestion_()
    {}
  TO_STRING_KV(K_(merge_type), K_(tenant_id), K_(ls_id), K_(tablet_id), K_(merge_start_time),
      K_(merge_finish_time), K_(suggestion));

  storage::ObMergeType merge_type_;
  uint64_t tenant_id_;
  int64_t ls_id_;
  int64_t tablet_id_;
  int64_t merge_start_time_;
  int64_t merge_finish_time_;
  char suggestion_[common::OB_DIAGNOSE_INFO_LENGTH];
};

/*
 * ObCompactionSuggestionMgr
 * */
class ObCompactionSuggestionMgr {
public:
  ObCompactionSuggestionMgr()
    : allocator_("CompSuggestMgr"),
      array_(allocator_)
  {
  }
  ~ObCompactionSuggestionMgr() { array_.destroy(); }

  int init();
  int get(const int64_t pos, ObCompactionSuggestion &info)
  {
    return array_.get(pos, info);
  }

  static ObCompactionSuggestionMgr &get_instance() {
    static ObCompactionSuggestionMgr instance_;
    return instance_;
  }

  int analyze_merge_info(ObTabletMergeInfo &merge_info, ObPartitionMergeProgress &progress);

private:
  friend class ObCompactionSuggestionIterator;
private:
  static const int64_t SUGGESTION_MAX_CNT = 200;
  static const int64_t SCAN_AVERAGE_PARAM = 300;
  static const int64_t INC_ROW_CNT_PARAM = 5 * 1000 * 1000; // 5 Million
  static const int64_t MERGE_COST_TIME_PARAM = 1000L * 1000L * 60L * 60L; // 1 hour
  static const int64_t SINGLE_PARTITION_MACRO_CNT_PARAM = 256 * 1024; // single partition size 500G
  static const int64_t MACRO_CNT_PARAM = 10 * 1000; // 10 k
  static constexpr float MACRO_MULTIPLEXED_PARAM = 0.3;

  ObArenaAllocator allocator_;
  ObInfoRingArray<ObCompactionSuggestion> array_;
};

/*
 * ObCompactionSuggestionIterator
 * */

class ObCompactionSuggestionIterator
{
public:
  ObCompactionSuggestionIterator() :
    tenant_id_(OB_INVALID_TENANT_ID),
    cur_idx_(0),
    is_opened_(false)
  {
  }
  virtual ~ObCompactionSuggestionIterator() { reset(); }
  int open(const int64_t tenant_id);
  int get_next_info(ObCompactionSuggestion &info);
  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    cur_idx_ = 0;
    is_opened_ = false;
  }
private:
  int64_t tenant_id_;
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
