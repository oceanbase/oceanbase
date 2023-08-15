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
#pragma once

#include "lib/container/ob_heap.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "share/ob_errno.h"
#include "storage/direct_load/ob_direct_load_external_interface.h"

namespace oceanbase
{
namespace storage
{

template <typename T, typename Compare>
class ObDirectLoadExternalMerger
{
  static const int64_t DEFAULT_ITERATOR_NUM = 64;
  typedef ObDirectLoadExternalIterator<T> ExternalIterator;
public:
  ObDirectLoadExternalMerger();
  virtual ~ObDirectLoadExternalMerger() = default;
  int init(const common::ObIArray<ExternalIterator *> &iters, Compare *compare);
  int get_next_item(const T *&item);
  void reset();
  bool is_inited() const { return is_inited_; }
private:
  int direct_get_next_item(const T *&item);
  int heap_get_next_item(const T *&item);
  int build_heap();
private:
  struct HeapItem
  {
    const T *item_;
    int64_t idx_;
    HeapItem() : item_(nullptr), idx_(0) {}
    void reset()
    {
      item_ = nullptr;
      idx_ = 0;
    }
    TO_STRING_KV(K_(item), K_(idx));
  };
  class HeapCompare
  {
  public:
    explicit HeapCompare() : compare_(nullptr), error_code_(common::OB_SUCCESS) {}
    bool operator()(const HeapItem &left_item, const HeapItem &right_item);
    void set_compare(Compare *compare) { compare_ = compare; }
    int get_error_code() const { return error_code_; }
    void reset()
    {
      compare_ = nullptr;
      error_code_ = common::OB_SUCCESS;
    }
  private:
    Compare *compare_;
    int error_code_;
  };
private:
  HeapCompare compare_;
  const common::ObIArray<ExternalIterator *> *iters_;
  common::ObBinaryHeap<HeapItem, HeapCompare, DEFAULT_ITERATOR_NUM> heap_;
  int64_t last_iter_idx_;
  bool is_inited_;
};

template <typename T, typename Compare>
bool ObDirectLoadExternalMerger<T, Compare>::HeapCompare::operator()(const HeapItem &left_item,
                                                                     const HeapItem &right_item)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, external_heap_compare_time_us);
  int ret = common::OB_SUCCESS;
  bool bret = false;
  if (OB_ISNULL(compare_)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), KP(compare_));
  } else if (OB_ISNULL(left_item.item_) || OB_ISNULL(right_item.item_)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid compare items", KR(ret), KP(left_item.item_), KP(right_item.item_));
  } else {
    bret = !compare_->operator()(left_item.item_, right_item.item_);
  }
  if (OB_FAIL(ret)) {
    error_code_ = ret;
  } else if (OB_FAIL(compare_->get_error_code())) {
    error_code_ = compare_->get_error_code();
  }
  return bret;
}

template <typename T, typename Compare>
ObDirectLoadExternalMerger<T, Compare>::ObDirectLoadExternalMerger()
  : iters_(nullptr), heap_(compare_), last_iter_idx_(-1), is_inited_(false)
{
}

template <typename T, typename Compare>
void ObDirectLoadExternalMerger<T, Compare>::reset()
{
  is_inited_ = false;
  last_iter_idx_ = -1;
  compare_.reset();
  iters_ = nullptr;
  heap_.reset();
}

template <typename T, typename Compare>
int ObDirectLoadExternalMerger<T, Compare>::init(const common::ObIArray<ExternalIterator *> &iters,
                                                 Compare *compare)
{
  int ret = common::OB_SUCCESS;
  if (IS_INIT) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDirectLoadExternalMerger init twice", KR(ret), KP(this));
  } else if (0 == iters.count() || OB_ISNULL(compare)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(iters.count()), KP(compare));
  } else {
    compare_.set_compare(compare);
    iters_ = &iters;
    if (iters.count() > 1 && OB_FAIL(build_heap())) {
      STORAGE_LOG(WARN, "fail to build heap", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObDirectLoadExternalMerger<T, Compare>::build_heap()
{
  int ret = common::OB_SUCCESS;
  const T *item = nullptr;
  HeapItem heap_item;
  for (int64_t i = 0; OB_SUCC(ret) && i < iters_->count(); ++i) {
    if (OB_FAIL(iters_->at(i)->get_next_item(item))) {
      if (OB_UNLIKELY(common::OB_ITER_END != ret)) {
        STORAGE_LOG(WARN, "fail to get next item", KR(ret), K(i));
      } else {
        ret = common::OB_SUCCESS;
      }
    } else if (OB_ISNULL(item)) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid item", KR(ret), KP(item));
    } else {
      heap_item.item_ = item;
      heap_item.idx_ = i;
      if (OB_FAIL(heap_.push(heap_item))) {
        STORAGE_LOG(WARN, "fail to push heap", KR(ret));
      } else if (OB_FAIL(compare_.get_error_code())) {
        STORAGE_LOG(WARN, "fail to compare items", KR(ret));
      }
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObDirectLoadExternalMerger<T, Compare>::get_next_item(const T *&item)
{
  int ret = common::OB_SUCCESS;
  item = nullptr;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDirectLoadExternalMerger not init", KR(ret), KP(this));
  } else if (1 == iters_->count()) {
    if (OB_FAIL(direct_get_next_item(item))) {
      if (OB_UNLIKELY(common::OB_ITER_END != ret)) {
        STORAGE_LOG(WARN, "fail to directly get next item", KR(ret));
      }
    }
  } else if (OB_FAIL(heap_get_next_item(item))) {
    if (OB_UNLIKELY(common::OB_ITER_END != ret)) {
      STORAGE_LOG(WARN, "fail to get next item from heap", KR(ret));
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObDirectLoadExternalMerger<T, Compare>::direct_get_next_item(const T *&item)
{
  int ret = common::OB_SUCCESS;
  item = nullptr;
  if (OB_UNLIKELY(1 != iters_->count())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(iters_->count()));
  } else if (OB_FAIL(iters_->at(0)->get_next_item(item))) {
    if (OB_UNLIKELY(common::OB_ITER_END != ret)) {
      STORAGE_LOG(WARN, "fail to get next item", KR(ret));
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObDirectLoadExternalMerger<T, Compare>::heap_get_next_item(const T *&item)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(iters_->count() <= 1)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(iters_->count()));
  } else if (last_iter_idx_ >= 0 && last_iter_idx_ < iters_->count()) {
    ExternalIterator *iter = iters_->at(last_iter_idx_);
    HeapItem heap_item;
    heap_item.idx_ = last_iter_idx_;
    if (OB_FAIL(iter->get_next_item(heap_item.item_))) {
      if (OB_UNLIKELY(common::OB_ITER_END != ret)) {
        STORAGE_LOG(WARN, "fail to get next item", KR(ret));
      } else {
        if (OB_FAIL(heap_.pop())) {
          STORAGE_LOG(WARN, "fail to pop heap item", KR(ret));
        } else if (OB_FAIL(compare_.get_error_code())) {
          STORAGE_LOG(WARN, "fail to compare items", KR(ret));
        }
      }
    } else if (OB_ISNULL(heap_item.item_)) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid item", KR(ret), KP(heap_item.item_));
    } else {
      if (OB_FAIL(heap_.replace_top(heap_item))) {
        STORAGE_LOG(WARN, "fail to replace heap top", KR(ret));
      } else if (OB_FAIL(compare_.get_error_code())) {
        STORAGE_LOG(WARN, "fail to compare items", KR(ret));
      }
    }
    last_iter_idx_ = -1;
  }

  if (OB_SUCC(ret) && heap_.empty()) {
    ret = common::OB_ITER_END;
  }

  if (OB_SUCC(ret)) {
    const HeapItem *head_item = nullptr;
    if (OB_FAIL(heap_.top(head_item))) {
      STORAGE_LOG(WARN, "fail to get heap top item", KR(ret));
    } else if (OB_FAIL(compare_.get_error_code())) {
      STORAGE_LOG(WARN, "fail to compare items", KR(ret));
    } else if (OB_ISNULL(head_item) || OB_ISNULL(head_item->item_)) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid heap item", KR(ret), KP(head_item));
    } else {
      item = head_item->item_;
      last_iter_idx_ = head_item->idx_;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
