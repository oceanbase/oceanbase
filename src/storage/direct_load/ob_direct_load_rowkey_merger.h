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

#include "storage/direct_load/ob_direct_load_rowkey_iterator.h"

namespace oceanbase
{
namespace storage
{

template <class Rowkey, class Compare>
class ObDirectLoadRowkeyMerger : public ObIDirectLoadRowkeyIterator<Rowkey>
{
  typedef ObIDirectLoadRowkeyIterator<Rowkey> RowkeyIterator;
public:
  ObDirectLoadRowkeyMerger();
  virtual ~ObDirectLoadRowkeyMerger() = default;
  int init(const common::ObIArray<RowkeyIterator *> &iters, Compare *compare);
  int get_next_rowkey(const Rowkey *&rowkey);
private:
  int direct_get_next_rowkey(const Rowkey *&rowkey);
  int build_heap();
  int heap_get_next_rowkey(const Rowkey *&rowkey);
private:
  struct HeapItem
  {
    const Rowkey *item_;
    int64_t idx_;
    HeapItem() : item_(nullptr), idx_(0) {}
    void reset()
    {
      item_ = nullptr;
      idx_ = 0;
    }
    TO_STRING_KV(K_(item), K_(idx));
  };
  struct HeapCompare
  {
    HeapCompare() : compare_(nullptr), error_code_(common::OB_SUCCESS) {}
    bool operator()(const HeapItem &lhs, const HeapItem &rhs);
    int get_error_code() const { return error_code_; }
    Compare *compare_;
    int error_code_;
  };
private:
  HeapCompare compare_;
  common::ObBinaryHeap<HeapItem, HeapCompare, 64> heap_;
  const common::ObIArray<RowkeyIterator *> *iters_;
  int64_t last_iter_idx_;
  bool is_inited_;
};

template <class Rowkey, class Compare>
bool ObDirectLoadRowkeyMerger<Rowkey, Compare>::HeapCompare::operator()(const HeapItem &lhs,
                                                                        const HeapItem &rhs)
{
  int ret = common::OB_SUCCESS;
  bool bret = false;
  if (OB_ISNULL(compare_)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), KP(compare_));
  } else if (OB_ISNULL(lhs.item_) || OB_ISNULL(rhs.item_)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid compare items", KR(ret), KP(lhs.item_), KP(rhs.item_));
  } else {
    bret = !compare_->operator()(lhs.item_, rhs.item_);
  }
  if (OB_FAIL(ret)) {
    error_code_ = ret;
  } else if (OB_FAIL(compare_->get_error_code())) {
    error_code_ = compare_->get_error_code();
  }
  return bret;
}

template <class Rowkey, class Compare>
ObDirectLoadRowkeyMerger<Rowkey, Compare>::ObDirectLoadRowkeyMerger()
  : heap_(compare_), iters_(nullptr), last_iter_idx_(-1), is_inited_(false)
{
}

template <class Rowkey, class Compare>
int ObDirectLoadRowkeyMerger<Rowkey, Compare>::init(const common::ObIArray<RowkeyIterator *> &iters,
                                                    Compare *compare)
{
  int ret = common::OB_SUCCESS;
  if (IS_INIT) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDirectLoadMacroBlockEndRowkeyMerger init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(iters.empty() || OB_ISNULL(compare))) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", KR(ret), K(iters.count()), KP(compare));
  } else {
    compare_.compare_ = compare;
    iters_ = &iters;
    if (iters_->count() > 1 && OB_FAIL(build_heap())) {
      STORAGE_LOG(WARN, "fail to build heap", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

template <class Rowkey, class Compare>
int ObDirectLoadRowkeyMerger<Rowkey, Compare>::get_next_rowkey(const Rowkey *&rowkey)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDirectLoadRowkeyMerger not init", KR(ret), KP(this));
  } else {
    if (1 == iters_->count()) {
      if (OB_FAIL(direct_get_next_rowkey(rowkey))) {
        if (OB_UNLIKELY(common::OB_ITER_END != ret)) {
          STORAGE_LOG(WARN, "fail to direct get next rowkey", KR(ret));
        }
      }
    } else {
      if (OB_FAIL(heap_get_next_rowkey(rowkey))) {
        if (OB_UNLIKELY(common::OB_ITER_END != ret)) {
          STORAGE_LOG(WARN, "fail to heap get next rowkey", KR(ret));
        }
      }
    }
  }
  return ret;
}

template <class Rowkey, class Compare>
int ObDirectLoadRowkeyMerger<Rowkey, Compare>::direct_get_next_rowkey(const Rowkey *&rowkey)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(1 != iters_->count())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(iters_->count()));
  } else {
    ret = iters_->at(0)->get_next_rowkey(rowkey);
  }
  return ret;
}

template <class Rowkey, class Compare>
int ObDirectLoadRowkeyMerger<Rowkey, Compare>::build_heap()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(iters_->count() <= 1)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(iters_->count()));
  } else {
    const Rowkey *rowkey = nullptr;
    HeapItem heap_item;
    for (int64_t i = 0; OB_SUCC(ret) && i < iters_->count(); ++i) {
      if (OB_FAIL(iters_->at(i)->get_next_rowkey(rowkey))) {
        if (OB_UNLIKELY(common::OB_ITER_END != ret)) {
          STORAGE_LOG(WARN, "fail to get next item", KR(ret), K(i));
        } else {
          ret = common::OB_SUCCESS;
        }
      } else if (OB_ISNULL(rowkey)) {
        ret = common::OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid rowkey", KR(ret), KP(rowkey));
      } else {
        heap_item.item_ = rowkey;
        heap_item.idx_ = i;
        if (OB_FAIL(heap_.push(heap_item))) {
          STORAGE_LOG(WARN, "fail to push heap", KR(ret), K(i));
        } else if (OB_FAIL(compare_.get_error_code())) {
          STORAGE_LOG(WARN, "fail to compare items", KR(ret));
        }
      }
    }
  }
  return ret;
}

template <class Rowkey, class Compare>
int ObDirectLoadRowkeyMerger<Rowkey, Compare>::heap_get_next_rowkey(const Rowkey *&rowkey)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(iters_->count() <= 1)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(iters_->count()));
  } else if (last_iter_idx_ >= 0 && last_iter_idx_ < iters_->count()) {
    RowkeyIterator *iter = iters_->at(last_iter_idx_);
    HeapItem heap_item;
    heap_item.idx_ = last_iter_idx_;
    if (OB_FAIL(iter->get_next_rowkey(heap_item.item_))) {
      if (OB_UNLIKELY(common::OB_ITER_END != ret)) {
        STORAGE_LOG(WARN, "fail to get next rowkey", KR(ret));
      } else {
        if (OB_FAIL(heap_.pop())) {
          STORAGE_LOG(WARN, "fail to pop heap item", KR(ret));
        } else if (OB_FAIL(compare_.get_error_code())) {
          STORAGE_LOG(WARN, "fail to compare items", KR(ret));
        } else {
          STORAGE_LOG(DEBUG, "pop a heap item");
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
      } else {
        STORAGE_LOG(DEBUG, "replace heap item", K(*heap_item.item_), K(last_iter_idx_));
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
      rowkey = head_item->item_;
      last_iter_idx_ = head_item->idx_;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
