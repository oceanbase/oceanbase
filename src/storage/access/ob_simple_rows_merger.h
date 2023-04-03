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

#ifndef OCEANBASE_STORAGE_OB_MULTIPLE_ROW_MERGER_
#define OCEANBASE_STORAGE_OB_MULTIPLE_ROW_MERGER_
#include "ob_scan_merge_loser_tree.h"

namespace oceanbase
{
namespace storage
{

template <typename T, typename Comparator>
class ObSimpleRowsMerger : public common::ObRowsMerger<T, Comparator>
{
public:
  static const int64_t USE_SIMPLE_MERGER_MAX_TABLE_CNT = 3;
  virtual ObRowMergerType type() override { return ObRowMergerType::SIMPLE_MERGER; }
  ObSimpleRowsMerger(Comparator& cmp);
  virtual ~ObSimpleRowsMerger();
  virtual int init(const int64_t table_cnt, common::ObIAllocator &allocator) override;
  virtual bool is_inited() const override { return is_inited_; }
  virtual int open(const int64_t table_cnt) override;
  virtual void reset() override;
  virtual void reuse() override;
  virtual int top(const T *&item) override;
  virtual int pop() override;
  virtual int push(const T &item) override;
  virtual int push_top(const T &item) override;
  virtual int rebuild() override;
  virtual int count() const override;
  virtual bool empty() const override;
  virtual bool is_unique_champion() const override;
  TO_STRING_KV(K_(is_inited), K_(table_cnt), K_(item_cnt))

private:
  bool is_inited_;
  int64_t table_cnt_;
  int64_t item_cnt_;
  Comparator& cmp_;
  T *items_;
  common::ObIAllocator *allocator_;
};
typedef ObSimpleRowsMerger<ObScanMergeLoserTreeItem, ObScanMergeLoserTreeCmp> ObScanSimpleMerger;

template <typename T, typename Comparator>
ObSimpleRowsMerger<T, Comparator>::ObSimpleRowsMerger(Comparator& cmp)
  : is_inited_(false),
    table_cnt_(0),
    item_cnt_(0),
    cmp_(cmp),
    items_(nullptr),
    allocator_(nullptr)
{}

template <typename T, typename Comparator>
ObSimpleRowsMerger<T, Comparator>::~ObSimpleRowsMerger()
{
  reset();
}

template <typename T, typename Comparator>
void ObSimpleRowsMerger<T, Comparator>::reset()
{
  is_inited_ = false;
  table_cnt_ = 0;
  item_cnt_ = 0;
  if (nullptr != allocator_ && nullptr != items_) {
    allocator_->free(items_);
  }
  items_ = nullptr;
  allocator_ = nullptr;
}

template <typename T, typename Comparator>
void ObSimpleRowsMerger<T, Comparator>::reuse()
{
  table_cnt_ = 0;
  item_cnt_ = 0;
}

template <typename T, typename Comparator>
int ObSimpleRowsMerger<T, Comparator>::init(const int64_t table_cnt, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "The merge has been inited", K(ret));
  } else if (table_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid table cnt", K(ret), K(table_cnt));
  } else {
    table_cnt_ = table_cnt;
    allocator_ = &allocator;
    if (nullptr == (items_ = static_cast<T*>(allocator_->alloc(sizeof(T) * USE_SIMPLE_MERGER_MAX_TABLE_CNT)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to allocate items", K(ret), K(table_cnt_));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

template <typename T, typename Comparator>
int ObSimpleRowsMerger<T, Comparator>::open(const int64_t table_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(ret));
  } else if (table_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid table cnt", K(ret), K(table_cnt));
  } else {
    table_cnt_ = table_cnt;
  }
  return ret;
}

template <typename T, typename Comparator>
bool ObSimpleRowsMerger<T, Comparator>::empty() const
{
  return item_cnt_ <= 0;
}

template <typename T, typename Comparator>
int ObSimpleRowsMerger<T, Comparator>::count() const
{
  return item_cnt_;
}

template <typename T, typename Comparator>
bool ObSimpleRowsMerger<T, Comparator>::is_unique_champion() const
{
  bool bret = true;
  if (!empty()) {
    bret = !items_[0].equal_with_next_;
  } else {
    bret = true;
  }
  return bret;
}

template <typename T, typename Comparator>
int ObSimpleRowsMerger<T, Comparator>::push(const T &item)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "The merger is not init", K(ret));
  } else if (item_cnt_ + 1 > table_cnt_) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "Unexpected state", K(ret), K(item_cnt_), K(table_cnt_));
  } else {
    if (0 == item_cnt_) {
      items_[0] = item;
    } else {
      int64_t cmp_ret = 0;
      bool equal_with_next = false;
      int64_t i = item_cnt_ - 1;
      for ( ; OB_SUCC(ret) && i >= 0; --i) {
        if (OB_FAIL(cmp_.cmp(item, items_[i], cmp_ret))) {
          STORAGE_LOG(WARN, "Fail to compare item", K(ret), K(i), K(items_[i]), K(item));
          break;
        } else if (cmp_ret < 0) {
          items_[i+1] = items_[i];
        } else if (0 == cmp_ret) {
          // item's rowkey to be pushed is equal with current item
          if (item.iter_idx_ < items_[i].iter_idx_) {
            items_[i+1] = items_[i];
            // the iter_idx_ is smaller, so put the item before current item
            equal_with_next = true;
          } else {
            // the iter_idx_ is bigger, so put the item behind current item
            items_[i].equal_with_next_ = true;
            break;
          }
        } else {
          // the item to be pushed will be inserted behind current item
          break;
        }
      }
      if (OB_SUCC(ret)) {
        items_[i+1] = item;
        if (equal_with_next) {		
          // item to be pushed is equal with next item		
          items_[i+1].equal_with_next_ = true;		
        }
      }
    }
  }
  
  if (OB_SUCC(ret)) {
    ++item_cnt_;
  }
  return ret;
}

template <typename T, typename Comparator>
int ObSimpleRowsMerger<T, Comparator>::push_top(const T &item)
{
  return push(item);
}

template <typename T, typename Comparator>
int ObSimpleRowsMerger<T, Comparator>::rebuild()
{
  int ret = OB_SUCCESS;
  return ret;
}

template <typename T, typename Comparator>
int ObSimpleRowsMerger<T, Comparator>::top(const T *&item)
{
  int ret = OB_SUCCESS;
  if (empty()) {
    ret = OB_EMPTY_RESULT;
    STORAGE_LOG(WARN, "The merger is empty", K(ret), K(*this));
  } else {
    item = &items_[0];
  }
  return ret;
}

template <typename T, typename Comparator>
int ObSimpleRowsMerger<T, Comparator>::pop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "The merger is not init", K(ret));
  } else if (empty()) {
    ret = OB_EMPTY_RESULT;
    STORAGE_LOG(WARN, "The merger is empty", K(ret), K(*this));
  } else {
    for (int64_t i = 0; i < item_cnt_ - 1; ++i) {
      items_[i] = items_[i+1];
    }
    --item_cnt_;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MULTIPLE_ROW_MERGER_
