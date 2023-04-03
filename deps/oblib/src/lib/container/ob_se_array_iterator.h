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

#ifndef _OB_SE_ARRAY_ITERATOR_H
#define _OB_SE_ARRAY_ITERATOR_H 1
#include "lib/container/ob_se_array.h"
namespace oceanbase
{
namespace common
{
template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
typename ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::iterator ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::begin()
{
  return iterator(this, 0);
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
typename ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::iterator ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::end()
{
  return iterator(this, count_);
}

namespace array
{
template <typename T, int64_t LOCAL_ARRAY_SIZE,
          typename BlockAllocatorT,
          bool auto_free>
class ObSEArrayIterator
{
  friend class ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>;
public:
  typedef T value_type;
  typedef int64_t difference_type;
  typedef T *pointer;
  typedef T &reference;
  typedef std::random_access_iterator_tag iterator_category;

public:
  ObSEArrayIterator() : arr_(NULL), index_(0) {}
  explicit ObSEArrayIterator(ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free> *arr, int64_t index)
  {
    arr_ = arr;
    index_ = index;
  }
  inline T &operator*()
  {
    OB_ASSERT(arr_ != NULL);
    return arr_->at(index_);
  }
  inline T *operator->()
  {
    OB_ASSERT(arr_ != NULL);
    return &arr_->at(index_);
  }
  inline ObSEArrayIterator operator++(int)// ObSEArrayIterator++
  {
    OB_ASSERT(arr_ != NULL);
    return ObSEArrayIterator(arr_, index_++);
  }
  inline ObSEArrayIterator operator++()
  {
    OB_ASSERT(arr_ != NULL);
    index_++;
    return *this;
  }
  inline ObSEArrayIterator operator--(int)
  {
    OB_ASSERT(arr_ != NULL);
    return ObSEArrayIterator(arr_, index_--);
  }
  inline ObSEArrayIterator operator--()
  {
    OB_ASSERT(arr_ != NULL);
    index_--;
    return *this;
  }
  inline ObSEArrayIterator operator+(int64_t off)
  {
    OB_ASSERT(arr_ != NULL);
    return ObSEArrayIterator(arr_, index_ + off);
  }
  inline ObSEArrayIterator &operator+=(int64_t off)
  {
    OB_ASSERT(arr_ != NULL);
    index_ += off;
    return *this;
  }
  inline difference_type operator-(const ObSEArrayIterator &rhs)
  {
    OB_ASSERT(arr_ == rhs.arr_);
    return index_ - rhs.index_;
  }
  inline ObSEArrayIterator operator-(int64_t index)
  {
    OB_ASSERT(arr_ != NULL);
    return ObSEArrayIterator(arr_, this->index_ - index);
  }
  inline bool operator==(const ObSEArrayIterator &rhs) const
  {
    OB_ASSERT(arr_ == rhs.arr_);
    return (this->index_ == rhs.index_);
  }
  inline bool operator!=(const ObSEArrayIterator &rhs) const
  {
    OB_ASSERT(arr_ == rhs.arr_);
    return (this->index_ != rhs.index_);
  }
  inline bool operator<(const ObSEArrayIterator &rhs) const
  {
    OB_ASSERT(arr_ == rhs.arr_);
    return (index_ < rhs.index_);
  }

  inline bool operator<=(const ObSEArrayIterator &rhs) const
  {
    OB_ASSERT(arr_ == rhs.arr_);
    return (index_ <= rhs.index_);
  }
private:
  ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free> *arr_;
  int64_t index_;
};

}
}
}

#endif /* _OB_SE_ARRAY_ITERATOR_H */



