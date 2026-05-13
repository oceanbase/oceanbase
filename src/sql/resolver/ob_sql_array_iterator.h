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

#ifndef OCEANBASE_SQL_OB_SQL_ARRAY_ITERATOR_H_
#define OCEANBASE_SQL_OB_SQL_ARRAY_ITERATOR_H_ 1
#include "sql/resolver/ob_sql_array.h"
namespace oceanbase
{
namespace sql
{
template <typename T, bool construct_with_allocator>
typename ObSqlArrayImpl<T, construct_with_allocator>::iterator ObSqlArrayImpl<T, construct_with_allocator>::begin()
{
  return iterator(this, 0);
}

template <typename T, bool construct_with_allocator>
typename ObSqlArrayImpl<T, construct_with_allocator>::iterator ObSqlArrayImpl<T, construct_with_allocator>::end()
{
  return iterator(this, count_);
}

template <typename T, bool construct_with_allocator>
class ObSqlArrayIterator
{
  friend class ObSqlArrayImpl<T, construct_with_allocator>;
public:
  typedef T value_type;
  typedef int64_t difference_type;
  typedef T *pointer;
  typedef T &reference;
  typedef std::random_access_iterator_tag iterator_category;

public:
  ObSqlArrayIterator() : arr_(NULL), index_(0) {}
  explicit ObSqlArrayIterator(ObSqlArrayImpl<T, construct_with_allocator> *arr, int64_t index)
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
  inline ObSqlArrayIterator operator++(int)// ObSqlArrayIterator++
  {
    OB_ASSERT(arr_ != NULL);
    return ObSqlArrayIterator(arr_, index_++);
  }
  inline ObSqlArrayIterator operator++()
  {
    OB_ASSERT(arr_ != NULL);
    index_++;
    return *this;
  }
  inline ObSqlArrayIterator operator--(int)
  {
    OB_ASSERT(arr_ != NULL);
    return ObSqlArrayIterator(arr_, index_--);
  }
  inline ObSqlArrayIterator operator--()
  {
    OB_ASSERT(arr_ != NULL);
    index_--;
    return *this;
  }
  inline ObSqlArrayIterator operator+(int64_t off)
  {
    OB_ASSERT(arr_ != NULL);
    return ObSqlArrayIterator(arr_, index_ + off);
  }
  inline ObSqlArrayIterator &operator+=(int64_t off)
  {
    OB_ASSERT(arr_ != NULL);
    index_ += off;
    return *this;
  }
  inline difference_type operator-(const ObSqlArrayIterator &rhs)
  {
    OB_ASSERT(arr_ == rhs.arr_);
    return index_ - rhs.index_;
  }
  inline ObSqlArrayIterator operator-(int64_t index)
  {
    OB_ASSERT(arr_ != NULL);
    return ObSqlArrayIterator(arr_, this->index_ - index);
  }
  inline bool operator==(const ObSqlArrayIterator &rhs) const
  {
    OB_ASSERT(arr_ == rhs.arr_);
    return (this->index_ == rhs.index_);
  }
  inline bool operator!=(const ObSqlArrayIterator &rhs) const
  {
    OB_ASSERT(arr_ == rhs.arr_);
    return (this->index_ != rhs.index_);
  }
  inline bool operator<(const ObSqlArrayIterator &rhs) const
  {
    OB_ASSERT(arr_ == rhs.arr_);
    return (index_ < rhs.index_);
  }

  inline bool operator<=(const ObSqlArrayIterator &rhs) const
  {
    OB_ASSERT(arr_ == rhs.arr_);
    return (index_ <= rhs.index_);
  }
private:
  ObSqlArrayImpl<T, construct_with_allocator> *arr_;
  int64_t index_;
};

}
}

#endif /* OCEANBASE_SQL_OB_SQL_ARRAY_ITERATOR_H_ */
