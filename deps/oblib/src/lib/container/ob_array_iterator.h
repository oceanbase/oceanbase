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

#ifndef _OB_ARRAY_ITERATOR_H
#define _OB_ARRAY_ITERATOR_H 1

#include "lib/container/ob_array.h"

namespace oceanbase
{

namespace common
{

template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
typename ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::const_iterator ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::begin() const
{
  return const_iterator(data_);
}
template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
typename ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::const_iterator ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::end() const
{
  return const_iterator(data_ + count_);
}
template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
typename ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::iterator ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::begin()
{
  return iterator(data_);
}
template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
typename ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::iterator ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::end()
{
  return iterator(data_ + count_);
}


template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
class ObArray;
////////////////////////////////////////////////////////////////
namespace array
{
template <class ObArray, class T>
class Iterator
{
  typedef Iterator<ObArray, T> self_t;
public:
  typedef typename std::random_access_iterator_tag iterator_category;
  typedef int64_t difference_type;
  typedef T value_type;
  typedef T *value_ptr_t;
  typedef T *pointer;
  typedef T &reference;
public:
  Iterator() : value_ptr_(NULL)
  {
  };
  Iterator(const self_t &other)
  {
    *this = other;
  };
  self_t &operator =(const self_t &other)
  {
    value_ptr_ = other.value_ptr_;
    return *this;
  };
  explicit Iterator(value_ptr_t value_ptr)
  {
    value_ptr_ = value_ptr;
  };
public:
  reference operator *() const
  {
    return *value_ptr_;
  };
  value_ptr_t operator ->() const
  {
    return value_ptr_;
  };
  operator value_ptr_t() const
  {
    return value_ptr_;
  }
  bool operator ==(const self_t &other) const
  {
    return (value_ptr_ == (other.value_ptr_));
  };
  bool operator !=(const self_t &other) const
  {
    return (value_ptr_ != (other.value_ptr_));
  };
  bool operator <(const self_t &other) const
  {
    return (value_ptr_ < (other.value_ptr_));
  };
  difference_type operator- (const self_t &rhs)
  {
    return value_ptr_ - rhs.value_ptr_;
  };
  self_t operator-(difference_type step)
  {
    return self_t(value_ptr_ - step);
  };
  self_t operator+(difference_type step)
  {
    return self_t(value_ptr_ + step);
  };
  self_t &operator+=(difference_type step)
  {
    value_ptr_ += step;
    return *this;
  };
  self_t &operator-=(difference_type step)
  {
    value_ptr_ -= step;
    return *this;
  };
  self_t &operator ++()
  {
    value_ptr_++;
    return *this;
  };
  self_t operator ++(int)
  {
    self_t tmp = *this;
    value_ptr_++;
    return tmp;
  };
  self_t &operator --()
  {
    value_ptr_--;
    return *this;
  };
  self_t operator --(int)
  {
    self_t tmp = *this;
    value_ptr_--;
    return tmp;
  };
private:
  value_ptr_t value_ptr_;
};

template <class ObArray, class T>
Iterator<ObArray, T> operator+(
  typename Iterator<ObArray, T>::difference_type diff,
  const Iterator<ObArray, T>& iter)
{
  Iterator<ObArray, T> iter2 = iter;
  iter2 += diff;
  return iter2;
}
}


}
}

#endif /* _OB_ARRAY_ITERATOR_H */


