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

#ifndef OCEANBASE_SORT_WRAPPER_H_
#define OCEANBASE_SORT_WRAPPER_H_

#include <algorithm>
#include "lib/utility/ob_tracepoint.h"
#include "lib/oblog/ob_log_module.h"
namespace oceanbase
{
namespace lib
{
#ifdef FATAL_ERROR_HANG
template <class Iterator>
class IteratorWrapper
{
public:
  using value_type = typename std::iterator_traits<Iterator>::value_type;
  using difference_type = typename std::iterator_traits<Iterator>::difference_type;
  using pointer = value_type*;
  using reference = value_type&;
  using iterator_category = std::random_access_iterator_tag;
  
  IteratorWrapper(Iterator iter, int64_t index, int64_t end)
    : iter_(iter), index_(index), end_(end)
  {}
  bool abort_maybe_bad_compare()
  {
    abort();
  }
  inline value_type &operator*()
  {
    return *iter_;
  }
  inline value_type *operator->()
  {
    return &(*iter_);
  }
  inline IteratorWrapper operator++(int)
  {
    if (OB_UNLIKELY(index_ >= end_)) abort_maybe_bad_compare();
    return IteratorWrapper(iter_++, index_++, end_);
  }
  inline IteratorWrapper operator++()
  {
    if (OB_UNLIKELY(index_ >= end_)) abort_maybe_bad_compare();
    ++iter_;
    ++index_;
    return *this;
  }
  inline IteratorWrapper operator--(int)
  {
    if (OB_UNLIKELY(index_ < 0)) abort_maybe_bad_compare();
    return IteratorWrapper(iter_--, index_--, end_);
  }
  inline IteratorWrapper operator--()
  {
    if (OB_UNLIKELY(index_ < 0)) abort_maybe_bad_compare();
    --iter_;
    --index_;
    return *this;
  }
  inline IteratorWrapper operator+(int64_t off)
  {
    if (OB_UNLIKELY(index_ >= end_)) abort_maybe_bad_compare();
    return IteratorWrapper(iter_ + off, index_ + off, end_);
  }
  inline IteratorWrapper &operator+=(int64_t off)
  {
    if (OB_UNLIKELY(index_ >= end_)) abort_maybe_bad_compare();
    iter_ += off;
    index_ += off;
    return *this;
  }
  inline IteratorWrapper operator-(int64_t off)
  {
    if (OB_UNLIKELY(index_ < 0)) abort_maybe_bad_compare();
    return IteratorWrapper(iter_ - off, index_ - off, end_);
  }
  inline difference_type operator-(const IteratorWrapper &rhs)
  {
    return index_ - rhs.index_;
  }
  inline bool operator==(const IteratorWrapper &rhs) const
  {
    return (index_ == rhs.index_);
  }
  inline bool operator!=(const IteratorWrapper &rhs) const
  {
    return (index_ != rhs.index_);
  }
  inline bool operator<(const IteratorWrapper &rhs) const
  {
    return (index_ < rhs.index_);
  }

  inline bool operator<=(const IteratorWrapper &rhs) const
  {
    return (index_ <= rhs.index_);
  }
  
private:
  Iterator iter_;
  int64_t index_;
  int64_t end_;
};
#endif
template <class Iterator, class Compare>
void ob_sort(Iterator first, Iterator last, Compare comp)
{
  int ret = OB_E(EventTable::EN_CHECK_SORT_CMP) OB_SUCCESS;
  if (OB_FAIL(ret) && std::is_empty<Compare>::value) {
    ret = OB_SUCCESS;
    for (Iterator iter = first; OB_SUCC(ret) && iter != last; ++iter) {
      if (comp(*iter, *iter)) {
        ret = common::OB_ERR_UNEXPECTED;
        OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED,"check irreflexivity failed");
      }
    }
  }
#ifdef FATAL_ERROR_HANG
  int64_t end = last - first;
  typedef IteratorWrapper<Iterator>  Wrapper;
  Wrapper first_wrapper = Wrapper(first, 0, end);
  Wrapper last_wrapper = Wrapper(last, end, end);
  std::sort(first_wrapper, last_wrapper, comp);
#else
  std::sort(first, last, comp);
#endif
}

template <class Iterator>
void ob_sort(Iterator first, Iterator last)
{
  using ValueType = typename std::iterator_traits<Iterator>::value_type;
  struct Compare
  {
    bool operator()(ValueType& l, ValueType& r)
    {
      return l < r;
    }
  };
  ob_sort(first, last, Compare());
}
} // end of namespace lib
} // end of namespace oceanbase
#endif
