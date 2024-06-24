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

template <class T, class Compare>
class CompareWrapper
{
public:
  CompareWrapper(Compare& comp)
    : comp_(comp)
  {}
  bool operator()(T &left, T &right)
  {
    int ret = OB_E(EventTable::EN_CHECK_SORT_CMP) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      if (comp_(left, left) || comp_(right, right)) {
        OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED,"check irreflexivity failed");
      }
    }
    return comp_(left, right);
  }
private:
  Compare& comp_;
};

template <class RandomAccessIterator, class Compare>
void ob_sort(RandomAccessIterator first, RandomAccessIterator last, Compare comp)
{
  using ValueType = typename std::iterator_traits<RandomAccessIterator>::value_type;
  CompareWrapper<ValueType, Compare> compare_wrapper(comp);
  std::sort(first, last, compare_wrapper);
}

template <class RandomAccessIterator>
void ob_sort(RandomAccessIterator first, RandomAccessIterator last)
{
  using ValueType = typename std::iterator_traits<RandomAccessIterator>::value_type;
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
