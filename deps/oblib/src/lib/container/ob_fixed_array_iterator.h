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

#ifndef OB_FIXED_ARRAY_ITERATOR_H_
#define OB_FIXED_ARRAY_ITERATOR_H_

#include "ob_array_iterator.h"

namespace oceanbase
{

namespace common
{
template<typename T, typename AllocatorT>
typename ObFixedArrayImpl<T, AllocatorT>::const_iterator ObFixedArrayImpl<T, AllocatorT>::begin() const
{
  return const_iterator(data_);
}

template<typename T, typename AllocatorT>
typename ObFixedArrayImpl<T, AllocatorT>::const_iterator ObFixedArrayImpl<T, AllocatorT>::end() const
{
  return const_iterator(data_ + count_);
}

template<typename T, typename AllocatorT>
typename ObFixedArrayImpl<T, AllocatorT>::iterator ObFixedArrayImpl<T, AllocatorT>::begin()
{
  return iterator(data_);
}

template<typename T, typename AllocatorT>
typename ObFixedArrayImpl<T, AllocatorT>::iterator ObFixedArrayImpl<T, AllocatorT>::end()
{
  return iterator(data_ + count_);
}
}  // end namespace common
}  // end namespace oceanbase

#endif  // OB_FIXED_ARRAY_ITERATOR_H_
