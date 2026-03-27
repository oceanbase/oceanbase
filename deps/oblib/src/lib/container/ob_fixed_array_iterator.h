/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
