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

#ifndef OB_TEMPLATE_HELPER_H
#define OB_TEMPLATE_HELPER_H

#include <stddef.h>


template< size_t N >
constexpr size_t str_length( char const (&)[N] )
{
  return N-1;
}

template <class T, size_t N>
constexpr size_t array_elements(T (&)[N]) noexcept {
  return N;
}

/**
  Casts from one pointer type, to another, without using
  reinterpret_cast or C-style cast:
    foo *f; bar *b= pointer_cast<bar*>(f);
  This avoids having to do:
    foo *f; bar *b= static_cast<bar*>(static_cast<void*>(f));
 */
template <typename T>
inline T pointer_cast(void *p) {
  return static_cast<T>(p);
}

template <typename T>
inline const T pointer_cast(const void *p) {
  return static_cast<T>(p);
}

#endif // OB_TEMPLATE_HELPER_H
