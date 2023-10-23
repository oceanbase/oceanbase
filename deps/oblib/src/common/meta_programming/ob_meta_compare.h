/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef DEPS_OBLIB_SRC_COMMON_META_PROGRAMMING_OB_META_COMPARE_H
#define DEPS_OBLIB_SRC_COMMON_META_PROGRAMMING_OB_META_COMPARE_H

#include "lib/ob_define.h"
#include "ob_type_traits.h"

namespace oceanbase
{
namespace common
{
namespace meta
{

template <typename T, typename std::enable_if<OB_TRAIT_IS_ORIGIN_COMPAREABLE(T), bool>::type = true>
int compare(const T &lhs, const T&rhs, int &result)
{
  int ret = OB_SUCCESS;
  result = 0;
  if (lhs < rhs) {
    result = -1;
  } else if (lhs == rhs) {
    result = 0;
  } else {
    result = 1;
  }
  return ret;
}

template <typename T,
          typename std::enable_if<!OB_TRAIT_IS_ORIGIN_COMPAREABLE(T) &&\
                                   OB_TRAIT_IS_METHOD_COMPAREABLE(T), bool>::type = true>
int compare(const T &lhs, const T&rhs, int &result)
{
  return lhs.compare(rhs, result);
}

template <typename T,
          typename std::enable_if<!OB_TRAIT_IS_ORIGIN_COMPAREABLE(T) &&\
                                  !OB_TRAIT_IS_METHOD_COMPAREABLE(T), bool>::type = true>
int compare(const T &lhs, const T&rhs, int &result)
{
  static_assert(!(!OB_TRAIT_IS_ORIGIN_COMPAREABLE(T) &&
                !OB_TRAIT_IS_METHOD_COMPAREABLE(T)),
                "your type NEITHER has opertor< and operator== "
                "NOR has int T::comapre(cosnt T&)");
  return OB_NOT_SUPPORTED;
}

}
}
}
#endif