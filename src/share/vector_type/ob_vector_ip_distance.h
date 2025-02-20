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

#ifndef OCEANBASE_LIB_OB_VECTOR_IP_DISTANCE_H_
#define OCEANBASE_LIB_OB_VECTOR_IP_DISTANCE_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"
#include "common/object/ob_obj_compare.h"

namespace oceanbase
{
namespace common
{
template <typename T>
struct ObVectorIpDistance
{
  static int ip_distance_func(const T *a, const T *b, const int64_t len, double &distance);

  // normal func
  OB_INLINE static int ip_distance_normal(const T *a, const T *b, const int64_t len, double &distance);
  // TODO(@jingshui) add simd func
};

template <typename T>
int ObVectorIpDistance<T>::ip_distance_func(const T *a, const T *b, const int64_t len, double &distance)
{
return ip_distance_normal(a, b, len, distance);
}

template <typename T>
OB_INLINE int ObVectorIpDistance<T>::ip_distance_normal(const T *a, const T *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
    distance += a[i] * b[i];
    if (OB_UNLIKELY(0 != ::isinf(distance))) {
      ret = OB_NUMERIC_OVERFLOW;
      LIB_LOG(WARN, "value is overflow", K(ret), K(distance));
    }
  }
  return ret;
}

} // common
} // oceanbase
#endif
