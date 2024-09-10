/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_vector_l2_distance.h"
namespace oceanbase
{
namespace common
{
int ObVectorL2Distance::l2_square_func(const float *a, const float *b, const int64_t len, double &square)
{
  return l2_square_normal(a, b, len, square);
}

int ObVectorL2Distance::l2_distance_func(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  double square = 0;
  distance = 0;
  if (OB_FAIL(l2_square_func(a, b, len, square))) {
    LIB_LOG(WARN, "failed to cal l2 square", K(ret));
  } else {
    distance = sqrt(square);
  }
  return ret;
}

OB_INLINE int ObVectorL2Distance::l2_square_normal(const float *a, const float *b, const int64_t len, double &square)
{
  int ret = OB_SUCCESS;
  double sum = 0;
  double diff = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
    diff = a[i] - b[i];
    sum += (diff * diff);
    if (OB_UNLIKELY(0 != ::isinf(sum))) {
      ret = OB_NUMERIC_OVERFLOW;
      LIB_LOG(WARN, "value is overflow", K(ret), K(diff), K(sum));
    }
  }
  if (OB_SUCC(ret)) {
    square = sum;
  }
  return ret;
}
}
}