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

#include "ob_vector_norm.h"
namespace oceanbase
{
namespace common
{
int ObVectorNorm::vector_norm_square_func(const float *a, const int64_t len, double &norm_square)
{
  return vector_norm_square_normal(a, len, norm_square);
}

int ObVectorNorm::vector_norm_func(const float *a, const int64_t len, double &norm)
{
  int ret = OB_SUCCESS;
  double norm_square = 0;
  norm = 0;
  if (OB_FAIL(vector_norm_square_func(a, len, norm_square))) {
    LIB_LOG(WARN, "failed to cal l2 square", K(ret));
  } else {
    norm = sqrt(norm_square);
  }
  return ret;
}

OB_INLINE int ObVectorNorm::vector_norm_square_normal(const float *a, const int64_t len, double &norm_square)
{
  int ret = OB_SUCCESS;
  double sum = 0;
  double diff = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
    sum += (a[i] * a[i]);
    if (OB_UNLIKELY(0 != ::isinf(sum))) {
      ret = OB_NUMERIC_OVERFLOW;
      LIB_LOG(WARN, "value is overflow", K(ret), K(diff), K(sum));
    }
  }
  if (OB_SUCC(ret)) {
    norm_square = sum;
  }
  return ret;
}
}
}
