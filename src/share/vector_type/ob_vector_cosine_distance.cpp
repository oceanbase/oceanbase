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

#include "ob_vector_cosine_distance.h"
namespace oceanbase
{
namespace common
{
int ObVectorCosineDistance::cosine_similarity_func(const float *a, const float *b, const int64_t len, double &similarity)
{
  return cosine_similarity_normal(a, b, len, similarity);
}

int ObVectorCosineDistance::cosine_distance_func(const float *a, const float *b, const int64_t len, double &distance) {
  int ret = OB_SUCCESS;
  double similarity = 0;
  if (OB_FAIL(cosine_similarity_func(a, b, len, similarity))) {
    if (OB_ERR_NULL_VALUE != ret) {
      LIB_LOG(WARN, "failed to cal cosine similaity", K(ret));
    }
  } else {
    distance = get_cosine_distance(similarity);
  }
  return ret;
}

OB_INLINE double ObVectorCosineDistance::get_cosine_distance(double similarity)
{
  if (similarity > 1.0) {
    similarity = 1.0;
  } else if (similarity < -1.0) {
    similarity = -1.0;
  }
  return 1.0 - similarity;
}

OB_INLINE int ObVectorCosineDistance::cosine_calculate_normal(const float *a, const float *b, const int64_t len, double &ip, double &abs_dist_a, double &abs_dist_b)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
    ip += a[i] * b[i];
    abs_dist_a += a[i] * a[i];
    abs_dist_b += b[i] * b[i];
    if (OB_UNLIKELY(0 != ::isinf(ip) || 0 != ::isinf(abs_dist_a) || 0 != ::isinf(abs_dist_b))) {
      ret = OB_NUMERIC_OVERFLOW;
      LIB_LOG(WARN, "value is overflow", K(ret), K(ip), K(abs_dist_a), K(abs_dist_b));
    }
  }
  return ret;
}

OB_INLINE int ObVectorCosineDistance::cosine_similarity_normal(const float *a, const float *b, const int64_t len, double &similarity)
{
  int ret = OB_SUCCESS;
  double ip = 0;
  double abs_dist_a = 0;
  double abs_dist_b = 0;
  similarity = 0;
  if (OB_FAIL(cosine_calculate_normal(a, b, len, ip, abs_dist_a, abs_dist_b))) {
    LIB_LOG(WARN, "failed to cal cosine", K(ret), K(ip));
  } else if (0 == abs_dist_a || 0 == abs_dist_b) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    similarity = ip / (sqrt(abs_dist_a * abs_dist_b));
  }
  return ret;
}
}
}