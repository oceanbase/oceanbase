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
#include "ob_vector_op_common.h"

namespace oceanbase
{
namespace common
{
template <typename T>
struct ObVectorIpDistance
{
  static int ip_distance_func(const T *a, const T *b, const int64_t len, double &distance);
};

template<>
int ObVectorIpDistance<float>::ip_distance_func(const float *a, const float *b, const int64_t len, double &similarity);
template <>
int ObVectorIpDistance<uint8_t>::ip_distance_func(const uint8_t *a, const uint8_t *b, const int64_t len, double &similarity);

OB_INLINE int ip_distance_normal(const float *a, const float *b, const int64_t len, double &distance)
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

#if defined(__SSE2__) || defined(__AVX__) || defined(__AVX2__) || defined(__AVX512F__) || defined(__AVX512BW__)
// SSE
OB_INLINE static int ip_distance_simd4_avx128(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  const int64_t batch = 128 / (sizeof(float) * 8); // 4
  int64_t dim = len >> 2 << 2;
  double tmp_distance = 0;

  __m128 vsum = _mm_set1_ps(0);
  for (int64_t i = 0; i < dim; i += batch) {
    vsum = _mm_add_ps(vsum, _mm_mul_ps(_mm_loadu_ps(a + i), _mm_loadu_ps(b + i)));
  }

  float sum[4] CACHE_ALIGNED;
  _mm_store_ps(sum, vsum);

  tmp_distance = sum[0] + sum[1] + sum[2] + sum[3];
  distance += tmp_distance;

  return ret;
}

OB_INLINE static int ip_distance_simd4_avx128_extra(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  int64_t dim = len >> 2 << 2;
  double extra_distance = 0;
  if (OB_FAIL(ip_distance_simd4_avx128(a, b, dim, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(len), K(dim), K(distance));
  } else if (0 < len - dim && OB_FAIL(ip_distance_normal(a + dim, b + dim, len - dim, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(len), K(dim), K(distance));
  }
  return ret;
}

OB_INLINE static int ip_distance_avx128(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  distance = 0;
  if (4 < len) {
    if (OB_FAIL(ip_distance_simd4_avx128_extra(a, b, len, distance))) {
      LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(distance));
    }
  } else if (OB_FAIL(ip_distance_normal(a, b, len, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret));
  }
  return ret;
}
#endif

#if defined(__AVX__) || defined(__AVX2__) || defined(__AVX512F__) || defined(__AVX512BW__)
// AVX2
OB_INLINE static int ip_distance_simd8_avx256(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  const int64_t batch = 256 / (sizeof(float) * 8); // 8
  int64_t dim = len >> 3 << 3;
  double tmp_distance = 0;

  __m256 vsum = _mm256_set1_ps(0);
  for (int64_t i = 0; i < dim; i += batch) {
    vsum = _mm256_add_ps(vsum, _mm256_mul_ps(_mm256_loadu_ps(a + i), _mm256_loadu_ps(b + i)));
  }
  float sum[8] CACHE_ALIGNED;
  _mm256_store_ps(sum, vsum);

  tmp_distance = sum[0] + sum[1] + sum[2] + sum[3]
      + sum[4] + sum[5] + sum[6] + sum[7];
  distance += tmp_distance;

  return ret;
}

OB_INLINE static int ip_distance_simd8_avx256_extra(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  int64_t dim = len >> 3 << 3;
  if (OB_FAIL(ip_distance_simd8_avx256(a, b, dim, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(len), K(dim), K(distance));
  } else if (0 < len - dim && OB_FAIL(ip_distance_simd4_avx128_extra(a + dim, b + dim, len - dim, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(len), K(dim), K(distance));
  }
  return ret;
}

OB_INLINE static int ip_distance_avx256(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  distance = 0;
  if (8 < len) {
    if (OB_FAIL(ip_distance_simd8_avx256_extra(a, b, len, distance))) {
      LIB_LOG(WARN, "failed to cal ip distance", K(ret));
    }
  } else if (4 < len) {
    if (OB_FAIL(ip_distance_simd4_avx128_extra(a, b, len, distance))) {
      LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(distance));
    }
  } else if (OB_FAIL(ip_distance_normal(a, b, len, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret));
  }
  return ret;
}
#endif

#if defined(__AVX512F__) || defined(__AVX512BW__)
// AVX512
OB_INLINE static int ip_distance_simd16_avx512(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  const int64_t batch = 512 / (sizeof(float) * 8); // 16
  int64_t dim = len >> 4 << 4;
  double tmp_distance = 0;

  __m512 vsum = _mm512_set1_ps(0);
  for (int64_t i = 0; i < dim; i += batch) {
    vsum = _mm512_add_ps(vsum, _mm512_mul_ps(_mm512_loadu_ps(a + i), _mm512_loadu_ps(b + i)));
  }
  float sum[16] CACHE_ALIGNED;
  _mm512_store_ps(sum, vsum);

  tmp_distance = sum[0] + sum[1] + sum[2] + sum[3] + sum[4] + sum[5] + sum[6] + sum[7]
      + sum[8] + sum[9] + sum[10] + sum[11] + sum[12] + sum[13] + sum[14] + sum[15];
  distance += tmp_distance;

  return ret;
}

OB_INLINE static int ip_distance_simd16_avx512_extra(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  int64_t dim = len >> 4 << 4;
  if (OB_FAIL(ip_distance_simd16_avx512(a, b, dim, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(len), K(dim), K(distance));
  } else if (0 < len - dim && OB_FAIL(ip_distance_simd8_avx256_extra(a + dim, b + dim, len - dim, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(len), K(dim), K(distance));
  }
  return ret;
}

OB_INLINE static int ip_distance_avx512(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  distance = 0;
  if (16 < len) {
    if (OB_FAIL(ip_distance_simd16_avx512_extra(a, b, len, distance))) {
      LIB_LOG(WARN, "failed to cal ip distance", K(ret));
    }
  } else if (8 < len) {
    if (OB_FAIL(ip_distance_simd8_avx256_extra(a, b, len, distance))) {
      LIB_LOG(WARN, "failed to cal ip distance", K(ret));
    }
  } else if (4 < len) {
    if (OB_FAIL(ip_distance_simd4_avx128_extra(a, b, len, distance))) {
      LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(distance));
    }
  } else if (OB_FAIL(ip_distance_normal(a, b, len, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret));
  }
  return ret;
}
#endif

OB_DECLARE_DEFAULT_CODE (
inline static int ip_distance(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  #if defined(__SSE2__)
  ret = ip_distance_avx128(a, b, len, distance);
  #else
  ret = ip_distance_normal(a, b, len, distance);
  #endif
  return ret;
}
)

OB_DECLARE_AVX2_SPECIFIC_CODE (
inline static int ip_distance(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  #if defined(__AVX__) || defined(__AVX2__)
  ret = ip_distance_avx256(a, b, len, distance);
  #elif defined(__SSE2__)
  ret = ip_distance_avx128(a, b, len, distance);
  #else
  ret = ip_distance_normal(a, b, len, distance);
  #endif
  return ret;
}
)

OB_DECLARE_AVX512_SPECIFIC_CODE (
inline static int ip_distance(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  #if defined(__AVX512F__) || defined(__AVX512BW__)
  ret = ip_distance_avx512(a, b, len, distance);
  #elif defined(__AVX__) || defined(__AVX2__)
  ret = ip_distance_avx256(a, b, len, distance);
  #elif defined(__SSE2__)
  ret = ip_distance_avx128(a, b, len, distance);
  #else
  ret = ip_distance_normal(a, b, len, distance);
  #endif
  return ret;
}
)

} // common
} // oceanbase
#endif
