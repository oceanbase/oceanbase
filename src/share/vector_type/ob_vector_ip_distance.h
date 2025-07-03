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

OB_DECLARE_SSE_AND_AVX_CODE(
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
)

OB_DECLARE_SSE42_SPECIFIC_CODE(
inline static int ip_distance(const float *a, const float *b, const int64_t len, double &distance)
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
)

OB_DECLARE_AVX_ALL_CODE(
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
)

OB_DECLARE_AVX_AND_AVX2_CODE(
inline static int ip_distance(const float *a, const float *b, const int64_t len, double &distance)
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
)

OB_DECLARE_AVX512_SPECIFIC_CODE(
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

inline static int ip_distance(const float *a, const float *b, const int64_t len, double &distance)
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
)

OB_DECLARE_DEFAULT_CODE (
inline static int ip_distance(const float *a, const float *b, const int64_t len, double &distance)
{
  return ip_distance_normal(a, b, len, distance);
}
)

#if defined(__aarch64__)
inline static int ip_distance_neon(const float *x, const float *y, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  float32x4_t sum = vdupq_n_f32(0.0f);
  auto dim = len;
  auto d = len;
  while (d >= 16) {
    float32x4x4_t a = vld1q_f32_x4(x + dim - d);
    float32x4x4_t b = vld1q_f32_x4(y + dim - d);
    float32x4x4_t c;
    c.val[0] = vmulq_f32(a.val[0], b.val[0]);
    c.val[1] = vmulq_f32(a.val[1], b.val[1]);
    c.val[2] = vmulq_f32(a.val[2], b.val[2]);
    c.val[3] = vmulq_f32(a.val[3], b.val[3]);

    c.val[0] = vaddq_f32(c.val[0], c.val[1]);
    c.val[2] = vaddq_f32(c.val[2], c.val[3]);
    c.val[0] = vaddq_f32(c.val[0], c.val[2]);

    sum = vaddq_f32(sum, c.val[0]);
    d -= 16;
  }
  // calculate leftover

  if (d >= 8) {
    float32x4x2_t a = vld1q_f32_x2(x + dim - d);
    float32x4x2_t b = vld1q_f32_x2(y + dim - d);
    float32x4x2_t c;
    c.val[0] = vmulq_f32(a.val[0], b.val[0]);
    c.val[1] = vmulq_f32(a.val[1], b.val[1]);
    c.val[0] = vaddq_f32(c.val[0], c.val[1]);
    sum = vaddq_f32(sum, c.val[0]);
    d -= 8;
  }

  if (d >= 4) {
    float32x4_t a = vld1q_f32(x + dim - d);
    float32x4_t b = vld1q_f32(y + dim - d);
    float32x4_t c;
    c = vmulq_f32(a, b);
    sum = vaddq_f32(sum, c);
    d -= 4;
  }

  float32x4_t res_x = vdupq_n_f32(0.0f);
  float32x4_t res_y = vdupq_n_f32(0.0f);
  if (d >= 3) {
      res_x = vld1q_lane_f32(x + dim - d, res_x, 2);
      res_y = vld1q_lane_f32(y + dim - d, res_y, 2);
      d -= 1;
  }

  if (d >= 2) {
      res_x = vld1q_lane_f32(x + dim - d, res_x, 1);
      res_y = vld1q_lane_f32(y + dim - d, res_y, 1);
      d -= 1;
  }

  if (d >= 1) {
      res_x = vld1q_lane_f32(x + dim - d, res_x, 0);
      res_y = vld1q_lane_f32(y + dim - d, res_y, 0);
      d -= 1;
  }

  sum = vaddq_f32(sum, vmulq_f32(res_x, res_y));
  distance = vaddvq_f32(sum);
  return ret;
}
#endif

} // common
} // oceanbase
#endif