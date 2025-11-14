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

#ifndef OCEANBASE_LIB_OB_VECTOR_COSINE_DISTANCE_H_
#define OCEANBASE_LIB_OB_VECTOR_COSINE_DISTANCE_H_

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
struct ObVectorCosineDistance
{
  static int cosine_similarity_func(const T *a, const T *b, const int64_t len, double &similarity);
  static int cosine_distance_func(const T *a, const T *b, const int64_t len, double &distance);

  // normal func
  OB_INLINE static double get_cosine_distance(double similarity);
  // TODO(@jingshui) add simd func
};

OB_INLINE int cosine_calculate_normal(const float *a, const float *b, const int64_t len, double &ip, double &abs_dist_a, double &abs_dist_b)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
    ip += a[i] * b[i];
    abs_dist_a += a[i] * a[i];
    abs_dist_b += b[i] * b[i];
  }
  return ret;
}

OB_INLINE int cosine_calculate_normal(const uint8_t *a, const uint8_t *b, const int64_t len, double &ip, double &abs_dist_a, double &abs_dist_b)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
    ip += a[i] * b[i];
    abs_dist_a += a[i] * a[i];
    abs_dist_b += b[i] * b[i];
  }
  return ret;
}

OB_INLINE int cosine_similarity_normal(const float *a, const float *b, const int64_t len, double &similarity)
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

OB_INLINE int cosine_similarity_normal(const uint8_t *a, const uint8_t *b, const uint8_t len, double &similarity)
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

template <typename T>
OB_INLINE double ObVectorCosineDistance<T>::get_cosine_distance(double similarity)
{
  if (similarity > 1.0) {
    similarity = 1.0;
  } else if (similarity < -1.0) {
    similarity = -1.0;
  }
  return 1.0 - similarity;
}

template<>
int ObVectorCosineDistance<float>::cosine_distance_func(const float *a, const float *b, const int64_t len, double &distance);
template<>
int ObVectorCosineDistance<uint8_t>::cosine_distance_func(const uint8_t *a, const uint8_t *b, const int64_t len, double &distance);

template<>
int ObVectorCosineDistance<float>::cosine_similarity_func(const float *a, const float *b, const int64_t len, double &similarity);
template <>
int ObVectorCosineDistance<uint8_t>::cosine_similarity_func(const uint8_t *a, const uint8_t *b, const int64_t len, double &similarity);

OB_DECLARE_SSE_AND_AVX_CODE(
OB_INLINE static int cosine_calculate_simd4_avx128(const float *a, const float *b, const int64_t len, double &ip, double &abs_dist_a, double &abs_dist_b)
{
  int ret = OB_SUCCESS;
  const int64_t batch = 128 / (sizeof(float) * 8); // 4
  int64_t dim = len >> 2 << 2;
  double tmp_ip = 0;
  double tmp_abs_dist_a = 0;
  double tmp_abs_dist_b = 0;

  __m128 vsum = _mm_set1_ps(0);
  __m128 sum_v1 = _mm_set1_ps(0);
  __m128 sum_v2 = _mm_set1_ps(0);
  for (int64_t i = 0; i < dim; i += batch) {
    vsum = _mm_add_ps(vsum, _mm_mul_ps(_mm_loadu_ps(a + i), _mm_loadu_ps(b + i)));
    sum_v1 = _mm_add_ps(sum_v1, _mm_mul_ps(_mm_loadu_ps(a + i), _mm_loadu_ps(a + i)));
    sum_v2 = _mm_add_ps(sum_v2, _mm_mul_ps(_mm_loadu_ps(b + i), _mm_loadu_ps(b + i)));
  }

  float sum[4] CACHE_ALIGNED;
  _mm_store_ps(sum, vsum);

  tmp_ip = sum[0] + sum[1] + sum[2] + sum[3];
  ip += tmp_ip;

  float sum_a[4] CACHE_ALIGNED;
  float sum_b[4] CACHE_ALIGNED;
  _mm_store_ps(sum_a, sum_v1);
  _mm_store_ps(sum_b, sum_v2);

  tmp_abs_dist_a = sum_a[0] + sum_a[1] + sum_a[2] + sum_a[3];
  tmp_abs_dist_b = sum_b[0] + sum_b[1] + sum_b[2] + sum_b[3];

  abs_dist_a += tmp_abs_dist_a;
  abs_dist_b += tmp_abs_dist_b;

  return ret;
}

OB_INLINE static int cosine_similarity_simd4_avx128(const float *a, const float *b, const int64_t len, double &similarity)
{
  int ret = OB_SUCCESS;
  double ip = 0;
  double abs_dist_a = 0;
  double abs_dist_b = 0;
  similarity = 0;

  if (OB_FAIL(cosine_calculate_simd4_avx128(a, b, len, ip, abs_dist_a, abs_dist_b))) {
    LIB_LOG(WARN, "failed to cal cosine", K(ret), K(ip));
  } else if (0 == abs_dist_a || 0 == abs_dist_b) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    similarity = ip / (sqrt(abs_dist_a * abs_dist_b));
  }
  return ret;
}

OB_INLINE static int cosine_calculate_simd4_avx128_extra(const float *a, const float *b, const int64_t len, double &ip, double &abs_dist_a, double &abs_dist_b)
{
  int ret = OB_SUCCESS;
  int64_t dim = len >> 2 << 2;
  if (OB_FAIL(cosine_calculate_simd4_avx128(a, b, dim, ip, abs_dist_a, abs_dist_b))) {
    LIB_LOG(WARN, "failed to cal cosine", K(ret), K(len), K(dim), K(ip));
  } else if (0 < len - dim && OB_FAIL(cosine_calculate_normal(a + dim, b + dim, len - dim, ip, abs_dist_a, abs_dist_b))) {
    LIB_LOG(WARN, "failed to cal cosine", K(ret), K(len), K(dim), K(ip));
  }
  return ret;
}
)

OB_DECLARE_SSE42_SPECIFIC_CODE(
inline static int cosine_similarity(const float *a, const float *b, const int64_t len, double &similarity)
{
  int ret = OB_SUCCESS;
  double ip = 0;
  double abs_dist_a = 0;
  double abs_dist_b = 0;
  similarity = 0;
  if (4 < len) {
    if (OB_FAIL(cosine_calculate_simd4_avx128_extra(a, b, len, ip, abs_dist_a, abs_dist_b))) {
      LIB_LOG(WARN, "failed to cal cosine extra", K(ret), K(ip));
    }
  } else if (OB_FAIL(cosine_calculate_normal(a, b, len, ip, abs_dist_a, abs_dist_b))) {
    LIB_LOG(WARN, "failed to cal cosine normal", K(ret), K(ip));
  }

  if (OB_FAIL(ret)) {
  } else if (0 == abs_dist_a || 0 == abs_dist_b) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    similarity = ip / (sqrt(abs_dist_a * abs_dist_b));
  }
  return ret;
}
)

OB_DECLARE_AVX_ALL_CODE(
OB_INLINE static int cosine_calculate_simd8_avx256(const float *a, const float *b, const int64_t len, double &ip, double &abs_dist_a, double &abs_dist_b)
{
  int ret = OB_SUCCESS;
  const int64_t batch = 256 / (sizeof(float) * 8); // 8
  int64_t dim = len >> 3 << 3;
  double tmp_ip = 0;
  double tmp_abs_dist_a = 0;
  double tmp_abs_dist_b = 0;

  __m256 vsum = _mm256_set1_ps(0);
  __m256 sum_v1 = _mm256_set1_ps(0);
  __m256 sum_v2 = _mm256_set1_ps(0);
  for (int64_t i = 0; i < dim; i += batch) {
    vsum = _mm256_add_ps(vsum, _mm256_mul_ps(_mm256_loadu_ps(a + i), _mm256_loadu_ps(b + i)));
    sum_v1 = _mm256_add_ps(sum_v1, _mm256_mul_ps(_mm256_loadu_ps(a + i), _mm256_loadu_ps(a + i)));
    sum_v2 = _mm256_add_ps(sum_v2, _mm256_mul_ps(_mm256_loadu_ps(b + i), _mm256_loadu_ps(b + i)));
  }
  float sum[8] CACHE_ALIGNED;
  _mm256_store_ps(sum, vsum);

  tmp_ip = sum[0] + sum[1] + sum[2] + sum[3] + sum[4] + sum[5] + sum[6] + sum[7];
  ip += tmp_ip;

  float sum_a[8] CACHE_ALIGNED;
  float sum_b[8] CACHE_ALIGNED;
  _mm256_store_ps(sum_a, sum_v1);
  _mm256_store_ps(sum_b, sum_v2);

  tmp_abs_dist_a = sum_a[0] + sum_a[1] + sum_a[2] + sum_a[3] + sum_a[4] + sum_a[5] + sum_a[6] + sum_a[7];
  tmp_abs_dist_b = sum_b[0] + sum_b[1] + sum_b[2] + sum_b[3] + sum_b[4] + sum_b[5] + sum_b[6] + sum_b[7];

  abs_dist_a += tmp_abs_dist_a;
  abs_dist_b += tmp_abs_dist_b;

  return ret;
}

OB_INLINE static int cosine_calculate_simd8_avx256_extra(const float *a, const float *b, const int64_t len, double &ip, double &abs_dist_a, double &abs_dist_b)
{
  int ret = OB_SUCCESS;
  int64_t dim = len >> 3 << 3;
  if (OB_FAIL(cosine_calculate_simd8_avx256(a, b, dim, ip, abs_dist_a, abs_dist_b))) {
    LIB_LOG(WARN, "failed to cal cosine", K(ret), K(len), K(dim), K(ip));
  } else if (0 < len - dim && OB_FAIL(cosine_calculate_simd4_avx128_extra(a + dim, b + dim, len - dim, ip, abs_dist_a, abs_dist_b))) {
    LIB_LOG(WARN, "failed to cal cosine", K(ret), K(len), K(dim), K(ip));
  }
  return ret;
}
)

OB_DECLARE_AVX_AND_AVX2_CODE(
inline static int cosine_similarity(const float *a, const float *b, const int64_t len, double &similarity)
{
  int ret = OB_SUCCESS;
  double ip = 0;
  double abs_dist_a = 0;
  double abs_dist_b = 0;
  similarity = 0;
  if (8 < len) {
    if (OB_FAIL(cosine_calculate_simd8_avx256_extra(a, b, len, ip, abs_dist_a, abs_dist_b))) {
      LIB_LOG(WARN, "failed to cal cosine", K(ret), K(ip));
    }
  } else if (4 < len) {
    if (OB_FAIL(cosine_calculate_simd4_avx128_extra(a, b, len, ip, abs_dist_a, abs_dist_b))) {
      LIB_LOG(WARN, "failed to cal cosine", K(ret), K(ip));
    }
  } else {
    if (OB_FAIL(cosine_calculate_normal(a, b, len, ip, abs_dist_a, abs_dist_b))) {
      LIB_LOG(WARN, "failed to cal cosine", K(ret), K(ip));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (0 == abs_dist_a || 0 == abs_dist_b) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    similarity = ip / (sqrt(abs_dist_a * abs_dist_b));
  }
  return ret;
}
)

OB_DECLARE_AVX512_SPECIFIC_CODE(
OB_INLINE static int cosine_calculate_simd16_avx512(const float *a, const float *b, const int64_t len, double &ip, double &abs_dist_a, double &abs_dist_b)
{
  int ret = OB_SUCCESS;
  const int64_t batch = 512 / (sizeof(float) * 8); // 16
  int64_t dim = len >> 4 << 4;
  double tmp_ip = 0;
  double tmp_abs_dist_a = 0;
  double tmp_abs_dist_b = 0;

  __m512 vsum = _mm512_set1_ps(0);
  __m512 sum_v1 = _mm512_set1_ps(0);
  __m512 sum_v2 = _mm512_set1_ps(0);
  for (int64_t i = 0; i < dim; i += batch) {
    vsum = _mm512_add_ps(vsum, _mm512_mul_ps(_mm512_loadu_ps(a + i), _mm512_loadu_ps(b + i)));
    sum_v1 = _mm512_add_ps(sum_v1, _mm512_mul_ps(_mm512_loadu_ps(a + i), _mm512_loadu_ps(a + i)));
    sum_v2 = _mm512_add_ps(sum_v2, _mm512_mul_ps(_mm512_loadu_ps(b + i), _mm512_loadu_ps(b + i)));
  }
  float sum[16] CACHE_ALIGNED;
  _mm512_store_ps(sum, vsum);

  tmp_ip = sum[0] + sum[1] + sum[2] + sum[3] + sum[4] + sum[5] + sum[6] + sum[7]
      + sum[8] + sum[9] + sum[10] + sum[11] + sum[12] + sum[13] + sum[14] + sum[15];
  ip += tmp_ip;

  float sum_a[16] CACHE_ALIGNED;
  float sum_b[16] CACHE_ALIGNED;
  _mm512_store_ps(sum_a, sum_v1);
  _mm512_store_ps(sum_b, sum_v2);

  tmp_abs_dist_a = sum_a[0] + sum_a[1] + sum_a[2] + sum_a[3] + sum_a[4] + sum_a[5] + sum_a[6] + sum_a[7]
      + sum_a[8] + sum_a[9] + sum_a[10] + sum_a[11] + sum_a[12] + sum_a[13] + sum_a[14] + sum_a[15];
  tmp_abs_dist_b = sum_b[0] + sum_b[1] + sum_b[2] + sum_b[3] + sum_b[4] + sum_b[5] + sum_b[6] + sum_b[7]
      + sum_b[8] + sum_b[9] + sum_b[10] + sum_b[11] + sum_b[12] + sum_b[13] + sum_b[14] + sum_b[15];

  abs_dist_a += tmp_abs_dist_a;
  abs_dist_b += tmp_abs_dist_b;

  return ret;
}

OB_INLINE static int cosine_calculate_simd16_avx512_extra(const float *a, const float *b, const int64_t len, double &ip, double &abs_dist_a, double &abs_dist_b)
{
  int ret = OB_SUCCESS;
  int64_t dim = len >> 4 << 4;
  if (OB_FAIL(cosine_calculate_simd16_avx512(a, b, dim, ip, abs_dist_a, abs_dist_b))) {
    LIB_LOG(WARN, "failed to cal cosine", K(ret), K(len), K(dim), K(ip));
  } else if (0 < len - dim && OB_FAIL(cosine_calculate_simd8_avx256_extra(a + dim, b + dim, len - dim, ip, abs_dist_a, abs_dist_b))) {
    LIB_LOG(WARN, "failed to cal cosine extra", K(ret), K(len), K(dim), K(ip));
  }
  return ret;
}

inline static int cosine_similarity(const float *a, const float *b, const int64_t len, double &similarity)
{
  int ret = OB_SUCCESS;
  double ip = 0;
  double abs_dist_a = 0;
  double abs_dist_b = 0;
  similarity = 0;
  if (16 < len) {
    if (OB_FAIL(cosine_calculate_simd16_avx512_extra(a, b, len, ip, abs_dist_a, abs_dist_b))) {
      LIB_LOG(WARN, "failed to cal cosine", K(ret), K(ip));
    }
  } else if (8 < len) {
    if (OB_FAIL(cosine_calculate_simd8_avx256_extra(a, b, len, ip, abs_dist_a, abs_dist_b))) {
      LIB_LOG(WARN, "failed to cal cosine", K(ret), K(ip));
    }
  } else if (4 < len) {
    if (OB_FAIL(cosine_calculate_simd4_avx128_extra(a, b, len, ip, abs_dist_a, abs_dist_b))) {
      LIB_LOG(WARN, "failed to cal cosine", K(ret), K(ip));
    }
  } else {
    if (OB_FAIL(cosine_calculate_normal(a, b, len, ip, abs_dist_a, abs_dist_b))) {
      LIB_LOG(WARN, "failed to cal cosine", K(ret), K(ip));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (0 == abs_dist_a || 0 == abs_dist_b) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    similarity = ip / (sqrt(abs_dist_a * abs_dist_b));
  }
  return ret;
}
)

OB_DECLARE_DEFAULT_CODE (
inline static int cosine_similarity(const float *a, const float *b, const int64_t len, double &similarity)
{
  return cosine_similarity_normal(a, b, len, similarity);
}
)

#if defined(__aarch64__)

inline static float cosine_inner_product_neon(const float *x, const float *y, const int64_t len) {
  float32x4_t sum0 = vdupq_n_f32(0.0f);
  float32x4_t sum1 = vdupq_n_f32(0.0f);
  float32x4_t sum2 = vdupq_n_f32(0.0f);
  float32x4_t sum3 = vdupq_n_f32(0.0f);

  int64_t i = 0;

  for (; i + 15 < len; i += 16) {
    float32x4x4_t a = vld1q_f32_x4(x + i);
    float32x4x4_t b = vld1q_f32_x4(y + i);

    sum0 = vfmaq_f32(sum0, a.val[0], b.val[0]);
    sum1 = vfmaq_f32(sum1, a.val[1], b.val[1]);
    sum2 = vfmaq_f32(sum2, a.val[2], b.val[2]);
    sum3 = vfmaq_f32(sum3, a.val[3], b.val[3]);
  }

  if (i + 7 < len) {
    float32x4x2_t a = vld1q_f32_x2(x + i);
    float32x4x2_t b = vld1q_f32_x2(y + i);

    sum0 = vfmaq_f32(sum0, a.val[0], b.val[0]);
    sum1 = vfmaq_f32(sum1, a.val[1], b.val[1]);
    i += 8;
  }

  if (i + 3 < len) {
    float32x4_t a = vld1q_f32(x + i);
    float32x4_t b = vld1q_f32(y + i);
    sum0 = vfmaq_f32(sum0, a, b);
    i += 4;
  }

  sum0 = vaddq_f32(sum0, sum1);
  sum2 = vaddq_f32(sum2, sum3);
  sum0 = vaddq_f32(sum0, sum2);

  float result = vaddvq_f32(sum0);
  for (; i < len; i++) {
    result += x[i] * y[i];
  }

  return result;
}

inline static float cosine_norm_square_neon(const float *x, const int64_t len) {
  float32x4_t sum0 = vdupq_n_f32(0.0f);
  float32x4_t sum1 = vdupq_n_f32(0.0f);
  float32x4_t sum2 = vdupq_n_f32(0.0f);
  float32x4_t sum3 = vdupq_n_f32(0.0f);

  int64_t i = 0;

  for (; i + 15 < len; i += 16) {
    float32x4x4_t a = vld1q_f32_x4(x + i);

    sum0 = vfmaq_f32(sum0, a.val[0], a.val[0]);
    sum1 = vfmaq_f32(sum1, a.val[1], a.val[1]);
    sum2 = vfmaq_f32(sum2, a.val[2], a.val[2]);
    sum3 = vfmaq_f32(sum3, a.val[3], a.val[3]);
  }

  if (i + 7 < len) {
    float32x4x2_t a = vld1q_f32_x2(x + i);

    sum0 = vfmaq_f32(sum0, a.val[0], a.val[0]);
    sum1 = vfmaq_f32(sum1, a.val[1], a.val[1]);
    i += 8;
  }

  if (i + 3 < len) {
    float32x4_t a = vld1q_f32(x + i);
    sum0 = vfmaq_f32(sum0, a, a);
    i += 4;
  }

  sum0 = vaddq_f32(sum0, sum1);
  sum2 = vaddq_f32(sum2, sum3);
  sum0 = vaddq_f32(sum0, sum2);

  float result = vaddvq_f32(sum0);
  for (; i < len; i++) {
    result += x[i] * x[i];
  }

  return result;
}

inline static int cosine_similarity_neon(const float *a, const float *b, const int64_t len, double &similarity)
{
  int ret = OB_SUCCESS;

  double ip = cosine_inner_product_neon(a, b, len);
  double norm_a_sq = cosine_norm_square_neon(a, len);
  double norm_b_sq = cosine_norm_square_neon(b, len);

  if (0 == norm_a_sq || 0 == norm_b_sq) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    similarity = ip / (sqrt(norm_a_sq) * sqrt(norm_b_sq));
  }

  return ret;
}

#endif

} // common
} // oceanbase
#endif
