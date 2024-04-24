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

#ifndef OCEANBASE_LIB_OB_VECTOR_L2_DISTANCE_H_
#define OCEANBASE_LIB_OB_VECTOR_L2_DISTANCE_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"
#include "common/object/ob_obj_compare.h"
#include "common/ob_target_specific.h"
#if defined(__SSE2__)
#include <emmintrin.h>
#endif
#if defined(__SSE3__)
#include <tmmintrin.h>
#endif
#if defined(__AVX512F__) || defined(__AVX512BW__) || defined(__AVX__) || defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

namespace oceanbase
{
namespace common
{
OB_INLINE static int l2_square_normal(const float *a, const float *b, const int64_t len, double &square)
{
  int ret = OB_SUCCESS;
  double sum = 0;
  double diff = 0;
  for (int64_t i = 0; i < len; ++i) {
    diff = a[i] - b[i];
    sum += (diff * diff);
    // if (OB_UNLIKELY(isinf(diff * diff) || isinf(sum))) {
    //   ret = OB_NUMERIC_OVERFLOW;
    //   LIB_LOG(WARN, "value is overflow", K(ret), K(diff), K(sum));
    // }
  }
  if (OB_SUCC(ret)) {
    square = sum;
  }
  return ret;
}

OB_INLINE static int vector_add_normal(float *a, const float *b, const int64_t len)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < len; ++i) {
    a[i] = a[i] + b[i];
  }
  return ret;
}

#if defined(__SSE2__) || defined(__SSE3__) || defined(__AVX__) || defined(__AVX2__) || defined(__AVX512F__) || defined(__AVX512BW__)
// SSE
OB_INLINE static int vector_add_simd4_avx128(float *a, const float *b, const int64_t len)
{
  int ret = OB_SUCCESS;
  const int64_t batch = 128 / (sizeof(float) * 8); // 4
  int64_t dim = len >> 2 << 2;

  float sum[4] CACHE_ALIGNED;
  for (int64_t i = 0; i < dim; i += batch) {
    _mm_store_ps(sum, _mm_add_ps(_mm_loadu_ps(a+i), _mm_loadu_ps(b+i)));
    for (int64_t j = 0; j < 4; j++) {
      a[i+j] = sum[j];
    }
  }
  return ret;
}

OB_INLINE static int vector_add_simd4_avx128_extra(float *a, const float *b, const int64_t len)
{
  int ret = OB_SUCCESS;
  int64_t dim = len >> 2 << 2;
  if (OB_FAIL(vector_add_simd4_avx128(a, b, dim))) {
    LIB_LOG(WARN, "failed to cal vector add", K(ret), K(len), K(dim));
  } else if (0 < len-dim && OB_FAIL(vector_add_normal(a+dim, b+dim, len-dim))) {
    LIB_LOG(WARN, "failed to cal vector add", K(ret), K(len), K(dim));
  }
  return ret;
}

OB_INLINE static int vector_add_avx128(float *a, const float *b, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (4 < len) {
    if (OB_FAIL(vector_add_simd4_avx128_extra(a, b, len))) {
      LIB_LOG(WARN, "failed to cal vector add", K(ret));
    }
  } else if (OB_FAIL(vector_add_normal(a, b, len))) {
    LIB_LOG(WARN, "failed to cal vector add", K(ret));
  }
  return ret;
}

OB_INLINE static int l2_square_simd4_avx128(const float *a, const float *b, const int64_t len, double &square)
{
  int ret = OB_SUCCESS;
  square = 0;
  const int64_t batch = 128 / (sizeof(float) * 8); // 4
  int64_t dim = len >> 2 << 2;

  __m128 diff;
  __m128 vsum = _mm_set1_ps(0);
  for (int64_t i = 0; i < dim; i += batch) {
    diff = _mm_sub_ps(_mm_loadu_ps(a+i), _mm_loadu_ps(b+i));
    vsum = _mm_add_ps(vsum, _mm_mul_ps(diff, diff));
  }
#if defined(__SSE3__) || defined(__AVX__) || defined(__AVX2__) || defined(__AVX512F__) || defined(__AVX512BW__)
  vsum = _mm_hadd_ps(vsum, vsum);
  vsum = _mm_hadd_ps(vsum, vsum);
  square = _mm_cvtss_f32(vsum);
#else
  float sum[4] CACHE_ALIGNED;
  _mm_store_ps(sum, vsum);

  square = sum[0] + sum[1] + sum[2] + sum[3];
#endif
  return ret;
}
OB_INLINE static int l2_square_simd4_avx128_extra(const float *a, const float *b, const int64_t len, double &square)
{
  int ret = OB_SUCCESS;
  int64_t dim = len >> 2 << 2;
  double extra_square = 0;
  square = 0;
  if (OB_FAIL(l2_square_simd4_avx128(a, b, dim, square))) {
    LIB_LOG(WARN, "failed to cal l2 square", K(ret), K(len), K(dim), K(square));
  } else if (0 < len-dim && OB_FAIL(l2_square_normal(a+dim, b+dim, len-dim, extra_square))) {
    LIB_LOG(WARN, "failed to cal l2 square", K(ret), K(len), K(dim), K(extra_square));
  } else {
    square += extra_square;
  }
  return ret;
}
OB_INLINE static int l2_square_avx128(const float *a, const float *b, const int64_t len, double &square)
{
  int ret = OB_SUCCESS;
  square = 0;
  if (4 < len) {
    if (OB_FAIL(l2_square_simd4_avx128_extra(a, b, len, square))) {
      LIB_LOG(WARN, "failed to cal l2 square", K(ret), K(square));
    }
  } else if (OB_FAIL(l2_square_normal(a, b, len, square))) {
    LIB_LOG(WARN, "failed to cal l2 square", K(ret));
  }
  return ret;
}
#endif

#if defined(__AVX__) || defined(__AVX2__) || defined(__AVX512F__) || defined(__AVX512BW__)
// AVX2
OB_INLINE static int vector_add_simd8_avx256(float *a, const float *b, const int64_t len)
{
  int ret = OB_SUCCESS;
  const int64_t batch = 256 / (sizeof(float) * 8); // 8
  int64_t dim = len >> 3 << 3;

  __m256 vsum = _mm256_set1_ps(0);
  float sum[8] CACHE_ALIGNED;
  for (int64_t i = 0; i < dim; i += batch) {
    vsum = _mm256_add_ps(_mm256_loadu_ps(a+i), _mm256_loadu_ps(b+i));
    _mm256_store_ps(sum, vsum);
    for (int64_t j = 0; j < 8; j++) {
      a[i+j] = sum[j];
    }
  }
  return ret;
}

OB_INLINE static int vector_add_simd8_avx256_extra(float *a, const float *b, const int64_t len)
{
  int ret = OB_SUCCESS;
  int64_t dim = len >> 3 << 3;
  if (OB_FAIL(vector_add_simd8_avx256(a, b, dim))) {
    LIB_LOG(WARN, "failed to cal vector add", K(ret), K(len), K(dim));
  } else if (0 < len-dim && OB_FAIL(vector_add_simd4_avx128_extra(a+dim, b+dim, len-dim))) {
    LIB_LOG(WARN, "failed to cal vector add", K(ret), K(len), K(dim));
  }
  return ret;
}

OB_INLINE static int vector_add_avx256(float *a, const float *b, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (8 < len) {
    if (OB_FAIL(vector_add_simd8_avx256_extra(a, b, len))) {
      LIB_LOG(WARN, "failed to cal vector add", K(ret));
    }
  } else if (4 < len) {
    if (OB_FAIL(vector_add_simd4_avx128_extra(a, b, len))) {
      LIB_LOG(WARN, "failed to cal vector add", K(ret));
    }
  } else if (OB_FAIL(vector_add_normal(a, b, len))) {
    LIB_LOG(WARN, "failed to cal vector add", K(ret));
  }
  return ret;
}

OB_INLINE static int l2_square_simd8_avx256(const float *a, const float *b, const int64_t len, double &square)
{
  int ret = OB_SUCCESS;
  square = 0;
  const int64_t batch = 256 / (sizeof(float) * 8); // 8
  int64_t dim = len >> 3 << 3;

  __m256 diff;
  __m256 vsum = _mm256_set1_ps(0);
  for (int64_t i = 0; i < dim; i += batch) {
    diff = _mm256_sub_ps(_mm256_loadu_ps(a+i), _mm256_loadu_ps(b+i));
    vsum = _mm256_add_ps(vsum, _mm256_mul_ps(diff, diff));
  }
  __m256 permuted = _mm256_permute2f128_ps(vsum, vsum, 1);
  vsum = _mm256_add_ps(vsum, permuted);
  vsum = _mm256_hadd_ps(vsum, vsum);
  vsum = _mm256_hadd_ps(vsum, vsum);
  square = _mm256_cvtss_f32(vsum);
  return ret;
}
OB_INLINE static int l2_square_simd8_avx256_extra(const float *a, const float *b, const int64_t len, double &square)
{
  int ret = OB_SUCCESS;
  int64_t dim = len >> 3 << 3;
  double extra_square = 0;
  square = 0;
  if (OB_FAIL(l2_square_simd8_avx256(a, b, dim, square))) {
    LIB_LOG(WARN, "failed to cal l2 square", K(ret), K(len), K(dim), K(square));
  } else if (0 < len-dim && OB_FAIL(l2_square_simd4_avx128_extra(a+dim, b+dim, len-dim, extra_square))) {
    LIB_LOG(WARN, "failed to cal l2 square", K(ret), K(len), K(dim), K(extra_square));
  } else {
    square += extra_square;
  }
  return ret;
}
OB_INLINE static int l2_square_avx256(const float *a, const float *b, const int64_t len, double &square)
{
  int ret = OB_SUCCESS;
  if (8 < len) {
    if (OB_FAIL(l2_square_simd8_avx256_extra(a, b, len, square))) {
      LIB_LOG(WARN, "failed to cal l2 square", K(ret));
    }
  } else if (4 < len) {
    if (OB_FAIL(l2_square_simd4_avx128_extra(a, b, len, square))) {
      LIB_LOG(WARN, "failed to cal l2 square", K(ret), K(square));
    }
  } else if (OB_FAIL(l2_square_normal(a, b, len, square))) {
    LIB_LOG(WARN, "failed to cal l2 square", K(ret));
  }
  return ret;
}
#endif

#if defined(__AVX512F__) || defined(__AVX512BW__)
// AVX512
OB_INLINE static int vector_add_simd16_avx512(float *a, const float *b, const int64_t len)
{
  int ret = OB_SUCCESS;
  const int64_t batch = 512 / (sizeof(float) * 8); // 16
  int64_t dim = len >> 4 << 4;

  __m512 vsum = _mm512_set1_ps(0);
  float sum[16] CACHE_ALIGNED;
  for (int64_t i = 0; i < dim; i += batch) {
    vsum = _mm512_add_ps(_mm512_loadu_ps(a+i), _mm512_loadu_ps(b+i));
    _mm512_store_ps(sum, vsum); // TODO(@jingshui) _mm512_store_ps(a+i, vsum) will core
    for (int64_t j = 0; j < 16; j++) {
      a[i+j] = sum[j];
    }
  }
  return ret;
}

OB_INLINE static int vector_add_simd16_avx512_extra(float *a, const float *b, const int64_t len)
{
  int ret = OB_SUCCESS;
  int64_t dim = len >> 4 << 4;
  if (OB_FAIL(vector_add_simd16_avx512(a, b, dim))) {
    LIB_LOG(WARN, "failed to cal vector add", K(ret), K(len), K(dim));
  } else if (0 < len-dim && OB_FAIL(vector_add_simd8_avx256_extra(a+dim, b+dim, len-dim))) {
    LIB_LOG(WARN, "failed to cal vector add", K(ret), K(len), K(dim));
  }
  return ret;
}

OB_INLINE static int vector_add_avx512(float *a, const float *b, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (16 < len) {
    if (OB_FAIL(vector_add_simd16_avx512_extra(a, b, len))) {
      LIB_LOG(WARN, "failed to cal vector add", K(ret));
    }
  } else if (8 < len) {
    if (OB_FAIL(vector_add_simd8_avx256_extra(a, b, len))) {
      LIB_LOG(WARN, "failed to cal vector add", K(ret));
    }
  } else if (4 < len) {
    if (OB_FAIL(vector_add_simd4_avx128_extra(a, b, len))) {
      LIB_LOG(WARN, "failed to cal vector add", K(ret));
    }
  } else if (OB_FAIL(vector_add_normal(a, b, len))) {
    LIB_LOG(WARN, "failed to cal vector add", K(ret));
  }
  return ret;
}

OB_INLINE static int l2_square_simd16_avx512(const float *a, const float *b, const int64_t len, double &square)
{
  int ret = OB_SUCCESS;
  square = 0;
  const int64_t batch = 512 / (sizeof(float) * 8); // 16
  int64_t dim = len >> 4 << 4;
  __m512 diff;
  __m512 vsum = _mm512_set1_ps(0);
  for (int64_t i = 0; i < dim; i += batch) {
    diff = _mm512_sub_ps(_mm512_loadu_ps(a+i), _mm512_loadu_ps(b+i));
    vsum = _mm512_add_ps(vsum, _mm512_mul_ps(diff, diff));
  }
  square = _mm512_reduce_add_ps(vsum);
  return ret;
}
OB_INLINE static int l2_square_simd16_avx512_extra(const float *a, const float *b, const int64_t len, double &square)
{
  int ret = OB_SUCCESS;
  int64_t dim = len >> 4 << 4;
  double extra_square = 0;
  square = 0;
  if (OB_FAIL(l2_square_simd16_avx512(a, b, dim, square))) {
    LIB_LOG(WARN, "failed to cal l2 square", K(ret), K(len), K(dim), K(square));
  } else if (0 < len-dim && OB_FAIL(l2_square_simd8_avx256_extra(a+dim, b+dim, len-dim, extra_square))) {
    LIB_LOG(WARN, "failed to cal l2 square", K(ret), K(len), K(dim), K(extra_square));
  }
  if (OB_SUCC(ret)) {
    square += extra_square;
  }
  return ret;
}
OB_INLINE static int l2_square_avx512(const float *a, const float *b, const int64_t len, double &square)
{
  int ret = OB_SUCCESS;
  if (16 < len) {
    if (OB_FAIL(l2_square_simd16_avx512_extra(a, b, len, square))) {
      LIB_LOG(WARN, "failed to cal l2 square", K(ret));
    }
  } else if (8 < len) {
    if (OB_FAIL(l2_square_simd8_avx256_extra(a, b, len, square))) {
      LIB_LOG(WARN, "failed to cal l2 square", K(ret));
    }
  } else if (4 < len) {
    if (OB_FAIL(l2_square_simd4_avx128_extra(a, b, len, square))) {
      LIB_LOG(WARN, "failed to cal l2 square", K(ret), K(square));
    }
  } else if (OB_FAIL(l2_square_normal(a, b, len, square))) {
    LIB_LOG(WARN, "failed to cal l2 square", K(ret));
  }
  return ret;
}
#endif

OB_DECLARE_DEFAULT_CODE (
  inline static int l2_square(const float *a, const float *b, const int64_t len, double &square)
  {
    int ret = OB_SUCCESS;
    #if defined(__SSE2__)
    ret = l2_square_avx128(a, b, len, square);
    #else
    ret = l2_square_normal(a, b, len, square);
    #endif
    return ret;
  }

  inline static int vector_add(float *a, const float *b, const int64_t len)
  {
    int ret = OB_SUCCESS;
    #if defined(__SSE2__)
    ret = vector_add_avx128(a, b, len);
    #else
    ret = vector_add_normal(a, b, len);
    #endif
    return ret;
  }
)

OB_DECLARE_AVX2_SPECIFIC_CODE (
  inline static int l2_square(const float *a, const float *b, const int64_t len, double &square)
  {
    int ret = OB_SUCCESS;
    #if defined(__AVX__) || defined(__AVX2__)
    ret = l2_square_avx256(a, b, len, square);
    #elif defined(__SSE2__)
    ret = l2_square_avx128(a, b, len, square);
    #else
    ret = l2_square_normal(a, b, len, square);
    #endif
    return ret;
  }

  inline static int vector_add(float *a, const float *b, const int64_t len)
  {
    int ret = OB_SUCCESS;
    #if defined(__AVX__) || defined(__AVX2__)
    ret = vector_add_avx256(a, b, len);
    #elif defined(__SSE2__)
    ret = vector_add_avx128(a, b, len);
    #else
    ret = vector_add_normal(a, b, len);
    #endif
    return ret;
  }
)

OB_DECLARE_AVX512_SPECIFIC_CODE (
inline static int l2_square(const float *a, const float *b, const int64_t len, double &square)
{
  int ret = OB_SUCCESS;
  #if defined(__AVX512F__) || defined(__AVX512BW__)
  ret = l2_square_avx512(a, b, len, square);
  #elif defined(__AVX__) || defined(__AVX2__)
  ret = l2_square_avx256(a, b, len, square);
  #elif defined(__SSE2__)
  ret = l2_square_avx128(a, b, len, square);
  #else
  ret = l2_square_normal(a, b, len, square);
  #endif
  return ret;
}

inline static int vector_add(float *a, const float *b, const int64_t len)
{
  int ret = OB_SUCCESS;
  #if defined(__AVX512F__) || defined(__AVX512BW__)
  ret = vector_add_avx512(a, b, len);
  #elif defined(__AVX__) || defined(__AVX2__)
  ret = vector_add_avx256(a, b, len);
  #elif defined(__SSE2__)
  ret = vector_add_avx128(a, b, len);
  #else
  ret = vector_add_normal(a, b, len);
  #endif
  return ret;
}
)

struct ObVectorL2Distance
{
  static int l2_square_func(const float *a, const float *b, const int64_t len, double &square);
  static int l2_distance_func(const float *a, const float *b, const int64_t len, double &distance);
  static int vector_add_func(float *a, const float *b, const int64_t len);
};

} // common
} // oceanbase
#endif