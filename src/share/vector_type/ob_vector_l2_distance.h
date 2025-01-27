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

#ifndef OCEANBASE_LIB_OB_VECTOR_L2_DISTANCE_H_
#define OCEANBASE_LIB_OB_VECTOR_L2_DISTANCE_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"
#include "common/object/ob_obj_compare.h"
#include "ob_vector_op_common.h"

namespace oceanbase
{
namespace common
{
struct ObVectorL2Distance
{
  static int l2_square_func(const float *a, const float *b, const int64_t len, double &square);
  static int l2_distance_func(const float *a, const float *b, const int64_t len, double &distance);
  static int l2_distance_func(const uint8_t *a, const uint8_t *b, const int64_t len, double &distance);
  static int l2_square_normal(const uint8_t *a, const uint8_t *b, const int64_t len, double &square);
};

#define VEC_SUM(type, load_func, diff_func, calc_func, acc_func, vsum) \
  type diff = diff_func(load_func(a + i), load_func(b + i));           \
  vsum = acc_func(vsum, calc_func(diff, diff));

OB_INLINE static int l2_square_normal(const float *a, const float *b, const int64_t len,
                                      double &square)
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

// for others
OB_DECLARE_DEFAULT_CODE(inline static int l2_square(const float *a, const float *b,
                                                    const int64_t len, double &square) {
  return l2_square_normal(a, b, len, square);
})

OB_DECLARE_SSE_AND_AVX_CODE(
    inline static int l2_square_simd4_avx128(const float *a, const float *b, const int64_t len,
                                             double &square) {
      int ret = OB_SUCCESS;
      square = 0;
      const int64_t batch = 128 / (sizeof(float) * 8);  // 4
      int64_t dim = len >> 2 << 2;

      __m128 vsum = _mm_set1_ps(0);
      for (int64_t i = 0; i < dim; i += batch) {
        VEC_SUM(__m128, _mm_loadu_ps, _mm_sub_ps, _mm_mul_ps, _mm_add_ps, vsum)
      }
#if defined(__SSE3__) || defined(__AVX__) || defined(__AVX2__) || defined(__AVX512F__) \
    || defined(__AVX512BW__) || defined(__AVX512VL__)
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

    inline static int l2_square_simd4_avx128_extra(const float *a, const float *b,
                                                   const int64_t len, double &square) {
      int ret = OB_SUCCESS;
      int64_t dim = len >> 2 << 2;
      double extra_square = 0;
      square = 0;
      if (OB_FAIL(l2_square_simd4_avx128(a, b, dim, square))) {
        LIB_LOG(WARN, "failed to cal l2 square", K(ret), K(len), K(dim), K(square));
      } else if (0 < len - dim
                 && OB_FAIL(l2_square_normal(a + dim, b + dim, len - dim, extra_square))) {
        LIB_LOG(WARN, "failed to cal l2 square", K(ret), K(len), K(dim), K(extra_square));
      } else {
        square += extra_square;
      }
      return ret;
    })

// for sse,sse2,sse3,ssse3,sse4,popcnt
OB_DECLARE_SSE42_SPECIFIC_CODE(inline static int l2_square(const float *a, const float *b,
                                                           const int64_t len, double &square) {
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
})

OB_DECLARE_AVX_ALL_CODE(
    inline static int l2_square_simd8_avx256(const float *a, const float *b, const int64_t len,
                                             double &square) {
      int ret = OB_SUCCESS;
      square = 0;
      const int64_t batch = 256 / (sizeof(float) * 8);  // 8
      int64_t dim = len >> 3 << 3;
      __m256 vsum = _mm256_set1_ps(0);
      for (int64_t i = 0; i < dim; i += batch) {
        VEC_SUM(__m256, _mm256_loadu_ps, _mm256_sub_ps, _mm256_mul_ps, _mm256_add_ps, vsum)
      }
      vsum = _mm256_hadd_ps(vsum, vsum);
      vsum = _mm256_hadd_ps(vsum, vsum);
      square = _mm_cvtss_f32(_mm256_castps256_ps128(vsum)) + _mm_cvtss_f32(_mm256_extractf128_ps(vsum, 1));
      return ret;
    }

    inline static int l2_square_simd8_avx256_extra(const float *a, const float *b,
                                                   const int64_t len, double &square) {
      int ret = OB_SUCCESS;
      int64_t dim = len >> 3 << 3;
      double extra_square = 0;
      square = 0;
      if (OB_FAIL(l2_square_simd8_avx256(a, b, dim, square))) {
        LIB_LOG(WARN, "failed to cal l2 square", K(ret), K(len), K(dim), K(square));
      } else if (0 < len - dim
                 && OB_FAIL(
                     l2_square_simd4_avx128_extra(a + dim, b + dim, len - dim, extra_square))) {
        LIB_LOG(WARN, "failed to cal l2 square", K(ret), K(len), K(dim), K(extra_square));
      } else {
        square += extra_square;
      }
      return ret;
    })

// for avx2
OB_DECLARE_AVX_AND_AVX2_CODE(inline static int l2_square(const float *a, const float *b,
                                                         const int64_t len, double &square) {
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
})

// for avx512f,avx512bw,avx512vl
OB_DECLARE_AVX512_SPECIFIC_CODE(
    OB_INLINE static int l2_square_simd16_avx512(const float *a, const float *b, const int64_t len,
                                                 double &square) {
      int ret = OB_SUCCESS;
      square = 0;
      const int64_t batch = 512 / (sizeof(float) * 8);  // 16
      int64_t dim = len >> 4 << 4;
      __m512 vsum = _mm512_set1_ps(0);
      for (int64_t i = 0; i < dim; i += batch) {
        __m512 diff = _mm512_sub_ps(_mm512_loadu_ps(a + i), _mm512_loadu_ps(b + i));
        vsum = _mm512_fmadd_ps(diff, diff, vsum);  // diff * diff + sum
      }
      square = _mm512_reduce_add_ps(vsum);
      return ret;
    }

    OB_INLINE static int l2_square_simd16_avx512_extra(const float *a, const float *b,
                                                       const int64_t len, double &square) {
      int ret = OB_SUCCESS;
      int64_t dim = len >> 4 << 4;
      double extra_square = 0;
      square = 0;
      if (OB_FAIL(l2_square_simd16_avx512(a, b, dim, square))) {
        LIB_LOG(WARN, "failed to cal l2 square", K(ret), K(len), K(dim), K(square));
      } else if (0 < len - dim
                 && OB_FAIL(
                     l2_square_simd8_avx256_extra(a + dim, b + dim, len - dim, extra_square))) {
        LIB_LOG(WARN, "failed to cal l2 square", K(ret), K(len), K(dim), K(extra_square));
      }
      if (OB_SUCC(ret)) {
        square += extra_square;
      }
      return ret;
    }

    inline static int l2_square(const float *a, const float *b, const int64_t len, double &square) {
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
    })

}  // namespace common
}  // namespace oceanbase
#endif
