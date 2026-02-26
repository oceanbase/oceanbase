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

#ifndef OCEANBASE_LIB_OB_VECTOR_ADD_H_
#define OCEANBASE_LIB_OB_VECTOR_ADD_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"
#include "common/object/ob_obj_compare.h"
#include "ob_vector_op_common.h"

namespace oceanbase
{
namespace common
{
struct ObVectorAdd
{
  // a = a + b
  static int calc(float *a, float *b, const int64_t len);
};

OB_INLINE static int vector_add_normal(float *a, const float *b, const int64_t len)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < len; ++i) {
    a[i] = a[i] + b[i];
  }
  return ret;
}

// for others
OB_DECLARE_DEFAULT_CODE(inline static int vector_add(float *a, const float *b, const int64_t len) {
  return vector_add_normal(a, b, len);
})

OB_DECLARE_SSE_AND_AVX_CODE(
    inline static int vector_add_simd4_avx128(float *a, const float *b, const int64_t len) {
      int ret = OB_SUCCESS;
      const int64_t batch = 128 / (sizeof(float) * 8);  // 4
      int64_t dim = len >> 2 << 2;

      float sum[4] CACHE_ALIGNED;
      for (int64_t i = 0; i < dim; i += batch) {
        _mm_store_ps(sum, _mm_add_ps(_mm_loadu_ps(a + i), _mm_loadu_ps(b + i)));
        for (int64_t j = 0; j < 4; j++) {
          a[i + j] = sum[j];
        }
      }
      return ret;
    }

    inline static int vector_add_simd4_avx128_extra(float *a, const float *b, const int64_t len) {
      int ret = OB_SUCCESS;
      int64_t dim = len >> 2 << 2;
      if (OB_FAIL(vector_add_simd4_avx128(a, b, dim))) {
        LIB_LOG(WARN, "failed to cal vector add", K(ret), K(len), K(dim));
      } else if (0 < len - dim && OB_FAIL(vector_add_normal(a + dim, b + dim, len - dim))) {
        LIB_LOG(WARN, "failed to cal vector add", K(ret), K(len), K(dim));
      }
      return ret;
    })

// for sse,sse2,sse3,ssse3,sse4,popcnt
OB_DECLARE_SSE42_SPECIFIC_CODE(inline static int vector_add(float *a, const float *b,
                                                            const int64_t len) {
  int ret = OB_SUCCESS;
  if (4 < len) {
    if (OB_FAIL(vector_add_simd4_avx128_extra(a, b, len))) {
      LIB_LOG(WARN, "failed to cal add", K(ret));
    }
  } else if (OB_FAIL(vector_add_normal(a, b, len))) {
    LIB_LOG(WARN, "failed to cal add", K(ret));
  }
  return ret;
})

OB_DECLARE_AVX_ALL_CODE(
    inline static int vector_add_simd8_avx256(float *a, const float *b, const int64_t len) {
      int ret = OB_SUCCESS;
      const int64_t batch = 256 / (sizeof(float) * 8);  // 8
      int64_t dim = len >> 3 << 3;

      __m256 vsum = _mm256_set1_ps(0);
      float sum[8] CACHE_ALIGNED;
      for (int64_t i = 0; i < dim; i += batch) {
        vsum = _mm256_add_ps(_mm256_loadu_ps(a + i), _mm256_loadu_ps(b + i));
        _mm256_store_ps(sum, vsum);
        for (int64_t j = 0; j < 8; j++) {
          a[i + j] = sum[j];
        }
      }
      return ret;
    }

    inline static int vector_add_simd8_avx256_extra(float *a, const float *b, const int64_t len) {
      int ret = OB_SUCCESS;
      int64_t dim = len >> 3 << 3;
      if (OB_FAIL(vector_add_simd8_avx256(a, b, dim))) {
        LIB_LOG(WARN, "failed to cal vector add", K(ret), K(len), K(dim));
      } else if (0 < len - dim
                 && OB_FAIL(vector_add_simd4_avx128_extra(a + dim, b + dim, len - dim))) {
        LIB_LOG(WARN, "failed to cal vector add", K(ret), K(len), K(dim));
      }
      return ret;
    })

// for avx2
OB_DECLARE_AVX_AND_AVX2_CODE(inline static int vector_add(float *a, const float *b,
                                                          const int64_t len) {
  int ret = OB_SUCCESS;
  if (8 < len) {
    if (OB_FAIL(vector_add_simd8_avx256_extra(a, b, len))) {
      LIB_LOG(WARN, "failed to cal add", K(ret));
    }
  } else if (4 < len) {
    if (OB_FAIL(vector_add_simd4_avx128_extra(a, b, len))) {
      LIB_LOG(WARN, "failed to cal add", K(ret));
    }
  } else if (OB_FAIL(vector_add_normal(a, b, len))) {
    LIB_LOG(WARN, "failed to cal add", K(ret));
  }
  return ret;
})

// for avx512f,avx512bw,avx512vl
OB_DECLARE_AVX512_SPECIFIC_CODE(
    OB_INLINE static int vector_add_simd16_avx512(float *a, const float *b, const int64_t len) {
      int ret = OB_SUCCESS;
      const int64_t batch = 512 / (sizeof(float) * 8);  // 16
      int64_t dim = len >> 4 << 4;

      __m512 vsum = _mm512_set1_ps(0);
      float sum[16] CACHE_ALIGNED;
      for (int64_t i = 0; i < dim; i += batch) {
        vsum = _mm512_add_ps(_mm512_loadu_ps(a + i), _mm512_loadu_ps(b + i));
        // NOTE(liyao): use _mm512_store_ps will core because memory of a maybe misaligned
        _mm512_storeu_ps(&a[i], vsum);
      }
      return ret;
    }

    OB_INLINE static int vector_add_simd16_avx512_extra(float *a, const float *b,
                                                        const int64_t len) {
      int ret = OB_SUCCESS;
      int64_t dim = len >> 4 << 4;
      if (OB_FAIL(vector_add_simd16_avx512(a, b, dim))) {
        LIB_LOG(WARN, "failed to cal vector add", K(ret), K(len), K(dim));
      } else if (0 < len - dim
                 && OB_FAIL(vector_add_simd8_avx256_extra(a + dim, b + dim, len - dim))) {
        LIB_LOG(WARN, "failed to cal vector add", K(ret), K(len), K(dim));
      }
      return ret;
    }

    inline static int vector_add(float *a, const float *b, const int64_t len) {
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
    })

}  // namespace common
}  // namespace oceanbase
#endif