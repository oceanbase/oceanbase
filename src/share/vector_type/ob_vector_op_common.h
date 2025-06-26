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

#ifndef OCEANBASE_LIB_OB_VECTOR_OP_COMMON_H_
#define OCEANBASE_LIB_OB_VECTOR_OP_COMMON_H_

#include "common/ob_target_specific.h"
#include "ob_vector_pq_coder.h"

#if OB_USE_MULTITARGET_CODE
#include <emmintrin.h>
#include <immintrin.h>
#include <tmmintrin.h>
#include <smmintrin.h>
#include <nmmintrin.h>
#endif

#if defined (__aarch64__)
#include <arm_neon.h>
#endif

namespace oceanbase
{
namespace common
{
#define OB_DECLARE_SSE_AND_AVX_CODE(...)       \
  OB_DECLARE_SSE42_SPECIFIC_CODE(__VA_ARGS__)  \
  OB_DECLARE_AVX2_SPECIFIC_CODE(__VA_ARGS__)   \
  OB_DECLARE_AVX512_SPECIFIC_CODE(__VA_ARGS__) \
  OB_DECLARE_AVX_SPECIFIC_CODE(__VA_ARGS__)

#define OB_DECLARE_AVX_ALL_CODE(...)           \
  OB_DECLARE_AVX2_SPECIFIC_CODE(__VA_ARGS__)   \
  OB_DECLARE_AVX512_SPECIFIC_CODE(__VA_ARGS__) \
  OB_DECLARE_AVX_SPECIFIC_CODE(__VA_ARGS__)

#define OB_DECLARE_AVX_AND_AVX2_CODE(...)    \
  OB_DECLARE_AVX2_SPECIFIC_CODE(__VA_ARGS__) \
  OB_DECLARE_AVX_SPECIFIC_CODE(__VA_ARGS__)

// common simd utils
OB_DECLARE_SSE42_SPECIFIC_CODE(
  // reads 0 <= d < 4 floats as __m128
  inline static __m128 masked_read(int len, const float* a) {
    __attribute__((aligned(16))) float buf[4] = {0, 0, 0, 0};
    switch (len) {
      case 3:
        buf[2] = a[2];
      case 2:
        buf[1] = a[1];
      case 1:
        buf[0] = a[0];
    }
    return _mm_load_ps(buf);
  }
)


}  // namespace common
}  // namespace oceanbase
#endif