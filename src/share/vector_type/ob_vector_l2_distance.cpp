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
template<>
int ObVectorL2Distance<float>::l2_square_func(const float *a, const float *b, const int64_t len, double &square)
{
  int ret = OB_SUCCESS;
#if OB_USE_MULTITARGET_CODE
    // AVX512 slower than AVX2, maybe using AVX512 lower CPU frequency which leads to worse performance.
    if (common::is_arch_supported(ObTargetArch::AVX2)) {
      ret = common::specific::avx2::l2_square(a, b, len, square);
    } else if (common::is_arch_supported(ObTargetArch::AVX)) {
      ret = common::specific::avx::l2_square(a, b, len, square);
    } else if (common::is_arch_supported(ObTargetArch::SSE42)) {
      ret = common::specific::sse42::l2_square(a, b, len, square);
    } else {
      ret = common::specific::normal::l2_square(a, b, len, square);
    }
#else
    ret = common::specific::normal::l2_square(a, b, len, square);
#endif
  return ret;
}
template <>
int ObVectorL2Distance<uint8_t>::l2_square_func(const uint8_t *a, const uint8_t *b, const int64_t len, double &square)
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

template <>
float ObVectorL2Distance<float>::l2_square_flt_func(const float *a, const float *b, const int64_t len)
{
#if OB_USE_MULTITARGET_CODE
    // AVX512 slower than AVX2, maybe using AVX512 lower CPU frequency which leads to worse performance.
    if (common::is_arch_supported(ObTargetArch::AVX2)) {
      return common::specific::avx2::l2_square_flt(a, b, len);
    } else if (common::is_arch_supported(ObTargetArch::AVX)) {
      return common::specific::avx::l2_square_flt(a, b, len);
    } else if (common::is_arch_supported(ObTargetArch::SSE42)) {
      return common::specific::sse42::l2_square_flt(a, b, len);
    } else {
      return common::specific::normal::l2_square_flt(a, b, len);
    }
#else
    return common::specific::normal::l2_square_flt(a, b, len);
#endif
}

template <>
float ObVectorL2Distance<float>::l2_norm_square(const float *a, const int64_t len)
{
#if OB_USE_MULTITARGET_CODE
    if (common::is_arch_supported(ObTargetArch::AVX512)) {
      return common::specific::avx512::l2_norm_square(a, len);
    } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
      return common::specific::avx2::l2_norm_square(a, len);
    } else if (common::is_arch_supported(ObTargetArch::SSE42)) {
      return common::specific::sse42::l2_norm_square(a, len);
    } else {
      return common::specific::normal::l2_norm_square(a, len);
    }
#else
    return common::specific::normal::l2_norm_square(a, len);
#endif
}

template<>
void ObVectorL2Distance<float>::fvec_madd(size_t n, const float* a, float bf, const float* b, float* c)
{
#if OB_USE_MULTITARGET_CODE
  // AVX512 slower than AVX2, maybe using AVX512 lower CPU frequency which leads to worse performance.
  if (common::is_arch_supported(ObTargetArch::AVX2)) {
    return common::specific::avx2::fvec_madd(n, a, bf, b, c);
  } else if (common::is_arch_supported(ObTargetArch::SSE42)) {
    return common::specific::sse42::fvec_madd(n, a, bf, b, c);
  } else {
    return common::specific::normal::fvec_madd(n, a, bf, b, c);
  }
#else
  return common::specific::normal::fvec_madd(n, a, bf, b, c);
#endif
}

template<>
void ObVectorL2Distance<float>::distance_four_codes(
    const size_t M, const size_t nbits, const float* sim_table,
    const uint8_t* __restrict code0, const uint8_t* __restrict code1,
    const uint8_t* __restrict code2, const uint8_t* __restrict code3,
    float &result0, float &result1, float &result2, float &result3)
{
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(ObTargetArch::AVX2)) {
    return common::specific::avx2::distance_four_codes(M, nbits, sim_table, code0, code1, code2, code3, result0, result1, result2, result3);
  } else {
    return common::specific::normal::distance_four_codes(M, nbits, sim_table, code0, code1, code2, code3, result0, result1, result2, result3);
  }
#else
  return common::specific::normal::distance_four_codes(M, nbits, sim_table, code0, code1, code2, code3, result0, result1, result2, result3);
#endif
}

template<>
float ObVectorL2Distance<float>::distance_one_code(const size_t M, const size_t nbits, const float* sim_table, const uint8_t* code)
{
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(ObTargetArch::AVX2)) {
    return common::specific::avx2::distance_one_code(M, nbits, sim_table, code);
  } else {
    return common::specific::normal::distance_one_code(M, nbits, sim_table, code);
  }
#else
  return common::specific::normal::distance_one_code(M, nbits, sim_table, code);
#endif
}

}
}
