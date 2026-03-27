/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_vector_ip_distance.h"
namespace oceanbase
{
namespace common
{

template<>
int ObVectorIpDistance<float>::ip_distance_func(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
#if OB_USE_MULTITARGET_CODE
  // if (common::is_arch_supported(ObTargetArch::AVX512)) {
  //   ret = common::specific::avx512::ip_distance(a, b, len, distance);
  // } else
  if (common::is_arch_supported(ObTargetArch::AVX2)) {
    ret = common::specific::avx2::ip_distance(a, b, len, distance);
  } else if (common::is_arch_supported(ObTargetArch::AVX)) {
    ret = common::specific::avx::ip_distance(a, b, len, distance);
  } else if (common::is_arch_supported(ObTargetArch::SSE42)) {
    ret = common::specific::sse42::ip_distance(a, b, len, distance);
  } else {
    ret = common::specific::normal::ip_distance(a, b, len, distance);
  }
#elif defined(__aarch64__)
  if (common::is_arch_supported(ObTargetArch::NEON)) {
    ret = ip_distance_neon(a, b, len, distance);
  } else {
    ret = common::specific::normal::ip_distance(a, b, len, distance);
  }
#else
  ret = common::specific::normal::ip_distance(a, b, len, distance);
#endif
  return ret;
}

template<>
int ObVectorIpDistance<uint8_t>::ip_distance_func(const uint8_t *a, const uint8_t *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
    distance += a[i] * b[i];
  }
  return ret;
}

template <>
void ObVectorIpDistance<float>::fvec_inner_products_ny(float* ip, const float* x, const float* y, size_t d, size_t ny)
{
  #if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(ObTargetArch::SSE42)) {
    common::specific::sse42::fvec_inner_products_ny(ip, x, y, d, ny);
  } else {
    common::specific::normal::fvec_inner_products_ny(ip, x, y, d, ny);
  }
#elif defined(__aarch64__)
  if (common::is_arch_supported(ObTargetArch::NEON)) {
    fvec_inner_products_ny_neon(ip, x, y, d, ny);
  } else {
    common::specific::normal::fvec_inner_products_ny(ip, x, y, d, ny);
  }
#else
  common::specific::normal::fvec_inner_products_ny(ip, x, y, d, ny);
#endif
}

}
}
