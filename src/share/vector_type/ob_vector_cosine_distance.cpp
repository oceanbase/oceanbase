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

template <>
int ObVectorCosineDistance<float>::cosine_distance_func(const float *a, const float *b, const int64_t len, double &distance) {
  int ret = OB_SUCCESS;
  double similarity = 0;
  if (OB_FAIL(cosine_similarity_func(a, b, len, similarity))) {
    if (OB_ERR_NULL_VALUE != ret) {
      LIB_LOG(WARN, "failed to cal cosine similarity", K(ret));
    }
  } else if (::isnan(similarity)) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "cosine value is nan", K(ret), K(similarity));
      FORWARD_USER_ERROR(OB_INVALID_ARGUMENT, "cosine value is nan");
  } else {
    distance = get_cosine_distance(similarity);
  }
  return ret;
}

template <>
int ObVectorCosineDistance<uint8_t>::cosine_distance_func(const uint8_t *a, const uint8_t *b, const int64_t len, double &distance) {
  int ret = OB_SUCCESS;
  double similarity = 0;
  if (OB_FAIL(cosine_similarity_func(a, b, len, similarity))) {
    if (OB_ERR_NULL_VALUE != ret) {
      LIB_LOG(WARN, "failed to cal cosine similarity", K(ret));
    }
  } else {
    distance = get_cosine_distance(similarity);
  }
  return ret;
}

template <>
int ObVectorCosineDistance<float>::cosine_similarity_func(const float *a, const float *b, const int64_t len, double &similarity)
{
  int ret = OB_SUCCESS;
  #if OB_USE_MULTITARGET_CODE
    if (common::is_arch_supported(ObTargetArch::AVX512)) {
      ret = common::specific::avx512::cosine_similarity(a, b, len, similarity);
    } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
      ret = common::specific::avx2::cosine_similarity(a, b, len, similarity);
    } else {
      ret = common::specific::normal::cosine_similarity(a, b, len, similarity);
    }
  #elif defined(__aarch64__)
    if (common::is_arch_supported(ObTargetArch::NEON)) {
      ret = cosine_similarity_neon(a, b, len, similarity);
    } else {
      ret = common::specific::normal::cosine_similarity(a, b, len, similarity);
    }
  #else
    ret = common::specific::normal::cosine_similarity(a, b, len, similarity);
  #endif
  return ret;
}

template <>
int ObVectorCosineDistance<uint8_t>::cosine_similarity_func(const uint8_t *a, const uint8_t *b, const int64_t len, double &similarity)
{
  return cosine_similarity_normal(a, b, len, similarity);
}

}
}
