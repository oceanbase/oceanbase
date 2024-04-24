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

#include "lib/vector/ob_vector_cosine_distance.h"
namespace oceanbase
{
namespace common
{
  int ObVectorCosineDistance::cosine_similarity_func(const float *a, const float *b, const int64_t len, double &similarity) {
    int ret = OB_SUCCESS;
#if OB_USE_MULTITARGET_CODE
    if (common::is_arch_supported(ObTargetArch::AVX512)) {
      ret = common::specific::avx512::cosine_similarity(a, b, len, similarity);
    } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
      ret = common::specific::avx2::cosine_similarity(a, b, len, similarity);
    } else {
      ret = common::specific::normal::cosine_similarity(a, b, len, similarity);
    }
#else
    ret = common::specific::normal::cosine_similarity(a, b, len, similarity);
#endif
    return ret;
  }

  int ObVectorCosineDistance::cosine_distance_func(const float *a, const float *b, const int64_t len, double &distance) {
    int ret = OB_SUCCESS;
    double similarity = 0;
    if (OB_FAIL(ObVectorCosineDistance::cosine_similarity_func(a, b, len, similarity))) {
      LIB_LOG(WARN, "failed to cal cosine similaity", K(ret));
    } else {
      distance = get_cosine_distance(similarity);
    }
    return ret;
  }

  int ObVectorCosineDistance::angular_distance_func(const float *a, const float *b, const int64_t len, double &distance) {
    int ret = OB_SUCCESS;
    double cosine_sim = 0;
    if (OB_FAIL(ObVectorCosineDistance::cosine_similarity_func(a, b, len, cosine_sim))) {
      LIB_LOG(WARN, "failed to cal cosine similaity", K(ret));
    } else {
      // 夹角弧度值
      // double angle_rad = std::acos(cosine_sim);
      distance = std::acos(cosine_sim);
      // 弧度值转为度数值
      // distance = angle_rad * my_pi;
    }
    return ret;
  }
}
}