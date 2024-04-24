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

#include "lib/vector/ob_vector_l2_distance.h"
namespace oceanbase
{
namespace common
{
  int ObVectorL2Distance::l2_square_func(const float *a, const float *b, const int64_t len, double &square) {
    int ret = OB_SUCCESS;
#if OB_USE_MULTITARGET_CODE
    if (common::is_arch_supported(ObTargetArch::AVX512)) {
      ret = common::specific::avx512::l2_square(a, b, len, square);
    } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
      ret = common::specific::avx2::l2_square(a, b, len, square);
    } else {
      ret = common::specific::normal::l2_square(a, b, len, square);
    }
#else
    ret = common::specific::normal::l2_square(a, b, len, square);
#endif
    return ret;
  }
  int ObVectorL2Distance::l2_distance_func(const float *a, const float *b, const int64_t len, double &distance) {
    int ret = OB_SUCCESS;
    double square = 0;
    distance = 0;
    if (OB_FAIL(ObVectorL2Distance::l2_square_func(a, b, len, square))) {
      LIB_LOG(WARN, "failed to cal l2 square", K(ret));
    } else {
      distance = sqrt(square);
    }
    return ret;
  }
  int ObVectorL2Distance::vector_add_func(float *a, const float *b, const int64_t len) {
    int ret = OB_SUCCESS;
#if OB_USE_MULTITARGET_CODE
    if (common::is_arch_supported(ObTargetArch::AVX512)) {
      ret = common::specific::avx512::vector_add(a, b, len);
    } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
      ret = common::specific::avx2::vector_add(a, b, len);
    } else {
      ret = common::specific::normal::vector_add(a, b, len);
    }
#else
    ret = common::specific::normal::vector_add(a, b, len);
#endif
    return ret;
  }
}
}