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

#include "ob_vector_div.h"
namespace oceanbase
{
namespace common
{
int ObVectorDiv::calc(float *a, float f, const int64_t len)
{
  int ret = OB_SUCCESS;
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(ObTargetArch::AVX512)) {
    ret = common::specific::avx512::vector_div(a, f, len);
  } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
    ret = common::specific::avx2::vector_div(a, f, len);
  } else if (common::is_arch_supported(ObTargetArch::AVX)) {
    ret = common::specific::avx::vector_div(a, f, len);
  } else if (common::is_arch_supported(ObTargetArch::SSE42)) {
    ret = common::specific::sse42::vector_div(a, f, len);
  } else {
    ret = common::specific::normal::vector_div(a, f, len);
  }
#else
  ret = common::specific::normal::vector_div(a, f, len);
#endif
  return ret;
}
}  // namespace common
}  // namespace oceanbase