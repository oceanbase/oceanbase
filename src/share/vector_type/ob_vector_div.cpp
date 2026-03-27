/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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