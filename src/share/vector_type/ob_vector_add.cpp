/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_vector_add.h"
namespace oceanbase
{
namespace common
{
int ObVectorAdd::calc(float *a, float *b, const int64_t len)
{
  int ret = OB_SUCCESS;
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(ObTargetArch::AVX512)) {
    ret = common::specific::avx512::vector_add(a, b, len);
  } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
    ret = common::specific::avx2::vector_add(a, b, len);
  } else if (common::is_arch_supported(ObTargetArch::AVX)) {
    ret = common::specific::avx::vector_add(a, b, len);
  } else if (common::is_arch_supported(ObTargetArch::SSE42)) {
    ret = common::specific::sse42::vector_add(a, b, len);
  } else {
    ret = common::specific::normal::vector_add(a, b, len);
  }
#else
  ret = common::specific::normal::vector_add(a, b, len);
#endif
  return ret;
}
}  // namespace common
}  // namespace oceanbase