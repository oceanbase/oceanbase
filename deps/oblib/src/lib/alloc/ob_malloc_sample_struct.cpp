/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/alloc/ob_malloc_sample_struct.h"

namespace oceanbase
{
namespace lib
{
#if defined(__x86_64__)
int32_t ObMallocSampleLimiter::min_sample_size = 16384;
#else
int32_t ObMallocSampleLimiter::min_sample_size = 0;
#endif
bool malloc_sample_allowed(const int64_t size, const ObMemAttr &attr)
{
  return ObMallocSampleLimiter::malloc_sample_allowed(size, attr);
}
} // end of namespace lib
} // end of namespace oceanbase
