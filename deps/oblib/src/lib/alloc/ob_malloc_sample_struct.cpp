/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "lib/alloc/ob_malloc_sample_struct.h"

namespace oceanbase
{
namespace lib
{
#if defined(__x86_64__)
int32_t ObMallocSampleLimiter::min_malloc_sample_interval = 16;
int32_t ObMallocSampleLimiter::max_malloc_sample_interval = 256;
#else
int32_t ObMallocSampleLimiter::min_malloc_sample_interval = 10000;
int32_t ObMallocSampleLimiter::max_malloc_sample_interval = 10000;
#endif

} // end of namespace lib
} // end of namespace oceanbase
