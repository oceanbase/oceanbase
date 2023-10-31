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

#define USING_LOG_PREFIX COMMON

#include "ob_target_specific.h"
#include "lib/cpu/ob_cpu_topology.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{
uint32_t get_supported_archs()
{
  uint32_t result = 0;
  if (ObCpuFlagsCache::support_sse42()) {
    result |= static_cast<uint32_t>(ObTargetArch::SSE42);
  }
  if (ObCpuFlagsCache::support_avx()) {
    result |= static_cast<uint32_t>(ObTargetArch::AVX);
  }
  if (ObCpuFlagsCache::support_avx2()) {
    result |= static_cast<uint32_t>(ObTargetArch::AVX2);
  }
  if (ObCpuFlagsCache::support_avx512()) {
    result |= static_cast<uint32_t>(ObTargetArch::AVX512);
  }
  return result;
}

bool is_arch_supported(ObTargetArch arch)
{
  static uint32_t arches = get_supported_archs();
  return arch == ObTargetArch::Default || (arches & static_cast<uint32_t>(arch));
}
} // namespace common
} // namespace oceanbase