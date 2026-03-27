/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON
#include "common/ob_target_specific.h"

namespace oceanbase
{
namespace common
{

uint32_t arches;
void init_arches()
{
  arches = 0;
  const CpuFlagSet flags;
  if (flags.have_flag(CpuFlag::SSE4_2)) {
    arches |= static_cast<uint32_t>(ObTargetArch::SSE42);
  }
  if (flags.have_flag(CpuFlag::AVX)) {
    arches |= static_cast<uint32_t>(ObTargetArch::AVX);
  }
  if (flags.have_flag(CpuFlag::AVX2)) {
    arches |= static_cast<uint32_t>(ObTargetArch::AVX2);
  }
  if (flags.have_flag(CpuFlag::AVX512BW)) {
    arches |= static_cast<uint32_t>(ObTargetArch::AVX512);
  }
  if (flags.have_flag(CpuFlag::NEON)) {
    arches |= static_cast<uint32_t>(ObTargetArch::NEON);
  }
}

} // namespace common
} // namespace oceanbase