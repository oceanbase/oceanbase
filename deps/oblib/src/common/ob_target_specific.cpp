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