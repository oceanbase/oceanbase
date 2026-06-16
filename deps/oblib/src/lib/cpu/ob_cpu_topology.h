/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_OB_CPU_TOPOLOGY_
#define OCEANBASE_LIB_OB_CPU_TOPOLOGY_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace common
{

int64_t get_cpu_count();

enum class CpuFlag { SSE4_2 = 0, AVX, AVX2, AVX512BW, NEON, MAX };
class CpuFlagSet {
  public:
  DISABLE_COPY_ASSIGN(CpuFlagSet);
  bool have_flag(const CpuFlag flag) const;
  CpuFlagSet();
private:
  void init_from_cpu(uint64_t& flags);
  int init_from_os(uint64_t& flags);
  int64_t flags_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_OB_CPU_TOPOLOGY_
