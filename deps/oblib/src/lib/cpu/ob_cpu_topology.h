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

#ifndef OCEANBASE_LIB_OB_CPU_TOPOLOGY_
#define OCEANBASE_LIB_OB_CPU_TOPOLOGY_

#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"

#include "lib/container/ob_bit_set.h"

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
