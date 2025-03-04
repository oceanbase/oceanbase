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

#include "lib/cpu/ob_cpu_topology.h"

#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include "lib/ob_define.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace common {

int64_t __attribute__((weak)) get_cpu_count()
{
  return get_cpu_num();
}

#if defined(__x86_64__)
void get_cpuid(int reg[4], int func_id)
{
  __asm__("cpuid\n\t"
           : "=a"(reg[0]), "=b"(reg[1]), "=c"(reg[2]),
             "=d"(reg[3])
           : "a"(func_id), "c"(0));
}

uint64_t our_xgetbv(uint32_t xcr) noexcept
{
  uint32_t eax;
  uint32_t edx;
  __asm__ volatile("xgetbv"
                    : "=a"(eax), "=d"(edx)
                    : "c"(xcr));
  return (static_cast<uint64_t>(edx) << 32) | eax;
}
#endif

bool cpu_haveOSXSAVE()
{
#if defined(__x86_64__)
  int regs[4];
  get_cpuid(regs, 0x1);
  return (regs[2] >> 27) & 1u;
#else
  return false;
#endif
}

bool cpu_have_avx()
{
#if defined(__x86_64__)
  int regs[4];
  get_cpuid(regs, 0x1);
  return cpu_haveOSXSAVE() && ((our_xgetbv(0) & 6u) == 6u) && (regs[2] >> 28 & 1);
#else
  return false;
#endif
}

} // common
} // oceanbase

