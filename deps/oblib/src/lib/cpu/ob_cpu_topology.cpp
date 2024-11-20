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

#include "lib/ob_define.h"
#include "lib/container/ob_bit_set.h"

namespace oceanbase {
namespace common {

int64_t __attribute__((weak)) get_cpu_count()
{
  return get_cpu_num();
}

static bool cpu_haveOSXSAVE();
static bool cpu_have_sse42();
static bool cpu_have_avx();
static bool cpu_have_avx2();
static bool cpu_have_avxf();
static bool cpu_have_avx512bw();

const CpuFlagSet& CpuFlagSet::get_instance()
{
  static CpuFlagSet instance;
  return instance;
}

bool CpuFlagSet::have_flag(const CpuFlag flag) const
{
  return flags_ & (1 << (int)flag);
}

CpuFlagSet::CpuFlagSet() : flags_(0)
{
  uint64_t flags_from_cpu = init_from_cpu();
  uint64_t flags_from_os = init_from_os();
  if (flags_from_cpu != flags_from_os) {
    COMMON_LOG_RET(ERROR,
        OB_ERR_SYS,
        "There is a mismatch between the cpu flags from cpu and those from os, "
        "ISA extension like avx512bw may be not supported by your virtualization setup");
    flags_ = flags_from_cpu & flags_from_os;
  } else {
    flags_ = flags_from_cpu;
  }
}

uint64_t CpuFlagSet::init_from_cpu()
{
  uint64_t flags = 0;
#define cpu_have(flag, FLAG)            \
  if (cpu_have_##flag()) {              \
    flags |= (1 << (int)CpuFlag::FLAG); \
  }
  cpu_have(sse42, SSE4_2);
  cpu_have(avx, AVX);
  cpu_have(avx2, AVX2);
  cpu_have(avx512bw, AVX512BW);
  return flags;
}

uint64_t CpuFlagSet::init_from_os()
{
  uint64_t flags = 0;
  int ret = OB_SUCCESS;
  const char* const CPU_FLAG_CMDS[(int)CpuFlag::MAX] = {
      "grep -E ' sse4_2( |$)' /proc/cpuinfo",
      "grep -E ' avx( |$)' /proc/cpuinfo",
      "grep -E ' avx2( |$)' /proc/cpuinfo",
      "grep -E ' avx512bw( |$)' /proc/cpuinfo"};
  for (int i = 0; i < (int)CpuFlag::MAX; ++i) {
    flags |= (0 == system(CPU_FLAG_CMDS[i])) << i;
  }
  return flags;
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
bool cpu_have_sse42()
{
#if defined(__x86_64__)
  int regs[4];
  get_cpuid(regs, 0x1);
  return regs[2] >> 20 & 1;
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
bool cpu_have_avx2()
{
#if defined(__x86_64__)
  int regs[4];
  get_cpuid(regs, 0x7);
  return cpu_have_avx() && (regs[1] >> 5 & 1);
#else
  return false;
#endif
}
bool cpu_have_avx512f()
{
#if defined(__x86_64__)
  int regs[4];
  get_cpuid(regs, 0x7);
  return regs[1] >> 16 & 1;
#else
  return false;
#endif
}
bool cpu_have_avx512bw()
{
#if defined(__x86_64__)
  int regs[4];
  get_cpuid(regs, 0x7);
  return cpu_have_avx512f() && (regs[1] >> 30 & 1);
#else
  return false;
#endif
}


} // common
} // oceanbase

