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

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{
int64_t get_cpu_count();

#if defined(__x86_64__)
inline void get_cpuid(int reg[4], int func_id)
{
  __asm__("cpuid\n\t"
           : "=a"(reg[0]), "=b"(reg[1]), "=c"(reg[2]),
             "=d"(reg[3])
           : "a"(func_id), "c"(0));
}
inline uint64_t our_xgetbv(uint32_t xcr) noexcept
{
  uint32_t eax;
  uint32_t edx;
  __asm__ volatile("xgetbv"
                    : "=a"(eax), "=d"(edx)
                    : "c"(xcr));
  return (static_cast<uint64_t>(edx) << 32) | eax;
}
#endif
inline bool haveOSXSAVE();
inline bool have_sse42();
inline bool have_avx();
inline bool have_avx2();
inline bool have_avxf();
inline bool have_avx512bw();

inline bool haveOSXSAVE()
{
#if defined(__x86_64__)
  int regs[4];
  get_cpuid(regs, 0x1);
  return (regs[2] >> 27) & 1u;
#else
  return false;
#endif
}

inline bool have_sse42()
{
#if defined(__x86_64__)
  int regs[4];
  get_cpuid(regs, 0x1);
  return regs[2] >> 20 & 1;
#else
  return false;
#endif
}
inline bool have_avx()
{
#if defined(__x86_64__)
  int regs[4];
  get_cpuid(regs, 0x1);
  return haveOSXSAVE() && ((our_xgetbv(0) & 6u) == 6u) && (regs[2] >> 28 & 1);
#else
  return false;
#endif
}
inline bool have_avx2()
{
#if defined(__x86_64__)
  int regs[4];
  get_cpuid(regs, 0x7);
  return have_avx() && (regs[1] >> 5 & 1);
#else
  return false;
#endif
}
inline bool have_avx512f()
{
#if defined(__x86_64__)
  int regs[4];
  get_cpuid(regs, 0x7);
  return regs[1] >> 16 & 1;
#else
  return false;
#endif
}
inline bool have_avx512bw()
{
#if defined(__x86_64__)
  int regs[4];
  get_cpuid(regs, 0x7);
  return have_avx512f() && (regs[1] >> 30 & 1);
#else
  return false;
#endif
}

struct ObCpuFlagsCache
{
  static inline bool support_sse42() {
    static const bool is_supported = have_sse42();
    return is_supported;
  }

  static inline bool support_avx() {
    static const bool is_supported = have_avx();
    return is_supported;
  }

  static inline bool support_avx2() {
    static const bool is_supported = have_avx2();
    return is_supported;
  }

  static inline bool support_avx512() {
    static const bool is_supported = have_avx512bw();
    return is_supported;
  }
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_OB_CPU_TOPOLOGY_
