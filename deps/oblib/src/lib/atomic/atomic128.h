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

#ifndef OCEANBASE_CLOG_ATOMIC128_
#define OCEANBASE_CLOG_ATOMIC128_

#include <cstdint>
#include "lib/utility/ob_macro_utils.h"

namespace types
{
struct uint128_t
{
  uint64_t lo;
  uint64_t hi;
}
__attribute__((__aligned__(16)));
}

#if defined(__x86_64__)
inline bool cas128(volatile types::uint128_t *src, types::uint128_t *cmp, types::uint128_t with)
{
  bool result;
  __asm__ __volatile__
  (
      "\n\tlock cmpxchg16b %1"
      "\n\tsetz %0\n"
      : "=q"(result), "+m"(*src), "+d"(cmp->hi), "+a"(cmp->lo)
      : "c"(with.hi), "b"(with.lo)
      : "cc"
  );
  return result;
}

inline void load128(__uint128_t &dest, types::uint128_t *src)
{
  __asm__ __volatile__("\n\txor %%rax, %%rax;"
                       "\n\txor %%rbx, %%rbx;"
                       "\n\txor %%rcx, %%rcx;"
                       "\n\txor %%rdx, %%rdx;"
                       "\n\tlock cmpxchg16b %1;\n"
                       : "=&A"(dest)
                       : "m"(*src)
                       : "%rbx", "%rcx", "cc");
}

#define CAS128(src, cmp, with) cas128((types::uint128_t*)(src), ((types::uint128_t*)&(cmp)), *((types::uint128_t*)&(with)))
#define LOAD128(dest, src) load128((__uint128_t&)(dest), (types::uint128_t*)(src))

#elif defined(__aarch64__)
inline void load128(__uint128_t  &target, types::uint128_t  *source )
{
  __uint128_t *ptr = reinterpret_cast<__uint128_t*>(&target);
  __uint128_t *desired = reinterpret_cast<__uint128_t*>(source);
  __atomic_load((__uint128_t *)desired,ptr, __ATOMIC_ACQUIRE);
}

/* lock free */
inline void load128_lf(types::uint128_t &dst, types::uint128_t *src)
{
    __asm__ __volatile__("ldaxp %0, %1, %2;\n"
    : "=&r" (dst.lo),
      "=&r" (dst.hi)
    : "Q" (src->lo)
    : "memory");
}

inline uint32_t store128_lf(types::uint128_t *dst, types::uint128_t src)
{
    uint32_t ret = 1;
    __asm__ __volatile__("stlxp %w0, %2, %3, %1;\n"
    : "=&r" (ret),
      "=Q" (dst->lo)
    : "r" (src.lo),
      "r" (src.hi)
    : "memory");

    return ret;
}

inline bool cas128_lf(types::uint128_t  *dst, types::uint128_t & expected, types::uint128_t val)
{
  int ret=0;
  types::uint128_t old;

  do {
    load128_lf(old, dst);
    if (old.lo == expected.lo && old.hi == expected.hi)
        ret = store128_lf(dst, val);
    else {
        expected = old;
        return false;
    }
  } while(OB_UNLIKELY(ret));

  return true;
}

#define CAS128_ASM(src, cmp, with) __atomic_compare_exchange(((types::uint128_t*)(src)), ((types::uint128_t*)(&(cmp))), ((types::uint128_t*)&(with)),false,__ATOMIC_SEQ_CST,__ATOMIC_SEQ_CST)
#define LOAD128_ASM(dest, src) load128_lf((types::uint128_t&)(dest), (types::uint128_t*)(src))
#define CAS128(src, cmp, with) cas128_lf(((types::uint128_t*)(src)), *((types::uint128_t*)&(cmp)), *((types::uint128_t*)&(with)))
#define LOAD128(dest, src) load128_lf((types::uint128_t&)(dest), (types::uint128_t*)(src))

#else
  #error arch unsupported
#endif
#endif //OCEANBASE_CLOG_ATOMIC128_
