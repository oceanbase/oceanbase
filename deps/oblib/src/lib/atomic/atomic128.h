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

namespace types {
struct uint128_t {
  uint64_t lo;
  uint64_t hi;
} __attribute__((__aligned__(16)));
}  // namespace types

#if defined(__x86_64__)
inline bool cas128(volatile types::uint128_t* src, types::uint128_t* cmp, types::uint128_t with)
{
  bool result;
  __asm__ __volatile__("\n\tlock cmpxchg16b %1"
                       "\n\tsetz %0\n"
                       : "=q"(result), "+m"(*src), "+d"(cmp->hi), "+a"(cmp->lo)
                       : "c"(with.hi), "b"(with.lo)
                       : "cc");
  return result;
}

inline void load128(__uint128_t& dest, types::uint128_t* src)
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

#define CAS128(src, cmp, with) \
  cas128((types::uint128_t*)(src), ((types::uint128_t*)&(cmp)), *((types::uint128_t*)&(with)))
#define LOAD128(dest, src) load128((__uint128_t&)(dest), (types::uint128_t*)(src))

#elif defined(__aarch64__)
inline bool cas128(volatile types::uint128_t* target, types::uint128_t* cmp, types::uint128_t with)
{
  int ret = 0;

  types::uint128_t tmp;
  tmp.lo = cmp->lo;
  tmp.hi = cmp->hi;
  ret = __atomic_compare_exchange((types::uint128_t*)(target),
      ((types::uint128_t*)&(tmp)),
      ((types::uint128_t*)&(with)),
      false,
      __ATOMIC_SEQ_CST,
      __ATOMIC_SEQ_CST);
  return ret;
}

inline void load128(__uint128_t& target, types::uint128_t* source)
{
  __uint128_t* ptr = reinterpret_cast<__uint128_t*>(&target);
  __uint128_t* desired = reinterpret_cast<__uint128_t*>(source);
  __atomic_load((__uint128_t*)desired, ptr, __ATOMIC_SEQ_CST);
}
#define CAS128_tmp(src, cmp, with) \
  cas128((types::uint128_t*)(src), ((types::uint128_t*)(&(cmp))), *((types::uint128_t*)(&(with))))
#define LOAD128_asm(dest, src) load128((__uint128_t&)(dest), ((types::uint128_t*)(src)))
#define CAS128(src, cmp, with)                          \
  __atomic_compare_exchange(((types::uint128_t*)(src)), \
      ((types::uint128_t*)(&(cmp))),                    \
      ((types::uint128_t*)&(with)),                     \
      false,                                            \
      __ATOMIC_SEQ_CST,                                 \
      __ATOMIC_SEQ_CST)
#define LOAD128(dest, src) __atomic_load(((types::uint128_t*)(src)), ((types::uint128_t*)(&(dest))), __ATOMIC_SEQ_CST)

#else
#error arch unsupported
#endif
#endif  // OCEANBASE_CLOG_ATOMIC128_
