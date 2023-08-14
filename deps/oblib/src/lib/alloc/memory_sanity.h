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

#ifndef _MEMORY_SANITY_H_
#define _MEMORY_SANITY_H_

#ifndef ENABLE_SANITY
#define SANITY_ENABLE_CHECK_RANGE()
#define SANITY_DISABLE_CHECK_RANGE()
#define SANITY_POISON(args...)
#define SANITY_UNPOISON(args...)
#define SANITY_BOOL_EXPR(expr) false
#define SANITY_ADDR_IN_RANGE(args...) false
#define SANITY_TO_SHADOW(args...) 0x0
#define SANITY_TO_SHADOW_SIZE(args...) 0x0
#define SANITY_MIN_CANONICAL_ADDR 0x0
#define SANITY_MAX_CANONICAL_ADDR 0x0
#define SANITY_CHECK_RANGE(args...)
#else
#define _DEFINE_SANITY_GUARD(check, var_name) SanityCheckRangeGuard<check> var_name;
#define SANITY_ENABLE_CHECK_RANGE()  _DEFINE_SANITY_GUARD(true, CONCAT(sanity_guard, __COUNTER__))
#define SANITY_DISABLE_CHECK_RANGE() _DEFINE_SANITY_GUARD(false, CONCAT(sanity_guard, __COUNTER__))
#define SANITY_POISON(args...) sanity_poison(args)
#define SANITY_UNPOISON(args...) sanity_unpoison(args)
#define SANITY_BOOL_EXPR(expr) ({expr;})
#define SANITY_ADDR_IN_RANGE(args...) sanity_addr_in_range(args)
#define SANITY_TO_SHADOW(args...) sanity_to_shadow(args)
#define SANITY_TO_SHADOW_SIZE(size) sanity_to_shadow_size(size)
#define SANITY_MIN_CANONICAL_ADDR sanity_min_canonical_addr
#define SANITY_MAX_CANONICAL_ADDR sanity_max_canonical_addr
#define SANITY_CHECK_RANGE(args...) sanity_check_range(args)

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <dlfcn.h>
#include <stdlib.h>
#include "lib/utility/ob_macro_utils.h"

extern __thread bool enable_sanity_check;
extern "C"
{
  void memory_sanity_abort();
}

template<const bool CHECK>
class SanityCheckRangeGuard
{
public:
  SanityCheckRangeGuard()
    : bak_(enable_sanity_check)
  {
    enable_sanity_check = CHECK;
  }
  ~SanityCheckRangeGuard()
  {
    enable_sanity_check = bak_;
  }
private:
  const bool bak_;
};

static constexpr int64_t sanity_min_canonical_addr = 0x100000000000;
static constexpr int64_t sanity_max_canonical_addr = 0x4f210376cf1c;

static inline bool sanity_addr_in_range(const void *ptr)
{
  return (int64_t)ptr < sanity_max_canonical_addr && (int64_t)ptr >= sanity_min_canonical_addr;
}

static inline void* sanity_to_shadow(const void *ptr)
{
  return (void*)((uint64_t)ptr >> 3);
}

static inline int64_t sanity_to_shadow_size(int64_t size)
{
  return size >> 3;
}

static inline void sanity_poison(const void *ptr, ssize_t len)
{
  if (!sanity_addr_in_range(ptr)) return;
  if (((uint64_t)ptr & 0x7) != 0) abort();
  char *shadow = (char*)sanity_to_shadow(ptr);
  int32_t n_bytes = static_cast<int32_t>(sanity_to_shadow_size(len));
  if (n_bytes > 0) {
    static void *(*real_memset)(void *, int, size_t)
      = (__typeof__(real_memset)) dlsym(RTLD_NEXT, "memset");
    real_memset(shadow, 0xF0, n_bytes);
  }
  int8_t n_bits =  len & 0x7;
  if (n_bits > 0) {
    shadow[n_bytes] = 0xF0;
  }
}

static inline void sanity_unpoison(const void *ptr, ssize_t len)
{
  if (!sanity_addr_in_range(ptr)) return;
  if (((uint64_t)ptr & 0x7) != 0) abort();
  char *shadow = (char*)sanity_to_shadow(ptr);
  int32_t n_bytes = static_cast<int32_t>(sanity_to_shadow_size(len));
  if (n_bytes > 0) {
    static void *(*real_memset)(void *, int, size_t)
      = (__typeof__(real_memset)) dlsym(RTLD_NEXT, "memset");
    real_memset(shadow, 0x0, n_bytes);
  }
  int8_t n_bits =  len & 0x7;
  if (n_bits > 0) {
    shadow[n_bytes] = n_bits;
  }
}

static inline uint64_t sanity_align_down(uint64_t x, uint64_t align)
{
  return x & ~(align - 1);
}

static inline uint64_t sanity_align_up(uint64_t x, uint64_t align)
{
  return (x + (align - 1)) & ~(align - 1);
}

static inline void sanity_check_range(const void *ptr, ssize_t len)
{
  if (!enable_sanity_check) return;
  if (0 == len) return;
  if (!sanity_addr_in_range(ptr)) return;
  char *start = (char*)ptr;
  char *end = start + len;
  if (end <= start) memory_sanity_abort();
  char *start_align = (char*)sanity_align_up((uint64_t)start, 8);
  char *end_align = (char*)sanity_align_down((uint64_t)end, 8);
  if (start_align > start &&
      (*(int8_t*)sanity_to_shadow(start_align - 8) != 0x0 &&
       *(int8_t*)sanity_to_shadow(start_align - 8) < (len + start - (start_align - 8)))) {
    memory_sanity_abort();
  }
  if (end_align >= start_align + 8) {
    if (*(int8_t*)sanity_to_shadow(start_align) != 0x0) {
      memory_sanity_abort();
    }
    if (end_align > start_align + 8) {
      static void *(*real_memcmp)(const void *, const void *, size_t)
        = (__typeof__(real_memcmp)) dlsym(RTLD_NEXT, "memcmp");
      if (real_memcmp(sanity_to_shadow(start_align), sanity_to_shadow(start_align + 8),
                      sanity_to_shadow_size(end_align - start_align - 8)) != 0) {
        memory_sanity_abort();
      }
    }
  }
  if (end_align < end &&
      (*(int8_t*)sanity_to_shadow(end_align) != 0x0 &&
       *(int8_t*)sanity_to_shadow(end_align) < (end - end_align))) {
    memory_sanity_abort();
  }
}

extern void sanity_set_whitelist(const char *str);
using SymbolizeCb = std::function<void(void *, const char *, const char *, uint32_t)>;
typedef int (*BacktraceSymbolizeFunc)(void **addrs, int32_t n_addr, SymbolizeCb cb);
extern BacktraceSymbolizeFunc backtrace_symbolize_func;

#endif /* ENABLE_SANITY */
#endif /* _MEMORY_SANITY_H_ */
