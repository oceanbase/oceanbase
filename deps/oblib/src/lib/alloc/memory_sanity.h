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
#define SANITY_DISABLE_CHECK_RANGE()
#define SANITY_POISON(args...)
#define SANITY_UNPOISON(args...)
#define SANITY_BOOL_EXPR(expr) false
#define SANITY_ADDR_IN_RANGE(args...) false
#define SANITY_TO_SHADOW(args...) 0x0
#define SANITY_TO_SHADOW_SIZE(args...) 0x0
#define SANITY_CHECK_RANGE(args...)
#define SANITY_MMAP(args...) NULL
#define SANITY_MUNMAP(args...)
#else
#include "sanity/sanity.h"
#define _DEFINE_SANITY_GUARD(var_name) SanityDisableCheckRangeGuard var_name;
#define SANITY_DISABLE_CHECK_RANGE() _DEFINE_SANITY_GUARD(CONCAT(sanity_guard, __COUNTER__))
#define SANITY_POISON(args...) sanity_poison(args)
#define SANITY_UNPOISON(args...) sanity_unpoison(args)
#define SANITY_BOOL_EXPR(expr) ({expr;})
#define SANITY_ADDR_IN_RANGE(args...) sanity_addr_in_range(args)
#define SANITY_TO_SHADOW(args...) sanity_to_shadow(args)
#define SANITY_TO_SHADOW_SIZE(size) sanity_to_shadow_size(size)
#define SANITY_CHECK_RANGE(args...) sanity_check_range(args)
#define SANITY_MMAP(args...) sanity_mmap(args)
#define SANITY_MUNMAP(args...) sanity_munmap(args)

extern int64_t get_global_addr();
extern bool init_sanity();
extern void *sanity_mmap(void *ptr, size_t size);
extern void sanity_munmap(void *ptr, size_t size);
extern void sanity_set_whitelist(const char *str);
using SymbolizeCb = std::function<void(void *, const char *, const char *, uint32_t)>;
typedef int (*BacktraceSymbolizeFunc)(void **addrs, int32_t n_addr, SymbolizeCb cb);
extern BacktraceSymbolizeFunc backtrace_symbolize_func;

#endif /* ENABLE_SANITY */
#endif /* _MEMORY_SANITY_H_ */
