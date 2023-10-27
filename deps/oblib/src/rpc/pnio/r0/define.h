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

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stddef.h>

#define RK_CACHE_ALIGNED __attribute__((aligned(64)))

inline void __unused(void* p, ...) { (void)p; }
#define unused(args...) __unused(NULL, args)
#define structof(p, T, m) (T*)((char*)p - offsetof(T, m))
#define arrlen(x) (sizeof(x)/sizeof(x[0]))
#define ef(x) if ((x)) goto el
#define el() el:

#define STR(x) XSTR(x)
#define XSTR(x) #x
#define RK_WEAK __attribute__((weak))
#define rk_max(a,b) ({ typeof (a) _a = (a), _b = (b); _a > _b ? _a : _b; })
#define rk_min(a,b) ({ typeof (a) _a = (a), _b = (b); _a < _b ? _a : _b; })

#include <assert.h>
//#define rk_debug(...) DEBUG(rk_info(__VA_ARGS__))
#define rk_debug(...)

#ifndef likely
#define likely(x)       __builtin_expect((x),1)
#endif
#ifndef unlikely

#define unlikely(x)     __builtin_expect((x),0)
#endif
static uint64_t upalign8(uint64_t x) { return (x + 7) & ~7ULL; }
#define ignore_ret_value(exp)  {int ignore __attribute__ ((unused)) = (exp);}