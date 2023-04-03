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

#ifndef EASY_UTIL_H_
#define EASY_UTIL_H_

#include "easy_define.h"

const char* easy_lbt();
const char* easy_lbt_str();
const char* easy_get_func_name(void *addr);
#if defined(__x86_64__)
static inline cycles_t easy_get_cycles()
{
    unsigned low, high;
    unsigned long long val;
    asm volatile("rdtsc" : "=a"(low), "=d"(high));
    //__asm__ __volatile__("rdtsc" : "=a"(low), "=d"(high));
    val = high;
    val = (val << 32) | low;
    return val;
}

#else

static inline uint64_t easy_rdtscp()
{
    int64_t virtual_timer_value;
    asm volatile("mrs %0, cntvct_el0" : "=r"(virtual_timer_value));
    return virtual_timer_value;
}

static inline cycles_t easy_get_cycles()
{
    return easy_rdtscp();
}
#endif

double easy_get_cpu_mhz(int no_cpu_freq_fail);

#endif
