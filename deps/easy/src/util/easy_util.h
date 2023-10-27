#ifndef EASY_UTIL_H_
#define EASY_UTIL_H_

#include "easy_define.h"

extern int ob_backtrace_c(void **buffer, int size);
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
