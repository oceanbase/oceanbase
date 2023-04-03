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

#ifndef OB_TSC_TIMESTAMP_H_
#define OB_TSC_TIMESTAMP_H_
#if defined(__x86_64__)
#include<cpuid.h>
#endif
#include "lib/ob_define.h"

#if defined(__i386__)
static inline uint64_t rdtsc()
{
  uint64_t x;
  asm volatile (".byte 0x0f, 0x31" : "=A" (x));
  return x;
}
static inline uint64_t rdtscp()
{
  uint64_t rax, rdx, aux;
  asm volatile ( "rdtscp\n" : "=a" (rax), "=d" (rdx), "=c" (aux) : : );
  return ((uint64_t) rax) | (((uint64_t) rdx) << 32);
}
static inline uint64_t rdtscp_id(uint64_t &cpuid)
{
  uint64_t rax, rdx, rcx;
  asm volatile ( "rdtscp" : "=a" (rax), "=d" (rdx), "=c" (rcx) : : );
  cpuid = rcx;
  return ((uint64_t) rax) | (((uint64_t) rdx) << 32);
}
#elif defined(__x86_64__)
static inline uint64_t rdtsc()
{
  uint64_t rax,rdx;
  asm volatile ( "rdtsc" : "=a" (rax), "=d" (rdx) :: "%rcx" );
  return ((uint64_t) rax) | (((uint64_t) rdx) << 32);
}
static inline uint64_t rdtscp()
{
  uint64_t rax,rdx;
  // rdtscp will record cpuid in rcx register, record it in modify domain if not need to avoid misuse of rcs by compiler.
  asm volatile ( "rdtscp" : "=a" (rax), "=d" (rdx) :: "%rcx" );
  return ((uint64_t) rax) | (((uint64_t) rdx) << 32);
}
static inline uint64_t rdtscp_id(uint64_t &cpuid)
{
  uint64_t rax, rdx, rcx;
  asm volatile ( "rdtscp" : "=a" (rax), "=d" (rdx), "=c" (rcx) : : );
  cpuid = rcx;
  return ((uint64_t) rax) | (((uint64_t) rdx) << 32);
}

#elif defined(__aarch64__)
static __inline__ uint64_t rdtscp()
{
    int64_t virtual_timer_value;
    asm volatile("mrs %0, cntvct_el0" : "=r"(virtual_timer_value));
    return virtual_timer_value;
}
static __inline__ uint64_t rdtsc()
{
  return rdtscp();
}

static inline uint64_t rdtscp_id(uint64_t &cpuid)
{
  cpuid = 0;
  return rdtscp();
}
#else
// if it is not intel architecture, not use tsc and return 0.
static inline uint64_t rdtsc()
{
  return 0;
}
static inline uint64_t rdtscp()
{
  return 0;
}
static inline uint64_t rdtscp_id(uint64_t &cpuid)
{
  cpuid = 0;
  return 0;
}
#endif

// get cpu id with cpuid instruction
#if defined(__x86_64__)
static __inline__ void getcpuid(unsigned int cpu_info[4], unsigned int info_type) {
  __cpuid(info_type, cpu_info[0], cpu_info[1], cpu_info[2], cpu_info[3]);
}
#endif

namespace oceanbase
{
namespace common
{

class ObTscBase
{
public:
  ObTscBase() : start_us_(0), tsc_count_(0) {}
  ~ObTscBase() { start_us_ = 0; tsc_count_ = 0; }
  void init(int64_t tsc_count);
  bool is_valid() const { return start_us_ > 0 && tsc_count_ > 0; }
  int64_t start_us_;
  uint64_t tsc_count_;
};

class ObTscTimestamp
{
public:
  ObTscTimestamp()
    : is_init_(false), start_us_(0), tsc_count_(0), scale_(0)
  {
  }
  ~ObTscTimestamp() {}
  int init();
  int64_t current_time();
  int64_t current_monotonic_time();

  static ObTscTimestamp &get_instance()
  {
    static ObTscTimestamp instance;
    return instance;
  }
private:
  static const int64_t MAX_CPU_COUNT = 1024;
#if defined(__x86_64__)
  uint64_t get_cpufreq_khz_();
  // judge if it support tsc, entry is CPUID.80000007H:EDX[8].
  bool is_support_invariant_tsc_();
#elif defined(__aarch64__)
  uint64_t get_cpufreq_khz_(void);
  bool is_support_invariant_tsc_()
  {
    return true;
  }
#else
  uint64_t get_cpufreq_khz_(void)
  {
    return 0;
  }

  bool is_support_invariant_tsc_()
  {
    return false;
  }
#endif
private:
  bool is_init_;
  int64_t start_us_;
  uint64_t tsc_count_;
  // cycles2ns scale
  uint64_t scale_;
  // for monotonic tsc
  ObTscBase base_per_cpu_[MAX_CPU_COUNT];
};

}//common
}//oceanbase

#define OB_TSC_TIMESTAMP (oceanbase::common::ObTscTimestamp::get_instance())

#endif
