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

#include "lib/time/ob_tsc_timestamp.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase;
using namespace oceanbase::common;

void ObTscBase::init(int64_t tsc_count)
{
  int ret = OB_SUCCESS;
  struct timeval tv;
  if (gettimeofday(&tv, NULL) < 0) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "get time of day error", K(ret));
  } else {
    tsc_count_ = tsc_count;
    start_us_ = tv.tv_sec * 1000000 + tv.tv_usec;
  }
}

int ObTscTimestamp::init()
{
  int ret = OB_SUCCESS;
  struct timeval tv;
  if (gettimeofday(&tv, NULL) < 0) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "get time of day error", K(ret));
  } else {
    if (!is_support_invariant_tsc_()) {
      ret = OB_NOT_SUPPORTED;
      LIB_LOG(WARN, "invariant TSC not support", K(ret));
    } else {
      const int64_t cpu_freq_khz = get_cpufreq_khz_();
      tsc_count_ = rdtscp();
      start_us_ = tv.tv_sec * 1000000 + tv.tv_usec;
      // judge if cpu frequency and tsc are legal
      if (tsc_count_ > 0 && cpu_freq_khz > 0) {
        // bitwise will lose precision.
        scale_ = (1000 << 20) / cpu_freq_khz;
        MEM_BARRIER();
        WEAK_BARRIER();
        is_init_ = true;
        LIB_LOG(INFO, "TSC TIMESTAMP init succ", K(cpu_freq_khz));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "TSC TIMESTAMP init fail", K(ret), K_(tsc_count), K(cpu_freq_khz));
      }
    }
  }
  if (OB_NOT_SUPPORTED == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int64_t ObTscTimestamp::current_time()
{
  int ret = OB_SUCCESS;
  // init failed, use system call.
  struct timeval tv;
  if (gettimeofday(&tv, NULL) < 0) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "sys gettimeofday unexpected", K(ret));
  }
  return (static_cast<int64_t>(tv.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(tv.tv_usec));
}

int64_t ObTscTimestamp::fast_current_time()
{
  int64_t result_time = 0;
  if (OB_UNLIKELY(!is_init_)) {
    result_time = current_time();
  } else {
    const uint64_t current_tsc = rdtsc();
    result_time = current_tsc * scale_ >> 20;
  }
  return result_time;
}


#if defined(__x86_64__)
uint64_t ObTscTimestamp::get_cpufreq_khz_()
{
  int ret = OB_SUCCESS;
  char line[256];
  FILE *stream = NULL;
  double freq_mhz = 0.0;
  uint64_t freq_khz = 0;
  stream = fopen("/proc/cpuinfo", "r");
  if (NULL == stream) {
    ret = OB_FILE_NOT_EXIST;
    LIB_LOG(WARN, "/proc/cpuinfo not exist", K(ret));
  } else {
    while (fgets(line, sizeof(line), stream)) {
      // FIXME: TSC frequency is not easy to retrieve from user-space
      // see: https://stackoverflow.com/questions/35123379/getting-tsc-rate-from-x86-kernel/57835630#57835630
      // cpu MHz was not stable and not equals to TSC frequency
      // don't depends on to calculate and then comapre to real clock time
      if (sscanf(line, "cpu MHz\t: %lf", &freq_mhz) == 1) {
        freq_khz = (uint64_t)(freq_mhz * 1000UL);
        break;
      }
    }
    fclose(stream);
  }
  LIB_LOG(INFO, "TSC freq : ", K(freq_khz));
  return freq_khz;
}

// judge if it support tsc, entry is CPUID.80000007H:EDX[8].
bool ObTscTimestamp::is_support_invariant_tsc_()
{
  int ret = true;
  unsigned int cpu_info[4];
  cpu_info[3] = 0;
  getcpuid(cpu_info, 0x80000007);
  if (cpu_info[3] & 0x100) {
    // invariant tsc is supported
  } else {
    ret = false;
  }
  if (ret) {
    cpu_info[3] = 0;
    getcpuid(cpu_info, 0x80000001);
    if (cpu_info[3] & (1<<27)) {
      // RDTSCP is supported
    } else {
      ret = false;
    }
  }
  return ret;
}

#elif defined(__aarch64__)

uint64_t ObTscTimestamp::get_cpufreq_khz_(void)
{
  uint64_t timer_frequency = 0;
  asm volatile("mrs %0, cntfrq_el0":"=r"(timer_frequency));
  LIB_LOG(INFO, "TSC freq : ", "freq", timer_frequency/1000);
  return timer_frequency/1000;
}

#else

#endif
