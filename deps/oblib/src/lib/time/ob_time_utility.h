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

#ifndef _OCEANBASE_COMMON_OB_TIME_UTILITY_H_
#define _OCEANBASE_COMMON_OB_TIME_UTILITY_H_
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include "lib/coro/co_var.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_define.h"
#include "lib/time/ob_tsc_timestamp.h"

namespace oceanbase
{
namespace common
{
class ObTimeUtility
{
public:
  static int64_t current_time();
	static int64_t current_time_ns();
  static int64_t fast_current_time();
  static int64_t current_monotonic_time();
  static int64_t current_time_coarse();
  static int64_t current_monotonic_raw_time();
private:
  static int64_t current_monotonic_time_();
  ObTimeUtility() = delete;
};

static bool IS_SYSTEM_SUPPORT_MONOTONIC_RAW = true;

// high performance current time, number of instructions is 1/6 of vdso_gettimeofday's.
OB_INLINE int64_t ObTimeUtility::fast_current_time()
{
  return ObTimeUtility::current_time();
}

inline int64_t ObTimeUtility::current_monotonic_time_()
{
  int64_t ret_val = 0;

  #ifdef _POSIX_MONOTONIC_CLOCK
  int err_ret = 0;
  struct timespec ts;
  if (OB_UNLIKELY((err_ret = clock_gettime(CLOCK_MONOTONIC, &ts)) != 0)) {
    ret_val = current_time();
  }
  // TODO: div 1000 can be replace to bitwise
  ret_val = ret_val > 0 ? ret_val : (static_cast<int64_t>(ts.tv_sec) * 1000000L +
                                     static_cast<int64_t>(ts.tv_nsec / 1000));
  #else
  ret_val = current_time();
  #endif

  return ret_val;
}

inline int64_t ObTimeUtility::current_monotonic_time()
{
  // if defined _POSIX_MONOTONIC_CLOCK
  // if _POSIX_MONOTONIC_CLOCK > 0, function in compile time and runtime can be used.
  // if _POSIX_MONOTONIC_CLOCK == 0, function in runtime may not be used.
  // if _POSIX_MONOTONIC_CLOCK < 0, function can not be used.
  #ifndef _POSIX_MONOTONIC_CLOCK
  return current_time();
  #elif _POSIX_MONOTONIC_CLOCK >= 0
  return current_monotonic_time_();
  #else
  return current_time();
  #endif
}

typedef ObTimeUtility ObTimeUtil;
} //common
} //oceanbase

#define TC_REACH_TIME_INTERVAL(i) \
  ({ \
    bool bret = false; \
    static thread_local int64_t last_time = 0; \
    int64_t cur_time = OB_TSC_TIMESTAMP.current_time(); \
    if (OB_UNLIKELY((i + last_time) < cur_time)) \
    { \
      last_time = cur_time; \
      bret = true; \
    } \
    bret; \
  })

#define TC_REACH_COUNT_INTERVAL(i) \
  ({ \
    bool bret = false; \
    static thread_local int64_t count = 0; \
    if (0 == (++count % i)) \
    { \
      bret = true; \
    } \
    bret; \
  })

#endif //_OCEANBASE_COMMON_OB_TIME_UTILITY_H_
