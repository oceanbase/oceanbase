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

#include "lib/time/ob_time_utility.h"
#include "lib/time/ob_tsc_timestamp.h"
#include "lib/ob_abort.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_print_utils.h"
#include <ctime>

using namespace oceanbase;
using namespace oceanbase::common;

OB_SERIALIZE_MEMBER(ObMonotonicTs, mts_);

int64_t ObTimeUtility::current_time()
{
  int err_ret = 0;
  struct timeval t;
  if (OB_UNLIKELY((err_ret = gettimeofday(&t, nullptr)) < 0)) {
    LIB_LOG_RET(ERROR, err_ret, "gettimeofday error", K(err_ret), K(errno));
    ob_abort();
  }
  return (static_cast<int64_t>(t.tv_sec) * 1000000L +
          static_cast<int64_t>(t.tv_usec));
}

int64_t ObTimeUtility::current_time_ns()
{
	int err_ret = 0;
  struct timespec ts;
  if (OB_UNLIKELY((err_ret = clock_gettime(CLOCK_REALTIME, &ts)) != 0)) {
      LIB_LOG_RET(WARN, err_ret, "current system not support CLOCK_REALTIME", K(err_ret), K(errno));
			ob_abort();
	}
	return static_cast<int64_t>(ts.tv_sec) * 1000000000L +
		static_cast<int64_t>(ts.tv_nsec);
}

int64_t ObTimeUtility::current_monotonic_raw_time()
{
  int64_t ret_val = 0;
  int err_ret = 0;
  struct timespec ts;

  if (IS_SYSTEM_SUPPORT_MONOTONIC_RAW) {
    if (OB_UNLIKELY((err_ret = clock_gettime(CLOCK_MONOTONIC_RAW, &ts)) != 0)) {
      LIB_LOG_RET(WARN, err_ret, "current system not support CLOCK_MONOTONIC_RAW", K(err_ret), K(errno));
      IS_SYSTEM_SUPPORT_MONOTONIC_RAW = false;
      ret_val = current_time();
    } else {
      // TODO: div 1000 can be replace to bitwise
      ret_val = static_cast<int64_t>(ts.tv_sec) * 1000000L +
        static_cast<int64_t>(ts.tv_nsec / 1000);
    }
  } else {
    // not support monotonic raw, use real time instead
    ret_val = current_time();
  }

  return ret_val;
}

int64_t ObTimeUtility::current_time_coarse()
{
  struct timespec t;
  if (OB_UNLIKELY(clock_gettime(
#ifdef HAVE_REALTIME_COARSE
                      CLOCK_REALTIME_COARSE,
#else
                      CLOCK_REALTIME,
#endif
                      &t))) {
    ob_abort();
  }
  return (static_cast<int64_t>(t.tv_sec) * 1000000L +
          static_cast<int64_t>(t.tv_nsec / 1000));
}

int64_t ObMonotonicTs::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "[mts=%ld]", mts_);
  return pos;
}
