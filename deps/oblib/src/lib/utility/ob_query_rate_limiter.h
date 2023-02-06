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

#ifndef OB_QUERY_RATE_LIMITER_H
#define OB_QUERY_RATE_LIMITER_H

#include <stdint.h>
#include <sys/time.h>
#include <algorithm>
#include "lib/atomic/ob_atomic.h"
#include "lib/utility/ob_rate_limiter.h"
#include "lib/ob_errno.h"

namespace oceanbase {
namespace lib {

class ObQueryRateLimiter
    : public lib::ObRateLimiter
{
public:
  ObQueryRateLimiter() : last_ship_time_(0) {}

  bool is_force_allows() const override { return false; }
  void reset_force_allows() override {}
  virtual int try_acquire(const int64_t permits = 1,
                          const int64_t arg0 = 0,
                          const int64_t arg1 = 0) override;
  virtual int acquire(const int64_t permits = 1,
                      const int64_t arg0 = 0,
                      const int64_t arg1 = 0) override;

private:
  static int64_t current_time();

private:
  int64_t last_ship_time_;
};

inline int ObQueryRateLimiter::acquire(const int64_t permits,
                                       const int64_t arg0,
                                       const int64_t arg1)
{
  UNUSEDx(permits, arg0, arg1);
  return OB_NOT_SUPPORTED;
}

inline int ObQueryRateLimiter::try_acquire(const int64_t permits,
                                           const int64_t arg0,
                                           const int64_t arg1)
{
  UNUSEDx(arg0, arg1);
  int ret = OB_SUCCESS;
  constexpr int64_t SEC2MICROS = 1000000L;
  if (OB_LIKELY(rate_ > 0)) {
    while (true) {
      int64_t cur_time = current_time();
      int64_t last_ship_time = ATOMIC_LOAD(&last_ship_time_);
      int64_t due_time = last_ship_time + (SEC2MICROS * permits)/ rate_;
      int64_t cur_ship_time = cur_time;
      int64_t slide_window = (SEC2MICROS * 50) / rate_;
      if (due_time > cur_time + slide_window) {
        // The next 50 tokens are already oversold, reject the current request
        ret = OB_EAGAIN;
        break;
      } else if (due_time > cur_time) {
        // The time for token generation has not been reached, prefetch a token
        cur_ship_time = due_time;
      } else {
        // The token generation time has been reached, and the token can be obtained normally
        cur_ship_time = cur_time;
      }
      if (ATOMIC_BCAS(&last_ship_time_, last_ship_time, cur_ship_time)) {
        break;
      }
    }
  }
  return ret;
}

inline int64_t ObQueryRateLimiter::current_time()
{
  int64_t time = 0;
  struct timeval t;
  if (gettimeofday(&t, nullptr) >= 0) {
    time = (static_cast<int64_t>(t.tv_sec)
            * static_cast<int64_t>(1000000)
            + static_cast<int64_t>(t.tv_usec));
  }
  return time;
}

}
}


#endif // OB_QUERY_RATE_LIMITER_H
