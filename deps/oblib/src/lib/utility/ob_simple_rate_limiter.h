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

#ifndef OB_SIMPLE_RATE_LIMITER_H
#define OB_SIMPLE_RATE_LIMITER_H

#include <stdint.h>
#include <sys/time.h>
#include <algorithm>
#include "lib/atomic/ob_atomic.h"
#include "lib/utility/ob_rate_limiter.h"
#include "lib/ob_errno.h"

namespace oceanbase {
namespace lib {

class ObSimpleRateLimiter : public lib::ObRateLimiter {
  static constexpr int64_t DEFAULT_RATE = 100;

public:
  ObSimpleRateLimiter(int64_t rate = DEFAULT_RATE) : last_ts_(0), permits_(rate)
  {
    last_ts_ = current_time();
    set_rate(rate);
  }

  bool is_force_allows() const override
  {
    return false;
  }
  void reset_force_allows() override{};
  int acquire(int64_t permits = 1) override;
  int try_acquire(int64_t permits = 1) override;

public:
  // This is a copy of ObTimeUtility::current_time() that depends on
  // nothing other than system library.
  static int64_t current_time();

private:
  int64_t last_ts_;
  int64_t permits_;
};

inline int ObSimpleRateLimiter::acquire(int64_t permits)
{
  UNUSED(permits);
  return common::OB_NOT_SUPPORTED;
}

inline int ObSimpleRateLimiter::try_acquire(int64_t permits)
{
  int ret = common::OB_SUCCESS;
  constexpr int64_t SEC2MICROS = 1000000L;
  while (true) {
    const int64_t cur_time = current_time();
    const int64_t second_delta = (cur_time - last_ts_) / SEC2MICROS * rate_;
    const int64_t micro_delta = (cur_time - last_ts_) % SEC2MICROS * rate_ / SEC2MICROS;
    const int64_t delta = second_delta + micro_delta;
    const int64_t oldval = permits_;
    const int64_t newval = std::min(rate_, permits_ + delta) - permits;
    if (oldval + delta > 0) {  // have permits
      if (ATOMIC_BCAS(&permits_, oldval, newval)) {
        if (delta > 0) {
          last_ts_ = cur_time;
        }
        break;
      } else {
        // continue next loop
      }
    } else {  // permits is run out
      ret = common::OB_EAGAIN;
      break;
    }
  }
  return ret;
}

inline int64_t ObSimpleRateLimiter::current_time()
{
  int64_t time = 0;
  struct timeval t;
  if (gettimeofday(&t, nullptr) >= 0) {
    time = (static_cast<int64_t>(t.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(t.tv_usec));
  }
  return time;
}

}  // namespace lib
}  // namespace oceanbase

#endif /* OB_SIMPLE_RATE_LIMITER_H */
