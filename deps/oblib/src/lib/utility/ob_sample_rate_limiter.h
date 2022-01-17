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

#ifndef OB_SAMPLE_RATE_LIMITER_H
#define OB_SAMPLE_RATE_LIMITER_H

#include <stdint.h>
#include <sys/time.h>
#include "lib/atomic/ob_atomic.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_rate_limiter.h"

namespace oceanbase {
namespace lib {

/*
  In the duration time unit, if acquire count exceeds initial number,
  it will be allowed every thereafter number
*/
class ObSampleRateLimiter : public lib::ObRateLimiter {
public:
  ObSampleRateLimiter() : ObSampleRateLimiter(0, 0)
  {}
  ObSampleRateLimiter(int64_t initial, int64_t thereafter, int64_t duration = 1000L * 1000L /*1s*/)
      : initial_(initial), thereafter_(thereafter), duration_(duration), reset_ts_(current_time()), count_(0)
  {}
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
  int64_t initial_;
  int64_t thereafter_;
  int64_t duration_;
  int64_t reset_ts_;
  int64_t count_;
};

inline int ObSampleRateLimiter::acquire(int64_t permits)
{
  UNUSED(permits);
  return common::OB_NOT_SUPPORTED;
}

inline int ObSampleRateLimiter::try_acquire(int64_t permits)
{
  int ret = common::OB_SUCCESS;
  int64_t cur_ts = current_time();
  int64_t reset_ts = ATOMIC_LOAD(&reset_ts_);
  int64_t n = 0;
  if (cur_ts < reset_ts) {
    n = ATOMIC_AAF(&count_, permits);
  } else {
    ATOMIC_STORE(&count_, permits);
    int64_t new_reset_ts = cur_ts + duration_;
    if (!ATOMIC_BCAS(&reset_ts_, reset_ts, new_reset_ts)) {
      n = ATOMIC_AAF(&count_, permits);
    } else {
      n = permits;
    }
  }
  if (n > initial_ && (n - initial_) % thereafter_ != 0) {
    ret = common::OB_EAGAIN;
  }
  return ret;
}

inline int64_t ObSampleRateLimiter::current_time()
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

#endif /* OB_SAMPLE_RATE_LIMITER_H */
