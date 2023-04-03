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


#include "ob_syslog_rate_limiter.h"
#include "lib/time/ob_time_utility.h"


using namespace oceanbase;
using namespace common;

ObSimpleRateLimiterImpl::ObSimpleRateLimiterImpl(int64_t &rate)
  : rate_(rate), permits_(rate), last_ts_(ObTimeUtility::current_time())
{
}

ObSyslogSimpleRateLimiter::ObSyslogSimpleRateLimiter(int64_t rate)
    : ObISyslogRateLimiter(rate), impl_(rate_)
{
}

ObSyslogSampleRateLimiter::ObSyslogSampleRateLimiter(int64_t initial, int64_t thereafter,
                                                     int64_t duration)
  : initial_(initial), thereafter_(thereafter), duration_(duration),
    reset_ts_(ObTimeUtility::current_time()), count_(0)
{
  set_rate(initial);
}

int64_t ObSimpleRateLimiterImpl::current_time()
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

int ObSimpleRateLimiterImpl::try_acquire(int64_t permits)
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

int ObSyslogSampleRateLimiter::do_acquire(int64_t permits, int log_level, int errcode)
{
  UNUSED(log_level);
  UNUSED(errcode);
  int ret = common::OB_SUCCESS;
  int64_t cur_ts = ObTimeUtility::current_time();
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


