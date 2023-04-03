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

#include "lib/metrics/ob_meter.h"
#include "lib/time/ob_time_utility.h"
#include "lib/atomic/ob_atomic.h"
using namespace oceanbase::common;

ObMeter::ObMeter()
    :value_(0)
{
  start_ts_ = ObTimeUtility::current_time();
  last_tick_ = start_ts_;
  legacy_last_tick_ = start_ts_;
}

void ObMeter::legacy_tick_if_necessary()
{
  int64_t old_tick = ATOMIC_LOAD(&legacy_last_tick_);
  int64_t cur_tick = ObTimeUtility::current_time();
  int64_t age = cur_tick - old_tick;

  // compatibility code
  if (age > LEGACY_TICK_INTERVAL_IN_USEC) {
    int64_t new_tick = cur_tick - age % LEGACY_TICK_INTERVAL_IN_USEC;
    if (ATOMIC_BCAS(&legacy_last_tick_, old_tick, new_tick)) {  //     legacy_last_tick_ = new_tick
      int64_t tick_times = age / LEGACY_TICK_INTERVAL_IN_USEC;
      static const int64_t FACTOR = 5; // performance. exp(-1)^5 = 0.00673794699 old value can be ignored.
      if (OB_UNLIKELY(tick_times > FACTOR * LEGACY_WINDOW_IN_SEC / LEGACY_TICK_INTERVAL_IN_SEC)) {
        legacy_rate_.retick();
      } else {
        for (int64_t i = 0; i < tick_times; ++i) {
          legacy_rate_.tick();
        }
      }
    }
  }
  // compatibility code
}


void ObMeter::tick_if_necessary()
{
  int64_t old_tick = ATOMIC_LOAD(&last_tick_);
  int64_t cur_tick = ObTimeUtility::current_time();
  int64_t age = cur_tick - old_tick;
  if (age > TICK_INTERVAL_IN_USEC) {
    int64_t new_tick = cur_tick - age % TICK_INTERVAL_IN_USEC;
    if (ATOMIC_BCAS(&last_tick_, old_tick, new_tick)) {  //     last_tick_ = new_tick
      int64_t tick_times = age / TICK_INTERVAL_IN_USEC;
      static const int64_t FACTOR = 5; // performance. exp(-1)^5 = 0.00673794699 old value can be ignored.
      if (OB_UNLIKELY(tick_times > FACTOR * WINDOW_IN_SEC / TICK_INTERVAL_IN_SEC)) {
        rate_.retick();
      } else {
        for (int64_t i = 0; i < tick_times; ++i) {
          rate_.tick();
        }
      }
    }
  }
}

void ObMeter::mark(uint64_t n /*= 1*/)
{
  legacy_tick_if_necessary();
  tick_if_necessary();
  ATOMIC_FAA(&value_, n);
  legacy_rate_.update(n);
  rate_.update(n);
}

double ObMeter::get_mean_rate(int64_t rate_unit)
{
  double rate = 0.0;
  int64_t v = ATOMIC_LOAD(&value_);
  if (v > 0) {
    int64_t elapsed_in_usec = ObTimeUtility::current_time() - start_ts_;
    rate = static_cast<double>(v) * static_cast<double>(rate_unit) / static_cast<double>(elapsed_in_usec);
  }
  return rate;
}

double ObMeter::get_fifteen_minute_rate(int64_t rate_unit)
{
  legacy_tick_if_necessary();
  tick_if_necessary();
  return legacy_rate_.get_rate(rate_unit);
}

double ObMeter::get_rate(int64_t rate_unit)
{
  tick_if_necessary();
  return rate_.get_rate(rate_unit);
}

