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

#ifndef _OB_EMA_H
#define _OB_EMA_H 1
#include <cmath>  // exp
#include "lib/utility/ob_macro_utils.h"
#include "lib/atomic/ob_atomic.h"
namespace oceanbase
{
namespace common
{
// See https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
template <int64_t TICK_INTERVAL_IN_SEC, int64_t SAMPLING_WINDOWN_IN_SEC>
class ObExponentialMovingAverage
{
public:
  ObExponentialMovingAverage()
      :inited_(false),
       ema_(0.0),
       uncounted_(0)
  {}
  ~ObExponentialMovingAverage() {}

  void update(uint64_t n)
  {
    UNUSED(ATOMIC_FAA(&uncounted_, n));
  }
  void tick()
  {
    uint64_t value = ATOMIC_SET(&uncounted_, 0);
    const double instant_rate = static_cast<double>(value) / TICK_INTERVAL_IN_USEC;
    if (inited_) {
      // EMA_new = EMA_old + alpha * (value - EMA_old)
      const double old_ema = ATOMIC_LOAD64(&ema_);
      double new_ema = old_ema + (ALPHA * (instant_rate - old_ema));
      ATOMIC_STORE(reinterpret_cast<int64_t*>(&ema_), *reinterpret_cast<int64_t*>(&new_ema));
    } else {
      inited_ = true;
      ema_ = instant_rate;
    }
  }
  void retick()
  {
    uint64_t value = ATOMIC_SET(&uncounted_, 0);
    if (inited_) {
      double new_ema = 0; // count from 0
      ATOMIC_STORE(reinterpret_cast<int64_t*>(&ema_), *reinterpret_cast<int64_t*>(&new_ema));
    } else {
      const double instant_rate = static_cast<double>(value) / TICK_INTERVAL_IN_USEC;
      inited_ = true;
      ema_ = instant_rate;
    }
  }
  double get_rate(int64_t rate_unit) const
  {
    return ATOMIC_LOAD64(&ema_) * static_cast<double>(rate_unit);
  }
private:
  // TICK_INTERVAL_IN_USEC is microsecond.
  // if need per_sec, set rate_unit = PER_SECOND when get_rate
  static const int64_t TICK_INTERVAL_IN_USEC = TICK_INTERVAL_IN_SEC * 1000000LL;
  static const double ALPHA;
private:
  bool inited_;  // atomic
  double ema_;  // atomic
  uint64_t uncounted_;  // atomic
  //
  DISALLOW_COPY_AND_ASSIGN(ObExponentialMovingAverage);
};

template <int64_t TICK_INTERVAL_IN_SEC, int64_t SAMPLING_WINDOWN_IN_SEC>
const double ObExponentialMovingAverage<TICK_INTERVAL_IN_SEC, SAMPLING_WINDOWN_IN_SEC>::ALPHA =
    1 - std::exp(static_cast<double>(-(TICK_INTERVAL_IN_SEC)) / SAMPLING_WINDOWN_IN_SEC);


} // end namespace common
} // end namespace oceanbase

#endif /* _OB_EMA_H */
