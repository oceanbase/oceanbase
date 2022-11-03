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

#ifndef _OB_EMA_V2_H
#define _OB_EMA_V2_H 1
#include "lib/utility/ob_macro_utils.h"
#include "lib/atomic/ob_atomic.h"
namespace oceanbase
{
namespace common
{
// See https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
class ObExponentialMovingAverageV2
{
public:
  ObExponentialMovingAverageV2(const double alpha)
      :inited_(false),
       ema_(0.0),
       alpha_(alpha)
  {}
  ~ObExponentialMovingAverageV2() {}

  void update(uint64_t value)
  {
    const double instant_rate = static_cast<double>(value);
    if (ATOMIC_LOAD(&inited_)) {
      // EMA_new = EMA_old + alpha * (value - EMA_old)
      const double old_ema = ATOMIC_LOAD64(&ema_);
      double new_ema = old_ema + (alpha_ * (instant_rate - old_ema));
      ATOMIC_SET(reinterpret_cast<int64_t*>(&ema_), *reinterpret_cast<int64_t*>(&new_ema));
    } else {
      inited_ = true;
      ema_ = instant_rate;
    }
  }
  double get_value() const
  {
    return ATOMIC_LOAD64(&ema_);
  }

private:
  bool inited_;  // atomic
  double ema_;  // atomic
  const double alpha_; // const
  DISALLOW_COPY_AND_ASSIGN(ObExponentialMovingAverageV2);
};

typedef ObExponentialMovingAverageV2 ObEMA;

} // end namespace common
} // end namespace oceanbase

#endif /* _OB_EMA_V2_H */
