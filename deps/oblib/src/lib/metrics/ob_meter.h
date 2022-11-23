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

#ifndef _OB_METER_H
#define _OB_METER_H 1
#include <stdint.h>
#include "lib/metrics/ob_ema.h"

namespace oceanbase
{
namespace common
{
// thread-safe
class ObMeter
{
public:
  static const int64_t PER_SEC  = 1000000LL;  // PER SECOND
  static const int64_t PER_USEC = 1LL;        // PER uSECOND
public:
  ObMeter();
  ~ObMeter() {}
  void mark(uint64_t n = 1);
  double get_mean_rate(int64_t rate_unit = PER_SEC);
  double get_fifteen_minute_rate(int64_t rate_unit = PER_SEC);
  double get_rate(int64_t rate_unit = PER_SEC);
  uint64_t get_value() const;
private:
  void tick_if_necessary();
  void legacy_tick_if_necessary();
private:
  // types and constants
  // parameters reference http://blog.csdn.net/maray/article/details/73104923
  static const int64_t TICK_INTERVAL_IN_SEC = 10 * 60;
  static const int64_t TICK_INTERVAL_IN_USEC = TICK_INTERVAL_IN_SEC * 1000000LL;
  static const int64_t WINDOW_IN_SEC = 24 * 60 * 60; // 24 hour


  static const int64_t LEGACY_TICK_INTERVAL_IN_SEC = 5;
  static const int64_t LEGACY_TICK_INTERVAL_IN_USEC = LEGACY_TICK_INTERVAL_IN_SEC * 1000000LL;
  static const int64_t LEGACY_WINDOW_IN_SEC = 15 * 60; // 15 min
private:
  uint64_t value_;  // atomic
  int64_t start_ts_;
  int64_t last_tick_;  // atomic
  int64_t legacy_last_tick_;  // atomic
  ObExponentialMovingAverage<TICK_INTERVAL_IN_SEC, WINDOW_IN_SEC> rate_;
  ObExponentialMovingAverage<LEGACY_TICK_INTERVAL_IN_SEC, LEGACY_WINDOW_IN_SEC> legacy_rate_;
  DISALLOW_COPY_AND_ASSIGN(ObMeter);
};

inline uint64_t ObMeter::get_value() const
{
  return value_;
}

} // end namespace common
} // end namespace oceanbase

#endif /* _OB_METER_H */
