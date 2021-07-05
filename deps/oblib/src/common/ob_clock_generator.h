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

#ifndef OCEANBASE_COMMON_OB_CLOCK_GENERATOR_
#define OCEANBASE_COMMON_OB_CLOCK_GENERATOR_

#include <stdint.h>
#include <pthread.h>
#include "lib/oblog/ob_log.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/lock/Monitor.h"
#include "lib/lock/Mutex.h"
#include "lib/time/ob_time_utility.h"
#include "lib/thread/thread_pool.h"

namespace oceanbase {
namespace common {

class ObClockGenerator : public lib::ThreadPool {
private:
  ObClockGenerator() : inited_(false), ready_(false), cur_ts_(0), last_used_time_(0)
  {}
  ~ObClockGenerator()
  {
    destroy();
  }

public:
  static int init();
  static void destroy();
  static int64_t getClock();
  static int64_t getRealClock();
  static int64_t getCurrentTime();
  static void msleep(const int64_t ms);
  static void usleep(const int64_t us);

private:
  int64_t get_us();
  void run1() final;

private:
  bool inited_;
  bool ready_;
  int64_t cur_ts_;
  int64_t last_used_time_;
  static ObClockGenerator clock_generator_;
};

inline int64_t ObClockGenerator::getClock()
{
  int64_t ts = 0;

  if (!clock_generator_.inited_) {
    TRANS_LOG(WARN, "clock generator not inited");
    ts = clock_generator_.get_us();
  } else {
    ts = ATOMIC_LOAD(&clock_generator_.cur_ts_);
  }

  return ts;
}

inline int64_t ObClockGenerator::getRealClock()
{
  return clock_generator_.get_us();
}

inline int64_t ObClockGenerator::getCurrentTime()
{
  int64_t ret_val = ATOMIC_LOAD(&clock_generator_.last_used_time_);
  while (true) {
    const int64_t last = ATOMIC_LOAD(&clock_generator_.last_used_time_);
    const int64_t now = ObTimeUtility::current_time();
    if (now < last) {
      ret_val = last;
      break;
    } else if (ATOMIC_BCAS(&clock_generator_.last_used_time_, last, now)) {
      ret_val = now;
      break;
    } else {
      PAUSE();
    }
  }
  return ret_val;
}

inline void ObClockGenerator::msleep(const int64_t ms)
{
  if (ms > 0) {
    tbutil::Monitor<tbutil::Mutex> monitor_;
    (void)monitor_.timedWait(tbutil::Time(ms * 1000));
  }
}

inline void ObClockGenerator::usleep(const int64_t us)
{
  if (us > 0) {
    tbutil::Monitor<tbutil::Mutex> monitor_;
    (void)monitor_.timedWait(tbutil::Time(us));
  }
}

inline int64_t ObClockGenerator::get_us()
{
  return common::ObTimeUtility::current_time();
}

}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_COMMON_OB_CLOCK_GENERATOR_
