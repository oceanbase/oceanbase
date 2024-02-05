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
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/lock/ob_monitor.h"
#include "lib/lock/mutex.h"
#include "lib/time/ob_time_utility.h"
#include "lib/thread/thread_pool.h"

namespace oceanbase
{
namespace common
{

class ObClockGenerator
    : public lib::ThreadPool
{
private:
  ObClockGenerator()
      : inited_(false), stopped_(true), ready_(false), cur_ts_(0), last_used_time_(0)
  {}
  ~ObClockGenerator() { destroy(); }
public:
  static ObClockGenerator &get_instance();
  void stop();
  void wait();
  static int init();
  static void destroy();
  static int64_t getClock();
  static int64_t getRealClock();
  static void msleep(const int64_t ms);
  static void usleep(const int64_t us);
  static void try_advance_cur_ts(const int64_t cur_ts);

private:
  int64_t get_us();
  void run1() final;

private:
  bool inited_;
  bool stopped_;
  bool ready_;
  int64_t cur_ts_;
  int64_t last_used_time_;
  static ObClockGenerator clock_generator_;
};

inline int64_t ObClockGenerator::getClock()
{
  int64_t ts = 0;

  if (OB_UNLIKELY(!clock_generator_.inited_)) {
    TRANS_LOG_RET(WARN, common::OB_NOT_INIT, "clock generator not inited");
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

inline void ObClockGenerator::msleep(const int64_t ms)
{
  if (ms > 0) {
    obutil::ObMonitor<obutil::Mutex> monitor_;
    (void)monitor_.timed_wait(obutil::ObSysTime(ms * 1000));
  }
}

inline void ObClockGenerator::usleep(const int64_t us)
{
  if (us > 0) {
    obutil::ObMonitor<obutil::Mutex> monitor_;
    (void)monitor_.timed_wait(obutil::ObSysTime(us));
  }
}

inline void ObClockGenerator::try_advance_cur_ts(const int64_t cur_ts)
{
  int64_t origin_cur_ts = OB_INVALID_TIMESTAMP;
  do {
    origin_cur_ts = ATOMIC_LOAD(&clock_generator_.cur_ts_);
    if (origin_cur_ts < cur_ts) {
      break;
    } else {
      TRANS_LOG_RET(WARN, common::OB_ERR_SYS, "timestamp rollback, need advance cur ts", K(origin_cur_ts), K(cur_ts));
    }
  } while (false == ATOMIC_BCAS(&clock_generator_.cur_ts_, origin_cur_ts, cur_ts));
}

OB_INLINE int64_t ObClockGenerator::get_us()
{
  return common::ObTimeUtility::current_time();
}

} // oceanbase
} // common

#endif //OCEANBASE_COMMON_OB_CLOCK_GENERATOR_
