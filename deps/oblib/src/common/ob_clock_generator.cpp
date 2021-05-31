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

#include "common/ob_clock_generator.h"
#include "lib/oblog/ob_log.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/lock/Monitor.h"
#include "lib/lock/Mutex.h"
#include "lib/time/ob_time_utility.h"
#include "lib/coro/routine.h"
#include "lib/thread/ob_thread_name.h"

using namespace oceanbase::lib;

namespace oceanbase {
namespace common {

ObClockGenerator ObClockGenerator::clock_generator_;

int ObClockGenerator::init()
{
  int ret = OB_SUCCESS;

  if (clock_generator_.inited_) {
    TRANS_LOG(WARN, "ObClockGenerator inited twice");
    ret = OB_INIT_TWICE;
  } else {
    clock_generator_.ready_ = false;
    if (OB_FAIL(clock_generator_.start())) {
      TRANS_LOG(ERROR, "create thread fail", K(ret));
    } else {
      ret = OB_SUCCESS;
      clock_generator_.cur_ts_ = clock_generator_.get_us();
      clock_generator_.last_used_time_ = clock_generator_.get_us();
      clock_generator_.inited_ = true;
      clock_generator_.ready_ = true;
      TRANS_LOG(INFO, "clock generator inited success");
    }
  }

  return ret;
}

void ObClockGenerator::destroy(void)
{
  if (clock_generator_.inited_) {
    clock_generator_.inited_ = false;
    clock_generator_.stop();
    clock_generator_.wait();
    clock_generator_.destroy();
    // Global variables and thread local variables when printing logs disable_logging_
    // Uncertain release order may lead to core dump
    // TRANS_LOG(INFO, "clock generator destroyed");
  }
}

void ObClockGenerator::run1()
{
  const int64_t MAX_RETRY = 3;
  const int64_t SLEEP_US = 1000;
  const int64_t PRINT_LOG_INTERVAL_US = 100 * 1000;
  const int64_t MAX_JUMP_TIME_US = 20 * SLEEP_US;

  lib::set_thread_name("ClockGenerator");
  while (!ready_) {
    this_routine::usleep(SLEEP_US);
  }
  while (inited_) {
    int64_t retry = 0;
    int64_t cur_ts = 0;
    int64_t delta = 0;
    while (retry++ < MAX_RETRY) {
      cur_ts = get_us();
      delta = cur_ts - ATOMIC_LOAD(&cur_ts_);
      if (delta > 0 && MAX_JUMP_TIME_US > delta) {
        break;
      } else {
        if (REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL_US)) {
          TRANS_LOG(WARN, "clock out of order", K(cur_ts), K(cur_ts_), K(delta));
        }
        this_routine::usleep(SLEEP_US);
      }
    }
    if (delta < 0) {
      // generate monotone increasing clock
      (void)ATOMIC_AAF(&cur_ts_, 1);
    } else {
      ATOMIC_STORE(&cur_ts_, cur_ts);
    }
    this_routine::usleep(SLEEP_US);
  }
}

}  // namespace common
}  // namespace oceanbase
