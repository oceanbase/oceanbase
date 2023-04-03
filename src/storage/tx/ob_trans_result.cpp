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

#include "ob_trans_result.h"
#include "lib/worker.h"
#include "common/ob_clock_generator.h"

namespace oceanbase
{
namespace transaction
{

using namespace common;
using namespace obutil;

void ObTransCond::reset()
{
  finished_ = false;
  result_ = OB_SUCCESS;
}

// when SQL submit or abort transaction, it must wait for transaction response
/*
int ObTransCond::wait(const int64_t wait_time_us)
{
  int result = OB_SUCCESS;
  int ret = wait(wait_time_us, result);
  if (OB_LIKELY(OB_SUCCESS == ret)) {
    ret = result;
  }
  return ret;
}
*/
// when SQL submit or abort transaction, it must wait for transaction response
int ObTransCond::wait(const int64_t wait_time_us, int &result)
{
  int ret = OB_SUCCESS;

  if (wait_time_us < 0) {
    TRANS_LOG(WARN, "invalid argument", K(wait_time_us));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t left_time_us = wait_time_us;
    int64_t start_time_us = ObClockGenerator::getClock();
    THIS_WORKER.sched_wait();
    {
      ObMonitor<Mutex>::Lock guard(monitor_);
      while (!finished_ && OB_SUCC(ret)) {
        left_time_us = wait_time_us - (ObClockGenerator::getClock() - start_time_us);
        if (left_time_us <= 0 || !monitor_.timed_wait(ObSysTime(left_time_us))) { // timeout
          ret = OB_TIMEOUT;
        }
      }
      if (finished_) {
        result = result_;
      }
    }
    THIS_WORKER.sched_run();
  }

  return ret;
}

// set transaction result by transaction context
void ObTransCond::notify(const int result)
{
  ObMonitor<Mutex>::Lock guard(monitor_);
  if (finished_) {
    TRANS_LOG(DEBUG, "transaction has already get result", "old_result", result_, "new_result", result);
  }
  finished_ = true;
  result_ = result;
  monitor_.notify_all();
}

void ObTransCond::usleep(const int64_t us)
{
  if (us > 0) {
    ObMonitor<Mutex> monitor;
    THIS_WORKER.sched_wait();
    (void)monitor.timed_wait(ObSysTime(us));
    THIS_WORKER.sched_run();
  }
}

} // transaction
} // oceanbase
