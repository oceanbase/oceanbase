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

#include "co_sched.h"
#include "lib/ob_errno.h"
#include "lib/coro/co_timer.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace lib {
__thread CoSched* CoSched::instance_ = nullptr;
__thread CoRoutine* CoSched::active_routine_ = nullptr;

int CoSched::co_wait(int64_t duration)
{
  int ret = OB_SUCCESS;
  auto& cur_routine = *CoSched::get_active_routine();
  if (cur_routine.set_waiting()) {
    if (duration > 0) {
      auto expires = co_current_time() + duration;
      if (expires < 0) {
        expires = INT64_MAX;
      }
      CoTimerSleeper t(co_current(), expires);

      start_timer(t);
      reschedule();

      // If no other routine wakes up itself, it must be timeout.
      if (t.routine_ == nullptr) {
        ret = OB_TIMEOUT;
      } else {
        cancel_timer(t);
      }
    } else {
      reschedule();
    }
  }
  return ret;
}

}  // namespace lib
}  // namespace oceanbase
