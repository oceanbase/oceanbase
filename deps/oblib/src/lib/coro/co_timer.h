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

#ifndef CO_TIMER_H
#define CO_TIMER_H

#include "lib/coro/co_timer_queue.h"
#include "lib/coro/co_routine.h"

namespace oceanbase {
namespace lib {

class CoTimer : public CoTimerQueue::Node {
public:
  CoTimer(int64_t expires) : CoTimerQueue::Node(expires)
  {}
};

class CoTimerSleeper : public CoTimer {
public:
  CoTimerSleeper(CoRoutine& routine, int64_t expires) : CoTimer(expires), routine_(&routine)
  {}
  CoRoutine* routine_;
};

}  // namespace lib
}  // namespace oceanbase

#endif /* CO_TIMER_H */
