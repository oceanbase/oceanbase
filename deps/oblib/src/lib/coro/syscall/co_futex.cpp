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

#include "co_futex.h"
#include <linux/futex.h>
#include "lib/coro/co.h"
#include "lib/coro/co_timer.h"

static struct timespec make_timespec(int64_t us)
{
  timespec ts;
  ts.tv_sec = us / 1000000;
  ts.tv_nsec = 1000 * (us % 1000000);
  return ts;
}

#define futex(...) syscall(SYS_futex, __VA_ARGS__)
inline int futex_wake(volatile int* p, int val)
{
  return static_cast<int>(futex((int*)p, FUTEX_WAKE_PRIVATE, val, NULL, NULL, 0));
}

inline int64_t futex_wake(volatile int* p, int val, int wake_mask)
{
  int64_t ret = futex((int*)p, FUTEX_WAKE_BITSET_PRIVATE, val, NULL, NULL, wake_mask);
  /* NOTE: we ignore errors on wake for the case of a futex
     guarding its own destruction, similar to this
     glibc bug with sem_post/sem_wait:
     https://sourceware.org/bugzilla/show_bug.cgi?id=12674 */
  if (ret < 0) {
    ret = 0;
  }
  return ret;
}

inline int futex_wait(volatile int* p, int val, const timespec* timeout)
{
  int ret = 0;
  if (0 != futex((int*)p, FUTEX_WAIT_PRIVATE, val, timeout, NULL, 0)) {
    ret = errno;
  }
  return ret;
}

inline int futex_wait_until(volatile int* p, int val, const timespec* timeout, int wait_mask)
{
  int ret = 0;
  if (0 != futex((int*)p, FUTEX_WAIT_BITSET_PRIVATE, val, timeout, NULL, wait_mask)) {
    ret = errno;
  }
  return ret;
}

using namespace oceanbase::lib;
using namespace oceanbase::common;

namespace oceanbase {
namespace lib {

bool CoFutex::force_syscall_futex_ = false;

int CoFutex::wait(int v, int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(force_syscall_futex_) || OB_UNLIKELY(!CO_IS_ENABLED())) {
    const auto ts = make_timespec(timeout);
    ATOMIC_INC(&sys_waiters_);
    int eret = futex_wait(&v_, v, &ts);
    if (OB_UNLIKELY(eret != 0)) {
      if (OB_UNLIKELY(eret == ETIMEDOUT)) {
        // only return timeout error code, others treat as success.
        ret = OB_TIMEOUT;
      }
    }
    ATOMIC_DEC(&sys_waiters_);
  } else {
    abort_unless(&CO_CURRENT() != static_cast<CoRoutine*>(&CO_SETSCHED()));
    auto expires = co_current_time() + timeout;
    if (expires < 0) {
      expires = INT64_MAX;
    }
    CoTimerSleeper t(CO_CURRENT(), expires);
    Node node(CO_CURRENT());
    spin_.lock();
    ATOMIC_INC(&waiters_);
    if (v_ == v) {
      q_.add_last(&node);
      // After invoking CO_WAIT0() other routine's waking up would be
      // accept because of the *current routine* has been changed to
      // the scheduler and CoBaseSched::on_wakeup() can put this
      // routine into it's RQ.
      CO_WAIT0();
      spin_.unlock();
      if (timeout >= 0) {
        CO_START_TIMER(t);
        CO_SCHEDULE();
        if (t.routine_ != nullptr) {
          CO_CANCEL_TIMER(t);
        } else {
          ret = OB_TIMEOUT;
        }
      } else {
        CO_SCHEDULE();
      }
      // 1. Someone wakes me up
      // 2. Time is out
      // 3. Wakeup by mistake.
      spin_.lock();
      if (node.get_prev() != nullptr) {
        q_.remove(&node);
      }
    }
    ATOMIC_DEC(&waiters_);
    spin_.unlock();
  }
  return ret;
}

int CoFutex::wake(int64_t n)
{
  int cnt = 0;
  if (OB_LIKELY(force_syscall_futex_)) {
    if (n >= INT32_MAX) {
      cnt = futex_wake(&v_, INT32_MAX);
    } else {
      cnt = futex_wake(&v_, static_cast<int32_t>(n));
    }
  } else {
    if (ATOMIC_LOAD(&sys_waiters_) > 0) {
      if (n >= INT32_MAX) {
        cnt = futex_wake(&v_, INT32_MAX);
      } else {
        cnt = futex_wake(&v_, static_cast<int32_t>(n));
      }
    }
    if (ATOMIC_LOAD(&waiters_) > 0) {
      spin_.lock();
      while (n--) {
        auto node_ptr = q_.remove_first();
        if (nullptr != node_ptr) {
          CO_WAKEUP(node_ptr->routine_);
          cnt++;
        } else {
          break;
        }
      }
      spin_.unlock();
    }
  }
  return cnt;
}

}  // namespace lib
}  // namespace oceanbase
