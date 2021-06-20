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

#ifndef CO_FUTEX_H
#define CO_FUTEX_H

#include <atomic>
#include "lib/ob_errno.h"
#include "lib/coro/co_routine.h"
#include "lib/coro/co_utils.h"
#include "lib/list/ob_dlist.h"

namespace oceanbase {
namespace lib {
class TmpSpinLock {
  std::atomic_flag locked = ATOMIC_FLAG_INIT;

public:
  void lock()
  {
    while (locked.test_and_set(std::memory_order_acquire)) {
      ;
    }
  }
  void unlock()
  {
    locked.clear(std::memory_order_release);
  }
};

class CoFutex {
  class Node;
  using Queue = common::ObDList<Node>;

public:
  CoFutex() : spin_(), v_(), q_(), waiters_(), sys_waiters_()
  {}

  int32_t& val()
  {
    return v_;
  }
  uint32_t& uval()
  {
    return reinterpret_cast<uint32_t&>(v_);
  }

  // This function atomically verifies v_ still equals to argument v,
  // and sleeps awaiting wake call on this futex. If the timeout
  // argument is positive, it contains the maximum duration described
  // in milliseconds of the wait, which is infinite otherwise.
  int wait(int v, int64_t timeout);
  // This function wakes at most n, which is in argument list,
  // routines up waiting on the futex.
  int wake(int64_t n);

public:
  // Force using syscall futex instead of choosing automatically. This
  // method is designed for performance tuning or as a global switch
  // before coroutine getting stable. It must be called at the most
  // beginning before \c CoFutex being used.
  static void force_syscall_futex()
  {
    force_syscall_futex_ = true;
  }

private:
  static bool force_syscall_futex_;

  TmpSpinLock spin_;
  int v_;
  Queue q_;
  int waiters_;
  int sys_waiters_;
} CACHE_ALIGNED;

class CoFutex::Node : public common::ObDLinkBase<CoFutex::Node> {
  friend class CoFutex;

public:
  Node(CoRoutine& routine) : routine_(routine)
  {}

private:
  CoRoutine& routine_;
};

}  // namespace lib
}  // namespace oceanbase

#endif /* CO_FUTEX_H */
