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

#ifndef OCEANBASE_LIB_LOCK_OB_FCOND_H
#define OCEANBASE_LIB_LOCK_OB_FCOND_H
#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/coro/co.h"

namespace oceanbase {
namespace common {
struct ObFCond {
public:
  ObFCond() : seq_(0), n_waiters_(0)
  {}
  ~ObFCond()
  {}
  void signal()
  {
    (void)ATOMIC_FAA(&futex_.val(), 1);
    if (ATOMIC_LOAD(&n_waiters_) > 0) {
      futex_.wake(INT32_MAX);
    }
  }
  uint32_t get_seq()
  {
    return ATOMIC_LOAD(&futex_.val());
  }
  int wait(uint32_t cmp, int64_t timeout)
  {
    int err = 0;
    (void)ATOMIC_FAA(&n_waiters_, 1);
    err = futex_.wait(cmp, timeout);
    (void)ATOMIC_FAA(&n_waiters_, -1);
    return err;
  }

private:
  lib::CoFutex futex_;
  uint32_t seq_;
  uint32_t n_waiters_;
};
};      // namespace common
};      // end namespace oceanbase
#endif  // OCEANBASE_LIB_LOCK_OB_FCOND_H
