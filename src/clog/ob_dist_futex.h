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

#ifndef OCEANBASE_CLOG_OB_DIST_FUTEX_H_
#define OCEANBASE_CLOG_OB_DIST_FUTEX_H_

#include "lib/coro/syscall/co_futex.h"  // CoFutex

namespace oceanbase {
namespace common {

template <typename ValueType, int CONCURRENCY = 1024>
class ObDistFutex {
public:
  ObDistFutex()
  {}
  ~ObDistFutex()
  {}

public:
  int wait(volatile ValueType* p, ValueType v, int64_t timeout);
  int wake(volatile ValueType* p, int64_t n);

private:
  typedef lib::CoFutex FutexType;
  FutexType futex_[CONCURRENCY];

private:
  DISALLOW_COPY_AND_ASSIGN(ObDistFutex);
};

template <typename ValueType, int CONCURRENCY>
int ObDistFutex<ValueType, CONCURRENCY>::wait(volatile ValueType* p, ValueType v, int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(p)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t end_time = ObTimeUtility::current_time() + timeout;
    int64_t wait_time = timeout;
    int64_t index = reinterpret_cast<int64_t>(p) % CONCURRENCY;
    FutexType& futex = futex_[index];

    // firstly get futex value
    int32_t cur_val = futex.val();

    // if value is v, loop waiting
    while (OB_SUCCESS == ret && *p == v) {
      if (wait_time <= 0) {
        ret = OB_TIMEOUT;
      } else {
        // Waiting on the futex, check whether the value has changed,
        // if there is no change, it means that there is no wake in the middle and enter the waiting state
        ret = futex.wait(cur_val, wait_time);
        if (OB_SUCCESS == ret) {
          wait_time = end_time - ObTimeUtility::current_time();
          // If the futex is awakened by someone, or the value changes, save the current value first
          cur_val = futex.val();
        }
      }
    }
  }
  return ret;
}

template <typename ValueType, int CONCURRENCY>
int ObDistFutex<ValueType, CONCURRENCY>::wake(volatile ValueType* p, int64_t n)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(p)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t index = reinterpret_cast<int64_t>(p) % CONCURRENCY;
    FutexType& futex = futex_[index];

    // increase futex value
    ATOMIC_INC(&futex.val());

    // Wake up the waiting thread corresponding to futex
    ret = futex.wake(n);
  }
  return ret;
}

}  // namespace common
}  // namespace oceanbase

#endif
