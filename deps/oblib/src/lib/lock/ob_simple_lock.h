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

#ifndef OB_SIMPLE_LOCK_H_
#define OB_SIMPLE_LOCK_H_
#include "lib/lock/ob_futex.h"
namespace oceanbase
{
namespace common
{
struct ObSimpleLock
{
public:
  static constexpr int32_t WAIT_MASK = 1<<31;
  static constexpr int32_t WRITE_MASK = 1<<30;
  ObSimpleLock() : v_(0)
  {}
  void lock()
  {
    if (!ATOMIC_BCAS(&v_, 0, WRITE_MASK)) {
      wait();
    }
  }
  void unlock()
  {
    int32_t v = ATOMIC_SET(&v_, 0);
    if (OB_UNLIKELY(WAIT_MASK == (v & WAIT_MASK))) {
      futex_wake(&v_, 1);
    }
  }
  void wait()
  {
    static constexpr timespec TIMEOUT = {0, 100 * 1000 * 1000};
    const int32_t MAX_TRY_CNT = 16;
    bool locked = false;
    for (int i = 0; i < MAX_TRY_CNT && !locked; ++i) {
      sched_yield();
      if (ATOMIC_BCAS(&v_, 0, WRITE_MASK)) {
        locked = true;
      }
    }
    while (!locked) {
      const int32_t v = v_;
      if (WAIT_MASK == (v & WAIT_MASK) || ATOMIC_BCAS(&v_, v | WRITE_MASK, v | WAIT_MASK)) {
        futex_wait(&v_, v | WRITE_MASK | WAIT_MASK, &TIMEOUT);
      }
      if (ATOMIC_BCAS(&v_, 0, WRITE_MASK | WAIT_MASK)) {
        locked = true;
      }
    }
  }
private:
  int32_t v_;
};
}
}
#endif