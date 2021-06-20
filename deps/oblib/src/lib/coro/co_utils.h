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

#ifndef CO_UTILS_H
#define CO_UTILS_H

#include <stdint.h>
#include <sys/time.h>
#include "lib/atomic/ob_atomic.h"

namespace oceanbase {
namespace lib {

union CoIdx {
  struct {
    uint64_t tidx_ : 12;
    uint64_t cidx_ : 48;
  };
  uint64_t idx_;
};

uint64_t alloc_coid();
CoIdx alloc_coidx();
void free_coidx(CoIdx coidx);

class CoSpinLock {
public:
  CoSpinLock();
  void lock();
  bool trylock();
  void unlock();

private:
  volatile int v_;
};

OB_INLINE CoSpinLock::CoSpinLock() : v_(0)
{}

OB_INLINE void CoSpinLock::lock()
{
  while (!trylock()) {
    PAUSE();
  }
}

OB_INLINE bool CoSpinLock::trylock()
{
  return ATOMIC_BCAS(&v_, 0, 1);
}

OB_INLINE void CoSpinLock::unlock()
{
  ATOMIC_STORE(&v_, 0);
}

OB_INLINE int64_t co_current_time()
{
  int64_t now = 0;
  struct timeval t;
  if (gettimeofday(&t, nullptr) >= 0) {
    now = (static_cast<int64_t>(t.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(t.tv_usec));
  }
  return now;
}

#if defined(__x86_64__)
OB_INLINE uint64_t co_rdtscp(void)
{
  return co_current_time() * 1000;
}

#elif defined(__aarch64__)
OB_INLINE uint64_t co_rdtscp(void)
{
  int64_t virtual_timer_value;
  asm volatile("mrs %0, cntvct_el0" : "=r"(virtual_timer_value));
  return virtual_timer_value;
}
#else
#error arch unsupported
#endif

}  // namespace lib
}  // namespace oceanbase

#endif /* CO_UTILS_H */
