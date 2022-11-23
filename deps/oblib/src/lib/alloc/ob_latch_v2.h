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

#ifndef ENABLE_SANITY
#else
#ifndef  OCEANBASE_LOCK_LATCH_v2_H_
#define  OCEANBASE_LOCK_LATCH_v2_H_

#include "lib/ob_define.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/alloc/ob_futex_v2.h"
#include "lib/stat/ob_latch_define.h"
#include "lib/lock/ob_lock_guard.h"

namespace oceanbase
{
namespace common
{
class ObLatchMutexV2
{
public:
  ObLatchMutexV2();
  ~ObLatchMutexV2();
  int lock(
      const uint32_t latch_id,
      const int64_t abs_timeout_us = INT64_MAX);
  int try_lock(
      const uint32_t latch_id,
      const uint32_t *puid = NULL);
  int wait(const int64_t abs_timeout_us, const uint32_t uid);
  int unlock();
  inline bool is_locked();
  inline uint32_t get_wid();
  int64_t to_string(char* buf, const int64_t buf_len);

private:
  OB_INLINE uint64_t low_try_lock(const int64_t max_spin_cnt, const uint32_t lock_value);

private:
  static const int64_t MAX_SPIN_CNT_AFTER_WAIT = 1;
  static const uint32_t WRITE_MASK = 1<<30;
  static const uint32_t WAIT_MASK = 1<<31;
  lib::ObFutexV2 lock_;
  //volatile int32_t lock_;
};

OB_INLINE uint64_t ObLatchMutexV2::low_try_lock(const int64_t max_spin_cnt, const uint32_t lock_value)
{
  uint64_t spin_cnt = 0;
  for (; spin_cnt < max_spin_cnt; ++spin_cnt) {
    if (0 == lock_.val()) {
      if (ATOMIC_BCAS(&lock_.val(), 0, lock_value)) {
        break;
      }
    }
    PAUSE();
  }
  return spin_cnt;
}

inline bool ObLatchMutexV2::is_locked()
{
  return 0 != ATOMIC_LOAD(&lock_.val());
}

inline uint32_t ObLatchMutexV2::get_wid()
{
  uint32_t lock = ATOMIC_LOAD(&lock_.val());
  return (lock & ~(WAIT_MASK | WRITE_MASK));
}

}

namespace lib {
class ObMutexV2 {
public:
  explicit ObMutexV2(uint32_t latch_id = common::ObLatchIds::DEFAULT_MUTEX)
    : latch_(), latch_id_(latch_id)
  {
  }
  ~ObMutexV2() { }
  inline int lock() { return latch_.lock(latch_id_); }
  inline int trylock() { return latch_.try_lock(latch_id_); }
  inline int unlock() { return latch_.unlock(); }
private:
  common::ObLatchMutexV2 latch_;
  uint32_t latch_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMutexV2);
};
}
}

#endif
#endif
