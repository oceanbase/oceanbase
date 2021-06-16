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

#ifndef OCEANBASE_LOCK_OB_SEQ_SEM_H_
#define OCEANBASE_LOCK_OB_SEQ_SEM_H_
#include "lib/ob_define.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/coro/co.h"

namespace oceanbase {
namespace common {
class ObSeqSem {
public:
  enum { MAX_SEM_COUNT = 4096 };

  static int64_t get_us()
  {
    return ::oceanbase::common::ObTimeUtility::current_time();
  }

  static uint64_t faa_bounded(uint64_t* addr, uint64_t limit)
  {
    uint64_t ov = 0;
    uint64_t nv = ATOMIC_LOAD(addr);
    while ((ov = nv) < limit && ov != (nv = ATOMIC_VCAS(addr, ov, ov + 1))) {
      PAUSE();
    }
    return nv;
  }

  static uint32_t dec_bounded(uint32_t* addr)
  {
    uint32_t ov = 0;
    uint32_t nv = ATOMIC_LOAD(addr);
    while ((ov = nv) > 0 && ov != (nv = ATOMIC_VCAS(addr, ov, ov - 1))) {
      PAUSE();
    }
    return nv;
  }

  struct Sem {
    Sem() : n_waiters_(0), dummy_(0)
    {}
    ~Sem()
    {}

    bool wait(int64_t timeout_us)
    {
      uint32_t stock = 0;
      if ((stock = dec_bounded(&futex_.uval())) <= 0 && timeout_us > 0) {
        (void)ATOMIC_FAA(&n_waiters_, 1);
        if (0 == futex_.wait(stock, timeout_us)) {
          stock = dec_bounded(&futex_.uval());
        }
        (void)ATOMIC_FAA(&n_waiters_, -1);
      }
      return stock > 0;
    }

    void post()
    {
      (void)ATOMIC_FAA(&futex_.uval(), 1);
      if (ATOMIC_LOAD(&n_waiters_) > 0) {
        futex_.wake(INT32_MAX);
      }
    }

    bool wait_dummy()
    {
      return dec_bounded(&dummy_) > 0;
    }
    void post_dummy()
    {
      (void)ATOMIC_FAA(&dummy_, 1);
    }

    lib::CoFutex futex_;
    uint32_t n_waiters_;
    uint32_t dummy_;
  } CACHE_ALIGNED;

  ObSeqSem() : push_(0), pop_(0)
  {}
  ~ObSeqSem()
  {}

  int wait(int64_t timeout)
  {
    int err = -EAGAIN;
    int64_t cur_us = get_us();
    int64_t abs_timeout = cur_us + timeout;
    while (-EAGAIN == (err = wait_one(abs_timeout)) && abs_timeout > get_us())
      ;
    return err;
  }

  int wait_one(int64_t abs_timeout)
  {
    int err = 0;
    int64_t pop_idx = ATOMIC_FAA(&pop_, 1);
    Sem* sem = sems_ + idx(pop_idx);
    int64_t cur_us = get_us();
    while (!sem->wait(abs_timeout - cur_us)) {
      cur_us = get_us();
      if ((abs_timeout - cur_us) <= 0) {
        post_dummy(pop_idx + 1);
      }
    }
    if (sem->wait_dummy()) {
      err = -EAGAIN;
    }
    return err;
  }

  int post()
  {
    int err = 0;
    uint64_t push_idx = ATOMIC_FAA(&push_, 1);
    Sem* sem = sems_ + idx(push_idx);
    sem->post();
    return err;
  }

  int64_t value() const
  {
    uint64_t pop = ATOMIC_LOAD(&pop_);
    uint64_t push = ATOMIC_LOAD(&push_);
    return (int64_t)(push - pop);
  }

private:
  uint64_t idx(int64_t x)
  {
    return x & (MAX_SEM_COUNT - 1);
  }

  void post_dummy(uint64_t limit)
  {
    uint64_t push_idx = faa_bounded(&push_, limit);
    if (push_idx < limit) {
      Sem* sem = sems_ + idx(push_idx);
      sem->post_dummy();
      sem->post();
    }
  }

private:
  uint64_t push_ CACHE_ALIGNED;
  uint64_t pop_ CACHE_ALIGNED;
  Sem sems_[MAX_SEM_COUNT];
};

};  // end namespace common
};  // end namespace oceanbase

#endif /* OCEANBASE_LOCK_OB_SEQ_SEM_H_ */
