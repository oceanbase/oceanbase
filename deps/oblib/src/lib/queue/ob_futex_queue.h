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

#ifndef OCEANBASE_QUEUE_OB_FUTEX_QUEUE_H_
#define OCEANBASE_QUEUE_OB_FUTEX_QUEUE_H_
#include <stdint.h>
#include <stddef.h>
#include "lib/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/coro/co.h"

namespace oceanbase {
namespace common {
class ObFutexQueue {
public:
  static bool is2n(uint64_t n)
  {
    return 0 == (n & (n - 1));
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

  struct Cond {
  public:
    Cond() : n_waiters_(0)
    {}
    ~Cond()
    {}

    void signal()
    {
      (void)ATOMIC_FAA(&futex_.uval(), 1);
      if (ATOMIC_LOAD(&n_waiters_) > 0) {
        futex_.wake(INT32_MAX);
      }
    }
    uint32_t get_seq()
    {
      return ATOMIC_LOAD(&futex_.uval());
    }
    void wait(uint32_t cmp, int64_t timeout)
    {
      if (timeout > 0) {
        (void)ATOMIC_FAA(&n_waiters_, 1);
        futex_.wait(cmp, timeout);
        (void)ATOMIC_FAA(&n_waiters_, -1);
      }
    }

  private:
    lib::CoFutex futex_;
    uint32_t n_waiters_;
  };

  struct Item {
    Item() : cond_(), data_(NULL)
    {}
    ~Item()
    {}

    void push(void* data)
    {
      if (NULL != data) {
        while (!ATOMIC_BCAS(&data_, NULL, data)) {
          PAUSE();
        }
        cond_.signal();
      }
    }

    void* pop(int64_t timeout)
    {
      void* data = NULL;
      if (NULL == (data = ATOMIC_TAS(&data_, NULL)) && timeout > 0) {
        uint32_t seq = cond_.get_seq();
        if (NULL == (data = ATOMIC_TAS(&data_, NULL))) {
          cond_.wait(seq, timeout);
        }
      }
      return data;
    }

    Cond cond_;
    void* data_;
  };

public:
  const static uint64_t WAKE_ID = ~(0LL);
  ObFutexQueue() : push_(0), pop_(0), capacity_(0), allocated_(NULL), items_(NULL)
  {}
  ~ObFutexQueue()
  {
    destroy();
  }

  static uint64_t calc_mem_usage(uint64_t capacity)
  {
    return sizeof(Item) * capacity;
  }

  int init(const uint64_t capacity, const lib::ObLabel& label = ObModIds::OB_LIGHTY_QUEUE)
  {
    int err = OB_SUCCESS;
    if (capacity <= 0 || !is2n(capacity)) {
      err = OB_INVALID_ARGUMENT;
    } else if (NULL == (allocated_ = ob_malloc(calc_mem_usage(capacity), label))) {
      err = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      err = init(capacity, allocated_);
    }
    if (OB_SUCCESS != err) {
      destroy();
    }
    return err;
  }

  int init(uint64_t capacity, void* data)
  {
    int err = OB_SUCCESS;
    ;
    if (capacity <= 0 || !is2n(capacity) || NULL == data) {
      err = OB_INVALID_ARGUMENT;
    } else {
      capacity_ = capacity;
      items_ = (Item*)data;
      memset(data, 0, calc_mem_usage(capacity));
    }
    return err;
  }

  void destroy()
  {
    if (NULL != items_) {
      push_ = 0;
      pop_ = 0;
      capacity_ = 0;
      items_ = NULL;
    }
    if (NULL != allocated_) {
      ob_free(allocated_);
      allocated_ = NULL;
    }
  }

  int push(void* data)
  {
    int err = OB_SUCCESS;
    ;
    if (NULL == data) {
      err = OB_INVALID_ARGUMENT;
    } else if (NULL == items_) {
      err = OB_NOT_INIT;
    } else {
      err = do_push(data, ATOMIC_LOAD(&pop_) + capacity_);
    }
    return err;
  }

  int pop(void*& data, int64_t timeout_us)
  {
    int err = OB_SUCCESS;
    ;
    if (NULL == items_) {
      err = OB_NOT_INIT;
    } else {
      err = do_pop(data, ATOMIC_FAA(&pop_, 1), timeout_us);
    }
    return err;
  }

  int64_t size() const
  {
    uint64_t pop = ATOMIC_LOAD(&pop_);
    uint64_t push = ATOMIC_LOAD(&push_);
    return (int64_t)(push - pop);
  }

private:
  uint64_t idx(uint64_t x)
  {
    return x & (capacity_ - 1);
  }

  int do_pop(void*& data, uint64_t pop_idx, int64_t timeout)
  {
    int err = OB_SUCCESS;
    ;
    Item* item = items_ + idx(pop_idx);
    if (NULL == (data = item->pop(timeout))) {
      while (NULL == (data = item->pop(0))) {
        do_push((void*)WAKE_ID, pop_idx + 1);
      }
    }
    if ((void*)WAKE_ID == data) {
      err = OB_ENTRY_NOT_EXIST;
      data = NULL;
    }
    return err;
  }

  int do_push(void* data, uint64_t limit)
  {
    int err = OB_SUCCESS;
    uint64_t push_idx = faa_bounded(&push_, limit);
    if (push_idx >= limit) {
      err = OB_SIZE_OVERFLOW;
    } else {
      items_[idx(push_idx)].push(data);
    }
    return err;
  }

private:
  uint64_t push_ CACHE_ALIGNED;
  uint64_t pop_ CACHE_ALIGNED;
  uint64_t capacity_ CACHE_ALIGNED;
  void* allocated_;
  Item* items_;
};

};     // namespace common
};     // end namespace oceanbase
#endif /* OCEANBASE_QUEUE_OB_FUTEX_QUEUE_H_ */
