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

#ifndef OB_COMMON_OB_LIGHTY_QUEUE_
#define OB_COMMON_OB_LIGHTY_QUEUE_

#include <stdint.h>
#include <stddef.h>
#include "lib/allocator/ob_malloc.h"
#include "lib/ob_define.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/coro/co.h"

namespace oceanbase {
namespace common {
struct ObLightyCond {
public:
  ObLightyCond() : n_waiters_(0)
  {}
  ~ObLightyCond()
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
  void wait(const uint32_t cmp, const int64_t timeout)
  {
    if (timeout > 0) {
      (void)ATOMIC_FAA(&n_waiters_, 1);
      (void)futex_.wait(cmp, timeout);
      (void)ATOMIC_FAA(&n_waiters_, -1);
    }
  }

private:
  lib::CoFutex futex_;
  uint32_t n_waiters_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLightyCond);
};

class ObLightyQueue {
public:
  typedef ObLightyCond Cond;
  ObLightyQueue() : capacity_(0), n_cond_(0), data_(NULL), cond_(NULL), push_(0), pop_(0)
  {}
  ~ObLightyQueue()
  {
    destroy();
  }
  int init(const uint64_t capacity, const lib::ObLabel& label = ObModIds::OB_LIGHTY_QUEUE,
      const uint64_t tenant_id = common::OB_SERVER_TENANT_ID)
  {
    int ret = OB_SUCCESS;
    uint64_t n_cond = calc_n_cond(capacity);
    ObMemAttr attr;
    attr.tenant_id_ = tenant_id;
    attr.label_ = label;
    if (is_inited()) {
      ret = OB_INIT_TWICE;
    } else if (NULL == (data_ = (void**)ob_malloc(capacity * sizeof(void*) + n_cond * sizeof(Cond), attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      memset(data_, 0, capacity * sizeof(void*));
      capacity_ = capacity;
      n_cond_ = n_cond;
      cond_ = (Cond*)(data_ + capacity);
      for (int i = 0; i < n_cond; i++) {
        new (cond_ + i) Cond();
      }
    }
    return ret;
  }
  void destroy()
  {
    if (NULL != data_) {
      ob_free(data_);
      data_ = NULL;
      cond_ = NULL;
    }
  }
  void reset()
  {
    clear();
  }
  void clear()
  {
    void* p = NULL;
    while (OB_SUCCESS == pop(p, 0))
      ;
  }
  int64_t size() const
  {
    return (int64_t)(ATOMIC_LOAD(&push_) - ATOMIC_LOAD(&pop_));
  }
  int64_t capacity() const
  {
    return capacity_;
  }
  bool is_inited() const
  {
    return NULL != data_;
  }
  int push(void* p)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(data_)) {
      ret = OB_NOT_INIT;
    } else {
      uint64_t limit = ATOMIC_LOAD(&pop_) + capacity_;
      uint64_t seq = push_bounded(p, limit);
      if (seq >= limit) {
        ret = OB_SIZE_OVERFLOW;
      }
    }
    return ret;
  }
  int pop(void*& p, int64_t timeout = 0)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(data_)) {
      ret = OB_NOT_INIT;
    } else {
      void* DUMMY = (void*)(~0ULL);
      uint64_t seq = ATOMIC_FAA(&pop_, 1);
      uint64_t push_idx = ATOMIC_LOAD(&push_);
      if (push_idx <= seq) {
        int64_t abs_timeout = (timeout > 0 ? (get_us() + timeout) : 0);
        while ((push_idx = wait_push(seq, timeout)) <= seq && (timeout = abs_timeout - get_us()) > 0) {
          PAUSE();
        }
        while ((push_idx = push_bounded(DUMMY, seq + 1)) < seq) {
          PAUSE();
        }
      }
      if (DUMMY == (p = fetch(seq))) {
        p = NULL;
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
    return ret;
  }

private:
  static uint64_t calc_n_cond(uint64_t capacity)
  {
    return std::min(1024ULL, 1ULL << (63 - __builtin_clzll(capacity)));
  }
  uint64_t push_bounded(void* p, uint64_t limit)
  {
    uint64_t seq = inc_if_lt(&push_, limit);
    if (seq < limit) {
      store(seq, p);
      get_cond(seq).signal();
    }
    return seq;
  }
  uint64_t wait_push(uint64_t seq, int64_t timeout)
  {
    uint32_t wait_id = get_cond(seq).get_seq();
    uint64_t push_idx = ATOMIC_LOAD(&push_);
    if (push_idx <= seq) {
      get_cond(seq).wait(wait_id, timeout);
    }
    return push_idx;
  }
  static int64_t get_us()
  {
    return ::oceanbase::common::ObTimeUtility::current_time();
  }
  uint64_t idx(uint64_t x)
  {
    return x % capacity_;
  }
  Cond& get_cond(uint64_t seq)
  {
    return cond_[seq % n_cond_];
  }
  static uint64_t inc_if_lt(uint64_t* addr, uint64_t b)
  {
    uint64_t ov = ATOMIC_LOAD(addr);
    uint64_t nv = 0;
    while (ov < b && ov != (nv = ATOMIC_VCAS(addr, ov, ov + 1))) {
      ov = nv;
    }
    return ov;
  }
  void* fetch(uint64_t seq)
  {
    void* p = NULL;
    void** addr = data_ + idx(seq);
    while (NULL == ATOMIC_LOAD(addr) || NULL == (p = ATOMIC_TAS(addr, NULL))) {
      PAUSE();
    }
    return p;
  }
  void store(uint64_t seq, void* p)
  {
    void** addr = data_ + idx(seq);
    while (!ATOMIC_BCAS(addr, NULL, p)) {
      PAUSE();
    }
  }

private:
  uint64_t capacity_;
  uint64_t n_cond_;
  void** data_;
  Cond* cond_;
  uint64_t push_ CACHE_ALIGNED;
  uint64_t pop_ CACHE_ALIGNED;
};

class LightyQueue {
public:
  typedef ObLightyCond Cond;
  LightyQueue()
  {}
  ~LightyQueue()
  {
    destroy();
  }

public:
  int init(const uint64_t capacity, const lib::ObLabel& label = ObModIds::OB_LIGHTY_QUEUE);
  void destroy()
  {
    queue_.destroy();
  }
  void reset();
  int64_t size() const
  {
    return queue_.get_total();
  }
  int64_t curr_size() const
  {
    return queue_.get_total();
  }
  int64_t max_size() const
  {
    return queue_.capacity();
  }
  bool is_inited() const
  {
    return queue_.is_inited();
  }
  int push(void* data, const int64_t timeout = 0);
  int pop(void*& data, const int64_t timeout = 0);
  int multi_pop(void** data, const int64_t data_count, int64_t& avail_count, const int64_t timeout = 0);

private:
  typedef ObFixedQueue<void> Queue;
  Queue queue_;
  Cond cond_;

private:
  DISALLOW_COPY_AND_ASSIGN(LightyQueue);
};
};  // end namespace common
};  // end namespace oceanbase

#endif /* __OB_COMMON_OB_LIGHTY_QUEUE_H__ */
