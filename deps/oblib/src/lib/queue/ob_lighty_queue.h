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

#include "lib/alloc/alloc_struct.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/lock/ob_futex.h"
#include "lib/queue/ob_fixed_queue.h"

namespace oceanbase
{
namespace common
{
struct ObLightyCond
{
public:
  ObLightyCond(): n_waiters_(0) {}
  ~ObLightyCond() {}

  void signal();
  uint32_t get_seq() { return ATOMIC_LOAD(&futex_.uval()); }
  void wait(const uint32_t cmp, const int64_t timeout);
private:
  lib::ObFutex futex_;
  uint32_t n_waiters_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLightyCond);
};

class ObLightyQueue
{
public:
  typedef ObLightyCond Cond;
  ObLightyQueue(): capacity_(0), n_cond_(0), data_(NULL), cond_(NULL), push_(0), pop_(0) {}
  ~ObLightyQueue() { destroy(); }
  int init(const uint64_t capacity,
           const lib::ObLabel &label = ObModIds::OB_LIGHTY_QUEUE,
           const uint64_t tenant_id = common::OB_SERVER_TENANT_ID);
  void destroy();
  void reset() { clear(); }
  void clear();
  int64_t size() const { return (int64_t)(ATOMIC_LOAD(&push_) - ATOMIC_LOAD(&pop_)); }
  int64_t capacity() const { return capacity_; }
  bool is_inited() const { return NULL != data_; }
  int push(void* p);
  int pop(void*& p, int64_t timeout = 0);
private:
  static uint64_t calc_n_cond(uint64_t capacity)
  {
    OB_ASSERT(0ULL != capacity);
    return std::min(1024ULL, 1ULL<<(63 - __builtin_clzll(capacity)));
  }
  uint64_t push_bounded(void* p, uint64_t limit);
  uint64_t wait_push(uint64_t seq, int64_t timeout);
  static int64_t get_us();
  uint64_t idx(uint64_t x) { return x % capacity_; }
  Cond& get_cond(uint64_t seq) { return cond_[seq % n_cond_]; }
  static uint64_t inc_if_lt(uint64_t *addr, uint64_t *limit_addr, uint64_t delta, uint64_t &limit);
  static uint64_t inc_if_lt(uint64_t* addr, uint64_t b);
  void* fetch(uint64_t seq);
  void store(uint64_t seq, void* p);
private:
  uint64_t capacity_;
  uint64_t n_cond_;
  void** data_;
  Cond* cond_;
  uint64_t push_ CACHE_ALIGNED;
  uint64_t pop_ CACHE_ALIGNED;
};

class LightyQueue
{
public:
  typedef ObLightyCond Cond;
  LightyQueue() {}
  ~LightyQueue() { destroy(); }
public:
  int init(const uint64_t capacity,
           const lib::ObLabel &label = ObModIds::OB_LIGHTY_QUEUE,
           const uint64_t tenant_id = common::OB_SERVER_TENANT_ID);
  void destroy() { queue_.destroy(); }
  void reset();
  int64_t size() const { return queue_.get_total(); }
  int64_t curr_size() const { return queue_.get_total(); }
  int64_t max_size() const { return queue_.capacity(); }
  bool is_inited() const { return queue_.is_inited(); }
  int push(void *data, const int64_t timeout = 0);
  int pop(void *&data, const int64_t timeout = 0);
  int multi_pop(void **data, const int64_t data_count, int64_t &avail_count, const int64_t timeout = 0);
private:
  typedef ObFixedQueue<void> Queue;
  Queue queue_;
  Cond cond_;
private:
  DISALLOW_COPY_AND_ASSIGN(LightyQueue);
};
}; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_LIGHTY_QUEUE_H__ */
