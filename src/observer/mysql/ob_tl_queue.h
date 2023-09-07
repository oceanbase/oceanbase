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

#ifndef OCEANBASE_MYSQL_OB_TL_QUEUE_H_
#define OCEANBASE_MYSQL_OB_TL_QUEUE_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "share/ob_define.h"
#include "observer/mysql/ob_ra_queue.h" // use ObRaQueue::Ref

namespace oceanbase
{
namespace common
{
using Ref = ObRaQueue::Ref;
/**
 * Two-Level Queue supports:
 *  1. dynamic increase and shrink of memory.
 *  2. high concurrency and thread safety.
*/
class ObTLQueue {
 private:
  class ObSubQueue {
   public:
    ObSubQueue() : next_(0), capacity_(0), array_(nullptr) {}
    int init(const char *label, uint64_t capacity, uint64_t tenant_id);
    // note(fhkong): 上层保证 destroy 一定只有一个线程操作
    void destroy(ObConcurrentFIFOAllocator *allocator);
    // 多个生产者
    int push(void *p, int64_t &seq);
    // note(fhkong): get之前首先在外层将ref++
    void *get(uint64_t idx);
    uint64_t get_next_idx() { return ATOMIC_LOAD(&next_); }

   private:
    uint64_t next_;
    uint64_t capacity_;
    void **array_;
  }; /* end define ObSubQueue */

 public:
  static constexpr uint64_t LOCK = ~0LL;
  ObTLQueue()
      : pop_(0), push_(0), capacity_(0), subcapacity_(0), mem_label_(NULL),
        ref_(NULL), array_(NULL), allocator_(NULL) 
  { }
  ~ObTLQueue() { destroy(); }
  int init(const char *label, uint64_t capacity, uint64_t subcapacity,
           ObConcurrentFIFOAllocator *allocator = NULL, 
           uint64_t tenant_id = OB_INVALID_TENANT_ID);
  // 插入失败表明当前二级队列已满，需要 retry
  int push_with_retry(void *p, int64_t &seq);
  int push(void *p, int64_t &seq);

  /* 1. ref为0的时候才允许pop
  *  2. 后台单线程pop 
  *  3. 允许pop正在push的二级队列 */
  int pop_batch();
  void *get(uint64_t idx, Ref *ref);
  void destroy();
  uint64_t get_head_idx() const { return get_pop_idx() * subcapacity_; }
  uint64_t get_tail_idx() const {
    uint64_t pop = get_pop_idx();
    uint64_t push = get_push_idx();
    return push * subcapacity_ + (push < pop + capacity_ ? get_subq_size(push) : 0);
  }
  uint64_t get_size() const {
    return get_tail_idx() - get_head_idx();
  }

  uint64_t get_alloc() const {
    uint64_t pop = get_pop_idx();
    uint64_t push = get_push_idx();
    return (push - pop) * subcapacity_ + 
       ((push < pop + capacity_ && get_subq_size(push)) > 0 ? subcapacity_ : 0);
  }

  uint64_t get_capacity() const {
    return capacity_ * subcapacity_;
  }
  void revert(Ref *ref) {
    if (NULL != ref) {
      xref(ref->idx_ / subcapacity_, -1);
      ref->reset();
    }
  }

 private:
  uint64_t get_pop_idx() const { return ATOMIC_LOAD(&pop_); }
  uint64_t get_push_idx() const { return ATOMIC_LOAD(&push_); }
  ObSubQueue **get_subq_addr(uint64_t x) { return array_ + idx(x); }
  uint64_t idx(uint64_t x) const { return x % capacity_; }
  uint64_t faa_bounded(uint64_t *addr, uint64_t *limit_addr, uint64_t &limit) {
    uint64_t val = ATOMIC_LOAD(addr);
    uint64_t ov = 0;
    limit = ATOMIC_LOAD(limit_addr);
    while (((ov = val) < limit || val < (limit = ATOMIC_LOAD(limit_addr))) &&
           ov != (val = ATOMIC_VCAS(addr, ov, ov + 1))) {
      PAUSE();
    }
    return val;
  }
  void wait_ref_clear(int64_t x) {
    while (0 != ATOMIC_LOAD(ref_ + idx(x))) {
      usleep(1000);
    }
  }
  int64_t xref(int64_t x, int64_t v) { return ATOMIC_AAF(ref_ + idx(x), v); }
  uint64_t get_subq_size(uint64_t x) const {
    uint64_t ret = 0;
    if (NULL != array_) {
      ObSubQueue *subq = array_[idx(x)];
      if (NULL != subq && (ObSubQueue *)LOCK != subq) {
        ret = subq->get_next_idx();
      }
    }
    return ret;
  }

 private:
  uint64_t pop_;
  uint64_t push_;
  uint64_t capacity_;
  uint64_t subcapacity_;
  const char *mem_label_;
  uint64_t tenant_id_;
  int64_t *ref_;
  ObSubQueue **array_;
  ObConcurrentFIFOAllocator *allocator_;  
};
} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBASE_MYSQL_OB_RA_QUEUE_H_ */
