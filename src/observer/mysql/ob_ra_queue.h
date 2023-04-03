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

#ifndef OCEANBASE_MYSQL_OB_RA_QUEUE_H_
#define OCEANBASE_MYSQL_OB_RA_QUEUE_H_

#include "lib/allocator/ob_allocator.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace common
{
class ObRaQueue
{
public:
  const uint64_t LOCK = ~0LL;
  enum { N_REF = 1024 };
  struct Ref
  {
    Ref(): idx_(-1), val_(NULL) {}
    ~Ref() {}
    void set(int64_t idx, void* val) {
      idx_ = idx;
      val_ = val;
    }
    void reset() {
      idx_ = -1;
      val_ = NULL;
    }
    TO_STRING_KV("idx", idx_, "val", val_);
    int64_t idx_;
    void* val_;
  };
  ObRaQueue(): push_(0), pop_(0), capacity_(0), array_(NULL) {
    memset(ref_, 0, sizeof(ref_));
  }
  ~ObRaQueue() { destroy(); }
  int init(const char *label, uint64_t size) {
    int ret = OB_SUCCESS;
    if (NULL == (array_ = (void**)ob_malloc(sizeof(void*) * size, label))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      memset(array_, 0, sizeof(void*) * size);
      capacity_ = size;
    }
    return ret;
  }

  int init(const char *label, uint64_t size, uint64_t tenant_id) {
    int ret = OB_SUCCESS;
    ObMemAttr mem_attr;
    mem_attr.label_ = label;
    mem_attr.tenant_id_ = tenant_id;
    if (size <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_ISNULL(array_ = (void**)ob_malloc(sizeof(void*) * size, mem_attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMSET(array_, 0, sizeof(void*) * size);
      capacity_ = size;
    }
    return ret;
  }
  void destroy() {
    if (NULL != array_) {
      ob_free(array_);
      array_ = NULL;
    }
  }
  uint64_t get_push_idx() const { return ATOMIC_LOAD(&push_); }
  uint64_t get_pop_idx() const { return ATOMIC_LOAD(&pop_); }
  uint64_t get_capacity() const { return capacity_; }
  uint64_t get_size() const {
    uint64_t pop_idx = get_pop_idx();
    uint64_t push_idx = get_push_idx();
    return push_idx - pop_idx;
  }
  void* pop() {
    void* p = NULL;
    if (NULL == array_) {
      // ret = OB_NOT_INIT;
    } else {
      uint64_t pop_limit = 0;
      uint64_t pop_idx = faa_bounded(&pop_, &push_, pop_limit);
      if (pop_idx < pop_limit) {
        void** addr = get_addr(pop_idx);
        while(NULL == (p = ATOMIC_LOAD(addr)) || LOCK == (uint64_t)p || !ATOMIC_BCAS(addr, p, NULL))
          ;
        wait_ref_clear(pop_idx);
      } else {
        // ret = OB_SIZE_OVERFLOW;
      }
    }
    return p;
  }
  int push(void* p, int64_t& seq) {
    int ret = OB_SUCCESS;
    if (NULL == array_) {
      ret = OB_NOT_INIT;
    } else if (NULL == p) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      uint64_t push_limit = ATOMIC_LOAD(&pop_) + capacity_;
      uint64_t push_idx = faa_bounded(&push_, &push_limit, push_limit);
      if (push_idx < push_limit) {
        void** addr = get_addr(push_idx);
        while(!ATOMIC_BCAS(addr, NULL, p))
          ;
        seq = push_idx;
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
    return ret;
  }
  int push_with_imme_seq(void* p, int64_t& seq) {
    int ret = OB_SUCCESS;
    if (NULL == array_) {
      ret = OB_NOT_INIT;
    } else if (NULL == p) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      uint64_t push_limit = ATOMIC_LOAD(&pop_) + capacity_;
      uint64_t push_idx = faa_bounded(&push_, &push_limit, push_limit);
      if (push_idx < push_limit) {
        void** addr = get_addr(push_idx);
        seq = push_idx;
        while(!ATOMIC_BCAS(addr, NULL, p))
          ;
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
    return ret;
  }
  void* get(uint64_t seq, Ref* ref) {
    void* ret = NULL;
    if (NULL != array_) {
      void** addr = get_addr(seq);
      while(LOCK == (uint64_t)(ret = ATOMIC_TAS(addr, (void*)LOCK)))
        ;
      if (NULL == ret) {
        ATOMIC_STORE(addr, NULL);
      } else {
        ref->set(seq, ret);
      }
    }
    if (NULL != ret) {
      xref(seq, 1);
      ATOMIC_STORE(get_addr(seq), ret);
    }
    return ret;
  }
  void revert(Ref* ref) {
    if (NULL != ref) {
      xref(ref->idx_, -1);
      ref->reset();
    }
  }
private:
  void wait_ref_clear(int64_t seq) {
    while(0 != ATOMIC_LOAD(ref_ + seq % N_REF)) {
      ob_usleep(1000);
    }
  }
  int64_t xref(int64_t seq, int64_t x) {
    return ATOMIC_AAF(ref_ + seq % N_REF, x);
  }
  void do_revert(uint64_t seq, void* p) {
    if (NULL != array_ && NULL != p) {
      void** addr = get_addr(seq);
      ATOMIC_STORE(addr, p);
    }
  }
  static uint64_t faa_bounded(uint64_t* addr, uint64_t* limit_addr, uint64_t& limit) {
    uint64_t val = ATOMIC_LOAD(addr);
    uint64_t ov = 0;
    limit = ATOMIC_LOAD(limit_addr);
    while (((ov = val) < limit || val < (limit = ATOMIC_LOAD(limit_addr)))
           && ov != (val = ATOMIC_VCAS(addr, ov, ov + 1))) {
      PAUSE();
    }
    return val;
  }
  void** get_addr(uint64_t x) { return array_ + idx(x); }
  uint64_t idx(uint64_t x) { return x % capacity_; }
private:
  uint64_t push_ CACHE_ALIGNED;
  uint64_t pop_ CACHE_ALIGNED;
  uint64_t capacity_ CACHE_ALIGNED;
  int64_t ref_[N_REF] CACHE_ALIGNED;
  void** array_ CACHE_ALIGNED;
};
}; // end namespace container
}; // end namespace oceanbase

#endif /* OCEANBASE_MYSQL_OB_RA_QUEUE_H_ */
