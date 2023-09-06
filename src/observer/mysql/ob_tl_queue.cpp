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

#define USING_LOG_PREFIX SERVER

#include "observer/mysql/ob_tl_queue.h"
#include <iostream>

namespace oceanbase {
namespace common {
int ObTLQueue::ObSubQueue::init(const char *label, uint64_t capacity,
                                uint64_t tenant_id) {
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  mem_attr.label_ = label;
  mem_attr.tenant_id_ = tenant_id;
  if (capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(array_ = (void **)ob_malloc(sizeof(void *) * capacity,
                                                   mem_attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    memset(array_, 0, sizeof(void *) * capacity);
    next_ = 0;
    capacity_ = capacity;
  }
  return ret;
}

void ObTLQueue::ObSubQueue::destroy(ObConcurrentFIFOAllocator *allocator) {
  if (OB_ISNULL(array_)) {
  } else if(OB_ISNULL(allocator)) {
  } else {
    void *p = NULL;
    uint64_t idx = 0;
    while (idx < next_) {
      if (OB_NOT_NULL(p = array_[idx++])) {
        allocator->free(p);
        p = NULL;
      }
    }
    ob_free(array_);
    array_ = NULL;
  }
}

int ObTLQueue::ObSubQueue::push(void *p, int64_t &seq) {
  int ret = OB_SUCCESS;
  if (NULL == array_) {
    ret = OB_NOT_INIT;
  } else if (NULL == p) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint64_t next = ATOMIC_LOAD(&next_);
    uint64_t ov = next;
    while ((ov = next) < capacity_ &&
           ov != (next = ATOMIC_VCAS(&next_, ov, ov + 1))) {
      ;
    }
    if (next >= capacity_) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      array_[next] = p;
      seq = next;
    }
  }
  return ret;
}

void *ObTLQueue::ObSubQueue::get(uint64_t idx) {
  void *p = NULL;
  if (NULL == array_) {
  } else if (idx >= capacity_) {
  } else {
    p = array_[idx];
  }
  return p;
}

int ObTLQueue::init(const char *label, uint64_t capacity, uint64_t subcapacity,
                    ObConcurrentFIFOAllocator *allocator, uint64_t tenant_id) {
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  mem_attr.label_ = label;
  mem_attr.tenant_id_ = tenant_id;
  if (capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(array_ = (ObSubQueue **)ob_malloc(
                           sizeof(ObSubQueue *) * capacity, mem_attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(ref_ = (int64_t *)ob_malloc(sizeof(int64_t) * capacity,
                                                   mem_attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    memset(ref_, 0, sizeof(int64_t) * capacity);
    memset(array_, 0, sizeof(ObSubQueue *) * capacity);
    capacity_ = capacity;
    subcapacity_ = subcapacity;
    mem_label_ = label;
    tenant_id_ = tenant_id;
    allocator_ = allocator;
    for (uint64_t i = 0; OB_SUCC(ret) && i < QUEUE_POOL_SIZE; ++i) {
      if (OB_ISNULL(queue_pool_[i] = (ObSubQueue *)ob_malloc(sizeof(ObSubQueue),
                                                             mem_attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(
                     queue_pool_[i]->init(label, subcapacity_, tenant_id_))) {
        /* do nothing */
      }
    }
  }
  return ret;
}

int ObTLQueue::push_with_retry(void *p, int64_t &seq) {
  int ret = OB_SUCCESS;
  while (OB_ENTRY_NOT_EXIST == (ret = push(p, seq))) {
    ;
  }
  return ret;
}
int ObTLQueue::push(void *p, int64_t &seq) {
  int ret = OB_SUCCESS;
  if (NULL == array_) {
    ret = OB_NOT_INIT;
  } else if (NULL == p) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint64_t pop = get_pop_idx();
    uint64_t push = get_push_idx();
    // 外层队列已满
    if (push >= pop + capacity_) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      ObSubQueue **subq_addr = get_subq_addr(push);
      ObSubQueue *subq = NULL;
      while ((ObSubQueue *)LOCK ==
             (subq = ATOMIC_VCAS(subq_addr, NULL, (ObSubQueue *)LOCK))) {
      }
      // 该线程首先进入需要申请内存
      if (OB_ISNULL(subq)) {
        // todo(fhkong): 后面将malloc and init操作替换为 memory pool;
        // fixed(fhkong): queue_pool 操作需要上锁
        ObMemAttr mem_attr;
        mem_attr.label_ = mem_label_;
        mem_attr.tenant_id_ = tenant_id_;
        if (NULL != (subq = get_subq_from_pool())) {
        } else if (OB_ISNULL(subq = (ObSubQueue *)ob_malloc(sizeof(ObSubQueue), mem_attr))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_FAIL(subq->init(mem_label_, subcapacity_, tenant_id_))) {
          ret = OB_NOT_INIT;
        }
        ATOMIC_STORE(subq_addr, subq);
      }
      // 多个线程同时插入一个subq
      int64_t subseq = 0;
      xref(push, 1);
      if (OB_SUCC(subq->push(p, subseq))) {
        seq = push * subcapacity_ + subseq;
      } else {
        // 插入失败, head++, 重新尝试插入
        ATOMIC_BCAS(&push_, push, push + 1);
      }
      xref(push, -1);
    }
  }
  return ret;
}

int ObTLQueue::pop_batch() {
  int ret = OB_SUCCESS;
  if (NULL == array_) {
    ret = OB_NOT_INIT;
  } else {
    uint64_t pop_limit = 0;
    uint64_t pop = faa_bounded(&pop_, &push_, pop_limit);
    if (pop <= pop_limit) {
      ObSubQueue **subq_addr = get_subq_addr(pop);
      ObSubQueue *subq = NULL;
      while (NULL != (subq = ATOMIC_LOAD(subq_addr)) && 
            (LOCK == (uint64_t)subq || !ATOMIC_BCAS(subq_addr, subq, NULL))) {
      }
      if (OB_ISNULL(subq)) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        wait_ref_clear(pop);
        subq->destroy(allocator_);
        ob_free(subq);
      }
    }
  }
  return ret;
}

void *ObTLQueue::get(uint64_t idx, Ref *ref) {
  void *ret = NULL;
  ObSubQueue *subq = NULL;
  if (OB_ISNULL(array_)) {
  } else {
    uint64_t fidx = idx / subcapacity_;
    uint64_t sidx = idx % subcapacity_;
    ObSubQueue **subq_addr = get_subq_addr(fidx);
    while (LOCK ==
           (uint64_t)(subq = ATOMIC_TAS(subq_addr, (ObSubQueue *)LOCK))) {
    }
    if (NULL == subq) {
      ATOMIC_STORE(subq_addr, NULL);
    } else {
      xref(fidx, 1);
      ATOMIC_STORE(subq_addr, subq);
      ret = subq->get(sidx);
      ref->set(idx, ret);
    }
  }
  return ret;
}

void ObTLQueue::destroy() {
  // free queue_pool_
  for (uint64_t i = 0; i < QUEUE_POOL_SIZE; ++i) {
    if (NULL != queue_pool_[i]) {
      queue_pool_[i]->destroy(allocator_);
      ob_free(queue_pool_[i]);
      queue_pool_[i] = NULL;
    }
  }
  if (NULL != array_) {
    ObSubQueue *subq = NULL;
    while (pop_ <= push_) {
      if (OB_ISNULL(subq = array_[idx(pop_++)])) {
      } else {
        subq->destroy(allocator_);
        ob_free(subq);
        subq = NULL;
      }
    }
    ob_free(array_);
    array_ = NULL;
  }
  if (NULL != ref_) {
    ob_free(ref_);
    ref_ = NULL;
  }
}
}; // end namespace common
}; // end namespace oceanbase
