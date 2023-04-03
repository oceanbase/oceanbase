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

#ifndef OCEANBASE_QUEUE_OB_LINK_QUEUE_
#define OCEANBASE_QUEUE_OB_LINK_QUEUE_
#include "lib/ob_define.h"
#include "lib/queue/ob_link.h"  // ObLink

namespace oceanbase
{
namespace common
{
typedef ObLink QLink;

static inline uint64_t faa_bounded(uint64_t *addr, uint64_t *limit_addr, uint64_t &limit)
{
  uint64_t rr = ATOMIC_LOAD(addr);
  limit = ATOMIC_LOAD(limit_addr);
  if (rr < limit) {
    rr = ATOMIC_FAA(addr,1);
    if (rr >= limit) {
      while (true) {
        if (ATOMIC_BCAS(addr, rr + 1, rr)) {
          break;
        }
        limit = ATOMIC_LOAD(limit_addr);
        if (rr < limit) {
          break;
        }
        PAUSE();
      }
    }
  }
  return rr;
}

static inline uint64_t faa_bounded2(uint64_t *addr, uint64_t *limit_addr, uint64_t &limit)
{
  uint64_t ov = 0;
  uint64_t nv = ATOMIC_LOAD(addr);
  while (((ov = nv) < limit || ov < (limit = ATOMIC_LOAD(limit_addr)))
          && ov != (nv = ATOMIC_CAS(addr, ov, ov + 1))) {
    PAUSE();
  }
  return nv;
}

class ObSpScLinkQueue
{
public:
  typedef QLink Link;
  ObSpScLinkQueue(): tail_(&head_) {}
  ~ObSpScLinkQueue() {}
  Link* top() const { return ATOMIC_LOAD(&head_.next_); }
  bool empty() const { return top() == NULL; }
  void push(Link* p) {
    p->next_ = NULL;
    Link* ot = ATOMIC_TAS(&tail_, p);
    ATOMIC_STORE(&ot->next_, p);
  }
  Link* pop() {
    Link* ret = top();
    if (NULL != ret) {
      Link* next = ATOMIC_LOAD(&ret->next_);
      if (NULL != next) {
        ATOMIC_STORE(&head_.next_, next);
      } else {
        if (ATOMIC_BCAS(&tail_, ret, &head_)) {
          // if update successfully, there is no parallel push, else there is no need to update,
          IGNORE_RETURN ATOMIC_BCAS(&head_.next_, ret, NULL);
        } else {
          while(NULL == (next = ATOMIC_LOAD(&ret->next_))) {
            PAUSE();
          }
          ATOMIC_STORE(&head_.next_, next);
        }
      }
    }
    return ret;
  }
private:
  Link head_;
  Link* tail_;
};

class ObSpLinkQueue
{
public:
  typedef QLink Link;
  ObSpLinkQueue() : head_(&dummy_), tail_(&dummy_), dummy_() {}
  ~ObSpLinkQueue() {}
public:
  int top(Link *&p)
  {
    int ret = OB_SUCCESS;
    Link *head = ATOMIC_LOAD(&head_);
    if (head != &dummy_) {
      p = head;
    } else if (NULL == head->next_) {
      ret = OB_EAGAIN;
    } else {
      ATOMIC_STORE(&head_, head->next_);
      p = ATOMIC_LOAD(&head_);
      ret = push(&dummy_);
    }
    return ret;
  }
  int pop(Link *&p)
  {
    int ret = OB_SUCCESS;
    while(OB_SUCCESS == (ret = do_pop(p)) && p == &dummy_) {
      ret = push(p);
    }
    if (OB_SUCCESS != ret) {
      p = NULL;
    }
    return ret;
  }
  int push(Link *p)
  {
    int ret = OB_SUCCESS;
    Link *tail = NULL;
    if (OB_UNLIKELY(NULL == p)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      p->next_ = NULL;
      tail = ATOMIC_TAS(&tail_, p);
      ATOMIC_STORE(&tail->next_, p);
    }
    return ret;
  }
  int push_front(Link *p)
  {
    int ret = OB_SUCCESS;
    Link *head = NULL;
    if (OB_UNLIKELY(NULL == p)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      while(NULL == (head = ATOMIC_TAS(&head_, NULL))) {
        PAUSE();
      }
      ATOMIC_STORE(&p->next_, head);
      ATOMIC_STORE(&head_, p);
    }
    return ret;
  }
  bool is_empty() const { return ATOMIC_LOAD(&head_) == ATOMIC_LOAD(&tail_); }
private:
  int do_pop(Link *&p)
  {
    int ret = OB_SUCCESS;
    Link *head = NULL;
    while(NULL == (head = ATOMIC_TAS(&head_, NULL))) {
      PAUSE();
    }
    if (head == ATOMIC_LOAD(&tail_)) {
      ret = OB_EAGAIN;
      ATOMIC_STORE(&head_, head);
    } else {
      Link* next = NULL;
      while(NULL == (next = ATOMIC_LOAD(&head->next_))) {
        PAUSE();
      }
      ATOMIC_STORE(&head_, next);
      p = head;
    }
    return ret;
  }
private:
  Link *head_ CACHE_ALIGNED;
  Link *tail_ CACHE_ALIGNED;
  Link dummy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSpLinkQueue);
};

class ObLinkQueue
{
public:
  typedef QLink Link;
  enum { QUEUE_COUNT = 512 };
  ObLinkQueue() : queue_(), push_(0), pop_(0)
  {}
  ~ObLinkQueue() {}
  int push(Link *p)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(NULL == p)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      uint64_t push_idx = ATOMIC_FAA(&push_, 1);
      ObSpLinkQueue *pqueue = queue_ + idx(push_idx);
      while(OB_SUCCESS != pqueue->push(p)) {
        ;
      }
    }
    return ret;
  }
  int push_front(Link *p)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(NULL == p)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      uint64_t push_idx = ATOMIC_SAF(&pop_, 1);
      ObSpLinkQueue *pqueue = queue_ + idx(push_idx);
      while(OB_SUCCESS != pqueue->push_front(p)) {
        ;
      }
    }
    return ret;
  }
  int pop(Link *&p)
  {
    int ret = OB_SUCCESS;
    uint64_t pop_limit = 0;
    uint64_t pop_idx = faa_bounded(&pop_, &push_, pop_limit);
    if (pop_idx >= pop_limit) {
      ret = OB_EAGAIN;
      p = NULL;
    } else {
      ObSpLinkQueue *pqueue = queue_ + idx(pop_idx);
      while(OB_SUCCESS != pqueue->pop(p)) {
        ;
      }
    }
    return ret;
  }
  int64_t size() const
  {
    uint64_t pop = ATOMIC_LOAD(&pop_);
    uint64_t push = ATOMIC_LOAD(&push_);
    return (int64_t)(push - pop);
  }
  int merge(ObLinkQueue &other)
  {
    int ret = OB_SUCCESS;
    while(other.size() > 0) {
      ObLink *p = NULL;
      (void)other.pop(p);
      if (OB_FAIL(push(p))) {
        break;
      }
    }
    return ret;
  }
private:
  static int64_t idx(int64_t x) { return x & (QUEUE_COUNT - 1); }
private:
  ObSpLinkQueue queue_[QUEUE_COUNT];
  uint64_t push_ CACHE_ALIGNED;
  uint64_t pop_ CACHE_ALIGNED;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLinkQueue);
};

class ObSimpleLinkQueue
{
public:
  typedef QLink Link;
  ObSimpleLinkQueue() : head_(&dummy_), tail_(&dummy_), dummy_() {}
  ~ObSimpleLinkQueue() {}
public:
  int top(Link *&p);
  int pop(Link *&p);
  int push(Link *p);
  inline bool is_empty() const
  {
    return head_ == tail_;
  }
private:
  int do_pop(Link *&p);
private:
  Link *head_ CACHE_ALIGNED;
  Link *tail_ CACHE_ALIGNED;
  Link dummy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSimpleLinkQueue);
};

} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_QUEUE_OB_LINK_QUEUE_
