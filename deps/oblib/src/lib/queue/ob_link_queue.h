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

namespace oceanbase {
namespace common {
typedef ObLink QLink;

class ObSpScLinkQueue {
public:
  typedef QLink Link;
  ObSpScLinkQueue() : tail_(&head_)
  {}
  ~ObSpScLinkQueue()
  {}
  Link* top()
  {
    return ATOMIC_LOAD(&head_.next_);
  }
  bool is_empty() const
  {
    return (NULL == ATOMIC_LOAD(&head_.next_));
  }
  void push(Link* p)
  {
    p->next_ = NULL;
    Link* ot = ATOMIC_TAS(&tail_, p);
    ATOMIC_STORE(&ot->next_, p);
  }
  Link* pop()
  {
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
          while (NULL == (next = ATOMIC_LOAD(&ret->next_))) {
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

class ObSpLinkQueue {
public:
  typedef QLink Link;
  ObSpLinkQueue() : head_(&dummy_), tail_(&dummy_), dummy_()
  {}
  ~ObSpLinkQueue()
  {}

public:
  int top(Link*& p);
  int pop(Link*& p);
  int push(Link* p);
  int push_front(Link* p);
  bool is_empty() const;

private:
  int do_pop(Link*& p);

private:
  Link* head_ CACHE_ALIGNED;
  Link* tail_ CACHE_ALIGNED;
  Link dummy_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSpLinkQueue);
};

class ObLinkQueue {
public:
  typedef QLink Link;
  enum { QUEUE_COUNT = 256 };
  ObLinkQueue() : queue_(), push_(0), pop_(0)
  {}
  ~ObLinkQueue()
  {}
  int push(Link* p);
  int push_front(Link* p);
  int pop(Link*& p);
  int64_t size() const;

private:
  static int64_t idx(int64_t x)
  {
    return x & (QUEUE_COUNT - 1);
  }

private:
  ObSpLinkQueue queue_[QUEUE_COUNT];
  uint64_t push_ CACHE_ALIGNED;
  uint64_t pop_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLinkQueue);
};

class ObSimpleLinkQueue {
public:
  typedef QLink Link;
  ObSimpleLinkQueue() : head_(&dummy_), tail_(&dummy_), dummy_()
  {}
  ~ObSimpleLinkQueue()
  {}

public:
  int top(Link*& p);
  int pop(Link*& p);
  int push(Link* p);
  inline bool is_empty() const
  {
    return head_ == tail_;
  }

private:
  int do_pop(Link*& p);

private:
  Link* head_ CACHE_ALIGNED;
  Link* tail_ CACHE_ALIGNED;
  Link dummy_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSimpleLinkQueue);
};

}  // end namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_QUEUE_OB_LINK_QUEUE_
