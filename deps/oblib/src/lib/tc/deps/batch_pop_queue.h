/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
class BatchPopQueue
{
public:
  BatchPopQueue(): top_(NULL) {}
  ~BatchPopQueue() {}
  void push(TCLink* p) {
    TCLink *nv = NULL;
    p->next_ = ATOMIC_LOAD(&top_);
    while(p->next_ != (nv = ATOMIC_VCAS(&top_, p->next_, p))) {
      p->next_ = nv;
    }
  }
  TCLink* pop() {
    TCLink* h = ATOMIC_TAS(&top_, NULL);
    return link_reverse(h);
  }
private:
  static TCLink* link_reverse(TCLink* h) {
    TCLink* nh = NULL;
    while(h) {
      TCLink* next = h->next_;
      h->next_ = nh;
      nh = h;
      h = next;
    }
    return nh;
  }
private:
  TCLink* top_ CACHE_ALIGNED;
};
