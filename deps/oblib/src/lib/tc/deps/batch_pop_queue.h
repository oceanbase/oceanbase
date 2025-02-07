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
