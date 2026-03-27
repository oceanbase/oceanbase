/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

class SimpleTCLinkQueue
{
public:
  SimpleTCLinkQueue(): head_(NULL), tail_(NULL), cnt_(0) {}
  ~SimpleTCLinkQueue() {}
public:
  int64_t cnt() { return cnt_; }
  TCLink* top() { return head_; }
  TCLink* pop() {
    TCLink* p = NULL;
    if (NULL != head_) {
      cnt_--;
      p = head_;
      head_ = head_->next_;
      if (NULL == head_) {
        tail_ = NULL;
      }
    }
    return p;
  }
  void push(TCLink* p) {
    cnt_++;
    p->next_ = NULL;
    if (NULL == tail_) {
      head_ = tail_ = p;
    } else {
      tail_->next_ = p;
      tail_ = p;
    }
  }
private:
  TCLink* head_;
  TCLink* tail_;
  int64_t cnt_;
};
