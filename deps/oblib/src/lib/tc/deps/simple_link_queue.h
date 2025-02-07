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
