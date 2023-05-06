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
 *
 * double linked list
 */

#ifndef OCEANBASE_LOG_FETCHER_DLIST_H__
#define OCEANBASE_LOG_FETCHER_DLIST_H__

#include "lib/utility/ob_print_utils.h"   // TO_STRING_KV

namespace oceanbase
{
namespace logfetcher
{
template <class T>
class ObLogDList;

////////////////////////////////// ObLogDListNode //////////////////////////////////
// Bidirectional linked list
// Requires the T type to be a derived class of ObLogDListNode<T>
template <class T>
class ObLogDListNode
{
  friend class ObLogDList<T>;
public:
  ObLogDListNode() { reset(); }
  virtual ~ObLogDListNode() { reset(); }

public:
  void reset()
  {
    next_ = NULL;
    prev_ = NULL;
  }

  void reset_list_node()
  {
    reset();
  }

  T *get_next() { return next_; }
  T *get_prev() { return prev_; }

protected:
  T *next_;
  T *prev_;
};

////////////////////////////////// ObLogDList //////////////////////////////////
// Bidirectional linked list
// Requires the T type to be a derived class of ObLogDListNode<T>
template <class T>
class ObLogDList
{
public:
  ObLogDList() { reset(); }
  virtual ~ObLogDList() { reset(); }

public:
  void reset()
  {
    count_ = 0;
    head_ = NULL;
  }

  T *head() { return head_; }
  int64_t count() const { return count_; }

  // Add to header node
  void add_head(T &node)
  {
    node.prev_ = NULL;
    node.next_ = head_;

    if (NULL != head_) {
      head_->prev_ = &node;
    }

    head_ = &node;
    count_++;
  }

  // delete node
  void erase(T &node)
  {
    if (head_ == &node) {
      head_ = node.next_;
    }

    if (NULL != node.prev_) {
      node.prev_->next_ = node.next_;
    }

    if (NULL != node.next_) {
      node.next_->prev_ = node.prev_;
    }

    // reset node info
    node.reset_list_node();
    count_--;
  }

  bool operator == (const ObLogDList<T> &other)
  {
    return other.head_ == head_ && other.count_ == count_;
  }

  bool operator != (const ObLogDList<T> &other)
  {
    return ! (*this == other);
  }

  TO_STRING_KV(K_(count), KP_(head));

private:
  T       *head_;
  int64_t count_;
};

}
}

#endif
