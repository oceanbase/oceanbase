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

#ifndef CO_TIMER_QUEUE_H
#define CO_TIMER_QUEUE_H

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_rbtree.h"

namespace oceanbase {
namespace lib {

class CoTimerQueue {
public:
  struct Node {
    Node(int64_t expires);
    int compare(const Node* node) const
    {
      int ret = (node->expires_ > node->expires_) - (node->expires_ < node->expires_);
      if (0 == ret) {
        ret = (((uintptr_t)this) > ((uintptr_t)node)) - (((uintptr_t)this) < ((uintptr_t)node));
      }
      return ret;
    }
    RBNODE(Node, rblink);
    int64_t expires_;
  };

public:
  CoTimerQueue();

  bool add(Node* node);
  int del(Node* node);

  Node* get_next();

private:
  container::ObRbTree<Node, container::ObDummyCompHelper<Node>> rb_tree_;
  Node* next_;
};

OB_INLINE CoTimerQueue::Node::Node(int64_t expires) : expires_(expires)
{}

OB_INLINE CoTimerQueue::CoTimerQueue()
{
  next_ = nullptr;
}

OB_INLINE CoTimerQueue::Node* CoTimerQueue::get_next()
{
  return next_;
}

}  // namespace lib
}  // namespace oceanbase

#endif /* CO_TIMER_QUEUE_H */
