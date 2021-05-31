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

#include "co_timer_queue.h"

using namespace oceanbase::lib;

bool CoTimerQueue::add(Node* node)
{
  int bret = false;

  /* Make sure we don't add nodes that are already added */

  rb_tree_.insert(node);
  if (!next_ || node->compare(next_) < 0) {
    next_ = node;
    bret = true;
  }
  return bret;
}

int CoTimerQueue::del(Node* node)
{
  // update next pointer
  if (next_ == node) {
    rb_tree_.get_next(node, next_);
  }
  rb_tree_.remove(node);
  return next_ != nullptr;
}
