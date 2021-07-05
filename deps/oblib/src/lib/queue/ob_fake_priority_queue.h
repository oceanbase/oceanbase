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

#ifndef OCEANBASE_QUEUE_OB_PRIORITY_QUEUE_H_
#define OCEANBASE_QUEUE_OB_PRIORITY_QUEUE_H_
#include "lib/lock/ob_seq_sem.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/queue/ob_futex_queue.h"

namespace oceanbase {
namespace common {

class ObFakePriorityQueue {
public:
  typedef ObLink Link;
  enum { Q_CAPACITY = 1 << 16, PRIO_CNT = 2 };

  ObFakePriorityQueue()
  {
    queue_.init(Q_CAPACITY);
  }
  ~ObFakePriorityQueue()
  {}

  int64_t size() const
  {
    return queue_.size();
  }
  int push(Link* data, int priority)
  {
    UNUSED(priority);
    return queue_.push((void*)data);
  }

  int pop(Link*& data, int64_t timeout_us)
  {
    return queue_.pop((void*&)data, timeout_us);
  }

private:
  ObFutexQueue queue_;
};

};  // end namespace common
};  // end namespace oceanbase

#endif /* OCEANBASE_QUEUE_OB_PRIORITY_QUEUE_H_ */
