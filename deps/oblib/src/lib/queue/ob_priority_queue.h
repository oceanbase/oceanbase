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

#ifndef OCEANBASE_QUEUE_OB_PRIORITY_QUEUE_
#define OCEANBASE_QUEUE_OB_PRIORITY_QUEUE_

#include "lib/lock/ob_seq_sem.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/lock/ob_scond.h"

namespace oceanbase {
namespace common {

template <int PRIOS>
class ObPriorityQueue {
public:
  enum { PRIO_CNT = PRIOS };

  ObPriorityQueue() : sem_(), queue_(), size_(0), limit_(INT64_MAX)
  {}
  ~ObPriorityQueue()
  {}

  void set_limit(int64_t limit)
  {
    limit_ = limit;
  }
  inline int64_t size() const
  {
    return ATOMIC_LOAD(&size_);
  }

  int push(ObLink* data, int priority)
  {
    int ret = OB_SUCCESS;
    if (ATOMIC_FAA(&size_, 1) > limit_) {
      ret = OB_SIZE_OVERFLOW;
    } else if (OB_UNLIKELY(NULL == data) || OB_UNLIKELY(priority < 0) || OB_UNLIKELY(priority >= PRIO_CNT)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "push error, invalid argument", KP(data), K(priority));
    } else if (OB_FAIL(queue_[priority].push(data))) {
      // do nothing
    } else {
      ret = sem_.post();
    }
    if (OB_FAIL(ret)) {
      (void)ATOMIC_FAA(&size_, -1);
    }
    return ret;
  }

  int push_front(ObLink* data, int priority)
  {
    int ret = OB_SUCCESS;
    ATOMIC_FAA(&size_, 1);
    if (OB_UNLIKELY(NULL == data) || OB_UNLIKELY(priority < 0) || OB_UNLIKELY(priority >= PRIO_CNT)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "push front error, invalid argument", KP(data), K(priority));
    } else if (OB_FAIL(queue_[priority].push_front(data))) {
      // do nothing
    } else {
      ret = sem_.post();
    }
    if (OB_FAIL(ret)) {
      (void)ATOMIC_FAA(&size_, -1);
    }
    return ret;
  }

  int pop(ObLink*& data, int64_t timeout_us)
  {
    int ret = OB_ENTRY_NOT_EXIST;
    if (OB_UNLIKELY(timeout_us < 0)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(ERROR, "timeout is invalid", K(ret), K(timeout_us));
    } else {
      if (0 == sem_.wait(timeout_us)) {
        for (int i = 0; OB_ENTRY_NOT_EXIST == ret && i < PRIO_CNT; i++) {
          if (OB_SUCCESS == queue_[i].pop(data)) {
            ret = OB_SUCCESS;
          }
        }
      }
      if (OB_FAIL(ret)) {
        data = NULL;
      } else {
        (void)ATOMIC_FAA(&size_, -1);
      }
    }
    return ret;
  }

private:
  ObSeqSem sem_;
  ObLinkQueue queue_[PRIO_CNT];
  int64_t size_ CACHE_ALIGNED;
  int64_t limit_ CACHE_ALIGNED;
  DISALLOW_COPY_AND_ASSIGN(ObPriorityQueue);
};

template <int HIGH_PRIOS, int LOW_PRIOS>
class ObPriorityQueue2 {
public:
  enum { PRIO_CNT = HIGH_PRIOS + LOW_PRIOS };

  ObPriorityQueue2() : queue_(), size_(0), limit_(INT64_MAX)
  {}
  ~ObPriorityQueue2()
  {}

  void set_limit(int64_t limit)
  {
    limit_ = limit;
  }
  inline int64_t size() const
  {
    return ATOMIC_LOAD(&size_);
  }
  int64_t queue_size(const int i) const
  {
    return queue_[i].size();
  }
  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    common::databuff_printf(buf, buf_len, pos, "total_size=%ld ", size());
    for (int i = 0; i < PRIO_CNT; i++) {
      common::databuff_printf(buf, buf_len, pos, "queue[%d]=%ld ", i, queue_[i].size());
    }
    return pos;
  }

  int push(ObLink* data, int priority)
  {
    int ret = OB_SUCCESS;
    if (ATOMIC_FAA(&size_, 1) > limit_) {
      ret = OB_SIZE_OVERFLOW;
    } else if (OB_UNLIKELY(NULL == data) || OB_UNLIKELY(priority < 0) || OB_UNLIKELY(priority >= PRIO_CNT)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "push error, invalid argument", KP(data), K(priority));
    } else if (OB_FAIL(queue_[priority].push(data))) {
      // do nothing
    } else {
      cond_.signal();
      // if (priority < HIGH_PRIOS) {
      //   high_cond_.signal();
      // }
    }

    if (OB_FAIL(ret)) {
      (void)ATOMIC_FAA(&size_, -1);
    }
    return ret;
  }

  inline int do_pop(ObLink*& data, int64_t plimit, int64_t timeout_us)
  {
    int ret = OB_ENTRY_NOT_EXIST;
    if (OB_UNLIKELY(timeout_us < 0)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(ERROR, "timeout is invalid", K(ret), K(timeout_us));
    } else {
      cond_.prepare();
      for (int i = 0; OB_ENTRY_NOT_EXIST == ret && i < plimit; i++) {
        if (OB_SUCCESS == queue_[i].pop(data)) {
          ret = OB_SUCCESS;
        }
      }
      if (OB_FAIL(ret)) {
        cond_.wait(timeout_us);
        data = NULL;
      } else {
        (void)ATOMIC_FAA(&size_, -1);
      }
    }
    return ret;
  }

  int pop(ObLink*& data, int64_t timeout_us)
  {
    return do_pop(data, PRIO_CNT, timeout_us);
  }

  int pop_high(ObLink*& data, int64_t timeout_us)
  {
    return do_pop(data, HIGH_PRIOS, timeout_us);
  }

private:
  SCond cond_;
  ObLinkQueue queue_[PRIO_CNT];
  int64_t size_ CACHE_ALIGNED;
  int64_t limit_ CACHE_ALIGNED;
  DISALLOW_COPY_AND_ASSIGN(ObPriorityQueue2);
};
}  // end namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_QUEUE_OB_PRIORITY_QUEUE_
