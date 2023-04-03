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

#ifndef _OB_LIB_QUEUE_OB_SEQ_QUEUE_H_
#define _OB_LIB_QUEUE_OB_SEQ_QUEUE_H_

#include "lib/ob_define.h"
#include "lib/allocator/ob_allocator.h"   // ObIAllocator
#include "lib/atomic/ob_atomic.h"         // PAUSE
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{
class ObCoSeqQueue
{
  enum { MIN_QUEUE_SIZE = 4, READY = 1 };
public:
  ObCoSeqQueue(): next_(0), len_(0), items_(NULL), allocator_(NULL) {}
  ~ObCoSeqQueue(){ destroy(); }
public:
  int init(const int64_t limit, ObIAllocator *allocator)
  {
    int ret = common::OB_SUCCESS;
    if (limit <= MIN_QUEUE_SIZE || NULL == allocator) {
      ret = common::OB_INVALID_ARGUMENT;
      LIB_LOG(ERROR, "invalid args", K(limit), K(allocator));
    } else if (NULL == (items_ = static_cast<int64_t*>(allocator->alloc(sizeof(int64_t) * limit)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(ERROR, "alloc memory for items failed", "size", sizeof(int64_t) * limit);
    }

    for (int64_t i = 0; common::OB_SUCCESS == ret && i < limit; i++) {
      items_[i] = i;
    }

    if (OB_SUCCESS != ret) {
      destroy();
    } else {
      len_ = limit;
      allocator_ = allocator;
    }

    return ret;
  }

  int destroy()
  {
    if (NULL != items_ && NULL != allocator_) {
      allocator_->free((void*)items_);
      items_ = NULL;
    }

    len_ = 0;
    next_ = 0;
    allocator_ = NULL;
    return common::OB_SUCCESS;
  }

  int64_t get_next() const { return next_; }
  bool is_ready(const int64_t id) const
  {
    bool bret = false;
    if (NULL == items_) {
      LIB_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid item", K(items_));
    } else if (id < 0) {
      LIB_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid id", K(id));
    } else if (0 == len_) {
      LIB_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid len", K(len_));
    } else {
      bret = items_[id % len_] >= id + READY;
    }
    return bret;
  }

  int wait(const int64_t id, int64_t &target_id) const
  {
    int ret = common::OB_SUCCESS;
    if (NULL == items_) {
      ret = common::OB_NOT_INIT;
    } else if (id < 0) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      while (items_[id % len_] < id)
      {
        PAUSE();
      }

      target_id = items_[id % len_];
    }
    return ret;
  }

  int64_t add(const int64_t id)
  {
    int64_t last_got = 0;
    if (NULL != items_) {
      while (!__sync_bool_compare_and_swap(items_ + (id % len_), id, id + READY)) {
        PAUSE();
      }

      while(true) {
        int64_t next = next_;
        if (__sync_bool_compare_and_swap(items_ + (next % len_), next + READY, next + len_)) {
          last_got = next + 1;
          next_++;
        } else if (items_[next % len_] < next + READY) {
          break;
        }
      }
    }
    return last_got >= next_? last_got: 0;
  }
private:
  volatile int64_t next_ CACHE_ALIGNED;
  int64_t len_ CACHE_ALIGNED;
  volatile int64_t *items_;
  common::ObIAllocator *allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObCoSeqQueue);
};
}; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_LIB_QUEUE_OB_SEQ_QUEUE_H__ */
