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

#ifndef __OB_UPDATESERVER_SPIN_QUEUE_H__
#define __OB_UPDATESERVER_SPIN_QUEUE_H__
#include "lib/allocator/ob_malloc.h"
namespace oceanbase {
namespace common {
class SpinQueue {
public:
  enum { MIN_QLEN = 4 };

public:
  SpinQueue() : push_(0), pop_(0), pos_mask_(0), items_(NULL)
  {}
  ~SpinQueue()
  {
    destroy();
  }
  int init(uint64_t len)
  {
    int err = 0;
    // int64_t len  = 1 << (_len? (64 - __builtin_clzl(_len)): 0);
    ObMemAttr memattr(OB_SERVER_TENANT_ID, ObModIds::TEST);
    if (len < MIN_QLEN || 0 != ((len - 1) & len)) {
      err = OB_INVALID_ARGUMENT;
    } else if (NULL == (items_ = (void**)ob_malloc(sizeof(void*) * len, memattr))) {
      err = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      pos_mask_ = len - 1;
      memset(items_, 0, sizeof(void*) * len);
    }
    return err;
  }
  void destroy()
  {
    if (!items_) {
      ob_free(items_);
      items_ = NULL;
    }
  }
  int push(void* data)
  {
    int err = 0;
    uint64_t seq = __sync_fetch_and_add(&push_, 1);
    void* volatile* pi = items_ + (seq & pos_mask_);
    while (!__sync_bool_compare_and_swap(pi, NULL, data))
      ;
    return err;
  }
  int pop(void*& data)
  {
    int err = 0;
    uint64_t seq = __sync_fetch_and_add(&pop_, 1);
    void* volatile* pi = items_ + (seq & pos_mask_);
    while (NULL == (data = *pi) || !__sync_bool_compare_and_swap(pi, data, NULL))
      ;
    return err;
  }
  bool inited() const
  {
    return NULL != items_;
  }
  int64_t get_total() const
  {
    return push_ - pop_;
  }
  int64_t get_free() const
  {
    return pop_ + pos_mask_ + 1 - push_;
  }

private:
  volatile uint64_t push_ CACHE_ALIGNED;
  volatile uint64_t pop_ CACHE_ALIGNED;
  uint64_t pos_mask_ CACHE_ALIGNED;
  void** items_;
};

};  // namespace common
};  // end namespace oceanbase

#endif /* __OB_UPDATESERVER_SPIN_QUEUE_H__ */
