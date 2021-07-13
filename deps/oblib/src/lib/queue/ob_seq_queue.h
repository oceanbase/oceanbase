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

#ifndef __OB_COMMON_OB_SEQ_QUEUE_H__
#define __OB_COMMON_OB_SEQ_QUEUE_H__
#include "lib/allocator/ob_malloc.h"
#include "lib/lock/ob_thread_cond.h"

namespace oceanbase {
namespace common {
// Suppose there are multiple threads adding elements to the queue, but only one thread will fetch elements, and no two
// threads will add elements with the same number to the queue at the same time.
// there is only one thread get object, and there are not two threads adding common-id object.
class ObSeqQueue {
  struct BufHolder {
    BufHolder() : buf_(NULL)
    {}
    ~BufHolder()
    {
      if (NULL != buf_) {
        ob_free(buf_);
        buf_ = NULL;
      }
    }
    const void* get(const int64_t size)
    {
      void* buf = NULL;
      ObMemAttr memattr;
      memattr.label_ = ObModIds::OB_SEQ_QUEUE;
      if (NULL != buf_) {
        _OB_LOG(ERROR, "buf_holder.get(size=%ld): not allowed to get second time", size);
      } else if (NULL == (buf = ob_malloc(size, memattr))) {
        _OB_LOG(ERROR, "ob_malloc(size=%ld)=>NULL", size);
      } else {
        buf_ = buf;
      }
      return buf;
    }
    void* buf_;
  };

  static const int64_t N_COND = 256;
  struct SeqItem {
    volatile int64_t seq_;
    void* volatile data_;
  };

public:
  ObSeqQueue();
  ~ObSeqQueue();

public:
  int init(const int64_t limit, SeqItem* buf = NULL);
  int start(const int64_t seq);
  int add(const int64_t seq, void* data);
  int get(int64_t& seq, void*& data, const int64_t timeout_us);
  int update(const int64_t seq);
  bool next_is_ready() const;
  int64_t get_seq();

protected:
  bool is_inited() const;
  ObThreadCond* get_cond(const int64_t seq);

private:
  BufHolder buf_holder_;
  volatile int64_t seq_;
  SeqItem* items_ CACHE_ALIGNED;
  int64_t limit_;
  ObThreadCond cond_[N_COND] CACHE_ALIGNED;
};
};  // end namespace common
};  // end namespace oceanbase

#endif /* __OB_COMMON_OB_SEQ_QUEUE_H__ */
