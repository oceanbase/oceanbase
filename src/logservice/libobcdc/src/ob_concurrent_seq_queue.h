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

#ifndef OCEANBASE_COMMON_OB_CONCURRENT_SEQ_QUEUE_H__
#define OCEANBASE_COMMON_OB_CONCURRENT_SEQ_QUEUE_H__

#include "lib/utility/ob_macro_utils.h"       // DISALLOW_COPY_AND_ASSIGN
#include "lib/allocator/ob_malloc.h"          // default_memattr

namespace oceanbase
{
namespace common
{
// A fixed-length, concurrent Sequence Queue, where the sequence number is specified for push and pop.

// Usage scenarios:
// 1. Different seqs are pushed in parallel, and a specific seq can only be pushed by one thread
// 2. Different seqs are popped in parallel, and a specific seq can only be popped by one thread
// 3. different seqs do not guarantee the order of push and pop, there may be 0 <= M < N, pop(N) operation earlier than push(M)
//
// Implementation idea.
//    Assuming that there are a total of H slots, each of which holds an element whose seq number is predetermined.
//    For slot X, it stores the element seq number: X + K * H, K >= 0
//
//    Each slot element is accompanied by a seq variable, which takes the following values, for slot X.
//    1. X + K * H: data is not ready, pending production
//    2. X + K * H + 1: data is ready, pending consumption
//
//    Producer push element No. M: wait for the value of the element's seq variable to change to M, set the data, and then mark seq as M + 1, indicating that the data is ready.
//    Consumer pop element #M: wait for the element's seq variable value to become M + 2, take the data out, then change seq to M + H and wait for the next round of push
//
class ObConcurrentSeqQueue
{
public:
  ObConcurrentSeqQueue();
  ~ObConcurrentSeqQueue();
public:
  int init(const int64_t queue_size, const ObMemAttr &memattr);
  /// @retval OB_SUCCESS  success
  /// @retval OB_TIMEOUT  timeout
  /// @retval other value fail
  int push(void *data, const int64_t seq, const int64_t timeout_us);

  /// @retval OB_SUCCESS  success
  /// @retval OB_TIMEOUT  timeout
  /// @retval other value fail
  int pop(void *&data, const int64_t seq, const int64_t timeout_us);
  void destroy();

  /// valid task number;
  int64_t size() const { return ATOMIC_LOAD(&size_); }
private:
  static const int64_t INT32_MASK = ((1LL << 32) - 1LL);
  struct SeqItem
  {
    volatile int64_t seq_;
    void *volatile data_;
  };
private:
  bool is_inited_() const;
  SeqItem &seq_item_(const int64_t seq);
  int wait_on_item_(SeqItem &item, const int64_t cmp_val, const int64_t end_time);
  int update_seq_(SeqItem &item,
      const int64_t expected_cur_seq,
      const int64_t new_seq);
private:
  SeqItem *items_ CACHE_ALIGNED;
  int64_t limit_;
  // task number
  int64_t size_ CACHE_ALIGNED;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConcurrentSeqQueue);
};
} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBASE_COMMON_OB_CONCURRENT_SEQ_QUEUE_H__ */
