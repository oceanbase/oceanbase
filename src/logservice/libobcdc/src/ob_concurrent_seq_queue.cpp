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

#define USING_LOG_PREFIX LIB

#include "ob_concurrent_seq_queue.h"

#include <linux/futex.h>                  // futex

#include "lib/ob_define.h"
#include "lib/time/ob_time_utility.h"     // ObTimeUtility
#include "share/ob_errno.h"               // KR

namespace oceanbase
{
namespace common
{

static struct timespec make_timespec(int64_t us)
{
  timespec ts;
  ts.tv_sec = us / 1000000;
  ts.tv_nsec = 1000 * (us % 1000000);
  return ts;
}

inline int futex_wake(volatile int *p, int val)
{
  return static_cast<int>(futex((uint *)p, FUTEX_WAKE_PRIVATE, val, NULL));
}
// 0: Woken up by FUTEX_WAKE
// ETIMEDOUT: Timeout
// EWOULDBLOCK: Target value changed, not equal to the incoming comparison value
// EINTR: woken up by various signals

/////////////////////////////////////////////////////////////////////////////
ObConcurrentSeqQueue::ObConcurrentSeqQueue(): items_(NULL), limit_(0), size_(0)
{}

ObConcurrentSeqQueue::~ObConcurrentSeqQueue()
{
  destroy();
}

int ObConcurrentSeqQueue::init(const int64_t limit, const ObMemAttr &memattr)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = sizeof(SeqItem) * limit;
  if (OB_UNLIKELY(0 >= limit)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(limit));
  } else if (OB_UNLIKELY(limit_ > 0 || NULL != items_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(items_ = (SeqItem *)ob_malloc(alloc_size, memattr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory failed", K(alloc_size), KR(ret), K(items_));
  } else {
    limit_ = limit;
    size_ = 0;
    memset(items_, 0, sizeof(SeqItem) * limit);
    for (int64_t i = 0; i < limit; i++) {
      // default to NOT READY state
      items_[i].seq_ = i;
    }
  }
  return ret;
}

void ObConcurrentSeqQueue::destroy()
{
  if (NULL != items_) {
    ob_free(items_);
    items_ = NULL;
  }
  limit_ = 0;
  size_ = 0;
}

inline bool ObConcurrentSeqQueue::is_inited_() const
{
  return NULL != items_ && limit_ > 0;
}

inline ObConcurrentSeqQueue::SeqItem &ObConcurrentSeqQueue::seq_item_(const int64_t seq)
{
  return items_[seq % limit_];
}

int ObConcurrentSeqQueue::wait_on_item_(SeqItem &item,
    const int64_t cmp_val,
    const int64_t end_time)
{
  int ret = OB_SUCCESS;
  // Sleep time for one operation
  // We set it relatively short to avoid the cost of "false sleep" caused by int32_t overflow
  static const int64_t WAIT_TIME_ON_OP = 10L * 1000L;

  int64_t wait_time_us = end_time - ObTimeUtility::current_time();
  wait_time_us = std::min(wait_time_us, WAIT_TIME_ON_OP);
  if (wait_time_us <= 0) {
    ret = OB_TIMEOUT;
  } else {
    volatile int *p = reinterpret_cast<volatile int *>(&item.seq_);
    int cmp_val_int32 = static_cast<int>(cmp_val & INT32_MASK);
    timespec ts = make_timespec(wait_time_us);

    /// Note: Our data is int64_t, but futex only supports int32_t.
    /// This is a direct comparison of the lower 32 bits of int64_t, since our data is incremented
    /// Only after the int32_t value overflows will the value be misclassified, leading to a false sleep.
    /// We consider the probability of this occurring to be extremely low, and even if it does,
    /// we reduce the impact by making the sleep wait time relatively short.
    int futex_err = futex_wait(p, cmp_val_int32, &ts);
    if (futex_err == ETIMEDOUT) {
      ret = OB_TIMEOUT;
    } else {
      // 成功
    }
  }
  return ret;
}

int ObConcurrentSeqQueue::update_seq_(SeqItem &item,
    const int64_t expected_cur_seq,
    const int64_t new_seq)
{
  int ret = OB_SUCCESS;
  // If the setup fails, it means there is a concurrent scenario and exit with an error
  int64_t cur_seq = ATOMIC_CAS(&item.seq_, expected_cur_seq, new_seq);
  if (OB_UNLIKELY(cur_seq != expected_cur_seq)) {
    LOG_ERROR("update seq value fail, must have other threads updating the same item",
        K(cur_seq), K(expected_cur_seq), K(new_seq), K(item.seq_), K(item.data_));
    ret = OB_STATE_NOT_MATCH;
  } else {
    volatile int *p = reinterpret_cast<volatile int *>(&item.seq_);
    // wake up all threads waiting on that element
    // Note: The reason for waking up all threads here is to avoid waking up threads that are not the target threads
    // For example: there may be multiple consuming/producing threads waiting for different seq values on this slot.
    // In order to successfully wake up the target thread, we can only broadcast, because we don't have the ability to wake up a specific thread via futex
    int64_t wake_num = futex_wake(p, INT32_MAX);
    LOG_DEBUG("wake_up after update seq", "old_val", expected_cur_seq, "new_val", new_seq,
        K(wake_num));
  }
  return ret;
}

int ObConcurrentSeqQueue::push(void *data, const int64_t seq, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_())) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObConcurrentSeqQueue not init", K(items_), K(limit_));
  } else if (OB_UNLIKELY(seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(seq));
  } else {
    bool ready_to_push = false;
    SeqItem &item = seq_item_(seq);
    int64_t end_time = ObTimeUtility::current_time() + timeout_us;

    while (! ready_to_push && OB_SUCCESS == ret) {
      // First save the current seq value
      int64_t item_seq = ATOMIC_LOAD(&item.seq_);

      // should not be greater than target seq
      if (OB_UNLIKELY(item_seq > seq)) {
        ret = OB_ENTRY_EXIST;
        LOG_ERROR("invalid sequence: ENTRY_EXIST", K(seq), K(item.seq_));
      } else {
        ready_to_push = (item_seq == seq);
      }

      // Wait if the data is not ready to be pushed
      if (OB_SUCCESS == ret && ! ready_to_push) {
        ret = wait_on_item_(item, item_seq, end_time);
      }
    }

    if (OB_SUCCESS == ret) {
      // Set the data once the element is ready
      item.data_ = data;
      __sync_synchronize();

      // seq -> seq + 1
      if (OB_FAIL(update_seq_(item, seq, seq + 1))) {
        LOG_ERROR("update seq fail after push data", KR(ret), K(seq));
      } else {
        ATOMIC_INC(&size_);
      }
    }
  }
  return ret;
}

int ObConcurrentSeqQueue::pop(void *&data, const int64_t asked_seq, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_())) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(asked_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(ERROR, "invalid sequence number", K(asked_seq));
  } else {
    bool ready_to_pop = false;
    SeqItem &item = seq_item_(asked_seq);
    int64_t end_time = ObTimeUtility::current_time() + timeout_us;
    // The value becomes seq + 1, indicating that the data is ready
    int64_t ready_seq = asked_seq + 1;

    // Loop to wait for seq to become ready
    while (! ready_to_pop && OB_SUCCESS == ret) {
      // First save the current seq value
      int64_t item_seq = ATOMIC_LOAD(&item.seq_);

      if (OB_UNLIKELY(item_seq > ready_seq)) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_ERROR("invalid sequence: ENTRY_NOT_EXIST", K(asked_seq), K(ready_seq), K(item_seq));
      } else {
        ready_to_pop = (item_seq == ready_seq);
      }

      // waif if data is not ready
      if (! ready_to_pop && OB_SUCCESS == ret) {
        ret = wait_on_item_(item, item_seq, end_time);
      }
    }

    // Take out the data, update the seq and prepare the next round of slots
    if (OB_SUCCESS == ret) {
      data = item.data_;
      __sync_synchronize();

      // update value of seq FROM asked_seq + 1 TO asked_seq + limit_
      if (OB_FAIL(update_seq_(item, asked_seq + 1, asked_seq + limit_))) {
        LOG_ERROR("update seq fail after pop data", K(asked_seq));
      } else {
        ATOMIC_DEC(&size_);
      }
    }
  }

  return ret;
}

}; // end namespace common
}; // end namespace oceanbase
