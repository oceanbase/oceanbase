/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "share/ai_service/ob_ai_circuit_sliding_window.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{

int ObAiCircuitSlidingWindow::get_failure_rate(int64_t &total, int64_t &failed) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sliding window not initialized", K(ret));
  } else {
    total = total_success_ + total_fail_;
    failed = total_fail_;
  }
  return ret;
}

int ObAiCircuitSlidingWindow::init(common::ObIAllocator *allocator, int64_t window_size_seconds)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sliding window already initialized", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret));
  } else if (window_size_seconds <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid window size", K(ret), K(window_size_seconds));
  } else {
    allocator_ = allocator;
    window_size_ = MIN(window_size_seconds, MAX_WINDOW_SIZE);
    void *buf = allocator_->alloc(sizeof(Bucket) * window_size_);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate buckets", K(ret), K(window_size_));
    } else {
      buckets_ = new (buf) Bucket[window_size_];
      total_success_ = 0;
      total_fail_ = 0;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObAiCircuitSlidingWindow::record(bool success)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sliding window not initialized", K(ret));
  } else {
    const int64_t now_sec = common::ObTimeUtility::current_time() / 1000000;

    if (last_active_sec_ > 0 && now_sec > last_active_sec_) {
      const int64_t gap = now_sec - last_active_sec_;
      if (gap >= window_size_) {
        for (int64_t i = 0; i < window_size_; i++) {
          buckets_[i].reset();
        }
        total_success_ = 0;
        total_fail_ = 0;
      } else {
        for (int64_t sec = last_active_sec_ + 1; sec <= now_sec; sec++) {
          const int64_t idx = sec % window_size_;
          Bucket &b = buckets_[idx];
          if (b.epoch_second_ != 0 && b.epoch_second_ != sec) {
            total_success_ -= b.success_count_;
            total_fail_ -= b.fail_count_;
            if (OB_UNLIKELY(total_success_ < 0)) { total_success_ = 0; }
            if (OB_UNLIKELY(total_fail_ < 0)) { total_fail_ = 0; }
            b.reset();
          }
        }
      }
    }

    const int64_t slot_idx = now_sec % window_size_;
    Bucket &bucket = buckets_[slot_idx];
    if (bucket.epoch_second_ != now_sec) {
      total_success_ -= bucket.success_count_;
      total_fail_ -= bucket.fail_count_;
      if (OB_UNLIKELY(total_success_ < 0)) { total_success_ = 0; }
      if (OB_UNLIKELY(total_fail_ < 0)) { total_fail_ = 0; }
      bucket.reset();
      bucket.epoch_second_ = now_sec;
    }

    if (success) {
      bucket.success_count_++;
      total_success_++;
    } else {
      bucket.fail_count_++;
      total_fail_++;
    }

    last_active_sec_ = now_sec;
  }
  return ret;
}

void ObAiCircuitSlidingWindow::evict_expired_slots()
{
  if (OB_LIKELY(is_inited_) && OB_NOT_NULL(buckets_)) {
    const int64_t now_sec = common::ObTimeUtility::current_time() / 1000000;
    for (int64_t i = 0; i < window_size_; ++i) {
      Bucket &bucket = buckets_[i];
      if (bucket.epoch_second_ != 0
          && (now_sec - bucket.epoch_second_) >= window_size_) {
        total_success_ -= bucket.success_count_;
        total_fail_ -= bucket.fail_count_;
        bucket.reset();
      }
    }
    if (OB_UNLIKELY(total_success_ < 0)) { total_success_ = 0; }
    if (OB_UNLIKELY(total_fail_ < 0)) { total_fail_ = 0; }
  }
}

int ObAiCircuitSlidingWindow::rebuild(common::ObIAllocator *allocator, int64_t new_window_size_seconds)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sliding window not initialized", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret));
  } else if (new_window_size_seconds <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid new window size", K(ret), K(new_window_size_seconds));
  } else {
    const int64_t new_window_size = MIN(new_window_size_seconds, MAX_WINDOW_SIZE);
    void *buf = allocator->alloc(sizeof(Bucket) * new_window_size);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate new buckets for rebuild",
               K(ret), K(new_window_size), K_(window_size));
    } else {
      Bucket *new_buckets = new (buf) Bucket[new_window_size];
      allocator_->free(buckets_);
      buckets_ = new_buckets;
      window_size_ = new_window_size;
      total_success_ = 0;
      total_fail_ = 0;
      last_active_sec_ = 0;
      allocator_ = allocator;
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
