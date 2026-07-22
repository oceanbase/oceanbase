/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_AI_CIRCUIT_SLIDING_WINDOW_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_AI_CIRCUIT_SLIDING_WINDOW_H_

#include "lib/ob_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"

#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX SHARE
#endif

namespace oceanbase
{
namespace share
{

// Time-based sliding window counter for AI Gateway circuit breaker.
//
// Uses a ring buffer of per-second buckets (configurable, max 300, default 60),
// a TIME_BASED sliding window approach.
//
// Each bucket stores (epoch_second, success_count, fail_count). Running totals
// are maintained for O(1) failure rate reads.
//
// Thread safety: this class does NOT perform internal locking.
// The caller must hold a gateway-level spinlock before calling any method.
class ObAiCircuitSlidingWindow
{
public:
  static const int64_t MAX_WINDOW_SIZE = 300;
  static const int64_t DEFAULT_WINDOW_SIZE = 60;

  struct Bucket
  {
    int64_t epoch_second_;
    int64_t success_count_;
    int64_t fail_count_;
    Bucket() : epoch_second_(0), success_count_(0), fail_count_(0) {}
    void reset()
    {
      epoch_second_ = 0;
      success_count_ = 0;
      fail_count_ = 0;
    }
  };

  ObAiCircuitSlidingWindow()
      : buckets_(NULL),
        window_size_(0),
        total_success_(0),
        total_fail_(0),
        last_active_sec_(0),
        is_inited_(false),
        allocator_(NULL)
  {}

  ~ObAiCircuitSlidingWindow() { destroy(); }

  int init(common::ObIAllocator *allocator, int64_t window_size_seconds);

  void destroy()
  {
    if (OB_NOT_NULL(buckets_) && OB_NOT_NULL(allocator_)) {
      allocator_->free(buckets_);
      buckets_ = NULL;
    }
    window_size_ = 0;
    total_success_ = 0;
    total_fail_ = 0;
    last_active_sec_ = 0;
    is_inited_ = false;
    allocator_ = NULL;
  }

  // Must be called under gateway spinlock.
  int record(bool success);

  // Must be called under gateway spinlock.
  int get_failure_rate(int64_t &total, int64_t &failed) const;

  // Must be called under gateway spinlock.
  void reset()
  {
    if (is_inited_ && OB_NOT_NULL(buckets_)) {
      for (int64_t i = 0; i < window_size_; ++i) {
        buckets_[i].reset();
      }
      total_success_ = 0;
      total_fail_ = 0;
      last_active_sec_ = 0;
    }
  }

  int64_t get_total_success() const { return total_success_; }
  int64_t get_total_fail() const { return total_fail_; }
  int64_t get_window_size() const { return window_size_; }

  // Called by the V$ virtual table before reading totals so displayed
  // total_requests/failure_rate reflect only in-window data.
  // Must be called under gateway spinlock.
  void evict_expired_slots();

  // Alloc-first + swap: the old buffer stays usable until the new allocation
  // succeeds, so OOM leaves the window functional. Old counts are cleared.
  // Must be called under gateway spinlock.
  int rebuild(common::ObIAllocator *allocator, int64_t new_window_size_seconds);

  TO_STRING_KV(K_(window_size), K_(total_success), K_(total_fail), K_(last_active_sec), K_(is_inited));

private:
  Bucket *buckets_;
  int64_t window_size_;
  int64_t total_success_;
  int64_t total_fail_;
  int64_t last_active_sec_;
  bool is_inited_;
  common::ObIAllocator *allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObAiCircuitSlidingWindow);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_AI_CIRCUIT_SLIDING_WINDOW_H_
