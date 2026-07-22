/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_DDL_OB_BATCH_FILE_SLOT_RING_H_
#define OCEANBASE_STORAGE_DDL_OB_BATCH_FILE_SLOT_RING_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_latch.h"

namespace oceanbase {
namespace storage {

// Slot ring for tracking BatchFile task status
// Only manages slot status, not result data
class ObBatchFileSlotRing {
public:
  enum SlotStatus {
    SLOT_EMPTY = 0,
    SLOT_SUBMITTED = 1,
    SLOT_READY = 2,
    SLOT_FAILED = 3
  };

  struct Slot {
    SlotStatus status_;
    common::ObString task_id_;    // AI task ID for this sub-task
    int error_code_;              // Error code (set when FAILED)

    Slot() : status_(SLOT_EMPTY), error_code_(OB_SUCCESS) {}
    void reset() {
      status_ = SLOT_EMPTY;
      task_id_.reset();
      error_code_ = OB_SUCCESS;
    }
    TO_STRING_KV(K_(status), K_(task_id), K_(error_code));
  };

  ObBatchFileSlotRing();
  ~ObBatchFileSlotRing();

  int init(const int64_t capacity);
  void destroy();

  // Reserve next slot, returns slot_idx. Grows the slot array dynamically as needed.
  int reserve_slot(int64_t &slot_idx);

  // Mark slot as submitted with task_id
  int mark_slot_submitted(int64_t slot_idx, const common::ObString &task_id);

  // Mark slot as ready (task finished, results can be fetched). Requires SUBMITTED state.
  int mark_slot_ready(int64_t slot_idx);

  // Mark slot as ready directly from EMPTY state (for skip-only batches with no API call).
  int mark_slot_directly_ready(int64_t slot_idx);

  // Mark slot as failed
  int mark_slot_failed(int64_t slot_idx, int error_code);

  // Non-blocking check if head is ready. Sets error_code if head is FAILED.
  bool head_is_ready(int &error_code) const;

  // Check if any active slot has failed. Sets error_code to the first failed slot's error.
  bool has_any_failed(int &error_code) const;

  // Pop head slot. Only works if head is READY.
  int pop_head();

  bool is_empty() const { return head_idx_ == next_idx_; }
  int64_t get_head_idx() const { return head_idx_; }
  int64_t get_next_idx() const { return next_idx_; }
  int64_t get_pending_count() const { return next_idx_ - head_idx_; }

  // Get task_id for a specific slot (for polling)
  int get_slot_task_id(int64_t slot_idx, common::ObString &task_id) const;
  int get_slot_status(int64_t slot_idx, SlotStatus &status) const;


  TO_STRING_KV(K_(capacity), K_(head_idx), K_(next_idx), K_(is_inited));

private:
  bool is_valid_slot_idx_(int64_t slot_idx) const;

private:
  bool is_inited_;
  mutable common::ObSpinLock lock_;
  int64_t capacity_;
  common::ObArray<Slot> slots_;
  int64_t head_idx_;
  int64_t next_idx_;

  DISALLOW_COPY_AND_ASSIGN(ObBatchFileSlotRing);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_DDL_OB_BATCH_FILE_SLOT_RING_H_