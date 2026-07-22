/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ddl/ob_batch_file_slot_ring.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase {
namespace storage {

// -------------------------------- ObBatchFileSlotRing --------------------------------

ObBatchFileSlotRing::ObBatchFileSlotRing()
    : is_inited_(false),
      lock_(common::ObLatchIds::OB_TASK_SLOT_RING_LOCK),
      capacity_(0),
      slots_(),
      head_idx_(0),
      next_idx_(0)
{
}

ObBatchFileSlotRing::~ObBatchFileSlotRing()
{
  destroy();
}

int ObBatchFileSlotRing::init(const int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("batch file slot ring already initialized", K(ret));
  } else if (capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid capacity", K(ret), K(capacity));
  } else {
    capacity_ = capacity;
    head_idx_ = 0;
    next_idx_ = 0;
    is_inited_ = true;
    LOG_INFO("batch file slot ring initialized", K(capacity_));
  }
  return ret;
}

void ObBatchFileSlotRing::destroy()
{
  ObSpinLockGuard guard(lock_);
  for (int64_t i = 0; i < slots_.count(); ++i) {
    slots_.at(i).reset();
  }
  slots_.reset();
  capacity_ = 0;
  head_idx_ = 0;
  next_idx_ = 0;
  is_inited_ = false;
}

int ObBatchFileSlotRing::reserve_slot(int64_t &slot_idx)
{
  int ret = OB_SUCCESS;
  slot_idx = -1;
  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("batch file slot ring not initialized", K(ret));
  } else {
    Slot new_slot;
    if (OB_FAIL(slots_.push_back(new_slot))) {
      LOG_WARN("failed to grow slots array", K(ret), K_(next_idx));
    } else {
      slot_idx = next_idx_;
      next_idx_++;
      LOG_DEBUG("reserved slot", K(slot_idx), K_(head_idx), K_(next_idx));
    }
  }
  return ret;
}

int ObBatchFileSlotRing::mark_slot_submitted(int64_t slot_idx, const common::ObString &task_id)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("batch file slot ring not initialized", K(ret));
  } else if (!is_valid_slot_idx_(slot_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid slot idx", K(ret), K(slot_idx), K_(head_idx), K_(next_idx));
  } else {
    const int64_t pos = slot_idx;
    Slot &slot = slots_.at(pos);
    if (SLOT_EMPTY != slot.status_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("slot status not empty", K(ret), K(slot_idx), K(slot.status_));
    } else {
      slot.task_id_ = task_id;  // shallow copy, caller manages lifetime
      slot.status_ = SLOT_SUBMITTED;
      LOG_DEBUG("slot marked submitted", K(slot_idx), K(task_id));
    }
  }
  return ret;
}

int ObBatchFileSlotRing::mark_slot_ready(int64_t slot_idx)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("batch file slot ring not initialized", K(ret));
  } else if (!is_valid_slot_idx_(slot_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid slot idx", K(ret), K(slot_idx), K_(head_idx), K_(next_idx));
  } else {
    const int64_t pos = slot_idx;
    Slot &slot = slots_.at(pos);
    if (SLOT_SUBMITTED != slot.status_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("slot status not submitted", K(ret), K(slot_idx), K(slot.status_));
    } else {
      slot.status_ = SLOT_READY;
      LOG_DEBUG("slot marked ready", K(slot_idx));
    }
  }
  return ret;
}

int ObBatchFileSlotRing::mark_slot_directly_ready(int64_t slot_idx)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("batch file slot ring not initialized", K(ret));
  } else if (!is_valid_slot_idx_(slot_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid slot idx", K(ret), K(slot_idx), K_(head_idx), K_(next_idx));
  } else {
    Slot &slot = slots_.at(slot_idx);
    if (SLOT_EMPTY != slot.status_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("slot status not empty for directly-ready", K(ret), K(slot_idx), K(slot.status_));
    } else {
      slot.status_ = SLOT_READY;
      LOG_DEBUG("slot marked directly ready (skip-only)", K(slot_idx));
    }
  }
  return ret;
}

int ObBatchFileSlotRing::mark_slot_failed(int64_t slot_idx, int error_code)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("batch file slot ring not initialized", K(ret));
  } else if (!is_valid_slot_idx_(slot_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid slot idx", K(ret), K(slot_idx), K_(head_idx), K_(next_idx));
  } else {
    const int64_t pos = slot_idx;
    Slot &slot = slots_.at(pos);
    slot.error_code_ = error_code;
    slot.status_ = SLOT_FAILED;
    LOG_WARN("slot marked failed", K(slot_idx), K(error_code));
  }
  return ret;
}

bool ObBatchFileSlotRing::head_is_ready(int &error_code) const
{
  bool bret = false;
  error_code = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (head_idx_ != next_idx_) {
    const int64_t pos = head_idx_;
    const Slot &slot = slots_.at(pos);
    if (SLOT_READY == slot.status_) {
      bret = true;
    } else if (SLOT_FAILED == slot.status_) {
      error_code = slot.error_code_;
    }
  }
  return bret;
}

bool ObBatchFileSlotRing::has_any_failed(int &error_code) const
{
  bool bret = false;
  error_code = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  for (int64_t idx = head_idx_; idx < next_idx_; ++idx) {
    const int64_t pos = idx;
    const Slot &slot = slots_.at(pos);
    if (SLOT_FAILED == slot.status_) {
      error_code = slot.error_code_;
      bret = true;
      break;
    }
  }
  return bret;
}

int ObBatchFileSlotRing::pop_head()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("batch file slot ring not initialized", K(ret));
  } else if (head_idx_ == next_idx_) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("slot ring is empty", K(ret), K_(head_idx), K_(next_idx));
  } else {
    const int64_t pos = head_idx_;
    Slot &slot = slots_.at(pos);
    if (SLOT_READY != slot.status_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("head slot not ready", K(ret), K_(head_idx), K(slot.status_));
    } else {
      slot.reset();
      head_idx_++;
      LOG_DEBUG("popped head slot", K_(head_idx), K_(next_idx));
    }
  }
  return ret;
}

int ObBatchFileSlotRing::get_slot_task_id(int64_t slot_idx, common::ObString &task_id) const
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("batch file slot ring not initialized", K(ret));
  } else if (!is_valid_slot_idx_(slot_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid slot idx", K(ret), K(slot_idx), K_(head_idx), K_(next_idx));
  } else {
    const int64_t pos = slot_idx;
    task_id = slots_.at(pos).task_id_;
  }
  return ret;
}

int ObBatchFileSlotRing::get_slot_status(int64_t slot_idx, SlotStatus &status) const
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("batch file slot ring not initialized", K(ret));
  } else if (!is_valid_slot_idx_(slot_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid slot idx", K(ret), K(slot_idx), K_(head_idx), K_(next_idx));
  } else {
    const int64_t pos = slot_idx;
    status = slots_.at(pos).status_;
  }
  return ret;
}

bool ObBatchFileSlotRing::is_valid_slot_idx_(int64_t slot_idx) const
{
  return slot_idx >= head_idx_ && slot_idx < next_idx_;
}

} // namespace storage
} // namespace oceanbase