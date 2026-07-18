/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/high_availability/ob_migration_sliding_window_controller.h"
#include "storage/high_availability/ob_migration_vector_index_processor.h"

#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/stat/ob_latch_define.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace storage
{

// ======================== ObMigrationSlidingWindowController ========================

int64_t ObMigrationSlidingWindowController::dec_ref_()
{
  const int64_t new_ref = ATOMIC_SAF(&ref_count_, 1);
  if (new_ref < 0) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "controller ref count underflow",
                  K(new_ref), K(role_type_), K_(controller_id), KP(this));
  }
  return new_ref;
}

ObMigrationSlidingWindowController::Slot::Slot()
  : state_(State::RESERVED),
    seq_(common::OB_INVALID_INDEX_INT64),
    data_len_(0),
    mgr_slot_()
{
}

void ObMigrationSlidingWindowController::Slot::reset()
{
  state_ = State::RESERVED;
  seq_ = common::OB_INVALID_INDEX_INT64;
  data_len_ = 0;
  mgr_slot_.reset();
}

ObMigrationSlidingWindowController::ObMigrationSlidingWindowController(const ObMigrationControllerInfo::Role role_type)
  : role_type_(role_type),
    controller_id_(common::OB_INVALID_INDEX_INT64),
    is_inited_(false),
    stopped_(false),
    window_lock_(common::ObLatchIds::OB_STORAGE_HA_STRUCT_LOCK),
    window_size_(0),
    head_seq_(0),
    total_task_count_(INT64_MAX),
    slot_map_(),
    window_mgr_(nullptr),
    on_window_slid_(nullptr),
    last_slide_head_log_us_(0),
    ref_count_(0)
{
}

ObMigrationSlidingWindowController::~ObMigrationSlidingWindowController()
{
  destroy();
}

int ObMigrationSlidingWindowController::get_runtime_snapshot(
    int64_t &head_seq, int64_t &window_size)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] controller not inited", KR(ret), K(role_type_),
            K_(controller_id), KP(this));
  } else {
    common::ObSpinLockGuard w_guard(window_lock_);
    head_seq = head_seq_;
    window_size = window_size_;
  }
  return ret;
}

int ObMigrationSlidingWindowController::get_controller_id(int64_t &controller_id) const
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] controller not inited", KR(ret), K(role_type_), K_(controller_id), KP(this));
  } else {
    controller_id = controller_id_;
  }
  return ret;
}

int ObMigrationSlidingWindowController::get_slot_buf_size(int64_t &slot_buf_size)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] controller not inited", KR(ret), K(role_type_), K_(controller_id), KP(this));
  } else {
    ObMigrationTenantWindowMgr *mgr = ATOMIC_LOAD(&window_mgr_);
    if (OB_ISNULL(mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[MIG VEC] window mgr null while inited", KR(ret), K(role_type_), K_(controller_id), KP(this));
    } else if (OB_FAIL(mgr->get_slot_buf_size(slot_buf_size))) {
      LOG_WARN("[MIG VEC] get slot buf size from mgr failed", KR(ret),
              K(role_type_), K_(controller_id), KP(this));
    }
  }
  return ret;
}

int ObMigrationSlidingWindowController::get_role_type(ObMigrationControllerInfo::Role &role_type) const
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] controller not inited", KR(ret), K(role_type_), K_(controller_id), KP(this));
  } else {
    role_type = role_type_;
  }
  return ret;
}

int ObMigrationSlidingWindowController::init(
    ObMigrationTenantWindowMgr *window_mgr,
    ObIMigrationWindowSlidCallback *on_window_slid,
    const share::ObDagPrio::ObDagPrioEnum dag_prio)
{
  int ret = OB_SUCCESS;
  ObArray<ObMigrationTenantWindowSlot> granted_slots;
  int64_t granted_count = 0;

  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[MIG VEC] init twice", KR(ret), K(role_type_), K_(controller_id), KP(this));
  } else if (OB_UNLIKELY(OB_ISNULL(window_mgr))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[MIG VEC] invalid args", KR(ret), K(role_type_),
                          KP(window_mgr), K(dag_prio), KP(this));
  } else if (OB_FAIL(window_mgr->register_controller(role_type_, dag_prio, controller_id_))) {
    LOG_WARN("[MIG VEC] register controller failed", KR(ret), K(role_type_), K(dag_prio));
  } else {
    ATOMIC_STORE(&window_mgr_, window_mgr);
    uint64_t mgr_tenant_id = OB_INVALID_TENANT_ID;
    if (OB_FAIL(window_mgr->get_tenant_id(mgr_tenant_id))) {
      LOG_WARN("[MIG VEC] get mgr tenant id failed", KR(ret), K(role_type_), K_(controller_id), KP(this));
    } else if (OB_FAIL(slot_map_.create(SLOT_MAP_INIT_BUCKET_COUNT,
                  ObMemAttr(mgr_tenant_id, "MigVecIdxSlots"),
                  ObMemAttr(mgr_tenant_id, "MigVecIdxSlots")))) {
      LOG_WARN("[MIG VEC] create slot map failed", KR(ret), K(role_type_), K_(controller_id), KP(this));
    }
  }

  // apply initial slots with retry loop
  if (OB_SUCC(ret)) {
    const int64_t apply_deadline_us =
        ObTimeUtility::current_time() + DEFAULT_OP_TIMEOUT_US;
    bool got_slots = false;
    while (OB_SUCC(ret) && !got_slots) {
      const int64_t remain = apply_deadline_us - ObTimeUtility::current_time();
      if (remain <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("[MIG VEC] apply initial slots timeout", KR(ret), K(role_type_), K_(controller_id),
                KP(this));
      } else {
        granted_slots.reuse();
        granted_count = 0;
        ret = window_mgr->apply_slots(PER_APPLY_SLOT_COUNT, controller_id_, granted_slots, granted_count);
        if (OB_SUCC(ret) && granted_count > 0) {
          got_slots = true;
        } else if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
          const int64_t sleep_us = MIN(remain, SLOT_ACQUIRE_RETRY_INTERVAL_US);
          ob_usleep(static_cast<uint64_t>(sleep_us));
        } else if (OB_FAIL(ret)) {
          LOG_WARN("[MIG VEC] apply initial slots failed", KR(ret), K(role_type_), K_(controller_id), KP(this));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("apply slots contract violated: success with zero granted",
                    KR(ret), K(granted_count), K(role_type_), K_(controller_id), KP(this));
        }
      }
    }
  }

  // Insert granted slots into the per-sequence slot map.
  Slot init_slot;
  for (int64_t i = 0; OB_SUCC(ret) && i < granted_count; ++i) {
    init_slot.reset();
    init_slot.state_ = Slot::State::RESERVED;
    init_slot.seq_ = i;
    init_slot.data_len_ = 0;
    init_slot.mgr_slot_ = granted_slots.at(i);
    if (OB_FAIL(slot_map_.set_refactored(i, init_slot))) {
      LOG_WARN("[MIG VEC] insert slot into slot map failed", KR(ret), K(role_type_),
              K_(controller_id), KP(this), K(i));
    }
  }

  if (OB_SUCC(ret)) {
    window_size_ = granted_count;
    head_seq_ = 0;
    total_task_count_ = INT64_MAX;
    on_window_slid_ = on_window_slid;
    ATOMIC_STORE(&last_slide_head_log_us_, 0);
    ATOMIC_STORE(&stopped_, false);
    ATOMIC_STORE(&is_inited_, true);
    LOG_INFO("sliding window controller init", K(role_type_), K_(controller_id), KP(this), K(granted_count));
  } else if (OB_INIT_TWICE != ret) {
    // rollback (skip when init-twice — nothing new was done, nothing to undo)
    int tmp_ret = OB_SUCCESS;
    if (slot_map_.created()) {
      common::ObSpinLockGuard w_guard(window_lock_);
      slot_map_.clear();
    }
    if (granted_slots.count() > 0 && OB_NOT_NULL(window_mgr)
        && OB_TMP_FAIL(window_mgr->free_slots(granted_slots, controller_id_))) {
      LOG_WARN_RET(tmp_ret, "free granted slots during init rollback failed",
                   K(role_type_), K_(controller_id));
    }
  }

  return ret;
}

void ObMigrationSlidingWindowController::stop()
{
  if (is_inited()) {
    ATOMIC_STORE(&stopped_, true);
    ObMigrationTenantWindowMgr *mgr = ATOMIC_LOAD(&window_mgr_);
    if (OB_NOT_NULL(mgr)) {
      mgr->broadcast_slot_release();
    }
  }
}

void ObMigrationSlidingWindowController::wakeup_waiters()
{
  ObMigrationTenantWindowMgr *mgr = ATOMIC_LOAD(&window_mgr_);
  if (OB_NOT_NULL(mgr)) {
    mgr->broadcast_slot_release();
  }
}

void ObMigrationSlidingWindowController::destroy()
{
  int tmp_ret = OB_SUCCESS;
  ATOMIC_STORE(&stopped_, true);
  ObMigrationTenantWindowMgr *const mgr = ATOMIC_LOAD(&window_mgr_);
  if (OB_NOT_NULL(mgr) && slot_map_.created()) {
    ObArray<ObMigrationTenantWindowSlot> mgr_slots;
    {
      common::ObSpinLockGuard w_guard(window_lock_);
      for (SlotMap::iterator it = slot_map_.begin(); it != slot_map_.end(); ++it) {
        if (it->second.mgr_slot_.is_valid()
            && OB_TMP_FAIL(mgr_slots.push_back(it->second.mgr_slot_))) {
          LOG_WARN_RET(tmp_ret, "push back mgr slot failed", K_(controller_id));
        }
      }
    }
    if (mgr_slots.count() > 0
        && OB_TMP_FAIL(mgr->free_slots(mgr_slots, controller_id_))) {
      LOG_WARN_RET(tmp_ret, "free slots failed", K_(controller_id));
    }
  }

  if (slot_map_.created()) {
    common::ObSpinLockGuard w_guard(window_lock_);
    if (OB_TMP_FAIL(slot_map_.destroy())) {
      LOG_WARN_RET(tmp_ret, "slot map destroy failed", K_(controller_id));
    }
  }

  if (OB_NOT_NULL(mgr)
      && controller_id_ != common::OB_INVALID_INDEX_INT64
      && OB_TMP_FAIL(mgr->unregister_controller(controller_id_))) {
    LOG_WARN_RET(tmp_ret, "unregister failed during destroy", K_(controller_id));
  }
  controller_id_ = common::OB_INVALID_INDEX_INT64;
  window_mgr_ = nullptr;
  ATOMIC_STORE(&is_inited_, false);
}

int ObMigrationSlidingWindowController::try_grow_window_(
    int64_t &out_used_count, int64_t *out_granted_start_seq)
{
  int ret = OB_SUCCESS;
  int64_t granted_count = 0;
  int64_t used_count = 0;
  out_used_count = 0;

  if (ATOMIC_LOAD(&stopped_) || !is_inited()) {
    // Stopped or not initialized: nothing to do.
  } else {
    ObMigrationTenantWindowMgr *const mgr = ATOMIC_LOAD(&window_mgr_);
    if (OB_ISNULL(mgr)) {
    } else {
      ObArray<ObMigrationTenantWindowSlot> granted;
      ret = mgr->apply_slots(PER_APPLY_SLOT_COUNT, controller_id_, granted, granted_count);
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        granted_count = 0;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("[MIG VEC] apply slots failed while try grow window", KR(ret), K(role_type_),
                K_(controller_id), KP(this));
      } else if (granted_count <= 0) {
      } else if (OB_FAIL(try_grow_commit_granted_slots_(
                     mgr, granted, granted_count, out_granted_start_seq, used_count))) {
        LOG_WARN("[MIG VEC] try grow commit granted slots failed", KR(ret), K(role_type_),
                K_(controller_id), KP(this), K(granted_count));
      }
    }
  }
  out_used_count = used_count;
  return ret;
}

int ObMigrationSlidingWindowController::try_grow_commit_granted_slots_(
    ObMigrationTenantWindowMgr *mgr,
    const common::ObIArray<ObMigrationTenantWindowSlot> &granted,
    const int64_t granted_count,
    int64_t *out_granted_start_seq,
    int64_t &used_count)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  used_count = 0;
  {
    common::ObSpinLockGuard w_guard(window_lock_);
    const int64_t can_use_by_task =
        (total_task_count_ > head_seq_ + window_size_) ? total_task_count_ - (head_seq_ + window_size_) : 0;
    int64_t can_use = MIN(can_use_by_task, granted_count);
    if (can_use < 0) {
      can_use = 0;
    }
    if (OB_NOT_NULL(out_granted_start_seq)) {
      *out_granted_start_seq = head_seq_ + window_size_;
    }
    int64_t actually_inserted = 0;
    Slot grow_slot;
    for (int64_t i = 0; OB_SUCC(ret) && i < can_use; ++i) {
      const int64_t new_seq = head_seq_ + window_size_ + i;
      grow_slot.reset();
      grow_slot.state_ = Slot::State::RESERVED;
      grow_slot.seq_ = new_seq;
      grow_slot.data_len_ = 0;
      grow_slot.mgr_slot_ = granted.at(i);
      if (OB_FAIL(slot_map_.set_refactored(new_seq, grow_slot))) {
        LOG_WARN("[MIG VEC] insert slot into slot map failed in grow", KR(ret), K(role_type_),
                K_(controller_id), KP(this), K(new_seq));
      } else {
        ++actually_inserted;
      }
    }
    if (OB_SUCC(ret)) {
      window_size_ += can_use;
      used_count = can_use;
    } else {
      for (int64_t i = 0; i < actually_inserted; ++i) {
        const int64_t rollback_seq = head_seq_ + window_size_ + i;
        if (OB_TMP_FAIL(slot_map_.erase_refactored(rollback_seq))) {
          LOG_WARN_RET(tmp_ret, "erase partially inserted slot failed during rollback",
                      K(rollback_seq), K(role_type_), K_(controller_id), KP(this));
        }
      }
      LOG_WARN("[MIG VEC] rollback partially inserted slots", KR(ret),
              K(actually_inserted), K(can_use), K(role_type_), K_(controller_id), KP(this));
    }
    LOG_DEBUG("window grew ", K(role_type_), K_(controller_id), KP(this),
              K(can_use), K_(window_size), K_(head_seq));
  }
  if (used_count < granted_count && OB_NOT_NULL(mgr)) {
    ObArray<ObMigrationTenantWindowSlot> excess_slots;
    for (int64_t i = used_count; i < granted_count; ++i) {
      if (OB_TMP_FAIL(excess_slots.push_back(granted.at(i)))) {
        LOG_WARN_RET(tmp_ret,
                    "push back excess slot failed, slot leaked until destroy",
                    K(role_type_), K_(controller_id), KP(this));
      }
    }
    if (excess_slots.count() > 0 && OB_TMP_FAIL(mgr->free_slots(excess_slots, controller_id_))) {
      LOG_WARN_RET(tmp_ret, "free excess slots failed", K(role_type_), K_(controller_id), KP(this));
    }
  }
  return ret;
}

int ObMigrationSlidingWindowController::check_seq_in_window_(
    const int64_t seq_idx, bool &slot_in_window)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard w_guard(window_lock_);
  if (seq_idx < head_seq_) { // head already past this slot; caller ordering contract
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] seq already slid past", KR(ret), K(role_type_), K_(controller_id), K(seq_idx), K_(head_seq));
  } else {
    slot_in_window = (seq_idx < head_seq_ + window_size_);
  }
  return ret;
}

int ObMigrationSlidingWindowController::fill_slot_materialize_payload_(
    const int64_t seq_idx, const char *data, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  // Step 1: check slot state and transition RESERVED to FILLING (under window spin lock).
  // FILLING prevents concurrent fill on the same sequence index from
  // passing the state check, defending against duplicate RPC deliveries.
  char *buf_ptr = nullptr;
  Slot fill_slot;
  {
    common::ObSpinLockGuard w_guard(window_lock_);
    fill_slot.reset();
    if (OB_FAIL(slot_map_.get_refactored(seq_idx, fill_slot))) {
      LOG_WARN("[MIG VEC] slot not found in slot map", KR(ret), K(role_type_), K_(controller_id), KP(this),
              K(seq_idx));
    } else if (fill_slot.state_ != Slot::State::RESERVED || fill_slot.seq_ != seq_idx) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("[MIG VEC] slot state mismatch in fill slot", KR(ret), K(role_type_), K_(controller_id), KP(this),
              K(seq_idx), "slot state", fill_slot.state_, "slot seq", fill_slot.seq_);
    } else if (OB_ISNULL(fill_slot.mgr_slot_.buf()) || data_len > fill_slot.mgr_slot_.buf_cap()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[MIG VEC] slot buf invalid", KR(ret), K(role_type_), K_(controller_id), KP(this),
              K(seq_idx), KP(fill_slot.mgr_slot_.buf()), K(fill_slot.mgr_slot_.buf_cap()), K(data_len));
    } else {
      buf_ptr = fill_slot.mgr_slot_.buf();
      // Atomically claim the slot: RESERVED -> FILLING
      fill_slot.state_ = Slot::State::FILLING;
      if (OB_FAIL(slot_map_.set_refactored(seq_idx, fill_slot, 1 /*overwrite*/))) {
        LOG_WARN("[MIG VEC] update slot to FILLING failed", KR(ret), K(role_type_), K_(controller_id),
                KP(this), K(seq_idx));
      }
    }
  }
  // Step 2: MEMCPY outside lock (buffer is exclusively owned by this seq while FILLING)
  if (OB_SUCC(ret) && data_len > 0) {
    MEMCPY(buf_ptr, data, static_cast<size_t>(data_len));
  }
  // Step 3: transition FILLING to READY (under window spin lock).
  if (OB_SUCC(ret)) {
    common::ObSpinLockGuard w_guard(window_lock_);
    fill_slot.reset();
    if (OB_FAIL(slot_map_.get_refactored(seq_idx, fill_slot))) {
      LOG_WARN("[MIG VEC] slot disappeared during fill", KR(ret), K(role_type_), K_(controller_id), KP(this),
              K(seq_idx));
    } else if (OB_UNLIKELY(fill_slot.state_ != Slot::State::FILLING)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[MIG VEC] slot state unexpected in fill Step 3", KR(ret), K(role_type_), K_(controller_id),
              KP(this), K(seq_idx), "slot state", fill_slot.state_);
    } else {
      fill_slot.data_len_ = data_len;
      fill_slot.state_ = Slot::State::READY;
      if (OB_FAIL(slot_map_.set_refactored(seq_idx, fill_slot, 1 /*overwrite*/))) {
        LOG_WARN("[MIG VEC] update slot in slot_map failed, rollback to RESERVED",
                KR(ret), K(role_type_), K_(controller_id), KP(this), K(seq_idx));
        // Rollback
        int tmp_ret = OB_SUCCESS;
        fill_slot.state_ = Slot::State::RESERVED;
        fill_slot.data_len_ = 0;
        if (OB_TMP_FAIL(slot_map_.set_refactored(seq_idx, fill_slot, 1 /*overwrite*/))) {
          LOG_ERROR_RET(tmp_ret,
              "rollback slot to RESERVED failed, slot stuck in FILLING; "
              "window will deadlock until destroy drain",
              K(seq_idx), K(role_type_), K_(controller_id), KP(this));
        }
      }
    }
  }
  return ret;
}

int ObMigrationSlidingWindowController::fill_slot_(
    const int64_t seq_idx, const char *data, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  bool slot_in_window = false;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] controller not inited", KR(ret), K(role_type_), K_(controller_id), KP(this));
  } else if (OB_UNLIKELY(data_len < 0 || (OB_ISNULL(data) && data_len > 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[MIG VEC] invalid argument", KR(ret), K(data_len), KP(data));
  } else if (OB_FAIL(check_seq_in_window_(seq_idx, slot_in_window))) {
    LOG_WARN("[MIG VEC] check seq in window failed in fill slot", KR(ret),
            K(role_type_), K_(controller_id), KP(this), K(seq_idx));
  } else if (OB_UNLIKELY(!slot_in_window)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("[MIG VEC] seq_idx not in window when fill_slot_ called, "
              "caller contract violated", KR(ret),
              K(role_type_), K_(controller_id), KP(this), K(seq_idx));
  } else if (OB_FAIL(fill_slot_materialize_payload_(seq_idx, data, data_len))) {
    LOG_WARN("[MIG VEC] materialize payload failed", KR(ret),
            K(role_type_), K_(controller_id), KP(this), K(seq_idx));
  }
  return ret;
}

int ObMigrationSlidingWindowController::try_consume_slot_(
    const int64_t seq_idx, char *out_buf, const int64_t out_buf_len,
    int64_t &data_len)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_consumed = false;
  char *src_buf_ptr = nullptr;
  int64_t src_data_len = 0;
  ObMigrationTenantWindowSlot consumed_mgr_slot;
  Slot consume_slot;
  if (ATOMIC_LOAD(&stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("[MIG VEC] try consume slot stopped", KR(ret), K(role_type_), K_(controller_id), KP(this), K(seq_idx));
  } else {
    // Step 1: check iterator end, then slot state; collect buffer info and erase (under lock).
    common::ObSpinLockGuard w_guard(window_lock_);
    if (total_task_count_ != INT64_MAX && seq_idx >= total_task_count_) {
      ret = OB_ITER_END;
      LOG_INFO("try consume slot reached end", KR(ret), K(role_type_), K_(controller_id), KP(this),
              K(seq_idx), K_(total_task_count));
    } else {
      consume_slot.reset();
      const int slot_lookup_ret = slot_map_.get_refactored(seq_idx, consume_slot);
      if (OB_SUCCESS == slot_lookup_ret && consume_slot.state_ == Slot::State::READY && consume_slot.seq_ == seq_idx) {
        if (consume_slot.data_len_ > out_buf_len) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("[MIG VEC] output buf not enough", KR(ret), K(role_type_), K_(controller_id), KP(this),
                  K(seq_idx), K(consume_slot.data_len_), K(out_buf_len));
        } else {
          src_buf_ptr = consume_slot.mgr_slot_.buf();
          src_data_len = consume_slot.data_len_;
          consumed_mgr_slot = consume_slot.mgr_slot_;
          is_consumed = true;
          if (OB_FAIL(slot_map_.erase_refactored(seq_idx))) {
            LOG_WARN("[MIG VEC] erase consumed slot from slot map failed", KR(ret), K(role_type_),
                    K_(controller_id), KP(this), K(seq_idx));
          }
        }
      }
      // Else: slot absent or not READY; caller gets EAGAIN below.
    }
  }
  // Step 2: memcpy outside window spin lock (buffer is safe: slot already erased, no one else
  // touches it until we free it back to mgr).
  if (OB_SUCC(ret) && is_consumed) {
    MEMCPY(out_buf, src_buf_ptr, static_cast<size_t>(src_data_len));
    data_len = src_data_len;
    if (consumed_mgr_slot.is_valid()) {
      ObMigrationTenantWindowMgr *mgr = ATOMIC_LOAD(&window_mgr_);
      if (OB_NOT_NULL(mgr)) {
        ObArray<ObMigrationTenantWindowSlot> slots_to_free;
        if (OB_FAIL(slots_to_free.push_back(consumed_mgr_slot))) {
          LOG_WARN("[MIG VEC] push back consumed mgr slot failed", KR(ret), K(role_type_),
                  K_(controller_id), KP(this), K(consumed_mgr_slot));
        } else if (OB_FAIL(mgr->free_slots(slots_to_free, controller_id_))) {
          LOG_WARN("[MIG VEC] free consumed slot to mgr failed", KR(ret), K(role_type_),
                  K_(controller_id), KP(this), K(consumed_mgr_slot));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_TMP_FAIL(try_slide_head_())) {
        LOG_WARN_RET(tmp_ret, "try slide head failed", KR(tmp_ret),
                K(role_type_), K_(controller_id), KP(this), K(seq_idx));
      }
    }
  } else if (OB_SUCC(ret) && !is_consumed) {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObMigrationSlidingWindowController::try_slide_head_()
{
  LOG_DEBUG("try slide head", K(role_type_), K_(controller_id), KP(this));
  // Slot lifecycle: RESERVED insert, fill path RESERVED to FILLING to READY,
  // try-consume removes READY and erases. Slide-head advances head sequence
  // for each consecutive consumed index missing from slot map.
  int ret = OB_SUCCESS;
  int64_t slid_count = 0;

  {
    common::ObSpinLockGuard w_guard(window_lock_);
    bool keep_sliding = true;
    Slot slot;
    while (OB_SUCC(ret) && keep_sliding && window_size_ > 0) {
      const int64_t slid_head_seq = head_seq_;
      slot.reset();
      const int head_slot_lookup_ret = slot_map_.get_refactored(slid_head_seq, slot);
      if (OB_HASH_NOT_EXIST == head_slot_lookup_ret) {
        // Slot erased by try-consume (consumed); advance head.
        head_seq_++;
        window_size_--;
        ++slid_count;
        LOG_DEBUG("migration sliding window slid", K(role_type_), K_(controller_id), KP(this),
                K(slid_head_seq), K_(head_seq), K_(window_size));
      } else if (OB_SUCCESS == head_slot_lookup_ret) {
        // slot still exists (RESERVED or READY but not yet consumed), stop sliding
        keep_sliding = false;
      } else {
        ret = head_slot_lookup_ret;
        LOG_WARN("[MIG VEC] slot map get head slot failed in try slide head", KR(ret), K(role_type_),
                K_(controller_id), KP(this), K(slid_head_seq));
        keep_sliding = false;
      }
    }
  }

  if (OB_SUCC(ret) && slid_count > 0) {
    on_after_slide_head_();
    const int64_t now = ObTimeUtility::current_time();
    const int64_t prev_last = ATOMIC_LOAD(&last_slide_head_log_us_);
    if (0 == prev_last
        || now - prev_last >= SLIDE_HEAD_SUCCESS_LOG_INTERVAL_US) {
      ATOMIC_STORE(&last_slide_head_log_us_, now);
      LOG_INFO("[MIG VEC] try slide head success", K(role_type_), K_(controller_id), KP(this));
    }
  }
  return ret;
}

int ObMigrationSlidingWindowController::set_total_task_count(
    const int64_t total_task_count)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] sliding window controller not inited, skip set total task count",
            K(ret), K(role_type_), K_(controller_id), KP(this), K(total_task_count));
  } else if (OB_UNLIKELY(total_task_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[MIG VEC] invalid total task count", KR(ret), K(role_type_), K_(controller_id),
            KP(this), K(total_task_count));
  } else {
    common::ObSpinLockGuard w_guard(window_lock_);
    total_task_count_ = total_task_count;
    LOG_INFO("set total task count", K(role_type_), K_(controller_id), KP(this),
            K(total_task_count), K_(head_seq), K_(window_size));
  }
  return ret;
}

int ObMigrationSlidingWindowController::shrink_total_task_count(
    const int64_t total_task_count)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] sliding window controller not inited, skip shrink total task count",
            K(ret), K(role_type_), K_(controller_id), KP(this), K(total_task_count));
  } else if (OB_UNLIKELY(total_task_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[MIG VEC] invalid total task count", KR(ret), K(role_type_), K_(controller_id),
            KP(this), K(total_task_count));
  } else {
    common::ObSpinLockGuard w_guard(window_lock_);
    if (total_task_count < total_task_count_) {
      total_task_count_ = total_task_count;
      LOG_INFO("shrink total task count", K(role_type_), K_(controller_id), KP(this),
              K(total_task_count), K_(head_seq), K_(window_size));
    }
  }
  return ret;
}

// =========================================================================
// ObMigrationSlidingWindowSourceController
// =========================================================================

ObMigrationSlidingWindowSourceController::ObMigrationSlidingWindowSourceController()
  : ObMigrationSlidingWindowController(ObMigrationControllerInfo::Role::SOURCE),
    next_generate_seq_(0)
{
}

int ObMigrationSlidingWindowSourceController::acquire_window_slot_(
    const int64_t seq_idx, const int64_t wait_timeout_us)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] controller not inited", KR(ret), K(role_type_), K_(controller_id), KP(this));
  } else if (OB_UNLIKELY(seq_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[MIG VEC] invalid seq_idx", KR(ret), K(role_type_), K_(controller_id), KP(this), K(seq_idx));
  } else {
    const int64_t deadline_us = ObTimeUtility::current_time()
        + (0 != wait_timeout_us ? wait_timeout_us : DEFAULT_OP_TIMEOUT_US);
    bool slot_in_window = false;
    while (OB_SUCC(ret) && !slot_in_window) {
      int64_t grew_count = 0;
      int tmp_ret = OB_SUCCESS;
      if (is_stopped_()) {
        ret = OB_CANCELED;
        LOG_WARN("[MIG VEC] acquire window slot stopped", KR(ret),
                K(role_type_), K_(controller_id), K(seq_idx));
      } else {
        if (OB_TMP_FAIL(try_grow_window_(grew_count))) {
          LOG_WARN_RET(tmp_ret, "try grow window failed in acquire window slot",
                      K(role_type_), K_(controller_id), KP(this), K(seq_idx));
        }
        if (OB_FAIL(check_seq_in_window_(seq_idx, slot_in_window))) {
          LOG_WARN("[MIG VEC] check seq in window failed in acquire window slot",
                  KR(ret), K(role_type_), K_(controller_id), K(seq_idx));
        } else if (!slot_in_window) {
          const int64_t remain = deadline_us - ObTimeUtility::current_time();
          if (remain <= 0) {
            ret = OB_TIMEOUT;
            LOG_WARN("[MIG VEC] acquire window slot wait timeout", KR(ret),
                    K(role_type_), K_(controller_id), KP(this), K(seq_idx));
          } else {
            const int64_t sleep_us = MIN(remain, SLOT_POLL_INTERVAL_US);
            ob_usleep(static_cast<uint32_t>(sleep_us));
          }
        }
      }
    }
  }
  return ret;
}

int ObMigrationSlidingWindowSourceController::generate_next_data(
    const char *data, const int64_t data_len, const int64_t wait_timeout_us)
{
  int ret = OB_SUCCESS;
  const int64_t cur_seq = next_generate_seq_;
  if (OB_FAIL(acquire_window_slot_(cur_seq, wait_timeout_us))) {
    LOG_WARN("[MIG VEC] failed to acquire window slot for next data", KR(ret),
            K(role_type_), K_(controller_id), KP(this), K(cur_seq));
  } else if (OB_FAIL(fill_slot_(cur_seq, data, data_len))) {
    LOG_WARN("[MIG VEC] failed to fill slot for next data", KR(ret),
            K(role_type_), K_(controller_id), KP(this), K(cur_seq));
  } else {
    ATOMIC_STORE(&next_generate_seq_, cur_seq + 1);
  }
  return ret;
}

int ObMigrationSlidingWindowSourceController::try_get_data(
    const int64_t seq_idx, char *out_buf, const int64_t out_buf_len,
    int64_t &data_len)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] controller not inited", KR(ret), K(role_type_), K_(controller_id), KP(this));
  } else if (OB_UNLIKELY(OB_ISNULL(out_buf) || out_buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[MIG VEC] invalid argument", KR(ret), KP(out_buf), K(out_buf_len));
  } else if (OB_FAIL(try_consume_slot_(seq_idx, out_buf, out_buf_len, data_len))) {
    if (OB_EAGAIN != ret && OB_ITER_END != ret) {
      LOG_WARN("[MIG VEC] failed to try get data", KR(ret),
               K(role_type_), K_(controller_id), KP(this), K(seq_idx));
    }
  }
  return ret;
}

// =========================================================================
// ObMigrationSlidingWindowDestController
// =========================================================================

ObMigrationSlidingWindowDestController::ObMigrationSlidingWindowDestController()
  : ObMigrationSlidingWindowController(ObMigrationControllerInfo::Role::DEST),
    next_consume_seq_(0)
{
}

void ObMigrationSlidingWindowDestController::on_after_slide_head_()
{
  int tmp_ret = OB_SUCCESS;
  int64_t granted_start_seq = 0;
  int64_t granted_slot_count = 0;
  if (OB_TMP_FAIL(try_grow_window_(granted_slot_count, &granted_start_seq))) {
    LOG_WARN_RET(tmp_ret, "[MIG VEC] try grow window failed after slide head",
                K(role_type_), K_(controller_id), KP(this));
  }

  ObIMigrationWindowSlidCallback *callback = get_on_window_slid_();
  if (OB_NOT_NULL(callback)) {
    callback->on_window_slid(granted_start_seq, granted_slot_count);
  }
}

int ObMigrationSlidingWindowDestController::fill_data(
    const int64_t seq_idx, const char *data, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fill_slot_(seq_idx, data, data_len))) {
    LOG_WARN("[MIG VEC] failed to fill data", KR(ret),
            K(role_type_), K_(controller_id), KP(this), K(seq_idx));
  }
  return ret;
}

int ObMigrationSlidingWindowDestController::get_next_consume_data(
    char *out_buf, const int64_t out_buf_len, int64_t &data_len,
    const int64_t wait_timeout_us)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] controller not inited", KR(ret), K(role_type_), K_(controller_id), KP(this));
  } else if (OB_UNLIKELY(OB_ISNULL(out_buf) || out_buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[MIG VEC] invalid argument", KR(ret), KP(out_buf), K(out_buf_len));
  } else {
    const int64_t deadline_us = ObTimeUtility::current_time()
        + (0 != wait_timeout_us ? wait_timeout_us : DEFAULT_OP_TIMEOUT_US);
    // Poll try_consume_slot_: success leaves loop; EAGAIN sleeps until deadline; other ret ends loop.
    while (true) {
      if (OB_SUCC(try_consume_slot_(next_consume_seq_, out_buf, out_buf_len, data_len))) {
        break;
      }
      if (OB_EAGAIN != ret) {
        if (OB_ITER_END != ret && OB_CANCELED != ret) {
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
            LOG_WARN("[MIG VEC] failed to get next consume data", KR(ret),
                    K(role_type_), K_(controller_id), KP(this), K(next_consume_seq_));
          }
        }
        break;
      }
      int64_t grew = 0;
      int64_t start_seq = 0;
      int tmp_ret = try_grow_window_(grew, &start_seq);
      ObIMigrationWindowSlidCallback *cb = get_on_window_slid_();
      if (OB_SUCCESS == tmp_ret && grew > 0 && OB_NOT_NULL(cb)) {
        cb->on_window_slid(start_seq, grew);
      }

      const int64_t remain_us = deadline_us - ObTimeUtility::current_time();
      if (remain_us <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("[MIG VEC] get next consume data wait timeout", KR(ret),
                K(role_type_), K_(controller_id), KP(this), K(next_consume_seq_));
        break;
      }
      ob_usleep(static_cast<uint32_t>(MIN(remain_us, SLOT_POLL_INTERVAL_US)));
    }
    if (OB_SUCC(ret)) {
      next_consume_seq_++;
    }
  }
  return ret;
}

int ObMigrationSlidingWindowSourceController::create(
    ObMigrationTenantWindowMgr *window_mgr,
    ObIMigrationWindowSlidCallback *on_window_slid,
    const share::ObDagPrio::ObDagPrioEnum dag_prio,
    ObMigrationSlidingWindowSourceHandle &out_handle)
{
  int ret = OB_SUCCESS;
  ObMigrationSlidingWindowSourceController *ctrl = nullptr;
  if (OB_ISNULL(ctrl = OB_NEW(ObMigrationSlidingWindowSourceController, "MigVecIdxCtrl"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[MIG VEC] alloc source controller failed", KR(ret));
  } else if (OB_FAIL(ctrl->init(window_mgr, on_window_slid, dag_prio))) {
    LOG_WARN("[MIG VEC] init source controller failed", KR(ret), K(dag_prio));
    OB_DELETE(ObMigrationSlidingWindowSourceController, "MigVecIdxCtrl", ctrl);
    ctrl = nullptr;
  } else {
    out_handle = ObMigrationSlidingWindowSourceHandle(ctrl);
  }
  return ret;
}

int ObMigrationSlidingWindowDestController::create(
    ObMigrationTenantWindowMgr *window_mgr,
    ObIMigrationWindowSlidCallback *on_window_slid,
    const share::ObDagPrio::ObDagPrioEnum dag_prio,
    ObMigrationSlidingWindowDestHandle &out_handle)
{
  int ret = OB_SUCCESS;
  ObMigrationSlidingWindowDestController *ctrl = nullptr;
  if (OB_ISNULL(ctrl = OB_NEW(ObMigrationSlidingWindowDestController, "MigVecIdxCtrl"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[MIG VEC] alloc dest controller failed", KR(ret));
  } else if (OB_FAIL(ctrl->init(window_mgr, on_window_slid, dag_prio))) {
    LOG_WARN("[MIG VEC] init dest controller failed", KR(ret), K(dag_prio));
    OB_DELETE(ObMigrationSlidingWindowDestController, "MigVecIdxCtrl", ctrl);
    ctrl = nullptr;
  } else {
    out_handle = ObMigrationSlidingWindowDestHandle(ctrl);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
