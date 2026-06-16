/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_transfer_diag_ctx.h"
#include "lib/time/ob_time_utility.h"
#include "storage/high_availability/ob_storage_ha_diagnose_mgr.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

ObTransferDiagCtx::ObTransferDiagCtx()
  : ls_id_(),
    diag_(),
    task_id_(),
    registered_(false),
    lock_(common::ObLatchIds::OB_STORAGE_HA_DIAGNOSE_MGR_LOCK)
{
}

int ObTransferDiagCtx::init(const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id));
  } else {
    ls_id_ = ls_id;
  }
  return ret;
}

void ObTransferDiagCtx::register_task(const ObTransferTaskID &task_id)
{
  common::SpinWLockGuard guard(lock_);
  register_locked_(task_id);
}

int ObTransferDiagCtx::flush_history(const int32_t result_code)
{
  int ret = OB_SUCCESS;
  ObHADiagFlushItem item;
  bool has_payload = false;
  // Snapshot + unregister under lock; hand off outside it. merge_external_state
  // from replay / backfill dag workers also wants this wlock, so keep the
  // critical section short.
  {
    common::SpinWLockGuard guard(lock_);
    prepare_flush_locked_(result_code, item.snapshot_, has_payload);
    unregister_locked_();
  }
  if (has_payload) {
    ObStorageHADiagMgr *mgr = MTL(ObStorageHADiagMgr *);
    if (OB_ISNULL(mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObStorageHADiagMgr is null", K(ret), K_(ls_id));
    } else {
      item.ls_id_ = ls_id_;
      if (OB_FAIL(mgr->submit_flush(item))) {
        LOG_WARN("failed to submit ha diag flush item", K(ret), K(item));
      }
    }
  }
  return ret;
}

void ObTransferDiagCtx::set_phase(const ObStorageHADiagTaskType phase)
{
  if (registered_) {
    diag_.set_phase(phase);
  }
}

void ObTransferDiagCtx::set_cost_item(const ObStorageHACostItemName item)
{
  if (registered_) {
    diag_.set_cost_item(item);
  }
}

void ObTransferDiagCtx::update_cost_item(const int ret, const ObStorageHACostItemName item)
{
  if (registered_) {
    diag_.update_cost_item(ret, item);
  }
}

void ObTransferDiagCtx::record_error(const int err_code, const ObStorageHACostItemName msg)
{
  if (registered_) {
    ObHAInflightDiagState peek;
    diag_.snapshot(peek);
    LOG_INFO("[TRANSFER_DIAG] record_error", K_(ls_id), K_(task_id),
        K(err_code), K(msg), "prev_err", peek.last_err_code_);
    diag_.record_error(err_code, msg);
  } else {
    LOG_INFO("[TRANSFER_DIAG] record_error dropped (not registered)",
        K_(ls_id), K(err_code), K(msg));
  }
}

void ObTransferDiagCtx::register_locked_(const ObTransferTaskID &task_id)
{
  ObStorageHADiagMgr *mgr = nullptr;
  if (!ls_id_.is_valid() || !task_id.is_valid()) {
    // nothing to do
  } else if (registered_ && task_id_ == task_id) {
    // already tracking this task; keep accumulated state
  } else {
    if (registered_) {
      // A different task is still tracked — handler tick hadn't got around
      // to flushing it yet. Drop its residual state silently; no handoff from
      // here because this path may be driven by a replay thread, and the
      // authoritative history row will be produced by the leader's flush.
      unregister_locked_();
    }
    if (OB_NOT_NULL(mgr = MTL(ObStorageHADiagMgr *))) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(mgr->add_inflight(ls_id_))) {
        LOG_WARN_RET(tmp_ret, "failed to register transfer inflight ls id", K_(ls_id));
      } else {
        registered_ = true;
        diag_.on_task_begin(task_id, ObTimeUtility::current_time());
        task_id_ = task_id;
        LOG_INFO("[TRANSFER_DIAG] register_task", K_(ls_id), K(task_id));
      }
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "[TRANSFER_DIAG] mgr is null on register_task",
          K_(ls_id), K(task_id));
    }
  }
}

void ObTransferDiagCtx::unregister_locked_()
{
  ObStorageHADiagMgr *mgr = nullptr;
  if (registered_ && ls_id_.is_valid()) {
    if (OB_ISNULL(mgr = MTL(ObStorageHADiagMgr *))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "ObStorageHADiagMgr is null");
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(mgr->remove_inflight(ls_id_))) {
        LOG_WARN_RET(tmp_ret, "failed to unregister transfer inflight ls id", K_(ls_id));
      }
    }
    diag_.reset();
    task_id_.reset();
    registered_ = false;
  }
}

void ObTransferDiagCtx::prepare_flush_locked_(
    const int32_t result_code,
    ObHAInflightDiagState &snapshot,
    bool &has_payload)
{
  has_payload = false;
  if (OB_UNLIKELY(!registered_ || !ls_id_.is_valid())) {
    LOG_TRACE("[TRANSFER_DIAG] flush skipped — nothing to flush",
        K_(registered), K_(ls_id), K(result_code));
  } else {
    diag_.snapshot(snapshot);
    if (!snapshot.task_id_.is_valid()) {
      LOG_TRACE("[TRANSFER_DIAG] flush skipped — task never truly began",
          K_(ls_id), K_(task_id), K(result_code));
    } else if (!is_valid_ha_diag_task_type(snapshot.cur_phase_)) {
      // Would hit OB_INVALID_ARGUMENT in the DML layer anyway. This happens
      // when replay merged state into a ctx that never had a phase labelled
      // (e.g. a transient race where the handler has not yet ticked). Safe
      // to drop — the next tick will set the phase and try again, and the
      // leader-side row is the authoritative one regardless.
      LOG_INFO("[TRANSFER_DIAG] flush skipped — phase not labelled yet",
          K_(ls_id), "task_id", snapshot.task_id_, K(result_code));
    } else {
      snapshot.end_ts_ = ObTimeUtility::current_time();
      // Fold caller's terminating result into last_err_code_ only when no
      // in-task error was recorded; prefer the recorded error otherwise.
      if (OB_SUCCESS == snapshot.last_err_code_ && OB_SUCCESS != result_code) {
        snapshot.last_err_code_ = result_code;
      }
      const bool will_write_perf = snapshot.has_any_phase_filled();
      const bool will_write_error = (OB_SUCCESS != snapshot.last_err_code_);
      has_payload = will_write_perf || will_write_error;
    }
  }
}

int ObTransferDiagCtx::merge_external_state(
    const ObTransferTaskID &task_id,
    const ObHAInflightDiagState &local)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  bool can_merge = false;
  if (task_id.is_valid()) {
    register_locked_(task_id);
    can_merge = registered_ && task_id_ == task_id;
  } else {
    // Backfill-style callers: merge into whatever is currently tracked;
    // silently drop if the handler has not registered a task yet.
    can_merge = registered_;
  }
  if (can_merge) {
    diag_.merge_from(local);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
