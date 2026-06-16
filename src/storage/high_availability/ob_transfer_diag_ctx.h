/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_TRANSFER_DIAG_CTX_
#define OCEANBASE_STORAGE_TRANSFER_DIAG_CTX_

#include "lib/lock/ob_spin_rwlock.h"
#include "share/ob_balance_define.h"
#include "share/storage/ob_ha_inflight_diag.h"
#include "share/ob_ls_id.h"
#include "share/ob_storage_ha_diagnose_struct.h"

namespace oceanbase
{
namespace storage
{

// Aggregates all transfer diagnosis state that used to be scattered across
// ObTransferHandler (inflight diag object, registration bookkeeping, and
// the rwlock that guards cross-thread writes). The handler holds one of
// these and forwards every diagnosis-related call here. External call
// sites (backfill dags, replay helpers, ObTxFinishTransfer) build a
// stack-local ObHAInflightDiagState and submit it through
// ObTransferUtils::merge_transfer_diag, which resolves to this ctx via
// the dest-LS transfer handler.
class ObTransferDiagCtx final
{
public:
  ObTransferDiagCtx();
  ~ObTransferDiagCtx() = default;

  // Bind to the owning LS id. Called from ObTransferHandler::init once the LS
  // is fully set up. flush_history hands the payload to the tenant diag
  // worker, so no sql proxy is needed here anymore.
  int init(const share::ObLSID &ls_id);

  // Start tracking a task. Acquires the wlock. If a different task is already
  // tracked, its residual state is flushed with OB_SUCCESS before the switch.
  void register_task(const share::ObTransferTaskID &task_id);

  // Terminal path: snapshot the tracked state, unregister, and enqueue the
  // payload on the tenant ObStorageHADiagMgr; the global
  // ObStorageHADiagService drains the queue and writes rows into the history
  // tables asynchronously. Returns quickly — no SQL here. Invoked from:
  //   - ObTransferHandler destructor (LS teardown).
  //   - process() tick observing that the task has vanished from
  //     __all_transfer_task.
  // retry_id is read from the owned state_ (previously stamped via
  // inc_retry_count), so the snapshot handed to the history writer carries it
  // directly — no need for the caller to thread it through.
  int  flush_history(const int32_t result_code);

  // Progress updates from the handler's own (single-threaded) process()
  // path. These intentionally skip the wlock; the embedded ObHAInflightDiag
  // has its own lock and `registered_` races here are benign (they can only
  // drop updates, never corrupt state).
  void set_phase(const share::ObStorageHADiagTaskType phase);
  void set_cost_item(const share::ObStorageHACostItemName item);
  // On success, stamps item as the latest completed step. On failure,
  // records (ret, item) as the error — so the err_code is captured at the
  // failure site rather than relying on an outer record_error fallback.
  void update_cost_item(const int ret, const share::ObStorageHACostItemName item);
  // Records (err_code, msg) as the task's error. First-wins: the underlying
  // ObHAInflightDiag drops subsequent calls once an error has been pinned,
  // so cleanup-branch reports don't clobber the root cause.
  void record_error(const int err_code, const share::ObStorageHACostItemName msg);
  // Per-task retry counter owned by the inflight state. Handler's tick calls
  // inc_retry_count() each time it schedules another attempt; history writer
  // reads the current value from the flush snapshot.
  void inc_retry_count() { diag_.inc_retry_count(); }
  int64_t get_retry_count() const { return diag_.get_retry_count(); }
  // Stamped by the leader handler right after register_task, once
  // task_info_.tablet_list_ is available. Surfaces on the perf virtual table
  // and the perf history row's TABLET_COUNT column.
  void set_tablet_count(const int64_t tablet_count) { diag_.set_tablet_count(tablet_count); }

  // Read-only snapshot for virtual tables.
  void peek(share::ObHAInflightDiagState &state) const { diag_.snapshot(state); }

  // Cheap registration probe. Used by the handler tick to decide whether a
  // follower-side ctx has any pending state to watch (put there by replay
  // merges) — if not, the tick can skip the __all_transfer_task probe
  // entirely. Racy but benign: a false read just drops this tick's flush
  // check, the next tick catches it.
  bool is_registered() const { return registered_; }

  // Unified external entry. Callers hold a stack-local
  // ObHAInflightDiagState, stamp steps and a final error into it, then
  // submit the whole batch here. Safe to call concurrently with the
  // handler thread; takes the wlock once to serialize against
  // register/unregister/flush.
  //   - Valid `task_id` (replay helpers / ObTxFinishTransfer): register the
  //     task if not yet tracked, then match & merge. Stale batches whose
  //     task has been superseded are dropped.
  //   - Invalid `task_id` (backfill dags that don't naturally carry the
  //     transfer task id): merge into whatever task is currently tracked.
  //     Dropped silently when nothing is registered.
  int merge_external_state(
      const share::ObTransferTaskID &task_id,
      const share::ObHAInflightDiagState &local);

private:
  void register_locked_(const share::ObTransferTaskID &task_id);
  void unregister_locked_();
  // Under wlock: snapshot the state and decide whether the handoff is worth
  // making. `has_payload` is true iff the snapshot carries an actionable
  // phase/task pair that the worker will translate into history rows.
  void prepare_flush_locked_(
      const int32_t result_code,
      share::ObHAInflightDiagState &snapshot,
      bool &has_payload);

private:
  share::ObLSID                 ls_id_;
  share::ObHAInflightDiag       diag_;
  share::ObTransferTaskID       task_id_;
  bool                          registered_;
  mutable common::SpinRWLock    lock_;

  DISALLOW_COPY_AND_ASSIGN(ObTransferDiagCtx);
};

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_TRANSFER_DIAG_CTX_
