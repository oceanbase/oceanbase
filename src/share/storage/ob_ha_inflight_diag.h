/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_HA_INFLIGHT_DIAG_
#define OCEANBASE_SHARE_HA_INFLIGHT_DIAG_

#include "lib/container/ob_array.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "share/ob_storage_ha_diagnose_struct.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace share
{

// Pure-data inflight diagnostic state. Safe to copy and use as a stack-local
// accumulator at external call sites (backfill dags, replay helpers,
// ObTxFinishTransfer doing path). Callers stamp steps via set_cost_item and
// final error via record_error, then submit the whole batch to the owning
// ObTransferDiagCtx through ObTransferUtils::merge_transfer_diag.
struct ObHAInflightDiagState
{
  ObHAInflightDiagState();
  void reset();
  bool has_any_phase_filled() const;

  TO_STRING_KV(K_(task_id), K_(retry_count), K_(tablet_count),
               K_(start_ts), K_(end_ts),
               "cur_phase", ha_diag_task_type_str(cur_phase_),
               K_(cur_cost_item),
               K_(last_err_code), K_(last_result_msg));

  // Stamp a step at now (no-op on invalid item). When called on a
  // stack-local state, no lock is needed; on the handler-owned state the
  // enclosing ObHAInflightDiag takes its rwlock before delegating here.
  void set_cost_item(const ObStorageHACostItemName item);
  // First-wins error recording: drops subsequent calls once a failure has
  // been pinned, so cleanup-branch reports don't clobber the root cause.
  void record_error(const int err_code, const ObStorageHACostItemName msg);
  // Convenience for the ubiquitous "on success stamp the step, on failure
  // record it as the error" pattern at call sites.
  void update_cost_item(const int ret, const ObStorageHACostItemName item);
  // Merge another state's step timestamps and (first-wins) error into this one.
  // Non-zero ts slots in `other` that are still 0 in this state are copied over;
  // cur_cost_item_ advances to `other`'s latest item when strictly newer.
  void merge_from(const ObHAInflightDiagState &other);

  ObTransferTaskID task_id_;
  // Per-task retry counter stamped by the handler driver (leader side only).
  // External merges from replay / backfill dags leave this alone; the snapshot
  // taken at flush_history time carries the handler's authoritative value into
  // the history row's RETRY_ID column.
  int64_t retry_count_;
  // Number of tablets in the transfer task, stamped once on the leader after
  // register_task when task_info_.tablet_list_ is available. Follower-side
  // state keeps the default 0. Exposed via the perf virtual table's
  // TABLET_COUNT column.
  int64_t tablet_count_;
  int64_t start_ts_;
  int64_t end_ts_;
  // Observer-local execution phase — carries src/dst role and BACKFILLED
  // sub-phase that the task-level ObTransferStatus can't express. Handler
  // driver sets TRANSFER_{START,DOING,ABORT}; mds replay helpers set the
  // *_IN/*_OUT variants; backfill dags set TRANSFER_BACKFILLED.
  ObStorageHADiagTaskType cur_phase_;
  // Fine-grained sub-step tracking. Indexed by ObStorageHACostItemName; each
  // set_cost_item call stamps the current time into the slot.
  ObStorageHACostItemName cur_cost_item_;
  int64_t cost_item_ts_[static_cast<int64_t>(ObStorageHACostItemName::MAX_NAME)];
  int last_err_code_;
  ObStorageHACostItemName last_result_msg_;
};

// Embedded as a member of each transfer handler. All writers and readers go
// through its rwlock; the pure-data state_ can be snapshotted out under rlock
// for cheap downstream rendering.
class ObHAInflightDiag
{
public:
  ObHAInflightDiag();
  ~ObHAInflightDiag() = default;

  void on_task_begin(const ObTransferTaskID &task_id, const int64_t start_ts);
  void set_phase(const ObStorageHADiagTaskType phase);
  void set_cost_item(const ObStorageHACostItemName item);
  void record_error(const int err_code, const ObStorageHACostItemName msg);
  // Convenience helper: on success stamp the step, on failure pin (ret, item).
  void update_cost_item(const int ret, const ObStorageHACostItemName item);
  // Increment the handler-side retry counter. Called from the transfer
  // handler's tick when a retryable error triggers another attempt.
  void inc_retry_count();
  int64_t get_retry_count() const;
  // Stamp the tablet count of the task. Idempotent — callers re-stamp each
  // tick; the value is a property of the task, not of the progress.
  void set_tablet_count(const int64_t tablet_count);
  // Merge a caller-supplied local state into the owned state_ under wlock.
  // Used by ObTransferDiagCtx to absorb batches from external call sites.
  void merge_from(const ObHAInflightDiagState &other);
  void reset();

  void snapshot(ObHAInflightDiagState &out) const;

  // Render cost_item_ts_[] into a pipe-separated "NAME:+elapsed|NAME:+elapsed|..."
  // string, where elapsed is time since state.start_ts_ formatted as us/ms/s.
  static int serialize_info(const ObHAInflightDiagState &state, char *buf, const int64_t buf_len);

private:
  mutable common::SpinRWLock diag_lock_;
  ObHAInflightDiagState state_;

  DISALLOW_COPY_AND_ASSIGN(ObHAInflightDiag);
};

// Shared iterator for the inflight HA-diagnose virtual tables. Owns the
// snapshot of ls_ids that have an active transfer task and resolves each to
// its ObTransferHandler on demand. Both error/perf virtual tables emit one
// row per ls_id.
class ObHAInflightVirtualIter final
{
public:
  ObHAInflightVirtualIter();
  ~ObHAInflightVirtualIter() = default;

  void reset();
  int open();
  bool is_opened() const { return opened_; }

  // Advances to the next ls_id whose transfer handler resolves and whose
  // state carries a valid task_id. Returns OB_ITER_END when drained.
  int next(share::ObLSID &out_ls_id, share::ObHAInflightDiagState &out_state);

private:
  int load_snapshot_();
  bool try_resolve_state_(const share::ObLSID &ls_id, share::ObHAInflightDiagState &out_state);

  common::ObArray<share::ObLSID> keys_;
  int64_t idx_;
  // opened_ tracks the iter state machine (open()/reset()); loaded_ tracks
  // whether the ls_id snapshot has been lazily materialized on first next().
  bool opened_;
  bool loaded_;

  DISALLOW_COPY_AND_ASSIGN(ObHAInflightVirtualIter);
};

} // namespace share
} // namespace oceanbase
#endif // OCEANBASE_SHARE_HA_INFLIGHT_DIAG_
