/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/storage/ob_ha_inflight_diag.h"
#include "lib/time/ob_time_utility.h"
#include "storage/high_availability/ob_storage_ha_diagnose_mgr.h"
#include "storage/high_availability/ob_transfer_handler.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace storage;
namespace share
{

ObHAInflightDiagState::ObHAInflightDiagState()
{
  reset();
}

void ObHAInflightDiagState::reset()
{
  task_id_.reset();
  retry_count_ = 0;
  tablet_count_ = 0;
  start_ts_ = 0;
  end_ts_ = 0;
  cur_phase_ = ObStorageHADiagTaskType::MAX_TYPE;
  cur_cost_item_ = ObStorageHACostItemName::MAX_NAME;
  MEMSET(cost_item_ts_, 0, sizeof(cost_item_ts_));
  last_err_code_ = OB_SUCCESS;
  last_result_msg_ = ObStorageHACostItemName::MAX_NAME;
}

bool ObHAInflightDiagState::has_any_phase_filled() const
{
  bool filled = false;
  for (int64_t i = 0; !filled && i < static_cast<int64_t>(ObStorageHACostItemName::MAX_NAME); ++i) {
    if (cost_item_ts_[i] != 0) {
      filled = true;
    }
  }
  return filled;
}

void ObHAInflightDiagState::set_cost_item(const ObStorageHACostItemName item)
{
  const int64_t idx = static_cast<int64_t>(item);
  if (idx >= 0 && idx < static_cast<int64_t>(ObStorageHACostItemName::MAX_NAME)) {
    cur_cost_item_ = item;
    cost_item_ts_[idx] = common::ObTimeUtility::current_time();
  }
}

void ObHAInflightDiagState::record_error(const int err_code, const ObStorageHACostItemName msg)
{
  // First-wins: only the primary (first) failure survives. Subsequent
  // record_error calls — including the replay-side ones, or error
  // fallbacks fired from outer layers — are ignored. This keeps the
  // reported cause pinned to the root failure instead of the last cleanup
  // mishap that happened to overwrite it.
  if (OB_SUCCESS == last_err_code_ && OB_SUCCESS != err_code) {
    last_err_code_ = err_code;
    last_result_msg_ = msg;
  }
}

void ObHAInflightDiagState::update_cost_item(const int ret, const ObStorageHACostItemName item)
{
  if (OB_SUCCESS == ret) {
    set_cost_item(item);
  } else {
    record_error(ret, item);
  }
}

void ObHAInflightDiagState::merge_from(const ObHAInflightDiagState &other)
{
  // Last-wins on the phase label. Replay helpers (start/finish_transfer_in)
  // and backfill dags both advance the phase through a sequence on the
  // dst side, and on follower replicas — where the handler never calls
  // set_phase directly — the only way for TYPE to progress past its first
  // stamp is for each merge to overwrite. On leaders the handler's periodic
  // set_phase re-stamps the authoritative outer phase every tick, so any
  // stale replay merge gets corrected on the next wakeup.
  if (is_valid_ha_diag_task_type(other.cur_phase_)) {
    cur_phase_ = other.cur_phase_;
  }
  const int64_t max_idx = static_cast<int64_t>(ObStorageHACostItemName::MAX_NAME);
  int64_t newest_idx = -1;
  int64_t newest_ts = 0;
  for (int64_t i = 0; i < max_idx; ++i) {
    if (other.cost_item_ts_[i] > 0) {
      if (cost_item_ts_[i] == 0) {
        cost_item_ts_[i] = other.cost_item_ts_[i];
      }
      if (other.cost_item_ts_[i] > newest_ts) {
        newest_ts = other.cost_item_ts_[i];
        newest_idx = i;
      }
    }
  }
  if (newest_idx >= 0) {
    const int64_t cur_idx = static_cast<int64_t>(cur_cost_item_);
    const int64_t cur_ts = (cur_idx >= 0 && cur_idx < max_idx) ? cost_item_ts_[cur_idx] : 0;
    if (newest_ts > cur_ts) {
      cur_cost_item_ = static_cast<ObStorageHACostItemName>(newest_idx);
    }
  }
  record_error(other.last_err_code_, other.last_result_msg_);
}

ObHAInflightDiag::ObHAInflightDiag()
    : diag_lock_(common::ObLatchIds::OB_STORAGE_HA_DIAGNOSE_MGR_LOCK), state_()
{
}

void ObHAInflightDiag::on_task_begin(const ObTransferTaskID &task_id, const int64_t start_ts)
{
  common::SpinWLockGuard guard(diag_lock_);
  state_.reset();
  state_.task_id_ = task_id;
  state_.start_ts_ = start_ts;
}

void ObHAInflightDiag::set_phase(const ObStorageHADiagTaskType phase)
{
  if (is_valid_ha_diag_task_type(phase)) {
    common::SpinWLockGuard guard(diag_lock_);
    state_.cur_phase_ = phase;
  }
}

void ObHAInflightDiag::set_cost_item(const ObStorageHACostItemName item)
{
  common::SpinWLockGuard guard(diag_lock_);
  state_.set_cost_item(item);
}

void ObHAInflightDiag::record_error(const int err_code, const ObStorageHACostItemName msg)
{
  common::SpinWLockGuard guard(diag_lock_);
  state_.record_error(err_code, msg);
}

void ObHAInflightDiag::update_cost_item(const int ret, const ObStorageHACostItemName item)
{
  common::SpinWLockGuard guard(diag_lock_);
  state_.update_cost_item(ret, item);
}

void ObHAInflightDiag::inc_retry_count()
{
  common::SpinWLockGuard guard(diag_lock_);
  ++state_.retry_count_;
}

int64_t ObHAInflightDiag::get_retry_count() const
{
  common::SpinRLockGuard guard(diag_lock_);
  return state_.retry_count_;
}

void ObHAInflightDiag::set_tablet_count(const int64_t tablet_count)
{
  if (tablet_count >= 0) {
    common::SpinWLockGuard guard(diag_lock_);
    state_.tablet_count_ = tablet_count;
  }
}

void ObHAInflightDiag::merge_from(const ObHAInflightDiagState &other)
{
  common::SpinWLockGuard guard(diag_lock_);
  state_.merge_from(other);
}

void ObHAInflightDiag::reset()
{
  common::SpinWLockGuard guard(diag_lock_);
  state_.reset();
}

void ObHAInflightDiag::snapshot(ObHAInflightDiagState &out) const
{
  common::SpinRLockGuard guard(diag_lock_);
  out.start_ts_ = state_.start_ts_;
  out.end_ts_ = state_.end_ts_;
  out.cur_phase_ = state_.cur_phase_;
  out.cur_cost_item_ = state_.cur_cost_item_;
  MEMCPY(out.cost_item_ts_, state_.cost_item_ts_, sizeof(out.cost_item_ts_));
  out.last_err_code_ = state_.last_err_code_;
  out.last_result_msg_ = state_.last_result_msg_;
  out.task_id_ = state_.task_id_;
  out.retry_count_ = state_.retry_count_;
  out.tablet_count_ = state_.tablet_count_;
}

// Format a microsecond duration into the buffer using us/ms/s — mirrors the
// ObCompactionTimeGuard rendering convention so operators see "12.34ms" rather
// than raw microseconds.
static int fmt_duration_us_(char *buf, const int64_t buf_len, int64_t &pos, const int64_t us)
{
  int ret = OB_SUCCESS;
  constexpr int64_t US_PER_MS = 1000;
  constexpr int64_t US_PER_S = 1000 * 1000;
  if (us < US_PER_MS) {
    ret = databuff_printf(buf, buf_len, pos, "%ldus", us);
  } else if (us < US_PER_S) {
    ret = databuff_printf(buf, buf_len, pos, "%.2lfms", static_cast<double>(us) / US_PER_MS);
  } else {
    ret = databuff_printf(buf, buf_len, pos, "%.2lfs", static_cast<double>(us) / US_PER_S);
  }
  return ret;
}

int ObHAInflightDiag::serialize_info(
    const ObHAInflightDiagState &state, char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else {
    buf[0] = '\0';
    const int64_t base_ts = state.start_ts_;
    bool truncated = false;
    for (int64_t i = 0; !truncated && OB_SUCC(ret)
         && i < static_cast<int64_t>(ObStorageHACostItemName::MAX_NAME); ++i) {
      const int64_t ts = state.cost_item_ts_[i];
      if (ts == 0) {
        continue;
      }
      const char *name = ObStorageHACostItemNameStr[i];
      const int64_t elapsed = (base_ts > 0 && ts >= base_ts) ? (ts - base_ts) : 0;
      ret = databuff_printf(buf, buf_len, pos, "%s:+", name);
      if (OB_SUCC(ret)) {
        ret = fmt_duration_us_(buf, buf_len, pos, elapsed);
      }
      if (OB_SUCC(ret)) {
        ret = databuff_printf(buf, buf_len, pos, "|");
      }
      if (OB_SIZE_OVERFLOW == ret) {
        ret = OB_SUCCESS;
        truncated = true;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to print cost item", K(ret), K(i), K(name));
      }
    }
  }
  return ret;
}

ObHAInflightVirtualIter::ObHAInflightVirtualIter()
  : keys_(),
    idx_(0),
    opened_(false),
    loaded_(false)
{
}

void ObHAInflightVirtualIter::reset()
{
  keys_.reset();
  idx_ = 0;
  opened_ = false;
  loaded_ = false;
}

int ObHAInflightVirtualIter::open()
{
  int ret = OB_SUCCESS;
  if (opened_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObHAInflightVirtualIter already opened", K(ret));
  } else {
    // Snapshot load is lazy — deferred to first next() so tenants with no
    // inflight ls_ids pay nothing beyond the open call.
    opened_ = true;
  }
  return ret;
}

int ObHAInflightVirtualIter::load_snapshot_()
{
  int ret = OB_SUCCESS;
  ObStorageHADiagMgr *mgr = MTL(ObStorageHADiagMgr *);
  if (OB_ISNULL(mgr)) {
    // Tenant has no mgr yet — present as empty iter.
  } else if (OB_FAIL(mgr->get_inflight_snapshot(keys_))) {
    LOG_WARN("failed to snapshot inflight index", K(ret));
  }
  idx_ = 0;
  loaded_ = true;
  return ret;
}

bool ObHAInflightVirtualIter::try_resolve_state_(
    const ObLSID &ls_id, ObHAInflightDiagState &out_state)
{
  bool resolved = false;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle handle;
  ObLS *ls = nullptr;
  ObTransferHandler *handler = nullptr;
  if (OB_ISNULL(ls_service)) {
    // ls service not ready in this tenant
  } else if (OB_SUCCESS != ls_service->get_ls(ls_id, handle, ObLSGetMod::HA_MOD)) {
    // ls has been dropped / not found
  } else if (OB_ISNULL(ls = handle.get_ls())) {
    // ls handle empty
  } else if (OB_ISNULL(handler = ls->get_transfer_handler())) {
    // handler not created yet
  } else {
    handler->peek_inflight_diag(out_state);
    resolved = true;
  }
  return resolved;
}

int ObHAInflightVirtualIter::next(ObLSID &out_ls_id, ObHAInflightDiagState &out_state)
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (!loaded_ && OB_FAIL(load_snapshot_())) {
    LOG_WARN("failed to load inflight snapshot", K(ret));
  } else {
    while (OB_SUCC(ret) && !found && idx_ < keys_.count()) {
      const ObLSID &ls_id = keys_.at(idx_);
      ++idx_;
      if (try_resolve_state_(ls_id, out_state) && out_state.task_id_.is_valid()) {
        out_ls_id = ls_id;
        found = true;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
