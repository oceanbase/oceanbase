/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX PALF
#include "tenant_log_block_reclaim_service.h"
#include "common/ob_clock_generator.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_time_fmt.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/rc/ob_tenant_base.h"
#include "log_block_pool_interface.h"
#include "log_define.h"

namespace oceanbase
{
using namespace common;
namespace palf
{

const int64_t TenantLogBlockReclaimService::GC_STALL_GRACE_US;

int TenantReclaimSnapshot::init(const uint64_t tenant_id,
                                const int64_t retention_expired_blocks,
                                const int64_t retention_unexpired_blocks,
                                const int64_t last_reclaim_progress_ts,
                                const bool shared_reclaim_enabled)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || retention_expired_blocks < 0
      || retention_unexpired_blocks < 0
      || last_reclaim_progress_ts < 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid tenant reclaim snapshot", KR(ret), K(tenant_id),
             K(retention_expired_blocks), K(retention_unexpired_blocks),
             K(last_reclaim_progress_ts), K(shared_reclaim_enabled));
  } else {
    tenant_id_ = tenant_id;
    retention_expired_blocks_ = retention_expired_blocks;
    retention_unexpired_blocks_ = retention_unexpired_blocks;
    last_reclaim_progress_ts_ = last_reclaim_progress_ts;
    shared_reclaim_enabled_ = shared_reclaim_enabled;
  }
  return ret;
}

void TenantLogBlockReclaimService::report_reclaim_mode_transition_(
    const bool prev_shared,
    const bool curr_shared,
    const LogReclaimState server_state,
    const char *reason)
{
  const int64_t report_ts = ObClockGenerator::getRealClock();
  const char *happen_time =
      ObTime2Str::ob_timestamp_str_range<YEAR, USECOND>(report_ts);
  const uint64_t tenant_id = MTL_ID();
  const char *prev_mode = prev_shared ? "SHARED" : "LOCAL";
  const char *curr_mode = curr_shared ? "SHARED" : "LOCAL";
  const char *server_state_str = log_reclaim_state_to_string(server_state);
  PALF_LOG(INFO, "[SHARED_LOG_RECLAIM] TENANT_RECLAIM_MODE_TRANSITION",
           K(tenant_id), K(prev_mode), K(curr_mode), K(server_state_str), K(reason),
           K(happen_time));
  const int tmp_ret = SERVER_EVENT_ADD_WITH_RETRY(
      "LOG", "TENANT_RECLAIM_MODE_TRANSITION",
      "TENANT_ID", tenant_id,
      "PREV_MODE", prev_mode,
      "CURR_MODE", curr_mode,
      "SERVER_STATE", server_state_str,
      "REASON", reason,
      "HAPPEN_TIME", happen_time);
  if (OB_SUCCESS != tmp_ret) {
    PALF_LOG_RET(WARN, tmp_ret, "report tenant reclaim mode transition failed",
                 K(tenant_id), K(prev_mode), K(curr_mode), K(server_state_str), K(reason));
  }
}

void TenantLogBlockReclaimService::report_reclaim_suspend_transition_(
    const bool prev_suspended,
    const bool curr_suspended,
    const int64_t pending_blocks,
    const int64_t no_progress_us,
    const char *reason)
{
  const int64_t report_ts = ObClockGenerator::getRealClock();
  const char *happen_time =
      ObTime2Str::ob_timestamp_str_range<YEAR, USECOND>(report_ts);
  const uint64_t tenant_id = MTL_ID();
  PALF_LOG(INFO, "[SHARED_LOG_RECLAIM] TENANT_RECLAIM_SUSPEND_TRANSITION",
           K(tenant_id), K(prev_suspended), K(curr_suspended), K(pending_blocks),
           K(no_progress_us), K(reason), K(happen_time));
  // Keep the existing six fields; extra_info carries the task creation time.
  char extra_info[64] = {0};
  int tmp_ret = databuff_printf(extra_info, sizeof(extra_info), "HAPPEN_TIME=%s",
      happen_time);
  if (OB_SUCCESS == tmp_ret) {
    tmp_ret = SERVER_EVENT_ADD_WITH_RETRY(
        "LOG", "TENANT_RECLAIM_SUSPEND_TRANSITION",
        "TENANT_ID", tenant_id,
        "PREV_SUSPENDED", prev_suspended,
        "CURR_SUSPENDED", curr_suspended,
        "PENDING_BLOCKS", pending_blocks,
        "NO_PROGRESS_US", no_progress_us,
        "REASON", reason, extra_info);
  }
  if (OB_SUCCESS != tmp_ret) {
    PALF_LOG_RET(WARN, tmp_ret, "report tenant reclaim suspend transition failed",
                 K(tenant_id), K(prev_suspended), K(curr_suspended), K(pending_blocks),
                 K(no_progress_us), K(reason));
  }
}

TenantLogBlockReclaimService::TenantLogBlockReclaimService()
    : is_inited_(false),
      retention_expired_blocks_(0),
      retention_unexpired_blocks_(0),
      pending_blocks_(0),
      last_summary_ts_(OB_INVALID_TIMESTAMP),
      last_progress_ts_(0),
      shared_reclaim_enabled_(false),
      reclaim_suspended_(false),
      gc_stall_start_ts_(OB_INVALID_TIMESTAMP),
      lock_(common::ObLatchIds::PALF_ENV_LOCK)
{}

int TenantLogBlockReclaimService::init(const bool shared_reclaim_enabled)
{
  int ret = OB_SUCCESS;
  {
    ObSpinLockGuard guard(lock_);
    if (!is_valid_tenant_id(MTL_ID())) {
      ret = OB_INVALID_ARGUMENT;
    } else if (is_inited_) {
      ret = OB_INIT_TWICE;
    } else {
      shared_reclaim_enabled_ = shared_reclaim_enabled;
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    PALF_LOG(WARN, "init tenant log block reclaim service failed", KR(ret), K(MTL_ID()),
             K(shared_reclaim_enabled));
  }
  return ret;
}

void TenantLogBlockReclaimService::reset()
{
  ObSpinLockGuard guard(lock_);
  is_inited_ = false;
  reset_reclaim_snapshot_();
  reset_reclaim_quota_();
  shared_reclaim_enabled_ = false;
}

void TenantLogBlockReclaimService::reset_reclaim_snapshot_()
{
  retention_expired_blocks_ = 0;
  retention_unexpired_blocks_ = 0;
  last_summary_ts_ = OB_INVALID_TIMESTAMP;
}

void TenantLogBlockReclaimService::reset_reclaim_quota_()
{
  // Retire the shared quota and its no-progress timer, and clear the suspension.
  // The caller manages candidate counts and the shared/local mode separately.
  pending_blocks_ = 0;
  last_progress_ts_ = 0;
  reclaim_suspended_ = false;
  reset_gc_work_();
}

void TenantLogBlockReclaimService::reset_gc_work_()
{
  ATOMIC_STORE(&gc_stall_start_ts_, OB_INVALID_TIMESTAMP);
}

void TenantLogBlockReclaimService::sync_reclaim_mode(
    const LogReclaimState state,
    const bool server_need_reclaim,
    const bool local_reclaim_done)
{
  bool prev_shared = false;
  bool curr_shared = false;
  bool prev_suspended = false;
  bool curr_suspended = false;
  int64_t pending_blocks = 0;
  {
    ObSpinLockGuard guard(lock_);
    prev_shared = shared_reclaim_enabled_;
    prev_suspended = reclaim_suspended_;
    pending_blocks = pending_blocks_;
    if (LogReclaimState::ENABLED == state) {
      // Already shared and still under server pressure: preserve quota, progress time
      // and suspension across GC turns. A suspended tenant may finish its old quota.
      if (!shared_reclaim_enabled_) {
        // Enter shared mode with a fresh candidate snapshot and no previous quota.
        reset_reclaim_snapshot_();
        reset_reclaim_quota_();
        shared_reclaim_enabled_ = true;
      } else if (!server_need_reclaim) {
        // The server target is reached: cancel the remaining quota and allow a
        // suspended tenant to participate when shared reclaim is needed again.
        reset_reclaim_quota_();
      }
    } else if (LogReclaimState::DRAINING == state) {
      // GC now follows the local watermark, so shared quotas no longer apply.
      // Keep the tenant marked shared until its local watermark is reached; the
      // server uses this flag to wait for all tenants before becoming DISABLED.
      reset_reclaim_quota_();
      if (local_reclaim_done && shared_reclaim_enabled_) {
        reset_reclaim_snapshot_();
        shared_reclaim_enabled_ = false;
      }
    } else {
      // DISABLED uses local GC only; discard the shared snapshot and quota state.
      reset_reclaim_snapshot_();
      reset_reclaim_quota_();
      shared_reclaim_enabled_ = false;
    }
    curr_shared = shared_reclaim_enabled_;
    curr_suspended = reclaim_suspended_;
  }
  // Report actual mode/suspension changes outside the lock using the captured states.
  if (prev_shared != curr_shared) {
    report_reclaim_mode_transition_(
        prev_shared, curr_shared, state,
        curr_shared ? "SERVER_ENABLED"
                    : (LogReclaimState::DRAINING == state
                        ? "LOCAL_WATERMARK_REACHED" : "SERVER_DISABLED"));
  }
  if (prev_suspended != curr_suspended) {
    const char *reason = LogReclaimState::DRAINING == state
        ? "SERVER_DRAINING"
        : (LogReclaimState::ENABLED == state && !server_need_reclaim
            ? "SERVER_TARGET_REACHED" : "RECLAIM_MODE_CHANGED");
    report_reclaim_suspend_transition_(
        prev_suspended, curr_suspended, pending_blocks, 0, reason);
  }
}

int TenantLogBlockReclaimService::check_need_update_snapshot(bool &need_update)
{
  int ret = OB_SUCCESS;
  need_update = false;
  {
    ObSpinLockGuard guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
    } else if (shared_reclaim_enabled_
               && palf_reach_time_interval(SUMMARY_INTERVAL_US, last_summary_ts_)) {
      need_update = true;
    }
  }
  if (OB_FAIL(ret)) {
    PALF_LOG(WARN, "check tenant reclaim snapshot update failed",
             KR(ret), K(MTL_ID()));
  }
  return ret;
}

void TenantLogBlockReclaimService::update_reclaim_snapshot(
    const int64_t retention_expired_blocks,
    const int64_t retention_unexpired_blocks)
{
  bool updated = false;
  bool resumed = false;
  int64_t pending_blocks = 0;
  {
    ObSpinLockGuard guard(lock_);
    if (shared_reclaim_enabled_) {
      retention_expired_blocks_ = retention_expired_blocks;
      retention_unexpired_blocks_ = retention_unexpired_blocks;
      const int64_t candidate_blocks = retention_expired_blocks + retention_unexpired_blocks;
      if (pending_blocks_ > candidate_blocks) {
        pending_blocks_ = candidate_blocks;
      }
      if (0 == pending_blocks_) {
        last_progress_ts_ = 0;
        reset_gc_work_();
        if (reclaim_suspended_) {
          reclaim_suspended_ = false;
          resumed = true;
        }
      }
      pending_blocks = pending_blocks_;
      updated = true;
    }
  }
  if (updated) {
    PALF_LOG(INFO, "[SHARED_LOG_RECLAIM] update tenant reclaim snapshot", K(MTL_ID()),
             K(retention_expired_blocks), K(retention_unexpired_blocks), K(pending_blocks));
  }
  if (resumed) {
    report_reclaim_suspend_transition_(true, false, 0, 0, "NO_RECLAIMABLE_BLOCK");
  }
}

int TenantLogBlockReclaimService::get_reclaim_snapshot(
    const int64_t now_us,
    const bool server_need_reclaim,
    TenantReclaimSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  int64_t retention_expired_blocks = 0;
  int64_t retention_unexpired_blocks = 0;
  int64_t last_reclaim_progress_ts = 0;
  bool shared_reclaim_enabled = false;
  bool became_suspended = false;
  int64_t pending_blocks = 0;
  int64_t no_progress_us = 0;
  {
    ObSpinLockGuard guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
    } else if (now_us <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      if (server_need_reclaim && shared_reclaim_enabled_ && !reclaim_suspended_
          && pending_blocks_ > 0 && last_progress_ts_ > 0
          && now_us - last_progress_ts_ >= SUMMARY_INTERVAL_US) {
        // Skip this tenant in new plans, but keep its quota for the returning GC thread.
        pending_blocks = pending_blocks_;
        no_progress_us = now_us - last_progress_ts_;
        reclaim_suspended_ = true;
        became_suspended = true;
      }
      retention_expired_blocks = reclaim_suspended_ ? 0 : retention_expired_blocks_;
      retention_unexpired_blocks = reclaim_suspended_ ? 0 : retention_unexpired_blocks_;
      // Finishing a suspended quota must not hold up the next round for other tenants.
      last_reclaim_progress_ts = !reclaim_suspended_ && pending_blocks_ > 0
          ? last_progress_ts_ : 0;
      shared_reclaim_enabled = shared_reclaim_enabled_;
    }
  }
  if (OB_SUCCESS != ret) {
    PALF_LOG(WARN, "get tenant reclaim snapshot failed",
             KR(ret), K(MTL_ID()), K(now_us), K(server_need_reclaim));
  } else if (OB_FAIL(snapshot.init(
                 MTL_ID(), retention_expired_blocks, retention_unexpired_blocks,
                 last_reclaim_progress_ts,
                 shared_reclaim_enabled))) {
    PALF_LOG(WARN, "init tenant reclaim snapshot failed", KR(ret), K(MTL_ID()));
  }
  // Report the transition outside the lock, even if exporting the snapshot failed.
  if (became_suspended) {
    report_reclaim_suspend_transition_(
        false, true, pending_blocks, no_progress_us, "NO_PROGRESS");
  }
  return ret;
}

int TenantLogBlockReclaimService::apply_reclaim_quota(const int64_t reclaim_blocks)
{
  int ret = OB_SUCCESS;
  {
    ObSpinLockGuard guard(lock_);
    if (reclaim_blocks < 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (!is_inited_) {
      ret = OB_NOT_INIT;
    } else if (reclaim_suspended_) {
      // A new plan may publish zero for this skipped tenant. Preserve its old quota.
    } else {
      const int64_t quota = shared_reclaim_enabled_
          ? common::min(reclaim_blocks,
                     retention_expired_blocks_ + retention_unexpired_blocks_) : 0;
      if (quota > 0 && pending_blocks_ <= 0) {
        last_progress_ts_ = ObTimeUtility::current_monotonic_time();
        reset_gc_work_();
        pending_blocks_ = quota;
        begin_gc_work_(last_progress_ts_);
      } else if (0 == quota) {
        last_progress_ts_ = 0;
        reset_gc_work_();
        pending_blocks_ = quota;
      } else {
        pending_blocks_ = quota;
      }
    }
  }
  if (OB_FAIL(ret)) {
    PALF_LOG(WARN, "apply tenant reclaim quota failed", KR(ret), K(MTL_ID()), K(reclaim_blocks));
  }
  return ret;
}

bool TenantLogBlockReclaimService::can_recycle() const
{
  ObSpinLockGuard guard(lock_);
  return shared_reclaim_enabled_ && pending_blocks_ > 0;
}

bool TenantLogBlockReclaimService::is_shared_reclaim_enabled() const
{
  ObSpinLockGuard guard(lock_);
  return shared_reclaim_enabled_;
}

void TenantLogBlockReclaimService::begin_gc_work_(const int64_t now_us)
{
  if (!shared_reclaim_enabled_ || pending_blocks_ <= 0) {
    reset_gc_work_();
  } else if (now_us >= 0
      && OB_INVALID_TIMESTAMP == ATOMIC_LOAD(&gc_stall_start_ts_)) {
    ATOMIC_STORE(&gc_stall_start_ts_, now_us + GC_STALL_GRACE_US);
  }
}

void TenantLogBlockReclaimService::record_gc_progress()
{
  bool resumed = false;
  {
    ObSpinLockGuard guard(lock_);
    if (shared_reclaim_enabled_ && pending_blocks_ > 0) {
      --pending_blocks_;
      // Shared reclaim assigns the oldest candidates first. A physical return from
      // block GC or rebuild consumes one slot from the same cached inventory.
      if (retention_expired_blocks_ > 0) {
        --retention_expired_blocks_;
      } else if (retention_unexpired_blocks_ > 0) {
        --retention_unexpired_blocks_;
      }
      if (0 == pending_blocks_) {
        last_progress_ts_ = 0;
        reset_gc_work_();
        if (reclaim_suspended_) {
          // Rejoin planning with a fresh snapshot after finishing the old quota.
          reset_reclaim_snapshot_();
          reclaim_suspended_ = false;
          resumed = true;
        }
      } else {
        last_progress_ts_ = ObTimeUtility::current_monotonic_time();
        const int64_t gc_stall_start_ts = ATOMIC_LOAD(&gc_stall_start_ts_);
        if (OB_INVALID_TIMESTAMP != gc_stall_start_ts
            && last_progress_ts_ < gc_stall_start_ts) {
          ATOMIC_STORE(&gc_stall_start_ts_, last_progress_ts_ + GC_STALL_GRACE_US);
        }
      }
    }
  }
  if (resumed) {
    report_reclaim_suspend_transition_(true, false, 0, 0, "QUOTA_FINISHED");
  }
}

} // namespace palf
} // namespace oceanbase
