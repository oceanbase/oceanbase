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

#define USING_LOG_PREFIX CLOG
#include "ob_server_log_block_reclaim_service.h"
#include "common/ob_clock_generator.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_time_fmt.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_errno.h"
#include "ob_log_service.h"
#include "ob_server_log_block_mgr.h"
#include "palf/palf_env.h"

namespace oceanbase
{
using namespace common;
using namespace palf;
namespace logservice
{

ERRSIM_POINT_DEF(ERRSIM_SERVER_LOG_POOL_WRITE_STOP_THRESHOLD);

const int64_t ObServerLogBlockReclaimService::MAX_RECLAIM_BLOCKS_PER_TENANT_PER_ROUND;
const int64_t ObServerLogBlockReclaimService::MAX_RECLAIM_TENANT_COUNT;

ObServerLogBlockReclaimService::ObServerLogBlockReclaimService()
    : block_mgr_(NULL),
      reclaim_state_(LogReclaimState::INITIALIZING),
      pool_write_stop_refresh_lock_(),
      pool_write_stopped_(false),
      last_reported_reclaim_state_(LogReclaimState::INITIALIZING),
      pool_write_stop_flip_count_(0),
      reclaim_plan_lock_(),
      last_periodic_check_ts_(0),
      reclaim_rr_cursor_(0),
      is_inited_(false)
{}

int ObServerLogBlockReclaimService::init(ObServerLogBlockMgr *block_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "server log block reclaim service is initialized twice", KR(ret), KPC(this));
  } else if (OB_ISNULL(block_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), KP(block_mgr), KPC(this));
  } else {
    block_mgr_ = block_mgr;
    const bool cross_tenant_reclaim_enabled = block_mgr_->is_cross_tenant_reclaim_enabled();
    // A persisted enable marker survives restart. When the runtime switch is off,
    // resume in DRAINING so every tenant can clear its local reclaim mode first.
    const int64_t utilization_threshold = GCONF.server_log_disk_utilization_threshold;
    const LogReclaimState state = cross_tenant_reclaim_enabled
        ? (0 < utilization_threshold ? LogReclaimState::ENABLED : LogReclaimState::DRAINING)
        : LogReclaimState::DISABLED;
    ATOMIC_STORE64(&reclaim_state_, state);
    is_inited_ = true;
  }
  return ret;
}

void ObServerLogBlockReclaimService::reset()
{
  ObSpinLockGuard guard(pool_write_stop_refresh_lock_);
  block_mgr_ = NULL;
  ATOMIC_STORE64(&reclaim_state_, LogReclaimState::INITIALIZING);
  ATOMIC_STORE(&pool_write_stopped_, false);
  ATOMIC_STORE64(&last_reported_reclaim_state_, LogReclaimState::INITIALIZING);
  ATOMIC_STORE(&pool_write_stop_flip_count_, 0);
  ATOMIC_STORE(&last_periodic_check_ts_, 0);
  reclaim_rr_cursor_ = 0;
  is_inited_ = false;
}

int ObServerLogBlockReclaimService::persist_cross_tenant_reclaim_enabled_(
    const bool enabled)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(block_mgr_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "server log block reclaim service is not initialized",
             KR(ret), K(enabled), KPC(this));
  } else if (is_cross_tenant_reclaim_enabled() != enabled
             && OB_FAIL(block_mgr_->persist_cross_tenant_reclaim_enabled(enabled))) {
    CLOG_LOG(WARN, "persist shared reclaim mode failed", KR(ret), K(enabled), KPC(this));
  }
  return ret;
}

int ObServerLogBlockReclaimService::get_block_usage_(
    int64_t &used_blocks,
    int64_t &total_blocks) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(block_mgr_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "server log block reclaim service is not initialized", KR(ret), KPC(this));
  } else if (OB_FAIL(block_mgr_->get_block_usage(used_blocks, total_blocks))) {
    CLOG_LOG(WARN, "get shared log pool block usage failed", KR(ret), KPC(this));
  }
  return ret;
}

int ObServerLogBlockReclaimService::try_advance_reclaim(const int64_t now_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "server log block reclaim service is not initialized", KR(ret), KPC(this));
  } else if (now_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(now_us), KPC(this));
  } else if (need_periodic_check_(now_us)
             && OB_SUCCESS == reclaim_plan_lock_.trylock()) {
    // Every tenant block-GC timer enters here, so shared scans stay behind the
    // server-wide cadence check and a non-blocking lock attempt.
    const bool need_periodic_check = need_periodic_check_(now_us);
    if (need_periodic_check) {
      // Reserve this tick before shared work so later tenant timers skip it.
      ATOMIC_STORE(&last_periodic_check_ts_, now_us);
      const int64_t utilization_threshold = GCONF.server_log_disk_utilization_threshold;
      if (OB_FAIL(advance_reclaim_state_guarded_by_lock_(utilization_threshold))) {
        CLOG_LOG(WARN, "advance server log reclaim state failed", KR(ret), KPC(this));
      }
      run_periodic_check_guarded_by_lock_(now_us);
    }
    reclaim_plan_lock_.unlock();
    if (need_periodic_check) {
      // Event history enqueue stays outside the reclaim planner critical section.
      report_status_();
    }
  }
  return ret;
}

bool ObServerLogBlockReclaimService::need_periodic_check_(const int64_t now_us) const
{
  const int64_t last_check_ts = ATOMIC_LOAD(&last_periodic_check_ts_);
  return 0 == last_check_ts || now_us - last_check_ts >= RUN_INTERVAL;
}

LogReclaimState ObServerLogBlockReclaimService::get_next_reclaim_state_(
    const LogReclaimState current_state,
    const int64_t utilization_threshold)
{
  LogReclaimState next_state = current_state;
  if (0 < utilization_threshold) {
    if (LogReclaimState::DISABLED == current_state
        || LogReclaimState::DRAINING == current_state) {
      next_state = LogReclaimState::ENABLED;
    }
  } else if (LogReclaimState::ENABLED == current_state) {
    next_state = LogReclaimState::DRAINING;
  }
  return next_state;
}

int ObServerLogBlockReclaimService::advance_reclaim_state_guarded_by_lock_(
    const int64_t utilization_threshold)
{
  int ret = OB_SUCCESS;
  const LogReclaimState current_state = get_reclaim_state();
  const LogReclaimState next_state =
      get_next_reclaim_state_(current_state, utilization_threshold);
  // Enabling is durable before it becomes visible. Disabling stays in DRAINING
  // until every tenant has acknowledged the mode change and meta can be cleared.
  if (current_state != next_state) {
    if (LogReclaimState::DISABLED == current_state
        && OB_FAIL(persist_cross_tenant_reclaim_enabled_(true))) {
      CLOG_LOG(WARN, "persist shared reclaim enabled failed", KR(ret), KPC(this));
    } else {
      ATOMIC_STORE64(&reclaim_state_, next_state);
    }
  }
  return ret;
}

void ObServerLogBlockReclaimService::run_periodic_check_guarded_by_lock_(
    const int64_t now_us)
{
  // Keep the global hard write-stop active through draining.
  refresh_pool_write_stopped();
  const LogReclaimState state = get_reclaim_state();
  // ENABLED publishes quotas; DRAINING polls tenant acknowledgements for shutdown.
  if (LogReclaimState::ENABLED == state || LogReclaimState::DRAINING == state) {
    const int tmp_ret = plan_reclaim_(now_us);
    if (OB_SUCCESS != tmp_ret) {
      CLOG_LOG_RET(WARN, tmp_ret, "plan shared reclaim failed", KPC(this));
    }
  }
}

LogReclaimState ObServerLogBlockReclaimService::get_reclaim_state() const
{
  return ATOMIC_LOAD64(&reclaim_state_);
}

bool ObServerLogBlockReclaimService::need_reclaim() const
{
  int64_t reclaim_block_cnt = 0;
  const int64_t utilization_threshold = GCONF.server_log_disk_utilization_threshold;
  return LogReclaimState::ENABLED == get_reclaim_state()
         && OB_NOT_NULL(block_mgr_)
         && OB_SUCCESS == block_mgr_->get_reclaim_block_count(
                              utilization_threshold, reclaim_block_cnt)
         && 0 < reclaim_block_cnt;
}

bool ObServerLogBlockReclaimService::is_pool_write_stopped() const
{
  return is_shared_log_enabled() && ATOMIC_LOAD(&pool_write_stopped_);
}

bool ObServerLogBlockReclaimService::is_cross_tenant_reclaim_enabled() const
{
  return OB_NOT_NULL(block_mgr_) && block_mgr_->is_cross_tenant_reclaim_enabled();
}

void ObServerLogBlockReclaimService::report_status_()
{
  // Capture task creation time before enqueue; retries reuse the generated SQL.
  const int64_t report_ts = ObClockGenerator::getRealClock();
  int64_t used_blocks = 0;
  int64_t total_blocks = 0;
  const int64_t utilization_threshold = GCONF.server_log_disk_utilization_threshold;
  const int64_t configured_limit_threshold = GCONF.server_log_disk_utilization_limit_threshold;
  const int64_t errsim_value = ERRSIM_SERVER_LOG_POOL_WRITE_STOP_THRESHOLD;
  const int64_t errsim_limit_threshold = errsim_value < 0 ? -errsim_value : errsim_value;
  const int64_t limit_threshold =
      common::max(configured_limit_threshold, errsim_limit_threshold);
  const char *happen_time =
      ObTime2Str::ob_timestamp_str_range<YEAR, USECOND>(report_ts);
  const LogReclaimState state = get_reclaim_state();
  const LogReclaimState reported_state = ATOMIC_LOAD64(&last_reported_reclaim_state_);
  // Lifecycle reporting is edge-triggered; a failed event enqueue is retried next period.
  if (reported_state != state) {
    // Reason priority and normal transition:
    // RECOVER: first report after initialization, regardless of the recovered state.
    // CONFIG_DISABLE: the reclaim threshold became zero and state entered DRAINING.
    // DRAIN_COMPLETE: all tenants left shared reclaim and state entered DISABLED.
    // CONFIG_ENABLE: the remaining transition into ENABLED after a nonzero threshold.
    const char *reason = "CONFIG_ENABLE";
    if (LogReclaimState::INITIALIZING == reported_state) {
      reason = "RECOVER";
    } else if (LogReclaimState::DRAINING == state) {
      reason = "CONFIG_DISABLE";
    } else if (LogReclaimState::DISABLED == state) {
      reason = "DRAIN_COMPLETE";
    }
    const char *prev_state = palf::log_reclaim_state_to_string(reported_state);
    const char *curr_state = palf::log_reclaim_state_to_string(state);
    const bool meta_enabled = is_cross_tenant_reclaim_enabled();
    CLOG_LOG(INFO, "[SHARED_LOG_RECLAIM] SERVER_RECLAIM_STATE_TRANSITION",
             K(prev_state), K(curr_state), K(utilization_threshold), K(meta_enabled), K(reason),
             K(happen_time));
    const int tmp_ret = SERVER_EVENT_ADD_WITH_RETRY(
        "LOG", "SERVER_RECLAIM_STATE_TRANSITION",
        "PREV_STATE", prev_state,
        "CURR_STATE", curr_state,
        "THRESHOLD", utilization_threshold,
        "META_ENABLED", meta_enabled,
        "REASON", reason,
        "HAPPEN_TIME", happen_time);
    if (OB_SUCCESS == tmp_ret) {
      // Advance the reporting watermark only after the event is accepted.
      ATOMIC_STORE64(&last_reported_reclaim_state_, state);
    } else {
      CLOG_LOG_RET(WARN, tmp_ret, "report server reclaim state transition failed",
                   K(reported_state), K(state), K(reason));
    }
  }
  // The two config items are set independently, so diagnose an invalid pair periodically.
  if (0 < utilization_threshold && utilization_threshold >= limit_threshold) {
    CLOG_LOG_RET(ERROR, OB_INVALID_CONFIG,
                 "[SHARED_LOG_RECLAIM] invalid shared log reclaim thresholds",
                 K(utilization_threshold), K(limit_threshold));
  }
  // Log the final hard-stop state every period while stopped, but aggregate all
  // transitions since the previous report into one event-history record.
  if (OB_SUCCESS == get_block_usage_(used_blocks, total_blocks)) {
    // The exchange closes this reporting window; later flips stay in the next one.
    const int64_t flip_count = ATOMIC_TAS(&pool_write_stop_flip_count_, 0);
    const bool current_write_stopped = is_pool_write_stopped();
    if (current_write_stopped) {
      CLOG_LOG_RET(ERROR, OB_LOG_OUTOF_DISK_SPACE,
                   "[SHARED_LOG_RECLAIM] server log pool write is stopped",
                   K(used_blocks), K(total_blocks), K(limit_threshold));
    }
    if (0 < flip_count) {
      const char *reason = "PERIODIC";
      CLOG_LOG(INFO, "[SHARED_LOG_RECLAIM] SERVER_WRITE_STOP_TRANSITION",
               K(current_write_stopped), K(flip_count), K(used_blocks), K(total_blocks),
               K(limit_threshold), K(reason), K(happen_time));
      // All six name/value slots are used; keep their positions and append the time.
      char extra_info[64] = {0};
      int tmp_ret = databuff_printf(extra_info, sizeof(extra_info), "HAPPEN_TIME=%s",
          happen_time);
      if (OB_SUCCESS == tmp_ret) {
        tmp_ret = SERVER_EVENT_ADD_WITH_RETRY(
            "LOG", "SERVER_WRITE_STOP_TRANSITION",
            "WRITE_STOPPED", current_write_stopped,
            "FLIP_COUNT", flip_count,
            "USED_BLOCKS", used_blocks,
            "TOTAL_BLOCKS", total_blocks,
            "LIMIT_THRESHOLD", limit_threshold,
            "REASON", reason, extra_info);
      }
      if (OB_SUCCESS != tmp_ret) {
        // Preserve the count for the next periodic attempt without opening a hot retry loop.
        (void)ATOMIC_AAF(&pool_write_stop_flip_count_, flip_count);
        CLOG_LOG_RET(WARN, tmp_ret, "report server write stop transition failed",
                     K(current_write_stopped), K(flip_count), K(used_blocks), K(total_blocks),
                     K(limit_threshold));
      }
    }
  }
}

void ObServerLogBlockReclaimService::refresh_pool_write_stopped()
{
  ObSpinLockGuard guard(pool_write_stop_refresh_lock_);
  int64_t used_blocks = 0;
  int64_t total_blocks = 0;
  const int64_t configured_limit_threshold = GCONF.server_log_disk_utilization_limit_threshold;
  const int64_t errsim_value = ERRSIM_SERVER_LOG_POOL_WRITE_STOP_THRESHOLD;
  const int64_t errsim_limit_threshold = errsim_value < 0 ? -errsim_value : errsim_value;
  const int64_t limit_threshold =
      common::max(configured_limit_threshold, errsim_limit_threshold);
  if (OB_SUCCESS == get_block_usage_(used_blocks, total_blocks)) {
    const bool write_stopped = is_shared_log_enabled()
        && 0 < total_blocks && 0 < limit_threshold
        && used_blocks * 100 >= total_blocks * limit_threshold;
    if (0 < errsim_limit_threshold) {
      CLOG_LOG(INFO, "[SHARED_LOG_RECLAIM] use errsim server log pool write stop threshold",
               K(configured_limit_threshold), K(errsim_limit_threshold), K(limit_threshold),
               K(used_blocks), K(total_blocks));
    }
    const bool old_write_stopped = ATOMIC_LOAD(&pool_write_stopped_);
    const bool state_changed = old_write_stopped != write_stopped;
    if (state_changed) {
      ATOMIC_STORE(&pool_write_stopped_, write_stopped);
      (void)ATOMIC_AAF(&pool_write_stop_flip_count_, 1);
    }
  }
}

int ObServerLogBlockReclaimService::allocate_stage_quota_(
    const ReclaimQuotaArray &candidates,
    const int64_t blocks_to_allocate,
    ReclaimQuotaArray &quotas,
    int64_t &rr_cursor)
{
  int ret = OB_SUCCESS;
  int64_t total_candidates = 0;
  int64_t assigned_blocks = 0;
  quotas.reuse();
  if (blocks_to_allocate < 0 || rr_cursor < 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid stage quota arguments", KR(ret), K(blocks_to_allocate),
             K(rr_cursor));
  } else if (candidates.count() > MAX_RECLAIM_TENANT_COUNT) {
    ret = OB_SIZE_OVERFLOW;
    CLOG_LOG(WARN, "reclaim tenant count exceeds limit", KR(ret),
             "tenant_count", candidates.count(), K(MAX_RECLAIM_TENANT_COUNT));
  } else if (OB_FAIL(quotas.prepare_allocate(candidates.count()))) {
    CLOG_LOG(WARN, "prepare stage quotas failed", KR(ret), "count", candidates.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candidates.count(); ++i) {
    const int64_t candidate = candidates.at(i);
    if (candidate < 0) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid reclaim candidate count", KR(ret), K(i), K(candidates));
    } else if (candidate > MAX_RECLAIM_BLOCKS_PER_TENANT_PER_ROUND) {
      ret = OB_SIZE_OVERFLOW;
      CLOG_LOG(WARN, "tenant reclaim candidate count exceeds limit", KR(ret),
               K(i), K(candidate), K(MAX_RECLAIM_BLOCKS_PER_TENANT_PER_ROUND));
    } else {
      total_candidates += candidate;
    }
  }
  const int64_t target = OB_SUCC(ret) ? common::min(blocks_to_allocate, total_candidates) : 0;
  for (int64_t i = 0; OB_SUCC(ret) && target > 0 && i < candidates.count(); ++i) {
    const int64_t candidate = candidates.at(i);
    // candidate <= 2^16 and target <= 2^46, so the product is at most 2^62.
    const int64_t quota = candidate * target / total_candidates;
    quotas.at(i) = quota;
    assigned_blocks += quota;
  }
  int64_t remaining = target - assigned_blocks;
  const int64_t count = candidates.count();
  const int64_t start_idx = 0 < count ? rr_cursor % count : 0;
  // Proportional floors leave fewer than count rounding blocks, so one rotating
  // pass completes the target without sorting or repeatedly scanning remainders.
  for (int64_t offset = 0; OB_SUCC(ret) && remaining > 0 && offset < count; ++offset) {
    const int64_t idx = (start_idx + offset) % count;
    if (quotas.at(idx) < candidates.at(idx)) {
      ++quotas.at(idx);
      rr_cursor = (idx + 1) % count;
      --remaining;
    }
  }
  if (OB_SUCC(ret) && remaining > 0) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "failed to allocate all stage quota", KR(ret), K(remaining),
             K(target), K(total_candidates), K(rr_cursor));
  }
  return ret;
}

int ObServerLogBlockReclaimService::allocate_reclaim_quota_(
    const ReclaimSnapshotArray &snapshots,
    const int64_t blocks_needed,
    ReclaimQuotaArray &quotas,
    int64_t &rr_cursor)
{
  int ret = OB_SUCCESS;
  int64_t remaining = blocks_needed;
  ReclaimQuotaArray candidates;
  ReclaimQuotaArray stage_quotas;
  quotas.reuse();
  if (blocks_needed < 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid reclaim quota arguments", KR(ret), K(blocks_needed),
             "snapshot_count", snapshots.count(), K(rr_cursor));
  } else if (snapshots.count() > MAX_RECLAIM_TENANT_COUNT) {
    ret = OB_SIZE_OVERFLOW;
    CLOG_LOG(WARN, "reclaim tenant count exceeds limit", KR(ret),
             "tenant_count", snapshots.count(), K(MAX_RECLAIM_TENANT_COUNT));
  } else if (OB_FAIL(quotas.prepare_allocate(snapshots.count()))) {
    CLOG_LOG(WARN, "prepare reclaim quotas failed", KR(ret), "count", snapshots.count());
  }
  // Consume retention-expired candidates first, then use unexpired candidates
  // only for demand that remains.
  for (int64_t stage = 0; OB_SUCC(ret) && stage < 2 && remaining > 0; ++stage) {
    candidates.reuse();
    stage_quotas.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < snapshots.count(); ++i) {
      const TenantReclaimSnapshot &snapshot = snapshots.at(i);
      if (!snapshot.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        CLOG_LOG(WARN, "invalid tenant reclaim snapshot", KR(ret), K(i), K(snapshot));
      } else {
        int64_t candidate_blocks = 0;
        const int64_t remaining_tenant_quota =
            MAX_RECLAIM_BLOCKS_PER_TENANT_PER_ROUND - quotas.at(i);
        if (snapshot.is_shared_reclaim_enabled()
            && 0 == snapshot.get_last_reclaim_progress_ts()) {
          const int64_t reclaimable_blocks =
              0 == stage ? snapshot.get_retention_expired_blocks()
                         : snapshot.get_retention_unexpired_blocks();
          candidate_blocks = common::min(reclaimable_blocks, remaining_tenant_quota);
        }
        if (OB_FAIL(candidates.push_back(candidate_blocks))) {
          CLOG_LOG(WARN, "push reclaim candidate count failed", KR(ret), K(i), K(stage));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(allocate_stage_quota_(candidates, remaining, stage_quotas, rr_cursor))) {
        CLOG_LOG(WARN, "allocate stage reclaim quota failed", KR(ret), K(stage), K(remaining));
      } else {
        for (int64_t i = 0; i < stage_quotas.count(); ++i) {
          const int64_t tenant_quota = stage_quotas.at(i);
          quotas.at(i) += tenant_quota;
          remaining -= tenant_quota;
          if (1 == stage && tenant_quota > 0) {
            const uint64_t tenant_id = snapshots.at(i).get_tenant_id();
            CLOG_LOG(WARN, "[SHARED_LOG_RECLAIM] will reclaim retention-unexpired log blocks",
                     K(tenant_id), K(tenant_quota), K(blocks_needed), K(remaining));
          }
        }
      }
    }
  }
  return ret;
}

class ObServerLogBlockReclaimService::CollectSnapshotFunctor
{
public:
  CollectSnapshotFunctor(const int64_t now_us,
                         const bool server_need_reclaim,
                         ReclaimSnapshotArray &snapshots,
                         bool &scan_complete)
      : now_us_(now_us),
        server_need_reclaim_(server_need_reclaim),
        snapshots_(snapshots),
        scan_complete_(scan_complete)
  {}
  // Collect the current MTL tenant while the server scans all local tenants.
  int operator()() const
  {
    int ret = OB_SUCCESS;
    ObLogService *log_service = NULL;
    palf::PalfEnv *palf_env = NULL;
    palf::TenantReclaimSnapshot snapshot;
    if (OB_ISNULL(log_service = MTL(ObLogService*))
        || OB_ISNULL(palf_env = log_service->get_palf_env())) {
      scan_complete_ = false;
    } else if (OB_FAIL(palf_env->get_reclaim_snapshot(now_us_, server_need_reclaim_, snapshot))) {
      scan_complete_ = false;
      CLOG_LOG(WARN, "get tenant reclaim snapshot failed", KR(ret), K(MTL_ID()));
      ret = OB_SUCCESS;
    } else if (OB_FAIL(snapshots_.push_back(snapshot))) {
      CLOG_LOG(WARN, "push tenant reclaim snapshot failed", KR(ret), K(MTL_ID()));
    }
    return ret;
  }
private:
  const int64_t now_us_;
  const bool server_need_reclaim_;
  ReclaimSnapshotArray &snapshots_;
  bool &scan_complete_;
};

int ObServerLogBlockReclaimService::plan_reclaim_(const int64_t now_us)
{
  int ret = OB_SUCCESS;
  const LogReclaimState state = get_reclaim_state();
  const bool server_need_reclaim = LogReclaimState::ENABLED == state && need_reclaim();
  ReclaimSnapshotArray snapshots;
  bool scan_complete = true;
  omt::ObMultiTenant *omt = GCTX.omt_;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  // V1 plans with responsive tenants. A missing tenant is skipped for this round,
  // while DRAINING still requires a complete scan before clearing durable state.
  // Progress checks run here even when the inspected tenant's block GC is stalled.
  CollectSnapshotFunctor collect_snapshot(now_us, server_need_reclaim, snapshots, scan_complete);
  if (LogReclaimState::DISABLED == state) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "unexpected reclaim plan in disabled state", KR(ret), K(state), KPC(this));
  } else if (LogReclaimState::ENABLED == state && !server_need_reclaim) {
    // Tenant GC clears old quotas when the server target is reached; no scan is needed.
  } else if (OB_ISNULL(omt)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "multi tenant service is null", KR(ret), KP(omt));
  } else if (OB_FAIL(tenant_guard.switch_to(OB_SYS_TENANT_ID))) {
    CLOG_LOG(WARN, "switch to sys tenant for reclaim plan failed", KR(ret));
  } else if (OB_FAIL(omt->operate_in_each_tenant(collect_snapshot))) {
    CLOG_LOG(WARN, "collect tenant reclaim snapshots failed", KR(ret));
  } else if (LogReclaimState::DRAINING == state) {
    if (OB_FAIL(try_finish_draining_(snapshots, scan_complete, *omt))) {
      CLOG_LOG(WARN, "finish draining shared reclaim failed", KR(ret), KPC(this));
    }
  } else if (LogReclaimState::ENABLED == state) {
    if (OB_FAIL(plan_enabled_reclaim_(snapshots, now_us))) {
      CLOG_LOG(WARN, "plan enabled shared reclaim failed", KR(ret), KPC(this));
    }
  }
  return ret;
}

int ObServerLogBlockReclaimService::try_finish_draining_(
    const ReclaimSnapshotArray &snapshots,
    const bool scan_complete,
    omt::ObMultiTenant &omt)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 16> tenant_ids;
  if (OB_FAIL(omt.get_mtl_tenant_ids(tenant_ids))) {
    CLOG_LOG(WARN, "get mtl tenant ids failed", KR(ret));
  } else {
    // Before tenant replay or sync finishes, a scan covers only tenants loaded so far.
    bool all_tenants_use_local_reclaim =
        omt.has_synced() && scan_complete && snapshots.count() == tenant_ids.count();
    for (int64_t i = 0; all_tenants_use_local_reclaim && i < snapshots.count(); ++i) {
      all_tenants_use_local_reclaim = !snapshots.at(i).is_shared_reclaim_enabled();
    }
    // The durable marker is cleared only after every local tenant leaves shared reclaim.
    if (all_tenants_use_local_reclaim
        && LogReclaimState::DRAINING == get_reclaim_state()
        && 0 == GCONF.server_log_disk_utilization_threshold) {
      if (OB_FAIL(persist_cross_tenant_reclaim_enabled_(false))) {
        CLOG_LOG(WARN, "persist shared reclaim disabled failed", KR(ret), KPC(this));
      } else {
        ATOMIC_STORE64(&reclaim_state_, LogReclaimState::DISABLED);
      }
    }
  }
  return ret;
}

bool ObServerLogBlockReclaimService::can_start_next_reclaim_round_(
    const ReclaimSnapshotArray &snapshots,
    const int64_t now_us)
{
  // With a valid scan time, each shared tenant in this scan must have no pending
  // quota, be suspended from new plans, or have made no progress for at least one second.
  bool can_start = now_us > 0;
  for (int64_t i = 0; can_start && i < snapshots.count(); ++i) {
    const TenantReclaimSnapshot &snapshot = snapshots.at(i);
    const int64_t last_progress_ts = snapshot.get_last_reclaim_progress_ts();
    // Local tenants do not block. Shared tenants with no pending quota or with
    // planning suspended export a zero timestamp, so they do not block either.
    if (snapshot.is_shared_reclaim_enabled() && last_progress_ts > 0) {
      // The timestamp is the quota start or the latest successful recycle.
      // GC may advance after now_us was sampled; keep the old round in that case.
      can_start = now_us >= last_progress_ts
          && now_us - last_progress_ts >= RUN_INTERVAL;
    }
  }
  return can_start;
}

int ObServerLogBlockReclaimService::plan_enabled_reclaim_(
    const ReclaimSnapshotArray &snapshots,
    const int64_t now_us)
{
  int ret = OB_SUCCESS;
  const int64_t utilization_threshold = GCONF.server_log_disk_utilization_threshold;
  int64_t blocks_needed = 0;
  if (!can_start_next_reclaim_round_(snapshots, now_us)) {
    // Keep the current quotas while any tenant is still making progress.
  } else if (OB_ISNULL(block_mgr_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "server log block reclaim service is not initialized", KR(ret), KPC(this));
  } else if (OB_FAIL(block_mgr_->get_reclaim_block_count(
                 utilization_threshold, blocks_needed))) {
    CLOG_LOG(WARN, "get reclaim block count failed", KR(ret), KPC(this));
  } else {
    ReclaimQuotaArray quotas;
    if (0 == blocks_needed) {
      // Usage may fall below the target while tenant snapshots are being collected.
    } else if (OB_FAIL(allocate_reclaim_quota_(snapshots, blocks_needed, quotas, reclaim_rr_cursor_))) {
      CLOG_LOG(WARN, "allocate tenant reclaim quota failed", KR(ret), K(blocks_needed));
    } else {
      for (int64_t i = 0; i < snapshots.count(); ++i) {
        // Preserve a quota that was still in progress when its snapshot was collected,
        // and skip tenants that received no quota in this round.
        if (0 == snapshots.at(i).get_last_reclaim_progress_ts() && quotas.at(i) > 0) {
          const uint64_t tenant_id = snapshots.at(i).get_tenant_id();
          int tmp_ret = OB_SUCCESS;
          ObLogService *log_service = NULL;
          palf::PalfEnv *palf_env = NULL;
          MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
          // Any tenant's GC thread can run this server plan, so allow cross-tenant switches.
          // A tenant removed after the scan must not block quota delivery to other tenants.
          if (OB_TMP_FAIL(guard.switch_to(tenant_id, /* need_check_allow */ false))) {
            CLOG_LOG(WARN, "switch to tenant for reclaim quota failed", KR(tmp_ret), K(tenant_id));
          } else if (OB_ISNULL(log_service = MTL(ObLogService*))
                     || OB_ISNULL(palf_env = log_service->get_palf_env())) {
            tmp_ret = OB_ERR_UNEXPECTED;
            CLOG_LOG(WARN, "tenant palf env is null", KR(tmp_ret), K(tenant_id));
          } else if (OB_TMP_FAIL(palf_env->apply_reclaim_quota(quotas.at(i)))) {
            CLOG_LOG(WARN, "apply tenant reclaim quota failed", KR(tmp_ret), K(tenant_id),
                     K(quotas.at(i)));
          }
        }
      }
    }
  }
  return ret;
}

} // namespace logservice
} // namespace oceanbase
