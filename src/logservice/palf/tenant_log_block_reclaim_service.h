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

#ifndef OCEANBASE_LOGSERVICE_TENANT_LOG_BLOCK_RECLAIM_SERVICE_
#define OCEANBASE_LOGSERVICE_TENANT_LOG_BLOCK_RECLAIM_SERVICE_

#include <stdint.h>
#include "lib/atomic/ob_atomic.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace palf
{

enum class LogReclaimState : int64_t;

// Per-tenant reclaim candidates consumed by the server-level planner.
class TenantReclaimSnapshot
{
public:
  TenantReclaimSnapshot()
      : tenant_id_(common::OB_INVALID_TENANT_ID), retention_expired_blocks_(0),
        retention_unexpired_blocks_(0),
        last_reclaim_progress_ts_(0),
        shared_reclaim_enabled_(false)
  {}
  // Initialize a complete per-tenant reclaim snapshot.
  int init(const uint64_t tenant_id,
           const int64_t retention_expired_blocks,
           const int64_t retention_unexpired_blocks,
           const int64_t last_reclaim_progress_ts,
           const bool shared_reclaim_enabled);
  // Return whether the tenant identity, candidate counts, and progress time are valid.
  bool is_valid() const
  {
    return common::is_valid_tenant_id(tenant_id_)
        && 0 <= retention_expired_blocks_
        && 0 <= retention_unexpired_blocks_
        && 0 <= last_reclaim_progress_ts_;
  }
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_retention_expired_blocks() const { return retention_expired_blocks_; }
  int64_t get_retention_unexpired_blocks() const { return retention_unexpired_blocks_; }
  int64_t get_last_reclaim_progress_ts() const { return last_reclaim_progress_ts_; }
  bool is_shared_reclaim_enabled() const { return shared_reclaim_enabled_; }
  TO_STRING_KV(K_(tenant_id), K_(retention_expired_blocks), K_(retention_unexpired_blocks),
               K_(last_reclaim_progress_ts), K_(shared_reclaim_enabled));
private:
  uint64_t tenant_id_;
  // Blocks reclaimable without violating log_retention_time.
  int64_t retention_expired_blocks_;
  // Blocks reclaimable only by relaxing log_retention_time.
  int64_t retention_unexpired_blocks_;
  // Zero means the previous quota is complete; otherwise this is its latest progress time.
  int64_t last_reclaim_progress_ts_;
  // Whether this tenant currently follows server-assigned reclaim quotas.
  bool shared_reclaim_enabled_;
};

// Tenant-local reclaim state accessed in the owning tenant's MTL context.
class TenantLogBlockReclaimService
{
public:
  TenantLogBlockReclaimService();
  // Initialize the recovered shared-reclaim mode.
  int init(const bool shared_reclaim_enabled);
  void reset();
  // Follow the server lifecycle and finish draining at the local watermark.
  void sync_reclaim_mode(const LogReclaimState state,
                         const bool server_need_reclaim,
                         const bool local_reclaim_done);
  // Reserve at most one candidate snapshot refresh per second.
  int check_need_update_snapshot(bool &need_update);
  // Publish candidate counts collected by PalfEnvImpl outside the service lock.
  void update_reclaim_snapshot(const int64_t retention_expired_blocks,
                               const int64_t retention_unexpired_blocks);
  // Export candidates and mode; suspend new quotas after one second without progress
  // only under server reclaim pressure. now_us is the server scan's monotonic timestamp.
  int get_reclaim_snapshot(const int64_t now_us,
                           const bool server_need_reclaim,
                           TenantReclaimSnapshot &snapshot);
  // Set the pending quota, capped by candidate counts; preserve a suspended old quota.
  int apply_reclaim_quota(const int64_t reclaim_blocks);
  // Return whether the current block GC turn may recycle one block.
  bool can_recycle() const;
  // Return the tenant's shared-reclaim lifecycle marker, including unfinished draining.
  bool is_shared_reclaim_enabled() const;
  // Each physical return consumes one shared quota and cached candidate; zero ends the round.
  void record_gc_progress();
  // Invalid while idle; a future timestamp marks the end of the progress grace interval.
  int64_t get_gc_stall_start_ts() const
  {
    return ATOMIC_LOAD(&gc_stall_start_ts_);
  }
  TO_STRING_KV(K_(is_inited), K_(retention_expired_blocks),
               K_(retention_unexpired_blocks), K_(pending_blocks),
               K_(last_summary_ts), K_(last_progress_ts),
               K_(shared_reclaim_enabled), K_(reclaim_suspended),
               K_(gc_stall_start_ts));
private:
  static void report_reclaim_mode_transition_(const bool prev_shared,
                                              const bool curr_shared,
                                              const LogReclaimState server_state,
                                              const char *reason);
  static void report_reclaim_suspend_transition_(const bool prev_suspended,
                                                 const bool curr_suspended,
                                                 const int64_t pending_blocks,
                                                 const int64_t no_progress_us,
                                                 const char *reason);
  // Reset candidate counts and refresh timing; caller holds lock_.
  void reset_reclaim_snapshot_();
  // Reset the pending quota, progress timer and suspension; caller holds lock_.
  void reset_reclaim_quota_();
  // Reset GC-stall tracking; caller holds lock_.
  void reset_gc_work_();
  // Start the GC-stall timer; caller holds lock_.
  void begin_gc_work_(const int64_t now_us);
private:
  // Snapshot refresh interval and pending-quota no-progress timeout, in microseconds.
  static const int64_t SUMMARY_INTERVAL_US = 1 * 1000 * 1000;
  static const int64_t GC_STALL_GRACE_US = 1 * 1000 * 1000;
  // Set by init() and cleared by reset().
  bool is_inited_;
  // Latest successful snapshot counts, reduced by each physical return under shared reclaim.
  // Candidates past log_retention_time are allocated first.
  int64_t retention_expired_blocks_;
  // Remaining candidates may be reclaimed early if expired blocks cannot meet the server target.
  int64_t retention_unexpired_blocks_;
  // Remaining server-assigned quota; each physical return under shared reclaim consumes one block.
  int64_t pending_blocks_;
  // Last snapshot-refresh reservation from ObClockGenerator, in microseconds.
  // OB_INVALID_TIMESTAMP allows an immediate refresh.
  int64_t last_summary_ts_;
  // No-progress baseline in monotonic microseconds: quota start or latest physical return.
  // Checked only while pending_blocks_ > 0; reset_reclaim_quota_ clears it.
  int64_t last_progress_ts_;
  // Stays true during DRAINING until this tenant reaches its local reclaim watermark.
  bool shared_reclaim_enabled_;
  // Skip new plans after one second without progress, while GC keeps its old quota.
  // Cleared on quota completion, server target reached, or reclaim mode change.
  bool reclaim_suspended_;
  // Published atomically because LogLoop reads it without taking lock_.
  int64_t gc_stall_start_ts_;
  // Guards state shared by this tenant's block GC and cross-tenant server scans.
  mutable common::ObSpinLock lock_;
  DISALLOW_COPY_AND_ASSIGN(TenantLogBlockReclaimService);
};

} // namespace palf
} // namespace oceanbase
#endif
