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

#ifndef OCEANBASE_LOGSERVICE_OB_SERVER_LOG_BLOCK_RECLAIM_SERVICE_
#define OCEANBASE_LOGSERVICE_OB_SERVER_LOG_BLOCK_RECLAIM_SERVICE_

#include "lib/atomic/ob_atomic.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "palf/log_block_pool_interface.h"
#include "palf/tenant_log_block_reclaim_service.h"

namespace oceanbase
{
namespace omt
{
class ObMultiTenant;
}
namespace logservice
{

class ObServerLogBlockMgr;

// Server-level reclaim lifecycle and quota planning driven by tenant block GC threads.
class ObServerLogBlockReclaimService final : public palf::ILogBlockReclaimCoordinator
{
public:
  ObServerLogBlockReclaimService();
  // Bind the shared block pool and restore the lifecycle from durable metadata.
  int init(ObServerLogBlockMgr *block_mgr);
  // Clear the binding and all in-memory lifecycle state.
  void reset();
  // Refresh the global hard write-stop state and accumulate its transitions.
  void refresh_pool_write_stopped();
  // Return the durable cross-tenant reclaim marker represented in memory.
  bool is_cross_tenant_reclaim_enabled() const;
  // Advance lifecycle transitions and publish per-tenant reclaim quotas.
  int try_advance_reclaim(const int64_t now_us) override final;
  // Return the current server-level reclaim lifecycle state.
  palf::LogReclaimState get_reclaim_state() const override final;
  // Return whether current shared-pool usage exceeds the reclaim watermark.
  bool need_reclaim() const override final;
  // Return whether the shared pool has reached the hard write-stop watermark.
  bool is_pool_write_stopped() const override final;
  TO_STRING_KV(KP_(block_mgr),
               "cross_tenant_reclaim_enabled", is_cross_tenant_reclaim_enabled(),
               "reclaim_state", get_reclaim_state(),
               "pool_write_stopped", is_pool_write_stopped(),
               "last_periodic_check_ts", ATOMIC_LOAD(&last_periodic_check_ts_), K_(is_inited));

private:
  static const int64_t RUN_INTERVAL = 1 * 1000 * 1000;
  static const int64_t MAX_RECLAIM_BLOCKS_PER_TENANT_PER_ROUND = 1L << 16;
  static const int64_t MAX_RECLAIM_TENANT_COUNT = 1L << 30;
  typedef common::ObSEArray<palf::TenantReclaimSnapshot, 16> ReclaimSnapshotArray;
  typedef common::ObSEArray<int64_t, 16> ReclaimQuotaArray;
  class CollectSnapshotFunctor;
  int persist_cross_tenant_reclaim_enabled_(const bool enabled);
  int get_block_usage_(int64_t &used_blocks, int64_t &total_blocks) const;
  // Periodically report lifecycle, configuration, and write-stop status.
  void report_status_();
  // Return whether the server-wide periodic check is due.
  bool need_periodic_check_(const int64_t now_us) const;
  // Resolve config-driven transitions; draining completion is handled separately.
  static palf::LogReclaimState get_next_reclaim_state_(
      const palf::LogReclaimState current_state,
      const int64_t utilization_threshold);
  int advance_reclaim_state_guarded_by_lock_(const int64_t utilization_threshold);
  void run_periodic_check_guarded_by_lock_(const int64_t now_us);
  int plan_reclaim_(const int64_t now_us);
  // Clear durable shared-reclaim state after every tenant finishes draining.
  int try_finish_draining_(const ReclaimSnapshotArray &snapshots,
                           const bool scan_complete,
                           omt::ObMultiTenant &omt);
  // Compute and publish per-tenant reclaim quotas while reclaim is enabled.
  int plan_enabled_reclaim_(const ReclaimSnapshotArray &snapshots,
                            const int64_t now_us);
  // Return whether every active tenant finished its quota or made no progress for one interval.
  static bool can_start_next_reclaim_round_(const ReclaimSnapshotArray &snapshots,
                                            const int64_t now_us);
  static int allocate_reclaim_quota_(const ReclaimSnapshotArray &snapshots,
                                     const int64_t blocks_needed,
                                     ReclaimQuotaArray &quotas,
                                     int64_t &rr_cursor);
  // Split one stage target proportionally and rotate the rounding blocks.
  static int allocate_stage_quota_(const ReclaimQuotaArray &candidates,
                                   const int64_t blocks_to_allocate,
                                   ReclaimQuotaArray &quotas,
                                   int64_t &rr_cursor);
private:
  ObServerLogBlockMgr *block_mgr_;
  palf::LogReclaimState reclaim_state_;
  // Serialize the usage snapshot and hard write-stop publication.
  common::ObSpinLock pool_write_stop_refresh_lock_;
  bool pool_write_stopped_;
  palf::LogReclaimState last_reported_reclaim_state_;
  int64_t pool_write_stop_flip_count_;
  common::ObSpinLock reclaim_plan_lock_;
  int64_t last_periodic_check_ts_;
  int64_t reclaim_rr_cursor_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObServerLogBlockReclaimService);
};

} // namespace logservice
} // namespace oceanbase
#endif
