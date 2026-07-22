/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OMT_OB_AI_GATEWAY_CIRCUIT_MANAGER_H_
#define OCEANBASE_OBSERVER_OMT_OB_AI_GATEWAY_CIRCUIT_MANAGER_H_

#include "share/ai_service/ob_ai_gateway_circuit_state.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/task/ob_timer.h"
#include "lib/allocator/ob_slice_alloc.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualAiGatewayEndpointStat;
} // namespace observer
namespace omt
{

class ObAiGatewayCircuitManager;

class ObAiGatewayCleanupTask : public common::ObTimerTask
{
public:
  ObAiGatewayCleanupTask() : mgr_(NULL) {}
  void init(ObAiGatewayCircuitManager *mgr) { mgr_ = mgr; }
  virtual void runTimerTask() override;
private:
  ObAiGatewayCircuitManager *mgr_;
};

// Tenant-level manager for per-gateway circuit breaker states.
//
// Concurrency: relies on ObHashMap's per-bucket read/write locks instead of a
// single global lock. The hot lookup path (get_or_create on an existing
// gateway) runs under a bucket read lock and so does not serialize with other
// gateways. The compound "lookup-or-insert" is made atomic via set_or_update
// under a bucket write lock. A separate stale_lock_ guards the stale list, and
// the cleanup timer is registered once via a lock-free CAS.
class ObAiGatewayCircuitManager
{
  friend class observer::ObAllVirtualAiGatewayEndpointStat;
public:
  static const int64_t GATEWAY_MAP_BUCKET_NUM = 16;
  static const int64_t CLEANUP_INTERVAL_US = 60 * 1000000L; // 60 seconds

  ObAiGatewayCircuitManager();
  ~ObAiGatewayCircuitManager() { destroy(); }

  int init();
  void stop();
  void wait();
  void destroy();

  // The returned state carries a caller-owned ref; caller MUST release it via
  // dec_ref_and_release() when done, so the state stays alive even if concurrent
  // cleanup removes it from the map.
  int get_or_create_gateway_state(uint64_t gateway_id,
                                  const common::ObString &endpoints_json,
                                  const common::ObString &circuit_breaker_json,
                                  int64_t schema_version,
                                  share::ObAiGatewayCircuitState *&state);

  int push_stale_gateway(uint64_t gateway_id);
  void drain_stale_gateways();

private:
  int register_cleanup_timer_();

  class DestroyGatewayStateFunc
  {
  public:
    int operator()(common::hash::HashMapPair<uint64_t, share::ObAiGatewayCircuitState *> &kv);
  };

  bool is_inited_;
  // Default LatchReadWriteDefendMode gives per-bucket read/write locks.
  common::hash::ObHashMap<uint64_t, share::ObAiGatewayCircuitState *> gateway_circuit_map_;
  common::ObSpinLock stale_lock_;
  common::ObSEArray<uint64_t, 4> stale_gateways_;
  ObAiGatewayCleanupTask cleanup_task_;
  bool timer_registered_;
  common::ObSliceAlloc gateway_state_pool_;

  DISALLOW_COPY_AND_ASSIGN(ObAiGatewayCircuitManager);
};

} // namespace omt
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_OMT_OB_AI_GATEWAY_CIRCUIT_MANAGER_H_
