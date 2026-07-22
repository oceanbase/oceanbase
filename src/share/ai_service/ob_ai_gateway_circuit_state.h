/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_AI_GATEWAY_CIRCUIT_STATE_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_AI_GATEWAY_CIRCUIT_STATE_H_

#include "lib/ob_define.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/allocator/ob_slice_alloc.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log.h"
#include "share/ai_service/ob_ai_endpoint_circuit_state.h"
#include "share/ai_service/ob_ai_service_struct.h"

#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX SHARE
#endif

namespace oceanbase
{
namespace share
{

// Per-gateway circuit breaker state. Guard all members with lock_ except the
// atomic ref_count_, which drives the ref-counted lifetime.
struct ObAiGatewayCircuitState
{
  common::ObSpinLock lock_;
  volatile int64_t ref_count_;
  common::ObSEArray<ObAiGatewayEndpoint, 4> endpoints_;
  // Indexed parallel to endpoints_.
  common::ObSEArray<ObAiEndpointCircuitState *, 4> endpoint_states_;
  ObAiCircuitBreakerParams cb_params_;
  int64_t cached_schema_version_;
  common::ObArenaAllocator cfg_arena_a_;
  common::ObArenaAllocator cfg_arena_b_;
  common::ObArenaAllocator *cfg_active_;
  // FIFO (not arena) so removed endpoints free immediately; idle_size=0 avoids
  // retaining 256 KB of empty pages.
  common::ObFIFOAllocator ep_alloc_;
  // NULL for stack-allocated unittest instances.
  common::ObSliceAlloc *pool_;
  bool is_inited_;

  explicit ObAiGatewayCircuitState(common::ObSliceAlloc *pool = NULL)
    : lock_(common::ObLatchIds::LATCH_WAIT_QUEUE_LOCK),
      ref_count_(1),
      cached_schema_version_(OB_INVALID_VERSION),
      cfg_arena_a_("AiGwCfg"),
      cfg_arena_b_("AiGwCfg"),
      cfg_active_(&cfg_arena_a_),
      pool_(pool),
      is_inited_(false)
  {}

  ~ObAiGatewayCircuitState() { destroy(); }

  void inc_ref() { ATOMIC_INC(&ref_count_); }

  int64_t dec_ref() { return ATOMIC_SAF(&ref_count_, 1); }

  static void dec_ref_and_release(ObAiGatewayCircuitState *s)
  {
    if (OB_NOT_NULL(s) && 0 == s->dec_ref()) {
      common::ObSliceAlloc *p = s->pool_;
      s->~ObAiGatewayCircuitState();
      if (OB_NOT_NULL(p)) {
        p->free(s);
      }
    }
  }

  int init();

  void destroy()
  {
    for (int64_t i = 0; i < endpoint_states_.count(); i++) {
      if (OB_NOT_NULL(endpoint_states_.at(i))) {
        endpoint_states_.at(i)->~ObAiEndpointCircuitState();
        ep_alloc_.free(endpoint_states_.at(i));
      }
    }
    endpoint_states_.reset();
    endpoints_.reset();
    cached_schema_version_ = OB_INVALID_VERSION;
    is_inited_ = false;
  }

  // Must be called under gateway spinlock.
  int refresh_from_schema(const common::ObString &endpoints_json,
                          const common::ObString &circuit_breaker_json,
                          int64_t schema_version);

  // Acquires lock_ internally; returns OB_ERR_UNEXPECTED if already destroyed.
  int refresh_if_needed(const common::ObString &endpoints_json,
                        const common::ObString &circuit_breaker_json,
                        int64_t schema_version);

  TO_STRING_KV(K_(cached_schema_version), K_(is_inited),
               "endpoint_count", endpoints_.count());
};

// RAII ref guard for ObAiGatewayCircuitState.
class ObAiGatewayStateRefGuard
{
public:
  ObAiGatewayStateRefGuard() : entry_(NULL) {}
  ~ObAiGatewayStateRefGuard()
  {
    if (OB_NOT_NULL(entry_)) {
      ObAiGatewayCircuitState::dec_ref_and_release(entry_);
      entry_ = NULL;
    }
  }
  void set(ObAiGatewayCircuitState *entry)
  {
    entry_ = entry;
    entry_->inc_ref();
  }
  // Takes over a ref the caller already inc_ref'd (e.g. inside a read_atomic
  // callback); does NOT inc_ref again.
  void adopt(ObAiGatewayCircuitState *entry)
  {
    entry_ = entry;
  }
  ObAiGatewayCircuitState *get() { return entry_; }
  ObAiGatewayCircuitState *release()
  {
    ObAiGatewayCircuitState *p = entry_;
    entry_ = NULL;
    return p;
  }
private:
  ObAiGatewayCircuitState *entry_;
  DISALLOW_COPY_AND_ASSIGN(ObAiGatewayStateRefGuard);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_AI_GATEWAY_CIRCUIT_STATE_H_
