/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER_OMT
#include "ob_ai_gateway_circuit_manager.h"
#include "ob_multi_tenant.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace omt
{

const int64_t ObAiGatewayCircuitManager::CLEANUP_INTERVAL_US;

typedef common::hash::HashMapPair<uint64_t, share::ObAiGatewayCircuitState *> GatewayMapPair;

// inc_ref inside the bucket read lock closes the UAF window against drain's erase_if.
class GatewayLookupOp
{
public:
  GatewayLookupOp() : entry_(NULL) {}
  void operator()(GatewayMapPair &kv)
  {
    entry_ = kv.second;
    if (OB_NOT_NULL(entry_)) {
      entry_->inc_ref();
    }
  }
  share::ObAiGatewayCircuitState *entry_;
};

// inc_ref under write lock so drain cannot reach ref=0 before the caller takes ownership.
class GatewayInsertOp
{
public:
  int operator()(const GatewayMapPair &kv)
  {
    if (OB_NOT_NULL(kv.second)) {
      kv.second->inc_ref();
    }
    return OB_SUCCESS;
  }
};

// Lost-insert race: capture pre-existing entry and inc_ref under write lock (same UAF rationale).
class GatewayUpdateOp
{
public:
  GatewayUpdateOp() : existed_(false), entry_(NULL) {}
  void operator()(GatewayMapPair &kv)
  {
    entry_ = kv.second;
    existed_ = true;
    if (OB_NOT_NULL(entry_)) {
      entry_->inc_ref();
    }
  }
  bool existed_;
  share::ObAiGatewayCircuitState *entry_;
};

class GatewayErasePred
{
public:
  bool operator()(GatewayMapPair &kv)
  {
    UNUSED(kv);
    return true;
  }
};

ObAiGatewayCircuitManager::ObAiGatewayCircuitManager()
  : is_inited_(false),
    stale_lock_(common::ObLatchIds::LATCH_WAIT_QUEUE_LOCK),
    timer_registered_(false)
{
}

int ObAiGatewayCircuitManager::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAiGatewayCircuitManager already initialized", K(ret));
  } else {
    ObMemAttr map_attr(MTL_ID(), "AiGwMapBkt");
    ObMemAttr pool_attr(MTL_ID(), "AiGwCircuit");
    stale_gateways_.set_attr(ObMemAttr(MTL_ID(), "AiGwStale"));
    if (OB_FAIL(gateway_state_pool_.init(sizeof(share::ObAiGatewayCircuitState),
                                         OB_MALLOC_NORMAL_BLOCK_SIZE,
                                         default_blk_alloc,
                                         pool_attr))) {
      LOG_WARN("failed to init gateway_state_pool_", KR(ret));
    } else if (OB_FAIL(gateway_circuit_map_.create(GATEWAY_MAP_BUCKET_NUM, map_attr, map_attr))) {
      LOG_WARN("failed to create gateway circuit map", KR(ret));
    } else {
      cleanup_task_.init(this);
      is_inited_ = true;
    }
  }
  return ret;
}

void ObAiGatewayCircuitManager::stop()
{
  if (is_inited_ && ATOMIC_LOAD(&timer_registered_)) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), cleanup_task_);
  }
}

void ObAiGatewayCircuitManager::wait()
{
  if (is_inited_ && ATOMIC_LOAD(&timer_registered_)) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), cleanup_task_);
  }
}

void ObAiGatewayCircuitManager::destroy()
{
  if (is_inited_) {
    if (ATOMIC_LOAD(&timer_registered_)) {
      bool is_exist = true;
      if (OB_SUCCESS == TG_TASK_EXIST(MTL(omt::ObSharedTimer*)->get_tg_id(), cleanup_task_, is_exist)
          && is_exist) {
        TG_CANCEL_R(MTL(omt::ObSharedTimer*)->get_tg_id(), cleanup_task_);
      }
      ATOMIC_STORE(&timer_registered_, false);
    }

    {
      ObSpinLockGuard guard(stale_lock_);
      stale_gateways_.reset();
    }

    if (gateway_circuit_map_.created()) {
      DestroyGatewayStateFunc destroy_func;
      gateway_circuit_map_.foreach_refactored(destroy_func);
      gateway_circuit_map_.destroy();
    }

    gateway_state_pool_.destroy();
    is_inited_ = false;
  }
}

int ObAiGatewayCircuitManager::DestroyGatewayStateFunc::operator()(
    common::hash::HashMapPair<uint64_t, share::ObAiGatewayCircuitState *> &kv)
{
  int ret = OB_SUCCESS;
  share::ObAiGatewayCircuitState *state = kv.second;
  if (OB_NOT_NULL(state)) {
    const int64_t ref_count = ATOMIC_LOAD(&state->ref_count_);
    if (ref_count > 1) {
      LOG_ERROR("gateway circuit state still referenced at tenant destroy",
                K(ref_count), "gateway_id", kv.first);
    }
    share::ObAiGatewayCircuitState::dec_ref_and_release(state);
    kv.second = NULL;
  }
  return ret;
}

int ObAiGatewayCircuitManager::get_or_create_gateway_state(
    uint64_t gateway_id,
    const common::ObString &endpoints_json,
    const common::ObString &circuit_breaker_json,
    int64_t schema_version,
    share::ObAiGatewayCircuitState *&state)
{
  int ret = OB_SUCCESS;
  state = NULL;
  share::ObAiGatewayCircuitState *entry = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiGatewayCircuitManager not initialized", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == gateway_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid gateway_id", KR(ret), K(gateway_id));
  } else {
    share::ObAiGatewayStateRefGuard caller_guard;
    GatewayLookupOp lookup_op;
    ret = gateway_circuit_map_.read_atomic(gateway_id, lookup_op);
    if (OB_SUCC(ret)) {
      caller_guard.adopt(lookup_op.entry_);
      entry = lookup_op.entry_;
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      void *buf = gateway_state_pool_.alloc();
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate gateway circuit state", KR(ret), K(gateway_id));
      } else {
        share::ObAiGatewayCircuitState *new_entry =
            new (buf) share::ObAiGatewayCircuitState(&gateway_state_pool_);
        GatewayInsertOp insert_op;
        GatewayUpdateOp update_op;
        if (OB_FAIL(new_entry->init())) {
          LOG_WARN("failed to init gateway circuit state", KR(ret), K(gateway_id));
          new_entry->~ObAiGatewayCircuitState();
          gateway_state_pool_.free(buf);
        } else if (OB_FAIL(gateway_circuit_map_.set_or_update(gateway_id, new_entry, insert_op, update_op))) {
          LOG_WARN("failed to insert gateway circuit state into map", KR(ret), K(gateway_id));
          new_entry->~ObAiGatewayCircuitState();
          gateway_state_pool_.free(buf);
        } else if (update_op.existed_) {
          entry = update_op.entry_;
          caller_guard.adopt(entry);
          new_entry->~ObAiGatewayCircuitState();
          gateway_state_pool_.free(buf);
        } else {
          entry = new_entry;
          caller_guard.adopt(entry);
        }
      }
    } else {
      LOG_WARN("failed to read gateway state from map", KR(ret), K(gateway_id));
    }

    if (OB_SUCC(ret)) {
      int tmp_ret = register_cleanup_timer_();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to register cleanup timer, will retry later", K(tmp_ret));
      }
    }

    // Refreshes are monotonic: a stale schema version never overwrites newer cached data.
    if (OB_SUCC(ret) && OB_NOT_NULL(entry)) {
      if (OB_FAIL(entry->refresh_if_needed(endpoints_json, circuit_breaker_json, schema_version))) {
        LOG_WARN("failed to refresh gateway circuit state from schema",
                 KR(ret), K(gateway_id), K(schema_version));
      } else {
        state = caller_guard.release();
      }
    }
  }

  return ret;
}

int ObAiGatewayCircuitManager::push_stale_gateway(uint64_t gateway_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiGatewayCircuitManager not initialized", KR(ret));
  } else {
    ObSpinLockGuard guard(stale_lock_);
    if (OB_FAIL(stale_gateways_.push_back(gateway_id))) {
      LOG_WARN("failed to push stale gateway", KR(ret), K(gateway_id));
    }
  }
  return ret;
}

void ObAiGatewayCircuitManager::drain_stale_gateways()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ObSEArray<uint64_t, 4> to_clean;
    to_clean.set_attr(ObMemAttr(MTL_ID(), "AiGwStale"));
    {
      ObSpinLockGuard guard(stale_lock_);
      if (OB_FAIL(to_clean.assign(stale_gateways_))) {
        LOG_WARN("failed to snapshot stale gateways, retry next round", KR(ret));
      } else {
        stale_gateways_.reset();
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < to_clean.count(); i++) {
      const uint64_t gw_id = to_clean.at(i);
      share::ObAiGatewayCircuitState *erased = NULL;
      bool is_erased = false;
      GatewayErasePred pred;
      const int tmp_ret = gateway_circuit_map_.erase_if(gw_id, pred, is_erased, &erased);
      if (OB_SUCCESS != tmp_ret && OB_HASH_NOT_EXIST != tmp_ret) {
        LOG_WARN("failed to erase stale gateway from map", K(tmp_ret), K(gw_id));
      } else if (is_erased && OB_NOT_NULL(erased)) {
        if (1 == ATOMIC_LOAD(&erased->ref_count_)) {
          LOG_INFO("cleaned up stale gateway circuit state", K(gw_id));
        } else {
          LOG_INFO("stale gateway removed from map but deferred destroy (active refs)",
                   K(gw_id), "ref_count", ATOMIC_LOAD(&erased->ref_count_));
        }
        share::ObAiGatewayCircuitState::dec_ref_and_release(erased);
      }
    }
  }
}

int ObAiGatewayCircuitManager::register_cleanup_timer_()
{
  int ret = OB_SUCCESS;
  if (ATOMIC_BCAS(&timer_registered_, false, true)) {
    if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(),
                            cleanup_task_,
                            CLEANUP_INTERVAL_US,
                            true /*repeat*/))) {
      ATOMIC_STORE(&timer_registered_, false); // allow a later call to retry
      LOG_WARN("failed to schedule gateway cleanup task", KR(ret));
    } else {
      LOG_INFO("gateway cleanup timer registered", K(CLEANUP_INTERVAL_US));
    }
  }
  return ret;
}

void ObAiGatewayCleanupTask::runTimerTask()
{
  if (OB_NOT_NULL(mgr_)) {
    mgr_->drain_stale_gateways();
  }
}

} // namespace omt
} // namespace oceanbase
