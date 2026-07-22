/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "share/ai_service/ob_ai_gateway_circuit_state.h"
#include "lib/string/ob_string.h"
#include "share/rc/ob_tenant_base.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{

int ObAiGatewayCircuitState::init()
{
  int ret = OB_SUCCESS;
  const int64_t ep_page_size = OB_MALLOC_NORMAL_BLOCK_SIZE;
  ObMemAttr attr(MTL_ID(), "AiGwEp");
  cfg_arena_a_.set_attr(ObMemAttr(MTL_ID(), "AiGwCfg"));
  cfg_arena_b_.set_attr(ObMemAttr(MTL_ID(), "AiGwCfg"));
  cfg_active_ = &cfg_arena_a_;
  endpoints_.set_attr(ObMemAttr(MTL_ID(), "AiGwEp"));
  endpoint_states_.set_attr(ObMemAttr(MTL_ID(), "AiGwEp"));
  if (OB_FAIL(ep_alloc_.init(NULL /*use inner malloc*/, ep_page_size, attr, 0 /*init_size*/, 0 /*idle_size*/))) {
    LOG_WARN("failed to init ep_alloc_", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObAiGatewayCircuitState::refresh_from_schema(const common::ObString &endpoints_json,
                                                 const common::ObString &circuit_breaker_json,
                                                 int64_t schema_version)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_arena(ObMemAttr(MTL_ID(), "AiGwTmp"));
  common::ObSEArray<ObAiGatewayEndpoint, 4> new_endpoints;
  ObAiCircuitBreakerParams new_params;
  common::ObSEArray<ObAiEndpointCircuitState *, 4> new_states;        // final order, reused + new
  common::ObSEArray<ObAiEndpointCircuitState *, 4> allocated_states;  // allocated here, freed on rollback
  common::ObSEArray<ObAiGatewayEndpoint, 4> staged_endpoints;
  common::ObArenaAllocator *staging_arena =
      (&cfg_arena_a_ == cfg_active_) ? &cfg_arena_b_ : &cfg_arena_a_;
  const ObMemAttr tmp_attr(MTL_ID(), "AiGwTmp");
  new_endpoints.set_attr(tmp_attr);
  new_states.set_attr(tmp_attr);
  allocated_states.set_attr(tmp_attr);
  staged_endpoints.set_attr(tmp_attr);
  staging_arena->reuse();

  if (OB_FAIL(parse_gateway_endpoints_json(tmp_arena, endpoints_json, new_endpoints))) {
    LOG_WARN("failed to parse gateway endpoints json", K(ret));
  } else if (new_endpoints.count() > ObAiCircuitBreakerParams::MAX_ENDPOINTS_PER_GATEWAY) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too many endpoints in gateway",
             K(ret), "count", new_endpoints.count(),
             "max", ObAiCircuitBreakerParams::MAX_ENDPOINTS_PER_GATEWAY);
  } else if (OB_FAIL(parse_gateway_circuit_breaker_json(tmp_arena, circuit_breaker_json, new_params))) {
    LOG_WARN("failed to parse circuit breaker json", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < new_endpoints.count(); i++) {
    const common::ObString &new_name = new_endpoints.at(i).endpoint_name_;
    const common::ObString &new_model = new_endpoints.at(i).model_;
    ObAiEndpointCircuitState *reused_state = NULL;
    for (int64_t j = 0; OB_ISNULL(reused_state) && j < endpoints_.count(); j++) {
      if (endpoints_.at(j).endpoint_name_ == new_name
          && endpoints_.at(j).model_ == new_model) {
        reused_state = endpoint_states_.at(j);
      }
    }
    if (OB_NOT_NULL(reused_state)) {
      const int64_t old_window_size = reused_state->get_sliding_window().get_window_size();
      if (old_window_size != new_params.window_size_seconds_) {
        reused_state->reset_state();
        if (OB_FAIL(reused_state->rebuild_sliding_window(
                &ep_alloc_, new_params.window_size_seconds_))) {
          LOG_WARN("failed to rebuild sliding window after state reset",
                   K(ret), K(new_name),
                   K(old_window_size), K(new_params.window_size_seconds_));
        } else {
          LOG_INFO("endpoint state reset due to window_size_seconds change",
                   K(new_name),
                   K(old_window_size), K(new_params.window_size_seconds_));
        }
      }
      if (OB_SUCC(ret)) {
        // PL dedup keeps endpoint names unique; a duplicate would push the same
        // state twice and double-free it later, so reject it as a broken invariant.
        bool already_used = false;
        for (int64_t k = 0; !already_used && k < new_states.count(); k++) {
          already_used = (new_states.at(k) == reused_state);
        }
        if (already_used) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("duplicate endpoint reuses the same circuit state",
                    K(ret), K(new_name), K(new_model));
        } else if (OB_FAIL(new_states.push_back(reused_state))) {
          LOG_WARN("failed to push back reused state", K(ret));
        }
      }
    } else {
      void *buf = ep_alloc_.alloc(sizeof(ObAiEndpointCircuitState));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate endpoint circuit state", K(ret));
      } else {
        ObAiEndpointCircuitState *new_state = new (buf) ObAiEndpointCircuitState();
        if (OB_FAIL(new_state->init(&ep_alloc_, new_params.window_size_seconds_))) {
          LOG_WARN("failed to init endpoint circuit state", K(ret),
                   K(new_params.window_size_seconds_));
          new_state->~ObAiEndpointCircuitState();
          ep_alloc_.free(buf);
        } else if (OB_FAIL(allocated_states.push_back(new_state))) {
          LOG_WARN("failed to track allocated state", K(ret));
          new_state->destroy();
          ep_alloc_.free(buf);
        } else if (OB_FAIL(new_states.push_back(new_state))) {
          LOG_WARN("failed to push back new state", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < new_endpoints.count(); i++) {
      ObAiGatewayEndpoint ep;
      if (OB_FAIL(ob_write_string(*staging_arena, new_endpoints.at(i).endpoint_name_, ep.endpoint_name_))) {
        LOG_WARN("failed to copy endpoint_name", K(ret));
      } else if (OB_FAIL(ob_write_string(*staging_arena, new_endpoints.at(i).model_, ep.model_))) {
        LOG_WARN("failed to copy model", K(ret));
      } else if (OB_FAIL(ob_write_string(*staging_arena, new_endpoints.at(i).provider_, ep.provider_))) {
        LOG_WARN("failed to copy provider", K(ret));
      } else if (OB_FAIL(ob_write_string(*staging_arena, new_endpoints.at(i).model_name_, ep.model_name_))) {
        LOG_WARN("failed to copy model_name", K(ret));
      } else {
        ep.weight_ = new_endpoints.at(i).weight_;
        if (OB_FAIL(staged_endpoints.push_back(ep))) {
          LOG_WARN("failed to push staged endpoint", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(endpoints_.reserve(staged_endpoints.count()))) {
      LOG_WARN("failed to reserve endpoints", K(ret), "count", staged_endpoints.count());
    } else if (OB_FAIL(endpoint_states_.reserve(new_states.count()))) {
      LOG_WARN("failed to reserve endpoint states", K(ret), "count", new_states.count());
    } else {
      for (int64_t j = 0; j < endpoint_states_.count(); j++) {
        ObAiEndpointCircuitState *old_state = endpoint_states_.at(j);
        bool kept = false;
        for (int64_t k = 0; !kept && k < new_states.count(); k++) {
          kept = (new_states.at(k) == old_state);
        }
        if (!kept && OB_NOT_NULL(old_state)) {
          old_state->~ObAiEndpointCircuitState();
          ep_alloc_.free(old_state);
        }
      }
      endpoints_.reuse();
      endpoint_states_.reuse();
      for (int64_t i = 0; OB_SUCC(ret) && i < staged_endpoints.count(); i++) {
        if (OB_FAIL(endpoints_.push_back(staged_endpoints.at(i)))) {
          LOG_ERROR("push back must not fail after reserve", K(ret));
        } else if (OB_FAIL(endpoint_states_.push_back(new_states.at(i)))) {
          LOG_ERROR("push back must not fail after reserve", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        cfg_active_ = staging_arena;
        cb_params_ = new_params;
        cached_schema_version_ = schema_version;
        LOG_INFO("gateway circuit state refreshed from schema",
                 K(schema_version), "endpoint_count", endpoints_.count(), K_(cb_params));
      }
    }
  }

  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < allocated_states.count(); i++) {
      allocated_states.at(i)->~ObAiEndpointCircuitState();
      ep_alloc_.free(allocated_states.at(i));
    }
  }

  return ret;
}

int ObAiGatewayCircuitState::refresh_if_needed(const common::ObString &endpoints_json,
                                               const common::ObString &circuit_breaker_json,
                                               int64_t schema_version)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gateway circuit entry was concurrently erased between lookup and lock", K(ret));
  } else if (cached_schema_version_ < schema_version) {
    if (OB_FAIL(refresh_from_schema(endpoints_json, circuit_breaker_json, schema_version))) {
      LOG_WARN("failed to refresh gateway circuit state from schema", K(ret), K(schema_version));
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
