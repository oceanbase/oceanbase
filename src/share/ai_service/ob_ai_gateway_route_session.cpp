/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "share/ai_service/ob_ai_gateway_route_session.h"
#include "lib/random/ob_random.h"
#include "share/rc/ob_tenant_base.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{

ObAiGatewayRouteSession::ObAiGatewayRouteSession()
    : gw_state_(NULL),
      tried_mask_(0),
      current_idx_(-1),
      is_inited_(false),
      outcome_pending_(false)
{
  arena_.set_attr(common::ObMemAttr(MTL_ID(), "AiGwRoute"));
}

ObAiGatewayRouteSession::~ObAiGatewayRouteSession()
{
  reset();
}

void ObAiGatewayRouteSession::reset()
{
  if (outcome_pending_ && OB_NOT_NULL(gw_state_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "endpoint outcome not reported, auto-recording as failure (code defect)",
                  K_(current_idx));
    (void)on_failure();  // best-effort, suppress return value
  }
  if (OB_NOT_NULL(gw_state_)) {
    ObAiGatewayCircuitState::dec_ref_and_release(gw_state_);
    gw_state_ = NULL;
  }
  arena_.reuse();
  selected_endpoint_name_.reset();
  selected_model_.reset();
  tried_mask_ = 0;
  current_idx_ = -1;
  outcome_pending_ = false;
  is_inited_ = false;
}

void ObAiGatewayRouteSession::cancel_pending()
{
  outcome_pending_ = false;
  current_idx_ = -1;
}

int ObAiGatewayRouteSession::init(ObAiGatewayCircuitState *gw_state)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("route session already initialized", K(ret));
  } else if (OB_ISNULL(gw_state)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("gw_state is null", K(ret));
  } else {
    gw_state_ = gw_state;
    gw_state_->inc_ref();  // session holds a reference
    tried_mask_ = 0;
    current_idx_ = -1;
    outcome_pending_ = false;
    selected_endpoint_name_.reset();
    selected_model_.reset();
    is_inited_ = true;
  }
  return ret;
}

int ObAiGatewayRouteSession::get_next_endpoint(ObAiGatewayEndpoint &endpoint)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("route session not initialized", K(ret));
  } else {
    ObSpinLockGuard guard(gw_state_->lock_);
    const int64_t ep_count = gw_state_->endpoints_.count();
    if (OB_UNLIKELY(ep_count <= 0)) {
      ret = OB_ITER_END;
      LOG_DEBUG("no endpoints available", K(ret));
    } else {
      int64_t selected_idx = -1;
      // Tier 1: weighted random among {available, weight>0, not tried}.
      if (OB_FAIL(select_weighted_available_(selected_idx, tried_mask_))) {
        LOG_WARN("failed to select weighted endpoint", K(ret));
      } else if (selected_idx < 0
                 && OB_FAIL(select_zero_weight_available_(selected_idx, tried_mask_))) {
        // Tier 2: uniform random among {available, weight==0, not tried}.
        LOG_WARN("failed to select zero-weight endpoint", K(ret));
      }

      if (OB_SUCC(ret) && selected_idx >= 0) {
        // Selection is pre-filtered by is_available under this lock; the matching
        // try_route in the same critical section must allow it. The call is kept
        // for its side effect only (probe accounting, OPEN -> HALF_OPEN).
        bool allowed = false;
        gw_state_->endpoint_states_.at(selected_idx)->try_route(allowed, gw_state_->cb_params_);
        if (OB_UNLIKELY(!allowed)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("endpoint passed is_available but try_route rejected it in the same "
                    "critical section", K(ret), K(selected_idx));
        } else {
          mark_tried_(selected_idx);
          if (OB_FAIL(assign_selected_endpoint_(selected_idx, endpoint))) {
            LOG_WARN("failed to assign selected endpoint", K(ret));
          } else {
            LOG_DEBUG("selected endpoint", K(selected_idx), K(endpoint), K_(tried_mask));
          }
        }
      } else if (OB_SUCC(ret)) {
        // Tier 3 last-resort: every endpoint is tried or unavailable. Route to a
        // random one regardless of circuit state so a request never hard-fails here.
        const int64_t rand_idx = ObRandom::rand(0, ep_count - 1);
        bool allowed = false;
        gw_state_->endpoint_states_.at(rand_idx)->try_route(allowed, gw_state_->cb_params_);
        if (OB_FAIL(assign_selected_endpoint_(rand_idx, endpoint))) {
          LOG_WARN("failed to assign last-resort endpoint", K(ret));
        } else {
          LOG_DEBUG("last-resort endpoint selected (all exhausted)", K(rand_idx), K(endpoint));
        }
      }
    }
  }
  return ret;
}

int ObAiGatewayRouteSession::assign_selected_endpoint_(int64_t idx, ObAiGatewayEndpoint &endpoint)
{
  int ret = OB_SUCCESS;
  current_idx_ = idx;
  // Deep-copy endpoint strings to session's own arena (safe under refresh).
  const ObAiGatewayEndpoint &src_ep = gw_state_->endpoints_.at(idx);
  if (OB_FAIL(ob_write_string(arena_, src_ep.endpoint_name_, endpoint.endpoint_name_))) {
    LOG_WARN("failed to copy endpoint_name", K(ret));
  } else if (OB_FAIL(ob_write_string(arena_, src_ep.model_, endpoint.model_))) {
    LOG_WARN("failed to copy model", K(ret));
  } else if (OB_FAIL(ob_write_string(arena_, src_ep.provider_, endpoint.provider_))) {
    LOG_WARN("failed to copy provider", K(ret));
  } else if (OB_FAIL(ob_write_string(arena_, src_ep.model_name_, endpoint.model_name_))) {
    LOG_WARN("failed to copy model_name", K(ret));
  } else {
    endpoint.weight_ = src_ep.weight_;
    // Save stable identity for later outcome recording.
    selected_endpoint_name_ = endpoint.endpoint_name_;
    selected_model_ = endpoint.model_;
    outcome_pending_ = true;
  }
  return ret;
}

int ObAiGatewayRouteSession::on_success()
{
  outcome_pending_ = false;
  return record_outcome_(true);
}

int ObAiGatewayRouteSession::on_failure(int64_t http_code)
{
  int ret = OB_SUCCESS;
  outcome_pending_ = false;
  // 4xx non-retriable errors (400-499 except 429) indicate client errors, not server
  // failures. They should not count toward circuit breaker failure rate.
  bool is_cb_failure = !(http_code >= 400 && http_code < 500 && http_code != 429);
  if (is_cb_failure) {
    ret = record_outcome_(false);
  }
  return ret;
}

int ObAiGatewayRouteSession::record_outcome_(bool success)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("route session not initialized", K(ret));
  } else if (current_idx_ < 0) {
    LOG_DEBUG("no endpoint selected, skip recording outcome");
  } else {
    ObSpinLockGuard guard(gw_state_->lock_);
    // Re-find current index by stable (name, model) identity; index may have
    // changed due to concurrent refresh
    int64_t found_idx = -1;
    for (int64_t i = 0; i < gw_state_->endpoints_.count() && found_idx < 0; ++i) {
      const ObAiGatewayEndpoint &ep = gw_state_->endpoints_.at(i);
      if (ep.endpoint_name_ == selected_endpoint_name_ && ep.model_ == selected_model_) {
        found_idx = i;
      }
    }
    if (OB_UNLIKELY(found_idx < 0)) {
      // Endpoint removed by refresh; skip recording outcome
      LOG_DEBUG("endpoint no longer exists, skip recording outcome", K(selected_endpoint_name_));
    } else if (OB_ISNULL(gw_state_->endpoint_states_.at(found_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("endpoint state is null", K(ret), K(found_idx));
    } else if (OB_FAIL(gw_state_->endpoint_states_.at(found_idx)->on_request_done(
                  success, gw_state_->cb_params_))) {
      LOG_WARN("failed to record request outcome", K(ret),
               K(found_idx), K(success));
    }
  }
  return ret;
}

int ObAiGatewayRouteSession::select_weighted_available_(int64_t &selected_idx,
                                                        uint64_t exclude_mask)
{
  int ret = OB_SUCCESS;
  selected_idx = -1;
  const int64_t ep_count = gw_state_->endpoints_.count();

  int64_t total_weight = 0;
  for (int64_t i = 0; i < ep_count; i++) {
    if (!is_excluded_(i, exclude_mask) && gw_state_->endpoints_.at(i).weight_ > 0
        && gw_state_->endpoint_states_.at(i)->is_available(gw_state_->cb_params_)) {
      total_weight += gw_state_->endpoints_.at(i).weight_;
    }
  }

  if (total_weight > 0) {
    const int64_t rand_val = ObRandom::rand(0, total_weight - 1);
    int64_t cumulative = 0;
    for (int64_t i = 0; i < ep_count && selected_idx < 0; i++) {
      if (!is_excluded_(i, exclude_mask) && gw_state_->endpoints_.at(i).weight_ > 0
          && gw_state_->endpoint_states_.at(i)->is_available(gw_state_->cb_params_)) {
        cumulative += gw_state_->endpoints_.at(i).weight_;
        if (rand_val < cumulative) {
          selected_idx = i;
        }
      }
    }
    if (OB_UNLIKELY(selected_idx < 0)) {
      // total_weight > 0 guarantees a pick; reaching here is a broken invariant.
      LOG_WARN("weighted selection produced no result despite positive total weight",
               K(total_weight), K(rand_val));
    }
  }
  return ret;
}

int ObAiGatewayRouteSession::select_zero_weight_available_(int64_t &selected_idx,
                                                           uint64_t exclude_mask)
{
  int ret = OB_SUCCESS;
  selected_idx = -1;
  const int64_t ep_count = gw_state_->endpoints_.count();

  int64_t cnt = 0;
  for (int64_t i = 0; i < ep_count; i++) {
    if (!is_excluded_(i, exclude_mask) && 0 == gw_state_->endpoints_.at(i).weight_
        && gw_state_->endpoint_states_.at(i)->is_available(gw_state_->cb_params_)) {
      cnt++;
    }
  }

  if (cnt > 0) {
    const int64_t nth = ObRandom::rand(0, cnt - 1);
    int64_t seen = 0;
    for (int64_t i = 0; i < ep_count && selected_idx < 0; i++) {
      if (!is_excluded_(i, exclude_mask) && 0 == gw_state_->endpoints_.at(i).weight_
          && gw_state_->endpoint_states_.at(i)->is_available(gw_state_->cb_params_)) {
        if (seen == nth) {
          selected_idx = i;
        } else {
          seen++;
        }
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
