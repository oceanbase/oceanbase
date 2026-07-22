/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_AI_GATEWAY_ROUTE_SESSION_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_AI_GATEWAY_ROUTE_SESSION_H_

#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "share/ai_service/ob_ai_gateway_circuit_state.h"

namespace oceanbase
{
namespace share
{

// Request-scoped AI Gateway routing session: selects endpoints in tiers with circuit
// breaker. The referenced ObAiGatewayCircuitState is shared and requires its spinlock.
class ObAiGatewayRouteSession
{
public:
  static const int64_t MAX_TRACKED_ENDPOINTS = 64;

  ObAiGatewayRouteSession();
  ~ObAiGatewayRouteSession();

  int init(ObAiGatewayCircuitState *gw_state);

  void reset();

  // Select next endpoint in three tiers (weighted → zero-weight → last-resort),
  // skipping already-tried ones. Always returns an endpoint when ep_count > 0.
  int get_next_endpoint(ObAiGatewayEndpoint &endpoint);

  int on_success();

  // http_code=0 means network failure; 4xx (400-499 except 429) skips CB recording.
  int on_failure(int64_t http_code = 0);

  void cancel_pending();

  bool is_valid() const { return OB_NOT_NULL(gw_state_); }

  TO_STRING_KV(K_(is_inited), K_(current_idx),
               K_(tried_mask), K_(outcome_pending), KP_(gw_state));

private:
  int select_weighted_available_(int64_t &selected_idx, uint64_t exclude_mask);
  int select_zero_weight_available_(int64_t &selected_idx, uint64_t exclude_mask);
  int record_outcome_(bool success);
  int assign_selected_endpoint_(int64_t idx, ObAiGatewayEndpoint &endpoint);
  bool is_excluded_(int64_t idx, uint64_t mask) const
  {
    if (idx < 0 || idx >= MAX_TRACKED_ENDPOINTS) {
      return false; // beyond bitmask range, always accessible
    }
    return (mask & (1ULL << idx)) != 0;
  }

  void mark_tried_(int64_t idx)
  {
    if (idx >= 0 && idx < MAX_TRACKED_ENDPOINTS) {
      tried_mask_ |= (1ULL << idx);
    }
  }

  ObAiGatewayCircuitState *gw_state_;
  common::ObArenaAllocator arena_;   // stores deep-copied endpoint strings
  ObString selected_endpoint_name_;
  ObString selected_model_;
  uint64_t tried_mask_;        // bitmask of tried endpoint indices (supports up to 64)
  int64_t current_idx_;        // index of last selected endpoint (-1 if none)
  bool is_inited_;
  bool outcome_pending_;       // true after get_next_endpoint, false after on_success/on_failure

  DISALLOW_COPY_AND_ASSIGN(ObAiGatewayRouteSession);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_AI_GATEWAY_ROUTE_SESSION_H_
