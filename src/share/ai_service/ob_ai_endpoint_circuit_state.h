/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_AI_ENDPOINT_CIRCUIT_STATE_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_AI_ENDPOINT_CIRCUIT_STATE_H_

#include "lib/ob_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "share/ai_service/ob_ai_circuit_sliding_window.h"
#include "share/ai_service/ob_ai_service_struct.h"

#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX SHARE
#endif

namespace oceanbase
{
namespace share
{

enum class ObAiCircuitState : int8_t
{
  CLOSED = 0,
  OPEN = 1,
  HALF_OPEN = 2
};

inline const char *circuit_state_to_str(ObAiCircuitState state)
{
  switch (state) {
    case ObAiCircuitState::CLOSED:    return "CLOSED";
    case ObAiCircuitState::OPEN:      return "OPEN";
    case ObAiCircuitState::HALF_OPEN: return "HALF_OPEN";
    default:                          return "UNKNOWN";
  }
}

// Three-state circuit breaker for a single AI Gateway endpoint.
//
// State machine:
//   CLOSED ---(failure_rate >= threshold AND total >= min_requests)---> OPEN
//   OPEN   ---(cooldown expired)---> HALF_OPEN
//   HALF_OPEN ---(total_success >= probe_requests)---> CLOSED
//   HALF_OPEN ---(any failure)---> OPEN
//
// Thread safety: this class does NOT perform internal locking.
// The caller must hold a gateway-level spinlock before calling any method.
class ObAiEndpointCircuitState
{
public:
  ObAiEndpointCircuitState()
      : state_(ObAiCircuitState::CLOSED),
        open_ts_(0),
        probe_sent_(0),
        probe_success_count_(0),
        last_failure_time_us_(0),
        sliding_window_()
  {}

  ~ObAiEndpointCircuitState() { destroy(); }

  int init(common::ObIAllocator *allocator, int64_t window_size_seconds);

  void destroy()
  {
    sliding_window_.destroy();
    state_ = ObAiCircuitState::CLOSED;
    open_ts_ = 0;
    probe_sent_ = 0;
    probe_success_count_ = 0;
    last_failure_time_us_ = 0;
  }

  // Determine whether a request may be routed to this endpoint.
  // Must be called under gateway spinlock.
  void try_route(bool &allowed, const ObAiCircuitBreakerParams &params);

  // Record the outcome of a completed request and drive state transitions.
  // Must be called under gateway spinlock.
  int on_request_done(bool success, const ObAiCircuitBreakerParams &params);

  // Rebuild the sliding window with a new window size while preserving the circuit
  // state machine fields (state_, open_ts_, probe_sent_, last_failure_time_us_).
  // Used by refresh_from_schema when ALTER changes window_size_seconds.
  // OOM-safe: on allocation failure the old sliding window remains usable.
  // Must be called under gateway spinlock.
  int rebuild_sliding_window(common::ObIAllocator *allocator, int64_t new_window_size_seconds)
  {
    return sliding_window_.rebuild(allocator, new_window_size_seconds);
  }

  // Reset to initial CLOSED state.
  // Must be called under gateway spinlock.
  void reset_state()
  {
    state_ = ObAiCircuitState::CLOSED;
    open_ts_ = 0;
    probe_sent_ = 0;
    probe_success_count_ = 0;
    last_failure_time_us_ = 0;
    sliding_window_.reset();
  }

  ObAiCircuitState get_state() const { return state_; }
  int64_t get_open_ts() const { return open_ts_; }
  int64_t get_probe_sent() const { return probe_sent_; }
  int64_t get_probe_success_count() const { return probe_success_count_; }
  int64_t get_last_failure_time_us() const { return last_failure_time_us_; }
  const ObAiCircuitSlidingWindow &get_sliding_window() const { return sliding_window_; }
  ObAiCircuitSlidingWindow &get_sliding_window_mut() { return sliding_window_; }

  // Read-only availability check. Does NOT trigger state transitions or increment probe_sent.
  // Used by weighted selection to filter out unavailable endpoints without side effects.
  // Must be called under gateway spinlock.
  bool is_available(const ObAiCircuitBreakerParams &params) const;

  TO_STRING_KV("state", circuit_state_to_str(state_), K_(open_ts), K_(probe_sent),
               K_(probe_success_count), K_(last_failure_time_us), K_(sliding_window));

private:
  ObAiCircuitState state_;
  int64_t open_ts_;
  int64_t probe_sent_;
  int64_t probe_success_count_;
  int64_t last_failure_time_us_;
  ObAiCircuitSlidingWindow sliding_window_;
  DISALLOW_COPY_AND_ASSIGN(ObAiEndpointCircuitState);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_AI_ENDPOINT_CIRCUIT_STATE_H_
