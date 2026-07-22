/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "share/ai_service/ob_ai_endpoint_circuit_state.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{

int ObAiEndpointCircuitState::init(ObIAllocator *allocator, int64_t window_size_seconds)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sliding_window_.init(allocator, window_size_seconds))) {
    LOG_WARN("failed to init sliding window", K(ret), K(window_size_seconds));
  } else {
    state_ = ObAiCircuitState::CLOSED;
    open_ts_ = 0;
    probe_sent_ = 0;
    probe_success_count_ = 0;
    last_failure_time_us_ = 0;
  }
  return ret;
}

void ObAiEndpointCircuitState::try_route(bool &allowed, const ObAiCircuitBreakerParams &params)
{
  switch (state_) {
    case ObAiCircuitState::CLOSED: {
      allowed = true;
      break;
    }
    case ObAiCircuitState::OPEN: {
      const int64_t now = common::ObTimeUtility::current_time();
      if (now - open_ts_ >= params.break_duration_seconds_ * ObAiCircuitBreakerParams::US_PER_SECOND) {
        state_ = ObAiCircuitState::HALF_OPEN;
        probe_sent_ = 0;
        probe_success_count_ = 0;
        sliding_window_.reset();
        LOG_INFO("circuit breaker OPEN -> HALF_OPEN, cooldown expired",
                 K(open_ts_), K(now), K(params.break_duration_seconds_));
        probe_sent_++;
        allowed = true;
      } else {
        allowed = false;
      }
      break;
    }
    case ObAiCircuitState::HALF_OPEN: {
      if (probe_sent_ < params.probe_requests_) {
        probe_sent_++;
        allowed = true;
      } else {
        allowed = false;
      }
      break;
    }
    default: {
      allowed = false;
      break;
    }
  }
}

int ObAiEndpointCircuitState::on_request_done(bool success, const ObAiCircuitBreakerParams &params)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sliding_window_.record(success))) {
    LOG_WARN("failed to record in sliding window", K(ret), K(success));
  }

  // Persist last failure time across state transitions: never cleared on
  // CLOSED<->OPEN<->HALF_OPEN jumps; refreshed only when a new failure occurs.
  if (OB_SUCC(ret) && !success) {
    last_failure_time_us_ = common::ObTimeUtility::current_time();
  }

  if (OB_SUCC(ret)) {
    switch (state_) {
      case ObAiCircuitState::CLOSED: {
        int64_t total = 0;
        int64_t failed = 0;
        if (OB_FAIL(sliding_window_.get_failure_rate(total, failed))) {
          LOG_WARN("failed to get failure rate", K(ret));
        } else if (total >= params.minimum_requests_ && total > 0
                   && (failed * ObAiCircuitBreakerParams::FAILURE_RATE_PERCENT_BASE / total)
                          >= params.failure_rate_threshold_) {
          // Do NOT reset sliding window
          state_ = ObAiCircuitState::OPEN;
          open_ts_ = common::ObTimeUtility::current_time();
          LOG_INFO("circuit breaker CLOSED -> OPEN",
                   K(total), K(failed), K(open_ts_),
                   K(params.failure_rate_threshold_), K(params.minimum_requests_));
        }
        break;
      }
      case ObAiCircuitState::HALF_OPEN: {
        if (success) {
          probe_success_count_++;
          if (probe_success_count_ >= params.probe_requests_) {
            // Do NOT reset sliding window
            state_ = ObAiCircuitState::CLOSED;
            open_ts_ = 0;
            LOG_INFO("circuit breaker HALF_OPEN -> CLOSED",
                     K_(probe_success_count), K(params.probe_requests_));
          }
        } else {
          state_ = ObAiCircuitState::OPEN;
          open_ts_ = common::ObTimeUtility::current_time();
          probe_sent_ = 0;
          probe_success_count_ = 0;
          sliding_window_.reset();
          LOG_INFO("circuit breaker HALF_OPEN -> OPEN, probe failed",
                   K(open_ts_));
        }
        break;
      }
      case ObAiCircuitState::OPEN: {
        // No action; transitions happen in try_route when cooldown expires
        break;
      }
      default: {
        break;
      }
    }
  }
  return ret;
}

bool ObAiEndpointCircuitState::is_available(const ObAiCircuitBreakerParams &params) const
{
  bool bret = false;
  switch (state_) {
    case ObAiCircuitState::CLOSED:
      bret = true;
      break;
    case ObAiCircuitState::OPEN: {
      const int64_t now = common::ObTimeUtility::current_time();
      bret = (now - open_ts_ >= params.break_duration_seconds_ * ObAiCircuitBreakerParams::US_PER_SECOND);
      break;
    }
    case ObAiCircuitState::HALF_OPEN:
      bret = (probe_sent_ < params.probe_requests_);
      break;
    default:
      break;
  }
  return bret;
}

} // namespace share
} // namespace oceanbase
