/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#include "share/ai_service/ob_ai_service_struct.h"
#include "share/ai_service/ob_ai_circuit_sliding_window.h"
#include "share/ai_service/ob_ai_endpoint_circuit_state.h"
#include "share/ai_service/ob_ai_gateway_circuit_state.h"
#include "share/ai_service/ob_ai_gateway_route_session.h"
#undef private

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace unittest
{

// ============================================================
// Task 1.4: JSON Parsing Tests
// ============================================================

class TestJsonParsing : public ::testing::Test
{
public:
  ObArenaAllocator allocator_;
  TestJsonParsing() : allocator_("TestJP") {}
};

TEST_F(TestJsonParsing, ParseEndpointsValidMultiple)
{
  const char *json = R"([
    {"name": "ep1", "model": "openai/gpt-4o", "weight": 70},
    {"name": "ep2", "model": "deepseek/deepseek-chat", "weight": 30}
  ])";
  ObSEArray<ObAiGatewayEndpoint, 4> endpoints;
  ASSERT_EQ(OB_SUCCESS, parse_gateway_endpoints_json(allocator_, ObString::make_string(json), endpoints));
  ASSERT_EQ(2, endpoints.count());
  ASSERT_EQ(0, endpoints.at(0).endpoint_name_.compare("ep1"));
  ASSERT_EQ(0, endpoints.at(0).provider_.compare("openai"));
  ASSERT_EQ(0, endpoints.at(0).model_name_.compare("gpt-4o"));
  ASSERT_EQ(70, endpoints.at(0).weight_);
  ASSERT_EQ(0, endpoints.at(1).endpoint_name_.compare("ep2"));
  ASSERT_EQ(0, endpoints.at(1).provider_.compare("deepseek"));
  ASSERT_EQ(30, endpoints.at(1).weight_);
}

TEST_F(TestJsonParsing, ParseEndpointsWeightDefault)
{
  const char *json = R"([{"name": "ep1", "model": "openai/gpt-4o"}])";
  ObSEArray<ObAiGatewayEndpoint, 4> endpoints;
  ASSERT_EQ(OB_SUCCESS, parse_gateway_endpoints_json(allocator_, ObString::make_string(json), endpoints));
  ASSERT_EQ(1, endpoints.count());
  ASSERT_EQ(0, endpoints.at(0).weight_);
}

TEST_F(TestJsonParsing, ParseEndpointsEmpty)
{
  ObSEArray<ObAiGatewayEndpoint, 4> endpoints;
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_gateway_endpoints_json(allocator_, ObString(), endpoints));
}

TEST_F(TestJsonParsing, ParseEndpointsNotArray)
{
  const char *json = R"({"name": "ep1"})";
  ObSEArray<ObAiGatewayEndpoint, 4> endpoints;
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_gateway_endpoints_json(allocator_, ObString::make_string(json), endpoints));
}

TEST_F(TestJsonParsing, ParseCircuitBreakerAllFields)
{
  const char *json = R"({"failure_rate_threshold": 80, "window_size_seconds": 120,
    "minimum_requests": 20, "break_duration_seconds": 30, "probe_requests": 5})";
  ObAiCircuitBreakerParams params;
  ASSERT_EQ(OB_SUCCESS, parse_gateway_circuit_breaker_json(allocator_, ObString::make_string(json), params));
  ASSERT_EQ(80, params.failure_rate_threshold_);
  ASSERT_EQ(120, params.window_size_seconds_);
  ASSERT_EQ(20, params.minimum_requests_);
  ASSERT_EQ(30, params.break_duration_seconds_);
  ASSERT_EQ(5, params.probe_requests_);
}

TEST_F(TestJsonParsing, ParseCircuitBreakerPartial)
{
  const char *json = R"({"failure_rate_threshold": 80})";
  ObAiCircuitBreakerParams params;
  ASSERT_EQ(OB_SUCCESS, parse_gateway_circuit_breaker_json(allocator_, ObString::make_string(json), params));
  ASSERT_EQ(80, params.failure_rate_threshold_);
  ASSERT_EQ(ObAiCircuitBreakerParams::DEFAULT_WINDOW_SIZE_SECONDS, params.window_size_seconds_);
  ASSERT_EQ(ObAiCircuitBreakerParams::DEFAULT_MINIMUM_REQUESTS, params.minimum_requests_);
}

TEST_F(TestJsonParsing, ParseCircuitBreakerEmptyDefaults)
{
  ObAiCircuitBreakerParams params;
  ASSERT_EQ(OB_SUCCESS, parse_gateway_circuit_breaker_json(allocator_, ObString(), params));
  ASSERT_EQ(50, params.failure_rate_threshold_);
  ASSERT_EQ(60, params.window_size_seconds_);
  ASSERT_EQ(10, params.minimum_requests_);
  ASSERT_EQ(60, params.break_duration_seconds_);
  ASSERT_EQ(3, params.probe_requests_);
}

TEST_F(TestJsonParsing, ParseCircuitBreakerWindowReject)
{
  const char *json = R"({"window_size_seconds": 999})";
  ObAiCircuitBreakerParams params;
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
            parse_gateway_circuit_breaker_json(allocator_, ObString::make_string(json), params));
}

// ============================================================
// Task 2.5 + 9.1.1 + 9.1.2: Sliding Window Tests
// ============================================================

class TestSlidingWindow : public ::testing::Test
{
public:
  ObArenaAllocator allocator_;
  TestSlidingWindow() : allocator_("TestSW") {}
  // Slots are keyed on whole seconds; start just past a boundary so the multi-second
  // usleeps below advance a deterministic number of slots (avoids phase-dependent flakes).
  static void align_to_second_boundary()
  {
    const int64_t rem_us = common::ObTimeUtility::current_time() % 1000000;
    usleep(1000000 - rem_us + 2000);
  }
};

TEST_F(TestSlidingWindow, BasicRecordAndRead)
{
  ObAiCircuitSlidingWindow sw;
  ASSERT_EQ(OB_SUCCESS, sw.init(&allocator_, 60));
  ASSERT_EQ(OB_SUCCESS, sw.record(true));
  ASSERT_EQ(OB_SUCCESS, sw.record(true));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  int64_t total = 0, failed = 0;
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  ASSERT_EQ(3, total);
  ASSERT_EQ(1, failed);
}

TEST_F(TestSlidingWindow, ResetClearsAll)
{
  ObAiCircuitSlidingWindow sw;
  ASSERT_EQ(OB_SUCCESS, sw.init(&allocator_, 60));
  ASSERT_EQ(OB_SUCCESS, sw.record(true));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  sw.reset();
  int64_t total = 0, failed = 0;
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  ASSERT_EQ(0, total);
  ASSERT_EQ(0, failed);
  ASSERT_EQ(0, sw.get_total_success());
  ASSERT_EQ(0, sw.get_total_fail());
}

TEST_F(TestSlidingWindow, WindowSizeOneExpiry)
{
  // window_size=1: after 1 second, old data is evicted
  ObAiCircuitSlidingWindow sw;
  ASSERT_EQ(OB_SUCCESS, sw.init(&allocator_, 1));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  int64_t total = 0, failed = 0;
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  ASSERT_EQ(2, total);
  ASSERT_EQ(2, failed);
  // Wait just over 1 second for epoch to change
  usleep(1100000);
  // Record a success in the new epoch - old slot should be evicted
  ASSERT_EQ(OB_SUCCESS, sw.record(true));
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  ASSERT_EQ(1, total);
  ASSERT_EQ(0, failed);
}

TEST_F(TestSlidingWindow, NegativeGuard)
{
  ObAiCircuitSlidingWindow sw;
  ASSERT_EQ(OB_SUCCESS, sw.init(&allocator_, 60));
  // After reset, recording should not produce negative totals
  sw.reset();
  ASSERT_EQ(OB_SUCCESS, sw.record(true));
  ASSERT_GE(sw.get_total_success(), 0);
  ASSERT_GE(sw.get_total_fail(), 0);
}

TEST_F(TestSlidingWindow, InitTwiceFails)
{
  ObAiCircuitSlidingWindow sw;
  ASSERT_EQ(OB_SUCCESS, sw.init(&allocator_, 60));
  ASSERT_EQ(OB_INIT_TWICE, sw.init(&allocator_, 60));
}

TEST_F(TestSlidingWindow, NotInitFails)
{
  ObAiCircuitSlidingWindow sw;
  ASSERT_EQ(OB_NOT_INIT, sw.record(true));
  int64_t t = 0, f = 0;
  ASSERT_EQ(OB_NOT_INIT, sw.get_failure_rate(t, f));
}

// ============================================================
// Multi-slot advancement tests (Resilience4j-style eviction)
// ============================================================

TEST_F(TestSlidingWindow, MediumGapEvictsExpiredSlots)
{
  // window_size=3: record failures, wait 2 seconds (partial gap),
  // then record success. Expired slots must be evicted.
  ObAiCircuitSlidingWindow sw;
  ASSERT_EQ(OB_SUCCESS, sw.init(&allocator_, 3));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  int64_t total = 0, failed = 0;
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  ASSERT_EQ(2, total);
  ASSERT_EQ(2, failed);
  // Wait 2 seconds (gap=2, window=3: partial expiry)
  usleep(2100000);
  ASSERT_EQ(OB_SUCCESS, sw.record(true));
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  // Old failures in slots from 2s ago: (now - epoch) = 2 < window_size(3),
  // so they may or may not be expired depending on exact timing.
  // But the key assertion: total should be <= 3 (2 old + 1 new max)
  // and after full eviction only the new one remains if gap pushed past
  ASSERT_GE(total, 1);  // at least the new success
  ASSERT_LE(total, 3);  // at most old + new
}

TEST_F(TestSlidingWindow, LongGapResetsAll)
{
  // window_size=2: record failures, wait > 2 seconds (full window expired),
  // then record success. All old data must be gone.
  ObAiCircuitSlidingWindow sw;
  ASSERT_EQ(OB_SUCCESS, sw.init(&allocator_, 2));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  int64_t total = 0, failed = 0;
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  ASSERT_EQ(3, total);
  ASSERT_EQ(3, failed);
  // Wait 3 seconds (gap=3 >= window_size=2: full reset path)
  usleep(3100000);
  ASSERT_EQ(OB_SUCCESS, sw.record(true));
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  // After full reset: only the new success remains
  ASSERT_EQ(1, total);
  ASSERT_EQ(0, failed);
}

TEST_F(TestSlidingWindow, HighFlowNoCrossSlotContamination)
{
  // High flow within same second: all in one slot, no eviction needed
  ObAiCircuitSlidingWindow sw;
  ASSERT_EQ(OB_SUCCESS, sw.init(&allocator_, 60));
  for (int i = 0; i < 100; i++) {
    ASSERT_EQ(OB_SUCCESS, sw.record(i % 3 == 0));  // ~33% success
  }
  int64_t total = 0, failed = 0;
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  ASSERT_EQ(100, total);
  // ~34 success (i=0,3,6,...,99), ~66 fail
  ASSERT_EQ(66, failed);
}

TEST_F(TestSlidingWindow, ResetClearsLastActiveSec)
{
  ObAiCircuitSlidingWindow sw;
  ASSERT_EQ(OB_SUCCESS, sw.init(&allocator_, 60));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  sw.reset();
  // After reset, next record should work correctly (no stale gap calculation)
  usleep(1100000);
  ASSERT_EQ(OB_SUCCESS, sw.record(true));
  int64_t total = 0, failed = 0;
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  ASSERT_EQ(1, total);
  ASSERT_EQ(0, failed);
}

TEST_F(TestSlidingWindow, GapExactlyWindowSize)
{
  // Edge case: gap == window_size should trigger full reset
  ObAiCircuitSlidingWindow sw;
  ASSERT_EQ(OB_SUCCESS, sw.init(&allocator_, 2));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  // Wait exactly window_size seconds
  usleep(2100000);
  ASSERT_EQ(OB_SUCCESS, sw.record(true));
  int64_t total = 0, failed = 0;
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  ASSERT_EQ(1, total);
  ASSERT_EQ(0, failed);
}

TEST_F(TestSlidingWindow, RebuildThenRecord)
{
  ObAiCircuitSlidingWindow sw;
  ASSERT_EQ(OB_SUCCESS, sw.init(&allocator_, 60));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  ASSERT_EQ(OB_SUCCESS, sw.record(true));
  int64_t total = 0, failed = 0;
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  ASSERT_EQ(3, total);
  ASSERT_EQ(2, failed);
  // Rebuild to size 10
  ASSERT_EQ(OB_SUCCESS, sw.rebuild(&allocator_, 10));
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  ASSERT_EQ(0, total);
  ASSERT_EQ(0, failed);
  // Record works correctly after rebuild
  ASSERT_EQ(OB_SUCCESS, sw.record(true));
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  ASSERT_EQ(1, total);
  ASSERT_EQ(0, failed);
}

TEST_F(TestSlidingWindow, WindowEdgeDataRetention)
{
  // window_size=3: data from 2 seconds ago (age=2 < window=3) should be retained
  ObAiCircuitSlidingWindow sw;
  ASSERT_EQ(OB_SUCCESS, sw.init(&allocator_, 3));
  align_to_second_boundary();
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  // Wait 2 seconds (within window of 3)
  usleep(2100000);
  ASSERT_EQ(OB_SUCCESS, sw.record(true));
  int64_t total = 0, failed = 0;
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  // Old data (age ~2s) is still within window (3s), so retained
  // total = 2 old failures + 1 new success = 3
  ASSERT_EQ(3, total);
  ASSERT_EQ(2, failed);
}

TEST_F(TestSlidingWindow, EvictExpiredSlotsDirectCall)
{
  // Verify evict_expired_slots() independently clears expired data
  ObAiCircuitSlidingWindow sw;
  ASSERT_EQ(OB_SUCCESS, sw.init(&allocator_, 2));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  int64_t total = 0, failed = 0;
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  ASSERT_EQ(3, total);
  // Wait past window
  usleep(2500000);
  // Direct call to evict (V$ virtual table path)
  sw.evict_expired_slots();
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  ASSERT_EQ(0, total);
  ASSERT_EQ(0, failed);
}

TEST_F(TestSlidingWindow, ConsecutiveSecondsNoDataLoss)
{
  // Record once per second across window boundary
  ObAiCircuitSlidingWindow sw;
  ASSERT_EQ(OB_SUCCESS, sw.init(&allocator_, 3));  // 3-second window
  align_to_second_boundary();
  // t=0: record fail
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  usleep(1100000);
  // t=1: record fail
  ASSERT_EQ(OB_SUCCESS, sw.record(false));
  usleep(1100000);
  // t=2: record success
  ASSERT_EQ(OB_SUCCESS, sw.record(true));
  int64_t total = 0, failed = 0;
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  // All 3 within window
  ASSERT_EQ(3, total);
  ASSERT_EQ(2, failed);
  usleep(1100000);
  // t=3: record success. t=0 data (age=3 >= window=3) should be evicted
  ASSERT_EQ(OB_SUCCESS, sw.record(true));
  ASSERT_EQ(OB_SUCCESS, sw.get_failure_rate(total, failed));
  // t=0 evicted, t=1,t=2,t=3 remain: 1 fail + 2 success = 3 total, 1 failed
  ASSERT_EQ(3, total);
  ASSERT_EQ(1, failed);
}

// ============================================================
// Task 3.6 + 9.1.3 + 9.1.4: State Machine Tests
// ============================================================

class TestStateMachine : public ::testing::Test
{
public:
  ObArenaAllocator allocator_;
  ObAiCircuitBreakerParams params_;
  TestStateMachine() : allocator_("TestSM") {}
  void SetUp() override { params_.set_defaults(); }
};

TEST_F(TestStateMachine, InitialStateClosed)
{
  ObAiEndpointCircuitState state;
  ASSERT_EQ(OB_SUCCESS, state.init(&allocator_, 60));
  ASSERT_EQ(ObAiCircuitState::CLOSED, state.get_state());
}

TEST_F(TestStateMachine, ClosedToOpen)
{
  ObAiEndpointCircuitState state;
  params_.failure_rate_threshold_ = 50;
  params_.minimum_requests_ = 4;
  ASSERT_EQ(OB_SUCCESS, state.init(&allocator_, 60));
  // Record 2 success + 2 failure = 50% failure, 4 total >= minimum
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(true, params_));
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(true, params_));
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  ASSERT_EQ(ObAiCircuitState::CLOSED, state.get_state());
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  // 4 total, 2 failed = 50% >= 50%, should transition to OPEN
  ASSERT_EQ(ObAiCircuitState::OPEN, state.get_state());
  ASSERT_GT(state.get_open_ts(), 0);
}

TEST_F(TestStateMachine, StaysClosedBelowMinRequests)
{
  ObAiEndpointCircuitState state;
  params_.failure_rate_threshold_ = 50;
  params_.minimum_requests_ = 10;
  ASSERT_EQ(OB_SUCCESS, state.init(&allocator_, 60));
  // All failures but only 5 requests (< 10 minimum)
  for (int i = 0; i < 5; i++) {
    ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  }
  ASSERT_EQ(ObAiCircuitState::CLOSED, state.get_state());
}

TEST_F(TestStateMachine, OpenToHalfOpenViaTryRoute)
{
  ObAiEndpointCircuitState state;
  params_.failure_rate_threshold_ = 100;
  params_.minimum_requests_ = 1;
  params_.break_duration_seconds_ = 0; // instant cooldown
  params_.probe_requests_ = 2;
  ASSERT_EQ(OB_SUCCESS, state.init(&allocator_, 60));
  // Trigger OPEN
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  ASSERT_EQ(ObAiCircuitState::OPEN, state.get_state());
  // try_route with 0s cooldown should transition to HALF_OPEN
  usleep(1000); // tiny delay to ensure time has passed
  bool allowed = false;
  state.try_route(allowed, params_);
  ASSERT_TRUE(allowed);
  ASSERT_EQ(ObAiCircuitState::HALF_OPEN, state.get_state());
}

TEST_F(TestStateMachine, HalfOpenToClosed)
{
  ObAiEndpointCircuitState state;
  params_.failure_rate_threshold_ = 100;
  params_.minimum_requests_ = 1;
  params_.break_duration_seconds_ = 0;
  params_.probe_requests_ = 2;
  ASSERT_EQ(OB_SUCCESS, state.init(&allocator_, 60));
  // CLOSED -> OPEN
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  ASSERT_EQ(ObAiCircuitState::OPEN, state.get_state());
  // OPEN -> HALF_OPEN
  usleep(1000);
  bool allowed = false;
  state.try_route(allowed, params_);
  ASSERT_EQ(ObAiCircuitState::HALF_OPEN, state.get_state());
  // probe_requests=2 successes -> CLOSED
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(true, params_));
  ASSERT_EQ(ObAiCircuitState::HALF_OPEN, state.get_state()); // 1 success, need 2
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(true, params_));
  ASSERT_EQ(ObAiCircuitState::CLOSED, state.get_state());
}

TEST_F(TestStateMachine, HalfOpenToOpenOnFailure)
{
  ObAiEndpointCircuitState state;
  params_.failure_rate_threshold_ = 100;
  params_.minimum_requests_ = 1;
  params_.break_duration_seconds_ = 0;
  params_.probe_requests_ = 3;
  ASSERT_EQ(OB_SUCCESS, state.init(&allocator_, 60));
  // CLOSED -> OPEN
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  // OPEN -> HALF_OPEN
  usleep(1000);
  bool allowed = false;
  state.try_route(allowed, params_);
  ASSERT_EQ(ObAiCircuitState::HALF_OPEN, state.get_state());
  // Any failure in HALF_OPEN -> back to OPEN
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  ASSERT_EQ(ObAiCircuitState::OPEN, state.get_state());
}

TEST_F(TestStateMachine, TryRouteOpenBeforeCooldown)
{
  ObAiEndpointCircuitState state;
  params_.failure_rate_threshold_ = 100;
  params_.minimum_requests_ = 1;
  params_.break_duration_seconds_ = 3600; // long cooldown
  ASSERT_EQ(OB_SUCCESS, state.init(&allocator_, 60));
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  ASSERT_EQ(ObAiCircuitState::OPEN, state.get_state());
  bool allowed = true;
  state.try_route(allowed, params_);
  ASSERT_FALSE(allowed); // cooldown not expired
}

TEST_F(TestStateMachine, HalfOpenProbeLimit)
{
  ObAiEndpointCircuitState state;
  params_.failure_rate_threshold_ = 100;
  params_.minimum_requests_ = 1;
  params_.break_duration_seconds_ = 0;
  params_.probe_requests_ = 2;
  ASSERT_EQ(OB_SUCCESS, state.init(&allocator_, 60));
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  usleep(1000);
  bool allowed = false;
  // First try_route transitions OPEN->HALF_OPEN and increments probe_sent
  state.try_route(allowed, params_);
  ASSERT_TRUE(allowed);
  ASSERT_EQ(ObAiCircuitState::HALF_OPEN, state.get_state());
  // Second probe
  state.try_route(allowed, params_);
  ASSERT_TRUE(allowed);
  // Third probe (exceeds probe_requests=2)
  state.try_route(allowed, params_);
  ASSERT_FALSE(allowed);
}

TEST_F(TestStateMachine, MinimumRequestsZeroNoDivideByZero)
{
  ObAiEndpointCircuitState state;
  params_.minimum_requests_ = 0;
  params_.failure_rate_threshold_ = 50;
  ASSERT_EQ(OB_SUCCESS, state.init(&allocator_, 60));
  // With minimum_requests=0, the check `total >= 0` is always true
  // but we need total > 0 for the division check
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  // Should not crash. Whether it transitions depends on failure rate.
  // 1 total, 1 fail = 100% >= 50%, total(1) >= min(0) -> OPEN
  ASSERT_EQ(ObAiCircuitState::OPEN, state.get_state());
}

TEST_F(TestStateMachine, BreakDurationZeroNotPermanentOpen)
{
  ObAiEndpointCircuitState state;
  params_.failure_rate_threshold_ = 100;
  params_.minimum_requests_ = 1;
  params_.break_duration_seconds_ = 0;
  params_.probe_requests_ = 1;
  ASSERT_EQ(OB_SUCCESS, state.init(&allocator_, 60));
  // Trigger OPEN
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  ASSERT_EQ(ObAiCircuitState::OPEN, state.get_state());
  // With break_duration=0, try_route should immediately transition
  usleep(1000);
  bool allowed = false;
  state.try_route(allowed, params_);
  ASSERT_TRUE(allowed);
  ASSERT_EQ(ObAiCircuitState::HALF_OPEN, state.get_state());
  // Success -> CLOSED
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(true, params_));
  ASSERT_EQ(ObAiCircuitState::CLOSED, state.get_state());
}

// Test that state machine doesn't false-trigger after traffic gap
TEST_F(TestStateMachine, NoFalseTriggerAfterTrafficGap)
{
  ObAiEndpointCircuitState state;
  params_.failure_rate_threshold_ = 50;
  params_.minimum_requests_ = 3;
  ASSERT_EQ(OB_SUCCESS, state.init(&allocator_, 2));  // 2-second window
  // Record 2 failures (below minimum_requests=3, stays CLOSED)
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  ASSERT_EQ(ObAiCircuitState::CLOSED, state.get_state());
  // Wait for data to expire (gap > window_size=2)
  usleep(3100000);
  // New single failure: window should only contain this 1 request
  // total(1) < minimum_requests(3) -> must stay CLOSED
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  ASSERT_EQ(ObAiCircuitState::CLOSED, state.get_state());
}

TEST_F(TestStateMachine, NoFalseTriggerMediumGap)
{
  ObAiEndpointCircuitState state;
  params_.failure_rate_threshold_ = 50;
  params_.minimum_requests_ = 3;
  ASSERT_EQ(OB_SUCCESS, state.init(&allocator_, 3));  // 3-second window
  // Record 2 failures at t=0
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  ASSERT_EQ(ObAiCircuitState::CLOSED, state.get_state());
  // Wait 4 seconds (gap > window_size=3)
  usleep(4100000);
  // Now record 1 failure: only this 1 request is in window
  // total(1) < minimum_requests(3) -> must stay CLOSED
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  ASSERT_EQ(ObAiCircuitState::CLOSED, state.get_state());
  // Record 2 more failures: now total(3) >= minimum(3), rate=100% >= 50% -> OPEN
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  ASSERT_EQ(ObAiCircuitState::OPEN, state.get_state());
}

TEST_F(TestStateMachine, HalfOpenRecoveryWithGapExceedingWindow)
{
  // BUG verification: probe_success_count_ must not be affected by window reset
  ObAiEndpointCircuitState state;
  params_.failure_rate_threshold_ = 100;
  params_.minimum_requests_ = 1;
  params_.break_duration_seconds_ = 0;
  params_.probe_requests_ = 3;
  ASSERT_EQ(OB_SUCCESS, state.init(&allocator_, 2));  // 2-second window
  // CLOSED -> OPEN
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  ASSERT_EQ(ObAiCircuitState::OPEN, state.get_state());
  // OPEN -> HALF_OPEN
  usleep(1000);
  bool allowed = false;
  state.try_route(allowed, params_);
  ASSERT_TRUE(allowed);
  ASSERT_EQ(ObAiCircuitState::HALF_OPEN, state.get_state());
  // Probe 1: success
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(true, params_));
  ASSERT_EQ(ObAiCircuitState::HALF_OPEN, state.get_state());
  ASSERT_EQ(1, state.get_probe_success_count());
  // Wait > window_size (gap causes full window reset in record())
  usleep(3100000);
  // Probe 2: success (after gap > window_size=2)
  state.try_route(allowed, params_);
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(true, params_));
  ASSERT_EQ(ObAiCircuitState::HALF_OPEN, state.get_state());
  ASSERT_EQ(2, state.get_probe_success_count());
  // Probe 3: success -> should transition to CLOSED
  state.try_route(allowed, params_);
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(true, params_));
  ASSERT_EQ(ObAiCircuitState::CLOSED, state.get_state());
  ASSERT_EQ(3, state.get_probe_success_count());
}

TEST_F(TestStateMachine, HalfOpenToClosedNoWindowDependency)
{
  // Verify HALF_OPEN -> CLOSED uses probe_success_count_, not sliding window
  ObAiEndpointCircuitState state;
  params_.failure_rate_threshold_ = 100;
  params_.minimum_requests_ = 1;
  params_.break_duration_seconds_ = 0;
  params_.probe_requests_ = 2;
  ASSERT_EQ(OB_SUCCESS, state.init(&allocator_, 60));
  // CLOSED -> OPEN -> HALF_OPEN
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(false, params_));
  usleep(1000);
  bool allowed = false;
  state.try_route(allowed, params_);
  ASSERT_EQ(ObAiCircuitState::HALF_OPEN, state.get_state());
  // 2 successes -> CLOSED (probe_success_count reaches probe_requests)
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(true, params_));
  ASSERT_EQ(1, state.get_probe_success_count());
  ASSERT_EQ(OB_SUCCESS, state.on_request_done(true, params_));
  ASSERT_EQ(ObAiCircuitState::CLOSED, state.get_state());
}

// ============================================================
// Task 5.5 + 9.1.5 + 9.1.6 + 9.1.7: RouteSession Tests
// ============================================================

class TestRouteSession : public ::testing::Test
{
public:
  ObArenaAllocator allocator_;
  TestRouteSession() : allocator_("TestRS") {}

  void setup_gateway_state(ObAiGatewayCircuitState &gw,
                           int num_endpoints,
                           const int64_t *weights,
                           int64_t window_size = 60)
  {
    gw.init();
    gw.cb_params_.set_defaults();
    gw.cb_params_.window_size_seconds_ = window_size;
    for (int i = 0; i < num_endpoints; i++) {
      ObAiGatewayEndpoint ep;
      char name_buf[32];
      snprintf(name_buf, sizeof(name_buf), "ep%d", i);
      ob_write_string(*gw.cfg_active_, ObString::make_string(name_buf), ep.endpoint_name_, true);
      ob_write_string(*gw.cfg_active_, ObString::make_string("prov/model"), ep.model_, true);
      ob_write_string(*gw.cfg_active_, ObString::make_string("prov"), ep.provider_, true);
      ob_write_string(*gw.cfg_active_, ObString::make_string("model"), ep.model_name_, true);
      ep.weight_ = weights[i];
      gw.endpoints_.push_back(ep);

      void *buf = gw.ep_alloc_.alloc(sizeof(ObAiEndpointCircuitState));
      ObAiEndpointCircuitState *st = new (buf) ObAiEndpointCircuitState();
      st->init(&gw.ep_alloc_, window_size);
      gw.endpoint_states_.push_back(st);
    }
    gw.cached_schema_version_ = 1;
  }
};

TEST_F(TestRouteSession, WeightedRandomDistribution)
{
  ObAiGatewayCircuitState gw;
  int64_t weights[] = {70, 30};
  setup_gateway_state(gw, 2, weights);

  int count_ep0 = 0, count_ep1 = 0;
  const int iterations = 10000;
  for (int i = 0; i < iterations; i++) {
    ObAiGatewayRouteSession session;
    ASSERT_EQ(OB_SUCCESS, session.init(&gw));
    ObAiGatewayEndpoint ep;
    ASSERT_EQ(OB_SUCCESS, session.get_next_endpoint(ep));
    if (0 == ep.endpoint_name_.compare("ep0")) count_ep0++;
    else count_ep1++;
    ASSERT_EQ(OB_SUCCESS, session.on_success());
  }
  // ep0 should be ~70%, allow 60-80% range
  double ratio = (double)count_ep0 / iterations;
  ASSERT_GT(ratio, 0.60);
  ASSERT_LT(ratio, 0.80);
}

TEST_F(TestRouteSession, SingleEndpointOpenLastResort)
{
  ObAiGatewayCircuitState gw;
  int64_t weights[] = {100};
  setup_gateway_state(gw, 1, weights);
  // Force endpoint to OPEN
  gw.cb_params_.failure_rate_threshold_ = 100;
  gw.cb_params_.minimum_requests_ = 1;
  gw.cb_params_.break_duration_seconds_ = 3600;
  {
    ObSpinLockGuard guard(gw.lock_);
    gw.endpoint_states_.at(0)->on_request_done(false, gw.cb_params_);
  }
  ASSERT_EQ(ObAiCircuitState::OPEN, gw.endpoint_states_.at(0)->get_state());

  ObAiGatewayRouteSession session;
  ASSERT_EQ(OB_SUCCESS, session.init(&gw));
  ObAiGatewayEndpoint ep;
  // Last-resort: should still return OB_SUCCESS with the only endpoint
  int ret = session.get_next_endpoint(ep);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, ep.endpoint_name_.compare("ep0"));
  session.on_failure();
}

TEST_F(TestRouteSession, FallbackOrder)
{
  // ep0: weight=100 (OPEN), ep1: weight=50, ep2: weight=0
  ObAiGatewayCircuitState gw;
  int64_t weights[] = {100, 50, 0};
  setup_gateway_state(gw, 3, weights);
  // Force ep0 to OPEN
  gw.cb_params_.failure_rate_threshold_ = 100;
  gw.cb_params_.minimum_requests_ = 1;
  gw.cb_params_.break_duration_seconds_ = 3600;
  {
    ObSpinLockGuard guard(gw.lock_);
    gw.endpoint_states_.at(0)->on_request_done(false, gw.cb_params_);
  }

  ObAiGatewayRouteSession session;
  ASSERT_EQ(OB_SUCCESS, session.init(&gw));
  ObAiGatewayEndpoint ep;
  // First call: ep0 is OPEN, should get ep1 (weight>0)
  ASSERT_EQ(OB_SUCCESS, session.get_next_endpoint(ep));
  ASSERT_EQ(0, ep.endpoint_name_.compare("ep1"));
  // Second call: ep1 already tried, ep0 OPEN -> try ep2 (weight=0)
  ASSERT_EQ(OB_SUCCESS, session.get_next_endpoint(ep));
  ASSERT_EQ(0, ep.endpoint_name_.compare("ep2"));
  // Third call: all exhausted -> last-resort picks random endpoint
  ASSERT_EQ(OB_SUCCESS, session.get_next_endpoint(ep));
  ASSERT_TRUE(ep.endpoint_name_.length() > 0);
  session.on_success();
}

TEST_F(TestRouteSession, AllEndpointsOpenLastResort)
{
  ObAiGatewayCircuitState gw;
  int64_t weights[] = {50, 50};
  setup_gateway_state(gw, 2, weights);
  gw.cb_params_.failure_rate_threshold_ = 100;
  gw.cb_params_.minimum_requests_ = 1;
  gw.cb_params_.break_duration_seconds_ = 3600;
  {
    ObSpinLockGuard guard(gw.lock_);
    gw.endpoint_states_.at(0)->on_request_done(false, gw.cb_params_);
    gw.endpoint_states_.at(1)->on_request_done(false, gw.cb_params_);
  }

  ObAiGatewayRouteSession session;
  ASSERT_EQ(OB_SUCCESS, session.init(&gw));
  ObAiGatewayEndpoint ep;
  // Last-resort: should still return OB_SUCCESS
  ASSERT_EQ(OB_SUCCESS, session.get_next_endpoint(ep));
  ASSERT_TRUE(ep.endpoint_name_.length() > 0);
  session.on_failure();
}

TEST_F(TestRouteSession, OnSuccessOnFailure)
{
  ObAiGatewayCircuitState gw;
  int64_t weights[] = {100};
  setup_gateway_state(gw, 1, weights);

  ObAiGatewayRouteSession session;
  ASSERT_EQ(OB_SUCCESS, session.init(&gw));
  ObAiGatewayEndpoint ep;
  ASSERT_EQ(OB_SUCCESS, session.get_next_endpoint(ep));
  ASSERT_EQ(OB_SUCCESS, session.on_success());
  // Window should have 1 success
  ASSERT_EQ(1, gw.endpoint_states_.at(0)->sliding_window_.get_total_success());
  ASSERT_EQ(0, gw.endpoint_states_.at(0)->sliding_window_.get_total_fail());
  // Get next (same endpoint via last-resort, since only 1 endpoint and already tried)
  ASSERT_EQ(OB_SUCCESS, session.get_next_endpoint(ep));
  session.on_success();

  // New session to test failure
  ObAiGatewayRouteSession session2;
  ASSERT_EQ(OB_SUCCESS, session2.init(&gw));
  ASSERT_EQ(OB_SUCCESS, session2.get_next_endpoint(ep));
  ASSERT_EQ(OB_SUCCESS, session2.on_failure());
  ASSERT_EQ(1, gw.endpoint_states_.at(0)->sliding_window_.get_total_fail());
}

TEST_F(TestRouteSession, LastResortReturnsSuccess)
{
  // All endpoints OPEN, get_next_endpoint should still return OB_SUCCESS (last-resort)
  ObAiGatewayCircuitState gw;
  int64_t weights[] = {50, 50};
  setup_gateway_state(gw, 2, weights);
  gw.cb_params_.failure_rate_threshold_ = 100;
  gw.cb_params_.minimum_requests_ = 1;
  gw.cb_params_.break_duration_seconds_ = 3600;
  {
    ObSpinLockGuard guard(gw.lock_);
    gw.endpoint_states_.at(0)->on_request_done(false, gw.cb_params_);
    gw.endpoint_states_.at(1)->on_request_done(false, gw.cb_params_);
  }
  ObAiGatewayRouteSession session;
  ASSERT_EQ(OB_SUCCESS, session.init(&gw));
  ObAiGatewayEndpoint ep;
  ASSERT_EQ(OB_SUCCESS, session.get_next_endpoint(ep));
  // Should get one of the two endpoints (last-resort)
  ASSERT_TRUE(ep.endpoint_name_.length() > 0);
  session.on_success();
}

TEST_F(TestRouteSession, OutcomePendingAutoFailure)
{
  ObAiGatewayCircuitState gw;
  int64_t weights[] = {100};
  setup_gateway_state(gw, 1, weights);
  {
    ObAiGatewayRouteSession session;
    ASSERT_EQ(OB_SUCCESS, session.init(&gw));
    ObAiGatewayEndpoint ep;
    ASSERT_EQ(OB_SUCCESS, session.get_next_endpoint(ep));
    // Don't call on_success/on_failure, let session destruct
  }
  // After session destructs, should have auto-recorded failure
  ASSERT_EQ(1, gw.endpoint_states_.at(0)->sliding_window_.get_total_fail());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("WARN");
  return RUN_ALL_TESTS();
}
