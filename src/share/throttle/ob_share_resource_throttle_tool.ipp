/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#ifndef OCEABASE_SHARE_THROTTLE_OB_SHARE_RESOURCE_THROTTLE_TOOL_IPP
#define OCEABASE_SHARE_THROTTLE_OB_SHARE_RESOURCE_THROTTLE_TOOL_IPP

#ifndef OCEABASE_SHARE_THROTTLE_OB_SHARE_RESOURCE_THROTTLE_TOOL_H_IPP
#define OCEABASE_SHARE_THROTTLE_OB_SHARE_RESOURCE_THROTTLE_TOOL_H_IPP
#include "ob_share_resource_throttle_tool.h"
#endif



namespace oceanbase {
namespace share {

#define ACQUIRE_THROTTLE_UNIT(ALLOC_TYPE, throttle_unit) \
  ObThrottleUnit<ALLOC_TYPE> &throttle_unit =            \
      module_throttle_tuple_.template element<ModuleThrottleTool<ALLOC_TYPE>>().module_throttle_unit_;
#define ACQUIRE_UNIT_ALLOCATOR(ALLOC_TYPE, throttle_unit, allocator)                                   \
  ObThrottleUnit<ALLOC_TYPE> &throttle_unit =                                                          \
      module_throttle_tuple_.template element<ModuleThrottleTool<ALLOC_TYPE>>().module_throttle_unit_; \
  ALLOC_TYPE *allocator = module_throttle_tuple_.template element<ModuleThrottleTool<ALLOC_TYPE>>().allocator_;

template <typename FakeAllocator, typename... Args>
template <typename HEAD, typename... OTHERS>
int ObShareResourceThrottleTool<FakeAllocator, Args...>::init(HEAD *head, OTHERS *...others)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_one_(head))) {
    SHARE_LOG(WARN, "init one throttle tool failed", KR(ret));
  } else if (OB_FAIL(init(others...))) {
    SHARE_LOG(WARN, "init share resource throttle tool failed", KR(ret));
  }
  return ret;
}

template <typename FakeAllocator, typename... Args>
template <typename ALLOCATOR>
int ObShareResourceThrottleTool<FakeAllocator, Args...>::init(ALLOCATOR *allocator)
{
  int ret = OB_SUCCESS;

  // init last real allocator
  if (OB_FAIL(init_one_(allocator))) {
    SHARE_LOG(WARN, "init one throttle tool failed", KR(ret));
  } else {
    // init share throttle unit
    ModuleThrottleTool<FakeAllocator> &mtt =
        module_throttle_tuple_.template element<ModuleThrottleTool<FakeAllocator>>();
    mtt.allocator_ = nullptr;
    if (OB_FAIL(mtt.module_throttle_unit_.init())) {
      SHARE_LOG(ERROR, "init share resource throttle tool failed", KR(ret));
    } else {
      SHARE_LOG(INFO, "init share resource throttle tool finish", KR(ret), KP(this));
    }
  }

  return ret;
}

template <typename FakeAllocator, typename... Args>
template <typename ALLOCATOR>
int ObShareResourceThrottleTool<FakeAllocator, Args...>::init_one_(ALLOCATOR *allocator)
{
  int ret = OB_SUCCESS;
  ModuleThrottleTool<ALLOCATOR> &mtt = module_throttle_tuple_.template element<ModuleThrottleTool<ALLOCATOR>>();
  mtt.allocator_ = allocator;
  if (OB_ISNULL(mtt.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(ERROR, "allocator is unexpected null", KR(ret), KP(allocator));
  } else if (OB_FAIL(mtt.module_throttle_unit_.init())) {
    SHARE_LOG(ERROR, "init module throttle unit failed", KR(ret));
  } else {
    SHARE_LOG(INFO, "init one allocator for throttle finish", KR(ret), K(mtt));
  }
  return ret;
}

template <typename FakeAllocator, typename... Args>
template <typename ALLOCATOR>
void ObShareResourceThrottleTool<FakeAllocator, Args...>::enable_adaptive_limit()
{
  ACQUIRE_THROTTLE_UNIT(ALLOCATOR, throttle_unit);
  throttle_unit.enable_adaptive_limit();
}

template <typename FakeAllocator, typename... Args>
template <typename ALLOCATOR>
void ObShareResourceThrottleTool<FakeAllocator, Args...>::alloc_resource(const int64_t alloc_size,
                                                                         const int64_t abs_expire_time,
                                                                         bool &is_throttled)
{
  ACQUIRE_THROTTLE_UNIT(FakeAllocator, share_throttle_unit);
  ACQUIRE_UNIT_ALLOCATOR(ALLOCATOR, module_throttle_unit, allocator);

  is_throttled = false;
  bool share_throttled = false;
  bool module_throttled = false;
  int64_t module_hold = allocator->hold();
  SumModuleHoldResourceFunctor sum_hold_func;
  (void)module_throttle_tuple_.for_each(sum_hold_func);

  int share_ret = share_throttle_unit.alloc_resource(sum_hold_func.sum_, alloc_size, abs_expire_time, share_throttled);
  int module_ret = module_throttle_unit.alloc_resource(allocator->hold(), alloc_size, abs_expire_time, module_throttled);

  if (OB_UNLIKELY(OB_SUCCESS != share_ret || OB_SUCCESS != module_ret)) {
    SHARE_LOG_RET(WARN, 0, "throttle alloc resource failed", KR(share_ret), KR(module_ret), KPC(this));
    is_throttled = false;
  } else {
    is_throttled = (share_throttled | module_throttled);
  }
}

template <typename FakeAllocator, typename... Args>
template <typename ALLOCATOR>
bool ObShareResourceThrottleTool<FakeAllocator, Args...>::has_triggered_throttle()
{
  ACQUIRE_THROTTLE_UNIT(FakeAllocator, share_throttle_unit);
  ACQUIRE_UNIT_ALLOCATOR(ALLOCATOR, module_throttle_unit, allocator);

  int64_t module_hold = allocator->hold();
  SumModuleHoldResourceFunctor sum_hold_func;
  (void)module_throttle_tuple_.for_each(sum_hold_func);

  bool share_throttled = share_throttle_unit.has_triggered_throttle(sum_hold_func.sum_);
  bool module_throttled = module_throttle_unit.has_triggered_throttle(allocator->hold());

  bool has_triggered_throttle = (share_throttled | module_throttled);
  return has_triggered_throttle;
}

template <typename FakeAllocator, typename... Args>
template <typename ALLOCATOR>
bool ObShareResourceThrottleTool<FakeAllocator, Args...>::is_throttling(ObThrottleInfoGuard &share_ti_guard,
                                                                        ObThrottleInfoGuard &module_ti_guard)
{
  ACQUIRE_THROTTLE_UNIT(FakeAllocator, share_throttle_unit);
  ACQUIRE_THROTTLE_UNIT(ALLOCATOR, module_throttle_unit);

  bool is_throttling = false;
  // check share throttle unit
  if (share_throttle_unit.is_throttling(share_ti_guard)) {
    is_throttling = true;
  }
  // check module throttle unit
  if (module_throttle_unit.is_throttling(module_ti_guard)) {
    is_throttling = true;
  }
  return is_throttling;
}

template <typename FakeAllocator, typename... Args>
template <typename ALLOCATOR>
bool ObShareResourceThrottleTool<FakeAllocator, Args...>::still_throttling(ObThrottleInfoGuard &share_ti_guard,
                                                                           ObThrottleInfoGuard &module_ti_guard)
{
  ACQUIRE_THROTTLE_UNIT(FakeAllocator, share_throttle_unit);
  ACQUIRE_UNIT_ALLOCATOR(ALLOCATOR, module_throttle_unit, allocator);

  bool still_throttling = false;
  if (module_ti_guard.is_valid() && module_throttle_unit.still_throttling(module_ti_guard, allocator->hold())) {
    // if this module is throttling, skip checking share throttle unit
    still_throttling = true;
  } else if (share_ti_guard.is_valid()) {
    SumModuleHoldResourceFunctor sum_hold_func;
    (void)module_throttle_tuple_.for_each(sum_hold_func);
    if (share_throttle_unit.still_throttling(share_ti_guard, sum_hold_func.sum_)) {
      still_throttling = true;
    }
  }

  return still_throttling;
}

template <typename FakeAllocator, typename... Args>
template <typename ALLOCATOR>
int64_t ObShareResourceThrottleTool<FakeAllocator, Args...>::expected_wait_time(ObThrottleInfoGuard &share_ti_guard,
                                                                                ObThrottleInfoGuard &module_ti_guard)
{
  ACQUIRE_THROTTLE_UNIT(FakeAllocator, share_throttle_unit);
  ACQUIRE_UNIT_ALLOCATOR(ALLOCATOR, module_throttle_unit, allocator);

  int64_t expected_wait_time = 0;

  // step 1 : calculate module throttle time
  if (module_ti_guard.is_valid()) {
    expected_wait_time = module_throttle_unit.expected_wait_time(module_ti_guard, allocator->hold());
  }

  // stpe 2 : if module throttle done, calculate share unit throttle time
  if (expected_wait_time <= 0 && share_ti_guard.is_valid()) {
    SumModuleHoldResourceFunctor sum_hold_func;
    (void)module_throttle_tuple_.for_each(sum_hold_func);
    expected_wait_time = share_throttle_unit.expected_wait_time(share_ti_guard, sum_hold_func.sum_);
  }

  return expected_wait_time;
}

template <typename FakeAllocator, typename... Args>
template <typename ALLOCATOR>
void ObShareResourceThrottleTool<FakeAllocator, Args...>::do_throttle(const int64_t abs_expire_time)
{
  int ret = OB_SUCCESS;
  int64_t left_interval = (0 == abs_expire_time ? ObThrottleUnit<ALLOCATOR>::DEFAULT_MAX_THROTTLE_TIME
                                                : abs_expire_time - ObClockGenerator::getClock());
  int64_t sleep_time = 0;
  ObThrottleInfoGuard share_ti_guard;
  ObThrottleInfoGuard module_ti_guard;
  if (left_interval < 0) {
    // exit directly
  } else if (is_throttling<ALLOCATOR>(share_ti_guard, module_ti_guard)) {
    // loop to do throttle
    bool has_printed_lbt = false;
    while (still_throttling<ALLOCATOR>(share_ti_guard, module_ti_guard) && left_interval > 0) {
      int64_t expected_wait_t = min(left_interval, expected_wait_time<ALLOCATOR>(share_ti_guard, module_ti_guard));
      if (expected_wait_t < 0) {
        SHARE_LOG_RET(ERROR,
                      OB_ERR_UNEXPECTED,
                      "expected wait time should not smaller than 0",
                      K(expected_wait_t),
                      KPC(share_ti_guard.throttle_info()),
                      KPC(module_ti_guard.throttle_info()),
                      K(clock));
        if (module_ti_guard.is_valid()) {
          module_ti_guard.throttle_info()->reset();
        }
      } else {
        int64_t sleep_interval = min(DEFAULT_THROTTLE_SLEEP_INTERVAL, expected_wait_t);
        if (0 < sleep_interval) {
          sleep_time += sleep_interval;
          left_interval -= sleep_interval;
          ::usleep(sleep_interval);
        }
      }
      PrintThrottleUtil::pirnt_throttle_info(ret,
                                             ALLOCATOR::throttle_unit_name(),
                                             sleep_time,
                                             left_interval,
                                             expected_wait_t,
                                             abs_expire_time,
                                             share_ti_guard,
                                             module_ti_guard,
                                             has_printed_lbt);
    }
    PrintThrottleUtil::print_throttle_statistic(ret, ALLOCATOR::throttle_unit_name(), sleep_time);
  }
}

template <typename FakeAllocator, typename... Args>
template <typename ALLOCATOR>
void ObShareResourceThrottleTool<FakeAllocator, Args...>::skip_throttle(const int64_t skip_size)
{
  ObThrottleInfoGuard share_ti_guard;
  ObThrottleInfoGuard module_ti_guard;
  if (is_throttling<ALLOCATOR>(share_ti_guard, module_ti_guard)) {
    skip_throttle<ALLOCATOR>(skip_size, share_ti_guard, module_ti_guard);
  }
}

template <typename FakeAllocator, typename... Args>
template <typename ALLOCATOR>
void ObShareResourceThrottleTool<FakeAllocator, Args...>::skip_throttle(const int64_t skip_size,
                                                                        ObThrottleInfoGuard &share_ti_guard,
                                                                        ObThrottleInfoGuard &module_ti_guard)
{
  ACQUIRE_THROTTLE_UNIT(FakeAllocator, share_throttle_unit);
  ACQUIRE_THROTTLE_UNIT(ALLOCATOR, module_throttle_unit);

  if (module_ti_guard.is_valid()) {
    (void)module_throttle_unit.skip_throttle(skip_size, module_ti_guard.throttle_info()->sequence_);
  }
  if (share_ti_guard.is_valid()) {
    (void)share_throttle_unit.skip_throttle(skip_size, share_ti_guard.throttle_info()->sequence_);
  }
}

template <typename FakeAllocator, typename... Args>
template <typename ALLOCATOR>
void ObShareResourceThrottleTool<FakeAllocator, Args...>::set_resource_limit(const int64_t resource_limit)
{
  ACQUIRE_THROTTLE_UNIT(ALLOCATOR, module_throttle_unit);
  module_throttle_unit.set_resource_limit(resource_limit);
}

template <typename FakeAllocator, typename... Args>
template <typename ALLOCATOR>
void ObShareResourceThrottleTool<FakeAllocator, Args...>::update_throttle_config(const int64_t resource_limit,
                                                                                 const int64_t trigger_percentage,
                                                                                 const int64_t max_duration,
                                                                                 bool &config_changed)
{
  ACQUIRE_THROTTLE_UNIT(ALLOCATOR, throttle_unit);
  (void)throttle_unit.update_throttle_config(resource_limit, trigger_percentage, max_duration, config_changed);
}

#undef ACQUIRE_THROTTLE_UNIT

}  // namespace share
}  // namespace oceanbase

#endif