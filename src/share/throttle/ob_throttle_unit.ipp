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

#ifndef OCEANBASE_SHARE_THROTTLE_OB_THROTTLE_UNIT_IPP
#define OCEANBASE_SHARE_THROTTLE_OB_THROTTLE_UNIT_IPP

#ifndef OCEANBASE_SHARE_THROTTLE_OB_THROTTLE_UNIT_H_IPP
#define OCEANBASE_SHARE_THROTTLE_OB_THROTTLE_UNIT_H_IPP
#include "ob_throttle_unit.h"
#endif

#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/thread_local/ob_tsi_utils.h"

namespace oceanbase {
namespace share {

#define THROTTLE_UNIT_INFO                                                                                             \
  KP(this), K(enable_adaptive_limit_), "Unit Name", unit_name_, "Config Specify Resource Limit(MB)",                   \
      config_specify_resource_limit_ / 1024 / 1024, "Resource Limit(MB)", resource_limit_ / 1024 / 1024,               \
      "Throttle Trigger(MB)", resource_limit_ *throttle_trigger_percentage_ / 100 / 1024 / 1024,                       \
      "Throttle Percentage", throttle_trigger_percentage_, "Max Duration(us)", throttle_max_duration_, "Decay Factor", \
      decay_factor_

template <typename ALLOCATOR>
int ObThrottleUnit<ALLOCATOR>::init()
{
  int ret = OB_SUCCESS;
  ObMemAttr attr;
  attr.tenant_id_ = MTL_ID();
  attr.label_ = "ThrottleInfoMap";
  attr.ctx_id_ = ObCtxIds::DEFAULT_CTX_ID;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    SHARE_LOG(WARN, "init throttle unit failed", KR(ret));
  } else if (OB_FAIL(throttle_info_map_.init(attr))) {
    SHARE_LOG(WARN, "init throttle unit failed", KR(ret));
  } else {
    (void)ALLOCATOR::init_throttle_config(resource_limit_, throttle_trigger_percentage_, throttle_max_duration_);
    (void)update_decay_factor_();
    config_specify_resource_limit_ = resource_limit_;
    enable_adaptive_limit_ = false;
    tenant_id_ = MTL_ID();
    is_inited_ = true;
    SHARE_LOG(INFO,
              "[Throttle]Init throttle config finish",
              K(tenant_id_),
              K(unit_name_),
              K(resource_limit_),
              K(config_specify_resource_limit_),
              K(throttle_trigger_percentage_),
              K(throttle_max_duration_));
  }
  return ret;
}

template <typename ALLOCATOR>
int ObThrottleUnit<ALLOCATOR>::alloc_resource(const int64_t holding_size,
                                              const int64_t alloc_size,
                                              const int64_t abs_expire_time,
                                              bool &is_throttled)
{
  int ret = OB_SUCCESS;
  int64_t trigger_percentage = throttle_trigger_percentage_;

  if (OB_LIKELY(trigger_percentage < 100)) {
    // do adaptive update resource limit if needed
    if (enable_adaptive_limit_) {
      bool is_updated = false;
      ALLOCATOR::adaptive_update_limit(
          tenant_id_, holding_size, config_specify_resource_limit_, resource_limit_, last_update_limit_ts_, is_updated);
      if (is_updated) {
        (void)update_decay_factor_(true /* is_adaptive_update */);
      }
    }

    // check if need throttle
    int64_t throttle_trigger = resource_limit_ * trigger_percentage / 100;
    if (OB_UNLIKELY(holding_size < 0 || alloc_size <= 0 || resource_limit_ <= 0 || trigger_percentage <= 0)) {
      SHARE_LOG(ERROR, "invalid arguments", K(holding_size), K(alloc_size), K(resource_limit_), K(trigger_percentage));
    } else if (holding_size > throttle_trigger) {
      is_throttled = true;
      int64_t sequence = ATOMIC_AAF(&sequence_num_, alloc_size);

      int64_t alloc_duration = throttle_max_duration_;

      (void)advance_clock(holding_size);
      (void)set_throttle_info_(sequence, alloc_size, abs_expire_time);
      (void)print_throttle_info_(holding_size, alloc_size, sequence, throttle_trigger);
    }
  }
  return ret;
}

template <typename ALLOCATOR>
bool ObThrottleUnit<ALLOCATOR>::has_triggered_throttle(const int64_t holding_size)
{
  int ret = OB_SUCCESS;
  bool triggered_throttle = false;
  int64_t trigger_percentage = throttle_trigger_percentage_;

  if (OB_LIKELY(trigger_percentage < 100)) {
    int64_t throttle_trigger = resource_limit_ * trigger_percentage / 100;
    if (OB_UNLIKELY(holding_size < 0 || trigger_percentage <= 0)) {
      triggered_throttle = true;
      SHARE_LOG(ERROR, "invalid arguments", K(holding_size), K(resource_limit_), K(trigger_percentage));
    } else if (holding_size > throttle_trigger) {
      triggered_throttle = true;
    } else {
      triggered_throttle = false;
    }
  }
  return triggered_throttle;
}

template <typename ALLOCATOR>
bool ObThrottleUnit<ALLOCATOR>::is_throttling(ObThrottleInfoGuard &ti_guard)
{
  int ret = OB_SUCCESS;
  bool is_throttling = false;

  if (OB_FAIL(get_throttle_info_(share::ThrottleID(common::get_itid()), ti_guard))) {
    if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
      is_throttling = false;
    } else {
      SHARE_LOG(WARN, "get throttle info failed", K(ti_guard), THROTTLE_UNIT_INFO);
    }
  } else {
    is_throttling = ti_guard.throttle_info()->is_throttling();
  }

  return is_throttling;
}

template <typename ALLOCATOR>
void ObThrottleUnit<ALLOCATOR>::set_throttle_info_(const int64_t queue_sequence,
                                                   const int64_t allocated_size,
                                                   const int64_t abs_expire_time)
{
  int ret = OB_SUCCESS;
  ObThrottleInfo *throttle_info = nullptr;

  if (OB_FAIL(inner_get_throttle_info_(throttle_info, abs_expire_time))) {
    SHARE_LOG(WARN, "get throttle info failed", KR(ret), THROTTLE_UNIT_INFO);
  } else if (OB_ISNULL(throttle_info)) {
    SHARE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "throttle_info should not be nullptr");
  } else {
    throttle_info->need_throttle_ = true;
    throttle_info->sequence_ = queue_sequence;
    throttle_info->allocated_size_ += allocated_size;

    inner_revert_throttle_info_(throttle_info);
  }
}

template <typename ALLOCATOR>
void ObThrottleUnit<ALLOCATOR>::print_throttle_info_(const int64_t holding_size,
                                                     const int64_t alloc_size,
                                                     const int64_t sequence,
                                                     const int64_t throttle_trigger)
{
  // ATTENTION!! DO NOT MODIFY THIS LOG!!
  // This function is used to print some statistic info, which will be collected by a awk script.
  const int64_t PRINT_THROTTLE_INFO_INTERVAL = 1L * 1000L * 1000L;  // one second
  int64_t last_print_ts = last_print_throttle_info_ts_;

  int64_t current_ts = ObClockGenerator::getClock();
  if (current_ts - last_print_ts > PRINT_THROTTLE_INFO_INTERVAL &&
      ATOMIC_BCAS(&last_print_throttle_info_ts_, last_print_ts, current_ts)) {
    // release_speed means the allocated resource size per second
    int64_t release_speed = 0;
    int64_t cur_clock = clock_;
    if (pre_clock_ > 0) {
      release_speed = (cur_clock - pre_clock_) / ((current_ts - last_print_ts) / PRINT_THROTTLE_INFO_INTERVAL);
    }
    pre_clock_ = cur_clock;

    SHARE_LOG(INFO,
              "[Throttling] (report write throttle info) Size Info",
              "tenant_id",                  tenant_id_,
              "Throttle Unit Name",         unit_name_,
              "Allocating Resource Size",   alloc_size,
              "Holding Resource Size",      holding_size,
              "Queueing Sequence",          sequence,
              "Released Sequence",          cur_clock,
              "Release Speed",              release_speed,
              "Total Resource Limit",       resource_limit_,
              "Config Specify Limit",       config_specify_resource_limit_,
              "Throttle Trigger Threshold", throttle_trigger,
              "Decay Factor",               decay_factor_);
  }
}

template <typename ALLOCATOR>
int ObThrottleUnit<ALLOCATOR>::inner_get_throttle_info_(share::ObThrottleInfo *&throttle_info,
                                                        const int64_t input_abs_expire_time)
{
  int ret = OB_SUCCESS;
  ThrottleID tid(common::get_itid());
  bool get_throttle_info_done = false;
  int64_t abs_expire_time = input_abs_expire_time;
  throttle_info = nullptr;
  while (OB_SUCC(ret) && !get_throttle_info_done) {
    if (OB_FAIL(throttle_info_map_.get(tid, throttle_info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        if (0 == abs_expire_time) {
          abs_expire_time = DEFAULT_MAX_THROTTLE_TIME + ObClockGenerator::getClock();
        }
        if (OB_FAIL(throttle_info_map_.alloc_value(throttle_info))) {
          if (OB_ALLOCATE_MEMORY_FAILED == ret) {
            if (REACH_TIME_INTERVAL(10L * 1000L * 1000L)) {
              SHARE_LOG(WARN, "allocate throttle info failed", KR(ret), K(tid));
            }
            if (ObClockGenerator::getClock() > abs_expire_time) {
              SHARE_LOG(WARN, "allocate throttle info failed", KR(ret), K(tid));
            } else {
              // sleep 10 ms and retry
              usleep(10 * 1000);
              ret = OB_SUCCESS;
            }
          } else {
            SHARE_LOG(ERROR, "allocate throttle info failed", KR(ret), K(tid));
          }
        } else if (OB_FAIL(throttle_info_map_.insert(tid, throttle_info))) {
          SHARE_LOG(ERROR, "insert throttle info failed", KR(ret), K(tid));
          (void)throttle_info_map_.free_value(throttle_info);
        }
      }
    } else {
      get_throttle_info_done = true;
    }
  }
  return ret;
}

template <typename ALLOCATOR>
void ObThrottleUnit<ALLOCATOR>::inner_revert_throttle_info_(share::ObThrottleInfo *throttle_info)
{
  (void)throttle_info_map_.revert(throttle_info);
}

template <typename ALLOCATOR>
int ObThrottleUnit<ALLOCATOR>::get_throttle_info_(const ThrottleID &throttle_id, share::ObThrottleInfoGuard &ti_guard)
{
  int ret = OB_SUCCESS;
  ObThrottleInfo *throttle_info = nullptr;

  if (OB_FAIL(throttle_info_map_.get(throttle_id, throttle_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      SHARE_LOG(WARN, "get throttle info failed", THROTTLE_UNIT_INFO);
    }
  } else {
    (void)ti_guard.init(throttle_info, &throttle_info_map_);
  }

  return ret;
}

template <typename ALLOCATOR>
void ObThrottleUnit<ALLOCATOR>::update_decay_factor_(const bool is_adaptive_update /* default : false */)
{
  int64_t avaliable_resources = (100 - throttle_trigger_percentage_) * resource_limit_ / 100;
  double N = static_cast<double>(avaliable_resources) / static_cast<double>(RESOURCE_UNIT_SIZE_);
  double decay_factor =
      (static_cast<double>(throttle_max_duration_) - N) / static_cast<double>((((N * (N + 1) * N * (N + 1))) / 4));
  decay_factor_ = decay_factor < 0 ? 0 : decay_factor;
  if (decay_factor < 0) {
    SHARE_LOG_RET(
        ERROR, OB_ERR_UNEXPECTED, "decay factor is smaller than 0", K(decay_factor), K(throttle_max_duration_), K(N));
  }

  if (OB_UNLIKELY(is_adaptive_update)) {
    if (REACH_TIME_INTERVAL(1LL * 1000LL * 1000LL /* one second */)) {
      SHARE_LOG(INFO, "[Throttle] Update Throttle Unit Config", K(is_adaptive_update), K(N), THROTTLE_UNIT_INFO);
    }
  } else {
    SHARE_LOG(INFO, "[Throttle] Update Throttle Unit Config", K(is_adaptive_update),  K(N), THROTTLE_UNIT_INFO);
  }
}

template <typename ALLOCATOR>
void ObThrottleUnit<ALLOCATOR>::advance_clock(const int64_t holding_size)
{
  int64_t cur_ts = ObClockGenerator::getClock();
  int64_t old_ts = last_advance_clock_ts_us_;
  const int64_t advance_us = cur_ts - old_ts;
  if ((advance_us > ADVANCE_CLOCK_INTERVAL) && ATOMIC_BCAS(&last_advance_clock_ts_us_, old_ts, cur_ts)) {
    if (enable_adaptive_limit_) {
      bool is_updated = false;
      ALLOCATOR::adaptive_update_limit(
          tenant_id_, holding_size, config_specify_resource_limit_, resource_limit_, last_update_limit_ts_, is_updated);
      if (is_updated) {
        (void)update_decay_factor_(true /* is_adaptive_update */);
      }
    }

    bool unused = false;
    const int64_t throttle_trigger = resource_limit_ * throttle_trigger_percentage_ / 100;
    const int64_t avaliable_resource = avaliable_resource_after_dt_(holding_size, throttle_trigger, advance_us);
    const int64_t clock = ATOMIC_LOAD(&clock_);
    const int64_t cur_seq = ATOMIC_LOAD(&sequence_num_);
    ATOMIC_SET(&clock_, min(cur_seq, clock + avaliable_resource));
    if (REACH_TIME_INTERVAL(10LL * 1000LL * 1000LL/* 10 seconds */)) {
      SHARE_LOG(INFO,
                "[Throttling] Advance Clock",
                K(avaliable_resource),
                K(clock),
                K(clock_),
                K(cur_seq),
                THROTTLE_UNIT_INFO);
    }
  }
}

template <typename ALLOCATOR>
void ObThrottleUnit<ALLOCATOR>::skip_throttle(const int64_t skip_size, const int64_t queue_sequence)
{
  int64_t old_clock = 0;
  // loop until 1. (clock >= queue_sequence) or 2. (skip clock success)
  while ((old_clock = ATOMIC_LOAD(&clock_)) < queue_sequence) {
    int64_t new_clock = old_clock + min(skip_size, queue_sequence - old_clock);
    if (ATOMIC_BCAS(&clock_, old_clock, new_clock)) {
      // skip throttle finish
      break;
    }
  }
}

template <typename ALLOCATOR>
void ObThrottleUnit<ALLOCATOR>::reset_thread_throttle_()
{
  int ret = OB_SUCCESS;
  ObThrottleInfoGuard ti_guard;
  if (OB_FAIL(get_throttle_info_(common::get_itid(), ti_guard))) {
    SHARE_LOG(WARN, "get throttle info from map failed", KR(ret), THROTTLE_UNIT_INFO);
  } else if (ti_guard.is_valid()) {
    ti_guard.throttle_info()->need_throttle_ = false;
    ti_guard.throttle_info()->sequence_ = 0;
  }
}

template <typename ALLOCATOR>
int64_t ObThrottleUnit<ALLOCATOR>::avaliable_resource_after_dt_(const int64_t cur_resource_hold,
                                                                const int64_t throttle_trigger,
                                                                const int64_t dt)
{
  int ret = OB_SUCCESS;
  int64_t avaliable_resource = 0;

  const double decay_factor = decay_factor_;
  int64_t init_seq = 0;
  int64_t init_page_left_size = 0;
  double init_page_left_interval = 0;
  double past_interval = 0;
  double last_page_interval = 0;
  double mid_result = 0;
  double approx_max_chunk_seq = 0;
  int64_t max_seq = 0;
  double accumulate_interval = 0;
  if (cur_resource_hold < throttle_trigger) {
    // there is no speed limit now
    // we can get all the memory before speed limit
    avaliable_resource = throttle_trigger - cur_resource_hold;
  } else if (decay_factor <= 0) {
    avaliable_resource = 0;
    SHARE_LOG(WARN, "decay factor invalid", K(cur_resource_hold), K(throttle_trigger), K(dt), THROTTLE_UNIT_INFO);
  } else {
    init_seq = ((cur_resource_hold - throttle_trigger) + RESOURCE_UNIT_SIZE_ - 1) / (RESOURCE_UNIT_SIZE_);
    init_page_left_size = RESOURCE_UNIT_SIZE_ - (cur_resource_hold - throttle_trigger) % RESOURCE_UNIT_SIZE_;
    init_page_left_interval = (1.0 * decay_factor * pow(init_seq, 3) * init_page_left_size / RESOURCE_UNIT_SIZE_);
    past_interval = decay_factor * pow(init_seq, 2) * pow(init_seq + 1, 2) / 4;
    // there is speed limit
    if (init_page_left_interval > dt) {
      last_page_interval = decay_factor * pow(init_seq, 3);
      avaliable_resource = dt / last_page_interval * RESOURCE_UNIT_SIZE_;
    } else {
      mid_result = 4.0 * (dt + past_interval - init_page_left_interval) / decay_factor;
      approx_max_chunk_seq = pow(mid_result, 0.25);
      max_seq = floor(approx_max_chunk_seq);
      for (int i = 0; i < 2; i++) {
        if (pow(max_seq, 2) * pow(max_seq + 1, 2) < mid_result) {
          max_seq = max_seq + 1;
        }
      }
      accumulate_interval =
          pow(max_seq, 2) * pow(max_seq + 1, 2) * decay_factor / 4 - past_interval + init_page_left_interval;
      avaliable_resource = init_page_left_size + (max_seq - init_seq) * RESOURCE_UNIT_SIZE_;
      if (accumulate_interval > dt) {
        last_page_interval = decay_factor * pow(max_seq, 3);
        avaliable_resource -= (accumulate_interval - dt) / last_page_interval * RESOURCE_UNIT_SIZE_;
      }
    }

    // defensive code
    if (pow(max_seq, 2) * pow(max_seq + 1, 2) < mid_result) {
      SHARE_LOG(ERROR, "unexpected result", K(max_seq), K(mid_result));
    }
  }
  // defensive code
  if (avaliable_resource <= 0) {
    SHARE_LOG(
        WARN, "we can not get memory now", K(avaliable_resource), K(cur_resource_hold), K(dt), THROTTLE_UNIT_INFO);
  }
  return avaliable_resource;
}

template <typename ALLOCATOR>
bool ObThrottleUnit<ALLOCATOR>::still_throttling(ObThrottleInfoGuard &ti_guard, const int64_t holding_size)
{
  (void)advance_clock(holding_size);
  bool still_throttling = false;
  int64_t trigger_percentage = throttle_trigger_percentage_;
  ObThrottleInfo *ti_info = ti_guard.throttle_info();

  // check if still throttling
  if (trigger_percentage < 100 && OB_NOT_NULL(ti_info)) {
    int64_t throttle_trigger = resource_limit_ * trigger_percentage / 100;
    if (ti_info->sequence_ <= clock_) {
      still_throttling = false;
    } else {
      still_throttling = true;
    }
  } else {
    still_throttling = false;
  }

  // reset status if do not need throttle
  if (!still_throttling && OB_NOT_NULL(ti_info)) {
    ti_info->reset();
  }
  return still_throttling;
}

template <typename ALLOCATOR>
int64_t ObThrottleUnit<ALLOCATOR>::expected_wait_time(share::ObThrottleInfoGuard &ti_guard, const int64_t holding_size)
{
  int64_t expected_wait_time = 0;
  int64_t clock = clock_;
  int64_t queue_sequence = OB_NOT_NULL(ti_guard.throttle_info()) ? ti_guard.throttle_info()->sequence_ : 0;
  if (clock >= queue_sequence) {
    // do not need wait cause clock has reached queue_sequence
    expected_wait_time = 0;
  } else {
    int64_t throttle_trigger = resource_limit_ * throttle_trigger_percentage_ / 100;
    int64_t can_assign_in_next_period =
        avaliable_resource_after_dt_(holding_size, throttle_trigger, ADVANCE_CLOCK_INTERVAL);
    if (can_assign_in_next_period != 0) {
      expected_wait_time = (queue_sequence - clock) * ADVANCE_CLOCK_INTERVAL / can_assign_in_next_period;
    } else {
      expected_wait_time = ADVANCE_CLOCK_INTERVAL;
    }
  }
  return expected_wait_time;
}

template <typename ALLOCATOR>
void ObThrottleUnit<ALLOCATOR>::set_resource_limit(const int64_t value)
{
  if (value < 0) {
    int ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "update throttle config failed cause invalid argument", K(value));
  } else {
    (void)inner_set_resource_limit_(value);
    (void)update_decay_factor_();
  }
}

template <typename ALLOCATOR>
void ObThrottleUnit<ALLOCATOR>::inner_set_resource_limit_(const int64_t value)
{
  // record config specified resource limit
  ATOMIC_STORE(&config_specify_resource_limit_, value);

  if (OB_LIKELY(!enable_adaptive_limit_)) {
    // if adaptive limit is not enabled, dircetly update resource_limit_
    ATOMIC_STORE(&resource_limit_, value);
  } else {
    // adaptive update logic will update resource_limit_
  }
}

template <typename ALLOCATOR>
void ObThrottleUnit<ALLOCATOR>::set_throttle_trigger_percentage(const int64_t value)
{
  if (value <= 0 || value > 100) {
    int ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "update throttle config failed cause invalid argument", K(value));
  } else {
    throttle_trigger_percentage_ = value;
    (void)update_decay_factor_();
  }
}

template <typename ALLOCATOR>
void ObThrottleUnit<ALLOCATOR>::set_throttle_max_duration(const int64_t value)
{
  if (value <= 0) {
    int ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "update throttle config failed cause invalid argument", K(value));
  } else {
    throttle_max_duration_ = value;
    (void)update_decay_factor_();
  }
}

template <typename ALLOCATOR>
void ObThrottleUnit<ALLOCATOR>::update_throttle_config(const int64_t resource_limit,
                                                       const int64_t throttle_trigger_percentage,
                                                       const int64_t throttle_max_duration,
                                                       bool &config_changed)
{
  config_changed = false;
  if (resource_limit < 0 || throttle_trigger_percentage <= 0 || throttle_trigger_percentage > 100 ||
      throttle_max_duration <= 0) {
    int ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN,
              "update throttle config failed cause invalid argument",
              K(resource_limit),
              K(throttle_trigger_percentage),
              K(throttle_max_duration));
  } else if (config_specify_resource_limit_ != resource_limit ||
             throttle_trigger_percentage_ != throttle_trigger_percentage ||
             throttle_max_duration_ != throttle_max_duration) {
    config_changed = true;
    SHARE_LOG(INFO,
              "[Throttle] Update Config",
              THROTTLE_UNIT_INFO,
              "New Resource Limit(MB)", resource_limit / 1024 / 1024,
              "New trigger percentage", throttle_trigger_percentage,
              "New Throttle Duration", throttle_max_duration);
    throttle_trigger_percentage_ = throttle_trigger_percentage;
    throttle_max_duration_ = throttle_max_duration;
    (void)inner_set_resource_limit_(resource_limit);
    (void)update_decay_factor_();
  }
}

template <typename ALLOCATOR>
void ObThrottleUnit<ALLOCATOR>::enable_adaptive_limit()
{
  ATOMIC_STORE(&enable_adaptive_limit_, true);
}

}  // namespace share
}  // namespace oceanbase

#endif