/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON

#include "share/scheduler/ob_shared_compaction_thread_limiter.h"

namespace oceanbase
{
namespace share
{

ObSharedCompactionThreadLimiter::ObSharedCompactionThreadLimiter()
  : user_state_(),
    meta_state_()
{
  reset();
}

void ObSharedCompactionThreadLimiter::reset()
{
  (void)set_user_state(false /*enable*/, 0 /*limit*/, 0 /*user_running*/);
  (void)set_meta_state(false /*meta_use_shared_limit*/, 0 /*meta_running*/);
  set_meta_has_waiting_dag(false /*has_waiting_dag*/);
}

bool ObSharedCompactionThreadLimiter::is_enabled() const
{
  return (0 != user_state_.enable_shared_by_type_) && (0 != meta_state_.meta_use_shared_limit_);
}

int64_t ObSharedCompactionThreadLimiter::get_limit() const
{
  return user_state_.limit_;
}

int64_t ObSharedCompactionThreadLimiter::get_user_running_count() const
{
  return user_state_.user_running_count_;
}

int64_t ObSharedCompactionThreadLimiter::get_meta_running_count() const
{
  return meta_state_.meta_running_count_;
}

bool ObSharedCompactionThreadLimiter::get_meta_has_waiting_dag() const
{
  return 0 != meta_state_.meta_has_waiting_dag_;
}

void ObSharedCompactionThreadLimiter::get_schedule_snapshot(
    bool &out_enable_shared,
    int64_t &out_user_running,
    int64_t &out_meta_running,
    int64_t &out_limit,
    bool &out_meta_has_waiting_dag) const
{
  UserState tmp_user_state;
  MetaState tmp_meta_state;
  tmp_user_state.value_ = user_state_.value_;
  tmp_meta_state.value_ = meta_state_.value_;
  out_enable_shared         = (0 != tmp_user_state.enable_shared_by_type_) && (0 != tmp_meta_state.meta_use_shared_limit_);
  out_user_running          = tmp_user_state.user_running_count_;
  out_meta_running          = tmp_meta_state.meta_running_count_;
  out_limit                 = tmp_user_state.limit_;
  out_meta_has_waiting_dag  = (0 != tmp_meta_state.meta_has_waiting_dag_);
}

void ObSharedCompactionThreadLimiter::set_meta_has_waiting_dag(const bool has_waiting_dag)
{
  MetaState tmp_meta_state;
  tmp_meta_state.value_ = meta_state_.value_;
  tmp_meta_state.meta_has_waiting_dag_ = has_waiting_dag ? 1 : 0;
  meta_state_.value_ = tmp_meta_state.value_;
}

void ObSharedCompactionThreadLimiter::check_can_schedule(
    const bool is_meta,
    bool &out_enable_shared,
    bool &out_can_schedule) const
{
  bool meta_has_waiting_dag = false;
  int64_t user_running = 0;
  int64_t meta_running = 0;
  int64_t shared_limit = 0;
  out_can_schedule = false;
  get_schedule_snapshot(out_enable_shared, user_running, meta_running, shared_limit, meta_has_waiting_dag);
  if (out_enable_shared) {
    const int64_t shared_running = user_running + meta_running;
    // When limit > 1, reserve 1 slot for user so it cannot starve.
    // When limit <= 1, no slot is reserved for user (reserve = 0).
    const int64_t reserve_for_user = (shared_limit <= 1) ? 0 : 1;
    if (is_meta) {
      // meta: may use up to (limit - reserve_for_user) so user keeps its reserved slot.
      out_can_schedule = (meta_running < shared_limit - reserve_for_user)
                         && (shared_running < shared_limit);
    } else {
      // user: always allow up to reserve_for_user tasks to guarantee the reserved slot.
      // Beyond that, yield to meta when it has ready dags.
      out_can_schedule = (shared_running < shared_limit)
                         && (!meta_has_waiting_dag || user_running < reserve_for_user);
    }
  }
}

void ObSharedCompactionThreadLimiter::check_need_pause(
    bool &out_enable_shared,
    bool &out_need_pause) const
{
  int64_t user_running = 0;
  int64_t meta_running = 0;
  int64_t shared_limit = 0;
  bool meta_has_waiting_dag = false;
  out_need_pause = false;
  get_schedule_snapshot(out_enable_shared, user_running, meta_running, shared_limit, meta_has_waiting_dag);
  if (out_enable_shared) {
    out_need_pause = (user_running + meta_running) > shared_limit;
  }
}

int ObSharedCompactionThreadLimiter::set_user_state(
    const bool enable, const int64_t limit, const int64_t user_running)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_packed_field_in_range(limit) || !is_packed_field_in_range(user_running))) {
    ret = OB_SIZE_OVERFLOW;
    COMMON_LOG(WARN, "packed field value out of range", K(ret), K(limit), K(user_running),
        "max", MAX_PACKED_FIELD, "bits", PACKED_COUNT_FIELD_BITS);
  } else {
    UserState tmp_user_state;
    tmp_user_state.enable_shared_by_type_ = enable ? 1 : 0;
    tmp_user_state.limit_ = limit;
    tmp_user_state.user_running_count_ = user_running;
    user_state_.value_ = tmp_user_state.value_;
  }
  return ret;
}

int ObSharedCompactionThreadLimiter::set_user_running_count(const int64_t user_running)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_packed_field_in_range(user_running))) {
    ret = OB_SIZE_OVERFLOW;
    COMMON_LOG(WARN, "user_running_count out of range", K(ret), K(user_running),
        "max", MAX_PACKED_FIELD, "bits", PACKED_COUNT_FIELD_BITS);
  } else {
    UserState tmp_user_state;
    tmp_user_state.value_ = user_state_.value_;
    tmp_user_state.user_running_count_ = user_running;
    user_state_.value_ = tmp_user_state.value_;
  }
  return ret;
}

int ObSharedCompactionThreadLimiter::set_user_limit(const int64_t limit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_packed_field_in_range(limit))) {
    ret = OB_SIZE_OVERFLOW;
    COMMON_LOG(WARN, "limit out of range", K(ret), K(limit), "max", MAX_PACKED_FIELD,
        "bits", PACKED_COUNT_FIELD_BITS);
  } else {
    UserState tmp_user_state;
    tmp_user_state.value_ = user_state_.value_;
    tmp_user_state.limit_ = limit;
    user_state_.value_ = tmp_user_state.value_;
  }
  return ret;
}

int ObSharedCompactionThreadLimiter::set_meta_state(
    const bool meta_use_shared_limit, const int64_t meta_running)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_packed_field_in_range(meta_running))) {
    ret = OB_SIZE_OVERFLOW;
    COMMON_LOG(WARN, "meta_running_count out of range", K(ret), K(meta_running),
        "max", MAX_PACKED_FIELD, "bits", PACKED_COUNT_FIELD_BITS);
  } else {
    MetaState tmp_meta_state;
    tmp_meta_state.value_ = meta_state_.value_;
    tmp_meta_state.meta_use_shared_limit_ = meta_use_shared_limit ? 1 : 0;
    tmp_meta_state.meta_running_count_ = meta_running;
    meta_state_.value_ = tmp_meta_state.value_;
  }
  return ret;
}

int ObSharedCompactionThreadLimiter::set_meta_running_count(const int64_t meta_running)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_packed_field_in_range(meta_running))) {
    ret = OB_SIZE_OVERFLOW;
    COMMON_LOG(WARN, "meta_running_count out of range", K(ret), K(meta_running),
        "max", MAX_PACKED_FIELD, "bits", PACKED_COUNT_FIELD_BITS);
  } else {
    MetaState tmp_meta_state;
    tmp_meta_state.value_ = meta_state_.value_;
    tmp_meta_state.meta_running_count_ = meta_running;
    meta_state_.value_ = tmp_meta_state.value_;
  }
  return ret;
}

bool ObSharedCompactionThreadLimiter::is_packed_field_in_range(const int64_t value)
{
  return value >= 0 && value <= MAX_PACKED_FIELD;
}



/*---------------------------------ObSharedCompactionLimiterPool--------------------------------------*/

void ObSharedCompactionLimiterPool::Slot::reset()
{
  for (int64_t i = 0; i < MERGE_DAG_PRIO_CNT; ++i) {
    limiters_[i].reset();
  }
  user_tenant_id_ = OB_INVALID_TENANT_ID;
  meta_tenant_id_ = OB_INVALID_TENANT_ID;
}

ObSharedCompactionLimiterPool &ObSharedCompactionLimiterPool::get_instance()
{
  static ObSharedCompactionLimiterPool instance;
  return instance;
}

ObSharedCompactionLimiterPool::ObSharedCompactionLimiterPool()
  : allocated_slot_cnt_(0),
    lock_(common::ObLatchIds::SHARED_COMPACTION_THREAD_LIMITER_LOCK)
{
}

uint64_t ObSharedCompactionLimiterPool::get_slot_key_(const Slot &slot)
{
  uint64_t key = OB_INVALID_TENANT_ID;
  if (OB_INVALID_TENANT_ID != slot.user_tenant_id_) {
    key = slot.user_tenant_id_;
  } else if (OB_INVALID_TENANT_ID != slot.meta_tenant_id_) {
    key = gen_user_tenant_id(slot.meta_tenant_id_);
  }
  return key;
}

int ObSharedCompactionLimiterPool::occupy_slot_side_(
    const uint64_t tenant_id, const bool is_user, Slot &slot)
{
  int ret = OB_SUCCESS;
  uint64_t &side_tenant_id = is_user ? slot.user_tenant_id_ : slot.meta_tenant_id_;
  if (OB_INVALID_TENANT_ID == side_tenant_id) {
    side_tenant_id = tenant_id;
  } else {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "shared compaction limiter occupy on already occupied side",
        K(ret), K(tenant_id), K(is_user), K(side_tenant_id));
  }
  return ret;
}

int ObSharedCompactionLimiterPool::vacate_slot_side_(const bool is_user, Slot &slot)
{
  int ret = OB_SUCCESS;
  uint64_t &side_tenant_id = is_user ? slot.user_tenant_id_ : slot.meta_tenant_id_;
  if (OB_INVALID_TENANT_ID != side_tenant_id) {
    side_tenant_id = OB_INVALID_TENANT_ID;
  } else {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "shared compaction limiter vacate on already vacated side",
        K(ret), K(is_user), K(side_tenant_id));
  }
  return ret;
}

void ObSharedCompactionLimiterPool::find_occupied_or_free_slot_(
    const uint64_t key, int64_t &occupied_idx, int64_t &free_idx) const
{
  occupied_idx = -1;
  free_idx = -1;
  for (int64_t i = 0; i < allocated_slot_cnt_ && occupied_idx < 0; ++i) {
    const uint64_t slot_key = get_slot_key_(slots_[i]);
    if (key == slot_key) {
      occupied_idx = i; // sibling already created the slot
    } else if (free_idx < 0 && OB_INVALID_TENANT_ID == slot_key) {
      free_idx = i; // remember the first free slot as fallback
    }
  }
}

int ObSharedCompactionLimiterPool::allocate_new_slot_(const uint64_t key, int64_t &slot_idx)
{
  int ret = OB_SUCCESS;
  if (allocated_slot_cnt_ < MAX_SLOT_CNT) {
    slot_idx = allocated_slot_cnt_++;
  } else {
    ret = OB_SIZE_OVERFLOW;
    COMMON_LOG(WARN, "shared compaction limiter pool exhausted",
        K(ret), K(key), K(allocated_slot_cnt_), K(MAX_SLOT_CNT));
  }
  return ret;
}

int ObSharedCompactionLimiterPool::acquire_slot_for_bind_locked_(
    const uint64_t key,
    int64_t &slot_idx,
    bool &occupied_by_sibling)
{
  int ret = OB_SUCCESS;
  int64_t occupied_idx = -1;
  int64_t free_idx = -1;
  find_occupied_or_free_slot_(key, occupied_idx, free_idx);
  if (occupied_idx >= 0) {
    // sibling already occupies this slot: keep its state, just occupy our own side later.
    occupied_by_sibling = true;
    slot_idx = occupied_idx;
  } else {
    // no sibling yet: take a free slot, or grow the pool if none is free.
    occupied_by_sibling = false;
    if (free_idx >= 0) {
      slot_idx = free_idx;
    } else if (OB_FAIL(allocate_new_slot_(key, slot_idx))) {
      COMMON_LOG(WARN, "failed to allocate new slot", K(ret), K(key));
    }
    if (OB_SUCC(ret)) {
      slots_[slot_idx].reset(); // first tenant of this slot: start clean before use
    }
  }
  return ret;
}

int ObSharedCompactionLimiterPool::bind(const uint64_t tenant_id,
                                        ObSharedCompactionThreadLimiter *&out_limiters)
{
  int ret = OB_SUCCESS;
  out_limiters = nullptr;
  // Both siblings (user N and meta N-1) normalize to the same key so they share one slot.
  const uint64_t key = gen_user_tenant_id(tenant_id);
  if (OB_UNLIKELY(!is_user_tenant(tenant_id) && !is_meta_tenant(tenant_id))) {
    // skip, do nothing
  } else {
    const bool is_user = is_user_tenant(tenant_id);
    lib::ObMutexGuard guard(lock_);
    int64_t slot_idx = -1;
    bool occupied_by_sibling = false;
    if (OB_FAIL(acquire_slot_for_bind_locked_(key, slot_idx, occupied_by_sibling))) {
      COMMON_LOG(WARN, "failed to acquire slot for bind", K(ret), K(tenant_id), K(key));
    } else {
      Slot &slot = slots_[slot_idx];
      if (OB_FAIL(occupy_slot_side_(tenant_id, is_user, slot))) {
        COMMON_LOG(WARN, "failed to occupy slot side",
            K(ret), K(tenant_id), K(key), "slot_id", slot_idx, K(is_user));
      } else {
        out_limiters = slot.limiters_;
        COMMON_LOG(INFO, "shared compaction limiter bind succeed",
            K(tenant_id), K(key), "slot_id", slot_idx, K(occupied_by_sibling),
            K(is_user), K(slot), K(allocated_slot_cnt_), K(MAX_SLOT_CNT));
      }
    }
  }
  return ret;
}

void ObSharedCompactionLimiterPool::release(const uint64_t tenant_id)
{
  const uint64_t key = gen_user_tenant_id(tenant_id);
  if (OB_UNLIKELY(!is_user_tenant(tenant_id) && !is_meta_tenant(tenant_id))) {
    // skip, do nothing
  } else {
    const bool is_user = is_user_tenant(tenant_id);
    lib::ObMutexGuard guard(lock_);
    for (int64_t i = 0; i < allocated_slot_cnt_; ++i) {
      Slot &slot = slots_[i];
      if (key == get_slot_key_(slot)) {
        const uint64_t user_id_before = slot.user_tenant_id_;
        const uint64_t meta_id_before = slot.meta_tenant_id_;
        (void)vacate_slot_side_(is_user, slot);
        bool do_reset_slot = false;
        if (OB_INVALID_TENANT_ID == slot.user_tenant_id_
            && OB_INVALID_TENANT_ID == slot.meta_tenant_id_) {
          // both siblings gone: recycle the slot (state wiped for reuse).
          slot.reset();
          do_reset_slot = true;
        }
        COMMON_LOG(INFO, "shared compaction limiter release succeed",
            K(tenant_id), K(key), "slot_id", i, K(is_user),
            K(user_id_before), K(meta_id_before),
            K(slot), K(do_reset_slot), K(allocated_slot_cnt_), K(MAX_SLOT_CNT));
        break;
      }
    }
  }
}

} // namespace share
} // namespace oceanbase
