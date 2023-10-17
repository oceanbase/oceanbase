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

#define USING_LOG_PREFIX COMMON
#include "ob_fifo_arena.h"
#ifdef OB_USE_ASAN
#include <malloc.h>
#endif
#include "math.h"
#include "ob_memstore_allocator_mgr.h"
#include "share/ob_tenant_mgr.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/throttle/ob_throttle_common.h"

using namespace oceanbase::lib;
using namespace oceanbase::omt;
namespace oceanbase
{
namespace common
{
#define myassert(x) if (!x) { ob_abort(); }
int64_t ObFifoArena::total_hold_ = 0;

int64_t ObFifoArena::Page::get_actual_hold_size()
{
#ifdef OB_USE_ASAN
  return malloc_usable_size(this);
#else
  //every time of alloc_page, ruturn a chunk actually
  return ObTenantCtxAllocator::get_obj_hold(this);
#endif
}

void ObFifoArena::ObWriteThrottleInfo::reset()
{
  decay_factor_ = 0.0;
  alloc_duration_ = 0;
  trigger_percentage_ = 0;
  memstore_threshold_ = 0;
  ATOMIC_SET(&period_throttled_count_, 0);
  ATOMIC_SET(&period_throttled_time_, 0);
  ATOMIC_SET(&total_throttled_count_, 0);
  ATOMIC_SET(&total_throttled_time_, 0);
}

void ObFifoArena::ObWriteThrottleInfo::reset_period_stat_info()
{
  ATOMIC_SET(&period_throttled_count_, 0);
  ATOMIC_SET(&period_throttled_time_, 0);
}

void ObFifoArena::ObWriteThrottleInfo::record_limit_event(int64_t interval)
{
  ATOMIC_INC(&period_throttled_count_);
  ATOMIC_FAA(&period_throttled_time_, interval);
  ATOMIC_INC(&total_throttled_count_);
  ATOMIC_FAA(&total_throttled_time_, interval);
}

int ObFifoArena::ObWriteThrottleInfo::check_and_calc_decay_factor(int64_t memstore_threshold,
                                                                  int64_t trigger_percentage,
                                                                  int64_t alloc_duration)
{
  int ret = OB_SUCCESS;
  if (memstore_threshold != memstore_threshold_
      || trigger_percentage != trigger_percentage_
      || alloc_duration != alloc_duration_
      || decay_factor_ <= 0) {
    memstore_threshold_ = memstore_threshold;
    trigger_percentage_ = trigger_percentage;
    alloc_duration_ = alloc_duration;
    int64_t available_mem = (100 - trigger_percentage_) * memstore_threshold_ / 100;
    double N =  static_cast<double>(available_mem) / static_cast<double>(MEM_SLICE_SIZE);
    double decay_factor = (static_cast<double>(alloc_duration) - N * static_cast<double>(MIN_INTERVAL))/ static_cast<double>((((N*(N+1)*N*(N+1)))/4));
    decay_factor_ = decay_factor < 0 ? 0 : decay_factor;
    COMMON_LOG(INFO, "recalculate decay factor", K(memstore_threshold_), K(trigger_percentage_),
               K(decay_factor_), K(alloc_duration), K(available_mem), K(N));
    if (decay_factor < 0) {
      LOG_ERROR("decay factor is smaller than 0", K(decay_factor), K(alloc_duration), K(N));
    }
  }
  return ret;
}

int ObFifoArena::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  lib::ObMallocAllocator *allocator = lib::ObMallocAllocator::get_instance();
  uint64_t ctx_id = ObCtxIds::MEMSTORE_CTX_ID;

  if (OB_ISNULL(allocator)) {
    ret = OB_INIT_FAIL;
    OB_LOG(ERROR, "mallocator instance is NULLL", K(ret));
  } else {
    allocator_ = allocator;
  }

  if (OB_SUCC(ret)) {
    attr_.tenant_id_ = tenant_id;
    attr_.label_ = ObNewModIds::OB_MEMSTORE;
    attr_.ctx_id_ = ctx_id;
  }
  return ret;
}

void ObFifoArena::reset()
{
  COMMON_LOG(INFO, "MTALLOC.reset", "tenant_id", get_tenant_id());
  shrink_cached_page(0);
}

void ObFifoArena::update_nway_per_group(int64_t nway)
{
  if (nway <= 0) {
    nway = 1;
  } else if (nway > Handle::MAX_NWAY) {
    nway = Handle::MAX_NWAY;
  }
  if (nway > nway_) {
    ATOMIC_STORE(&nway_, nway);
  } else if (nway < nway_) {
    ATOMIC_STORE(&nway_, nway);
    WaitQuiescent(get_qs());
    shrink_cached_page(nway);
  }
}

void ObFifoArena::shrink_cached_page(int64_t nway)
{
  for(int64_t i = 0; i < MAX_CACHED_PAGE_COUNT; i++) {
    if ((i % Handle::MAX_NWAY) >= nway) {
      Page** paddr = cur_pages_ + i;
      Page* page = NULL;
      CriticalGuard(get_qs());
      if (NULL != (page = ATOMIC_LOAD(paddr))) {
        Ref* ref = page->frozen();
        if (NULL != ref) {
          // There may be concurrent removal, no need to pay attention to the return value
          UNUSED(ATOMIC_BCAS(paddr, page, NULL));
          IGNORE_RETURN ATOMIC_FAA(&retired_, page->get_actual_hold_size());
          release_ref(ref);
        }
      }
    }
  }
}

void* ObFifoArena::alloc(int64_t adv_idx, Handle& handle, int64_t size)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  int64_t rsize = size + sizeof(Page) + sizeof(Ref);
  speed_limit(ATOMIC_LOAD(&hold_), size);
  CriticalGuard(get_qs());
  int64_t way_id = get_way_id();
  int64_t idx = get_idx(adv_idx, way_id);
  Page** paddr = cur_pages_ + idx;
  if (adv_idx < 0 || size < 0) {
    COMMON_LOG(INFO, "invalid argument", K(adv_idx), K(size));
    ret = OB_INVALID_ARGUMENT;
  } else if (rsize > PAGE_SIZE) {
    Page* page = NULL;
    if (NULL == (page = alloc_page(rsize))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      bool need_switch = false;
      handle.add_allocated(page->get_actual_hold_size());
      ptr = handle.ref_and_alloc(way_id, need_switch, page, size);
      page->frozen();
      retire_page(way_id, handle, page);
    }
  } else {
    while (OB_SUCC(ret) && NULL == ptr) {
      Page* page = NULL;
      bool need_switch = false;
      if (NULL != (page = ATOMIC_LOAD(paddr))) {
        Ref* ref = handle.get_match_ref(way_id, page);
        if (NULL != ref) {
          ptr = handle.alloc(need_switch, ref, page, size);
        } else {
          LockGuard guard(handle.lock_);
          if (NULL == (ref = handle.get_match_ref(way_id, page))) {
            ptr = handle.ref_and_alloc(way_id, need_switch, page, size);
          }
        }
      }
      if (NULL == page || need_switch) {
        Page* new_page = NULL;
        int64_t alloc_size = PAGE_SIZE;
        if (NULL != page) {
          retire_page(way_id, handle, page);
        }
        if (NULL == (new_page = alloc_page(alloc_size))) {
          // There may be concurrent removal, no need to pay attention to the return value
          UNUSED(ATOMIC_BCAS(paddr, page, NULL));
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (ATOMIC_BCAS(paddr, page, new_page)) {
          handle.add_allocated(new_page->get_actual_hold_size());
        } else {
          destroy_page(new_page);
        }
      }
    }
  }
  return ptr;
}

void ObFifoArena::release_ref(Ref* ref)
{
  if (0 == ref->page_->xref(ref->allocated_)) {
    free_page(ref->page_);
  }
}

void ObFifoArena::free(Handle& handle)
{
  bool wait_qs_done = false;
  for(int i = 0; i < Handle::MAX_NWAY; i++) {
    Ref* ref = NULL;
    Ref* next_ref = handle.ref_[i];
    if (NULL != next_ref && !wait_qs_done) {
      WaitQuiescent(get_qs());
      wait_qs_done = true;
    }
    while(NULL != (ref = next_ref)) {
      next_ref = ref->next_;
      release_ref(ref);
    }
  }
  handle.reset();
}

ObFifoArena::Page* ObFifoArena::alloc_page(int64_t size)
{
  Page* page = (Page*)allocator_->alloc(size, attr_);
  if (NULL != page) {
    ATOMIC_FAA(&allocated_, page->get_actual_hold_size());
    ATOMIC_FAA(&total_hold_, page->get_actual_hold_size());
    ATOMIC_AAF(&hold_, page->get_actual_hold_size());
    page->set(size);
  }
  return page;
}

void ObFifoArena::free_page(Page* page)
{
  if (NULL != page && NULL != allocator_) {
    ATOMIC_FAA(&reclaimed_, page->get_actual_hold_size());
    ATOMIC_FAA(&total_hold_, -page->get_actual_hold_size());
    ATOMIC_FAA(&hold_, -page->get_actual_hold_size());
    allocator_->free(page);
  }
}

void ObFifoArena::retire_page(int64_t idx, Handle& handle, Page* page)
{
  if (NULL != page) {
    ATOMIC_FAA(&retired_, page->get_actual_hold_size());
    handle.add_ref(idx, &page->self_ref_);
  }
}

void ObFifoArena::destroy_page(Page* page)
{
  if (NULL != page && NULL != allocator_) {
    ATOMIC_FAA(&allocated_, -page->get_actual_hold_size());
    ATOMIC_FAA(&total_hold_, -page->get_actual_hold_size());
    ATOMIC_FAA(&hold_, -page->get_actual_hold_size());
    allocator_->free(page);
  }
}

bool ObFifoArena::need_do_writing_throttle() const
{
  bool need_do_writing_throttle = false;
  int64_t trigger_percentage = get_writing_throttling_trigger_percentage_();
  if (trigger_percentage < 100) {
    int64_t trigger_mem_limit = lastest_memstore_threshold_ * trigger_percentage / 100;
    int64_t cur_mem_hold = ATOMIC_LOAD(&hold_);
    need_do_writing_throttle = cur_mem_hold > trigger_mem_limit;
  }

  return need_do_writing_throttle;
}

void ObFifoArena::speed_limit(const int64_t cur_mem_hold, const int64_t alloc_size)
{
  int ret = OB_SUCCESS;
  int64_t trigger_percentage = get_writing_throttling_trigger_percentage_();
  int64_t trigger_mem_limit = 0;
  bool need_speed_limit = false;
  int64_t seq = max_seq_;
  int64_t throttling_interval = 0;
  if (trigger_percentage < 100) {
    if (OB_UNLIKELY(cur_mem_hold < 0 || alloc_size <= 0 || lastest_memstore_threshold_ <= 0 || trigger_percentage <= 0)) {
      COMMON_LOG(ERROR, "invalid arguments", K(cur_mem_hold), K(alloc_size), K(lastest_memstore_threshold_), K(trigger_percentage));
    } else if (cur_mem_hold > (trigger_mem_limit = lastest_memstore_threshold_ * trigger_percentage / 100)) {
      need_speed_limit = true;
      seq = ATOMIC_AAF(&max_seq_, alloc_size);
      int64_t alloc_duration = get_writing_throttling_maximum_duration_();
      if (OB_FAIL(throttle_info_.check_and_calc_decay_factor(lastest_memstore_threshold_, trigger_percentage, alloc_duration))) {
        COMMON_LOG(WARN, "failed to check_and_calc_decay_factor", K(cur_mem_hold), K(alloc_size), K(throttle_info_));
      }
    }
    advance_clock();
    get_seq() = seq;
    tl_need_speed_limit() = need_speed_limit;
    share::get_thread_alloc_stat() += alloc_size;

    if (need_speed_limit && REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
      COMMON_LOG(INFO, "report write throttle info", K(alloc_size), K(attr_), K(throttling_interval),
                  "max_seq_", ATOMIC_LOAD(&max_seq_), K(clock_),
                  K(cur_mem_hold), K(throttle_info_), K(seq));
    }
  }
}

bool ObFifoArena::check_clock_over_seq(const int64_t req)
{
  advance_clock();
  int64_t clock = ATOMIC_LOAD(&clock_);
  return req <= clock;
}

int64_t ObFifoArena::get_clock()
{
  advance_clock();
  return clock_;
}

void ObFifoArena::skip_clock(const int64_t skip_size)
{
  int64_t ov = 0;
  int64_t nv = ATOMIC_LOAD(&clock_);
  while ((ov = nv) < ATOMIC_LOAD(&max_seq_)
         && ov != (nv = ATOMIC_CAS(&clock_, ov, min(ATOMIC_LOAD(&max_seq_), ov + skip_size)))) {
    PAUSE();
    if (REACH_TIME_INTERVAL(100 * 1000L)) {
      const int64_t max_seq = ATOMIC_LOAD(&max_seq_);
      const int64_t cur_mem_hold = ATOMIC_LOAD(&hold_);
      COMMON_LOG(INFO, "skip clock",
                 K(clock_), K(max_seq_), K(skip_size), K(cur_mem_hold), K(attr_.tenant_id_));
    }
  }
}

void ObFifoArena::advance_clock()
{
  int64_t cur_ts = ObTimeUtility::current_time();
  int64_t old_ts = last_update_ts_;
  const int64_t advance_us = cur_ts - old_ts;
  if ((advance_us > ADVANCE_CLOCK_INTERVAL) &&
       old_ts == ATOMIC_CAS(&last_update_ts_, old_ts, cur_ts)) {
    const int64_t trigger_percentage = get_writing_throttling_trigger_percentage_();
    const int64_t trigger_mem_limit = lastest_memstore_threshold_ * trigger_percentage / 100;
    const int64_t cur_mem_hold = ATOMIC_LOAD(&hold_);
    const int64_t mem_limit = calc_mem_limit(cur_mem_hold, trigger_mem_limit, advance_us);
    const int64_t clock = ATOMIC_LOAD(&clock_);
    const int64_t max_seq = ATOMIC_LOAD(&max_seq_);
    ATOMIC_SET(&clock_, min(max_seq, clock + mem_limit));
    if (REACH_TIME_INTERVAL(100 * 1000L)) {
      COMMON_LOG(INFO, "current clock is ",
                K(clock_), K(max_seq_), K(mem_limit), K(cur_mem_hold), K(attr_.tenant_id_));
    }
  }
}

int64_t ObFifoArena::expected_wait_time(const int64_t seq) const
{
  int64_t expected_wait_time = 0;
  int64_t trigger_percentage = get_writing_throttling_trigger_percentage_();
  int64_t trigger_mem_limit = lastest_memstore_threshold_ * trigger_percentage / 100;
  int64_t can_assign_in_next_period = calc_mem_limit(hold_, trigger_mem_limit, ADVANCE_CLOCK_INTERVAL);
  int64_t clock = ATOMIC_LOAD(&clock_);
  if (seq > clock) {
    if (can_assign_in_next_period != 0) {
      expected_wait_time = (seq - clock) * ADVANCE_CLOCK_INTERVAL / can_assign_in_next_period;
    } else {
      expected_wait_time = ADVANCE_CLOCK_INTERVAL;
    }
  }
  return expected_wait_time;
}

// how much memory we can get after dt time.
int64_t ObFifoArena::calc_mem_limit(const int64_t cur_mem_hold, const int64_t trigger_mem_limit, const int64_t dt) const
{
  int ret = OB_SUCCESS;
  int64_t mem_can_be_assigned = 0;

  const double decay_factor = throttle_info_.decay_factor_;
  int64_t init_seq = 0;
  int64_t init_page_left_size = 0;
  double init_page_left_interval = 0;
  double past_interval = 0;
  double last_page_interval = 0;
  double mid_result = 0;
  double approx_max_chunk_seq = 0;
  int64_t max_seq = 0;
  double accumulate_interval = 0;
  if (cur_mem_hold < trigger_mem_limit) {
    // there is no speed limit now
    // we can get all the memory before speed limit
    mem_can_be_assigned = trigger_mem_limit - cur_mem_hold;
  } else if (decay_factor <= 0) {
    mem_can_be_assigned = 0;
    LOG_WARN("we should limit speed, but the decay factor not calculate now", K(cur_mem_hold), K(trigger_mem_limit), K(dt));
  } else {
    init_seq = ((cur_mem_hold - trigger_mem_limit) + MEM_SLICE_SIZE - 1) / (MEM_SLICE_SIZE);
    init_page_left_size = MEM_SLICE_SIZE - (cur_mem_hold - trigger_mem_limit) % MEM_SLICE_SIZE;
    init_page_left_interval =  (1.0 * decay_factor * pow(init_seq, 3) *
                                init_page_left_size / MEM_SLICE_SIZE);
    past_interval = decay_factor * pow(init_seq, 2) * pow(init_seq + 1, 2) / 4;
    // there is speed limit
    if (init_page_left_interval > dt) {
      last_page_interval = decay_factor * pow(init_seq, 3);
      mem_can_be_assigned = dt / last_page_interval * MEM_SLICE_SIZE;
    } else {
      mid_result = 4.0 * (dt + past_interval - init_page_left_interval) / decay_factor;
      approx_max_chunk_seq = pow(mid_result, 0.25);
      max_seq = floor(approx_max_chunk_seq);
      for (int i = 0; i < 2; i++) {
        if (pow(max_seq, 2) * pow(max_seq + 1, 2) < mid_result) {
          max_seq = max_seq + 1;
        }
      }
      accumulate_interval = pow(max_seq, 2) * pow(max_seq + 1, 2) * decay_factor / 4 - past_interval + init_page_left_interval;
      mem_can_be_assigned = init_page_left_size + (max_seq - init_seq) * MEM_SLICE_SIZE;
      if (accumulate_interval > dt) {
        last_page_interval = decay_factor * pow(max_seq, 3);
        mem_can_be_assigned -= (accumulate_interval - dt) / last_page_interval * MEM_SLICE_SIZE;
      }
    }

    // defensive code
    if (pow(max_seq, 2) * pow(max_seq + 1, 2) < mid_result) {
      LOG_ERROR("unexpected result", K(max_seq), K(mid_result));
    }
  }
  // defensive code
  if (mem_can_be_assigned <= 0) {
    LOG_WARN("we can not get memory now", K(mem_can_be_assigned), K(decay_factor), K(cur_mem_hold), K(trigger_mem_limit), K(dt));
  }
  return mem_can_be_assigned;
}

int64_t ObFifoArena::get_throttling_interval(const int64_t cur_mem_hold,
                                             const int64_t alloc_size,
                                             const int64_t trigger_mem_limit)
{
  constexpr int64_t MIN_INTERVAL_PER_ALLOC = 20;
  int64_t chunk_cnt = ((alloc_size + MEM_SLICE_SIZE - 1) / (MEM_SLICE_SIZE));
  int64_t chunk_seq = ((cur_mem_hold - trigger_mem_limit) + MEM_SLICE_SIZE - 1)/ (MEM_SLICE_SIZE);
  int64_t ret_interval = 0;
  double cur_chunk_seq  = 1.0;
  for (int64_t i = 0; i < chunk_cnt && cur_chunk_seq > 0.0; ++i) {
    cur_chunk_seq = static_cast<double>(chunk_seq - i);
    ret_interval += static_cast<int64_t>(throttle_info_.decay_factor_ * cur_chunk_seq * cur_chunk_seq * cur_chunk_seq);
  }
  return alloc_size * ret_interval / MEM_SLICE_SIZE + MIN_INTERVAL_PER_ALLOC;
}

void ObFifoArena::set_memstore_threshold(int64_t memstore_threshold)
{
  ATOMIC_STORE(&lastest_memstore_threshold_, memstore_threshold);
}

template<int64_t N>
struct INTEGER_WRAPPER
{
  INTEGER_WRAPPER() : v_(N), tenant_id_(0) {}
  int64_t v_;
  uint64_t tenant_id_;
};

int64_t ObFifoArena::get_writing_throttling_trigger_percentage_() const
{
  RLOCAL(INTEGER_WRAPPER<DEFAULT_TRIGGER_PERCENTAGE>, wrapper);
  int64_t &trigger_v = (&wrapper)->v_;
  uint64_t &tenant_id = (&wrapper)->tenant_id_;
  if (tenant_id != attr_.tenant_id_ || TC_REACH_TIME_INTERVAL(5 * 1000 * 1000)) { // 5s
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(attr_.tenant_id_));
    if (!tenant_config.is_valid()) {
      COMMON_LOG(INFO, "failed to get tenant config", K(attr_));
    } else {
      trigger_v = tenant_config->writing_throttling_trigger_percentage;
      tenant_id = attr_.tenant_id_;
    }
  }
  return trigger_v;
}

int64_t ObFifoArena::get_writing_throttling_maximum_duration_() const
{
  RLOCAL(INTEGER_WRAPPER<DEFAULT_DURATION>, wrapper);
  int64_t &duration_v = (&wrapper)->v_;
  uint64_t &tenant_id = (&wrapper)->tenant_id_;
  if (tenant_id != attr_.tenant_id_ || TC_REACH_TIME_INTERVAL(1 * 1000 * 1000)) { // 1s
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(attr_.tenant_id_));
    if (!tenant_config.is_valid()) {
      //keep default
      COMMON_LOG(INFO, "failed to get tenant config", K(attr_));
    } else {
      duration_v = tenant_config->writing_throttling_maximum_duration;
      tenant_id = attr_.tenant_id_;
    }
  }
  return duration_v;
}

}; // end namespace allocator
}; // end namespace oceanbase
