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

#include "ob_memstore_allocator.h"
#include "ob_shared_memory_allocator_mgr.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
using namespace share;
namespace share
{

int FrozenMemstoreInfoLogger::operator()(ObDLink* link)
{
  int ret = OB_SUCCESS;
  ObMemstoreAllocator::AllocHandle* handle = CONTAINER_OF(link, typeof(*handle), total_list_);
  memtable::ObMemtable& mt = handle->mt_;
  if (handle->is_frozen()) {
    if (OB_FAIL(databuff_print_obj(buf_, limit_, pos_, mt))) {
    } else {
      ret = databuff_printf(buf_, limit_, pos_, ",");
    }
  }
  return ret;
}

int ActiveMemstoreInfoLogger::operator()(ObDLink* link)
{
  int ret = OB_SUCCESS;
  ObMemstoreAllocator::AllocHandle* handle = CONTAINER_OF(link, typeof(*handle), total_list_);
  memtable::ObMemtable& mt = handle->mt_;
  if (handle->is_active()) {
    if (OB_FAIL(databuff_print_obj(buf_, limit_, pos_, mt))) {
    } else {
      ret = databuff_printf(buf_, limit_, pos_, ",");
    }
  }
  return ret;
}

int ObMemstoreAllocator::AllocHandle::init()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ObMemstoreAllocator &host = MTL(ObSharedMemAllocMgr *)->memstore_allocator();
  (void)host.init_handle(*this);
  return ret;
}

int ObMemstoreAllocator::init()
{
  int ret = OB_SUCCESS;
  throttle_tool_ = &(MTL(ObSharedMemAllocMgr *)->share_resource_throttle_tool());
  if (OB_FAIL(arena_.init())) {
    COMMON_LOG(ERROR, "failed to init large arena", K(ret));
  } else if (OB_FAIL(small_arena_.init())) {
    COMMON_LOG(ERROR, "failed to init small arena", K(ret));
  }
  return ret;
}

void ObMemstoreAllocator::mark_small_active(AllocHandle& handle)
{
  if (AllocHandle::SMALL_INACTIVE == handle.get_small_pool_state() &&
      handle.is_active() &&
      ATOMIC_BCAS(&handle.small_pool_state_,
                  AllocHandle::SMALL_INACTIVE,
                  AllocHandle::SMALL_ACTIVE)) {
    ATOMIC_INC(&small_pool_active_cnt_);
    // set_frozen() can race with the first successful small allocation.
    // Recheck the list state so a late mark cannot leave a frozen handle in
    // the allocator's active-small count.
    if (!handle.is_active()) {
      retire_small_active(handle);
    }
  }
}

void ObMemstoreAllocator::promote(AllocHandle& handle)
{
  int64_t old_state = ATOMIC_LOAD(&handle.small_pool_state_);
  while (AllocHandle::SMALL_PROMOTED != old_state) {
    int64_t prev_state = ATOMIC_VCAS(&handle.small_pool_state_, old_state, AllocHandle::SMALL_PROMOTED);
    if (old_state == prev_state) {
      if (AllocHandle::SMALL_ACTIVE == old_state) {
        ATOMIC_DEC(&small_pool_active_cnt_);
      }
      ATOMIC_INC(&promoted_cnt_);
      ATOMIC_INC(&total_promoted_cnt_);
      break;
    }
    old_state = prev_state;
  }
}

void ObMemstoreAllocator::retire_small_active(AllocHandle& handle)
{
  if (ATOMIC_BCAS(&handle.small_pool_state_, AllocHandle::SMALL_ACTIVE, AllocHandle::SMALL_INACTIVE)) {
    ATOMIC_DEC(&small_pool_active_cnt_);
  }
}

bool ObMemstoreAllocator::should_promote(const int64_t used,
                                         const int64_t alloc_size,
                                         const int64_t promote_threshold)
{
  return 0 == promote_threshold ||                // 1. threshold is 0, meaning always promote
         used >= promote_threshold ||             // 2. existing small-arena usage has reached the threshold
         alloc_size >= promote_threshold - used;  // 3. this allocation would reach or cross the threshold
}

void ObMemstoreAllocator::repair_small_pool_scan_cursor(AllocHandle& handle)
{
  if (small_pool_scan_cursor_ == &handle.active_list_) {
    small_pool_scan_cursor_ = hlist_.prev_active(small_pool_scan_cursor_);
  }
}

void ObMemstoreAllocator::init_handle(AllocHandle& handle)
{
  handle.do_reset();
  handle.set_host(this);
  {
    int64_t nway = nway_per_group();
    LockGuard guard(lock_);
    hlist_.init_handle(handle);
    arena_.update_nway_per_group(nway);
    // ObMemstoreSmallFifoArenaSpec caps the small arena at four ways.
    small_arena_.update_nway_per_group(nway);
  }
  COMMON_LOG(TRACE, "MTALLOC.init", KP(&handle.mt_));
}

void ObMemstoreAllocator::destroy_handle(AllocHandle& handle)
{
  ObTimeGuard time_guard("ObMemstoreAllocator::destroy_handle", 100 * 1000);
  COMMON_LOG(TRACE, "MTALLOC.destroy", KP(&handle.mt_));
  const int64_t old_word = ATOMIC_TAS(&handle.acct_word_, 0);
  const int64_t old_size = AllocHandle::acct_size(old_word);
  retire_small_active(handle);
  arena_.free(handle.arena_handle_);
  small_arena_.free(handle.small_arena_handle_);
  if (old_size > 0) {
    switch (AllocHandle::acct_state(old_word)) {
      case AllocHandle::ACCT_FROZEN:
        ATOMIC_FAA(&frozen_used_, -old_size);
        break;
      case AllocHandle::ACCT_MERGING:
        ATOMIC_FAA(&merging_used_, -old_size);
        break;
      case AllocHandle::ACCT_RELEASED:
        ATOMIC_FAA(&released_used_, -old_size);
        break;
      default:
        break;
    }
  }
  time_guard.click();
  {
    LockGuard guard(lock_);
    time_guard.click();
    repair_small_pool_scan_cursor(handle);
    hlist_.destroy_handle(handle);
    // hlist_.destroy_handle() makes the handle inactive first. A racing
    // mark_small_active() either completes before this exchange, or observes
    // the inactive handle in its post-check and retires itself.
    const int64_t old_small_pool_state = ATOMIC_TAS(&handle.small_pool_state_, AllocHandle::SMALL_INACTIVE);
    if (AllocHandle::SMALL_ACTIVE == old_small_pool_state) {
      ATOMIC_DEC(&small_pool_active_cnt_);
    } else if (AllocHandle::SMALL_PROMOTED == old_small_pool_state) {
      ATOMIC_DEC(&promoted_cnt_);
    }
    time_guard.click();
    if (hlist_.is_empty()) {
      arena_.reset();
      small_arena_.reset();
      reset_page_churn_sample();
      // every handle retires its accounting before removing itself from
      // hlist_, so once the list is empty any residual value is drift and
      // can be cleared to let the counters self-heal
      ATOMIC_STORE(&frozen_used_, 0);
      ATOMIC_STORE(&merging_used_, 0);
      ATOMIC_STORE(&released_used_, 0);
      ATOMIC_STORE(&small_pool_active_cnt_, 0);
      ATOMIC_STORE(&promoted_cnt_, 0);
      small_pool_scan_cursor_ = nullptr;
    }
    time_guard.click();
  }
  handle.do_reset();
}

void* ObMemstoreAllocator::alloc(AllocHandle& handle, int64_t size, const int64_t expire_ts)
{
  int ret = OB_SUCCESS;
  int64_t align_size = upper_align(size, sizeof(int64_t));
  uint64_t tenant_id = arena_.get_tenant_id();

  bool is_out_of_mem = false;
  if (!handle.is_id_valid()) {
    COMMON_LOG(TRACE, "MTALLOC.first_alloc", KP(&handle.mt_));
    LockGuard guard(lock_);
    if (handle.is_frozen()) {
      ret = OB_EAGAIN;
      if (!handle.mt_.get_offlined()) {
        COMMON_LOG(ERROR, "cannot alloc because allocator is frozen", K(ret), K(handle.mt_));
      } else {
        COMMON_LOG(WARN, "cannot alloc because allocator is frozen", K(ret), K(handle.mt_));
      }
    } else if (!handle.is_id_valid()) {
      handle.set_clock(arena_.retired() + small_arena_.retired());
      hlist_.set_active(handle);
    }
  }

  if (OB_SUCC(ret)) {
    storage::ObTenantFreezer *freezer = nullptr;
    if (is_virtual_tenant_id(tenant_id)) {
      // virtual tenant should not have memstore.
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "virtual tenant should not have memstore", K(ret), K(tenant_id));
    } else if (FALSE_IT(freezer = MTL(storage::ObTenantFreezer *))) {
    } else if (OB_FAIL(freezer->check_memstore_full_internal(is_out_of_mem))) {
      COMMON_LOG(ERROR, "fail to check tenant out of mem limit", K(ret), K(tenant_id));
    }
  }

  void *res = nullptr;
  if (OB_FAIL(ret) || is_out_of_mem) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      STORAGE_LOG(WARN, "this tenant is already out of memstore limit or some thing wrong.", K(tenant_id));
    }
    res = nullptr;
  } else {
    bool is_throttled = false;
    (void)throttle_tool_->alloc_resource<ObMemstoreAllocator>(align_size, expire_ts, is_throttled);
    if (is_throttled) {
      share::memstore_throttled_alloc() += align_size;
    }
    const bool enable_small_pool = ATOMIC_LOAD(&enable_small_pool_);
    if (!enable_small_pool) {
      res = arena_.alloc(handle.id_, handle.arena_handle_, align_size);
    } else {
      int64_t small_pool_state = handle.get_small_pool_state();
      if (AllocHandle::SMALL_PROMOTED != small_pool_state) {
        const int64_t promote_threshold = ATOMIC_LOAD(&promote_threshold_);
        if (should_promote(handle.small_arena_handle_.get_used(), align_size, promote_threshold)) {
          promote(handle);
        }
        // Another allocation may promote this handle concurrently, so reload
        // the state before deciding which arena serves this allocation.
        small_pool_state = handle.get_small_pool_state();
      }
      if (AllocHandle::SMALL_PROMOTED == small_pool_state) {
        res = arena_.alloc(handle.id_, handle.arena_handle_, align_size);
      } else {
        res = small_arena_.alloc(handle.id_, handle.small_arena_handle_, align_size);
        if (nullptr != res) {
          mark_small_active(handle);
        }
      }
    }
  }
  return res;
}

void ObMemstoreAllocator::set_frozen(AllocHandle& handle)
{
  COMMON_LOG(TRACE, "MTALLOC.set_frozen", KP(&handle.mt_));
  if (handle.is_active()) {
    const int64_t used = handle.arena_handle_.get_used() + handle.small_arena_handle_.get_used();
    const int64_t old_word = ATOMIC_LOAD(&handle.acct_word_);
    if (AllocHandle::ACCT_NONE == AllocHandle::acct_state(old_word)) {
      const int64_t new_word = AllocHandle::make_acct_word(AllocHandle::ACCT_FROZEN, used);
      if (ATOMIC_BCAS(&handle.acct_word_, old_word, new_word) && used > 0) {
        ATOMIC_FAA(&frozen_used_, used);
      }
    }
  }
  {
    LockGuard guard(lock_);
    repair_small_pool_scan_cursor(handle);
    hlist_.set_frozen(handle);
    retire_small_active(handle);
  }
}

void ObMemstoreAllocator::set_merging(AllocHandle& handle)
{
  // refresh the accounted size to the current used, since late in-flight
  // writes may have enlarged the memtable after the frozen snapshot
  const int64_t cur_used = handle.arena_handle_.get_used() + handle.small_arena_handle_.get_used();
  const int64_t new_word = AllocHandle::make_acct_word(AllocHandle::ACCT_MERGING, cur_used);
  int64_t old_word = ATOMIC_LOAD(&handle.acct_word_);
  while (AllocHandle::ACCT_FROZEN == AllocHandle::acct_state(old_word)) {
    const int64_t prev = ATOMIC_VCAS(&handle.acct_word_, old_word, new_word);
    if (prev == old_word) {
      const int64_t frozen_size = AllocHandle::acct_size(old_word);
      if (frozen_size > 0) {
        ATOMIC_FAA(&frozen_used_, -frozen_size);
      }
      if (cur_used > 0) {
        ATOMIC_FAA(&merging_used_, cur_used);
      }
      break;
    }
    old_word = prev;
  }
}

void ObMemstoreAllocator::unset_merging(AllocHandle& handle)
{
  int64_t old_word = ATOMIC_LOAD(&handle.acct_word_);
  while (AllocHandle::ACCT_MERGING == AllocHandle::acct_state(old_word)) {
    const int64_t size = AllocHandle::acct_size(old_word);
    const int64_t new_word = AllocHandle::make_acct_word(AllocHandle::ACCT_FROZEN, size);
    const int64_t prev = ATOMIC_VCAS(&handle.acct_word_, old_word, new_word);
    if (prev == old_word) {
      if (size > 0) {
        ATOMIC_FAA(&merging_used_, -size);
        ATOMIC_FAA(&frozen_used_, size);
      }
      break;
    }
    old_word = prev;
  }
}

void ObMemstoreAllocator::set_released(AllocHandle& handle)
{
  int64_t old_word = ATOMIC_LOAD(&handle.acct_word_);
  while (AllocHandle::ACCT_RELEASED != AllocHandle::acct_state(old_word)) {
    const int64_t size = AllocHandle::acct_size(old_word);
    const int64_t new_word = AllocHandle::make_acct_word(AllocHandle::ACCT_RELEASED, size);
    const int64_t prev = ATOMIC_VCAS(&handle.acct_word_, old_word, new_word);
    if (prev == old_word) {
      if (size > 0) {
        const int64_t old_state = AllocHandle::acct_state(old_word);
        if (AllocHandle::ACCT_FROZEN == old_state) {
          ATOMIC_FAA(&frozen_used_, -size);
        } else if (AllocHandle::ACCT_MERGING == old_state) {
          ATOMIC_FAA(&merging_used_, -size);
        }
        ATOMIC_FAA(&released_used_, size);
      }
      break;
    }
    old_word = prev;
  }
}

static int64_t calc_nway(int64_t cpu, int64_t mem)
{
  return std::min(cpu, mem/20/common::ObMemstoreFifoArenaSpec::PAGE_SIZE);
}

int64_t ObMemstoreAllocator::nway_per_group()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arena_.get_tenant_id();
  double min_cpu = 0;
  double max_cpu = 0;
  int64_t max_memory = 0;
  int64_t min_memory = 0;
  omt::ObMultiTenant *omt = GCTX.omt_;

  MTL_SWITCH(tenant_id) {
    storage::ObTenantFreezer *freezer = nullptr;
    if (NULL == omt) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "omt should not be null", K(tenant_id), K(ret));
    } else if (OB_FAIL(omt->get_tenant_cpu(tenant_id, min_cpu, max_cpu))) {
      COMMON_LOG(WARN, "get tenant cpu failed", K(tenant_id), K(ret));
    } else if (FALSE_IT(freezer = MTL(storage::ObTenantFreezer *))) {
    } else if (OB_FAIL(freezer->get_tenant_mem_limit(min_memory, max_memory))) {
      COMMON_LOG(WARN, "get tenant mem limit failed", K(tenant_id), K(ret));
    }
  }
  return OB_SUCCESS == ret? calc_nway((int64_t)max_cpu, min_memory): 0;
}

int ObMemstoreAllocator::set_memstore_threshold()
{
  int ret = OB_SUCCESS;
  bool enable_small_pool = false;
  int64_t promote_threshold = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    enable_small_pool = tenant_config->_enable_memstore_small_pool;
    promote_threshold = tenant_config->_memstore_small_pool_promote_threshold;
  } else {
    enable_small_pool = ATOMIC_LOAD(&enable_small_pool_);
    promote_threshold = ATOMIC_LOAD(&promote_threshold_);
  }
  if (promote_threshold < 0) {
    ret = OB_INVALID_CONFIG;
    COMMON_LOG_RET(WARN, ret, "invalid small pool promote threshold", K(promote_threshold));
  } else {
    {
      LockGuard guard(lock_);
      ret = set_memstore_threshold_without_lock();
    }
    apply_small_pool_config(enable_small_pool, promote_threshold);
  }
  return ret;
}

int ObMemstoreAllocator::set_memstore_threshold_without_lock()
{
  int ret = OB_SUCCESS;
  int64_t memstore_threshold = INT64_MAX;

  storage::ObTenantFreezer *freezer = nullptr;
  if (FALSE_IT(freezer = MTL(storage::ObTenantFreezer *))) {
  } else if (OB_FAIL(freezer->get_tenant_memstore_limit(memstore_threshold))) {
    COMMON_LOG(WARN, "failed to get_tenant_memstore_limit", K(ret));
  } else {
    throttle_tool_->set_resource_limit<ObMemstoreAllocator>(memstore_threshold);
  }

  return ret;
}

void ObMemstoreAllocator::apply_small_pool_config(const bool enable_small_pool,
                                                  const int64_t promote_threshold)
{
  // This runs on every periodic tenant-config refresh. Do not rewrite the
  // routing cache line when the values are unchanged.
  if (promote_threshold != ATOMIC_LOAD(&promote_threshold_)) {
    ATOMIC_STORE(&promote_threshold_, promote_threshold);
  }
  if (!enable_small_pool) {
    if (ATOMIC_LOAD(&enable_small_pool_)) {
      ATOMIC_STORE(&enable_small_pool_, false);
    }
    // An in-flight allocation may have observed the old enabled value and
    // installed a cached Page after the first disable-side cleanup. Config
    // refresh is periodic, so keep retiring cached Pages while disabled
    // instead of adding another atomic check to every small allocation.
    // Retiring the cache does not invalidate data already returned to a
    // memtable: its handle reference keeps that Page alive.
    // Most tenants never enable the small pool. Avoid walking its cache on
    // every periodic config refresh when it has never held a Page. If an
    // in-flight allocation creates a Page after this check, a later refresh
    // observes the non-zero hold and retires it.
    if (small_arena_.hold() > 0) {
      LockGuard guard(lock_);
      if (small_arena_.hold() > 0) {
        // Serialize with reset() and cache shrinking. retire_cached_pages()
        // unpublishes cached Pages before waiting for in-flight readers. The
        // rare WaitQuiescent may briefly delay operations waiting for lock_.
        small_arena_.retire_cached_pages();
      }
    }
  } else {
    if (!ATOMIC_LOAD(&enable_small_pool_)) {
      ATOMIC_STORE(&enable_small_pool_, true);
    }
  }
}

int64_t ObMemstoreAllocator::resource_unit_size()
{
  static const int64_t MEMSTORE_RESOURCE_UNIT_SIZE = 2LL * 1024LL * 1024LL; /* 2MB */
  return MEMSTORE_RESOURCE_UNIT_SIZE;
}

void ObMemstoreAllocator::init_throttle_config(int64_t &resource_limit,
                                               int64_t &trigger_percentage,
                                               int64_t &max_duration)
{
  // define some default value
  const int64_t MEMSTORE_THROTTLE_TRIGGER_PERCENTAGE = 60;
  const int64_t MEMSTORE_THROTTLE_MAX_DURATION = 2LL * 60LL * 60LL * 1000LL * 1000LL;  // 2 hours

  int64_t total_memory = lib::get_tenant_memory_limit(MTL_ID());

  // Use tenant config to init throttle config
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    trigger_percentage = tenant_config->writing_throttling_trigger_percentage;
    max_duration = tenant_config->writing_throttling_maximum_duration;
  } else {
    COMMON_LOG_RET(WARN, OB_INVALID_CONFIG, "init throttle config with default value");
    trigger_percentage = MEMSTORE_THROTTLE_TRIGGER_PERCENTAGE;
    max_duration = MEMSTORE_THROTTLE_MAX_DURATION;
  }
  resource_limit = total_memory * MTL(storage::ObTenantFreezer *)->get_memstore_limit_percentage() / 100;
}

void ObMemstoreAllocator::adaptive_update_limit(const int64_t tenant_id,
                                                const int64_t holding_size,
                                                const int64_t config_specify_resource_limit,
                                                int64_t &resource_limit,
                                                int64_t &last_update_limit_ts,
                                                bool &is_updated)
{
  // do nothing
}

void ObMemstoreAllocator::reset_page_churn_sample()
{
  churn_sample_.last_ts_us_ = 0;
  churn_sample_.last_allocated_ = 0;
  churn_sample_.last_reclaimed_ = 0;
  ATOMIC_STORE(&page_create_rate_, 0);
  ATOMIC_STORE(&page_reclaim_rate_, 0);
}

void ObMemstoreAllocator::sample_page_churn()
{
  // reset_page_churn_sample() is called under the same lock when the last
  // memstore handle is destroyed; protect the non-atomic sampling baseline.
  LockGuard guard(lock_);
  const int64_t now = ObTimeUtility::fast_current_time();
  const int64_t cur_alloc = get_memstore_allocated_pos();
  const int64_t cur_reclaim = get_memstore_reclaimed_pos();
  const int64_t last_ts = churn_sample_.last_ts_us_;
  if (last_ts > 0 && now > last_ts) {
    const int64_t dt_us = now - last_ts;
    const int64_t alloc_delta = cur_alloc - churn_sample_.last_allocated_;
    const int64_t reclaim_delta = cur_reclaim - churn_sample_.last_reclaimed_;
    const int64_t create_rate = alloc_delta > 0 ? alloc_delta * 1000000LL / dt_us : 0;
    const int64_t reclaim_rate = reclaim_delta > 0 ? reclaim_delta * 1000000LL / dt_us : 0;
    ATOMIC_STORE(&page_create_rate_, create_rate);
    ATOMIC_STORE(&page_reclaim_rate_, reclaim_rate);
  } else {
    ATOMIC_STORE(&page_create_rate_, 0);
    ATOMIC_STORE(&page_reclaim_rate_, 0);
  }
  churn_sample_.last_ts_us_ = now;
  churn_sample_.last_allocated_ = cur_alloc;
  churn_sample_.last_reclaimed_ = cur_reclaim;
}

};  // namespace share
};  // namespace oceanbase
