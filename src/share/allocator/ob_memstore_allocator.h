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

#ifndef OCEANBASE_ALLOCATOR_OB_GMEMSTORE_ALLOCATOR_H_
#define OCEANBASE_ALLOCATOR_OB_GMEMSTORE_ALLOCATOR_H_
#include "ob_handle_list.h"
#include "ob_fifo_arena.h"
#include "lib/lock/ob_spin_lock.h"
#include "share/throttle/ob_share_throttle_define.h"

namespace oceanbase
{
namespace memtable
{
class ObMemtable;
};

namespace share
{

// record the throttled alloc size of memstore in this thread
OB_INLINE int64_t &memstore_throttled_alloc()
{
  RLOCAL_INLINE(int64_t, throttled_alloc);
  return throttled_alloc;
}

struct FrozenMemstoreInfoLogger
{
  FrozenMemstoreInfoLogger(char* buf, int64_t limit): buf_(buf), limit_(limit), pos_(0) {}
  ~FrozenMemstoreInfoLogger() {}
  int operator()(ObDLink* link);
  char* buf_;
  int64_t limit_;
  int64_t pos_;
};

struct ActiveMemstoreInfoLogger
{
  ActiveMemstoreInfoLogger(char* buf, int64_t limit): buf_(buf), limit_(limit), pos_(0) {}
  ~ActiveMemstoreInfoLogger() {}
  int operator()(ObDLink* link);
  char* buf_;
  int64_t limit_;
  int64_t pos_;
};


class ObMemstoreAllocator
{
public:
  DEFINE_CUSTOM_FUNC_FOR_THROTTLE(Memstore);

  typedef ObSpinLock Lock;
  typedef ObSpinLockGuard LockGuard;
  typedef ObMemstoreAllocator GAlloc;
  typedef common::ObMemstoreFifoArena Arena;
  typedef common::ObMemstoreSmallFifoArena SmallArena;
  typedef ObHandleList HandleList;
  typedef HandleList::Handle ListHandle;
  typedef Arena::Handle ArenaHandle;

  class AllocHandle: public ListHandle, public ObIAllocator
  {
  public:
    // which of frozen_used_/merging_used_/released_used_ currently holds this
    // handle's contribution; packed with the contributed bytes into acct_word_
    // so that state and size always change together in one CAS
    enum AcctState : int64_t {
      ACCT_NONE = 0,
      ACCT_FROZEN = 1,
      ACCT_MERGING = 2,
      ACCT_RELEASED = 3,
    };

    // Tracks both the allocation route and eligibility for active small-pool
    // batch freeze; it does not indicate whether small_arena_handle_ owns
    // memory. SMALL_INACTIVE may still hold small-arena allocations briefly
    // before the first allocation is marked, or after the memtable is frozen.
    // SMALL_PROMOTED remains unchanged after freeze so in-flight writes keep
    // allocating from the large arena. Existing small-arena allocations stay
    // valid until small_arena_handle_ is released with the memtable.
    enum SmallPoolState : int64_t {
      SMALL_INACTIVE = 0, // not tracked as an active small-pool freeze candidate
      SMALL_ACTIVE = 1,   // active memtable eligible for small-pool batch freeze
      SMALL_PROMOTED = 2, // route subsequent allocations to the large arena until destroy
    };
    static const int64_t ACCT_STATE_BITS = 2;
    static int64_t make_acct_word(const int64_t state, const int64_t size)
    { return (size << ACCT_STATE_BITS) | state; }
    static int64_t acct_state(const int64_t word)
    { return word & ((1LL << ACCT_STATE_BITS) - 1); }
    static int64_t acct_size(const int64_t word)
    { return word >> ACCT_STATE_BITS; }
  public:
    memtable::ObMemtable& mt_;
    GAlloc* host_;
    ArenaHandle arena_handle_;
    ArenaHandle small_arena_handle_;
    int64_t small_pool_state_;
    AllocHandle(memtable::ObMemtable& mt): mt_(mt), host_(NULL) {
      do_reset();
    }
    void do_reset() {
      ListHandle::reset();
      arena_handle_.reset();
      small_arena_handle_.reset();
      small_pool_state_ = SMALL_INACTIVE;
      host_ = NULL;
      acct_word_ = 0;
    }
    int64_t get_group_id() const { return id_ < 0? INT64_MAX: (id_ % Arena::DEFAULT_MAX_CACHED_GROUP_COUNT); }
    int init();
    void set_host(GAlloc* host) { host_ = host; }
    void destroy() {
      if (NULL != host_) {
        host_->destroy_handle(*this);
        host_ = NULL;
      }
    }
    int64_t get_protection_clock() const { return get_clock(); }
    int64_t get_retire_clock() const
    {
      int64_t retire_clock = INT64_MAX;
      if (NULL != host_) {
        retire_clock = host_->get_retire_clock();
      }
      return retire_clock;
    }
    int64_t get_size() const { return arena_handle_.get_allocated() + small_arena_handle_.get_allocated(); }
    int64_t get_occupied_size() const { return get_size(); }
    int64_t get_small_pool_state() const { return ATOMIC_LOAD(&small_pool_state_); }
    bool is_promoted() const { return SMALL_PROMOTED == get_small_pool_state(); }
    void* alloc(const int64_t size) {
      return NULL == host_? NULL: host_->alloc(*this, size);
    }
    void* alloc(const int64_t size, const ObMemAttr &attr)
    {
      UNUSEDx(attr);
      return alloc(size);
    }
    void free(void* ptr) {
      UNUSED(ptr);
    }
    void set_frozen() {
      if (NULL != host_) {
        host_->set_frozen(*this);
      }
    }
    void set_merging() {
      if (NULL != host_) {
        host_->set_merging(*this);
      }
    }
    void unset_merging() {
      if (NULL != host_) {
        host_->unset_merging(*this);
      }
    }
    void set_released() {
      if (NULL != host_) {
        host_->set_released(*this);
      }
    }
    INHERIT_TO_STRING_KV("ListHandle", ListHandle, KP_(host), K_(arena_handle),  K_(small_arena_handle), K_(small_pool_state));
    int64_t acct_word_; // access with ATOMIC_* only
  };

public:
  ObMemstoreAllocator()
      : throttle_tool_(nullptr), lock_(common::ObLatchIds::MEMSTORE_ALLOCATOR_LOCK),
        hlist_(), arena_(), small_arena_(), enable_small_pool_(false),
        promote_threshold_(64 * 1024), small_pool_active_cnt_(0), small_pool_scan_cursor_(nullptr),
        frozen_used_(0), merging_used_(0), released_used_(0),
        page_create_rate_(0), page_reclaim_rate_(0), churn_sample_{0, 0, 0}, promoted_cnt_(0), total_promoted_cnt_(0) {}
  ~ObMemstoreAllocator() {}
public:
  int init();
  int start() { return OB_SUCCESS; }
  void stop() {}
  void wait() {}
  void destroy() {}
  void init_handle(AllocHandle& handle);
  void destroy_handle(AllocHandle& handle);
  void* alloc(AllocHandle& handle, int64_t size, const int64_t expire_ts = 0);
  void set_frozen(AllocHandle& handle);
  void set_merging(AllocHandle& handle);
  void unset_merging(AllocHandle& handle);
  void set_released(AllocHandle& handle);
  template<typename Func>
  int for_each(Func& f, const bool reverse=false) {
    int ret = common::OB_SUCCESS;
    ObDLink* iter = NULL;
    LockGuard guard(lock_);
    while(OB_SUCC(ret) && NULL != (iter = (reverse ? hlist_.prev(iter) : hlist_.next(iter)))) {
      ret = f(iter);
    }
    return ret;
  }
  template<typename Func>
  int scan_small_pool_active_handles(Func& f, const int64_t max_scan_count, int64_t& scanned_count)
  {
    int ret = common::OB_SUCCESS;
    scanned_count = 0;
    LockGuard guard(lock_);
    ObDLink* iter = small_pool_scan_cursor_;
    if (nullptr == iter) {
      iter = hlist_.prev_active(nullptr);
    }

    while (OB_SUCC(ret) && nullptr != iter && scanned_count < max_scan_count) {
      ObDLink* current = iter;
      iter = hlist_.prev_active(current);
      ++ scanned_count;
      ret = f(current);
    }

    ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
    small_pool_scan_cursor_ = iter;
    return ret;
  }

public:
  int64_t get_active_memstore_used() {
    int64_t hazard = hlist_.hazard();
    return  hazard == INT64_MAX? 0: (arena_.allocated() + small_arena_.allocated() - hazard);
  }
  int64_t get_freezable_active_memstore_used() {
    int64_t hazard = hlist_.hazard();
    return  hazard == INT64_MAX? 0: (arena_.retired() + small_arena_.retired() - hazard);
  }
  int64_t get_max_cached_memstore_size() const {
    return arena_.get_max_cached_memstore_size() + small_arena_.get_max_cached_memstore_size();
  }
  int64_t hold() const { return arena_.hold() + small_arena_.hold(); }
  int64_t get_total_memstore_used() const { return arena_.hold() + small_arena_.hold(); }
  int64_t get_total_real_used() const { return arena_.get_carved() + small_arena_.get_carved(); }
  int64_t get_page_alloc_fail_cnt() const {
    return arena_.get_page_alloc_fail_cnt() + small_arena_.get_page_alloc_fail_cnt();
  }
  int64_t get_frozen_used() const { return ATOMIC_LOAD(&frozen_used_); }
  int64_t get_merging_used() const { return ATOMIC_LOAD(&merging_used_); }
  int64_t get_released_used() const { return ATOMIC_LOAD(&released_used_); }
  int64_t get_frozen_memstore_pos() const {
    int64_t hazard = hlist_.hazard();
    return  hazard == INT64_MAX? 0: hazard;
  }
  int64_t get_memstore_reclaimed_pos() const { return arena_.reclaimed() + small_arena_.reclaimed(); }
  int64_t get_memstore_allocated_pos() const { return arena_.allocated() + small_arena_.allocated(); }
  int64_t get_retire_clock() const { return arena_.retired() + small_arena_.retired(); }
  bool is_small_pool_enabled() const { return ATOMIC_LOAD(&enable_small_pool_); }
  int64_t get_large_arena_hold() const { return arena_.hold(); }
  int64_t get_large_arena_real_used() const { return arena_.get_carved(); }
  int64_t get_large_arena_max_cached_size() const { return arena_.get_max_cached_memstore_size(); }
  int64_t get_large_arena_retired_pending_hold() const { return arena_.get_retired_pending_hold(); }
  int64_t get_small_arena_hold() const { return small_arena_.hold(); }
  int64_t get_small_arena_max_cached_size() const { return small_arena_.get_max_cached_memstore_size(); }
  int64_t get_small_pool_active_count() const { return ATOMIC_LOAD(&small_pool_active_cnt_); }
  int64_t get_promoted_cnt() const { return ATOMIC_LOAD(&promoted_cnt_); }
  int64_t get_total_promoted_cnt() const { return ATOMIC_LOAD(&total_promoted_cnt_); }
  int64_t get_small_arena_real_used() const { return small_arena_.get_carved(); }
  int64_t get_small_arena_retired_pending_hold() const { return small_arena_.get_retired_pending_hold(); }
  int64_t get_small_arena_page_alloc_fail_cnt() const { return small_arena_.get_page_alloc_fail_cnt(); }
  void log_frozen_memstore_info(char* buf, int64_t limit) {
    if (NULL != buf && limit > 0) {
      FrozenMemstoreInfoLogger logger(buf, limit);
      buf[0] = 0;
      (void)for_each(logger, true /* reverse  */);
    }
  }
  void log_active_memstore_info(char *buf, int64_t limit) {
    if (NULL != buf && limit > 0) {
      ActiveMemstoreInfoLogger logger(buf, limit);
      buf[0] = 0;
      (void)for_each(logger, true /* reverse */);
    }
  }
  void sample_page_churn();
  int64_t get_page_create_rate() const { return ATOMIC_LOAD(&page_create_rate_); }
  int64_t get_page_reclaim_rate() const { return ATOMIC_LOAD(&page_reclaim_rate_); }

public:
  int set_memstore_threshold();

private:
  void reset_page_churn_sample();
  int64_t nway_per_group();
  int set_memstore_threshold_without_lock();
  void apply_small_pool_config(const bool enable_small_pool, const int64_t promote_threshold);
  void mark_small_active(AllocHandle& handle);
  void promote(AllocHandle& handle);
  void retire_small_active(AllocHandle& handle);
  void repair_small_pool_scan_cursor(AllocHandle& handle);
  static bool should_promote(const int64_t used, const int64_t alloc_size, const int64_t promote_threshold);
private:
  struct ObMemstorePageChurnSample {
    int64_t last_ts_us_;
    int64_t last_allocated_;
    int64_t last_reclaimed_;
  };
  share::TxShareThrottleTool *throttle_tool_;
  Lock lock_;
  HandleList hlist_;
  Arena arena_;
  SmallArena small_arena_;
  bool enable_small_pool_; // single state source for the small allocation route and cache cleanup
  int64_t promote_threshold_;
  int64_t small_pool_active_cnt_;
  ObDLink* small_pool_scan_cursor_;
  int64_t frozen_used_;
  int64_t merging_used_;
  int64_t released_used_;
  int64_t page_create_rate_;  // total bytes/s across both arenas, sampled by ObTenantFreezer timer
  int64_t page_reclaim_rate_; // total bytes/s across both arenas, sampled by ObTenantFreezer timer
  ObMemstorePageChurnSample churn_sample_;
  int64_t promoted_cnt_;
  int64_t total_promoted_cnt_;
};

};     // namespace share
};     // namespace oceanbase

#endif /* OCEANBASE_ALLOCATOR_OB_GMEMSTORE_ALLOCATOR_H_ */
