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

#ifndef OCEANBASE_SHARE_FIFO_ARENA_H_
#define OCEANBASE_SHARE_FIFO_ARENA_H_

#include "share/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/ob_qsync.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/lock/ob_spin_rwlock.h"           // SpinRWLock
#include "lib/metrics/ob_counter.h"
#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace common
{

class ObFifoArenaBase
{
public:
  static int64_t total_hold_;
  struct Page;
  struct Ref
  {
    void set_page(Page* page) {
      next_ = NULL;
      page_ = page;
      allocated_ = 0;
    }
    void add_alloc_size(int64_t size) {
      ATOMIC_FAA(&allocated_, size);
    }
    Ref* next_;
    Page* page_;
    int64_t allocated_; // page xref accounting; also tracks carved bytes for user refs
  };

  struct Page
  {
    void set(int64_t size) {
      self_ref_.set_page(this);
      limit_ = size - sizeof(*this);
      pos_ = 0;
      ref_ = 0;
    }
    int64_t hold() { return limit_ + sizeof(*this); }
    int64_t xref(int64_t x) { return ATOMIC_AAF(&ref_, x); }
    char* alloc(bool& need_switch, int64_t size) {
      char* ret = NULL;
      int64_t pos = 0;
      int64_t limit = ATOMIC_LOAD(&limit_);
      if ((pos = ATOMIC_LOAD(&pos_)) <= limit) {
        pos = ATOMIC_FAA(&pos_, size);
        ret = (pos + size <= limit)? buf_ + pos: NULL;
      }
      need_switch =  pos <= limit && (NULL == ret);
      if (need_switch) {
        self_ref_.add_alloc_size(-pos);
      }
      return ret;
    }
    Ref* frozen() {
      Ref* ref = NULL;
      bool need_switch = false;
      (void)alloc(need_switch, ATOMIC_LOAD(&limit_) + 1);
      if (need_switch) {
        ref = &self_ref_;
      }
      return ref;
    }
    int64_t get_actual_hold_size();
    Ref self_ref_;  // record the allocated bytes from page, include self_ref_ itself
    int64_t limit_; // the max bytes of a page that can be used
    int64_t pos_;   // the position after which can be allocated
    int64_t ref_;
    char buf_[0];
  };
  struct LockGuard
  {
    LockGuard(int64_t& lock): lock_(lock) {
      while(ATOMIC_TAS(&lock_, 1)) {
        PAUSE();
      }
    }
    ~LockGuard() {
      ATOMIC_STORE(&lock_, 0);
    }
    int64_t& lock_;
  };
  struct Handle
  {
    enum { MAX_NWAY = 32 };
    void reset() {
      lock_ = 0;
      memset(ref_, 0, sizeof(ref_));
      allocated_ = 0;
      used_ = 0;
    }
    Ref* get_match_ref(int64_t idx, Page* page) {
      Ref* ref = ATOMIC_LOAD(ref_ + idx);
      if (NULL != ref && page != ref->page_) {
        ref = NULL;
      }
      return ref;
    }
    void* alloc(bool& need_switch, Ref* ref, Page* page, int64_t size) {
      void* ptr = NULL;
      if (NULL != (ptr = page->alloc(need_switch, size))) {
        ref->add_alloc_size(size);
        add_used(size);
      }
      return ptr;
    }
    void* ref_and_alloc(int64_t idx, bool& need_switch, Page* page, int64_t size) {
      void* ptr = NULL;
      Ref* ref = NULL;
      if (NULL != (ref = (Ref*)page->alloc(need_switch, size + sizeof(*ref)))) {
        ref->set_page(page);
        ref->add_alloc_size(size + sizeof(*ref));
        add_used(size + sizeof(*ref));
        add_ref(idx, ref);
        ptr = (void*)(ref + 1);
      }
      return ptr;
    }
    void add_ref(int64_t idx, Ref* ref) {
      Ref* old_ref = ATOMIC_TAS(ref_ + idx, ref);
      ATOMIC_STORE(&ref->next_, old_ref);
    }
    int64_t get_allocated() const { return ATOMIC_LOAD(&allocated_); }
    void add_allocated(int64_t size) { ATOMIC_FAA(&allocated_, size); }
    int64_t get_used() const { return ATOMIC_LOAD(&used_); }
    void add_used(int64_t size) { ATOMIC_FAA(&used_, size); }
    TO_STRING_KV(K_(allocated), K_(used));
    int64_t lock_;
    Ref* ref_[MAX_NWAY];
    int64_t allocated_;  // record all the memory hold by pages, include the size of page structure, AObject and so on.
                         // only increase while a page is created.
    int64_t used_;       // record actual bytes carved from pages.
  };
};

struct ObMemstoreFifoArenaSpec
{
  static const int64_t PAGE_SIZE = OB_MALLOC_BIG_BLOCK_SIZE + sizeof(ObFifoArenaBase::Page) + sizeof(ObFifoArenaBase::Ref);
  static const int64_t MAX_CACHED_GROUP_COUNT = 16;
  static const int64_t MAX_NWAY = ObFifoArenaBase::Handle::MAX_NWAY;
  static const int64_t MAX_CACHED_PAGE_COUNT = MAX_CACHED_GROUP_COUNT * ObFifoArenaBase::Handle::MAX_NWAY;
  static const int64_t EXTRA_CACHED_HOLD = ACHUNK_PRESERVE_SIZE;
};

struct ObMemstoreSmallFifoArenaSpec
{
  static const int64_t PAGE_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE + sizeof(ObFifoArenaBase::Page) + sizeof(ObFifoArenaBase::Ref);
  static const int64_t MAX_CACHED_GROUP_COUNT = 4;
  static const int64_t MAX_NWAY = 4;
  // ObFifoArenaBase::Handle is shared by both arena types and therefore still
  // reserves 32 Ref pointers for every small-arena handle, although the small
  // arena can use only the first four. Specializing the Handle layout could
  // remove this per-memtable space overhead in a follow-up change.
  static const int64_t MAX_CACHED_PAGE_COUNT = MAX_CACHED_GROUP_COUNT * ObFifoArenaBase::Handle::MAX_NWAY;
  static const int64_t EXTRA_CACHED_HOLD = 0;
};

template <typename Spec>
class ObFifoArena : public ObFifoArenaBase
{
public:
  static const int64_t DEFAULT_PAGE_SIZE = Spec::PAGE_SIZE;
  static const int64_t DEFAULT_MAX_CACHED_GROUP_COUNT = Spec::MAX_CACHED_GROUP_COUNT;

  ObFifoArena()
      : allocator_(NULL),
        nway_(0),
        allocated_(0),
        reclaimed_(0),
        hold_(0),
        page_alloc_fail_cnt_(0),
        retired_(0),
        last_reclaimed_(0),
        lastest_memstore_threshold_(0),
        is_inited_(false)
  {
    memset(cur_pages_, 0, sizeof(cur_pages_));
  }
  ~ObFifoArena() { reset(); }
public:
  int init();
  void reset();
  void update_nway_per_group(int64_t nway);
  void* alloc(int64_t idx, Handle& handle, int64_t size);
  void free(Handle& ref);
  // Retire cache self-references only. Pages referenced by handles stay valid
  // until those handles release their references. The caller must serialize
  // this with reset() and update_nway_per_group().
  void retire_cached_pages();
  int64_t allocated() const { return ATOMIC_LOAD(&allocated_); }
  int64_t retired() const { return ATOMIC_LOAD(&retired_); }
  int64_t reclaimed() const { return ATOMIC_LOAD(&reclaimed_); }

  void set_memstore_threshold(int64_t memstore_threshold);
  int64_t hold() const {
    return ATOMIC_LOAD(&hold_);
  }
  int64_t get_carved() const { return carved_counter_.value(); }
  // Retired pages are no longer allocation candidates, but remain held until
  // every handle sharing them has released its page ref.
  int64_t get_retired_pending_hold() const
  {
    // retired_ is increased before reclaimed_ for each page. Read reclaimed_
    // first to avoid combining an old retired_ with a new reclaimed_. This is
    // still an approximate snapshot, so clamp the result defensively.
    const int64_t reclaimed = ATOMIC_LOAD(&reclaimed_);
    const int64_t retired = ATOMIC_LOAD(&retired_);
    return max(0, retired - reclaimed);
  }
  int64_t get_page_alloc_fail_cnt() const { return ATOMIC_LOAD(&page_alloc_fail_cnt_); }
  uint64_t get_tenant_id() const { return attr_.tenant_id_; }
  int64_t get_page_size() const { return Spec::PAGE_SIZE; }
  int64_t get_max_cached_memstore_size() const
  {
    return Spec::MAX_CACHED_GROUP_COUNT * ATOMIC_LOAD(&nway_) * (Spec::PAGE_SIZE + Spec::EXTRA_CACHED_HOLD);
  }

private:
  ObQSync& get_qs() {
    static ObQSync s_qs;
    return s_qs;
  }
  int64_t get_way_id(const int64_t nway) { return icpu_id() % nway; }
  int64_t get_idx(int64_t grp_id, int64_t way_id)
  { return (grp_id % Spec::MAX_CACHED_GROUP_COUNT) * Handle::MAX_NWAY + way_id; }

private:
  void release_ref(Ref* ref);
  Page* alloc_page(int64_t size);
  void free_page(Page* ptr);
  void retire_page(int64_t way_id, Handle& handle, Page* ptr);
  void destroy_page(Page* page);
  void shrink_cached_page(int64_t nway);

private:
  lib::ObMemAttr attr_;
  lib::ObIAllocator *allocator_;

  int64_t nway_;
  int64_t allocated_; // record all the memory hold by pages in history.
                      // increase while a page created and decrease only if a failed page destroyed.
  int64_t reclaimed_; // record all the memory reclaimed by pages in history.
                      // increase while a page freed.
  int64_t hold_;      // record all the memory hold by pages current.
                      // increase while a page created and decrease while a page freed or destroyed.
                      // (may be: hold_ = allocated_ - reclaimed_)
  ObPCCounter carved_counter_; // per-CPU sharded carved bytes inside live pages.
  int64_t page_alloc_fail_cnt_; // cumulative alloc_page() failures from malloc layer
  int64_t retired_;   // record all the memory hold by not active pages in history.

  int64_t last_reclaimed_;
  int64_t lastest_memstore_threshold_;//Save the latest memstore_threshold
  bool is_inited_;
  Page* cur_pages_[Spec::MAX_CACHED_PAGE_COUNT];
  DISALLOW_COPY_AND_ASSIGN(ObFifoArena);
};

typedef ObFifoArena<ObMemstoreFifoArenaSpec> ObMemstoreFifoArena;
typedef ObFifoArena<ObMemstoreSmallFifoArenaSpec> ObMemstoreSmallFifoArena;

}//end of namespace share
}//end of namespace oceanbase

#endif
