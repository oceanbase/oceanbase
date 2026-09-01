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
#ifdef OB_USE_ASAN
#include "ob_fifo_arena.h"
#include <malloc.h>
#endif
#include "share/allocator/ob_shared_memory_allocator_mgr.h"

using namespace oceanbase::lib;
using namespace oceanbase::omt;
using namespace oceanbase::share;
namespace oceanbase
{
namespace common
{
#define myassert(x) if (!x) { ob_abort(); }
int64_t ObFifoArenaBase::total_hold_ = 0;

int64_t ObFifoArenaBase::Page::get_actual_hold_size()
{
#ifdef OB_USE_ASAN
  return malloc_usable_size(this);
#else
  //every time of alloc_page, ruturn a chunk actually
  return ObTenantCtxAllocator::get_obj_hold(this);
#endif
}

template <typename Spec>
int ObFifoArena<Spec>::init()
{
  int ret = OB_SUCCESS;
  lib::ObMallocAllocator *allocator = lib::ObMallocAllocator::get_instance();
  uint64_t ctx_id = ObCtxIds::MEMSTORE_CTX_ID;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "fifo arena init twice", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INIT_FAIL;
    OB_LOG(ERROR, "mallocator instance is NULLL", K(ret));
  } else {
    allocator_ = allocator;
  }

  if (OB_SUCC(ret)) {
    attr_.tenant_id_ = MTL_ID();
    attr_.label_ = ObNewModIds::OB_MEMSTORE;
    attr_.ctx_id_ = ctx_id;
    is_inited_ = true;
  }
  return ret;
}

template <typename Spec>
void ObFifoArena<Spec>::reset()
{
  COMMON_LOG(INFO, "MTALLOC.reset", "tenant_id", get_tenant_id());
  shrink_cached_page(0);
  carved_counter_.reset();
}

template <typename Spec>
void ObFifoArena<Spec>::retire_cached_pages()
{
  Page* retired_pages[Spec::MAX_CACHED_PAGE_COUNT];
  int64_t retired_page_count = 0;
  for (int64_t group_id = 0; group_id < Spec::MAX_CACHED_GROUP_COUNT; ++group_id) {
    for (int64_t way_id = 0; way_id < Spec::MAX_NWAY; ++way_id) {
      const int64_t i = group_id * Handle::MAX_NWAY + way_id;
      Page* page = ATOMIC_TAS(cur_pages_ + i, static_cast<Page*>(NULL));
      if (NULL != page) {
        retired_pages[retired_page_count++] = page;
      }
    }
  }
  if (retired_page_count > 0) {
    // A concurrent allocation may have loaded a cached Page before it was
    // unpublished. Wait until all such readers have either installed their
    // handle Ref or left the allocation path before releasing the cache Ref.
    WaitQuiescent(get_qs());
    for (int64_t i = 0; i < retired_page_count; ++i) {
      Page* page = retired_pages[i];
      Ref* ref = page->frozen();
      if (NULL != ref) {
        IGNORE_RETURN ATOMIC_FAA(&retired_, page->get_actual_hold_size());
        release_ref(ref);
      }
    }
  }
}

template <typename Spec>
void ObFifoArena<Spec>::update_nway_per_group(int64_t nway)
{
  if (nway <= 0) {
    nway = 1;
  } else if (nway > Spec::MAX_NWAY) {
    nway = Spec::MAX_NWAY;
  }

  const int64_t old_nway = ATOMIC_LOAD(&nway_);
  if (nway > old_nway) {
    ATOMIC_STORE(&nway_, nway);
  } else if (nway < old_nway) {
    ATOMIC_STORE(&nway_, nway);
    WaitQuiescent(get_qs());
    shrink_cached_page(nway);
  }
}

template <typename Spec>
void ObFifoArena<Spec>::shrink_cached_page(int64_t nway)
{
  for (int64_t group_id = 0; group_id < Spec::MAX_CACHED_GROUP_COUNT; ++group_id) {
    for (int64_t way_id = nway; way_id < Spec::MAX_NWAY; ++way_id) {
      const int64_t i = group_id * Handle::MAX_NWAY + way_id;
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

template <typename Spec>
void* ObFifoArena<Spec>::alloc(int64_t adv_idx, Handle& handle, int64_t size)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  int64_t rsize = 0;
  int64_t nway = 0;
  if (!is_inited_) {
    COMMON_LOG(WARN, "fifo arena is not initialized", K(adv_idx), K(size));
    ret = OB_NOT_INIT;
  } else if (adv_idx < 0 ||
             size < 0 ||
             size > INT64_MAX - static_cast<int64_t>(sizeof(Page) + sizeof(Ref))) {
    COMMON_LOG(WARN, "invalid argument", K(adv_idx), K(size));
    ret = OB_INVALID_ARGUMENT;
  } else {
    rsize = size + sizeof(Page) + sizeof(Ref);
    CriticalGuard(get_qs());
    nway = ATOMIC_LOAD(&nway_);
    if (nway <= 0 || nway > Spec::MAX_NWAY) {
      COMMON_LOG(WARN, "invalid fifo arena nway", K(nway), K(adv_idx), K(size));
      ret = OB_INVALID_ARGUMENT;
    } else {
      int64_t way_id = get_way_id(nway);
      int64_t idx = get_idx(adv_idx, way_id);
      if (idx < 0 || idx >= Spec::MAX_CACHED_PAGE_COUNT) {
        COMMON_LOG(ERROR, "fifo arena cache index out of range", K(idx), K(adv_idx), K(way_id));
        ret = OB_ERR_UNEXPECTED;
      } else if (rsize > Spec::PAGE_SIZE) {
        Page* page = NULL;
        if (NULL == (page = alloc_page(rsize))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          bool need_switch = false;
          handle.add_allocated(page->get_actual_hold_size());
          ptr = handle.ref_and_alloc(way_id, need_switch, page, size);
          if (NULL != ptr) {
            carved_counter_.inc(size + sizeof(Ref));
          }
          page->frozen();
          retire_page(way_id, handle, page);
        }
      } else {
        Page** paddr = cur_pages_ + idx;
        while (OB_SUCC(ret) && NULL == ptr) {
          Page* page = NULL;
          bool need_switch = false;
          if (NULL != (page = ATOMIC_LOAD(paddr))) {
            Ref* ref = handle.get_match_ref(way_id, page);
            if (NULL != ref) {
              ptr = handle.alloc(need_switch, ref, page, size);
              if (NULL != ptr) {
                carved_counter_.inc(size);
              }
            } else {
              LockGuard guard(handle.lock_);
              if (NULL == (ref = handle.get_match_ref(way_id, page))) {
                ptr = handle.ref_and_alloc(way_id, need_switch, page, size);
                if (NULL != ptr) {
                  carved_counter_.inc(size + sizeof(Ref));
                }
              }
            }
          }
          if (NULL == page || need_switch) {
            Page* new_page = NULL;
            int64_t alloc_size = Spec::PAGE_SIZE;
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
    }
  }
  return ptr;
}

template <typename Spec>
void ObFifoArena<Spec>::release_ref(Ref* ref)
{
  if (ref != &ref->page_->self_ref_) {
    const int64_t ref_alloc = ATOMIC_LOAD(&ref->allocated_);
    if (ref_alloc > 0) {
      carved_counter_.dec(ref_alloc);
    }
  }
  if (0 == ref->page_->xref(ref->allocated_)) {
    free_page(ref->page_);
  }
}

template <typename Spec>
void ObFifoArena<Spec>::free(Handle& handle)
{
  bool wait_qs_done = false;
  for(int i = 0; i < Spec::MAX_NWAY; i++) {
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

template <typename Spec>
typename ObFifoArena<Spec>::Page* ObFifoArena<Spec>::alloc_page(int64_t size)
{
  Page* page = (Page*)allocator_->alloc(size, attr_);
  if (NULL != page) {
    const int64_t hold_size = page->get_actual_hold_size();
    ATOMIC_FAA(&allocated_, hold_size);
    ATOMIC_FAA(&total_hold_, hold_size);
    ATOMIC_AAF(&hold_, hold_size);
    page->set(size);
  } else {
    ATOMIC_INC(&page_alloc_fail_cnt_);
  }
  return page;
}

template <typename Spec>
void ObFifoArena<Spec>::free_page(Page* page)
{
  if (NULL != page && NULL != allocator_) {
    const int64_t hold_size = page->get_actual_hold_size();
    ATOMIC_FAA(&reclaimed_, hold_size);
    ATOMIC_FAA(&total_hold_, -hold_size);
    ATOMIC_FAA(&hold_, -hold_size);
    allocator_->free(page);
  }
}

template <typename Spec>
void ObFifoArena<Spec>::retire_page(int64_t idx, Handle& handle, Page* page)
{
  if (NULL != page) {
    ATOMIC_FAA(&retired_, page->get_actual_hold_size());
    handle.add_ref(idx, &page->self_ref_);
  }
}

template <typename Spec>
void ObFifoArena<Spec>::destroy_page(Page* page)
{
  if (NULL != page && NULL != allocator_) {
    const int64_t hold_size = page->get_actual_hold_size();
    ATOMIC_FAA(&allocated_, -hold_size);
    ATOMIC_FAA(&total_hold_, -hold_size);
    ATOMIC_FAA(&hold_, -hold_size);
    allocator_->free(page);
  }
}

template class ObFifoArena<ObMemstoreFifoArenaSpec>;
template class ObFifoArena<ObMemstoreSmallFifoArenaSpec>;

}; // end namespace allocator
}; // end namespace oceanbase
