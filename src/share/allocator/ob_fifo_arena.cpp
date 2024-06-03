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
#include "lib/alloc/alloc_struct.h"
#include "lib/stat/ob_diagnose_info.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "share/ob_tenant_mgr.h"

using namespace oceanbase::lib;
using namespace oceanbase::omt;
using namespace oceanbase::share;
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

int ObFifoArena::init()
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
    attr_.tenant_id_ = MTL_ID();
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

}; // end namespace allocator
}; // end namespace oceanbase
