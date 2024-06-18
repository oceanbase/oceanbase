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

#define USING_LOG_PREFIX LIB

#include <new>
#include <stdlib.h>
#include <sys/mman.h>
#include "lib/resource/achunk_mgr.h"
#include "lib/utility/utility.h"
#include "lib/allocator/ob_tc_malloc.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/alloc/alloc_failed_reason.h"
#include "lib/alloc/memory_sanity.h"
#include "deps/oblib/src/lib/alloc/malloc_hook.h"

using namespace oceanbase::lib;

int ObLargePageHelper::large_page_type_ = INVALID_LARGE_PAGE_TYPE;

void ObLargePageHelper::set_param(const char *param)
{
  if (OB_NOT_NULL(param)) {
    if (0 == strcasecmp(param, "false")) {
      large_page_type_ = NO_LARGE_PAGE;
    } else if (0 == strcasecmp(param, "true")) {
      large_page_type_ = PREFER_LARGE_PAGE;
    } else if (0 == strcasecmp(param, "only")) {
      large_page_type_ = ONLY_LARGE_PAGE;
    }
    LOG_INFO("set large page param", K(large_page_type_));
  }
}

int ObLargePageHelper::get_type()
{
#ifndef ENABLE_SANITY
  return large_page_type_;
#else
  return NO_LARGE_PAGE;
#endif
}

AChunkMgr &AChunkMgr::instance()
{
  static AChunkMgr mgr;
  return mgr;
}

AChunkMgr::AChunkMgr()
  : limit_(DEFAULT_LIMIT), urgent_(0), hold_(0),
    total_hold_(0), cache_hold_(0), shadow_hold_(0),
    max_chunk_cache_size_(limit_)
{
  // only cache normal_chunk or large_chunk
  for (int i = 0; i < ARRAYSIZEOF(slots_); ++i) {
    new (slots_ + i) Slot();
  }
  slots_[HUGE_ACHUNK_INDEX]->set_max_chunk_cache_size(0);
}

void *AChunkMgr::direct_alloc(const uint64_t size, const bool can_use_huge_page, bool &huge_page_used, const bool alloc_shadow)
{
  common::ObTimeGuard time_guard(__func__, 1000 * 1000);
  int orig_errno = errno;

  void *ptr = nullptr;
  ptr = low_alloc(size, can_use_huge_page, huge_page_used, alloc_shadow);
  if (nullptr != ptr) {
    if (((uint64_t)ptr & (ACHUNK_ALIGN_SIZE - 1)) != 0) {
      // not aligned
      low_free(ptr, size);

      uint64_t new_size = size + ACHUNK_ALIGN_SIZE;
      ptr = low_alloc(new_size, can_use_huge_page, huge_page_used, alloc_shadow);
      if (nullptr != ptr) {
        const uint64_t addr = align_up2((uint64_t)ptr, ACHUNK_ALIGN_SIZE);
        if (addr - (uint64_t)ptr > 0) {
          low_free(ptr, addr - (uint64_t)ptr);
        }
        if (ACHUNK_ALIGN_SIZE - (addr - (uint64_t)ptr) > 0) {
          low_free((void*)(addr + size), ACHUNK_ALIGN_SIZE - (addr - (uint64_t)ptr));
        }
        ptr = (void*)addr;
      }
    } else {
      // aligned address returned
    }
  }
  if (ptr != nullptr) {
    inc_maps(size);
    IGNORE_RETURN ATOMIC_FAA(&total_hold_, size);
  } else {
    LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "low alloc fail", K(size), K(orig_errno), K(errno));
    auto &afc = g_alloc_failed_ctx();
    afc.reason_ = PHYSICAL_MEMORY_EXHAUST;
    afc.alloc_size_ = size;
    afc.errno_ = errno;
  }

  return ptr;
}

void AChunkMgr::direct_free(const void *ptr, const uint64_t size)
{
  common::ObTimeGuard time_guard(__func__, 1000 * 1000);

  inc_unmaps(size);
  IGNORE_RETURN ATOMIC_FAA(&total_hold_, -size);
  low_free(ptr, size);
}

static int64_t global_canonical_addr = SANITY_MIN_CANONICAL_ADDR;

void *AChunkMgr::low_alloc(const uint64_t size, const bool can_use_huge_page, bool &huge_page_used, const bool alloc_shadow)
{
  void *ptr = nullptr;
  huge_page_used = false;
  const int prot = PROT_READ | PROT_WRITE;
  int flags = MAP_PRIVATE | MAP_ANONYMOUS | (SANITY_BOOL_EXPR(alloc_shadow) ? MAP_FIXED : 0);
  int huge_flags = flags;
#ifdef MAP_HUGETLB
  if (OB_LIKELY(can_use_huge_page)) {
    huge_flags = flags | MAP_HUGETLB;
  }
#endif
  // for debug more efficiently
  const int fd = -1234;
  const int offset = 0;
  const int large_page_type = ObLargePageHelper::get_type();
  set_ob_mem_mgr_path();
  if (SANITY_BOOL_EXPR(alloc_shadow)) {
    int64_t new_addr = ATOMIC_FAA(&global_canonical_addr, size);
    if (!SANITY_ADDR_IN_RANGE((void*)new_addr)) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "sanity address exhausted", K(errno), KP(new_addr));
      ATOMIC_FAA(&global_canonical_addr, -size);
      ptr = NULL; // let it goon, it means no shadow, same as out of checker!
      // in aarch64, mmap will return EPERM error when NULL address and MAP_FIXED are privided at the same time
      flags &= ~MAP_FIXED;
    } else {
      ptr = (void*)new_addr;
    }
  }
  if (OB_LIKELY(ObLargePageHelper::PREFER_LARGE_PAGE != large_page_type) &&
      OB_LIKELY(ObLargePageHelper::ONLY_LARGE_PAGE != large_page_type)) {
    if (MAP_FAILED == (ptr = ::mmap(ptr, size, prot, flags, fd, offset))) {
      ptr = nullptr;
    }
  } else {
    if (MAP_FAILED == (ptr = ::mmap(ptr, size, prot, huge_flags, fd, offset))) {
      ptr = nullptr;
      if (ObLargePageHelper::PREFER_LARGE_PAGE == large_page_type) {
        if (MAP_FAILED == (ptr = ::mmap(ptr, size, prot, flags, fd, offset))) {
          ptr = nullptr;
        }
      }
    } else {
      huge_page_used = huge_flags != flags;
    }
  }
  if (ptr && SANITY_ADDR_IN_RANGE(ptr)) {
    void *shad_ptr  = SANITY_TO_SHADOW(ptr);
    ssize_t shad_size = SANITY_TO_SHADOW_SIZE(size);
    if (MAP_FAILED == ::mmap(shad_ptr, shad_size, prot, flags, fd, offset)) {
      LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "sanity alloc shadow failed", K(errno), KP(shad_ptr));
      this->munmap(ptr, size);
      ptr = nullptr;
    } else {
      IGNORE_RETURN ATOMIC_FAA(&shadow_hold_, shad_size);
      //memset(shad_ptr, 0, shad_size);
      //SANITY_UNPOISON(shad_ptr, shad_size); // maybe no need?
      //SANITY_UNPOISON(ptr, size); // maybe no need?
    }
  }
  unset_ob_mem_mgr_path();
  return ptr;
}

void AChunkMgr::low_free(const void *ptr, const uint64_t size)
{
  set_ob_mem_mgr_path();
  if (SANITY_ADDR_IN_RANGE(ptr)) {
    void *shad_ptr  = SANITY_TO_SHADOW((void*)ptr);
    ssize_t shad_size = SANITY_TO_SHADOW_SIZE(size);
    IGNORE_RETURN ATOMIC_FAA(&shadow_hold_, -shad_size);
    this->munmap(shad_ptr, shad_size);
  }
  this->munmap((void*)ptr, size);
  unset_ob_mem_mgr_path();
}

AChunk *AChunkMgr::alloc_chunk(const uint64_t size, bool high_prio)
{
  const int64_t hold_size = hold(size);
  const int64_t all_size = aligned(size);

  AChunk *chunk = nullptr;
  // Reuse chunk from self-cache
  if (OB_NOT_NULL(chunk = pop_chunk_with_size(all_size))) {
    int64_t orig_hold_size = chunk->hold();
    bool need_free = false;
    if (hold_size == orig_hold_size) {
      // do-nothing
    } else if (hold_size > orig_hold_size) {
      need_free = !update_hold(hold_size - orig_hold_size, high_prio);
    } else if (chunk->is_hugetlb_) {
      need_free = true;
    } else {
      int result = this->madvise((char*)chunk + hold_size, orig_hold_size - hold_size, MADV_DONTNEED);
      if (-1 == result) {
        LOG_WARN_RET(OB_ERR_SYS, "madvise failed", K(errno));
        need_free = true;
      } else {
        IGNORE_RETURN update_hold(hold_size - orig_hold_size, false);
      }
    }
    if (need_free) {
      direct_free(chunk, all_size);
      IGNORE_RETURN update_hold(-orig_hold_size, false);
      chunk = nullptr;
    }
  }
  if (OB_ISNULL(chunk)) {
    bool updated = false;
    for (int i = MAX_LARGE_ACHUNK_INDEX; !updated && i >= 0; --i) {
      while (!(updated = update_hold(hold_size, high_prio)) &&
          OB_NOT_NULL(chunk = pop_chunk_with_index(i))) {
        // Wash chunk from all-cache when observer's hold reaches limit
        int64_t orig_all_size = chunk->aligned();
        int64_t orig_hold_size = chunk->hold();
        direct_free(chunk, orig_all_size);
        IGNORE_RETURN update_hold(-orig_hold_size, false);
        chunk = nullptr;
      }
    }
    if (updated) {
      bool hugetlb_used = false;
      void *ptr = direct_alloc(all_size, true, hugetlb_used, SANITY_BOOL_EXPR(true));
      if (ptr != nullptr) {
        chunk = new (ptr) AChunk();
        chunk->is_hugetlb_ = hugetlb_used;
      } else {
        IGNORE_RETURN update_hold(-hold_size, false);
      }
    }
  }
  if (OB_NOT_NULL(chunk)) {
    chunk->alloc_bytes_ = size;
    SANITY_UNPOISON(chunk, all_size); // maybe no need?
  } else if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
    LOG_DBA_WARN_V2(OB_LIB_ALLOCATE_MEMORY_FAIL, OB_ALLOCATE_MEMORY_FAILED,
        "[OOPS]: over total memory limit. ", "The details: ",
        "hold= ", get_hold(), ", limit= ", get_limit());
  }

  return chunk;
}

void AChunkMgr::free_chunk(AChunk *chunk)
{
  if (OB_NOT_NULL(chunk)) {
    const int64_t hold_size = chunk->hold();
    const uint64_t all_size = chunk->aligned();
    const double max_large_cache_ratio = 0.5;
    int64_t max_large_cache_size = min(limit_ - get_used(), max_chunk_cache_size_) * max_large_cache_ratio;
    const int64_t large_cache_hold = cache_hold_ - get_freelist(NORMAL_ACHUNK_INDEX).hold();
    bool freed = true;
    if (cache_hold_ + hold_size <= max_chunk_cache_size_
        && (NORMAL_ACHUNK_SIZE == all_size || large_cache_hold <= max_large_cache_size)
        && 0 == chunk->washed_size_) {
      freed = !push_chunk(chunk);
    }
    if (freed) {
      direct_free(chunk, all_size);
      IGNORE_RETURN update_hold(-hold_size, false);
    }
  }
}

AChunk *AChunkMgr::alloc_co_chunk(const uint64_t size)
{
  const int64_t hold_size = hold(size);
  const int64_t all_size = aligned(size);

  AChunk *chunk = nullptr;
  bool updated = false;
  for (int i = MAX_LARGE_ACHUNK_INDEX; !updated && i >= 0; --i) {
    while (!(updated = update_hold(hold_size, true)) &&
      OB_NOT_NULL(chunk = pop_chunk_with_index(i))) {
      int64_t all_size = chunk->aligned();
      int64_t hold_size = chunk->hold();
      direct_free(chunk, all_size);
      IGNORE_RETURN update_hold(-hold_size, false);
      chunk = nullptr;
    }
  }
  if (updated) {
    // there is performance drop when thread stack on huge_page memory.
    bool hugetlb_used = false;
    void *ptr = direct_alloc(all_size, false, hugetlb_used, SANITY_BOOL_EXPR(false));
    if (ptr != nullptr) {
      chunk = new (ptr) AChunk();
      chunk->is_hugetlb_ = hugetlb_used;
    } else {
      IGNORE_RETURN update_hold(-hold_size, true);
    }
  }

  if (OB_NOT_NULL(chunk)) {
    chunk->alloc_bytes_ = size;
    //SANITY_UNPOISON(chunk, all_size); // maybe no need?
  } else if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
    LOG_DBA_WARN_V2(OB_LIB_ALLOCATE_MEMORY_FAIL, OB_ALLOCATE_MEMORY_FAILED,
        "[OOPS]: over total memory limit. ", "The details: ",
        "hold= ", get_hold(), ", limit= ", get_limit());
  }

  return chunk;
}

void AChunkMgr::free_co_chunk(AChunk *chunk)
{
  if (OB_NOT_NULL(chunk)) {
    const int64_t hold_size = chunk->hold();
    const uint64_t all_size = chunk->aligned();
    direct_free(chunk, all_size);
    IGNORE_RETURN update_hold(-hold_size, false);
  }
}

bool AChunkMgr::update_hold(int64_t bytes, bool high_prio)
{
  bool bret = true;
  const int64_t limit = high_prio ? limit_ + urgent_ : limit_;
  if (bytes <= 0) {
    IGNORE_RETURN ATOMIC_AAF(&hold_, bytes);
  } else if (hold_ + bytes <= limit) {
    const int64_t nvalue = ATOMIC_AAF(&hold_, bytes);
    if (nvalue > limit) {
      IGNORE_RETURN ATOMIC_AAF(&hold_, -bytes);
      bret = false;
    }
  } else {
    bret = false;
  }
  if (!bret) {
    auto &afc = g_alloc_failed_ctx();
    afc.reason_ = SERVER_HOLD_REACH_LIMIT;
    afc.alloc_size_ = bytes;
    afc.server_hold_ = hold_;
    afc.server_limit_ = limit_;
  }
  return bret;
}

int AChunkMgr::madvise(void *addr, size_t length, int advice)
{
  int result = 0;
  if (length > 0) {
    do {
      result = ::madvise(addr, length, advice);
    } while (result == -1 && errno == EAGAIN);
  }
  return result;
}

void AChunkMgr::munmap(void *addr, size_t length)
{
  int orig_errno = errno;
  if (-1 == ::munmap(addr, length)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "munmap failed", KP(addr), K(length), K(orig_errno), K(errno));
  }
}

int64_t AChunkMgr::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t resident_size = 0;
  int64_t memory_used = get_virtual_memory_used(&resident_size);
  int64_t large_maps = 0;
  int64_t large_unmaps = 0;
  for (int i = MIN_LARGE_ACHUNK_INDEX; i <= MAX_LARGE_ACHUNK_INDEX; ++i) {
    large_maps += get_maps(i);
    large_unmaps += get_unmaps(i);
  }
  int64_t total_maps = 0;
  int64_t total_unmaps = 0;
  for (int i = 0; i <= MAX_ACHUNK_INDEX; ++i) {
    total_maps += get_maps(i);
    total_unmaps += get_unmaps(i);
  }
  ret = databuff_printf(buf, buf_len, pos,
      "[CHUNK_MGR] limit=%'15ld hold=%'15ld total_hold=%'15ld used=%'15ld freelists_hold=%'15ld"
      " total_maps=%'15ld total_unmaps=%'15ld large_maps=%'15ld large_unmaps=%'15ld huge_maps=%'15ld huge_unmaps=%'15ld"
      " memalign=%d resident_size=%'15ld"
#ifndef ENABLE_SANITY
      " virtual_memory_used=%'15ld\n",
#else
      " virtual_memory_used=%'15ld actual_virtual_memory_used=%'15ld\n",
#endif
      limit_, hold_, total_hold_, get_used(), cache_hold_,
      total_maps, total_unmaps, large_maps, large_unmaps, get_maps(HUGE_ACHUNK_INDEX), get_unmaps(HUGE_ACHUNK_INDEX),
      0, resident_size,
#ifndef ENABLE_SANITY
      memory_used
#else
      memory_used - shadow_hold_, memory_used
#endif
      );
  for (int i = 0; OB_SUCC(ret) && i <= MAX_LARGE_ACHUNK_INDEX; ++i) {
    const AChunkList &free_list = get_freelist(i);
    ret = databuff_printf(buf, buf_len, pos,
        "[CHUNK_MGR] %'2d MB_CACHE: hold=%'15ld free=%'15ld pushes=%'15ld pops=%'15ld maps=%'15ld unmaps=%'15ld\n",
        (i + 1) * 2, free_list.hold(), free_list.count(),
        free_list.get_pushes(), free_list.get_pops(),
        get_maps(i), get_unmaps(i));
  }
  return pos;
}

int64_t AChunkMgr::sync_wash()
{
  int64_t washed_size = 0;
  for (int i = 0; i <= MAX_LARGE_ACHUNK_INDEX; ++i) {
    int64_t cache_hold = 0;
    AChunk *head = popall_with_index(i, cache_hold);
    if (OB_NOT_NULL(head)) {
      AChunk *chunk = head;
      do {
        const int64_t all_size = chunk->aligned();
        AChunk *next_chunk = chunk->next_;
        direct_free(chunk, all_size);
        chunk = next_chunk;
      } while (chunk != head);
      ATOMIC_FAA(&cache_hold_, -cache_hold);
      IGNORE_RETURN update_hold(-cache_hold, false);
      washed_size += cache_hold;
    }
  }
  return washed_size;
}