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
  : free_list_(), large_free_list_(),
    chunk_bitmap_(nullptr), limit_(DEFAULT_LIMIT), urgent_(0), hold_(0),
    total_hold_(0), maps_(0), unmaps_(0), large_maps_(0), large_unmaps_(0),
    huge_maps_(0), huge_unmaps_(0),
    shadow_hold_(0)
{
  large_free_list_.set_max_chunk_cache_size(0);
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
    ATOMIC_FAA(&maps_, 1);
    if (size > LARGE_ACHUNK_SIZE) {
      ATOMIC_FAA(&huge_maps_, 1);
    } else if (size > NORMAL_ACHUNK_SIZE) {
      ATOMIC_FAA(&large_maps_, 1);
    }
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

  ATOMIC_FAA(&unmaps_, 1);
  if (size > LARGE_ACHUNK_SIZE) {
    ATOMIC_FAA(&huge_unmaps_, 1);
  } else if (size > NORMAL_ACHUNK_SIZE) {
    ATOMIC_FAA(&large_unmaps_, 1);
  }
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
      LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "sanity alloc shadow failed", K(errno), KP(shad_ptr));
      abort();
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
    ::munmap(shad_ptr, shad_size);
  }
  ::munmap((void*)ptr, size);
  unset_ob_mem_mgr_path();
}

AChunk *AChunkMgr::alloc_chunk(const uint64_t size, bool high_prio)
{
  const int64_t hold_size = hold(size);
  const int64_t all_size = aligned(size);

  AChunk *chunk = nullptr;
  if (NORMAL_ACHUNK_SIZE == all_size) {
    // TODO by fengshuo.fs: chunk cached by freelist may not use all memory in it,
    //                      so update_hold can use hold_size too.
    if (free_list_.count() > 0) {
      chunk = free_list_.pop();
    }
    if (OB_ISNULL(chunk)) {
      bool updated = false;
      while (!(updated = update_hold(hold_size, high_prio)) && large_free_list_.count() > 0) {
        if (OB_NOT_NULL(chunk = large_free_list_.pop())) {
          int64_t all_size = chunk->aligned();
          int64_t hold_size = chunk->hold();
          direct_free(chunk, all_size);
          IGNORE_RETURN update_hold(-hold_size, false);
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
  } else if (LARGE_ACHUNK_SIZE == all_size) {
    if (large_free_list_.count() > 0) {
      chunk = large_free_list_.pop();
    }
    if (chunk != NULL) {
      int64_t orig_hold_size = chunk->hold();
      if (hold_size == orig_hold_size) {
        // do-nothing
      } else if (hold_size > orig_hold_size) {
        if (!update_hold(hold_size - orig_hold_size, high_prio)) {
          direct_free(chunk, all_size);
          IGNORE_RETURN update_hold(-orig_hold_size, false);
          chunk = nullptr;
        }
      } else {
        int result = 0;
        do {
          result = this->madvise((char*)chunk + hold_size, orig_hold_size - hold_size, MADV_DONTNEED);
        } while (result == -1 && errno == EAGAIN);
        if (-1 == result) {
          LOG_WARN_RET(OB_ERR_SYS, "madvise failed", K(errno));
          direct_free(chunk, all_size);
          IGNORE_RETURN update_hold(-orig_hold_size, false);
          chunk = nullptr;
        } else {
          IGNORE_RETURN update_hold(hold_size - orig_hold_size, false);
        }
      }
    }
    if (OB_ISNULL(chunk)) {
      bool updated = false;
      while (!(updated = update_hold(hold_size, high_prio)) && free_list_.count() > 0) {
        if (OB_NOT_NULL(chunk = free_list_.pop())) {
          int64_t all_size = chunk->aligned();
          int64_t hold_size = chunk->hold();
          direct_free(chunk, all_size);
          IGNORE_RETURN update_hold(-hold_size, false);
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
  } else {
    bool updated = false;
    while (!(updated = update_hold(hold_size, high_prio)) &&
           (free_list_.count() > 0 || large_free_list_.count() > 0)) {
      if (OB_NOT_NULL(chunk = free_list_.pop()) || OB_NOT_NULL(chunk = large_free_list_.pop())) {
        int64_t all_size = chunk->aligned();
        int64_t hold_size = chunk->hold();
        direct_free(chunk, all_size);
        IGNORE_RETURN update_hold(-hold_size, false);
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
    LOG_DBA_WARN(OB_ALLOCATE_MEMORY_FAILED, "msg", "oops, over total memory limit" ,
                "hold", get_hold(), "limit", get_limit());
  }

  return chunk;
}

void AChunkMgr::free_chunk(AChunk *chunk)
{
  if (OB_NOT_NULL(chunk)) {
    const int64_t hold_size = chunk->hold();
    const uint64_t all_size = chunk->aligned();
    bool freed = true;
    if (NORMAL_ACHUNK_SIZE == hold_size) {
      if (hold_ + hold_size <= limit_) {
        freed = !free_list_.push(chunk);
      }
      if (freed) {
        direct_free(chunk, all_size);
        IGNORE_RETURN update_hold(-hold_size, false);
      }
    } else if (LARGE_ACHUNK_SIZE == all_size) {
      if (hold_ + hold_size <= limit_) {
        freed = !large_free_list_.push(chunk);
      }
      if (freed) {
        direct_free(chunk, all_size);
        IGNORE_RETURN update_hold(-hold_size, false);
      }
    } else {
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
  while (!(updated = update_hold(hold_size, true)) &&
         (free_list_.count() > 0 || large_free_list_.count() > 0)) {
    if (OB_NOT_NULL(chunk = free_list_.pop()) || OB_NOT_NULL(chunk = large_free_list_.pop())) {
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
    LOG_DBA_WARN(OB_ALLOCATE_MEMORY_FAILED, "msg", "oops, over total memory limit" ,
                "hold", get_hold(), "limit", get_limit());
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
  return ::madvise(addr, length, advice);
}
