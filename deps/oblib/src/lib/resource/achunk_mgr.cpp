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
#include "lib/stat/ob_diagnose_info.h"

using namespace oceanbase::lib;

int ObLargePageHelper::large_page_type_ = INVALID_LARGE_PAGE_TYPE;

void ObLargePageHelper::set_param(const char* param)
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
  return large_page_type_;
}

AChunkMgr& AChunkMgr::instance()
{
  static AChunkMgr mgr;
  return mgr;
}

AChunkMgr::AChunkMgr()
    : free_list_(),
      chunk_bitmap_(nullptr),
      limit_(DEFAULT_LIMIT),
      urgent_(0),
      hold_(0),
      maps_(0),
      unmaps_(0),
      large_maps_(0),
      large_unmaps_(0)
{
#if MEMCHK_LEVEL >= 1
  uint64_t bmsize = sizeof(ChunkBitMap) + ChunkBitMap::buf_len(CHUNK_BITMAP_SIZE);
  AChunk* chunk = alloc_chunk(bmsize);
  if (nullptr != chunk) {
    chunk_bitmap_ = new (chunk->data_) ChunkBitMap(CHUNK_BITMAP_SIZE, chunk->data_ + sizeof(ChunkBitMap));
    chunk_bitmap_->set((int)((uint64_t)chunk >> MEMCHK_CHUNK_ALIGN_BITS));
  }
#endif
}

void* AChunkMgr::direct_alloc(const uint64_t size)
{
  common::ObTimeGuard time_guard(__func__, 1000 * 1000);
  int orig_errno = errno;
  EVENT_INC(MMAP_COUNT);
  EVENT_ADD(MMAP_SIZE, size);

  void* ptr = nullptr;
  ptr = low_alloc(size);
  if (nullptr != ptr) {
#if MEMCHK_LEVEL >= 1
    if (((uint64_t)ptr & (ALIGN_SIZE - 1)) != 0) {
      // not aligned
      low_free(ptr, size);

      uint64_t new_size = size + ALIGN_SIZE;
      ptr = low_alloc(new_size);
      if (nullptr != ptr) {
        const uint64_t addr = align_up2((uint64_t)ptr, ALIGN_SIZE);
        if (addr - (uint64_t)ptr > 0) {
          low_free(ptr, addr - (uint64_t)ptr);
        }
        if (ALIGN_SIZE - (addr - (uint64_t)ptr) > 0) {
          low_free((void*)(addr + size), ALIGN_SIZE - (addr - (uint64_t)ptr));
        }
        ptr = (void*)addr;
      }
    } else {
      // aligned address returned
    }

    if (nullptr != chunk_bitmap_ && nullptr != ptr) {
      chunk_bitmap_->set((int)((uint64_t)ptr >> MEMCHK_CHUNK_ALIGN_BITS));
    }
#endif
  }

  if (ptr != nullptr) {
    ATOMIC_FAA(&maps_, 1);
    if (size > INTACT_ACHUNK_SIZE) {
      ATOMIC_FAA(&large_maps_, 1);
    }
  } else {
    LOG_ERROR("low alloc fail", K(size), K(orig_errno), K(errno));
    auto& afc = g_alloc_failed_ctx();
    afc.reason_ = PHYSICAL_MEMORY_EXHAUST;
    afc.alloc_size_ = size;
    afc.errno_ = errno;
  }

  return ptr;
}

void AChunkMgr::direct_free(const void* ptr, const uint64_t size)
{
  common::ObTimeGuard time_guard(__func__, 1000 * 1000);
  EVENT_INC(MUNMAP_COUNT);
  EVENT_ADD(MUNMAP_SIZE, size);
#if MEMCHK_LEVEL >= 1
  const int idx = (int)((uint64_t)ptr >> MEMCHK_CHUNK_ALIGN_BITS);

  abort_unless(ptr);
  abort_unless(((uint64_t)ptr & (MEMCHK_CHUNK_ALIGN - 1)) == 0);
  abort_unless(chunk_bitmap_ != nullptr);
  abort_unless(chunk_bitmap_->isset(idx));
  chunk_bitmap_->unset(idx);
#endif
  ATOMIC_FAA(&unmaps_, 1);
  if (size > INTACT_ACHUNK_SIZE) {
    ATOMIC_FAA(&large_unmaps_, 1);
  }
  low_free(ptr, size);
}

void* AChunkMgr::low_alloc(const uint64_t size)
{
  void* ptr = nullptr;
  const int prot = PROT_READ | PROT_WRITE;
  const int flags = MAP_PRIVATE | MAP_ANONYMOUS;
#ifdef MAP_HUGETLB
  const int huge_flags = flags | MAP_HUGETLB;
#else
  const int huge_flags = flags;
#endif
  const int fd = -1;
  const int offset = 0;
  const int large_page_type = ObLargePageHelper::get_type();
  if (OB_LIKELY(ObLargePageHelper::PREFER_LARGE_PAGE != large_page_type) &&
      OB_LIKELY(ObLargePageHelper::ONLY_LARGE_PAGE != large_page_type)) {
    if (MAP_FAILED == (ptr = ::mmap(nullptr, size, prot, flags, fd, offset))) {
      ptr = nullptr;
    }
  } else {
    if (MAP_FAILED == (ptr = ::mmap(nullptr, size, prot, huge_flags, fd, offset))) {
      ptr = nullptr;
      if (ObLargePageHelper::PREFER_LARGE_PAGE == large_page_type) {
        if (MAP_FAILED == (ptr = ::mmap(nullptr, size, prot, flags, fd, offset))) {
          ptr = nullptr;
        }
      }
    }
  }
  return ptr;
}

void AChunkMgr::low_free(const void* ptr, const uint64_t size)
{
  ::munmap((void*)ptr, size);
}

AChunk* AChunkMgr::alloc_chunk(const uint64_t size, bool high_prio)
{
  const uint64_t all_size = aligned(size);
  const uint64_t adj_size = all_size - ACHUNK_HEADER_SIZE;

  AChunk* chunk = nullptr;
  if (INTACT_ACHUNK_SIZE == all_size) {
    if (free_list_.count() > 0) {
      chunk = free_list_.pop();
    }
    if (OB_ISNULL(chunk)) {
      if (update_hold(all_size, high_prio)) {
        void* ptr = direct_alloc(all_size);
        if (ptr != nullptr) {
          chunk = new (ptr) AChunk(adj_size);
        } else {
          IGNORE_RETURN update_hold(-all_size, high_prio);
        }
      }
    }
  } else {
    if (update_hold(all_size, high_prio)) {
      void* ptr = direct_alloc(all_size);
      if (ptr != nullptr) {
        chunk = new (ptr) AChunk(adj_size);
      } else {
        IGNORE_RETURN update_hold(-all_size, high_prio);
      }
    } else {
      int64_t left_size = all_size;
      while (free_list_.count() > 0 && left_size > 0) {
        direct_free(free_list_.pop(), INTACT_ACHUNK_SIZE);
        left_size -= INTACT_ACHUNK_SIZE;
      }
      if (left_size <= 0) {
        void* ptr = direct_alloc(all_size);
        if (ptr != nullptr) {
          chunk = new (ptr) AChunk(adj_size);
        }
      } else {
        IGNORE_RETURN update_hold(left_size - all_size, high_prio);
        if (update_hold(all_size, high_prio)) {
          void* ptr = direct_alloc(all_size);
          if (ptr != nullptr) {
            chunk = new (ptr) AChunk(adj_size);
          } else {
            IGNORE_RETURN update_hold(-all_size, high_prio);
          }
        }
      }
    }
  }

  if (chunk != nullptr) {
    chunk->alloc_bytes_ = size;
  }

  if (OB_ISNULL(chunk) && REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
    _OB_LOG(WARN, "oops, over total memory limit, hold=%ld limit=%ld", get_hold(), get_limit());
  }

  return chunk;
}

void AChunkMgr::free_chunk(AChunk* chunk)
{
  if (chunk != nullptr) {
    const int64_t size = chunk->size_;
    const uint64_t all_size = aligned(size);
    if (INTACT_ACHUNK_SIZE == all_size) {
      bool free = false;
      if (hold_ + all_size > limit_) {
        free = true;
      } else {
        const bool push_succ = free_list_.push(chunk);
        free = !push_succ;
      }
      if (free) {
        direct_free(chunk, INTACT_ACHUNK_SIZE);
        IGNORE_RETURN update_hold(-static_cast<int64_t>(INTACT_ACHUNK_SIZE), false);
      }
    } else {
      const int64_t large_space = limit_ - hold_;
      if (large_space < LARGE_CHUNK_FREE_SPACE) {
        direct_free(chunk, all_size);
        IGNORE_RETURN update_hold(-all_size, false);
      } else {
        char* ptr = (char*)chunk;
        char* end = (char*)chunk + all_size;
        while (ptr < end) {
          AChunk* new_chunk = new (ptr) AChunk(INTACT_ACHUNK_SIZE - ACHUNK_HEADER_SIZE);
          const bool push_succ = free_list_.push(new_chunk);
          if (push_succ) {
            ptr += INTACT_ACHUNK_SIZE;
          } else {
            direct_free(ptr, end - ptr);
            IGNORE_RETURN update_hold(ptr - end, false);
            ptr = end;
          }
        }
      }
    }
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
    auto& afc = g_alloc_failed_ctx();
    afc.reason_ = SERVER_HOLD_REACH_LIMIT;
    afc.alloc_size_ = bytes;
    afc.server_hold_ = hold_;
    afc.server_limit_ = limit_;
  }
  return bret;
}
