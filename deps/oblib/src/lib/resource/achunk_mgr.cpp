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

#include "achunk_mgr.h"
#include "lib/utility/utility.h"
#include "lib/resource/ob_affinity_ctrl.h"
#include "lib/utility/ob_tracepoint.h"
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
    total_hold_(0), cache_hold_(0), large_cache_hold_(0),
    max_chunk_cache_size_(limit_)
{
  // not to cache huge_chunk
  for (int i = 0; i < OB_MAX_NUMA_NUM; ++i) {
    slots_[i][HUGE_ACHUNK_INDEX]->set_max_chunk_cache_size(0);
  }
}

ERRSIM_POINT_DEF(EN_PHYSICAL_MEMORY_EXHAUST);
void *AChunkMgr::direct_alloc(const uint64_t size, const int32_t numa_id, const bool can_use_huge_page, bool &huge_page_used, const bool alloc_shadow)
{
  common::ObTimeGuard time_guard(__func__, 1000 * 1000);
  int orig_errno = errno;

  void *ptr = nullptr;
  bool is_errsim = false;
  if (!(is_errsim = EN_PHYSICAL_MEMORY_EXHAUST)) {
    ptr = low_alloc(size, numa_id, can_use_huge_page, huge_page_used, alloc_shadow);
  }
  if (nullptr != ptr) {
    if (((uint64_t)ptr & (ACHUNK_ALIGN_SIZE - 1)) != 0) {
      // not aligned
      low_free(ptr, size);

      uint64_t new_size = size + ACHUNK_ALIGN_SIZE;
      /* alloc_shadow should be set to false since partitial sanity_munmap is not supported */
      ptr = low_alloc(new_size, numa_id, can_use_huge_page, huge_page_used, false/*alloc_shadow*/);
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
    AFFINITY_CTRL.memory_bind_to_node(ptr, size, numa_id);
    inc_maps(size, numa_id);
    IGNORE_RETURN ATOMIC_FAA(&total_hold_, size);
  } else {
    LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "low alloc fail", K(size), K(orig_errno), K(errno));
    auto &afc = g_alloc_failed_ctx();
    afc.reason_ = is_errsim ? ERRSIM_PHYSICAL_MEMORY_EXHAUST : PHYSICAL_MEMORY_EXHAUST;
    afc.alloc_size_ = size;
    afc.errno_ = errno;
  }

  return ptr;
}

void AChunkMgr::direct_free(const void *ptr, const uint64_t size)
{
  common::ObTimeGuard time_guard(__func__, 1000 * 1000);
  int32_t numa_id = ((AChunk*)ptr)->numa_id_;
  inc_unmaps(size, numa_id);
  IGNORE_RETURN ATOMIC_FAA(&total_hold_, -size);
  low_free(ptr, size);
}


void *AChunkMgr::low_alloc(const uint64_t size, const int32_t numa_id, const bool can_use_huge_page, bool &huge_page_used, const bool alloc_shadow)
{
  void *ptr = nullptr;
  huge_page_used = false;
  const int prot = PROT_READ | PROT_WRITE;
  int flags = MAP_PRIVATE | MAP_ANONYMOUS;
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
  ObUnmanagedMemoryStat::DisableGuard guard;
  if (SANITY_BOOL_EXPR(alloc_shadow)) {
    ptr = SANITY_MMAP(size);
  }
  if (NULL == ptr) {
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
  }
  return ptr;
}

void AChunkMgr::low_free(const void *ptr, const uint64_t size)
{
  ObUnmanagedMemoryStat::DisableGuard guard;
  if (SANITY_ADDR_IN_RANGE(ptr, size)) {
    AChunk *chunk = (AChunk*)ptr;
#ifdef ENABLE_SANITY
    void *ref = chunk->ref_;
    *(void**)ptr = ref;
#endif
    SANITY_MUNMAP((void*)ptr, size);
  } else {
    this->munmap((void*)ptr, size);
  }
}

AChunk *AChunkMgr::alloc_chunk(const uint64_t size, const int32_t numa_id, bool high_prio)
{
  const int64_t hold_size = hold(size);
  const int64_t all_size = aligned(size);

  AChunk *chunk = nullptr;
  // Reuse chunk from self-cache
  if (OB_NOT_NULL(chunk = pop_chunk_with_size(all_size, numa_id))) {
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
    for (int i = 0; !updated && i < OB_MAX_NUMA_NUM; ++i) {
      int32_t free_numa_id = (i + numa_id) % OB_MAX_NUMA_NUM;
      for (int chunk_idx = MAX_LARGE_ACHUNK_INDEX; !updated && chunk_idx >= 0; --chunk_idx) {
        while (!(updated = update_hold(hold_size, high_prio)) &&
            OB_NOT_NULL(chunk = pop_chunk_with_index(chunk_idx, free_numa_id))) {
          int64_t orig_all_size = chunk->aligned();
          int64_t orig_hold_size = chunk->hold();
          direct_free(chunk, orig_all_size);
          IGNORE_RETURN update_hold(-orig_hold_size, false);
          chunk = nullptr;
        }
      }
    }
    if (updated) {
      bool hugetlb_used = false;
      void *ptr = direct_alloc(all_size, numa_id, true, hugetlb_used, SANITY_BOOL_EXPR(true));
      if (ptr != nullptr) {
#ifdef ENABLE_SANITY
        void *ref = *(void**)ptr;
        chunk = new (ptr) AChunk();
        chunk->ref_ = ref;
#else
        chunk = new (ptr) AChunk();
#endif
        chunk->is_hugetlb_ = hugetlb_used;
        chunk->numa_id_ = numa_id;
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
    bool freed = true;
    if (cache_hold_ + hold_size <= max_chunk_cache_size_
        && (NORMAL_ACHUNK_SIZE == all_size || large_cache_hold_ <= max_large_cache_size)
        && 0 == chunk->washed_size_) {
      freed = !push_chunk(chunk, all_size, hold_size);
    }
    if (freed) {
      direct_free(chunk, all_size);
      IGNORE_RETURN update_hold(-hold_size, false);
    }
  }
}

AChunk *AChunkMgr::alloc_co_chunk(const uint64_t size, const int32_t numa_id)
{
  const int64_t hold_size = hold(size);
  const int64_t all_size = aligned(size);

  AChunk *chunk = nullptr;
  bool updated = false;
  for (int i = 0; !updated && i < OB_MAX_NUMA_NUM; ++i) {
    int32_t free_numa_id = (i + numa_id) % OB_MAX_NUMA_NUM;
    for (int chunk_idx = MAX_LARGE_ACHUNK_INDEX; !updated && chunk_idx >= 0; --chunk_idx) {
      while (!(updated = update_hold(hold_size, true)) &&
        OB_NOT_NULL(chunk = pop_chunk_with_index(chunk_idx, free_numa_id))) {
        int64_t all_size = chunk->aligned();
        int64_t hold_size = chunk->hold();
        direct_free(chunk, all_size);
        IGNORE_RETURN update_hold(-hold_size, false);
        chunk = nullptr;
      }
    }
  }
  if (updated) {
    // there is performance drop when thread stack on huge_page memory.
    bool hugetlb_used = false;
    void *ptr = direct_alloc(all_size, numa_id, false, hugetlb_used, SANITY_BOOL_EXPR(false));
    if (ptr != nullptr) {
      chunk = new (ptr) AChunk();
      chunk->is_hugetlb_ = hugetlb_used;
      chunk->numa_id_ = numa_id;
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

ERRSIM_POINT_DEF(EN_SERVER_HOLD_REACH_LIMIT);
bool AChunkMgr::update_hold(int64_t bytes, bool high_prio)
{
  bool bret = true;
  const int64_t limit = high_prio ? limit_ + urgent_ : limit_;
  bool is_errsim = false;
  if (bytes <= 0) {
    IGNORE_RETURN ATOMIC_AAF(&hold_, bytes);
  } else if (!(is_errsim = EN_SERVER_HOLD_REACH_LIMIT)
      && hold_ + bytes <= limit) {
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
    afc.reason_ = is_errsim ? ERRSIM_SERVER_HOLD_REACH_LIMIT : SERVER_HOLD_REACH_LIMIT;
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
  int64_t normal_maps = 0;
  int64_t normal_unmaps = 0;
  int64_t large_maps = 0;
  int64_t large_unmaps = 0;
  int64_t huge_maps = 0;
  int64_t huge_unmaps = 0;

  for (int i = 0; i <= MAX_NORMAL_ACHUNK_INDEX; ++i) {
    for (int j = 0; j < OB_MAX_NUMA_NUM; ++j) {
      normal_maps += get_maps(i, j);
      normal_unmaps += get_unmaps(i, j);
    }
  }
  for (int i = MIN_LARGE_ACHUNK_INDEX; i <= MAX_LARGE_ACHUNK_INDEX; ++i) {
    for (int j = 0; j < OB_MAX_NUMA_NUM; ++j) {
      large_maps += get_maps(i, j);
      large_unmaps += get_unmaps(i, j);
    }
  }
  for (int i = 0; i < OB_MAX_NUMA_NUM; ++i) {
    huge_maps += get_maps(HUGE_ACHUNK_INDEX, i);
    huge_unmaps += get_unmaps(HUGE_ACHUNK_INDEX, i);
  }
  int64_t total_maps = normal_maps + large_maps + huge_maps;
  int64_t total_unmaps = normal_unmaps + large_unmaps + huge_unmaps;
  int64_t virtual_memory_used = get_virtual_memory_used(&resident_size);

  int64_t max_map_count = 0;
  (void)read_one_int("/proc/sys/vm/max_map_count", max_map_count);
  const char *thp_status = get_transparent_hugepage_status();

  ret = databuff_printf(buf, buf_len, pos,
      "[CHUNK_MGR] limit=%'15ld hold=%'15ld total_hold=%'15ld used=%'15ld freelists_hold=%'15ld"
      " total_maps=%'15ld total_unmaps=%'15ld large_maps=%'15ld large_unmaps=%'15ld huge_maps=%'15ld huge_unmaps=%'15ld"
      " resident_size=%'15ld unmanaged_memory_size=%'15ld"
      " virtual_memory_used=%'15ld",
      limit_, hold_, total_hold_, get_used(), cache_hold_,
      total_maps, total_unmaps, large_maps, large_unmaps, huge_maps, huge_unmaps,
      resident_size, get_unmanaged_memory_size(), virtual_memory_used);
#ifdef ENABLE_SANITY
  if (OB_SUCC(ret)) {
    ret = databuff_printf(buf, buf_len, pos,
        " sanity_min_addr=0x%lx sanity_max_addr=0x%lx max_used_addr=0x%lx",
        sanity_min_addr, sanity_max_addr, get_global_addr());
  }
#endif
  if (OB_SUCC(ret)) {
    ret = databuff_printf(buf, buf_len, pos,
        " [OS_PARAMS] vm.max_map_count=%'15ld transparent_hugepages=%15s",
        max_map_count, thp_status);
  }

  if (OB_SUCC(ret)) {
    int64_t hold = 0, count = 0, pushes = 0, pops = 0;
    for (int i = 0; i <= MAX_NORMAL_ACHUNK_INDEX; ++i) {
      for (int j = 0; j < OB_MAX_NUMA_NUM; ++j) {
        const AChunkList &free_list = get_freelist(i, j);
        hold += free_list.hold();
        count += free_list.count();
        pushes += free_list.get_pushes();
        pops += free_list.get_pops();
      }
    }
    ret = databuff_printf(buf, buf_len, pos,
        "\n[CHUNK_MGR] %'2d MB_CACHE: hold=%'15ld count=%'15ld pushes=%'15ld pops=%'15ld maps=%'15ld unmaps=%'15ld\n",
        2, hold, count, pushes, pops, normal_maps, normal_unmaps);
  }

  for (int i = MIN_LARGE_ACHUNK_INDEX; OB_SUCC(ret) && i <= MAX_LARGE_ACHUNK_INDEX; ++i) {
    int64_t hold = 0, count = 0, pushes = 0, pops = 0;
    int64_t maps = 0, unmaps = 0;
    for (int j = 0; j < OB_MAX_NUMA_NUM; ++j) {
      const AChunkList &free_list = get_freelist(i, j);
      hold += free_list.hold();
      count += free_list.count();
      pushes += free_list.get_pushes();
      pops += free_list.get_pops();
      maps += get_maps(i, j);
      unmaps += get_unmaps(i, j);
    }
    ret = databuff_printf(buf, buf_len, pos,
        "[CHUNK_MGR] %'2d MB_CACHE: hold=%'15ld count=%'15ld pushes=%'15ld pops=%'15ld maps=%'15ld unmaps=%'15ld\n",
        LARGE_ACHUNK_SIZE_MAP[i - MIN_LARGE_ACHUNK_INDEX], hold, count, pushes, pops, maps, unmaps);
  }
  return pos;
}

int64_t AChunkMgr::sync_wash()
{
  int64_t washed_size = 0;
  for (int i = 0; i <= MAX_LARGE_ACHUNK_INDEX; ++i) {
    for (int j = 0; j < OB_MAX_NUMA_NUM; ++j) {
      int64_t cache_hold = 0;
      AChunk *head = popall_with_index(i, j, cache_hold);
      if (OB_NOT_NULL(head)) {
        AChunk *chunk = head;
        do {
          const int64_t all_size = chunk->aligned();
          AChunk *next_chunk = chunk->next_;
          direct_free(chunk, all_size);
          chunk = next_chunk;
        } while (chunk != head);
        IGNORE_RETURN update_hold(-cache_hold, false);
        washed_size += cache_hold;
      }
    }
  }
  return washed_size;
}
