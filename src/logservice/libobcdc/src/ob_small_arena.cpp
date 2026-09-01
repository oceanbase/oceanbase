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
 *
 * A Small Arena Allocator for OBCDC
 */

#define USING_LOG_PREFIX OBLOG

#include <algorithm>
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
#include <stdio.h>
#endif

#include "ob_small_arena.h"

#include "lib/utility/utility.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
using namespace common;
namespace libobcdc
{
const int64_t ObSmallArena::SMALL_PAGE_HEADER_SIZE;
const int64_t ObSmallArena::LARGE_PAGE_HEADER_SIZE;
const int64_t ObSmallArena::MAX_FIND_PAGE_DEPTH;
const int64_t ObSmallArena::FIRST_SMALL_PAGE_SIZE;

#ifdef ENABLE_CDC_PERF_DEBUG_STAT
namespace
{
const int64_t SMALL_ARENA_LIFE_STAT_PRINT_INTERVAL = 10L * 1000L * 1000L;
const int64_t SMALL_ARENA_LIFE_STAT_BUCKET_LIMITS[] =
    {0, 512, 1024, 2048, 4096, 8192, 16 * 1024, 32 * 1024,
     64 * 1024, 256 * 1024, 1024 * 1024};
const int64_t SMALL_ARENA_LIFE_STAT_BUCKET_COUNT =
    sizeof(SMALL_ARENA_LIFE_STAT_BUCKET_LIMITS)
    / sizeof(SMALL_ARENA_LIFE_STAT_BUCKET_LIMITS[0]) + 1;

struct SmallArenaLifeStatSummary
{
  int64_t sample_count_;
  int64_t request_bytes_;
  int64_t used_bytes_;
  int64_t hold_bytes_;
  int64_t local_page_hold_bytes_;
  int64_t small_page_hold_bytes_;
  int64_t large_page_hold_bytes_;
  int64_t small_page_used_bytes_;
  int64_t large_page_used_bytes_;
  int64_t small_alloc_count_;
  int64_t large_alloc_count_;
  int64_t local_page_count_;
  int64_t small_page_count_;
  int64_t large_page_count_;
  int64_t max_request_bytes_;
  int64_t max_used_bytes_;
  int64_t max_hold_bytes_;
  int64_t max_alloc_size_;
  int64_t max_local_page_high_water_;
  int64_t request_buckets_[SMALL_ARENA_LIFE_STAT_BUCKET_COUNT];
  int64_t used_buckets_[SMALL_ARENA_LIFE_STAT_BUCKET_COUNT];
  int64_t hold_buckets_[SMALL_ARENA_LIFE_STAT_BUCKET_COUNT];
  int64_t local_page_high_water_buckets_[SMALL_ARENA_LIFE_STAT_BUCKET_COUNT];
};

SmallArenaLifeStatSummary g_small_arena_life_stat;

int64_t get_life_stat_bucket_idx_(const int64_t value)
{
  int64_t idx = 0;
  while (idx < SMALL_ARENA_LIFE_STAT_BUCKET_COUNT - 1
      && value > SMALL_ARENA_LIFE_STAT_BUCKET_LIMITS[idx]) {
    ++idx;
  }
  return idx;
}

void update_max_life_stat_(int64_t *target, const int64_t value)
{
  if (NULL != target) {
    int64_t old_value = ATOMIC_LOAD(target);
    while (value > old_value && old_value != ATOMIC_CAS(target, old_value, value)) {
      old_value = ATOMIC_LOAD(target);
    }
  }
}

void append_life_stat_bucket_(char *buf, const int64_t buf_len, int64_t &pos,
    const int64_t idx, const int64_t count)
{
  if (NULL != buf && pos < buf_len) {
    int n = 0;
    if (idx < SMALL_ARENA_LIFE_STAT_BUCKET_COUNT - 1) {
      n = snprintf(buf + pos, buf_len - pos, "%s<=%ld:%ld",
          0 == pos ? "" : ",", SMALL_ARENA_LIFE_STAT_BUCKET_LIMITS[idx], count);
    } else {
      n = snprintf(buf + pos, buf_len - pos, "%s>%ld:%ld",
          0 == pos ? "" : ",", SMALL_ARENA_LIFE_STAT_BUCKET_LIMITS[idx - 1], count);
    }
    if (n > 0) {
      pos += (n < buf_len - pos) ? n : (buf_len - pos);
    }
  }
}

void build_life_stat_bucket_info_(int64_t *buckets, char *buf, const int64_t buf_len)
{
  if (NULL != buckets && NULL != buf && buf_len > 0) {
    int64_t pos = 0;
    buf[0] = '\0';
    for (int64_t idx = 0; idx < SMALL_ARENA_LIFE_STAT_BUCKET_COUNT; ++idx) {
      const int64_t count = ATOMIC_TAS(buckets + idx, 0);
      append_life_stat_bucket_(buf, buf_len, pos, idx, count);
    }
    buf[buf_len - 1] = '\0';
  }
}

void print_small_arena_life_stat_()
{
  SmallArenaLifeStatSummary &summary = g_small_arena_life_stat;
  const int64_t sample_count = ATOMIC_TAS(&summary.sample_count_, 0);
  if (sample_count > 0) {
    const int64_t request_bytes = ATOMIC_TAS(&summary.request_bytes_, 0);
    const int64_t used_bytes = ATOMIC_TAS(&summary.used_bytes_, 0);
    const int64_t hold_bytes = ATOMIC_TAS(&summary.hold_bytes_, 0);
    const int64_t local_page_hold_bytes = ATOMIC_TAS(&summary.local_page_hold_bytes_, 0);
    const int64_t small_page_hold_bytes = ATOMIC_TAS(&summary.small_page_hold_bytes_, 0);
    const int64_t large_page_hold_bytes = ATOMIC_TAS(&summary.large_page_hold_bytes_, 0);
    const int64_t small_page_used_bytes = ATOMIC_TAS(&summary.small_page_used_bytes_, 0);
    const int64_t large_page_used_bytes = ATOMIC_TAS(&summary.large_page_used_bytes_, 0);
    const int64_t small_alloc_count = ATOMIC_TAS(&summary.small_alloc_count_, 0);
    const int64_t large_alloc_count = ATOMIC_TAS(&summary.large_alloc_count_, 0);
    const int64_t local_page_count = ATOMIC_TAS(&summary.local_page_count_, 0);
    const int64_t small_page_count = ATOMIC_TAS(&summary.small_page_count_, 0);
    const int64_t large_page_count = ATOMIC_TAS(&summary.large_page_count_, 0);
    const int64_t max_request_bytes = ATOMIC_TAS(&summary.max_request_bytes_, 0);
    const int64_t max_used_bytes = ATOMIC_TAS(&summary.max_used_bytes_, 0);
    const int64_t max_hold_bytes = ATOMIC_TAS(&summary.max_hold_bytes_, 0);
    const int64_t max_alloc_size = ATOMIC_TAS(&summary.max_alloc_size_, 0);
    const int64_t max_local_page_high_water = ATOMIC_TAS(&summary.max_local_page_high_water_, 0);
    const int64_t align_waste_bytes = used_bytes - request_bytes;
    const int64_t unused_hold_bytes = hold_bytes - used_bytes;
    char request_bucket_info[512];
    char used_bucket_info[512];
    char hold_bucket_info[512];
    char local_page_high_water_bucket_info[512];

    build_life_stat_bucket_info_(summary.request_buckets_,
        request_bucket_info, sizeof(request_bucket_info));
    build_life_stat_bucket_info_(summary.used_buckets_, used_bucket_info, sizeof(used_bucket_info));
    build_life_stat_bucket_info_(summary.hold_buckets_, hold_bucket_info, sizeof(hold_bucket_info));
    build_life_stat_bucket_info_(summary.local_page_high_water_buckets_,
        local_page_high_water_bucket_info, sizeof(local_page_high_water_bucket_info));

    LOG_INFO("[STAT] [SMALL_ARENA_LIFE]",
        K(sample_count), K(request_bytes), K(used_bytes), K(hold_bytes),
        K(align_waste_bytes), K(unused_hold_bytes),
        K(local_page_hold_bytes), K(small_page_hold_bytes), K(large_page_hold_bytes),
        K(small_page_used_bytes), K(large_page_used_bytes),
        K(small_alloc_count), K(large_alloc_count));

    LOG_INFO("[STAT] [SMALL_ARENA_LIFE] [BUCKET]",
        K(sample_count), K(local_page_count), K(small_page_count), K(large_page_count),
        K(max_request_bytes), K(max_used_bytes), K(max_hold_bytes),
        K(max_alloc_size), K(max_local_page_high_water),
        KCSTRING(request_bucket_info), KCSTRING(used_bucket_info),
        KCSTRING(hold_bucket_info), KCSTRING(local_page_high_water_bucket_info));
  }
}
} // namespace
#endif

ObSmallArena::ObSmallArena() :
    large_allocator_(NULL),
    page_size_(0),
    next_small_page_size_(FIRST_SMALL_PAGE_SIZE),
    local_page_(NULL),
    small_page_list_(NULL),
    large_page_list_(NULL),
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
    small_alloc_count_(0),
    large_alloc_count_(0),
    life_stat_(),
#endif
    lock_(common::ObLatchIds::OB_CDC_SMALL_ARENA_LATCH_ID)
{
}

ObSmallArena::~ObSmallArena()
{
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  LifeStat stat;
  if (collect_life_stat_(stat)) {
    record_life_stat_(stat);
  }
#endif

  do_reset_small_pages_();
  do_reset_large_pages_();

  large_allocator_ = NULL;
  page_size_ = 0;
  local_page_ = NULL;
  small_page_list_ = NULL;
  large_page_list_ = NULL;
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  small_alloc_count_ = 0;
  large_alloc_count_ = 0;
  clear_life_stat_();
#endif
}

void ObSmallArena::reset()
{
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  LifeStat stat;
  bool need_record_stat = false;
#endif
  {
    ObSmallSpinLockGuard<ObByteLock> guard(lock_);
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
    need_record_stat = collect_life_stat_(stat);
#else
    reset_next_small_page_size_();
#endif

    do_reset_small_pages_();
    do_reset_large_pages_();

    // Require external local cache pages to be reclaimed before resetting
    if (NULL != local_page_) {
      local_page_->reset();
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
      record_local_page_hold_();
#endif
    }
  }

#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  if (need_record_stat) {
    record_life_stat_(stat);
  }
#endif
}

void ObSmallArena::set_allocator(const int64_t page_size,
    common::ObIAllocator &large_allocator)
{
  large_allocator_ = &large_allocator;
  page_size_ = page_size;
  reset_next_small_page_size_();
}

void ObSmallArena::set_prealloc_page(void *page, const int64_t page_size)
{
  ObSmallSpinLockGuard<ObByteLock> guard(lock_);
  const int64_t local_page_size = page_size > 0 ? page_size : page_size_;
  if (NULL != local_page_) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "prealloc page has been set", K(local_page_), K(page),
        K(local_page_size));
  } else if (NULL != page && local_page_size <= SMALL_PAGE_HEADER_SIZE) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid prealloc page size", K(page), K(local_page_size),
        K(SMALL_PAGE_HEADER_SIZE));
  } else if (NULL != page) {
    local_page_ = new(page) SmallPage(local_page_size);
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
    record_local_page_hold_();
#endif
  }
}

void ObSmallArena::revert_prealloc_page(void *&page)
{
  ObSmallSpinLockGuard<ObByteLock> guard(lock_);
  page = local_page_;

  if (NULL != local_page_) {
    local_page_->~SmallPage();
  }

  local_page_ = NULL;
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  if (0 == life_stat_.request_bytes_
      && 0 == life_stat_.used_bytes_
      && 0 == life_stat_.small_page_hold_bytes_
      && 0 == life_stat_.large_page_hold_bytes_) {
    clear_life_stat_();
  }
#endif
}

void* ObSmallArena::alloc(const int64_t size)
{
  const int64_t default_align = sizeof(void*);
  return alloc_aligned(size, default_align);
}

bool ObSmallArena::is_valid_() const
{
  return NULL != large_allocator_ && page_size_ > 0;
}

void* ObSmallArena::alloc_aligned(const int64_t size, const int64_t align)
{
  int tmp_ret = OB_SUCCESS;
  void *ret_ptr = NULL;
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  int64_t hold_bytes = 0;
  int64_t used_bytes = 0;
  bool from_local_page = false;
#endif
  ObSmallSpinLockGuard<ObByteLock> guard(lock_);
  if (OB_UNLIKELY(!is_valid_())) {
    tmp_ret = OB_ERR_UNEXPECTED;
    LOG_ERROR_RET(tmp_ret, "small arena is not valid", K(large_allocator_), K(page_size_));
  } else if (OB_UNLIKELY(0 >= size)
             || OB_UNLIKELY(0 >= align)
             || OB_UNLIKELY(0 != (align & (align - 1)))
             || OB_UNLIKELY(align > (page_size_ / 2))) {
    tmp_ret = OB_INVALID_ARGUMENT;
    LOG_ERROR_RET(tmp_ret, "small arena alloc error, invalid argument", "ret", tmp_ret, K(size),
              K(align), K(page_size_));
  } else if (need_large_page_(size, align)) {
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
    ret_ptr = do_alloc_large_(size, align, hold_bytes, used_bytes);
#else
    ret_ptr = do_alloc_large_(size, align);
#endif
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
    ++large_alloc_count_;
    if (NULL != ret_ptr) {
      life_stat_.hold_bytes_ += hold_bytes;
      life_stat_.large_page_hold_bytes_ += hold_bytes;
      life_stat_.large_page_count_ += 1;
      update_alloc_stat_(size, used_bytes, false/*from_local_page*/, true/*from_large_page*/);
    }
#endif
  } else {
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
    ret_ptr = do_alloc_normal_(size, align, used_bytes, from_local_page);
#else
    ret_ptr = do_alloc_normal_(size, align);
#endif
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
    ++small_alloc_count_;
    if (NULL != ret_ptr) {
      update_alloc_stat_(size, used_bytes, from_local_page, false/*from_large_page*/);
    }
#endif
  }
  return ret_ptr;
}

inline bool ObSmallArena::need_large_page_(const int64_t size, const int64_t align)
{
  // Use subtraction-based bounds so an extreme request cannot overflow the comparison.
  const int64_t align_padding = align - 1;
  return SMALL_PAGE_HEADER_SIZE >= page_size_
      || align_padding > page_size_ - SMALL_PAGE_HEADER_SIZE
      || size > page_size_ - SMALL_PAGE_HEADER_SIZE - align_padding;
}

// alloc large page from large_arena
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
void* ObSmallArena::do_alloc_large_(const int64_t size, const int64_t align,
    int64_t &hold_bytes, int64_t &used_bytes)
#else
void* ObSmallArena::do_alloc_large_(const int64_t size, const int64_t align)
#endif
{
  void *ret_ptr = NULL;
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  hold_bytes = 0;
  used_bytes = 0;
#endif
  if (OB_ISNULL(large_allocator_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid large allocator", K(large_allocator_));
  } else if (OB_UNLIKELY(size > INT64_MAX - LARGE_PAGE_HEADER_SIZE)
      || OB_UNLIKELY(align - 1 > INT64_MAX - LARGE_PAGE_HEADER_SIZE - size)) {
    LOG_ERROR_RET(OB_SIZE_OVERFLOW, "large page allocation size overflow",
        K(size), K(align), K(LARGE_PAGE_HEADER_SIZE));
  } else {
    const int64_t alloc_size = size + LARGE_PAGE_HEADER_SIZE + align - 1;
    LargePage *large_page = static_cast<LargePage *>(large_allocator_->alloc(alloc_size));
    if (OB_ISNULL(large_page)) {
      LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "alloc large page fail", K(alloc_size));
    } else {
      const int64_t start_addr = reinterpret_cast<int64_t>(large_page->addr_);
      const int64_t aligned_addr = upper_align(start_addr, align);
      ret_ptr = reinterpret_cast<void *>(aligned_addr);
      large_page->next_ = large_page_list_;
      large_page_list_ = large_page;
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
      hold_bytes = alloc_size;
      used_bytes = aligned_addr + size - start_addr;
#endif
    }
  }
  return ret_ptr;
}

bool ObSmallArena::alloc_small_page_(const int64_t size, const int64_t align)
{
  bool success = false;
  SmallPage *new_cur_page = NULL;
  void *ptr = NULL;
  const int64_t alloc_size = get_dynamic_small_page_size_(size, align);

  if (OB_ISNULL(large_allocator_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid page allocator", K(large_allocator_));
  } else if (OB_UNLIKELY(alloc_size <= SMALL_PAGE_HEADER_SIZE || alloc_size > page_size_)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid small page size",
        K(alloc_size), K(page_size_), K(size), K(align), K(SMALL_PAGE_HEADER_SIZE));
  } else if (OB_ISNULL(ptr = large_allocator_->alloc(alloc_size))) {
    LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "alloc small page error",
        K(ptr), K(alloc_size), K(page_size_), K(size), K(align));
  } else {
    new_cur_page = new (ptr) SmallPage(alloc_size);
    new_cur_page->next_ = small_page_list_;
    small_page_list_ = new_cur_page;
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
    life_stat_.hold_bytes_ += alloc_size;
    life_stat_.small_page_hold_bytes_ += alloc_size;
    life_stat_.small_page_count_ += 1;
#endif
    update_next_small_page_size_(alloc_size);
    success = true;
  }

  return success;
}

#ifdef ENABLE_CDC_PERF_DEBUG_STAT
void *ObSmallArena::alloc_from_page_(SmallPage &page, const int64_t size,
    const int64_t align, int64_t &used_bytes)
#else
void *ObSmallArena::alloc_from_page_(SmallPage &page, const int64_t size, const int64_t align)
#endif
{
  void *ptr = NULL;
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  used_bytes = 0;
#endif
  const int64_t start_addr = reinterpret_cast<int64_t>(page.addr_);
  const int64_t old_offset = page.offset_;
  const int64_t cur_addr = start_addr + old_offset;
  const int64_t aligned_addr = upper_align(cur_addr, align);
  const int64_t avail_size = page.capacity_ - (aligned_addr - start_addr + SMALL_PAGE_HEADER_SIZE);

  // Find pages with more free space than the requested size
  if (avail_size >= size) {
    ptr = reinterpret_cast<void *>(aligned_addr);
    page.offset_ = aligned_addr + size - start_addr;
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
    used_bytes = page.offset_ - old_offset;
#endif
  }

  return ptr;
}

#ifdef ENABLE_CDC_PERF_DEBUG_STAT
void* ObSmallArena::try_alloc_(const int64_t size, const int64_t align,
    int64_t &used_bytes, bool &from_local_page)
#else
void* ObSmallArena::try_alloc_(const int64_t size, const int64_t align)
#endif
{
  void* ret_ptr = NULL;
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  used_bytes = 0;
  from_local_page = false;
#endif

  if (NULL != small_page_list_) {
    int64_t depth = 0;
    SmallPage *page = small_page_list_;

    // Iterate through the list of small pages to find pages with enough free space
    // The purpose is to avoid having too many empty pages
    while (NULL == ret_ptr && NULL != page && depth++ < MAX_FIND_PAGE_DEPTH) {
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
      ret_ptr = alloc_from_page_(*page, size, align, used_bytes);
#else
      ret_ptr = alloc_from_page_(*page, size, align);
#endif
      page = page->next_;
    }
  }

  // If no suitable page is found from the small page list, check if there is enough space on the local cache page
  if (NULL == ret_ptr && NULL != local_page_) {
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
    ret_ptr = alloc_from_page_(*local_page_, size, align, used_bytes);
    from_local_page = (NULL != ret_ptr);
#else
    ret_ptr = alloc_from_page_(*local_page_, size, align);
#endif
  }

  return ret_ptr;
}

#ifdef ENABLE_CDC_PERF_DEBUG_STAT
void* ObSmallArena::do_alloc_normal_(const int64_t size, const int64_t align,
    int64_t &used_bytes, bool &from_local_page)
#else
void* ObSmallArena::do_alloc_normal_(const int64_t size, const int64_t align)
#endif
{
  void *ret_ptr = NULL;
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  ret_ptr = try_alloc_(size, align, used_bytes, from_local_page);
#else
  ret_ptr = try_alloc_(size, align);
#endif
  if (NULL == ret_ptr) {
    if (alloc_small_page_(size, align)) {
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
      ret_ptr = try_alloc_(size, align, used_bytes, from_local_page);
#else
      ret_ptr = try_alloc_(size, align);
#endif
    }
  }
  return ret_ptr;
}

int64_t ObSmallArena::get_dynamic_small_page_size_(const int64_t size, const int64_t align) const
{
  const int64_t required_page_size = size + SMALL_PAGE_HEADER_SIZE
      + (align <= static_cast<int64_t>(sizeof(void *)) ? 0 : align - 1);
  int64_t alloc_size = std::min(page_size_, std::max(FIRST_SMALL_PAGE_SIZE, next_small_page_size_));
  while (alloc_size < required_page_size && alloc_size < page_size_) {
    alloc_size = alloc_size > page_size_ / 2 ? page_size_ : alloc_size * 2;
  }
  return alloc_size;
}

void ObSmallArena::update_next_small_page_size_(const int64_t allocated_page_size)
{
  if (page_size_ > 0) {
    const int64_t doubled_page_size = allocated_page_size > page_size_ / 2
        ? page_size_
        : allocated_page_size * 2;
    next_small_page_size_ = std::min(page_size_, std::max(FIRST_SMALL_PAGE_SIZE, doubled_page_size));
  }
}

void ObSmallArena::reset_next_small_page_size_()
{
  next_small_page_size_ = page_size_ > 0
      ? std::min(page_size_, FIRST_SMALL_PAGE_SIZE)
      : FIRST_SMALL_PAGE_SIZE;
}

#ifdef ENABLE_CDC_PERF_DEBUG_STAT
void ObSmallArena::update_alloc_stat_(const int64_t request_bytes,
    const int64_t used_bytes, const bool from_local_page, const bool from_large_page)
{
  life_stat_.request_bytes_ += request_bytes;
  life_stat_.used_bytes_ += used_bytes;
  if (from_large_page) {
    life_stat_.large_page_used_bytes_ += used_bytes;
  } else {
    life_stat_.small_page_used_bytes_ += used_bytes;
  }
  if (request_bytes > life_stat_.max_alloc_size_) {
    life_stat_.max_alloc_size_ = request_bytes;
  }
  if (from_local_page
      && NULL != local_page_
      && local_page_->offset_ > life_stat_.local_page_high_water_) {
    life_stat_.local_page_high_water_ = local_page_->offset_;
  }
}

void ObSmallArena::record_local_page_hold_()
{
  if (NULL != local_page_) {
    life_stat_.hold_bytes_ += local_page_->capacity_;
    life_stat_.local_page_hold_bytes_ += local_page_->capacity_;
    life_stat_.local_page_count_ += 1;
  }
}

bool ObSmallArena::collect_life_stat_(LifeStat &stat)
{
  stat = life_stat_;
  stat.small_alloc_count_ = small_alloc_count_;
  stat.large_alloc_count_ = large_alloc_count_;
  clear_life_stat_();

  return stat.hold_bytes_ > 0
      || stat.request_bytes_ > 0
      || stat.used_bytes_ > 0
      || stat.small_alloc_count_ > 0
      || stat.large_alloc_count_ > 0;
}

void ObSmallArena::clear_life_stat_()
{
  life_stat_ = LifeStat();
  reset_next_small_page_size_();
}

void ObSmallArena::record_life_stat_(const LifeStat &stat)
{
  SmallArenaLifeStatSummary &summary = g_small_arena_life_stat;
  (void)ATOMIC_AAF(&summary.sample_count_, 1);
  (void)ATOMIC_AAF(&summary.request_bytes_, stat.request_bytes_);
  (void)ATOMIC_AAF(&summary.used_bytes_, stat.used_bytes_);
  (void)ATOMIC_AAF(&summary.hold_bytes_, stat.hold_bytes_);
  (void)ATOMIC_AAF(&summary.local_page_hold_bytes_, stat.local_page_hold_bytes_);
  (void)ATOMIC_AAF(&summary.small_page_hold_bytes_, stat.small_page_hold_bytes_);
  (void)ATOMIC_AAF(&summary.large_page_hold_bytes_, stat.large_page_hold_bytes_);
  (void)ATOMIC_AAF(&summary.small_page_used_bytes_, stat.small_page_used_bytes_);
  (void)ATOMIC_AAF(&summary.large_page_used_bytes_, stat.large_page_used_bytes_);
  (void)ATOMIC_AAF(&summary.small_alloc_count_, stat.small_alloc_count_);
  (void)ATOMIC_AAF(&summary.large_alloc_count_, stat.large_alloc_count_);
  (void)ATOMIC_AAF(&summary.local_page_count_, stat.local_page_count_);
  (void)ATOMIC_AAF(&summary.small_page_count_, stat.small_page_count_);
  (void)ATOMIC_AAF(&summary.large_page_count_, stat.large_page_count_);
  update_max_life_stat_(&summary.max_request_bytes_, stat.request_bytes_);
  update_max_life_stat_(&summary.max_used_bytes_, stat.used_bytes_);
  update_max_life_stat_(&summary.max_hold_bytes_, stat.hold_bytes_);
  update_max_life_stat_(&summary.max_alloc_size_, stat.max_alloc_size_);
  update_max_life_stat_(&summary.max_local_page_high_water_, stat.local_page_high_water_);
  (void)ATOMIC_AAF(summary.request_buckets_ + get_life_stat_bucket_idx_(stat.request_bytes_), 1);
  (void)ATOMIC_AAF(summary.used_buckets_ + get_life_stat_bucket_idx_(stat.used_bytes_), 1);
  (void)ATOMIC_AAF(summary.hold_buckets_ + get_life_stat_bucket_idx_(stat.hold_bytes_), 1);
  (void)ATOMIC_AAF(summary.local_page_high_water_buckets_
      + get_life_stat_bucket_idx_(stat.local_page_high_water_), 1);

  if (REACH_TIME_INTERVAL(SMALL_ARENA_LIFE_STAT_PRINT_INTERVAL)) {
    print_small_arena_life_stat_();
  }
}
#endif

void ObSmallArena::do_reset_small_pages_()
{
  SmallPage *iter = NULL;
  SmallPage *next = NULL;

  iter = small_page_list_;
  while (NULL != iter) {
    next = iter->next_;
    iter->~SmallPage();
    if (NULL != large_allocator_) {
      large_allocator_->free(iter);
    }
    iter = next;
  }

  small_page_list_ = NULL;
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  small_alloc_count_ = 0;
#endif
}

void ObSmallArena::do_reset_large_pages_()
{
  if (NULL != large_allocator_) {
    LargePage *iter = NULL;
    LargePage *next = NULL;
    iter = large_page_list_;
    while (NULL != iter) {
      next = iter->next_;
      iter->~LargePage();
      large_allocator_->free(iter);
      iter = next;
    }
    large_page_list_ = NULL;
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
    large_alloc_count_ = 0;
#endif
  }
}

#ifdef ENABLE_CDC_PERF_DEBUG_STAT
int64_t ObSmallArena::get_small_alloc_count() const
{
  ObSmallSpinLockGuard<ObByteLock> guard(lock_);
  return small_alloc_count_;
}

int64_t ObSmallArena::get_large_alloc_count() const
{
  ObSmallSpinLockGuard<ObByteLock> guard(lock_);
  return large_alloc_count_;
}
#endif

} // namespace libobcdc
} // ns oceanbase
