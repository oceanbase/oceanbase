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

#ifndef OCEANBASE_LIBOBCDC_SRC_OB_SMALL_ARENA_
#define OCEANBASE_LIBOBCDC_SRC_OB_SMALL_ARENA_

#include "lib/allocator/ob_allocator.h"
#include "lib/lock/ob_small_spin_lock.h"

namespace oceanbase
{
namespace libobcdc
{

/*
  Allocator for libobcdc specific scenarios.
  Note: The user needs to ensure that the parameter @sa passed in remains valid for the lifetime of this SmallArena.
*/
class ObSmallArena : public common::ObIAllocator
{
  struct SmallPage
  {
    explicit SmallPage(const int64_t capacity = 0) : offset_(0), capacity_(capacity), next_(NULL) {}
    ~SmallPage() { reset(); }

    void reset() { offset_ = 0; next_ = NULL; }

    int64_t   offset_;
    int64_t   capacity_;
    SmallPage *next_;
    char      addr_[0];
  };

  struct LargePage
  {
    LargePage() : next_(NULL) {}
    ~LargePage() { next_ = NULL; }

    LargePage   *next_;
    char        addr_[0];
  };

  // This diagnostic switch changes ObSmallArena's layout and must be defined build-wide.
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  struct LifeStat
  {
    LifeStat()
        : request_bytes_(0),
          used_bytes_(0),
          hold_bytes_(0),
          local_page_hold_bytes_(0),
          small_page_hold_bytes_(0),
          large_page_hold_bytes_(0),
          small_page_used_bytes_(0),
          large_page_used_bytes_(0),
          local_page_high_water_(0),
          max_alloc_size_(0),
          small_alloc_count_(0),
          large_alloc_count_(0),
          local_page_count_(0),
          small_page_count_(0),
          large_page_count_(0)
    {}

    int64_t request_bytes_;
    int64_t used_bytes_;
    int64_t hold_bytes_;
    int64_t local_page_hold_bytes_;
    int64_t small_page_hold_bytes_;
    int64_t large_page_hold_bytes_;
    int64_t small_page_used_bytes_;
    int64_t large_page_used_bytes_;
    int64_t local_page_high_water_;
    int64_t max_alloc_size_;
    int64_t small_alloc_count_;
    int64_t large_alloc_count_;
    int64_t local_page_count_;
    int64_t small_page_count_;
    int64_t large_page_count_;
  };
#endif

public:
  static const int64_t SMALL_PAGE_HEADER_SIZE = sizeof(SmallPage);
  static const int64_t LARGE_PAGE_HEADER_SIZE = sizeof(LargePage);
  static const int64_t MAX_FIND_PAGE_DEPTH = 10;
  static const int64_t FIRST_SMALL_PAGE_SIZE = 256;

public:
  ObSmallArena();
  ~ObSmallArena();
  void *alloc_aligned(const int64_t size, const int64_t align);
  virtual void *alloc(const int64_t size, const common::ObMemAttr &attr) override
  {
    UNUSEDx(attr);
    return alloc(size);
  }
  virtual void *alloc(const int64_t size) override;
  virtual void free(void *ptr) override { UNUSED(ptr); }
  void reset();
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  int64_t get_small_alloc_count() const;
  int64_t get_large_alloc_count() const;
#endif

  void set_allocator(const int64_t page_size, common::ObIAllocator &large_allocator);

  // Set pre-assigned pages
  void set_prealloc_page(void *page, const int64_t page_size = 0);

  // Recycle pre-allocated pages
  void revert_prealloc_page(void *&page);

private:
  bool is_valid_() const;
  bool need_large_page_(const int64_t size, const int64_t align);
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  void *do_alloc_large_(const int64_t size, const int64_t align,
      int64_t &hold_bytes, int64_t &used_bytes);
  void *try_alloc_(const int64_t size, const int64_t align,
      int64_t &used_bytes, bool &from_local_page);
#else
  void *do_alloc_large_(const int64_t size, const int64_t align);
  void *try_alloc_(const int64_t size, const int64_t align);
#endif
  bool alloc_small_page_(const int64_t size, const int64_t align);
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  void *do_alloc_normal_(const int64_t size, const int64_t align,
      int64_t &used_bytes, bool &from_local_page);
#else
  void *do_alloc_normal_(const int64_t size, const int64_t align);
#endif
  void do_reset_small_pages_();
  void do_reset_large_pages_();
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  void *alloc_from_page_(SmallPage &page, const int64_t size,
      const int64_t align, int64_t &used_bytes);
#else
  void *alloc_from_page_(SmallPage &page, const int64_t size, const int64_t align);
#endif
  int64_t get_dynamic_small_page_size_(const int64_t size, const int64_t align) const;
  void update_next_small_page_size_(const int64_t allocated_page_size);
  void reset_next_small_page_size_();
#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  void update_alloc_stat_(const int64_t request_bytes,
      const int64_t used_bytes, const bool from_local_page, const bool from_large_page);
  void record_local_page_hold_();
  bool collect_life_stat_(LifeStat &stat);
  void clear_life_stat_();
  static void record_life_stat_(const LifeStat &stat);
#endif

private:
  common::ObIAllocator        *large_allocator_;  // large allocator
  int64_t                     page_size_;         // size of page
  int64_t                     next_small_page_size_;

  // Local cache pages are only used to allocate small blocks of memory
  // Local cache pages are not considered when determining whether a large page needs to be allocated
  //
  // Allow local cache to be empty
  SmallPage                   *local_page_;                     // page cache in local
  SmallPage                   *small_page_list_ CACHE_ALIGNED;  // page list for small page
  LargePage                   *large_page_list_ CACHE_ALIGNED;  // page list for large page

#ifdef ENABLE_CDC_PERF_DEBUG_STAT
  int64_t                     small_alloc_count_;
  int64_t                     large_alloc_count_;
  LifeStat                    life_stat_;
#endif

  mutable common::ObByteLock  lock_;

  DISALLOW_COPY_AND_ASSIGN(ObSmallArena);
};

} // namespace libobcdc
} // ns oceanbase

#endif
