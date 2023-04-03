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
    SmallPage() : offset_(0), next_(NULL) {}
    ~SmallPage() { reset(); }

    void reset() { offset_ = 0; next_ = NULL; }

    int64_t   offset_;
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

public:
  static const int64_t SMALL_PAGE_HEADER_SIZE = sizeof(SmallPage);
  static const int64_t LARGE_PAGE_HEADER_SIZE = sizeof(LargePage);
  static const int64_t MAX_FIND_PAGE_DEPTH = 10;

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
  int64_t get_small_alloc_count() const;
  int64_t get_large_alloc_count() const;

  void set_allocator(const int64_t page_size, common::ObIAllocator &large_allocator);

  // Set pre-assigned pages
  void set_prealloc_page(void *page);

  // Recycle pre-allocated pages
  void revert_prealloc_page(void *&page);

private:
  bool is_valid_() const;
  bool need_large_page_(const int64_t size, const int64_t align);
  void *do_alloc_large_(const int64_t size, const int64_t align);
  void *try_alloc_(const int64_t size, const int64_t align);
  void alloc_small_page_();
  void *do_alloc_normal_(const int64_t size, const int64_t align);
  void do_reset_small_pages_();
  void do_reset_large_pages_();
  void *alloc_from_page_(SmallPage &page, const int64_t size, const int64_t align);

private:
  common::ObIAllocator        *large_allocator_;  // large allocator
  int64_t                     page_size_;         // size of page

  // Local cache pages are only used to allocate small blocks of memory
  // Local cache pages are not considered when determining whether a large page needs to be allocated
  //
  // Allow local cache to be empty
  SmallPage                   *local_page_;                     // page cache in local
  SmallPage                   *small_page_list_ CACHE_ALIGNED;  // page list for small page
  LargePage                   *large_page_list_ CACHE_ALIGNED;  // page list for large page

  int64_t                     small_alloc_count_ CACHE_ALIGNED;
  int64_t                     large_alloc_count_ CACHE_ALIGNED;

  mutable common::ObByteLock  lock_;

  DISALLOW_COPY_AND_ASSIGN(ObSmallArena);
};

} // namespace libobcdc
} // ns oceanbase

#endif
