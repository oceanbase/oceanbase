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

#include "ob_small_arena.h"

#include "lib/utility/utility.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
using namespace common;
namespace libobcdc
{
ObSmallArena::ObSmallArena() :
    large_allocator_(NULL),
    page_size_(0),
    local_page_(NULL),
    small_page_list_(NULL),
    large_page_list_(NULL),
    small_alloc_count_(0),
    large_alloc_count_(0),
    lock_()
{
}

ObSmallArena::~ObSmallArena()
{
  do_reset_small_pages_();
  do_reset_large_pages_();

  large_allocator_ = NULL;
  page_size_ = 0;
  local_page_ = NULL;
  small_page_list_ = NULL;
  large_page_list_ = NULL;
  small_alloc_count_ = 0;
  large_alloc_count_ = 0;
}

void ObSmallArena::reset()
{
  ObSmallSpinLockGuard<ObByteLock> guard(lock_);
  do_reset_small_pages_();
  do_reset_large_pages_();

  // Require external local cache pages to be reclaimed before resetting
  if (NULL != local_page_) {
    local_page_->reset();
  }
}

void ObSmallArena::set_allocator(const int64_t page_size,
    common::ObIAllocator &large_allocator)
{
  large_allocator_ = &large_allocator;
  page_size_ = page_size;
}

void ObSmallArena::set_prealloc_page(void *page)
{
  if (NULL != local_page_) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "prealloc page has been set", K(local_page_), K(page));
  } else if (NULL != page) {
    local_page_ = new(page) SmallPage();
  }
}

void ObSmallArena::revert_prealloc_page(void *&page)
{
  page = local_page_;

  if (NULL != local_page_) {
    local_page_->~SmallPage();
  }

  local_page_ = NULL;
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
  ObSmallSpinLockGuard<ObByteLock> guard(lock_);
  if (OB_UNLIKELY(!is_valid_())) {
    tmp_ret = OB_ERR_UNEXPECTED;
    LOG_ERROR_RET(tmp_ret, "small arena is not valid", K(large_allocator_), K(page_size_));
  } else if (OB_UNLIKELY(0 >= size)
             || OB_UNLIKELY(0 != (align & (align - 1)))
             || OB_UNLIKELY(align > (page_size_ / 2))) {
    tmp_ret = OB_INVALID_ARGUMENT;
    LOG_ERROR_RET(tmp_ret, "small arena alloc error, invalid argument", "ret", tmp_ret, K(size),
              K(align), K(page_size_));
  } else if (need_large_page_(size, align)) {
    ret_ptr = do_alloc_large_(size, align);
    ATOMIC_INC(&large_alloc_count_);
  } else {
    ret_ptr = do_alloc_normal_(size, align);
    ATOMIC_INC(&small_alloc_count_);
  }
  return ret_ptr;
}

inline bool ObSmallArena::need_large_page_(const int64_t size, const int64_t align)
{
  return (size + SMALL_PAGE_HEADER_SIZE + (align - 1) > page_size_);
}

// alloc large page from large_arena
void* ObSmallArena::do_alloc_large_(const int64_t size, const int64_t align)
{
  void *ret_ptr = NULL;
  if (OB_ISNULL(large_allocator_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid large allocator", K(large_allocator_));
  } else {
    int64_t alloc_size = size + LARGE_PAGE_HEADER_SIZE + align - 1;
    LargePage *large_page = static_cast<LargePage *>(large_allocator_->alloc(alloc_size));
    if (OB_ISNULL(large_page)) {
      LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "alloc large page fail", K(alloc_size));
    } else {
      int64_t start_addr = reinterpret_cast<int64_t>(large_page->addr_);
      ret_ptr = reinterpret_cast<void *>(upper_align(start_addr, align));
      large_page->next_ = large_page_list_;
      large_page_list_ = large_page;
    }
  }
  return ret_ptr;
}

void ObSmallArena::alloc_small_page_()
{
  SmallPage *new_cur_page = NULL;
  void *ptr = NULL;
  ObMemAttr mem_attr;
  mem_attr.label_ = common::ObModIds::OB_LOG_PART_TRANS_TASK_SMALL;

  if (OB_ISNULL(ptr = ob_malloc(page_size_, mem_attr))) {
    LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "alloc small page error", K(ptr), K(page_size_));
  } else {
    new_cur_page = new (ptr) SmallPage();
    new_cur_page->next_ = small_page_list_;
    small_page_list_ = new_cur_page;
  }
}

void *ObSmallArena::alloc_from_page_(SmallPage &page, const int64_t size, const int64_t align)
{
  void *ptr = NULL;
  int64_t start_addr = reinterpret_cast<int64_t>(page.addr_);
  int64_t cur_addr = start_addr + page.offset_;
  int64_t aligned_addr = upper_align(cur_addr, align);
  int64_t avail_size = page_size_ - (aligned_addr - start_addr + SMALL_PAGE_HEADER_SIZE);

  // Find pages with more free space than the requested size
  if (avail_size >= size) {
    ptr = reinterpret_cast<void *>(aligned_addr);
    page.offset_ = aligned_addr + size - start_addr;
  }

  return ptr;
}

void* ObSmallArena::try_alloc_(const int64_t size, const int64_t align)
{
  void* ret_ptr = NULL;

  if (NULL != small_page_list_) {
    int64_t depth = 0;
    SmallPage *page = small_page_list_;

    // Iterate through the list of small pages to find pages with enough free space
    // The purpose is to avoid having too many empty pages
    while (NULL == ret_ptr && NULL != page && depth++ < MAX_FIND_PAGE_DEPTH) {
      ret_ptr = alloc_from_page_(*page, size, align);
      page = page->next_;
    }
  }

  // If no suitable page is found from the small page list, check if there is enough space on the local cache page
  if (NULL == ret_ptr && NULL != local_page_) {
    ret_ptr = alloc_from_page_(*local_page_, size, align);
  }

  return ret_ptr;
}

void* ObSmallArena::do_alloc_normal_(const int64_t size, const int64_t align)
{
  void *ret_ptr = NULL;
  ret_ptr = try_alloc_(size, align);
  if (NULL == ret_ptr) {
    alloc_small_page_();
    ret_ptr = try_alloc_(size, align);
  }
  return ret_ptr;
}

void ObSmallArena::do_reset_small_pages_()
{
  SmallPage *iter = NULL;
  SmallPage *next = NULL;
  iter = small_page_list_;
  while (NULL != iter) {
    next = iter->next_;
    iter->~SmallPage();
    ob_free(iter);
    iter = next;
  }

  small_page_list_ = NULL;
  small_alloc_count_ = 0;
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
    large_alloc_count_ = 0;
  }
}

int64_t ObSmallArena::get_small_alloc_count() const
{
  return ATOMIC_LOAD(&small_alloc_count_);
}

int64_t ObSmallArena::get_large_alloc_count() const
{
  return ATOMIC_LOAD(&large_alloc_count_);
}

} // namespace libobcdc
} // ns oceanbase
