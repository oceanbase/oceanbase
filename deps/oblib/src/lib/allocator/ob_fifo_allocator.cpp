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

#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{
ObFIFOAllocator::ObFIFOAllocator(const uint64_t tenant_id /*= OB_SERVER_TENANT_ID */)
    : is_inited_(false),
      allocator_(nullptr),
      page_size_(0),
      attr_(tenant_id, ObModIds::OB_FIFO_ALLOC),
      idle_size_(0),
      max_size_(0),
      current_using_(nullptr),
      normal_used_(0),
      special_total_(0),
      lock_(ObLatchIds::OB_FIFO_ALLOCATOR_LOCK)
{
}

ObFIFOAllocator::~ObFIFOAllocator()
{
  reset();
  allocator_ = nullptr;
  is_inited_ = false;
}

int ObFIFOAllocator::init(ObIAllocator *allocator,
         const int64_t page_size,
         const ObMemAttr &attr,
         const int64_t init_size,
         const int64_t idle_size,
         const int64_t max_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(page_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret),
                  KP(allocator), K(page_size));
  }
  // For simplicity, here we let max_size not less than one page
  else if (init_size < 0 ||
           idle_size < 0 ||
           max_size < page_size ||
           init_size > max_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret),
                  K(init_size), K(idle_size), K(max_size), K(page_size));
  } else {
    if (NULL == allocator) {
      allocator_ = &malloc_allocator_;
    } else {
      allocator_ = allocator;
    }
    page_size_ = page_size;
    attr_ = attr;
    // reserve
    if (init_size > 0) {
      if (OB_FAIL(sync_idle(init_size, max_size))) {
        LOG_WARN("sync idle failed", K(init_size), K(ret));
      } else if (normal_total() < init_size) {
        ret = OB_INIT_FAIL;
        LOG_WARN("reserve failed", "normal total", normal_total(), K(init_size), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      idle_size_ = idle_size;
      max_size_ = max_size;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObFIFOAllocator::set_idle(const int64_t idle_size, const bool sync)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(lock_);
  if (OB_UNLIKELY(!is_inited_) || OB_UNLIKELY(nullptr == allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFIFOAllocator not init", K(is_inited_), KP(allocator_), KCSTRING(lbt()));
  } else if (idle_size < 0) {
    LOG_WARN("invalid arg", K(idle_size));
  } else {
    idle_size_ = idle_size;
    if (sync) {
      if (OB_FAIL(sync_idle(idle_size_, max_size_))) {
        LOG_WARN("sync idle failed", K(idle_size_), K(max_size_));
      }
    }
  }
  return ret;
}

// Separate this function is to bypass some states, such as lock, is_inited_
int ObFIFOAllocator::sync_idle(const int64_t idle_size, const int64_t max_size)
{
  int ret = OB_SUCCESS;
  if (normal_total() < idle_size) {
    int64_t need_size = std::min(idle_size, max_size) - normal_total();
    int64_t n = need_size / page_size_;
    if (n * page_size_ < need_size) n++;
    PageList new_pages;
    // 1. Apply for n pages
    // 2. If current_using_ is empty, split 1 page to current_using
    for (int64_t i = 0; i < n; i++) {
      void *ptr = allocator_->alloc(page_size_, attr_);
      if (nullptr == ptr) {
        // ignore ret
        LOG_WARN("alloc failed", K(page_size_));
        break;
      } else {
        NormalPageHeader *page = new (ptr) NormalPageHeader();
        new_pages.add_first(&page->node_);
      }
    }
    free_page_list_.push_range(new_pages);
    if (nullptr == current_using_ && free_page_list_.get_size() > 0) {
      current_using_ =
        static_cast<NormalPageHeader*>(free_page_list_.remove_first()->get_data());
      current_using_->offset_ = reinterpret_cast<char *>(current_using_) + sizeof(NormalPageHeader);
      current_using_->ref_count_ = 1;
    }
  } else {
    int64_t n = upper_align(normal_total() - std::min(idle_size, max_size), page_size_) / page_size_;
    DLIST_FOREACH_REMOVESAFE_X(iter, free_page_list_, n > 0) {
      free_page_list_.remove(iter);
      allocator_->free(iter->get_data());
      n--;
    }
    // Do not release current_using (even if no one uses current_using)
  }
  return ret;
}


int ObFIFOAllocator::set_max(const int64_t max_size, const bool sync)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(lock_);
  if (OB_UNLIKELY(!is_inited_) || OB_UNLIKELY(nullptr == allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFIFOAllocator not init", K(is_inited_), KP(allocator_), KCSTRING(lbt()));
  } else if (max_size < page_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(max_size), K(page_size_));
  } else {
    if (sync) {
      if (total() > max_size) {
        int64_t n = upper_align(total() - max_size, page_size_) / page_size_;
        DLIST_FOREACH_REMOVESAFE_X(iter, free_page_list_, n > 0) {
          free_page_list_.remove(iter);
          allocator_->free(iter->get_data());
          n--;
        }
      }
    }
    max_size_ = max_size;
  }
  return ret;
}

void ObFIFOAllocator::reset()
{
  ObLockGuard<ObSpinLock> guard(lock_);
  if (IS_NOT_INIT || OB_ISNULL(allocator_)) {
    // do nothing
  } else {
    DLIST_FOREACH_REMOVESAFE_NORET(iter, free_page_list_) {
      auto *page = iter->get_data();
      free_page_list_.remove(iter);
      allocator_->free(page);
    }

    // check if there is some pages using ?
    if (OB_ISNULL(current_using_)) {
      // reset already.
    } else if (OB_LIKELY(1 == current_using_->ref_count_)) {
      allocator_->free(current_using_);
    } else {
      LOG_ERROR_RET(OB_ERROR, "current_using_ is still used now",
                "ref_count", current_using_->ref_count_, KP(current_using_));
    }
    DLIST_FOREACH_NORET(iter, using_page_list_) {
      auto *page = iter->get_data();
      LOG_ERROR_RET(OB_ERROR, "dump using page list:  ", KP(page));
    }
    DLIST_FOREACH_NORET(iter, special_page_list_) {
      auto *page = iter->get_data();
      LOG_ERROR_RET(OB_ERROR, "dump special page list:  ", KP(page));
    }
    using_page_list_.clear();
    current_using_ = nullptr;
    special_page_list_.clear();
    normal_used_ = 0;
    special_total_ = 0;
    is_inited_ = false;
  }
}

ObFIFOAllocator::BasePageHeader *ObFIFOAllocator::get_page_header(void *p)
{
  BasePageHeader *page_header = nullptr;
  if (OB_ISNULL(p)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid argument");
  } else {
    AllocHeader *alloc_header = static_cast<AllocHeader *>(p) - 1;
    page_header = alloc_header->page_header_;
    abort_unless(PAGE_HEADER == page_header->magic_num_);
  }
  return page_header;
}

bool ObFIFOAllocator::check_param(const int64_t size, const int64_t align)
{
  bool bool_ret = true;
  if (align <= 0 || align > page_size_ / 2) {
    LOG_WARN_RET(OB_ERROR, "align is negative or too big. ", K(align));
    bool_ret = false;
  } else if (0 != (align & (align - 1))) {
    LOG_WARN_RET(OB_ERROR, "align should be 2^K. ", K(align));
    bool_ret = false;
  } else if (size <= 0 || size >= INT32_MAX) {
    // size shold < 4G, bcs we store @size at 4Bytes in alloc header.
    LOG_WARN_RET(OB_ERROR, "size should be positive and not to big.", K(size));
    bool_ret = false;
  } else {
    bool_ret = true;
  }
  return bool_ret;
}


bool ObFIFOAllocator::check_magic(void *p, int64_t &size)
{
  bool bool_ret = true;
  if (OB_ISNULL(p)) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid argument");
  } else {
    AllocHeader *alloc_header = static_cast<AllocHeader *>(p) - 1;
    size = alloc_header->size_;
    if (alloc_header->magic_num_ == ALLOC_HEADER) {
      bool_ret = true;
      alloc_header->magic_num_ = ALREADY_FREE;
    } else if (alloc_header->magic_num_ == ALREADY_FREE) {
      LOG_ERROR_RET(OB_ERROR, "Detect double free at address ", "addr", p);
      bool_ret = false;
    } else {
      // This means parameter address in free(void* p) is not a pointer
      // allocated by FIFOAllocator OR there is an overwrite
      LOG_ERROR_RET(OB_ERROR, "check allocation magic fail",  "free address", p);
      bool_ret = false;
    }
  }
  return bool_ret;
}


void *ObFIFOAllocator::alloc(const int64_t size)
{
  return alloc_align(size, 16);
}

void *ObFIFOAllocator::alloc(const int64_t size, const ObMemAttr &attr)
{
  UNUSED(attr);
  return alloc_align(size, 16);
}

void *ObFIFOAllocator::alloc_align(const int64_t size, const int64_t align)
{
  ObLockGuard<ObSpinLock> guard(lock_);
  void *ptr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    LOG_WARN_RET(OB_NOT_INIT, "ObFIFOAllocator not init");
  } else if (!check_param(size, align)) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "ObFIFOAllocator alloc(size, align) parameter Error.", K(size), K(align));
  } else if (is_normal_page_enough(size, align)) {
    ptr = alloc_normal(size, align);
  } else {
    ptr = alloc_special(size, align);
  }

  return ptr;
}

// get a new page, set current_using_ pointing to it.
void ObFIFOAllocator::alloc_new_normal_page()
{
  if (IS_NOT_INIT || OB_ISNULL(allocator_)) {
    LOG_ERROR_RET(OB_NOT_INIT, "ObFIFOAllocator not init");
  } else {
    NormalPageHeader *new_page = nullptr;
    if (free_page_list_.get_size() > 0) {
      new_page = static_cast<NormalPageHeader*>(free_page_list_.remove_first()->get_data());
    }
    if (nullptr == new_page) {
      if (total() + page_size_ <= max_size_) {
        void *ptr = allocator_->alloc(page_size_, attr_);
        if (OB_NOT_NULL(ptr)) {
          new_page = new (ptr) NormalPageHeader();
        } else {
          LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "underlying allocator return nullptr");
        }
      }
    }
    if (OB_NOT_NULL(new_page)) {
      new_page->ref_count_ = 1;
      new_page->offset_ =  reinterpret_cast<char *>(new_page) + sizeof(NormalPageHeader);
      current_using_->ref_count_--;
      if (0 == current_using_->ref_count_) {
        LOG_ERROR_RET(OB_ERROR, "current_using_->ref_count_ is 0. This could not happen.");
      }
      // we do NOT link current using into page_using_list.
      // bcs current_using_ may be not used indeed.
      using_page_list_.add_first(&current_using_->node_);
      current_using_ = new_page;
    }
  }
}

// try to alloc at current_using_,
// return nullptr if can not alloc(free space is not enough)
// or a non-zero address if succeed. */
void *ObFIFOAllocator::try_alloc(int64_t size, int64_t align)
{
  char *ptr = nullptr;
  if (nullptr == current_using_) {
    LOG_ERROR_RET(OB_ERROR, "current_using_ is null");
  } else {
    abort_unless(PAGE_HEADER == current_using_->magic_num_);
    char *offset = current_using_->offset_;
    char *end_offset = reinterpret_cast<char *>(current_using_) + page_size_;
    offset += sizeof(AllocHeader);
    offset = reinterpret_cast<char *>(upper_align(reinterpret_cast<int64_t>(offset), align));
    ptr = offset;
    offset += size;
    if (offset <= end_offset) { // good. enough space
      AllocHeader *alloc_header = reinterpret_cast<AllocHeader *>(ptr - sizeof(AllocHeader));
      alloc_header->magic_num_ = ALLOC_HEADER;
      alloc_header->size_ = static_cast<int32_t>(size);
      alloc_header->page_header_ = current_using_;
      current_using_->offset_ = offset;
      current_using_->ref_count_++;
    } else {
      ptr = nullptr;
    }
  }
  return ptr;
}

void ObFIFOAllocator::free(void *p)
{
  ObLockGuard<ObSpinLock> guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    LOG_ERROR_RET(OB_NOT_INIT, "ObFIFOAllocator not init");
  } else {
    int64_t size = 0;
    if (nullptr == p) {
      LOG_ERROR_RET(OB_ERROR, "user try to free nullptr Pointer");
    } else {
      if (!check_magic(p, size)) {
        LOG_ERROR_RET(OB_ERROR, "Error, check magic number error.");
      } else {
        BasePageHeader *page_header = get_page_header(p);
        if (OB_ISNULL(page_header)) {
          LOG_ERROR_RET(OB_ERROR, "page_header is null");
        } else {
          if (SPECIAL_FLAG == page_header->flag_) { // this is special page
            SpecialPageHeader *special_page = static_cast<SpecialPageHeader *>(page_header);
            free_special(special_page);
          } else { // this is normal page
            NormalPageHeader *normal_page = static_cast<NormalPageHeader *>(page_header);
            free_normal(normal_page, size);
          }
        }
      }
    }
  }
}

void *ObFIFOAllocator::alloc_normal(int64_t size, int64_t align)
{
  void *ptr = nullptr;
  void *new_space = nullptr;
  if (OB_UNLIKELY(!is_inited_) || OB_UNLIKELY(nullptr == allocator_)) {
    LOG_ERROR_RET(OB_NOT_INIT, "ObFIFOAllocator not init");
  } else {
    if (nullptr == current_using_) {
      if (total() + page_size_ <= max_size_) {
        new_space = allocator_->alloc(page_size_, attr_);
      }
      if (nullptr == new_space) {
        LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "can not allocate new page", K(page_size_));
      } else {
        current_using_ = new (new_space) NormalPageHeader();
        current_using_->offset_ = reinterpret_cast<char *>(current_using_) + sizeof(NormalPageHeader);
        current_using_->ref_count_ = 1;
      }
    }
    if (nullptr == current_using_ && nullptr == new_space) {
      // can not alloc new page from underlying allocator
      ptr = nullptr;
    } else {
      ptr = try_alloc(size, align);
      // current_page_do not have enough space.
      if (nullptr == ptr) {
        alloc_new_normal_page();
        ptr = try_alloc(size, align);
      }
      if (ptr != nullptr) {
        normal_used_ += size;
      }
    }
  }
  return ptr;
}

void ObFIFOAllocator::free_normal(NormalPageHeader *page, int64_t size)
{
  page->ref_count_--;
  if (page == current_using_ && 1 == current_using_->ref_count_) {
    current_using_->offset_ = reinterpret_cast<char *>(current_using_) + sizeof(NormalPageHeader);
  } else if (0 == page->ref_count_) {
    int64_t normal_total_size = normal_total();
    int64_t total_size = total();
    using_page_list_.remove(&page->node_);
    // move this page from page_using_list to page_free_list
    if (normal_total_size > idle_size_ || total_size > max_size_) {
      allocator_->free(page);
      page = nullptr;
    } else {
      free_page_list_.add_first(&page->node_);
    }
  }
  normal_used_ -= size;
}

/*
   |--------------------|
   |  SpecialPageHeader |
   |--------------------|
   |  align             |
   |--------------------|
   |  AllocHeader       |
   |--------------------|
   | user data.ptr here |
   |--------------------|
   |  hole              |
   |--------------------|
*/
void *ObFIFOAllocator::alloc_special(int64_t size, int64_t align)
{
  void *ptr = NULL;

  if (OB_UNLIKELY(!is_inited_) || OB_UNLIKELY(NULL == allocator_)) {
    LOG_ERROR_RET(OB_NOT_INIT, "ObFIFOAllocator not init", K(is_inited_));
  } else if (OB_UNLIKELY(size <= 0) || OB_UNLIKELY(align <= 0)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid argument", K(size), K(align));
  } else {
    // we have to alloc up to @real_size bytes.
    // these bytes may be before (align 1) AND after user data (align 2).
    // one of them can be zero.
    int64_t real_size = size + sizeof(SpecialPageHeader) + sizeof(AllocHeader) + align - 1;
    void *new_space = allocator_->alloc(real_size, attr_);
    if (NULL == new_space) {
      LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "can not alloc a page from underlying allocator", K(real_size));
    } else {
      ptr = reinterpret_cast<void *>(upper_align(reinterpret_cast<int64_t>(static_cast<char *>
              (new_space) + sizeof(SpecialPageHeader) + sizeof(AllocHeader)), align));
      SpecialPageHeader *special_page = new (new_space) SpecialPageHeader();
      AllocHeader *alloc_header = static_cast<AllocHeader *>(ptr) - 1;

      special_page->flag_ = SPECIAL_FLAG;
      special_page->real_size_ = real_size;

      alloc_header->magic_num_ = static_cast<int32_t>(ALLOC_HEADER);
      alloc_header->size_ = static_cast<int32_t>(size);
      alloc_header->page_header_ = special_page;

      special_page_list_.add_first(&special_page->node_);
      if (NULL != ptr) {
        special_total_ += real_size;
      }
    }
  }
  return ptr;
}

void ObFIFOAllocator::free_special(SpecialPageHeader *special_page)
{
  special_page_list_.remove(&special_page->node_);
  special_total_ -= special_page->real_size_;
  allocator_->free(special_page);
}

} // end of namespace common
} // end of namespace oceanbase
