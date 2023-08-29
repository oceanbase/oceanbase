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

#include "malloc_hook.h"
#include "lib/utility/ob_defer.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "lib/allocator/ob_malloc.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;

static bool g_malloc_hook_inited = false;

void init_malloc_hook()
{
  g_malloc_hook_inited = true;
}

uint64_t up_align(uint64_t x, uint64_t align)
{
  return (x + (align - 1)) & ~(align - 1);
}

struct Header
{
  static const uint32_t MAGIC_CODE = 0XA1B2C3D1;
  static const uint32_t SIZE;
  Header(int32_t size, bool from_mmap)
    : magic_code_(MAGIC_CODE),
      data_size_(size),
      offset_(0),
      from_mmap_(from_mmap)
  {}
  bool check_magic_code() const { return MAGIC_CODE == magic_code_; }
  void mark_unused() { magic_code_ &= ~0x1; }
  static Header *ptr2header(void *ptr) { return reinterpret_cast<Header*>((char*)ptr - SIZE); }
  uint32_t magic_code_;
  int32_t data_size_;
  uint32_t offset_;
  uint8_t from_mmap_;
  char padding_[3];
  char data_[0];
} __attribute__((aligned (16)));

const uint32_t Header::SIZE = offsetof(Header, data_);
const size_t max_retry_size = 2 << 30; // 2G

void *ob_malloc_retry(size_t size)
{
  void *ptr = nullptr;
  do {
    ObMemAttr attr = ObMallocHookAttrGuard::get_tl_mem_attr();
    SET_USE_500(attr);
    attr.ctx_id_ = ObCtxIds::GLIBC;
    ptr = ob_malloc(size, attr);
    if (OB_ISNULL(ptr)) {
      attr.tenant_id_ = OB_SERVER_TENANT_ID;
      ptr = ob_malloc(size, attr);
    }
    if (OB_ISNULL(ptr)) {
      ::usleep(10000);  // 10ms
    }
  } while (OB_ISNULL(ptr) && !(size > max_retry_size || 0 == size));
  return ptr;
}

static inline void *ob_mmap(void *addr, size_t length, int prot, int flags, int fd, loff_t offset)
{
  void *ptr = (void*)syscall(SYS_mmap, addr, length, prot, flags, fd, offset);
  if (OB_UNLIKELY(!is_ob_mem_mgr_path()) && OB_LIKELY(MAP_FAILED != ptr)) {
    const int64_t page_size = get_page_size();
    inc_divisive_mem_size(upper_align(length, page_size));
  }
  return ptr;
}

static inline int ob_munmap(void *addr, size_t length)
{
  if (OB_UNLIKELY(!is_ob_mem_mgr_path())) {
    const int64_t page_size = get_page_size();
    dec_divisive_mem_size(upper_align(length, page_size));
  }
  return syscall(SYS_munmap, addr, length);
}

void *ob_malloc_hook(size_t size, const void *)
{
  void *ptr = nullptr;
  size_t real_size = size + Header::SIZE;
  void *tmp_ptr = nullptr;
  bool from_mmap = false;
  if (OB_UNLIKELY(!g_malloc_hook_inited || in_hook())) {
    if (MAP_FAILED == (tmp_ptr = ob_mmap(nullptr, real_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
      tmp_ptr = nullptr;
    }
    from_mmap = true;
  } else {
    bool in_hook_bak = in_hook();
    in_hook()= true;
    DEFER(in_hook()= in_hook_bak);
    tmp_ptr = ob_malloc_retry(real_size);
  }
  if (OB_LIKELY(tmp_ptr != nullptr)) {
    abort_unless(size <= INT32_MAX);
    auto *header = new (tmp_ptr) Header((int32_t)size, from_mmap);
    ptr = header->data_;
  }
  return ptr;
}

void ob_free_hook(void *ptr, const void *)
{
  if (OB_LIKELY(ptr != nullptr)) {
    auto *header = Header::ptr2header(ptr);
    abort_unless(header->check_magic_code());
    header->mark_unused();
    void *orig_ptr = (char*)header - header->offset_;
    if (OB_UNLIKELY(header->from_mmap_)) {
      ob_munmap(orig_ptr, header->data_size_ + Header::SIZE + header->offset_);
    } else {
      bool in_hook_bak = in_hook();
      in_hook()= true;
      DEFER(in_hook()= in_hook_bak);
      ob_free(orig_ptr);
    }
  }
}

void *ob_realloc_hook(void *ptr, size_t size, const void *caller)
{
  void *nptr = nullptr;
  size_t real_size = size + Header::SIZE;
  void *tmp_ptr = nullptr;
  bool from_mmap = false;
  if (OB_UNLIKELY(!g_malloc_hook_inited || in_hook())) {
    if (MAP_FAILED == (tmp_ptr = ob_mmap(nullptr, real_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
      tmp_ptr = nullptr;
    }
    from_mmap = true;
  } else {
    bool in_hook_bak = in_hook();
    in_hook()= true;
    DEFER(in_hook()= in_hook_bak);
    tmp_ptr = ob_malloc_retry(real_size);
  }
  if (OB_LIKELY(tmp_ptr != nullptr)) {
    abort_unless(size <= INT32_MAX);
    auto *header = new (tmp_ptr) Header((int32_t)size, from_mmap);
    nptr = header->data_;
    if (ptr != nullptr) {
      auto *old_header = Header::ptr2header(ptr);
      abort_unless(old_header->check_magic_code());
      memmove(nptr, ptr, MIN(old_header->data_size_, size));
      ob_free_hook(old_header->data_, caller);
    }
  }
  return nptr;
}

void *ob_memalign_hook(size_t alignment, size_t size, const void *)
{
  void *ptr = nullptr;
  if (OB_LIKELY(size > 0)) {
    // Make sure alignment is power of 2
    {
      size_t a = 8;
      while (a < alignment)
        a <<= 1;
      alignment = a;
    }
    size_t real_size = 2 * MAX(alignment, Header::SIZE) + size;
    void *tmp_ptr = nullptr;
    bool from_mmap = false;
    if (OB_UNLIKELY(!g_malloc_hook_inited || in_hook())) {
      if (MAP_FAILED == (tmp_ptr = ob_mmap(nullptr, real_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
        tmp_ptr = nullptr;
      }
      from_mmap = true;
    } else {
      bool in_hook_bak = in_hook();
      in_hook()= true;
      DEFER(in_hook()= in_hook_bak);
      tmp_ptr = ob_malloc_retry(real_size);
    }
    if (OB_LIKELY(tmp_ptr != nullptr)) {
      char *start = (char *)tmp_ptr + Header::SIZE;
      char *align_ptr = (char *)up_align(reinterpret_cast<int64_t>(start), alignment);
      char *pheader = align_ptr - Header::SIZE;
      size_t offset = pheader - (char*)tmp_ptr;
      abort_unless(size <= INT32_MAX);
      auto *header = new (pheader) Header((int32_t)size, from_mmap);
      header->offset_ = (uint32_t)offset;
      ptr = header->data_;
    }
  }
  return ptr;
}

EXTERN_C_BEGIN

void *ob_mmap_hook(void *addr, size_t length, int prot, int flags, int fd, loff_t offset)
{
  return ob_mmap(addr, length, prot, flags, fd, offset);
}

int ob_munmap_hook(void *addr, size_t length)
{
  return ob_munmap(addr, length);
}

EXTERN_C_END

#if !defined(__MALLOC_HOOK_VOLATILE)
#define MALLOC_HOOK_MAYBE_VOLATILE /**/
#else
#define MALLOC_HOOK_MAYBE_VOLATILE __MALLOC_HOOK_VOLATILE
#endif

EXTERN_C_BEGIN

__attribute__((visibility("default"))) void *(*MALLOC_HOOK_MAYBE_VOLATILE __malloc_hook)(size_t, const void *) = ob_malloc_hook;
__attribute__((visibility("default"))) void (*MALLOC_HOOK_MAYBE_VOLATILE __free_hook)(void *, const void *) = ob_free_hook;
__attribute__((visibility("default"))) void *(*MALLOC_HOOK_MAYBE_VOLATILE __realloc_hook)(void *, size_t, const void *) = ob_realloc_hook;
__attribute__((visibility("default"))) void *(*MALLOC_HOOK_MAYBE_VOLATILE __memalign_hook)(size_t, size_t, const void *) = ob_memalign_hook;

__attribute__((visibility("default"))) void *mmap(void *addr, size_t, int, int, int, loff_t) __attribute__((weak,alias("ob_mmap_hook")));
__attribute__((visibility("default"))) void *mmap64(void *addr, size_t, int, int, int, loff_t) __attribute__((weak,alias("ob_mmap_hook")));
__attribute__((visibility("default"))) int munmap(void *addr, size_t length) __attribute__((weak,alias("ob_munmap_hook")));

size_t malloc_usable_size(void *ptr)
{
  size_t ret = 0;
  if (OB_LIKELY(ptr != nullptr)) {
    auto *header = Header::ptr2header(ptr);
    abort_unless(header->check_magic_code());
    ret = header->data_size_;
  }
  return ret;
}

EXTERN_C_END
