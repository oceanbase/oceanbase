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
#include "deps/oblib/src/lib/hash/ob_hashmap.h"
#include <dlfcn.h>

#define OBMALLOC_ATTR(s) __attribute__((s))
#define OBMALLOC_EXPORT __attribute__((visibility("default")))
#define OBMALLOC_ALLOC_SIZE(s) __attribute__((alloc_size(s)))
#define OBMALLOC_NOTHROW __attribute__((nothrow))
#define LIBC_ALIAS(fn)	__attribute__((alias (#fn), used))

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;
static bool g_malloc_hook_inited = false;
typedef void* (*MemsetPtr)(void*, int, size_t);
MemsetPtr memset_ptr = nullptr;
ObMallocHook &global_malloc_hook = ObMallocHook::get_instance();
void __attribute__((constructor(0))) init_malloc_hook()
{
  // The aim of calling memset is to initialize certain states in memset,
  // and to avoid nested deadlock of memset after malloc_hook inited.
  memset(&memset_ptr, 0, sizeof(memset_ptr));
  memset_ptr = memset;
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
  Header(uint32_t size, bool from_mmap)
    : magic_code_(MAGIC_CODE),
      data_size_(size),
      offset_(0),
      from_mmap_(from_mmap)
  {}
  bool check_magic_code() const { return MAGIC_CODE == magic_code_; }
  void mark_unused() { magic_code_ &= ~0x1; }
  static Header *ptr2header(void *ptr) { return reinterpret_cast<Header*>((char*)ptr - SIZE); }
  uint32_t magic_code_;
  uint32_t data_size_;
  uint32_t offset_;
  uint8_t from_mmap_;
  uint8_t from_malloc_hook_;
  char padding_[2];
  char data_[0];
} __attribute__((aligned (16)));

const uint32_t Header::SIZE = offsetof(Header, data_);
void *ob_malloc_retry(size_t size, bool &from_malloc_hook)
{
  void *ptr = nullptr;
  do {
    ptr = global_malloc_hook.alloc(size, from_malloc_hook);
    if (OB_ISNULL(ptr)) {
      ob_usleep(10000);  // 10ms
    }
  } while (OB_ISNULL(ptr) && 0 != size);
  return ptr;
}

static inline void *ob_mmap(void *addr, size_t length, int prot, int flags, int fd, loff_t offset)
{
  void *ptr = (void*)syscall(SYS_mmap, addr, length, prot, flags, fd, offset);
  if (OB_UNLIKELY(!UNMAMAGED_MEMORY_STAT.is_disabled()) && OB_LIKELY(MAP_FAILED != ptr)) {
    const int64_t page_size = get_page_size();
    UNMAMAGED_MEMORY_STAT.inc(upper_align(length, page_size));
  }
  return ptr;
}

static inline int ob_munmap(void *addr, size_t length)
{
  if (OB_UNLIKELY(!ObUnmanagedMemoryStat::is_disabled())) {
    const int64_t page_size = get_page_size();
    UNMAMAGED_MEMORY_STAT.dec(upper_align(length, page_size));
  }
  return syscall(SYS_munmap, addr, length);
}

EXTERN_C_BEGIN

OBMALLOC_EXPORT
void OBMALLOC_NOTHROW *
OBMALLOC_ATTR(malloc) OBMALLOC_ALLOC_SIZE(1)
malloc(size_t size)
{
  void *ptr = nullptr;
  size_t real_size = size + Header::SIZE;
  void *tmp_ptr = nullptr;
  bool from_mmap = false;
  bool from_malloc_hook = false;
  if (OB_UNLIKELY(!g_malloc_hook_inited || in_hook())) {
    if (MAP_FAILED == (tmp_ptr = ob_mmap(nullptr, real_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
      tmp_ptr = nullptr;
    }
    from_mmap = true;
  } else {
    bool in_hook_bak = in_hook();
    in_hook()= true;
    tmp_ptr = ob_malloc_retry(real_size, from_malloc_hook);
    in_hook()= in_hook_bak;
  }
  if (OB_LIKELY(tmp_ptr != nullptr)) {
    Header *header = new (tmp_ptr) Header((uint32_t)size, from_mmap);
    ptr = header->data_;
    header->from_malloc_hook_ = from_malloc_hook;
  }
  return ptr;
}

OBMALLOC_EXPORT void OBMALLOC_NOTHROW
free(void *ptr)
{
  if (OB_LIKELY(ptr != nullptr)) {
    Header *header = Header::ptr2header(ptr);
    abort_unless(header->check_magic_code());
    header->mark_unused();
    void *orig_ptr = (char*)header - header->offset_;
    if (OB_LIKELY(header->from_malloc_hook_)) {
      global_malloc_hook.free(orig_ptr);
    } else if (header->from_mmap_) {
      ob_munmap(orig_ptr, header->data_size_ + Header::SIZE + header->offset_);
    } else {
      ob_free(orig_ptr);
    }
  }
}

OBMALLOC_EXPORT
void OBMALLOC_NOTHROW *
OBMALLOC_ALLOC_SIZE(2)
realloc(void *ptr, size_t size)
{
  if (0 == size && nullptr != ptr) {
    free(ptr);
    return nullptr;
  }
  void *nptr = nullptr;
  size_t real_size = size + Header::SIZE;
  void *tmp_ptr = nullptr;
  bool from_mmap = false;
  bool from_malloc_hook = false;
  if (OB_UNLIKELY(!g_malloc_hook_inited || in_hook())) {
    if (MAP_FAILED == (tmp_ptr = ob_mmap(nullptr, real_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
      tmp_ptr = nullptr;
    }
    from_mmap = true;
  } else {
    bool in_hook_bak = in_hook();
    in_hook()= true;
    DEFER(in_hook()= in_hook_bak);
    tmp_ptr = ob_malloc_retry(real_size, from_malloc_hook);
  }
  if (OB_LIKELY(tmp_ptr != nullptr)) {
    Header *header = new (tmp_ptr) Header((uint32_t)size, from_mmap);
    nptr = header->data_;
    header->from_malloc_hook_ = from_malloc_hook;
    if (ptr != nullptr) {
      Header *old_header = Header::ptr2header(ptr);
      abort_unless(old_header->check_magic_code());
      memmove(nptr, ptr, MIN(old_header->data_size_, size));
      free(old_header->data_);
    }
  }
  return nptr;
}

OBMALLOC_EXPORT
void OBMALLOC_NOTHROW *
OBMALLOC_ATTR(malloc)
memalign(size_t alignment, size_t size)
{
  void *ptr = nullptr;
  // avoid alignment overflow
  // Make sure alignment is power of 2
  {
    size_t a = 8;
    while (a < alignment)
      a <<= 1;
    alignment = a;
  }
  size_t real_size = alignment + Header::SIZE + size;
  void *tmp_ptr = nullptr;
  bool from_mmap = false;
  bool from_malloc_hook = false;
  if (OB_UNLIKELY(!g_malloc_hook_inited || in_hook())) {
    if (MAP_FAILED == (tmp_ptr = ob_mmap(nullptr, real_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
      tmp_ptr = nullptr;
    }
    from_mmap = true;
  } else {
    bool in_hook_bak = in_hook();
    in_hook()= true;
    DEFER(in_hook()= in_hook_bak);
    tmp_ptr = ob_malloc_retry(real_size, from_malloc_hook);
  }
  if (OB_LIKELY(tmp_ptr != nullptr)) {
    char *start = (char *)tmp_ptr + Header::SIZE;
    char *align_ptr = (char *)up_align(reinterpret_cast<int64_t>(start), alignment);
    char *pheader = align_ptr - Header::SIZE;
    size_t offset = pheader - (char*)tmp_ptr;
    Header *header = new (pheader) Header((uint32_t)size, from_mmap);
    header->offset_ = (uint32_t)offset;
    ptr = header->data_;
    header->from_malloc_hook_ = from_malloc_hook;
  }
  return ptr;
}

void *ob_mmap_hook(void *addr, size_t length, int prot, int flags, int fd, loff_t offset)
{
  return ob_mmap(addr, length, prot, flags, fd, offset);
}

int ob_munmap_hook(void *addr, size_t length)
{
  return ob_munmap(addr, length);
}

__attribute__((visibility("default"))) void *mmap(void *addr, size_t, int, int, int, loff_t) __attribute__((weak,alias("ob_mmap_hook")));
__attribute__((visibility("default"))) void *mmap64(void *addr, size_t, int, int, int, loff_t) __attribute__((weak,alias("ob_mmap_hook")));
__attribute__((visibility("default"))) int munmap(void *addr, size_t length) __attribute__((weak,alias("ob_munmap_hook")));

OBMALLOC_EXPORT size_t OBMALLOC_NOTHROW
malloc_usable_size(void *ptr)
{
  size_t ret = 0;
  if (OB_LIKELY(nullptr != ptr)) {
    Header *header = Header::ptr2header(ptr);
    abort_unless(header->check_magic_code());
    ret = header->data_size_;
  }
  return ret;
}

void *__libc_malloc(size_t size) LIBC_ALIAS(malloc);
void *__libc_realloc(void* ptr, size_t size) LIBC_ALIAS(realloc);
void __libc_free(void* ptr) LIBC_ALIAS(free);
void *__libc_memalign(size_t align, size_t s) LIBC_ALIAS(memalign);

int pthread_getattr_np(pthread_t thread, pthread_attr_t *attr)
{
  // pthread_getattr_np has lock and will allocate memory
  // add in_hook to avoid deadlock with backtrace get_stackattr
  bool in_hook_bak = in_hook();
  in_hook() = true;
  DEFER(in_hook() = in_hook_bak);
  static int (*real_pthread_getattr_np)(pthread_t thread, pthread_attr_t *attr) =
      (typeof(real_pthread_getattr_np))dlsym(RTLD_NEXT, "pthread_getattr_np");
  return real_pthread_getattr_np(thread, attr);
}

EXTERN_C_END
