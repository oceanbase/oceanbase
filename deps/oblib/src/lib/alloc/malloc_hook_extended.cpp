/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <cstdlib>
#include <new>
#include <cerrno>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <gnu/libc-version.h>

#define LIKELY(x) __builtin_expect(!!(x),!!1)
#define UNLIKELY(x) __builtin_expect(!!(x),!!0)
#define MALLOC_ATTR(s) __attribute__((s))
#define MALLOC_EXPORT __attribute__((visibility("default")))
#define MALLOC_ALLOC_SIZE(s) __attribute__((alloc_size(s)))
#define MALLOC_ALLOC_SIZE2(s1, s2) __attribute__((alloc_size(s1, s2)))
#define MALLOC_NOTHROW __attribute__((nothrow))
#define powerof2(x) ((((x) - 1) & (x)) == 0)
#define LIBC_ALIAS(fn)	__attribute__((alias (#fn), used))

typedef void* (*MemsetPtr)(void*, int, size_t);
extern MemsetPtr memset_ptr;

static size_t get_page_size()
{
  static size_t ps = getpagesize();
  return ps;
}

void get_glibc_version(int &major, int &minor)
{
  major = 0;
  minor = 0;
  const char *glibc_version = gnu_get_libc_version();
  if (NULL != glibc_version) {
    sscanf(glibc_version, "%d.%d", &major, &minor);
  }
}

bool glibc_prereq(int major, int minor)
{
  int cur_major = 0;
  int cur_minor = 0;
  get_glibc_version(cur_major, cur_minor);
  return (cur_major > major) || (cur_major == major && cur_minor >= minor);
}

extern "C" {

extern void *malloc(size_t size);
extern void free(void *ptr);
extern void *realloc(void *ptr, size_t size);
extern void *memalign(size_t alignment, size_t size);

MALLOC_EXPORT
void MALLOC_NOTHROW *
MALLOC_ATTR(malloc) MALLOC_ALLOC_SIZE2(1, 2)
calloc(size_t nmemb, size_t size)
{
  size_t real_size;
  if (UNLIKELY(__builtin_mul_overflow(nmemb, size, &real_size))) {
    abort();
  }
  void *ptr = malloc(real_size);
  if (LIKELY(nullptr != ptr && real_size > 0)) {
    if (nullptr != memset_ptr) {
      memset_ptr(ptr, 0, real_size);
    } else {
      volatile char *tmp_ptr = (char *)ptr;
      for (size_t i = 0; i < real_size; ++i) {
        tmp_ptr[i] = 0;
      }
    }
  }
  return ptr;
}

MALLOC_EXPORT void MALLOC_NOTHROW
cfree(void* ptr)
{
  free(ptr);
}

MALLOC_EXPORT void MALLOC_NOTHROW
free_sized(void* ptr, size_t size)
{
  free(ptr);
}

MALLOC_EXPORT void MALLOC_NOTHROW
free_aligned_sized(void* ptr, size_t alignment, size_t size)
{
  free(ptr);
}

MALLOC_EXPORT
void MALLOC_NOTHROW *
MALLOC_ATTR(malloc) MALLOC_ALLOC_SIZE(2)
aligned_alloc(size_t alignment, size_t size)
{
  // glibc-2.38 and above adopt ISO 17 standard aligned_alloc
  static const bool newstd = glibc_prereq(2, 38);
  if (newstd) {
    if (!powerof2 (alignment) || alignment == 0) {
      errno = EINVAL;
      return nullptr;
    }
  }
  return memalign(alignment, size);
}

MALLOC_EXPORT int MALLOC_NOTHROW
MALLOC_ATTR(nonnull(1))
posix_memalign(void** memptr, size_t alignment, size_t size)
{
  int err = 0;
  if (0 != alignment % sizeof (void *)
      || 0 != !powerof2 (alignment / sizeof (void *))
      || 0 == alignment
      || nullptr == memptr) {
    return EINVAL;
  }

  *memptr = nullptr;
  void *ptr = memalign(alignment, size);
  if (nullptr == ptr) {
    err = ENOMEM;
  } else {
    *memptr = ptr;
  }
  return err;
}

MALLOC_EXPORT
void MALLOC_NOTHROW *
MALLOC_ATTR(malloc)
valloc(size_t size)
{
  return memalign(get_page_size(), size);
}

MALLOC_EXPORT
void MALLOC_NOTHROW *
MALLOC_ATTR(malloc)
pvalloc(size_t size)
{
  const size_t pagesize = get_page_size();
  size_t page_mask = pagesize - 1;
  size_t rounded_bytes;
  if (UNLIKELY(__builtin_add_overflow(size, page_mask, &rounded_bytes))) {
    abort();
  }
  rounded_bytes = rounded_bytes & ~(page_mask);
  return memalign(pagesize, rounded_bytes);
}

void *__libc_calloc(size_t n, size_t size) LIBC_ALIAS(calloc);
void __libc_free_sized(void* ptr, size_t size) LIBC_ALIAS(free_sized);
void __libc_free_aligned_sized(void* ptr, size_t alignment, size_t size) LIBC_ALIAS(free_aligned_sized);
void *__libc_valloc(size_t size) LIBC_ALIAS(valloc);
void *__libc_pvalloc(size_t size) LIBC_ALIAS(pvalloc);
int __posix_memalign(void** r, size_t a, size_t s) LIBC_ALIAS(posix_memalign);

} // extern "C" end

void *operator new(std::size_t size)
{
  void *ptr = malloc(size);
  if (UNLIKELY(nullptr == ptr)) {
    throw std::bad_alloc();
  }
  return ptr;
}

void *operator new[](std::size_t size)
{
  void *ptr = malloc(size);
  if (UNLIKELY(nullptr == ptr)) {
    throw std::bad_alloc();
  }
  return ptr;
}

void *operator new(std::size_t size, const std::nothrow_t &) noexcept {
	return malloc(size);
}

void *operator new[](std::size_t size, const std::nothrow_t &) noexcept {
	return malloc(size);
}

void operator delete(void *ptr) noexcept
{
  free(ptr);
}

void operator delete[](void *ptr) noexcept
{
  free(ptr);
}

void operator delete(void *ptr, const std::nothrow_t &) noexcept
{
  free(ptr);
}

void operator delete[](void *ptr, const std::nothrow_t &) noexcept
{
  free(ptr);
}

#if __cpp_sized_deallocation >= 201309
// C++14 sized-delete operators
void operator delete(void *ptr, std::size_t size) noexcept
{
  free(ptr);
}

void operator delete[](void *ptr, std::size_t size) noexcept
{
  free(ptr);
}
#endif

#if __cpp_aligned_new >= 201606
// C++17 aligned operators
void *operator new(std::size_t size, std::align_val_t alignment) {
	void *ptr = memalign(static_cast<std::size_t>(alignment), size);
  if (UNLIKELY(nullptr == ptr)) {
    throw std::bad_alloc();
  }
  return ptr;
}

void *operator new[](std::size_t size, std::align_val_t alignment) {
	void *ptr = memalign(static_cast<std::size_t>(alignment), size);
  if (UNLIKELY(nullptr == ptr)) {
    throw std::bad_alloc();
  }
  return ptr;
}

void *operator new(std::size_t size, std::align_val_t alignment, const std::nothrow_t &) noexcept {
	return memalign(static_cast<std::size_t>(alignment), size);
}

void *operator new[](std::size_t size, std::align_val_t alignment, const std::nothrow_t &) noexcept {
	return memalign(static_cast<std::size_t>(alignment), size);
}

void operator delete(void* ptr, std::align_val_t) noexcept
{
  free(ptr);
}

void operator delete(void* ptr, std::align_val_t, const std::nothrow_t &) noexcept
{
  free(ptr);
}

void operator delete(void* ptr, std::size_t size, std::align_val_t al) noexcept
{
  free(ptr);
}

void operator delete[](void* ptr, std::align_val_t) noexcept
{
  free(ptr);
}

void operator delete[](void* ptr, std::align_val_t, const std::nothrow_t &) noexcept
{
  free(ptr);
}

void operator delete[](void* ptr, std::size_t size, std::align_val_t al) noexcept
{
  free(ptr);
}
#endif
