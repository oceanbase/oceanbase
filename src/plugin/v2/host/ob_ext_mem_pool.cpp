/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include "plugin/v2/host/ob_ext_mem_pool.h"

#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace sql
{
namespace ext_plugin
{

// ============================================================================
// ObExtDefaultMemPool — ob_malloc_align(alignment, size, attr) for every alloc.
// Honors the requested alignment directly (arrow's 64, etc.). ob_malloc_align
// returns a non-NULL aligned block for size 0, so Malloc(0) satisfies arrow's
// placeholder requirement without a canary slot.
// ============================================================================

void *ObExtDefaultMemPool::Malloc(int64_t size, int64_t alignment)
{
  void *ret = nullptr;
  if (OB_UNLIKELY(size < 0)) {
    // ret stays nullptr
  } else {
    // alignment 0 => default 8 (a sane minimum; callers asking for "no special
    // alignment" get natural pointer alignment).
    const int64_t align = (alignment > 0) ? alignment : 8;
    ret = ob_malloc_align(align, size, mem_attr_);
    if (OB_ISNULL(ret)) {
      LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to allocate memory for ext plugin",
                   K(size), K(align), K(mem_attr_.tenant_id_));
    }
  }
  return ret;
}

void ObExtDefaultMemPool::Free(void *p, int64_t /*size*/)
{
  if (OB_NOT_NULL(p)) {
    ob_free_align(p);
  }
}

void *ObExtDefaultMemPool::Realloc(void *p, int64_t old_size, int64_t new_size,
                                   int64_t alignment)
{
  void *ret = nullptr;
  if (OB_ISNULL(p)) {
    ret = Malloc(new_size, alignment);
  } else if (new_size < 0) {
    Free(p, old_size);
  } else if (new_size == 0) {
    Free(p, old_size);
    ret = Malloc(0, alignment);  // non-NULL placeholder (arrow requires this)
  } else {
    void *n = Malloc(new_size, alignment);
    if (OB_NOT_NULL(n)) {
      MEMCPY(n, p, std::min(old_size, new_size));
      Free(p, old_size);
      ret = n;
    }
    // ret stays nullptr on failure; original `p` left intact
  }
  return ret;
}

// ============================================================================
// ObExtArrowMemPool — arrow-aware: Malloc(0) returns a static 64-aligned canary
// placeholder that Free never frees (arrow uses zero-size placeholders for
// identity + debug canary). size>0 honors the requested alignment via
// ob_malloc_align(alignment, size, attr).
// ============================================================================

namespace
{
constexpr int64_t kDebugXorSuffix = -0x181fe80e0b464188LL;
alignas(64) int64_t zero_size_area[1] = {kDebugXorSuffix};  // arrow's zero-size placeholder

inline bool is_zero_size_placeholder(const void *p) { return p == zero_size_area; }
} // namespace

void *ObExtArrowMemPool::Malloc(int64_t size, int64_t alignment)
{
  void *ret = nullptr;
  if (OB_UNLIKELY(size < 0)) {
    // ret stays nullptr
  } else if (size == 0) {
    // arrow requires a non-NULL, stable placeholder for Allocate(0); a static
    // 64-aligned canary slot (never freed) satisfies identity + debug canary.
    ret = zero_size_area;
  } else {
    const int64_t align = (alignment > 0) ? alignment : 8;
    ret = ob_malloc_align(align, size, mem_attr_);
    if (OB_ISNULL(ret)) {
      LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to allocate memory for ext plugin (arrow)",
                   K(size), K(align), K(mem_attr_.tenant_id_));
    }
  }
  return ret;
}

void ObExtArrowMemPool::Free(void *p, int64_t /*size*/)
{
  if (OB_ISNULL(p) || is_zero_size_placeholder(p)) {
    // never free the zero-size placeholder
  } else {
    ob_free_align(p);
  }
}

void *ObExtArrowMemPool::Realloc(void *p, int64_t old_size, int64_t new_size,
                                 int64_t alignment)
{
  void *ret = nullptr;
  // realloc of nullptr or the zero-size placeholder == allocate new_size (arrow
  // treats the placeholder as a 0-byte allocation).
  if (OB_ISNULL(p) || is_zero_size_placeholder(p)) {
    ret = Malloc(new_size, alignment);  // new_size==0 -> placeholder, <0 -> nullptr
  } else if (new_size < 0) {
    Free(p, old_size);
  } else if (new_size == 0) {
    Free(p, old_size);
    ret = zero_size_area;  // arrow's zero-size placeholder
  } else {
    void *n = Malloc(new_size, alignment);
    if (OB_NOT_NULL(n)) {
      MEMCPY(n, p, std::min(old_size, new_size));
      Free(p, old_size);
      ret = n;
    }
    // ret stays nullptr on failure; original `p` left intact
  }
  return ret;
}

} // namespace ext_plugin
} // namespace sql
} // namespace oceanbase

#undef USING_LOG_PREFIX
