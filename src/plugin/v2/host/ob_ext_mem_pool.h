/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_EXT_MEM_POOL_H
#define OB_EXT_MEM_POOL_H

#include "lib/allocator/ob_malloc.h"  // ob_malloc_align / ob_free_align / ObMemAttr
#include "lib/ob_define.h"            // OB_* / K / LOG macros

#include <algorithm>

namespace oceanbase
{
namespace sql
{
namespace ext_plugin
{

/// OB-internal memory pool backing the generic external-table host callbacks.
///
/// Standalone: it does NOT inherit any plugin-SDK C++ type. The plugin is a separately-compiled .so driven by a
/// pure-C vtable (`ObExtTableHostApi`); it cannot share a C++ vtable / RTTI /
/// STL with OB, so there is no shared `MemoryPool` base. Instead the plugin calls
/// the C `host->mem.mem_alloc(ctx, size, alignment)` trampoline, which delegates
/// here.
///
/// `ObExtHostCtx` owns one of these (by value) and exposes it through a base
/// pointer. No usage accounting is kept here: OB's malloc hook already charges
/// every `ob_malloc_align` to the tenant under the pool's label, so
/// `bytes_allocated()` returns -1 (not tracked).
class ObExtMemPool
{
public:
  ObExtMemPool() = default;
  virtual ~ObExtMemPool() = default;

  /// Allocate `size` bytes at `alignment` (0 => default 8). MUST return non-NULL
  /// for size==0 (arrow requires it; `ob_malloc_align(align, 0, ...)` does).
  virtual void *Malloc(int64_t size, int64_t alignment) = 0;
  /// Free an allocation previously returned by Malloc/Realloc. `size` is the
  /// size the plugin recorded (ignored by the plain pool; frees by pointer).
  virtual void Free(void *p, int64_t size) = 0;
  /// Resize `p` (from old_size to new_size) at `alignment`. Realloc of nullptr
  /// == Malloc.
  virtual void *Realloc(void *p, int64_t old_size, int64_t new_size,
                        int64_t alignment) = 0;

  /// Bytes currently outstanding through this pool. -1 => not tracked (OB's
  /// malloc hook already accounts to the tenant under the pool's label).
  virtual int64_t bytes_allocated() const { return -1; }

  void set_attr(const lib::ObMemAttr &attr) { mem_attr_ = attr; }
  const lib::ObMemAttr &get_attr() const { return mem_attr_; }

protected:
  lib::ObMemAttr mem_attr_;
};

/// Default pool: `ob_malloc_align(alignment, size, attr)` for every allocation.
/// Honors the requested alignment (arrow's 64, etc.) directly — no manual
/// over-alignment prefix needed in the plugin. `ob_malloc_align(align, 0, attr)`
/// returns a NON-NULL aligned block, so Malloc(0) returns non-NULL.
class ObExtDefaultMemPool : public ObExtMemPool
{
public:
  ObExtDefaultMemPool() = default;
  explicit ObExtDefaultMemPool(const lib::ObMemAttr &attr) { mem_attr_ = attr; }

  void *Malloc(int64_t size, int64_t alignment) override;
  void Free(void *p, int64_t size) override;
  void *Realloc(void *p, int64_t old_size, int64_t new_size, int64_t alignment) override;
};

/// Arrow-aware pool. arrow's memory pool has two requirements beyond a plain
/// malloc: (1) Allocate(0) returns a NON-NULL placeholder (arrow uses it for
/// identity / zero-size buffers and stamps a debug canary on it — a real
/// 0-size ob_malloc block can corrupt if arrow writes the canary past the end);
/// (2) the placeholder must be stable (arrow may compare pointers). This pool
/// returns a static 64-aligned canary slot for Malloc(0) (never freed), and
/// `ob_malloc_align(alignment, size, attr)` for size>0 (honoring the requested
/// alignment, e.g. arrow's 64). Use it for arrow-backed formats;
/// `ObExtHostCtx` selects it via `select_arrow_pool(true)`.
class ObExtArrowMemPool : public ObExtMemPool
{
public:
  ObExtArrowMemPool() = default;
  explicit ObExtArrowMemPool(const lib::ObMemAttr &attr) { mem_attr_ = attr; }

  void *Malloc(int64_t size, int64_t alignment) override;
  void Free(void *p, int64_t size) override;
  void *Realloc(void *p, int64_t old_size, int64_t new_size, int64_t alignment) override;
};

} // namespace ext_plugin
} // namespace sql
} // namespace oceanbase

#endif // OB_EXT_MEM_POOL_H
