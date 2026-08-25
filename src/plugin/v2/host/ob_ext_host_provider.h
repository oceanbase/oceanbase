/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_ext_host_provider.h
/// \brief Build the contract's `ObExtTableHostApi` (memory / executor / read-only
/// file system) from OB context, for the generic external-table plugin path.
///
/// Self-contained: defines its own `ObExtHostCtx` and callback implementations
/// (wrapping `ObExternalFileAccess` / `ObExternalFileInfoCollector` / OB allocators)
/// and does NOT reference a format-specific host API.
/// The callbacks are process-static; only `ctx` varies per scan. The caller keeps
/// `ctx` alive for the lifetime of every plugin object built through `out`.

#ifndef OB_EXT_HOST_PROVIDER_H
#define OB_EXT_HOST_PROVIDER_H

#include "plugin/v2/include/ob_external_table_plugin.h"  // ObExtTableHostApi

#include "plugin/v2/host/ob_ext_mem_pool.h"     // ObExtMemPool / ObExtDefaultMemPool
#include "plugin/v2/host/ob_ext_file_system.h"  // ObExtFileSystem / ObExtDefaultFileSystem
#include "sql/engine/table/ob_external_file_access.h"        // ObExternalFileCacheOptions

#include <cstdint>
#include <string>

namespace oceanbase
{
namespace sql
{
namespace lake_table
{
class ObLakeTableExecutor;
}

namespace ext_plugin
{

/// OB-owned bundle of the three capabilities a generic external-table plugin
/// needs from OB for one scan / schema-load: a memory pool, a read-only file
/// system, and an optional task executor. Passed to the plugin as the opaque
/// `void* ctx` of `ObExtTableHostApi`; every callback table receives that one
/// context. Must outlive every plugin object built from the host table.
///
/// Each capability is an interface object held by value (with a base pointer
/// defaulting to it), so a format that wants different semantics swaps the
/// pointer for its own subclass — and ctx itself stays a thin, readable bundle.
struct ObExtHostCtx
{
  ObExtHostCtx() = default;
  // ---- memory ----
  // Two pool flavors sharing one alignment-aware interface (the plugin passes
  // arrow's 64-byte alignment through `mem_alloc`'s alignment arg, so neither
  // needs a manual over-alignment prefix): the plain default, and an arrow-
  // aware pool whose Malloc(0) returns a static 64-aligned canary placeholder
  // (arrow stamps a debug canary on zero-size buffers and may compare pointer
  // identity — a real 0-size ob_malloc block can't serve that). `pool` points
  // at the selected one; pick via select_arrow_pool().
  ObExtDefaultMemPool default_pool;
  ObExtArrowMemPool arrow_pool;
  ObExtMemPool *pool = &default_pool;
  // ---- file system (read-only) ----
  ObExtDefaultFileSystem default_fs;
  ObExtFileSystem *fs = &default_fs;
  // ---- executor (nullptr => run tasks inline on the calling thread) ----
  lake_table::ObLakeTableExecutor *executor = nullptr;
  /// Select the memory pool: arrow-backed formats that require Arrow's zero-size
  /// placeholder canary take the arrow
  /// pool; everything else takes the plain default. Idempotent. Call before
  /// setting the attr so the attr lands on the chosen pool (`pool->set_attr`).
  void select_arrow_pool(bool use_arrow)
  {
    pool = use_arrow ? static_cast<ObExtMemPool *>(&arrow_pool)
                     : static_cast<ObExtMemPool *>(&default_pool);
  }
};

/// Fill `out` (the top-level host container with three embedded function
/// tables) and set `out.ctx = ctx`. The function
/// pointers are process-static; only `ctx` varies per scan. The caller keeps
/// `ctx` alive for the lifetime of all plugin objects built through `out`.
void build_ext_host_api(ObExtTableHostApi &out, ObExtHostCtx *ctx);

} // namespace ext_plugin
} // namespace sql
} // namespace oceanbase

#endif // OB_EXT_HOST_PROVIDER_H
