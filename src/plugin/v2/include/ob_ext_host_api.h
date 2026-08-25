/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_ext_host_api.h
/// \brief Host API: the capabilities OB provides TO every plugin (memory /
/// executor / io / log), split out of ob_external_table_plugin.h (the OTHER
/// direction — the vtable the plugin implements FOR OB).
///
/// Pure C, int error codes, NO plugin/format types: this surface is generic
/// enough to serve any v2 plugin type. The plugin wraps these callbacks into
/// its own internal adapters. Included by ob_external_table_plugin.h; plugins
/// normally do not include it directly.

#ifndef OB_EXT_HOST_API_H
#define OB_EXT_HOST_API_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// =============================================================================
// Host API: OB capabilities exposed to the plugin, split into three function
// tables — memory / executor / io — under one top-level `ObExtTableHostApi`.
// Every callback receives the top-level `host->ctx`; the function tables carry
// no duplicate context of their own. Pure C, int error codes, NO plugin/format
// types. The plugin wraps these into its own internal adapters.
//
// Conventions: file_read/file_read_at return bytes (>0), 0 on EOF, -1 on error;
// file_seek returns 0/-1 (origin is one of OB_EXT_SEEK_*); file_open returns a
// stream handle or NULL; file_exists returns 1/0/-1; file_status returns 0 on
// success and fills all output values, or an OB errno on error.
// =============================================================================

typedef enum {
  OB_EXT_SEEK_SET = 0,
  OB_EXT_SEEK_CUR = 1,
  OB_EXT_SEEK_END = 2,
} ob_ext_seek_origin;

// Plugin-side log severity, mapped by the host into OB's log levels. The plugin
// formats the message itself (snprintf) and passes the finished `msg`; the host
// decides whether to print (level filtering) and writes it via OB's logger.
typedef enum {
  OB_EXT_LOG_TRACE = 0,
  OB_EXT_LOG_INFO  = 1,
  OB_EXT_LOG_WARN  = 2,
  OB_EXT_LOG_ERROR = 3,
} ob_ext_log_level;

// ---- memory: the ONLY allocator whose memory may cross the boundary ----
// `alignment` is the requested byte alignment (0 => a sane default the host
// chooses, e.g. 8). Hosts MUST honor a non-zero alignment (e.g. arrow's 64) —
// Arrow-backed plugins pass their buffer alignment through here and will DCHECK on a
// misaligned pointer. `mem_alloc(0)` MUST return a non-NULL placeholder (arrow
// requires this); `ob_malloc_align(align, 0, ...)` already does. `mem_free`
// takes no alignment (the host frees by pointer). `mem_free`'s `size` is
// informational only — it is whatever size the freeing side recorded (an
// output-JSON buffer's destroy passes payload_len + 1 for the NUL);
// implementations MUST free by pointer and MUST NOT depend on `size`.
typedef struct ObExtMemApi {
  void* (*mem_alloc)(void* ctx, int64_t size, int64_t alignment);
  void  (*mem_free)(void* ctx, void* ptr, int64_t size);
  void* (*mem_realloc)(void* ctx, void* ptr, int64_t old_size, int64_t new_size,
                       int64_t alignment);
  int64_t (*mem_bytes_allocated)(void* ctx);
} ObExtMemApi;

// ---- executor ----
// exec_submit returns an OB errno (OB_EXT_*): 0 = the task was accepted (the
// host GUARANTEES an accepted task will run — even during shutdown it must be
// drained, because the plugin reclaims its per-task resources only when the
// task runs) or was already executed inline; non-zero = the host rejected the
// task WITHOUT taking ownership — the plugin must reclaim the task's resources
// itself (it may fall back to running the task inline).
typedef struct ObExtExecutorApi {
  int32_t (*exec_submit)(void* ctx, void (*fn)(void*), void* arg);
  int32_t (*exec_thread_count)(void* ctx);
} ObExtExecutorApi;

// ---- io: read-only file system + directory listing ----
typedef struct ObExtIoApi {
  void* (*file_open)(void* ctx, const char* path);
  int64_t (*file_read)(void* ctx, void* stream, char* buf, int64_t size);
  int64_t (*file_read_at)(void* ctx, void* stream, char* buf, int64_t size, int64_t offset);
  int64_t (*file_tell)(void* ctx, void* stream);
  int32_t (*file_seek)(void* ctx, void* stream, int64_t offset, int32_t origin);
  int64_t (*file_length)(void* ctx, void* stream);
  void  (*file_close)(void* ctx, void* stream);
  int32_t (*file_exists)(void* ctx, const char* path);
  // Composite path-level metadata callback used by file-system adapters.
  int32_t (*file_status)(void* ctx, const char* path, int64_t* size,
                         int64_t* mtime_ms, int32_t* is_dir);
  // Iterates child entries under `dir` (non-recursive). For each entry the host
  // invokes `cb(user_data, name, is_dir)`: `name` is the NUL-terminated basename
  // (valid only during the callback); `is_dir` is 1 for a directory, 0 for a
  // file. `user_data` is the caller's opaque pointer, passed through untouched
  // (distinct from the host callback `ctx`). Returns 0 on success — including an
  // empty / non-existent directory (0 with zero callbacks) — non-zero OB errno
  // on a real storage/host error. Old plugins compiled before this slot existed
  // leave it NULL; the plugin null-checks before use and degrades.
  int32_t (*list_dir)(void* ctx, const char* dir,
                      void (*cb)(void* user_data, const char* name, int32_t is_dir),
                      void* user_data);
} ObExtIoApi;

// ---- top-level container ----
// `ctx` is the single OB-owned opaque bundle (ObExtHostCtx*) passed to every
// mem / executor / io / log callback.
typedef struct ObExtTableHostApi {
  void* ctx;
  ObExtMemApi mem;
  ObExtExecutorApi executor;
  ObExtIoApi io;
  // Routes a pre-formatted plugin message into OB's logger. `file`/`line`/`func`
  // are the plugin's own call site (pass __FILE__/__LINE__/__func__ for full
  // traceability; any may be NULL). `msg` is a NUL-terminated UTF-8 string (NULL
  // treated as empty). OB always fills this slot; new plugins SHOULD use it
  // instead of stdout/stderr so their diagnostics land in observer.log.
  void (*log)(void* ctx, int32_t level, const char* file, int32_t line,
              const char* func, const char* msg);
} ObExtTableHostApi;

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // OB_EXT_HOST_API_H
