/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// USING_LOG_PREFIX must be defined before including headers that expand
// LOG_INFO/LOG_WARN inline.
#define USING_LOG_PREFIX SQL

#include "plugin/v2/host/ob_ext_host_provider.h"

#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/table_format/common/utils/ob_lake_table_executor.h"

#include <functional>

namespace oceanbase
{
namespace sql
{
namespace ext_plugin
{

namespace
{

// ---------------------------------------------------------------------------
// Host callbacks. `ctx` is always an ObExtHostCtx*. Each is a thin trampoline
// into one of ctx's interface objects (pool / fs / executor); the real logic
// lives in ob_ext_mem_pool.* / ob_ext_file_system.* / the executor. Errors are
// reported via int return codes (no plugin types here).
// ---------------------------------------------------------------------------

// ---- memory ----
void *host_mem_alloc(void *ctx, int64_t size, int64_t alignment)
{
  const auto *c = static_cast<const ObExtHostCtx *>(ctx);
  return (OB_NOT_NULL(c) && OB_NOT_NULL(c->pool)) ? c->pool->Malloc(size, alignment) : nullptr;
}

void host_mem_free(void *ctx, void *ptr, int64_t size)
{
  const auto *c = static_cast<const ObExtHostCtx *>(ctx);
  if (OB_NOT_NULL(c) && OB_NOT_NULL(c->pool)) {
    c->pool->Free(ptr, size);
  }
}

void *host_mem_realloc(void *ctx, void *ptr, int64_t old_size, int64_t new_size,
                       int64_t alignment)
{
  const auto *c = static_cast<const ObExtHostCtx *>(ctx);
  return (OB_NOT_NULL(c) && OB_NOT_NULL(c->pool))
             ? c->pool->Realloc(ptr, old_size, new_size, alignment)
             : nullptr;
}

int64_t host_mem_bytes_allocated(void *ctx)
{
  const auto *c = static_cast<const ObExtHostCtx *>(ctx);
  return (OB_NOT_NULL(c) && OB_NOT_NULL(c->pool)) ? c->pool->bytes_allocated() : -1;
}

// ---- executor ----

int32_t host_exec_submit(void *ctx, void (*fn)(void *), void *arg)
{
  int32_t rc = OB_SUCCESS;
  if (OB_ISNULL(fn)) {
    rc = OB_INVALID_ARGUMENT;
  } else {
    const auto *c = static_cast<const ObExtHostCtx *>(ctx);
    if (OB_ISNULL(c) || OB_ISNULL(c->executor)) {
      // No backing executor: run inline on the calling thread.
      fn(arg);
    } else {
      // ObLakeTableExecutor::Add is fire-and-forget; the contract requires an
      // accepted task to run, so acceptance is reported as success here. If a
      // future executor can reject tasks, surface that errno instead.
      c->executor->Add(std::bind(fn, arg));
    }
  }
  return rc;
}

int32_t host_exec_thread_count(void * /*ctx*/)
{
  return 1;
}

// ---- file system (read-only) ----

void *host_file_open(void *ctx, const char *path)
{
  const auto *c = static_cast<const ObExtHostCtx *>(ctx);
  return (OB_NOT_NULL(c) && OB_NOT_NULL(c->fs)) ? c->fs->open(path) : nullptr;
}

int64_t host_file_read(void *ctx, void *stream, char *buf, int64_t size)
{
  const auto *c = static_cast<const ObExtHostCtx *>(ctx);
  return (OB_NOT_NULL(c) && OB_NOT_NULL(c->fs)) ? c->fs->read(stream, buf, size) : -1;
}

int64_t host_file_read_at(void *ctx, void *stream, char *buf, int64_t size, int64_t offset)
{
  const auto *c = static_cast<const ObExtHostCtx *>(ctx);
  return (OB_NOT_NULL(c) && OB_NOT_NULL(c->fs))
             ? c->fs->read_at(stream, buf, size, offset)
             : -1;
}

int64_t host_file_tell(void *ctx, void *stream)
{
  const auto *c = static_cast<const ObExtHostCtx *>(ctx);
  return (OB_NOT_NULL(c) && OB_NOT_NULL(c->fs)) ? c->fs->tell(stream) : -1;
}

int32_t host_file_seek(void *ctx, void *stream, int64_t offset, int32_t origin)
{
  const auto *c = static_cast<const ObExtHostCtx *>(ctx);
  return (OB_NOT_NULL(c) && OB_NOT_NULL(c->fs)) ? c->fs->seek(stream, offset, origin) : -1;
}

int64_t host_file_length(void *ctx, void *stream)
{
  const auto *c = static_cast<const ObExtHostCtx *>(ctx);
  return (OB_NOT_NULL(c) && OB_NOT_NULL(c->fs)) ? c->fs->length(stream) : -1;
}

void host_file_close(void *ctx, void *stream)
{
  const auto *c = static_cast<const ObExtHostCtx *>(ctx);
  if (OB_NOT_NULL(c) && OB_NOT_NULL(c->fs)) {
    c->fs->close(stream);
  }
}

int32_t host_file_exists(void *ctx, const char *path)
{
  const auto *c = static_cast<const ObExtHostCtx *>(ctx);
  return (OB_NOT_NULL(c) && OB_NOT_NULL(c->fs)) ? c->fs->exists(path) : -1;
}

int32_t host_file_status(void *ctx, const char *path, int64_t *size, int64_t *mtime_ms,
                         int32_t *is_dir)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_ISNULL(size) || OB_ISNULL(mtime_ms) || OB_ISNULL(is_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ext file_status invalid argument", K(ret), KP(ctx), KP(path), KP(size),
             KP(mtime_ms), KP(is_dir));
  } else {
    const ObExtHostCtx *c = static_cast<const ObExtHostCtx *>(ctx);
    if (OB_ISNULL(c) || OB_ISNULL(c->fs)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ext file_status invalid host context", K(ret), KP(c));
    } else {
      ret = c->fs->file_status(path, *size, *mtime_ms, *is_dir);
    }
  }
  return ret;
}

// Bridges the plugin's list_dir host slot to ObExtFileSystem::list_dir. The
// plugin streams (name, is_dir) pairs back through `cb`; OB's fs does two
// passes (files then dirs) under one storage util open. See ob_ext_file_system.cpp.
int32_t host_list_dir(void *ctx, const char *dir,
                      void (*cb)(void *cb_ctx, const char *name, int32_t is_dir),
                      void *cb_ctx)
{
  const auto *c = static_cast<const ObExtHostCtx *>(ctx);
  return (OB_NOT_NULL(c) && OB_NOT_NULL(c->fs))
             ? c->fs->list_dir(dir, cb, cb_ctx)
             : OB_INVALID_ARGUMENT;
}

// ---- logging ----
// Routes a plugin's pre-formatted message into OB's logger under module
// "[ExtPlugin]". The plugin passes its own __FILE__/__LINE__/__func__ for
// traceability; all of msg/file/func may be NULL. Level filtering honors the
// current log level so trace/info messages are dropped when not enabled.
void host_log(void * /*ctx*/, int32_t level, const char *file, int32_t line,
              const char *func, const char *msg)
{
  int32_t ob_level = OB_LOG_LEVEL_WARN;
  switch (level) {
    case OB_EXT_LOG_TRACE: ob_level = OB_LOG_LEVEL_TRACE; break;
    case OB_EXT_LOG_INFO:  ob_level = OB_LOG_LEVEL_INFO;  break;
    case OB_EXT_LOG_WARN:  ob_level = OB_LOG_LEVEL_WARN;  break;
    case OB_EXT_LOG_ERROR: ob_level = OB_LOG_LEVEL_ERROR; break;
    default:               ob_level = OB_LOG_LEVEL_WARN;  break;
  }
  if (OB_LOGGER.need_to_print(ob_level)) {
    OB_LOGGER.log_message_fmt("ExtPlugin",
                              ob_level,
                              OB_NOT_NULL(file) ? file : "",
                              line,
                              OB_NOT_NULL(func) ? func : "",
                              0,  // location_hash_val (unused)
                              0,  // errcode
                              "%s",
                              OB_NOT_NULL(msg) ? msg : "");
  }
}

} // namespace

void build_ext_host_api(ObExtTableHostApi &out, ObExtHostCtx *ctx)
{
  out.ctx = ctx;
  out.log = host_log;
  // The callback tables live directly in the host API and all use out.ctx.
  out.mem.mem_alloc = host_mem_alloc;
  out.mem.mem_free = host_mem_free;
  out.mem.mem_realloc = host_mem_realloc;
  out.mem.mem_bytes_allocated = host_mem_bytes_allocated;
  out.executor.exec_submit = host_exec_submit;
  out.executor.exec_thread_count = host_exec_thread_count;
  out.io.file_open = host_file_open;
  out.io.file_read = host_file_read;
  out.io.file_read_at = host_file_read_at;
  out.io.file_tell = host_file_tell;
  out.io.file_seek = host_file_seek;
  out.io.file_length = host_file_length;
  out.io.file_close = host_file_close;
  out.io.file_exists = host_file_exists;
  out.io.file_status = host_file_status;
  out.io.list_dir = host_list_dir;
}

} // namespace ext_plugin
} // namespace sql
} // namespace oceanbase

#undef USING_LOG_PREFIX
