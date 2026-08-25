/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_EXT_FILE_SYSTEM_H
#define OB_EXT_FILE_SYSTEM_H

#include "sql/engine/table/ob_external_file_access.h"  // ObExternalFileCacheOptions
#include "lib/allocator/page_arena.h"                  // ObArenaAllocator
#include "lib/hash/ob_hashmap.h"

#include <cstdint>

namespace oceanbase
{
namespace sql
{
namespace ext_plugin
{

/// Read-only external file system exposed to a generic external-table plugin.
///
/// Mirrors `ObExtMemPool` / `ObLakeTableExecutor`: an interface object held by
/// `ObExtHostCtx` so ctx stays a thin bundle of a few capabilities (pool / fs /
/// executor) instead of a bag of loose path/access/tenant fields. The C host
/// callbacks (`host_file_*`) are thin trampolines that delegate to
/// `ctx->fs->...`.
///
/// The base carries the scan's location config (base path, access info, cache
/// options, tenant) shared by all implementations; subclasses provide the
/// actual open/read/.../stat operations. Streams are opaque `void*` handles the
/// implementation owns (created by `open`, destroyed by `close`), so a subclass
/// is free to use whatever internal stream type it wants.
///
/// String ownership convention: paths arrive over the C boundary as
/// `const char*` and all *persistent* copies (path_/access_info_ and, in the
/// default fs, resolved paths and cache keys) live in the fs's own arena and
/// are NUL-terminated, so they may be consumed both as ObString and as C
/// strings. Everything else stays a non-owning view valid for the duration of
/// one call.
class ObExtFileSystem
{
public:
  ObExtFileSystem() : arena_("ExtFs") {}
  virtual ~ObExtFileSystem() = default;

  // ---- location config (set by OB before the plugin runs) ----
  // Deep-copy into the fs arena (NUL-terminated). `set_path` additionally
  // normalizes a malformed `file:/...` prefix to `file://...`.
  int set_path(const common::ObString &p);
  int set_access_info(const common::ObString &a);
  void set_cache_options(const ObExternalFileCacheOptions &c) { cache_options_ = c; }
  void set_tenant_id(uint64_t t) { tenant_id_ = t; }
  // Returned views live in the fs arena and are NUL-terminated.
  const common::ObString &path() const { return path_; }
  const common::ObString &access_info() const { return access_info_; }
  uint64_t tenant_id() const { return tenant_id_; }

  // ---- stream operations (handle from open) ----
  // Returns a stream handle or NULL. read/read_at: >0 bytes, 0 EOF, -1 error.
  // seek: 0 ok / -1 error (origin is OB_EXT_SEEK_*). length/file_status:
  // -1 on error. exists: 1/0/-1.
  virtual void *open(const char *path) = 0;
  virtual int64_t read(void *stream, char *buf, int64_t size) = 0;
  virtual int64_t read_at(void *stream, char *buf, int64_t size, int64_t offset) = 0;
  virtual int64_t tell(void *stream) = 0;
  virtual int32_t seek(void *stream, int64_t offset, int32_t origin) = 0;
  virtual int64_t length(void *stream) = 0;
  virtual void close(void *stream) = 0;

  // ---- path-level metadata (no stream) ----
  virtual int32_t exists(const char *path) = 0;
  // One-shot path metadata: fills size (bytes), modification time (ms), and
  // directory flag through one composite host-side metadata operation.
  virtual int file_status(const char *path, int64_t &size, int64_t &mtime_ms,
                          int32_t &is_dir) = 0;

  // ---- directory listing (non-recursive) ----
  // Iterates child entries under `dir`. For each entry `cb(cb_ctx, name, is_dir)`
  // is invoked with the basename and 1=dir/0=file. Returns OB_SUCCESS on success
  // (including an empty/non-existent dir: OB_SUCCESS with zero callbacks), or an
  // OB errno on a real storage error. Mirrors the host API `list_dir` slot.
  virtual int32_t list_dir(const char *dir,
                           void (*cb)(void *cb_ctx, const char *name, int32_t is_dir),
                           void *cb_ctx) = 0;

protected:
  // Copies `s` (+1 NUL) into the fs arena and assigns it to `out`.
  int deep_copy_nul_(const common::ObString &s, common::ObString &out);

  common::ObString path_;
  common::ObString access_info_;
  ObExternalFileCacheOptions cache_options_;
  uint64_t tenant_id_ = 0;
  // Owns every persistent string this fs holds (path_/access_info_ and, in the
  // default fs, resolved paths backing cache keys). Lives exactly as long as
  // the fs instance itself (one query stage).
  common::ObArenaAllocator arena_;
};

/// Default file system backed by OB's `ObExternalFileAccess` /
/// `ObExternalFileInfoCollector`. Resolves relative paths against the base,
/// opens one `ObExtStream` per `open` (an internal class), and answers file
/// metadata via the info collector. This is the implementation `ObExtHostCtx`
/// holds and uses by default.
class ObExtDefaultFileSystem : public ObExtFileSystem
{
public:
  ObExtDefaultFileSystem();
  ~ObExtDefaultFileSystem() override;

  void *open(const char *path) override;
  int64_t read(void *stream, char *buf, int64_t size) override;
  int64_t read_at(void *stream, char *buf, int64_t size, int64_t offset) override;
  int64_t tell(void *stream) override;
  int32_t seek(void *stream, int64_t offset, int32_t origin) override;
  int64_t length(void *stream) override;
  void close(void *stream) override;
  int32_t exists(const char *path) override;
  int file_status(const char *path, int64_t &size, int64_t &mtime_ms, int32_t &is_dir) override;
  int32_t list_dir(const char *dir,
                   void (*cb)(void *cb_ctx, const char *name, int32_t is_dir),
                   void *cb_ctx) override;

  // Drops all cached stats AND reclaims the arena holding their keys, so a
  // (re)scan starts from fresh metadata with no leftover key memory — actual
  // tables may have a very large number of files, so key buffers must not
  // accumulate across rescans. Callers must invoke this only when no stream
  // handed out by this fs is still open (the row iterator calls it at the end
  // of reset(), after the plugin has closed all its streams).
  void reset_stat_cache();

private:
  // Joins a (possibly relative) plugin path against the scan's base path. The
  // result is allocated in `cache_arena_` (NUL-terminated) so it can serve as
  // a cache key; it is reclaimed by reset_stat_cache().
  int resolve_path(const char *path, common::ObString &out);
  // One-shot file stat: fills size (bytes) /
  // mtime (ms) / is_dir. `get_file_stat` fails on object-store directories, so
  // the fallback below (is_directory / non-empty listing) still flags those as
  // directories instead of erroring out. Returns OB_SUCCESS on a definite answer.
  int get_file_status_(const common::ObString &path, int64_t &size, int64_t &mtime_ms,
                       bool &is_dir) const;

  // ---- instance-scoped stat cache ----
  // This fs instance lives exactly one query stage (stack-local host ctx in
  // load_schema/plan_create; row-iter member during execute), so the cache
  // naturally dies with the stage: no TTL, no cross-query staleness. Caching
  // a definite "absent" answer avoids re-probing paths the plugin checks
  // repeatedly (e.g. SnapshotManager's snapshot-(N+1) probe). Never shared
  // across instances/stages on purpose — freshness across stages comes from
  // the snapshot pinning, not from shared stats. Keys are ObString views into
  // `cache_arena_` (the resolved paths), so no extra key copying is needed.
  //
  // Two-arena split: the base-class arena holds the long-lived config
  // strings (path_/access_info_, set once per stage, never reclaimed), while
  // `cache_arena_` holds only the resolved-path cache keys so rescan can reuse
  // it wholesale without touching the config strings.
  struct StatEntry
  {
    int64_t size_ = -1;     // bytes; 0 for directories
    int64_t mtime_ms_ = -1;
    bool is_dir_ = false;
    bool exists_ = false;   // false records a definite absence
  };

  // Stat with cache: on miss performs one combined get_file_status_ call
  // (single storage RPC for size+mtime) and stores the definite answer;
  // real storage errors are returned uncached.
  int get_stat_cached_(const common::ObString &path, StatEntry &out);

  static const int64_t STAT_CACHE_BUCKET_NUM = 32;
  common::hash::ObHashMap<common::ObString, StatEntry> stat_cache_;
  // Owns the resolved-path buffers backing stat_cache_ keys. Reused (together
  // with the map) by reset_stat_cache().
  common::ObArenaAllocator cache_arena_{"ExtFsCache"};
};

} // namespace ext_plugin
} // namespace sql
} // namespace oceanbase

#endif // OB_EXT_FILE_SYSTEM_H
