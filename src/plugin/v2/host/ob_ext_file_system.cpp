/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include "plugin/v2/host/ob_ext_file_system.h"

#include "lib/allocator/ob_malloc.h"  // ob_malloc / ob_free / ObMemAttr
#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "plugin/v2/include/ob_external_table_plugin.h"  // OB_EXT_SEEK_*
#include "share/external_table/ob_external_table_utils.h"
#include "share/io/ob_io_define.h"
#include "share/rc/ob_tenant_base.h"
#include "plugin/v2/host/ob_ext_file_system_common.h"

// ob_ext_file_system_common.h ends with #undef USING_LOG_PREFIX (so the header
// does not leak a log prefix into its includer); restore it before any LOG_*
// expands in this TU.
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX SQL
#endif

#include <algorithm>
#include <cstring>
#include <new>

namespace oceanbase
{
namespace sql
{
namespace ext_plugin
{

namespace
{
static constexpr int64_t EXT_IO_TIMEOUT_MS = DEFAULT_IO_WAIT_TIME_MS;

// ---- ObString path helpers (this TU avoids std::string; persistent copies
// live in the owning fs's arena and are NUL-terminated) ----

/// Index of the first occurrence of `needle` in `hay`, or -1.
int64_t find_substring(const common::ObString &hay, const char *needle)
{
  const int64_t nlen = STRLEN(needle);
  int64_t pos = -1;
  if (nlen > 0 && hay.length() >= nlen) {
    for (int64_t i = 0; pos < 0 && i + nlen <= hay.length(); ++i) {
      if (0 == MEMCMP(hay.ptr() + i, needle, nlen)) {
        pos = i;
      }
    }
  }
  return pos;
}

bool is_absolute(const common::ObString &p)
{
  return !p.empty() && '/' == p[0];
}

/// Copy `src` into `alloc` with an appended NUL; `out` aliases the copy.
/// Uses ob_write_string(c_style=true); empty input still yields a valid
/// NUL-terminated empty buffer so callers may always pass ptr() as a C string.
int deep_copy_nul(common::ObIAllocator &alloc, const common::ObString &src,
                  common::ObString &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  if (OB_FAIL(common::ob_write_string(alloc, src, out, true /*c_style*/))) {
    LOG_WARN("ext deep_copy_nul ob_write_string failed", K(ret), K(src));
  } else if (out.empty()) {
    char *buf = static_cast<char *>(alloc.alloc(1));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("ext deep_copy_nul alloc failed", K(ret));
    } else {
      buf[0] = '\0';
      out.assign_ptr(buf, 0);
    }
  }
  return ret;
}

/// Normalizes malformed local file URIs like file:/tmp/a to file:///tmp/a so
/// they are treated as full file URIs instead of relative paths. On a change
/// the result is allocated in `alloc` (NUL-terminated); otherwise `out`
/// aliases `src`.
int normalize_file_uri(common::ObIAllocator &alloc, const common::ObString &src,
                       common::ObString &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  if (src.prefix_match("file:/") && !src.prefix_match("file://")) {
    // Replace the 5-byte "file:" prefix with the 7-byte "file://" prefix.
    const int64_t tail_len = src.length() - 5;
    char *buf = static_cast<char *>(alloc.alloc(7 + tail_len + 1));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("ext normalize_file_uri alloc failed", K(ret), K(src));
    } else {
      MEMCPY(buf, "file://", 7);
      if (tail_len > 0) {
        MEMCPY(buf + 7, src.ptr() + 5, tail_len);
      }
      buf[7 + tail_len] = '\0';
      out.assign_ptr(buf, static_cast<int32_t>(7 + tail_len));
    }
  } else {
    out = src;
  }
  return ret;
}

/// Concatenate two path segments with a single '/' between them if needed;
/// result is allocated in `alloc` (NUL-terminated).
int join_path_nul(common::ObIAllocator &alloc, const common::ObString &lhs,
                  const common::ObString &rhs, common::ObString &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  if (lhs.empty()) {
    ret = deep_copy_nul(alloc, rhs, out);
  } else if (rhs.empty()) {
    ret = deep_copy_nul(alloc, lhs, out);
  } else {
    const bool need_slash = ('/' != lhs[lhs.length() - 1]);
    const int64_t total = lhs.length() + (need_slash ? 1 : 0) + rhs.length();
    char *buf = static_cast<char *>(alloc.alloc(total + 1));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("ext join_path alloc failed", K(ret), K(lhs), K(rhs));
    } else {
      MEMCPY(buf, lhs.ptr(), lhs.length());
      int64_t off = lhs.length();
      if (need_slash) {
        buf[off++] = '/';
      }
      MEMCPY(buf + off, rhs.ptr(), rhs.length());
      buf[total] = '\0';
      out.assign_ptr(buf, static_cast<int32_t>(total));
    }
  }
  return ret;
}

/// Counts directory entries; callers only need to know whether the listing is
/// non-empty (the object-store "directory" fallback in exists/file_status).
class ObExtCountingCollector final : public common::ObBaseDirEntryOperator
{
public:
  ObExtCountingCollector() = default;
  int func(const dirent *entry) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(entry)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ext counting collector got null entry", K(ret));
    } else {
      ++count_;
    }
    return ret;
  }
  int64_t count() const { return count_; }

private:
  int64_t count_ = 0;
};

// Lists files (and, with_directories, directories) under `dir`, only counting
// entries. Both buffers must stay valid for the duration of the call (they do:
// they live in the fs arena).
int collect_entries_(const common::ObString &dir, bool with_directories,
                     int64_t &entry_count, uint64_t tenant_id,
                     const common::ObString &access_info)
{
  int ret = OB_SUCCESS;
  entry_count = 0;
  StorageInfoHolder storage_holder;
  if (OB_FAIL(storage_holder.init(dir, access_info))) {
    LOG_WARN("ext collect_entries init storage failed", K(ret), K(dir));
  } else {
    ExtStorageTenantGuard storage_tenant_guard(tenant_id);
    common::ObStorageUtil util;
    ObExtCountingCollector file_collector;
    if (OB_FAIL(util.open(storage_holder.storage_info_))) {
      LOG_WARN("ext collect_entries open storage util failed", K(ret), K(dir));
    } else if (OB_FAIL(util.list_files(dir, file_collector))) {
      util.close();
      LOG_WARN("ext collect_entries list_files failed", K(ret), K(dir));
    } else {
      entry_count += file_collector.count();
      if (with_directories && 0 == entry_count) {
        ObExtCountingCollector dir_collector;
        dir_collector.set_dir_flag();
        if (OB_FAIL(util.list_directories(dir, dir_collector))) {
          LOG_WARN("ext collect_entries list_directories failed", K(ret), K(dir));
        } else {
          entry_count += dir_collector.count();
        }
      }
      util.close();
    }
  }
  return ret;
}

// Direct-callback collector: instead of buffering names into a container, it
// forwards each entry straight to the host's list_dir callback `cb`. `is_dir`
// is fixed at construction (list_files channel uses is_dir=0; the
// list_directories channel uses is_dir=1 and calls set_dir_flag() so
// ObStorageUtil routes to the directory-listing path). This is what
// ObExtDefaultFileSystem::list_dir uses to honor the host API contract without
// a cross-boundary container.
class ObExtListDirCbCollector final : public common::ObBaseDirEntryOperator
{
public:
  ObExtListDirCbCollector(void (*cb)(void *, const char *, int32_t), void *cb_ctx, int32_t is_dir)
      : cb_(cb), cb_ctx_(cb_ctx), is_dir_(is_dir) {}

  int func(const dirent *entry) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(entry) || OB_ISNULL(cb_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ext list_dir collector got null entry/cb", K(ret), KP(entry), KP(cb_));
    } else {
      cb_(cb_ctx_, entry->d_name, is_dir_);
    }
    return ret;
  }

private:
  void (*cb_)(void *, const char *, int32_t);
  void *cb_ctx_;
  int32_t is_dir_;
};

/// One open read stream over an external file. An internal detail of
/// ObExtDefaultFileSystem: created by `open` (heap, returned as the opaque
/// void* handle), destroyed by `close`. It carries the file's size/mtime
/// (resolved by the owning fs instance) and reads through
/// `ObExternalFileAccess` (the device-aware DAM read path). `path_` /
/// `access_info_` are non-owning views into the owning fs's arena; the fs
/// always outlives every stream it has handed out.
class ObExtStream
{
public:
  ObExtStream() = default;
  ~ObExtStream() { close_impl(); }

  int init(const common::ObString &path, const common::ObString &access_info,
           const ObExternalFileCacheOptions &cache_options, uint64_t tenant_id,
           const int64_t file_length, const int64_t modify_time);
  void close_impl();

  int64_t read(char *buf, int64_t size);
  int64_t read_at(char *buf, int64_t size, int64_t offset);
  int64_t tell() const { return pos_; }
  int32_t seek(int64_t offset, int32_t origin);
  int64_t length() const { return file_length_; }

private:
  common::ObString path_;          // view into the owning fs's arena
  common::ObString access_info_;   // view into the owning fs's arena
  ObExternalFileCacheOptions cache_options_;
  uint64_t tenant_id_ = 0;
  int64_t pos_ = 0;
  int64_t file_length_ = -1;
  int64_t modify_time_ = -1;
  ObExternalFileAccess file_access_;
};

int ObExtStream::init(const common::ObString &path, const common::ObString &access_info,
                      const ObExternalFileCacheOptions &cache_options, uint64_t tenant_id,
                      const int64_t file_length, const int64_t modify_time)
{
  int ret = OB_SUCCESS;
  path_ = path;
  access_info_ = access_info;
  cache_options_ = cache_options;
  tenant_id_ = tenant_id;
  // size/mtime are resolved by the owning fs instance (single combined stat,
  // instance-scoped cache) so the stream just carries them into the access
  // layer; it no longer stats by itself.
  file_length_ = file_length;
  modify_time_ = modify_time;
  // Both views point into the owning fs's arena and stay valid for this
  // stream's lifetime (the fs outlives every stream it hands out).
  MTL_SWITCH(tenant_id_)
  {
    ObExternalFileUrlInfo file_url_info(path_,
                                        access_info_,
                                        path_,
                                        common::ObString::make_empty_string(),
                                        file_length_,
                                        modify_time_);
    if (OB_FAIL(file_access_.open(file_url_info, cache_options_))) {
      LOG_WARN("ext stream file_access open failed", K(ret), K(path_));
    }
  }
  if (OB_FAIL(ret)) {
    close_impl();
  }
  return ret;
}

void ObExtStream::close_impl()
{
  int ret = OB_SUCCESS;
  if (file_access_.is_opened()) {
    MTL_SWITCH(tenant_id_)
    {
      if (OB_FAIL(file_access_.close())) {
        LOG_WARN("ext stream close failed", K(ret), K(path_));
      }
    }
  }
  file_length_ = -1;
  modify_time_ = -1;
  pos_ = 0;
}

int64_t ObExtStream::read(char *buf, int64_t size)
{
  int64_t result = -1;  // -1 = error sentinel; 0 = EOF; >0 = bytes read
  if (OB_ISNULL(buf) || size < 0) {
    // result stays -1
  } else if (0 == size) {
    result = 0;
  } else if (!file_access_.is_opened()) {
    // result stays -1
  } else {
    int64_t offset = pos_;
    if (offset >= file_length_) {
      result = 0;  // EOF
    } else {
      int64_t req = std::min<int64_t>(size, file_length_ - offset);
      ObExternalReadInfo read_info(offset, buf, req, EXT_IO_TIMEOUT_MS);
      int64_t read_size = 0;
      int ret = OB_SUCCESS;
      if (OB_FAIL(file_access_.pread(read_info, read_size))) {
        LOG_WARN("ext stream read failed", K(ret), K(path_));
        // result stays -1
      } else {
        pos_ += read_size;
        result = read_size;
      }
    }
  }
  return result;
}

int64_t ObExtStream::read_at(char *buf, int64_t size, int64_t offset)
{
  int64_t result = -1;  // -1 = error sentinel; 0 = EOF; >0 = bytes read
  if (OB_ISNULL(buf) || size < 0) {
    // result stays -1
  } else if (0 == size) {
    result = 0;
  } else if (!file_access_.is_opened()) {
    // result stays -1
  } else if (offset >= file_length_) {
    result = 0;  // EOF
  } else {
    int64_t req = std::min<int64_t>(size, file_length_ - offset);
    ObExternalReadInfo read_info(offset, buf, req, EXT_IO_TIMEOUT_MS);
    int64_t read_size = 0;
    int ret = OB_SUCCESS;
    if (OB_FAIL(file_access_.pread(read_info, read_size))) {
      LOG_WARN("ext stream read_at failed", K(ret), K(path_));
      // result stays -1
    } else {
      result = read_size;
    }
  }
  return result;
}

int32_t ObExtStream::seek(int64_t offset, int32_t origin)
{
  int32_t result = -1;  // -1 = error; 0 = ok
  int64_t next = pos_;
  bool valid_origin = true;
  switch (origin) {
    case OB_EXT_SEEK_SET: next = offset; break;
    case OB_EXT_SEEK_CUR: next = pos_ + offset; break;
    case OB_EXT_SEEK_END: next = file_length_ + offset; break;
    default: valid_origin = false; break;  // result stays -1
  }
  if (valid_origin && next >= 0 && next <= file_length_) {
    pos_ = next;
    result = 0;
  }
  return result;
}
} // namespace

// ============================================================================
// ObExtFileSystem (base)
// ============================================================================

int ObExtFileSystem::deep_copy_nul_(const common::ObString &s, common::ObString &out)
{
  return deep_copy_nul(arena_, s, out);
}

int ObExtFileSystem::set_path(const common::ObString &p)
{
  int ret = OB_SUCCESS;
  common::ObString normalized;
  if (OB_FAIL(normalize_file_uri(arena_, p, normalized))) {
    LOG_WARN("ext set_path normalize failed", K(ret), K(p));
  } else if (!normalized.ptr()) {
    // Empty input: keep path_ empty (absolute/base-less paths pass through).
    path_.reset();
  } else if (normalized.ptr() == p.ptr()) {
    // normalize_file_uri aliased the input (no rewrite); copy so the buffer is
    // owned by the fs arena and NUL-terminated.
    ret = deep_copy_nul_(p, path_);
  } else {
    path_ = normalized;  // already arena-allocated and NUL-terminated
  }
  return ret;
}

int ObExtFileSystem::set_access_info(const common::ObString &a)
{
  return deep_copy_nul_(a, access_info_);
}

// ============================================================================
// ObExtDefaultFileSystem
// ============================================================================

ObExtDefaultFileSystem::ObExtDefaultFileSystem()
{
  int ret = common::OB_SUCCESS;
  // Best-effort init: the map is created on the stack together with its host
  // ctx, so a rare allocation failure degrades the fs to stat-without-cache
  // (handled per-call below) instead of failing the whole query stage.
  if (common::OB_SUCCESS != (ret = stat_cache_.create(
      STAT_CACHE_BUCKET_NUM, lib::ObLabel("ExtFsStat")))) {
    LOG_WARN_RET(ret, "ext stat cache create failed, run without cache");
  }
}

ObExtDefaultFileSystem::~ObExtDefaultFileSystem()
{
  stat_cache_.destroy();
}

int ObExtDefaultFileSystem::resolve_path(const char *path, common::ObString &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  const common::ObString raw(path != nullptr ? static_cast<int32_t>(STRLEN(path)) : 0,
                             path != nullptr ? path : "");
  common::ObString normalized_path;
  // All persistent outputs go into cache_arena_: they either back stat cache
  // keys or alias arena-held members (path_), both reclaimed consistently.
  // (path_ itself lives in the base-class arena and is never reclaimed here.)
  if (OB_FAIL(normalize_file_uri(cache_arena_, raw, normalized_path))) {
    LOG_WARN("ext resolve_path normalize failed", K(ret), K(raw));
  } else if (normalized_path.empty()) {
    out = path_;
  } else if (find_substring(normalized_path, "://") >= 0 || is_absolute(normalized_path)
             || path_.empty()) {
    // Absolute/full URI (or no base to resolve against): take it as-is. If
    // normalize aliased the input, move it into cache_arena_ first.
    if (normalized_path.ptr() == raw.ptr()) {
      ret = deep_copy_nul(cache_arena_, normalized_path, out);
    } else {
      out = normalized_path;
    }
  } else {
    ret = join_path_nul(cache_arena_, path_, normalized_path, out);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("ext resolve_path failed", K(ret), K(raw));
  }
  return ret;
}

int ObExtDefaultFileSystem::get_file_status_(const common::ObString &path, int64_t &size,
                                              int64_t &mtime_ms, bool &is_dir) const
{
  int ret = OB_SUCCESS;
  size = -1;
  mtime_ms = -1;
  is_dir = false;
  StorageInfoHolder storage_holder;
  if (OB_FAIL(storage_holder.init(path, access_info_))) {
    LOG_WARN("ext get_file_status init storage failed", K(ret), K(path));
  } else {
    ExtStorageTenantGuard storage_tenant_guard(tenant_id_);
    common::ObStorageUtil util;
    common::ObIODFileStat statbuf;
    bool is_directory = false;
    if (OB_FAIL(util.open(storage_holder.storage_info_))) {
      LOG_WARN("ext get_file_status open storage util failed", K(ret), K(path));
    } else {
      const int stat_ret = util.get_file_stat(path, false, statbuf);
      const int dir_ret = util.is_directory(path, false, is_directory);
      util.close();

      if (OB_SUCCESS == stat_ret) {
        if (OB_SUCCESS != dir_ret) {
          ret = dir_ret;
          LOG_WARN("ext get_file_status is_directory failed", K(ret), K(path));
        } else {
          size = static_cast<int64_t>(statbuf.size_);
          mtime_ms = statbuf.mtime_s_ * 1000;
          is_dir = is_directory;
        }
      } else {
        // get_file_stat fails on object-store directories (no real metadata).
        // Fallback 1: is_directory (uses detect_storage_obj_meta).
        // Fallback 2: a non-empty listing means it is a directory after all.
        bool treat_as_dir = (OB_SUCCESS == dir_ret) && is_directory;
        if (!treat_as_dir) {
          int64_t entry_count = 0;
          const int list_ret = collect_entries_(path, true, entry_count, tenant_id_, access_info_);
          if (OB_SUCCESS == list_ret) {
            treat_as_dir = entry_count > 0;
          } else {
            ret = list_ret;
            LOG_WARN("ext get_file_status collect_entries failed", K(ret), K(path));
          }
        }
        if (OB_SUCC(ret) && treat_as_dir) {
          is_dir = true;
          size = 0;
          mtime_ms = 0;
        } else if (OB_SUCC(ret)) {
          ret = stat_ret;
          LOG_WARN("ext get_file_status get_file_stat failed", K(ret), K(path));
        }
      }
    }
  }
  return ret;
}

void *ObExtDefaultFileSystem::open(const char *path)
{
  int ret = OB_SUCCESS;
  void *stream = nullptr;
  common::ObString resolved;
  StatEntry st;
  if (OB_FAIL(resolve_path(path, resolved))) {
    // stream stays nullptr
  } else if (OB_FAIL(get_stat_cached_(resolved, st))) {
    if (OB_OBJECT_NOT_EXIST != ret) {
      LOG_WARN("ext open stat failed", K(ret), K(resolved));
    }
    // absence or error: stream stays nullptr
  } else if (!st.exists_ || st.is_dir_) {
    // stream stays nullptr
  } else {
    // ObExtStream is opened by the plugin on one thread and closed on another, so
    // allocate through OB's thread-safe global allocator (ob_malloc + placement
    // new) — tenant-accounted under "ExtStream" — not global new (which bypasses
    // OB accounting). Freed by ~ObExtStream() + ob_free in close().
    void *mem = ob_malloc(sizeof(ObExtStream), ObMemAttr(tenant_id_, "ExtStream"));
    if (OB_ISNULL(mem)) {
      // stream stays nullptr
    } else {
      ObExtStream *s = new (mem) ObExtStream();
      if (OB_FAIL(s->init(resolved, access_info_, cache_options_, tenant_id_,
                          st.size_, st.mtime_ms_))) {
        s->~ObExtStream();
        ob_free(s);
      } else {
        stream = s;
      }
    }
  }
  return stream;
}

int64_t ObExtDefaultFileSystem::read(void *stream, char *buf, int64_t size)
{
  auto *s = static_cast<ObExtStream *>(stream);
  return OB_NOT_NULL(s) ? s->read(buf, size) : -1;
}

int64_t ObExtDefaultFileSystem::read_at(void *stream, char *buf, int64_t size, int64_t offset)
{
  auto *s = static_cast<ObExtStream *>(stream);
  return OB_NOT_NULL(s) ? s->read_at(buf, size, offset) : -1;
}

int64_t ObExtDefaultFileSystem::tell(void *stream)
{
  auto *s = static_cast<ObExtStream *>(stream);
  return OB_NOT_NULL(s) ? s->tell() : -1;
}

int32_t ObExtDefaultFileSystem::seek(void *stream, int64_t offset, int32_t origin)
{
  auto *s = static_cast<ObExtStream *>(stream);
  return OB_NOT_NULL(s) ? s->seek(offset, origin) : -1;
}

int64_t ObExtDefaultFileSystem::length(void *stream)
{
  auto *s = static_cast<ObExtStream *>(stream);
  return OB_NOT_NULL(s) ? s->length() : -1;
}

void ObExtDefaultFileSystem::close(void *stream)
{
  auto *s = static_cast<ObExtStream *>(stream);
  if (OB_NOT_NULL(s)) {
    s->~ObExtStream();  // calls close_impl
    ob_free(s);
  }
}

int32_t ObExtDefaultFileSystem::exists(const char *path)
{
  // Object-store directories report is_exist=false but
  // have non-empty children, so fall back to a listing before declaring "not
  // exist". Plugins may probe a table root this way.
  int32_t result = -1;  // -1 = error; 0/1 = exists answer
  int ret = OB_SUCCESS;
  common::ObString resolved;
  if (OB_FAIL(resolve_path(path, resolved))) {
    // result stays -1
  } else {
    // Instance cache first: definite answers from earlier stats/probes in this
    // same query stage (covers e.g. repeated snapshot-(N+1) probes).
    StatEntry cached;
    if (OB_SUCCESS == stat_cache_.get_refactored(resolved, cached)) {
      result = cached.exists_ ? 1 : 0;
    }
  }
  if (-1 == result && OB_SUCC(ret)) {
    StorageInfoHolder storage_holder;
    bool is_exist = false;
    if (OB_FAIL(storage_holder.init(resolved, access_info_))) {
      LOG_WARN("ext exists init storage failed", K(ret), K(resolved));
    } else {
      ExtStorageTenantGuard storage_tenant_guard(tenant_id_);
      common::ObStorageUtil util;
      if (OB_FAIL(util.open(storage_holder.storage_info_))) {
        LOG_WARN("ext exists open storage util failed", K(ret), K(resolved));
      } else if (OB_FAIL(util.is_exist(resolved, false, is_exist))) {
        util.close();
        LOG_WARN("ext exists is_exist failed", K(ret), K(resolved));
      } else {
        util.close();
        if (!is_exist) {
          int64_t entry_count = 0;
          const int list_ret = collect_entries_(resolved, true, entry_count, tenant_id_,
                                                access_info_);
          if (OB_SUCCESS == list_ret) {
            result = entry_count > 0 ? 1 : 0;
          } else if (OB_HDFS_PATH_NOT_FOUND == list_ret) {
            // HDFS reports a missing path as OB_HDFS_PATH_NOT_FOUND when
            // listing it; combined with is_exist=false above this is a
            // definitive "does not exist" answer, not an error. (Local/OSS
            // backends return SUCCESS with an empty listing instead.)
            result = 0;
            LOG_TRACE("ext exists path not found on hdfs", K(list_ret), K(resolved));
          } else {
            LOG_WARN("ext exists collect_entries failed", K(list_ret), K(resolved));
          }
        } else {
          result = 1;
        }
      }
    }
    // Record definite answers; errors are not cached. size/mtime stay -1: a
    // later get_stat_cached_ will upgrade the entry with a real stat when the
    // file is actually opened.
    if (1 == result) {
      StatEntry entry;
      entry.exists_ = true;
      (void)stat_cache_.set_refactored(resolved, entry, 1 /*overwrite*/);
    } else if (0 == result) {
      StatEntry entry;
      entry.exists_ = false;
      (void)stat_cache_.set_refactored(resolved, entry, 1 /*overwrite*/);
    }
  }
  return result;
}

int ObExtDefaultFileSystem::get_stat_cached_(const common::ObString &path, StatEntry &out)
{
  int ret = OB_SUCCESS;
  bool need_stat = true;
  StatEntry cached;
  const bool cache_inited = stat_cache_.created();
  if (cache_inited && OB_SUCCESS == stat_cache_.get_refactored(path, cached)) {
    if (!cached.exists_) {
      ret = OB_OBJECT_NOT_EXIST;  // definite absence, cached
      need_stat = false;
    } else if (cached.size_ >= 0) {
      out = cached;  // complete hit
      need_stat = false;
    }
    // exists_ with size_<0 is a partial entry recorded by exists(); fall
    // through and refresh it with a real stat.
  }
  if (OB_SUCC(ret) && need_stat) {
    StatEntry fresh;
    bool directory = false;
    // One combined stat: a single storage RPC returns size + mtime together.
    const int stat_ret = get_file_status_(path, fresh.size_, fresh.mtime_ms_, directory);
    if (OB_SUCCESS == stat_ret) {
      fresh.is_dir_ = directory;
      fresh.exists_ = true;
      out = fresh;
      if (cache_inited) {
        // `path` aliases a persistent cache_arena_ copy (resolve_path output),
        // so the key stays valid until reset_stat_cache().
        (void)stat_cache_.set_refactored(path, fresh, 1 /*overwrite*/);
      }
    } else if (OB_OBJECT_NOT_EXIST == stat_ret || OB_ENTRY_NOT_EXIST == stat_ret
               || OB_FILE_NOT_EXIST == stat_ret || OB_HDFS_PATH_NOT_FOUND == stat_ret) {
      // Definite absence. Cache it so repeated probes (paimon checks
      // snapshot-(N+1) on every query) do not re-hit storage.
      fresh.exists_ = false;
      if (cache_inited) {
        (void)stat_cache_.set_refactored(path, fresh, 1 /*overwrite*/);
      }
      ret = OB_OBJECT_NOT_EXIST;
    } else {
      ret = stat_ret;
      LOG_WARN("ext get_stat_cached stat failed", K(ret), K(path));
    }
  }
  return ret;
}

void ObExtDefaultFileSystem::reset_stat_cache()
{
  if (stat_cache_.created()) {
    stat_cache_.reuse();
  }
  // Reclaim the resolved-path buffers backing the (now empty) cache keys.
  // Safety: callers invoke this only after the plugin has closed every stream
  // handed out by this fs (row iter reset() runs the plugin's reader_close_*
  // first), so no live stream still references a cache_arena_ buffer.
  cache_arena_.reuse();
}

int ObExtDefaultFileSystem::file_status(
    const char *path,
    int64_t &size,
    int64_t &mtime_ms,
    int32_t &is_dir)
{
  int ret = OB_SUCCESS;
  size = -1;
  mtime_ms = -1;
  is_dir = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ext file_status invalid argument", K(ret), KP(path));
  } else {
    common::ObString resolved;
    bool directory = false;
    if (OB_FAIL(resolve_path(path, resolved))) {
      LOG_WARN("ext file_status resolve_path failed", K(ret), KP(path));
    } else if (OB_FAIL(get_file_status_(resolved, size, mtime_ms, directory))) {
      LOG_WARN("ext file_status get_file_status failed", K(ret), KP(path));
    } else {
      is_dir = directory ? 1 : 0;
    }
  }
  return ret;
}

int32_t ObExtDefaultFileSystem::list_dir(const char *dir,
                                         void (*cb)(void *cb_ctx, const char *name, int32_t is_dir),
                                         void *cb_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dir) || OB_ISNULL(cb)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ext list_dir invalid argument", K(ret), KP(dir), KP(cb));
  } else {
    common::ObString resolved;
    if (OB_FAIL(resolve_path(dir, resolved))) {
      LOG_WARN("ext list_dir resolve_path failed", K(ret), KP(dir));
    } else {
      // Two passes (files then directories) so each entry carries an accurate
      // is_dir flag — d_type is DT_UNKNOWN on object stores, so we can't rely
      // on the dirent. Each entry is streamed straight to `cb` via
      // ObExtListDirCbCollector (no cross-boundary container). An empty /
      // non-existent dir yields OB_SUCCESS with zero callbacks.
      StorageInfoHolder storage_holder;
      if (OB_FAIL(storage_holder.init(resolved, access_info_))) {
        LOG_WARN("ext list_dir init storage failed", K(ret), K(resolved));
      } else {
        ExtStorageTenantGuard storage_tenant_guard(tenant_id_);
        common::ObStorageUtil util;
        if (OB_FAIL(util.open(storage_holder.storage_info_))) {
          LOG_WARN("ext list_dir open storage util failed", K(ret), K(resolved));
        } else {
          ObExtListDirCbCollector file_collector(cb, cb_ctx, /*is_dir=*/0);
          if (OB_FAIL(util.list_files(resolved, file_collector))) {
            util.close();
            LOG_WARN("ext list_dir list_files failed", K(ret), K(resolved));
          } else {
            ObExtListDirCbCollector dir_collector(cb, cb_ctx, /*is_dir=*/1);
            dir_collector.set_dir_flag();
            if (OB_FAIL(util.list_directories(resolved, dir_collector))) {
              LOG_WARN("ext list_dir list_directories failed", K(ret), K(resolved));
            }
            util.close();
          }
        }
      }
    }
  }
  return ret;
}

} // namespace ext_plugin
} // namespace sql
} // namespace oceanbase

#undef USING_LOG_PREFIX
