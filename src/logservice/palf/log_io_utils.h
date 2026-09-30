/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_LOG_IO_UTILS_
#define OCEANBASE_LOGSERVICE_LOG_IO_UTILS_
#include <dirent.h>                                      // dirent
#include "lib/function/ob_function.h"
#include "log_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace palf
{

int openat_with_retry(const int dir_fd,
                      const char *block_path,
                      const int flag,
                      const int mode,
                      int &fd);
int close_with_ret(const int fd);

int rename_with_retry(const char *src_name, const char *dest_name);

int renameat_with_retry(const int srd_dir_fd, const char *src_name,
                        const int dest_dir_fd, const char *dest_name);

int fsync_with_retry(const int dir_fd);

class ObBaseDirFunctor
{
public:
  virtual int func(const dirent *entry) = 0;
};

int scan_dir(const char *dir_name, ObBaseDirFunctor &functor);

class GetBlockCountFunctor : public ObBaseDirFunctor
{
public:
  GetBlockCountFunctor(const char *dir)
    : dir_(dir), count_(0)
  {
  }
  virtual ~GetBlockCountFunctor() = default;

  int func(const dirent *entry) override final;
	int64_t get_block_count() {return count_;}
private:
  const char *dir_;
	int64_t count_;

  DISALLOW_COPY_AND_ASSIGN(GetBlockCountFunctor);
};

class TrimLogDirectoryFunctor : public ObBaseDirFunctor
{
public:
  TrimLogDirectoryFunctor(const char *dir, ILogBlockPool *log_block_pool)
    : dir_(dir),
      min_block_id_(LOG_INVALID_BLOCK_ID),
      max_block_id_(LOG_INVALID_BLOCK_ID),
      log_block_pool_(log_block_pool)
  {
  }
  virtual ~TrimLogDirectoryFunctor() = default;

  int func(const dirent *entry) override final;
  block_id_t get_min_block_id() const { return min_block_id_; }
  block_id_t get_max_block_id() const { return max_block_id_; }
private:
	int rename_flashback_to_normal_(const char *file_name);
  int try_to_remove_block_(const int dir_fd, const char *file_name);
  const char *dir_;
  block_id_t min_block_id_;
  block_id_t max_block_id_;
  ILogBlockPool *log_block_pool_;

  DISALLOW_COPY_AND_ASSIGN(TrimLogDirectoryFunctor);
};

int reuse_block_at(const int fd, const char *block_path);
bool check_rename_success(const char *src_name,
                          const char *dest_name);
bool check_renameat_success(const int src_dir_fd,
                            const char *src_name,
                            const int dest_dir_fd,
                            const char *dest_name);
int check_file_exist(const char *file_name,
                     bool &exist);
int check_file_exist(const int dir_fd,
                     const char *file_name,
                     bool &exist);

struct ObFallocateProbeOps
{
  typedef common::ObFunction<int(const char *dir, int &fd)> OpenOp;
  typedef common::ObFunction<int(const char *path, int &fd)> CompatOpenOp;
  typedef common::ObFunction<int(const int fd,
                                 const int mode,
                                 const int64_t offset,
                                 const int64_t len)> FallocateOp;
  typedef common::ObFunction<int(const int fd)> CloseOp;
  typedef common::ObFunction<int(const char *path)> UnlinkOp;

  // Ops without compat members can only probe with an anonymous O_TMPFILE file.
  ObFallocateProbeOps(const OpenOp &open_op,
                      const FallocateOp &fallocate_op,
                      const CloseOp &close_op)
    : open_(open_op),
      fallocate_(fallocate_op),
      close_(close_op)
  {}

  ObFallocateProbeOps(const OpenOp &open_op,
                      const FallocateOp &fallocate_op,
                      const CloseOp &close_op,
                      const CompatOpenOp &open_compat_op,
                      const UnlinkOp &unlink_op)
    : open_(open_op),
      fallocate_(fallocate_op),
      close_(close_op),
      open_compat_(open_compat_op),
      unlink_(unlink_op)
  {}

  bool is_valid() const
  {
    return open_.is_valid() && fallocate_.is_valid() && close_.is_valid();
  }

  bool is_compat_valid() const
  {
    return open_compat_.is_valid() && unlink_.is_valid();
  }

  OpenOp open_;
  FallocateOp fallocate_;
  CloseOp close_;
  CompatOpenOp open_compat_;
  UnlinkOp unlink_;
};

int posix_probe_open(const char *dir, int &fd);
int posix_probe_open_compat(const char *path, int &fd);
int posix_probe_fallocate(const int fd,
                          const int mode,
                          const int64_t offset,
                          const int64_t len);
int posix_probe_close(const int fd);
int posix_probe_unlink(const char *path);

extern const ObFallocateProbeOps POSIX_FALLOCATE_PROBE_OPS;

// Default fallocate probe operations accessor, defined as weak symbol in log_io_utils.cpp
// and can be overridden by unit test binaries.
const ObFallocateProbeOps &get_default_fallocate_probe_ops();

class ObFallocateProbeParam
{
public:
  ObFallocateProbeParam() = delete;

  ObFallocateProbeParam(
      const char *disk_role,
      const char *directory,
      const int64_t probe_size,
      const int extra_mode = 0)
    : disk_role_(disk_role),
      directory_(directory),
      probe_size_(probe_size),
      extra_mode_(extra_mode),
      extra_mode_name_(get_fallocate_mode_name(extra_mode))
  {}

  bool is_valid() const
  {
    return nullptr != disk_role_ && '\0' != disk_role_[0]
        && nullptr != directory_ && '\0' != directory_[0]
        && probe_size_ > 0 && nullptr != extra_mode_name_;
  }

  const char *get_disk_role() const
  {
    return disk_role_;
  }

  int64_t get_probe_size() const
  {
    return probe_size_;
  }

  int get_extra_mode() const
  {
    return extra_mode_;
  }

  const char *get_extra_mode_name() const
  {
    return extra_mode_name_;
  }

  const char *get_probe_file_dir() const
  {
    return directory_;
  }

  // Path of the named probe file used by the compat fallback for filesystems
  // without O_TMPFILE support (e.g. NFS). Keep it in sync with the stale-file
  // cleanup in do_load_ which removes leftover "*.tmp" files.
  int get_probe_file_path(char *buf, const int64_t buf_len) const
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(buf) || buf_len <= 0 || !is_valid()) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      ret = databuff_printf(buf, buf_len, "%s/.ob_fallocate_probe_%s.tmp", directory_, disk_role_);
    }
    return ret;
  }

  static const char *get_fallocate_mode_name(const int mode);
  static ObFallocateProbeParam get_clog_param(const char *directory);
#ifdef OB_BUILD_ARBITRATION
  static ObFallocateProbeParam get_arbitration_param(const char *directory);
#endif

  TO_STRING_KV(K_(disk_role),
               K_(directory),
               K_(probe_size),
               K_(extra_mode),
               K_(extra_mode_name));

private:
  const char *disk_role_;
  const char *directory_;
  int64_t probe_size_;
  int extra_mode_;
  const char *extra_mode_name_;
};

// General probe implementation parameterized with target probe parameters and ops.
// Probes with an anonymous O_TMPFILE file; if that is unsupported by the file
// system (e.g. NFS), falls back to a named temporary file probe with ops' compat
// members.
int probe_fallocate_capability_with_ops(const ObFallocateProbeParam &param,
                                        const ObFallocateProbeOps &ops);

// Probe required fallocate modes before any local I/O service is initialized.
int check_file_system_fallocate_capability(const char *clog_dir);

#ifdef OB_BUILD_ARBITRATION
// Arbitration PALF stores only meta blocks and requires fallocate mode=0.
int check_arbitration_file_system_fallocate_capability(const char *clog_dir);
#endif

} // end namespace palf
} // end namespace oceanbase
#endif
