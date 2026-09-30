/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "log_io_utils.h"
#include "log_block_pool_interface.h"                  // ILogBlockPool
#include <linux/falloc.h> // FALLOC_FL_ZERO_RANGE for linux kernel 3.15
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#ifdef OB_BUILD_ARBITRATION
#include "logservice/arbserver/palf_lite_define.h"
#endif

namespace oceanbase
{
namespace palf
{

const int64_t RETRY_INTERVAL = 10*1000;
int openat_with_retry(const int dir_fd,
                      const char *block_path,
                      const int flag,
                      const int mode,
                      int &fd)
{
  int ret = OB_SUCCESS;
  if (-1 == dir_fd || NULL == block_path || -1 == flag || -1 == mode) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument", K(dir_fd), KP(block_path), K(flag), K(mode));
  } else {
    do {
      if (-1 == (fd = ::openat(dir_fd, block_path, flag, mode))) {
        ret = convert_sys_errno();
        PALF_LOG(ERROR, "open block failed", K(ret), K(errno), K(block_path), K(dir_fd));
        ob_usleep(RETRY_INTERVAL);
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } while (OB_FAIL(ret));
  }
  return ret;
}
int close_with_ret(const int fd)
{
  int ret = OB_SUCCESS;
  if (-1 == fd) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument", K(fd));
  } else if (-1 == (::close(fd))) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "close block failed", K(ret), K(errno), K(fd));
  } else {
  }
  return ret;
}

int check_file_exist(const char *file_name,
                     bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  struct stat64 file_info;
  if (OB_ISNULL(file_name) || OB_UNLIKELY(strlen(file_name) == 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments.", KCSTRING(file_name), K(ret));
  } else {
    exist = (0 == ::stat64(file_name, &file_info));
  }
  return ret;
}

int check_file_exist(const int dir_fd,
                     const char *file_name,
                     bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  struct stat64 file_info;
  const int64_t flag = 0;
  if (OB_ISNULL(file_name) || OB_UNLIKELY(strlen(file_name) == 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments.", KCSTRING(file_name), K(ret));
  } else {
    exist = (0 == ::fstatat64(dir_fd, file_name, &file_info, flag));
  }
  return ret;
}

bool check_rename_success(const char *src_name,
                          const char *dest_name)
{
  bool bool_ret = false;
  bool src_exist = false;
  bool dest_exist = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_file_exist(src_name, src_exist))) {
    PALF_LOG(WARN, "check_file_exist failed", KR(ret), K(src_name), K(dest_name));
  } else if (!src_exist && OB_FAIL(check_file_exist(dest_name, dest_exist))) {
    PALF_LOG(WARN, "check_file_exist failed", KR(ret), K(src_name), K(dest_name));
  } else if (!src_exist && dest_exist) {
    bool_ret = true;
    PALF_LOG(INFO, "check_rename_success return true",
             KR(ret), K(src_name), K(dest_name), K(src_exist), K(dest_exist));
  } else {
    bool_ret = false;
    LOG_DBA_ERROR(OB_ERR_UNEXPECTED, "msg", "rename file failed, unexpected error",
                  KR(ret), K(errno), K(src_name), K(dest_name), K(src_exist), K(dest_exist));
  }
  return bool_ret;
}

bool check_renameat_success(const int src_dir_fd,
                            const char *src_name,
                            const int dest_dir_fd,
                            const char *dest_name)
{
  bool bool_ret = false;
  bool src_exist = false;
  bool dest_exist = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_file_exist(src_dir_fd, src_name, src_exist))) {
    PALF_LOG(WARN, "check_file_exist failed", KR(ret), K(src_name), K(dest_name));
  } else if (!src_exist && OB_FAIL(check_file_exist(dest_dir_fd, dest_name, dest_exist))) {
    PALF_LOG(WARN, "check_file_exist failed", KR(ret), K(src_name), K(dest_name));
  } else if (!src_exist && dest_exist) {
    bool_ret = true;
    PALF_LOG(INFO, "check_renameat_success return true",
             KR(ret), K(src_name), K(dest_name), K(src_dir_fd), K(dest_dir_fd), K(src_exist), K(dest_exist));
  } else {
    bool_ret = false;
    LOG_DBA_ERROR(OB_ERR_UNEXPECTED, "msg", "renameat file failed, unexpected error",
                  KR(ret), K(errno), K(src_name), K(dest_name), K(src_exist), K(dest_exist));
  }
  return bool_ret;
}

int rename_with_retry(const char *src_name,
                      const char *dest_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_name) || OB_ISNULL(dest_name)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KP(src_name), KP(dest_name));
  } else {
    do {
      if (-1 == ::rename(src_name, dest_name)) {
        ret  = convert_sys_errno();
        LOG_DBA_WARN(OB_IO_ERROR, "msg", "rename file failed",
                     KR(ret), K(errno), K(src_name), K(dest_name));
        // for xfs, source file not exist and dest file exist after rename return ENOSPC, therefore, next rename will return
        // OB_NO_SUCH_FILE_OR_DIRECTORY, however, for some reason, we can not return OB_SUCCESS when rename return OB_NO_SUCH_FILE_OR_DIRECTORY.
        // consider that, if file names with 'src_name' has been delted by human and file names with 'dest_name' not exist.
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret && check_rename_success(src_name, dest_name)) {
          ret = OB_SUCCESS;
          break;
        }
        ob_usleep(RETRY_INTERVAL);
      }
    } while(OB_FAIL(ret));
  }
  return ret;
}

int renameat_with_retry(const int src_dir_fd,
                        const char *src_name,
                        const int dest_dir_fd,
                        const char *dest_name)
{
  int ret = OB_SUCCESS;
  if (src_dir_fd < 0 || OB_ISNULL(src_name)
      || dest_dir_fd < 0 || OB_ISNULL(dest_name)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KP(src_name), KP(dest_name));
  } else {
    do {
      if (-1 == ::renameat(src_dir_fd, src_name, dest_dir_fd, dest_name)) {
        ret  = convert_sys_errno();
        LOG_DBA_WARN(OB_IO_ERROR, "msg", "renameat file failed",
                     KR(ret), K(errno), K(src_name), K(dest_name), K(src_dir_fd), K(dest_dir_fd));
        // for xfs, source file not exist and dest file exist after renameat return ENOSPC, therefore, next renameat will return
        // OB_NO_SUCH_FILE_OR_DIRECTORY, however, for some reason, we can not return OB_SUCCESS when renameat return OB_NO_SUCH_FILE_OR_DIRECTORY.
        // consider that, if file names with 'src_name' has been delted by human and file names with 'dest_name' not exist.
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret && check_renameat_success(src_dir_fd, src_name, dest_dir_fd, dest_name)) {
          ret = OB_SUCCESS;
          break;
        }
        ob_usleep(RETRY_INTERVAL);
      }
    } while(OB_FAIL(ret));
  }
  return ret;
}

int fsync_with_retry(const int dir_fd)
{
  int ret = OB_SUCCESS;
  do {
    if (-1 == ::fsync(dir_fd)) {
      ret = convert_sys_errno();
      CLOG_LOG(ERROR, "fsync dest dir failed", K(ret), K(dir_fd));
      ob_usleep(RETRY_INTERVAL);
    } else {
      ret = OB_SUCCESS;
      CLOG_LOG(TRACE, "fsync_until_success_ success", K(ret), K(dir_fd));
      break;
    }
  } while (OB_FAIL(ret));
  return ret;

}

int scan_dir(const char *dir_name, ObBaseDirFunctor &functor)
{
  int ret = OB_SUCCESS;
  DIR *open_dir = NULL;
  struct dirent *result = NULL;

  if (OB_ISNULL(dir_name)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(dir_name));
  } else if (OB_ISNULL(open_dir = ::opendir(dir_name))) {
    if (ENOENT != errno) {
      ret = OB_FILE_NOT_OPENED;
      PALF_LOG(WARN, "Fail to open dir, ", K(ret), K(dir_name));
    } else {
      ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
      PALF_LOG(WARN, "dir does not exist", K(ret), K(dir_name));
    }
  } else {
    while ((NULL != (result = ::readdir(open_dir))) && OB_SUCC(ret)) {
      if (0 != STRCMP(result->d_name, ".") && 0 != STRCMP(result->d_name, "..")
          && OB_FAIL((functor.func)(result))) {
        PALF_LOG(WARN, "fail to operate dir entry", K(ret), K(dir_name));
      }
    }
  }
  // close dir
  if (NULL != open_dir) {
    ::closedir(open_dir);
  }
  return ret;
}

int GetBlockCountFunctor::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid args", K(ret), KP(entry));
  } else {
    const char *entry_name = entry->d_name;
		// NB: if there is '0123' or 'xxx.flashback' in log directory,
		// restart will be failed, the solution is that read block.
    if (false == is_number(entry_name) && false == is_flashback_block(entry_name)) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(WARN, "this is block is not used for palf!!!", K(ret), K(entry_name));
      // do nothing, skip invalid block like tmp
    } else {
      count_ ++;
    }
  }
  return ret;
}

int TrimLogDirectoryFunctor::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid args", K(ret), KP(entry));
  } else {
    const char *entry_name = entry->d_name;
    bool str_is_number = is_number(entry_name);
    bool str_is_flashback_block = is_flashback_block(entry_name);
    if (false == str_is_number && false == str_is_flashback_block) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(WARN, "this is block is not used for palf!!!", K(ret), K(entry_name));
      // do nothing, skip invalid block like tmp
    } else {
      if (true == str_is_flashback_block
        && OB_FAIL(rename_flashback_to_normal_(entry_name))) {
        PALF_LOG(ERROR, "rename_flashback_to_normal failed", K(ret), K(dir_), K(entry_name));
      }
      if (OB_SUCC(ret)) {
        uint32_t block_id = static_cast<uint32_t>(strtol(entry->d_name, nullptr, 10));
        if (LOG_INVALID_BLOCK_ID == min_block_id_ || block_id < min_block_id_) {
          min_block_id_ = block_id;
        }
        if (LOG_INVALID_BLOCK_ID == max_block_id_ || block_id > max_block_id_) {
          max_block_id_ = block_id;
        }
      }
    }
  }
  return ret;
}

int TrimLogDirectoryFunctor::rename_flashback_to_normal_(const char *file_name)
{
  int ret = OB_SUCCESS;
  int dir_fd = -1;
  char normal_file_name[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  MEMCPY(normal_file_name, file_name, strlen(file_name) - strlen(FLASHBACK_SUFFIX));
  const int64_t SLEEP_TS_US = 10 * 1000;
  if (-1 == (dir_fd = ::open(dir_, O_DIRECTORY | O_RDONLY))) {
    ret = convert_sys_errno();
  } else if (OB_FAIL(try_to_remove_block_(dir_fd, normal_file_name))) {
    PALF_LOG(ERROR, "try_to_remove_block_ failed", K(file_name), K(normal_file_name));
  } else if (OB_FAIL(renameat_with_retry(dir_fd, file_name, dir_fd, normal_file_name))) {
    PALF_LOG(ERROR, "renameat_with_retry failed", K(file_name), K(normal_file_name));
  } else {}
  if (-1 != dir_fd) {
    ::close(dir_fd);
  }

  return ret;
}

int TrimLogDirectoryFunctor::try_to_remove_block_(const int dir_fd, const char *file_name)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  if (-1 == (fd = ::openat(dir_fd, file_name, LOG_READ_FLAG))) {
    ret = convert_sys_errno();
  }
  // if file not exist, return OB_SUCCESS;
  if (OB_FAIL(ret)) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      PALF_LOG(INFO, "before rename flashback to normal and after delete normal file, restart!!!", K(file_name));
    } else {
      PALF_LOG(ERROR, "open file failed", K(file_name));
    }
  } else if (OB_FAIL(log_block_pool_->remove_block_at(dir_fd, file_name))) {
    PALF_LOG(ERROR, "remove_block_at failed", K(dir_fd), K(file_name));
  }
  if (-1 != fd && -1 == ::close(fd)) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "close fd failed", K(file_name));
  }
  return ret;
}

int reuse_block_at(const int dir_fd, const char *block_path)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  if (-1 == (fd = ::openat(dir_fd, block_path, LOG_WRITE_FLAG))) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "::openat failed", K(ret), K(block_path));
  } else if (-1 == ::fallocate(fd, FALLOC_FL_ZERO_RANGE, 0, PALF_PHY_BLOCK_SIZE)) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "::fallocate failed", K(ret), K(block_path));
  } else {
    PALF_LOG(INFO, "reuse_block_at success", K(ret), K(block_path));
  }

  if (-1 != fd) {
    ::close(fd);
  }
  return ret;
}

const char *ObFallocateProbeParam::get_fallocate_mode_name(const int mode)
{
  const char *name = nullptr;
  switch (mode) {
    case 0:
      name = "mode=0";
      break;
    case FALLOC_FL_ZERO_RANGE:
      name = "FALLOC_FL_ZERO_RANGE";
      break;
    default:
      name = nullptr;
      break;
  }
  return name;
}

// The capability verdict is independent of the allocation length and there
// is no length-dependent limit below the physical block size: unsupported
// filesystem/mode combinations fail in vfs_fallocate before any allocation
// happens, and ENOSPC is downgraded to a skip by the caller. One DIO-aligned
// unit is therefore enough and keeps the transient space reservation of the
// mode=0 probe minimal.
static constexpr int64_t FALLOCATE_PROBE_SIZE = LOG_DIO_ALIGN_SIZE;

ObFallocateProbeParam ObFallocateProbeParam::get_clog_param(const char *directory)
{
  return ObFallocateProbeParam(
      "clog",
      directory,
      FALLOCATE_PROBE_SIZE,
      FALLOC_FL_ZERO_RANGE);
}

#ifdef OB_BUILD_ARBITRATION
ObFallocateProbeParam ObFallocateProbeParam::get_arbitration_param(const char *directory)
{
  return ObFallocateProbeParam(
      "arbitration_clog",
      directory,
      FALLOCATE_PROBE_SIZE);
}
#endif

int posix_probe_open(const char *dir, int &fd)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dir)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid dir for probe open", KR(ret));
#ifndef O_TMPFILE
  } else {
    // glibc < 2.19 (e.g. CentOS 7 / Alinux 2.1903 with glibc 2.17) does not
    // define O_TMPFILE; report unsupported so the caller falls back to the
    // named-file compat probe.
    UNUSED(fd);
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "O_TMPFILE is undefined, anonymous probe file is unavailable",
        KR(ret), K(dir));
  }
#else
  } else if (0 > (fd = ::open(dir, O_TMPFILE | LOG_WRITE_FLAG, 0600))) {
    const int sys_errno = errno;
    if (EOPNOTSUPP == sys_errno || ENOTSUP == sys_errno
        || ENOSYS == sys_errno || EINVAL == sys_errno
        // Kernels < 3.11 reject O_TMPFILE with EISDIR because O_TMPFILE
        // implies O_DIRECTORY.
        || EISDIR == sys_errno) {
      ret = OB_NOT_SUPPORTED;
    } else {
      ret = palf::convert_sys_errno();
    }
    PALF_LOG(WARN, "failed to open probe file",
        KR(ret), K(dir), K(sys_errno), KERRNOMSG(sys_errno));
  }
#endif
  return ret;
}

int posix_probe_fallocate(const int fd,
                          const int mode,
                          const int64_t offset,
                          const int64_t len)
{
  int ret = OB_SUCCESS;
  if (0 > fd || 0 > offset || 0 > len) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument for probe fallocate",
        KR(ret), K(fd), K(mode), K(offset), K(len));
  } else if (0 != ::fallocate(fd, mode, static_cast<off_t>(offset), static_cast<off_t>(len))) {
    const int sys_errno = errno;
    if (EOPNOTSUPP == sys_errno || ENOTSUP == sys_errno
        || ENOSYS == sys_errno || EINVAL == sys_errno) {
      ret = OB_NOT_SUPPORTED;
    } else {
      ret = palf::convert_sys_errno();
    }
    PALF_LOG(WARN, "failed to fallocate probe file",
        KR(ret), K(fd), K(mode), K(offset), K(len), K(sys_errno), KERRNOMSG(sys_errno));
  }
  return ret;
}

int posix_probe_close(const int fd)
{
  int ret = OB_SUCCESS;
  if (0 > fd) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid fd for probe close", KR(ret), K(fd));
  } else if (0 != ::close(fd)) {
    const int sys_errno = errno;
    ret = palf::convert_sys_errno();
    PALF_LOG(WARN, "failed to close probe file",
        KR(ret), K(fd), K(sys_errno), KERRNOMSG(sys_errno));
  }
  return ret;
}

// Compat fallback ops for filesystems without O_TMPFILE support (e.g. NFS):
// probe with an exclusively created named file instead. LOG_WRITE_FLAG keeps
// the O_DIRECT/O_SYNC coverage of the runtime log path in the fallback.
int posix_probe_open_compat(const char *path, int &fd)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid path for compat probe open", KR(ret));
  } else if (0 > (fd = ::open(path, O_CREAT | O_EXCL | LOG_WRITE_FLAG, 0600))) {
    const int sys_errno = errno;
    if (EOPNOTSUPP == sys_errno || ENOTSUP == sys_errno
        || ENOSYS == sys_errno || EINVAL == sys_errno) {
      ret = OB_NOT_SUPPORTED;
    } else {
      ret = palf::convert_sys_errno();
    }
    PALF_LOG(WARN, "failed to open compat probe file",
        KR(ret), K(path), K(sys_errno), KERRNOMSG(sys_errno));
  }
  return ret;
}

// ENOENT is reported as OB_NO_SUCH_FILE_OR_DIRECTORY without WARN and the
// caller treats it as "no stale probe file" success.
int posix_probe_unlink(const char *path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid path for probe unlink", KR(ret));
  } else if (0 != ::unlink(path)) {
    const int sys_errno = errno;
    ret = palf::convert_sys_errno();
    if (ENOENT != sys_errno) {
      PALF_LOG(WARN, "failed to unlink probe file",
          KR(ret), K(path), K(sys_errno), KERRNOMSG(sys_errno));
    }
  }
  return ret;
}

const ObFallocateProbeOps POSIX_FALLOCATE_PROBE_OPS(
    posix_probe_open,
    posix_probe_fallocate,
    posix_probe_close,
    posix_probe_open_compat,
    posix_probe_unlink);

const ObFallocateProbeOps & __attribute__((weak)) get_default_fallocate_probe_ops()
{
  return POSIX_FALLOCATE_PROBE_OPS;
}

// Probe required fallocate modes on an opened fd, then close it whatever the
// probe result is. Disk space exhaustion degrades to ERROR and skips the probe;
// the first error is never masked by a later cleanup error.
static int do_probe_fallocate_and_close_(
    const ObFallocateProbeParam &param,
    const ObFallocateProbeOps &ops,
    const int fd,
    const char *probe_file)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_FAIL(ops.fallocate_(fd, 0, 0, param.get_probe_size()))) {
    PALF_LOG(ERROR, "file system fallocate capability probe failed", KR(ret), K(param));
  } else if (0 != param.get_extra_mode()
      && OB_FAIL(ops.fallocate_(fd, param.get_extra_mode(), 0, param.get_probe_size()))) {
    PALF_LOG(ERROR, "file system fallocate capability probe failed", KR(ret), K(param));
  }
  if (OB_TMP_FAIL(ops.close_(fd))) {
    PALF_LOG(ERROR, "failed to close fallocate capability probe file", KR(tmp_ret), K(param));
    ret = (OB_SUCCESS == ret && OB_NO_SUCH_FILE_OR_DIRECTORY != tmp_ret) ? tmp_ret : ret;
  }
  if (nullptr != probe_file && OB_TMP_FAIL(ops.unlink_(probe_file))) {
    PALF_LOG(ERROR, "failed to unlink fallocate capability probe file",
        KR(tmp_ret), K(param), K(probe_file));
    ret = (OB_SUCCESS == ret && OB_NO_SUCH_FILE_OR_DIRECTORY != tmp_ret) ? tmp_ret : ret;
  }
  return ret;
}

// Compat fallback for filesystems without O_TMPFILE support (e.g. NFS):
// probe with a named temporary file. The stale file must be unlinked first
// because this probe runs earlier than the log directory scans of
// ObServerLogBlockMgr / PalfEnvLiteMgr, and O_EXCL creation would fail on a
// leftover file of a previously crashed probe.
static int probe_fallocate_capability_with_compat_ops_(
    const ObFallocateProbeParam &param,
    const ObFallocateProbeOps &ops)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int fd = -1;
  char probe_file[common::MAX_PATH_SIZE] = {'\0'};

  if (!ops.is_compat_valid()) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(ERROR, "compat fallocate capability probe ops is not available",
        KR(ret), K(param));
  } else if (OB_FAIL(param.get_probe_file_path(probe_file, sizeof(probe_file)))) {
    PALF_LOG(ERROR, "failed to build fallocate capability probe path",
        KR(ret), K(param), "path_buffer_size", sizeof(probe_file));
  } else if (OB_TMP_FAIL(
    ops.unlink_(probe_file)) && OB_NO_SUCH_FILE_OR_DIRECTORY != tmp_ret) {
    // A missing stale file is the normal case on a clean directory: the
    // ENOENT tolerance must fall through to the exclusive open below instead
    // of consuming the branch chain, otherwise the compat probe never runs.
    ret = tmp_ret;
    PALF_LOG(ERROR, "failed to clean up stale fallocate capability probe file",
        KR(ret), K(param), K(probe_file));
  } else if (OB_FAIL(ops.open_compat_(probe_file, fd))) {
    PALF_LOG(ERROR, "failed to create fallocate capability probe file",
        KR(ret), K(param), K(probe_file));
  } else {
    ret = do_probe_fallocate_and_close_(param, ops, fd, probe_file);
  }
  return ret;
}

int probe_fallocate_capability_with_ops(
    const ObFallocateProbeParam &param,
    const ObFallocateProbeOps &ops)
{
  int ret = OB_SUCCESS;
  int fd = -1;

  if (!param.is_valid() || !ops.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid fallocate capability probe argument", KR(ret), K(param));
  } else if (OB_FAIL(ops.open_(param.get_probe_file_dir(), fd))) {
    if (OB_NOT_SUPPORTED == ret) {
      // Anonymous temporary files (O_TMPFILE) are unavailable on this file
      // system, which does not affect fallocate itself; fall back to the
      // named-file probe.
      PALF_LOG(WARN, "anonymous tmp file is not supported, fall back to named probe file",
          KR(ret), K(param));
      ret = probe_fallocate_capability_with_compat_ops_(param, ops);
    } else {
      PALF_LOG(ERROR, "failed to create fallocate capability probe file",
          KR(ret), K(param));
    }
  } else {
    ret = do_probe_fallocate_and_close_(param, ops, fd, nullptr);
  }
  if (OB_ALLOCATE_DISK_SPACE_FAILED == ret) {
    PALF_LOG(ERROR, "disk space not enough during fallocate probe, skip", KR(ret), K(param));
    ret = OB_SUCCESS;
  }
  return ret;
}

int check_file_system_fallocate_capability(
    const char *clog_dir)
{
  int ret = OB_SUCCESS;
  const ObFallocateProbeOps &ops = get_default_fallocate_probe_ops();
  if (!ops.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid fallocate capability probe operations", KR(ret));
  } else if (OB_FAIL(probe_fallocate_capability_with_ops(
      ObFallocateProbeParam::get_clog_param(clog_dir), ops))) {
    PALF_LOG(ERROR, "clog fallocate capability probe failed", KR(ret), K(clog_dir));
  } else {
    PALF_LOG(INFO, "clog fallocate capability probe success", KR(ret), K(clog_dir));
  }
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
int check_arbitration_file_system_fallocate_capability(const char *clog_dir)
{
  int ret = OB_SUCCESS;
  const ObFallocateProbeOps &ops = get_default_fallocate_probe_ops();
  if (!ops.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid fallocate capability probe operations", KR(ret));
  } else if (OB_FAIL(probe_fallocate_capability_with_ops(
      // PalfHandleLite uses a 2 MiB meta block and DummyBlockPool allocates it with mode=0.
      ObFallocateProbeParam::get_arbitration_param(clog_dir), ops))) {
    PALF_LOG(ERROR, "arbitration clog fallocate capability probe failed", KR(ret), K(clog_dir));
  } else {
    PALF_LOG(INFO, "arbitration clog fallocate capability probe success", KR(ret), K(clog_dir));
  }
  return ret;
}
#endif

} // end namespace palf
} // end namespace oceanbase
