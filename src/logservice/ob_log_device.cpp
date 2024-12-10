/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_log_device.h"
#include <sys/vfs.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <linux/falloc.h>
#include "share/ob_errno.h"                      // errno
#include "lib/utility/ob_utility.h"              // ob_pread
#include "share/ob_define.h"
#include "logservice/palf/log_io_utils.h"
#include "logservice/ob_log_io_adapter.h"

using namespace newlogstorepb;
using namespace oceanbase::common;

namespace oceanbase {
namespace logservice {
// ===================== log store env =======================
int init_log_store_env()
{
  return OB_SUCCESS;
}

void fin_log_store_env()
{}
// ===========================================================

ObLogDevice::ObLogDevice()
  : lock_(),
    version_(LOG_COMPATIBLE_VERSION),
    epoch_(INITIAL_LOG_STORE_EPOCH),
    pwrite_seq_(0),
    grpc_adapter_(),
    ref_(0),
    is_reloading_(false),
    is_running_(false),
    is_inited_(false)
{
}

ObLogDevice::~ObLogDevice()
{
  destroy();
}

int ObLogDevice::init(const common::ObIODOpts &opts)
{
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObLogDevice::init(const ObAddr &addr,
                      const int64_t cluster_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "The log device has been inited, ", K(ret));
  } else if (OB_FAIL(grpc_adapter_.init(addr, cluster_idx))) {
    CLOG_LOG(WARN, "fail to init grpc adapter", K(ret));
  } else {
    pwrite_seq_ = 0;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObLogDevice init success", K(addr));
  }

  return ret;
}

int ObLogDevice::reconfig(const common::ObIODOpts &opts)
{
  UNUSED(opts);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::get_config(ObIODOpts &opts)
{
  UNUSED(opts);
  return OB_NOT_SUPPORTED;
}

// TODO by runlin shake
int ObLogDevice::start(const common::ObIODOpts &opts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "The ObLogDevice has not been inited, ", K(ret));
  } else if (OB_FAIL(reload_epoch_())) {
    CLOG_LOG(WARN, "fail to reload epoch", K(ret));
  } else {
    is_running_ = true;
    CLOG_LOG(INFO, "ObLogDevice start running");
  }
  return ret;
}

void ObLogDevice::destroy()
{
  assert(ref_ == 0);
  pwrite_seq_ = 0;
  grpc_adapter_.destroy();
  epoch_ = INITIAL_LOG_STORE_EPOCH;
  version_ = LOG_COMPATIBLE_VERSION;
  is_reloading_ = false;
  is_running_ = false;
  is_inited_ = false;
  CLOG_LOG(INFO, "destroy ObLogDevice success");
}

//file/dir interfaces
int ObLogDevice::open(const char *pathname,
                      const int flags,
                      const mode_t mode,
                      ObIOFd &fd,
                      common::ObIODOpts *opts)
{
  UNUSED(opts);
  int ret = OB_SUCCESS;
  int local_fd = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogDevice has not been started", K(ret), K_(is_running));
  } else if (OB_ISNULL(pathname)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "Invalid pathname, ", K(ret), KP(pathname));
  } else {

    OpenReq req;
    OpenResp resp;
    req.set_epoch(ATOMIC_LOAD(&epoch_));
    req.set_flags(flags);
    req.set_mode(mode);
    req.set_pathname(pathname);

    do {
      if (OB_FAIL(grpc_adapter_.open(req, resp))) {
        CLOG_LOG(ERROR, "grpc call failed for Open", K(ret));
      } else if (FALSE_IT(ret = get_resp_ret_code(resp))) {
      } else if (OB_SUCC(ret)) {
        fd.first_id_ = ObIOFd::NORMAL_FILE_ID;
        fd.second_id_ = resp.fd();
        const int64_t ref = ATOMIC_AAF(&ref_, 1);
        CLOG_LOG(INFO, "ObLogDevice open successfully", K(fd), K(ref));
      } else if (OB_STATE_NOT_MATCH == ret) {
        CLOG_LOG(WARN, "epoch changed, need to retry", K(ret));
        deal_with_epoch_changed(req, resp);
      } else {
        CLOG_LOG(WARN, "some errors happen in logstore, maybe need to retry", K(ret));
      }
    } while (OB_EAGAIN == ret);
  }
  return ret;
}

int ObLogDevice::complete(const ObIOFd &fd)
{
  UNUSED(fd);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::abort(const ObIOFd &fd)
{
  UNUSED(fd);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::close(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  int64_t ref = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogDevice has not been started", K(ret), K_(is_running));
  } else if (!fd.is_valid() || OB_UNLIKELY(fd.is_block_file())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "The block file does not need close, ", K(ret), K(fd));
  } else if (0 == (ref = ATOMIC_LOAD(&ref_))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "all opens has been closed for current epoch, try to close the fd twice", K(ret), K(fd), K(ref));
  } else {
    CloseReq req;
    CloseResp resp;
    req.set_epoch(ATOMIC_LOAD(&epoch_));
    req.set_fd(fd.second_id_);

    // close don't return eagain
    do {
      if (OB_FAIL(grpc_adapter_.close(req, resp))) {
        CLOG_LOG(ERROR, "grpc call failed for close", K(ret));
      } else if (FALSE_IT(ret = get_resp_ret_code(resp))) {
      } else if (OB_SUCCESS == ret || OB_STATE_NOT_MATCH == ret) {
        ret = OB_SUCCESS;
        const int64_t ref = ATOMIC_AAF(&ref_, -1);
        if (ref == 0) {
          // close don't reload epoch, if ref = 0 & is_reloading = false, the next request which returns 4109 will reload epoch
          CLOG_LOG(INFO, "ref becomes 0, enable reload epoch", K(ref), K(fd));
        } else {
          CLOG_LOG(INFO, "ObLogDevice close successfully", K(ret), K(ref), K(fd));
        }
      } else {
        CLOG_LOG(WARN, "some errors happen in logstore, maybe need to retry", K(ret));
      }
    } while (OB_EAGAIN == ret);

  }
  return ret;
}

int ObLogDevice::mkdir(const char *pathname, mode_t mode)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogDevice has not been started", K(ret), K_(is_running));
  } else if (OB_ISNULL(pathname) || OB_UNLIKELY(0 == STRLEN(pathname))) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments.", K(pathname), K(ret));
  } else if (true == get_reloading_state_()) {
    ret = OB_STATE_NOT_MATCH;
  } else {

    MkdirReq req;
    MkdirResp resp;
    req.set_epoch(ATOMIC_LOAD(&epoch_));
    req.set_pathname(pathname);
    req.set_mode(mode);

    do {
      if (OB_FAIL(grpc_adapter_.mkdir(req, resp))) {
        CLOG_LOG(ERROR, "grpc call failed for mkdir", K(ret));
      } else if (FALSE_IT(ret = get_resp_ret_code(resp))) {
      } else if (OB_SUCC(ret)) {
        CLOG_LOG(TRACE, "ObLogDevice mkdir successfully", K(pathname), K(mode));
      } else if (OB_STATE_NOT_MATCH == ret) {
        CLOG_LOG(WARN, "epoch changed, need to retry", K(ret));
        deal_with_epoch_changed(req, resp);
      } else {
        CLOG_LOG(WARN, "some errors happen in logstore, maybe need to retry", K(ret));
      }
    } while (OB_EAGAIN == ret);
  }

  return ret;
}

// TODO by nianguan:distinguish 'rm' from 'rm -r'
int ObLogDevice::rmdir(const char *pathname)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogDevice has not been started", K(ret), K_(is_running));
  } else if (OB_ISNULL(pathname) || OB_UNLIKELY(0 == STRLEN(pathname))) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments.", K(pathname), K(ret));
  } else if (true == get_reloading_state_()) {
    ret = OB_STATE_NOT_MATCH;
  } else {

    RmdirReq req;
    RmdirResp resp;
    req.set_epoch(ATOMIC_LOAD(&epoch_));
    req.set_pathname(pathname);

    do {
      if (OB_FAIL(grpc_adapter_.rmdir(req, resp))) {
        CLOG_LOG(ERROR, "grpc call failed for rmdir", K(ret));
      } else if (FALSE_IT(ret = get_resp_ret_code(resp))) {
      } else if (OB_SUCC(ret)) {
        CLOG_LOG(TRACE, "ObLogDevice rmdir successfully", K(pathname));
      } else if (OB_STATE_NOT_MATCH == ret) {
        CLOG_LOG(WARN, "epoch changed, need to retry", K(ret));
        deal_with_epoch_changed(req, resp);
      } else {
        CLOG_LOG(WARN, "some errors happen in logstore, maybe need to retry", K(ret));
      }
    } while (OB_EAGAIN == ret);
  }

  return ret;
}

int ObLogDevice::unlink(const char *pathname)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogDevice has not been started", K(ret), K_(is_running));
  } else if (OB_ISNULL(pathname) || OB_UNLIKELY(0 == STRLEN(pathname))) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments.", K(pathname), K(ret));
  } else if (true == get_reloading_state_()) {
    ret = OB_STATE_NOT_MATCH;
  } else {

    UnlinkReq req;
    UnlinkResp resp;
    req.set_epoch(ATOMIC_LOAD(&epoch_));
    req.set_pathname(pathname);

    do {
      if (OB_FAIL(grpc_adapter_.unlink(req, resp))) {
        CLOG_LOG(ERROR, "grpc call failed for unlink", K(ret));
      } else if (FALSE_IT(ret = get_resp_ret_code(resp))) {
      } else if (OB_SUCC(ret)) {
        CLOG_LOG(INFO, "ObLogDevice unlink successfully", K(pathname));
      } else if (OB_STATE_NOT_MATCH == ret) {
        CLOG_LOG(WARN, "epoch changed, need to retry", K(ret));
        deal_with_epoch_changed(req, resp);
      } else {
        CLOG_LOG(WARN, "some errors happen in logstore, maybe need to retry", K(ret));
      }
    } while (OB_EAGAIN == ret);
  }
  return ret;
}

int ObLogDevice::rename(const char *oldpath, const char *newpath)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogDevice has not been started", K(ret), K_(is_running));
  } else if (OB_ISNULL(oldpath) || OB_ISNULL(newpath)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "Invalid argument, ", K(ret), KP(oldpath), KP(newpath));
  } else if (true == get_reloading_state_()) {
    ret = OB_STATE_NOT_MATCH;
  } else {

    RenameReq req;
    RenameResp resp;
    req.set_epoch(ATOMIC_LOAD(&epoch_));
    req.set_oldpath(oldpath);
    req.set_newpath(newpath);

    do {
      if (OB_FAIL(grpc_adapter_.rename(req, resp))) {
        CLOG_LOG(ERROR, "grpc call failed for mkdir", K(ret));
      } else if (FALSE_IT(ret = get_resp_ret_code(resp))) {
      } else if (OB_SUCC(ret)) {
        CLOG_LOG(TRACE, "ObLogDevice rename successfully", K(oldpath), K(newpath));
      } else if (OB_STATE_NOT_MATCH == ret) {
        CLOG_LOG(WARN, "epoch changed, need to retry", K(ret));
        deal_with_epoch_changed(req, resp);
      } else {
        CLOG_LOG(WARN, "some errors happen in logstore, maybe need to retry", K(ret));
      }
    } while (OB_EAGAIN == ret);
  }
  return ret;
}

int ObLogDevice::seal_file(const ObIOFd &fd)
{
  UNUSED(fd);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::scan_dir(const char *dir_name, int (*func)(const dirent *entry))
{
  UNUSED(dir_name);
  UNUSED(func);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::is_tagging(const char *pathname, bool &is_tagging)
{
  UNUSED(pathname);
  UNUSED(is_tagging);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::scan_dir(const char *dir_name, common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  DIR *open_dir = nullptr;
  struct dirent entry;
  struct dirent *result;

  if (OB_ISNULL(dir_name)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(dir_name));
  } else if (OB_ISNULL(open_dir = ::opendir(dir_name))) {
    if (ENOENT != errno) {
      ret = OB_FILE_NOT_OPENED;
      CLOG_LOG(WARN, "Fail to open dir, ", K(ret), K(dir_name));
    } else {
      ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
      CLOG_LOG(WARN, "dir does not exist", K(ret), K(dir_name));
    }
  } else {
    while (OB_SUCC(ret) && NULL != open_dir) {
      if (0 != ::readdir_r(open_dir, &entry, &result)) {
        ret = convert_sys_errno(errno);
        CLOG_LOG(WARN, "read dir error", K(ret), KERRMSG);
      } else if (NULL != result
          && 0 != STRCMP(entry.d_name, ".")
          && 0 != STRCMP(entry.d_name, "..")) {
        if (OB_FAIL(op.func(&entry))) {
          CLOG_LOG(WARN, "fail to operate dir entry", K(ret), K(dir_name));
        }
      } else if (NULL == result) {
        break; //end file
      }
    }
    //close dir
    if (NULL != open_dir) {
      ::closedir(open_dir);
    }
  }
  return ret;
}

int ObLogDevice::fsync(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  bool is_reloading = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogDevice has not been started", K(ret), K_(is_running));
  } else if (!fd.is_valid() || OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid fd, not normal file, ", K(ret), K(fd));
  } else if (true == get_reloading_state_()) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    FsyncReq req;
    FsyncResp resp;
    req.set_epoch(ATOMIC_LOAD(&epoch_));
    req.set_fd(fd.second_id_);

    do {
      if (OB_FAIL(grpc_adapter_.fsync(req, resp))) {
        CLOG_LOG(ERROR, "grpc call failed for Fsync", K(ret));
      } else if (FALSE_IT(ret = get_resp_ret_code(resp))) {
      } else if (OB_SUCC(ret)) {
        CLOG_LOG(TRACE, "ObLogDevice fsync successfully");
      } else if (OB_STATE_NOT_MATCH == ret) {
        CLOG_LOG(WARN, "epoch changed, maybe logstore process restart. Current fd should be closed", K(ret), K(fd));
      } else {
        CLOG_LOG(WARN, "some errors happen in logstore, maybe need to retry", K(ret));
      }
    } while (OB_EAGAIN == ret);
  }
  return ret;
}

int ObLogDevice::fdatasync(const ObIOFd &fd)
{
  UNUSED(fd);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::fallocate(const ObIOFd &fd,
                           mode_t mode,
                           const int64_t offset,
                           const int64_t len)
{
  int ret = OB_SUCCESS;
  bool is_reloading = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogDevice has not been started", K(ret), K_(is_running));
  } else if (!fd.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(fd));
  } else if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid args, not normal file", K(ret), K(fd));
  } else if (true == get_reloading_state_()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "epoch changed, should close current fd", K(is_reloading), K(fd));
  } else {

    FallocateReq req;
    FallocateResp resp;
    req.set_epoch(ATOMIC_LOAD(&epoch_));
    req.set_fd(fd.second_id_);
    req.set_mode(mode);
    req.set_offset(offset);
    req.set_len(len);

    do {
      if (OB_FAIL(grpc_adapter_.fallocate(req, resp))) {
        CLOG_LOG(ERROR, "grpc call failed for fallocate", K(ret));
      } else if (FALSE_IT(ret = get_resp_ret_code(resp))) {
      } else if (OB_SUCC(ret)) {
        CLOG_LOG(TRACE, "ObLogDevice fallocate successfully");
      } else if (OB_STATE_NOT_MATCH == ret) {
        CLOG_LOG(WARN, "epoch changed, maybe logstore process restart. Current fd should be closed", K(ret), K(fd));
      } else {
        CLOG_LOG(WARN, "some errors happen in logstore, maybe need to retry", K(ret));
      }
    } while (OB_EAGAIN == ret);
  }
  return ret;
}

int ObLogDevice::lseek(const ObIOFd &fd,
                       const int64_t offset,
                       const int whence,
                       int64_t &result_offset)
{
  UNUSED(fd);
  UNUSED(offset);
  UNUSED(whence);
  UNUSED(result_offset);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::truncate(const char *pathname, const int64_t len)
{
  UNUSED(pathname);
  UNUSED(len);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::ftruncate(const ObIOFd &fd, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogDevice has not been started", K(ret), K_(is_running));
  } else if (!fd.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(fd));
  } else if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid args, not normal file", K(ret), K(fd));
  } else if (true == get_reloading_state_()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "epoch changed, should close current fd", K(fd));
  } else {
    FtruncateReq req;
    FtruncateResp resp;
    req.set_epoch(ATOMIC_LOAD(&epoch_));
    req.set_fd(fd.second_id_);
    req.set_len(len);

    do {
      if (OB_FAIL(grpc_adapter_.ftruncate(req, resp))) {
        CLOG_LOG(ERROR, "grpc call failed for ftruncate", K(ret));
      } else if (FALSE_IT(ret = get_resp_ret_code(resp))) {
      } else if (OB_SUCC(ret)) {
        CLOG_LOG(TRACE, "ObLogDevice ftruncate successfully");
      } else if (OB_STATE_NOT_MATCH == ret) {
        CLOG_LOG(WARN, "epoch changed, maybe logstore process restart. Current fd should be closed", K(ret), K(fd));
      } else {
        CLOG_LOG(WARN, "some errors happen in logstore, maybe need to retry", K(ret));
      }
    } while (OB_EAGAIN == ret);
  }

  return ret;
}

int ObLogDevice::exist(const char *pathname, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogDevice has not been started", K(ret), K_(is_running));
  } else if (OB_ISNULL(pathname) || OB_UNLIKELY(0 == STRLEN(pathname))) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments.", K(pathname), K(ret));
  } else if (true == get_reloading_state_()) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    //wait_if_epoch_changing();
    StatReq req;
    StatResp resp;
    req.set_epoch(ATOMIC_LOAD(&epoch_));
    req.set_pathname(pathname);

    do {
      if (OB_FAIL(grpc_adapter_.stat(req, resp))) {
        CLOG_LOG(ERROR, "grpc call failed for stat", K(ret));
      } else if (FALSE_IT(ret = get_resp_ret_code(resp))) {
      } else if (OB_SUCC(ret)) {
        mode_t mode = 0;
        if (resp.is_dir()) {
          mode |= S_IFDIR;
        }
        CLOG_LOG(TRACE, "ObLogDevice stat successfully", K(mode), K(S_ISDIR(mode)));
      } else if (OB_STATE_NOT_MATCH == ret) {
        CLOG_LOG(WARN, "epoch changed, need to retry", K(ret));
        deal_with_epoch_changed(req, resp);
        //ret = OB_EAGAIN;
      } else if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
      }
    } while (OB_EAGAIN == ret);

    if (OB_SUCC(ret)) {
      is_exist = true;
    }
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      is_exist = false;
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObLogDevice::stat(const char *pathname, ObIODFileStat &statbuf)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogDevice has not been started", K(ret), K_(is_running));
  } else if (OB_ISNULL(pathname) || OB_UNLIKELY(0 == STRLEN(pathname))) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments.", K(pathname), K(ret));
  } else if (true == get_reloading_state_()) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    //wait_if_epoch_changing();
    StatReq req;
    StatResp resp;
    req.set_epoch(ATOMIC_LOAD(&epoch_));
    req.set_pathname(pathname);

    do {
      if (OB_FAIL(grpc_adapter_.stat(req, resp))) {
        CLOG_LOG(ERROR, "grpc call failed for stat", K(ret));
      } else if (FALSE_IT(ret = get_resp_ret_code(resp))) {
      } else if (OB_SUCC(ret)) {
        statbuf.size_ = resp.st_size();
        mode_t mode = 0;
        if (resp.is_dir()) {
          mode |= S_IFDIR;
        }
        statbuf.mode_ = mode;
        CLOG_LOG(TRACE, "ObLogDevice stat successfully", K(mode), K(S_ISDIR(mode)));
      } else if (OB_STATE_NOT_MATCH == ret) {
        CLOG_LOG(WARN, "epoch changed, need to retry", K(ret));
        deal_with_epoch_changed(req, resp);
        //ret = OB_EAGAIN;
      } else if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
        CLOG_LOG(WARN, "some errors happen in logstore, maybe need to retry", K(ret));
      }
    } while (OB_EAGAIN == ret);
  }

  return ret;
}

int ObLogDevice::fstat(const ObIOFd &fd, ObIODFileStat &statbuf)
{
  int ret = OB_SUCCESS;
  // if (IS_NOT_INIT) {
  //   ret = OB_NOT_INIT;
  // } else if (!is_running_) {
  //   ret = OB_STATE_NOT_MATCH;
  //   CLOG_LOG(WARN, "ObLogDevice has not been started", K(ret), K_(is_running));
  // } else if (!fd.is_valid()) {
  //   ret = OB_INVALID_ARGUMENT;
  //   CLOG_LOG(WARN, "invalid arguments.", K(fd), K(ret));
  // } else {
  //   FstatReq req;
  //   StatResp resp;
  //   req.set_epoch(epoch_);
  //   req.set_fd(fd.second_id_);

  //   if (OB_FAIL(grpc_adapter_.fstat(req, resp))) {
  //     CLOG_LOG(ERROR, "grpc call failed for fstat", K(ret));
  //   } else if (true == is_epoch_changed(resp)) {
  //     ret = OB_STATE_NOT_MATCH;
  //     CLOG_LOG(WARN, "epoch changed, maybe logstore process restart", K(ret));
  //   } else if (true == has_sys_errno(resp)) {
  //     ret = convert_sys_errno(resp.err_no());
  //     CLOG_LOG(WARN, "logstore encounters erros", K(ret), K(resp.err_no()));
  //   } else {
  //     CLOG_LOG(TRACE, "ObLogDevice fstat successfully");
  //   }
  // }

  return ret;
}

int ObLogDevice::del_unmerged_parts(const char *pathname)
{
  UNUSED(pathname);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::adaptive_exist(const char *pathname, bool &is_exist)
{
  UNUSED(pathname);
  UNUSED(is_exist);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::adaptive_stat(const char *pathname, ObIODFileStat &statbuf)
{
  UNUSED(pathname);
  UNUSED(statbuf);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::adaptive_unlink(const char *pathname)
{
  UNUSED(pathname);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::adaptive_scan_dir(const char *dir_name, ObBaseDirEntryOperator &op)
{
  UNUSED(dir_name);
  UNUSED(op);
  return OB_NOT_SUPPORTED;
}

//block interfaces
int ObLogDevice::mark_blocks(ObIBlockIterator &block_iter)
{
  UNUSED(block_iter);
  return OB_NOT_SUPPORTED;
}


int ObLogDevice::alloc_block(const common::ObIODOpts *opts, ObIOFd &block_id)
{
  UNUSED(opts);
  UNUSED(block_id);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::alloc_blocks(
    const common::ObIODOpts *opts,
    const int64_t count,
    common::ObIArray<ObIOFd> &blocks)
{
  UNUSED(opts);
  UNUSED(count);
  UNUSED(blocks);
  return OB_NOT_SUPPORTED;
}

void ObLogDevice::free_block(const ObIOFd &block_id)
{
  UNUSED(block_id);
}

int ObLogDevice::fsync_block()
{
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::get_restart_sequence(uint32_t &restart_id) const
{
  UNUSED(restart_id);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::mark_blocks(const common::ObIArray<ObIOFd> &blocks)
{
  UNUSED(blocks);
  return OB_NOT_SUPPORTED;
}

//sync io interfaces
int ObLogDevice::pread(
    const ObIOFd &fd,
    const int64_t offset,
    const int64_t size,
    void *buf,
    int64_t &read_size,
    common::ObIODPreadChecker *checker)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "The ObLogDevice has not been inited, ", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogDevice has not been started", K(ret), K_(is_running));
  } else if (!fd.is_valid() || 0 > offset || 0 > size || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(offset), K(size), KP(buf));
  } else if (!fd.is_normal_file()) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(ERROR, "It's not normal file, not suppor", K(ret));
  } else if (true == get_reloading_state_()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "epoch changed, should close current fd", K(fd));
  } else if (0 == size) {
    CLOG_LOG(TRACE, "size is equal to 0, return directly", K(fd), K(size));
  } else {
    PreadReq req;
    PreadResp resp;
    req.set_epoch(ATOMIC_LOAD(&epoch_));
    req.set_fd(fd.second_id_);
    req.set_size(size);
    req.set_offset(offset);

    do {
      if (OB_FAIL(grpc_adapter_.pread(req, resp))) {
        CLOG_LOG(ERROR, "grpc call failed for pread", K(ret));
      } else if (FALSE_IT(ret = get_resp_ret_code(resp))) {
      } else if (OB_SUCC(ret)) {
        if (size != resp.size()) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "partital read", K(ret), K(size));
        } else {
          MEMCPY(buf, resp.buf().c_str(), resp.size());
          read_size = resp.size();
          CLOG_LOG(TRACE, "ObLogDevice pread successfully", K(fd), K(offset), K(size), K(read_size));
        }
      } else if (OB_STATE_NOT_MATCH == ret) {
        CLOG_LOG(WARN, "epoch changed, maybe logstore process restart. Current fd should be closed", K(ret), K(fd));
      } else if (OB_EAGAIN == ret) {
        CLOG_LOG(WARN, "logstore return OB_EAGAIN, wait and retry", K(ret), K(fd), K(offset), K(size), K(read_size));
        ob_usleep(ObLogDevice::RETRY_INTERVAL);
      } else {
        CLOG_LOG(WARN, "some errors happen in logstore, please pay attention", K(ret));
      }
    } while (OB_EAGAIN == ret);
  }

  return ret;
}

int ObLogDevice::pwrite(
    const ObIOFd &fd,
    const int64_t offset,
    const int64_t size,
    const void *buf,
    int64_t &write_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "The ObLogDevice has not been inited, ", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogDevice has not been started", K(ret), K_(is_running));
  } else if (!fd.is_valid() || 0 > offset || 0 >= size || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(fd), K(offset), K(size), KP(buf));
  } else if (!fd.is_normal_file()) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(ERROR, "It's not normal file, not suppor", K(ret));
  } else if (true == get_reloading_state_()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "epoch changed, should close current fd", K(fd), K(offset), K(size));
  } else {
    CLOG_LOG(INFO, "before pwrite", K(fd), K(offset), K(size));
    PwriteReq req;
    PwriteResp resp;
    int64_t pwrite_seq = ATOMIC_AAF(&pwrite_seq_, 1);
    req.set_epoch(ATOMIC_LOAD(&epoch_));
    req.set_seq(pwrite_seq);
    req.set_fd(fd.second_id_);
    req.set_buf(buf, size);
    req.set_size(size);
    req.set_offset(offset);
    // 1: write to disk
    // 2: write to memory
    const int32_t write_mode = (check_fd_is_in_sync_mode(fd.second_id_) ? 1 : 2);
    req.set_write_mode(write_mode);

    do {
      if (OB_FAIL(grpc_adapter_.pwrite(req, resp))) {
        CLOG_LOG(ERROR, "grpc call failed for pwrite", K(ret));
      } else if (FALSE_IT(ret = get_resp_ret_code(resp))) {
      } else if (OB_SUCC(ret)) {
        if (size != resp.size()) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "partital write", K(ret), K(size));
        } else {
          write_size = resp.size();
          CLOG_LOG(TRACE, "ObLogDevice pwrite success", K(fd), K(offset), K(size), K(write_size));
        }
      } else if (OB_STATE_NOT_MATCH == ret) {
        CLOG_LOG(WARN, "epoch changed, maybe logstore process restart. Current fd should be closed", K(ret), K(fd));
      } else if (OB_EAGAIN == ret) {
        CLOG_LOG(WARN, "logstore return OB_EAGAIN, wait and retry", K(ret), K(fd), K(offset), K(size), K(write_size));
        ob_usleep(ObLogDevice::RETRY_INTERVAL);
      } else {
        CLOG_LOG(WARN, "some errors happen in logstore, please pay attention", K(ret), K(fd), K(offset), K(size), K(write_size));
      }
    } while (OB_EAGAIN == ret);
  }

  return ret;
}

int ObLogDevice::read(
  const ObIOFd &fd,
  void *buf,
  const int64_t size,
  int64_t &read_size)
{
  UNUSED(fd);
  UNUSED(buf);
  UNUSED(size);
  UNUSED(read_size);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::write(
  const ObIOFd &fd,
  const void *buf,
  const int64_t size,
  int64_t &write_size)
{
  UNUSED(fd);
  UNUSED(buf);
  UNUSED(size);
  UNUSED(write_size);
  return OB_NOT_SUPPORTED;
}

//async io interfaces
int ObLogDevice::io_setup(
    uint32_t max_events,
    common::ObIOContext *&io_context)
{
  UNUSED(max_events);
  UNUSED(io_context);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::io_destroy(common::ObIOContext *io_context)
{
  UNUSED(io_context);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::io_prepare_pwrite(
    const ObIOFd &fd,
    void *buf,
    size_t count,
    int64_t offset,
    ObIOCB *iocb,
    void *callback)
{
  UNUSED(fd);
  UNUSED(buf);
  UNUSED(count);
  UNUSED(offset);
  UNUSED(iocb);
  UNUSED(callback);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::io_prepare_pread(
  const ObIOFd &fd,
  void *buf,
  size_t count,
  int64_t offset,
  ObIOCB *iocb,
  void *callback)
{
  UNUSED(fd);
  UNUSED(buf);
  UNUSED(count);
  UNUSED(offset);
  UNUSED(iocb);
  UNUSED(callback);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::io_submit(
    common::ObIOContext *io_context,
    common::ObIOCB *iocb)
{
  UNUSED(io_context);
  UNUSED(iocb);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::io_cancel(
    common::ObIOContext *io_context,
    common::ObIOCB *iocb)
{
  UNUSED(io_context);
  UNUSED(iocb);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::io_getevents(
    common::ObIOContext *io_context,
    int64_t min_nr,
    common::ObIOEvents *events,
    struct timespec *timeout)
{
  UNUSED(io_context);
  UNUSED(min_nr);
  UNUSED(events);
  UNUSED(timeout);
  return OB_NOT_SUPPORTED;
}

common::ObIOCB* ObLogDevice::alloc_iocb()
{
  ob_assert(false);
  return nullptr;
}

common::ObIOEvents *ObLogDevice::alloc_io_events(const uint32_t max_events)
{
  ob_assert(false);
  UNUSED(max_events);
  return nullptr;
}

void ObLogDevice::free_iocb(common::ObIOCB *iocb)
{
  ob_assert(false);
  UNUSED(iocb);
}

void ObLogDevice::free_io_events(common::ObIOEvents *io_event)
{
  ob_assert(false);
  UNUSED(io_event);
}

int64_t ObLogDevice::get_total_block_size() const
{
  ob_assert(false);
  return 0;
}

int64_t ObLogDevice::get_free_block_count() const
{
  ob_assert(false);
  return 0;
}

int64_t ObLogDevice::get_reserved_block_count() const
{
  ob_assert(false);
  return 0;
}

int64_t ObLogDevice::get_max_block_count(int64_t reserved_size) const
{
  ob_assert(false);
  return 0;
}

int64_t ObLogDevice::get_max_block_size(int64_t reserved_size) const
{
  ob_assert(false);
  return 0;
}

int ObLogDevice::check_space_full(const int64_t required_size) const
{
  UNUSED(required_size);
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::check_write_limited() const
{
  return OB_NOT_SUPPORTED;
}

int ObLogDevice::batch_fallocate(const char *dir_name,
                                 const int64_t block_count,
                                 const int64_t block_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogDevice has not been started", K(ret), K_(is_running));
  } else if (OB_ISNULL(dir_name)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "Invalid dir_name, ", K(ret), KP(dir_name));
  } else {
    BatchFallocateReq req;
    BatchFallocateResp resp;
    req.set_epoch(ATOMIC_LOAD(&epoch_));
    req.set_dirname(dir_name);
    req.set_count(block_count);
    req.set_size(block_size);
    do {
      if (OB_FAIL(grpc_adapter_.batch_fallocate(req, resp))) {
        CLOG_LOG(ERROR, "grpc call failed for batch_fallocate", K(ret));
      } else if (FALSE_IT(ret = get_resp_ret_code(resp))) {
      } else if (OB_SUCC(ret)) {
        CLOG_LOG(TRACE, "ObLogDevice batch_fallocate successfully", K(dir_name), K(block_count));
      } else if (OB_STATE_NOT_MATCH == ret) {
        CLOG_LOG(WARN, "epoch changed, need to retry", K(ret));
        deal_with_epoch_changed(req, resp);
      } else {
        CLOG_LOG(WARN, "some errors happen in logstore, maybe need to retry", K(ret));
      }
    } while (OB_EAGAIN == ret);
  }
  return ret;
}


int ObLogDevice::convert_sys_errno(const int32_t err_no)
{
  int ret = OB_IO_ERROR;
  switch (err_no) {
  case EACCES:
    ret = OB_FILE_OR_DIRECTORY_PERMISSION_DENIED;
    break;
  case ENOENT:
    ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
    break;
  case EEXIST:
  case ENOTEMPTY:
    ret = OB_FILE_OR_DIRECTORY_EXIST;
    break;
  case EAGAIN:
    ret = OB_EAGAIN;
    break;
  case EDQUOT:
  case ENOSPC:
    ret = OB_ALLOCATE_DISK_SPACE_FAILED;
    break;
  case ENOMEM:
    ret = OB_ALLOCATE_MEMORY_FAILED;
    break;
  default:
    ret = OB_IO_ERROR;
  }
  return ret;
}

int ObLogDevice::reload_epoch_()
{
  int ret = OB_SUCCESS;
  LoadLogStoreReq req;
  LoadLogStoreResp resp;
  req.set_version(version_);
  if (OB_FAIL(grpc_adapter_.load_log_store(req, resp))) {
    CLOG_LOG(ERROR, "grpc call failed for LoadLogStore", K(ret));
  } else {
    // 除了网络错误，LoadLogStoreResp预期只会返回OB_SUCCESS
    assert(OB_SUCCESS == resp.ret_code());
    version_ = resp.version();
    epoch_ = resp.epoch();
    ATOMIC_STORE(&is_reloading_, false);
    CLOG_LOG(INFO, "epoch reloads successfully", K_(epoch), K_(version));
  }
  return ret;
}

bool ObLogDevice::get_reloading_state_() const
{
  return ATOMIC_LOAD(&is_reloading_);
}

void ObLogDevice::wait_if_epoch_changing()
{
  bool is_reloading = ATOMIC_LOAD(&is_reloading_);
  int64_t last_print_time_us = 0;
  while (true == is_reloading) {
    ob_usleep(ObLogDevice::RETRY_INTERVAL);
    is_reloading = ATOMIC_LOAD(&is_reloading_);
    if (palf::palf_reach_time_interval(2 * 1000 * 1000, last_print_time_us)) {
      CLOG_LOG(INFO, "wait for completing epoch change", K(is_reloading), K(lbt()), K_(ref));
    }
  }
}

template <typename GrpcReq, typename GrpcResp>
void ObLogDevice::deal_with_epoch_changed(GrpcReq &req, GrpcResp &resp)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  // when requests come into this function, only following two situations may happen:
  // 1. req.epoch() == epoch_, which means logstore is retarted, it should shake with logstore
  // 2. req.epoch() < epoch_, which means the shake has been completed recently by the first shake request or this request is an an very old request,
  //    it should take the new epoch to resend

  // req.epoch() > epoch_ is unexpected
  assert(req.epoch() <= epoch_);
  if (req.epoch() == epoch_) {
    ATOMIC_STORE(&is_reloading_, true);
    int64_t ref = ATOMIC_LOAD(&ref_);
    int64_t last_print_time_us = 0;
    if (ref > 0) {

      ob_usleep(ObLogDevice::RETRY_INTERVAL);
      ref = ATOMIC_LOAD(&ref_);
      if (palf::palf_reach_time_interval(2 * 1000 * 1000, last_print_time_us)) {
        CLOG_LOG(INFO, "wait for ref becoming 0", K(ref), K(is_reloading_));
      }
      return;
    }
    CLOG_LOG(INFO, "ref becomes 0, start to reload epoch");
    assert(ref == 0);
    if (OB_FAIL(reload_epoch_())) {
      CLOG_LOG(WARN, "fail to reload epoch", K(ret));
    }
  }

  ret = OB_EAGAIN;
  req.set_epoch(epoch_);
  resp.Clear();
  CLOG_LOG(INFO, "reload epoch successfully, resend requst after epoch changed", K(ret), K_(epoch));
}

template <typename GrpcResp>
int ObLogDevice::get_resp_ret_code(const GrpcResp &resp)
{
  int ret = OB_SUCCESS;
  if (true == has_sys_errno(resp)) {
    ret = convert_sys_errno(resp.err_no());
    CLOG_LOG(WARN, "logstore encounters erros about file system", K(ret), K(resp.err_no()));
  } else {
    ret = resp.ret_code();
  }
  return ret;
}

template <typename GrpcResp>
bool ObLogDevice::has_sys_errno(const GrpcResp &resp) const
{
  return 0 != resp.err_no();
}

} // namespace logservice
} // namespace oceanbase
