/** Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_log_io_adapter.h"
#include "share/ob_local_device.h"                            // ObLocalDevice
#include "share/io/ob_io_manager.h"                           // ObIOManager
#include "lib/string/ob_string.h"                             // ObString
#include "share/ob_device_manager.h"                          // ObDeviceManager
#include "palf/log_define.h"                                  // convert_sys_errno
#include "ob_log_device.h"                                    // ObLogDevice
#include "ob_logstore_mgr.h"                                  // ObLogstoreMgr
#include "palf/log_io_utils.h"
namespace oceanbase
{
using namespace share;
using namespace common;
using namespace palf;

namespace logservice
{
bool is_valid_log_io_mode(const ObLogIOMode mode)
{
  return ObLogIOMode::LOCAL == mode || ObLogIOMode::REMOTE == mode;
}

ObLogIOInfo::ObLogIOInfo() : io_mode_(ObLogIOMode::INVALID) {}
ObLogIOInfo::~ObLogIOInfo()
{
  reset();
}

int ObLogIOInfo::init(const ObLogIOMode &io_mode,
                      const char *log_store_addr,
                      const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (ObLogIOMode::INVALID == io_mode
      || (ObLogIOMode::REMOTE == io_mode_ && (NULL == log_store_addr || !is_valid_cluster_id(cluster_id)))) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid logstore_server_addr", K(ret), KP(log_store_addr), K(io_mode), K(cluster_id));
  } else if (ObLogIOMode::REMOTE == io_mode && OB_FAIL(log_store_addr_.parse_from_cstring(log_store_addr))) {
    CLOG_LOG(WARN, "parse_from_cstring failed", K(ret), K(log_store_addr));
  } else {
    io_mode_ = io_mode;
    cluster_id_ = cluster_id;
    CLOG_LOG(INFO, "init ObLogIOInfo success", KPC(this));
  }

  return ret;
}

bool ObLogIOInfo::is_valid() const
{
  return ObLogIOMode::INVALID != io_mode_
         && (ObLogIOMode::LOCAL == io_mode_
             || (ObLogIOMode::REMOTE == io_mode_ && log_store_addr_.is_valid() && is_valid_cluster_id(cluster_id_)));
}

void ObLogIOInfo::reset()
{
  io_mode_ = ObLogIOMode::INVALID;
  log_store_addr_.reset();
  cluster_id_ = OB_INVALID_CLUSTER_ID;
}

const ObLogIOMode &ObLogIOInfo::get_log_io_mode() const
{
  return io_mode_;
}

const int64_t &ObLogIOInfo::get_cluster_id() const
{
  return cluster_id_;
}

const ObAddr &ObLogIOInfo::get_addr() const
{
  return log_store_addr_;
}

bool is_valid_io_fd(const ObIOFd &io_fd)
{
  return io_fd.is_normal_file() && NULL != io_fd.device_handle_;
}

bool operator==(const SwitchLogIOModeCbKey &lhs, const SwitchLogIOModeCbKey &rhs)
{
  return 0 == strncmp(lhs.value_, rhs.value_, OB_MAX_FILE_NAME_LENGTH);
}

SwitchLogIOModeCbKey::SwitchLogIOModeCbKey()
{
  memset(value_, 0, OB_MAX_FILE_NAME_LENGTH);
}

SwitchLogIOModeCbKey::SwitchLogIOModeCbKey(const char *key)
{
  memset(value_, 0, OB_MAX_FILE_NAME_LENGTH);
  memcpy(value_, key, strlen(key));
}

SwitchLogIOModeCbKey::~SwitchLogIOModeCbKey()
{}

uint64_t SwitchLogIOModeCbKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(value_, strlen(value_), hash_val);
  return hash_val;
}

int SwitchLogIOModeCbKey::hash(uint64_t &val) const
{
  val = hash();
  return OB_SUCCESS;
}

int SwitchLogIOModeCbKey::assign(const SwitchLogIOModeCbKey &key)
{
  memcpy(value_, key.value_, OB_MAX_FILE_NAME_LENGTH);
  CLOG_LOG(TRACE, "SwitchLogIOModeCbKey assign success");
  return OB_SUCCESS;
}

// ========================= ObLogIOAdapter=====================
int ObLogIOAdapter::init(const char *clog_dir,
                         const int64_t disk_io_thread_count,
                         const int64_t max_io_depth,
                         const ObLogIOInfo &log_io_info,
                         ObIOManager *io_manager,
                         ObDeviceManager *device_manager)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(clog_dir) || 0 >= disk_io_thread_count || 0 >= max_io_depth ||
      OB_ISNULL(io_manager) || OB_ISNULL(device_manager) || !log_io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(clog_dir), K(disk_io_thread_count),
             K(max_io_depth), KP(io_manager), KP(device_manager), K(log_io_info));
  } else if (OB_FAIL(cb_map_.create(BUCKET_NUM, SET_USE_500("SwitchCBMap")))) {
    CLOG_LOG(WARN, "fail to create cb map", K(ret));
  } else if (OB_FAIL(init_io_device_(clog_dir, log_io_info, disk_io_thread_count, max_io_depth, device_manager, io_manager))) {
    CLOG_LOG(WARN, "init_io_device_ failed", K(ret), KP(clog_dir), K(disk_io_thread_count),
             K(max_io_depth), KP(io_manager), KP(device_manager));
  } else {
    using_mode_ = log_io_info.get_log_io_mode();
    device_manager_ = device_manager;
    io_manager_ = io_manager;
    strncpy(clog_dir_, clog_dir, OB_MAX_FILE_NAME_LENGTH);
    disk_io_thread_count_ = disk_io_thread_count;
    max_io_depth_ = max_io_depth;
    is_inited_ = true;
    CLOG_LOG(INFO, "log_local_device_ init successfully", KP_(log_local_device), KP_(log_remote_device), KP_(using_device), K(clog_dir), K(disk_io_thread_count));

  }
  return ret;
}

void ObLogIOAdapter::destroy()
{
  WLockGuard guard(using_device_lock_);
  while (0 != flying_fd_count_) {
    CLOG_LOG_RET(WARN, OB_NEED_WAIT, "flying_fd_count_ not zero, need wait", K(flying_fd_count_));
  }
  if (is_inited_) {
    {
      ObSpinLockGuard cb_guard(cb_map_lock_);
      cb_map_.destroy();
    }
    if (OB_NOT_NULL(log_local_device_)) {
      log_local_device_->destroy();
      device_manager_->release_device((ObIODevice*&) log_local_device_);
    }
    if (OB_NOT_NULL(log_remote_device_)) {
      log_remote_device_->destroy();
      device_manager_->release_device((ObIODevice*&) log_remote_device_);
    }
    using_device_ = NULL;
    log_local_device_ = NULL;
    log_remote_device_ = NULL;
    device_manager_ = NULL;
    is_inited_ = false;
    CLOG_LOG(INFO, "ObLogIOAdapter destory");
  }
}

int ObLogIOAdapter::try_switch_log_io_mode()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogIOInfo io_info;
  ObLogIOMode using_mode = get_using_mode_();
  const ObLogIOMode mode = (0 == GCONF._ob_flush_log_at_trx_commit ? logservice::ObLogIOMode::REMOTE : logservice::ObLogIOMode::LOCAL);
  ObAddr logstore_address;
  bool is_active = false;
  int64_t last_active_ts = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogIOAdapter not inited", KR(ret));
  } else if (OB_FAIL(io_info.init(mode, GCONF._ob_logstore_service_addr, GCONF.cluster_id.get_value()))) {
    CLOG_LOG(WARN, "init ObLogIOInfo failed", KR(ret), K(mode), "_ob_logstore_service_addr", GCONF._ob_logstore_service_addr, "cluster_id", GCONF.cluster_id.get_value());
  } else if (mode == using_mode) {
    CLOG_LOG(INFO, "no need switch_log_io_mode", K(io_info), K(using_mode_));
  } else if (logservice::ObLogIOMode::REMOTE == mode &&
      OB_TMP_FAIL(LOGSTORE_MGR.init()) && OB_INIT_TWICE != tmp_ret) {
    ret = tmp_ret;
    CLOG_LOG(WARN, "init logstore_mgr failed", KR(ret), KR(tmp_ret), K(using_mode_), K(mode));
  } else if (logservice::ObLogIOMode::REMOTE == mode &&
      (OB_TMP_FAIL(LOGSTORE_MGR.get_logstore_service_status(logstore_address, is_active, last_active_ts)) || !is_active)) {
    ret = OB_EAGAIN;
    CLOG_LOG(WARN, "logstore service is not active when switch mode to REMOTE, need retry", KR(ret), K(io_info), K(using_mode_), K(mode),
        K(logstore_address), K(is_active), K(last_active_ts));
  } else if (OB_FAIL(switch_log_io_mode(io_info))) {
    CLOG_LOG(WARN, "switch_log_io_mode failed", KR(ret), K(io_info), K(using_mode_));
  } else {
    CLOG_LOG(INFO, "switch_log_io_mode success", KR(ret), K(io_info), K(using_mode_), KPC(this));
  }
  return ret;
}

int ObLogIOAdapter::switch_log_io_mode(const ObLogIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  const ObLogIOMode &mode = io_info.get_log_io_mode();
  WLockGuard guard(using_device_lock_);
  if (mode == using_mode_) {
    CLOG_LOG(INFO, "no need switch_log_io_mode", K(io_info), K(using_mode_));
  } else if (ObLogIOMode::LOCAL == mode) {
    ret = switch_log_io_mode_to_local_();
  } else {
    ret = switch_log_io_mode_to_remote_(io_info);
  }
  return ret;
}

share::ObLocalDevice *ObLogIOAdapter::get_local_device()
{
  abort_unless(NULL != log_local_device_);
  return log_local_device_;
}

int64_t ObLogIOAdapter::choose_align_size() const
{
  RLockGuard guard(using_device_lock_);
  return choose_align_size_(using_mode_);
}

// ============================= file interface ===========================
int ObLogIOAdapter::open(const char *block_path,
                         const int flags,
                         const mode_t mode,
                         ObIOFd &io_fd)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (OB_ISNULL(block_path) || 0 == STRLEN(block_path) || -1 == flags || -1 == mode) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument!", K(ret), KP(block_path), K(flags), K(mode));
  } else {
    do {
      RLockGuard guard(using_device_lock_);
      if (OB_FAIL(using_device_->open(block_path, flags, mode, io_fd))) {
        CLOG_LOG(WARN, "failed to open file", K(ret), K(block_path), K(flags), K(mode));
      } else {
        ATOMIC_INC(&flying_fd_count_);
        io_fd.device_handle_ = using_device_;
        set_fd_holder(io_fd.second_id_, true);
        CLOG_LOG(INFO, "open file sucessfully", K(block_path), K(flags), K(mode), K(io_fd), KPC(this));
      }
    } while (OB_STATE_NOT_MATCH == ret && OB_SUCC(deal_with_state_not_match_()));
  }
  return ret;
}

int ObLogIOAdapter::close(ObIOFd &io_fd)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (!is_valid_io_fd(io_fd)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, " the block has been closed", K(ret), K(io_fd));
  } else if (OB_FAIL(io_fd.device_handle_->close(io_fd))) {
    CLOG_LOG(ERROR, "close block failed", K(ret), K(io_fd));
  } else if (!check_fd_holder_by_io_adapter(io_fd.second_id_)) {
    CLOG_LOG(INFO, "fd is not holder by io adapter, no need dec flying_fd_count", K(io_fd));
  } else {
    io_fd.reset();
    ATOMIC_DEC(&flying_fd_count_);
    CLOG_LOG(TRACE, "close block successfully", K(io_fd), KPC(this));
  }

  return ret;
}

int ObLogIOAdapter::pread(const int64_t in_read_size,
                          ObIOInfo &io_info,
                          int64_t &read_size,
                          ObIOManager *io_manager)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(io_manager_->pread(io_info, read_size))) {
    CLOG_LOG(WARN, "io_manager_ pread failed", K(ret), K(io_info));
  } else if (read_size != in_read_size) {
    // partial write
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "partital read", K(ret), K(io_info), K(in_read_size), K(read_size));
  } else {
    CLOG_LOG(TRACE, "pread by io_manager_ successfully", K(io_info), K(read_size));
  }
  return ret;
}

int ObLogIOAdapter::pwrite(const int64_t in_write_size,
                           ObIOInfo &io_info,
                           int64_t &write_size,
                           ObIOManager *io_manager)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(io_manager_->pwrite(io_info, write_size))) {
    CLOG_LOG(WARN, "io_manager_ pwrite failed", K(ret), K(io_info));
  } else if (write_size != in_write_size) {
    // partial write
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "partital write", K(ret), K(io_info), K(write_size), K(in_write_size));
  } else {
    CLOG_LOG(TRACE, "pwrite by io_manager_ successfully", K(io_info), K(write_size));
  }
  return ret;
}

int ObLogIOAdapter::pread(const ObIOFd &io_fd,
                          const int64_t count,
                          const int64_t offset,
                          char *buf,
                          int64_t &out_read_size)
{
  int ret = OB_SUCCESS;
  out_read_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (!is_valid_io_fd(io_fd) || OB_ISNULL(buf) || 0 > count || 0 > offset) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(io_fd), KP(buf), K(count), K(offset));
  } else if (OB_FAIL(io_fd.device_handle_->pread(io_fd, offset, count, buf, out_read_size))) {
    CLOG_LOG(WARN, "pread failed", K(ret), K(errno), K(offset), K(count), K(out_read_size), K(io_fd));
  } else if (count != out_read_size ) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "pread failed", K(io_fd), K(count), K(offset), K(out_read_size));
  } else {
    CLOG_LOG(TRACE, "pread successfully", K(offset), K(count), K(out_read_size), K(io_fd), KPC(this));
  }
  return ret;
}

int ObLogIOAdapter::pwrite(const ObIOFd &io_fd,
                           const char *buf,
                           const int64_t count,
                           const int64_t offset,
                           int64_t &write_size)
{
  int ret = OB_SUCCESS;
  write_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (!is_valid_io_fd(io_fd) || OB_ISNULL(buf) || 0 > count || 0 > offset) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(io_fd), KP(buf), K(count), K(offset));
  } else if (OB_FAIL(io_fd.device_handle_->pwrite(io_fd, offset, count, buf, write_size))) {
    CLOG_LOG(WARN, "pwrite failed", K(ret), K(errno), K(offset), K(count), K(write_size), K(io_fd));
  } else if (count != write_size) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "pwrite failed", K(io_fd), K(count), K(offset), K(write_size));
  } else {
    CLOG_LOG(TRACE, "pwrite successfully", K(offset), K(count), K(write_size), K(io_fd), KPC(this));
  }
  return ret;
}

int ObLogIOAdapter::truncate(const ObIOFd &io_fd,
                             const int64_t offset)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (!is_valid_io_fd(io_fd) || 0 > offset) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(io_fd), K(offset));
  } else if (OB_FAIL(io_fd.device_handle_->ftruncate(io_fd, offset))) {
    CLOG_LOG(WARN, "ftruncate failed", K(ret), K(errno), K(io_fd), K(offset));
  } else {
    CLOG_LOG(INFO, "truncate success", K(io_fd), K(offset), KPC(this));
  }
  return ret;
}

int ObLogIOAdapter::fallocate(const ObIOFd &io_fd,
                              mode_t mode,
                              const int64_t offset,
                              const int64_t len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (!is_valid_io_fd(io_fd) || 0 > offset || 0 > len) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(io_fd), K(offset), K(len));
  } else if (OB_FAIL(io_fd.device_handle_->fallocate(io_fd, mode, offset, len))) {
    CLOG_LOG(WARN, "fallocate failed", K(ret), K(errno), K(io_fd), K(offset));
  } else {
    CLOG_LOG(TRACE, "fallocate success", K(ret), K(errno), K(io_fd), K(offset));
  }
  return ret;
}

int ObLogIOAdapter::fsync(const ObIOFd &io_fd)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (!is_valid_io_fd(io_fd)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(io_fd));
  } else if (OB_FAIL(io_fd.device_handle_->fsync(io_fd))) {
    CLOG_LOG(WARN, "fsync failed", K(ret), K(errno), K(io_fd));
  } else {
    CLOG_LOG(INFO, "fsync success", K(ret), K(io_fd), KPC(this));
  }
  return ret;
}

int ObLogIOAdapter::fsync_dir(const char *dir_name)
{
  int ret = OB_SUCCESS;
  constexpr int OPEN_DIR_FLAG = O_DIRECTORY | O_RDONLY;
  constexpr mode_t FILE_OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  ObIOFd dir_fd;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (NULL == dir_name) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KP(dir_name));
  } else if (OB_FAIL(open(dir_name, OPEN_DIR_FLAG, FILE_OPEN_MODE, dir_fd))) {
    CLOG_LOG(WARN, "open failed", K(ret), K(errno), K(dir_fd));
  } else if (OB_FAIL(fsync(dir_fd))) {
    CLOG_LOG(WARN, "fsync failed", K(ret), K(errno), K(dir_fd));
  } else {
    CLOG_LOG(INFO, "fsync_dir success", K(ret), K(dir_fd), KPC(this));
  }
  if (dir_fd.is_valid()) {
    close(dir_fd);
  }
  return ret;
}

int ObLogIOAdapter::stat(const char *pathname,
                         common::ObIODFileStat &statbuf)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (NULL == pathname) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KP(pathname));
  } else {
    do {
      RLockGuard guard(using_device_lock_);
      if (OB_FAIL(using_device_->stat(pathname, statbuf))) {
        CLOG_LOG(TRACE, "stat failed", K(ret), K(errno));
      } else {
        CLOG_LOG(TRACE, "stat success", K(ret), K(statbuf), K(pathname), KPC(this));
      }
    } while (OB_STATE_NOT_MATCH == ret && OB_SUCC(deal_with_state_not_match_()));
  }
  return ret;
}

int ObLogIOAdapter::fstat(const common::ObIOFd &io_fd,
                          common::ObIODFileStat &statbuf)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (!is_valid_io_fd(io_fd)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(io_fd));
  } else if (OB_FAIL(io_fd.device_handle_->fstat(io_fd, statbuf))) {
    CLOG_LOG(WARN, "fstat failed", K(ret), K(io_fd));
  } else {
    CLOG_LOG(INFO, "fsync success", K(ret), K(io_fd));
  }
  return ret;
}

int ObLogIOAdapter::rename(const char *oldpath,
                           const char *newpath)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (NULL == oldpath || NULL == newpath) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KP(oldpath), KP(newpath));
  } else  {
    do {
      RLockGuard guard(using_device_lock_);
      if (OB_FAIL(using_device_->rename(oldpath, newpath))) {
        CLOG_LOG(WARN, "rename failed", K(ret), KP(oldpath), KP(newpath));
      } else {
        CLOG_LOG(TRACE, "rename success", K(oldpath), K(newpath), KPC(this));
      }
    } while (OB_STATE_NOT_MATCH == ret && OB_SUCC(deal_with_state_not_match_()));
  }
  return ret;
}
int ObLogIOAdapter::unlink(const char *pathname)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (NULL == pathname) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KP(pathname));
  } else {
    do {
      RLockGuard guard(using_device_lock_);
      if (OB_FAIL(using_device_->unlink(pathname))) {
        CLOG_LOG(WARN, "unlink failed", K(ret), KP(pathname));
      } else {
        CLOG_LOG(TRACE, "unlink success", K(pathname), KPC(this));
      }
    } while (OB_STATE_NOT_MATCH == ret && OB_SUCC(deal_with_state_not_match_()));
  }
  return ret;
}

// dir interface
int ObLogIOAdapter::scan_dir(const char *dir_name,
                             common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (NULL == dir_name) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KP(dir_name));
  } else {
    do {
      RLockGuard guard(using_device_lock_);
      if (OB_FAIL(using_device_->scan_dir(dir_name, op))) {
        CLOG_LOG(WARN, "scan_dir failed", K(ret), KP(dir_name));
      } else {
        CLOG_LOG(INFO, "scan_dir success", K(dir_name));
      }
    } while (OB_STATE_NOT_MATCH == ret && OB_SUCC(deal_with_state_not_match_()));
  }
  return ret;
}

int ObLogIOAdapter::mkdir(const char *pathname,
                          mode_t mode)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (NULL == pathname) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KP(pathname));
  } else {
    do {
      RLockGuard guard(using_device_lock_);
      if (OB_FAIL(using_device_->mkdir(pathname, mode))) {
        CLOG_LOG(WARN, "mkdir failed", K(ret), K(pathname), K(mode));
      } else {
        CLOG_LOG(INFO, "mkdir success", K(pathname), K(mode));
      }
    } while (OB_STATE_NOT_MATCH == ret && OB_SUCC(deal_with_state_not_match_()));
  }
  return ret;
}

int ObLogIOAdapter::rmdir(const char *pathname)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (NULL == pathname) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KP(pathname));
  } else {
    do {
      RLockGuard guard(using_device_lock_);
      if (OB_FAIL(using_device_->rmdir(pathname))) {
        CLOG_LOG(WARN, "rmdir failed", K(ret), K(pathname));
      } else {
        CLOG_LOG(INFO, "rmdir success", K(pathname));
      }
    } while (OB_STATE_NOT_MATCH == ret && OB_SUCC(deal_with_state_not_match_()));
  }
  return ret;
}

int ObLogIOAdapter::register_cb(const SwitchLogIOModeCbKey &key, SwitchLogIOModeCb &cb)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(cb_map_lock_);
  if (OB_FAIL(cb_map_.set_refactored(key, cb))) {
    CLOG_LOG(WARN, "set_refactored failed");
  } else {
    CLOG_LOG(INFO, "register_cb success", K(key));
  }
  return ret;
}

void ObLogIOAdapter::unregister_cb(const SwitchLogIOModeCbKey &key)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(cb_map_lock_);
  do {
    if (OB_FAIL(cb_map_.erase_refactored(key)) && OB_HASH_NOT_EXIST != ret) {
      CLOG_LOG(WARN, "erase_refactored failed", K(key));
    }
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
    CLOG_LOG(INFO, "unregister_cb success", K(key));
  } while (OB_FAIL(ret));
}

int ObLogIOAdapter::init_io_device_(const char *clog_dir,
                                    const ObLogIOInfo &io_info,
                                    const int64_t disk_io_thread_count,
                                    const int64_t max_io_depth,
                                    ObDeviceManager *device_manager,
                                    ObIOManager *io_manager)
{
  int ret = OB_SUCCESS;
  // useless except for being used to init log_local_device
  ObIODOpts iod_opts;
  const ObLogIOMode &mode = io_info.get_log_io_mode();
  if (ObLogIOMode::LOCAL == mode) {
    if (OB_FAIL(init_local_device_(clog_dir, disk_io_thread_count, max_io_depth, device_manager, io_manager))) {
      CLOG_LOG(WARN, "init_local_device_ failed", KR(ret));
    } else {
      using_device_ = log_local_device_;
    }
  } else if (ObLogIOMode::REMOTE == mode) {
    const ObAddr &addr = io_info.get_addr();
    const int64_t cluster_idx = io_info.get_cluster_id();
    if (OB_FAIL(init_remote_device_(clog_dir, disk_io_thread_count, max_io_depth, addr, cluster_idx, device_manager, io_manager))) {
      CLOG_LOG(WARN, "init_remote_device_ failed", KR(ret));
    } else {
      using_device_ = log_remote_device_;
    }
  }
  if (OB_SUCC(ret)){
    CLOG_LOG(INFO, "init_io_device_ success", K(clog_dir), K(mode), K(disk_io_thread_count));
  }
  return ret;
}

int ObLogIOAdapter::init_local_device_(const char *clog_dir,
                                       const int64_t disk_io_thread_count,
                                       const int64_t max_io_depth,
                                       ObDeviceManager *device_manager,
                                       ObIOManager *io_manager)
{
  int ret = OB_SUCCESS;
  ObIODOpts iod_opts;
  ObString storage_info(clog_dir);
  ObString storage_type_local_prefix(OB_LOCAL_PREFIX);
  ObIODevice* local_device_handle = NULL;
  if (OB_FAIL(device_manager->get_device(storage_info, storage_type_local_prefix, local_device_handle))) {
    CLOG_LOG(WARN, "failed to get device from ObDeviceManager", K(ret), K(storage_info), K(storage_type_local_prefix), KP(local_device_handle));
  } else if (OB_ISNULL(log_local_device_ = static_cast<ObLocalDevice *>(local_device_handle))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected situations!", K(ret));
  } else if (OB_FAIL(log_local_device_->init(iod_opts))) {
    CLOG_LOG(WARN, "fail to init io device", K(ret), KP(local_device_handle), KP(log_local_device_));
  } else if (OB_FAIL(io_manager->add_device_channel(log_local_device_, disk_io_thread_count, disk_io_thread_count, max_io_depth))) {
    CLOG_LOG(WARN, "failed to add device channel", K(ret));
  } else {
    CLOG_LOG(INFO, "init_local_device_ success");
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(local_device_handle)) {
    (void) device_manager->release_device(local_device_handle);
    local_device_handle = NULL;
    log_local_device_ = NULL;
  }
  return ret;
}

int ObLogIOAdapter::init_remote_device_(const char *clog_dir,
                                        const int64_t disk_io_thread_count,
                                        const int64_t max_io_depth,
                                        const ObAddr &addr,
                                        const int64_t cluster_idx,
                                        ObDeviceManager *device_manager,
                                        ObIOManager *io_manager)
{
  int ret = OB_SUCCESS;
  ObIODOpts iod_opts;
  char remote_clog_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  snprintf(remote_clog_dir, OB_MAX_FILE_NAME_LENGTH, "log://%s", clog_dir);
  ObString remote_storage_info(remote_clog_dir);
  ObString storage_type_log_store_prefix(OB_LOG_STORE_PREFIX);
  ObIODevice* remote_device_handle = NULL;
  if (OB_FAIL(device_manager->get_device(remote_storage_info, storage_type_log_store_prefix, remote_device_handle))) {
    CLOG_LOG(WARN, "failed to get device from ObDeviceManager", K(ret), K(storage_type_log_store_prefix), KP(remote_device_handle));
  } else if (OB_ISNULL(log_remote_device_ = static_cast<ObLogDevice *>(remote_device_handle))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected situations!", K(ret));
  } else if (OB_FAIL(log_remote_device_->init(addr, cluster_idx))) {
    CLOG_LOG(WARN, "fail to init remote io device", K(ret), K(storage_type_log_store_prefix));
    // TODO by runlin: add device optional
  } else if (OB_FAIL(log_remote_device_->start(iod_opts))) {
    CLOG_LOG(WARN, "fail to start remote io device", K(ret));
  } else if (OB_FAIL(io_manager->add_device_channel(log_remote_device_, 0, disk_io_thread_count, max_io_depth))) {
    CLOG_LOG(WARN, "failed to add device channel", K(ret), K(max_io_depth));
  } else {
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(remote_device_handle)) {
    (void) device_manager->release_device(remote_device_handle);
    remote_device_handle = NULL;
    log_remote_device_ = NULL;
  }
  return ret;
}

int ObLogIOAdapter::switch_log_io_mode_to_remote_(const ObLogIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  const int64_t print_threshold = 100 * 1000;
  ObTimeGuard time_guard("switch_log_io_mode_to_remote_", print_threshold);
  if (NULL == log_remote_device_ &&
      OB_FAIL(init_remote_device_(clog_dir_, disk_io_thread_count_, max_io_depth_, io_info.get_addr(), io_info.get_cluster_id(), device_manager_, io_manager_))) {
    CLOG_LOG(WARN, "init_remote_device_ failed", KR(ret));
  } else if (FALSE_IT(time_guard.click("init_remote_device_"))) {
  } else if (OB_FAIL(execute_switch_log_io_mode_cb_(log_remote_device_, ObLogIOMode::REMOTE))) {
    CLOG_LOG(WARN, "execute_switch_log_io_mode_cb_ failed", KR(ret));
  } else if (FALSE_IT(time_guard.click("execute_switch_log_io_mode_cb_"))) {
  } else if (OB_FAIL(io_manager_->remove_device_channel(log_local_device_))
             && OB_HASH_NOT_EXIST != ret) {
    CLOG_LOG(WARN, "remove_device_channel failed", KR(ret));
  } else if (FALSE_IT(time_guard.click("remove_device_channel"))) {
  } else {
    ObIODevice *io_device = log_local_device_;
    log_local_device_->destroy();
    time_guard.click("destroy device");
    (void) device_manager_->release_device(io_device);
    time_guard.click("release_device");
    log_local_device_ = NULL;
    using_device_ = log_remote_device_;
    using_mode_ = ObLogIOMode::REMOTE;
    ret = OB_SUCCESS;
    CLOG_LOG(INFO, "switch_log_io_mode_to_remote_ success", K(time_guard));
  }
  return ret;
}

int ObLogIOAdapter::switch_log_io_mode_to_local_()
{
  int ret = OB_SUCCESS;
  const int64_t print_threshold = 100 * 1000;
  ObTimeGuard time_guard("switch_log_io_mode_to_local_", print_threshold);
  if (NULL == log_local_device_
      && OB_FAIL(init_local_device_(clog_dir_, disk_io_thread_count_, max_io_depth_, device_manager_, io_manager_))) {
    CLOG_LOG(WARN, "init_local_device_ failed", KR(ret));
  } else if (FALSE_IT(time_guard.click("init_local_device_"))) {
  } else if (OB_FAIL(execute_switch_log_io_mode_cb_(log_local_device_, ObLogIOMode::LOCAL))) {
    CLOG_LOG(WARN, "execute_switch_log_io_mode_cb_ failed", KR(ret));
  } else if (FALSE_IT(time_guard.click("execute_switch_log_io_mode_cb_"))) {
  } else if (OB_FAIL(io_manager_->remove_device_channel(log_remote_device_))
             && OB_HASH_NOT_EXIST != ret) {
    CLOG_LOG(WARN, "remove_device_channel failed", KR(ret));
  } else if (FALSE_IT(time_guard.click("remove_device_channel"))) {
  } else {
    ObIODevice *io_device = log_remote_device_;
    log_remote_device_->destroy();
    time_guard.click("destroy device");
    (void) device_manager_->release_device(io_device);
    time_guard.click("release_device");
    log_remote_device_ = NULL;
    using_device_ = log_local_device_;
    using_mode_ = ObLogIOMode::LOCAL;
    ret = OB_SUCCESS;
    CLOG_LOG(INFO, "switch_log_io_mode_to_local_ success", K(time_guard));
  }
  return ret;
}

ObLogIOAdapter::ExecuteCbFunctor::ExecuteCbFunctor(ObIODevice *prev_io_device, ObIODevice *io_device, int64_t align_size)
  : prev_io_device_(prev_io_device), io_device_(io_device), align_size_(align_size) {}

ObLogIOAdapter::ExecuteCbFunctor::~ExecuteCbFunctor()
{
  prev_io_device_ = NULL;
  io_device_ = NULL;
  align_size_ =  0;
}

int ObLogIOAdapter::ExecuteCbFunctor::operator()(common::hash::HashMapPair<SwitchLogIOModeCbKey, SwitchLogIOModeCb> &pair)
{
  return pair.second(prev_io_device_, io_device_, align_size_);
}

int ObLogIOAdapter::execute_switch_log_io_mode_cb_(ObIODevice *io_device,
                                                   const ObLogIOMode &io_mode)
{
  int ret = OB_SUCCESS;
  const int64_t print_threshold = 100 * 1000;
  ObTimeGuard time_guard("execute_switch_log_io_mode_cb_", print_threshold);
  ExecuteCbFunctor functor(using_device_, io_device, choose_align_size_(io_mode));
  ObSpinLockGuard guad(cb_map_lock_);
  CLOG_LOG(INFO, "execute cb first phase, make flying_fd_count to zero", K(flying_fd_count_));
  if (OB_FAIL(cb_map_.foreach_refactored(functor))) {
    CLOG_LOG(WARN, "foreach_refactored failed", KR(ret), KP(io_device));
  }
  time_guard.click("execute_cb_first_phase");
  wait_flying_fd_count_to_zero_();
  time_guard.click("wait_flying_fd_count_to_zero_");
  CLOG_LOG(INFO, "execute cb second phase, fsync each writeable file", K(flying_fd_count_));
  if (OB_FAIL(cb_map_.foreach_refactored(functor))) {
    CLOG_LOG(WARN, "foreach_refactored failed", KR(ret), KP(io_device));
  }
  time_guard.click("execute_cb_second_phase");
  CLOG_LOG(INFO, "execute cb third phase, reopen each writeable file", K(flying_fd_count_));
  if (OB_FAIL(cb_map_.foreach_refactored(functor))) {
    CLOG_LOG(WARN, "foreach_refactored failed", KR(ret), KP(io_device));
  }
  time_guard.click("execute_cb_third_phase");

  CLOG_LOG(INFO, "execute_switch_log_io_mode_cb_ finish", K(time_guard));
  return ret;
}

void ObLogIOAdapter::wait_flying_fd_count_to_zero_()
{
  int ret = OB_NEED_WAIT;
  int64_t flying_fd_count = 0;
  while (0 != (flying_fd_count = ATOMIC_LOAD(&flying_fd_count_))) {
    CLOG_LOG(WARN, "fd count is not zero, need wait", K(flying_fd_count));
    ob_usleep(10*1000);
  }
}

int ObLogIOAdapter::deal_with_state_not_match_()
{
  WLockGuard guard(using_device_lock_);
  CLOG_LOG(INFO, "deal_with_state_not_match_", K(flying_fd_count_));
  execute_switch_log_io_mode_cb_(using_device_, using_mode_);
  return OB_SUCCESS;
}

const ObLogIOMode &ObLogIOAdapter::get_using_mode_()
{
  RLockGuard guard(using_device_lock_);
  return using_mode_;
}

int ObLogIOAdapter::batch_fallocate(const char *dir_name,
                                    const int64_t block_count,
                                    const int64_t block_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (OB_ISNULL(dir_name) || 0 > block_count) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument!", K(ret), KP(dir_name), K(block_count), K(block_size));
  } else {
    RLockGuard guard(using_device_lock_);
    if (ObLogIOMode::LOCAL == using_mode_) {
      if (OB_FAIL(batch_fallocate_for_local_(dir_name, block_count, block_size))) {
        CLOG_LOG(WARN, "batch_fallocate_for_local_ failed", KR(ret), K(dir_name), K(block_count), K(block_size));
      }
    } else if (ObLogIOMode::REMOTE == using_mode_) {
      if (OB_FAIL(batch_fallocate_for_remote_(dir_name, block_count, block_size))) {
        CLOG_LOG(WARN, "batch_fallocate_for_remote failed", KR(ret), K(dir_name), K(block_count), K(block_size));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "invalid using_mode_", KR(ret), K(dir_name), K(block_count), K(block_size));
    }
  }
  if (OB_SUCC(ret)) {
    CLOG_LOG(INFO, "batch_fallocate failed", K(dir_name), K(block_count), K(block_size));
  }
  return ret;
}

int fallocate_with_retry(const char *block_path,
                         const int64_t block_size,
                         ObIODevice *io_device)
{
  int ret = OB_SUCCESS;
  bool has_create_success = false;
  constexpr int64_t RETRY_INTERVAL = 10 * 1000;
  do {
    ObIOFd io_fd;
    int flags = (!has_create_success ? CREATE_FILE_FLAG : OPEN_FILE_FLAG);
    mode_t mode = (!has_create_success ? CREATE_FILE_MODE : FILE_OPEN_MODE);
    if (OB_FAIL(io_device->open(block_path, flags, mode, io_fd))) {
      PALF_LOG(WARN, "open failed", K(ret), K(block_path));
      ob_usleep(RETRY_INTERVAL);
    } else if (FALSE_IT(has_create_success = true)) {
    } else if (OB_FAIL(io_device->fallocate(io_fd, 0, 0, block_size))) {
      PALF_LOG(WARN, "fallocate failed", K(ret), K(block_path));
      ob_usleep(RETRY_INTERVAL);
    } else {
      ret = OB_SUCCESS;
      PALF_LOG(TRACE, "fallocate block success", K(ret), K(block_path));
    }
    if (io_fd.is_valid()) {
      io_device->close(io_fd);
    }
  } while (OB_LOG_STORE_EPOCH_CHANGED == ret);
  return ret;
}

int ObLogIOAdapter::batch_fallocate_for_local_(const char *dir_name,
                                               const int64_t block_count,
                                               const int64_t block_size)
{
  int ret = OB_SUCCESS;
  int64_t remain_block_cnt = block_count;
  block_id_t block_id = 0;
  constexpr int64_t PRINT_INTERVAL = 1000 * 1000;
  while (OB_SUCC(ret) && remain_block_cnt > 0) {
    char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    if (OB_FAIL(construct_absolute_block_path(dir_name, block_id, OB_MAX_FILE_NAME_LENGTH, block_path))) {
      CLOG_LOG(ERROR, "construct_absolute_block_path failed", K(ret), KPC(this), K(block_id));
    } else if (OB_FAIL(fallocate_with_retry(block_path, block_size, using_device_))) {
      CLOG_LOG(ERROR, "fallocate_with_retry failed", K(ret), KPC(this), K(dir_name), K(block_id),
               K(errno));
    } else {
      if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
        CLOG_LOG(INFO, "allocate_block_at_ success", K(ret), KPC(this), K(dir_name), K(block_id));
      }
      remain_block_cnt--;
      block_id++;
    }
  }
  return ret;
}

int ObLogIOAdapter::batch_fallocate_for_remote_(const char *dir_name,
                                                const int64_t block_count,
                                                const int64_t block_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(log_remote_device_->batch_fallocate(dir_name, block_count, block_size))) {
    CLOG_LOG(WARN, "batch_fallocate failed", KR(ret), KPC(this), K(dir_name), K(block_count), K(block_size));
  } else {
    CLOG_LOG(WARN, "batch_fallocate_for_remote_ success", K(dir_name), K(block_count), K(block_size));
  }
  return ret;
}

int64_t ObLogIOAdapter::choose_align_size_(const ObLogIOMode &io_mode) const
{
  const int64_t align_size = (ObLogIOMode::REMOTE == io_mode ? 0 : LOG_DIO_ALIGN_SIZE);
  return align_size;
}

} // namespace logservice
} // namespace oceanbase
