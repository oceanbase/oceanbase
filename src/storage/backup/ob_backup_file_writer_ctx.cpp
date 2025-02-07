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

#define USING_LOG_PREFIX STORAGE
#include "ob_backup_file_writer_ctx.h"
#include "ob_backup_data_struct.h"
#include "share/backup/ob_backup_io_adapter.h"
namespace oceanbase {

namespace backup {

ERRSIM_POINT_DEF(EN_BACKUP_TRIGGER_BANDWIDTH_THROTTLE);

/* ObBackupFileWriteCtx */

ObBackupFileWriteCtx::ObBackupFileWriteCtx()
    : is_inited_(false),
      file_size_(0),
      max_file_size_(0),
      io_fd_(),
      dev_handle_(NULL),
      data_buffer_("BackupFileWriteCtx"),
      bandwidth_throttle_(NULL),
      last_active_time_(0)
{}

ObBackupFileWriteCtx::~ObBackupFileWriteCtx()
{}

int ObBackupFileWriteCtx::open(const int64_t max_file_size, const common::ObIOFd &io_fd, common::ObIODevice &dev_handle,
    common::ObInOutBandwidthThrottle &bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup data ctx init twice", K(ret));
  } else if (max_file_size < 0 || !io_fd.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(max_file_size), K(io_fd));
  } else if (OB_FAIL(data_buffer_.ensure_space(OB_DEFAULT_MACRO_BLOCK_SIZE))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else {
    file_size_ = 0;
    max_file_size_ = max_file_size;
    io_fd_ = io_fd;
    dev_handle_ = &dev_handle;
    bandwidth_throttle_ = &bandwidth_throttle;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupFileWriteCtx::append_buffer(const blocksstable::ObBufferReader &buffer, const bool is_last_part)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file write ctx do not init", K(ret));
  } else if (!buffer.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(buffer));
  } else if (OB_FAIL(write_buffer_(buffer.data(), buffer.length(), is_last_part))) {
    LOG_WARN("failed to write buffer", K(ret), K(buffer), K(is_last_part));
  } else {
    LOG_DEBUG("append buffer to file write ctx", K(buffer));
  }
  return ret;
}

int ObBackupFileWriteCtx::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file write ctx do not init", K(ret));
  } else if (OB_FAIL(commit_file_())) {
    LOG_WARN("failed to commit file", K(ret));
  } else {
    is_inited_ = false;
  }
  return ret;
}

int ObBackupFileWriteCtx::write_buffer_(const char *buf, const int64_t len, const bool is_last_part)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(buf), K(len));
  } else if (OB_FAIL(data_buffer_.write(buf, len))) {
    LOG_WARN("failed to write file writer", K(ret), K(buf), K(len));
  } else if (OB_FAIL(flush_buffer_(is_last_part))) {
    LOG_WARN("failed to flush buffer", K(ret), K(is_last_part));
  }
  return ret;
}

bool ObBackupFileWriteCtx::check_can_flush_(const bool is_last_part) const
{
  return (is_last_part && data_buffer_.length() > 0) || data_buffer_.length() >= OB_MAX_BACKUP_MEM_BUF_LEN;
}

int ObBackupFileWriteCtx::flush_buffer_(const bool is_last_part)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  const int64_t offset = file_size_;
  common::ObBackupIoAdapter io_adapter;
  if (!check_can_flush_(is_last_part)) {
    LOG_DEBUG("can not flush now", K(is_last_part), K(data_buffer_));
  } else if (OB_ISNULL(dev_handle_) || OB_ISNULL(bandwidth_throttle_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dev handle should not be null", K(ret));
  } else if (OB_FAIL(io_adapter.pwrite(*dev_handle_, io_fd_, data_buffer_.data(), offset,
                                       data_buffer_.length(), write_size, false/*is_can_seal*/))) {
    LOG_WARN("failed to write data buffer", K(ret), K(data_buffer_));
  } else if (data_buffer_.length() != write_size) {
    ret = OB_IO_ERROR;
    LOG_WARN("write length not equal buffer length", K(offset), K(data_buffer_.length()), K(write_size));
  } else {
    int64_t bytes = write_size;
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_BACKUP_TRIGGER_BANDWIDTH_THROTTLE ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_BACKUP_TRIGGER_BANDWIDTH_THROTTLE", K(ret));
        // in case of errsim, that the bytes be larger to trigger bandwidth throttle
        bytes = write_size * 100;
        ret = OB_SUCCESS;
      }
    }
#endif
    if (OB_FAIL(bandwidth_throttle_->limit_out_and_sleep(bytes, last_active_time_, INT64_MAX))) {
      LOG_WARN("failed to limit out and sleep", K(ret));
    } else {
      file_size_ += write_size;
      data_buffer_.reuse();
      last_active_time_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

int ObBackupFileWriteCtx::commit_file_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dev_handle_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dev handle should not be null", K(ret));
  } else if (OB_FAIL(flush_buffer_(true /*is_last_part*/))) {
    LOG_WARN("failed to flush buffer", K(ret));
  } else {
    LOG_INFO("backup file write ctx commit file");
  }
  return ret;
}

}  // namespace backup
}  // namespace oceanbase
