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

#include "ob_async_log_struct.h"
#include "lib/objectpool/ob_concurrency_objpool.h"


namespace oceanbase
{
namespace common
{
ObPLogItem::ObPLogItem()
  : ObIBaseLogItem(), fd_type_(MAX_FD_FILE),
    log_level_(OB_LOG_LEVEL_NONE), tl_type_(common::OB_INVALID_INDEX), is_force_allow_(false),
    is_size_overflow_(false), timestamp_(0), header_pos_(0), buf_size_(0), pos_(0)
{
}

ObPLogFileStruct::ObPLogFileStruct()
  : fd_(STDERR_FILENO), wf_fd_(STDERR_FILENO), write_count_(0), write_size_(0),
    file_size_(0)
{
  filename_[0] = '\0';
  MEMSET(&stat_, 0, sizeof(stat_));
  MEMSET(&wf_stat_, 0, sizeof(wf_stat_));
}

int ObPLogFileStruct::open(const char *file_name, const bool open_wf_flag, const bool redirect_flag)
{
  int ret = OB_SUCCESS;
  size_t fname_len = 0;
  if (OB_ISNULL(file_name)) {
    LOG_STDERR("invalid argument log_file = %p\n", file_name);
    ret = OB_INVALID_ARGUMENT;
  //need care .wf.??
  } else if (OB_UNLIKELY((fname_len = strlen(file_name)) > MAX_LOG_FILE_NAME_SIZE - 5)) {
    LOG_STDERR("fname' size is overflow, log_file = %p\n", file_name);
    ret = OB_SIZE_OVERFLOW;
  } else {
    if (OB_UNLIKELY(is_opened())) {
      LOG_STDOUT("old log_file need close, old = %s new = %s\n", filename_, file_name);
    }
    MEMCPY(filename_, file_name, fname_len);
    filename_[fname_len] = '\0';
    if (OB_FAIL(reopen(redirect_flag))) {
      LOG_STDERR("reopen error, ret= %d\n", ret);
    } else if (open_wf_flag && OB_FAIL(reopen_wf())) {
      LOG_STDERR("reopen_wf error, ret= %d\n", ret);
    } else {
      LOG_STDERR("succ to open, filename=%s, fd=%d, wf_fd=%d\n", filename_, fd_, wf_fd_);
    }
  }
  return ret;
}


int ObPLogFileStruct::reopen(const bool redirect_flag)
{
  int ret = OB_SUCCESS;
  int32_t tmp_fd = -1;
  if (OB_UNLIKELY(strlen(filename_) <= 0)) {
    LOG_STDERR("invalid argument log_file = %p\n", filename_);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY((tmp_fd = ::open(filename_, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, LOG_FILE_MODE)) < 0)) {
    LOG_STDERR("open file = %s errno = %d error = %m\n", filename_, errno);
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(0 != fstat(tmp_fd, &stat_))) {
    LOG_STDERR("fstat file = %s error\n", filename_);
    ret = OB_ERR_UNEXPECTED;
    (void)close(tmp_fd);
    tmp_fd = -1;
  } else {
    if (redirect_flag) {
      (void)dup2(tmp_fd, STDERR_FILENO);
      (void)dup2(tmp_fd, STDOUT_FILENO);

      if (fd_ > STDERR_FILENO) {
        (void)dup2(tmp_fd, fd_);
        (void)close(tmp_fd);
      } else {
        fd_ = tmp_fd;
      }
    } else {
      if (fd_ > STDERR_FILENO) {
        (void)dup2(tmp_fd, fd_);
        (void)close(tmp_fd);
      } else {
        fd_ = tmp_fd;
      }
    }
    file_size_ = stat_.st_size;
  }
  return ret;
}

int ObPLogFileStruct::reopen_wf()
{
  int ret = OB_SUCCESS;
  int32_t tmp_fd = -1;
  if (OB_UNLIKELY(strlen(filename_) <= 0)) {
    LOG_STDERR("invalid argument log_file = %p\n", filename_);
    ret = OB_INVALID_ARGUMENT;
  } else {
    char tmp_file_name[MAX_LOG_FILE_NAME_SIZE];
    (void)snprintf(tmp_file_name, sizeof(tmp_file_name), "%s.wf", filename_);
    if (OB_UNLIKELY((tmp_fd = ::open(tmp_file_name, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, LOG_FILE_MODE)) < 0)) {
      LOG_STDERR("open file = %s errno = %d error = %m\n", tmp_file_name, errno);
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_UNLIKELY(0 != fstat(tmp_fd, &wf_stat_))) {
      LOG_STDERR("fstat file = %s error\n", tmp_file_name);
      ret = OB_ERR_UNEXPECTED;
      (void)close(tmp_fd);
      tmp_fd = -1;
    } else {
      if (wf_fd_ > STDERR_FILENO) {
        (void)dup2(tmp_fd, wf_fd_);
        (void)close(tmp_fd);
      } else {
        wf_fd_ = tmp_fd;
      }
    }
  }
  return ret;
}

int ObPLogFileStruct::close_all()
{
  int ret = OB_SUCCESS;
  if (fd_ > STDERR_FILENO) {
    (void)close(fd_);
    fd_ = STDERR_FILENO;
  }
  if (wf_fd_ > STDERR_FILENO) {
    (void)close(wf_fd_);
    wf_fd_ = STDERR_FILENO;
  }
  return ret;
}

} // end common
} // end oceanbase
