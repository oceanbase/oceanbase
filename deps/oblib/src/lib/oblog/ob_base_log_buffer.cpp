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

#include "ob_base_log_buffer.h"
#include <string.h>
#include <errno.h>
#include <new>
#include <sys/mman.h>
#include <sys/sysinfo.h>
#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_utility.h"
#include "lib/oblog/ob_log.h"
#include "lib/file/file_directory_utils.h"

namespace oceanbase {
namespace common {
ObBaseLogBufferMgr::ObBaseLogBufferMgr() : log_buf_cnt_(0)
{}

ObBaseLogBufferMgr::~ObBaseLogBufferMgr()
{
  destroy();
}

ObBaseLogBufferMgr& ObBaseLogBufferMgr::get_instance()
{
  static ObBaseLogBufferMgr instance_;
  return instance_;
}

void ObBaseLogBufferMgr::destroy()
{
  for (int64_t i = 0; i < log_buf_cnt_; ++i) {
    if (NULL != log_ctrls_[i].base_buf_) {
      munmap(log_ctrls_[i].base_buf_, SHM_BUFFER_SIZE);
      log_ctrls_[i].base_buf_ = NULL;
      log_ctrls_[i].data_buf_ = NULL;
    }
  }
  log_buf_cnt_ = 0;
}

int ObBaseLogBufferMgr::get_buffer(const char* buf_file_path, ObBaseLogBufferCtrl*& log_ctrl)
{
  int ret = OB_SUCCESS;
  struct sysinfo sys_info;
  int fd = -1;
  const char* abs_file_path = NULL;
  bool is_exist = false;
  void* map_buf = NULL;
  ObBaseLogBuffer* log_buf = NULL;

  if (OB_FAIL(get_abs_dir(buf_file_path, abs_file_path))) {
    LIB_LOG(WARN, "Fail to get abs dir, ", K(ret));
  } else {
    lib::ObMutexGuard guard(mutex_);
    int64_t i = 0;
    for (i = 0; i < log_buf_cnt_; ++i) {
      if (NULL != log_ctrls_[i].base_buf_ && 0 == STRCMP(log_ctrls_[i].base_buf_->log_dir_, abs_file_path)) {
        // found
        log_ctrl = &(log_ctrls_[i]);
        break;
      }
    }

    // not found
    if (i == log_buf_cnt_) {
      if (i >= MAX_LOG_BUF_CNT) {
        ret = OB_SIZE_OVERFLOW;
        LIB_LOG(WARN, "There are too many log bufs, ", K(ret), K(i));
      } else if (STRLEN(abs_file_path) >= ObBaseLogBuffer::MAX_LOG_DIR_LENGTH) {
        ret = OB_SIZE_OVERFLOW;
        LIB_LOG(WARN, "shm file name too long", K(ret), K(abs_file_path));
      } else {
        if (OB_FAIL(FileDirectoryUtils::is_exists(abs_file_path, is_exist))) {
          LIB_LOG(WARN, "Fail to check if file exist, ", K(ret), K(abs_file_path));
        } else {
          if (is_exist) {
            // shm file is exist
            if ((fd = ::open(abs_file_path, O_RDWR)) < 0) {
              ret = OB_IO_ERROR;
              LIB_LOG(ERROR, "open shm file error, ", K(ret), K(abs_file_path), K(errno), KERRMSG);
            } else if (MAP_FAILED ==
                       (map_buf = mmap(NULL, SHM_BUFFER_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0))) {
              ret = OB_ERR_SYS;
              LIB_LOG(ERROR, "Fail to mmap buf, ", K(ret), K(errno), KERRMSG);
            } else if (0 != sysinfo(&sys_info)) {
              ret = OB_ERR_SYS;
              LIB_LOG(WARN, "Fail to get sys info, ", K(ret), K(errno), KERRMSG);
            } else {
              log_buf = reinterpret_cast<ObBaseLogBuffer*>(map_buf);
              LIB_LOG(INFO,
                  "Old shm buf info, ",
                  K(log_buf->log_dir_),
                  K(log_buf->file_flush_pos_),
                  K(log_buf->file_write_pos_),
                  K(log_buf->shm_stamp_));
              if (ObTimeUtility::current_time() / 1000000 - log_buf->shm_stamp_ >= sys_info.uptime) {
                // server has been restarted
                STRNCPY(log_buf->log_dir_, abs_file_path, sizeof(log_buf->log_dir_) - 1);
                log_buf->buf_len_ = MAX_LOG_BUFFER_SIZE;
                log_buf->file_flush_pos_.atomic_ = 0;
                log_buf->file_write_pos_.atomic_ = 0;
                log_buf->shm_stamp_ = ObTimeUtility::current_time() / 1000000;
              }
            }
          } else {
            // need create new shm file
            if ((fd = ::open(abs_file_path, O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) < 0) {
              ret = OB_IO_ERROR;
              LIB_LOG(ERROR, "open shm file error", K(ret), K(abs_file_path), K(errno), KERRMSG);
            } else if (0 != ftruncate(fd, SHM_BUFFER_SIZE)) {
              ret = OB_IO_ERROR;
              LIB_LOG(ERROR, "Fail to ftruncate shm file, ", K(ret));
            } else if (MAP_FAILED ==
                       (map_buf = mmap(NULL, SHM_BUFFER_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0))) {
              ret = OB_ERR_SYS;
              LIB_LOG(ERROR, "Fail to mmap buf, ", K(ret), K(errno), KERRMSG);
            } else {
              MEMSET(map_buf, 0, SHM_BUFFER_SIZE);
              log_buf = reinterpret_cast<ObBaseLogBuffer*>(map_buf);
              STRNCPY(log_buf->log_dir_, abs_file_path, sizeof(log_buf->log_dir_) - 1);
              log_buf->buf_len_ = MAX_LOG_BUFFER_SIZE;
              log_buf->file_flush_pos_.atomic_ = 0;
              log_buf->file_write_pos_.atomic_ = 0;
              log_buf->shm_stamp_ = ObTimeUtility::current_time() / 1000000;
            }
          }

          if (OB_SUCC(ret)) {
            log_ctrl = &(log_ctrls_[log_buf_cnt_]);
            log_ctrls_[log_buf_cnt_].base_buf_ = log_buf;
            log_ctrls_[log_buf_cnt_].data_buf_ = reinterpret_cast<char*>(map_buf) + SHM_HEADER_SIZE;
            log_buf_cnt_++;
          }
          if (fd >= 0) {
            ::close(fd);
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(map_buf) && MAP_FAILED != map_buf) {
      munmap(map_buf, SHM_BUFFER_SIZE);
    }
    log_ctrl = NULL;
  }
  if (NULL != abs_file_path && buf_file_path != abs_file_path) {
    free((void*)abs_file_path);
    abs_file_path = NULL;
  }
  return ret;
}

int ObBaseLogBufferMgr::get_abs_dir(const char* log_dir, const char*& abs_log_dir)
{
  int ret = OB_SUCCESS;
  if (NULL == log_dir || strlen(log_dir) >= ObBaseLogBuffer::MAX_LOG_DIR_LENGTH || strlen(log_dir) <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Invalid argument, ", K(ret), KP(log_dir));
  } else {
    if ('/' == log_dir[0]) {
      // absolute dir
      abs_log_dir = log_dir;
    } else if (NULL == (abs_log_dir = realpath(log_dir, NULL))) {
      ret = OB_ERR_SYS;
      LIB_LOG(WARN, "Fail to get real path, ", K(ret), KERRMSG);
    } else if (strlen(abs_log_dir) >= ObBaseLogBuffer::MAX_LOG_DIR_LENGTH) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "The log path is too long, ", K(ret), K(log_dir));
    }

    if (OB_SUCC(ret)) {
      LIB_LOG(INFO, "Success to get abs dir, ", K(abs_log_dir));
    }
  }
  return ret;
}

}  // namespace common
}  // namespace oceanbase
