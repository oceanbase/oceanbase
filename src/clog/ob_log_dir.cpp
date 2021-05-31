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

#include <sys/types.h>
#include <dirent.h>
#include <sys/vfs.h>
#include <unistd.h>  // close()
#include "lib/file/ob_file.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/utility/ob_tracepoint.h"
#include "common/log/ob_log_data_writer.h"
#include "ob_log_common.h"
#include "ob_clog_mgr.h"
#include "ob_log_dir.h"

namespace oceanbase {
using namespace common;
namespace clog {
// Judge whether the file name is only composed of numbers
// 1. The file name is not 0 and does not start with 0
// 2. The maximum length allowed for the file name definition is 512B, but it is finally converted to int64_t type, and
// an overflow error is reported
static bool is_number(const char* str)
{
  bool bool_ret = true;
  if (OB_UNLIKELY(NULL == str) || OB_UNLIKELY('\0' == (*str)) || OB_UNLIKELY('0' == (*str))) {
    bool_ret = false;
  } else {
    while ((*str) != '\0') {
      if (!isdigit(*str)) {
        bool_ret = false;
        break;
      }
      str++;
    }
  }
  return bool_ret;
}

static bool is_special_dir(const char* dir_name)
{
  bool bool_ret = false;
  if (OB_UNLIKELY(NULL == dir_name)) {
    bool_ret = true;
    int tmp_ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "dir_name is null", K(tmp_ret));
  } else if (0 == STRNCMP(".", dir_name, strnlen(dir_name, MAX_PATH_SIZE))) {
    bool_ret = true;
  } else if (0 == STRNCMP("..", dir_name, strnlen(dir_name, MAX_PATH_SIZE))) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

static bool is_valid_file_id(const char* str, file_id_t& cur_file_id)
{
  bool bool_ret = true;
  if (OB_ISNULL(str)) {
    bool_ret = false;
    CLOG_LOG(WARN, "file name string is null");
  } else if (!is_number(str)) {
    bool_ret = false;
    if (0 == STRNCMP(str, "shm", 3) || 0 == STRNCMP(str, "user", 4) || 0 == STRNCMP(str, "system", 6)) {
      // shm_stamp, user or system dir, ignore
    } else {
      CLOG_LOG(WARN, "file name is not a number", "file name", str);
    }
  } else if (OB_INVALID_FILE_ID ==
             (cur_file_id = static_cast<file_id_t>(strtol(str, NULL, 10)))) {  // converted to decimal
    bool_ret = false;
    CLOG_LOG(WARN, "file name overflow", "file name", str);
  } else {
  }
  return bool_ret;
}

static bool is_tmp_file(const char* str)
{
  bool bool_ret = false;
  if (NULL != str && NULL != strstr(str, TMP_SUFFIX)) {
    bool_ret = true;
  }
  return bool_ret;
}

int ObDir::init(const char* dir_name)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (NULL == dir_name) {
    ret = OB_INVALID_ARGUMENT;
  } else if (0 != mkdir(dir_name, MKDIR_MODE) && EEXIST != errno) {
    ret = OB_IO_ERROR;
    CLOG_LOG(ERROR, "mkdir error", K(ret), K(dir_name), KERRMSG);
  } else if (OB_UNLIKELY(0 > (dir_fd_ = open(dir_name, OPEN_MODE)))) {
    ret = OB_IO_ERROR;
    CLOG_LOG(ERROR, "open directory error", K(dir_fd_), K(dir_name), KERRMSG);
  } else {
    dir_name_[sizeof(dir_name_) - 1] = '\0';
    (void)snprintf(dir_name_, sizeof(dir_name_) - 1, dir_name);
    is_inited_ = true;
    CLOG_LOG(INFO, "init log dir success", K(*this));
  }
  return ret;
}

void ObDir::destroy()
{
  if (is_inited_) {
    if (dir_fd_ >= 0) {
      int close_ret = close(dir_fd_);
      if (0 != close_ret) {
        CLOG_LOG(WARN, "close log dir error", K(close_ret), K(dir_name_), K(is_inited_), K(errno), KERRMSG);
      }
    }

    dir_name_[0] = '\0';
    is_inited_ = false;
  }
}

bool ObILogDir::GetFileIdRange::operator()(const char* dir_name, const char* entry)
{
  int ret = OB_SUCCESS;
  if (NULL == dir_name || NULL == entry) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(dir_name), KP(entry));
  } else {
    file_id_t cur_file_id = OB_INVALID_FILE_ID;
    if (is_special_dir(entry)) {
      CLOG_LOG(DEBUG, "skip special dir", K(dir_name), K(entry));
    } else if (is_tmp_file(entry) || !is_valid_file_id(entry, cur_file_id)) {
    } else {
      if (OB_INVALID_FILE_ID == min_file_id_ || cur_file_id < min_file_id_) {
        min_file_id_ = cur_file_id;
      }
      if (OB_INVALID_FILE_ID == max_file_id_ || cur_file_id > max_file_id_) {
        max_file_id_ = cur_file_id;
      }
    }
  }
  return (OB_SUCCESS == ret);
}

bool ObILogDir::GetFileSizeFunctor::operator()(const char* dir_name, const char* entry)
{
  int ret = OB_SUCCESS;
  if (NULL == dir_name || NULL == entry) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), KP(dir_name), KP(entry));
  } else {
    file_id_t cur_file_id = OB_INVALID_FILE_ID;
    int64_t file_size = 0;
    if (is_special_dir(entry)) {
      CLOG_LOG(DEBUG, "skip special dir", K(dir_name), K(entry));
    } else if (is_tmp_file(entry) || !is_valid_file_id(entry, cur_file_id)) {
      // do nothing
    } else if (OB_FAIL(get_size_(cur_file_id, file_size))) {
      CLOG_LOG(ERROR, "get_size_ failed", K(ret), K(cur_file_id));
    } else {
      total_size_ += file_size;
    }
  }
  return (OB_SUCCESS == ret);
}

int ObILogDir::GetFileSizeFunctor::get_size_(const file_id_t file_id, int64_t& size)
{
  int ret = OB_SUCCESS;
  int dir_fd = -1;
  int fd = -1;
  char fname[MAX_PATH_SIZE];
  struct stat stat;
  const int OPEN_FLAG = O_RDONLY | O_DIRECT;
  const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  if (!is_valid_file_id(file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret));
  } else if (OB_FAIL(log_dir_->get_path(file_id, fname, sizeof(fname), dir_fd))) {
    CLOG_LOG(ERROR, "get_path failed", K(ret));
  } else if ((fd = openat(dir_fd, fname, OPEN_FLAG, OPEN_MODE)) < 0) {
    if (ENOENT == errno) {
      ret = OB_SUCCESS;
      size = 0;
    } else {
      ret = OB_IO_ERROR;
      CLOG_LOG(ERROR, "openat failed", K(ret), K(fname), K(errno));
    }
  } else if (0 != fstat(fd, &stat)) {
    if (ENOENT == errno) {
      ret = OB_SUCCESS;
      size = 0;
    } else {
      ret = OB_IO_ERROR;
      CLOG_LOG(ERROR, "fstat failed", K(ret), K(fname), K(errno));
    }
  } else {
    size = stat.st_size;
  }
  if (fd > 0) {
    int close_ret = close_fd(fd);
    if (OB_SUCCESS != close_ret) {
      CLOG_LOG(ERROR, "close fd failed", K(ret), K(file_id));
    }
  }
  return ret;
}

int ObLogDir::init(const char* dir_name)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (NULL == dir_name) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KP(dir_name));
  } else if (OB_FAIL(dir_.init(dir_name))) {
    CLOG_LOG(WARN, "dir init failed", K(ret), K(dir_name));
  } else {
    is_inited_ = true;
    CLOG_LOG(INFO, "log dir init success", K(*this));
  }
  return ret;
}

void ObLogDir::destroy()
{
  dir_.destroy();
  is_inited_ = false;
}

int ObLogDir::get_file_id_range(file_id_t& min_file_id, file_id_t& max_file_id) const
{
  int ret = OB_SUCCESS;
  GetFileIdRange fn;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    if (OB_FAIL(dir_.for_each(fn))) {
      CLOG_LOG(WARN, "iterate log dir fail", K(ret), K(*this));
    } else {
      min_file_id = fn.get_min_file_id();
      max_file_id = fn.get_max_file_id();
    }
    if (OB_INVALID_FILE_ID == min_file_id || OB_INVALID_FILE_ID == max_file_id) {
      ret = OB_ENTRY_NOT_EXIST;
      if (REACH_TIME_INTERVAL(1000 * 1000L)) {
        CLOG_LOG(INFO, "log dir is empty", K(ret), K(*this));
      }
    }
  }
  return ret;
}

int ObLogDir::get_min_file_id(file_id_t& file_id) const
{
  int ret = OB_SUCCESS;
  file_id_t min_file_id = OB_INVALID_FILE_ID;
  file_id_t max_file_id = OB_INVALID_FILE_ID;
  if (OB_FAIL(get_file_id_range(min_file_id, max_file_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      CLOG_LOG(WARN, "get file id range fail", K(ret), K(*this));
    }
  } else {
    file_id = min_file_id;
  }
  return ret;
}

int ObLogDir::get_max_file_id(file_id_t& file_id) const
{
  int ret = OB_SUCCESS;
  file_id_t min_file_id = OB_INVALID_FILE_ID;
  file_id_t max_file_id = OB_INVALID_FILE_ID;
  if (OB_FAIL(get_file_id_range(min_file_id, max_file_id))) {
    CLOG_LOG(WARN, "get file id range fail", K(ret), K(*this));
  } else {
    file_id = max_file_id;
  }
  return ret;
}

int ObLogDir::get_path(const file_id_t file_id, char* path, const int64_t size, int& dir_fd) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    dir_fd = dir_.get_dir_fd();
    (void)snprintf(path, size, "%d", file_id);
  }
  return ret;
}

int ObLogDir::get_tmp_path(const file_id_t file_id, char* path, const int64_t size, int& dir_fd) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (NULL == path || 0 >= size) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    dir_fd = dir_.get_dir_fd();
    (void)snprintf(path, size, "%d%s", file_id, TMP_SUFFIX);
  }
  return ret;
}

int ObLogDir::get_total_size(int64_t& total_size) const
{
  int ret = OB_SUCCESS;
  GetFileSizeFunctor fn(this);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "get_total_size failed", K(ret));
  } else if (OB_FAIL(dir_.for_each(fn))) {
    CLOG_LOG(ERROR, "dir_ for_each failed", K(ret));
  } else {
    total_size = fn.get_total_size();
  }
  return ret;
}
}  // end namespace clog
}  // end namespace oceanbase
