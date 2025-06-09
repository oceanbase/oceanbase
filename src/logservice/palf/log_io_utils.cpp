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
#include "log_io_utils.h"
#include <linux/falloc.h> // FALLOC_FL_ZERO_RANGE for linux kernel 3.15
#include "log_block_pool_interface.h"
#include "share/ob_errno.h"
#include "logservice/ob_log_io_adapter.h"
namespace oceanbase
{
namespace palf
{

const int64_t RETRY_INTERVAL = 10*1000;

int open_with_retry(const char *pathname,
                    const int flags,
                    const mode_t mode,
                    ObIOFd &fd,
                    ObIODevice *io_device)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pathname) || OB_ISNULL(io_device)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KP(pathname), KP(io_device));
  } else {
    do {
      if (OB_FAIL(io_device->open(pathname, flags, mode, fd))) {
        PALF_LOG(WARN, "open file failed", KR(ret), K(pathname));
        ob_usleep(RETRY_INTERVAL);
      } else {
        fd.device_handle_ = io_device;
        PALF_LOG(INFO, "open successfully", K(pathname), K(fd));
      }
    } while(OB_LOG_STORE_EPOCH_CHANGED == ret);
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
  } else if (OB_FAIL(palf::is_exists(file_name, exist))){
    PALF_LOG(WARN, "is_exists failed.", KCSTRING(file_name), K(ret));
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

int rename_with_retry(const char *src_name,
                      const char *dest_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_name) || OB_ISNULL(dest_name)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KP(src_name), KP(dest_name));
  } else {
    do {
      if (OB_FAIL(LOG_IO_ADAPTER.rename(src_name, dest_name))) {
        PALF_LOG(WARN, "rename file failed", KR(ret), K(src_name), K(dest_name));
        // for xfs, source file not exist and dest file exist after rename return ENOSPC, therefore, next rename will return
        // OB_NO_SUCH_FILE_OR_DIRECTORY.
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret && check_rename_success(src_name, dest_name)) {
          ret = OB_SUCCESS;
          PALF_LOG(WARN, "rename file failed, source file not exist, return OB_SUCCESS.", K(src_name), K(dest_name));
        } else {
          ob_usleep(RETRY_INTERVAL);
        }
      }
    } while(OB_FAIL(ret));
  }
  return ret;
}

int fsync_with_retry(const char *dir_name)
{
  int ret = OB_SUCCESS;
  do {
    ObIOFd dir_fd;
    if (OB_FAIL(LOG_IO_ADAPTER.open(dir_name, O_RDONLY, FILE_OPEN_MODE, dir_fd))) {
      CLOG_LOG(WARN, "open failed", K(ret), K(dir_name));
      ob_usleep(RETRY_INTERVAL);
    } else if (OB_FAIL(LOG_IO_ADAPTER.fsync(dir_fd))) {
      CLOG_LOG(WARN, "fsync dest dir failed", K(ret), K(dir_fd));
      ob_usleep(RETRY_INTERVAL);
    } else {
      ret = OB_SUCCESS;
      CLOG_LOG(TRACE, "fsync_until_success_ success", K(ret), K(dir_fd));
    }
    if (dir_fd.is_valid()) {
      LOG_IO_ADAPTER.close(dir_fd);
    }
  } while (OB_FAIL(ret));
  return ret;

}

int fsync_with_retry(const char *path, ObIODevice *io_device)
{
  int ret = OB_SUCCESS;
  do {
    ObIOFd io_fd;
    if (OB_FAIL(io_device->open(path, LOG_WRITE_FLAG, FILE_OPEN_MODE, io_fd))) {
      CLOG_LOG(WARN, "open failed", K(ret), K(path));
      ob_usleep(RETRY_INTERVAL);
    } else if (OB_FAIL(io_device->fsync(io_fd))) {
      CLOG_LOG(WARN, "fsync dest dir failed", K(ret), K(io_fd));
      ob_usleep(RETRY_INTERVAL);
    } else {
      ret = OB_SUCCESS;
      CLOG_LOG(INFO, "fsync_until_success_ success", K(ret), K(io_fd));
    }
    if (io_fd.is_valid()) {
      io_device->close(io_fd);
    }
  } while (OB_FAIL(ret));
  return ret;

}

int fsync(const char *path, ObIODevice *io_device)
{
  int ret = OB_SUCCESS;
  ObIOFd io_fd;
  if (OB_FAIL(io_device->open(path, LOG_WRITE_FLAG, FILE_OPEN_MODE, io_fd))) {
    CLOG_LOG(WARN, "open failed", K(ret), K(path));
  } else if (OB_FAIL(io_device->fsync(io_fd))) {
    CLOG_LOG(WARN, "fsync failed", K(ret), K(io_fd));
  } else {
    CLOG_LOG(INFO, "fsync success", K(ret), K(io_fd));
  }
  if (io_fd.is_valid()) {
    io_device->close(io_fd);
  }
  return ret;

}

int write_until_success(const char *pathname,
                        const char *src_buf,
                        const int64_t src_buf_len,
                        const int64_t offset)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  int64_t time_interval = OB_INVALID_TIMESTAMP;
  do {
    ObIOFd io_fd;
    if (OB_FAIL(LOG_IO_ADAPTER.open(pathname, LOG_WRITE_FLAG, FILE_OPEN_MODE, io_fd))) {
      CLOG_LOG(WARN, "open file failed", K(pathname));
      ob_usleep(RETRY_INTERVAL);
    } else if (OB_FAIL(LOG_IO_ADAPTER.pwrite(io_fd, src_buf, src_buf_len, offset, write_size))) {
      if (palf_reach_time_interval(1000 * 1000, time_interval)) {
        CLOG_LOG(WARN, "ob_pwrite failed", K(ret), K(offset), K(write_size));
      }
      ob_usleep(RETRY_INTERVAL);
    } else {
      ret = OB_SUCCESS;
    }
    if (io_fd.is_valid()) {
      LOG_IO_ADAPTER.close(io_fd);
    }
  } while (OB_LOG_STORE_EPOCH_CHANGED == ret);
  return ret;
}

int read_until_success(const char *pathname, char *dest_buf,
                       const int64_t dest_buf_len,
                       const int64_t offset)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  int64_t time_interval = OB_INVALID_TIMESTAMP;
  do {
    ObIOFd io_fd;
    if (OB_FAIL(LOG_IO_ADAPTER.open(pathname, LOG_READ_FLAG, FILE_OPEN_MODE, io_fd))) {
      CLOG_LOG(WARN, "open failed", K(ret), K(offset), K(read_size));
      ob_usleep(RETRY_INTERVAL);
    } else if (OB_FAIL(LOG_IO_ADAPTER.pread(io_fd, dest_buf_len, offset, dest_buf, read_size))) {
      if (palf_reach_time_interval(1000 * 1000, time_interval)) {
        CLOG_LOG(WARN, "pread failed", K(ret), K(offset), K(read_size));
      }
      ob_usleep(RETRY_INTERVAL);
    } else {
      ret = OB_SUCCESS;
    }
    if (io_fd.is_valid()) {
      LOG_IO_ADAPTER.close(io_fd);
    }
  } while (OB_LOG_STORE_EPOCH_CHANGED == ret);
  return ret;
}

int read_until_success(const ObIOFd &io_fd, char *dest_buf,
                       const int64_t dest_buf_len,
                       const int64_t offset,
                       int64_t &read_size)
{
  int ret = OB_SUCCESS;
  ObIODevice *io_device = io_fd.device_handle_;
  if (!io_fd.is_valid() || OB_ISNULL(dest_buf) || 0 >= dest_buf_len || 0 > offset || OB_ISNULL(io_device)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(io_fd), KP(dest_buf), K(dest_buf_len), K(offset), K(read_size), K(io_fd));
  } else if (OB_FAIL(io_device->pread(io_fd, offset, dest_buf_len, dest_buf, read_size))) {
    CLOG_LOG(WARN, "pread failed", K(ret), K(offset), K(read_size), K(io_fd));
  } else {
    CLOG_LOG(INFO, "pread success", K(ret), K(offset), K(read_size), K(io_fd));
  }
  return ret;
}

int mkdir(const char *path)
{
  int ret = OB_SUCCESS;
  do {
    if (OB_FAIL(LOG_IO_ADAPTER.mkdir(path, CREATE_DIR_MODE))) {
      PALF_LOG(WARN, "open failed", K(ret), K(path));
      ob_usleep(RETRY_INTERVAL);
    } else {
      ret = OB_SUCCESS;
      PALF_LOG(INFO, "mkdir success", K(ret), K(path));
    }
  } while (OB_LOG_STORE_EPOCH_CHANGED == ret);
  return ret;
}

int rmdir(const char *path)
{
  int ret = OB_SUCCESS;
  do {
    if (OB_FAIL(LOG_IO_ADAPTER.rmdir(path))) {
      PALF_LOG(WARN, "open failed", K(ret), K(path));
      ob_usleep(RETRY_INTERVAL);
    } else {
      ret = OB_SUCCESS;
      PALF_LOG(INFO, "mkdir success", K(ret), K(path));
    }
  } while (OB_LOG_STORE_EPOCH_CHANGED == ret);
  return ret;
}

int scan_dir(const char *dir_name, ObBaseDirFunctor &functor)
{
  int ret = OB_SUCCESS;
  do {
    if (OB_FAIL(LOG_IO_ADAPTER.scan_dir(dir_name, functor))) {
      PALF_LOG(WARN, "open failed", K(ret), K(dir_name));
      ob_usleep(RETRY_INTERVAL);
    } else {
      ret = OB_SUCCESS;
      PALF_LOG(INFO, "mkdir success", K(ret), K(dir_name));
    }
  } while (OB_LOG_STORE_EPOCH_CHANGED == ret);
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
        PALF_LOG(WARN, "rename_flashback_to_normal failed", K(ret), K(dir_), K(entry_name));
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
  char abnormal_file_name[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char normal_file_name[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  MEMCPY(normal_file_name, file_name, strlen(file_name) - strlen(FLASHBACK_SUFFIX));
  const int64_t SLEEP_TS_US = 10 * 1000;
  int pos = 0;
  if (OB_FAIL(databuff_printf(abnormal_file_name, OB_MAX_FILE_NAME_LENGTH, "%s/%s", dir_, file_name))) {
    PALF_LOG(ERROR, "databuff_printf failed", KR(ret), K(file_name));
  } else if (OB_FAIL(databuff_printf(normal_file_name, OB_MAX_FILE_NAME_LENGTH, "%s/%s", dir_, file_name))) {
    PALF_LOG(ERROR, "databuff_printf failed", KR(ret), K(file_name));
  } else if (FALSE_IT(pos = strlen(normal_file_name)-strlen(FLASHBACK_SUFFIX))) {
  } else if (FALSE_IT(normal_file_name[pos] = '\0')) {
  } else if (OB_FAIL(try_to_remove_block_(normal_file_name))) {
    PALF_LOG(ERROR, "try_to_remove_block_ failed", K(file_name), K(normal_file_name));
  } else if (OB_FAIL(rename_with_retry(abnormal_file_name, normal_file_name))) {
    PALF_LOG(ERROR, "rename_with_retry failed", K(file_name), K(normal_file_name));
  } else {}

  return ret;
}

int TrimLogDirectoryFunctor::try_to_remove_block_(const char *file_name)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  if (OB_FAIL(palf::is_exists(file_name, exist))) {
    PALF_LOG(WARN, "open file failed", KR(ret), K(file_name));
  } else if (!exist) {
    PALF_LOG(INFO, "before rename flashback to normal and after delete normal file, restart!!!", K(file_name));
  } else if (OB_FAIL(log_block_pool_->remove_block(file_name))) {
    PALF_LOG(WARN, "remove_block_at failed", K(file_name));
  }
  return ret;
}

ListDir::ListDir(const char *base_dir) :
  alloc_(ObModIds::OB_MODULE_PAGE_ALLOCATOR,
         OB_MALLOC_NORMAL_BLOCK_SIZE)

{
  strncpy(base_dir_, base_dir, OB_MAX_FILE_NAME_LENGTH);
}

ListDir::~ListDir()
{}

int ListDir::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  char pathname[OB_MAX_FILE_NAME_LENGTH] = {0};
  bool is_dir = false;
  if (NULL == entry) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "unexpected error", K(ret), KP(entry));
  } else if (0 >= snprintf(pathname, OB_MAX_FILE_NAME_LENGTH, "%s/%s", base_dir_, entry->d_name)) {
  } else if (OB_FAIL(FileDirectoryUtils::is_directory(pathname, is_dir))) {
  } else if (is_dir) {
    const int size = strlen(entry->d_name);
    char *dptr = static_cast<char*>(alloc_.alloc(size+1));
    dptr[size] = '\0';
    if (NULL == dptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PALF_LOG(WARN, "allocate memory failed", KR(ret));
    } else if (FALSE_IT(strncpy(dptr, entry->d_name, size))) {
    } else if (OB_FAIL(contents_.push_back(dptr))) {
      PALF_LOG(WARN, "push_back failed", KR(ret), K(dptr));
    } else {
      PALF_LOG(TRACE, "push dir name into se array success", K(dptr), K(size), K(pathname));
    }
  } else {
  }
  return ret;
}

int reuse_block(const char *block_path)
{
  int ret = OB_SUCCESS;
  do {
    ObIOFd io_fd;
    if (OB_FAIL(LOG_IO_ADAPTER.open(block_path, LOG_WRITE_FLAG, FILE_OPEN_MODE, io_fd))) {
      PALF_LOG(WARN, "open failed", K(ret), K(block_path));
      ob_usleep(RETRY_INTERVAL);
    } else if (OB_FAIL(LOG_IO_ADAPTER.fallocate(io_fd, FALLOC_FL_ZERO_RANGE, 0, PALF_PHY_BLOCK_SIZE))) {
      PALF_LOG(WARN, "fallocate failed", K(ret), K(block_path));
      ob_usleep(RETRY_INTERVAL);
    } else {
      ret = OB_SUCCESS;
      PALF_LOG(INFO, "reuse_block success", K(ret), K(block_path));
    }
    if (io_fd.is_valid()) {
      LOG_IO_ADAPTER.close(io_fd);
    }
  } while (OB_LOG_STORE_EPOCH_CHANGED == ret);
  return ret;
}

int fallocate_with_retry(const char *block_path,
                         const int64_t block_size)
{
  int ret = OB_SUCCESS;
  bool has_create_success = false;
  do {
    ObIOFd io_fd;
    int flags = (!has_create_success ? CREATE_FILE_FLAG : OPEN_FILE_FLAG);
    mode_t mode = (!has_create_success ? CREATE_FILE_MODE : FILE_OPEN_MODE);
    if (OB_FAIL(LOG_IO_ADAPTER.open(block_path, flags, mode, io_fd))) {
      PALF_LOG(WARN, "open failed", K(ret), K(block_path));
      ob_usleep(RETRY_INTERVAL);
    } else if (FALSE_IT(has_create_success = true)) {
    } else if (OB_FAIL(LOG_IO_ADAPTER.fallocate(io_fd, 0, 0, block_size))) {
      PALF_LOG(WARN, "fallocate failed", K(ret), K(block_path));
      ob_usleep(RETRY_INTERVAL);
    } else {
      ret = OB_SUCCESS;
      PALF_LOG(TRACE, "fallocate block success", K(ret), K(block_path));
    }
    if (io_fd.is_valid()) {
      LOG_IO_ADAPTER.close(io_fd);
    }
  } while (OB_LOG_STORE_EPOCH_CHANGED == ret);
  return ret;
}

int ftruncate_with_retry(const char *block_path,
                         const int64_t block_size)
{
  int ret = OB_SUCCESS;
  do {
    ObIOFd io_fd;
    int flags = OPEN_FILE_FLAG;
    mode_t mode = FILE_OPEN_MODE;
    if (OB_FAIL(LOG_IO_ADAPTER.open(block_path, flags, mode, io_fd))) {
      PALF_LOG(WARN, "open failed", K(ret), K(block_path));
      ob_usleep(RETRY_INTERVAL);
    } else if (OB_FAIL(LOG_IO_ADAPTER.truncate(io_fd, block_size))) {
      PALF_LOG(WARN, "fallocate failed", K(ret), K(block_path));
      ob_usleep(RETRY_INTERVAL);
    } else {
      ret = OB_SUCCESS;
      PALF_LOG(TRACE, "fallocate block success", K(ret), K(block_path));
    }
    if (io_fd.is_valid()) {
      LOG_IO_ADAPTER.close(io_fd);
    }
  } while (OB_LOG_STORE_EPOCH_CHANGED == ret);
  return ret;
}

int is_directory(const char *block_path, bool &result)
{
  result = false;
  int ret = OB_SUCCESS;
  common::ObIODFileStat file_stat;
  if (OB_FAIL(LOG_IO_ADAPTER.stat(block_path, file_stat))) {
    PALF_LOG(WARN, "stat file failed", KR(ret), K(block_path));
  } else {
    result = S_ISDIR(file_stat.mode_);
  }
  return ret;
}

int is_exists(const char *block_path, bool &result)
{
  result = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(LOG_IO_ADAPTER.exist(block_path, result))) {
    PALF_LOG(WARN, "exist file failed", KR(ret), K(block_path));
  } else {
  }
  return ret;
}

int get_file_size(const char *block_path, int64_t &file_size)
{
  int ret = OB_SUCCESS;
  common::ObIODFileStat file_stat;
  if (OB_FAIL(LOG_IO_ADAPTER.stat(block_path, file_stat))
      && OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
    PALF_LOG(WARN, "stat file failed", KR(ret), K(block_path));
  } else if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
    PALF_LOG(WARN, "file not exist", KR(ret), K(block_path));
  } else {
    file_size = file_stat.size_;
  }
  return ret;
}

int block_id_to_string(const block_id_t block_id, char *str, const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (false == is_valid_block_id(block_id) || OB_ISNULL(str) || 0 >= str_len) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t pos = 0;
    ret = databuff_printf(str, str_len, pos, "%lu", block_id);
  }
  return ret;
}

int block_id_to_tmp_string(const block_id_t block_id, char *str, const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (false == is_valid_block_id(block_id) || OB_ISNULL(str) || 0 >= str_len) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t pos = 0;
    ret = databuff_printf(str, str_len, pos, "%lu.tmp", block_id);
  }
  return ret;
}

int block_id_to_flashback_string(const block_id_t block_id, char *str, const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (false == is_valid_block_id(block_id) || OB_ISNULL(str) || 0 >= str_len) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t pos = 0;
    ret = databuff_printf(str, str_len, pos, "%lu.flashback", block_id);
  }
  return ret;
}

int construct_absolute_block_path(const char *dir_path, const block_id_t block_id, const int64_t buf_len, char *absolute_block_path)
{
  int ret = OB_SUCCESS;
  if (false == is_valid_block_id(block_id) || OB_ISNULL(dir_path) || 0 >= buf_len || OB_ISNULL(absolute_block_path)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t pos = 0;
    ret = databuff_printf(absolute_block_path, buf_len, pos, "%s/%lu", dir_path, block_id);
  }
  return ret;
}

int construct_absolute_tmp_block_path(const char *dir_path, const block_id_t block_id, const int64_t buf_len, char *absolute_tmp_block_path)
{
  int ret = OB_SUCCESS;
  if (false == is_valid_block_id(block_id) || OB_ISNULL(dir_path) || 0 >= buf_len || OB_ISNULL(absolute_tmp_block_path)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t pos = 0;
    ret = databuff_printf(absolute_tmp_block_path, buf_len, pos, "%s/%lu.tmp", dir_path, block_id);
  }
  return ret;
}

int construct_absolute_flashback_block_path(const char *dir_path, const block_id_t block_id, const int64_t buf_len, char *absolute_flashback_block_path)
{
  int ret = OB_SUCCESS;
  if (false == is_valid_block_id(block_id) || OB_ISNULL(dir_path) || 0 >= buf_len || OB_ISNULL(absolute_flashback_block_path)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t pos = 0;
    ret = databuff_printf(absolute_flashback_block_path, buf_len, pos, "%s/%lu.flashback", dir_path, block_id);
  }
  return ret;
}

int convert_sys_errno()
{
  int ret = OB_IO_ERROR;
  switch (errno) {
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

bool is_number(const char* str)
{
	bool bool_ret = true;
  for (int64_t i = 0; true == bool_ret; ++i) {
    if ('\0' == str[i]) {
      break;
    } else if (!isdigit(str[i])) {
      bool_ret = false;
		}
  }
	return bool_ret;
}

} // end namespace palf
} // end namespace oceanbase
