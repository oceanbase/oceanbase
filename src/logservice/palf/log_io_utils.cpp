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
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "log_block_pool_interface.h"
#include "share/ob_errno.h"
#include "logservice/ob_server_log_block_mgr.h"

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
        usleep(RETRY_INTERVAL);
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

} // end namespace palf
} // end namespace oceanbase
