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

#include "log_block_pool_interface.h"
#include <cstdlib>
#include <fcntl.h>
#include <sys/stat.h>
#include "lib/file/file_directory_utils.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_utility.h"
#include "log_define.h"
#include "log_block_header.h"

namespace oceanbase
{
namespace palf
{

int is_block_used_for_palf(const int fd, const char *path, bool &result)
{
  int ret = OB_SUCCESS;
  result = false;
  struct stat st;
  if (-1 == ::fstatat(fd, path, &st, 0)) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "::fstat failed", K(ret), K(path), K(errno));
  } else if (st.st_size == PALF_PHY_BLOCK_SIZE) {
    result = true;
  } else {
    result = false;
  }
  return ret;
}

int remove_file_at(const char *dir, const char *path, ILogBlockPool *log_block_pool)
{
  int ret = OB_SUCCESS;
  int fd = ::open(dir, O_DIRECTORY | O_RDONLY);
  bool result = false;
  if (-1 == fd) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "::open directory failed", K(ret), K(dir));
  } else if (OB_FAIL(log_block_pool->remove_block_at(fd, path))) {
    PALF_LOG(ERROR, "remove_block_at failed", K(ret));
    // otherwise, unlink it.
  } else {
    PALF_LOG(INFO, "remove_file_at success", K(dir), K(path));
  }

  if (-1 != fd) {
    ::fsync(fd);
    ::close(fd);
  }
  return ret;
}

int remove_directory_rec(const char *path, ILogBlockPool *log_block_pool)
{
  int ret = OB_SUCCESS;
  DIR *dir = NULL;
  struct dirent *entry = NULL;
  if (NULL == (dir = opendir(path))) {
    ret = convert_sys_errno();
    PALF_LOG(WARN, "opendir failed", K(path));
  } else {
    char current_file_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    while ((entry = readdir(dir)) != NULL && OB_SUCC(ret)) {
      bool is_dir = false;
      MEMSET(current_file_path, '\0', OB_MAX_FILE_NAME_LENGTH);
      if (0 == strcmp(entry->d_name, ".") || 0 == strcmp(entry->d_name, "..")) {
        // do nothing
      } else if (0 >= snprintf(current_file_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s", path, entry->d_name)) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "snprintf failed", K(ret), K(current_file_path), K(path), K(entry->d_name));
      } else if (OB_FAIL(FileDirectoryUtils::is_directory(current_file_path, is_dir))) {
        PALF_LOG(WARN, "is_directory failed", K(ret), K(entry->d_name));
        // delecte directory recursive
      } else if (true == is_dir && OB_FAIL(remove_directory_rec(current_file_path, log_block_pool))) {
        PALF_LOG(WARN, "remove directory failed", K(ret), K(entry->d_name), K(path));
        // delete normal file
      } else if (false == is_dir && OB_FAIL(remove_file_at(path, entry->d_name, log_block_pool))) {
        PALF_LOG(WARN, "remove_file_at failed", K(ret), K(current_file_path));
      } else {
        PALF_LOG(INFO, "remove directory or file success", K(path), K(current_file_path));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(FileDirectoryUtils::delete_directory(path))) {
    PALF_LOG(WARN, "delete_directory failed", K(ret), K(path));
  }
  if (NULL != dir) {
    closedir(dir);
  }
  return ret;
}

int remove_tmp_file_or_directory_at(const char *path, ILogBlockPool *log_block_pool)
{
  int ret = OB_SUCCESS;
  DIR *dir = NULL;
  struct dirent *entry = NULL;
  if (NULL == (dir = opendir(path))) {
    ret = OB_ERR_SYS;
    PALF_LOG(WARN, "opendir failed", K(path));
  } else {
    auto check_is_tmp_file_or_dir = [](const char* file_name) -> bool {
      return NULL != strstr(file_name, ".tmp");
    };
    char current_file_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    while ((entry = readdir(dir)) != NULL && OB_SUCC(ret)) {
      bool is_dir = false;
      MEMSET(current_file_path, '\0', OB_MAX_FILE_NAME_LENGTH);
      if (0 == strcmp(entry->d_name, ".") || 0 == strcmp(entry->d_name, "..")) {
        // do nothing
      } else if (0 >= snprintf(current_file_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s", path, entry->d_name)) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "snprintf failed", K(ret), K(current_file_path), K(path), K(entry->d_name));
      } else if (OB_FAIL(FileDirectoryUtils::is_directory(current_file_path, is_dir))) {
        PALF_LOG(WARN, "is_directory failed", K(ret), K(entry->d_name));
      } else if (true == check_is_tmp_file_or_dir(current_file_path)) {
        if (true == is_dir && OB_FAIL(remove_directory_rec(current_file_path, log_block_pool))) {
          PALF_LOG(WARN, "delete_directory_rec failed", K(ret), K(entry->d_name), K(path));
        } else if (false == is_dir && OB_FAIL(remove_file_at(path, entry->d_name, log_block_pool))) {
          PALF_LOG(WARN, "delete_file failed", K(ret), K(current_file_path));
        } else {
        }
      } else if (true == is_dir && OB_FAIL(remove_tmp_file_or_directory_at(current_file_path, log_block_pool))) {
        PALF_LOG(WARN, "delete_tmp_file_or_directory_at failed", K(ret), K(current_file_path));
      } else {
      }
    }
  }
  if (NULL != dir) {
    closedir(dir);
  }
  return ret;
}
} // end namespace oceanbase
} // end namespace palf
