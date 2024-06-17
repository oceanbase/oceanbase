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

#include "lib/file/file_directory_utils.h"

#include "lib/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/ob_errno.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <dirent.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/statvfs.h>


namespace oceanbase
{
namespace common
{

//return true if filename is exists
int FileDirectoryUtils::is_exists(const char *file_path, bool &result)
{
  int ret = OB_SUCCESS;
  result = false;
  struct stat64 file_info;
  if (OB_ISNULL(file_path) || OB_UNLIKELY(strlen(file_path) == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", KCSTRING(file_path), K(ret));
  } else {
    result = (0 == stat64(file_path, &file_info));
  }

  return ret;
}

//return true if file is accessible
int FileDirectoryUtils::is_accessible(const char *file_path, bool &result)
{
  int ret = OB_SUCCESS;
  result = false;
  if (OB_ISNULL(file_path) || OB_UNLIKELY(strlen(file_path) == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", KCSTRING(file_path), K(ret));
  } else {
    if (0 == access(file_path, R_OK)) {
      result = true;
    } else {
      LIB_LOG(WARN, "access file failed", KERRMSG, K(file_path));
    }
  }
  return ret;
}

//return ture if dirname is a directory
int FileDirectoryUtils::is_directory(const char *directory_path, bool &result)
{
  int ret = OB_SUCCESS;
  result = false;
  struct stat64 file_info;
  if (NULL == directory_path ||  strlen(directory_path) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", KCSTRING(directory_path), K(ret));
  } else {
    result = (0 == stat64(directory_path, &file_info) && S_ISDIR(file_info.st_mode));
  }

  return ret;
}

int FileDirectoryUtils::is_link(const char *link_path, bool &result)
{
  int ret = OB_SUCCESS;
  if (NULL == link_path || strlen(link_path) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", KCSTRING(link_path), K(ret));
  }  else {
    struct stat64 file_info;
    result = (0 == lstat64(link_path, &file_info) && S_ISLNK(file_info.st_mode));
  }
  return ret;
}

//create the give dirname, return true on success or dirname exists
int FileDirectoryUtils::create_directory(const char *directory_path)
{
  int ret = OB_SUCCESS;
  mode_t umake_value = umask(0);
  umask(umake_value);
  mode_t mode = (S_IRWXUGO & (~umake_value)) | S_IWUSR | S_IXUSR;

  if (NULL == directory_path || strlen(directory_path) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", KCSTRING(directory_path), K(ret));
  } else if (::mkdir(directory_path, mode) != 0) {
    if (EEXIST == errno) {
      ret = OB_SUCCESS;
    } else {
      ret = OB_IO_ERROR;
      LIB_LOG(WARN, "create directory failed.",
              KCSTRING(directory_path), K(errno), KERRMSG, K(ret));
    }
  }

  return ret;
}

//creates the full path of fullpath, return true on success
int FileDirectoryUtils::create_full_path(const char *fullpath)
{

  int ret = OB_SUCCESS;
  struct stat64 file_info;
  int64_t len = 0;
  if (NULL == fullpath || (len = strlen(fullpath)) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", KCSTRING(fullpath), K(ret));
  } else {
    ret = ::stat64(fullpath, &file_info);
    if (0 == ret) {
      if (!S_ISDIR(file_info.st_mode)) {
        ret = OB_ENTRY_EXIST;
        LIB_LOG(WARN, "file is exists but not a directory.", KCSTRING(fullpath), K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      ret = OB_SUCCESS;
      // path not exists.
      char dirpath[MAX_PATH + 1];
      strncpy(dirpath, fullpath, len);
      dirpath[len] = '\0';
      char *path = dirpath;

      // skip leading char '/'
      while (*path++ == '/');

      while (OB_SUCC(ret)) {
        path = strchr(path, '/');
        if (NULL == path) {
          break;
        }

        *path = '\0';
        if (OB_FAIL(create_directory(dirpath))) {
          LIB_LOG(WARN, "create directory failed.", KCSTRING(dirpath), K(ret));
        } else {
          *path++ = '/';
          // skip '/'
          while (*path++ == '/')
            ;
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(create_directory(dirpath))) {
          LIB_LOG(WARN, "create directory failed.", KCSTRING(dirpath), K(ret));
        }
      }
    }
  }

  return ret;
}

//delete the given file, return true if filename exists
// return OB_SUCCESS on success;
int FileDirectoryUtils::delete_file(const char *filename)
{
  int ret = OB_SUCCESS;
  struct stat64 file_info;
  if (NULL == filename || strlen(filename) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", KCSTRING(filename), K(ret));
  } else {
    ret = ::stat64(filename, &file_info);
    if (0 != ret) {
      ret = OB_FILE_NOT_EXIST;
      LIB_LOG(WARN, "file is not exists.", KCSTRING(filename), K(ret));
    } else if (S_ISDIR(file_info.st_mode)) {
      ret = OB_FILE_NOT_EXIST;
      LIB_LOG(WARN, "file is directory, use delete_directory.",
              KCSTRING(filename), K(ret));
    } else if (0 != unlink(filename)){
      ret = OB_IO_ERROR;
      LIB_LOG(WARN, "unlink file failed.",
              KCSTRING(filename), K(errno), KERRMSG, K(ret));
    }
  }
  return ret;
}

//delete the given directory and anything under it. Returns true on success
int FileDirectoryUtils::delete_directory(const char *dirname)
{
  int ret = OB_SUCCESS;
  bool is_dir = false;
  if (NULL == dirname || strlen(dirname) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", KCSTRING(dirname), K(ret));
  } else if (OB_FAIL(is_directory(dirname, is_dir))) {
    LIB_LOG(WARN, "check if directory failed.", KCSTRING(dirname), K(ret));
  } else if (!is_dir) {
    ret = OB_FILE_NOT_EXIST;
    LIB_LOG(WARN, "file path is not a directory.", KCSTRING(dirname), K(ret));
  } else if (0 != rmdir(dirname)) {
    ret = OB_IO_ERROR;
    LIB_LOG(WARN, "rmdir failed.",
            KCSTRING(dirname), K(errno), KERRMSG, K(ret));
  }
  return ret;
}

//return the size of filename
int FileDirectoryUtils::get_file_size(const char *filename, int64_t &size)
{
  int ret = OB_SUCCESS;
  struct stat64 file_info;
  if (NULL == filename || strlen(filename) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", KCSTRING(filename), K(ret));
  } else {
    ret = ::stat64(filename, &file_info);
    if (0 != ret) {
      ret = OB_FILE_NOT_EXIST;
      LIB_LOG(WARN, "file is not exists.", KCSTRING(filename), K(ret));
    } else if (S_ISDIR(file_info.st_mode)) {
      ret = OB_FILE_NOT_EXIST;
      LIB_LOG(WARN, "file is not a file.", KCSTRING(filename), K(ret));
    } else {
      size = file_info.st_size;
    }
  }
  return ret;
}

int FileDirectoryUtils::is_valid_path(const char *path, const bool print_error)
{
  int ret = OB_SUCCESS;

  if (NULL == path) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "path must not null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && '\0' != path[i]; ++i) {
      if (!isalnum(path[i]) && '_' != path[i] && '/' != path[i] && '.' != path[i]) {
        ret = OB_INVALID_ARGUMENT;
        if (print_error) {
          LIB_LOG(WARN, "invalid path", K(ret), K(i), K(path[i]), KCSTRING(path));
        }
        break;
      }
    }
  }
  return ret;
}

int FileDirectoryUtils::is_empty_directory(const char *directory_path, bool &result)
{
  int ret = OB_SUCCESS;
  DIR *dir = NULL;
  struct dirent *entry = NULL;
  int64_t num = 0;
  if (NULL == directory_path) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "directory path is NULL, ", K(ret));
  } else if (NULL == (dir = opendir(directory_path))) {
    ret = OB_ERR_SYS;
    LIB_LOG(WARN, "Fail to open dir, ", K(ret), K(errno), KCSTRING(directory_path));
  } else {
    while(NULL != (entry = readdir(dir))) {
      ++num;
    }
    if (2 == num) {
      result = true;
    } else {
      result = false;
    }
  }

  if (NULL != dir) {
    closedir(dir);
  }
  return ret;
}

int FileDirectoryUtils::open(const char *pathname, int flags, mode_t mode, int &fd)
{
  int ret = OB_SUCCESS;
  if (NULL == pathname || strlen(pathname) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", KCSTRING(pathname), K(ret));
  } else {
    fd = ::open(pathname, flags, mode);
    if (fd < 0) {
      ret = OB_IO_ERROR;
      LIB_LOG(WARN, "Fail to open", K(ret), K(errno), KCSTRING(pathname), K(mode), K(flags));
    }
  }
  return ret;
}

int FileDirectoryUtils::close(const int fd)
{
  int ret = OB_SUCCESS;
  if (fd < 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", K(fd), K(ret));
  } else {
    if (0 != ::close(fd)) {
      ret = OB_IO_ERROR;
      LIB_LOG(WARN, "fail to close", K(ret), K(errno), K(fd));
    }
  }

  return ret;
}

int FileDirectoryUtils::symlink(const char *oldpath, const char *newpath)
{
  int ret = OB_SUCCESS;
  if (NULL == oldpath || strlen(oldpath) == 0 || NULL == newpath || strlen(newpath) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "path must not null", K(ret), KCSTRING(oldpath), KCSTRING(newpath));
  } else {
    if (0 != ::symlink(oldpath, newpath)) {
      ret = OB_IO_ERROR;
      LIB_LOG(WARN, "fail to symlink", K(ret), K(errno), KCSTRING(oldpath), KCSTRING(newpath));
    }
  }
  return ret;
}

int FileDirectoryUtils::unlink_symlink(const char *link_path)
{
  int ret = OB_SUCCESS;
  bool is_link_file = false;
  if (NULL == link_path || strlen(link_path) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", KCSTRING(link_path), K(ret));
  } else if (OB_FAIL(is_link(link_path, is_link_file))) {
    LIB_LOG(WARN, "fail to exectue is_link", K(ret), KCSTRING(link_path));
  } else if (is_link_file) {
    if (0 != ::unlink(link_path)) {
      ret = OB_IO_ERROR;
      LIB_LOG(WARN, "fail to unlink", K(ret), K(errno), KERRMSG, KCSTRING(link_path));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "not a symlink", K(ret), KCSTRING(link_path));
  }

  return ret;
}

int FileDirectoryUtils::dup_fd(const int fd, int &dup_fd)
{
  int ret = OB_SUCCESS;
  if (fd < 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", K(fd), K(ret));
  } else if (0  > (dup_fd = ::dup(fd))) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "fail to dup", K(ret), K(fd), K(errno), KERRMSG);
  }
  return ret;
}

int FileDirectoryUtils::get_disk_space(
    const char *path,
    int64_t &total_space,
    int64_t &free_space)
{
  int ret = OB_SUCCESS;
  struct statvfs svfs;
  total_space = 0;
  free_space = 0;

  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid args", K(ret), KP(path));
  } else if (OB_FAIL(statvfs(path, &svfs))) {
    ret = OB_IO_ERROR;
    LIB_LOG(WARN, "get svfs fail", K(ret), KCSTRING(path), K(errno), KERRNOMSG(errno));
  } else {
    // Remove the space reserved by the root user
    total_space =  (svfs.f_blocks + svfs.f_bavail - svfs.f_bfree) * svfs.f_bsize;
    free_space = svfs.f_bavail * svfs.f_bsize;
  }
  return ret;
}

int FileDirectoryUtils::delete_directory_rec(const char *path)
{
  int ret = OB_SUCCESS;
  DIR *dir = NULL;
  struct dirent *entry;
  if (NULL == (dir = opendir(path))) {
    ret = OB_ERR_SYS;
    LIB_LOG(WARN, "opendir failed", K(path));
  } else {
    char current_file_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    while ((entry = readdir(dir)) != NULL && OB_SUCC(ret)) {
      bool is_dir = false;
      MEMSET(current_file_path, '\0', OB_MAX_FILE_NAME_LENGTH);
      if (0 == strcmp(entry->d_name, ".") || 0 == strcmp(entry->d_name, "..")) {
        // do nothing
      } else if (0 >= snprintf(current_file_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s", path, entry->d_name)) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "snprintf failed", K(ret), K(current_file_path), K(path), K(entry->d_name));
      } else if (OB_FAIL(FileDirectoryUtils::is_directory(current_file_path, is_dir))) {
        LIB_LOG(WARN, "is_directory failed", K(ret), K(entry->d_name));
        // delecte directory recursive
      } else if (true == is_dir && OB_FAIL(delete_directory_rec(current_file_path))) {
        LIB_LOG(WARN, "delete directory failed", K(ret), K(entry->d_name), K(path));
        // delete normal file
      } else if (false == is_dir && OB_FAIL(FileDirectoryUtils::delete_file(current_file_path))) {
        LIB_LOG(WARN, "delete_file failed", K(ret), K(current_file_path));
      } else {
        LIB_LOG(INFO, "delete_directory_rec success", K(path));
      }
    }
  }
  if (OB_FAIL(ret)) {
    LIB_LOG(WARN, "delete directory rec failed", K(ret), K(path));
  } else if (OB_FAIL(delete_directory(path))) {
    LIB_LOG(WARN, "delete_directory failed", K(ret), K(path));
  }
  if (NULL != dir) {
    closedir(dir);
  }
  return ret;
}

int FileDirectoryUtils::delete_tmp_file_or_directory_at(const char *path)
{
  int ret = OB_SUCCESS;
  DIR *dir = NULL;
  struct dirent *entry;
  if (NULL == (dir = opendir(path))) {
    ret = OB_ERR_SYS;
    LIB_LOG(WARN, "opendir failed", K(path));
  } else {
    auto check_is_tmp_file = [](const char* file_name) -> bool {
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
        LIB_LOG(WARN, "snprintf failed", K(ret), K(current_file_path), K(path), K(entry->d_name));
      } else if (OB_FAIL(FileDirectoryUtils::is_directory(current_file_path, is_dir))) {
        LIB_LOG(WARN, "is_directory failed", K(ret), K(entry->d_name));
      } else if (true == check_is_tmp_file(current_file_path)) {
        if (true == is_dir && OB_FAIL(delete_directory_rec(current_file_path))) {
          LIB_LOG(WARN, "delete_directory_rec failed", K(ret), K(entry->d_name), K(path));
        } else if (false == is_dir && OB_FAIL(FileDirectoryUtils::delete_file(current_file_path))) {
          LIB_LOG(WARN, "delete_file failed", K(ret), K(current_file_path));
        } else {
        }
      } else if (true == is_dir && OB_FAIL(delete_tmp_file_or_directory_at(current_file_path))) {
        LIB_LOG(WARN, "delete_tmp_file_or_directory_at failed", K(ret), K(current_file_path));
      } else {
      }
    }
  }
  if (NULL != dir) {
    closedir(dir);
  }
  return ret;
}

int FileDirectoryUtils::fsync_dir(const char *dir_path)
{
  int ret = OB_SUCCESS;
  int fd = ::open(dir_path, O_DIRECTORY | O_RDONLY);
  if (-1 == fd) {
    ret = OB_IO_ERROR;
    LIB_LOG(WARN, "::open failed", K(ret), K(dir_path), K(errno));
  } else if (-1 == ::fsync(fd)) {
    ret = OB_IO_ERROR;
    LIB_LOG(WARN, "::fsync failed", K(ret), K(dir_path), K(errno));
  } else {
  }
  if (-1 != fd) {
    ::close(fd);
  }
  return ret;
}

}//end namespace common
}//end namespace oceanbase
