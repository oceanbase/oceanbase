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

#ifndef OCEANBASE_COMMON_FILE_DIRECTORY_UTILS_H_
#define OCEANBASE_COMMON_FILE_DIRECTORY_UTILS_H_

#include <string>
#include <vector>
#include <stdint.h>

namespace oceanbase
{
namespace common
{
#ifndef S_IRWXUGO
# define S_IRWXUGO (S_IRWXU | S_IRWXG | S_IRWXO)
#endif

class FileDirectoryUtils
{
public:
  static const int MAX_PATH = 512;
  static int is_exists(const char *file_path, bool &result);
  static int is_accessible(const char *file_path, bool &result);
  static int is_directory(const char *directory_path, bool &result);
  static int is_link(const char *link_path, bool &result);
  static int create_directory(const char *directory_path);
  static int create_full_path(const char *fullpath);
  static int delete_file(const char *filename);
  static int delete_directory(const char *dirname);
  static int get_file_size(const char *filename, int64_t &size);
  static int is_valid_path(const char *path, const bool print_error);
  static int is_empty_directory(const char *directory_path, bool &result);
  static int open(const char *directory_path, int flags, mode_t mode, int &fd);
  static int close(const int fd);
  static int symlink(const char *oldpath, const char *newpath);
  static int unlink_symlink(const char *link_path);
  static int dup_fd(const int fd, int &dup_fd);
  static int get_disk_space(const char *path, int64_t &total_space, int64_t &free_space);
  static int delete_directory_rec(const char *path);
  static int delete_tmp_file_or_directory_at(const char *path);
  static int fsync_dir(const char *dir_path);
};

typedef FileDirectoryUtils FSU;
}       //end namespace common
}       //end namespace oceanbase
#endif
