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

#include "lib/file/fileutil.h"
namespace oceanbase {
namespace obsys {
bool CFileUtil::mkdirs(char* szDirPath)
{
  struct stat stats;
  if (stat(szDirPath, &stats) == 0 && S_ISDIR(stats.st_mode))
    return true;

  mode_t umask_value = umask(0);
  umask(umask_value);
  mode_t mode = (S_IRWXUGO & (~umask_value)) | S_IWUSR | S_IXUSR;

  char* slash = szDirPath;
  while (*slash == '/')
    slash++;

  while (1) {
    slash = strchr(slash, '/');
    if (slash == NULL)
      break;

    *slash = '\0';
    int ret = mkdir(szDirPath, mode);
    *slash++ = '/';
    if (0 != ret && errno != EEXIST) {
      return false;
    }

    while (*slash == '/')
      slash++;
  }
  if (0 != mkdir(szDirPath, mode)) {
    return false;
  }
  return true;
}

// Is it a directory
bool CFileUtil::isDirectory(const char* szDirPath)
{
  struct stat stats;
  if (lstat(szDirPath, &stats) == 0 && S_ISDIR(stats.st_mode))
    return true;
  return false;
}

// Is islnk
bool CFileUtil::isSymLink(const char* szDirPath)
{
  struct stat stats;
  if (lstat(szDirPath, &stats) == 0 && S_ISLNK(stats.st_mode))
    return true;
  return false;
}
}  // namespace obsys
}  // namespace oceanbase

//////////////////END
