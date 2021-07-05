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

#ifndef TBSYS_FILE_UTIL_H
#define TBSYS_FILE_UTIL_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>

namespace oceanbase {
namespace obsys {

#ifndef S_IRWXUGO
#define S_IRWXUGO (S_IRWXU | S_IRWXG | S_IRWXO)
#endif

class CFileUtil {
public:
  /** Create a multi-level directory */
  static bool mkdirs(char* szDirPath);
  /** Is it a directory */
  static bool isDirectory(const char* szDirPath);
  /** Is it a SymLink file */
  static bool isSymLink(const char* szDirPath);
};
}  // namespace obsys
}  // namespace oceanbase

#endif

//////////////////END
