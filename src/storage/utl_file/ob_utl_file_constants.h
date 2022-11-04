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

#ifndef OCEANBASE_STORAGE_UTL_FILE_OB_UTL_CONSTANTS_H_
#define OCEANBASE_STORAGE_UTL_FILE_OB_UTL_CONSTANTS_H_

#include <sys/stat.h>
#include <sys/types.h>
#include "lib/alloc/alloc_assist.h"

namespace oceanbase
{
namespace storage
{
class ObUtlFileConstants
{
public:
  static constexpr int MAX_LINE_SIZE_LOWER_LIMIT = 1;
  static constexpr int MAX_LINE_SIZE_UPPER_LIMIT = 32767;
  static constexpr int DEFAULT_MAX_LINE_SIZE = 1024;
  static constexpr int DEFAULT_IO_RETRY_CNT = 3;
  static constexpr int UTF_FILE_BUFFER_ALIGN_SIZE = 4 * 1024; // 4KB
  static constexpr int UTF_FILE_WRITE_BUFFER_SIZE = 32 * 1024; // 32KB
  static constexpr int MAX_OPEN_FILE_COUNT = 50;
  static constexpr int UTL_PATH_SIZE_LIMIT = 256;
  static constexpr mode_t UTL_FILE_ACCESS_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_UTL_FILE_OB_UTL_CONSTANTS_H_
