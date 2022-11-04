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

#ifndef OCEANBASE_COMMON_OB_LOG_DEFINITION_H_
#define OCEANBASE_COMMON_OB_LOG_DEFINITION_H_

#include <fcntl.h>

namespace oceanbase
{
namespace common
{
enum ObRedoLogType
{
  OB_REDO_TYPE_INVALID = 0,
  OB_REDO_TYPE_CLOG = 1,
  OB_REDO_TYPE_ILOG = 2,
  OB_REDO_TYPE_SLOG = 3,
};

class ObLogDefinition
{
public:
  static constexpr int LOG_READ_FLAG = O_RDONLY | O_DIRECT;
  static constexpr int LOG_WRITE_FLAG = O_RDWR | O_DIRECT | O_SYNC | O_CREAT;
  static constexpr int LOG_APPEND_FLAG = O_RDWR | O_DIRECT | O_SYNC | O_CREAT | O_APPEND;
  static constexpr int FILE_OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  static constexpr mode_t DIR_CREATE_MODE = S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
  static constexpr int DEFAULT_IO_RETRY_CNT = 3;
  static constexpr int64_t RETRY_SLEEP_TIME_IN_US = 100 * 1000;
};
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_LOG_DEFINITION_H_
