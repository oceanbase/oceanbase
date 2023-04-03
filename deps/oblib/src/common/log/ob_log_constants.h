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

#ifndef OCEANBASE_COMMON_OB_LOG_CONSTANTS_H_
#define OCEANBASE_COMMON_OB_LOG_CONSTANTS_H_

#include <stdint.h>

namespace oceanbase
{
namespace common
{
class ObLogConstants
{
public:
  static constexpr int64_t MAX_LOG_FILE_SIZE = 64 << 20; // 64MB
  static constexpr int64_t LOG_FILE_ALIGN_SIZE = 4 << 10; // 4KB
  static constexpr int64_t LOG_FILE_ALIGN_MASK = LOG_FILE_ALIGN_SIZE - 1;
  static constexpr int64_t LOG_BUF_RESERVED_SIZE = 3 * LOG_FILE_ALIGN_SIZE; // NOP + switch_log
  static constexpr int64_t LOG_ITEM_MAX_LENGTH = 63 << 20; // 63MB
  static constexpr int64_t NOP_SWITCH_LOG_SEQ = 0;
};
} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_LOG_CONSTANTS_H_
