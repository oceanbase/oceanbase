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

#ifndef OCEANBASE_LIB_OBLOG_OB_LOG_LEVEL_
#define OCEANBASE_LIB_OBLOG_OB_LOG_LEVEL_

#include <cstddef>
#include <cstdint>

namespace oceanbase {
namespace common {
#define OB_LOG_LEVEL_NONE 7
#define OB_LOG_LEVEL_NP -1  // set this level, would not print log
#define OB_LOG_LEVEL_ERROR 0
//#define OB_LOG_LEVEL_USER_ERROR  1
#define OB_LOG_LEVEL_WARN 2
#define OB_LOG_LEVEL_INFO 3
#define OB_LOG_LEVEL_TRACE 4
#define OB_LOG_LEVEL_DEBUG 5

struct ObLogIdLevelMap;
//@class ObThreadLogLevel
//@brief Deliver the id_level_map of the session which set the session variable
//'log_level'
struct ObThreadLogLevel {
  ObThreadLogLevel() : id_level_map_(NULL), level_(OB_LOG_LEVEL_NONE)
  {}
  const ObLogIdLevelMap* id_level_map_;
  int8_t level_;  // Used for transmit log_level in packet.
};

}  // namespace common
}  // namespace oceanbase

#endif
