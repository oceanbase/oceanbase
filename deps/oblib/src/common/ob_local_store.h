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

#ifndef OCEANBASE_COMMON_LOCAL_STORE_
#define OCEANBASE_COMMON_LOCAL_STORE_

#include "lib/oblog/ob_log_level.h"

#include <cstdint>

namespace oceanbase {
namespace common {
struct ObLocalStore {
  ObLocalStore() : stack_addr_(nullptr), stack_size_(0)
  {}
  ObThreadLogLevel log_level_;
  void* stack_addr_;
  size_t stack_size_;
};
}  // namespace common
}  // namespace oceanbase
#endif
