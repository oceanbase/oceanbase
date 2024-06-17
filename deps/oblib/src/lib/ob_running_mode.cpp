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

#define USING_LOG_PREFIX LIB
#include "ob_running_mode.h"

namespace oceanbase {
namespace lib {
const int64_t ObRunningModeConfig::MIN_MEM = 1L << 30;  // The minimum value for memory_limit.
const int64_t ObRunningModeConfig::MINI_MEM_LOWER = 4L << 30;
const int64_t ObRunningModeConfig::MINI_MEM_UPPER = 12L << 30;
const int64_t ObRunningModeConfig::MINI_CPU_UPPER = 8;

bool __attribute__((weak)) mtl_is_mini_mode() { return false; }
} //end of namespace lib
} //end of namespace oceanbase

extern "C" {
  bool use_ipv6_c()
  {
    return oceanbase::lib::use_ipv6();
  }
} /* extern "C" */
