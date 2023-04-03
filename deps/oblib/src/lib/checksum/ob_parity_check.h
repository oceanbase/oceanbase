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

#ifndef  OCEANBASE_COMMON_PARITY_CHECK_H_
#define  OCEANBASE_COMMON_PARITY_CHECK_H_

#include <stdint.h>

namespace oceanbase
{
namespace common
{
// If val contains an even number of 1, the value is 0, otherwise the value is 1.
bool parity_check(const uint16_t value);
// If val contains an even number of 1, the value is 0, otherwise the value is 1.
bool parity_check(const uint32_t value);
// If val contains an even number of 1, the value is 0, otherwise the value is 1.
bool parity_check(const uint64_t value);
}
}
#endif
