/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
