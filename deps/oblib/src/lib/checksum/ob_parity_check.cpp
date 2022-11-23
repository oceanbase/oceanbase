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

#include "ob_parity_check.h"
#include <algorithm>
#include <string.h>
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
#define P2(n) n, n^1, n^1, n
#define P4(n) P2(n), P2(n^1), P2(n^1), P2(n)
#define P6(n) P4(n), P4(n^1), P4(n^1), P4(n)

// The table shows the number of 1 for 0~255.
// If it contains an even number of 1, the value is 0, otherwise the value is 1.
const bool ParityTable[256] =
{
  P6(0), P6(1), P6(1), P6(0)
};

// If val contains an even number of 1, the value is 0, otherwise the value is 1.
bool parity_check(const uint16_t value)
{
  bool bool_ret = false;
  uint16_t val = value;
  val ^= val >> 8;
  if (ParityTable[val & 0xff])
  {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

// If val contains an even number of 1, the value is 0, otherwise the value is 1.
bool parity_check(const uint32_t value)
{
  bool bool_ret = false;
  uint32_t val = value;
  val ^= val >> 16;
  val ^= val >> 8;
  if (ParityTable[val & 0xff])
  {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

// If val contains an even number of 1, the value is 0, otherwise the value is 1.
bool parity_check(const uint64_t value)
{
  bool bool_ret = false;
  uint64_t val = value;
  val ^= val >> 32;
  val ^= val >> 16;
  val ^= val >> 8;
  if (ParityTable[val & 0xff])
  {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

}
}
