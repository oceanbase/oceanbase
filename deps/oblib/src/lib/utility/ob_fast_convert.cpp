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

#include "lib/utility/ob_fast_convert.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
const char ObFastFormatInt::DIGITS[] =
    "0001020304050607080910111213141516171819"
    "2021222324252627282930313233343536373839"
    "4041424344454647484950515253545556575859"
    "6061626364656667686970717273747576777879"
    "8081828384858687888990919293949596979899";

char *ObFastFormatInt::format_unsigned(uint64_t value)
{
  char *ptr = buf_ + (MAX_DIGITS10_STR_SIZE - 1);
  uint32_t index = 0;
  while (value >= 100) {
    index = static_cast<uint32_t>(value % 100) << 1;
    value /= 100;
    *--ptr = DIGITS[index + 1];
    *--ptr = DIGITS[index];
  }
  if (value < 10) {
    *--ptr = (char)('0' + value);
  } else {
    index = static_cast<uint32_t>(value) << 1;
    *--ptr = DIGITS[index + 1];
    *--ptr = DIGITS[index];
  }
  return ptr;
}

void ObFastFormatInt::format_signed(int64_t value)
{
  uint64_t abs_value = static_cast<uint64_t>(value);
  if (value < 0) {
    abs_value = ~abs_value + 1;
    ptr_ = format_unsigned(abs_value);
    *--ptr_ = '-';
  } else {
    ptr_ = format_unsigned(abs_value);
  }
  len_ = buf_ - ptr_ + MAX_DIGITS10_STR_SIZE - 1;
}

int64_t ObFastFormatInt::format_unsigned(uint64_t value, char *buf)
{
  int64_t len = ob_fast_digits10(value);
  buf += len;
  *buf = '\0';

  uint32_t index = 0;
  while (value >= 100) {
    index = static_cast<uint32_t>(value % 100) << 1;
    value /= 100;
    *--buf = DIGITS[index + 1];
    *--buf = DIGITS[index];
  }
  if (value < 10) {
    *--buf = (char)('0' + value);
  } else {
    index = static_cast<uint32_t>(value) << 1;
    *--buf = DIGITS[index + 1];
    *--buf = DIGITS[index];
  }
  return len;
}

int64_t ObFastFormatInt::format_signed(int64_t value, char *buf)
{
  int64_t len = 0;
  uint64_t abs_value = static_cast<uint64_t>(value);
  if (value < 0) {
    abs_value = ~abs_value + 1;
    *buf++ = '-';
    ++len;
  }
  len += format_unsigned(abs_value, buf);
  return len;
}

} // end namespace common
} // end namespace oceanbase
