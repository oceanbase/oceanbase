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
#ifndef DEPS_OBLIB_SRC_LIB_LITERALS_OB_LITERALS_H
#define DEPS_OBLIB_SRC_LIB_LITERALS_OB_LITERALS_H

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
namespace oceanbase
{
// Time literals define : 1us = 1
constexpr unsigned long long operator "" _us(unsigned long long us)
{
  return us;
}

constexpr unsigned long long operator "" _ms(long double ms)
{
  return ms * 1000_us;
}

constexpr unsigned long long operator "" _ms(unsigned long long ms)
{
  return ms * 1000_us;
}

constexpr unsigned long long operator "" _s(long double s)
{
  return s * 1000_ms;
}

constexpr unsigned long long operator "" _s(unsigned long long s)
{
  return s * 1000_ms;
}

constexpr unsigned long long operator "" _min(long double min)
{
  return min * 60_s;
}

constexpr unsigned long long operator "" _min(unsigned long long min)
{
  return min * 60_s;
}

constexpr unsigned long long operator "" _hour(long double hour)
{
  return hour * 60_min;
}

constexpr unsigned long long operator "" _hour(unsigned long long hour)
{
  return hour * 60_min;
}

constexpr unsigned long long operator "" _day(long double day)
{
  return day * 24_hour;
}

constexpr unsigned long long operator "" _day(unsigned long long day)
{
  return day * 24_hour;
}

// Size literals define : 1 byte = 1
constexpr unsigned long long operator "" _B(unsigned long long byte)
{
  return byte;
}

constexpr unsigned long long operator "" _KB(unsigned long long kilobyte)
{
  return kilobyte * 1024_B;
}

constexpr unsigned long long operator "" _KB(long double kilobyte)
{
  return kilobyte * 1024_B;
}

constexpr unsigned long long operator "" _MB(unsigned long long megabyte)
{
  return megabyte * 1024_KB;
}

constexpr unsigned long long operator "" _MB(long double megabyte)
{
  return megabyte * 1024_KB;
}

constexpr unsigned long long operator "" _GB(unsigned long long gigabyte)
{
  return gigabyte * 1024_MB;
}

constexpr unsigned long long operator "" _GB(long double gigabyte)
{
  return gigabyte * 1024_MB;
}

constexpr unsigned long long operator "" _TB(unsigned long long terabyte)
{
  return terabyte * 1024_GB;
}

constexpr unsigned long long operator "" _TB(long double terabyte)
{
  return terabyte * 1024_GB;
}

struct ObSizeLiteralPrettyPrinter {
  constexpr ObSizeLiteralPrettyPrinter(unsigned long long val) : val_(val) {}
  int64_t to_string(char *buf, const int64_t len) const {
    int64_t pos = 0;
    if (val_ < 1_KB) {
      databuff_printf(buf, len, pos, "%lld_B", val_);
    } else if (val_ < 1_MB) {
      if ((val_ % 1_KB) == 0) {
        databuff_printf(buf, len, pos, "%lld_KB", val_ / 1_KB);
      } else {
        databuff_printf(buf, len, pos, "%.2f_KB", val_ * 1.0 / 1_KB);
      }
    } else if (val_ < 1_GB) {
      if ((val_ % 1_MB) == 0) {
        databuff_printf(buf, len, pos, "%lld_MB", val_ / 1_MB);
      } else {
        databuff_printf(buf, len, pos, "%.2f_MB", val_ * 1.0 / 1_MB);
      }
    } else if (val_ < 1_TB) {
      if ((val_ % 1_GB) == 0) {
        databuff_printf(buf, len, pos, "%lld_GB", val_ / 1_GB);
      } else {
        databuff_printf(buf, len, pos, "%.2f_GB", val_ * 1.0 / 1_GB);
      }
    } else {
      if ((val_ % 1_TB) == 0) {
        databuff_printf(buf, len, pos, "%lld_TB", val_ / 1_TB);
      } else {
        databuff_printf(buf, len, pos, "%.2f_TB", val_ * 1.0 / 1_TB);
      }
    }
    return pos;
  }
  unsigned long long val_;
};

struct ObTimeLiteralPrettyPrinter {
  constexpr ObTimeLiteralPrettyPrinter(unsigned long long val) : val_(val) {}
  int64_t to_string(char *buf, const int64_t len) const {
    int64_t pos = 0;
    if (val_ < 1_ms) {
      databuff_printf(buf, len, pos, "%lld_us", val_);
    } else if (val_ < 1_s) {
      if ((val_ % 1_s) == 0) {
        databuff_printf(buf, len, pos, "%lld_ms", val_ / 1_s);
      } else {
        databuff_printf(buf, len, pos, "%.2f_ms", val_ * 1.0 / 1_s);
      }
    } else if (val_ < 1_min) {
      if ((val_ % 1_s) == 0) {
        databuff_printf(buf, len, pos, "%lld_s", val_ / 1_s);
      } else {
        databuff_printf(buf, len, pos, "%.2f_s", val_ * 1.0 / 1_s);
      }
    } else if (val_ < 1_hour) {
      if ((val_ % 1_min) == 0) {
        databuff_printf(buf, len, pos, "%lld_min", val_ / 1_min);
      } else {
        databuff_printf(buf, len, pos, "%.2f_min", val_ * 1.0 / 1_min);
      }
    } else if (val_ < 1_day) {
      if ((val_ % 1_hour) == 0) {
        databuff_printf(buf, len, pos, "%lld_hour", val_ / 1_hour);
      } else {
        databuff_printf(buf, len, pos, "%.2f_hour", val_ * 1.0 / 1_hour);
      }
    } else {
      if ((val_ % 1_day) == 0) {
        databuff_printf(buf, len, pos, "%lld_day", val_ / 1_day);
      } else {
        databuff_printf(buf, len, pos, "%.2f_day", val_ * 1.0 / 1_day);
      }
    }
    return pos;
  }
  unsigned long long val_;
};

}// oceanbase
#endif
