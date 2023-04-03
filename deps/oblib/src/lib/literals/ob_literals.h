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
}// oceanbase