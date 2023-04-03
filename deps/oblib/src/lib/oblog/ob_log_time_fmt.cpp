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

#include "ob_log_time_fmt.h"
#include <time.h>
#include <stdio.h>
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace common
{

const int TIME_RANGE_INDEX[] = {0,4,
                                5,7,
                                8,10,
                                11,13,
                                14,16,
                                17,19,
                                20,23,
                                23,26};

constexpr int TIME_BUFFER_SIZE = 27;
constexpr int MAX_TIMESTAMP_BUFFER = 1 << 4;// 16
constexpr int IDX_MASK = MAX_TIMESTAMP_BUFFER - 1;
using TimestampStrBuffer_ = char[TIME_BUFFER_SIZE];
using TimestampStrBuffer = TimestampStrBuffer_[MAX_TIMESTAMP_BUFFER];
TLOCAL(TimestampStrBuffer, timestamp_str_buffer);
TLOCAL(int, buffer_idx) = 0;

const char *ObTime2Str::ob_timestamp_str(const int64_t ts)
{
  struct tm t;
  time_t ts_s = ts / 1000000;// convert to second precision
  TimestampStrBuffer_ &buffer = timestamp_str_buffer[buffer_idx++ & IDX_MASK];
  size_t idx = strftime(&buffer[0],
                        sizeof(buffer),
                        "%F %T",
                        localtime_r(&ts_s, &t));
  idx += snprintf(&buffer[idx], TIME_BUFFER_SIZE - idx, ".%ld", ts % 1000000);
  buffer[idx] = '\0';
  return buffer;
}

const char *ObTime2Str::ob_timestamp_str_range_(const int64_t ts, TimeRange begin, TimeRange to)
{
  const char *str = ObTime2Str::ob_timestamp_str(ts);
  const_cast<char*>(str)[TIME_RANGE_INDEX[2 * static_cast<int>(to) + 1]] = '\0';
  str = &str[TIME_RANGE_INDEX[2 * static_cast<int>(begin)]];
  return str;
}

}// namespace common
}// namespace oceanbase