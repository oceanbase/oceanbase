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

#ifndef OCEANBASE_COMMON_OB_LOG_TIME_FMT_H
#define OCEANBASE_COMMON_OB_LOG_TIME_FMT_H
#include <stdint.h>

enum TimeRange {
  YEAR = 0,
  MONTH = 1,
  DAY = 2,
  HOUR = 3,
  MINUTE = 4,
  SECOND = 5,
  MSECOND = 6,
  USECOND = 7
};

namespace oceanbase
{
namespace common
{

struct ObTime2Str {
  static const char *ob_timestamp_str(const int64_t ts);
  template <TimeRange BEGIN, TimeRange TO>
  static const char *ob_timestamp_str_range(const int64_t ts)
  {
    static_assert(int(BEGIN) <= int(TO), "send range must not less than first range.");
    return ob_timestamp_str_range_(ts, BEGIN, TO);
  }
  static const char *ob_timestamp_str_range_(const int64_t ts, TimeRange begin, TimeRange to);
};

} //end common
} //end oceanbase
#endif