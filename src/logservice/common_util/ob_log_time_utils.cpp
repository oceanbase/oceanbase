/**
* Copyright (c) 2023 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*
*/

#include "ob_log_time_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"  // databuff_printf

using namespace oceanbase::common;

namespace oceanbase
{
namespace logservice
{
int print_human_tstamp(char *buf, const int64_t buf_len, int64_t &pos,
    const int64_t usec_tstamp)
{
  int ret = common::OB_SUCCESS;
  if (common::OB_INVALID_TIMESTAMP == usec_tstamp) {
    ret = common::databuff_printf(buf, buf_len, pos, "[INVALID]");
  }
  else {
    struct timeval tv;
    tv.tv_sec = usec_tstamp / _SEC_;
    tv.tv_usec = usec_tstamp % _SEC_;
    struct tm tm;
    ::localtime_r((const time_t *) &tv.tv_sec, &tm);
    ret = common::databuff_printf(buf, buf_len, pos,
                                  "[%04d-%02d-%02d %02d:%02d:%02d.%06ld]",
                                  tm.tm_year + 1900,
                                  tm.tm_mon + 1,
                                  tm.tm_mday,
                                  tm.tm_hour,
                                  tm.tm_min,
                                  tm.tm_sec,
                                  tv.tv_usec);
  }
  return ret;
}

int print_human_timeval(char *buf,
    const int64_t buf_len,
    int64_t &pos,
    const int64_t usec_tval)
{
  int ret = common::OB_SUCCESS;
  if (INT64_MAX == usec_tval) {
    ret = common::databuff_printf(buf, buf_len, pos, "[INVALID_TVAL]");
  }
  else {
    bool negative = (usec_tval < 0);
    struct timeval tv;
    if (negative) {
      tv.tv_sec = (0 - usec_tval) / _SEC_;
      tv.tv_usec = (0 - usec_tval) % _SEC_;
    }
    else {
      tv.tv_sec = usec_tval / _SEC_;
      tv.tv_usec = usec_tval % _SEC_;
    }
    int64_t hr = static_cast<int64_t>(tv.tv_sec) / 3600;
    int64_t min = (static_cast<int64_t>(tv.tv_sec) / 60) % 60;
    int64_t sec = static_cast<int64_t>(tv.tv_sec) % 60;
    ret = common::databuff_printf(buf, buf_len, pos,
                                  "[%s%02ld:%02ld:%02ld.%06ld]",
                                  negative ? "-" : "",
                                  hr,
                                  min,
                                  sec,
                                  tv.tv_usec);
  }
  return ret;
}

} // namespace logservice
} // namespace oceanbase
