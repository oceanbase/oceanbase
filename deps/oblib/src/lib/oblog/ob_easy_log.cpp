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

#include "easy_define.h"
#include "util/easy_string.h"
#include "io/easy_baseth_pool.h"
#include "util/easy_time.h"
#include "io/easy_io.h"
#include "io/easy_log.h"
#include <pthread.h>
#include "lib/ob_define.h"
#include "lib/profile/ob_trace_id.h"
namespace oceanbase
{
namespace common
{
extern const char *const OB_EASY_LOG_LEVEL_STR[];

void ob_easy_log_format(int level, const char *file, int line, const char *function,
                        uint64_t location_hash_val,
                        const char *fmt, ...)
{
  UNUSED(location_hash_val);
  // TODO: use OBLOG interface.
  constexpr int time_buf_len = 32;
  char time_str[time_buf_len];
  ev_tstamp now = 0.0;
  int len = 0;
  int vlen = 0;
  const int buf_len= 4096;
  char buffer[buf_len];
  UNUSED(function);

  // Take from the loop
  if (OB_ISNULL(easy_baseth_self) || OB_ISNULL(easy_baseth_self->loop)) {
    now = static_cast<ev_tstamp>(time(NULL));
  } else {
    now = ev_now(easy_baseth_self->loop);
  }

  {
    time_t t;
    struct tm tm;
    t = (time_t) now;
    easy_localtime((const time_t *)&t, &tm);
    if (OB_UNLIKELY(0 > snprintf(time_str, time_buf_len, "[%04d-%02d-%02d %02d:%02d:%02d.%06d]",
                                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                                 tm.tm_hour, tm.tm_min, tm.tm_sec,
                                 (int)((now - static_cast<ev_tstamp>(t)) * 1000 * 1000)))) {
      OB_LOG_RET(ERROR, OB_ERR_SYS, "fail to snprint timestr to buf", KCSTRING(time_str));
    }
  }

  // print
  len = lnprintf(buffer, 256, "%s %-5s %s:%d [%ld][%s] ", time_str,
                 OB_EASY_LOG_LEVEL_STR[level - EASY_LOG_OFF], file, line, GETTID(), ObCurTraceId::get_trace_id_str());
  va_list args;
  va_start(args, fmt);
  vlen = vsnprintf(buffer + len, buf_len - len - 1,  fmt, args);
  len += easy_min(vlen, buf_len - len - 2);
  va_end(args);

  // Remove the last'\n'
  while (buffer[len - 1] == '\n') { len --; }
  if (OB_UNLIKELY(len >= buf_len - 1)) {
    OB_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid buf len", K(len), K(buf_len));
  } else {
    buffer[len++] = '\n';
    buffer[len] = '\0';
  }
  // @todo use easy_log_print instead
  easy_log_print_default(buffer);
}

// @see easy_log_level_t in easy_log.h
const char *const OB_EASY_LOG_LEVEL_STR[] = {"UNKNOWN", "OFF"/*offset 1*/, "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE", "ALL"};
} //end common
} //end oceanbase
