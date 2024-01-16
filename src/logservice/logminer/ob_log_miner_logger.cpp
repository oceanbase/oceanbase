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
 */

#define USING_LOG_PREFIX LOGMNR

#include "ob_log_miner_logger.h"
#include "lib/timezone/ob_time_convert.h"             // ObTimeConverter

namespace oceanbase
{
namespace oblogminer
{

LogMinerLogger &LogMinerLogger::get_logminer_logger_instance()
{
  static LogMinerLogger logger_instance;
  return logger_instance;
}
LogMinerLogger::LogMinerLogger():
    verbose_(false),
    tz_info_() { }

int LogMinerLogger::set_timezone(const char *timezone)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tz_info_.set_timezone(ObString(timezone)))) {
    LOG_ERROR("parse timezone failed", K(ret), KCSTRING(timezone));
    LOGMINER_STDOUT("parse timezone failed\n");
  }
  return ret;
}

void LogMinerLogger::log_stdout(const char *format, ...)
{
  va_list vl;
  va_start(vl, format);
  vfprintf(stdout, format, vl);
  va_end(vl);
}

void LogMinerLogger::log_stdout_v(const char *format, ...)
{
  if (verbose_) {
    log_stdout(format);
  }
}

int LogMinerLogger::log_progress(int64_t record_num, int64_t current_ts, int64_t begin_ts, int64_t end_ts)
{
  int ret = OB_SUCCESS;
  double percentage = double(current_ts - begin_ts) / double(end_ts - begin_ts);
  double progress = 0;
  int lpad = 0;
  int rpad = 0;
  int64_t pos = 0;
  const ObString nls_format;
  char time_buf[128] = {0};
  // current_ts may exceed end_ts
  if (percentage > 1) {
    percentage = 1;
  }
  progress = percentage * 100;
  lpad = (int)(percentage * PB_WIDTH);
  rpad = PB_WIDTH - lpad;
  if (OB_FAIL(ObTimeConverter::datetime_to_str(current_ts, &tz_info_,
      nls_format, 0, time_buf, sizeof(time_buf), pos))) {
    LOG_WARN("datetime to string failed", K(current_ts), K(tz_info_));
  }
  if (OB_SUCC(ret)) {
    fprintf(stdout, "\r%s [%.*s%*s]%.1lf%%, written records: %jd", time_buf, lpad,
        PB_STR, rpad, "", progress, record_num);
    fflush(stdout);
  }
  return ret;
}

}
}