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

#include <sys/ioctl.h>
#include <unistd.h>
#include "ob_log_miner_logger.h"
#include "ob_log_miner_timezone_getter.h"
#include "lib/string/ob_string.h"
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
    begin_ts_(0),
    last_ts_(0),
    last_record_num_(0)
{ 
  memset(pb_str_, '>', sizeof(pb_str_)); 
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
  int pb_width = 0;
  int terminal_width = 0;
  int lpad = 0;
  int rpad = 0;
  int64_t pos = 0;
  const ObString nls_format;
  char time_buf[128] = {0};
  char pb_buf[MAX_SCREEN_WIDTH] = {0};
  // current_ts may exceed end_ts
  if (percentage > 1) {
    percentage = 1;
  }
  progress = percentage * 100;
  terminal_width = get_terminal_width();
  terminal_width = (terminal_width < MAX_SCREEN_WIDTH) ? terminal_width : MAX_SCREEN_WIDTH;
  pb_width = terminal_width - FIXED_TERMINAL_WIDTH;
  pb_width = (pb_width < MIN_PB_WIDTH) ? 0 : pb_width;

  if (0 < pb_width) {
    lpad = (int)(percentage * pb_width);
    rpad = pb_width - lpad;
    sprintf(pb_buf, "[%.*s%*s]", lpad, pb_str_, rpad, "");
  }
  if (OB_FAIL(ObTimeConverter::datetime_to_str(current_ts, &LOGMINER_TZ.get_tz_info(),
      nls_format, 0, time_buf, sizeof(time_buf), pos))) {
    LOG_WARN("datetime to string failed", K(current_ts), K(LOGMINER_TZ.get_tz_info()));
  } else {
    int64_t current_time = ObTimeUtility::current_time();
    int64_t average_rps = 0;
    int64_t current_rps = 0;
    int64_t inc_record_num = 0;
    if (begin_ts_ == 0) {
      begin_ts_ = current_time;
    }
    if (last_ts_ == 0) {
      last_ts_ = current_time;
    }
    if (current_time - begin_ts_ > 0) {
      // calculated in seconds
      average_rps = record_num * 1000 * 1000 / (current_time - begin_ts_);
    }
    if (current_time - last_ts_ > 0) {
      inc_record_num = record_num - last_record_num_;
      // calculated in seconds
      current_rps = inc_record_num * 1000 * 1000 / (current_time - last_ts_);
      last_ts_ = current_time;
      last_record_num_ = record_num;
    }
    fprintf(stdout, "\r%s %s %5.1lf%%, written records: %5.1jd, current rps: %5.1jd, average rps: %5.1jd", 
        time_buf, pb_buf, progress, record_num, current_rps, average_rps);
    fflush(stdout);
  }
  return ret;
}

int LogMinerLogger::get_terminal_width()
{
  int tmp_ret = OB_SUCCESS;
  struct winsize terminal_size {};
  if (isatty(STDOUT_FILENO)) {
    if (-1 == ioctl(STDOUT_FILENO, TIOCGWINSZ, &terminal_size)) { // On error, -1 is returned
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN_RET(tmp_ret, "get terminal width failed", K(errno), K(strerror(errno)));
    }
  }
  // if get terminal width failed, return 0.
  return tmp_ret == OB_SUCCESS ? terminal_size.ws_col : 0;
}

}
}