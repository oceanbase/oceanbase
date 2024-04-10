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
    verbose_(false) { memset(pb_str_, '>', sizeof(pb_str_)); }

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
  struct winsize w;
  int pb_width = 0;
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

  if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &w) < 0) {
    // don't update pb_width when terminal information cannot be obtained
    if (isatty(STDOUT_FILENO)) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "stdout was directed to terminal but ioctl failed");
    }
  } else {
    // get the information of terminal window successfully
    w.ws_col = (w.ws_col < MAX_SCREEN_WIDTH) ? w.ws_col : MAX_SCREEN_WIDTH;
    // 68: 20->datatime, 2->[], 7->percentage, 19->", written records: ", 20->the length of INT64_MAX
    pb_width = w.ws_col - 68;
  }
  pb_width = (pb_width < 5) ? 0 : pb_width;

  if (pb_width) {
    lpad = (int)(percentage * pb_width);
    rpad = pb_width - lpad;
    sprintf(pb_buf, "[%.*s%*s]", lpad, pb_str_, rpad, "");
  }
  
  if (OB_FAIL(ObTimeConverter::datetime_to_str(current_ts, &LOGMINER_TZ.get_tz_info(),
      nls_format, 0, time_buf, sizeof(time_buf), pos))) {
    LOG_WARN("datetime to string failed", K(current_ts), K(LOGMINER_TZ.get_tz_info()));
  }
  if (OB_SUCC(ret)) {
    fprintf(stdout, "\r%s %s %5.1lf%%, written records: %-20jd", time_buf, pb_buf,
        progress, record_num);
    fflush(stdout);
  }
  return ret;
}

}
}