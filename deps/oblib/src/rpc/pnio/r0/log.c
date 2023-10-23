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

__thread format_t g_log_fbuf = { sizeof(g_log_fbuf.buf), 0, "" };
#include <time.h>
void do_log(int level, const char* file, int lineno, const char* func, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  g_log_func(level, file, lineno, func, format, ap);
  va_end(ap);
}

pid_t rk_gettid() { return syscall(SYS_gettid); }

static const char* get_log_level_str(int level) {
  const char* log_level_str[] = {"ERROR", "USER_ERROR", "WARN", "INFO", "TRACE", "DEBUG"};
  return (level >= 0 && level < (int)arrlen(log_level_str)) ? log_level_str[level]: "XXX";
}

static const char* format_ts(char* buf, int64_t limit, int64_t time_us) {
  const char* format = "%Y-%m-%d %H:%M:%S";
  buf[0] = '\0';
  struct tm time_struct;
  int64_t time_s = time_us / 1000000;
  int64_t cur_second_time_us = time_us % 1000000;
  if (NULL != localtime_r(&time_s, &time_struct)) {
    int64_t pos = strftime(buf, limit, format, &time_struct);
    if (pos < limit) {
      snprintf(buf + pos, limit - pos, ".%.6ld", cur_second_time_us);
    }
  }
  return buf;
}

static void log_print(const char* s) { fprintf(stderr, "%s\n", s); }
void default_log_func(int level, const char* file, int lineno, const char* func, const char* format, va_list ap) {
  char time_str[128] = "";
  format_t f = {sizeof(f.buf), 0, ""};
  format_append(&f, "[%s] %s %s %s:%d [%d] ", format_ts(time_str, sizeof(time_str), rk_get_us()), get_log_level_str(level), func, file, lineno, rk_gettid());
  format_vsf(&f, format, ap);
  log_print(format_gets(&f));
}

int g_log_level = LOG_LEVEL_INFO;
log_func_t g_log_func = default_log_func;
