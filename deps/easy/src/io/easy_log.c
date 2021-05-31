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

#include "io/easy_log.h"
#include "util/easy_time.h"
#include <pthread.h>
#include <unistd.h>
#include <sys/syscall.h>
#include "io/easy_io.h"

#define gettid() syscall(__NR_gettid)

easy_log_print_pt easy_log_print = easy_log_print_default;
easy_log_format_pt easy_log_format = easy_log_format_default;
easy_log_level_t easy_log_level = EASY_LOG_INFO;

__attribute__((constructor)) void init_easy_warn_threshold()
{
  ev_loop_warn_threshold = atoll(getenv("easy_warn_threshold") ?: "10000");
}

void easy_log_set_print(easy_log_print_pt p)
{
  easy_log_print = p;
  ev_set_syserr_cb(easy_log_print);
}

int64_t get_us()
{
  return fast_current_time();
}

uint64_t easy_fnv_hash(const char* str)
{
  uint32_t offset_basis = 0x811C9DC5;
  uint32_t prime = 0x01000193;
  uint32_t fnv1 = offset_basis;
  uint32_t fnv1a = offset_basis;
  const char* p = str;
  while (*p != '\0') {
    fnv1 *= prime;
    fnv1 ^= *p;
    fnv1a ^= *p;
    fnv1a *= prime;
    p++;
  }
  return (uint64_t)fnv1 << 32 | fnv1a;
}

void easy_log_set_format(easy_log_format_pt p)
{
  easy_log_format = p;
}

void easy_log_format_default(
    int level, const char* file, int line, const char* function, uint64_t location_hash_val, const char* fmt, ...)
{
  ((void)(location_hash_val));
  static __thread ev_tstamp oldtime = 0.0;
  static __thread char time_str[32];
  ev_tstamp now;
  int len;
  char buffer[4096];

  if (easy_baseth_self && easy_baseth_self->loop) {
    now = ev_now(easy_baseth_self->loop);
  } else {
    now = time(NULL);
  }

  if (oldtime != now) {
    time_t t;
    struct tm tm;
    oldtime = now;
    t = (time_t)now;
    easy_localtime((const time_t*)&t, &tm);
    lnprintf(time_str,
        32,
        "[%04d-%02d-%02d %02d:%02d:%02d.%03d]",
        tm.tm_year + 1900,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec,
        (int)((now - t) * 1000));
  }

  // print
  len = lnprintf(buffer, 128, "%s %s:%d(tid:%lx)[%ld] ", time_str, file, line, pthread_self(), gettid());
  va_list args;
  va_start(args, fmt);
  len += easy_vsnprintf(buffer + len, 4090 - len, fmt, args);
  va_end(args);

  while (buffer[len - 1] == '\n') {
    len--;
  }

  buffer[len++] = '\n';
  buffer[len] = '\0';

  easy_log_print(buffer);
}

void easy_log_print_default(const char* message)
{
  easy_ignore(write(2, message, strlen(message)));
}

void __attribute__((constructor)) easy_log_start_()
{
  char* p = getenv("easy_log_level");

  if (p)
    easy_log_level = (easy_log_level_t)atoi(p);
}
