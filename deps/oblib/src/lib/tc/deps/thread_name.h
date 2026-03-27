/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <sys/prctl.h>
#include <stdarg.h>
static void tc_set_thread_name(const char* name)
{
  prctl(PR_SET_NAME, name, 0, 0, 0);
}

static void tc_format_thread_name(const char* fmt, ...) __attribute__((format(printf, 1, 2)));
static void tc_format_thread_name(const char* fmt, ...)
{
  char buf[16];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  tc_set_thread_name(buf);
}
