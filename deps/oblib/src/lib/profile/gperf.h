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

#ifndef __OB_COMMON_GPERF_H__
#define __OB_COMMON_GPERF_H__

#ifdef __NEED_PERF__

#include <gperftools/profiler.h>
#include <signal.h>

#define SIG_PROFILER_TRIGGER 35

struct PerfGuard
{
  PerfGuard(const char *key): file_(getenv(key)) { if (file_) { ProfilerStart(file_); } }
  ~PerfGuard() { if (file_) { ProfilerStop(); } }
  void register_threads() { ProfilerRegisterThread(); }
  const char *file_;
};

void sig_profiler_start_handler()
{
  char * profile_output = getenv("PROFILEOUTPUT");
  if (NULL != profile_output) {
    ProfilerStart(profile_output);
    //ProfilerRegisterThread();
  } else {
    ProfilerStart("/tmp/gperf.prof");
  }
}

void sig_profiler_stop_handler()
{
  ProfilerStop();
  ProfilerFlush();
}

void sig_profiler_trigger_handler(int sig)
{
  (void)sig;
  static int gperf_state = 0;
  switch (gperf_state) {
  case 0: sig_profiler_start_handler(); break;
  case 1: sig_profiler_stop_handler(); break;
  }
  if (++gperf_state == 2) {
    gperf_state = 0;
  }
}

void register_gperf_handlers()
{
  signal(SIG_PROFILER_TRIGGER, sig_profiler_trigger_handler);
}

#else

struct PerfGuard
{
  PerfGuard(const char *str) { UNUSED(str); }
  ~PerfGuard() {}
  void register_threads() {}
};

#endif

#endif /* __OB_COMMON_GPERF_H__ */
