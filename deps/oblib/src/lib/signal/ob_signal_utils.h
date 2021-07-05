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

#ifndef OCEANBASE_SIGNAL_UTILS_H_
#define OCEANBASE_SIGNAL_UTILS_H_

#include <stdio.h>
#include <setjmp.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <poll.h>
#include <sys/syscall.h>
#include <fcntl.h>
#define UNW_LOCAL_ONLY
#include <libunwind.h>
#include "lib/signal/ob_signal_struct.h"
#include "lib/signal/safe_snprintf.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_defer.h"
#include "lib/ob_abort.h"

namespace oceanbase {
namespace common {
void safe_sleep_micros(int64_t usec);
int safe_backtrace(unw_context_t& context, char* buf, int64_t len, int64_t& pos);
int safe_backtrace(char* buf, int64_t len, int64_t& pos);

struct DLogLevel {
  enum DLogLevelEnum { DEBUG, INFO, WARN, ERROR };
};

extern int g_log_level_;

class LoggerSwitchGuard {
public:
  static __thread bool g_logger_open_;
  LoggerSwitchGuard(bool open) : bak_(g_logger_open_)
  {
    g_logger_open_ = open;
  }
  ~LoggerSwitchGuard()
  {
    g_logger_open_ = bak_;
  }

private:
  bool bak_;
};

#define __FILENAME__ (strrchr(__FILE__, '/') ? (strrchr(__FILE__, '/') + 1) : __FILE__)

#define DFMT_BEGIN "[%s] %s [%s@%s:%d] [%ld][Y7369676E616C-%lx] "
#define DFMT_END "\n"
#define DFMT(fmt) DFMT_BEGIN fmt DFMT_END

// Tid will change after fork, but TLS remains unchanged, so TLS is not used for caching temporarily
#define DLOG(log_level, fmt, args...)                                                                \
  do {                                                                                               \
    if (common::LoggerSwitchGuard::g_logger_open_ && common::DLogLevel::log_level >= g_log_level_) { \
      int64_t pos = 0;                                                                               \
      char buf[32];                                                                                  \
      safe_current_datetime_str_v2(buf, sizeof(buf), pos);                                           \
      buf[pos] = '\0';                                                                               \
      static __thread char log_buf[256];                                                             \
      int64_t tid = static_cast<int64_t>(syscall(__NR_gettid));                                      \
      ssize_t log_len = safe_snprintf(log_buf,                                                       \
          sizeof(log_buf),                                                                           \
          DFMT(fmt),                                                                                 \
          buf,                                                                                       \
          #log_level,                                                                                \
          __FUNCTION__,                                                                              \
          __FILENAME__,                                                                              \
          __LINE__,                                                                                  \
          tid,                                                                                       \
          get_tl_trace_id().value(),                                                                 \
          ##args);                                                                                   \
      log_buf[sizeof(log_buf) - 1] = '\n';                                                           \
      write(STDERR_FILENO, log_buf, log_len);                                                        \
    }                                                                                                \
  } while (0)

typedef sigjmp_buf ObJumpBuf;
extern __thread ObJumpBuf* g_jmp;

extern void crash_restore_handler(int, siginfo_t*, void*);

template <typename Function>
void do_with_crash_restore(Function&& func, bool& has_crash)
{
  has_crash = false;

  signal_handler_t handler_bak = tl_handler;
  ObJumpBuf* g_jmp_bak = g_jmp;
  ObJumpBuf jmp;
  g_jmp = &jmp;
  int js = sigsetjmp(*g_jmp, 1);
  if (0 == js) {
    tl_handler = crash_restore_handler;
    func();
  } else if (1 == js) {
    has_crash = true;
  } else {
    // unexpected
    ob_abort();
  }
  g_jmp = g_jmp_bak;
  tl_handler = handler_bak;
}

template <typename Function>
void do_with_crash_restore(Function&& func, bool& has_crash, decltype(func())& return_value)
{
  do_with_crash_restore([&]() { return_value = func(); }, has_crash);
}

#define CLOSE(fd) \
  if (fd != -1) { \
    ::close(fd);  \
    fd = -1;      \
  }

void safe_current_datetime_str(char* buf, int64_t len, int64_t& pos);
void safe_current_datetime_str_v2(char* buf, int64_t len, int64_t& pos);

int wait_readable(int fd, int64_t timeout);

}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_SIGNAL_UTILS_H_
