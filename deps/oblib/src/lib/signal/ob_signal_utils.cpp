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

#define USING_LOG_PREFIX COMMON

#include "lib/signal/ob_signal_utils.h"
#include <time.h>
#include <sys/time.h>
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/charset/ob_mysql_global.h"
#include "lib/signal/ob_libunwind.h"

extern "C" {
extern int ob_poll(struct pollfd *__fds, nfds_t __nfds, int __timeout);
};

namespace oceanbase
{
namespace common
{
int g_log_level_ = DLogLevel::INFO;
_RLOCAL(bool, LoggerSwitchGuard::g_logger_open_);

/* From mongodb */
void safe_sleep_micros(int64_t usec)
{
  auto nsec = usec * 1000;
  constexpr static int64_t k1E9 = 1000000000;
  timespec ts{nsec / k1E9, nsec % k1E9};
  nanosleep(&ts, nullptr);
}

_RLOCAL(ObJumpBuf *, g_jmp);
_RLOCAL(ByteBuf<256>, crash_restore_buffer);

void crash_restore_handler(int sig, siginfo_t *s, void *p)
{
  if (SIGSEGV == sig || SIGABRT == sig ||
      SIGBUS == sig || SIGFPE == sig) {
    int64_t len = 0;
#ifdef __x86_64__
    safe_backtrace(crash_restore_buffer, 255, &len);
#endif
    crash_restore_buffer[len++] = '\0';
    siglongjmp(*g_jmp, 1);
  } else {
    ob_signal_handler(sig, s, p);
  }
}

#define LEAPOCH (946684800LL + 86400*(31+29))
#define DAYS_PER_400Y (365*400 + 97)
#define DAYS_PER_100Y (365*100 + 24)
#define DAYS_PER_4Y   (365*4   + 1)

int safe_secs_to_tm(long long t, struct tm *tm)
{
  long long days, secs;
  int remdays, remsecs, remyears;
  int qc_cycles, c_cycles, q_cycles;
  int years, months;
  int wday, yday, leap;
  static const char days_in_month[] = {31,30,31,30,31,31,30,31,30,31,31,29};

  if (t < INT_MIN * 31622400LL || t > INT_MAX * 31622400LL)
    return -1;

  secs = t - LEAPOCH;
  days = secs / 86400;
  remsecs = secs % 86400;
  if (remsecs < 0) {
    remsecs += 86400;
    days--;
  }

  wday = (3+days)%7;
  if (wday < 0) wday += 7;

  qc_cycles = days / DAYS_PER_400Y;
  remdays = days % DAYS_PER_400Y;
  if (remdays < 0) {
    remdays += DAYS_PER_400Y;
    qc_cycles--;
  }

  c_cycles = remdays / DAYS_PER_100Y;
  if (c_cycles == 4) c_cycles--;
  remdays -= c_cycles * DAYS_PER_100Y;

  q_cycles = remdays / DAYS_PER_4Y;
  if (q_cycles == 25) q_cycles--;
  remdays -= q_cycles * DAYS_PER_4Y;

  remyears = remdays / 365;
  if (remyears == 4) remyears--;
  remdays -= remyears * 365;

  leap = !remyears && (q_cycles || !c_cycles);
  yday = remdays + 31 + 28 + leap;
  if (yday >= 365+leap) yday -= 365+leap;

  years = remyears + 4*q_cycles + 100*c_cycles + 400*qc_cycles;

  for (months=0; days_in_month[months] <= remdays; months++)
    remdays -= days_in_month[months];

  if (years+100 > INT_MAX || years+100 < INT_MIN)
    return -1;

  tm->tm_year = years + 100;
  tm->tm_mon = months + 2;
  if (tm->tm_mon >= 12) {
    tm->tm_mon -=12;
    tm->tm_year++;
  }
  tm->tm_mday = remdays + 1;
  tm->tm_wday = wday;
  tm->tm_yday = yday;

  tm->tm_hour = remsecs / 3600;
  tm->tm_min = remsecs / 60 % 60;
  tm->tm_sec = remsecs % 60;

  return 0;
}

void safe_current_datetime_str(char *buf, int64_t len, int64_t &pos)
{
  pos = 0;
  struct timespec ts = {0, 0};
  clock_gettime(CLOCK_REALTIME, &ts);
  struct tm tm;
  safe_secs_to_tm(ts.tv_sec, &tm);
  int64_t count = safe_snprintf(buf, len, "%d%d%d%d%d%d",
                                tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                                tm.tm_hour, tm.tm_min, tm.tm_sec);
  if (count >= 0 && count < len) {
    pos = count;
  } else { pos = 0;/*overflow*/}
}

void safe_current_datetime_str_v2(char *buf, int64_t len, int64_t &pos)
{
  pos = 0;
  struct timespec ts = {0, 0};
  clock_gettime(CLOCK_REALTIME, &ts);
  struct tm tm;
  safe_secs_to_tm(ts.tv_sec, &tm);
  int64_t count = safe_snprintf(buf, len, "%d-%d-%d %d:%d:%d.%ld",
                                tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                                tm.tm_hour, tm.tm_min, tm.tm_sec, (long)(ts.tv_nsec * 1e-3));
  if (count >= 0 && count < len) {
    pos = count;
  } else { pos = 0;/*overflow*/}
}

int wait_readable(int fd, int64_t timeout)
{
  int ret = OB_SUCCESS;
  int flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  DEFER(fcntl(fd, F_SETFL, flags););
  struct pollfd pfd;
  bzero(&pfd, sizeof(pfd));
  pfd.fd = fd;
  pfd.events = POLLIN;
  int n = ob_poll(&pfd, 1, timeout);
  if (-1 == n) {
    ret = OB_ERR_SYS;
    DLOG(WARN, "poll failed, errno=%d", errno);
  } else if (0 == n) {
    ret = OB_TIMEOUT;
    DLOG(DEBUG, "timeout");
  } else if (n != 1) {
    ret = OB_ERR_UNEXPECTED;
    DLOG(WARN, "unexpected fd number");
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
