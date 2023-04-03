// Copyright (c) 2014 Alibaba Inc. All Rights Reserved.
// Author:
//
// ob_hrtime.h
// This file contains code supporting the Inktomi high-resolution
// timer.

#ifndef OB_LIB_HRTIEM_H_
#define OB_LIB_HRTIEM_H_

#include <time.h>
#include <sys/time.h>
#include <stdlib.h>
#include "lib/ob_define.h"

typedef int64_t ObHRTime;

namespace oceanbase
{
namespace common
{

#define HAVE_CLOCK_GETTIME 1

// Factors to multiply units by to obtain coresponding HRTime values.

#define HRTIME_FOREVER  (10*HRTIME_DECADE)
#define HRTIME_DECADE   (10*HRTIME_YEAR)
#define HRTIME_YEAR     (365*HRTIME_DAY+HRTIME_DAY/4)
#define HRTIME_WEEK     (7*HRTIME_DAY)
#define HRTIME_DAY      (24*HRTIME_HOUR)
#define HRTIME_HOUR     (60*HRTIME_MINUTE)
#define HRTIME_MINUTE   (60*HRTIME_SECOND)
#define HRTIME_SECOND   (1000*HRTIME_MSECOND)
#define HRTIME_MSECOND  (1000*HRTIME_USECOND)
#define HRTIME_USECOND  (1000*HRTIME_NSECOND)
#define HRTIME_NSECOND  (1LL)

#define HRTIME_APPROX_SECONDS(x) ((x)>>30)    // off by 7.3%
#define HRTIME_APPROX_FACTOR     ((static_cast<float>((1<<30)))/((static_cast<float>(HRTIME_SECOND))))

// Map from units to HRTime values,
// imple macros

#define HRTIME_YEARS(x)    ((x)*HRTIME_YEAR)
#define HRTIME_WEEKS(x)    ((x)*HRTIME_WEEK)
#define HRTIME_DAYS(x)     ((x)*HRTIME_DAY)
#define HRTIME_HOURS(x)    ((x)*HRTIME_HOUR)
#define HRTIME_MINUTES(x)  ((x)*HRTIME_MINUTE)
#define HRTIME_SECONDS(x)  ((x)*HRTIME_SECOND)
#define HRTIME_MSECONDS(x) ((x)*HRTIME_MSECOND)
#define HRTIME_USECONDS(x) ((x)*HRTIME_USECOND)
#define HRTIME_NSECONDS(x) ((x)*HRTIME_NSECOND)

// gratuituous wrappers

static inline ObHRTime hrtime_from_years(int64_t years)
{
  return (HRTIME_YEARS(years));
}
static inline ObHRTime hrtime_from_weeks(int64_t weeks)
{
  return (HRTIME_WEEKS(weeks));
}
static inline ObHRTime hrtime_from_days(int64_t days)
{
  return (HRTIME_DAYS(days));
}
static inline ObHRTime hrtime_from_mins(int64_t mins)
{
  return (HRTIME_MINUTES(mins));
}
static inline ObHRTime hrtime_from_sec(int64_t sec)
{
  return (HRTIME_SECONDS(sec));
}
static inline ObHRTime hrtime_from_msec(int64_t msec)
{
  return (HRTIME_MSECONDS(msec));
}
static inline ObHRTime hrtime_from_usec(int64_t usec)
{
  return (HRTIME_USECONDS(usec));
}
static inline ObHRTime hrtime_from_nsec(int64_t nsec)
{
  return (HRTIME_NSECONDS(nsec));
}

static inline ObHRTime hrtime_from_timespec(const struct timespec * ts)
{
  return hrtime_from_sec(ts->tv_sec) + hrtime_from_nsec(ts->tv_nsec);
}

static inline ObHRTime hrtime_from_timeval(const struct timeval * tv)
{
  return hrtime_from_sec(tv->tv_sec) + hrtime_from_usec(tv->tv_usec);
}

// Map from HRTime values to other units

static inline ObHRTime hrtime_to_years(ObHRTime t)
{
  return ((ObHRTime) (t / HRTIME_YEAR));
}
static inline ObHRTime hrtime_to_weeks(ObHRTime t)
{
  return ((ObHRTime) (t / HRTIME_WEEK));
}
static inline ObHRTime hrtime_to_days(ObHRTime t)
{
  return ((ObHRTime) (t / HRTIME_DAY));
}
static inline ObHRTime hrtime_to_mins(ObHRTime t)
{
  return ((ObHRTime) (t / HRTIME_MINUTE));
}
static inline ObHRTime hrtime_to_sec(ObHRTime t)
{
  return ((ObHRTime) (t / HRTIME_SECOND));
}
static inline ObHRTime hrtime_to_msec(ObHRTime t)
{
  return ((ObHRTime) (t / HRTIME_MSECOND));
}
static inline ObHRTime hrtime_to_usec(ObHRTime t)
{
  return ((ObHRTime) (t / HRTIME_USECOND));
}
static inline ObHRTime hrtime_to_nsec(ObHRTime t)
{
  return ((ObHRTime) (t / HRTIME_NSECOND));
}

static inline int64_t usec_to_msec(const int64_t t)
{
  return (static_cast<int64_t> (t / 1000));
}
static inline int64_t usec_to_sec(const int64_t t)
{
  return (static_cast<int64_t> (t / 1000000));
}
static inline int64_t msec_to_sec(const int64_t t)
{
  return (static_cast<int64_t> (t / 1000));
}
static inline int64_t msec_to_usec(const int64_t t)
{
  return (static_cast<int64_t> (t * 1000));
}
static inline int64_t sec_to_msec(const int64_t t)
{
  return (static_cast<int64_t> (t * 1000));
}
static inline int64_t sec_to_usec(const int64_t t)
{
  return (static_cast<int64_t> (t * 1000000));
}

static inline struct timespec hrtime_to_timespec(ObHRTime t)
{
  struct timespec ts;

  ts.tv_sec = hrtime_to_sec(t);
  ts.tv_nsec = t % HRTIME_SECOND;
  return (ts);
}

static inline struct timeval hrtime_to_timeval(ObHRTime t)
{
  int64_t usecs = 0;
  struct timeval tv;

  usecs = hrtime_to_usec(t);
  tv.tv_sec = usecs / 1000000;
  tv.tv_usec = usecs % 1000000;
  return (tv);
}

static inline int hrtime_to_timeval2(ObHRTime t, struct timeval *tv)
{
  int64_t usecs = hrtime_to_usec(t);
  tv->tv_sec = usecs / 1000000;
  tv->tv_usec = usecs % 1000000;
  return 0;
}

static inline ObHRTime get_hrtime_internal()
{
#if HAVE_CLOCK_GETTIME
  timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return hrtime_from_timespec(&ts);
#else
  timeval tv;
  gettimeofday(&tv, NULL);
  return hrtime_from_timeval(&tv);
#endif
}

static inline int64_t hrtime_diff_msec(ObHRTime t1, ObHRTime t2)
{
  return hrtime_to_msec(t1 - t2);
}

static inline ObHRTime hrtime_diff(ObHRTime t1, ObHRTime t2)
{
  return (t1 - t2);
}

static inline ObHRTime hrtime_add(ObHRTime t1, ObHRTime t2)
{
  return (t1 + t2);
}

static inline void hrtime_sleep(ObHRTime delay)
{
  struct timespec ts = hrtime_to_timespec(delay);
  nanosleep(&ts, NULL);
}

} // end of namespace common
} // end of namespace oceanbase

#endif // OB_LIB_HRTIEM_H_
