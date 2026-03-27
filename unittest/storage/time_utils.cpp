/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "time_utils.h"
#include <stdlib.h>             /* exit */
#include <stdio.h>              /* printf */
#include <sched.h>              /* sched_**** */
#include <unistd.h>
#include <stdint.h>
#include <iostream>
#include <limits.h>
#include <time.h>

uint64_t get_cycle_count()
{
  unsigned int lo, hi;
  __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
  return ((uint64_t)hi << 32) | lo;
}


uint64_t get_time(int type)
{
  uint64_t ret = 0;
  struct timespec time;
  clock_gettime(type, &time);
  ret = time.tv_nsec + static_cast<uint64_t>(time.tv_sec) * 1000000000;
  return ret;
}

uint64_t my_get_time(TIME_TYPE type)
{
  uint64_t ret = 0;
  switch (type)
  {
  case CLOCK_CIRCLE: {
    ret = get_cycle_count();
    break;
  }
  case REAL: {
    ret = get_time(CLOCK_REALTIME);
    break;
  }
  case PROCESS: {
    ret = get_time(CLOCK_PROCESS_CPUTIME_ID);
    break;
  }
  case THREAD: {
    ret = get_time(CLOCK_THREAD_CPUTIME_ID);
    break;
  }
  default:
    break;
  }
  return ret;
}
