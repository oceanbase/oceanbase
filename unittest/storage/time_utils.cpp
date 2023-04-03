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
