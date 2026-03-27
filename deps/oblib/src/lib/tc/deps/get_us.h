/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// static int64_t tc_get_us()
// {
//   struct timespec tp;
//   clock_gettime(CLOCK_REALTIME_COARSE, &tp);
//   //clock_gettime(CLOCK_REALTIME, &tp);
//   return tp.tv_sec * 1000000 + tp.tv_nsec/1000;
// }

static int64_t tc_get_ns()
{
  struct timespec tp;
  clock_gettime(CLOCK_REALTIME, &tp);
  return tp.tv_sec * 1000000000 + tp.tv_nsec;
}
