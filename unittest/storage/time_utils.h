/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef UNITTEST_STORAGE_TIME_UTILS_H_
#define UNITTEST_STORAGE_TIME_UTILS_H_

#include <time.h>
#include <stdint.h>

uint64_t get_cycle_count();
uint64_t get_time(int type);

enum TIME_TYPE {
  CLOCK_CIRCLE,
  REAL,
  PROCESS,
  THREAD
};

uint64_t my_get_time(TIME_TYPE type = THREAD);


#endif /* UNITTEST_STORAGE_TIME_UTILS_H_ */
