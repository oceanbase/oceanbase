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
