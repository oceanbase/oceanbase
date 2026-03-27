/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_MEMORY_TRACKER_WRAPPER_H
#define OCEANBASE_SQL_OB_MEMORY_TRACKER_WRAPPER_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

int check_mem_status();
int try_check_mem_status(int64_t check_try_times);

#ifdef __cplusplus
}
#endif

#endif /* OCEANBASE_SQL_OB_MEMORY_TRACKER_WRAPPER_H */
