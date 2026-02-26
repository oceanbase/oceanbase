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
