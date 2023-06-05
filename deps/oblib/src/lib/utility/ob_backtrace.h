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

#ifndef OCEANBASE_COMMON_OB_BACKTRACE_H_
#define OCEANBASE_COMMON_OB_BACKTRACE_H_

#include<inttypes.h>
namespace oceanbase
{
namespace common
{
void init_proc_map_info();
extern bool g_enable_backtrace;
const int64_t LBT_BUFFER_LENGTH = 1024;
int ob_backtrace(void **buffer, int size);
// save one layer of call stack
#define OB_BACKTRACE_M(buffer, size)                      \
  ({                                                      \
    int rv = 0;                                           \
    if (OB_LIKELY(::oceanbase::common::g_enable_backtrace)) {   \
      rv = backtrace(buffer, size);                       \
    }                                                     \
  rv;                                                     \
  })
char *lbt();
char *lbt(char *buf, int32_t len);
char *parray(int64_t *array, int size);
char *parray(char *buf, int64_t len, int64_t *array, int size);
void addrs_to_offsets(void **buffer, int size);
} // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_BACKTRACE_H_
