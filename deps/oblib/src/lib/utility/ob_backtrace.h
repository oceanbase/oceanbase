/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
int light_backtrace(void **buffer, int size);
int light_backtrace(void **buffer, int size, int64_t rbp);
// save one layer of call stack
#define ob_backtrace(buffer, size)                                \
  ({                                                              \
    int rv = 0;                                                   \
    if (OB_LIKELY(::oceanbase::common::g_enable_backtrace)) {     \
      rv = backtrace(buffer, size);                               \
    }                                                             \
    rv;                                                           \
  })
char *lbt();
char *lbt(char *buf, int32_t len);
char *parray(int64_t *array, int size);
char *parray(char *buf, int64_t len, int64_t *array, int size);
void addrs_to_offsets(void **buffer, int size);
} // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_BACKTRACE_H_
