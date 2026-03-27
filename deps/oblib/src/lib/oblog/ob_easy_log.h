/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_EASY_LOG_H
#define OCEANBASE_COMMON_OB_EASY_LOG_H
#include <cstdint>
namespace oceanbase
{
namespace common
{
// customize libeasy log format function
void ob_easy_log_format(int level, const char *file, int line, const char *function,
                        uint64_t location_hash_val, const char *fmt, ...);
} //end common
} //end oceanbase
#endif /* _OB_EASY_LOG_H */
