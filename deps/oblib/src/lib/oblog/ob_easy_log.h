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

#ifndef OCEANBASE_COMMON_OB_EASY_LOG_H
#define OCEANBASE_COMMON_OB_EASY_LOG_H
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
