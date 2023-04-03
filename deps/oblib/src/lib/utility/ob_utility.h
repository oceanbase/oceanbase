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

#ifndef OB_UTILITY_H_
#define OB_UTILITY_H_

#include <stdint.h>
#include <time.h>

namespace oceanbase
{
namespace common
{
int ob_get_abs_timeout(const uint64_t timeout_us, struct timespec &abs_timeout);
int64_t lower_align(int64_t input, int64_t align);
int64_t upper_align(int64_t input, int64_t align);
char* upper_align_buf(char *in_buf, int64_t align);
int64_t ob_pwrite(const int fd, const char *buf, const int64_t count, const int64_t offset);
int64_t ob_pread(const int fd, char *buf, const int64_t count, const int64_t offset);
int mprotect_page(const void *mem_ptr, int64_t len, int prot, const char *addr_name);
}
}
#endif /* OB_UTILITY_H_ */
