/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
