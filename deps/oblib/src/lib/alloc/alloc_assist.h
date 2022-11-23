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

#ifndef _OCEABASE_LIB_ALLOC_ALLOC_ASSIST_H_
#define _OCEABASE_LIB_ALLOC_ALLOC_ASSIST_H_

#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "lib/utility/ob_macro_utils.h"

#define MEMSET(s, c, n) memset(s, c, n)
#define MEMCPY(dest, src, n) memcpy(dest, src, n)
#define MEMCCPY(dest, src, c, n) memccpy(dest, src, c, n)
#define MEMMOVE(dest, src, n) memmove(dest, src, n)
#define BCOPY(src, dest, n) bcopy(src, dest, n)
#define MEMCMP(s1, s2, n) memcmp(s1, s2, n)
#define MEMMEM(s1, n1, s2, n2) memmem(s1, n1, s2, n2)
#define STRCPY(dest, src) strcpy(dest, src)
#define STRNCPY(dest, src, n) strncpy(dest, src, n)
#define STRCMP(s1, s2) strcmp(s1, s2)
#define STRNCMP(s1, s2, n) strncmp(s1, s2, n)
#define STRCASECMP(s1, s2) strcasecmp(s1, s2)
#define STRNCASECMP(s1, s2, n) strncasecmp(s1, s2, n)
#define STRCOLL(s1, s2) strcoll(s1, s2)
#define STRSTR(haystack, needle) strstr(haystack, needle)
#define STRCASESTR(haystack, needle) strcasestr(haystack, needle)
#define MEMCHR(s, c, n) memchr(s, c, n)
#define MEMRCHR(s, c, n) memrchr(s, c, n)
#define RAWMEMCHR(s, c) rawmemchr(s, c)
#define STRCHR(s, c) strchr(s, c)
#define STRRCHR(s, c) strrchr(s, c)
#define STRCHRNUL(s, c) strchrnul(s, c)
#define STRLEN(s) strlen(s)
#define STRPBRK(s, accept) strpbrk(s, accept)
#define STRSEP(stringp, delim) strsep(stringp, delim)
#define STRSPN(s, accept) strspn(s, accept)
#define STRCSPN(s, reject) strcspn(s, reject)
#define STRTOK(str, delim) strtok(str, delim)
#define STRTOK_R(str, delim, saveptr) strtok_r(str, delim, saveptr)

static const uint32_t ACHUNK_PRESERVE_SIZE = 17L << 10;

// memory operation wrappers

#endif /* _OCEABASE_LIB_ALLOC_ALLOC_ASSIST_H_ */
