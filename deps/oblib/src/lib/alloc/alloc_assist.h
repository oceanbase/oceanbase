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
#include "lib/ob_abort.h"
#include "lib/utility/ob_macro_utils.h"

// we use int instead of bool to compatible with c code.
#ifdef __cplusplus
static inline void abort_unless(bool result)
#else
static inline void abort_unless(int result)
#endif
{
  if (!result) {
    ob_abort();
  }
}

#if MEMCHK_LEVEL >= 1
void check_ptr(const void* ptr, const int64_t len);

#define CHKPTR1(ptr) check_ptr(ptr, 0)
#define CHKPTR2(ptr, n) check_ptr(ptr, n)
#define CHKPTR(...) SELECT(3, __VA_ARGS__, CHKPTR2, CHKPTR1)(__VA_ARGS__)

inline void* MEMSET(void* s, int c, size_t n)
{
  CHKPTR(s, n);
  return memset(s, c, n);
}

inline void* MEMCPY(void* dest, const void* src, size_t n)
{
  CHKPTR(dest, n);
  CHKPTR(src, n);
  return memcpy(dest, src, n);
}

inline void* MEMCCPY(void* dest, const void* src, int c, size_t n)
{
  CHKPTR(dest, n);
  CHKPTR(src, n);
  return memccpy(dest, src, c, n);
}

inline void* MEMMOVE(void* dest, const void* src, size_t n)
{
  CHKPTR(dest, n);
  CHKPTR(src, n);
  return memmove(dest, src, n);
}

inline void BCOPY(const void* src, void* dest, size_t n)
{
  CHKPTR(src, n);
  CHKPTR(dest, n);
  bcopy(src, dest, n);
}

inline int MEMCMP(const void* s1, const void* s2, size_t n)
{
  CHKPTR(s1, n);
  CHKPTR(s2, n);
  return memcmp(s1, s2, n);
}

inline char* STRCPY(char* dest, const char* src)
{
  CHKPTR(dest);
  CHKPTR(src);
  return strcpy(dest, src);
}

inline char* STRNCPY(char* dest, const char* src, size_t n)
{
  CHKPTR(dest, n);
  CHKPTR(src, n);
  return strncpy(dest, src, n);
}

inline int STRCMP(const char* s1, const char* s2)
{
  CHKPTR(s1);
  CHKPTR(s2);
  return strcmp(s1, s2);
}

inline int STRNCMP(const char* s1, const char* s2, size_t n)
{
  CHKPTR(s1, n);
  CHKPTR(s2, n);
  return strncmp(s1, s2, n);
}

inline int STRCASECMP(const char* s1, const char* s2)
{
  CHKPTR(s1);
  CHKPTR(s2);
  return strcasecmp(s1, s2);
}

inline int STRNCASECMP(const char* s1, const char* s2, size_t n)
{
  CHKPTR(s1, n);
  CHKPTR(s2, n);
  return strncasecmp(s1, s2, n);
}

inline int STRCOLL(const char* s1, const char* s2)
{
  CHKPTR(s1);
  CHKPTR(s2);
  return strcoll(s1, s2);
}

inline const char* STRSTR(char* haystack, const char* needle)
{
  CHKPTR(haystack);
  CHKPTR(needle);
  return strstr(haystack, needle);
}

inline const char* STRCASESTR(const char* haystack, const char* needle)
{
  CHKPTR(haystack);
  CHKPTR(needle);
  return strcasestr(haystack, needle);
}

inline const void* MEMCHR(const void* s, int c, size_t n)
{
  CHKPTR(s, n);
  return memchr(s, c, n);
}

inline const void* MEMRCHR(const void* s, int c, size_t n)
{
  CHKPTR(s, n);
  return memrchr(s, c, n);
}

inline const void* RAWMEMCHR(const void* s, int c)
{
  CHKPTR(s);
  return rawmemchr(s, c);
}

inline const char* STRCHR(const char* s, int c)
{
  CHKPTR(s);
  return strchr(s, c);
}

inline const char* STRRCHR(const char* s, int c)
{
  CHKPTR(s);
  return strrchr(s, c);
}

inline const char* STRCHRNUL(const char* s, int c)
{
  CHKPTR(s);
  return strchrnul(s, c);
}

inline size_t STRLEN(const char* s)
{
  CHKPTR(s);
  return strlen(s);
}

inline const char* STRPBRK(const char* s, const char* accept)
{
  CHKPTR(s);
  return strpbrk(s, accept);
}

inline char* STRSEP(char** stringp, const char* delim)
{
  CHKPTR(delim);
  return strsep(stringp, delim);
}

inline size_t STRSPN(const char* s, const char* accept)
{
  CHKPTR(s);
  CHKPTR(accept);
  return strspn(s, accept);
}

inline size_t STRCSPN(const char* s, const char* reject)
{
  CHKPTR(s);
  CHKPTR(reject);
  return strcspn(s, reject);
}

inline char* STRTOK(char* str, const char* delim)
{
  CHKPTR(str);
  CHKPTR(delim);
  return strtok(str, delim);
}

inline char* STRTOK_R(char* str, const char* delim, char** saveptr)
{
  CHKPTR(str);
  CHKPTR(delim);
  return strtok_r(str, delim, saveptr);
}

#else  // not in MEMCHK mode

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

#endif  // MEMCHK_LEVEL >= 1

// memory operation wrappers

#endif /* _OCEABASE_LIB_ALLOC_ALLOC_ASSIST_H_ */
