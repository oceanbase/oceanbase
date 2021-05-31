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

#ifndef OCEANBASE_LIB_OBMYSQL_OB_MYSQL_GLOBAL_
#define OCEANBASE_LIB_OBMYSQL_OB_MYSQL_GLOBAL_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <memory.h>
#include <stddef.h>
#include <stdarg.h>
#include <pthread.h>

#include <math.h>
#include <limits.h>
#include <float.h>
#include <fenv.h>

#include <sys/types.h>
#include <fcntl.h>
#include <time.h>
#include <unistd.h>
#include <alloca.h>

#include <errno.h>
#include <crypt.h>

#include <assert.h>

#define TRUE (1)
#define FALSE (0)

#define MY_MAX(a, b) ((a) > (b) ? (a) : (b))
#define MY_MIN(a, b) ((a) < (b) ? (a) : (b))

typedef unsigned char uchar;
typedef signed char int8;
typedef unsigned char uint8;
typedef short int16;
typedef unsigned short uint16;
typedef int int32;
typedef unsigned int uint32;
typedef unsigned long ulong;
typedef unsigned long ulonglong;
typedef long longlong;
typedef longlong int64;
typedef ulonglong uint64;
typedef char ob_bool;
typedef unsigned int uint;
typedef unsigned short ushort;

#define OB_ALIGN(A, L) (((A) + (L)-1) & ~((L)-1))

//===================================================================

#define ob_sint1korr(P) (*((int8_t*)(P)))
#define ob_uint1korr(P) (*((uint8_t*)P))

#define ob_sint2korr(P) (int16)(*((int16*)(P)))
#define ob_sint3korr(P)                                                                                     \
  ((int32)((((uchar)(P)[2]) & 128)                                                                          \
               ? (((uint32)255L << 24) | (((uint32)(uchar)(P)[2]) << 16) | (((uint32)(uchar)(P)[1]) << 8) | \
                     ((uint32)(uchar)(P)[0]))                                                               \
               : (((uint32)(uchar)(P)[2]) << 16) | (((uint32)(uchar)(P)[1]) << 8) | ((uint32)(uchar)(P)[0])))
#define ob_sint4korr(P) (int32)(*((int32*)(P)))
#define ob_uint2korr(P) (uint16)(*((uint16*)(P)))
#define ob_uint3korr(P) \
  (uint32)(((uint32)((uchar)(P)[0])) + (((uint32)((uchar)(P)[1])) << 8) + (((uint32)((uchar)(P)[2])) << 16))
#define ob_uint4korr(P) (uint32)(*((uint32*)(P)))
#define ob_uint5korr(P)                                                                                           \
  ((ulonglong)(((uint32)((uchar)(P)[0])) + (((uint32)((uchar)(P)[1])) << 8) + (((uint32)((uchar)(P)[2])) << 16) + \
               (((uint32)((uchar)(P)[3])) << 24)) +                                                               \
      (((ulonglong)((uchar)(P)[4])) << 32))
#define ob_uint6korr(P)                                                                                           \
  ((ulonglong)(((uint32)((uchar)(P)[0])) + (((uint32)((uchar)(P)[1])) << 8) + (((uint32)((uchar)(P)[2])) << 16) + \
               (((uint32)((uchar)(P)[3])) << 24)) +                                                               \
      (((ulonglong)((uchar)(P)[4])) << 32) + (((ulonglong)((uchar)(P)[5])) << 40))
#define ob_uint8korr(P) (ulonglong)(*((ulonglong*)(P)))
#define ob_sint8korr(P) (longlong)(*((longlong*)(P)))

#define ob_int1store(P, V)           \
  do {                               \
    *((uint8_t*)(P)) = (uint8_t)(V); \
  } while (0)

#define ob_int2store(P, V)          \
  do {                              \
    uchar* pP = (uchar*)(P);        \
    *((uint16*)(pP)) = (uint16)(V); \
  } while (0)

#define ob_int3store(P, V)                \
  do {                                    \
    *(P) = (uchar)((V));                  \
    *(P + 1) = (uchar)(((uint)(V) >> 8)); \
    *(P + 2) = (uchar)(((V) >> 16));      \
  } while (0)
#define ob_int4store(P, V)          \
  do {                              \
    uchar* pP = (uchar*)(P);        \
    *((uint32*)(pP)) = (uint32)(V); \
  } while (0)

#define ob_int5store(P, V)             \
  do {                                 \
    *(P) = (uchar)((V));               \
    *((P) + 1) = (uchar)(((V) >> 8));  \
    *((P) + 2) = (uchar)(((V) >> 16)); \
    *((P) + 3) = (uchar)(((V) >> 24)); \
    *((P) + 4) = (uchar)(((V) >> 32)); \
  } while (0)
#define ob_int6store(P, V)             \
  do {                                 \
    *(P) = (uchar)((V));               \
    *((P) + 1) = (uchar)(((V) >> 8));  \
    *((P) + 2) = (uchar)(((V) >> 16)); \
    *((P) + 3) = (uchar)(((V) >> 24)); \
    *((P) + 4) = (uchar)(((V) >> 32)); \
    *((P) + 5) = (uchar)(((V) >> 40)); \
  } while (0)
#define ob_int8store(P, V)                \
  do {                                    \
    uchar* pP = (uchar*)(P);              \
    *((ulonglong*)(pP)) = (ulonglong)(V); \
  } while (0)

enum loglevel { ERROR_LEVEL = 0, WARNING_LEVEL = 1, INFORMATION_LEVEL = 2 };

#endif /* OCEANBASE_LIB_OBMYSQL_OB_MYSQL_GLOBAL_ */
