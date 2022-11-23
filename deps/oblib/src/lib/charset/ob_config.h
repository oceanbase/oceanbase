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

#ifndef OB_CONFIG_H
#define OB_CONFIG_H

#define _GNU_SOURCE 1


#define SIZEOF_SIZE_T 8
#define SIZEOF_CHARP  8
#define SIZEOF_VOIDP  8
#define SIZEOF_LONG   8

#define SIZEOF_CHAR 1
#define HAS_CHAR 1
#define HAS_LONG 1

#define HAS_CHARP 1
#define SIZEOF_SHORT 2
#define HAS_SHORT 1
#define SIZEOF_INT 4
#define HAS_INT 1
#define SIZEOF_LONG_LONG 8
#define HAS_LONG_LONG 1
#define SIZEOF_OFF_T 8
#define HAS_OFF_T 1
#define SIZEOF_SIGSET_T 128
#define HAS_SIGSET_T 1
#define HAS_SIZE_T 1
#define SIZEOF_UINT 4
#define HAS_UINT 1
#define SIZEOF_ULONG 8
#define HAS_ULONG 1
#define HAS_U_INT32_T 1
#define SIZEOF_U_INT32_T 4
#define HAS_MBSTATE_T
#define MAX_INDEXES 64U
#define QSORT_TYPE_IS_VOID 1
#define SIGNAL_RETURN_TYPE_IS_VOID 1
#define VOID_SIGHANDLER 1
#define RETSIGTYPE void
#define RETQSORTTYPE void
#define STRUCT_RLIMIT struct rlimit
#define SOCKET_SIZE_TYPE socklen_t


#endif
