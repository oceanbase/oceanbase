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

#ifndef OCEANBASE_SAFE_SNPRINTF_H_
#define OCEANBASE_SAFE_SNPRINTF_H_

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdarg.h>
#include "lib/utility/ob_macro_utils.h"

EXTERN_C_BEGIN

/**
   A (very) limited version of snprintf.
   @param   to   Destination buffer.
   @param   n    Size of destination buffer.
   @param   fmt  printf() style format string.
   @returns Number of bytes written, including terminating '\0'
   Supports 'd' 'i' 'u' 'x' 'p' 's' conversion.
   Supports 'l' and 'll' modifiers for integral types.
   Does not support any width/precision.
   Implemented with simplicity, and async-signal-safety in mind.
*/
int _safe_vsnprintf(char *to, size_t size, const char *format, va_list ap);
int _safe_snprintf(char *to, size_t n, const char *fmt, ...);

#define safe_snprintf(_s, _n, ...)                            \
  _safe_snprintf((char *)(_s), (size_t)(_n), __VA_ARGS__)

#define safe_vsnprintf(_s, _n, _f, _a)                \
  _safe_vsnprintf((char *)(_s), (size_t)(_n), _f, _a)

EXTERN_C_END

#endif // OCEANBASE_SAFE_SNPRINTF_H_
