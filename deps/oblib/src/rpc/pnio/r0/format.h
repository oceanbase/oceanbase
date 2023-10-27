/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <stdarg.h>
typedef struct format_t {
  int64_t limit;
  int64_t pos;
  char buf[1024];
} format_t;

extern void format_init(format_t* f, int64_t limit);
extern void format_reset(format_t* f);
extern char* format_gets(format_t* f);
extern char* format_append(format_t* f, const char* format, ...) __attribute__((format(printf, 2, 3)));
extern char* format_sf(format_t* f, const char* format, ...) __attribute__((format(printf, 2, 3)));
extern char* strf(char* buf, int64_t size, const char *f, ...) __attribute__((format(printf, 3, 4))) ;
