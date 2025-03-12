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

#ifndef CHARSET_SJIS_TAB_H_
#define CHARSET_SJIS_TAB_H_

#include "lib/charset/ob_ctype.h"

extern unsigned char ctype_sjis[];
extern unsigned char to_lower_sjis[];
extern unsigned char to_upper_sjis[];
extern unsigned char sort_order_sjis[];
extern const ObUnicaseInfoChar *ob_caseinfo_pages_sjis[];
extern const uint16 sjis_to_unicode[];
extern const uint16 unicode_to_sjis[];

#define issjishead(c) \
  ((0x81 <= (c) && (c) <= 0x9f) || ((0xe0 <= (c)) && (c) <= 0xfc))
#define issjistail(c) \
  ((0x40 <= (c) && (c) <= 0x7e) || (0x80 <= (c) && (c) <= 0xfc))
#define sjiscode(c, d) ((((uint)(uchar)(c)) << 8) | (uint)(uchar)(d))

#endif  // CHARSET_SJIS_TAB_H_
