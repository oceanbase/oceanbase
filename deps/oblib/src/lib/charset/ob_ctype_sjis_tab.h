/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
