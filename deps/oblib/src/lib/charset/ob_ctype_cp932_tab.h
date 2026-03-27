/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef CHARSET_CP932_TAB_H_
#define CHARSET_CP932_TAB_H_

#include "lib/charset/ob_ctype.h"
extern unsigned char ctype_cp932[257];
extern unsigned char to_lower_cp932[];
extern unsigned char to_upper_cp932[];
extern unsigned char sort_order_cp932[];
extern ObUnicaseInfo ob_caseinfo_cp932;
extern  uint16_t cp932_to_unicode[65536];
extern  uint16_t unicode_to_cp932[65536];

#define iscp932head(c) \
  ((0x81 <= (c) && (c) <= 0x9f) || ((0xe0 <= (c)) && (c) <= 0xfc))
#define iscp932tail(c) \
  ((0x40 <= (c) && (c) <= 0x7e) || (0x80 <= (c) && (c) <= 0xfc))

#endif  // CHARSET_CP932_TAB_H_
