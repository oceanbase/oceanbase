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

#ifndef CHARSET_BIG5_TAB_H_
#define CHARSET_BIG5_TAB_H_

#include <unordered_map>
#include "lib/charset/ob_ctype.h"

using std::unordered_map;
extern unsigned char ctype_big5[257];
extern unsigned char to_lower_big5[];
extern unsigned char to_upper_big5[];
extern unsigned char sort_order_big5[];
extern const ObUnicaseInfoChar big5cA2[256];
extern const ObUnicaseInfoChar big5cA3[256];
extern const ObUnicaseInfoChar big5cC7[256];
extern const ObUnicaseInfoChar *ob_caseinfo_pages_big5[256];
extern const uint16 tab_big5_uni0[];
extern const uint16 tab_big5_uni1[];

extern const uint16 tab_uni_big50[];
extern const uint16 tab_uni_big51[];
extern const uint16 tab_uni_big52[];
extern const uint16 tab_uni_big53[];
extern const uint16 tab_uni_big54[];
extern const uint16 tab_uni_big55[];
extern const uint16 tab_uni_big56[];
extern const uint16 tab_uni_big57[];
extern const uint16 tab_uni_big58[];
extern const uint16 tab_uni_big59[];
extern const uint16 tab_uni_big510[];

#define hasbig5head(c) (0xa1 <= (unsigned char)(c) && \
                               (unsigned char)(c) <= 0xf9)
#define hasbig5tail(c) \
  ((0x40 <= (unsigned char)(c) && \
            (unsigned char)(c) <= 0x7e) || \
   (0xa1 <= (unsigned char)(c) && \
            (unsigned char)(c) <= 0xfe))

#define isbig5code(c, d) (hasbig5head(c) && hasbig5tail(d))
#define big5code(c, d) (((unsigned char)(c) << 8) | (unsigned char)(d))
#define getbig5head(e) ((unsigned char)(e >> 8))
#define getbig5tail(e) ((unsigned char)(e & 0xff))

int func_big5_uni_onechar(int code);
#endif  // CHARSET_BIG5_TAB_H_