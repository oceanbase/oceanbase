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

#ifndef CHARSET_HKSCS_TAB_H_
#define CHARSET_HKSCS_TAB_H_

#include "lib/charset/ob_ctype.h"
#include <unordered_map>

using std::unordered_map;
using std::pair;

extern unsigned char ctype_hkscs[257];
extern unsigned char to_lower_hkscs[];
extern unsigned char to_upper_hkscs[];
extern unsigned char sort_order_hkscs[];
extern const ObUnicaseInfoChar *ob_caseinfo_pages_hkscs[256];

extern const pair<uint16_t,uint16_t> uni_to_hkscs_map_array[];
extern unordered_map<uint16_t,uint16_t> uni_to_hkscs_map;
extern const pair<uint16_t, uint16_t> hkscs_to_uni_map_array[];
extern unordered_map<uint16_t, uint16_t> hkscs_to_uni_map;

extern const int size_of_hkscs_to_uni_map_array;
extern const int size_of_uni_to_hkscs_map_array;

/*
    this is different form mysql is hkscs for the newly added char in hkscs
*/
#define ishkscshead(c) (0x81 <= (unsigned char)(c) && (unsigned char)(c) <= 0xfe)
#define ishkscstail(c)                            \
  ((0x40 <= (unsigned char)(c) && (unsigned char)(c) <= 0x7e) || \
   (0xa1 <= (unsigned char)(c) && (unsigned char)(c) <= 0xfe))

#define ishkscscode(c, d) (ishkscshead(c) && ishkscstail(d))
#define hkscscode(c, d) (((unsigned char)(c) << 8) | (unsigned char)(d))
#define hkscshead(e) ((unsigned char)(e >> 8))
#define hkscstail(e) ((unsigned char)(e & 0xff))

int func_hkscs_uni_onechar(int code);
int func_uni_hkscs_onechar(int code);

#endif  // CHARSET_HKSCS_TAB_H_