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

#ifndef CHARSET_GB18030_TAB_H_
#define CHARSET_GB18030_TAB_H_

#include "lib/charset/ob_ctype.h"

extern unsigned char ctype_gb18030[257];
extern unsigned char to_lower_gb18030[];
extern unsigned char to_upper_gb18030[];
extern unsigned char sort_order_gb18030[];

extern unsigned char sort_order_gb18030_ci[];
const extern ObUnicaseInfoChar *ob_caseinfo_pages_gb18030[256];
extern ObUnicaseInfo ob_caseinfo_gb18030;
extern const uint16_t tab_gb18030_2_uni[24192];
extern const uint16_t tab_gb18030_4_uni[10922];
extern const uint16_t tab_uni_gb18030_p1[40742];
extern const uint16_t tab_uni_gb18030_p2[3897];
extern const uint16_t gb18030_2_weight_py[];
extern const uint16_t gb18030_4_weight_py_p1[];
extern const uint16_t gb18030_4_weight_py_p2[];

/*[GB+82358F33, GB+82359134] or [U+9FA6, U+9FBB]*/
const static int GB_2022_CNT_PART_1 = 0x9FBB - 0x9FA6 + 1;
/*[GB+84318236, GB+84318537]*/
const static int GB_2022_CNT_PART_2 = (0x85 - 0x82) * 10 + (0x37 - 0x36) + 1;

extern uint16 tab_gb18030_2022_2_uni[];
extern uint16 tab_gb18030_2022_4_uni[];
extern uint16 tab_uni_gb18030_2022_p1[];
extern uint16 tab_uni_gb18030_2022_p2[];

#endif  // CHARSET_GB18030_TAB_H_