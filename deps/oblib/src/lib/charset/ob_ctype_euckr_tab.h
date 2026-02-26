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

#ifndef CHARSET_EUCKR_TAB_H_
#define CHARSET_EUCKR_TAB_H_

#include "lib/charset/ob_ctype.h"
extern unsigned char ctype_euc_kr[257];
extern unsigned char to_lower_euc_kr[];
extern  unsigned char to_upper_euc_kr[];
extern  unsigned char sort_order_euc_kr[];
extern ObUnicaseInfo ob_caseinfo_euckr;
extern int func_ksc5601_uni_onechar(int code);
extern int func_uni_ksc5601_onechar(int code);

#define iseuc_kr_head(c) ((0x81 <= (uint8_t)(c) && (uint8_t)(c) <= 0xfe))
#define iseuc_kr_tail1(c) ((uint8_t)(c) >= 0x41 && (uint8_t)(c) <= 0x5A)
#define iseuc_kr_tail2(c) ((uint8_t)(c) >= 0x61 && (uint8_t)(c) <= 0x7A)
#define iseuc_kr_tail3(c) ((uint8_t)(c) >= 0x81 && (uint8_t)(c) <= 0xFE)
#define iseuc_kr_tail(c) \
  (iseuc_kr_tail1(c) || iseuc_kr_tail2(c) || iseuc_kr_tail3(c))

#endif  // CHARSET_EUCKR_TAB_H_