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

#ifndef CHARSET_UJIS_TAB_H_
#define CHARSET_UJIS_TAB_H_

#include "lib/charset/ob_ctype.h"

extern unsigned char ctype_ujis[257];
extern unsigned char to_lower_ujis[];
extern unsigned char to_upper_ujis[];
extern ObUnicaseInfo ob_caseinfo_ujis;
extern unsigned char sort_order_ujis[];
extern uint16_t jisx0208_eucjp_to_unicode[65536];
extern uint16_t jisx0212_eucjp_to_unicode[65536];
extern uint16_t unicode_to_jisx0208_eucjp[65536];
extern uint16_t unicode_to_jisx0212_eucjp[65536];

#define isujis(c) ((0xa1 <= ((c)&0xff) && ((c)&0xff) <= 0xfe))
#define iskata(c) ((0xa1 <= ((c)&0xff) && ((c)&0xff) <= 0xdf))
#define isujis_ss2(c) (((c)&0xff) == 0x8e)
#define isujis_ss3(c) (((c)&0xff) == 0x8f)

#endif  // CHARSET_UJIS_TAB_H_
