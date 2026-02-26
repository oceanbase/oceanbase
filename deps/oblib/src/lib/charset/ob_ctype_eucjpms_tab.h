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
#ifndef CHARSET_EUCJPMS_TAB_H_
#define CHARSET_EUCJPMS_TAB_H_

#include "lib/charset/ob_ctype.h"
extern unsigned char ctype_eucjpms[257];
extern unsigned char to_lower_eucjpms[];
extern unsigned char to_upper_eucjpms[];
extern unsigned char sort_order_eucjpms[];
extern ObUnicaseInfo my_caseinfo_eucjpms;
extern const uint16_t jisx0208_eucjpms_to_unicode[65536];
extern const uint16_t unicode_to_jisx0208_eucjpms[65536];
extern const uint16_t jisx0212_eucjpms_to_unicode[65536];
extern const uint16_t unicode_to_jisx0212_eucjpms[65536];

#define iseucjpms(c) ((0xa1 <= ((c)&0xff) && ((c)&0xff) <= 0xfe))
#define iskata(c) ((0xa1 <= ((c)&0xff) && ((c)&0xff) <= 0xdf))
#define iseucjpms_ss2(c) (((c)&0xff) == 0x8e)
#define iseucjpms_ss3(c) (((c)&0xff) == 0x8f)

#endif  // CHARSET_EUCJPMS_TAB_H_
