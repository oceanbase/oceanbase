/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
