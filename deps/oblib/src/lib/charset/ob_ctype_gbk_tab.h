/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef CHARSET_GBK_TAB_H_
#define CHARSET_GBK_TAB_H_

#include "lib/charset/ob_ctype.h"

extern unsigned char ctype_gbk[257];
extern unsigned char to_lower_gbk[];
extern unsigned char to_upper_gbk[];
extern const ObUnicaseInfoChar cA2[256];
extern const ObUnicaseInfoChar cA3[256];
extern const ObUnicaseInfoChar cA6[256];
extern const ObUnicaseInfoChar cA7[256];
extern const ObUnicaseInfoChar *ob_caseinfo_pages_gbk[256];
extern ObUnicaseInfo ob_caseinfo_gbk;
extern unsigned char sort_order_gbk[];
extern const uint16_t gbk_order[];
extern const uint16_t tab_gbk_uni0[];
extern const uint16_t tab_uni_gbk0[];
extern const uint16_t tab_uni_gbk1[];
extern const uint16_t tab_uni_gbk2[];
extern const uint16_t tab_uni_gbk3[];
extern const uint16_t tab_uni_gbk4[];
extern const uint16_t tab_uni_gbk5[];
extern const uint16_t tab_uni_gbk6[];
extern const uint16_t tab_uni_gbk_pua[];
extern const uint16_t tab_uni_gbk7[];
extern const uint16_t tab_uni_gbk8[];
int func_gbk_uni_onechar(int code);
int func_uni_gbk_onechar(int code);
#endif  // CHARSET_GBK_TAB_H_
