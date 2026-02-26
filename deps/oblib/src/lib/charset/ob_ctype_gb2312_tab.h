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

#ifndef CHARSET_GB2312_TAB_H_
#define CHARSET_GB2312_TAB_H_

#include "lib/charset/ob_ctype.h"
extern unsigned char ctype_gb2312[257];
// clang-format off
extern unsigned char to_lower_gb2312[];
extern unsigned char to_upper_gb2312[];
extern unsigned char sort_order_gb2312[];
// clang-format on

#define isgb2312head(c) (0xa1 <= (unsigned char)(c) && (unsigned char)(c) <= 0xf7)
#define isgb2312tail(c) (0xa1 <= (unsigned char)(c) && (unsigned char)(c) <= 0xfe)
extern ObUnicaseInfo ob_caseinfo_gb2312;
extern int func_gb2312_uni_onechar(int code);
extern int func_uni_gb2312_onechar(int code);

#endif  // CHARSET_GB2312_TAB_H_