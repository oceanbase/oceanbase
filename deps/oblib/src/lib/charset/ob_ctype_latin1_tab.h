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
#ifndef CHARSET_LATIN1_TAB_H_
#define CHARSET_LATIN1_TAB_H_

#include "lib/charset/ob_ctype.h"
extern unsigned char ctype_latin1[];
extern unsigned char to_lower_latin1[];
extern unsigned char to_upper_latin1[];
extern unsigned char sort_order_latin1[];
extern unsigned char sort_order_latin1_german1_ci[];
extern unsigned char sort_order_latin1_danish_ci[];
extern unsigned char sort_order_latin1_general_ci[];
extern unsigned char sort_order_latin1_general_cs[];
extern unsigned char sort_order_latin1_spanish_ci[];
extern unsigned char sort_order_latin1_de[];
extern const unsigned char combo1map[];
extern const unsigned char combo2map[];
extern unsigned short cs_to_uni[];
extern unsigned char *uni_to_cs[];

#endif  // CHARSET_LATIN1_TAB_H_