/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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