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
#ifndef CHARSET_HKSCS31_TAB_H_
#define CHARSET_HKSCS31_TAB_H_

#include "lib/charset/ob_ctype.h"
#include <unordered_map>

using std::unordered_map;
using std::pair;

extern const int size_of_hkscs31_to_uni_map_array;
extern const int size_of_uni_to_hkscs31_map_array;

extern const std::pair<uint16_t,int> hkscs31_to_uni_map_array[];
extern unordered_map<uint16_t, int> hkscs31_to_uni_map;
extern const std::pair<int,uint16_t>  uni_to_hkscs31_map_array[];
extern unordered_map<int,uint16_t> uni_to_hkscs31_map;

int func_hkscs31_uni_onechar(int code);
int func_uni_hkscs31_onechar(int code);


#endif  // CHARSET_HKSCS31_TAB_H_
