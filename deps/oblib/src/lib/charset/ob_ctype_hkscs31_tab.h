/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
