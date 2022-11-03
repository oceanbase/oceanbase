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

#ifndef OCEANBASE_OB_STRING_UTIL_H_
#define OCEANBASE_OB_STRING_UTIL_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <time.h>
#include <vector>

namespace obsys {

class ObStringUtil {
public:
    static int str_to_int(const char *str, int d);
    static bool is_int(const char *p);
    static char *str_to_lower(char *str);
    static char *str_to_upper(char *str);
    static void split(char *str, const char *delim, std::vector<char *> &list);
};

}

#endif
