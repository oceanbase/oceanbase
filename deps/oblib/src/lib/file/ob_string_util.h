/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
