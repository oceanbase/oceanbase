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

#include "lib/file/ob_string_util.h"

namespace obsys {
    
    bool ObStringUtil::is_int(const char *p) {
        if (NULL == p || (*p) == '\0') {
            return false;
        }
        if ((*p) == '-') p++;
        while((*p)) {
            if ((*p) < '0' || (*p) > '9') return false;
            p++;
        }
        return true;
    }

    int ObStringUtil::str_to_int(const char *str, int d)
    {
        if (is_int(str)) {
            return atoi(str);
        } else {
            return d;
        }
    }

    char *ObStringUtil::str_to_lower(char *pszBuf)
    {
        if (NULL == pszBuf) {
            return pszBuf;
        }

        char *p = pszBuf;
        while (*p) {
            if (((*p) & 0x80) != 0) {
                p++;
            } else if ((*p) >= 'A' && (*p) <= 'Z') {
              (*p) = static_cast<char>((*p)+32);
            }
            p++;
        }
        return pszBuf;
    }

    char *ObStringUtil::str_to_upper(char *pszBuf)
    {
        if (NULL == pszBuf) {
            return pszBuf;
        }

        char *p = pszBuf;
        while (*p) {
            if (((*p) & 0x80) != 0) {
                p++;
            } else if ((*p) >= 'a' && (*p) <= 'z') {
              (*p) = static_cast<char>((*p) - 32);
            }
            p++;
        }
        return pszBuf;
    }

    void ObStringUtil::split(char *str, const char *delim, std::vector<char*> &list)
    {
        if (NULL == str) {
            return;
        }

        if (NULL == delim) {
            list.push_back(str);
            return;
        }

        char *s;
        const char *spanp;

        s = str;
        while(*s)
        {
            spanp = delim;
            while(*spanp) {
                if (*s == *spanp) {
                    list.push_back(str);
                    *s = '\0';
                    str = s+1;
                    break;
                }
                spanp ++;
            }
            s ++;
        }
        if (0 != *str) {
            list.push_back(str);
        }
    }
}
