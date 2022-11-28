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

#ifndef LIBCOS_STRING_H
#define LIBCOS_STRING_H

#include "cos_sys_define.h"


COS_CPP_START

typedef struct {
    int len;
    char *data;
} cos_string_t;

#define cos_string(str)     { sizeof(str) - 1, (char *) str }
#define cos_null_string     { 0, NULL }
#define cos_str_set(str, text)                                  \
    (str)->len = strlen(text); (str)->data = (char *) text
#define cos_str_null(str)   (str)->len = 0; (str)->data = NULL

#define cos_tolower(c)      (char) ((c >= 'A' && c <= 'Z') ? (c | 0x20) : c)
#define cos_toupper(c)      (char) ((c >= 'a' && c <= 'z') ? (c & ~0x20) : c)

static APR_INLINE void cos_string_tolower(cos_string_t *str)
{
    int i = 0;
    while (i < str->len) {
        str->data[i] = cos_tolower(str->data[i]);
        ++i;
    }
}

static APR_INLINE char *cos_strlchr(char *p, char *last, char c)
{
    while (p < last) {
        if (*p == c) {
            return p;
        }
        p++;
    }
    return NULL;
}

static APR_INLINE int cos_is_quote(char c)
{
    return c == '\"';
}

static APR_INLINE int cos_is_space(char c)
{
    return ((c == ' ') || (c == '\t'));
}

static APR_INLINE int cos_is_space_or_cntrl(char c)
{
    return c <= ' ';
}

static APR_INLINE int cos_is_null_string(cos_string_t *str)
{
    if (str == NULL || str->data == NULL || str->len == 0) {
        return COS_TRUE;
    }
    return COS_FALSE;
}

void cos_strip_space(cos_string_t *str);
void cos_trip_space_and_cntrl(cos_string_t *str);
void cos_unquote_str(cos_string_t *str);

char *cos_pstrdup(cos_pool_t *p, const cos_string_t *s);

int cos_ends_with(const cos_string_t *str, const cos_string_t *suffix);

COS_CPP_END

#endif
