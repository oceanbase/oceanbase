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

#ifndef OCEANBASE_SQL_PARSER_PARSE_MALLOC_
#define OCEANBASE_SQL_PARSER_PARSE_MALLOC_


#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

// NB: Be careful!!!, it is only used in parser module
// NOTE, parse_malloc will memset the allocated memory to 0
extern void *parse_malloc(const size_t nbyte, void *malloc_pool);
extern void *parse_realloc(void *ptr, size_t nbyte, void *malloc_pool);
extern void parse_free(void *ptr);
extern char *parse_strndup(const char *str, size_t nbyte, void *malloc_pool);
extern char *parse_strdup(const char *str, void *malloc_pool, int64_t *out_len);
extern char *parse_str_convert_utf8(const struct ObCharsetInfo* src_cs, const char *str, void *malloc_pool, int64_t *out_len, int *extra_errno);
extern char *replace_invalid_character(const struct ObCharsetInfo* src_cs,
                                       const struct ObCharsetInfo* oracle_db_cs,
                                       const char *str, int64_t *out_len, void *malloc_pool, int *extra_errno);
extern char *strndup_with_prefix(const char *prefix, const char *str, size_t nbyte, void *malloc_pool);
extern char *strndup_with_prefix_and_postfix(const char *prefix, const char *postfix, const char *str, size_t nbyte, void *malloc_pool);
extern char *cp_str_value(const char *src, const size_t nbyte, void *malloc_pool);
extern char *parse_strdup_with_replace_multi_byte_char(const char *str, int *connection_collation_,
                                                       void *malloc_pool, int64_t *out_len);
extern bool check_real_escape(const struct ObCharsetInfo *cs, char *str, int64_t str_len,
                              int64_t last_escape_check_pos);
extern void *parser_alloc(void *malloc_pool, const int64_t alloc_size);

extern void *malloc_parentheses_info(const size_t nbyte, void *malloc_pool);
#ifdef __cplusplus
}
#endif

#endif //OCEANBASE_SQL_PARSER_PARSE_MALLOC_

