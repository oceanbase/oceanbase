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

#ifndef OCEANBASE_SQL_PARSER_NON_RESERVED_KEYWORDS_
#define OCEANBASE_SQL_PARSER_NON_RESERVED_KEYWORDS_

#include <stdint.h>

#define LENGTH_OF(arr) (sizeof (arr) / sizeof (arr[0]))
#define CHAR_LEN 37 //sum of A-Z,0-9,_

typedef struct _NonReservedKeyword
{
  const char *keyword_name;
  int keyword_type;
} NonReservedKeyword;

typedef NonReservedKeyword ReservedKeyword;

typedef struct trie_node
{
  int32_t idx;
  struct trie_node *next[CHAR_LEN];
} t_node;

extern const NonReservedKeyword *mysql_non_reserved_keyword_lookup(const char *word);
extern const NonReservedKeyword *oracle_non_reserved_keyword_lookup(const char *word);
extern int mysql_sql_reserved_keyword_lookup(const char *word);
extern int oracle_sql_reserved_keyword_lookup(const char *word);
extern const ReservedKeyword *oracle_pl_reserved_keyword_lookup(const char *word);
extern const ReservedKeyword *oracle_reserved_keyword_lookup(const char *word);
extern int window_function_name_compare(const char *dup_value, int *window_fun_idx);
#ifdef __cplusplus
extern "C" {
#endif

extern int verify_non_reserved_keywords_order();
extern int create_trie_tree(const NonReservedKeyword *words, int32_t count, t_node **root);
extern int add_word(t_node *root, const char *str, const int32_t idx);
extern const NonReservedKeyword *find_word(const char *word, const t_node *root, const NonReservedKeyword *words);
extern int32_t get_next_id(char c);
extern int casesame_cstr(const char *a, const char *b);

#ifdef __cplusplus
}
#endif

#endif /*OCEANBASE_SQL_PARSER_NON_RESERVED_KEYWORDS_*/
