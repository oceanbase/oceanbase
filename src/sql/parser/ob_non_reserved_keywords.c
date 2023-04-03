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

#include "ob_non_reserved_keywords.h"
#include <stdio.h>
#include <ctype.h>
#include <strings.h>
#include <string.h>
#include <stdlib.h>
#include "lib/alloc/alloc_assist.h"
#include "sql/parser/sql_parser_mysql_mode_tab.h"
#include "sql/parser/parse_define.h"

int32_t get_next_id(char c)
{
  int32_t ch_id = -1;
  if (c >= 'A' && c <= 'Z') {
    c += 'a' - 'A';
  }

  if ('_' == c) {
    ch_id = 36;
  } else if (c >= 'a' && c <= 'z') {
    ch_id = c - 'a';
  } else if (c >= '0' && c <= '9') {
    ch_id = c - '0' + 26;
  }
  return ch_id;
}

int casesame_cstr(const char *a, const char *b)
{
  size_t len1 = strlen(a);
  size_t len2 = strlen(b);
  return (len1 == len2) && (strncasecmp(a, b, len1) == 0);
}

//return 0 if succ, return 1 if fail
int add_word(t_node *root, const char *str, const int32_t idx)
{
  int ret = 0;
  t_node *pt = root;
  if (OB_UNLIKELY(NULL == root)) {
    ret = 1;
    printf("ERROR root is NULL! \n");
  } else if (OB_UNLIKELY(NULL == str)) {
    ret = 1;
    printf("ERROR word str is NULL! \n");
  } else if (OB_UNLIKELY(idx < 0)) {
    printf("ERROR invalid idx:%d\n", idx);
  } else {
    for ( ; '\0' != *str && 0 == ret; ++str) {
      int32_t ch_id = get_next_id(*str);
      if (ch_id >= 0 && NULL == pt->next[ch_id]) {
        t_node *new_node = (t_node *)calloc(1, sizeof(t_node));
        if (OB_UNLIKELY(NULL == new_node)) {
          ret = OB_PARSER_ERR_NO_MEMORY;
          printf("ERROR malloc memory failed! \n");
        } else {
          new_node->idx = -1;
          pt->next[ch_id] = new_node;
        }
      }
      if (OB_LIKELY(0 == ret)) {
        if (OB_LIKELY(ch_id >= 0)) {
          pt = pt->next[ch_id];
        } else {
          printf("ERROR ob_non_reserved_keywords.c: wrong index! \n");
          ret = 1;
        }
      }
    }
  }
  if (0 == ret) {
    pt->idx = idx;
  }
  return ret;
}

const NonReservedKeyword *find_word(const char *word, const t_node *root, const NonReservedKeyword *words)
{
  const NonReservedKeyword *res_word = NULL;
  const t_node *pt = root;
  if (OB_UNLIKELY(NULL == word)) {
    //do nothing
  } else {
    for (; *word != '\0' && NULL != pt; ++word) {
      char c = *word;
      int32_t ch_id = get_next_id(c);
      if (ch_id < 0) {
        pt = NULL;
      } else {
        pt = pt->next[ch_id];
      }
    }
  }
  if (OB_LIKELY(NULL != pt && -1 != pt->idx)) {
    res_word = &words[pt->idx];
  }
  return res_word;
}

//return 0 if succ, return 1 if fail
int create_trie_tree(const NonReservedKeyword *words, int32_t count, t_node **root)
{
  int ret = 0;
  if (OB_UNLIKELY(NULL == root)) {
    (void)printf("ERROR invalid root! \n");
    ret = 1;
  } else {
    *root = (t_node *)calloc(1, sizeof(t_node));
    if (OB_UNLIKELY(NULL == *root)) {
      (void)printf("ERROR malloc memory failed! \n");
      ret = 1;
    } else {
      (*root)->idx = -1;
      int32_t i = 0;
      for (; 0 == ret && i < count; ++i) {
        ret = add_word(*root, words[i].keyword_name, i);
      }
    }
  }
  return ret;
}
