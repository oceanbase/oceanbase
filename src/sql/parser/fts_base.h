/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SRC_SQL_PARSER_FTS_BASE_H_
#define OCEANBASE_SRC_SQL_PARSER_FTS_BASE_H_

#include <stdint.h>
#include <assert.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include <math.h>
#include <stdbool.h>
#include "lib/utility/ob_macro_utils.h"

struct ObCharsetInfo;
#include "parse_malloc.h"

enum FtsError {
  FTS_OK = 0,
  FTS_ERROR_MEMORY = 1,
  FTS_ERROR_SYNTAX = 2,
  FTS_ERROR_OTHER = 3,
};

enum FtsNodeType {
  FTS_NODE_OPER,               /*!< Operator */
  FTS_NODE_TERM,               /*!< Term (or word) */
  FTS_NODE_LIST,               /*!< Expression list */
  FTS_NODE_SUBEXP_LIST         /*!< Sub-Expression list */
};

enum FtsOperType {
  FTS_NONE, /*!< No operator */

  FTS_IGNORE, /*!< Ignore rows that contain
              this word */

  FTS_EXIST, /*!< Include rows that contain
             this word */
};

typedef struct FtsString{
  char *str_;
  uint32_t len_;
} FtsString;

typedef struct FtsNode FtsNode;

typedef struct {
  FtsNode *head; /*!< Children list head */
  FtsNode *tail; /*!< Children list tail */
} FtsNodeList;


struct FtsNode{
  enum FtsNodeType type;        /*!< The type of node */
  FtsString term;        /*!< Term node */
  enum FtsOperType oper;        /*!< Operator value */
  FtsNodeList list;        /*!< Expression list */
  FtsNode *next;       /*!< Link for expr list */
  FtsNode *last; /*!< Direct up node */
  bool go_up;              /*!< Flag if go one level up */
};

typedef struct {
  int     ret_;
  FtsString err_info_;
  void    *yyscanner_;
  void    *malloc_pool_; // ObIAllocator
  void    *charset_info_;
  FtsNode *root_;
  FtsNodeList list_;
} FtsParserResult;

/********************************************************************
Create an AST expr list node */
extern int fts_create_node_list(
    void *arg,
    FtsNode *expr,
    FtsNode **node);

int fts_string_create(FtsParserResult *arg, const char *str, uint32_t len, FtsString **ast_str);

extern int fts_create_node_term(void *arg, const FtsString *term, FtsNode **node);
extern int fts_create_node_oper(void *arg, enum FtsOperType oper, FtsNode **node);
extern int fts_add_node(FtsNode *node, FtsNode *elem);
extern int fts_create_node_subexp_list(void *arg, FtsNode *expr, FtsNode **node);
extern int init_fts_parser_result(FtsParserResult *result);
#endif /* OCEANBASE_SRC_SQL_PARSER_FTS_BASE_H_ */