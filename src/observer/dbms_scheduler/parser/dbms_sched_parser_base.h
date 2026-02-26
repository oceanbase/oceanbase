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

#ifndef SRC_DBMS_SCHEDULER_CALENDAR_PARSER_BASE_H_
#define SRC_DBMS_SCHEDULER_CALENDAR_PARSER_BASE_H_

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include <time.h>
#include "pl/parser/parse_stmt_node.h"
#include "pl/parser/parse_stmt_item_type.h"
#include "sql/parser/parse_malloc.h"
#include "sql/parser/ob_non_reserved_keywords.h"
#include "sql/parser/parse_define.h"

#define check_ptr(ptr)                                                          \
do {                                                                            \
  if (NULL_PTR(ptr)) {                                                          \
    YY_UNEXPECTED_ERROR("'%s' is null", #ptr);                                  \
  }                                                                             \
} while (0)

#define malloc_new_node(node, mem_pool, type, num)                              \
do {                                                                            \
  if (OB_UNLIKELY(NULL == (node = new_node(mem_pool, type, num)))) {            \
    YY_FATAL_ERROR("No memory to malloc '%s'\n", yytext);                         \
  }                                                                             \
} while (0)

#define YYLEX_PARAM parse_ctx->scanner_ctx_.yyscan_info_
#define MAX_VARCHAR_LENGTH 4194303
#define OUT_OF_STR_LEN -2
#define DEFAULT_STR_LENGTH -1
#define BINARY_COLLATION 63     /*需要重构，改为c++ parser避免重复定义 */
#define INVALID_COLLATION 0

extern ParseNode *merge_tree(void *malloc_pool, int *fatal_error, ObItemType node_tag, ParseNode *source_tree);
extern int count_child(ParseNode *root, void *malloc_pool, int *count);
extern int merge_child(ParseNode *node,
                       void *malloc_pool,
                       ParseNode *source_tree,
                       int *index);
extern ParseNode *new_terminal_node(void *malloc_pool, ObItemType type);
extern ParseNode *new_non_terminal_node(void *malloc_pool, ObItemType node_tag, int num, ...);
#define malloc_terminal_node(node, mem_pool, type)                              \
  do {                                                                          \
    if (NULL_PTR(node = new_terminal_node(mem_pool, type))) {                   \
      YY_FATAL_ERROR("no memory to create terminal node '%s'\n", #type);          \
    }                                                                           \
  } while(0)

#define malloc_new_non_terminal_node(node, mem_pool, node_tag, num)               \
  do {                                                                            \
    if (OB_UNLIKELY(num <= 0)) {                                                  \
      YY_FATAL_ERROR("ERROR invalid num:%d\n", num);                              \
    } else if (OB_UNLIKELY(NULL == (node = new_node(mem_pool, node_tag, num)))) { \
      YY_FATAL_ERROR("No memory to create non_terminal node '%s'\n", #node_tag);  \
    }                                                                             \
  } while(0)

#define malloc_non_terminal_node(node, mem_pool, node_tag, ...)                         \
  do {                                                                                  \
    if (NULL_PTR(node = new_non_terminal_node(mem_pool, node_tag, ##__VA_ARGS__))) {    \
      YY_FATAL_ERROR("no memory to create non_terminal node '%s'\n", #node_tag);          \
    }                                                                                   \
  } while(0)

#define merge_nodes(node, malloc_pool, node_tag, source_tree)                                                   \
  do {                                                                                                          \
    node = merge_tree(malloc_pool, &(parse_ctx->global_errno_), node_tag, source_tree);                         \
    if (parse_ctx->global_errno_ != OB_PARSER_SUCCESS) {                                                        \
      YY_FATAL_ERROR("merge source tree '%s' failed\n", #source_tree);                                            \
    }                                                                                                           \
  } while (0)

#endif /* SRC_DBMS_SCHEDULER_CALENDAR_PARSER_BASE_H_ */
