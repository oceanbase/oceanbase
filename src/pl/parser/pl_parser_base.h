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

#ifndef OCEANBASE_SRC_PL_PARSER_PL_PARSER_BASE_H_
#define OCEANBASE_SRC_PL_PARSER_PL_PARSER_BASE_H_

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

extern int64_t ob_strntoll(const char *ptr, size_t len, int base, char **end, int *err);
extern int64_t ob_strntoull(const char *ptr, size_t len, int base, char **end, int *err);

#define YY_USER_ACTION                                                         \
do {                                                                           \
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;                               \
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);                      \
  yylloc->first_line = yylloc->last_line = yylineno;                           \
  yylloc->first_column = scanner_ctx->token_len_;                              \
  yylloc->last_column = yylloc->first_column + yyleng - 1;                     \
  yylloc->abs_first_column = yycolumn;                                         \
  yylloc->abs_last_column = yylloc->abs_first_column + yyleng - 1;             \
  scanner_ctx->token_len_ += yyleng;                                           \
  yycolumn += yyleng;                                                          \
} while(0);

#define check_ptr(ptr)                                                          \
do {                                                                            \
  if (NULL_PTR(ptr)) {                                                          \
    YY_UNEXPECTED_ERROR("'%s' is null", #ptr);                                  \
  }                                                                             \
} while (0)

#define check_identifier_convert_result(errno)                                     \
do {                                                                               \
  if (OB_PARSER_ERR_ILLEGAL_NAME == errno) {                                       \
    YY_NAME_ERROR("name '%s' is illegal\n", yytext);                         \
  }                                                                                \
} while (0);

#define malloc_new_node(node, mem_pool, type, num)                              \
do {                                                                            \
  if (OB_UNLIKELY(NULL == (node = new_node(mem_pool, type, num)))) {            \
    YY_FATAL_ERROR("No memory to malloc '%s'\n", yytext);                         \
  }                                                                             \
} while (0)

#define prepare_literal_buffer(scanner_ctx, buf_len, mem_pool)                            \
do {                                                                                      \
  check_ptr(scanner_ctx);                                                                 \
  if (NULL_PTR(scanner_ctx->tmp_literal_)) {                                              \
    if (NULL_PTR(scanner_ctx->tmp_literal_ = (char*) parse_malloc(buf_len, mem_pool))) {  \
      YY_FATAL_ERROR("No memory to malloc literal buffer\n");                               \
    } else {                                                                              \
      scanner_ctx->tmp_literal_len_ = buf_len;                                            \
    }                                                                                     \
  }                                                                                       \
} while (0)

#define malloc_time_node(mem_pool, type, find_char)                                \
do {                                                                               \
  ParseNode *node = NULL;                                                          \
  malloc_new_node(node, mem_pool, type, 0);                                        \
  char *src = strchr(yytext, find_char);                                           \
  char *dest = NULL;                                                               \
  int64_t len = 0;                                                                 \
  dest = parse_strdup(src + 1, mem_pool, &len);                                    \
  check_ptr(dest);                                                                 \
  dest[len - 1] = '\0';                                                            \
  node->str_value_ = dest;                                                         \
  node->str_len_ = len - 1;                                                        \
  check_ptr(yylval);                                                               \
  yylval->node = node;                                                             \
} while (0)

#define malloc_time_node_s(mem_pool, type) malloc_time_node(mem_pool, type, '\'')
#define malloc_time_node_d(mem_pool, type) malloc_time_node(mem_pool, type, '\"')

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
extern int parse_sql_stmt(ParseResult *parse_result);

#ifdef OB_BUILD_ORACLE_PL
extern const NonReservedKeyword *oracle_pl_non_reserved_keyword_lookup(const char *word);
#endif

extern const NonReservedKeyword *mysql_pl_non_reserved_keyword_lookup(const char *word);

#define reset_current_location(__f_col, __l_col)                                  \
  do {                                                                            \
    yyloc.first_column = __f_col;                                                 \
    yyloc.last_column = __l_col;                                                  \
  } while (0)

#define copy_node_abs_location(dst_loc, src_loc)                                      \
  do {                                                                                \
    dst_loc.first_column_ = src_loc.abs_first_column;                                 \
    dst_loc.last_column_ = src_loc.abs_last_column;                                   \
    dst_loc.first_line_ = src_loc.first_line;                                         \
    dst_loc.last_line_ = src_loc.last_line;                                           \
  } while (0)

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

#define ISSPACE(c) ((c) == ' ' || (c) == '\n' || (c) == '\r' || (c) == '\t' || (c) == '\f' || (c) == '\v')

// for mysql mode
#define get_non_reserved_node(node, malloc_pool, expr_start, expr_end)  \
  do {                                                                  \
    malloc_terminal_node(node, malloc_pool, T_IDENT);                   \
    if (OB_UNLIKELY(NULL == node || NULL == parse_ctx || NULL == parse_ctx->stmt_str_)) {\
      YY_FATAL_ERROR("invalid argument, node:%p, result:%p or input_sql is NULL", node, parse_ctx);                                         \
    } else if (OB_UNLIKELY(expr_start <= 0 || expr_end <= 0 || expr_start > expr_end)) {               \
      YY_FATAL_ERROR("invalid argument, expr_start:%d, expr_end:%d", expr_start, expr_end);                     \
    } else {                                                               \
      int start = expr_start;                                              \
      char * upper_value = NULL;                                           \
      node->str_value_ = NULL;                                             \
      node->str_len_ = 0;                                                  \
      node->raw_text_ = NULL;                                             \
      node->text_len_ = 0;                                                 \
      while (start <= expr_end && ISSPACE(parse_ctx->stmt_str_[start])) { \
        start++;                                                           \
      }                                                                    \
      if (start >= expr_start                                              \
          && (OB_UNLIKELY((NULL == (upper_value = parse_strndup(parse_ctx->stmt_str_ + start, expr_end - start + 1, parse_ctx->mem_pool_)))))) { \
        YY_FATAL_ERROR("No more space for copying expression string");          \
      } else {                                                             \
        node->str_value_ = upper_value; \
        node->str_len_ = expr_end - start + 1;                             \
        node->raw_text_ = upper_value;                                      \
        node->text_len_ = expr_end - start + 1;                               \
      }                                                                    \
    }                                                                      \
  } while (0)

#ifdef OB_BUILD_ORACLE_PL
#define get_oracle_non_reserved_node(node, malloc_pool, expr_start, expr_end) \
  do {                                                                 \
    malloc_terminal_node(node, malloc_pool, T_IDENT);                   \
    if (OB_UNLIKELY(NULL == node || NULL == parse_ctx || NULL == parse_ctx->stmt_str_)) {\
      YY_FATAL_ERROR("invalid argument, node:%p, result:%p or input_sql is NULL", node, parse_ctx);                                         \
    } else if (OB_UNLIKELY(expr_start <= 0 || expr_end <= 0 || expr_start > expr_end)) {               \
      YY_FATAL_ERROR("invalid argument, expr_start:%d, expr_end:%d", expr_start, expr_end);                     \
    } else {                                                               \
      int start = expr_start;                                              \
      char * upper_value = NULL;                                           \
      char * raw_str = NULL;                                              \
      node->str_value_ = NULL;                                             \
      node->str_len_ = 0;                                                  \
      node->raw_text_ = NULL;                                             \
      node->text_len_ = 0;                                                 \
      while (start <= expr_end && ISSPACE(parse_ctx->stmt_str_[start])) { \
        start++;                                                           \
      }                                                                    \
      if (start >= expr_start                                              \
          && (OB_UNLIKELY((NULL == (upper_value = parse_strndup(parse_ctx->stmt_str_ + start, expr_end - start + 1, parse_ctx->mem_pool_)))))) { \
        YY_FATAL_ERROR("No more space for copying expression string");          \
      } else {                                                             \
        if (start >= expr_start                                              \
            && (OB_UNLIKELY((NULL == (raw_str = parse_strndup(parse_ctx->stmt_str_ + start, expr_end - start + 1, parse_ctx->mem_pool_)))))) { \
          YY_FATAL_ERROR("No more space for copying expression string");          \
        } else {                                                            \
          node->raw_text_ = raw_str;                                    \
          node->text_len_ = expr_end - start + 1;                            \
          node->str_value_ = str_toupper(upper_value, expr_end - start + 1); \
          node->str_len_ = expr_end - start + 1;                             \
        }                                                                   \
      }                                                                    \
    }                                                                      \
  } while(0)
#endif

#define make_name_node(node, malloc_pool, name)                         \
  do {                                                                  \
    malloc_terminal_node(node, malloc_pool, T_IDENT);                   \
    node->str_value_ = parse_strdup(name, malloc_pool, &(node->str_len_));\
    if (OB_UNLIKELY(NULL == node->str_value_)) {                        \
      YY_FATAL_ERROR("No more space for string duplicate");             \
    }                                                                   \
  } while (0)

// Check whether the decimal is a int with suffix zero.
// if true, store the int value in result, set the int_flag to true;
// Notice: we use the (int64_t) double == double here, this kind of comparison
// only supports up to 15 decimal place precision:
// e.g., (int64_t)3.00000000000000001 == 3.00000000000000001 is true;
// oracle supports up to 40 decimal place precision.
#ifdef OB_BUILD_ORACLE_PL
#define CHECK_ASSIGN_ZERO_SUFFIX_INT(decimal_str, result, int_flag)         \
  do                                                                        \
  {                                                                         \
    double temp_val = strtod(decimal_str, NULL);                            \
    if ((int64_t)temp_val == temp_val) {                                    \
      int_flag = true;                                                      \
    } else {                                                                \
      int_flag = false;                                                     \
    }                                                                       \
    result = (int64_t)temp_val;                                             \
  } while (0);
#endif

#define ESCAPE_PERCENT_CHARACTER(result, src, dst)\
do {\
  if (OB_NOT_NULL(result) && OB_NOT_NULL(src)) {\
    int64_t src_len = strlen(src);\
    int64_t dst_len = 2 * src_len + 1;\
    int64_t pos = 0;\
    dst = (char *)parse_malloc(dst_len, result->mem_pool_);\
    if (OB_LIKELY(NULL != dst)) {\
      for (int64_t i = 0; i < src_len; i++) {\
        if (src[i] == '%') {\
          dst[pos++] = '%';\
        }\
        dst[pos++] = src[i];\
      }\
      dst[pos] = 0;\
    }\
  }\
} while (0)

#endif /* OCEANBASE_SRC_PL_PARSER_PL_PARSER_BASE_H_ */
