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

#ifndef OCEANBASE_SRC_SQL_PARSER_SQL_PARSER_BASE_H_
#define OCEANBASE_SRC_SQL_PARSER_SQL_PARSER_BASE_H_

#include <stdint.h>
#include <assert.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include "lib/charset/ob_ctype.h"
#include "lib/ob_date_unit_type.h"
#include "share/schema/ob_priv_type.h"
#include "sql/ob_trans_character.h"
#include "parse_node.h"
#include "parse_malloc.h"
#include "ob_non_reserved_keywords.h"
#include "parse_define.h"
#include <math.h>

#define MAX_VARCHAR_LENGTH 4194303
#define INT16NUM_OVERFLOW INT16_MAX
#define OUT_OF_STR_LEN -2
#define DEFAULT_STR_LENGTH -1
#define BINARY_COLLATION 63
#define INVALID_COLLATION 0
#define INVALID_INDEX -1

#define YYLEX_PARAM result->yyscan_info_

#define JOIN_MERGE_NODES(node1, node2)                       \
  do {                                                       \
    if (T_LINK_NODE == node1->type_) {                       \
      merge_nodes(node1, result, T_TABLE_REFERENCES, node1); \
    }                                                        \
    if (T_LINK_NODE == node2->type_) {                       \
      merge_nodes(node2, result, T_TABLE_REFERENCES, node2); \
    }                                                        \
  } while (0)

extern void yyerror(void* yylloc, ParseResult* p, char* s, ...);
extern ParseNode* merge_tree(void* malloc_pool, int* fatal_error, ObItemType node_tag, ParseNode* source_tree);
extern ParseNode* new_terminal_node(void* malloc_pool, ObItemType type);
extern ParseNode* new_non_terminal_node(void* malloc_pool, ObItemType node_tag, int num, ...);
extern char* copy_expr_string(ParseResult* p, int expr_start, int expr_end);

#define ISSPACE(c) ((c) == ' ' || (c) == '\n' || (c) == '\r' || (c) == '\t' || (c) == '\f' || (c) == '\v')

#define YYABORT_NO_MEMORY                                \
  do {                                                   \
    if (OB_UNLIKELY(NULL == result)) {                   \
      (void)fprintf(stderr, "ERROR : result is NULL\n"); \
    } else if (0 == result->extra_errno_) {              \
      result->extra_errno_ = OB_PARSER_ERR_NO_MEMORY;    \
    } else { /*do nothing*/                              \
    }                                                    \
    YYABORT;                                             \
  } while (0)

#define YYABORT_UNEXPECTED                               \
  do {                                                   \
    if (OB_UNLIKELY(NULL == result)) {                   \
      (void)fprintf(stderr, "ERROR : result is NULL\n"); \
    } else if (0 == result->extra_errno_) {              \
      result->extra_errno_ = OB_PARSER_ERR_UNEXPECTED;   \
    } else { /*do nothing*/                              \
    }                                                    \
    YYABORT;                                             \
  } while (0)

#define YYABORT_PARSE_SQL_ERROR YYERROR

#define malloc_terminal_node(node, malloc_pool, type)                         \
  do {                                                                        \
    if (OB_UNLIKELY(NULL == (node = new_terminal_node(malloc_pool, type)))) { \
      yyerror(NULL, result, "No more space for malloc\n");                    \
      YYABORT_NO_MEMORY;                                                      \
    }                                                                         \
  } while (0)

#define malloc_non_terminal_node(node, malloc_pool, node_tag, ...)                                   \
  do {                                                                                               \
    if (OB_UNLIKELY(NULL == (node = new_non_terminal_node(malloc_pool, node_tag, ##__VA_ARGS__)))) { \
      yyerror(NULL, result, "No more space for malloc\n");                                           \
      YYABORT_NO_MEMORY;                                                                             \
    }                                                                                                \
  } while (0)

#define merge_nodes(node, result, node_tag, source_tree)                                                           \
  do {                                                                                                             \
    if (OB_UNLIKELY(NULL == source_tree)) {                                                                        \
      node = NULL;                                                                                                 \
    } else if (OB_UNLIKELY(NULL == (node = merge_tree(                                                             \
                                        result->malloc_pool_, &(result->extra_errno_), node_tag, source_tree)))) { \
      yyerror(NULL, result, "No more space for merging nodes\n");                                                  \
      YYABORT_NO_MEMORY;                                                                                           \
    }                                                                                                              \
  } while (0)

#define dup_expr_string(node, result, expr_start, expr_end)                                                            \
  do {                                                                                                                 \
    if (OB_UNLIKELY(NULL == node || NULL == result || NULL == result->input_sql_)) {                                   \
      yyerror(NULL, result, "invalid argument, node:%p, result:%p or input_sql is NULL\n", node, result);              \
      YYABORT_UNEXPECTED;                                                                                              \
    } else if (OB_UNLIKELY(expr_start < 0 || expr_end < 0 || expr_start > expr_end)) {                                 \
      yyerror(NULL, result, "invalid argument, expr_start:%d, expr_end:%d\n", (int32_t)expr_start, (int32_t)expr_end); \
      YYABORT_UNEXPECTED;                                                                                              \
    } else {                                                                                                           \
      int start = expr_start;                                                                                          \
      node->str_value_ = NULL;                                                                                         \
      node->str_len_ = 0;                                                                                              \
      while (start <= expr_end && ISSPACE(result->input_sql_[start - 1])) {                                            \
        start++;                                                                                                       \
      }                                                                                                                \
      if (start >= expr_start &&                                                                                       \
          (OB_UNLIKELY((NULL == (node->str_value_ = copy_expr_string(result, start, expr_end)))))) {                   \
        yyerror(NULL, result, "No more space for copying expression string\n");                                        \
        YYABORT_NO_MEMORY;                                                                                             \
      } else {                                                                                                         \
        node->str_len_ = expr_end - start + 1;                                                                         \
      }                                                                                                                \
    }                                                                                                                  \
  } while (0)

#define dup_string(node, result, start, end)                                                               \
  if (start > end || (OB_UNLIKELY((NULL == (node->str_value_ = copy_expr_string(result, start, end)))))) { \
    yyerror(NULL, result, "No more space for copying expression string\n");                                \
    YYABORT_NO_MEMORY;                                                                                     \
  } else {                                                                                                 \
    node->str_len_ = end - start + 1;                                                                      \
  }

#define dup_modify_key_string(node, result, expr_start, expr_end, prefix)                                              \
  do {                                                                                                                 \
    if (OB_UNLIKELY(NULL == node || NULL == result || NULL == result->input_sql_)) {                                   \
      yyerror(NULL, result, "invalid argument, node:%p, result:%p or input_sql is NULL\n", node, result);              \
      YYABORT_UNEXPECTED;                                                                                              \
    } else if (OB_UNLIKELY(expr_start < 0 || expr_end < 0 || expr_start > expr_end)) {                                 \
      yyerror(NULL, result, "invalid argument, expr_start:%d, expr_end:%d\n", (int32_t)expr_start, (int32_t)expr_end); \
      YYABORT_UNEXPECTED;                                                                                              \
    } else {                                                                                                           \
      int start = expr_start;                                                                                          \
      node->str_value_ = NULL;                                                                                         \
      node->str_len_ = 0;                                                                                              \
      while (start <= expr_end && ISSPACE(result->input_sql_[start - 1])) {                                            \
        start++;                                                                                                       \
      }                                                                                                                \
      int len = expr_end - start + sizeof(prefix);                                                                     \
      char* expr_string = (char*)parse_malloc(len + 1, result->malloc_pool_);                                          \
      if (OB_UNLIKELY(NULL == expr_string)) {                                                                          \
        yyerror(NULL, result, "No more space for copying expression string\n");                                        \
        YYABORT_NO_MEMORY;                                                                                             \
      } else {                                                                                                         \
        memmove(expr_string, (prefix), strlen(prefix));                                                                \
        memmove(expr_string + strlen(prefix), result->input_sql_ + start - 1, expr_end - start + 1);                   \
        expr_string[len] = '\0';                                                                                       \
        node->str_value_ = expr_string;                                                                                \
        node->str_len_ = len;                                                                                          \
      }                                                                                                                \
    }                                                                                                                  \
  } while (0)

#define get_non_reserved_node(node, malloc_pool, word_start, word_end)     \
  do {                                                                     \
    malloc_terminal_node(node, malloc_pool, T_IDENT);                      \
    dup_expr_string(node, result, word_start, word_end);                   \
    setup_token_pos_info(node, word_start - 1, word_end - word_start + 1); \
  } while (0)

#define make_name_node(node, malloc_pool, name)                            \
  do {                                                                     \
    malloc_terminal_node(node, malloc_pool, T_IDENT);                      \
    node->str_value_ = parse_strdup(name, malloc_pool, &(node->str_len_)); \
    if (OB_UNLIKELY(NULL == node->str_value_)) {                           \
      yyerror(NULL, result, "No more space for string duplicate\n");       \
      YYABORT_NO_MEMORY;                                                   \
    }                                                                      \
  } while (0)

#define dup_string_to_node(node, malloc_pool, name)                          \
  do {                                                                       \
    if (OB_UNLIKELY(NULL == node)) {                                         \
      yyerror(NULL, result, "invalid arguments node: %p\n", node);           \
      YYABORT_UNEXPECTED;                                                    \
    } else {                                                                 \
      node->str_value_ = parse_strdup(name, malloc_pool, &(node->str_len_)); \
      if (OB_UNLIKELY(NULL == node->str_value_)) {                           \
        yyerror(NULL, result, "No more space for string duplicate\n");       \
        YYABORT_NO_MEMORY;                                                   \
      }                                                                      \
    }                                                                        \
  } while (0)

#define dup_node_string(node_src, node_dest, malloc_pool)                                            \
  do {                                                                                               \
    if (OB_UNLIKELY((NULL == node_src || NULL == node_dest || NULL == node_src->str_value_))) {      \
      yyerror(NULL, result, "invalid arguments node_src: %p, node_dest: %p\n", node_src, node_dest); \
      YYABORT_UNEXPECTED;                                                                            \
    } else {                                                                                         \
      node_dest->str_value_ = parse_strndup(node_src->str_value_, node_src->str_len_, malloc_pool);  \
      if (OB_UNLIKELY(NULL == node_dest->str_value_)) {                                              \
        yyerror(NULL, result, "No more space for dup_node_string\n");                                \
        YYABORT_NO_MEMORY;                                                                           \
      } else {                                                                                       \
        node_dest->str_len_ = node_src->str_len_;                                                    \
      }                                                                                              \
    }                                                                                                \
  } while (0)

/* to minimize modification, we use stmt->value_ as question mark size */
#define question_mark_issue(node, result)                     \
  do {                                                        \
    if (OB_UNLIKELY(NULL == node || NULL == result)) {        \
      yyerror(NULL, result, "node or result is NULL\n");      \
      YYABORT_UNEXPECTED;                                     \
    } else if (OB_UNLIKELY(INT64_MAX != node->value_)) {      \
      yyerror(NULL, result, "node value is not INT64_MAX\n"); \
      YYABORT_UNEXPECTED;                                     \
    } else {                                                  \
      node->value_ = result->question_mark_ctx_.count_;       \
    }                                                         \
  } while (0)

#define check_question_mark(node, result)                                                                     \
  do {                                                                                                        \
    if (OB_UNLIKELY(NULL == node || NULL == result)) {                                                        \
      yyerror(NULL, result, "node or result is NULL\n");                                                      \
      YYABORT_UNEXPECTED;                                                                                     \
    } else if (OB_UNLIKELY(!result->pl_parse_info_.is_pl_parse_ && 0 != result->question_mark_ctx_.count_)) { \
      yyerror(NULL, result, "Unknown column '?'\n");                                                          \
      YYABORT_UNEXPECTED;                                                                                     \
    } else {                                                                                                  \
      node->value_ = result->question_mark_ctx_.count_;                                                       \
    }                                                                                                         \
  } while (0)

#define store_pl_symbol(node, head, tail)                                                 \
  do {                                                                                    \
    ParamList* param = (ParamList*)parse_malloc(sizeof(ParamList), result->malloc_pool_); \
    if (OB_UNLIKELY(NULL == param)) {                                                     \
      yyerror(NULL, result, "No more space for alloc ParamList\n");                       \
      YYABORT_NO_MEMORY;                                                                  \
    } else {                                                                              \
      param->node_ = node;                                                                \
      param->next_ = NULL;                                                                \
      if (NULL == head) {                                                                 \
        head = param;                                                                     \
      } else {                                                                            \
        tail->next_ = param;                                                              \
      }                                                                                   \
      tail = param;                                                                       \
    }                                                                                     \
  } while (0)

#define copy_and_replace_questionmark(result, start, end, idx)                                                       \
  do {                                                                                                               \
    if (NULL == result) {                                                                                            \
      YY_FATAL_ERROR("invalid var node\n");                                                                          \
    } else if ((result->pl_parse_info_.is_pl_parse_ && NULL == result->pl_parse_info_.pl_ns_) ||                     \
               result->is_dynamic_sql_) {                                                                            \
      if (result->no_param_sql_len_ + (start - result->pl_parse_info_.last_pl_symbol_pos_ - 1) + (int)(log10(idx)) + \
              3 >                                                                                                    \
          result->no_param_sql_buf_len_) {                                                                           \
        char* buf = parse_malloc(result->no_param_sql_buf_len_ * 2, result->malloc_pool_);                           \
        if (OB_UNLIKELY(NULL == buf)) {                                                                              \
          YY_FATAL_ERROR("no memory to alloc\n");                                                                    \
        } else {                                                                                                     \
          memmove(buf, result->no_param_sql_, result->no_param_sql_len_);                                            \
          result->no_param_sql_ = buf;                                                                               \
          result->no_param_sql_buf_len_ = result->no_param_sql_buf_len_ * 2;                                         \
        }                                                                                                            \
      }                                                                                                              \
      memmove(result->no_param_sql_ + result->no_param_sql_len_,                                                     \
          result->input_sql_ + result->pl_parse_info_.last_pl_symbol_pos_,                                           \
          start - result->pl_parse_info_.last_pl_symbol_pos_ - 1);                                                   \
      result->no_param_sql_len_ += start - result->pl_parse_info_.last_pl_symbol_pos_ - 1;                           \
      result->pl_parse_info_.last_pl_symbol_pos_ = end;                                                              \
      result->no_param_sql_[result->no_param_sql_len_++] = ':';                                                      \
      result->no_param_sql_len_ += sprintf(result->no_param_sql_ + result->no_param_sql_len_, "%ld", idx);           \
    }                                                                                                                \
  } while (0)

#define copy_and_skip_symbol(result, start, end)                                           \
  do {                                                                                     \
    if (NULL == result) {                                                                  \
      yyerror(NULL, result, "invalid var node\n");                                         \
      YYABORT_UNEXPECTED;                                                                  \
    } else if (NULL == result->pl_parse_info_.pl_ns_) {                                    \
    } else {                                                                               \
      memmove(result->no_param_sql_ + result->no_param_sql_len_,                           \
          result->input_sql_ + result->pl_parse_info_.last_pl_symbol_pos_,                 \
          start - result->pl_parse_info_.last_pl_symbol_pos_ - 1);                         \
      result->no_param_sql_len_ += start - result->pl_parse_info_.last_pl_symbol_pos_ - 1; \
      result->pl_parse_info_.last_pl_symbol_pos_ = end;                                    \
    }                                                                                      \
  } while (0)

#define store_pl_ref_object_symbol(node, result, type)                                          \
  do {                                                                                          \
    if (OB_UNLIKELY((NULL == node || NULL == result))) {                                        \
      yyerror(NULL, result, "invalid var node: %p\n", node);                                    \
      YYABORT_UNEXPECTED;                                                                       \
    } else if (NULL == result->pl_parse_info_.pl_ns_) {                                         \
    } else {                                                                                    \
      RefObjList* object = (RefObjList*)parse_malloc(sizeof(RefObjList), result->malloc_pool_); \
      if (OB_UNLIKELY(NULL == object)) {                                                        \
        yyerror(NULL, result, "No more space for alloc ParamList\n");                           \
        YYABORT_NO_MEMORY;                                                                      \
      } else {                                                                                  \
        object->type_ = type;                                                                   \
        object->node_ = node;                                                                   \
        object->next_ = NULL;                                                                   \
        if (NULL == result->pl_parse_info_.ref_object_nodes_) {                                 \
          result->pl_parse_info_.ref_object_nodes_ = object;                                    \
        } else {                                                                                \
          result->pl_parse_info_.tail_ref_object_node_->next_ = object;                         \
        }                                                                                       \
        result->pl_parse_info_.tail_ref_object_node_ = object;                                  \
      }                                                                                         \
    }                                                                                           \
  } while (0)

#define process_placeholder_for_dynamic                                                          \
  do {                                                                                           \
    if (p->pl_parse_info_.is_pl_parse_) {                                                        \
      yylval->node->value_ = get_question_mark(&p->question_mark_ctx_, p->malloc_pool_, yytext); \
    } else {                                                                                     \
      yylval->node->value_ = p->question_mark_ctx_.count_++;                                     \
    }                                                                                            \
  } while (0)

//////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

#define YY_USER_ACTION                                       \
  do {                                                       \
    check_value(yylloc);                                     \
    ParseResult* p = (ParseResult*)yyextra;                  \
    yylloc->first_line = yylloc->last_line = yylineno;       \
    yylloc->first_column = p->yycolumn_;                     \
    yylloc->last_column = yylloc->first_column + yyleng - 1; \
    p->yycolumn_ += yyleng;                                  \
  } while (0);

extern ParseNode* new_node(void* malloc_pool, ObItemType type, int num);

#define malloc_new_node(node, malloc_pool, type, num)                         \
  do {                                                                        \
    if (OB_UNLIKELY(NULL == (node = new_node(malloc_pool, type, num)))) {     \
      ((ParseResult*)yyextra)->extra_errno_ = OB_PARSER_ERR_NO_MEMORY;        \
      yyerror(yylloc, yyextra, "No more space for mallocing '%s'\n", yytext); \
      return ERROR;                                                           \
    }                                                                         \
  } while (0);

#define malloc_time_node(malloc_pool, type, find_char) \
  do {                                                 \
    ParseNode* node = NULL;                            \
    malloc_new_node(node, malloc_pool, type, 0);       \
    char* begin = strchr(yytext, find_char);           \
    check_value(begin);                                \
    char* end = strchr(begin + 1, find_char);          \
    check_value(end);                                  \
    char* dest = NULL;                                 \
    size_t len = end - begin - 1;                      \
    dest = parse_strndup(begin + 1, len, malloc_pool); \
    check_value(dest);                                 \
    node->str_value_ = dest;                           \
    node->str_len_ = len;                              \
    check_value(yylval);                               \
    yylval->node = node;                               \
  } while (0);

#define malloc_time_node_s(malloc_pool, type) malloc_time_node(malloc_pool, type, '\'');
#define malloc_time_node_d(malloc_pool, type) malloc_time_node(malloc_pool, type, '\"');

#define check_value(val_ptr)                                            \
  do {                                                                  \
    if (OB_UNLIKELY(NULL == val_ptr)) {                                 \
      ((ParseResult*)yyextra)->extra_errno_ = OB_PARSER_ERR_UNEXPECTED; \
      yyerror(yylloc, yyextra, "'%s' is NULL\n", #val_ptr);             \
      return ERROR;                                                     \
    }                                                                   \
  } while (0);

#define check_malloc(val_ptr, nbyte)                                                    \
  do {                                                                                  \
    if (OB_UNLIKELY(NULL == val_ptr)) {                                                 \
      ((ParseResult*)yyextra)->extra_errno_ = OB_PARSER_ERR_NO_MEMORY;                  \
      yyerror(yylloc, yyextra, "No more space for malloc(size: %ld)\n", (size_t)nbyte); \
      return ERROR;                                                                     \
    }                                                                                   \
  } while (0);

#define check_identifier_convert_result(errno)                    \
  do {                                                            \
    if (OB_PARSER_ERR_ILLEGAL_NAME == errno) {                    \
      yyerror(yylloc, yyextra, "name '%s' is illegal\n", yytext); \
      return ERROR;                                               \
    }                                                             \
  } while (0);

#define IS_FAST_PARAMETERIZE ((ParseResult*)yyextra)->is_fp_
#define IS_NEED_PARAMETERIZE ((ParseResult*)yyextra)->need_parameterize_
#define IS_FOR_TRIGGER ((ParseResult*)yyextra)->is_for_trigger_

#define COPY_STRING(src, src_len, dst)                    \
  do {                                                    \
    if (IS_FAST_PARAMETERIZE && IS_NEED_PARAMETERIZE) {   \
      dst = src;                                          \
    } else {                                              \
      ParseResult* p = (ParseResult*)yyextra;             \
      dst = parse_strndup(src, src_len, p->malloc_pool_); \
      check_value(dst);                                   \
    }                                                     \
  } while (0);

#define COPY_STR_NODE_TO_TMP_LITERAL(str_node)                                                  \
  do {                                                                                          \
    ParseResult* p = (ParseResult*)yyextra;                                                     \
    char** tmp_literal = &(p->tmp_literal_);                                                    \
    if (NULL == *tmp_literal) {                                                                 \
      *tmp_literal = (char*)parse_malloc(p->input_sql_len_ + 1, p->malloc_pool_);               \
      check_value(*tmp_literal);                                                                \
    }                                                                                           \
    if (str_node->str_value_ != NULL) {                                                         \
      memmove(((ParseResult*)yyextra)->tmp_literal_, str_node->str_value_, str_node->str_len_); \
      str_node->str_value_ = NULL;                                                              \
    }                                                                                           \
  } while (0);

#define STORE_STR_CONTENT(str_node)                                                 \
  do {                                                                              \
    ParseResult* p = (ParseResult*)yyextra;                                         \
    if (str_node->str_len_ <= 0 && IS_FAST_PARAMETERIZE && IS_NEED_PARAMETERIZE) {  \
      str_node->str_value_ = p->input_sql_ + yylloc->first_column - 1;              \
      str_node->str_len_ = yyleng;                                                  \
    } else {                                                                        \
      char** tmp_literal = &(p->tmp_literal_);                                      \
      if (NULL == *tmp_literal) {                                                   \
        *tmp_literal = (char*)parse_malloc(p->input_sql_len_ + 1, p->malloc_pool_); \
        check_value(*tmp_literal);                                                  \
      }                                                                             \
      memmove(*tmp_literal + str_node->str_len_, yytext, yyleng);                   \
      str_node->str_len_ += yyleng;                                                 \
    }                                                                               \
  } while (0);

#define FORMAT_STR_NODE(str_node)                                                                 \
  do {                                                                                            \
    ParseResult* p = (ParseResult*)yyextra;                                                       \
    if (str_node->str_value_ == NULL && str_node->str_len_ > 0) {                                 \
      char* tmp_literal = p->tmp_literal_;                                                        \
      tmp_literal[yylval->node->str_len_] = '\0';                                                 \
      str_node->str_value_ = parse_strndup(tmp_literal, str_node->str_len_ + 1, p->malloc_pool_); \
      check_value(str_node->str_value_);                                                          \
    }                                                                                             \
  } while (0);

#define COPY_WRITE()                                                  \
  do {                                                                \
    ParseResult* p = (ParseResult*)yyextra;                           \
    memmove(p->no_param_sql_ + p->no_param_sql_len_, yytext, yyleng); \
    p->no_param_sql_len_ += yyleng;                                   \
    p->no_param_sql_[p->no_param_sql_len_] = '\0';                    \
  } while (0);

#define REPUT_NEG_SIGN(parse_result)                                                         \
  do {                                                                                       \
    if (parse_result->minus_ctx_.has_minus_) {                                               \
      int start_pos = parse_result->no_param_sql_len_;                                       \
      int end_pos = parse_result->minus_ctx_.pos_;                                           \
      for (; start_pos > end_pos; start_pos--) {                                             \
        parse_result->no_param_sql_[start_pos] = parse_result->no_param_sql_[start_pos - 1]; \
      }                                                                                      \
      parse_result->no_param_sql_[end_pos] = '-';                                            \
      parse_result->no_param_sql_[++parse_result->no_param_sql_len_] = '\0';                 \
      parse_result->minus_ctx_.pos_ = -1;                                                    \
      parse_result->minus_ctx_.raw_sql_offset_ = -1;                                         \
      parse_result->minus_ctx_.has_minus_ = false;                                           \
    }                                                                                        \
  } while (0);

#define STORE_PARAM_NODE()                                                                        \
  do {                                                                                            \
    ParseResult* p = (ParseResult*)yyextra;                                                       \
    if (p->need_parameterize_) {                                                                  \
      if (p->minus_ctx_.has_minus_ && p->minus_ctx_.is_cur_numeric_) {                            \
        p->no_param_sql_[p->minus_ctx_.pos_] = '?';                                               \
        p->no_param_sql_len_ = p->minus_ctx_.pos_ + 1;                                            \
      } else {                                                                                    \
        if (p->minus_ctx_.has_minus_) {                                                           \
          REPUT_NEG_SIGN(p);                                                                      \
        }                                                                                         \
        p->no_param_sql_[p->no_param_sql_len_++] = '?';                                           \
      }                                                                                           \
      p->no_param_sql_[p->no_param_sql_len_] = '\0';                                              \
      size_t alloc_len = sizeof(ParamList);                                                       \
      ParamList* param = (ParamList*)parse_malloc(alloc_len, p->malloc_pool_);                    \
      check_malloc(param, alloc_len);                                                             \
      check_value(yylval);                                                                        \
      check_value(yylval->node);                                                                  \
      yylval->node->pos_ = p->no_param_sql_len_ - 1;                                              \
      yylval->node->raw_sql_offset_ = (p->minus_ctx_.has_minus_ && p->minus_ctx_.is_cur_numeric_) \
                                          ? p->minus_ctx_.raw_sql_offset_                         \
                                          : yylloc->first_column - 1;                             \
      param->node_ = yylval->node;                                                                \
      param->next_ = NULL;                                                                        \
      if (NULL == p->param_nodes_) {                                                              \
        p->param_nodes_ = param;                                                                  \
      } else {                                                                                    \
        p->tail_param_node_->next_ = param;                                                       \
      }                                                                                           \
      p->tail_param_node_ = param;                                                                \
      p->param_node_num_++;                                                                       \
      p->token_num_++;                                                                            \
      p->minus_ctx_.has_minus_ = false;                                                           \
      p->minus_ctx_.pos_ = -1;                                                                    \
      p->minus_ctx_.is_cur_numeric_ = false;                                                      \
      p->minus_ctx_.raw_sql_offset_ = -1;                                                         \
    } else {                                                                                      \
      return OUTLINE_DEFAULT_TOKEN;                                                               \
    }                                                                                             \
  } while (0);

#define STORE_UNIT_TYPE_NODE(str_val)                                                                            \
  do {                                                                                                           \
    ParseResult* p = (ParseResult*)yyextra;                                                                      \
    if (p->need_parameterize_) {                                                                                 \
      ParseNode* node = NULL;                                                                                    \
      malloc_new_node(node, ((ParseResult*)yyextra)->malloc_pool_, T_INT, 0);                                    \
      check_value(yylval);                                                                                       \
      yylval->node = node;                                                                                       \
      yylval->node->raw_text_ =                                                                                  \
          parse_strdup(yytext, ((ParseResult*)yyextra)->malloc_pool_, &(yylval->node->text_len_));               \
      check_value(yylval->node->raw_text_);                                                                      \
      node->str_value_ = parse_strdup((char*)str_val, ((ParseResult*)yyextra)->malloc_pool_, &(node->str_len_)); \
      check_value(node->str_value_);                                                                             \
      node->value_ = strtoll(node->str_value_, NULL, 10);                                                        \
      STORE_PARAM_NODE()                                                                                         \
    } else {                                                                                                     \
      return OUTLINE_DEFAULT_TOKEN;                                                                              \
    }                                                                                                            \
  } while (0);

#define HANDLE_UNIT_TYPE(type)                                             \
  do {                                                                     \
    if (IS_FAST_PARAMETERIZE) {                                            \
      ParseResult* p = (ParseResult*)yyextra;                              \
      if (p->need_parameterize_) {                                         \
        STORE_UNIT_TYPE_NODE(ob_date_unit_type_num_str(DATE_UNIT_##type)); \
      } else {                                                             \
        return OUTLINE_DEFAULT_TOKEN;                                      \
      }                                                                    \
    } else {                                                               \
      return type;                                                         \
    }                                                                      \
  } while (0);

#define HANDLE_ESCAPE(presult)                                   \
  do {                                                           \
    int with_back_slash = 1;                                     \
    unsigned char c = escaped_char(yytext[1], &with_back_slash); \
    if (with_back_slash) {                                       \
      presult->tmp_literal_[yylval->node->str_len_++] = '\\';    \
    }                                                            \
    presult->tmp_literal_[yylval->node->str_len_++] = c;         \
  } while (0);

#define HANDLE_FALSE_ESCAPE(presult)                        \
  do {                                                      \
    presult->tmp_literal_[yylval->node->str_len_++] = '\\'; \
    yyless(1);                                              \
    presult->yycolumn_--;                                   \
  } while (0);

#define CHECK_REAL_ESCAPE(is_real_escape)                                                                           \
  do {                                                                                                              \
    if (NULL != p->charset_info_ && p->charset_info_->escape_with_backslash_is_dangerous) {                         \
      char* cur_pos = p->tmp_literal_ + yylval->node->str_len_;                                                     \
      char* last_check_pos = p->tmp_literal_ + p->last_well_formed_len_;                                            \
      int error = 0;                                                                                                \
      int expected_well_formed_len = cur_pos - last_check_pos;                                                      \
      int real_well_formed_len =                                                                                    \
          p->charset_info_->cset->well_formed_len(last_check_pos, cur_pos - last_check_pos, UINT64_MAX, &error);    \
      if (error != 0) {                                                                                             \
        *cur_pos = '\\';                                                                                            \
        if (real_well_formed_len == expected_well_formed_len - 1 && p->charset_info_->cset->ismbchar(cur_pos, 2)) { \
          is_real_escape = false;                                                                                   \
          p->last_well_formed_len_ = yylval->node->str_len_ + 1;                                                    \
        } else {                                                                                                    \
          p->last_well_formed_len_ = yylval->node->str_len_;                                                        \
        }                                                                                                           \
      } else {                                                                                                      \
        p->last_well_formed_len_ = yylval->node->str_len_;                                                          \
      }                                                                                                             \
    }                                                                                                               \
  } while (0);

#define CHECK_NODE_STRING_VALUE_ASCII(token, my_str_ptr, my_str_len)                                               \
  do {                                                                                                             \
    if (p->charset_info_ != NULL && p->is_not_utf8_connection_) {                                                  \
      if (my_str_len != p->charset_info_->cset->numchars(p->charset_info_, my_str_ptr, my_str_ptr + my_str_len)) { \
        p->extra_errno_ = OB_PARSER_ERR_ILLEGAL_NAME;                                                              \
        yyerror(yylloc,                                                                                            \
            yyextra,                                                                                               \
            "multi-byte identifier '%.*s' not support in charset '%s'\n",                                          \
            my_str_len,                                                                                            \
            my_str_ptr,                                                                                            \
            p->charset_info_->csname);                                                                             \
        token = PARSER_SYNTAX_ERROR;                                                                               \
      }                                                                                                            \
    }                                                                                                              \
  } while (0)

#define ADD_YYLINENO(_yytext, _yyleng)       \
  for (int32_t _i = 0; _i < _yyleng; ++_i) { \
    if (_yytext[_i] == '\n') {               \
      ++yylineno;                            \
    }                                        \
  }

#define COPY_NUM_STRING(parse_result, node)                                              \
  do {                                                                                   \
    if (IS_FAST_PARAMETERIZE) {                                                          \
      if (parse_result->minus_ctx_.has_minus_) {                                         \
        int64_t start_pos = parse_result->minus_ctx_.raw_sql_offset_;                    \
        int64_t cp_len = yylloc->first_column - start_pos - 1 + yyleng;                  \
        COPY_STRING(p->input_sql_ + start_pos, cp_len, node->str_value_);                \
        node->str_len_ = cp_len;                                                         \
      } else {                                                                           \
        COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, node->str_value_); \
        node->str_len_ = yyleng;                                                         \
      }                                                                                  \
    } else {                                                                             \
      COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, node->str_value_);   \
      node->str_len_ = yyleng;                                                           \
    }                                                                                    \
  } while (0);

#define PARSE_INT_STR_MYSQL(param_node, malloc_pool, errno)                                               \
  do {                                                                                                    \
    if ('-' == param_node->str_value_[0]) {                                                               \
      char* copied_str = parse_strndup(param_node->str_value_, param_node->str_len_, malloc_pool);        \
      if (OB_ISNULL(copied_str)) {                                                                        \
        yyerror(NULL, yyextra, "No more space for mallocing");                                            \
        return ERROR;                                                                                     \
      } else {                                                                                            \
        int pos = 1;                                                                                      \
        for (; pos < param_node->str_len_ && ISSPACE(copied_str[pos]); pos++)                             \
          ;                                                                                               \
        copied_str[--pos] = '-';                                                                          \
        param_node->value_ = ob_strntoll(copied_str + pos, param_node->str_len_ - pos, 10, NULL, &errno); \
        if (ERANGE == errno) {                                                                            \
          param_node->type_ = T_NUMBER;                                                                   \
          token_ret = DECIMAL_VAL;                                                                        \
        }                                                                                                 \
      }                                                                                                   \
    } else {                                                                                              \
      uint64_t value = ob_strntoull(param_node->str_value_, param_node->str_len_, 10, NULL, &errno);      \
      param_node->value_ = value;                                                                         \
      if (ERANGE == errno) {                                                                              \
        param_node->type_ = T_NUMBER;                                                                     \
        token_ret = DECIMAL_VAL;                                                                          \
      } else if (value > INT64_MAX) {                                                                     \
        param_node->type_ = T_UINT64;                                                                     \
      }                                                                                                   \
    }                                                                                                     \
  } while (0);

#define RM_MULTI_STMT_END_P(p)                                \
  do {                                                        \
    int pos = p->no_param_sql_len_ - 1;                       \
    for (; pos >= 0 && ISSPACE(p->no_param_sql_[pos]); --pos) \
      ;                                                       \
    p->no_param_sql_len_ = pos + 1;                           \
    p->no_param_sql_[p->no_param_sql_len_] = '\0';            \
    p->end_col_ = p->no_param_sql_len_;                       \
  } while (0);

#define malloc_select_node(node, malloc_pool) \
  malloc_non_terminal_node(node,              \
      malloc_pool,                            \
      T_SELECT,                               \
      PARSE_SELECT_MAX_IDX,                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL,                                   \
      NULL);

// only used by setup_token_pos_info_and_dup_string for now
#define check_ret(stmt, loc, extra)                                        \
  int ret = (stmt);                                                        \
  if (OB_UNLIKELY(ret != OB_PARSER_SUCCESS)) {                             \
    yyerror((loc), (extra), "setup token pos info failed ret: %d\n", ret); \
  }

#define REPUT_TOKEN_NEG_SIGN(TOKEN)       \
  ParseResult* p = (ParseResult*)yyextra; \
  REPUT_NEG_SIGN(p);                      \
  return TOKEN;

extern void setup_token_pos_info(ParseNode* node, int off, int len);
// setup pos info and copy sql from p->input_sql_([strat, end]) to node->str_value_
extern int setup_token_pos_info_and_dup_string(ParseNode* node, ParseResult* p, int start, int end);
#ifdef SQL_PARSER_COMPILATION
int add_comment_list(ParseResult* p, const TokenPosInfo* info);
#endif

#endif /* OCEANBASE_SRC_SQL_PARSER_SQL_PARSER_BASE_H_ */
