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
#include <math.h>
#include "lib/ob_date_unit_type.h"
#include "share/schema/ob_priv_type.h"
#include "sql/ob_trans_character.h"
#include "parse_node.h"
#include "parse_malloc.h"
#include "ob_non_reserved_keywords.h"
#include "parse_define.h"

#define MAX_VARCHAR_LENGTH 4194303
#define INT16NUM_OVERFLOW INT16_MAX
#define OUT_OF_STR_LEN -2
#define DEFAULT_STR_LENGTH -1
#define BINARY_COLLATION 63     /* 需要重构，改为c++ parser避免重复定义 */
#define OB_STRXFRM_NLEVELS 6
#define OB_STRXFRM_DESC_SHIFT 8
#define OB_STRXFRM_REVERSE_SHIFT 16
#define OB_STRXFRM_PAD_WITH_SPACE 0x00000040

#define INVALID_COLLATION 0
#define INVALID_INDEX -1

#define YYLEX_PARAM result->yyscan_info_

#define JOIN_MERGE_NODES(node1, node2)                                                  \
do {                                                                                    \
  if (T_LINK_NODE == node1->type_) {                                                    \
    merge_nodes(node1, result, T_TABLE_REFERENCES, node1);                \
  }                                                                                     \
  if (T_LINK_NODE == node2->type_) {                                                    \
    merge_nodes(node2, result, T_TABLE_REFERENCES, node2);                \
  }                                                                                     \
} while(0)

extern void yyerror(void *yylloc, ParseResult *p, char *s,...);
extern ParseNode *merge_tree(void *malloc_pool, int *fatal_error, ObItemType node_tag, ParseNode *source_tree);
extern ParseNode *new_terminal_node(void *malloc_pool, ObItemType type);
extern ParseNode *new_non_terminal_node(void *malloc_pool, ObItemType node_tag, int num, ...);
extern char *copy_expr_string(ParseResult *p, int expr_start, int expr_end);
extern int64_t ob_strntoll(const char *ptr, size_t len, int base, char **end, int *err);
extern int64_t ob_strntoull(const char *ptr, size_t len, int base, char **end, int *err);
extern int store_prentthese_info(int left, int right, ParseResult *result);
extern bool check_real_escape(const struct ObCharsetInfo *cs, char *str, int64_t str_len,
                              int64_t last_escape_check_pos);

int add_alias_name(ParseNode *node, ParseResult *result, int end);

#define ISSPACE(c) ((c) == ' ' || (c) == '\n' || (c) == '\r' || (c) == '\t' || (c) == '\f' || (c) == '\v')

#define YYABORT_NO_MEMORY                                       \
  do {                                                          \
    if (OB_UNLIKELY(NULL == result)) {                          \
      (void)fprintf(stderr, "ERROR : result is NULL\n");        \
    } else if (0 == result->extra_errno_) {                     \
      result->extra_errno_ = OB_PARSER_ERR_NO_MEMORY;           \
    } else {/*do nothing*/}                                     \
    YYABORT;                                                    \
  } while(0)

#define YYABORT_UNEXPECTED                           \
  do {                                               \
    if (OB_UNLIKELY(NULL == result)) {                                    \
      (void)fprintf(stderr, "ERROR : result is NULL\n");        \
    } else if (0 == result->extra_errno_) {                     \
      result->extra_errno_ = OB_PARSER_ERR_UNEXPECTED;          \
    } else {/*do nothing*/}                                     \
    YYABORT;                                                    \
  } while(0)

#define YYABORT_TOO_BIG_DISPLAYWIDTH                           \
  do {                                               \
    if (OB_UNLIKELY(NULL == result)) {                                    \
      (void)fprintf(stderr, "ERROR : result is NULL\n");        \
    } else if (0 == result->extra_errno_) {                     \
      result->extra_errno_ = OB_PARSER_ERR_TOO_BIG_DISPLAYWIDTH;          \
    } else {/*do nothing*/}                                     \
    YYABORT;                                                    \
  } while(0)

#define YYABORT_STRING_LITERAL_TOO_LONG(result)                 \
  do {                                                          \
    if (OB_UNLIKELY(NULL == result)) {                          \
      (void)fprintf(stderr, "ERROR : result is NULL\n");        \
    } else if (0 == result->extra_errno_) {                     \
      result->extra_errno_ = OB_PARSER_ERR_STR_LITERAL_TOO_LONG;\
    } else {/*do nothing*/}                                     \
    yyerror(yylloc, yyextra, "string literal is too long\n", yytext); \
    return ERROR;                                               \
  } while(0)

#define YYABORT_UNDECLARE_VAR                 \
  do {                                                          \
    if (OB_UNLIKELY(NULL == result)) {                          \
      (void)fprintf(stderr, "ERROR : result is NULL\n");        \
    } else if (0 == result->extra_errno_) {                     \
      result->extra_errno_ = OB_PARSER_ERR_UNDECLARED_VAR;\
    } else {/*do nothing*/}                                     \
    YYABORT;                                                    \
  } while(0)

#define YYABORT_NOT_VALID_ROUTINE_NAME                          \
  do {                                                          \
    if (OB_UNLIKELY(NULL == result)) {                          \
      (void)fprintf(stderr, "ERROR : result is NULL\n");        \
    } else if (0 == result->extra_errno_) {                     \
      result->extra_errno_ = OB_PARSER_ERR_NOT_VALID_ROUTINE_NAME;\
    } else {/*do nothing*/}                                     \
    YYABORT;                                                    \
  } while(0)

#define YYABORT_PARSE_SQL_ERROR YYERROR

#define check_malloc(val_ptr)                                                      \
do {                                                                               \
  if (OB_UNLIKELY(NULL == val_ptr))                                                \
  {                                                                                \
    ((ParseResult *)yyextra)->extra_errno_ = OB_PARSER_ERR_NO_MEMORY;              \
    yyerror(yylloc, yyextra, "No more space for malloc\n");                        \
    return ERROR;                                                                  \
  }                                                                                \
} while (0);

#define malloc_terminal_node(node, malloc_pool, type)                       \
  do {                                                                      \
    if (OB_UNLIKELY(NULL == (node = new_terminal_node(malloc_pool, type)))) { \
      yyerror(NULL, result, "No more space for malloc\n");                    \
      YYABORT_NO_MEMORY;                                                    \
    }                                                                       \
  } while(0)

#define malloc_non_terminal_node(node, malloc_pool, node_tag, ...)      \
  do {                                                                  \
    if (OB_UNLIKELY(NULL == (node = new_non_terminal_node(malloc_pool, node_tag, ##__VA_ARGS__)))) {\
      yyerror(NULL, result, "No more space for malloc\n");                \
      YYABORT_NO_MEMORY;                                                \
    }                                                                   \
  } while(0)

#define merge_nodes(node, result, node_tag, source_tree)                \
  do {                                                                  \
    if (OB_UNLIKELY(NULL == source_tree)) {                             \
      node = NULL;                                                      \
    } else if (OB_UNLIKELY(NULL == (node = merge_tree(result->malloc_pool_, &(result->extra_errno_), node_tag, source_tree)))) { \
      yyerror(NULL, result, "No more space for merging nodes\n");         \
      YYABORT_NO_MEMORY;                                                \
    }                                                                   \
  } while (0)

#define dup_expr_string(node, result, expr_start, expr_end)                \
  do {                                                                     \
    if (OB_UNLIKELY(NULL == node || NULL == result || NULL == result->input_sql_)) {\
      yyerror(NULL, result, "invalid argument, node:%p, result:%p or input_sql is NULL\n", node, result);\
      YYABORT_UNEXPECTED;                                                  \
    } else if (OB_UNLIKELY(expr_start < 0 || expr_end < 0 || expr_start > expr_end)) {               \
      yyerror(NULL, result, "invalid argument, expr_start:%d, expr_end:%d\n", (int32_t)expr_start, (int32_t)expr_end);\
      YYABORT_UNEXPECTED;                                                  \
    } else {                                                               \
      int start = expr_start;                                              \
      node->str_value_ = NULL;                                             \
      node->str_len_ = 0;                                                  \
      while (start <= expr_end && ISSPACE(result->input_sql_[start - 1])) {\
        start++;                                                           \
      }                                                                    \
      if (start >= expr_start                                              \
          && (OB_UNLIKELY((NULL == (node->str_value_ = copy_expr_string(result, start, expr_end)))))) { \
        yyerror(NULL, result, "No more space for copying expression string\n"); \
        YYABORT_NO_MEMORY;                                                 \
      } else {                                                             \
        node->str_len_ = expr_end - start + 1;                              \
      }                                                                    \
    }                                                                      \
  } while (0)

#define dup_string(node, result, start, end)                             \
    if (start > end                                                      \
        || (OB_UNLIKELY((NULL == (node->str_value_ = copy_expr_string(result, start, end)))))) { \
      yyerror(NULL, result, "No more space for copying expression string\n"); \
      YYABORT_NO_MEMORY;                                                 \
    } else {                                                             \
      node->str_len_ = end - start + 1;                                  \
    }                                                                    \

#define dup_modify_key_string(node, result, expr_start, expr_end, prefix)      \
  do {                                                                     \
    if (OB_UNLIKELY(NULL == node || NULL == result || NULL == result->input_sql_)) {\
      yyerror(NULL, result, "invalid argument, node:%p, result:%p or input_sql is NULL\n", node, result);\
      YYABORT_UNEXPECTED;                                                  \
    } else if (OB_UNLIKELY(expr_start < 0 || expr_end < 0 || expr_start > expr_end)) {               \
      yyerror(NULL, result, "invalid argument, expr_start:%d, expr_end:%d\n", (int32_t)expr_start, (int32_t)expr_end);\
      YYABORT_UNEXPECTED;                                                  \
    } else {                                                               \
      int start = expr_start;                                              \
      node->str_value_ = NULL;                                             \
      node->str_len_ = 0;                                                  \
      while (start <= expr_end && ISSPACE(result->input_sql_[start - 1])) {\
        start++;                                                           \
      }                                                                    \
      int len = expr_end - start + sizeof(prefix);                \
      char *expr_string = (char *)parse_malloc(len + 1, result->malloc_pool_); \
      if (OB_UNLIKELY(NULL == expr_string)) {                           \
        yyerror(NULL, result, "No more space for copying expression string\n"); \
        YYABORT_NO_MEMORY;                                              \
      } else {                                                          \
        memmove(expr_string, (prefix), strlen(prefix));                       \
        memmove(expr_string+strlen(prefix), result->input_sql_ + start - 1, expr_end - start + 1); \
        expr_string[len] = '\0';                                        \
        node->str_value_ = expr_string;                                 \
        node->str_len_ = len;                                           \
      }                                                                 \
    }                                                                      \
  } while (0)

#define get_non_reserved_node(node, malloc_pool, word_start, word_end)  \
  do {                                                                  \
    malloc_terminal_node(node, malloc_pool, T_IDENT);                   \
    dup_expr_string(node, result, word_start, word_end);                \
    setup_token_pos_info(node, word_start - 1, word_end - word_start + 1);  \
  } while (0)

//oracle下生成非保留关键字结点请使用该宏,区别于mysql的是做了大写的转换
#define get_oracle_non_reserved_node(node, malloc_pool, expr_start, expr_end) \
  do {                                                                 \
    malloc_terminal_node(node, malloc_pool, T_IDENT);                   \
    if (OB_UNLIKELY(NULL == node || NULL == result || NULL == result->input_sql_)) {\
      yyerror(NULL, result, "invalid argument, node:%p, result:%p or input_sql is NULL", node, result);\
      YYABORT_UNEXPECTED;                                                  \
    } else if (OB_UNLIKELY(expr_start < 0 || expr_end < 0 || expr_start > expr_end)) {               \
      yyerror(NULL, result, "invalid argument, expr_start:%d, expr_end:%d", (int32_t)expr_start, (int32_t)expr_end);\
      YYABORT_UNEXPECTED;                                                  \
    } else {                                                               \
      int start = expr_start;                                              \
      char * upper_value = NULL;                                           \
      char * raw_str = NULL;                                              \
      node->str_value_ = NULL;                                             \
      node->str_len_ = 0;                                                  \
      node->raw_text_ = NULL;                                             \
      node->text_len_ = 0;                                                 \
      while (start <= expr_end && ISSPACE(result->input_sql_[start - 1])) {\
        start++;                                                           \
      }                                                                    \
      if ('"' == result->input_sql_[start - 1]) {                          \
        start++;                                                           \
        expr_end--;                                                        \
      }                                                                    \
      if (start >= expr_start                                              \
          && (OB_UNLIKELY((NULL == (upper_value = copy_expr_string(result, start, expr_end)))))) { \
        yyerror(NULL, result, "No more space for copying expression string"); \
        YYABORT_NO_MEMORY;                                                 \
      } else {                                                             \
        if (start >= expr_start                                              \
            && (OB_UNLIKELY((NULL == (raw_str = copy_expr_string(result, start, expr_end)))))) { \
          yyerror(NULL, result, "No more space for copying expression string"); \
          YYABORT_NO_MEMORY;                                                 \
        } else {                                                            \
          node->raw_text_ = raw_str;                                    \
          node->text_len_ = expr_end - start + 1;                            \
          node->str_value_ = str_toupper(upper_value, expr_end - start + 1); \
          node->str_len_ = expr_end - start + 1;                             \
          setup_token_pos_info(node, expr_start - 1, expr_end - expr_start + 1);  \
        }                                                                   \
      }                                                                    \
    }                                                                      \
  } while(0)

#define make_name_node(node, malloc_pool, name)                         \
  do {                                                                  \
    malloc_terminal_node(node, malloc_pool, T_IDENT);                   \
    node->str_value_ = parse_strdup(name, malloc_pool, &(node->str_len_));\
    if (OB_UNLIKELY(NULL == node->str_value_)) {                        \
      yyerror(NULL, result, "No more space for string duplicate\n");      \
      YYABORT_NO_MEMORY;                                                \
    }                                                                   \
  } while (0)

#define dup_string_to_node(node, malloc_pool, name)                     \
  do {                                                                  \
    if (OB_UNLIKELY(NULL == node)) {                                              \
      yyerror(NULL, result, "invalid arguments node: %p\n", node);        \
      YYABORT_UNEXPECTED;                                               \
    } else {                                                            \
      node->str_value_ = parse_strdup(name, malloc_pool, &(node->str_len_));\
      if (OB_UNLIKELY(NULL == node->str_value_)) {                      \
        yyerror(NULL, result, "No more space for string duplicate\n");    \
        YYABORT_NO_MEMORY;                                              \
      }                                                                 \
    }                                                                   \
  } while (0)

#define dup_node_string(node_src, node_dest, malloc_pool)               \
  do {                                                                  \
     if (OB_UNLIKELY((NULL == node_src || NULL == node_dest || NULL == node_src->str_value_))) {\
       yyerror(NULL, result, "invalid arguments node_src: %p, node_dest: %p\n", node_src, node_dest); \
       YYABORT_UNEXPECTED;                                              \
     } else {                                                           \
       node_dest->str_value_ = parse_strndup(node_src->str_value_, node_src->str_len_, malloc_pool); \
       if (OB_UNLIKELY(NULL == node_dest->str_value_)) {                \
         yyerror(NULL, result, "No more space for dup_node_string\n");    \
         YYABORT_NO_MEMORY;                                             \
       } else {                                                         \
         node_dest->str_len_ = node_src->str_len_;                      \
       }                                                                \
     }                                                                  \
  } while (0)

/* to minimize modification, we use stmt->value_ as question mark size */
#define question_mark_issue(node, result)                        \
  do {                                                           \
    if (OB_UNLIKELY(NULL == node || NULL == result)) {           \
      yyerror(NULL, result, "node or result is NULL\n");           \
      YYABORT_UNEXPECTED;                                        \
    } else if (OB_UNLIKELY(INT64_MAX != node->value_)) {         \
      yyerror(NULL, result, "node value is not INT64_MAX\n");      \
      YYABORT_UNEXPECTED;                                        \
    } else {                                                     \
      node->value_ = result->question_mark_ctx_.count_;                \
      /* 为了处理ps + anonymous 取消对question_mark_size 置0*/          \
      /* 以前置为0，是052中为了处理multi stmt*/                         \
      /*      result->question_mark_size_ = 0;                   */     \
    }                                                            \
 } while (0)

#define check_question_mark(node, result)                        \
  do {                                                           \
    if (OB_UNLIKELY(NULL == node || NULL == result)) {           \
      yyerror(NULL, result, "node or result is NULL\n");           \
      YYABORT_UNEXPECTED;                                        \
    } else if (OB_UNLIKELY(!result->pl_parse_info_.is_pl_parse_ && 0 != result->question_mark_ctx_.count_)) {  \
       /* 如果是PL过来的sql语句，不要检查：*/ \
      yyerror(NULL, result, "Unknown column '?'\n");               \
      YYABORT_PARSE_SQL_ERROR;                                        \
    } else {                                                     \
      node->value_ = result->question_mark_ctx_.count_;                \
    }                                                            \
  } while (0)

//把一个PL的变量存储在链表里
#define store_pl_symbol(node, head, tail) \
    do { \
      ParamList *param = (ParamList *)parse_malloc(sizeof(ParamList), result->malloc_pool_); \
      if (OB_UNLIKELY(NULL == param)) { \
        yyerror(NULL, result, "No more space for alloc ParamList\n"); \
        YYABORT_NO_MEMORY; \
      } else { \
        param->node_ = node; \
        param->next_ = NULL; \
        if (NULL == head) { \
         head = param; \
        } else { \
          tail->next_ = param; \
        } \
        tail = param; \
      } \
    } while (0)

/*
 * copy当前sql语句到当前QUESTIONMARK的位置，并把该QUESTIONMARK替换成:idx的QUESTIONMARK形式
 * 对PL整体进行parser时，parser到其中的一条sql时会走到这里，所以要判断is_pl_parse_和NULL == pl_ns_
 * 对oracle模式的动态sql进行prepare的时候也可能走到这里
 * Oracle模式的匿名块在sql里出现的？需要在PL里整体进行编号，所以需要做这个动作，mysql不需要
 * 替换的过程中可能会Buffer空间不足,如果发生空间不足则扩展Buffer
 */
#define copy_and_replace_questionmark(result, start, end, idx) \
    do { \
      if (NULL == result) { \
        YY_UNEXPECTED_ERROR("invalid var node\n"); \
      } else if ((result->pl_parse_info_.is_pl_parse_ && NULL == result->pl_parse_info_.pl_ns_) \
                 || result->is_dynamic_sql_) { \
        if (result->no_param_sql_len_ + (start - result->pl_parse_info_.last_pl_symbol_pos_ - 1) \
            + (int)(log10(idx)) + 3 \
            > result->no_param_sql_buf_len_) { \
          char *buf = parse_malloc(result->no_param_sql_buf_len_ * 2, result->malloc_pool_); \
          if (OB_UNLIKELY(NULL == buf)) { \
            YY_FATAL_ERROR("no memory to alloc\n"); \
          } else { \
            memmove(buf, result->no_param_sql_, result->no_param_sql_len_); \
            result->no_param_sql_ = buf; \
            result->no_param_sql_buf_len_ = result->no_param_sql_buf_len_ * 2; \
          } \
        } \
        memmove(result->no_param_sql_ + result->no_param_sql_len_, \
                result->input_sql_ + result->pl_parse_info_.last_pl_symbol_pos_, \
                start - result->pl_parse_info_.last_pl_symbol_pos_ - 1); \
        result->no_param_sql_len_ += start - result->pl_parse_info_.last_pl_symbol_pos_ - 1; \
        result->pl_parse_info_.last_pl_symbol_pos_ = end; \
        result->no_param_sql_[result->no_param_sql_len_++]  = ':'; \
        result->no_param_sql_len_ += sprintf(result->no_param_sql_ + result->no_param_sql_len_, "%ld", idx); \
      } \
    } while (0)

//copy当前sql语句到当前symbol的位置，并跳过该变量
#define copy_and_skip_symbol(result, start, end) \
    do { \
      if (NULL == result) { \
        yyerror(NULL, result, "invalid var node\n"); \
        YYABORT_UNEXPECTED; \
      } else if (NULL == result->pl_parse_info_.pl_ns_) { \
      } else { \
        memmove(result->no_param_sql_ + result->no_param_sql_len_, result->input_sql_ + result->pl_parse_info_.last_pl_symbol_pos_, start - result->pl_parse_info_.last_pl_symbol_pos_ - 1); \
        result->no_param_sql_len_ += start - result->pl_parse_info_.last_pl_symbol_pos_ - 1; \
        result->pl_parse_info_.last_pl_symbol_pos_ = end; \
      } \
    } while (0)

#define check_need_malloc(result, need_len) \
  do {  \
    if (result->no_param_sql_len_ + need_len >= result->no_param_sql_buf_len_) {  \
      char *buf = parse_malloc(result->no_param_sql_buf_len_ * 2, result->malloc_pool_);  \
      if (OB_UNLIKELY(NULL == buf)) { \
        yyerror(NULL, result, "fail to malloc\n");  \
        YYABORT_NO_MEMORY;  \
      } else {  \
        memmove(buf, result->no_param_sql_, result->no_param_sql_len_); \
        result->no_param_sql_ = buf;  \
        result->no_param_sql_buf_len_ = result->no_param_sql_buf_len_ * 2;  \
      } \
    } \
  } while (0)

//查找pl变量，并把该变量替换成:int形式
#define lookup_pl_exec_symbol(node, result, start, end, is_trigger_new, is_add_alas_name, is_report_error) \
    do { \
      if (OB_UNLIKELY((NULL == node || NULL == result || NULL == node->str_value_))) { \
        yyerror(NULL, result, "invalid var node: %p\n", node); \
        YYABORT_UNEXPECTED; \
      } else if (NULL == result->pl_parse_info_.pl_ns_) { \
      } else if (is_trigger_new) { \
        copy_and_skip_symbol(result, start, end); \
        check_need_malloc(result, 2 + node->children_[1]->str_len_ + node->children_[2]->str_len_); \
        result->no_param_sql_[result->no_param_sql_len_++]  = ':'; \
        result->no_param_sql_len_ += sprintf(result->no_param_sql_ + result->no_param_sql_len_, "%.*s", (int)node->children_[1]->str_len_, node->children_[1]->str_value_); \
        result->no_param_sql_[result->no_param_sql_len_++]  = '.'; \
        result->no_param_sql_len_ += sprintf(result->no_param_sql_ + result->no_param_sql_len_, "%.*s", (int)node->children_[2]->str_len_, node->children_[2]->str_value_); \
        store_pl_symbol(node, result->param_nodes_, result->tail_param_node_); \
      } else if (is_add_alas_name) { \
        int64_t idx = INVALID_INDEX; \
        if (NULL != node->children_[0] && T_COLUMN_REF == node->children_[0]->type_ && OB_UNLIKELY(0 != lookup_pl_symbol(result->pl_parse_info_.pl_ns_, node->str_value_, node->str_len_, &idx))) { \
          yyerror(NULL, result, "failed to lookup pl symbol\n");    \
          YYABORT_UNEXPECTED; \
        } else if (NULL != node->children_[0] && T_COLUMN_REF == node->children_[0]->type_ && INVALID_INDEX == idx) { \
          /*do nothing*/ \
        } else {\
          int ret = 0; \
          if (0 == (ret = add_alias_name(node, result, end))) {  \
          } else if (OB_PARSER_ERR_NO_MEMORY == ret) {           \
            yyerror(NULL, result, "fail to malloc\n");    \
            YYABORT_NO_MEMORY; \
          } else {           \
            yyerror(NULL, result, "failed to lookup pl symbol\n");    \
            YYABORT_UNEXPECTED; \
          }   \
        } \
      } \
      else { \
        int64_t idx = INVALID_INDEX; \
        if (OB_UNLIKELY(0 != lookup_pl_symbol(result->pl_parse_info_.pl_ns_, node->str_value_, node->str_len_, &idx))) { \
          yyerror(NULL, result, "failed to lookup pl symbol\n");    \
          YYABORT_UNEXPECTED; \
        } else if (INVALID_INDEX != idx) { \
          copy_and_skip_symbol(result, start, end); \
          check_need_malloc(result, 21); \
          result->no_param_sql_[result->no_param_sql_len_++]  = ':'; \
          result->no_param_sql_len_ += sprintf(result->no_param_sql_ + result->no_param_sql_len_, "%ld", idx); \
          store_pl_symbol(node, result->param_nodes_, result->tail_param_node_); \
        } else if (is_report_error) { \
          YYABORT_UNDECLARE_VAR;   \
        } else { /*do nothing*/ } \
      } \
    } while (0)

//存储依赖对象到依赖对象链表中
#define store_pl_ref_object_symbol(node, result, type) \
    do { \
      if (OB_UNLIKELY((NULL == node || NULL == result))) { \
        yyerror(NULL, result, "invalid var node: %p\n", node); \
        YYABORT_UNEXPECTED; \
      } else if (NULL == result->pl_parse_info_.pl_ns_) { \
      } else { \
        RefObjList *object = (RefObjList *)parse_malloc(sizeof(RefObjList), result->malloc_pool_); \
        if (OB_UNLIKELY(NULL == object)) { \
          yyerror(NULL, result, "No more space for alloc ParamList\n"); \
          YYABORT_NO_MEMORY; \
        } else { \
          object->type_ = type; \
          object->node_ = node; \
          object->next_ = NULL; \
          if (NULL == result->pl_parse_info_.ref_object_nodes_) { \
            result->pl_parse_info_.ref_object_nodes_ = object; \
          } else { \
            result->pl_parse_info_.tail_ref_object_node_->next_ = object; \
          } \
          result->pl_parse_info_.tail_ref_object_node_ = object; \
        } \
      } \
    } while (0)

#define process_placeholder_for_dynamic \
  do { \
    if (p->pl_parse_info_.is_pl_parse_) { \
      yylval->node->value_ = get_question_mark(&p->question_mark_ctx_, p->malloc_pool_, yytext); \
    } else { \
      yylval->node->value_ = p->question_mark_ctx_.count_++; \
    } \
  } while (0)

//////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

#define YY_USER_ACTION                                                         \
do {                                                                           \
  check_value(yylloc);                                                         \
  ParseResult *p = (ParseResult *)yyextra;                                     \
  yylloc->first_line = yylloc->last_line = yylineno;                       \
  yylloc->first_column = p->yycolumn_;                                         \
  yylloc->last_column = yylloc->first_column + yyleng - 1;                     \
  p->yycolumn_ += yyleng;                                                      \
} while(0);

extern ParseNode *new_node(void *malloc_pool, ObItemType type, int num);

#define malloc_new_node(node, malloc_pool, type, num)                              \
do {                                                                               \
  if (OB_UNLIKELY(NULL == (node = new_node(malloc_pool, type, num)))) {            \
    ((ParseResult *)yyextra)->extra_errno_ = OB_PARSER_ERR_NO_MEMORY;              \
    yyerror(yylloc, yyextra, "No more space for mallocing '%s'\n", yytext);        \
    return ERROR;                                                                  \
  }                                                                                \
} while (0);

#define malloc_time_node(malloc_pool, type, find_char)                             \
do {                                                                               \
  ParseNode *node = NULL;                                                          \
  malloc_new_node(node, malloc_pool, type, 0);                                     \
  char *begin = strchr(yytext, find_char);                                         \
  check_value(begin);                                                              \
  char *end = strchr(begin + 1, find_char);                                        \
  check_value(end);                                                                \
  char *dest = NULL;                                                               \
  size_t len = end - begin - 1;                                                    \
  dest = parse_strndup(begin + 1, len, malloc_pool);                               \
  check_malloc(dest);                                                               \
  node->str_value_ = dest;                                                         \
  node->str_len_ = len;                                                            \
  check_value(yylval);                                                             \
  yylval->node = node;                                                             \
} while (0);

#define malloc_time_node_s(malloc_pool, type) malloc_time_node(malloc_pool, type, '\'');
#define malloc_time_node_d(malloc_pool, type) malloc_time_node(malloc_pool, type, '\"');

// special case json object ( key :ident/intnum)
#define  malloc_object_key_node(malloc_pool)                                      \
do {                                                                              \
  ParseNode *key_node = NULL;                                                     \
  malloc_new_node(key_node, malloc_pool, T_CHAR, 0);                              \
  char *begin_k = strchr(yytext, '\'');                                           \
  check_value(begin_k);                                                           \
  char *end_k = strchr(begin_k + 1, '\'');                                        \
  check_value(end_k);                                                             \
  char *dest = NULL;                                                              \
  size_t len = end_k - begin_k - 1;                                               \
  dest = parse_strndup(begin_k + 1, len, malloc_pool);                            \
  check_malloc(dest);                                                             \
  key_node->str_value_ = dest;                                                    \
  key_node->str_len_ = len;                                                       \
  char *raw_dest = NULL;                                                          \
  raw_dest = parse_strndup(begin_k + 1, len, malloc_pool);                        \
  check_malloc(raw_dest);                                                         \
  key_node->str_value_ = raw_dest;                                                \
  key_node->str_len_ = len;                                                       \
  ParseNode *object_node = NULL;                                                  \
  malloc_new_node(object_node, malloc_pool, T_LINK_NODE, 2);                      \
  object_node->children_[0] = key_node;                                           \
  check_value(yylval);                                                            \
  yylval->node = object_node;                                                     \
} while (0);

#define  malloc_key_colon_ident_value_node(malloc_pool)                                 \
do {                                                                              \
  malloc_object_key_node(malloc_pool);                                            \
  ParseNode *value_node = NULL;                                                   \
  malloc_new_node(value_node, malloc_pool, T_IDENT, 0);                           \
  char *begin_v = strchr(yytext, ':');                                            \
  check_value(begin_v);                                                           \
  size_t end_v = strlen(begin_v);                                                 \
  size_t len_v = end_v - 1;                                                       \
  char *dest_v = NULL;                                                            \
  dest_v = parse_strndup(begin_v + 1, len_v, malloc_pool);                        \
  check_malloc(dest_v);                                                           \
  value_node->str_len_ = len_v;                                                   \
  value_node->str_value_ = dest_v;                                                \
  yylval->node->children_[1] = value_node;                                         \
  check_value(yylval);                                                            \
} while (0);

#define  malloc_key_colon_intnum_value_node(malloc_pool)                                 \
do {                                                                              \
  malloc_object_key_node(malloc_pool);                                            \
  ParseNode *value_node = NULL;                                                   \
  malloc_new_node(value_node, malloc_pool, T_INT, 0);                           \
  char *begin_v = strchr(yytext, ':');                                            \
  check_value(begin_v);                                                           \
  size_t end_v = strlen(begin_v);                                                 \
  size_t len_v = end_v - 1;                                                       \
  char *dest_v = NULL;                                                            \
  dest_v = parse_strndup(begin_v + 1, len_v, malloc_pool);                        \
  check_malloc(dest_v);                                                           \
  value_node->str_len_ = len_v;                                                   \
  value_node->str_value_ = dest_v;                                                \
  yylval->node->children_[1] = value_node;                                         \
  check_value(yylval);                                                            \
} while (0);

#define check_value(val_ptr)                                                       \
do {                                                                               \
  if (OB_UNLIKELY(NULL == val_ptr))                                                \
  {                                                                                \
    ((ParseResult *)yyextra)->extra_errno_ = OB_PARSER_ERR_UNEXPECTED;             \
    yyerror(yylloc, yyextra, "'%s' is NULL\n", #val_ptr);                          \
    return ERROR;                                                                  \
  }                                                                                \
} while (0);

#define check_identifier_convert_result(errno)                                     \
do {                                                                               \
  if (OB_PARSER_ERR_ILLEGAL_NAME == errno) {                                       \
    yyerror(yylloc, yyextra, "name '%s' is illegal\n", yytext);                    \
    return ERROR;                                                                  \
  }                                                                                \
} while (0);

#define check_parser_size_overflow(errno)                                          \
do {                                                                               \
  if (OB_PARSER_ERR_SIZE_OVERFLOW == errno) {                                      \
    yyerror(NULL, result, "stack overflow in parser\n");                           \
    YYABORT_PARSE_SQL_ERROR;                                                       \
  }                                                                                \
} while (0);

#define IS_FAST_PARAMETERIZE ((ParseResult *)yyextra)->is_fp_
#define IS_NEED_PARAMETERIZE ((ParseResult *)yyextra)->need_parameterize_
#define IS_FOR_TRIGGER       ((ParseResult *)yyextra)->is_for_trigger_
#define IF_FOR_PREPROCESS    ((ParseResult *)yyextra)->is_for_preprocess_
#define IS_FOR_REMAP         ((ParseResult *)yyextra)->is_for_remap_

#define COPY_STRING(src, src_len, dst)                                                          \
do {                                                                                            \
  if (IS_FAST_PARAMETERIZE && IS_NEED_PARAMETERIZE) {                                           \
    dst = src;                                                                                  \
  } else {                                                                                      \
    ParseResult *p = (ParseResult *)yyextra;                                                    \
    dst = parse_strndup(src, src_len, p->malloc_pool_);                                         \
    check_malloc(dst);                                                                           \
  }                                                                                             \
} while (0);

#define COPY_STR_NODE_TO_TMP_LITERAL(str_node)                                                  \
do {                                                                                            \
  ParseResult *p = (ParseResult *)yyextra;                                                      \
  char **tmp_literal = &(p->tmp_literal_);                                                      \
  /*str node中遇到特殊字符，需要做转义，需要创建临时buffer，并且将之前存储的文本拷到buffer中*/             \
  if (NULL == *tmp_literal) {                                                                   \
    *tmp_literal = (char*) parse_malloc(p->input_sql_len_ + 1, p->malloc_pool_);                \
    check_malloc(*tmp_literal);                                                                 \
  }                                                                                             \
  if (str_node->str_value_ != NULL) {                                                           \
    memmove(((ParseResult *)yyextra)->tmp_literal_, str_node->str_value_, str_node->str_len_);  \
    str_node->str_value_ = NULL;                                                                \
  }                                                                                             \
} while (0);

#define STORE_STR_CONTENT(str_node)                                                             \
do {                                                                                            \
  ParseResult *p = (ParseResult *)yyextra;                                                      \
  /*如果是fast parser第一次存储str node的值不需要深拷贝，直接用指针指向sql文本即可，到下次遇到特殊字符再深拷贝*/       \
  if (str_node->str_len_ <= 0 && IS_FAST_PARAMETERIZE && IS_NEED_PARAMETERIZE) {                \
    str_node->str_value_ = p->input_sql_ + yylloc->first_column - 1;                            \
    str_node->str_len_ = yyleng;                                                                \
  } else {                                                                                      \
    char **tmp_literal = &(p->tmp_literal_);                                                    \
    if (NULL == *tmp_literal) {                                                                 \
      *tmp_literal = (char*) parse_malloc(p->input_sql_len_ + 1, p->malloc_pool_);              \
      check_malloc(*tmp_literal);                                                               \
    }                                                                                           \
    memmove(*tmp_literal + str_node->str_len_, yytext, yyleng);                                 \
    str_node->str_len_ += yyleng;                                                               \
  }                                                                                             \
} while (0);

#define FORMAT_STR_NODE(str_node)                                                               \
do {                                                                                            \
  ParseResult *p = (ParseResult *)yyextra;                                                      \
  if (str_node->str_value_ == NULL && str_node->str_len_ > 0) {                                 \
    char *tmp_literal = p->tmp_literal_;                                                        \
    tmp_literal[yylval->node->str_len_] = '\0';                                                 \
    str_node->str_value_ = parse_strndup(tmp_literal, str_node->str_len_ + 1, p->malloc_pool_); \
    check_malloc(str_node->str_value_);                                                         \
  }                                                                                             \
} while (0);

#define COPY_WRITE()                                                          \
do {                                                                          \
  ParseResult *p = (ParseResult *)yyextra;                                    \
  memmove(p->no_param_sql_ + p->no_param_sql_len_, yytext, yyleng);           \
  p->no_param_sql_len_ += yyleng;                                             \
  p->no_param_sql_[p->no_param_sql_len_] = '\0';                              \
} while (0);

#define STORE_PARAM_NODE()                          \
do {                                                \
  ParseResult *p = (ParseResult *)yyextra;          \
  if (p->need_parameterize_) {                      \
    check_value(yylval);                            \
    check_value(yylval->node);                      \
    size_t alloc_len = sizeof(ParamList);           \
    ParamList *param = (ParamList *)parse_malloc(alloc_len, p->malloc_pool_); \
    check_malloc(param);                 \
    STORE_PARAM_NODE_NEED_PARAMETERIZE(param, yylval->node, p, yylloc->first_column);  \
  } else {                                          \
    return OUTLINE_DEFAULT_TOKEN;                   \
  }                                                 \
} while(0);

#define STORE_UNIT_TYPE_NODE(str_val)                                              \
do {                                                                               \
  ParseResult *p = (ParseResult *)yyextra;                                          \
  if (p->need_parameterize_) {                                                      \
  ParseNode* node = NULL;                                                          \
  malloc_new_node(node, ((ParseResult *)yyextra)->malloc_pool_, T_INT, 0);          \
  check_value(yylval);                                                             \
  yylval->node = node;                                                             \
  yylval->node->raw_text_ = parse_strdup(yytext,                                   \
                                         ((ParseResult *)yyextra)->malloc_pool_,   \
                                         &(yylval->node->text_len_));              \
  check_malloc(yylval->node->raw_text_);                                            \
  node->str_value_ = parse_strdup((char *)str_val,                                 \
                                  ((ParseResult*)yyextra)->malloc_pool_,           \
                                  &(node->str_len_));                              \
  check_malloc(node->str_value_);                                                   \
  node->value_ = strtoll(node->str_value_, NULL, 10);                              \
  STORE_PARAM_NODE()                                                               \
  } else {                                                                         \
    return OUTLINE_DEFAULT_TOKEN;                                                     \
  }                                                                                \
} while(0);

#define HANDLE_UNIT_TYPE(type)                                                     \
do {                                                                               \
  if (IS_FAST_PARAMETERIZE) {                                                      \
    ParseResult *p = (ParseResult *)yyextra;                                          \
    if (p->need_parameterize_) {                                                      \
      STORE_UNIT_TYPE_NODE(ob_date_unit_type_num_str(DATE_UNIT_##type));             \
    } else {                                                                           \
      return OUTLINE_DEFAULT_TOKEN;                                                     \
    }                                                                                  \
  } else {                                                                         \
    return type;                                                                   \
  }                                                                                \
} while(0);

#define HANDLE_ESCAPE(presult)                                                      \
do {                                                                                \
  int with_back_slash = 1;                                                        \
  unsigned char c = escaped_char(yytext[1], &with_back_slash);                    \
  if (with_back_slash)                                                            \
  {                                                                               \
    presult->tmp_literal_[yylval->node->str_len_++] = '\\';                       \
  }                                                                               \
  presult->tmp_literal_[yylval->node->str_len_++] = c;                            \
} while(0);

/*
 * 当转义符是假转义符时，'\'实际是前面一个字符的一部分，需要添加到literal，
 * 调用yyless，将'\'后面的字符需要回滚到input stream中重新进行匹配，
 * 因为多解析了一个字符，因此需要把yycolumn_减一，
 * 保证 YY_USER_ACTION 能根据yycolumn_计算出正确的column位置
 */

#define HANDLE_FALSE_ESCAPE(presult)                                              \
do {                                                                              \
  presult->tmp_literal_[yylval->node->str_len_++] = '\\';                         \
  yyless(1);                                                                      \
  presult->yycolumn_--;                                                           \
} while(0);

/*
 * 有escape_with_backslash_is_dangerous标记的字符集，如big5, cp932, gbk, sjis
 * 转义字符（0x5C）有可能是某个多字节字符的一部分，需要特殊判断
 */

#define CHECK_REAL_ESCAPE(is_real_escape)                                         \
  is_real_escape = check_real_escape(p->charset_info_, p->tmp_literal_,           \
                                     yylval->node->str_len_, p->last_escape_check_pos_)
  /*
do {                                                                              \
  if (NULL !=  p->charset_info_ && p->charset_info_->escape_with_backslash_is_dangerous) { \
    char *cur_pos = p->tmp_literal_ + yylval->node->str_len_;                     \
    char *last_check_pos = p->tmp_literal_ + p->last_well_formed_len_;            \
    int error = 0;                                                                \
    int expected_well_formed_len = cur_pos - last_check_pos;                      \
    int real_well_formed_len = p->charset_info_->cset->well_formed_len(           \
                p->charset_info_, last_check_pos, cur_pos, UINT64_MAX, &error);   \
    if (error != 0) {                                                             \
      *cur_pos = '\\';                                                            \
      if (real_well_formed_len == expected_well_formed_len - 1                    \
          && p->charset_info_->cset->ismbchar(p->charset_info_, cur_pos - 1, cur_pos + 1)) {  \
        is_real_escape = false;                                                   \
        p->last_well_formed_len_ = yylval->node->str_len_ + 1;                    \
      } else {                                                                    \
        p->last_well_formed_len_ = yylval->node->str_len_;                        \
      }                                                                           \
    } else {                                                                      \
      p->last_well_formed_len_ = yylval->node->str_len_;                          \
    }                                                                             \
  }                                                                               \
} while(0);*/
/*
#define CHECK_NODE_STRING_VALUE_ASCII(token, my_str_ptr, my_str_len)              \
do {                                                                              \
  if (p->charset_info_ != NULL && p->is_not_utf8_connection_) {                   \
    if (my_str_len != p->charset_info_->cset->numchars(p->charset_info_,          \
                                                       my_str_ptr,                \
                                                       my_str_ptr + my_str_len)) {\
      p->extra_errno_ = OB_PARSER_ERR_ILLEGAL_NAME;                                               \
      yyerror(yylloc, yyextra, "multi-byte identifier '%.*s' not support in charset '%s'\n",      \
              my_str_len, my_str_ptr, p->charset_info_->csname);                                  \
      token = PARSER_SYNTAX_ERROR;                                                                \
    }                                                                                             \
  }                                                                                               \
} while (0)
*/


#define ADD_YYLINENO(_yytext, _yyleng)                                            \
for (int32_t _i = 0; _i < _yyleng; ++_i) {                                        \
  if (_yytext[_i] == '\n') {                                                      \
    ++yylineno;                                                                   \
  }                                                                               \
}

#define COPY_NUM_STRING(parse_result, node)                                       \
  do {                                                                            \
    if (IS_FAST_PARAMETERIZE) {                                                   \
      if (parse_result->minus_ctx_.has_minus_) {                                  \
        int64_t start_pos = parse_result->minus_ctx_.raw_sql_offset_;             \
        int64_t cp_len = yylloc->first_column - start_pos - 1 + yyleng;           \
        COPY_STRING(p->input_sql_ + start_pos, cp_len, node->str_value_);         \
        node->str_len_ = cp_len;                                                  \
      } else {                                                                    \
        COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, node->str_value_);      \
        node->str_len_ = yyleng;                                                  \
      }                                                                           \
    } else {                                                                      \
      COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, node->str_value_);        \
      node->str_len_ = yyleng;                                                    \
    }                                                                             \
  } while (0);

#define PARSE_INT_STR_MYSQL(param_node, malloc_pool, errno)              \
  do {                                                                   \
    if ('-' == param_node->str_value_[0]) {                              \
      char *copied_str = parse_strndup(param_node->str_value_, param_node->str_len_, malloc_pool);   \
      if (OB_ISNULL(copied_str)) {                                       \
        ((ParseResult *)yyextra)->extra_errno_ = OB_PARSER_ERR_NO_MEMORY;\
        yyerror(NULL, yyextra, "No more space for mallocing");           \
        return ERROR;                                                    \
      } else {                                                           \
        int pos = 1;                                                     \
        for (; pos < param_node->str_len_ && ISSPACE(copied_str[pos]); pos++) ;                           \
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

#define PARSE_INT_STR_ORACLE(param_node, malloc_pool, errno)             \
  do {                                                                   \
    if ('-' == param_node->str_value_[0]) {                              \
      char *copied_str = parse_strndup(param_node->str_value_, param_node->str_len_, malloc_pool);   \
      if (OB_ISNULL(copied_str)) {                                       \
        ((ParseResult *)yyextra)->extra_errno_ = OB_PARSER_ERR_NO_MEMORY;\
        yyerror(NULL, yyextra, "No more space for mallocing");           \
        return ERROR;                                                    \
      } else {                                                           \
        int pos = 1;                                                     \
        for (; pos < param_node->str_len_ && ISSPACE(copied_str[pos]); pos++) ;                           \
        copied_str[--pos] = '-';                                                                          \
        param_node->value_ = ob_strntoll(copied_str + pos, param_node->str_len_ - pos, 10, NULL, &errno); \
        if (ERANGE == errno) {                                                                            \
          param_node->type_ = T_NUMBER;                                                                   \
          token_ret = DECIMAL_VAL;                                                                        \
        }                                                                                                 \
      }                                                                                                   \
    } else {                                                                                              \
      param_node->value_ = ob_strntoll(param_node->str_value_, param_node->str_len_, 10, NULL, &errno);   \
      if (ERANGE == errno) {                                                                              \
        param_node->type_ = T_NUMBER;                                                                     \
        token_ret = DECIMAL_VAL;                                                                          \
      }                                                                                                   \
    }                                                                                                     \
  } while (0);

#define RM_MULTI_STMT_END_P(p)                                \
  do                                                          \
  {                                                           \
    int pos = p->no_param_sql_len_ - 1;                       \
    for (; pos >= 0 && ISSPACE(p->no_param_sql_[pos]); --pos) \
      ;                                                       \
    p->no_param_sql_len_ = pos + 1;                           \
    p->no_param_sql_[p->no_param_sql_len_] = '\0';            \
    p->end_col_ = p->no_param_sql_len_;                       \
  } while (0);

#define malloc_select_node(node, malloc_pool)                                     \
      malloc_non_terminal_node(node, malloc_pool, T_SELECT, PARSE_SELECT_MAX_IDX, \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL, 	                                                \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL,                                                  \
                           NULL);

// only used by setup_token_pos_info_and_dup_string for now
#define check_ret(stmt, loc, extra)                                          \
  int ret = (stmt);                                                          \
  if (OB_UNLIKELY(ret != OB_PARSER_SUCCESS)) {                               \
    yyerror((loc), (extra), "setup token pos info failed ret: %d\n", ret);   \
  }

#define REPUT_TOKEN_NEG_SIGN(TOKEN)        \
  ParseResult *p = (ParseResult *)yyextra; \
  REPUT_NEG_SIGN(p);                       \
  return TOKEN;

extern void setup_token_pos_info(ParseNode *node, int off, int len);
// setup pos info and copy sql from p->input_sql_([strat, end]) to node->str_value_
extern int setup_token_pos_info_and_dup_string(ParseNode *node,
                                               ParseResult *p,
                                               int start,
                                               int end);
#ifdef SQL_PARSER_COMPILATION
int add_comment_list(ParseResult *p, const TokenPosInfo *info);
#endif

void REPUT_NEG_SIGN(ParseResult *p);
int STORE_PARAM_NODE_NEED_PARAMETERIZE(ParamList *param,
    struct _ParseNode *node, ParseResult *p, const int first_column);

// Check whether the decimal is a int with suffix zero.
// if true, store the int value in result, set the int_flag to true;
// Notice: we use the (int64_t) double == double here, this kind of comparison
// only supports up to 15 decimal place precision:
// e.g., (int64_t)3.00000000000000001 == 3.00000000000000001 is true;
// oracle supports up to 40 decimal place precision.
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

#define CHECK_VALID_PACKAGE_VARIABLE_NAME(node)                             \
  do {                                                                      \
    if (OB_UNLIKELY(NULL == node || NULL == node->str_value_)) {            \
      yyerror(NULL, result, "invalid arguments node: %p", node);            \
      YYABORT_UNEXPECTED;                                                   \
    } else if ((49 + 4) != node->str_len_) {                                \
      /* A valid package variable name like this: */                        \
      /*   pkg.019280808000eb8780808020018480808000d84ea84f0107 */          \
      yyerror(NULL, result, "invalid arguments node");                      \
    } else if (strncmp(node->str_value_, "pkg.", 4) != 0) {                 \
      yyerror(NULL, result, "invalid arguments node,not start with 'pkg.'");\
      YYABORT_UNEXPECTED;                                                   \
    } else {                                                                \
      for (int32_t i = 4; i < node->str_len_; ++i) {                        \
        if (!(node->str_value_[i] >= 0                                      \
              && node->str_value_[i] <= 9                                   \
              && node->str_value_[i] >= 'a'                                 \
              && node->str_value_[i] <= 'z')) {                             \
          yyerror(NULL, result, "invalid arguments node, include invalid char"); \
          YYABORT_UNEXPECTED;                                               \
        }                                                                   \
      }                                                                     \
    }                                                                       \
  } while (0);

// bugfix:
// convert '%' to '%%' for printf's format string.
#define ESCAPE_PERCENT(result, src, dst)\
do {\
  if (OB_NOT_NULL(result) && OB_NOT_NULL(src)) {\
    int64_t src_len = strlen(src);\
    int64_t dst_len = 2 * src_len + 1;\
    int64_t pos = 0;\
    dst = (char *)parse_malloc(dst_len, result->malloc_pool_);\
    if (OB_NOT_NULL(dst)) {\
      for (int64_t i = 0; i < src_len; i++) {\
        if (src[i] == '%') {\
          dst[pos++] = '%';\
        }\
        dst[pos++] = src[i];\
      }\
      dst[pos] = 0;\
    }\
  }\
} while (0)\

// bugfix:
// avoid '\0' in the middle of a str.
#define CHECK_STR_LEN_MATCH(src_str, str_len)                                   \
  do {                                                                          \
    if (OB_UNLIKELY(src_str == NULL)) {                                         \
    } else {                                                                    \
      for (int64_t i = 0; i < str_len; i++) {                                   \
        if (OB_UNLIKELY(src_str[i] == '\0')) {                                  \
          yyerror(yylloc, yyextra, "mismatch strlen, may cased by '\0' in str");\
          return PARSER_SYNTAX_ERROR;                                           \
        }                                                                       \
      }                                                                         \
    }                                                                           \
  } while(0);                                                                   \

// if the const is in /*! xx**/, we should ignore the const in fast parser.
#define CHECK_MYSQL_COMMENT(p, node)\
  do {\
    if (p->mysql_compatible_comment_) {\
      node->is_forbid_parameter_ = 1;\
    }\
  } while(0);\

#define set_data_type_collation(date_type, attribute, only_collation, enable_multi_collation)                \
  do {\
    ParseNode *collation = NULL; \
    bool found = false;\
    if (attribute != NULL) {\
      if (attribute->type_ == T_COLLATION) {\
        collation = attribute;\
      } else { \
        for (int i = 0; i < attribute->num_child_; i++) {                    \
          if (attribute->children_[i]->type_ == T_COLLATION) {                 \
            if (!enable_multi_collation && found) {  \
              yyerror(NULL, result, "Multiple COLLATE syntax not supported\n");\
              YYABORT;\
            } else {\
              collation = attribute->children_[i];\
              found = true;\
            }\
          } else if (only_collation) {\
            yyerror(NULL, result, "Only support COLLATE syntax here\n");\
            YYABORT;\
          }\
        }\
      }\
    }\
    if (collation == NULL) {\
    } else if (date_type->type_ == T_CHAR || date_type->type_ == T_VARCHAR || date_type->type_ == T_TEXT || date_type->type_ == T_TINYTEXT\
              || date_type->type_ == T_LONGTEXT || date_type->type_ == T_MEDIUMTEXT || date_type->type_ == T_SET || date_type->type_ == T_ENUM) {\
      if (date_type->num_child_ < 2) {\
      } else {\
        date_type->children_[1] = collation;\
      }\
    }\
  } while(0);\

// bugfix:
// avoid '"' in the middle of a str in oracle mode
#define CHECK_ORACLE_IDENTIFIER_VALID(src_str, str_len)                         \
  do {                                                                          \
    if (OB_UNLIKELY(src_str == NULL || str_len <= 0)) {                         \
    } else {                                                                    \
      for (int64_t i = 0; i < str_len; i++) {                                   \
        if (OB_UNLIKELY(src_str[i] == '\"')) {                                  \
          ((ParseResult *)yyextra)->extra_errno_ = OB_PARSER_ERR_UNSUPPORTED;   \
          yyerror(yylloc, yyextra, "identifier not support to have double quote");\
          return ERROR;                                                         \
        }                                                                       \
      }                                                                         \
    }                                                                           \
  } while(0);                                                                   \

#define malloc_select_values_stmt(node, result, values_node, order_by_node, limit_node)\
  do {\
    /*gen select list*/\
    ParseNode *star_node = NULL;\
    malloc_terminal_node(star_node, result->malloc_pool_, T_STAR);\
    dup_string_to_node(star_node, result->malloc_pool_, "*");\
    ParseNode *project_node = NULL;\
    malloc_non_terminal_node(project_node, result->malloc_pool_, T_PROJECT_STRING, 1, star_node);\
    ParseNode *project_list_node = NULL;\
    merge_nodes(project_list_node, result, T_PROJECT_LIST, project_node);\
    /*gen from list*/\
    ParseNode *alias_node = NULL;\
    malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, values_node, NULL);\
    ParseNode *from_list = NULL;\
    merge_nodes(from_list, result, T_FROM_LIST, alias_node);\
    /*malloc select node*/\
    malloc_select_node(node, result->malloc_pool_);\
    node->children_[PARSE_SELECT_SELECT] = project_list_node;\
    node->children_[PARSE_SELECT_FROM] = from_list;\
    node->children_[PARSE_SELECT_ORDER] = order_by_node;\
    node->children_[PARSE_SELECT_LIMIT] = limit_node;\
  } while(0);\

#define refine_insert_values_table(node)\
  do {\
     if (node != NULL && node->type_ == T_SELECT && node->children_[PARSE_SELECT_FROM] != NULL &&\
         node->children_[PARSE_SELECT_FROM]->type_ == T_FROM_LIST &&\
         node->children_[PARSE_SELECT_FROM]->num_child_ == 1 &&\
         node->children_[PARSE_SELECT_FROM]->children_ != NULL &&\
         node->children_[PARSE_SELECT_FROM]->children_[0] != NULL &&\
         node->children_[PARSE_SELECT_FROM]->children_[0]->type_ == T_ALIAS) {\
      ParseNode *alias_node = node->children_[PARSE_SELECT_FROM]->children_[0];\
      if (alias_node->children_ != NULL &&\
          alias_node->num_child_ == 2 && \
          alias_node->children_[0] != NULL &&\
          alias_node->children_[0]->type_ == T_VALUES_TABLE_EXPRESSION) {\
        ParseNode *values_table_node = alias_node->children_[0];\
        if (values_table_node->children_ != NULL &&\
            values_table_node->num_child_ == 1 && \
            values_table_node->children_[0] != NULL &&\
            values_table_node->children_[0]->type_ == T_VALUES_ROW_LIST) {\
          values_table_node->children_[0]->type_ = T_VALUE_LIST;\
          if (node->children_[PARSE_SELECT_ORDER] == NULL &&\
              node->children_[PARSE_SELECT_LIMIT] == NULL) {\
            node = values_table_node->children_[0]; \
          }\
        }\
      }\
     }\
  } while(0);\

#endif /* OCEANBASE_SRC_SQL_PARSER_SQL_PARSER_BASE_H_ */
