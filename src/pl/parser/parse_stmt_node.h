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

#ifndef OCEANBASE_SRC_PL_PARSER_PARSE_STMT_NODE_H_
#define OCEANBASE_SRC_PL_PARSER_PARSE_STMT_NODE_H_
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>
#include <setjmp.h>
#include "sql/parser/parse_node.h"
#ifdef __cplusplus
extern "C" {
#endif

typedef struct _ObParseErrorInfo
{
  int error_code_;
  ObStmtLoc stmt_loc_;
} ObParseErrorInfo;

//typedef struct _ObStmtNodeTree
//{
//  ObParseErrorInfo *error_info_;
//  ParseNode *result_tree_;
//} ObStmtNodeTree;
typedef ParseNode ObStmtNodeTree;

typedef struct _ObScannerCtx
{
  void *yyscan_info_;
  char *tmp_literal_;
  int tmp_literal_len_;
  ObSQLMode sql_mode_;
  int token_len_;
  int first_column_;
  int sql_start_loc;
  int sql_end_loc;
} ObScannerCtx;

typedef struct _ObParseCtx
{
  void *mem_pool_; // ObIAllocator
  ObStmtNodeTree *stmt_tree_;
  ObScannerCtx scanner_ctx_;
  jmp_buf jmp_buf_; //handle fatal error
  int global_errno_;
  char global_errmsg_[MAX_ERROR_MSG];
  ObParseErrorInfo *cur_error_info_;
  const char *stmt_str_;
  int stmt_len_;
  const char *orig_stmt_str_;
  int orig_stmt_len_;
  ObQuestionMarkCtx question_mark_ctx_;//用来记录整个anonymous中所有的question mark
  int comp_mode_;
  bool is_not_utf8_connection_;
  const struct ObCharsetInfo *charset_info_;
  const struct ObCharsetInfo *charset_info_oracle_db_;
  int64_t last_escape_check_pos_;  //解析quoted string时的一个临时变量，处理连接gbk字符集时遇到的转义字符问题
  int connection_collation_;
  bool mysql_compatible_comment_; //whether the parser is parsing "/*! xxxx */"
  int copied_pos_;
  char *no_param_sql_;
  int no_param_sql_len_;
  int no_param_sql_buf_len_;
  int param_node_num_;
  ParamList *param_nodes_;
  ParamList *tail_param_node_;
  struct
  {
    uint32_t is_inner_parse_:1;   //is inner parser, not from the user's call
    uint32_t is_for_trigger_:1;
    uint32_t is_dynamic_:1; //是否是从dynamic sql过来的
    uint32_t is_for_preprocess_:1;
    uint32_t is_include_old_new_in_trigger_:1; // indicates whether include :old/:new/:parent in trigger body
    uint32_t in_q_quote_:1;
    uint32_t is_pl_fp_  :1;
    uint32_t is_forbid_anony_parameter_ : 1; // 1 表示禁止匿名块参数化
    uint32_t reserved_:24;
  };
} ObParseCtx;

#ifdef __cplusplus
}
#endif
#endif /* OCEANBASE_SRC_PL_PARSER_PARSE_STMT_NODE_H_ */
