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

#include "sql_parser_base.h"

#define YY_EXTRA_TYPE void*
#define yyconst const
typedef void* yyscan_t;
typedef struct yy_buffer_state* YY_BUFFER_STATE;
#define IS_ORACLE_MODE(mode) (0 != (mode & SMO_ORACLE))
#define IS_ORACLE_COMPATIBLE (IS_ORACLE_MODE(p->sql_mode_))

extern int obsql_mysql_yylex_init_extra(YY_EXTRA_TYPE yy_user_defined, yyscan_t* ptr_yy_globals);
extern int obsql_mysql_yyparse(ParseResult* result);
extern int obsql_mysql_multi_fast_parse(ParseResult* p);
extern int obsql_mysql_fast_parse(ParseResult* p);
extern int obsql_mysql_yylex_destroy(yyscan_t yyscanner);
extern YY_BUFFER_STATE obsql_mysql_yy_scan_bytes(yyconst char* bytes, int len, yyscan_t yyscanner);
extern void obsql_mysql_yy_switch_to_buffer(YY_BUFFER_STATE new_buffer, yyscan_t yyscanner);
extern void obsql_mysql_yy_delete_buffer(YY_BUFFER_STATE b, yyscan_t yyscanner);

int parse_init(ParseResult* p)
{
  int ret = 0;  // can not include C++ file "ob_define.h"
  if (OB_UNLIKELY(NULL == p || NULL == p->malloc_pool_)) {
    ret = -1;
    if (NULL != p) {
      (void)snprintf(p->error_msg_, MAX_ERROR_MSG, "malloc_pool_ must be set");
    }
  }

  if (OB_LIKELY(0 == ret)) {
    ret = IS_ORACLE_COMPATIBLE ? OB_PARSER_ERR_PARSE_SQL : obsql_mysql_yylex_init_extra(p, &(p->yyscan_info_));
  }
  return ret;
}

int parse_reset(ParseResult* p)
{
  if (NULL != p) {
    p->yyscan_info_ = NULL;
    p->result_tree_ = NULL;
    p->input_sql_ = NULL;
    p->input_sql_len_ = 0;
    p->malloc_pool_ = NULL;
    p->extra_errno_ = 0;
    p->error_msg_[0] = '\0';
    p->start_col_ = 0;
    p->end_col_ = 0;
    p->line_ = 0;
    p->yycolumn_ = 0;
    p->yylineno_ = 0;
    p->tmp_literal_ = NULL;

    p->question_mark_ctx_.count_ = 0;
    p->question_mark_ctx_.by_ordinal_ = false;
    p->question_mark_ctx_.by_name_ = false;
    p->question_mark_ctx_.name_ = NULL;
    p->question_mark_ctx_.capacity_ = 0;
    p->sql_mode_ = 0;

    p->has_encount_comment_ = false;
    p->is_fp_ = false;
    p->is_multi_query_ = false;
    p->no_param_sql_ = NULL;
    p->no_param_sql_len_ = 0;
    p->no_param_sql_buf_len_ = 0;
    p->param_nodes_ = NULL;
    p->tail_param_node_ = NULL;
    p->param_node_num_ = 0;
    p->token_num_ = 0;
    p->is_ignore_hint_ = false;
    p->is_ignore_token_ = false;
    p->need_parameterize_ = false;
    /*for pl*/
    p->pl_parse_info_.is_pl_parse_ = false;
    p->pl_parse_info_.is_pl_parse_expr_ = false;
    p->pl_parse_info_.pl_ns_ = NULL;
    p->pl_parse_info_.last_pl_symbol_pos_ = 0;
    p->pl_parse_info_.ref_object_nodes_ = NULL;
    p->pl_parse_info_.tail_ref_object_node_ = NULL;
    /*for  q-quote*/
    p->in_q_quote_ = false;

    p->minus_ctx_.pos_ = 0;
    p->minus_ctx_.raw_sql_offset_ = 0;
    p->minus_ctx_.has_minus_ = false;
    p->minus_ctx_.is_cur_numeric_ = false;

    p->is_for_trigger_ = false;
    p->is_dynamic_sql_ = false;
    p->is_dbms_sql_ = false;
    p->is_batched_multi_enabled_split_ = false;
    p->is_not_utf8_connection_ = false;
    p->charset_info_ = NULL;
    p->last_well_formed_len_ = 0;
    p->may_bool_value_ = false;

#ifdef SQL_PARSER_COMPILATION
    p->comment_list_ = NULL;
    p->comment_cnt_ = 0;
    p->comment_cap_ = 0;
    p->realloc_cnt_ = 0;
    p->stop_add_comment_ = false;
#endif
  }
  return 0;
}

int parse_terminate(ParseResult* p)
{
  int ret = 0;
  if (OB_LIKELY(NULL != p->yyscan_info_)) {
    ret = IS_ORACLE_COMPATIBLE ? OB_PARSER_ERR_PARSE_SQL : obsql_mysql_yylex_destroy(p->yyscan_info_);
  }
  return ret;
}

int parse_sql(ParseResult* p, const char* buf, size_t len)
{
  int ret = OB_PARSER_ERR_PARSE_SQL;
  if (OB_UNLIKELY(NULL == p)) {
  } else if (IS_ORACLE_COMPATIBLE) {
    ret = OB_PARSER_ERR_PARSE_SQL;
  } else if (OB_UNLIKELY(NULL == buf || len <= 0)) {
    snprintf(p->error_msg_, MAX_ERROR_MSG, "Input SQL can not be empty");
    ret = OB_PARSER_ERR_EMPTY_QUERY;
  } else {
    p->result_tree_ = 0;
    p->error_msg_[0] = 0;
    p->input_sql_ = buf;
    p->input_sql_len_ = len;
    p->start_col_ = 1;
    p->end_col_ = 1;
    p->line_ = 1;
    p->yycolumn_ = 1;
    p->yylineno_ = 1;
    p->tmp_literal_ = NULL;
    p->last_well_formed_len_ = 0;
#ifdef SQL_PARSER_COMPILATION
    p->realloc_cnt_ = p->realloc_cnt_ <= 0 ? 10 : p->realloc_cnt_;
    p->comment_cap_ = p->realloc_cnt_;
    p->comment_cnt_ = 0;
    p->stop_add_comment_ = false;
#endif
    if (false == p->pl_parse_info_.is_pl_parse_) {
      p->question_mark_ctx_.count_ = 0;
    }

    // while (len > 0 && ISSPACE(buf[len - 1])) {
    //  --len;
    // }

    // if (OB_UNLIKELY(len <= 0)) {
    // (void)snprintf(p->error_msg_, MAX_ERROR_MSG, "Input SQL can not be white space only");
    //  ret = OB_PARSER_ERR_EMPTY_QUERY;
    //  } else {
    {
      int val = setjmp(p->jmp_buf_);
      if (val) {
        ret = OB_PARSER_ERR_NO_MEMORY;
#ifdef SQL_PARSER_COMPILATION
      } else if (OB_ISNULL(p->comment_list_ = (TokenPosInfo*)(parse_malloc(
                               p->realloc_cnt_ * sizeof(TokenPosInfo), p->malloc_pool_)))) {
        ret = OB_PARSER_ERR_NO_MEMORY;
#endif
      } else {
        // bp = yy_scan_string(buf, p->yyscan_info_);
        YY_BUFFER_STATE bp = obsql_mysql_yy_scan_bytes(buf, len, p->yyscan_info_);
        obsql_mysql_yy_switch_to_buffer(bp, p->yyscan_info_);
        int tmp_ret = -1;
        if (p->is_fp_) {
          tmp_ret = obsql_mysql_fast_parse(p);
        } else if (p->is_multi_query_) {
          tmp_ret = obsql_mysql_multi_fast_parse(p);
        } else {
          tmp_ret = obsql_mysql_yyparse(p);
        }

        if (0 == tmp_ret) {
          ret = OB_PARSER_SUCCESS;
        } else if (2 == tmp_ret) {
          ret = OB_PARSER_ERR_NO_MEMORY;
        } else {
          if (0 != p->extra_errno_) {
            ret = p->extra_errno_;
          } else {
            ret = OB_PARSER_ERR_PARSE_SQL;
          }
        }
        obsql_mysql_yy_delete_buffer(bp, p->yyscan_info_);
      }
    }
  }
  return ret;
}

int parse_sql_stmt(ParseResult* parse_result)
{
  int ret = -1;
  if (0 != (ret = parse_init(parse_result))) {
    // fix bug #16298144
    // fprintf(stderr, "init parse result failed, ret=%d\n", ret);
  } else if (0 != (ret = parse_sql(parse_result, parse_result->input_sql_, parse_result->input_sql_len_))) {
    // fix bug #16298144
    // fprintf(stderr, "parse sql failed, ret=%d, input_sql=%.*s\n", ret, parse_result->input_sql_len_,
    // parse_result->input_sql_);
  }
  return ret;
}

void setup_token_pos_info(
    ParseNode* node __attribute__((unused)), int off __attribute__((unused)), int len __attribute__((unused)))
{
#ifdef SQL_PARSER_COMPILATION
  node->token_off_ = off;
  node->token_len_ = len;
#else
  // do nothing
#endif
}

int setup_token_pos_info_and_dup_string(ParseNode* node __attribute__((unused)),
    ParseResult* result __attribute__((unused)), int start __attribute__((unused)), int end __attribute__((unused)))
{
  int ret = OB_PARSER_SUCCESS;
#ifdef SQL_PARSER_COMPILATION
  node->token_off_ = start - 1;
  node->token_len_ = end - start + 1;
  if (start > end) {
    ret = OB_PARSER_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY((NULL == (node->str_value_ = copy_expr_string(result, start, end))))) {
    ret = OB_PARSER_ERR_NO_MEMORY;
  } else {
    node->str_len_ = end - start + 1;
  }
#else
  // do nothing
#endif
  return ret;
}

#ifdef SQL_PARSER_COMPILATION
int add_comment_list(ParseResult* p, const TokenPosInfo* info)
{
  int ret = OB_PARSER_SUCCESS;
  if (OB_ISNULL(p) || OB_ISNULL(info)) {
    ret = OB_PARSER_ERR_UNEXPECTED;
  }
  if (OB_PARSER_SUCCESS == ret && p->comment_cnt_ + 1 >= p->comment_cap_) {
    int alloc_cnt = p->comment_cnt_ + p->realloc_cnt_;
    char* buf = parse_malloc(sizeof(TokenPosInfo) * alloc_cnt, p->malloc_pool_);
    if (OB_ISNULL(buf)) {
      ret = OB_PARSER_ERR_NO_MEMORY;
    } else {
      memset(buf, 0, sizeof(TokenPosInfo) * alloc_cnt);
      memcpy(buf, (void*)(p->comment_list_), sizeof(TokenPosInfo) * p->comment_cnt_);
      p->comment_list_ = (TokenPosInfo*)buf;
      p->comment_cap_ += p->realloc_cnt_;
    }
  }
  if (OB_PARSER_SUCCESS == ret) {
    p->comment_list_[p->comment_cnt_].token_off_ = info->token_off_;
    p->comment_list_[p->comment_cnt_].token_len_ = info->token_len_;
    p->comment_cnt_ += 1;
  }
  return ret;
}
#endif
