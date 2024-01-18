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

#define YY_EXTRA_TYPE void *
#define yyconst const
typedef void* yyscan_t;
typedef struct yy_buffer_state *YY_BUFFER_STATE;
#define IS_ORACLE_MODE(mode) (0 != (mode & SMO_ORACLE))
#define IS_ORACLE_COMPATIBLE (IS_ORACLE_MODE(p->sql_mode_))
extern int obsql_mysql_yylex_init_extra(YY_EXTRA_TYPE yy_user_defined,yyscan_t* ptr_yy_globals );
extern int obsql_mysql_yyparse(ParseResult *result);
extern int obsql_mysql_multi_fast_parse(ParseResult *p);
extern int obsql_mysql_multi_values_parse(ParseResult *p);
extern int obsql_mysql_fast_parse(ParseResult *p);
extern int obsql_mysql_yylex_destroy (yyscan_t yyscanner );
extern YY_BUFFER_STATE obsql_mysql_yy_scan_bytes (yyconst char *bytes,int len ,yyscan_t yyscanner );
extern void obsql_mysql_yy_switch_to_buffer (YY_BUFFER_STATE new_buffer ,yyscan_t yyscanner );
extern void obsql_mysql_yy_delete_buffer (YY_BUFFER_STATE b ,yyscan_t yyscanner );
#ifdef OB_BUILD_ORACLE_PARSER
extern int obsql_oracle_latin1_yylex_init_extra(YY_EXTRA_TYPE yy_user_defined,yyscan_t* ptr_yy_globals );
extern int obsql_oracle_latin1_yyparse(ParseResult *result);
extern int obsql_oracle_latin1_multi_fast_parse(ParseResult *p);
extern int obsql_oracle_latin1_multi_values_parse(ParseResult *p);
extern int obsql_oracle_latin1_fast_parse(ParseResult *p);
extern int obsql_oracle_latin1_yylex_destroy (yyscan_t yyscanner );
extern YY_BUFFER_STATE obsql_oracle_latin1_yy_scan_bytes (yyconst char *bytes,int len ,yyscan_t yyscanner );
extern void obsql_oracle_latin1_yy_switch_to_buffer (YY_BUFFER_STATE new_buffer ,yyscan_t yyscanner );
extern void obsql_oracle_latin1_yy_delete_buffer (YY_BUFFER_STATE b ,yyscan_t yyscanner );
extern int obsql_oracle_utf8_yylex_init_extra(YY_EXTRA_TYPE yy_user_defined,yyscan_t* ptr_yy_globals );
extern int obsql_oracle_utf8_yyparse(ParseResult *result);
extern int obsql_oracle_utf8_multi_fast_parse(ParseResult *p);
extern int obsql_oracle_utf8_multi_values_parse(ParseResult *p);
extern int obsql_oracle_utf8_fast_parse(ParseResult *p);
extern int obsql_oracle_utf8_yylex_destroy (yyscan_t yyscanner );
extern YY_BUFFER_STATE obsql_oracle_utf8_yy_scan_bytes (yyconst char *bytes,int len ,yyscan_t yyscanner );
extern void obsql_oracle_utf8_yy_switch_to_buffer (YY_BUFFER_STATE new_buffer ,yyscan_t yyscanner );
extern void obsql_oracle_utf8_yy_delete_buffer (YY_BUFFER_STATE b ,yyscan_t yyscanner );
extern int obsql_oracle_gbk_yylex_init_extra(YY_EXTRA_TYPE yy_user_defined,yyscan_t* ptr_yy_globals );
extern int obsql_oracle_gbk_yyparse(ParseResult *result);
extern int obsql_oracle_gbk_multi_fast_parse(ParseResult *p);
extern int obsql_oracle_gbk_multi_values_parse(ParseResult *p);
extern int obsql_oracle_gbk_fast_parse(ParseResult *p);
extern int obsql_oracle_gbk_yylex_destroy (yyscan_t yyscanner );
extern YY_BUFFER_STATE obsql_oracle_gbk_yy_scan_bytes (yyconst char *bytes,int len ,yyscan_t yyscanner );
extern void obsql_oracle_gbk_yy_switch_to_buffer (YY_BUFFER_STATE new_buffer ,yyscan_t yyscanner );
extern void obsql_oracle_gbk_yy_delete_buffer (YY_BUFFER_STATE b ,yyscan_t yyscanner );
#endif
int parse_init(ParseResult *p)
{
  int ret = 0;  // can not include C++ file "ob_define.h"
  static __thread char error_msg[MAX_ERROR_MSG] = {'\0'};
  p->error_msg_ = error_msg;
  if (OB_UNLIKELY(NULL == p || NULL == p->malloc_pool_)) {
    ret = OB_PARSER_ERR_UNEXPECTED;
    if (NULL != p) {
      (void)snprintf(p->error_msg_, MAX_ERROR_MSG, "malloc_pool_ must be set");
    }
  }

  if (OB_LIKELY( 0 == ret)) {
#ifdef OB_BUILD_ORACLE_PARSER
    if (IS_ORACLE_COMPATIBLE) {
      switch (p->connection_collation_) {
        case 28/*CS_TYPE_GBK_CHINESE_CI*/:
        case 87/*CS_TYPE_GBK_BIN*/:
        case 216/*CS_TYPE_GB18030_2022_BIN*/:
        case 217/*CS_TYPE_GB18030_2022_PINYIN_CI*/:
        case 218/*CS_TYPE_GB18030_2022_PINYIN_CS*/:
        case 219/*CS_TYPE_GB18030_2022_RADICAL_CI*/:
        case 220/*CS_TYPE_GB18030_2022_RADICAL_CS*/:
        case 221/*CS_TYPE_GB18030_2022_STROKE_CI*/:
        case 222/*CS_TYPE_GB18030_2022_STROKE_CS*/:
        case 248/*CS_TYPE_GB18030_CHINESE_CI*/:
        case 249/*CS_TYPE_GB18030_BIN*/:
          ret = obsql_oracle_gbk_yylex_init_extra(p, &(p->yyscan_info_));
          break;
        case 45/*CS_TYPE_UTF8MB4_GENERAL_CI*/:
        case 46/*CS_TYPE_UTF8MB4_BIN*/:
        case 63/*CS_TYPE_BINARY*/:
        case 224/*CS_TYPE_UTF8MB4_UNICODE_CI*/:
          ret = obsql_oracle_utf8_yylex_init_extra(p, &(p->yyscan_info_));
          break;
        case 8/*CS_TYPE_LATIN1_SWEDISH_CI*/:
        case 47/*CS_TYPE_LATIN1_BIN*/:
          ret = obsql_oracle_latin1_yylex_init_extra(p, &(p->yyscan_info_));
          break;
        default: {
          ret = -1;
          (void)snprintf(p->error_msg_, MAX_ERROR_MSG, "get not support connection collation: %u",
                                                                          p->connection_collation_);
          break;
        }
      }
    } else {
#endif
      ret = obsql_mysql_yylex_init_extra(p, &(p->yyscan_info_));
#ifdef OB_BUILD_ORACLE_PARSER
    }
#endif
#define	ENOMEM		12	/* Out of memory */
    //refine parser error code to OB error code
    if (0 == ret) {
      ret = OB_PARSER_SUCCESS;
    } else if (ENOMEM == ret) {
      ret = OB_PARSER_ERR_NO_MEMORY;
    } else {
      ret = OB_PARSER_ERR_PARSE_SQL;
    }
  }
  return ret;
}

int parse_terminate(ParseResult *p)
{
  int ret = 0;
  if (OB_LIKELY(NULL != p->yyscan_info_)) {
#ifdef OB_BUILD_ORACLE_PARSER
    if (IS_ORACLE_COMPATIBLE) {
      switch (p->connection_collation_) {
        case 28/*CS_TYPE_GBK_CHINESE_CI*/:
        case 87/*CS_TYPE_GBK_BIN*/:
        case 216/*CS_TYPE_GB18030_2022_BIN*/:
        case 217/*CS_TYPE_GB18030_2022_PINYIN_CI*/:
        case 218/*CS_TYPE_GB18030_2022_PINYIN_CS*/:
        case 219/*CS_TYPE_GB18030_2022_RADICAL_CI*/:
        case 220/*CS_TYPE_GB18030_2022_RADICAL_CS*/:
        case 221/*CS_TYPE_GB18030_2022_STROKE_CI*/:
        case 222/*CS_TYPE_GB18030_2022_STROKE_CS*/:
        case 248/*CS_TYPE_GB18030_CHINESE_CI*/:
        case 249/*CS_TYPE_GB18030_BIN*/:
          ret = obsql_oracle_gbk_yylex_destroy(p->yyscan_info_);
          break;
        case 45/*CS_TYPE_UTF8MB4_GENERAL_CI*/:
        case 46/*CS_TYPE_UTF8MB4_BIN*/:
        case 63/*CS_TYPE_BINARY*/:
        case 224/*CS_TYPE_UTF8MB4_UNICODE_CI*/:
          ret = obsql_oracle_utf8_yylex_destroy(p->yyscan_info_);
          break;
        case 8/*CS_TYPE_LATIN1_SWEDISH_CI*/:
        case 47/*CS_TYPE_LATIN1_BIN*/:
          ret = obsql_oracle_latin1_yylex_destroy(p->yyscan_info_);
          break;
        default: {
          ret = -1;
          (void)snprintf(p->error_msg_, MAX_ERROR_MSG, "get not support connection collation: %u",
                                                                          p->connection_collation_);
          break;
        }
      }
    } else {
#endif
      ret = obsql_mysql_yylex_destroy(p->yyscan_info_);
#ifdef OB_BUILD_ORACLE_PARSER
    }
#endif
  }
  return ret;
}

int parse_sql(ParseResult *p, const char *buf, size_t input_len)
{
  int ret = OB_PARSER_ERR_PARSE_SQL;
  if (OB_UNLIKELY(NULL == p)) {
  } else if (OB_UNLIKELY(NULL == buf || input_len <= 0 || input_len > INT32_MAX)) {
    snprintf(p->error_msg_, MAX_ERROR_MSG, "Input SQL can not be empty");
    ret = OB_PARSER_ERR_EMPTY_QUERY;
  } else {
    const int32_t len = (int32_t)input_len;
    p->input_sql_ = buf;
    p->input_sql_len_ = len;
    p->start_col_ = 1;
    p->end_col_ = 1;
    p->line_ = 1;
    p->yycolumn_ = 1;
    p->yylineno_ = 1;
#ifdef SQL_PARSER_COMPILATION
    p->realloc_cnt_ = p->realloc_cnt_ <= 0 ? 10 : p->realloc_cnt_;
    p->comment_cap_ = p->realloc_cnt_;
    p->comment_cnt_ = 0;
    p->stop_add_comment_ = false;
#endif
    if (false == p->pl_parse_info_.is_pl_parse_ && !p->is_for_udr_) {//如果是PLParse调用的该接口，不去重置
      p->question_mark_ctx_.count_ = 0;
    }

    // 删除SQL语句末尾的空格 (外层已做)
    // while (len > 0 && ISSPACE(buf[len - 1])) {
    //  --len;
    // }

    //if (OB_UNLIKELY(len <= 0)) {
    // (void)snprintf(p->error_msg_, MAX_ERROR_MSG, "Input SQL can not be white space only");
    //  ret = OB_PARSER_ERR_EMPTY_QUERY;
  //  } else {
    {
      static __thread jmp_buf jmp_buf_;
      p->jmp_buf_ = &jmp_buf_;
      int val = setjmp(*(p->jmp_buf_));
      if (val) {
        if (p->extra_errno_ == OB_PARSER_ERR_NO_MEMORY) {
          // for error other than NO_MEMORY, we return OB_PARSER_ERR_PARSE_SQL to avoid lost connection.
          ret = OB_PARSER_ERR_NO_MEMORY;
        } else {
          ret = OB_PARSER_ERR_PARSE_SQL;
        }
#ifdef SQL_PARSER_COMPILATION
      } else if (OB_ISNULL(p->comment_list_ = (TokenPosInfo*)(parse_malloc(
                                                p->realloc_cnt_ * sizeof(TokenPosInfo),
                                                p->malloc_pool_)))) {
        ret = OB_PARSER_ERR_NO_MEMORY;
#endif
      } else {
#ifdef OB_BUILD_ORACLE_PARSER
        if (IS_ORACLE_COMPATIBLE) {
          switch (p->connection_collation_) {
            case 28/*CS_TYPE_GBK_CHINESE_CI*/:
            case 87/*CS_TYPE_GBK_BIN*/:
            case 216/*CS_TYPE_GB18030_2022_BIN*/:
            case 217/*CS_TYPE_GB18030_2022_PINYIN_CI*/:
            case 218/*CS_TYPE_GB18030_2022_PINYIN_CS*/:
            case 219/*CS_TYPE_GB18030_2022_RADICAL_CI*/:
            case 220/*CS_TYPE_GB18030_2022_RADICAL_CS*/:
            case 221/*CS_TYPE_GB18030_2022_STROKE_CI*/:
            case 222/*CS_TYPE_GB18030_2022_STROKE_CS*/:
            case 248/*CS_TYPE_GB18030_CHINESE_CI*/:
            case 249/*CS_TYPE_GB18030_BIN*/: {
              YY_BUFFER_STATE bp = obsql_oracle_gbk_yy_scan_bytes(buf, len, p->yyscan_info_);
              obsql_oracle_gbk_yy_switch_to_buffer(bp, p->yyscan_info_);
              int tmp_ret = -1;
              if (p->is_fp_) {
                tmp_ret = obsql_oracle_gbk_fast_parse(p);
              } else if (p->is_multi_query_) {
                tmp_ret = obsql_oracle_gbk_multi_fast_parse(p);
              } else if (p->is_multi_values_parser_) {
                tmp_ret = obsql_oracle_gbk_multi_values_parse(p);
              } else {
                tmp_ret = obsql_oracle_gbk_yyparse(p);
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
              obsql_oracle_gbk_yy_delete_buffer(bp, p->yyscan_info_);
              break;
            }
            case 45/*CS_TYPE_UTF8MB4_GENERAL_CI*/:
            case 46/*CS_TYPE_UTF8MB4_BIN*/:
            case 63/*CS_TYPE_BINARY*/:
            case 224/*CS_TYPE_UTF8MB4_UNICODE_CI*/:{
              YY_BUFFER_STATE bp = obsql_oracle_utf8_yy_scan_bytes(buf, len, p->yyscan_info_);
              obsql_oracle_utf8_yy_switch_to_buffer(bp, p->yyscan_info_);
              int tmp_ret = -1;
              if (p->is_fp_) {
                tmp_ret = obsql_oracle_utf8_fast_parse(p);
              } else if (p->is_multi_query_) {
                tmp_ret = obsql_oracle_utf8_multi_fast_parse(p);
              } else if (p->is_multi_values_parser_) {
                tmp_ret = obsql_oracle_utf8_multi_values_parse(p);
              } else {
                tmp_ret = obsql_oracle_utf8_yyparse(p);
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
              obsql_oracle_utf8_yy_delete_buffer(bp, p->yyscan_info_);
              break;
            }
            case 8/*CS_TYPE_LATIN1_SWEDISH_CI*/:
            case 47/*CS_TYPE_LATIN1_BIN*/:{
              YY_BUFFER_STATE bp = obsql_oracle_latin1_yy_scan_bytes(buf, len, p->yyscan_info_);
              obsql_oracle_latin1_yy_switch_to_buffer(bp, p->yyscan_info_);
              int tmp_ret = -1;
              if (p->is_fp_) {
                tmp_ret = obsql_oracle_latin1_fast_parse(p);
              } else if (p->is_multi_query_) {
                tmp_ret = obsql_oracle_latin1_multi_fast_parse(p);
              } else if (p->is_multi_values_parser_) {
                tmp_ret = obsql_oracle_latin1_multi_values_parse(p);
              } else {
                tmp_ret = obsql_oracle_latin1_yyparse(p);
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
              obsql_oracle_latin1_yy_delete_buffer(bp, p->yyscan_info_);
              break;
            }
            default: {
              ret = OB_PARSER_ERR_UNEXPECTED;
              (void)snprintf(p->error_msg_, MAX_ERROR_MSG, "get not support conn collation: %u",
                                                                          p->connection_collation_);
              break;
            }
          }
#endif
#ifdef OB_BUILD_ORACLE_PARSER
        } else {
#endif
          YY_BUFFER_STATE bp = obsql_mysql_yy_scan_bytes(buf, len, p->yyscan_info_);
          obsql_mysql_yy_switch_to_buffer(bp, p->yyscan_info_);
          int tmp_ret = -1;
          if (p->is_fp_) {
            tmp_ret = obsql_mysql_fast_parse(p);
          } else if (p->is_multi_query_) {
            tmp_ret = obsql_mysql_multi_fast_parse(p);
          } else if (p->is_multi_values_parser_) {
            tmp_ret = obsql_mysql_multi_values_parse(p);
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
#ifdef OB_BUILD_ORACLE_PARSER
        }
#endif
      }
    }
  }
  return ret;
}

int parse_sql_stmt(ParseResult *parse_result)
{
  int ret = -1;
  if (0 != (ret = parse_init(parse_result))) {
    // fix bug #16298144
    // fprintf(stderr, "init parse result failed, ret=%d\n", ret);
  } else if (0 != (ret = parse_sql(parse_result, parse_result->input_sql_, parse_result->input_sql_len_))) {
    // fix bug #16298144
    // fprintf(stderr, "parse sql failed, ret=%d, input_sql=%.*s\n", ret, parse_result->input_sql_len_, parse_result->input_sql_);
  }
  return ret;
}

void setup_token_pos_info(ParseNode *node, int off, int len)
{
#ifdef SQL_PARSER_COMPILATION
  if (node != NULL) {
    node->token_off_ = off;
    node->token_len_ = len;
  }
#else
  // do nothing
#endif
}

int setup_token_pos_info_and_dup_string(ParseNode *node,
                                        ParseResult *result,
                                        int start,
                                        int end)
{
  int ret = OB_PARSER_SUCCESS;
#ifdef SQL_PARSER_COMPILATION
  node->token_off_ = start - 1;
  node->token_len_ = end - start + 1;
  if (start > end) {
    ret = OB_PARSER_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY((NULL == (node->str_value_ = copy_expr_string(result,
                                                                       start,
                                                                       end))))) {
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
int add_comment_list(ParseResult *p, const TokenPosInfo *info)
{
  int ret = OB_PARSER_SUCCESS;
  if (OB_ISNULL(p) || OB_ISNULL(info)) {
    ret = OB_PARSER_ERR_UNEXPECTED;
  }
  if (OB_PARSER_SUCCESS == ret && p->comment_cnt_ + 1 >= p->comment_cap_) {
    int alloc_cnt = p->comment_cnt_ + p->realloc_cnt_;
    char *buf = parse_malloc(sizeof(TokenPosInfo) * alloc_cnt, p->malloc_pool_);
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

void REPUT_NEG_SIGN(ParseResult *p)
{
  if (p->minus_ctx_.has_minus_) {
    int start_pos = p->no_param_sql_len_;
    int end_pos = p->minus_ctx_.pos_;
    for (; start_pos > end_pos; start_pos--) {
      p->no_param_sql_[start_pos] = p->no_param_sql_[start_pos - 1];
    }
    p->no_param_sql_[end_pos] = '-';
    p->no_param_sql_[++p->no_param_sql_len_] = '\0';
    p->minus_ctx_.pos_ = -1;
    p->minus_ctx_.raw_sql_offset_ = -1;
    p->minus_ctx_.has_minus_ = false;
  }
}

int STORE_PARAM_NODE_NEED_PARAMETERIZE(ParamList *param,
    struct _ParseNode *node, ParseResult *p, const int first_column)
{
  if (p->minus_ctx_.has_minus_ && p->minus_ctx_.is_cur_numeric_) {
    p->no_param_sql_[p->minus_ctx_.pos_] = '?';
    p->no_param_sql_len_ = p->minus_ctx_.pos_ + 1;
  } else {
    REPUT_NEG_SIGN(p);
    p->no_param_sql_[p->no_param_sql_len_++] = '?';
  }
  p->no_param_sql_[p->no_param_sql_len_] = '\0';
  node->pos_ = p->no_param_sql_len_ - 1;
  node->raw_sql_offset_ = (p->minus_ctx_.has_minus_
      && p->minus_ctx_.is_cur_numeric_) ?
          p->minus_ctx_.raw_sql_offset_ : first_column - 1;
  param->node_ = node;
  param->next_ = NULL;
  if (NULL == p->param_nodes_) {
    p->param_nodes_ = param;
  } else {
    p->tail_param_node_->next_ = param;
  }
  p->tail_param_node_ = param;
  p->param_node_num_++;
  p->token_num_++;
  p->minus_ctx_.has_minus_ = false;
  p->minus_ctx_.pos_ = -1;
  p->minus_ctx_.is_cur_numeric_ = false;
  p->minus_ctx_.raw_sql_offset_ = -1;
  return 0;
}

int add_alias_name(ParseNode *node, ParseResult *result, int end)
{
  int ret = OB_PARSER_SUCCESS;
  if (NULL == node || NULL == result || NULL == node->str_value_) {
    ret = OB_PARSER_ERR_UNEXPECTED;
  } else {
    int need_len = strlen(" as ") + node->str_len_ + 2 * strlen("\"");
    if (end > result->pl_parse_info_.last_pl_symbol_pos_) {
      need_len += end - result->pl_parse_info_.last_pl_symbol_pos_;
    }
    for (int i = 0; i < node->str_len_; ++i) {
      if ('\"' == node->str_value_[i] && !(i > 0 && '\\' == node->str_value_[i - 1])) {
        need_len++;
      }
    }
    if (result->no_param_sql_len_ + need_len >= result->no_param_sql_buf_len_) {
      char *buf = parse_malloc(result->no_param_sql_buf_len_ * 2, result->malloc_pool_);
      if (OB_UNLIKELY(NULL == buf)) {
        ret = OB_PARSER_ERR_NO_MEMORY;
      } else {
        memmove(buf, result->no_param_sql_, result->no_param_sql_len_);
        result->no_param_sql_ = buf;
        result->no_param_sql_buf_len_ = result->no_param_sql_buf_len_ * 2;
      }
    }
    if (OB_PARSER_SUCCESS == ret) {
      if (end > result->pl_parse_info_.last_pl_symbol_pos_) {
        memmove(result->no_param_sql_ + result->no_param_sql_len_, result->input_sql_ + result->pl_parse_info_.last_pl_symbol_pos_, end - result->pl_parse_info_.last_pl_symbol_pos_);
        result->no_param_sql_len_ += end - result->pl_parse_info_.last_pl_symbol_pos_;
        result->pl_parse_info_.last_pl_symbol_pos_ = end;
      }
      result->no_param_sql_len_ += sprintf(result->no_param_sql_ + result->no_param_sql_len_, "%.*s", (int)strlen(" as "), " as ");
      result->no_param_sql_len_ += sprintf(result->no_param_sql_ + result->no_param_sql_len_, "%.*s", (int)strlen("\""), "\"");
      int index1 = 0;
      int index2 = 0;
      char *trans_buf = parse_malloc((int)node->str_len_ * 2, result->malloc_pool_);
      if (OB_UNLIKELY(NULL == trans_buf)) {
        ret = OB_PARSER_ERR_NO_MEMORY;
      } else {
        bool is_ansi_quotes = false;
        IS_ANSI_QUOTES(result->sql_mode_, is_ansi_quotes);
        for (; index1 < node->str_len_; index1++) {
          if (is_ansi_quotes && '\"' == node->str_value_[index1]) {
            //do nothing, in mysql ansi quotes sql mode: " <==> `, just skip
          } else if ('\"' == node->str_value_[index1] && !(index1 > 0 && '\\' == node->str_value_[index1 - 1])) {
            trans_buf[index2++] = '\\';
            trans_buf[index2++] = '\"';
          } else {
            trans_buf[index2++] = node->str_value_[index1];
          }
        }
        result->no_param_sql_len_ += sprintf(result->no_param_sql_ + result->no_param_sql_len_, "%.*s", index2, trans_buf);
        result->no_param_sql_len_ += sprintf(result->no_param_sql_ + result->no_param_sql_len_, "%.*s", (int)strlen("\""), "\"");
      }
    }
  }
  return ret;
}
