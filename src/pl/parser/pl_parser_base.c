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

#include "pl/parser/pl_parser_base.h"

#define yyconst const
typedef void* yyscan_t;
#define YY_EXTRA_TYPE void *
typedef struct yy_buffer_state *YY_BUFFER_STATE;

#ifdef OB_BUILD_ORACLE_PL
extern YY_BUFFER_STATE obpl_oracle_yy_scan_bytes (yyconst char *bytes,int len ,yyscan_t yyscanner );
extern void obpl_oracle_yy_switch_to_buffer (YY_BUFFER_STATE new_buffer ,yyscan_t yyscanner );
extern void obpl_oracle_yy_delete_buffer (YY_BUFFER_STATE b ,yyscan_t yyscanner );
extern int obpl_oracle_yylex_init_extra (YY_EXTRA_TYPE user_defined,yyscan_t* scanner);
extern int obpl_oracle_yyparse(ObParseCtx *parse_ctx);
#define IS_ORACLE_COMPATIBLE (1/*ORACLE_MODE*/ == parse_ctx->comp_mode_)
#endif

extern YY_BUFFER_STATE obpl_mysql_yy_scan_bytes (yyconst char *bytes,int len ,yyscan_t yyscanner );
extern void obpl_mysql_yy_switch_to_buffer (YY_BUFFER_STATE new_buffer ,yyscan_t yyscanner );
extern void obpl_mysql_yy_delete_buffer (YY_BUFFER_STATE b ,yyscan_t yyscanner );
extern int obpl_mysql_yylex_init_extra (YY_EXTRA_TYPE user_defined,yyscan_t* scanner);
extern int obpl_mysql_yyparse(ObParseCtx *parse_ctx);

int obpl_parser_init(ObParseCtx *parse_ctx)
{
  int ret = 0;
  if (NULL_PTR(parse_ctx) || NULL_PTR(parse_ctx->mem_pool_)) {
    ret = -1;
  } else {
#ifndef OB_BUILD_ORACLE_PL
    ret = obpl_mysql_yylex_init_extra(parse_ctx, &(parse_ctx->scanner_ctx_.yyscan_info_));
#else
    ret = IS_ORACLE_COMPATIBLE ?
        obpl_oracle_yylex_init_extra(parse_ctx, &(parse_ctx->scanner_ctx_.yyscan_info_))
        : obpl_mysql_yylex_init_extra(parse_ctx, &(parse_ctx->scanner_ctx_.yyscan_info_));
#endif
  }
  return ret;
}

int obpl_parser_parse(ObParseCtx *parse_ctx)
{
  int ret;
  if (NULL_PTR(parse_ctx) || NULL_PTR(parse_ctx->stmt_str_) || OB_UNLIKELY(parse_ctx->stmt_len_ <= 0)) {
    ret = OB_PARSER_ERR_EMPTY_QUERY;
  } else {
    int val = setjmp(parse_ctx->jmp_buf_);
    if (val) {
      ret = parse_ctx->global_errno_;
    } else {
      ret = OB_PARSER_SUCCESS;
#ifdef IS_ORACLE_COMPATIBLE
      YY_BUFFER_STATE bp = (IS_ORACLE_COMPATIBLE ?
          obpl_oracle_yy_scan_bytes(parse_ctx->stmt_str_, parse_ctx->stmt_len_, parse_ctx->scanner_ctx_.yyscan_info_)
          : obpl_mysql_yy_scan_bytes(parse_ctx->stmt_str_, parse_ctx->stmt_len_, parse_ctx->scanner_ctx_.yyscan_info_));
      IS_ORACLE_COMPATIBLE ?
          obpl_oracle_yy_switch_to_buffer(bp, parse_ctx->scanner_ctx_.yyscan_info_)
          : obpl_mysql_yy_switch_to_buffer(bp, parse_ctx->scanner_ctx_.yyscan_info_);

      if (0 != (IS_ORACLE_COMPATIBLE ? obpl_oracle_yyparse(parse_ctx) : obpl_mysql_yyparse(parse_ctx))) {
        ret = OB_PARSER_ERR_PARSE_SQL;
      }
      IS_ORACLE_COMPATIBLE ?
          obpl_oracle_yy_delete_buffer(bp, parse_ctx->scanner_ctx_.yyscan_info_)
          : obpl_mysql_yy_delete_buffer(bp, parse_ctx->scanner_ctx_.yyscan_info_);
#else
      YY_BUFFER_STATE bp =
          obpl_mysql_yy_scan_bytes(parse_ctx->stmt_str_, parse_ctx->stmt_len_, parse_ctx->scanner_ctx_.yyscan_info_);
      obpl_mysql_yy_switch_to_buffer(bp, parse_ctx->scanner_ctx_.yyscan_info_);
      if (0 != obpl_mysql_yyparse(parse_ctx)) {
        ret = OB_PARSER_ERR_PARSE_SQL;
      }
      obpl_mysql_yy_delete_buffer(bp, parse_ctx->scanner_ctx_.yyscan_info_);
#endif
    }
  }
  return ret;
}
