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

#include "dbms_sched_parser_base.h"

#define yyconst const
typedef void* yyscan_t;
#define YY_EXTRA_TYPE void *
typedef struct yy_buffer_state *YY_BUFFER_STATE;

extern YY_BUFFER_STATE ob_dbms_sched_calendar_yy_scan_bytes (yyconst char *bytes,int len ,yyscan_t yyscanner );
extern void ob_dbms_sched_calendar_yy_switch_to_buffer (YY_BUFFER_STATE new_buffer ,yyscan_t yyscanner );
extern void ob_dbms_sched_calendar_yy_delete_buffer (YY_BUFFER_STATE b ,yyscan_t yyscanner );
extern int ob_dbms_sched_calendar_yylex_init_extra (YY_EXTRA_TYPE user_defined,yyscan_t* scanner);
extern int ob_dbms_sched_calendar_yyparse(ObParseCtx *parse_ctx);

int ob_dbms_sched_parser_calendar_init(ObParseCtx *parse_ctx)
{
  int ret = 0;
  if (NULL_PTR(parse_ctx) || NULL_PTR(parse_ctx->mem_pool_)) {
    ret = -1;
  } else {
    ret = ob_dbms_sched_calendar_yylex_init_extra(parse_ctx, &(parse_ctx->scanner_ctx_.yyscan_info_));
  }
  return ret;
}

int ob_dbms_sched_parser_calendar_parse(ObParseCtx *parse_ctx)
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
      YY_BUFFER_STATE bp = ob_dbms_sched_calendar_yy_scan_bytes(parse_ctx->stmt_str_, parse_ctx->stmt_len_, parse_ctx->scanner_ctx_.yyscan_info_);
       ob_dbms_sched_calendar_yy_switch_to_buffer(bp, parse_ctx->scanner_ctx_.yyscan_info_);

      if (0 != ob_dbms_sched_calendar_yyparse(parse_ctx)) {
        ret = OB_PARSER_ERR_PARSE_SQL;
      }
      ob_dbms_sched_calendar_yy_delete_buffer(bp, parse_ctx->scanner_ctx_.yyscan_info_);
    }
  }
  return ret;
}
