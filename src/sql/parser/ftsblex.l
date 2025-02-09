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

%option noyywrap nounput noinput case-insensitive
%option noyyalloc noyyrealloc noyyfree
%option reentrant bison-bridge bison-locations
%option prefix="obsql_fts_yy"
%option header-file="ftsblex_lex.h"
%top{
#include "ftsparser_tab.h" // 包含 Yacc 生成的头文件
#include "fts_base.h"

}

%{
#include "ftsparser_tab.h" // 包含 Yacc 生成的头文件
#include "fts_base.h"

extern int yyparse();
%}

%%
[\t \n]+  /* Ignore whitespace */ ;

[()+\-<>~@*]            {
        yylval_param->oper = obsql_fts_yyget_text(yyscanner)[0];

        return(yylval_param->oper);
}

[^" \n*()+\-<>~@]*     {
        FtsParserResult *p = yyget_extra(yyscanner);
        struct FtsString *ast_str = NULL;
        int ret = fts_string_create(p, (const char*)(obsql_fts_yyget_text(yyscanner)), obsql_fts_yyget_leng(yyscanner), &ast_str);
        if (FTS_OK != ret) {
          p->ret_ = ret;
        } else {
          yylval_param->token = ast_str;
        }
        return(FTS_TERM);
}

%%

void *yyalloc(size_t bytes, void *yyscanner)
{
  void *ptr_ret = NULL;
  FtsParserResult *p = yyget_extra(yyscanner);
  if (OB_UNLIKELY(NULL == p)) {
  } else {
    ptr_ret = parse_malloc(bytes, p->malloc_pool_);
  }
  return ptr_ret;
}

void *yyrealloc(void *ptr, size_t bytes, void *yyscanner)
{
  void *ptr_ret = NULL;
  FtsParserResult *p = yyget_extra(yyscanner);
  if (OB_UNLIKELY(NULL == p)) {
  } else {
    ptr_ret = parse_realloc(ptr, bytes, p->malloc_pool_);
  }
  return ptr_ret;
}

void yyfree(void *ptr, void *yyscanner)
{
  UNUSED(yyscanner);
  /* Do nothing -- we leave it to the garbage collector. */
  parse_free(ptr);
}
