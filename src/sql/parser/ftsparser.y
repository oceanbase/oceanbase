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

%parse-param {FtsParserResult *ftsParserResult}
%define api.pure
%name-prefix "obsql_fts_yy"

%{
#include <stdint.h>
#include "ftsparser_tab.h"
#include "fts_base.h"
extern void obsql_fts_yyerror(void *yylloc, FtsParserResult *ftsParserResult, const char *s);
extern  int obsql_fts_yylex(YYSTYPE*, YYLTYPE*, void *);
#define YYLEX_PARAM ((FtsParserResult *)ftsParserResult)->yyscanner_
#define YYERROR_VERBOSE
typedef struct FtsNode FtsNode;
%}

%locations
%union {
  int oper;
  struct FtsString *token;
  struct FtsNode *node;
}

%token<oper>    FTS_OPER
%token<token>   FTS_TERM FTS_NUMB

%type<node>     prefix term expr sub_expr expr_lst query
%nonassoc       '+' '-' '~' '<' '>'


%%
query   : expr_lst      {
                $$ = $1;
                if (FTS_OK != ((FtsParserResult*) ftsParserResult)->ret_) {
                        // do nothing
                } else {
                        ((FtsParserResult*) ftsParserResult)->root_ = $$;
                }
        }
        ;

expr_lst: /* Empty */   {
                $$ = NULL;
        }

        | expr_lst expr {
                $$ = $1;
                FtsNode *node = NULL;
                if (FTS_OK != ((FtsParserResult*) ftsParserResult)->ret_) {
                        // do nothing
                } else if (!$$ && $2) {
                        int ret = fts_create_node_list(ftsParserResult, $2, &node);
                        if (FTS_OK == ret) {
                          $$ = node;
                        } else {
                          ((FtsParserResult*) ftsParserResult)->ret_ = ret;
                        }
                } else if ($$) {
                        ((FtsParserResult*) ftsParserResult)->ret_ = fts_add_node($$, $2);
                }
        }

        | expr_lst sub_expr {
                $$ = $1;
                FtsNode *node = NULL;
                if (FTS_OK != ((FtsParserResult*) ftsParserResult)->ret_) {
                        // do nothing
                } else if (!$$ && $2) {
                        int ret = fts_create_node_list(ftsParserResult, $2, &node);
                        if (FTS_OK == ret) {
                          $$ = node;
                        } else {
                          ((FtsParserResult*) ftsParserResult)->ret_ = ret;
                        }
                } else if ($$) {
                        ((FtsParserResult*) ftsParserResult)->ret_ = fts_add_node($$, $2);
                }
        }
        ;


sub_expr: '(' expr_lst ')'              {
                $$ = $2;
                if (FTS_OK != ((FtsParserResult*) ftsParserResult)->ret_) {
                        // do nothing
                } else if ($$) {
                        FtsNode *node = NULL;
                        int ret = fts_create_node_subexp_list(ftsParserResult, $$, &node);
                        if (FTS_OK == ret) {
                          $$ = node;
                        } else {
                          ((FtsParserResult*) ftsParserResult)->ret_ = ret;
                        }
                }
        }

        | prefix '(' expr_lst ')'       {
                if (FTS_OK != ((FtsParserResult*) ftsParserResult)->ret_) {
                        // do nothing
                } else {
                        FtsNode *node = NULL;
                        int ret = fts_create_node_list(ftsParserResult, $1, &node);
                        if (FTS_OK == ret) {
                          $$ = node;
                        } else {
                          ((FtsParserResult*) ftsParserResult)->ret_ = ret;
                        }
                        if (FTS_OK == ret && $3) {
                          FtsNode *tmp_node = NULL;
                          ret = fts_create_node_subexp_list(ftsParserResult, $3, &tmp_node);
                          if (FTS_OK == ret) {
                            ((FtsParserResult*) ftsParserResult)->ret_ = fts_add_node($$, tmp_node);
                          } else {
                            ((FtsParserResult*) ftsParserResult)->ret_ = ret;
                          }
                        } else {
                          $$ = NULL;
                        }
                }
        }
        ;

expr    : term          {
                $$ = $1;
        }

        | prefix term   {
                if (FTS_OK != ((FtsParserResult*) ftsParserResult)->ret_) {
                        // do nothing
                } else if ($1 != NULL) {
                  FtsNode *node = NULL;
                  int ret = fts_create_node_list(ftsParserResult, $1, &node);
                  if (FTS_OK == ret) {
                    $$ = node;
                    ((FtsParserResult*) ftsParserResult)->ret_ = fts_add_node($$, $2);
                  } else {
                    ((FtsParserResult*) ftsParserResult)->ret_ = ret;
                  }
                }
        }
        ;

prefix  : '-'           {
                if (FTS_OK != ((FtsParserResult*) ftsParserResult)->ret_) {
                        // do nothing
                } else {
                        FtsNode *node = NULL;
                        int ret = fts_create_node_oper(ftsParserResult, FTS_IGNORE, &node);
                        if (FTS_OK == ret) {
                          $$ = node;
                        } else {
                          ((FtsParserResult*) ftsParserResult)->ret_ = ret;
                        }
                }
        }

        | '+'           {
                if (FTS_OK != ((FtsParserResult*) ftsParserResult)->ret_) {
                        // do nothing
                } else {
                        FtsNode *node = NULL;
                        int ret = fts_create_node_oper(ftsParserResult, FTS_EXIST, &node);
                        if (FTS_OK == ret) {
                          $$ = node;
                        } else {
                          ((FtsParserResult*) ftsParserResult)->ret_ = ret;
                        }
                }
        }

        | '~'           {
                yyerror(NULL, (FtsParserResult*) ftsParserResult, "syntax error: unsupport '~'");
                YYABORT;
                $$ = NULL;
        }
        | '<'           {
                yyerror(NULL, (FtsParserResult*) ftsParserResult, "syntax error: unsupport '<'");
                YYABORT;
                $$ = NULL;
        }

        | '>'           {
                yyerror(NULL, (FtsParserResult*) ftsParserResult, "syntax error: unsupport '>'");
                YYABORT;
                $$ = NULL;
        }
        | '@'           {
                yyerror(NULL, (FtsParserResult*) ftsParserResult, "syntax error: unsupport '@'");
                YYABORT;
                $$ = NULL;
        }
        | '*'           {
                yyerror(NULL, (FtsParserResult*) ftsParserResult, "syntax error: unsupport '*'");
                YYABORT;
                $$ = NULL;
        }
        | '%'           {
                yyerror(NULL, (FtsParserResult*) ftsParserResult, "syntax error: unsupport '%'");
                YYABORT;
                $$ = NULL;
        }
        ;

term    : FTS_TERM      {
                if (FTS_OK != ((FtsParserResult*) ftsParserResult)->ret_) {
                        // do nothing
                } else {
                        FtsNode *node = NULL;
                        int ret = fts_create_node_term(ftsParserResult, $1, &node);
                        if (FTS_OK == ret) {
                          $$ = node;
                        } else {
                          ((FtsParserResult*) ftsParserResult)->ret_ = ret;
                          $$ = NULL;
                        }
                }
        }
        ;

%%
void yyerror(void *yylloc, FtsParserResult *ftsParserResult, const char *s) {
  size_t length = strlen(s);
  char *ptr_ret = (char *)parse_malloc(length + 1, ftsParserResult->malloc_pool_);
  if (NULL == ptr_ret && FTS_OK != ftsParserResult->ret_) {
    // do nothing
  } else if (NULL == ptr_ret) {
    ftsParserResult->ret_ = FTS_ERROR_MEMORY;
  } else {
    ftsParserResult->ret_ = FTS_ERROR_SYNTAX;
    memcpy(ptr_ret, s, length);
    ptr_ret[length] = '\0';
    ftsParserResult->err_info_.str_ = ptr_ret;
    ftsParserResult->err_info_.len_ = length;
  }
}