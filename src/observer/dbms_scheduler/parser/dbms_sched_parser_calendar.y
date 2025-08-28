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

//first: declare
%define api.pure
%parse-param {ObParseCtx *parse_ctx}
%name-prefix "ob_dbms_sched_calendar_yy"
%locations
//%no-lines
%verbose
%error-verbose

%{
#include <stdint.h>
#define YYDEBUG 1
%}

%union {
  struct _ParseNode *node;
  const struct _NonReservedKeyword *non_reserved_keyword;
  int64_t ival;
}

%code requires {
#ifndef YYLTYPE_IS_DECLARED
#define YYLTYPE_IS_DECLARED 1
typedef struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
  int abs_first_column;
  int abs_last_column;
} YYLTYPE;
#endif /*YYLTYPE_IS_DECLARED*/

# define YYLLOC_DEFAULT(Current, Rhs, N)                        \
  do                                                            \
    if (YYID (N)) {                                             \
      (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;    \
      (Current).first_column = YYRHSLOC (Rhs, 1).first_column;  \
      (Current).last_line    = YYRHSLOC (Rhs, N).last_line;     \
      (Current).last_column  = YYRHSLOC (Rhs, N).last_column;   \
      (Current).abs_first_column = YYRHSLOC (Rhs, 1).abs_first_column;     \
      (Current).abs_last_column  = YYRHSLOC (Rhs, N).abs_last_column;      \
    } else {                                                    \
      (Current).first_line   = (Current).last_line   =          \
        YYRHSLOC (Rhs, 0).last_line;                            \
      (Current).first_column = (Current).last_column =          \
        YYRHSLOC (Rhs, 0).last_column;                          \
      (Current).abs_first_column = (Current).abs_last_column =          \
        YYRHSLOC (Rhs, 0).abs_last_column;                              \
    }                                                           \
  while (YYID (0))
}

%{
#include "dbms_sched_parser_calendar_lex.h"
#include "dbms_sched_parser_base.h"

typedef struct _YYLookaheadToken
{
  int *la_yychar;
  /* The semantic value of the lookahead symbol.  */
  YYSTYPE *la_yylval;
  /* Location data for the lookahead symbol.  */
  YYLTYPE *la_yylloc;
} YYLookaheadToken;

extern void ob_dbms_sched_calendar_yyerror(YYLTYPE *yylloc, ObParseCtx *parse_ctx, char *s);
extern void ob_dbms_sched_calendar_parse_fatal_error(int32_t errcode, yyscan_t yyscanner, yyconst char *msg, ...);

#define YY_FATAL_ERROR(msg, args...) (ob_dbms_sched_calendar_parse_fatal_error(OB_PARSER_ERR_NO_MEMORY, YYLEX_PARAM, msg, ##args))
%}

/*these tokens can't be used for obj names*/
%token END_P
%token PARSER_SYNTAX_ERROR /*used internal*/
/* reserved key words */
%token
//-----------------------------reserved keyword begin-----------------------------------------------
      FREQ INTERVAL YEARLY MONTHLY WEEKLY DAILY DAYLY HOURLY MINUTELY SECONDLY BYMONTH BYWEEKNO BYYEARDAY BYDATE
      BYMONTHDAY BYDAY BYHOUR BYMINUTE BYSECOND BYSETPOS JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC
      MON TUE WED THU FRI SAT SUN
//-----------------------------reserved keyword end-------------------------------------------------
%right END_KEY
%left ELSE IF ELSEIF

%nonassoc LOWER_PARENS
%nonassoc FUNCTION ZEROFILL SIGNED UNSIGNED
%nonassoc PARENS
%left '(' ')'
%nonassoc AUTHID INTERFACE
%nonassoc DECLARATION

%token <node> INTNUM

%type <node> dbms_sched_calendar regular_schedule sched_frequency_clause sched_interval_clause sched_predefined_frequency sched_by_clause
%type <node> sched_bylist sched_by_node sched_by_int_node sched_by_int_list
%type <node> sched_bymonth_clause sched_by_month_node sched_by_month_list sched_byweekno_clause sched_by_day_node sched_by_day_list
%type <node> sched_byyearday_clause sched_bymonthday_clause sched_byday_clause sched_byhour_clause sched_byminute_clause sched_bysecond_clause

%%
/*****************************************************************************
 *
 *      calendar pl grammar dbms_sched calendar syntax
 *
 *****************************************************************************/
dbms_sched_calendar:
  regular_schedule
  {
    $$ = $1;
    parse_ctx->stmt_tree_ = $$;
    YYACCEPT;
  }
;

regular_schedule:
  sched_frequency_clause
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR, 1, $1);
  }
  | sched_frequency_clause ';' sched_interval_clause
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR, 2, $1, $3);
  }
  | sched_frequency_clause ';' sched_interval_clause ';' sched_by_clause
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR, 3, $1, $3, $5);
  }
  | sched_frequency_clause ';' sched_by_clause
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR, 2, $1, $3);
  }
;

sched_frequency_clause:
  FREQ '=' sched_predefined_frequency
  {
    $$ = $3;
  } 
;

sched_interval_clause:
  INTERVAL '=' INTNUM
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_INTERVAL);
    $$->value_ = $3->value_;
  }
;

sched_by_clause:
  sched_bylist
  {
    merge_nodes($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY_LIST, $1);
  }
;

sched_bylist:
  sched_by_node 
  {
    $$ = $1;
  }
  | sched_bylist ';' sched_by_node
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
  }
;

sched_by_node:
  sched_bymonth_clause
  {
    $$ = $1; //type = 1
  }
  | sched_byweekno_clause
  {
    $$ = $1; //type = 2
  }
  | sched_byyearday_clause
  {
    $$ = $1; //type = 3
  }
  // sched_bydate_clause type = 4
  | sched_bymonthday_clause
  {
    $$ = $1; //type = 5
  }
  | sched_byday_clause 
  {
    $$ = $1; //type = 6
  }
  // sched_bytime_clause type = 7
  | sched_byhour_clause 
  {
    $$ = $1; //type = 8
  }
  | sched_byminute_clause 
  {
    $$ = $1; //type = 9
  }
  | sched_bysecond_clause
  {
    $$ = $1; //type = 10
  }
  // sched_bysetpos_clause type = 11
;

sched_bymonth_clause:
  BYMONTH '=' sched_by_int_list
  {
    merge_nodes($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY_LIST, $3);
    $$->value_ = 1;
  }
  | BYMONTH '=' sched_by_month_list
  {
    merge_nodes($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY_LIST, $3);
    $$->value_ = 1;
  }
;

sched_byweekno_clause:
  BYWEEKNO '=' sched_by_int_list
  {
    merge_nodes($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY_LIST, $3);
    $$->value_ = 2;
  }
;

sched_byyearday_clause:
  BYYEARDAY '=' sched_by_int_list
  {
    merge_nodes($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY_LIST, $3);
    $$->value_ = 3;
  }
;

sched_bymonthday_clause:
  BYMONTHDAY '=' sched_by_int_list
  {
    merge_nodes($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY_LIST, $3);
    $$->value_ = 5;
  }
;

sched_byday_clause:
  BYDAY '=' sched_by_day_list
  {
    merge_nodes($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY_LIST, $3);
    $$->value_ = 6;
  }
;

sched_byhour_clause:
  BYHOUR '=' sched_by_int_list
  {
    merge_nodes($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY_LIST, $3);
    $$->value_ = 8;
  }
;

sched_byminute_clause:
  BYMINUTE '=' sched_by_int_list
  {
    merge_nodes($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY_LIST, $3);
    $$->value_ = 9;
  }
;

sched_bysecond_clause:
  BYSECOND '=' sched_by_int_list
  {
    merge_nodes($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY_LIST, $3);
    $$->value_ = 10;
  }
;

sched_by_int_list:
  sched_by_int_node
  {
    $$ = $1;
  }
  | sched_by_int_list ',' sched_by_int_node
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
  }
;

sched_by_int_node:
  INTNUM
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); 
    $$->value_ = $1->value_;
  }
;

sched_by_month_list:
  sched_by_month_node
  {
    $$ = $1;
  }
  | sched_by_month_list ',' sched_by_month_node
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
  }
;

sched_by_day_list:
  sched_by_day_node
  {
    $$ = $1;
  }
  | sched_by_day_list ',' sched_by_day_node
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
  }
;

sched_by_month_node:
  JAN  { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 1;  }
  | FEB  { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 2; }
  | MAR  { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 3; }
  | APR  { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 4; }
  | MAY  { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 5; }
  | JUN  { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 6; }
  | JUL  { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 7; }
  | AUG  { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 8; }
  | SEP  { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 9; }
  | OCT  { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 10; }
  | NOV  { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 11; }
  | DEC  { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 12; }
;

sched_by_day_node:
  MON { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 1;  }
  | TUE { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 2;  }
  | WED { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 3;  }
  | THU { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 4;  }
  | FRI { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 5;  }
  | SAT { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 6;  }
  | SUN { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_BY); $$->value_ = 7;  }
;


sched_predefined_frequency:
  YEARLY   { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_FREQ); $$->value_ = 1; }
  | MONTHLY  { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_FREQ); $$->value_ = 2; }
  | WEEKLY   { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_FREQ); $$->value_ = 3; }
  | DAILY    { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_FREQ); $$->value_ = 4; }
  | DAYLY    { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_FREQ); $$->value_ = 4; }
  | HOURLY   { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_FREQ); $$->value_ = 5; }
  | MINUTELY { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_FREQ); $$->value_ = 6; } 
  | SECONDLY { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SCHED_CALENDAR_FREQ); $$->value_ = 7; }
;

%%

void ob_dbms_sched_calendar_yyerror(YYLTYPE *yylloc, ObParseCtx *parse_ctx, char *s)
{
  if (OB_LIKELY(NULL != parse_ctx)) {
    strncpy(parse_ctx->global_errmsg_, s, MAX_ERROR_MSG);
    parse_ctx->global_errmsg_[MAX_ERROR_MSG - 1] = '\0';
    // vfprintf(stderr, s, ap);
    if (OB_LIKELY(NULL != yylloc)) {
      ObParseErrorInfo *error_info = (ObParseErrorInfo*)parse_malloc(sizeof(ObParseErrorInfo), parse_ctx->mem_pool_);
      if (NULL == error_info) {
        YY_FATAL_ERROR("No memory for malloc parse error info\n");
      } else {
        memset(error_info, 0, sizeof(ObParseErrorInfo));
        error_info->stmt_loc_.first_column_ = yylloc->first_column;
        error_info->stmt_loc_.last_column_ = yylloc->last_column;
        error_info->stmt_loc_.first_line_ = yylloc->first_line;
        error_info->stmt_loc_.last_line_ = yylloc->last_line;
        parse_ctx->cur_error_info_ = error_info;
      }
    }
  }
}

