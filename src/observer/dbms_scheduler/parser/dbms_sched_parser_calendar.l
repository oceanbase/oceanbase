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

/*first: declare*/
%option noyywrap nounput noinput yylineno case-insensitive
%option noyyalloc noyyrealloc noyyfree
%option reentrant bison-bridge bison-locations
%option prefix="ob_dbms_sched_calendar_yy"
%option header-file="dbms_sched_parser_calendar_lex.h"
%{
#include "dbms_sched_parser_base.h"
#include "dbms_sched_parser_calendar_tab.h"

extern void ob_dbms_sched_calendar_yyerror(YYLTYPE *yylloc, ObParseCtx *parse_ctx, char *s);
extern void ob_dbms_sched_calendar_parse_fatal_error(int32_t errcode, yyscan_t yyscanner, yyconst char *msg, ...);

#define YY_FATAL_ERROR(msg, args...) (ob_dbms_sched_calendar_parse_fatal_error(OB_PARSER_ERR_NO_MEMORY, yyscanner, msg, ##args))
#define YY_UNEXPECTED_ERROR(msg, args...) (ob_dbms_sched_calendar_parse_fatal_error(OB_PARSER_ERR_UNEXPECTED, yyscanner, msg, ##args))
#define YY_NAME_ERROR(msg, args...) (ob_dbms_sched_calendar_parse_fatal_error(OB_PARSER_ERR_ILLEGAL_NAME, yyscanner, msg, ##args))
%}

%x in_c_comment
%x sq
%x dq
%x bt

U  [\x80-\xbf]
U_2  [\xc2-\xdf]
U_3  [\xe0-\xef]
U_4  [\xf0-\xf4]
GB_1 [\x81-\xfe]
GB_2 [\x40-\xfe]
GB_3 [\x30-\x39]
UTF8_GB_CHAR ({U_2}{U}|{U_3}{U}{U}|{U_4}{U}{U}{U}|{GB_1}{GB_2}|{GB_1}{GB_3}{GB_1}{GB_3})
space            [ \t\n\r\f]
non_newline      [^\n\r]
comment      ("--"[ \t]+{non_newline}*)|(#{non_newline}*|"--"[\n\r])
whitespace       ({space}+|{comment})
c_cmt_begin      \/\*
c_cmt_end        \*+\/
identifer        (([A-Za-z0-9$_]|{UTF8_GB_CHAR})*)
int_num          -?[0-9]+
system_variable  (@@[A-Za-z_][A-Za-z0-9_]*)
user_variable    (@[A-Za-z0-9_\.$]*)|(@[`'\"][`'\"A-Za-z0-9_\.$/%]*)|(@%)

quote         '
sqbegin       {quote}
sqend         {quote}
sqdouble      {quote}{quote}
sqcontent     [^\\']+
qescape       [\\](.|\n)
sqnewline     {quote}{whitespace}{quote}

dquote         \"
dqbegin       {dquote}
dqend         {dquote}
dqdouble      {dquote}{dquote}
dqcontent     [^\\"]+
dqnewline     {dquote}{whitespace}{dquote}

backtick      `
btbegin       {backtick}
btend         {backtick}
btdouble      {backtick}{backtick}
btcontent     [^`]+

calendar_compatible_comment_with_version \/\*\![0-9]{5}
calendar_compatible_comment_without_version \/\*\!
calendar_compatible_comment_end \*\/

%%
FREQ                          { return FREQ; }
INTERVAL                      { return INTERVAL; }

YEARLY                        { return YEARLY; }
MONTHLY                       { return MONTHLY; }
WEEKLY                        { return WEEKLY; }
DAILY                         { return DAILY; }
DAYLY                         { return DAYLY; }
HOURLY                        { return HOURLY; }
MINUTELY                      { return MINUTELY; }
SECONDLY                      { return SECONDLY; }

BYMONTH                       { return BYMONTH; }
BYWEEKNO                      { return BYWEEKNO; }
BYYEARDAY                     { return BYYEARDAY; }
BYDATE                        { return BYDATE; }
BYMONTHDAY                    { return BYMONTHDAY; }
BYDAY                         { return BYDAY; }
BYHOUR                        { return BYHOUR; }
BYMINUTE                      { return BYMINUTE; }
BYSECOND                      { return BYSECOND; }
BYSETPOS                      { return BYSETPOS; }

[A-Za-z0-9_]+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC|MON|TUE|WED|THU|FRI|SAT|SUN) {
  YY_NAME_ERROR("ilegal identifier %s", yytext);
  return PARSER_SYNTAX_ERROR;
}
(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC|MON|TUE|WED|THU|FRI|SAT|SUN)[A-Za-z0-9_]+ {
  YY_NAME_ERROR("ilegal identifier %s", yytext);
  return PARSER_SYNTAX_ERROR;
}

[A-Za-z0-9_]+(YEARLY|MONTHLY|WEEKLY|DAILY|DAYLY|HOURLY|MINUTELY|SECONDLY) {
  YY_NAME_ERROR("ilegal identifier %s", yytext);
  return PARSER_SYNTAX_ERROR;
}
(YEARLY|MONTHLY|WEEKLY|DAILY|DAYLY|HOURLY|MINUTELY|\SECONDLY)[A-Za-z0-9_]+ {
  YY_NAME_ERROR("ilegal identifier %s", yytext);
  return PARSER_SYNTAX_ERROR;
}

JAN                      { return JAN; }
FEB                      { return FEB; }
MAR                      { return MAR; }
APR                      { return APR; }
MAY                      { return MAY; }
JUN                      { return JUN; }
JUL                      { return JUL; }
AUG                      { return AUG; }
SEP                      { return SEP; }
OCT                      { return OCT; }
NOV                      { return NOV; }
DEC                      { return DEC; }

MON                      { return MON; }
TUE                      { return TUE; }
WED                      { return WED; }
THU                      { return THU; }
FRI                      { return FRI; }
SAT                      { return SAT; }
SUN                      { return SUN; }

{int_num} {
  int32_t token_ret = INTNUM;
  ParseNode *node = NULL;
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  malloc_new_node(node, parse_ctx->mem_pool_, T_INT, 0);
  yylval->node = node;
  node->str_value_ = parse_strdup(yytext, parse_ctx->mem_pool_, &(node->str_len_));
  check_ptr(node->str_value_);
  errno = 0;
  node->value_ = strtoll(node->str_value_, NULL, 10);
  if (ERANGE == errno) {
    /* if out of range, seem it as must NUMERIC type, now is double */
    node->type_ = T_NUMBER;
    // token_ret = DECIMAL_VAL;
  }
  return token_ret;
}

[-+&~|^/%*(),;.:!] { return yytext[0]; }

[ \t\r\n] { }
"--"[ \t].*;

. { return yytext[0]; }
%%

//parser function
void ob_dbms_sched_calendar_parse_fatal_error(int32_t errcode, yyscan_t yyscanner, yyconst char *msg, ...)
{
  ObParseCtx *parse_ctx = ob_dbms_sched_calendar_yyget_extra(yyscanner);
  if (parse_ctx != NULL) {
    parse_ctx->global_errno_ = errcode;
    if (OB_LIKELY(NULL != msg)) {
      va_list ap;
      va_start(ap, msg);
      vsnprintf(parse_ctx->global_errmsg_, MAX_ERROR_MSG, msg, ap);
      va_end(ap);
    }
  }
  longjmp(parse_ctx->jmp_buf_, 1);//the secord param must be non-zero value
}

void *yyalloc(size_t bytes, void *yyscanner)
{
  void *ptr_ret = NULL;
  ObParseCtx *parse_ctx = yyget_extra(yyscanner);
  check_ptr(parse_ctx);
  ptr_ret = parse_malloc(bytes, parse_ctx->mem_pool_);
  return ptr_ret;
}

void *yyrealloc(void *ptr, size_t bytes, void *yyscanner)
{
  void *ptr_ret = NULL;
  ObParseCtx *parse_ctx = yyget_extra(yyscanner);
  check_ptr(parse_ctx);
  ptr_ret = parse_realloc(ptr, bytes, parse_ctx->mem_pool_);
  return ptr_ret;
}

void yyfree(void *ptr, void *yyscanner)
{
  (void)yyscanner;
  /* Do nothing -- we leave it to the garbage collector. */
  parse_free(ptr);
}
