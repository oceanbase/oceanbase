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
%option prefix="obpl_mysql_yy"
%option header-file="../../../src/pl/parser/pl_parser_mysql_mode_lex.h"
%{
#include "pl/parser/pl_parser_base.h"
#include "pl_parser_mysql_mode_tab.h"

extern void obpl_mysql_yyerror(YYLTYPE *yylloc, ObParseCtx *parse_ctx, char *s);
extern void obpl_mysql_parse_fatal_error(int32_t errcode, yyscan_t yyscanner, yyconst char *msg, ...);

#define YY_FATAL_ERROR(msg, args...) (obpl_mysql_parse_fatal_error(OB_PARSER_ERR_NO_MEMORY, yyscanner, msg, ##args))
#define YY_UNEXPECTED_ERROR(msg, args...) (obpl_mysql_parse_fatal_error(OB_PARSER_ERR_UNEXPECTED, yyscanner, msg, ##args))
#define YY_NAME_ERROR(msg, args...) (obpl_mysql_parse_fatal_error(OB_PARSER_ERR_ILLEGAL_NAME, yyscanner, msg, ##args))
%}

%x in_c_comment
%x sq
%x dq
%x bt
/* the adq is used to process dq in ANSI_QUOTES sql_mode*/
%x adq

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
comment      ("--"{space}+{non_newline}*)|(#{non_newline}*)
whitespace       ({space}+|{comment})
c_cmt_begin      \/\*
c_cmt_end        \*+\/
identifer        (([A-Za-z0-9$_]|{UTF8_GB_CHAR})*)
int_num          [0-9]+
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

mysql_compatible_comment_with_version \/\*\![0-9]{5}
mysql_compatible_comment_without_version \/\*\!
mysql_compatible_comment_end \*\/

%%
ALTER                         { return ALTER; }
BINARY                        { return BINARY; }
CALL                          { return CALL; }
CREATE                        { return CREATE; }
DELETE                        { return DELETE; }
DO                            { return DO; }
DROP                          { return DROP; }
FUNCTION                      { return FUNCTION; }
INSERT                        { return INSERT; }
INTO                          { return INTO; }
PROCEDURE                     { return PROCEDURE; }
SELECT                        { return SELECT; }
SET                           { return SET; }
TABLE                         { return TABLE; }
UPDATE                        { return UPDATE; }
JSON                          { return JSON; }
REPLACE                       { return REPLACE; }
TRIGGER {
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  parse_ctx->is_for_trigger_ = 1;
  return TRIGGER;
}
NULL {
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  malloc_new_node(yylval->node, parse_ctx->mem_pool_, T_NULL, 0);
  return NULLX;
}

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
    token_ret = DECIMAL_VAL;
  }
  return token_ret;
}

[0-9]+E[-+]?[0-9]+ |
[0-9]+"."[0-9]*E[-+]?[0-9]+ |
"."[0-9]+E[-+]?[0-9]+ {
  ParseNode *node = NULL;
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  malloc_new_node(node, parse_ctx->mem_pool_, T_DOUBLE, 0);
  check_ptr(yylval);
  yylval->node = node;
  node->str_value_ = parse_strdup(yytext, parse_ctx->mem_pool_, &(node->str_len_));
  check_ptr(node->str_value_);
  return DECIMAL_VAL;
}

[0-9]+"."[0-9]* |
"."[0-9]+ {
  ParseNode *node = NULL;
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  malloc_new_node(node, parse_ctx->mem_pool_, T_NUMBER/* should be T_NUMBER,*/, 0);
  check_ptr(yylval);
  yylval->node = node;
  node->str_value_ = parse_strdup(yytext, parse_ctx->mem_pool_, &(node->str_len_));
  check_ptr(node->str_value_);
  return DECIMAL_VAL;
}

{sqbegin} {
  BEGIN(sq);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  scanner_ctx->first_column_ = yylloc->first_column;
  prepare_literal_buffer(scanner_ctx, parse_ctx->stmt_len_ + 1, parse_ctx->mem_pool_);
  check_ptr(yylval);
  malloc_new_node(yylval->node, parse_ctx->mem_pool_, T_VARCHAR, 0);
  yylval->node->str_len_ = 0;
}


<sq>{sqend} {
  BEGIN(INITIAL);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  char *tmp_literal = scanner_ctx->tmp_literal_;
  tmp_literal[yylval->node->str_len_] = '\0';
  yylloc->first_column = scanner_ctx->first_column_;
  yylval->node->str_value_ = parse_strndup(tmp_literal, yylval->node->str_len_ + 1, parse_ctx->mem_pool_);
  return STRING;
}

<sq>{sqdouble} {
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  char *tmp_literal = scanner_ctx->tmp_literal_;
  tmp_literal[yylval->node->str_len_++] = '\'';
}

<sq>{sqcontent} {
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  char *tmp_literal = scanner_ctx->tmp_literal_;
  memmove(tmp_literal + yylval->node->str_len_, yytext, yyleng);
  yylval->node->str_len_ += yyleng;
}

<sq>{qescape} {
  int with_back_slash = 1;
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  char *tmp_literal = scanner_ctx->tmp_literal_;
  check_ptr(yylval);
  unsigned char c = escaped_char(yytext[1], &with_back_slash);
  if (with_back_slash) {
    tmp_literal[yylval->node->str_len_++] = '\\';
  }
  tmp_literal[yylval->node->str_len_++] = c;
}

<sq>{sqnewline} {
  /*
     In case of two adjacent string literal, such as " 'a' 'b' ", the two string will be
     concatenate into 'ab'. However, the string 'a' will used as the column name if it appears
     in the select list, which means we must save it rather than just skipping the 'sqnewline'.

     One solution is to do this in the yacc and let the lexer produce all strings as individual
     tokens. However, it will generate ambiguity in the yacc according to our grammar definition.
     Instead, we remember the first string as a child of the 'T_VARCHAR' node which represents
     " 'a' 'b' ", whose str_value_ is 'ab'. This will save us from modifying our grammar and a
     a lot of troubles.
   */
  check_ptr(yylval);
  if (0 == yylval->node->num_child_) {
    ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
    ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
    char *tmp_literal = scanner_ctx->tmp_literal_;
    tmp_literal[yylval->node->str_len_] = '\0';
    yylval->node->children_ = (ParseNode **)parse_malloc(sizeof(ParseNode *), parse_ctx->mem_pool_);
    if (OB_UNLIKELY(NULL == yylval->node->children_)) {
      YY_FATAL_ERROR("No more space for mallocing '%s'\n", yytext);
    }

    malloc_new_node(yylval->node->children_[0], parse_ctx->mem_pool_, T_CONCAT_STRING, 0);
    (*yylval->node->children_)->str_value_ = parse_strndup(tmp_literal, yylval->node->str_len_ + 1,
                                            parse_ctx->mem_pool_);
    (*yylval->node->children_)->str_len_ = yylval->node->str_len_;
    yylval->node->num_child_ = 1;
  }
}

<sq><<EOF>>  {
  obpl_mysql_yyerror(yylloc, yyextra, "unterminated quoted string\n");
  return END_P;
}

{dqbegin} {
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  scanner_ctx->first_column_ = yylloc->first_column;
  prepare_literal_buffer(scanner_ctx, parse_ctx->stmt_len_ + 1, parse_ctx->mem_pool_);
  bool is_ansi_quotes = false;
  IS_ANSI_QUOTES(scanner_ctx->sql_mode_, is_ansi_quotes);
  if (is_ansi_quotes) {
    BEGIN(adq);
    malloc_new_node(yylval->node, parse_ctx->mem_pool_, T_IDENT, 0);
    yylval->node->str_len_ = 0;
  } else {
    BEGIN(dq);
    malloc_new_node(yylval->node, parse_ctx->mem_pool_, T_VARCHAR, 0);
    yylval->node->str_len_ = 0;
  }
}

<dq>{dqend} {
  BEGIN(INITIAL);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  yylloc->first_column = scanner_ctx->first_column_;
  char *tmp_literal = scanner_ctx->tmp_literal_;
  check_ptr(yylval);
  tmp_literal[yylval->node->str_len_] = '\0';
  yylval->node->str_value_ = parse_strndup(tmp_literal, yylval->node->str_len_ + 1, parse_ctx->mem_pool_);
  return STRING;
}

<dq>{dqdouble} {
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  char *tmp_literal = scanner_ctx->tmp_literal_;
  tmp_literal[yylval->node->str_len_++] = '\"';
}

<dq>{dqcontent} {
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  char *tmp_literal = scanner_ctx->tmp_literal_;
  memmove(tmp_literal + yylval->node->str_len_, yytext, yyleng);
  yylval->node->str_len_ += yyleng;
}

<dq>{qescape} {
  int with_back_slash = 1;
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  char *tmp_literal = scanner_ctx->tmp_literal_;
  check_ptr(yylval);
  unsigned char c = escaped_char(yytext[1], &with_back_slash);
  if (with_back_slash) {
    tmp_literal[yylval->node->str_len_++] = '\\';
  }
  tmp_literal[yylval->node->str_len_++] = c;
}

<dq>{dqnewline} {
  /* see 'sqnewline' */
  check_ptr(yylval);
  if (0 == yylval->node->num_child_) {
    ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
    ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
    char *tmp_literal = scanner_ctx->tmp_literal_;
    tmp_literal[yylval->node->str_len_] = '\0';

    yylval->node->children_ = (ParseNode **)parse_malloc(sizeof(ParseNode *), parse_ctx->mem_pool_);
    if (OB_UNLIKELY(NULL == yylval->node->children_)) {
      YY_FATAL_ERROR("No more space for mallocing '%s'\n", yytext);
    }

    malloc_new_node(yylval->node->children_[0], parse_ctx->mem_pool_, T_CONCAT_STRING, 0);
    (*yylval->node->children_)->str_value_ = parse_strndup(tmp_literal, yylval->node->str_len_ + 1,
                                            parse_ctx->mem_pool_);
    (*yylval->node->children_)->str_len_ = yylval->node->str_len_;
    yylval->node->num_child_ = 1;
  }
}

<dq><<EOF>>  {
  obpl_mysql_yyerror(yylloc, yyextra, "unterminated doublequoted string\n");
  return END_P;
}

<adq>{dqend} {
  BEGIN(INITIAL);
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  char *tmp_literal = scanner_ctx->tmp_literal_;
  yylloc->first_column = scanner_ctx->first_column_;
  tmp_literal[yylval->node->str_len_] = '\0';

  char *dup_value = NULL;
  if (parse_ctx->is_not_utf8_connection_) {
    dup_value = parse_str_convert_utf8(parse_ctx->charset_info_, tmp_literal,
                                       parse_ctx->mem_pool_, &(yylval->node->str_len_),
                                       &(parse_ctx->global_errno_));
    check_identifier_convert_result(parse_ctx->global_errno_);
  } else {
    dup_value = parse_strndup(tmp_literal, yylval->node->str_len_ + 1, parse_ctx->mem_pool_);
  }
  check_ptr(dup_value);
  yylval->node->str_value_ = dup_value;
  return IDENT;
}

<adq>{dqdouble} {
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  char *tmp_literal = scanner_ctx->tmp_literal_;
  tmp_literal[yylval->node->str_len_++] = '"';
}

<adq>{dqcontent} {
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  char *tmp_literal = scanner_ctx->tmp_literal_;
  memmove(tmp_literal + yylval->node->str_len_, yytext, yyleng);
  yylval->node->str_len_ += yyleng;
}

<adq><<EOF>>  {
  obpl_mysql_yyerror(yylloc, yyextra, "unterminated doublequoted string\n");
  return END_P;
}

X'([0-9A-F])*'|0X([0-9A-F])+ {
  char *src = yytext + 2;
  size_t len = yyleng - 2;
  // https://dev.mysql.com/doc/refman/5.7/en/hexadecimal-literals.html
  // Values written using X'val' notation must contain an even number of digits or a syntax error occurs. To correct the problem, pad the value with a leading zero.
  // Values written using 0xval notation that contain an odd number of digits are treated as having an extra leading 0. For example, 0xaaa is interpreted as 0x0aaa.
  if ('\'' == src[len - 1]) {
    // Values written using X'val' notation
    --len;
    if (0 != len % 2) {
      obpl_mysql_yyerror(yylloc, yyextra, "hex string contain an even number of digits\n");
      return PARSER_SYNTAX_ERROR;
    }
  }
  ParseNode *node = NULL;
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  check_ptr(yylval);
  malloc_new_node(node, parse_ctx->mem_pool_, T_HEX_STRING, 0);
  if (len > 0) {
    int64_t dest_len = ob_parse_binary_len(len);
    char *dest = (char *)parse_malloc(dest_len, parse_ctx->mem_pool_);
    check_ptr(dest);
    ob_parse_binary(src, len, dest);
    node->str_value_ = dest;
    node->str_len_ = dest_len;
  } else {
    node->str_value_ = NULL;
    node->str_len_ = 0;
  }
  yylval->node = node;
  return HEX_STRING_VALUE;
}

B'([01])*'|0B([01])+ {
  char* src = yytext + 2;
  size_t len = yyleng - 2;
  if(src[len - 1] == '\'') {
    --len;
  }
  ParseNode *node = NULL;
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  check_ptr(yylval);
  malloc_new_node(node, parse_ctx->mem_pool_, T_HEX_STRING, 0);
  if (len > 0) {
    int64_t dest_len = ob_parse_bit_string_len(len);
    char *dest = (char*)parse_malloc(dest_len, parse_ctx->mem_pool_);
    check_ptr(dest);
    ob_parse_bit_string(src, len, dest);
    node->str_value_ = dest;
    node->str_len_ = dest_len;
  } else {
    node->str_value_ = NULL;
    node->str_len_ = 0;
  }
  yylval->node = node;
  return HEX_STRING_VALUE;
}

Date{whitespace}?'[^']*' {
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  check_ptr(yylval);
  malloc_time_node_s(parse_ctx->mem_pool_, T_DATE);
  return DATE_VALUE;
}

Time{whitespace}?'[^']*' {
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  malloc_time_node_s(parse_ctx->mem_pool_, T_TIME);
  check_ptr(yylval);
  return DATE_VALUE;
}

Timestamp{whitespace}?'[^']*' {
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  check_ptr(yylval);
  malloc_time_node_s(parse_ctx->mem_pool_, T_TIMESTAMP);
  return DATE_VALUE;
}
Date{whitespace}?\"[^\"]*\" {
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  malloc_time_node_d(parse_ctx->mem_pool_, T_DATE);
  check_ptr(yylval);
  return DATE_VALUE;
}

Time{whitespace}?\"[^\"]*\" {
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  check_ptr(yylval);
  malloc_time_node_d(parse_ctx->mem_pool_, T_TIME);
  return DATE_VALUE;
}

Timestamp{whitespace}?\"[^\"]*\" {
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  check_ptr(yylval);
  malloc_time_node_d(parse_ctx->mem_pool_, T_TIMESTAMP);
  return DATE_VALUE;
}

{system_variable} {
  ParseNode *node = NULL;
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  check_ptr(yylval);
  malloc_new_node(node, parse_ctx->mem_pool_, T_SYSTEM_VARIABLE, 0);
  yylval->node = node;
  /* skip '@@' */
  node->str_value_ = parse_strdup(yytext + 2, parse_ctx->mem_pool_, &(node->str_len_));
  check_ptr(node->str_value_);
  node->value_ = 0;
  return SYSTEM_VARIABLE;
}

{user_variable} {
  ParseNode *node = NULL;
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  check_ptr(yylval);
  malloc_new_node(node, parse_ctx->mem_pool_, T_USER_VARIABLE_IDENTIFIER, 0);
  yylval->node = node;
  /* skip '@' */
  node->str_value_ = parse_strdup(yytext + 1, parse_ctx->mem_pool_, &(node->str_len_));
  if (NULL != node->str_value_
      && *(yytext + 1) == *(yytext + node->str_len_)
      && (*(yytext + 1) == '\'' || *(yytext + 1) == '\"' || *(yytext + 1) == '`')) {
    node->str_value_ += 1;
    node->str_len_ -= 2;
  }
  check_ptr(node->str_value_);
  return USER_VARIABLE;
}

{btbegin} {
  BEGIN(bt); /*fast parameterize don't handle connent in ``*/
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  scanner_ctx->first_column_ = yylloc->first_column;
  prepare_literal_buffer(scanner_ctx, parse_ctx->stmt_len_ + 1, parse_ctx->mem_pool_);
  malloc_new_node(yylval->node, parse_ctx->mem_pool_, T_IDENT, 0);
  yylval->node->str_len_ = 0;
}

<bt>{btdouble} {
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  char *tmp_literal = scanner_ctx->tmp_literal_;
  tmp_literal[yylval->node->str_len_++] = '`';
}

<bt>{btcontent} {
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  char *tmp_literal = scanner_ctx->tmp_literal_;
  memmove(tmp_literal + yylval->node->str_len_, yytext, yyleng);
  yylval->node->str_len_ += yyleng;
}

<bt>{btend} {
  BEGIN(INITIAL);
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  char *tmp_literal = scanner_ctx->tmp_literal_;
  yylloc->first_column = scanner_ctx->first_column_;
  tmp_literal[yylval->node->str_len_] = '\0';

  char *dup_value = NULL;
  if (parse_ctx->is_not_utf8_connection_) {
    dup_value = parse_str_convert_utf8(parse_ctx->charset_info_, tmp_literal,
                                       parse_ctx->mem_pool_, &(yylval->node->str_len_),
                                       &(parse_ctx->global_errno_));
    check_identifier_convert_result(parse_ctx->global_errno_);
  } else {
    dup_value = parse_strndup(tmp_literal, yylval->node->str_len_ + 1, parse_ctx->mem_pool_);
  }
  check_ptr(dup_value);
  yylval->node->str_value_ = dup_value;
  return IDENT;
}

<bt><<EOF>> {
  obpl_mysql_yyerror(yylloc, yyextra, "unterminated backtick string\n");
  return END_P;
}

{mysql_compatible_comment_without_version} {
  //refer to src/sql/parser/sql_parser_mysql_mode.l for more info.
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  parse_ctx->mysql_compatible_comment_ = true;
}

{mysql_compatible_comment_with_version} {
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  parse_ctx->mysql_compatible_comment_ = true;
}

{mysql_compatible_comment_end} {
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  if (parse_ctx->mysql_compatible_comment_) {
    parse_ctx->mysql_compatible_comment_ = false;
    BEGIN(INITIAL);
  } else {
    char c_ret = yytext[0];
    yyless(1);
    return c_ret;
  }
}

{c_cmt_begin} {
  BEGIN(in_c_comment);
}

<in_c_comment>{c_cmt_end} {
//  ((ParseResult *)yyextra)->has_encount_comment_ = true;
  BEGIN(INITIAL);
}

<in_c_comment><<EOF>>  {
  obpl_mysql_yyerror(yylloc, yyextra, "unterminated log_level string\n");
  return END_P;
}
<in_c_comment>[\n] { yylineno++; }
<in_c_comment>. {}

{comment} {
//  ((ParseResult *)yyextra)->has_encount_comment_ = true;
/* ignore */
}

{identifer} {
  int32_t token = mysql_sql_reserved_keyword_lookup(yytext);
  if (token < 0) {
    const NonReservedKeyword *word = NULL;
    if (NULL == (word = mysql_pl_non_reserved_keyword_lookup(yytext)))
    {
      token = IDENT;
      ParseNode *node = NULL;
      ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
      malloc_new_node(node, parse_ctx->mem_pool_, T_IDENT, 0);
      yylval->node = node;
      if (parse_ctx->is_not_utf8_connection_) {
        node->str_value_ = parse_str_convert_utf8(parse_ctx->charset_info_, yytext,
                                                  parse_ctx->mem_pool_, &(node->str_len_),
                                                  &(parse_ctx->global_errno_));
        check_identifier_convert_result(parse_ctx->global_errno_);
      } else {
        node->str_value_ = parse_strdup(yytext, parse_ctx->mem_pool_, &(node->str_len_));
      }
    } else {
      yylval->non_reserved_keyword = word;
      token = word->keyword_type;
    }
  } else {
    token = SQL_KEYWORD;
  }
  return token;
}

[-+&~|^/%*(),;.:!] { return yytext[0]; }

[ \t\r\n] { }
"--"[ \t].*;

"=>"    { return PARAM_ASSIGN_OPERATOR; }

<<EOF>> { return END_P; }
. { return yytext[0]; }
%%

//parser function
void obpl_mysql_parse_fatal_error(int32_t errcode, yyscan_t yyscanner, yyconst char *msg, ...)
{
  ObParseCtx *parse_ctx = obpl_mysql_yyget_extra(yyscanner);
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
