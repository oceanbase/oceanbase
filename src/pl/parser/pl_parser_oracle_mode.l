
/**
 * Copyright 2014-2016 Alibaba Inc. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 *
 * Date: 2016年10月13日
 *
 * pl_parser.l is for …
 *
 * Authors:
 */

/*first: declare*/
%option noyywrap nounput noinput yylineno case-insensitive
%option noyyalloc noyyrealloc noyyfree
%option reentrant bison-bridge bison-locations
%option prefix="obpl_oracle_yy"
%option header-file="../../../src/pl/parser/pl_parser_oracle_mode_lex.h"
%{
#include "pl_parser_base.h"
#include "pl_parser_oracle_mode_tab.h"

extern void obpl_oracle_yyerror(YYLTYPE *yylloc, ObParseCtx *parse_ctx, char *s,...);
extern void obpl_oracle_parse_fatal_error(int32_t errcode, yyscan_t yyscanner, yyconst char *msg, ...);

#define YY_FATAL_ERROR(msg, args...) (obpl_oracle_parse_fatal_error(OB_PARSER_ERR_NO_MEMORY, yyscanner, msg, ##args))
#define YY_UNEXPECTED_ERROR(msg, args...) (obpl_oracle_parse_fatal_error(OB_PARSER_ERR_UNEXPECTED, yyscanner, msg, ##args))
#define YY_NAME_ERROR(msg, args...) (obpl_oracle_parse_fatal_error(OB_PARSER_ERR_ILLEGAL_NAME, yyscanner, msg, ##args))
%}

%x in_c_comment
%x sq
%x dq
%x bt
%s in_range_operator
%x in_condition_compile


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
comment          ("--"{non_newline}*)
whitespace       ({space}+|{comment})
c_cmt_begin      \/\*
c_cmt_end        \*+\/
identifer        (([A-Za-z]|{UTF8_GB_CHAR})([A-Za-z0-9$_#]|{UTF8_GB_CHAR})*)
int_num          [0-9]+

quote         '
sqbegin       {quote}
sqend         {quote}
sqdouble      {quote}{quote}
sqslash       [\\]
sqcontent     [^\\']+
sqnewline     {quote}{whitespace}{quote}

dquote         \"
dqbegin       {dquote}
dqend         {dquote}
dqdouble      {dquote}{dquote}
dqcontent     [^\"]+

anycontent    [.]+

%%

  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  if (parse_ctx->is_for_preprocess_) {
    BEGIN(in_condition_compile);
  }

<INITIAL,in_range_operator,in_condition_compile>"$"IF {
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  if (parse_ctx->is_for_preprocess_ != 1) {
    parse_ctx->is_for_preprocess_ = 1;
    obpl_oracle_yyerror(yylloc, yyextra, "need to preprocess now!\n");
  }
  return IF;
}

<INITIAL,in_range_operator,in_condition_compile>"$"ERROR {
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  if (parse_ctx->is_for_preprocess_ != 1) {
    parse_ctx->is_for_preprocess_ = 1;
    obpl_oracle_yyerror(yylloc, yyextra, "need to preprocess now!\n");
  }
  return ERROR;
}

<in_condition_compile>"$"THEN     { return THEN; }
<in_condition_compile>"$"ELSE     { return ELSE; }
<in_condition_compile>"$"ELSIF    { return ELSIF; }
<INITIAL,in_range_operator,in_condition_compile>"$"END      { return END_KEY; }

<in_condition_compile>[;] { return STRING; }
<in_condition_compile>[^ \t\n\r\f;]+ {
  return STRING;
}

FOR                 { BEGIN(in_range_operator); return FOR; }
NULL {
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  malloc_new_node(yylval->node, parse_ctx->mem_pool_, T_NULL, 0);
  return NULLX;
}
RANGE               { BEGIN(in_range_operator); return RANGE; }
TRIGGER {
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  parse_ctx->is_for_trigger_ = 1;
  return TRIGGER;
}
SET                           { return SET; }
SELECT                        { return SQL_KEYWORD; }
UPDATE                        { return UPDATE; }
INSERT                        { return INSERT; }
DELETE                        { return DELETE; }
WITH                          { return WITH; }
FORALL                        { BEGIN(in_range_operator); return FORALL; }
"<<"                          { return LABEL_LEFT; }
">>"                          { return LABEL_RIGHT; }
":="                          { return ASSIGN_OPERATOR; }
".."                          { BEGIN(INITIAL); return RANGE_OPERATOR; }
"=>"                          { return PARAM_ASSIGN_OPERATOR; }
"$""$"                        { return NULLX; }

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
    node->value_ = -2;
  }
  return token_ret;
}


<INITIAL>[0-9]+E[-+]?[0-9]+F |
[0-9]+"."[0-9]*E[-+]?[0-9]+F |
"."[0-9]+E[-+]?[0-9]+F {
  ParseNode *node = NULL;
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  malloc_new_node(node, parse_ctx->mem_pool_, T_FLOAT, 0);
  check_ptr(yylval);
  yylval->node = node;
  node->str_value_ = parse_strdup(yytext, parse_ctx->mem_pool_, &(node->str_len_));
  check_ptr(node->str_value_);
  return DECIMAL_VAL;
}

<INITIAL>[0-9]+E[-+]?[0-9]+D |
[0-9]+"."[0-9]*E[-+]?[0-9]+D |
"."[0-9]+E[-+]?[0-9]+D {
  ParseNode *node = NULL;
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  malloc_new_node(node, parse_ctx->mem_pool_, T_DOUBLE, 0);
  check_ptr(yylval);
  yylval->node = node;
  node->str_value_ = parse_strdup(yytext, parse_ctx->mem_pool_, &(node->str_len_));
  check_ptr(node->str_value_);
  return DECIMAL_VAL;
}

<INITIAL>[0-9]+E[-+]?[0-9]+ |
[0-9]+"."[0-9]*E[-+]?[0-9]+ |
"."[0-9]+E[-+]?[0-9]+ {
  ParseNode *node = NULL;
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  malloc_new_node(node, parse_ctx->mem_pool_, T_NUMBER, 0);
  check_ptr(yylval);
  yylval->node = node;
  node->str_value_ = parse_strdup(yytext, parse_ctx->mem_pool_, &(node->str_len_));
  check_ptr(node->str_value_);
  return DECIMAL_VAL;
}


<INITIAL>[0-9]+"."[0-9]*F |
[0-9]+F |
"."[0-9]+F {
  ParseNode *node = NULL;
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  malloc_new_node(node, parse_ctx->mem_pool_, T_FLOAT, 0);
  check_ptr(yylval);
  yylval->node = node;
  node->str_value_ = parse_strdup(yytext, parse_ctx->mem_pool_, &(node->str_len_));
  check_ptr(node->str_value_);
  return DECIMAL_VAL;
}


<INITIAL>[0-9]+"."[0-9]*D |
[0-9]+D |
"."[0-9]+D {
  ParseNode *node = NULL;
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  malloc_new_node(node, parse_ctx->mem_pool_, T_DOUBLE, 0);
  check_ptr(yylval);
  yylval->node = node;
  node->str_value_ = parse_strdup(yytext, parse_ctx->mem_pool_, &(node->str_len_));
  check_ptr(node->str_value_);
  return DECIMAL_VAL;
}


<INITIAL>[0-9]+"."[0-9]* |
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

("Q"|"q"){sqbegin} {
  BEGIN(sq);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  parse_ctx->in_q_quote_ = true;
  scanner_ctx->first_column_ = yylloc->first_column;
  prepare_literal_buffer(scanner_ctx, parse_ctx->stmt_len_ + 1, parse_ctx->mem_pool_);
  check_ptr(yylval);
  malloc_new_node(yylval->node, parse_ctx->mem_pool_, T_VARCHAR, 0);
  yylval->node->str_len_ = 0;
}

<sq>{sqend} {
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  check_ptr(yylval);
  char *tmp_literal = scanner_ctx->tmp_literal_;
  if (!parse_ctx->in_q_quote_) {
    BEGIN(INITIAL);
    tmp_literal[yylval->node->str_len_] = '\0';
    yylloc->first_column = scanner_ctx->first_column_;
    yylval->node->str_value_ = parse_strndup(tmp_literal, yylval->node->str_len_ + 1, parse_ctx->mem_pool_);
    return STRING;
  } else {
    if (yylval->node->str_len_ >= 2
        && ((tmp_literal[0] == tmp_literal[yylval->node->str_len_ - 1]
               && tmp_literal[0] != '(' && tmp_literal[0] != '[' && tmp_literal[0] != '{'
               &&tmp_literal[0] != '<'&& tmp_literal[0] != ' ' && tmp_literal[0] != '\t'
               && tmp_literal[0] != '\r')|| ('(' == tmp_literal[0]
               && ')' == tmp_literal[yylval->node->str_len_ - 1])|| ('[' == tmp_literal[0]
               && ']' == tmp_literal[yylval->node->str_len_ - 1])|| ('{' == tmp_literal[0]
               && '}' == tmp_literal[yylval->node->str_len_ - 1])|| ('<' == tmp_literal[0]
               && '>' == tmp_literal[yylval->node->str_len_ - 1]))) {
      BEGIN(INITIAL);
      tmp_literal[yylval->node->str_len_] = '\0';
      yylloc->first_column = scanner_ctx->first_column_;
      yylval->node->str_len_ -= 2;
      yylval->node->str_value_ = parse_strndup(tmp_literal + 1, yylval->node->str_len_, parse_ctx->mem_pool_);
      parse_ctx->in_q_quote_ = false;
      return STRING;
    } else {
      tmp_literal[yylval->node->str_len_++] = '\'';
    }
  }
}

<sq>{sqdouble} {
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  char *tmp_literal = scanner_ctx->tmp_literal_;
  tmp_literal[yylval->node->str_len_++] = '\'';
  if (parse_ctx->in_q_quote_) {
    tmp_literal[yylval->node->str_len_++] = '\'';
  }
}

<sq>{sqslash} {
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  char *tmp_literal = scanner_ctx->tmp_literal_;
  tmp_literal[yylval->node->str_len_++] = '\\';
}

<sq>{sqcontent} {
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  char *tmp_literal = scanner_ctx->tmp_literal_;
  memmove(tmp_literal + yylval->node->str_len_, yytext, yyleng);
  yylval->node->str_len_ += yyleng;
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
  obpl_oracle_yyerror(yylloc, yyextra, "unterminated quoted string\n");
  return END_P;
}

{dqbegin} {
  BEGIN(dq); /*fast parameterize don't handle connent in ``*/
  check_ptr(yylval);
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ObScannerCtx *scanner_ctx = &(parse_ctx->scanner_ctx_);
  scanner_ctx->first_column_ = yylloc->first_column;
  prepare_literal_buffer(scanner_ctx, parse_ctx->stmt_len_ + 1, parse_ctx->mem_pool_);
  malloc_new_node(yylval->node, parse_ctx->mem_pool_, T_IDENT, 0);
  yylval->node->str_len_ = 0;
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

<dq>{dqend} {
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

<dq><<EOF>>  {
  obpl_oracle_yyerror(yylloc, yyextra, "unterminated doublequoted string\n");
  return END_P;
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

<bt><<EOF>> {
  obpl_oracle_yyerror(yylloc, yyextra, "unterminated backtick string\n");
  return END_P;
}

{c_cmt_begin} {
  BEGIN(in_c_comment);
}

<in_c_comment>{c_cmt_end} {
  BEGIN(INITIAL);
}

<in_c_comment><<EOF>>  {
  obpl_oracle_yyerror(yylloc, yyextra, "unterminated log_level string\n");
  return END_P;
}
<in_c_comment>([^*]|\n)+|.

{comment} {
//  ((ParseResult *)yyextra)->has_encount_comment_ = true;
/* ignore */
}

"?" {
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  malloc_new_node(yylval->node, parse_ctx->mem_pool_, T_QUESTIONMARK, 0);
  parse_ctx->question_mark_ctx_.by_ordinal_ = true;
  if (OB_UNLIKELY(parse_ctx->question_mark_ctx_.by_name_)) {
    YY_FATAL_ERROR("Ordinal binding and Named binding cannot be combined\n");
  }
  yylval->node->value_ = parse_ctx->question_mark_ctx_.count_++;
  return QUESTIONMARK;
}

":"{identifer} {
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  if (parse_ctx->is_for_trigger_) {
    ParseNode *node = NULL;
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
    node->str_value_ = str_toupper((char *)node->str_value_, node->str_len_);
    parse_ctx->is_include_old_new_in_trigger_ = true;
    return IDENT;
  } else {
    ParseNode *node = NULL;
    malloc_new_node(node, parse_ctx->mem_pool_, T_QUESTIONMARK, 0);
    yylval->node = node;
    parse_ctx->question_mark_ctx_.by_name_ = true;
    if (OB_UNLIKELY(parse_ctx->question_mark_ctx_.by_ordinal_)) {
      YY_FATAL_ERROR("Ordinal binding and Named binding cannot be combined\n");
    }
    yylval->node->raw_text_ = parse_strdup(yytext, parse_ctx->mem_pool_, &(node->str_len_));
    yylval->node->value_ = get_question_mark(&(parse_ctx->question_mark_ctx_), parse_ctx->mem_pool_, node->raw_text_);
    return QUESTIONMARK;
  }
}

":"{int_num} {
  ObParseCtx *parse_ctx = (ObParseCtx *)yyextra;
  ParseNode *node = NULL;
  malloc_new_node(node, parse_ctx->mem_pool_, T_QUESTIONMARK, 0);
  yylval->node = node;
  parse_ctx->question_mark_ctx_.by_name_ = true;
  if (OB_UNLIKELY(parse_ctx->question_mark_ctx_.by_ordinal_)) {
    YY_FATAL_ERROR("Ordinal binding and Named binding cannot be combined\n");
  }
  yylval->node->raw_text_ = parse_strdup(yytext, parse_ctx->mem_pool_, &(node->str_len_));
  yylval->node->value_ = get_question_mark(&(parse_ctx->question_mark_ctx_), parse_ctx->mem_pool_, node->raw_text_);
  return QUESTIONMARK;
}

{identifer} {
  int32_t token = oracle_sql_reserved_keyword_lookup(yytext);
  if (token < 0) {
    const NonReservedKeyword *word = NULL;
    if (NULL == (word = oracle_pl_non_reserved_keyword_lookup(yytext)))
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
        //fprintf(stderr, "parse identifier result, str=%s, str_len=%ld\n", node->str_value_, node->str_len_);
      } else {
        node->str_value_ = parse_strdup(yytext, parse_ctx->mem_pool_, &(node->str_len_));
      }
      node->str_value_ = str_toupper((char *)node->str_value_, node->str_len_);
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

<<EOF>> { return END_P; }
. { return yytext[0]; }
%%

//parser function
void obpl_oracle_parse_fatal_error(int32_t errcode, yyscan_t yyscanner, yyconst char *msg, ...)
{
  ObParseCtx *parse_ctx = obpl_oracle_yyget_extra(yyscanner);
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
  UNUSED(yyscanner);
  /* Do nothing -- we leave it to the garbage collector. */
  parse_free(ptr);
}
