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
/* unput() change the yyin but it doesn't change ParserResult->input_sql_.
// use unput() function may have unexpected result while copy string.
*/
%option noyywrap nounput noinput case-insensitive
%option noyyalloc noyyrealloc noyyfree
%option reentrant bison-bridge bison-locations
%option prefix="obsql_mysql_yy"
%option header-file="../../../src/sql/parser/sql_parser_mysql_mode_lex.h"
%{
#include "sql_parser_base.h"
#include "sql_parser_mysql_mode_tab.h"

extern void obsql_mysql_yyerror(YYLTYPE *yylloc, ParseResult *p, char *s,...);
extern void obsql_mysql_parser_fatal_error(int32_t errcoyyde, yyscan_t yyscanner, yyconst char *msg, ...);
#define yyerror obsql_mysql_yyerror
#define YY_FATAL_ERROR(msg, args...) (obsql_mysql_parser_fatal_error(OB_PARSER_ERR_NO_MEMORY, yyscanner, msg, ##args))
#define YY_UNEXPECTED_ERROR(msg, args...) (obsql_mysql_parser_fatal_error(OB_PARSER_ERR_UNEXPECTED, yyscanner, msg, ##args))
%}

%x hint
%x in_c_comment
%x sq
%x hint_sq
%x dq
%x bt
%x hint_bt
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
sql_comment      ("--"[ \t]+{non_newline}*)|(#{non_newline}*|"--"[\n\r])
whitespace       ({space}+|{sql_comment})
select_hint_begin       (select{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
update_hint_begin       (update{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
delete_hint_begin       (delete{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
insert_hint_begin       (insert{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
replace_hint_begin      (replace{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
load_data_hint_begin    (load{space}+data{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
create_hint_begin    (create{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
hint_hint_begin      (hint{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
common_hint_begin (\/\*\+({space}*hint{space}+)?)
c_cmt_begin      \/\*
c_cmt_end        \*+\/
comment          ({sql_comment})
identifier        (([A-Za-z0-9$_]|{UTF8_GB_CHAR})+)
system_variable  (@@[A-Za-z_][A-Za-z0-9_]*)|(@@[`][`A-Za-z_][`A-Za-z_]*)
user_variable    (@[A-Za-z0-9_\.$]*)|(@[`'\"][`'\"A-Za-z0-9_\.$/%]*)
version_num      ([0-9]+\.+[0-9]*)
int_num          [0-9]+
client_version   \([0-9]+(\.[0-9]+)*\)

quote         '
sqbegin       {quote}
sqend         {quote}
sqdouble      {quote}{quote}
sqcontent     [^\\\n\r']+
qescape       [\\](.|\n)
sqnewline     {quote}{whitespace}{quote}

dquote         \"
dqbegin       {dquote}
dqend         {dquote}
dqdouble      {dquote}{dquote}
dqcontent     [^\\\n\r"]+
adqcontent    [^"]+
dqnewline     {dquote}{whitespace}{dquote}

backtick      `
btbegin       {backtick}
btend         {backtick}
btdouble      {backtick}{backtick}
btcontent     [^`]+

mysql_compatible_comment_with_version \/\*\![0-9]{5}
mysql_compatible_comment_without_version \/\*\!
mysql_compatible_comment_end \*\/

rowidPattern  (WITH{whitespace}ROWID)

%%
{rowidPattern}                { REPUT_TOKEN_NEG_SIGN(WITH_ROWID); }
INTERVAL { 
  ParseResult *p = (ParseResult *)yyextra;
  REPUT_NEG_SIGN(p); 
  return INTERVAL; 
}
@@global                      { REPUT_TOKEN_NEG_SIGN(GLOBAL_ALIAS); }
@@session                     { REPUT_TOKEN_NEG_SIGN(SESSION_ALIAS); }
@@local                       { REPUT_TOKEN_NEG_SIGN(SESSION_ALIAS); }

_UTF8                         { REPUT_TOKEN_NEG_SIGN(_UTF8); }
_UTF8MB4                      { REPUT_TOKEN_NEG_SIGN(_UTF8MB4); }
_GBK                          { REPUT_TOKEN_NEG_SIGN(_GBK); }
_GB18030                      { REPUT_TOKEN_NEG_SIGN(_GB18030); }
_GB18030_2022                 { REPUT_TOKEN_NEG_SIGN(_GB18030_2022); }
_LATIN1                       { REPUT_TOKEN_NEG_SIGN(_LATIN1); }
_BINARY                       { REPUT_TOKEN_NEG_SIGN(_BINARY); }
_UTF16                        { REPUT_TOKEN_NEG_SIGN(_UTF16); }
NOT {
  int32_t token_ret = NOT; /*fast parameterize don't care NOT or NOT2*/
  if (!(IS_FAST_PARAMETERIZE)) {
    ObSQLMode mode = ((ParseResult *)yyextra)->sql_mode_;
    bool is_high_not_procedence = false;
      IS_HIGH_NOT_PRECEDENCE(mode, is_high_not_procedence);
    if (is_high_not_procedence) {
      token_ret = NOT2;
    } else {
      token_ret = NOT;
    }
  }
  return token_ret;
}
NULL {
  check_value(yylval);
  ParseResult *p = (ParseResult *)yyextra;
  malloc_new_node(yylval->node, p->malloc_pool_, T_NULL, 0);
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    yylval->node->sql_str_off_ = yylloc->first_column - 1;
    return NULLX;
  }
}

{int_num} {
  int32_t token_ret = INTNUM;
  ParseNode *node = NULL;
  check_value(yylval);
  ParseResult *p = (ParseResult *)yyextra;
  malloc_new_node(node, p->malloc_pool_, T_INT, 0);
  yylval->node = node;
  int err_no = 0;
  COPY_NUM_STRING(p, node);
  // we treated '-  12' as a const node, that is to say node->str_value_ = '-  12'
  // however `strtoll` can't parse '-  12' to a int value, for there are spaces between neg sign and numbers
  // so here, we have to strip spaces
  PARSE_INT_STR_MYSQL(node, p->malloc_pool_, err_no);
  node->raw_text_ = node->str_value_;
  node->text_len_ = node->str_len_;
  p->minus_ctx_.is_cur_numeric_ = true;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    node->sql_str_off_ = yylloc->first_column - 1;
    setup_token_pos_info(node, yylloc->first_column - 1, node->text_len_);
    return token_ret;
  }
}

<hint>{int_num} {
  ParseResult *p = (ParseResult *)yyextra;
  if (!p->is_ignore_token_) {
    if (IS_FAST_PARAMETERIZE) {
      COPY_WRITE();
    } else {
      int32_t token_ret = INTNUM;
      ParseNode *node = NULL;
      ParseResult *p = (ParseResult *)yyextra;
      malloc_new_node(node, p->malloc_pool_, T_INT, 0);
      check_value(yylval);
      yylval->node = node;
      node->str_value_ = parse_strdup(yytext, p->malloc_pool_, &(node->str_len_));
      check_malloc(node->str_value_);
      int err_no = 0;
      node->value_ = ob_strntoll(node->str_value_, node->str_len_, 10, NULL, &err_no);
      if (ERANGE == err_no)
      {
        /* if out of range, seem it as must NUMERIC type, now is double */
        node->type_ = T_NUMBER;
        token_ret = DECIMAL_VAL;
      }
      return token_ret;
    }
  }
}

[0-9]+E[-+]?[0-9]+ |
[0-9]+"."[0-9]*E[-+]?[0-9]+ |
"."[0-9]+E[-+]?[0-9]+ {
  ParseNode *node = NULL;
  ParseResult *p = (ParseResult *)yyextra;
  malloc_new_node(node, p->malloc_pool_, T_DOUBLE, 0);
  check_value(yylval);
  yylval->node = node;
  COPY_NUM_STRING(p, node);
  node->raw_text_ = node->str_value_;
  node->text_len_ = node->str_len_;
  p->minus_ctx_.is_cur_numeric_ = true;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    node->sql_str_off_ = yylloc->first_column - 1;
    return DECIMAL_VAL;
  }
}

[0-9]+"."[0-9]* |
"."[0-9]+ {
  ParseNode *node = NULL;
  ParseResult *p = (ParseResult *)yyextra;
  malloc_new_node(node, p->malloc_pool_, T_NUMBER/* should be T_NUMBER,*/, 0);
  check_value(yylval);
  yylval->node = node;
  COPY_NUM_STRING(p, node);
  node->raw_text_ = node->str_value_;
  node->text_len_ = node->str_len_;
  p->minus_ctx_.is_cur_numeric_ = true;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    node->sql_str_off_ = yylloc->first_column - 1;
    return DECIMAL_VAL;
  }
}

TRUE {
  check_value(yylval);
  ParseResult *p = (ParseResult *)yyextra;
  malloc_new_node(yylval->node, p->malloc_pool_, T_BOOL, 0);
  yylval->node->value_ = 1;
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    yylval->node->sql_str_off_ = yylloc->first_column - 1;
    return BOOL_VALUE;
  }
}

<hint>TRUE {
  check_value(yylval);
  ParseResult *p = (ParseResult *)yyextra;
  malloc_new_node(yylval->node, p->malloc_pool_, T_BOOL, 0);
  yylval->node->value_ = 1;
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  return BOOL_VALUE;
}

FALSE {
  check_value(yylval);
  ParseResult *p = (ParseResult *)yyextra;
  malloc_new_node(yylval->node, p->malloc_pool_, T_BOOL, 0);
  yylval->node->value_ = 0;
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    yylval->node->sql_str_off_ = yylloc->first_column - 1;
    return BOOL_VALUE;
  }
}

<hint>FALSE {
  check_value(yylval);
  ParseResult *p = (ParseResult *)yyextra;
  malloc_new_node(yylval->node, p->malloc_pool_, T_BOOL, 0);
  yylval->node->value_ = 0;
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  return BOOL_VALUE;
}

("N"|"n"){sqbegin} {
  BEGIN(sq);
  ParseResult *p = (ParseResult *)yyextra;
  p->start_col_ = yylloc->first_column;
  char **tmp_literal = &(p->tmp_literal_);
  if (NULL == *tmp_literal)
  {
    *tmp_literal = (char*) parse_malloc(p->input_sql_len_ + 1, p->malloc_pool_);
    check_malloc(*tmp_literal);
  }
  check_value(yylval);
  malloc_new_node(yylval->node, p->malloc_pool_, T_NCHAR, 0);

  yylval->node->str_len_ = 0;
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    COPY_WRITE();
  }
}


{sqbegin} {
  BEGIN(sq);
  ParseResult *p = (ParseResult *)yyextra;
  p->start_col_ = yylloc->first_column;
  check_value(yylval);
  malloc_new_node(yylval->node, p->malloc_pool_, T_VARCHAR, 0);
  yylval->node->str_len_ = 0;
  p->last_escape_check_pos_ = 0;
  yylval->node->str_value_ = NULL;
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    COPY_WRITE();
  }
}

<sq>{sqend} { // fast parameterize copy text here, don't return token
  BEGIN(INITIAL);
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  yylloc->first_column = p->start_col_;
  FORMAT_STR_NODE(yylval->node);
  int32_t token_ret = yylval->node->type_ == T_NCHAR ? NATIONAL_LITERAL : STRING_VALUE;
  yylval->node->text_len_ = yylloc->last_column - p->start_col_ + 1;
  COPY_STRING(p->input_sql_ + p->start_col_ - 1, yylval->node->text_len_, yylval->node->raw_text_);
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    yylval->node->sql_str_off_ = yylloc->first_column;
    setup_token_pos_info(yylval->node, yylloc->first_column, yylval->node->text_len_ - 2);
    return token_ret;
  }
}

<sq>{sqdouble} {
  check_value(yylval);
  COPY_STR_NODE_TO_TMP_LITERAL(yylval->node);
  ((ParseResult *)yyextra)->tmp_literal_[yylval->node->str_len_++] = '\'';
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    COPY_WRITE();
  }
}

<sq>{sqcontent} {
  check_value(yylval);
  STORE_STR_CONTENT(yylval->node);
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    COPY_WRITE();
  }
}

<sq>[\n\r] {
  check_value(yylval);
  ++yylineno;
  STORE_STR_CONTENT(yylval->node);
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    COPY_WRITE();
  }
}

<sq>{qescape} {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  COPY_STR_NODE_TO_TMP_LITERAL(yylval->node);
  bool is_real_escape = true;
  CHECK_REAL_ESCAPE(is_real_escape);
  ObSQLMode mode = p->sql_mode_;
  bool is_no_backslash_escapes = false;
  IS_NO_BACKSLASH_ESCAPES(mode, is_no_backslash_escapes);
  if (!is_real_escape || is_no_backslash_escapes) {
    HANDLE_FALSE_ESCAPE(p);
  } else {
    HANDLE_ESCAPE(p);
  }
  p->last_escape_check_pos_ = yylval->node->str_len_;
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    COPY_WRITE();
  }
}

<sq>{sqnewline} {
  /*TODO shengle fast parameterize*/
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
  check_value(yylval);
  ADD_YYLINENO(yytext, yyleng);
  COPY_STR_NODE_TO_TMP_LITERAL(yylval->node);
  if (0 == yylval->node->num_child_) {
    ParseResult *p = (ParseResult *)yyextra;
    char *tmp_literal = p->tmp_literal_;
    tmp_literal[yylval->node->str_len_] = '\0';
    yylval->node->children_ = (ParseNode **)parse_malloc(sizeof(ParseNode *), p->malloc_pool_);
    if (OB_UNLIKELY(NULL == yylval->node->children_)) {
      p->extra_errno_ = OB_PARSER_ERR_NO_MEMORY;
      yyerror(yylloc, yyextra, "No more space for mallocing '%s'\n", yytext);
      return ERROR;
    }

    malloc_new_node(yylval->node->children_[0], p->malloc_pool_, T_CONCAT_STRING, 0);
    (*yylval->node->children_)->str_value_ = parse_strndup(tmp_literal, yylval->node->str_len_ + 1,
                                            p->malloc_pool_);
    check_malloc((*yylval->node->children_)->str_value_);
    (*yylval->node->children_)->str_len_ = yylval->node->str_len_;
    yylval->node->num_child_ = 1;
  }
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    COPY_WRITE();
  }
}

<sq><<EOF>>  {
  yyerror(yylloc, yyextra, "unterminated quoted string\n");
  return PARSER_SYNTAX_ERROR;
}

{dqbegin} {
  ParseResult *p = (ParseResult *)yyextra;
  ObSQLMode mode = p->sql_mode_;
  bool is_ansi_quotes = false;
  IS_ANSI_QUOTES(mode, is_ansi_quotes);
  if (is_ansi_quotes) {
    BEGIN(adq); 
    if (IS_FAST_PARAMETERIZE) {
      COPY_WRITE();
    } else {
      check_value(yylval);
      p->start_col_ = yylloc->first_column;
      char **tmp_literal = &p->tmp_literal_;
      if (*tmp_literal == NULL)
      {
        *tmp_literal = (char*) parse_malloc(p->input_sql_len_ + 1, p->malloc_pool_);
        check_malloc(*tmp_literal);
      }
      malloc_new_node(yylval->node, p->malloc_pool_, T_IDENT, 0);
      yylval->node->str_len_ = 0;
    }
  } else {
    BEGIN(dq);
    p->start_col_ = yylloc->first_column;
    char **tmp_literal = &p->tmp_literal_;
    if (*tmp_literal == NULL)
    {
      *tmp_literal = (char*) parse_malloc(p->input_sql_len_ + 1, p->malloc_pool_);
      check_malloc(*tmp_literal);
    }
    malloc_new_node(yylval->node, p->malloc_pool_, T_VARCHAR, 0);
    yylval->node->str_len_ = 0;
    p->last_escape_check_pos_ = 0;
    if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
      return OUTLINE_DEFAULT_TOKEN;
    }
  }
}

<dq>{dqend} {
  BEGIN(INITIAL);
  ParseResult *p = (ParseResult *)yyextra;
  yylloc->first_column = p->start_col_;
  char *tmp_literal = p->tmp_literal_;
  check_value(yylval);
  tmp_literal[yylval->node->str_len_] = '\0';
  yylval->node->str_value_ = parse_strndup(tmp_literal, yylval->node->str_len_ + 1, p->malloc_pool_);
  check_malloc(yylval->node->str_value_);
  yylval->node->text_len_ = yylloc->last_column - p->start_col_ + 1;
  yylval->node->raw_text_ = parse_strndup(p->input_sql_ + p->start_col_ - 1, yylval->node->text_len_, p->malloc_pool_);
  check_malloc(yylval->node->raw_text_);
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    yylval->node->sql_str_off_ = yylloc->first_column;
    setup_token_pos_info(yylval->node, yylloc->first_column, yylval->node->str_len_);
    return STRING_VALUE;
  }
}

<dq>{dqdouble} {
  check_value(yylval);
  ((ParseResult *)yyextra)->tmp_literal_[yylval->node->str_len_++] = '\"';
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    return OUTLINE_DEFAULT_TOKEN;
  }
}

<dq>{dqcontent} {
  check_value(yylval);
  memmove(((ParseResult *)yyextra)->tmp_literal_ + yylval->node->str_len_, yytext, yyleng);
  yylval->node->str_len_ += yyleng;
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    return OUTLINE_DEFAULT_TOKEN;
  }
}

<dq>[\n\r] {
  check_value(yylval);
  ++yylineno;
  memmove(((ParseResult *)yyextra)->tmp_literal_ + yylval->node->str_len_, yytext, yyleng);
  yylval->node->str_len_ += yyleng;
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    return OUTLINE_DEFAULT_TOKEN;
  }
}

<dq>{qescape} {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  bool is_real_escape = true;
  CHECK_REAL_ESCAPE(is_real_escape);
  //check sql_mode
  ObSQLMode mode = p->sql_mode_;
  bool is_no_backslash_escapes = false;
  IS_NO_BACKSLASH_ESCAPES(mode, is_no_backslash_escapes);
  if (!is_real_escape || is_no_backslash_escapes) {
    HANDLE_FALSE_ESCAPE(p);
  } else {
    HANDLE_ESCAPE(p);
  }
  p->last_escape_check_pos_ = yylval->node->str_len_;
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    return OUTLINE_DEFAULT_TOKEN;
  }
}


<dq>{dqnewline} {
  /* see 'sqnewline' */
  check_value(yylval);
  ADD_YYLINENO(yytext, yyleng);
  if (0 == yylval->node->num_child_) {
    ParseResult *p = (ParseResult *)yyextra;
    char *tmp_literal = p->tmp_literal_;
    tmp_literal[yylval->node->str_len_] = '\0';

    yylval->node->children_ = (ParseNode **)parse_malloc(sizeof(ParseNode *), p->malloc_pool_);
    if (OB_UNLIKELY(NULL == yylval->node->children_)) {
      p->extra_errno_ = OB_PARSER_ERR_NO_MEMORY;
      yyerror(yylloc, yyextra, "No more space for mallocing '%s'\n", yytext);
      return ERROR;
    }

    malloc_new_node(yylval->node->children_[0], p->malloc_pool_, T_CONCAT_STRING, 0);
    (*yylval->node->children_)->str_value_ = parse_strndup(tmp_literal, yylval->node->str_len_ + 1,
                                            p->malloc_pool_);
    check_malloc((*yylval->node->children_)->str_value_);
    (*yylval->node->children_)->str_len_ = yylval->node->str_len_;
    yylval->node->num_child_ = 1;
  }
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    return OUTLINE_DEFAULT_TOKEN;
  }
}

<dq><<EOF>>  {
  yyerror(yylloc, yyextra, "unterminated doublequoted string\n");
  return PARSER_SYNTAX_ERROR;
}

<adq>{dqdouble} {
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    check_value(yylval);
    ((ParseResult *)yyextra)->tmp_literal_[yylval->node->str_len_++] = '"';
  }
}

<adq>{adqcontent} {
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    check_value(yylval);
    memmove(((ParseResult *)yyextra)->tmp_literal_ + yylval->node->str_len_, yytext, yyleng);
    yylval->node->str_len_ += yyleng;
  }
}

<adq>{dqend} {
  BEGIN(INITIAL);
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    ParseResult *p = (ParseResult *)yyextra;
    check_value(yylval);
    yylloc->first_column = p->start_col_;
    char *tmp_literal = p->tmp_literal_;
    tmp_literal[yylval->node->str_len_] = '\0';

    char *dup_value = NULL;
    if (p->is_not_utf8_connection_) {
      dup_value = parse_str_convert_utf8(p->charset_info_, tmp_literal, p->malloc_pool_, &(yylval->node->str_len_), &(p->extra_errno_));
      check_identifier_convert_result(p->extra_errno_);
      //fprintf(stderr, "parse identifier result, str=%s, str_len=%ld\n", node->str_value_, node->str_len_);
    } else {
      dup_value = parse_strndup(tmp_literal, yylval->node->str_len_ + 1, p->malloc_pool_);
    }
    check_malloc(dup_value);
    yylval->node->str_value_ = dup_value;
    yylval->node->is_input_quoted_ = 1;
    yylval->node->sql_str_off_ = yylloc->first_column;
    setup_token_pos_info(yylval->node, yylloc->first_column, yylval->node->str_len_);
    return NAME_OB;
  }
}

<adq><<EOF>> {
  yyerror(yylloc, yyextra, "unterminated backtick string\n");
  return PARSER_SYNTAX_ERROR;
}

{btbegin} {
  BEGIN(bt); /*fast parameterize don't handle connent in ``*/
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    check_value(yylval);
    ParseResult *p = (ParseResult *)yyextra;
    p->start_col_ = yylloc->first_column;
    char **tmp_literal = &p->tmp_literal_;
    if (NULL == *tmp_literal)
    {
      *tmp_literal = (char *)parse_malloc(p->input_sql_len_ + 1, p->malloc_pool_);
      check_malloc(*tmp_literal);
    }
    malloc_new_node(yylval->node, p->malloc_pool_, T_IDENT, 0);
    yylval->node->str_len_ = 0;
  }
}

<bt>{btdouble} {
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    check_value(yylval);
    ((ParseResult *)yyextra)->tmp_literal_[yylval->node->str_len_++] = '`';
  }
}
<bt>{btcontent} {
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    check_value(yylval);
    memmove(((ParseResult *)yyextra)->tmp_literal_ + yylval->node->str_len_, yytext, yyleng);
    yylval->node->str_len_ += yyleng;
  }
}

<bt>{btend} {
  BEGIN(INITIAL);
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    ParseResult *p = (ParseResult *)yyextra;
    check_value(yylval);
    yylloc->first_column = p->start_col_;
    char *tmp_literal = p->tmp_literal_;
    tmp_literal[yylval->node->str_len_] = '\0';
    CHECK_STR_LEN_MATCH(tmp_literal, yylval->node->str_len_);

    char *dup_value = NULL;
    if (p->is_not_utf8_connection_) {
      dup_value = parse_str_convert_utf8(p->charset_info_, tmp_literal, p->malloc_pool_, &(yylval->node->str_len_), &(p->extra_errno_));
      check_identifier_convert_result(p->extra_errno_);
      //fprintf(stderr, "parse identifier result, str=%s, str_len=%ld\n", node->str_value_, node->str_len_);
    } else {
      dup_value = parse_strndup(tmp_literal, yylval->node->str_len_ + 1, p->malloc_pool_);
    }
    check_malloc(dup_value);
    yylval->node->str_value_ = dup_value;
    yylval->node->is_input_quoted_ = 1;
    yylval->node->sql_str_off_ = yylloc->first_column;
    setup_token_pos_info(yylval->node, yylloc->first_column, yylval->node->str_len_);
    return NAME_OB;
  }
}

<bt><<EOF>>  {
  yyerror(yylloc, yyextra, "unterminated backtick string\n");
  return PARSER_SYNTAX_ERROR;
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
      yyerror(yylloc, yyextra, "hex string contain an even number of digits\n");
      return PARSER_SYNTAX_ERROR;
    }
  } else {
    // Values written using 0xval notation NOTE: 0Xval (use upper case 'X') notation is illegal in MySQL
    if (yytext[1] == 'X') {
      yyerror(yylloc, yyextra, "hex string with leading '0X' (use upper case 'X') is illegal\n");
      return PARSER_SYNTAX_ERROR;
    }
  }
  ParseNode *node = NULL;
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  malloc_new_node(node, p->malloc_pool_, T_HEX_STRING, 0);
  if (len > 0) {
    int64_t dest_len = ob_parse_binary_len(len);
    char *dest = (char *)parse_malloc(dest_len, p->malloc_pool_);
    check_malloc(dest);
    ob_parse_binary(src, len, dest);
    node->str_value_ = dest;
    node->str_len_ = dest_len;
  }
  else
  {
    node->str_value_ = NULL;
    node->str_len_ = 0;
  }
  yylval->node = node;
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  yylval->node->is_copy_raw_text_ = 1;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    yylval->node->sql_str_off_ = yylloc->first_column - 1;
    return HEX_STRING_VALUE;
  }
}

B'([01])*'|0B([01])+ {
  char* src = yytext + 2;
  size_t len = yyleng - 2;
  if(src[len - 1] == '\'')
  {
    --len;
  } else {
    // Values written using 0bval notation NOTE: 0Bval (use upper case 'B') notation is illegal in MySQL
    if (yytext[1] == 'B') {
      yyerror(yylloc, yyextra, "bit string with leading '0B' (use upper case 'B') is illegal\n");
      return PARSER_SYNTAX_ERROR;
    }
  }
  ParseNode *node = NULL;
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  malloc_new_node(node, p->malloc_pool_, T_HEX_STRING, 0);
  if (len > 0)
  {
    int64_t dest_len = ob_parse_bit_string_len(len);
    char *dest = (char*)parse_malloc(dest_len, p->malloc_pool_);
    check_malloc(dest);
    ob_parse_bit_string(src, len, dest);
    node->str_value_ = dest;
    node->str_len_ = dest_len;
  }
  else
  {
    node->str_value_ = NULL;
    node->str_len_ = 0;
  }
  yylval->node = node;
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  yylval->node->is_copy_raw_text_ = 1;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    yylval->node->sql_str_off_ = yylloc->first_column - 1;
    return HEX_STRING_VALUE;
  }
}

Date{whitespace}?'[^']*' {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  malloc_time_node_s(p->malloc_pool_, T_DATE);
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  yylval->node->is_copy_raw_text_ = 1;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
   yylval->node->sql_str_off_ = yylloc->first_column - 1;
   return DATE_VALUE;
  }
}

Time{whitespace}?'[^']*' {
  ParseResult *p = (ParseResult *)yyextra;
  malloc_time_node_s(p->malloc_pool_, T_TIME);
  check_value(yylval);
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  yylval->node->is_copy_raw_text_ = 1;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    yylval->node->sql_str_off_ = yylloc->first_column - 1;
    return DATE_VALUE;
  }
}

Timestamp{whitespace}?'[^']*' {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  malloc_time_node_s(p->malloc_pool_, T_TIMESTAMP);
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  yylval->node->is_copy_raw_text_ = 1;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    yylval->node->sql_str_off_ = yylloc->first_column - 1;
    return DATE_VALUE;
  }
}
Date{whitespace}?\"[^\"]*\" {
  ParseResult *p = (ParseResult *)yyextra;
  malloc_time_node_d(p->malloc_pool_, T_DATE);
  check_value(yylval);
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  yylval->node->is_copy_raw_text_ = 1;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    yylval->node->sql_str_off_ = yylloc->first_column - 1;
    return DATE_VALUE;
  }
}

Time{whitespace}?\"[^\"]*\" {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  malloc_time_node_d(p->malloc_pool_, T_TIME);
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  yylval->node->is_copy_raw_text_ = 1;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    yylval->node->sql_str_off_ = yylloc->first_column - 1;
    return DATE_VALUE;
  }
}

Timestamp{whitespace}?\"[^\"]*\" {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  malloc_time_node_d(p->malloc_pool_, T_TIMESTAMP);
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  yylval->node->is_copy_raw_text_ = 1;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    yylval->node->sql_str_off_ = yylloc->first_column - 1;
    return DATE_VALUE;
  }
}

\/\*HINT\+[^*]+\*\/    {
  if (!(IS_FAST_PARAMETERIZE)) {
    int64_t out_len = 0;
    char *dest = NULL;
    check_value(yylval);
    /* ParseNode *node = new_node(((ParseResult *)yyextra)->malloc_pool_, T_HINT, 0); */
    ParseNode *node = NULL;
    ParseResult *p = (ParseResult *)yyextra;
    malloc_new_node(node, p->malloc_pool_, T_HINT, 0);
    dest = parse_strdup(yytext + 7, p->malloc_pool_, &out_len);
    check_malloc(dest);
    dest[out_len - 2] = '\0';

    node->str_value_ = dest;
    node->str_len_ = out_len - 1;

    yylval->node = node;
  }
  return HINT_VALUE;
}

{select_hint_begin} {
  BEGIN(hint);
  ParseResult *p = (ParseResult *)yyextra;
  p->is_ignore_token_ = p->is_ignore_hint_;
  return SELECT_HINT_BEGIN;
}

{update_hint_begin} {
  BEGIN(hint);
  ParseResult *p = (ParseResult *)yyextra;
  p->is_ignore_token_ = p->is_ignore_hint_;
  return UPDATE_HINT_BEGIN;
}
{delete_hint_begin} {
  BEGIN(hint);
  ParseResult *p = (ParseResult *)yyextra;
  p->is_ignore_token_ = p->is_ignore_hint_;
  return DELETE_HINT_BEGIN;
}
{insert_hint_begin} {
  BEGIN(hint);
  ParseResult *p = (ParseResult *)yyextra;
  p->is_ignore_token_ = p->is_ignore_hint_;
  return INSERT_HINT_BEGIN;
}
{replace_hint_begin} {
  BEGIN(hint);
  ParseResult *p = (ParseResult *)yyextra;
  p->is_ignore_token_ = p->is_ignore_hint_;
  return REPLACE_HINT_BEGIN;
}
{hint_hint_begin} {
  BEGIN(hint);
  ParseResult *p = (ParseResult *)yyextra;
  p->is_ignore_token_ = p->is_ignore_hint_;
  return HINT_HINT_BEGIN;
}
{load_data_hint_begin} {
  BEGIN(hint);
  ParseResult *p = (ParseResult *)yyextra;
  p->is_ignore_token_ = p->is_ignore_hint_;
  return LOAD_DATA_HINT_BEGIN;
}
{create_hint_begin} {
  BEGIN(hint);
  ParseResult *p = (ParseResult *)yyextra;
  p->is_ignore_token_ = p->is_ignore_hint_;
  return CREATE_HINT_BEGIN;
}
<hint>{c_cmt_end} {
  BEGIN(INITIAL);
  return HINT_END;
}
<hint>[ \t\r\n] {
  ParseResult *p = (ParseResult *)yyextra;
  if (IS_FAST_PARAMETERIZE && !p->is_ignore_token_) {
    COPY_WRITE();
  }
}
<hint>INDEX { return INDEX_HINT; }
<hint>NO_INDEX { return NO_INDEX_HINT; }
<hint>USE_DAS { return USE_DAS_HINT; }
<hint>NO_USE_DAS { return NO_USE_DAS_HINT; }
<hint>INDEX_SS { return INDEX_SS_HINT; }
<hint>INDEX_SS_ASC { return INDEX_SS_ASC_HINT; }
<hint>INDEX_SS_DESC { return INDEX_SS_DESC_HINT; }
<hint>USE_NL { return USE_NL; }
<hint>NO_USE_NL { return NO_USE_NL; }
<hint>USE_NL_MATERIALIZATION { return USE_NL_MATERIALIZATION; }
<hint>NO_USE_NL_MATERIALIZATION { return NO_USE_NL_MATERIALIZATION; }
<hint>FROZEN_VERSION { return FROZEN_VERSION; }
<hint>TOPK { return TOPK; }
<hint>QUERY_TIMEOUT { return QUERY_TIMEOUT; }
<hint>READ_CONSISTENCY  { return READ_CONSISTENCY; }
<hint>WEAK  { return WEAK; }
<hint>STRONG  { return STRONG; }
<hint>FROZEN  { return FROZEN; }
<hint>NONE  { return NONE; }
<hint>DEFAULT  { return DEFAULT; }
<hint>MAX_CONCURRENT { return MAX_CONCURRENT; }
<hint>PARALLEL { return PARALLEL; }
<hint>MANUAL { return MANUAL; }
<hint>NO_PARALLEL { return NO_PARALLEL; }
<hint>MONITOR  { return MONITOR; }
<hint>AUTO { return AUTO; }
<hint>FORCE { return FORCE; }
<hint>[(),.@]  {
  ParseResult *p = (ParseResult *)yyextra;
  if (!p->is_ignore_token_) {
    if (IS_FAST_PARAMETERIZE) {
      if (!p->is_ignore_token_) {
        COPY_WRITE();
      }
    } else {
      return yytext[0];
    }
  }
}
<hint>NO_GATHER_OPTIMIZER_STATISTICS  { return NO_GATHER_OPTIMIZER_STATISTICS; }
<hint>GATHER_OPTIMIZER_STATISTICS  { return GATHER_OPTIMIZER_STATISTICS; }
<hint>DBMS_STATS  { return DBMS_STATS; }
<hint>LOG_LEVEL  { return LOG_LEVEL; }
<hint>LEADING  { return LEADING_HINT; }
<hint>ORDERED  { return ORDERED; }
<hint>NO_REWRITE { return NO_REWRITE; }
<hint>FULL  { return FULL_HINT; }
<hint>USE_MERGE  { return USE_MERGE; }
<hint>NO_USE_MERGE  { return NO_USE_MERGE; }
<hint>USE_HASH  { return USE_HASH; }
<hint>NO_USE_HASH  { return NO_USE_HASH; }
<hint>USE_PLAN_CACHE  { return USE_PLAN_CACHE; }
<hint>USE_HASH_AGGREGATION  { return USE_HASH_AGGREGATION; }
<hint>NO_USE_HASH_AGGREGATION  { return NO_USE_HASH_AGGREGATION; }
<hint>PARTITION_SORT  { return PARTITION_SORT; }
<hint>NO_PARTITION_SORT  { return NO_PARTITION_SORT; }
<hint>USE_LATE_MATERIALIZATION  { return USE_LATE_MATERIALIZATION; }
<hint>NO_USE_LATE_MATERIALIZATION  { return NO_USE_LATE_MATERIALIZATION; }
<hint>TRACE_LOG { return TRACE_LOG; }
<hint>LOAD_BATCH_SIZE { return LOAD_BATCH_SIZE; }
<hint>DIRECT { return DIRECT; }
<hint>APPEND { return APPEND; }
<hint>TRACING { return TRACING; }
<hint>DOP { return DOP; }
<hint>FORCE_REFRESH_LOCATION_CACHE { return FORCE_REFRESH_LOCATION_CACHE; }
<hint>STAT { return STAT; }
<hint>PX_JOIN_FILTER { return PX_JOIN_FILTER; }
<hint>NO_PX_JOIN_FILTER { return NO_PX_JOIN_FILTER; }
<hint>PX_PART_JOIN_FILTER { return PX_PART_JOIN_FILTER; }
<hint>NO_PX_PART_JOIN_FILTER { return NO_PX_PART_JOIN_FILTER; }
<hint>QB_NAME { return QB_NAME; }
<hint>BEGIN_OUTLINE_DATA { return BEGIN_OUTLINE_DATA; }
<hint>END_OUTLINE_DATA { return END_OUTLINE_DATA; }
<hint>OPTIMIZER_FEATURES_ENABLE { return OPTIMIZER_FEATURES_ENABLE; }
<hint>NO_QUERY_TRANSFORMATION { return NO_QUERY_TRANSFORMATION; }
<hint>NO_COST_BASED_QUERY_TRANSFORMATION { return NO_COST_BASED_QUERY_TRANSFORMATION; }
<hint>FLASHBACK_READ_TX_UNCOMMITTED { return FLASHBACK_READ_TX_UNCOMMITTED; }
<hint>TRANS_PARAM { return TRANS_PARAM; }
<hint>PQ_DISTRIBUTE { return PQ_DISTRIBUTE; }
<hint>PQ_DISTRIBUTE_WINDOW { return PQ_DISTRIBUTE_WINDOW; }
<hint>PQ_MAP { return PQ_MAP; }
<hint>PQ_SET { return PQ_SET; }
<hint>RANDOM_LOCAL { return RANDOM_LOCAL; }
<hint>RANDOM { return RANDOM; }
<hint>BROADCAST { return BROADCAST; }
<hint>PARTITION { return PARTITION; }
<hint>ALL { return ALL; }
<hint>HASH { return HASH; }
<hint>LOCAL { return LOCAL; }
<hint>BC2HOST { return BC2HOST; }
<hint>RANGE { return RANGE; }
<hint>LIST { return LIST; }
<hint>[-] { return NEG_SIGN; }
<hint>MERGE { return MERGE_HINT; }
<hint>NO_MERGE { return NO_MERGE_HINT; }
<hint>[>] { return COMP_GT; }
<hint>[<] { return COMP_LT; }
<hint>NO_EXPAND { return NO_EXPAND; }
<hint>USE_CONCAT { return USE_CONCAT; }
<hint>UNNEST { return UNNEST; }
<hint>NO_UNNEST { return NO_UNNEST; }
<hint>PLACE_GROUP_BY { return PLACE_GROUP_BY; }
<hint>NO_PLACE_GROUP_BY { return NO_PLACE_GROUP_BY; }
<hint>PRED_DEDUCE { return PRED_DEDUCE; }
<hint>NO_PRED_DEDUCE { return NO_PRED_DEDUCE; }
<hint>PUSH_PRED_CTE { return PUSH_PRED_CTE; }
<hint>NO_PUSH_PRED_CTE { return NO_PUSH_PRED_CTE; }
<hint>REPLACE_CONST { return REPLACE_CONST; }
<hint>NO_REPLACE_CONST { return NO_REPLACE_CONST; }
<hint>ENABLE_PARALLEL_DML  { return ENABLE_PARALLEL_DML; }
<hint>DISABLE_PARALLEL_DML  { return DISABLE_PARALLEL_DML; }
<hint>INLINE { return INLINE; }
<hint>MATERIALIZE { return MATERIALIZE; }
<hint>SEMI_TO_INNER { return SEMI_TO_INNER; }
<hint>NO_SEMI_TO_INNER { return NO_SEMI_TO_INNER; }
<hint>COALESCE_SQ  { return COALESCE_SQ; }
<hint>NO_COALESCE_SQ  { return NO_COALESCE_SQ; }
<hint>GBY_PUSHDOWN { return GBY_PUSHDOWN; }
<hint>NO_GBY_PUSHDOWN { return NO_GBY_PUSHDOWN; }
<hint>USE_HASH_DISTINCT { return USE_HASH_DISTINCT; }
<hint>NO_USE_HASH_DISTINCT { return NO_USE_HASH_DISTINCT; }
<hint>DISTINCT_PUSHDOWN { return DISTINCT_PUSHDOWN; }
<hint>NO_DISTINCT_PUSHDOWN { return NO_DISTINCT_PUSHDOWN; }
<hint>USE_HASH_SET { return USE_HASH_SET; }
<hint>NO_USE_HASH_SET { return NO_USE_HASH_SET; }
<hint>USE_DISTRIBUTED_DML { return USE_DISTRIBUTED_DML; }
<hint>NO_USE_DISTRIBUTED_DML { return NO_USE_DISTRIBUTED_DML; }
<hint>CURSOR_SHARING_EXACT {return CURSOR_SHARING_EXACT; }
<hint>SIMPLIFY_ORDER_BY { return SIMPLIFY_ORDER_BY; }
<hint>NO_SIMPLIFY_ORDER_BY { return NO_SIMPLIFY_ORDER_BY; }
<hint>SIMPLIFY_GROUP_BY { return SIMPLIFY_GROUP_BY; }
<hint>NO_SIMPLIFY_GROUP_BY { return NO_SIMPLIFY_GROUP_BY; }
<hint>SIMPLIFY_DISTINCT { return SIMPLIFY_DISTINCT; }
<hint>NO_SIMPLIFY_DISTINCT { return NO_SIMPLIFY_DISTINCT; }
<hint>SIMPLIFY_WINFUNC { return SIMPLIFY_WINFUNC; }
<hint>NO_SIMPLIFY_WINFUNC { return NO_SIMPLIFY_WINFUNC; }
<hint>SIMPLIFY_EXPR { return SIMPLIFY_EXPR; }
<hint>NO_SIMPLIFY_EXPR { return NO_SIMPLIFY_EXPR; }
<hint>SIMPLIFY_LIMIT { return SIMPLIFY_LIMIT; }
<hint>NO_SIMPLIFY_LIMIT { return NO_SIMPLIFY_LIMIT; }
<hint>SIMPLIFY_SUBQUERY { return SIMPLIFY_SUBQUERY; }
<hint>NO_SIMPLIFY_SUBQUERY { return NO_SIMPLIFY_SUBQUERY; }
<hint>FAST_MINMAX { return FAST_MINMAX; }
<hint>NO_FAST_MINMAX { return NO_FAST_MINMAX; }
<hint>PROJECT_PRUNE { return PROJECT_PRUNE; }
<hint>NO_PROJECT_PRUNE { return NO_PROJECT_PRUNE; }
<hint>SIMPLIFY_SET { return SIMPLIFY_SET; }
<hint>NO_SIMPLIFY_SET { return NO_SIMPLIFY_SET; }
<hint>OUTER_TO_INNER { return OUTER_TO_INNER; }
<hint>NO_OUTER_TO_INNER { return NO_OUTER_TO_INNER; }
<hint>COUNT_TO_EXISTS { return COUNT_TO_EXISTS; }
<hint>NO_COUNT_TO_EXISTS { return NO_COUNT_TO_EXISTS; }
<hint>LEFT_TO_ANTI { return LEFT_TO_ANTI; }
<hint>NO_LEFT_TO_ANTI { return NO_LEFT_TO_ANTI; }
<hint>PUSH_LIMIT { return PUSH_LIMIT; }
<hint>NO_PUSH_LIMIT { return NO_PUSH_LIMIT; }
<hint>ELIMINATE_JOIN { return ELIMINATE_JOIN; }
<hint>NO_ELIMINATE_JOIN { return NO_ELIMINATE_JOIN; }
<hint>WIN_MAGIC { return WIN_MAGIC; }
<hint>NO_WIN_MAGIC { return NO_WIN_MAGIC; }
<hint>PULLUP_EXPR { return PULLUP_EXPR; }
<hint>NO_PULLUP_EXPR { return NO_PULLUP_EXPR; }
<hint>AGGR_FIRST_UNNEST { return AGGR_FIRST_UNNEST; }
<hint>NO_AGGR_FIRST_UNNEST { return NO_AGGR_FIRST_UNNEST; }
<hint>JOIN_FIRST_UNNEST { return JOIN_FIRST_UNNEST; }
<hint>NO_JOIN_FIRST_UNNEST { return NO_JOIN_FIRST_UNNEST; }
<hint>OPT_PARAM { return OPT_PARAM; }
<hint>OB_DDL_SCHEMA_VERSION { return OB_DDL_SCHEMA_VERSION; }
<hint>DYNAMIC_SAMPLING { return DYNAMIC_SAMPLING; }
<hint>PUSHDOWN { return PUSHDOWN; }
<hint>{identifier} {
  if (!(IS_FAST_PARAMETERIZE)) {
    check_value(yylval);
    ParseResult *p = (ParseResult *)yyextra;
    ParseNode *node = NULL;
    malloc_new_node(node, p->malloc_pool_, T_IDENT, 0);
    yylval->node = node;
    int64_t out_len = 0;
    char *dup_value = NULL;
    if (p->is_not_utf8_connection_) {
      dup_value = parse_str_convert_utf8(p->charset_info_, yytext, p->malloc_pool_, &out_len, &(p->extra_errno_));
      check_identifier_convert_result(p->extra_errno_);
    } else {
      dup_value = parse_strdup(yytext, p->malloc_pool_, &out_len);
    }
    check_malloc(dup_value);
    node->str_value_ = str_tolower(dup_value, strlen(dup_value));
    node->str_len_ = out_len;
    node->sql_str_off_ = yylloc->first_column - 1;
    setup_token_pos_info(node, yylloc->first_column - 1, yylval->node->str_len_);
  }
  return NAME_OB;
}

<hint>{sqbegin} {
  ParseResult *p = (ParseResult *)yyextra;
  if (!p->is_ignore_token_) {
    BEGIN(hint_sq);
  }
  p->start_col_ = yylloc->first_column;
  check_value(yylval);
  malloc_new_node(yylval->node, p->malloc_pool_, T_VARCHAR, 0);
  yylval->node->str_len_ = 0;
  p->last_escape_check_pos_ = 0;
  yylval->node->str_value_ = NULL;
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE && !p->is_ignore_token_) {
    COPY_WRITE();
  }
}

<hint_sq>{sqend} {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  yylloc->first_column = p->start_col_;
  BEGIN(hint);
  FORMAT_STR_NODE(yylval->node);
  yylval->node->text_len_ = yylloc->last_column - p->start_col_ + 1;
  COPY_STRING(p->input_sql_ + p->start_col_ - 1, yylval->node->text_len_, yylval->node->raw_text_);
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    yylval->node->sql_str_off_ = yylloc->first_column;
    setup_token_pos_info(yylval->node, yylloc->first_column, yylval->node->text_len_ - 2);
    return STRING_VALUE;
  }
}

<hint_sq>{sqcontent} {
  check_value(yylval);
  STORE_STR_CONTENT(yylval->node);
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    COPY_WRITE();
  }
}

<hint_sq><<EOF>>  {
  yyerror(yylloc, yyextra, "unterminated quoted string in hint\n");
  return PARSER_SYNTAX_ERROR;
}

<hint>{btbegin} {
  BEGIN(hint_bt); /*fast parameterize don't handle connent in ``*/
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    check_value(yylval);
    ParseResult *p = (ParseResult *)yyextra;
    p->start_col_ = yylloc->first_column;
    char **tmp_literal = &p->tmp_literal_;
    if (NULL == *tmp_literal)
    {
      *tmp_literal = (char *)parse_malloc(p->input_sql_len_ + 1, p->malloc_pool_);
      check_malloc(*tmp_literal);
    }
    malloc_new_node(yylval->node, p->malloc_pool_, T_IDENT, 0);
    yylval->node->str_len_ = 0;
  }
}

<hint_bt>{btend} {
  BEGIN(hint);
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    ParseResult *p = (ParseResult *)yyextra;
    check_value(yylval);
    yylloc->first_column = p->start_col_;
    char *tmp_literal = p->tmp_literal_;
    tmp_literal[yylval->node->str_len_] = '\0';
    CHECK_STR_LEN_MATCH(tmp_literal, yylval->node->str_len_);

    char *dup_value = NULL;
    if (p->is_not_utf8_connection_) {
      dup_value = parse_str_convert_utf8(p->charset_info_, tmp_literal, p->malloc_pool_, &(yylval->node->str_len_), &(p->extra_errno_));
      check_identifier_convert_result(p->extra_errno_);
      //fprintf(stderr, "parse identifier result, str=%s, str_len=%ld\n", node->str_value_, node->str_len_);
    } else {
      dup_value = parse_strndup(tmp_literal, yylval->node->str_len_ + 1, p->malloc_pool_);
    }
    check_malloc(dup_value);
    yylval->node->str_value_ = dup_value;
    yylval->node->sql_str_off_ = yylloc->first_column;
    setup_token_pos_info(yylval->node, yylloc->first_column, yylval->node->str_len_);
    return NAME_OB;
  }
}

<hint_bt>{btdouble} {
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    check_value(yylval);
    ((ParseResult *)yyextra)->tmp_literal_[yylval->node->str_len_++] = '`';
  }
}

<hint_bt>{btcontent} {
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    check_value(yylval);
    memmove(((ParseResult *)yyextra)->tmp_literal_ + yylval->node->str_len_, yytext, yyleng);
    yylval->node->str_len_ += yyleng;
  }
}

<hint_bt><<EOF>>  {
  yyerror(yylloc, yyextra, "unterminated quoted string in hint\n");
  return PARSER_SYNTAX_ERROR;
}

{mysql_compatible_comment_without_version} {
  // if is a mysql comment without version. For example, /*!any sql str*/
  // mysql_comment without version, processed as common sql str;
  // place before `c_cmt_begin` to avoid (the '/*!') being hidden by '/*')
  ParseResult *p = (ParseResult *)yyextra;
  if (p->enable_compatible_comment_) {
    p->mysql_compatible_comment_ = true;
  } else {
    BEGIN(in_c_comment);
  }
}

{c_cmt_begin} {
BEGIN(in_c_comment);
#ifdef SQL_PARSER_COMPILATION
  ParseResult *p = (ParseResult *)yyextra;
  if (false == p->stop_add_comment_) {
    TokenPosInfo info;
    info.token_off_ = yylloc->first_column - 1;
    info.token_len_ = -1;
    if (OB_UNLIKELY(OB_PARSER_SUCCESS != add_comment_list(p, &info))) {
      YY_UNEXPECTED_ERROR("add comment to comment list failed");
    }
  }
#endif
}

<in_c_comment>{c_cmt_end} {
  ((ParseResult *)yyextra)->has_encount_comment_ = true;
  BEGIN(INITIAL);
#ifdef SQL_PARSER_COMPILATION
  ParseResult *p = (ParseResult *)yyextra;
  p->has_encount_comment_ = true;
  if (false == p->stop_add_comment_) {
    if (OB_UNLIKELY(0 >= p->comment_cnt_)) {
      YY_UNEXPECTED_ERROR("comment_cnt_ cannot less than 0 in comment end");
    } else {
      TokenPosInfo *info = &(p->comment_list_[p->comment_cnt_-1]);
      info->token_len_ = yylloc->last_column - info->token_off_;
    }
  }
#endif
}

{mysql_compatible_comment_end} {
  //for mysql compatible comment:
  // only "*/" should be matched, duplicated '*' (e.g., "***/") will report a error.
  ParseResult *p = (ParseResult *)yyextra;
  if (p->mysql_compatible_comment_){
    p->mysql_compatible_comment_ = false;
    BEGIN(INITIAL);
  } else {
    // The sql could be "select */*!xxx*/ from t1;". We can't directly raise a syntax
    // error here. We should treat the "*/" as '*' and '/' by return '*' and unput '/';

    // yyless will change the yytext and yyleng.
    char c_ret = yytext[0];
    yyless(1);
    p->yycolumn_ = p->yycolumn_ - 1;
    return c_ret;
  }
}

<in_c_comment><<EOF>>  {
  yyerror(yylloc, yyextra, "unterminated log_level string\n");
  return PARSER_SYNTAX_ERROR;
}
<in_c_comment>[\n] { yylineno++; }
<in_c_comment>. {}

<hint><<EOF>>  {
  yyerror(yylloc, yyextra, "unterminated hint string\n");
  return PARSER_SYNTAX_ERROR;
}
<hint>. {}

{comment} {
  ((ParseResult *)yyextra)->has_encount_comment_ = true;
/* ignore */ }

[-+&~|^/%*(),.:!{}] {
  if (IS_FAST_PARAMETERIZE) {
    ParseResult *p = (ParseResult *)yyextra;
    // for 'select -   -1 from dual'
    // the first neg sign is not copied when it is parsed
    // but when the second neg sign is parsed, the first neg sign must be put where is should be
    if ('-' == yytext[0]) {
      REPUT_NEG_SIGN(p);
      p->minus_ctx_.pos_ = p->no_param_sql_len_;
      p->minus_ctx_.raw_sql_offset_ = yylloc->first_column - 1;
      p->minus_ctx_.has_minus_ = true;
    } else {
      COPY_WRITE();
      REPUT_NEG_SIGN(p);
    }
  } else {
    return yytext[0];
  }
}

[;] {
  if (IS_FAST_PARAMETERIZE) {
    ParseResult *p = (ParseResult *)yyextra;
    if (p->is_batched_multi_enabled_split_) {
      // 去除末尾空格
      RM_MULTI_STMT_END_P(p);
    } else {
      COPY_WRITE();
    }
    return END_P; //遇到;时结束
  } else {
    return DELIMITER;
  }
}

"||" {
  int ret = CNNOP; /*fast parameterize don't care CNNOP or OR_OP*/
  if (!(IS_FAST_PARAMETERIZE)) {
    ObSQLMode mode = ((ParseResult *)yyextra)->sql_mode_;
    bool is_pipes_as_concat = false;
      IS_PIPES_AS_CONCAT(mode, is_pipes_as_concat);
    if (is_pipes_as_concat) {
      ret = CNNOP;
    } else {
      ret = OR_OP;
    }
  }
  return ret;
}

"&&" {return AND_OP;}
"="  {return COMP_EQ;}
"==" {
  ParseResult *p = (ParseResult *)yyextra;
  if (!p->pl_parse_info_.is_pl_parse_) {
    yyerror(yylloc, yyextra, "== operator not in pl context\n");
    return PARSER_SYNTAX_ERROR;
  } else {
    return COMP_EQ;
  }
}
":="  {return SET_VAR;}
"<=>"  {return COMP_NSEQ;}
">=" {return COMP_GE;}
">"  {return COMP_GT;}
"<=" {return COMP_LE;}
"<"  {return COMP_LT;}
"!="|"<>" {return COMP_NE;}
"<<"  {return SHIFT_LEFT;}
">>"  {return SHIFT_RIGHT;}
"->"  {return JSON_EXTRACT;}
"->>"  {return JSON_EXTRACT_UNQUOTED;}

"?" {
  //use for outline now, means any value
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  malloc_new_node(yylval->node, p->malloc_pool_, T_QUESTIONMARK, 0);
  yylval->node->value_ = p->question_mark_ctx_.count_++;
  p->question_mark_ctx_.by_ordinal_ = true;
  if (OB_UNLIKELY(p->question_mark_ctx_.by_name_)) {
    YY_UNEXPECTED_ERROR("Ordinal binding and Named binding cannot be combined\n");
  }
  if (IS_FAST_PARAMETERIZE) {
    yylval->node->raw_text_ = parse_strdup(yytext, p->malloc_pool_, &(yylval->node->text_len_));
    yylval->node->sql_str_off_ = yylloc->first_column - 1;
    check_malloc(yylval->node->raw_text_);
    STORE_PARAM_NODE();
  } else {
    yylval->node->sql_str_off_ = yylloc->first_column - 1;
    return QUESTIONMARK;
  }
}

":"{int_num} {
  /*
   * use for PL and ps
   * mysql模式prepare和第一次execute需要生成计划都会走到这里，这里的编号还是route_sql_的编号，即按入参的编号
   * 所以需要调用get_question_mark进行重新编号
   */
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  malloc_new_node(yylval->node, p->malloc_pool_, T_QUESTIONMARK, 0);
  yylval->node->value_ = get_question_mark(&p->question_mark_ctx_, p->malloc_pool_, yytext);
  if (IS_FAST_PARAMETERIZE) {
    yylval->node->raw_text_ = parse_strdup(yytext, p->malloc_pool_, &(yylval->node->text_len_));
    yylval->node->sql_str_off_ = yylloc->first_column - 1;
    check_malloc(yylval->node->raw_text_);
    STORE_PARAM_NODE();
  } else {
    yylval->node->sql_str_off_ = yylloc->first_column - 1;
    return QUESTIONMARK;
  }
}
":"{identifier} {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  malloc_new_node(yylval->node, p->malloc_pool_, T_QUESTIONMARK, 0);
  if (p->question_mark_ctx_.by_defined_name_) {
    yylval->node->value_ = get_question_mark_by_defined_name(&p->question_mark_ctx_, yytext);
    if (yylval->node->value_ < 0) {
      YY_UNEXPECTED_ERROR("Invalid question mark idx\n");
    }
  } else {
    yylval->node->value_ = get_question_mark(&p->question_mark_ctx_, p->malloc_pool_, yytext);
  }
  p->question_mark_ctx_.by_name_ = true;
  if (OB_UNLIKELY(p->question_mark_ctx_.by_ordinal_)) {
    YY_UNEXPECTED_ERROR("Ordinal binding and Named binding cannot be combined\n");
  }
  if (IS_FAST_PARAMETERIZE) {
    yylval->node->raw_text_ = parse_strdup(yytext, p->malloc_pool_, &(yylval->node->text_len_));
    yylval->node->sql_str_off_ = yylloc->first_column - 1;
    check_malloc(yylval->node->raw_text_);
    STORE_PARAM_NODE();
  } else {
    yylval->node->sql_str_off_ = yylloc->first_column - 1;
    yylval->node->pos_ = yylloc->first_column - 1;
    return QUESTIONMARK;
  }
}




{identifier}":"{identifier} {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  if (IS_FOR_REMAP) {
    char *colon_p = strstr(yytext, ":");
    if (NULL == colon_p) {
      YY_UNEXPECTED_ERROR("invalid remap table name\n");
    } else {
      size_t src_len = colon_p - yytext;
      ParseNode *node = NULL, *src_node = NULL, *target_node = NULL;
      malloc_new_node(node, p->malloc_pool_, T_RELATION_FACTOR, 2);
      yylval->node = node;
      malloc_new_node(src_node, p->malloc_pool_, T_INT, 0);
      malloc_new_node(target_node, p->malloc_pool_, T_INT, 0);
      src_node->str_value_ = parse_strndup(yytext, src_len, p->malloc_pool_);
      src_node->str_len_ = src_len;
      check_malloc(src_node->str_value_);
      target_node->str_value_ = parse_strdup(colon_p + 1, p->malloc_pool_, &(target_node->str_len_));
      check_malloc(target_node->str_value_);
      node->children_[0] = src_node;
      node->children_[1] = target_node;
      return REMAP_TABLE_VAL;
    }

  } else {
    yyerror(yylloc, yyextra, "Named binding is only for remap\n");
    return PARSER_SYNTAX_ERROR;
  }
}

{identifier}"."{identifier}":"{identifier} {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  if (IS_FOR_REMAP) {
    char *colon_p = strstr(yytext, ":");
    char *point_p = strstr(yytext, ".");
    if (NULL == colon_p || NULL == point_p) {
      YY_UNEXPECTED_ERROR("invalid remap table name\n");
    } else {
      size_t src_db_len = point_p - yytext;
      size_t src_table_len = colon_p - point_p - 1;
      ParseNode *node = NULL, *src_db_node = NULL, *src_table_node = NULL, *target_table_node = NULL;
      malloc_new_node(node, p->malloc_pool_, T_RELATION_FACTOR, 5);
      yylval->node = node;
      malloc_new_node(src_db_node, p->malloc_pool_, T_INT, 0);
      malloc_new_node(src_table_node, p->malloc_pool_, T_INT, 0);
      malloc_new_node(target_table_node, p->malloc_pool_, T_INT, 0);
      src_db_node->str_value_ = parse_strndup(yytext, src_db_len, p->malloc_pool_);
      src_db_node->str_len_ = src_db_len;
      check_malloc(src_db_node->str_value_);
      src_table_node->str_value_ = parse_strndup(point_p + 1, src_table_len, p->malloc_pool_);
      src_table_node->str_len_ = src_table_len;
      check_malloc(src_table_node->str_value_);
      target_table_node->str_value_ = parse_strdup(colon_p + 1, p->malloc_pool_, &(target_table_node->str_len_));
      check_malloc(target_table_node->str_value_);
      node->children_[0] = src_db_node;
      node->children_[1] = src_table_node;
      node->children_[4] = target_table_node;
      return REMAP_TABLE_VAL;
    }
  } else {
    yyerror(yylloc, yyextra, "Named binding is only for remap\n");
    return PARSER_SYNTAX_ERROR;
  }
}

{identifier}"."{identifier}":"{identifier}"."{identifier} {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  if (IS_FOR_REMAP) {
    char *first_point_p = strstr(yytext, ".");
    char *colon_p = strstr(yytext, ":");
    char *final_point_p = strrchr(yytext, '.');
    if (NULL == first_point_p || NULL == colon_p || NULL == final_point_p) {
      YY_UNEXPECTED_ERROR("invalid remap table name\n");
    } else {
      size_t src_db_len = first_point_p - yytext;
      size_t src_table_len = colon_p - first_point_p - 1;
      size_t target_db_len = final_point_p - colon_p - 1;
      ParseNode *node = NULL, *src_db_node = NULL, *src_table_node = NULL, *target_db_node = NULL, *target_table_node = NULL;
      malloc_new_node(node, p->malloc_pool_, T_RELATION_FACTOR, 5);
      yylval->node = node;
      malloc_new_node(src_db_node, p->malloc_pool_, T_INT, 0);
      malloc_new_node(src_table_node, p->malloc_pool_, T_INT, 0);
      malloc_new_node(target_db_node, p->malloc_pool_, T_INT, 0);
      malloc_new_node(target_table_node, p->malloc_pool_, T_INT, 0);
      src_db_node->str_value_ = parse_strndup(yytext, src_db_len, p->malloc_pool_);
      src_db_node->str_len_ = src_db_len;
      check_malloc(src_db_node->str_value_);
      src_table_node->str_value_ = parse_strndup(first_point_p + 1, src_table_len, p->malloc_pool_);
      src_table_node->str_len_ = src_table_len;
      check_malloc(src_table_node->str_value_);
      target_db_node->str_value_ = parse_strndup(colon_p + 1, target_db_len, p->malloc_pool_);
      target_db_node->str_len_ = target_db_len;
      check_malloc(target_db_node->str_value_);
      target_table_node->str_value_ = parse_strdup(final_point_p + 1, p->malloc_pool_, &(target_table_node->str_len_));
      check_malloc(target_table_node->str_value_);
      node->children_[0] = src_db_node;
      node->children_[1] = src_table_node;
      node->children_[3] = target_db_node;
      node->children_[4] = target_table_node;
      return REMAP_TABLE_VAL;
    }
  } else {
    yyerror(yylloc, yyextra, "Named binding is only for remap\n");
    return PARSER_SYNTAX_ERROR;
  }
}

{identifier}".*:"{identifier}".*" {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  if (IS_FOR_REMAP) {
    char *first_point_p = strstr(yytext, ".");
    char *colon_p = strstr(yytext, ":");
    char *final_point_p = strrchr(yytext, '.');
    if (NULL == first_point_p || NULL == colon_p || NULL == final_point_p) {
      YY_UNEXPECTED_ERROR("invalid remap table name\n");
    } else {
      size_t src_db_len = first_point_p - yytext;
      size_t target_db_len = final_point_p - colon_p - 1;
      ParseNode *node = NULL, *src_db_node = NULL, *target_db_node = NULL;
      malloc_new_node(node, p->malloc_pool_, T_RELATION_FACTOR, 5);
      yylval->node = node;
      malloc_new_node(src_db_node, p->malloc_pool_, T_INT, 0);
      malloc_new_node(target_db_node, p->malloc_pool_, T_INT, 0);
      src_db_node->str_value_ = parse_strndup(yytext, src_db_len, p->malloc_pool_);
      src_db_node->str_len_ = src_db_len;
      check_malloc(src_db_node->str_value_);
      target_db_node->str_value_ = parse_strndup(colon_p + 1, target_db_len, p->malloc_pool_);
      target_db_node->str_len_ = target_db_len;
      check_malloc(target_db_node->str_value_);
      node->children_[0] = src_db_node;
      node->children_[3] = target_db_node;
      return REMAP_TABLE_VAL;
    }
  } else {
    yyerror(yylloc, yyextra, "Named binding is only for remap\n");
    return PARSER_SYNTAX_ERROR;
  }
}

{identifier}"."{identifier}":"{identifier}":"{identifier}"."{identifier} {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  if (IS_FOR_REMAP) {
    char *first_point_p = strstr(yytext, ".");
    char *first_colon_p = strstr(yytext, ":");
    char *final_colon_p = strrchr(yytext, ':');
    char *final_point_p = strrchr(yytext, '.');
    if (NULL == first_point_p || NULL == first_colon_p || NULL == final_colon_p || NULL == final_point_p) {
      YY_UNEXPECTED_ERROR("invalid remap table name\n");
    } else {
      size_t src_db_len = first_point_p - yytext;
      size_t src_table_len = first_colon_p - first_point_p - 1;
      size_t src_part_len = final_colon_p - first_colon_p - 1;
      size_t target_db_len = final_point_p - final_colon_p - 1;
      ParseNode *node = NULL, *src_db_node = NULL, *src_table_node = NULL, *src_part_node = NULL;
      ParseNode *target_db_node = NULL, *target_table_node = NULL;
      malloc_new_node(node, p->malloc_pool_, T_RELATION_FACTOR, 5);
      yylval->node = node;
      malloc_new_node(src_db_node, p->malloc_pool_, T_INT, 0);
      malloc_new_node(src_table_node, p->malloc_pool_, T_INT, 0);
      malloc_new_node(src_part_node, p->malloc_pool_, T_INT, 0);
      malloc_new_node(target_db_node, p->malloc_pool_, T_INT, 0);
      malloc_new_node(target_table_node, p->malloc_pool_, T_INT, 0);
      src_db_node->str_value_ = parse_strndup(yytext, src_db_len, p->malloc_pool_);
      src_db_node->str_len_ = src_db_len;
      check_malloc(src_db_node->str_value_);
      src_table_node->str_value_ = parse_strndup(first_point_p + 1, src_table_len, p->malloc_pool_);
      src_table_node->str_len_ = src_table_len;
      check_malloc(src_table_node->str_value_);
      src_part_node->str_value_ = parse_strndup(first_colon_p + 1, src_part_len, p->malloc_pool_);
      src_part_node->str_len_ = src_part_len;
      check_malloc(src_part_node->str_value_);
      target_db_node->str_value_ = parse_strndup(final_colon_p + 1, target_db_len, p->malloc_pool_);
      target_db_node->str_len_ = target_db_len;
      check_malloc(target_db_node->str_value_);
      target_table_node->str_value_ = parse_strdup(final_point_p + 1, p->malloc_pool_, &(target_table_node->str_len_));
      check_malloc(target_table_node->str_value_);
      node->children_[0] = src_db_node;
      node->children_[1] = src_table_node;
      node->children_[2] = src_part_node;
      node->children_[3] = target_db_node;
      node->children_[4] = target_table_node;
      return REMAP_TABLE_VAL;
    }
  } else {
    yyerror(yylloc, yyextra, "Named binding is only for remap\n");
    return PARSER_SYNTAX_ERROR;
  }
}

{identifier}"."{identifier}":"{identifier}":"{identifier} {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  if (IS_FOR_REMAP) {
    char *first_point_p = strstr(yytext, ".");
    char *first_colon_p = strstr(yytext, ":");
    char *final_colon_p = strrchr(yytext, ':');
    if (NULL == first_point_p || NULL == first_colon_p || NULL == final_colon_p) {
      YY_UNEXPECTED_ERROR("invalid remap table name\n");
    } else {
      size_t src_db_len = first_point_p - yytext;
      size_t src_table_len = first_colon_p - first_point_p - 1;
      size_t src_part_len = final_colon_p - first_colon_p - 1;
      ParseNode *node = NULL, *src_db_node = NULL, *src_table_node = NULL, *src_part_node = NULL;
      ParseNode *target_table_node = NULL;
      malloc_new_node(node, p->malloc_pool_, T_RELATION_FACTOR, 5);
      yylval->node = node;
      malloc_new_node(src_db_node, p->malloc_pool_, T_INT, 0);
      malloc_new_node(src_table_node, p->malloc_pool_, T_INT, 0);
      malloc_new_node(src_part_node, p->malloc_pool_, T_INT, 0);
      malloc_new_node(target_table_node, p->malloc_pool_, T_INT, 0);
      src_db_node->str_value_ = parse_strndup(yytext, src_db_len, p->malloc_pool_);
      src_db_node->str_len_ = src_db_len;
      check_malloc(src_db_node->str_value_);
      src_table_node->str_value_ = parse_strndup(first_point_p + 1, src_table_len, p->malloc_pool_);
      src_table_node->str_len_ = src_table_len;
      check_malloc(src_table_node->str_value_);
      src_part_node->str_value_ = parse_strndup(first_colon_p + 1, src_part_len, p->malloc_pool_);
      src_part_node->str_len_ = src_part_len;
      check_malloc(src_part_node->str_value_);
      target_table_node->str_value_ = parse_strdup(final_colon_p + 1, p->malloc_pool_, &(target_table_node->str_len_));
      check_malloc(target_table_node->str_value_);
      node->children_[0] = src_db_node;
      node->children_[1] = src_table_node;
      node->children_[2] = src_part_node;
      node->children_[4] = target_table_node;
      return REMAP_TABLE_VAL;
    }
  } else {
    yyerror(yylloc, yyextra, "Named binding is only for remap\n");
    return PARSER_SYNTAX_ERROR;
  }
}


":"{identifier}"."{identifier} {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  char *header = str_toupper(yytext, 4);
  bool is_for_trigger = (0 == strncmp(":NEW", header, 4)) || (0 == strncmp(":OLD", header, 4));
  if (IS_FOR_REMAP) {
    bool is_contain_colon = (0 == strncmp(":", yytext, 1));
    char *dot_p = strstr(yytext, ".");
    if (!is_contain_colon || NULL == dot_p) {
      YY_UNEXPECTED_ERROR("invalid remap table name\n");
    } else {
      size_t db_len = dot_p - yytext - 1;
      ParseNode *node = NULL, *db_node = NULL, *tb_node = NULL;
      malloc_new_node(node, p->malloc_pool_, T_IDENT, 2);
      yylval->node = node;
      malloc_new_node(db_node, p->malloc_pool_, T_IDENT, 0);
      malloc_new_node(tb_node, p->malloc_pool_, T_IDENT, 0);
      db_node->str_value_ = parse_strndup(yytext + 1, db_len, p->malloc_pool_);
      db_node->str_len_ = db_len;
      check_malloc(db_node->str_value_);
      tb_node->str_value_ = parse_strdup(dot_p + 1, p->malloc_pool_, &(tb_node->str_len_));
      check_malloc(tb_node->str_value_);
      node->children_[0] = db_node;
      node->children_[1] = tb_node;
      return REMAP_TABLE_VAL;
    }
  } else if (is_for_trigger) {
    malloc_new_node(yylval->node, p->malloc_pool_, T_QUESTIONMARK, 0);
    yylval->node->value_ = get_question_mark(&p->question_mark_ctx_, p->malloc_pool_, yytext);
    if (IS_FAST_PARAMETERIZE) {
      yylval->node->raw_text_ = parse_strdup(yytext, p->malloc_pool_, &(yylval->node->text_len_));
      yylval->node->sql_str_off_ = yylloc->first_column - 1;
      check_malloc(yylval->node->raw_text_);
      STORE_PARAM_NODE();
    } else {
      yylval->node->sql_str_off_ = yylloc->first_column - 1;
      return QUESTIONMARK;
    }
  } else {
    YY_UNEXPECTED_ERROR("Named binding is only for trigger\n");
  }
}

{system_variable} {
  if (!(IS_FAST_PARAMETERIZE)) {
    /* ParseNode *node = new_node(((ParseResult *)yyextra)->malloc_pool_, T_SYSTEM_VARIABLE, 0); */
    ParseNode *node = NULL;
    ParseResult *p = (ParseResult *)yyextra;
    check_value(yylval);
    malloc_new_node(node, p->malloc_pool_, T_SYSTEM_VARIABLE, 0);
    yylval->node = node;
    /* skip '@@' */
    node->str_value_ = parse_strdup(yytext + 2, p->malloc_pool_, &(node->str_len_));
    if (NULL != node->str_value_
    		&& *(yytext + 2) == *(yytext + 1 + node->str_len_)
    		&& (*(yytext + 2) == '`')) {
      // bugfix:
      // support "select @@`xxx`;" as "select @@xxx;"
			node->str_value_ += 1;
			node->str_len_ -= 2;
		}
    check_malloc(node->str_value_);
    //node->value_ = 2;
    node->value_ = 0;
  } else {
    ParseResult *p = (ParseResult *)yyextra;
    REPUT_NEG_SIGN(p);
  }
  return SYSTEM_VARIABLE;
}

{user_variable} {
  /* ParseNode *node = new_node(p->malloc_pool_, T_TEMP_VARIABLE, 0); */
  if (!(IS_FAST_PARAMETERIZE)) {
    ParseNode *node = NULL;
    ParseResult *p = (ParseResult *)yyextra;
    check_value(yylval);
    malloc_new_node(node, p->malloc_pool_, T_USER_VARIABLE_IDENTIFIER, 0);
    yylval->node = node;
    /* skip '@' and quotes like '/"/` */
    node->str_value_ = parse_strdup(yytext + 1, p->malloc_pool_, &(node->str_len_));
    check_malloc(node->str_value_);
    if (NULL != node->str_value_
        && node->str_len_ > 1
    		&& *(yytext + 1) == *(yytext + node->str_len_)
    		&& (*(yytext + 1) == '\'' || *(yytext + 1) == '\"' || *(yytext + 1) == '`')) {
			node->str_value_ += 1;
			node->str_len_ -= 2;
    } else if (NULL != node->str_value_
               && (*(yytext + 1) == '\'' || *(yytext + 1) == '\"' || *(yytext + 1) == '`')) {
      yyerror(yylloc, yyextra, "unterminated user variable name\n");
      return PARSER_SYNTAX_ERROR;
    }
    check_value(node->str_value_);
  } else {
    ParseResult *p = (ParseResult *)yyextra;
    REPUT_NEG_SIGN(p);
  }
  return USER_VARIABLE;
}

{identifier} {
  const NonReservedKeyword *word = NULL;
  if (IS_FAST_PARAMETERIZE) {
    ParseResult *p = (ParseResult *)yyextra;
    REPUT_NEG_SIGN(p);
    return NAME_OB;
  } else {
    int ret = NAME_OB;
    if (NULL == (word = mysql_non_reserved_keyword_lookup(yytext)))
    {
      check_value(yylval);
      ParseNode *node = NULL;
      ParseResult *p = (ParseResult *)yyextra;
      malloc_new_node(node, p->malloc_pool_, T_IDENT, 0);
      yylval->node = node;
      if (p->is_not_utf8_connection_) {
        node->str_value_ = parse_str_convert_utf8(p->charset_info_, yytext, p->malloc_pool_, &(node->str_len_), &(p->extra_errno_));
        check_identifier_convert_result(p->extra_errno_);
        //fprintf(stderr, "parse identifier result, str=%s, str_len=%ld\n", node->str_value_, node->str_len_);
      } else {
        node->str_value_ = parse_strdup(yytext, p->malloc_pool_, &(node->str_len_));
      }
      check_malloc(node->str_value_);
      node->sql_str_off_ = yylloc->first_column-1;
      setup_token_pos_info(node, yylloc->first_column - 1, node->str_len_);
      ret = NAME_OB;
    } else {
      yylval->non_reserved_keyword = word;
      ret = word->keyword_type;
    }
    return ret;
  }
}

{mysql_compatible_comment_with_version} {
  // comment with version: /*!50600 any sql str*/
  // comment without version: /*!any sql str*/
  // we do not add a start_condition, since some sql string need to be processed in INITIAL state.
  // instead of a new start_condition, we use a extra field (mysql_compatible_comment_) to mark the adaptive comment.
  ParseResult *p = (ParseResult *)yyextra;
  if (p->enable_compatible_comment_) {
    p->mysql_compatible_comment_ = true;
  } else {
    BEGIN(in_c_comment);
  }
}

[\n] {
  yylineno ++;
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  }
}

[ \t\r] {
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  }
}
"--"[ \t].*;

<<EOF>> {return END_P;}
. {
  ParseResult *p = (ParseResult *)yyextra;
  if (p->input_sql_len_ == yylloc->first_column) {
    return END_P;
  } else {
    yyerror(yylloc, yyextra, "mystery charactor '%c'\n", *yytext);
    return PARSER_SYNTAX_ERROR;
  }
}
%%

void *yyalloc(size_t bytes, void *yyscanner)
{
  void *ptr_ret = NULL;
  ParseResult *p = yyget_extra(yyscanner);
  if (OB_UNLIKELY(NULL == p)) {
  } else {
    ptr_ret = parse_malloc(bytes, p->malloc_pool_);
  }
  return ptr_ret;
}

void *yyrealloc(void *ptr, size_t bytes, void *yyscanner)
{
  void *ptr_ret = NULL;
  ParseResult *p = yyget_extra(yyscanner);
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


void obsql_mysql_parser_fatal_error(int32_t errcode, yyscan_t yyscanner, yyconst char *msg, ...)
{
  ParseResult *p = obsql_mysql_yyget_extra(yyscanner);
  if (p != NULL) {
    p->extra_errno_ = errcode;
    if (OB_LIKELY(NULL != msg)) {
      va_list ap;
      va_start(ap, msg);
      vsnprintf(p->error_msg_, MAX_ERROR_MSG, msg, ap);
      va_end(ap);
    }
  }
  longjmp(*p->jmp_buf_, 1);//the secord param must be non-zero value
}
