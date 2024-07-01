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
%name-prefix "obpl_mysql_yy"
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
#include "pl/parser/pl_parser_mysql_mode_lex.h"
#include "pl/parser/pl_parser_base.h"

typedef struct _YYLookaheadToken
{
  int *la_yychar;
  /* The semantic value of the lookahead symbol.  */
  YYSTYPE *la_yylval;
  /* Location data for the lookahead symbol.  */
  YYLTYPE *la_yylloc;
} YYLookaheadToken;

extern ParseNode *obpl_mysql_read_sql_construct(ObParseCtx *parse_ctx, const char *prefix, YYLookaheadToken *la_token, int end_token_cnt, ...);
extern void obpl_mysql_yyerror(YYLTYPE *yylloc, ObParseCtx *parse_ctx, char *s);
extern void obpl_mysql_parse_fatal_error(int32_t errcode, yyscan_t yyscanner, yyconst char *msg, ...);

int obpl_mysql_check_specific_node(const ParseNode *node, const ObItemType type, int *is_contain) {
  int ret = OB_PARSER_SUCCESS;
  if (obpl_parser_check_stack_overflow()) {
    ret = OB_PARSER_ERR_SIZE_OVERFLOW;
  } else if (OB_UNLIKELY(NULL == is_contain)) {
    ret = OB_PARSER_ERR_UNEXPECTED;
  } else {
    *is_contain = false;
  }

  if (OB_PARSER_SUCCESS == ret && OB_NOT_NULL(node)) {
    if (type == node->type_) {
      *is_contain = true;
    } else {
      for (int64_t i = 0; OB_PARSER_SUCCESS == ret && !*is_contain && i < node->num_child_; ++i) {
        ret = obpl_mysql_check_specific_node(node->children_[i], type, is_contain);
      }
    }
  }
  return ret;
}

int obpl_mysql_wrap_node_into_subquery(ObParseCtx *_parse_ctx, ParseNode *node) {
  int ret = OB_PARSER_SUCCESS;
  if (OB_NOT_NULL(node) && OB_NOT_NULL(node->str_value_)) {
    int max_query_len = node->str_len_ + 10;
    char *subquery = (char *)parse_malloc(max_query_len, _parse_ctx->mem_pool_);
    int len = 0;
    if (OB_UNLIKELY(NULL == subquery)) {
      ret = OB_PARSER_ERR_NO_MEMORY;
    } else if ((len = snprintf(subquery, max_query_len, "(SELECT %s)", node->str_value_)) <= 0) {
      ret = OB_PARSER_ERR_UNEXPECTED;
    } else {
      ParseResult parse_result;
      memset(&parse_result, 0, sizeof(ParseResult));
      parse_result.input_sql_ = subquery;
      parse_result.input_sql_len_ = len;
      parse_result.malloc_pool_ = _parse_ctx->mem_pool_;
      parse_result.pl_parse_info_.is_pl_parse_ = true;
      parse_result.pl_parse_info_.is_pl_parse_expr_ = true;
      parse_result.is_for_trigger_ = (1 == _parse_ctx->is_for_trigger_);
      parse_result.question_mark_ctx_ = _parse_ctx->question_mark_ctx_;
      parse_result.charset_info_ = _parse_ctx->charset_info_;
      parse_result.charset_info_oracle_db_ = _parse_ctx->charset_info_oracle_db_;
      parse_result.is_not_utf8_connection_ = _parse_ctx->is_not_utf8_connection_;
      parse_result.connection_collation_ = _parse_ctx->connection_collation_;
      parse_result.sql_mode_ = _parse_ctx->scanner_ctx_.sql_mode_;
      parse_result.semicolon_start_col_ = INT32_MAX;
      if (0 == parse_sql_stmt(&parse_result)) {
        *node = *parse_result.result_tree_->children_[0];
        node->str_value_ = subquery;
        node->str_len_ = len;
        node->pl_str_off_ = -1;
      }
    }
  }
  return ret;
}

void obpl_mysql_wrap_get_user_var_into_subquery(ObParseCtx *parse_ctx, ParseNode *node) {
  int ret = OB_PARSER_SUCCESS;
  int is_contain = false;
  if (OB_PARSER_SUCCESS != (ret = obpl_mysql_check_specific_node(node, T_OP_GET_USER_VAR, &is_contain))) {
    obpl_mysql_parse_fatal_error(ret, YYLEX_PARAM, "failed to check T_OP_GET_USER_VAR in parse tree");
  } else if (is_contain) {
    if (OB_PARSER_SUCCESS != (ret = obpl_mysql_wrap_node_into_subquery(parse_ctx, node))) {
      obpl_mysql_parse_fatal_error(ret, YYLEX_PARAM, "failed to wrap T_OP_GET_USER_VAR into subquery");
    }
  }
}

#define YY_FATAL_ERROR(msg, args...) (obpl_mysql_parse_fatal_error(OB_PARSER_ERR_NO_MEMORY, YYLEX_PARAM, msg, ##args))
#define YY_UNEXPECTED_ERROR(msg, args...) (obpl_mysql_parse_fatal_error(OB_PARSER_ERR_UNEXPECTED, YYLEX_PARAM, msg, ##args))

#define do_parse_sql_stmt(node, _parse_ctx, start_loc, end_loc, end_tok_num, ...)                          \
  do {                                                                                                     \
    YYLookaheadToken la_token;                                                                             \
    la_token.la_yychar = &yychar;                                                                          \
    la_token.la_yylval = &yylval;                                                                          \
    la_token.la_yylloc = &yylloc;                                                                          \
    _parse_ctx->scanner_ctx_.sql_start_loc = start_loc;                                                    \
    _parse_ctx->scanner_ctx_.sql_end_loc = end_loc;                                                        \
    node = obpl_mysql_read_sql_construct(_parse_ctx, "", &la_token, end_tok_num, ##__VA_ARGS__);           \
    if (NULL == node) {                                                                                    \
      YYERROR;                                                                                             \
    }                                                                                                      \
    reset_current_location(_parse_ctx->scanner_ctx_.sql_start_loc, _parse_ctx->scanner_ctx_.sql_end_loc);  \
  } while (0)

#define do_parse_sql_expr_rule(node, _parse_ctx, end_tok_num, ...)                                         \
  do {                                                                                                     \
    ParseNode *select_node = NULL;                                                                         \
    YYLookaheadToken la_token;                                                                             \
    la_token.la_yychar = &yychar;                                                                          \
    la_token.la_yylval = &yylval;                                                                          \
    la_token.la_yylloc = &yylloc;                                                                          \
    _parse_ctx->scanner_ctx_.sql_start_loc = -1;                                                           \
    _parse_ctx->scanner_ctx_.sql_end_loc = -1;                                                             \
    select_node = obpl_mysql_read_sql_construct(_parse_ctx, "SELECT ", &la_token, end_tok_num, ##__VA_ARGS__);        \
    reset_current_location(_parse_ctx->scanner_ctx_.sql_start_loc, _parse_ctx->scanner_ctx_.sql_end_loc);  \
    if (select_node != NULL) {                                                                             \
      if (NULL == select_node->children_[2] || NULL == select_node->children_[2]->children_[0]) {          \
        YYERROR;                                                                                           \
      } else {                                                                                             \
        node = select_node->children_[2]->children_[0]->children_[0];                                      \
      }                                                                                                    \
    } else {                                                                                               \
      node = NULL;                                                                                         \
    }                                                                                                      \
} while (0)
%}

/*these tokens can't be used for obj names*/
%token END_P
%token SQL_KEYWORD SQL_TOKEN PARAM_ASSIGN_OPERATOR
%token PARSER_SYNTAX_ERROR /*used internal*/
%token <node> IDENT STRING INTNUM DECIMAL_VAL HEX_STRING_VALUE DATE_VALUE SYSTEM_VARIABLE USER_VARIABLE NULLX
%token <node> USER_NAME
/* reserved key words */
%token
//-----------------------------reserved keyword begin-----------------------------------------------
        ALTER BEFORE BY CALL CASE CONDITION CONTINUE CREATE CURRENT_USER CURSOR DECLARE
        DEFAULT DELETE DETERMINISTIC DROP EACH ELSE ELSEIF EXISTS EXIT FETCH FOR FROM IF IN
        INDEX INOUT INSERT INTO IS ITERATE LEAVE LIMIT LONG LOOP MODIFIES  NOT ON OR OUT
        PROCEDURE READS REPEAT REPLACE RESIGNAL RETURN SELECT SIGNAL SQL SQLEXCEPTION
        SQLSTATE SQLWARNING TABLE THEN TRIGGER UPDATE USING WHEN WHILE
        TINYINT SMALLINT MEDIUMINT INTEGER BIGINT FLOAT DOUBLE PRECISION DEC DECIMAL NUMERIC
        CHARACTER VARCHAR BINARY VARBINARY UNSIGNED
        ZEROFILL COLLATE SET BLOB TINYTEXT MEDIUMTEXT LONGTEXT TINYBLOB
        MEDIUMBLOB LONGBLOB VARYING
/* reserved key words only used in ob, in mysql these keywords are non reserved*/
        CHARSET COMMIT ROLLBACK DO UNTIL
//-----------------------------reserved keyword end-------------------------------------------------

/* non reserved key words */
%token <non_reserved_keyword>
//-----------------------------non_reserved keyword begin-------------------------------------------
      AFTER AUTHID BEGIN_KEY BINARY_INTEGER BODY C CATALOG_NAME CLASS_ORIGIN CLOSE COLUMN_NAME COMMENT
      CONSTRAINT_CATALOG CONSTRAINT_NAME CONSTRAINT_ORIGIN CONSTRAINT_SCHEMA CONTAINS COUNT CURSOR_NAME
      DATA DEFINER END_KEY EXTEND FOLLOWS FOUND FUNCTION HANDLER INTERFACE INVOKER JSON LANGUAGE
      MESSAGE_TEXT MYSQL_ERRNO NATIONAL NEXT NO OF OPEN PACKAGE PRAGMA PRECEDES RECORD RETURNS ROW ROWTYPE
      SCHEMA_NAME SECURITY SUBCLASS_ORIGIN TABLE_NAME USER TYPE VALUE DATETIME TIMESTAMP TIME DATE YEAR
      TEXT NCHAR NVARCHAR BOOL BOOLEAN ENUM BIT FIXED SIGNED ROLE
//-----------------------------non_reserved keyword end---------------------------------------------
%right END_KEY
%left ELSE IF ELSEIF

%nonassoc LOWER_PARENS
%nonassoc FUNCTION ZEROFILL SIGNED UNSIGNED
%nonassoc PARENS
%left '(' ')'
%nonassoc AUTHID INTERFACE
%nonassoc DECLARATION

%type <node> sql_keyword
%type <non_reserved_keyword> unreserved_keyword
%type <node> stmt_block stmt_list stmt outer_stmt sp_proc_outer_statement sp_proc_inner_statement sp_proc_independent_statement
%type <node> create_procedure_stmt sp_proc_stmt expr expr_list procedure_body default_expr
%type <node> create_function_stmt function_body
%type <node> drop_procedure_stmt drop_function_stmt
%type <node> alter_procedure_stmt alter_function_stmt opt_sp_alter_chistics
%type <node> sp_unlabeled_block
%type <node> sp_block_content opt_sp_decls sp_proc_stmts sp_decl sp_decls
%type <node> sp_labeled_block label_ident opt_sp_label
%type <node> sp_decl_idents sp_data_type opt_sp_decl_default opt_param_default
%type <node> sp_proc_stmt_if sp_if sp_proc_stmt_case sp_when_list sp_when sp_elseifs 
%type <node> sp_proc_stmt_return
%type <node> sql_stmt ident simple_ident
%type <node> into_clause
%type <node> sp_name sp_call_name opt_sp_param_list opt_sp_fparam_list sp_param_list sp_fparam_list
%type <node> sp_param sp_fparam sp_alter_chistics
%type <node> opt_sp_definer sp_create_chistics sp_create_chistic sp_chistic opt_parentheses user opt_host_name
%type <node> param_type sp_cparams opt_sp_cparam_list cexpr sp_cparam opt_sp_cparam_with_assign
%type <ival> opt_sp_inout opt_if_exists opt_if_not_exists
%type <node> call_sp_stmt do_sp_stmt
%type <node> sp_cond sp_hcond_list sp_hcond
%type <node> sp_unlabeled_control sp_labeled_control
%type <node> sp_proc_stmt_iterate
%type <node> sp_proc_stmt_leave
%type <node> sqlstate opt_value
%type <ival> sp_handler_type
%type <node> signal_stmt resignal_stmt
%type <node> signal_value opt_signal_value opt_set_signal_information signal_allowed_expr
%type <node> signal_information_item_list signal_information_item
%type <ival> scond_info_item_name
%type <node> sp_proc_stmt_open sp_proc_stmt_fetch sp_proc_stmt_close
%type <node> opt_package_stmts package_stmts package_stmt drop_package_stmt interface_pragma
%type <node> package_block package_body_block create_package_stmt create_package_body_stmt
%type <node> invoke_right proc_clause proc_clause_list opt_proc_clause
%type <node> decl_stmt_ext_list decl_stmt_ext func_decl proc_decl
%type <node> opt_tail_package_name
%type <ival> opt_replace
%type <node> create_trigger_stmt drop_trigger_stmt plsql_trigger_source
%type <node> trigger_definition trigger_event trigger_body pl_obj_access_ref
%type <ival> trigger_time
/*SQL data type*/
%type <node> scalar_data_type opt_charset collation opt_collation charset_name collation_name
%type <node> number_literal literal charset_key opt_float_precision opt_number_precision opt_binary
%type <node> string_list text_string
%type <ival> string_length_i opt_string_length_i opt_int_length_i opt_string_length_i_v2
%type <ival> opt_bit_length_i opt_datetime_fsp_i opt_year_i
%type <ival> int_type_i float_type_i datetime_type_i date_year_type_i text_type_i blob_type_i
%type <ival> nchar_type_i nvarchar_type_i
%type <node> variable number_type
%%
/*****************************************************************************
 *
 *      MULTI STMT BLOCK start grammar
 *
 *****************************************************************************/
stmt_block:
    stmt_list END_P
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_STMT_LIST, $1);
      parse_ctx->stmt_tree_ = $$;
      YYACCEPT;
    }
;

stmt_list:
    stmt_list ';' stmt
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
      parse_ctx->stmt_tree_ = $$;
    }
  | stmt
    {
      $$ = $1;
      parse_ctx->stmt_tree_ = $$;
    }
/*  | stmt_list ';' error
    {

    }
  | error
    {

    }*/
;

stmt:
  outer_stmt
  {
    if(NULL != $1 && !parse_ctx->is_inner_parse_) {
      switch($1->type_) {
        // wrap nodes of following types into an anonymous block to mock SQL execution.
        case T_SP_DO:
        case T_SP_SIGNAL:
        case T_SP_RESIGNAL: {
          ParseNode *proc_stmts = NULL;
          merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $1);
          ParseNode *block_content = NULL;
          merge_nodes(block_content, parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, proc_stmts);
          malloc_non_terminal_node($1, parse_ctx->mem_pool_, T_SP_ANONYMOUS_BLOCK, 1, block_content);
        } break;
        default:{
          // do nothing
        } break;
      }
    }

    $$ = $1;
    int32_t str_len = @1.last_column - @1.first_column + 1;
    $$->pos_ = @1.first_column;
    $$->str_len_ = str_len;
  }
  | /*Empty*/ { $$ = NULL; }

outer_stmt:
    create_procedure_stmt { $$ = $1; }
  | create_function_stmt { $$ = $1; }
  | drop_procedure_stmt { $$ = $1; }
  | drop_function_stmt { $$ = $1; }
  | alter_procedure_stmt { $$ = $1; }
  | alter_function_stmt { $$ = $1; }
  | create_package_stmt { $$ = $1; }
  | create_package_body_stmt { $$ = $1; }
  | drop_package_stmt { $$ = $1; }
  | sql_stmt { $$ = $1; }
  | call_sp_stmt { $$ = $1; }
  | do_sp_stmt { $$ = $1; }
  | signal_stmt { $$ = $1; }
  | resignal_stmt { $$ = $1; }
  | package_block { $$ = $1; }
  | package_body_block { $$ = $1; }
  | create_trigger_stmt { $$ = $1; }
  | drop_trigger_stmt { $$ = $1; }
  | plsql_trigger_source
    {
      if (!parse_ctx->is_inner_parse_) {
        obpl_mysql_yyerror(&@1, parse_ctx, "Syntax Error\n");
        YYERROR;
      }
      $$ = $1;
    }
;

/*****************************************************************************
 *
 *      sql_stmt grammar
 *
 *****************************************************************************/
sql_keyword:
    '(' sql_keyword { $$ = NULL; }
  | SQL_KEYWORD { $$ = NULL; }
  | TABLE { $$ = NULL; }
  | USER { $$ = NULL; }
  | INSERT { $$ = NULL; }
  | DELETE { $$ = NULL; }
  | UPDATE { $$ = NULL; }
;

sql_stmt:
    sql_keyword /*sql stmt tail*/
    {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, @1.first_column, @1.last_column, 2, ';', END_P);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
    }
  | REPLACE /*sql stmt tail*/
    {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, @1.first_column, @1.last_column, 2, ';', END_P);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
    }
  | CREATE sql_keyword /*sql stmt tail*/
    {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, @1.first_column, @1.last_column, 2, ';', END_P);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
    }
  | CREATE ROLE /*sql stmt tail*/
    {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, @1.first_column, @1.last_column, 2, ';', END_P);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
    }
  | DROP ROLE /*sql stmt tail*/
    {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, @1.first_column, @1.last_column, 2, ';', END_P);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
    }
  | CREATE OR REPLACE sql_keyword /*sql stmt tail*/
    {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, @1.first_column, @1.last_column, 2, ';', END_P);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
    }
  | DROP sql_keyword /*sql stmt tail*/
    {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, @1.first_column, @1.last_column, 2, ';', END_P);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
    }
  | ALTER sql_keyword /*sql stmt tail*/
    {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, @1.first_column, @1.last_column, 2, ';', END_P);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
    }
  | SET /*sql stmt tail*/
    {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, @1.first_column, @1.last_column, 2, ';', END_P);
      if (T_SET_PASSWORD == sql_stmt->type_ ||
          T_SET_NAMES == sql_stmt->type_ ||
          T_SET_CHARSET == sql_stmt->type_ ||
          T_SET_ROLE == sql_stmt->type_ ||
          T_ALTER_USER_DEFAULT_ROLE == sql_stmt->type_) {
        malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
      } else {
        $$ = sql_stmt;
      }
      if(T_VARIABLE_SET == $$->type_) {
        int64_t child_cnt = $$->num_child_;
        for(int64_t i = 0; i < $$->num_child_; ++i) {
          if (T_SET_NAMES == $$->children_[i]->type_ || T_SET_CHARSET == $$->children_[i]->type_) {
            malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
            if (child_cnt > 1) {
              obpl_mysql_yyerror(&@1, parse_ctx, "Syntax Error\n");
              YYERROR;
            }
          } else if(OB_UNLIKELY(NULL == $$->children_[i] || NULL == $$->children_[i]->children_[1])) {
            YY_UNEXPECTED_ERROR("value node in SET statement is NULL");
          } else {
            obpl_mysql_wrap_get_user_var_into_subquery(parse_ctx, $$->children_[i]->children_[1]);
          }
        }
      }
    }
  | COMMIT
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT);
      $$->str_value_ = parse_strdup("COMMIT", parse_ctx->mem_pool_, &($$->str_len_));
      if (OB_UNLIKELY(NULL == $$->str_value_)) {
        YY_FATAL_ERROR("no more memory to malloc 'COMMIT; string\n");
      }
    }
  | ROLLBACK
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT);
      $$->str_value_ = parse_strdup("ROLLBACK", parse_ctx->mem_pool_, &($$->str_len_));
      if (OB_UNLIKELY(NULL == $$->str_value_)) {
        YY_FATAL_ERROR("no more memory to malloc 'ROLLBACK; string\n");
      }
    }
  | SELECT //为了支持（select……）的用法，把SELECT也从sql_keyword分支里拿出来
  {
    //read sql query string直到读到token';'或者END_P
    ParseNode *sql_stmt = NULL;
    do_parse_sql_stmt(sql_stmt, parse_ctx, @1.first_column, @1.last_column, 2, ';', END_P);
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
  }
  | '(' SELECT %prec PARENS
  {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, @1.first_column, @1.last_column, 2, ';', END_P);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
  }
;

do_sp_stmt:
    DO expr_list
    {
      if (NULL == $2) {
        obpl_mysql_yyerror(&@2, parse_ctx, "Syntax Error, Invalid expr to be evaluated\n");
        YYERROR;
      } else if (T_OBJ_ACCESS_REF == $2->type_) {
        ParseNode *cur_node = $2;
        ParseNode *last_node = NULL;
        while (NULL != cur_node->children_[1] && T_OBJ_ACCESS_REF == cur_node->children_[1]->type_) {
          last_node = cur_node;
          cur_node = cur_node->children_[1];
        }
        if (OB_UNLIKELY(NULL == cur_node || NULL != cur_node->children_[1] || NULL == cur_node->children_[0])) {
          obpl_mysql_yyerror(&@2, parse_ctx, "Syntax Error, Invalid ObjAccess string\n");
          YYERROR;
        } else if (T_IDENT == cur_node->children_[0]->type_
            && 6 == cur_node->children_[0]->str_len_
            && 0 == strncasecmp(cur_node->children_[0]->str_value_, "EXTEND", 6)) {
          if (OB_UNLIKELY(NULL == last_node)) {
            obpl_mysql_yyerror(&@2, parse_ctx, "Syntax Error, Invalid ObjAccess string\n");
            YYERROR;
          } else {
            last_node->children_[1] = NULL;
            malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_EXTEND, 2, $2, NULL);
          }
        } else if (T_FUN_SYS == cur_node->children_[0]->type_
            && NULL != cur_node->children_[0]->children_[0]
            && T_IDENT == cur_node->children_[0]->children_[0]->type_
            && 6 == cur_node->children_[0]->children_[0]->str_len_
            && 0 == strncasecmp(cur_node->children_[0]->children_[0]->str_value_, "EXTEND", 6)) {
          if (OB_UNLIKELY(NULL == last_node)) {
            obpl_mysql_yyerror(&@2, parse_ctx, "Syntax Error, Invalid ObjAccess string\n");
            YYERROR;
          } else {
            last_node->children_[1] = NULL;
            malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_EXTEND, 2, $2, cur_node->children_[0]);
          }
        } else {
        ParseNode *do_expr_list = NULL;
        merge_nodes(do_expr_list, parse_ctx->mem_pool_, T_EXPR_LIST, $2);
        malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DO, 1, do_expr_list);
        }
      } else {
        ParseNode *do_expr_list = NULL;
        merge_nodes(do_expr_list, parse_ctx->mem_pool_, T_EXPR_LIST, $2);
        for (int64_t i = 0; i < do_expr_list->num_child_; ++i) {
          if (T_COLUMN_REF == do_expr_list->children_[i]->type_) {
            obpl_mysql_yyerror(&@2, parse_ctx, "Syntax Error, DO statement cannot reference to a table\n");
            YYERROR;
            break;
          }
        }
        malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DO, 1, do_expr_list);
      }
    }
  | DO sp_proc_stmt_open { $$ = $2; }
  | DO sp_proc_stmt_fetch { $$ = $2; }
  | DO sp_proc_stmt_close { $$ = $2; }
;

call_sp_stmt:
    CALL sp_call_name opt_sp_cparam_list
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_CALL_STMT, 2, $2, $3);
    }
  | CALL PROCEDURE opt_if_not_exists sp_name '(' opt_sp_param_list ')' sp_create_chistics procedure_body
    {
      if (!parse_ctx->is_inner_parse_) {
        obpl_mysql_yyerror(&@2, parse_ctx, "Syntax Error\n");
        YYERROR; //生成一个语法错误
      }
      $$ = $9;
    }
  | CALL PROCEDURE opt_if_not_exists sp_name '(' opt_sp_param_list ')' procedure_body
    {
      if (!parse_ctx->is_inner_parse_) {
        obpl_mysql_yyerror(&@2, parse_ctx, "Syntax Error\n");
        YYERROR; //生成一个语法错误
      }
      $$ = $8;
    }
  | CALL FUNCTION opt_if_not_exists sp_name '(' opt_sp_fparam_list ')' RETURNS sp_data_type sp_create_chistics function_body
    {
      if (!parse_ctx->is_inner_parse_) {
        obpl_mysql_yyerror(&@2, parse_ctx, "Syntax Error\n");
        YYERROR; //生成一个语法错误
      }
      $$ = $11;
    }
  | CALL FUNCTION opt_if_not_exists sp_name '(' opt_sp_fparam_list ')' RETURNS sp_data_type function_body
    {
      if (!parse_ctx->is_inner_parse_) {
        obpl_mysql_yyerror(&@2, parse_ctx, "Syntax Error\n");
        YYERROR; //生成一个语法错误
      }
      $$ = $10;
    }
;

opt_sp_cparam_list:
    /* Empty */ { $$ = NULL; }
  | '(' ')' { $$ = NULL; }
  | '(' sp_cparams ')'
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_SP_CPARAM_LIST, $2);
    }
;

sp_cparams:
    sp_cparams ',' sp_cparam
    {
      if ($1 == NULL || $3 == NULL) {
        YYERROR;
      }
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
  | sp_cparam { $$ = $1; }
;

sp_cparam:
  cexpr opt_sp_cparam_with_assign
  {
    if (NULL == $1 && NULL != $2) {
      YYERROR;
    }
    if (NULL == $2) {
      $$ = $1;
    }
    if (NULL != $2) {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_CPARAM, 2, $1, $2);
    }
      if (NULL != $1)
    copy_node_abs_location($1->stmt_loc_, @1);
  }
;

opt_sp_cparam_with_assign:
    /*EMPTY*/ { $$ = NULL; }
  | PARAM_ASSIGN_OPERATOR cexpr
  {
    if (NULL == $2) YYERROR; $$ = $2;
      if (NULL != $2)
    copy_node_abs_location($2->stmt_loc_, @2);
  }
;

cexpr:
    %prec LOWER_PARENS {
      //same as expr in sql rule, and terminate when read ';'
      do_parse_sql_expr_rule($$, parse_ctx, 3, ',', ')', PARAM_ASSIGN_OPERATOR);
      if (NULL == $$) {
        YYERROR;
      }
    }
;

/*****************************************************************************
 *
 *      common pl grammar
 *
 *****************************************************************************/
sp_name:
    ident '.' ident
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_NAME, 2, $1, $3);
    }
  | ident
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_NAME, 2, NULL, $1);
    }
;

sp_call_name:
    ident '.' ident '.' ident
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ACCESS_NAME, 3, $1, $3, $5);
    }
  | ident '.' ident
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ACCESS_NAME, 3, NULL, $1, $3);
    }
  | ident
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ACCESS_NAME, 3, NULL, NULL, $1);
    }
;

ident:
    IDENT
    {
      $$ = $1;
      $$->pl_str_off_ = @1.first_column;
    }
  | unreserved_keyword
    {
      get_non_reserved_node($$, parse_ctx->mem_pool_, @1.first_column, @1.last_column);
      $$->pl_str_off_ = @1.first_column;
    }
;

unreserved_keyword:
    AFTER
  | AUTHID
  | BEGIN_KEY %prec LOWER_PARENS
  | BINARY_INTEGER
  | BODY %prec LOWER_PARENS
  | C
  | CATALOG_NAME
  | CLASS_ORIGIN
  | CLOSE
  | COLUMN_NAME
  | COMMENT
  | CONSTRAINT_CATALOG
  | CONSTRAINT_NAME
  | CONSTRAINT_ORIGIN
  | CONSTRAINT_SCHEMA
  | CONTAINS
  | COUNT
  | CURSOR_NAME
  | DATA
  | DEFINER
  | END_KEY %prec LOWER_PARENS
  | EXTEND
  | FOUND
  | FUNCTION
  | HANDLER
  | INTERFACE 
  | INVOKER
  | JSON
  | LANGUAGE
  | MESSAGE_TEXT
  | MYSQL_ERRNO
  | NEXT
  | NO
  | OF
  | OPEN
  | USER
  | PACKAGE
  | PRAGMA
  | RECORD
  | RETURNS
  | ROW
  | ROWTYPE
  | ROLE
  | SCHEMA_NAME
  | SECURITY
  | SUBCLASS_ORIGIN
  | TABLE_NAME
  | TYPE
  | VALUE
  | FOLLOWS
  | PRECEDES
  | NATIONAL
  | DATETIME
  | TIMESTAMP
  | TIME
  | DATE
  | YEAR
  | TEXT
  | NCHAR
  | NVARCHAR
  | BOOL
  | BOOLEAN
  | ENUM
  | BIT
  | FIXED
  | SIGNED
;

/*****************************************************************************
 *
 *      CREATE PACKAGE grammar
 *
 *****************************************************************************/
create_package_stmt:
    CREATE opt_replace package_block
    {
      const char *stmt_str = parse_ctx->orig_stmt_str_ + @3.first_column;
      int32_t str_len = @3.last_column - @3.first_column + 1;
      $3->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($3->str_value_);
      $3->str_len_ = str_len;
      $3->pl_str_off_ = @3.first_column;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_CREATE, 1, $3);
      $$->int32_values_[0] = $2;
      $$->int32_values_[1] = 0;
    }
;

package_block:
    PACKAGE sp_name opt_proc_clause opt_package_stmts END_KEY opt_tail_package_name
    {
      ParseNode *pkg_decl_stmts = NULL;
      merge_nodes(pkg_decl_stmts, parse_ctx->mem_pool_, T_PACKAGE_STMTS, $4);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_BLOCK, 4, $2, $3, pkg_decl_stmts, $6);
      if (parse_ctx->is_inner_parse_) {
        const char *stmt_str = parse_ctx->orig_stmt_str_ + @4.first_column;
        int32_t str_len = @6.last_column - @4.first_column + 1;
        $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
        check_ptr($$->str_value_);
        $$->str_len_ = str_len;
        $$->pl_str_off_ = @4.first_column;
      }
    }
;

invoke_right:
    AUTHID CURRENT_USER
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INVOKE);
      $$->value_ = SP_CURRENT_USER;
    }
  | AUTHID DEFINER
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INVOKE);
      $$->value_ = SP_DEFINER;
    }
;

proc_clause:
    invoke_right { $$ = $1; }
;

proc_clause_list:
    proc_clause_list proc_clause
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
  | proc_clause { $$ = $1; }
;

opt_proc_clause:
    /*EMPTY*/ { $$ = NULL; }
  | proc_clause_list
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_SP_CLAUSE_LIST, $1);
    }
;

opt_replace:
      /*EMPTY*/       { $$ = 0; }
    | OR REPLACE      { $$ = 1; }
;

opt_package_stmts:
    /*Empty*/ { $$ = NULL; }
  | package_stmts 
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_PACKAGE_STMTS, $1);
    }
;

package_stmts:
    package_stmts package_stmt ';'
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
  | package_stmt ';' { $$ = $1; }
  
package_stmt:
    sp_decl    { $$ = $1; }
  | func_decl  { $$ = $1; }
  | proc_decl  { $$ = $1; }
;

func_decl:
    FUNCTION ident '(' opt_sp_param_list ')' RETURN sp_data_type
    {
      const char *stmt_str = parse_ctx->stmt_str_ + @1.first_column;
      int32_t str_len = @7.last_column - @1.first_column + 1;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SUB_FUNC_DECL, 5, $2, $4, $7, NULL, NULL);
      check_ptr($$);
      $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($$->str_value_);
      $$->str_len_ = str_len;
    }
;

proc_decl:
    PROCEDURE ident '(' opt_sp_param_list ')'
    {
      const char *stmt_str = parse_ctx->stmt_str_ + @1.first_column;
      int32_t str_len = @5.last_column - @1.first_column + 1;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SUB_PROC_DECL, 3, $2, $4, NULL);
      check_ptr($$);
      $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($$->str_value_);
      $$->str_len_ = str_len;
    }
;

opt_tail_package_name:
      /*EMPTY*/     { $$ = NULL; }
    | ident         { $$ = $1; }
;

/*****************************************************************************
 *
 *      CREATE PACKAGE BODY grammar
 *
 *****************************************************************************/
create_package_body_stmt:
    CREATE opt_replace package_body_block
    {
      check_ptr($3);
      const char *stmt_str = parse_ctx->orig_stmt_str_ + @3.first_column;
      int32_t str_len = @3.last_column - @3.first_column + 1;
      $3->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($3->str_value_);
      $3->str_len_ = str_len;
      $3->pl_str_off_ = @3.first_column;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_CREATE_BODY, 1, $3);
      $$->int32_values_[0] = $2;
      $$->int32_values_[1] = 0;
    }
;


package_body_block:
    PACKAGE BODY sp_name decl_stmt_ext_list sp_proc_stmts END_KEY opt_tail_package_name %prec PARENS
    {
      ParseNode *proc_stmts = NULL;
      ParseNode *decl_ext_list = NULL;
      merge_nodes(decl_ext_list, parse_ctx->mem_pool_, T_PACKAGE_BODY_STMTS, $4);
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $5);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_BODY_BLOCK, 4, $3, decl_ext_list, proc_stmts, $7);
      if (parse_ctx->is_inner_parse_) {
        const char *stmt_str = parse_ctx->orig_stmt_str_ + @4.first_column;
        int32_t str_len = @7.last_column - @4.first_column + 1;
        $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
        check_ptr($$->str_value_);
        $$->str_len_ = str_len;
        $$->pl_str_off_ = @4.first_column;
      }
    }
  | PACKAGE BODY sp_name sp_proc_stmts END_KEY opt_tail_package_name %prec PARENS
    {
      ParseNode *proc_stmts = NULL;
      ParseNode *decl_ext_list = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $4);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_BODY_BLOCK, 4, $3, decl_ext_list, proc_stmts, $6);
      if (parse_ctx->is_inner_parse_) {
        const char *stmt_str = parse_ctx->orig_stmt_str_ + @4.first_column;
        int32_t str_len = @6.last_column - @4.first_column + 1;
        $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
        check_ptr($$->str_value_);
        $$->str_len_ = str_len;
        $$->pl_str_off_ = @4.first_column;
      }
    }
  | PACKAGE BODY sp_name decl_stmt_ext_list END_KEY opt_tail_package_name %prec PARENS
    {
      ParseNode *proc_stmts = NULL;
      ParseNode *decl_ext_list = NULL;
      merge_nodes(decl_ext_list, parse_ctx->mem_pool_, T_PACKAGE_BODY_STMTS, $4);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_BODY_BLOCK, 4, $3, decl_ext_list, proc_stmts, $6);
      if (parse_ctx->is_inner_parse_) {
        const char *stmt_str = parse_ctx->orig_stmt_str_ + @4.first_column;
        int32_t str_len = @6.last_column - @4.first_column + 1;
        $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
        check_ptr($$->str_value_);
        $$->str_len_ = str_len;
      }
    }
  | PACKAGE BODY sp_name END_KEY opt_tail_package_name %prec PARENS
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_BODY_BLOCK, 4, $3, NULL, NULL, $5);
      if (parse_ctx->is_inner_parse_) {
        const char *stmt_str = parse_ctx->orig_stmt_str_ + @4.first_column;
        int32_t str_len = @5.last_column - @4.first_column + 1;
        $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
        check_ptr($$->str_value_);
        $$->str_len_ = str_len;
        $$->pl_str_off_ = @4.first_column;
      }
    }
;

/*****************************************************************************
 *
 *      DROP PACKAGE grammar
 *
 *****************************************************************************/
drop_package_stmt:
    DROP PACKAGE sp_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_DROP, 1, $3);
      $$->value_ = 0;
    }
    | DROP PACKAGE BODY sp_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_DROP, 1, $4);
      $$->value_ = 2;
    }
;

interface_pragma:
  PRAGMA INTERFACE '(' C ',' ident ')'
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PRAGMA_INTERFACE, 1, $6);
  }
;

decl_stmt_ext_list:
    decl_stmt_ext ';' { $$ = $1; }
  | decl_stmt_ext_list decl_stmt_ext ';'
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
;

decl_stmt_ext:
    sp_decl   { $$ = $1; }
  | func_decl  { $$ = $1; }
  | proc_decl  { $$ = $1; }
  | interface_pragma { $$ = $1; }
  | func_decl /*is_or_as*/ function_body
    {
      check_ptr($2);
      const char *stmt_str = parse_ctx->stmt_str_ + @1.first_column;
      int32_t str_len = @2.last_column - @1.first_column + 1;
      $2->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($2->str_value_);
      $2->str_len_ = str_len;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SUB_FUNC_DEF, 2, $1, $2);
    }
  | proc_decl /*is_or_as*/ procedure_body 
    {
      check_ptr($2);
      const char *stmt_str = parse_ctx->stmt_str_ + @1.first_column;
      int32_t str_len = @2.last_column - @1.first_column + 1;
      $2->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($2->str_value_);
      $2->str_len_ = str_len;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SUB_PROC_DEF, 2, $1, $2);
    }
;

/*****************************************************************************
 *
 *      CREATE TRIGGER grammar
 *
 *****************************************************************************/
create_trigger_stmt:
    CREATE opt_sp_definer plsql_trigger_source
    {
      check_ptr($3);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_CREATE, 2, $2, $3);
      $$->int32_values_[0] = 0; // or_replace = false
      $$->int32_values_[1] = 0; // editionable = false
    }
;

plsql_trigger_source:
    TRIGGER opt_if_not_exists sp_name trigger_definition
    {
      check_ptr($4);
      const char *stmt_str = parse_ctx->stmt_str_ + @1.first_column;
      int32_t str_len = @4.last_column - @1.first_column + 1;
      $4->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($4->str_value_);
      $4->str_len_ = str_len;
      $4->pl_str_off_ = @1.first_column;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_SOURCE, 2, $3, $4);
      $$->value_ = $2;
    }
;

trigger_definition:
    trigger_time trigger_event ON sp_name FOR EACH ROW trigger_body
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_SIMPLE_DML, 4, $2, $4, NULL, $8);
      $$->int16_values_[0] = $1;
    }
    | trigger_time trigger_event ON sp_name FOR EACH ROW FOLLOWS sp_name trigger_body
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_ORDER, 1, $9);
      $$->value_ = 1; // FOLLOWS
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_SIMPLE_DML, 4, $2, $4, $$, $10);
      $$->int16_values_[0] = $1;
    }
    | trigger_time trigger_event ON sp_name FOR EACH ROW PRECEDES sp_name trigger_body
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_ORDER, 1, $9);
      $$->value_ = 2; // PRECEDES
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_SIMPLE_DML, 4, $2, $4, $$, $10);
      $$->int16_values_[0] = $1;
    }

trigger_time:
    BEFORE { $$ = T_BEFORE; }
  | AFTER  { $$ = T_AFTER; }
;

trigger_event:
    INSERT { malloc_terminal_node($$, parse_ctx->mem_pool_, T_INSERT); }
  | DELETE { malloc_terminal_node($$, parse_ctx->mem_pool_, T_DELETE); }
  | UPDATE { malloc_terminal_node($$, parse_ctx->mem_pool_, T_UPDATE); }
;

trigger_body:
  sp_proc_stmt
  {
    $$ = $1;
    const char *body_str = parse_ctx->stmt_str_ + @1.first_column;
    int32_t body_len = @1.last_column - @1.first_column + 1 + 2;
    char *dup_body = NULL;
    if (OB_LIKELY(NULL != (dup_body = (char *)parse_malloc(body_len + 1, parse_ctx->mem_pool_)))) {
      memmove(dup_body, body_str, body_len - 2);
      dup_body[body_len - 2] = ';';
      dup_body[body_len - 1] = '\n';
      dup_body[body_len] = '\0';
    }
    check_ptr(dup_body);
    $$->str_value_ = dup_body;
    $$->str_len_ = body_len;
    $$->pl_str_off_ = @1.first_column;
  }
;

/*****************************************************************************
 *
 *      DROP TRIGGER grammar
 *
 *****************************************************************************/
drop_trigger_stmt:
    DROP TRIGGER opt_if_exists sp_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_DROP, 1, $4);
      $$->value_ = $3;
    }
;

/*****************************************************************************
 *
 *      CREATE PROCEDURE grammar
 *
 *****************************************************************************/
create_procedure_stmt:
    CREATE opt_sp_definer PROCEDURE opt_if_not_exists sp_name '(' opt_sp_param_list ')' sp_create_chistics procedure_body
    {
      check_ptr($10);
      ParseNode *sp_clause_node = NULL;
      merge_nodes(sp_clause_node, parse_ctx->mem_pool_, T_SP_CLAUSE_LIST, $9);
      const char *stmt_str = parse_ctx->stmt_str_ + @3.first_column;
      int32_t str_len = @10.last_column - @3.first_column + 1;
      $10->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($10->str_value_);
      $10->str_len_ = str_len;
      $10->raw_text_ = $10->str_value_ + @10.first_column - @3.first_column;
      $10->text_len_ = str_len - (@10.first_column - @3.first_column);
      if (NULL != $7) {
        const char *param_str = parse_ctx->stmt_str_ + @6.first_column + 1;
        int32_t param_len = @8.last_column - @6.last_column - 1;
        $7->str_value_ = parse_strndup(param_str, param_len, parse_ctx->mem_pool_);
        check_ptr($7->str_value_);
        $7->str_len_ = param_len;
      }
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_CREATE, 5, $2, $5, $7, sp_clause_node, $10);
      $$->value_ = $4;
    }
  | CREATE opt_sp_definer PROCEDURE opt_if_not_exists sp_name '(' opt_sp_param_list ')' procedure_body
    {
      check_ptr($9);
      const char *stmt_str = parse_ctx->stmt_str_ + @3.first_column;
      int32_t str_len = @9.last_column - @3.first_column + 1;
      $9->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($9->str_value_);
      $9->str_len_ = str_len;
      $9->raw_text_ = $9->str_value_ + @9.first_column - @3.first_column;
      $9->text_len_ = str_len - (@9.first_column - @3.first_column);
      if (NULL != $7) {
        const char *param_str = parse_ctx->stmt_str_ + @6.first_column + 1;
        int32_t param_len = @8.last_column - @6.last_column - 1;
        $7->str_value_ = parse_strndup(param_str, param_len, parse_ctx->mem_pool_);
        check_ptr($7->str_value_);
        $7->str_len_ = param_len;
      }
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_CREATE, 5, $2, $5, $7, NULL, $9);
      $$->value_ = $4;
    }
;

create_function_stmt:
    CREATE opt_sp_definer FUNCTION opt_if_not_exists sp_name '(' opt_sp_fparam_list ')' RETURNS sp_data_type sp_create_chistics function_body
    {
      check_ptr($12);
      ParseNode *sp_clause_node = NULL;
      merge_nodes(sp_clause_node, parse_ctx->mem_pool_, T_SP_CLAUSE_LIST, $11);
      const char *stmt_str = parse_ctx->stmt_str_ + @3.first_column;
      int32_t str_len = @12.last_column - @3.first_column + 1;
      $12->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($12->str_value_);
      $12->str_len_ = str_len;
      $12->raw_text_ = $12->str_value_ + @12.first_column - @3.first_column;
      $12->text_len_ = str_len - (@12.first_column - @3.first_column);
      if (NULL != $7) {
        const char *param_str = parse_ctx->stmt_str_ + @6.first_column + 1;
        int32_t param_len = @8.last_column - @6.last_column - 1;
        $7->str_value_ = parse_strndup(param_str, param_len, parse_ctx->mem_pool_);
        check_ptr($7->str_value_);
        $7->str_len_ = param_len;
      }
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SF_CREATE, 6, $2, $5, $7, $10, sp_clause_node, $12);
      $$->value_ = $4;
    }
  | CREATE opt_sp_definer FUNCTION opt_if_not_exists sp_name '(' opt_sp_fparam_list ')' RETURNS sp_data_type function_body
    {
      check_ptr($11);
      const char *stmt_str = parse_ctx->stmt_str_ + @3.first_column;
      int32_t str_len = @11.last_column - @3.first_column + 1;
      $11->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($11->str_value_);
      $11->str_len_ = str_len;
      $11->raw_text_ = $11->str_value_ + @11.first_column - @3.first_column;
      $11->text_len_ = str_len - (@11.first_column - @3.first_column);
      if (NULL != $7) {
        const char *param_str = parse_ctx->stmt_str_ + @6.first_column + 1;
        int32_t param_len = @8.last_column - @6.last_column - 1;
        $7->str_value_ = parse_strndup(param_str, param_len, parse_ctx->mem_pool_);
        check_ptr($7->str_value_);
        $7->str_len_ = param_len;
      }
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SF_CREATE, 6, $2, $5, $7, $10, NULL, $11);
      $$->value_ = $4;
    }
;

opt_sp_definer:
    /* empty */ { $$ = NULL; }
  | DEFINER '=' user opt_host_name
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_USER_WITH_HOST_NAME, 2, $3, $4);
  }
  | DEFINER '=' CURRENT_USER opt_parentheses
  {
    ParseNode *user_node = NULL;
    malloc_terminal_node(user_node, parse_ctx->mem_pool_, T_IDENT);
    user_node->str_value_ = "CURRENT_USER";
    user_node->str_len_ = strlen("CURRENT_USER");
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_USER_WITH_HOST_NAME, 2, user_node, NULL);
  }
;

user:
STRING
{
  $$ = $1;
}
| IDENT
{
  $$ = $1;
}

opt_host_name:
USER_VARIABLE
{
  $$ = $1;
}
|
{
  $$ = NULL;
}
;


opt_parentheses:
    /* empty */ { $$ = NULL; }
  | '(' ')' { $$ = NULL; }

/*stored procedure param list*/
opt_sp_param_list:
    /* empty */ { $$ = NULL; }
  | sp_param_list
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_SP_PARAM_LIST, $1);
    }
;

sp_param_list:
    sp_param_list ',' sp_param
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
  | sp_param { $$ = $1; }
;

sp_param:
    opt_sp_inout ident param_type opt_param_default
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PARAM, 3, $2, $3, $4);
      $$->value_ = $1;
    }
;

/*stored function param list*/
opt_sp_fparam_list:
    /* empty */ { $$ = NULL; }
  | sp_fparam_list
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_SP_PARAM_LIST, $1);
    }
;

sp_fparam_list:
    sp_fparam_list ',' sp_fparam
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
  | sp_fparam { $$ = $1; }
;

sp_fparam:
    ident param_type
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PARAM, 2, $1, $2);
      $$->value_ = MODE_IN;
    }
;

opt_sp_inout:
    /* Empty */ { $$ = MODE_IN; }
  | IN      { $$ = MODE_IN; }
  | OUT     { $$ = MODE_OUT; }
  | INOUT { $$ = MODE_INOUT; }
;

param_type:
    sp_data_type { $$ = $1; }

simple_ident:
    ident
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_COLUMN_REF, 3, NULL, NULL, $1);
    }
  | ident '.' ident
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_COLUMN_REF, 3, NULL, $1, $3);
    }
  | '.' ident '.' ident
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_COLUMN_REF, 3, NULL, $2, $4);
    }
  | ident '.' ident '.' ident
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_COLUMN_REF, 3, $1, $3, $5);
    }
;

sp_create_chistics:
    sp_create_chistic { $$ = $1; }
  | sp_create_chistics sp_create_chistic
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
;

sp_create_chistic:
    sp_chistic { $$ = $1; }
  | DETERMINISTIC
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_DETERMINISTIC);
  }
  | NOT DETERMINISTIC { }
;

sp_chistic:
    COMMENT STRING
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_COMMENT);
    $$->str_value_ = $2->str_value_;
    $$->str_len_ = $2->str_len_;
  }
  | LANGUAGE SQL { /* Just parse it, we only have one language for now. */ $$ = NULL; }
  | NO SQL
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_DATA_ACCESS);
    $$->value_ = SP_NO_SQL;
  }
  | CONTAINS SQL
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_DATA_ACCESS);
    $$->value_ = SP_CONTAINS_SQL;
  }
  | READS SQL DATA
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_DATA_ACCESS);
    $$->value_ = SP_READS_SQL_DATA;
  }
  | MODIFIES SQL DATA
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_DATA_ACCESS);
    $$->value_ = SP_MODIFIES_SQL_DATA;
  }
  | SQL SECURITY DEFINER
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INVOKE);
    $$->value_ = SP_DEFINER;
  }
  | SQL SECURITY INVOKER
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INVOKE);
    $$->value_ = SP_INVOKER;
  }
;

procedure_body:
  sp_proc_stmt { $$ = $1; }
;

function_body:
  sp_proc_independent_statement { $$ = $1; }
;

/*****************************************************************************
 *
 *      ALTER PROCEDURE grammar
 *
 *****************************************************************************/
alter_procedure_stmt:
    ALTER PROCEDURE sp_name opt_sp_alter_chistics
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ALTER, 2, $3, $4);
    }
;

alter_function_stmt:
    ALTER FUNCTION sp_name opt_sp_alter_chistics
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SF_ALTER, 2, $3, $4);
    }
;

opt_sp_alter_chistics:
    /* empty */ { $$ = NULL; }
  | sp_alter_chistics
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_SP_CLAUSE_LIST, $1);
    }
;

sp_alter_chistics:
  sp_chistic { $$ = $1; }
  | sp_alter_chistics sp_chistic
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
  }

sp_proc_stmt:
    sp_proc_outer_statement
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | sp_proc_inner_statement
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;

sp_proc_outer_statement:
    outer_stmt
    {
      $$ = $1;
      const char *stmt_str = parse_ctx->stmt_str_ + @1.first_column;
      int32_t str_len = @1.last_column - @1.first_column + 1;
      $1->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($1->str_value_);
      $1->str_len_ = str_len;
    }
;

sp_proc_inner_statement:
  sp_proc_independent_statement
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | sp_proc_stmt_iterate
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | sp_proc_stmt_leave
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | sp_proc_stmt_open
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | sp_proc_stmt_fetch
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | sp_proc_stmt_close
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;

sp_proc_independent_statement: //可以独立存在（不依赖于其他语句）的内部语句类型
  sp_proc_stmt_if
    {
      $$ = $1;
    }
  | sp_proc_stmt_case
    {
      $$ = $1;
    }
  | sp_unlabeled_block
    {
      $$ = $1;
    }
  | sp_labeled_block
    {
      $$ = $1;
    }
  | sp_unlabeled_control
    {
      $$ = $1;
    }
  | sp_labeled_control
    {
      $$ = $1;
    }
  | sp_proc_stmt_return
    {
      $$ = $1;
    }
;

sp_proc_stmt_if:
    IF sp_if END_KEY IF { $$ = $2; }
;

sp_if:
    expr THEN sp_proc_stmts sp_elseifs
    {
      if (NULL == $1) {
        YYERROR;
      }
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $3);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_IF, 3, $1, proc_stmts, $4);
    }
  | expr THEN sp_proc_stmts %prec PARENS
    {
      if (NULL == $1) {
        YYERROR;
      }
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $3);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_IF, 3, $1, proc_stmts, NULL);
    }
;

sp_proc_stmt_case:
    CASE expr sp_when_list sp_elseifs END_KEY CASE
    {
      ParseNode *when_list = NULL;
      merge_nodes(when_list, parse_ctx->mem_pool_, T_WHEN_LIST, $3);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_CASE, 3, $2, when_list, $4);
    }
  | CASE expr sp_when_list END_KEY CASE
    {
      ParseNode *when_list = NULL;
      merge_nodes(when_list, parse_ctx->mem_pool_, T_WHEN_LIST, $3);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_CASE, 3, $2, when_list, NULL);
    }
;

sp_when_list:
    sp_when { $$ = $1; }
    | sp_when_list sp_when
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
;

sp_when:
    WHEN expr THEN sp_proc_stmts %prec PARENS
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $4);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_WHEN, 2, $2, proc_stmts);
    }
;

sp_elseifs:
  // | ELSE IF sp_if
  //   {
  //     ParseNode *proc_stmts = NULL;
  //     merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $3);
  //     malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ELSE, 1, proc_stmts);
  //   }
    ELSEIF sp_if
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $2);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ELSE, 1, proc_stmts);
    }
  | ELSE sp_proc_stmts
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $2);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ELSE, 1, proc_stmts);
    }
;

sp_unlabeled_block:
    sp_block_content { $$ = $1; }
;

sp_block_content:
   BEGIN_KEY END_KEY
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, 2, NULL, NULL);
    }
  |  BEGIN_KEY sp_decls END_KEY
    {
      ParseNode *decl_list = NULL;
      merge_nodes(decl_list, parse_ctx->mem_pool_, T_SP_DECL_LIST, $2);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, 2, decl_list, NULL);
    }
  | BEGIN_KEY sp_proc_stmts END_KEY
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $2);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, 2, NULL, proc_stmts);
    }
  | BEGIN_KEY sp_decls sp_proc_stmts END_KEY
    {
      ParseNode *decl_list = NULL;
      ParseNode *proc_stmts = NULL;
      merge_nodes(decl_list, parse_ctx->mem_pool_, T_SP_DECL_LIST, $2);
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $3);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, 2, decl_list, proc_stmts);
    }
;

sp_labeled_block:
    label_ident ':' sp_block_content opt_sp_label
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LABELED_BLOCK, 3, $1, $3, $4);
    }
;

label_ident:
    ident { $$ = $1; }
;

opt_sp_label:
    /* Empty */ { $$ = NULL }
  | label_ident
    {
      $$ = $1;
    }
;

sp_proc_stmts:
    sp_proc_stmt ';' { $$ = $1; }
  | sp_proc_stmts sp_proc_stmt ';'
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
;

sp_decls:
    sp_decls ';'
    {
      $$ = $1;
    }
  | opt_sp_decls sp_decl ';'
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
;


opt_sp_decls:
    /*Empty*/ { $$ = NULL; }
  | opt_sp_decls sp_decl ';'
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
;

sp_decl:
    DECLARE sp_decl_idents sp_data_type opt_sp_decl_default
    {
      ParseNode *decl_idents = NULL;
      merge_nodes(decl_idents, parse_ctx->mem_pool_, T_SP_DECL_IDENT_LIST, $2);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL, 3, decl_idents, $3, $4);
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | DECLARE ident CONDITION FOR sp_cond
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL_COND, 2, $2, $5);
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | DECLARE sp_handler_type HANDLER FOR sp_hcond_list sp_proc_stmt
    {
      ParseNode *hcond_list = NULL;
      ParseNode *proc_stmts = NULL;
      malloc_non_terminal_node(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, 1, $6);
      merge_nodes(hcond_list, parse_ctx->mem_pool_, T_SP_HCOND_LIST, $5);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL_HANDLER, 2, hcond_list, proc_stmts);
      $$->value_ = $2;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | DECLARE ident CURSOR FOR sql_stmt
    {
      if ($5->children_[0]->type_ != T_SELECT) {
        obpl_mysql_yyerror(&@5, parse_ctx, "Syntax Error\n");
        YYERROR;
      }
      const char *stmt_str = parse_ctx->stmt_str_ + @5.first_column;
      int32_t str_len = @5.last_column - @5.first_column + 1;
      $5->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($5->str_value_);
      $5->str_len_ = str_len;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL_CURSOR, 4, $2, NULL, NULL, $5); //4参数和Oracle模式保持一致
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;

sp_handler_type:
    EXIT { $$ = SP_HANDLER_TYPE_EXIT; }
  | CONTINUE { $$ = SP_HANDLER_TYPE_CONTINUE; }
;

sp_hcond_list:
    sp_hcond { $$ = $1; }
  | sp_hcond_list ',' sp_hcond
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
;

sp_hcond:
    sp_cond { $$ = $1; }
  | ident { $$ = $1; }
  | SQLWARNING { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SQL_WARNING); }
  | NOT FOUND { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SQL_NOT_FOUND); }
  | SQLEXCEPTION { malloc_terminal_node($$, parse_ctx->mem_pool_, T_SQL_EXCEPTION); }
;

sp_cond:
    number_literal
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_CONDITION, 1, $1);
    }
  | sqlstate
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_CONDITION, 1, $1);
    }
;

sqlstate:
    SQLSTATE opt_value STRING
    {
      (void)($2);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STATE, 1, $3);
    }
;

sp_proc_stmt_open:
    OPEN ident
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PROC_OPEN, 3, $2, NULL, NULL, NULL); //4参数和Oracle模式保持一致
    }
;

sp_proc_stmt_close:
    CLOSE ident
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PROC_CLOSE, 1, $2);
    }
;

sp_proc_stmt_fetch:
    FETCH ident into_clause
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PROC_FETCH, 2, $2, $3);
    }
    | FETCH FROM ident into_clause
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PROC_FETCH, 2, $3, $4);
    }
    | FETCH NEXT FROM ident into_clause
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PROC_FETCH, 2, $4, $5);
    }
;

into_clause:
    INTO expr_list
    {
      ParseNode *vars_list = NULL;
      merge_nodes(vars_list, parse_ctx->mem_pool_, T_SP_INTO_LIST, $2);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_INTO_VARIABLES, 1, vars_list);
    }
;

opt_value:
    /*Empty*/ { $$ = NULL; }
  | VALUE { $$ = NULL; }

sp_decl_idents:
    ident { $$ = $1; }
  | sp_decl_idents ',' ident
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
;

sp_data_type:
    scalar_data_type { $$ = $1; }
;

opt_sp_decl_default:
    /*Empty*/ { $$ = NULL; }
  | DEFAULT default_expr
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL_DEFAULT, 1, $2);
    }
;

opt_param_default:
    /*Empty*/ { $$ = NULL; }
  | DEFAULT default_expr
    {
      if (NULL == $2) {
        YYERROR;
      }
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL_DEFAULT, 1, $2);
      // 拷贝EXPR语句
      const char *expr_str = parse_ctx->stmt_str_ + @2.first_column;
      int32_t expr_str_len = @2.last_column - @2.first_column + 1;
      $$->str_value_ = parse_strndup(expr_str, expr_str_len, parse_ctx->mem_pool_);
      check_ptr($$->str_value_);
      $$->str_len_ = expr_str_len;
      if (NULL != $2) {
        copy_node_abs_location($2->stmt_loc_, @2);
      }
    }
;

default_expr:
    {
      do_parse_sql_expr_rule($$, parse_ctx, 3, ',', ')', ';');
    }
;

expr_list:
    expr { $$ = $1; }
  | expr_list ',' expr
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
;

expr:
    {
      //same as expr in sql rule, and terminate when read ';'
      do_parse_sql_expr_rule($$, parse_ctx, 9, INTO, USING, WHEN, THEN, ';', DO, LIMIT, ',', END_KEY);
      obpl_mysql_wrap_get_user_var_into_subquery(parse_ctx, $$);
    }
;

sp_unlabeled_control:
    LOOP sp_proc_stmts END_KEY LOOP
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $2);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LOOP, 1, proc_stmts);
    }
  | WHILE expr DO sp_proc_stmts END_KEY WHILE
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $4);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_WHILE, 2, $2, proc_stmts);
    }
  | REPEAT sp_proc_stmts UNTIL expr END_KEY REPEAT %prec PARENS
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $2);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_REPEAT, 2, proc_stmts, $4);
    }
;

sp_labeled_control:
    label_ident ':' sp_unlabeled_control opt_sp_label
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LABELED_CONTROL, 3, $1, $3, $4);
    }
;

sp_proc_stmt_return:
    RETURN expr
    {
      if (NULL == $2) YYERROR;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_RETURN, 1, $2);
    }
;

sp_proc_stmt_iterate:
    ITERATE label_ident
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ITERATE, 2, $2, NULL);
    }
;

sp_proc_stmt_leave:
    LEAVE label_ident
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LEAVE, 2, $2, NULL);
    }
;

drop_procedure_stmt:
    DROP PROCEDURE opt_if_exists sp_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DROP, 1, $4);
      $$->value_ = $3;
    }
;

drop_function_stmt:
    DROP FUNCTION opt_if_exists sp_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SF_DROP, 1, $4);
      $$->value_ = $3;
    }
;

opt_if_exists:
    /*Empty*/ { $$ = 0; }
  | IF EXISTS { $$ = 1; }
;

opt_if_not_exists:
    /*Empty*/ { $$ = 0; }
  | IF NOT EXISTS { $$ = 1; }
;

scalar_data_type:
    int_type_i opt_int_length_i %prec LOWER_PARENS
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, $1);
      $$->int16_values_[0] = $2;
      $$->int16_values_[2] = 0;   /* 2 is the same index as float or number. */
    }
  | int_type_i opt_int_length_i ZEROFILL
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, $1 + (T_UTINYINT - T_TINYINT));
      $$->int16_values_[0] = $2;
      $$->int16_values_[2] = 1;   /* 2 is the same index as float or number. */
    }
  | int_type_i opt_int_length_i SIGNED
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, $1);
      $$->int16_values_[0] = $2;
      $$->int16_values_[2] = 0;   /* 2 is the same index as float or number. */
    }
  | int_type_i opt_int_length_i SIGNED ZEROFILL
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, $1 + (T_UTINYINT - T_TINYINT));
      $$->int16_values_[0] = $2;
      $$->int16_values_[2] = 1;   /* 2 is the same index as float or number. */
    }
  | int_type_i opt_int_length_i UNSIGNED
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, $1 + (T_UTINYINT - T_TINYINT));
      $$->int16_values_[0] = $2;
      $$->int16_values_[2] = 0;   /* 2 is the same index as float or number. */
    }
  | int_type_i opt_int_length_i UNSIGNED ZEROFILL
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, $1 + (T_UTINYINT - T_TINYINT));
      $$->int16_values_[0] = $2;
      $$->int16_values_[2] = 1;   /* 2 is the same index as float or number. */
    }
  | float_type_i opt_float_precision %prec LOWER_PARENS
    {
      if (T_FLOAT != $1 && NULL != $2 && -1 == $2->int16_values_[1]) {
        obpl_mysql_yyerror(&@2, parse_ctx, "double type not support double(M) syntax\n");
        YYERROR;
      }
      malloc_terminal_node($$, parse_ctx->mem_pool_, $1);
      if (NULL != $2) {
        $$->int16_values_[0] = $2->int16_values_[0];
        $$->int16_values_[1] = $2->int16_values_[1];
      }
      /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
      $$->int16_values_[2] = 0;
    }
  | float_type_i opt_float_precision ZEROFILL
    {
      if (T_FLOAT != $1 && NULL != $2 && -1 == $2->int16_values_[1]) {
        obpl_mysql_yyerror(&@2, parse_ctx, "double type not support double(M) syntax\n");
        YYERROR;
      }
      malloc_terminal_node($$, parse_ctx->mem_pool_, $1 + (T_UFLOAT - T_FLOAT));
      if (NULL != $2) {
        $$->int16_values_[0] = $2->int16_values_[0];
        $$->int16_values_[1] = $2->int16_values_[1];
      }
      /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
      $$->int16_values_[2] = 1;
    }
  | float_type_i opt_float_precision SIGNED
    {
      if (T_FLOAT != $1 && NULL != $2 && -1 == $2->int16_values_[1]) {
        obpl_mysql_yyerror(&@2, parse_ctx, "double type not support double(M) syntax\n");
        YYERROR;
      }
      malloc_terminal_node($$, parse_ctx->mem_pool_, $1);
      if (NULL != $2) {
        $$->int16_values_[0] = $2->int16_values_[0];
        $$->int16_values_[1] = $2->int16_values_[1];
      }
      /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
      $$->int16_values_[2] = 0;
    }
  | float_type_i opt_float_precision SIGNED ZEROFILL
    {
      if (T_FLOAT != $1 && NULL != $2 && -1 == $2->int16_values_[1]) {
        obpl_mysql_yyerror(&@2, parse_ctx, "double type not support double(M) syntax\n");
        YYERROR;
      }
      malloc_terminal_node($$, parse_ctx->mem_pool_, $1 + (T_UFLOAT - T_FLOAT));
      if (NULL != $2) {
        $$->int16_values_[0] = $2->int16_values_[0];
        $$->int16_values_[1] = $2->int16_values_[1];
      }
      /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
      $$->int16_values_[2] = 1;
    }
  | float_type_i opt_float_precision UNSIGNED
    {
      if (T_FLOAT != $1 && NULL != $2 && -1 == $2->int16_values_[1]) {
        obpl_mysql_yyerror(&@2, parse_ctx, "double type not support double(M) syntax\n");
        YYERROR;
      }
      malloc_terminal_node($$, parse_ctx->mem_pool_, $1 + (T_UFLOAT - T_FLOAT));
      if (NULL != $2) {
        $$->int16_values_[0] = $2->int16_values_[0];
        $$->int16_values_[1] = $2->int16_values_[1];
      }
      /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
      $$->int16_values_[2] = 0;
    }
  | float_type_i opt_float_precision UNSIGNED ZEROFILL
    {
      if (T_FLOAT != $1 && NULL != $2 && -1 == $2->int16_values_[1]) {
        obpl_mysql_yyerror(&@2, parse_ctx, "double type not support double(M) syntax\n");
        YYERROR;
      }
      malloc_terminal_node($$, parse_ctx->mem_pool_, $1 + (T_UFLOAT - T_FLOAT));
      if (NULL != $2) {
        $$->int16_values_[0] = $2->int16_values_[0];
        $$->int16_values_[1] = $2->int16_values_[1];
      }
      /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
      $$->int16_values_[2] = 1;
    }
  | number_type opt_number_precision %prec LOWER_PARENS
    {
      (void)($1);
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER);
      if (NULL != $2) {
        $$->int16_values_[0] = $2->int16_values_[0];
        $$->int16_values_[1] = $2->int16_values_[1];
      }
      /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
      $$->int16_values_[2] = 0;
    }
  | number_type opt_number_precision ZEROFILL
    {
      (void)($1);
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_UNUMBER);
      if (NULL != $2) {
        $$->int16_values_[0] = $2->int16_values_[0];
        $$->int16_values_[1] = $2->int16_values_[1];
      }
      /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
      $$->int16_values_[2] = 1;
    }
  | number_type opt_number_precision SIGNED
    {
      (void)($1);
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER);
      if (NULL != $2) {
        $$->int16_values_[0] = $2->int16_values_[0];
        $$->int16_values_[1] = $2->int16_values_[1];
      }
      /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
      $$->int16_values_[2] = 0;
    }
  | number_type opt_number_precision SIGNED ZEROFILL
    {
      (void)($1);
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_UNUMBER);
      if (NULL != $2) {
        $$->int16_values_[0] = $2->int16_values_[0];
        $$->int16_values_[1] = $2->int16_values_[1];
      }
      /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
      $$->int16_values_[2] = 1;
    }
  | number_type opt_number_precision UNSIGNED
    {
      (void)($1);
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_UNUMBER);
      if (NULL != $2) {
        $$->int16_values_[0] = $2->int16_values_[0];
        $$->int16_values_[1] = $2->int16_values_[1];
      }
      /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
      $$->int16_values_[2] = 0;
    }
  | number_type opt_number_precision UNSIGNED ZEROFILL
    {
      (void)($1);
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_UNUMBER);
      if (NULL != $2) {
        $$->int16_values_[0] = $2->int16_values_[0];
        $$->int16_values_[1] = $2->int16_values_[1];
      }
      /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
      $$->int16_values_[2] = 1;
    }
  | datetime_type_i opt_datetime_fsp_i
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, $1);
      $$->int16_values_[1] = $2;
    }
  | date_year_type_i
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, $1);
    }
  | CHARACTER opt_string_length_i opt_binary opt_charset opt_collation
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_CHAR, 3, $4, $5, $3);
      if ($2 < 0) {
        $2 = 1;
      }
      $$->int32_values_[0] = $2;
      $$->int32_values_[1] = 0; /* is char */
    }
  | nchar_type_i opt_string_length_i opt_binary opt_collation
    {
      ParseNode *charset_node = NULL;
      malloc_terminal_node(charset_node, parse_ctx->mem_pool_, T_CHARSET);
      charset_node->str_value_ = parse_strdup("utf8mb4", parse_ctx->mem_pool_, &(charset_node->str_len_));
      if (OB_UNLIKELY(NULL == charset_node->str_value_)) {
        obpl_mysql_yyerror(NULL, parse_ctx, "memory space for string is not enough\n");
        YYERROR;
      }

      malloc_non_terminal_node($$, parse_ctx->mem_pool_, $1, 3, charset_node, $4, $3);
      $$->int32_values_[0] = $2;
      $$->int32_values_[1] = 0; /* is char */
    }
  /*  | TEXT opt_binary opt_charset opt_collation
  //  {
  //    UNUSED($2);
  //    malloc_non_terminal_node($$, result->malloc_pool_, T_VARCHAR, 3, $3, $4, $2);
  //    $$->int32_values_[0] = 256;
  //    $$->int32_values_[1] = 0; /* is char */
  /*  }*/
  | CHARACTER VARYING string_length_i opt_binary opt_charset opt_collation
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_VARCHAR, 3, $5, $6, $4);
      $$->int32_values_[0] = $3;
      $$->int32_values_[1] = 0; /* is char */
    }
  | VARCHAR string_length_i opt_binary opt_charset opt_collation
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_VARCHAR, 3, $4, $5, $3);
      $$->int32_values_[0] = $2;
      $$->int32_values_[1] = 0; /* is char */
    }
  | nvarchar_type_i opt_string_length_i opt_binary opt_collation
    {
      ParseNode *charset_node = NULL;
      malloc_terminal_node(charset_node, parse_ctx->mem_pool_, T_CHARSET);
      charset_node->str_value_ = parse_strdup("utf8mb4", parse_ctx->mem_pool_, &(charset_node->str_len_));
      if (OB_UNLIKELY(NULL == charset_node->str_value_)) {
        obpl_mysql_yyerror(NULL, parse_ctx, "memory space for string is not enough\n");
        YYERROR;
      }

      malloc_non_terminal_node($$, parse_ctx->mem_pool_, $1, 3, charset_node, $4, $3);
      $$->int32_values_[0] = $2;
      $$->int32_values_[1] = 0; /* is char */
    }
  | BINARY opt_string_length_i
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_CHAR);
      if ($2 < 0) {
        $2 = 1;
      }
      $$->int32_values_[0] = $2;
      $$->int32_values_[1] = 1; /* is binary */
    }
  | VARBINARY string_length_i
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_VARCHAR);
      $$->int32_values_[0] = $2;
      $$->int32_values_[1] = 1; /* is binary */
    }
  | STRING /* wrong or unsupported data type */
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_INVALID);
      $$->str_value_ = $1->str_value_;
      $$->str_len_ = $1->str_len_;
    }
  | BIT opt_bit_length_i
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_BIT);
      $$->int16_values_[0] = $2;
    }
  | BOOL
	{
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_TINYINT);
  	  $$->int16_values_[0] = 1;
      $$->int16_values_[2] = 0;  // zerofill always false
    }
  | BOOLEAN
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_TINYINT);
      $$->int16_values_[0] = 1;
      $$->int16_values_[2] = 0; // zerofill always false
    }
  | blob_type_i opt_string_length_i_v2
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, $1);
      if ($1 != T_TEXT && $2 != 0) {
        obpl_mysql_yyerror(&@2, parse_ctx, "not support to specify the length in parentheses\n");
        YYERROR;
      }
      $$->int32_values_[0] = $2;
      $$->int32_values_[1] = 1; /* is binary */
    }
  | text_type_i opt_string_length_i_v2 opt_binary opt_charset opt_collation
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, $1, 3, $4, $5, $3);
      if ($1 != T_TEXT && $2 != 0) {
        obpl_mysql_yyerror(&@2, parse_ctx, "not support to specify the length in parentheses\n");
        YYERROR;
      }
      $$->int32_values_[0] = $2;
      $$->int32_values_[1] = 0; /* is text */
    }
  | ENUM '(' string_list ')' opt_binary opt_charset opt_collation
  {
    ParseNode *string_list_node = NULL;
    merge_nodes(string_list_node, parse_ctx->mem_pool_, T_STRING_LIST, $3);
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_ENUM, 4, $6, $7, $5, string_list_node);
    $$->int32_values_[0] = 0;//not used so far
    $$->int32_values_[1] = 0; /* is char */
  }
  | SET '(' string_list ')' opt_binary opt_charset opt_collation
  {
    ParseNode *string_list_node = NULL;
    merge_nodes(string_list_node, parse_ctx->mem_pool_, T_STRING_LIST, $3);
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SET, 4, $6, $7, $5, string_list_node);
    $$->int32_values_[0] = 0;//not used so far
    $$->int32_values_[1] = 0; /* is char */
  }
  | JSON
	{
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_JSON);
    $$->int32_values_[0] = 0;
  }
  | pl_obj_access_ref '%' ROWTYPE
  {
    if (parse_ctx->is_for_trigger_ && parse_ctx->is_inner_parse_) {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ROWTYPE, 2, $1, NULL);
    } else {
      obpl_mysql_yyerror(&@3, parse_ctx, "Syntax Error\n");
      YYERROR;
    }
  }
;

pl_obj_access_ref:
    sp_call_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ACCESS_REF, 2, $1, NULL);
    }

int_type_i:
    TINYINT     { $$ = T_TINYINT; }
  | SMALLINT    { $$ = T_SMALLINT; }
  | MEDIUMINT   { $$ = T_MEDIUMINT; }
  | INTEGER     { $$ = T_INT32; }
  | BIGINT      { $$ = T_INT; }
;

float_type_i:
    FLOAT              { $$ = T_FLOAT; }
  | DOUBLE             { $$ = T_DOUBLE; }
  | DOUBLE PRECISION   { $$ = T_DOUBLE; }
;

datetime_type_i:
    DATETIME    { $$ = T_DATETIME; }
  | TIMESTAMP   { $$ = T_TIMESTAMP; }
  | TIME        { $$ = T_TIME; }
;

number_type:
DEC { $$ = NULL; }
| DECIMAL { $$ = NULL; }
| NUMERIC { $$ = NULL; }
| FIXED { $$ = NULL; }

date_year_type_i:
    DATE        { $$ = T_DATE; }
  | YEAR opt_year_i { $$ = T_YEAR; }
;

nchar_type_i:
    NCHAR               { $$ = T_CHAR; }
  | NATIONAL CHARACTER  { $$ = T_CHAR; }
;

nvarchar_type_i:
    NVARCHAR                    { $$ = T_VARCHAR; }
  | NCHAR VARCHAR               { $$ = T_VARCHAR; }
  | NATIONAL VARCHAR            { $$ = T_VARCHAR; }
  | NATIONAL CHARACTER VARYING  { $$ = T_VARCHAR; }
;

opt_int_length_i:
    '(' INTNUM ')'  { $$ = $2->value_; }
  | /*EMPTY*/       { $$ = -1; }
;

opt_bit_length_i:
    '(' INTNUM ')'  { $$ = $2->value_; }
  | /*EMPTY*/       { $$ = 1; }
;

opt_float_precision:
    '(' INTNUM ',' INTNUM ')'
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE);
      $$->int16_values_[0] = $2->value_;
      $$->int16_values_[1] = $4->value_;
    }
  | '(' INTNUM ')'
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE);
      $$->int16_values_[0] = $2->value_;
      $$->int16_values_[1] = -1;
    }
  | '(' DECIMAL_VAL ')'
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE);
    int err_no = 0;
    $2->value_ = ob_strntoll($2->str_value_, $2->str_len_, 10, NULL, &err_no);
    $$->int16_values_[0] = $2->value_;
    $$->int16_values_[1] = -1;
  }
  | /*EMPTY*/ { $$ = NULL; }
;

opt_number_precision:
    '(' INTNUM ',' INTNUM ')'
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE);
      if($2->value_ > OB_MAX_PARSER_INT16_VALUE) {
        $$->int16_values_[0] = OB_MAX_PARSER_INT16_VALUE;
      } else {
        $$->int16_values_[0] = $2->value_;
      }
      if($4->value_ > OB_MAX_PARSER_INT16_VALUE) {
        $$->int16_values_[1] = OB_MAX_PARSER_INT16_VALUE;
      } else {
        $$->int16_values_[1] = $4->value_;
      }
      $$->param_num_ = 2;
    }
  | '(' INTNUM ')'
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE);
      if($2->value_ > OB_MAX_PARSER_INT16_VALUE) {
        $$->int16_values_[0] = OB_MAX_PARSER_INT16_VALUE;
      } else {
        $$->int16_values_[0] = $2->value_;
      }
      $$->int16_values_[1] = 0;
      $$->param_num_ = 1;
    }
  | /*EMPTY*/
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE);
      $$->int16_values_[0] = 10;
      $$->int16_values_[1] = 0;
      $$->param_num_ = 0;
    }
;

opt_year_i:
    '(' INTNUM  ')' { $$ = $2->value_; }
  | /*EMPTY*/ { $$ = 0; }
;

opt_datetime_fsp_i:
    '(' INTNUM ')'
    {
      $$ = $2->value_;
    }
  | /*EMPTY*/
    {
      $$ = 0;
    }
;

number_literal:
    INTNUM
    {
      $$ = $1;
      $$->param_num_ = 1;
    }
  | DECIMAL_VAL
    {
      $$ = $1;
      $$->param_num_ = 1;
    }
;

literal:
    number_literal { $$ = $1; }
  | DATE_VALUE { $$ = $1; }
  | HEX_STRING_VALUE { $$ = $1; }
  | NULLX { $$ = $1; }
;

text_type_i:
TINYTEXT     { $$ = T_TINYTEXT; }
| TEXT       { $$ = T_TEXT; }
| MEDIUMTEXT { $$ = T_MEDIUMTEXT; }
| LONGTEXT   { $$ = T_LONGTEXT;  }
| LONG VARCHAR { $$ = T_MEDIUMTEXT; }
| LONG { $$ = T_MEDIUMTEXT; }
;

blob_type_i:
TINYBLOB     { $$ = T_TINYTEXT; }
| BLOB       { $$ = T_TEXT; }
| MEDIUMBLOB { $$ = T_MEDIUMTEXT; }
| LONGBLOB   { $$ = T_LONGTEXT;  }
| LONG VARBINARY {$$ = T_MEDIUMTEXT; }
;

opt_string_length_i_v2:
string_length_i { $$ = $1; }
| /*EMPTY*/     { $$ = 0; }

string_length_i:
'(' number_literal ')'
{
  // 在 `*` 处报语法错误
  // select cast('' as BINARY(-1));
  //                          *
  // select cast('' as CHARACTER(-1));
  //                             *
  int64_t val = 0;
  if (T_NUMBER == $2->type_) {
    errno = 0;
    val = strtoll($2->str_value_, NULL, 10);
    if (ERANGE == errno) {
      $$ = OUT_OF_STR_LEN;// out of str_max_len
    } else if (val < 0) {
      obpl_mysql_yyerror(&@2, parse_ctx, "length cannot < 0\n");
      YYERROR;
    } else if (val > UINT32_MAX) {
      $$ = OUT_OF_STR_LEN;// out of str_max_len
    } else if (val > INT32_MAX) {
      $$ = DEFAULT_STR_LENGTH;
    } else {
      $$ = val;
    }
  } else if ($2->value_ < 0) {
    obpl_mysql_yyerror(&@2, parse_ctx, "length cannot < 0\n");
    YYERROR;
  } else if ($2->value_ > UINT32_MAX) {
    $$ = OUT_OF_STR_LEN;;
  } else if ($2->value_ > INT32_MAX) {
    $$ = DEFAULT_STR_LENGTH;
  } else {
    $$ = $2->value_;
  }
  // $$ = $2->param_num_;
}
;

string_list:
  text_string
  {
   $$ = $1;
  }
  | string_list ',' text_string
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
  }
;

text_string:
  STRING
  {
    $$ = $1;
  }
  | HEX_STRING_VALUE
  {
    $$ = $1;
  }
;

opt_string_length_i:
    string_length_i
    {
      $$ = $1;
    }
  | /*EMPTY*/
    {
      $$ = 1;
    }
;

opt_binary:
    BINARY
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_BINARY);
      $$->value_ = 1;
    }
  | /*EMPTY*/ {$$ = 0; }
;

collation_name:
    ident
    {
      $$ = $1;
      $$->type_ = T_VARCHAR;
      $$->param_num_ = 0;
    }
  | STRING
    {
      $$ = $1;
      $$->param_num_ = 1;
    }
;

charset_name:
    ident
    {
      $$ = $1;
      $$->type_ = T_VARCHAR;
      $$->param_num_ = 0;
      $$->is_hidden_const_ = 1;
    }
  | STRING
    {
      $$ = $1;
      $$->param_num_ = 1;
      $$->is_hidden_const_ = 0;
    }
  | BINARY
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_VARCHAR);
      $$->str_value_ = parse_strdup("binary", parse_ctx->mem_pool_, &($$->str_len_));
      if (OB_UNLIKELY(NULL == $$->str_value_)) {
        YY_FATAL_ERROR("no more memory to malloc 'binary; string\n");
      }
      $$->param_num_ = 0;
      $$->is_hidden_const_ = 1;
    }
;

opt_charset:
    charset_key charset_name
    {
      (void)($1);
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_CHARSET);
      $$->str_value_ = $2->str_value_;
      $$->str_len_ = $2->str_len_;
    }
  | /*EMPTY*/ { $$ = NULL; }
;

charset_key:
    CHARSET
    {
      $$ = NULL;
    }
  | CHARACTER SET
    {
      $$ = NULL;
    }
;

collation:
    COLLATE collation_name
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_COLLATION);
      $$->str_value_ = $2->str_value_;
      $$->str_len_ = $2->str_len_;
      $$->param_num_ = $2->param_num_;
    }
;

opt_collation:
    collation
    {
      $$ = $1;
    }
  | /*EMPTY*/ { $$ = NULL; }
;

signal_stmt:
    SIGNAL signal_value opt_set_signal_information
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_SIGNAL, 2, $2, $3);
    }
;

resignal_stmt:
    RESIGNAL opt_signal_value opt_set_signal_information
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_RESIGNAL, 2, $2, $3);
    }
;

opt_signal_value:
    /*Empty*/ { $$ = NULL; }
  | signal_value { $$ = $1; }
;

signal_value:
    ident { $$ = $1; }
  | sqlstate { $$ = $1; }
;

opt_set_signal_information:
    /*Empty*/ { $$ = NULL; }
  | SET signal_information_item_list
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_SP_SIGNAL_INFO_LIST, $2);
    }
;

signal_information_item_list:
    signal_information_item { $$ = $1; }
  | signal_information_item_list ',' signal_information_item
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
;

signal_information_item:
    scond_info_item_name '=' signal_allowed_expr
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_SIGNAL_INFO_ITEM, 1, $3);
      $$->value_ = $1;
    }
;

signal_allowed_expr:
    literal { $$ = $1; }
  | variable { $$ = $1; }
  | simple_ident { $$ = $1; }
  | STRING { $$ = $1; }
;

variable:
    SYSTEM_VARIABLE { $$ = $1; }
  | USER_VARIABLE { $$ = $1; }
;

scond_info_item_name:
    CLASS_ORIGIN { $$ = DIAG_CLASS_ORIGIN; }
  | SUBCLASS_ORIGIN { $$ = DIAG_SUBCLASS_ORIGIN; }
  | CONSTRAINT_CATALOG { $$ = DIAG_CONSTRAINT_CATALOG; }
  | CONSTRAINT_SCHEMA { $$ = DIAG_CONSTRAINT_SCHEMA; }
  | CONSTRAINT_NAME { $$ = DIAG_CONSTRAINT_NAME; }
  | CATALOG_NAME { $$ = DIAG_CATALOG_NAME; }
  | SCHEMA_NAME { $$ = DIAG_SCHEMA_NAME; }
  | TABLE_NAME { $$ = DIAG_TABLE_NAME; }
  | COLUMN_NAME { $$ = DIAG_COLUMN_NAME; }
  | CURSOR_NAME { $$ = DIAG_CURSOR_NAME; }
  | MESSAGE_TEXT { $$ = DIAG_MESSAGE_TEXT; }
  | MYSQL_ERRNO { $$ = DIAG_MYSQL_ERRNO; }
;

%%
/**
 * parser function
 * @param [out] pl_yychar, set the pl parser look-ahead token
 */
ParseNode *obpl_mysql_read_sql_construct(ObParseCtx *parse_ctx, const char *prefix, YYLookaheadToken *la_token, int end_token_cnt, ...)
{
  int errcode = OB_PARSER_SUCCESS;
  va_list va;
  bool is_break = false;
  int sql_str_len = -1;
  int parenlevel = (*(la_token->la_yychar) == '(' || *(la_token->la_yychar) == '[') ? 1 : 0;
  const char *sql_str = NULL;
  ParseResult parse_result;
  ParseNode *sql_node = NULL;
  if (*(la_token->la_yychar) != -2) { //#define YYEMPTY    (-2)
    parse_ctx->scanner_ctx_.sql_end_loc = la_token->la_yylloc->last_column;
    if (parse_ctx->scanner_ctx_.sql_start_loc < 0) {
      parse_ctx->scanner_ctx_.sql_start_loc = la_token->la_yylloc->first_column;
    }
  }
  while (!is_break) {
    *(la_token->la_yychar) = obpl_mysql_yylex(la_token->la_yylval, la_token->la_yylloc, parse_ctx->scanner_ctx_.yyscan_info_);
    if (parse_ctx->scanner_ctx_.sql_start_loc < 0) {
      //get sql rule start location
      parse_ctx->scanner_ctx_.sql_start_loc = la_token->la_yylloc->first_column;
    }
    va_start(va, end_token_cnt);
    int i = 0;
    for (; !is_break && i < end_token_cnt; ++i) {
      int end_token = va_arg(va, int);
      if (end_token == *(la_token->la_yychar) && parenlevel <= 0) { //正常应该用==0，但是可能外面已经读出了左括号，所以这里用<=
        is_break = true;
        if (END_P == end_token) {
          parse_ctx->scanner_ctx_.sql_end_loc = la_token->la_yylloc->last_column;
        } else {
          parse_ctx->scanner_ctx_.sql_end_loc = la_token->la_yylloc->first_column - 1;
        }
      }
    }
    va_end(va);
    if (*(la_token->la_yychar) == '(' || *(la_token->la_yychar) == '[') {
      ++parenlevel;
    } else if (*(la_token->la_yychar) == ')' || *(la_token->la_yychar) == ']') {
      --parenlevel;
    }
    if (END_P == *(la_token->la_yychar)) {
      is_break = true;
      parse_ctx->scanner_ctx_.sql_end_loc = la_token->la_yylloc->last_column;
    }
    if (!is_break) {
      //get sql rule end location
      parse_ctx->scanner_ctx_.sql_end_loc = la_token->la_yylloc->last_column;
    }
  }
  if (OB_PARSER_SUCCESS == errcode) {
    parse_ctx->scanner_ctx_.sql_end_loc = parse_ctx->stmt_len_ <= parse_ctx->scanner_ctx_.sql_end_loc ?
                                            parse_ctx->stmt_len_ - 1 : parse_ctx->scanner_ctx_.sql_end_loc;
    sql_str_len = parse_ctx->scanner_ctx_.sql_end_loc - parse_ctx->scanner_ctx_.sql_start_loc + 1;
  }
  if (OB_PARSER_SUCCESS == errcode && sql_str_len > 0) {
    sql_str = strndup_with_prefix(prefix, parse_ctx->stmt_str_ + parse_ctx->scanner_ctx_.sql_start_loc,
                                  sql_str_len, parse_ctx->mem_pool_);
    if (NULL == sql_str) {
      YY_FATAL_ERROR("no memory to strdup sql string\n");
    }
    //parse sql_str...
    memset(&parse_result, 0, sizeof(ParseResult));
    parse_result.input_sql_ = sql_str;
    parse_result.input_sql_len_ = sql_str_len + strlen(prefix);
    parse_result.malloc_pool_ = parse_ctx->mem_pool_;
    parse_result.pl_parse_info_.is_pl_parse_ = true;
    parse_result.pl_parse_info_.is_pl_parse_expr_ = false;
    parse_result.is_for_trigger_ = (1 == parse_ctx->is_for_trigger_);
    //将pl_parser的question_mark_size赋值给sql_parser，使得parser sql的question mark能够接着pl_parser的index
    parse_result.question_mark_ctx_ = parse_ctx->question_mark_ctx_;
    parse_result.charset_info_ = parse_ctx->charset_info_;
    parse_result.charset_info_oracle_db_ = parse_ctx->charset_info_oracle_db_;
    parse_result.is_not_utf8_connection_ = parse_ctx->is_not_utf8_connection_;
    parse_result.connection_collation_ = parse_ctx->connection_collation_;
    parse_result.sql_mode_ = parse_ctx->scanner_ctx_.sql_mode_;
    parse_result.semicolon_start_col_ = INT32_MAX;
  }
  if (sql_str_len <= 0) {
    //do nothing
  } else if (0 != parse_sql_stmt(&parse_result)) {
    if (parse_result.extra_errno_ != OB_PARSER_SUCCESS) {
      obpl_mysql_parse_fatal_error(parse_result.extra_errno_, YYLEX_PARAM, parse_result.error_msg_);
    } else {
      YYLTYPE sql_yylloc;
      memset(&sql_yylloc, 0, sizeof(YYLTYPE));
      sql_yylloc.first_column = parse_ctx->scanner_ctx_.sql_start_loc;
      sql_yylloc.last_column = parse_ctx->scanner_ctx_.sql_end_loc;
      sql_yylloc.first_line = la_token->la_yylloc->first_line;
      sql_yylloc.last_line = la_token->la_yylloc->last_line;
      obpl_mysql_yyerror(&sql_yylloc, parse_ctx, "Syntax Error\n");
    }
  } else {
    sql_node = parse_result.result_tree_->children_[0];
    //通过sql_parser的question_mark_size来更新pl_parser
    parse_ctx->question_mark_ctx_ = parse_result.question_mark_ctx_;
  }
  return sql_node;
}

void obpl_mysql_yyerror(YYLTYPE *yylloc, ObParseCtx *parse_ctx, char *s)
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

