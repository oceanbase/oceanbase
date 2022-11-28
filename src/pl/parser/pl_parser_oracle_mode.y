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
 * pl_parser.y is for …
 *
 * Authors:
 */

//first: declare
%define api.pure
%parse-param {ObParseCtx *parse_ctx}
%name-prefix "obpl_oracle_yy"
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
  int32_t ival[3]; //ival[0]表示value, ival[1]表示fast parse在对应的该node及其子node可识别的常量个数, ival[2] for length_semantics
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
#include "pl/parser/pl_parser_oracle_mode_lex.h"
#include "pl_parser_base.h"

typedef struct _YYLookaheadToken
{
  int *la_yychar;
  /* The semantic value of the lookahead symbol.  */
  YYSTYPE *la_yylval;
  /* Location data for the lookahead symbol.  */
  YYLTYPE *la_yylloc;
} YYLookaheadToken;

extern ParseNode *obpl_oracle_read_sql_construct(ObParseCtx *parse_ctx,
                                                 bool for_expr,
                                                 const char *prefix,
                                                 const char* postfix,
                                                 YYLookaheadToken *la_token,
                                                 int end_token_cnt, ...);
extern void obpl_oracle_yyerror(YYLTYPE *yylloc, ObParseCtx *parse_ctx, char *s, ...);
extern void obpl_oracle_parse_fatal_error(int32_t errcode, yyscan_t yyscanner, yyconst char *msg, ...);

#define YY_FATAL_ERROR(msg, args...) (obpl_oracle_parse_fatal_error(OB_PARSER_ERR_NO_MEMORY, YYLEX_PARAM, msg, ##args))
#define YY_UNEXPECTED_ERROR(msg, args...) (obpl_oracle_parse_fatal_error(OB_PARSER_ERR_UNEXPECTED, YYLEX_PARAM, msg, ##args))
#define YY_MUST_RETURN_SELF(msg, args...) (obpl_oracle_parse_fatal_error(OB_PARSER_ERR_MUST_RETURN_SELF, YYLEX_PARAM, msg, ##args))
#define YY_ONLY_FUNC_CAN_PIPELINED(msg, args...) (obpl_oracle_parse_fatal_error(OB_PARSER_ERR_ONLY_FUNC_CAN_PIPELINED, YYLEX_PARAM, msg, ##args))
#define YY_NO_ATTRIBUTE_FOUND(msg, args...) (obpl_oracle_parse_fatal_error(OB_PARSER_ERR_NO_ATTR_FOUND, YYLEX_PARAM, msg, ##args))
#define YY_NON_INT_LITERAL(msg, args...) (obpl_oracle_parse_fatal_error(OB_PARSER_ERR_NON_INT_LITERAL, YYLEX_PARAM, msg, ##args))
#define YY_ERR_NUMERIC_OR_VALUE_ERROR(msg, args...) (obpl_oracle_parse_fatal_error(OB_PARSER_ERR_NUMERIC_OR_VALUE_ERROR, YYLEX_PARAM, msg, ##args))
#define YY_ERR_NON_INTEGRAL_NUMERIC_LITERAL(msg, args...) (obpl_oracle_parse_fatal_error(OB_PARSER_ERR_NON_INTEGRAL_NUMERIC_LITERAL, YYLEX_PARAM, msg, ##args))

#define do_parse_sql_stmt(node, _parse_ctx, start_loc, end_loc, end_tok_num, ...)                          \
  do {                                                                                                     \
    YYLookaheadToken la_token;                                                                             \
    la_token.la_yychar = &yychar;                                                                          \
    la_token.la_yylval = &yylval;                                                                          \
    la_token.la_yylloc = &yylloc;                                                                          \
    _parse_ctx->scanner_ctx_.sql_start_loc = start_loc;                                                    \
    _parse_ctx->scanner_ctx_.sql_end_loc = end_loc;                                                        \
    node = obpl_oracle_read_sql_construct(_parse_ctx, false, "", "", &la_token, end_tok_num, ##__VA_ARGS__);      \
    if (NULL == node) {                                                                                    \
      YYERROR;                                                                                             \
    }                                                                                                      \
    reset_current_location(_parse_ctx->scanner_ctx_.sql_start_loc, _parse_ctx->scanner_ctx_.sql_end_loc);  \
  } while (0)

#define do_parse_sql_bool_expr_rule_version_2(node, _parse_ctx, end_tok_num, ...)                          \
  do {                                                                                                     \
    ParseNode *expr_node = NULL;                                                                           \
    YYLookaheadToken la_token;                                                                             \
    la_token.la_yychar = &yychar;                                                                          \
    la_token.la_yylval = &yylval;                                                                          \
    la_token.la_yylloc = &yylloc;                                                                          \
    _parse_ctx->scanner_ctx_.sql_start_loc = -1;                                                           \
    _parse_ctx->scanner_ctx_.sql_end_loc = -1;                                                             \
    _parse_ctx->cur_error_info_ = NULL;                                                                    \
    expr_node = obpl_oracle_read_sql_construct(_parse_ctx, true, "DO ", "", &la_token, end_tok_num, ##__VA_ARGS__);\
    if (NULL == expr_node && _parse_ctx->cur_error_info_ != NULL) {                                        \
      YYERROR;                                                                                             \
    }                                                                                                      \
    reset_current_location(_parse_ctx->scanner_ctx_.sql_start_loc, _parse_ctx->scanner_ctx_.sql_end_loc);  \
    if (expr_node != NULL && T_DEFAULT == expr_node->type_                                                 \
        && 1 == expr_node->num_child_ && expr_node->children_[0] != NULL) {                                \
      node = expr_node->children_[0];                                                                      \
    } else {                                                                                               \
      node = NULL;                                                                                         \
    }                                                                                                      \
} while (0)

%}

/*these tokens can't be used for obj names*/
%token END_P
%token SQL_KEYWORD LABEL_LEFT LABEL_RIGHT ASSIGN_OPERATOR RANGE_OPERATOR PARAM_ASSIGN_OPERATOR
%token <node> IDENT STRING INTNUM DECIMAL_VAL DATE_VALUE QUESTIONMARK
/* reserved key words */
/*
 * Oracle PL/SQL Reserved Words（93个)
 * https://docs.oracle.com/cd/E18283_01/appdev.112/e17126/reservewords.htm
 * */
 %token
      ALL ALTER AND ANY AS ASC AT
      BEGIN_KEY BETWEEN BY
      CASE CHECK CLUSTERS CLUSTER COLAUTH COLUMNS COMPRESS CONNECT CRASH CREATE CURSOR COMMIT
      DELETE DECLARE DEFAULT DESC DISTINCT DROP
      EXISTS ELSE ELSIF END_KEY EXCEPTION EXCLUSIVE
      FETCH FOR FROM FUNCTION
      GOTO GRANT GROUP
      HAVING
      IDENTIFIED IF IN INDEX INDEXES INSERT INTERSECT INTO IS
      LIKE LOCK
      MINUS MODE
      NOCOMPRESS NOT NOWAIT NULLX
      OF ON OPTION OR ORDER OVERLAPS
      PROCEDURE PUBLIC
      RESOURCE REVOKE RETURN ROLLBACK
      SELECT SHARE SIZE SQL START SUBTYPE SAVEPOINT SET
      TABAUTH TABLE THEN TO
      UNION UNIQUE UPDATE
      VALUES VIEW VIEWS
      WHEN WHERE WITH WHILE

/* non reserved key words */
%token <non_reserved_keyword>
  ACCESSIBLE AUTHID AFTER AGGREGATE AUTONOMOUS_TRANSACTION ARRAY
  BULK BYTE BINARY BOOL BLOB BINARY_DOUBLE BINARY_FLOAT BEFORE BODY
  C CALL CHARSET COLLATE COLLATION COLLECT COMPILE CURRENT_USER CUSTOMDATUM CHARACTER CLOB CONSTRUCTOR
  CONTINUE COMPOUND CLOSE CONSTANT
  DEFINER DETERMINISTIC DAY DEBUG DISABLE DATE
  EDITIONABLE EXECUTE EACH ENABLE ERROR EXCEPTIONS EXTERNAL EXCEPTION_INIT EXIT
  FINAL FORCE FORALL FLOAT
  HASH HOUR
  IMMEDIATE INDICES INSTEAD INTERFACE JAVA INTERVAL INSTANTIABLE
  LIMIT LOCAL LONG LANGUAGE LEVEL LOOP
  INLINE
  MONTH MINUTE MAP MEMBER MERGE
  NAME NESTED NO NONEDITIONABLE NATURALN NUMERIC NVARCHAR2 NCHAR NOCOPY NEW NATURAL NUMBER
  OBJECT OID OPAQUE ORADATA OVERRIDING OLD OPEN OTHERS OUT
  PARALLEL_ENABLE PIPE PIPELINED PLS_INTEGER BINARY_INTEGER POSITIVEN PARENT POSITIVE PRAGMA PACKAGE_P PARTITION
  REF RELIES_ON REPLACE RESTRICT_REFERENCES RESULT RESULT_CACHE RETURNING RNDS RNPS RAISE RECORD
  RAW REUSE REFERENCING ROW REVERSE ROWTYPE RANGE REAL ROWID
  SAVE SERIALLY_REUSABLE SETTINGS SPECIFICATION SQLDATA SECOND SELF SIGNTYPE SIMPLE_INTEGER STATIC
  SIMPLE_DOUBLE SIMPLE_FLOAT STATEMENT
  TIME TRIGGER TRUST TIMESTAMP TYPE
  UDF UNDER USING USING_NLS_COMP UROWID
  VALIDATE VALUE VARIABLE VARRAY VARYING VARCHAR VARCHAR2
  WNDS WNPS
  YEAR YES
  ZONE

/* precedence: lowest to highest */

%right END_KEY
%left ELSE IF
%left ';'
%left '.'
%left '[' ']'
%nonassoc LOWER_PARENS RAW TIMESTAMP CHARACTER FINAL INSTANTIABLE OVERRIDING UROWID NVARCHAR2
          NUMERIC
%left '(' ')'
%nonassoc AS NOCOPY MEMBER CONSTRUCTOR MAP STATIC FORALL RESTRICT_REFERENCES AUTHID FORCE OID UNDER
          AUTONOMOUS_TRANSACTION INLINE INTERFACE SERIALLY_REUSABLE UDF ACCESSIBLE EXCEPTION_INIT
          /*for solve conflict*/
%nonassoc AFTER BEFORE INSTEAD
%nonassoc DECLARATION

%type <node> sql_keyword identifier common_identifier sql_keyword_identifier
%type <non_reserved_keyword> unreserved_keyword unreserved_special_keyword
%type <node> exception_decl pl_entry pl_entry_stmt_list
%type <node> create_procedure_stmt plsql_procedure_source
%type <node> create_function_stmt plsql_function_source
%type <node> create_trigger_stmt drop_trigger_stmt plsql_trigger_source alter_trigger_stmt
%type <node> drop_procedure_stmt drop_function_stmt
%type <node> create_package_stmt create_package_body_stmt drop_package_stmt alter_package_stmt
%type <node> trigger_definition simple_dml_trigger compound_dml_trigger instead_of_dml_trigger
%type <ival> before_or_after opt_for_each_row opt_enable_or_disable opt_default_collation timing_point enable_or_disable
%type <node> dml_event_option opt_referencing_list referencing_list referencing_node ref_name opt_when_condition simple_trigger_body compound_trigger_body opt_as
%type <node> dml_event_list dml_event_tree dml_event update_column_list timing_point_section_list timing_point_section tps_body opt_column_list nested_table_column
%type <node> alter_package_clause opt_execute_section opt_tail_name null_stmt pipe_row_stmt
%type <node> func_decl func_def proc_decl proc_def
%type <node> package_block package_body_block is_or_as
%type <ival> opt_replace opt_editionable opt_debug opt_reuse_settings
%type <ival> opt_compile_unit unit_kind
%type <node> invoke_right accessor accessor_list accessible_by proc_clause proc_clause_list opt_proc_clause
%type <node> execute_section opt_exception_section exception_section pl_lang_stmt_list
%type <node> pl_inner_scalar_data_type pl_inner_data_type
%type <node> pl_outer_scalar_data_type pl_outer_data_type
%type <node> default default_expr bool_expr return_expr
%type <node> expr pl_left_value_list pl_obj_access_ref pl_obj_access_ref_suffix_list pl_obj_access_ref_suffix pl_left_value pl_right_value
%type <node> label_name opt_decl_stmt_ext_list pl_lang_stmt_without_semicolon
%type <node> assign_stmt pl_block pl_body opt_declare_section declare_section inner_call_stmt decl_stmt_ext_without_semicolon
%type <node> if_stmt sp_if opt_sp_elseifs case_stmt sp_when_list sp_when
%type <node> return_stmt decl_stmt_without_semicolon
%type <node> into_clause bulk_collect_into_clause
%type <node> using_list using_params using_param
%type <node> execute_immediate_stmt normal_into_clause opt_normal_into_clause
%type <node> opt_using_clause opt_dynamic_returning_clause
%type <node> pl_schema_name opt_sp_param_list sp_param_list sp_param sql_stmt cursor_sql_stmt
%type <node> opt_sp_cparams opt_cexpr sp_cparam opt_sp_cparam_with_assign opt_default
%type <node> sp_cparam_list opt_sp_cparam_list
%type <ival> opt_sp_inout opt_if_exists
%type <node> basic_loop_stmt while_loop_stmt for_loop_stmt for_expr
%type <node> cursor_for_loop_stmt cursor_for_loop_sql
%type <node> forall_stmt bound_clause forall_sql_stmt opt_between_bound
%type <ival> opt_save_exception
%type <node> continue_stmt pl_access_name
%type <node> exit_stmt exception_handler exception_item
%type <node> assoc_array_type_def record_member_list record_member type_def opt_record_member_default
%type <node> basetype_of_subtype opt_subtype_constraint
%type <node> subtype_range subtype_def
%type <node> record_type_def type_name collection_type_def
%type <node> coll_type_def nested_table_type_def index_type
%type <node> open_stmt for_sql fetch_stmt close_stmt opt_limit pl_impl_body pl_lang_stmt
%type <ival> opt_not_null
%type <node> pragma_stmt decl_stmt decl_stmt_ext call_spec cursor_def ref_cursor_type_def
%type <node> item_decl cursor_decl anonymous_stmt return_type opt_return_type
%type <node> cursor_name func_name proc_name var_common_name field_name
%type <node> param_name opt_label inline_pragma opt_decl_stmt_list
%type <node> udf_pragma serially_reusable_pragma assert_list restrict_references_pragma autonomous_transaction_pragma interface_pragma
%type <node> decl_stmt_list decl_stmt_ext_list
%type <node> pl_entry_stmt pl_ddl_stmt
%type <node> raise_stmt goto_stmt lower_bound upper_bound label_def label_list labeled_pl_lang_stmt
%type <node> var_decl exception_list exception_name exception_init_pragma error_code exception_init_param exception_init_param_list
%type <ival> opt_reverse
/*SQL data type*/
%type <node> opt_charset collation opt_collation charset_name collation_name
%type <node> number_literal charset_key number_precision opt_binary precision_decimal_num opt_interval_leading opt_datetime
%type <ival> string_length_i opt_length_semantics_i nvarchar_type_i nstring_length_i
%type <ival> signed_int_num

%type <node> column sp_deterministic hash_or_range
%type <node> argument column_list order_or_cluster stream_clause partition_by parallel_enable pipelined
%type <node> data_source data_source_list opt_data_source_list result_cache relies_on_clause  opt_relies_on_clause
%type <node> sf_clause sf_clause_list opt_sf_clause opt_pipelined
%type <node> alter_procedure_stmt alter_function_stmt procedure_compile_clause compiler_parameter
%type <node> compiler_parameter_list sp_editionable sp_alter_clause

%type <node> create_type_body_stmt plsql_type_body_decl subprog_decl_in_type constructor_def_in_type
%type <node> plsql_type_body_source plsql_type_body_decl_list proc_or_func_def_in_type plsql_type_spec_source
%type <node> create_type_stmt opt_oid_clause opt_type_def object_type_def object_or_under opt_sqlj_obj_type
%type <node> final_or_inst final_inst_list opt_final_inst_list attr_and_element_spec attr_list
%type <node> attr_spec opt_sqlj_obj_type_attr element_spec element_spec_long
%type <ival> overriding_clause final_clause instantiable_clause
%type <node> inheritance_final_instantiable_clause inheritance_overriding_instantiable_clause inheritance_overriding_final_clause
%type <node> default_or_string el_element_spec_list_cc el_element_spec plsql_type_body_decl_list_semicolon
%type <node> subprogram_spec proc_or_func_spec sqlj_func_decl sqlj_obj_type_sig type_or_self
%type <node> varname_or_name constructor_spec constructor_def
%type <node> map_order_function_spec opt_constructor_impl varray_type_def pre_varray opt_varying
%type <node> drop_type_stmt opt_force_or_validate
%type <ival> opt_force sqlj_using assert_item member_or_static
%type <ival> urowid_length_i
%type <ival> multi_left_brackets multi_right_brackets
%type <node> opaque_def

%type <node> preprocess_stmt preprocess_stmt_list preprocess pre_if_stmt
%type <node> text_content pre_if opt_pre_elseifs pre_bool_expr error_stmt

%%
/*****************************************************************************
 *
 *      oracle pl start grammar
 *
 *****************************************************************************/
pl_entry:
    pl_entry_stmt_list
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_STMT_LIST, $1);
      parse_ctx->stmt_tree_ = $$;
      YYACCEPT;
    }
;

pl_entry_stmt_list:
    pl_entry_stmt END_P
    {
      $$ = $1;
    }
  | pl_entry_stmt ';' END_P
    {
      $$ = $1;
    }
;
/*****************************************************************************
 *
 *      oracle pl grammar entry
 *
 *****************************************************************************/
pl_entry_stmt:
    anonymous_stmt  { $$ = $1; }
  | pl_ddl_stmt   { $$ = $1; }
  | plsql_procedure_source { $$ = $1; }
  | plsql_function_source  { $$ = $1; }
  | plsql_trigger_source { $$ = $1; }
  | update_column_list { $$ = $1; }
  | constructor_spec { $$ = $1; }
  | constructor_def { $$ = $1; }
  | preprocess_stmt { $$ = $1; }
;


pl_ddl_stmt:
    create_package_stmt      { $$ = $1; }
  | create_package_body_stmt { $$ = $1; }
  | alter_package_stmt       { $$ = $1; }
  | drop_package_stmt        { $$ = $1; }
  | create_procedure_stmt    { $$ = $1; }
  | create_function_stmt     { $$ = $1; }
  | create_trigger_stmt      { $$ = $1; }
  | alter_procedure_stmt     { $$ = $1; }
  | alter_function_stmt      { $$ = $1; }
  | alter_trigger_stmt       { $$ = $1; }
  | drop_procedure_stmt      { $$ = $1; }
  | drop_function_stmt       { $$ = $1; }
  | drop_trigger_stmt        { $$ = $1; }
  | package_block            { $$ = $1; }
  | package_body_block       { $$ = $1; }
  | create_type_stmt         { $$ = $1; }
  | drop_type_stmt           { $$ = $1; }
  | create_type_body_stmt    { $$ = $1; }
  | plsql_type_spec_source   { $$ = $1; }
  | plsql_type_body_source   { $$ = $1; }
;

/*****************************************************************************
 *
 *      preprocess stmt grammar
 *
 *****************************************************************************/

preprocess_stmt:
  preprocess_stmt_list
  {
    merge_nodes($$, parse_ctx->mem_pool_, T_SP_PRE_STMTS, $1);
    $$->str_value_ = parse_strndup(
      parse_ctx->stmt_str_, parse_ctx->stmt_len_, parse_ctx->mem_pool_);
    check_ptr($$->str_value_);
    $$->str_len_ = parse_ctx->stmt_len_;
  }
;

preprocess_stmt_list:
    preprocess { $$ = $1; }
  | preprocess_stmt preprocess
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }

preprocess:
    text_content
    {
      $$ = $1;
      $$->int32_values_[0] = @1.first_column;
      $$->int32_values_[1] = @1.last_column;
    }
  | pre_if_stmt { $$ = $1; }
  | error_stmt  { $$ = $1; }
;

pre_if_stmt:
  IF pre_if END_KEY
  {
    $$ = $2;
  }
;

text_content:
  STRING
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_IDENT);
  }
;

pre_if:
  pre_bool_expr THEN preprocess_stmt opt_pre_elseifs
  {
    if (NULL == $1) YYERROR;
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PRE_IF, 3, $1, $3, $4);
  }
;

opt_pre_elseifs:
    /*EMPTY*/ { $$ = NULL; }
  | ELSIF pre_if
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PRE_ELSE, 1, $2);
    }
  | ELSE preprocess_stmt
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PRE_ELSE, 1, $2);
    }
;

pre_bool_expr:
  {
    do_parse_sql_bool_expr_rule_version_2($$, parse_ctx, 2, THEN, END_KEY);
  }
;

error_stmt:
  ERROR pre_bool_expr END_KEY
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ERROR, 1, $2);
  }
;


/*****************************************************************************
 *
 *      call stmt grammar
 *
 *****************************************************************************/

inner_call_stmt:
    pl_obj_access_ref
    {
      if ($1 != NULL && $1->type_ == T_SP_OBJ_ACCESS_REF && $1->num_child_ == 2 &&
          $1->children_[0] != NULL && $1->children_[1] == NULL &&
          $1->children_[0]->type_ == T_SP_ACCESS_NAME && $1->children_[0]->num_child_ == 3 &&
          $1->children_[0]->children_[0] == NULL && $1->children_[0]->children_[1] == NULL &&
          $1->children_[0]->children_[2] != NULL) {
        ParseNode *obj_node = $1->children_[0]->children_[2];
        if (nodename_equal(obj_node, "CONTINUE", 8)) {
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ITERATE, 2, NULL, NULL);
        } else if (nodename_equal(obj_node, "RAISE", 5)) {
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_SIGNAL, 1, NULL);
        } else if (nodename_equal(obj_node, "EXIT", 4)) {
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LEAVE, 2, NULL, NULL);
        } else {
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_INNER_CALL_STMT, 1, $1);
        }
      } else {
        malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_INNER_CALL_STMT, 1, $1);
      }
    }
;

sp_cparam_list:
    '(' opt_sp_cparams ')'
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_SP_CPARAM_LIST, $2);
    }
;

opt_sp_cparam_list:
    /*EMPTY*/ { $$ = NULL; }
    | sp_cparam_list { $$ = $1; }
;

opt_sp_cparams:
    opt_sp_cparams ',' sp_cparam
    {
      if ($1 == NULL || $3 == NULL) {
        YYERROR;
      }
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
  | sp_cparam { $$ = $1; }
;

sp_cparam:
  opt_cexpr opt_sp_cparam_with_assign
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
  | PARAM_ASSIGN_OPERATOR opt_cexpr
  {
    if (NULL == $2) YYERROR; $$ = $2;
      if (NULL != $2)
    copy_node_abs_location($2->stmt_loc_, @2);
  }
;

opt_cexpr:
    {
      //same as expr in sql rule, and terminate when read ';'
      do_parse_sql_bool_expr_rule_version_2($$, parse_ctx, 3, ',', ')', PARAM_ASSIGN_OPERATOR);
    }
;

/*****************************************************************************
 *
 *      anonymous stmt grammar
 *
 *****************************************************************************/
anonymous_stmt:
    pl_block
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ANONYMOUS_BLOCK, 1, $1);
    }
  | label_list pl_block
    {
      ParseNode *block = NULL;
      ParseNode *label_node = NULL;
      merge_nodes(label_node, parse_ctx->mem_pool_, T_LABEL_LIST, $1);
      if (T_SP_BLOCK_CONTENT == $2->type_) {
        malloc_non_terminal_node(block, parse_ctx->mem_pool_, T_SP_LABELED_BLOCK, 3, label_node, $2, NULL);
      } else if (T_SP_LABELED_BLOCK == $2->type_) {
        $2->children_[0] = label_node;
        block = $2;
      }
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ANONYMOUS_BLOCK, 1, block);
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

unit_kind:
  FUNCTION { $$[0] = SP_FUNCTION; }
  | PROCEDURE { $$[0] = SP_PROCEDURE; }
  | PACKAGE_P { $$[0] = SP_PACKAGE; }
  | TRIGGER { $$[0] = SP_TRIGGER; }
  | TYPE { $$[0] = SP_TYPE; }
;

accessor:
  pl_schema_name
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ACCESSOR, 2, NULL, $1);
  }
  | unit_kind pl_schema_name
  {
    ParseNode *accessor_kind = NULL;
    malloc_terminal_node(accessor_kind, parse_ctx->mem_pool_, T_SP_ACCESSOR_KIND);
    accessor_kind->value_ = $1[0];
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ACCESSOR, 2, accessor_kind, $2);
  }
;

accessor_list:
    accessor_list ',' accessor
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
  | accessor
    {
      $$ = $1;
    }
;

accessible_by:
  ACCESSIBLE BY '(' accessor_list ')'
  {
    ParseNode *accessor_list = NULL;
    merge_nodes(accessor_list, parse_ctx->mem_pool_, T_SP_ACCESSOR_LIST, $4);
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ACCESSIBLE_BY, 1, accessor_list);
  }
;

proc_clause:
    invoke_right { $$ = $1; }
  | accessible_by { $$ = $1; }
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

sp_deterministic:
  DETERMINISTIC
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_DETERMINISTIC);
  }
;

hash_or_range:
    HASH { $$ = NULL; }
  | RANGE { $$ = NULL; }
;

common_identifier:
    IDENT { $$ = $1; }
  | unreserved_keyword
    {
      get_oracle_non_reserved_node($$, parse_ctx->mem_pool_, @1.first_column, @1.last_column);
    }
;

identifier:
    IDENT { $$ = $1; }
  | unreserved_keyword
    {
      get_oracle_non_reserved_node($$, parse_ctx->mem_pool_, @1.first_column, @1.last_column);
    }
  | unreserved_special_keyword
    {
      get_oracle_non_reserved_node($$, parse_ctx->mem_pool_, @1.first_column, @1.last_column);
    }
;


sql_keyword_identifier:
    MERGE
    {
      get_oracle_non_reserved_node($$, parse_ctx->mem_pool_, @1.first_column, @1.last_column);
    }
    | EXISTS
    {
      get_oracle_non_reserved_node($$, parse_ctx->mem_pool_, @1.first_column, @1.last_column);
    }
;

argument:
    identifier { $$ = $1; }
;

column:
    identifier { $$ = $1; }
;

column_list:
    column { $$ = $1; }
  | column_list ',' column
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
;

order_or_cluster:
    ORDER { $$ = NULL; }
  | CLUSTER { $$ = NULL; }
;

stream_clause:
  order_or_cluster expr BY '(' column_list ')'
  {
    $$ = NULL;
      if (NULL != $2)
    copy_node_abs_location($2->stmt_loc_, @2);
  }
;

partition_by:
    ANY { $$ = NULL; }
  | VALUE '(' column ')' { $$ = NULL; }
  | hash_or_range '(' column_list ')' stream_clause { $$ = NULL; }
  | hash_or_range '(' column_list ')' { $$ = NULL; }
;

parallel_enable:
    PARALLEL_ENABLE
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_PARALLEL_ENABLE);
    }
  | PARALLEL_ENABLE '(' PARTITION argument BY partition_by ')'
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PARALLEL_ENABLE, 2, $4, $6);
    }
;

data_source:
  identifier { $$ = $1; }
;

data_source_list:
    data_source_list ',' data_source
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
  | data_source { $$ = $1; }
;

opt_data_source_list:
    /* empty */ { $$ = NULL; }
  | data_source_list
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_SP_SOURCE_LIST, $1);
    }
;

relies_on_clause:
  RELIES_ON '(' opt_data_source_list ')'
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_RELIES_ON, 1, $3);
  }
;

opt_relies_on_clause:
    /* empty */ { $$ = NULL; }
  | relies_on_clause { $$ = $1; }
;

result_cache:
  RESULT_CACHE opt_relies_on_clause
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_RESULT_CACHE, 1, $2);
  }
;

sf_clause:
    invoke_right { $$ = $1; }
  | accessible_by { $$ = $1; }
  | sp_deterministic { $$ = $1; }
  | parallel_enable { $$ = $1; }
  | result_cache { $$ = $1; }
  | pipelined { $$ = $1; }
;

sf_clause_list:
    sf_clause { $$ = $1; }
  | sf_clause_list sf_clause
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
;

opt_sf_clause:
    /* empty */ { $$ = NULL; }
  | sf_clause_list
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_SP_CLAUSE_LIST, $1);
    }
;

pipelined:
  PIPELINED
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_PIPELINED);
  }
;

opt_pipelined:
    /* empty */ { $$ = NULL; }
  | pipelined
    {
      $$ = $1;
    }
;
/*****************************************************************************
 *
 *      CREATE PACKAGE grammar
 *
 *****************************************************************************/
create_package_stmt:
    CREATE opt_replace opt_editionable package_block
    {
      const char *stmt_str = parse_ctx->orig_stmt_str_ + @4.first_column;
      int32_t str_len = @4.last_column - @4.first_column + 1;
      $4->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($4->str_value_);
      $4->str_len_ = str_len;
      $4->str_off_ = @4.first_column;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_CREATE, 1, $4);
      $$->int32_values_[0] = $2[0];
      $$->int32_values_[1] = $3[0];
    }
;

package_block:
    PACKAGE_P pl_schema_name opt_proc_clause is_or_as opt_decl_stmt_list END_KEY opt_tail_name
    {
      ParseNode *pkg_decl_stmts = NULL;
      merge_nodes(pkg_decl_stmts, parse_ctx->mem_pool_, T_PACKAGE_STMTS, $5);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_BLOCK, 4, $2, $3, pkg_decl_stmts, $7);
      if (parse_ctx->is_inner_parse_) {
        const char *stmt_str = parse_ctx->orig_stmt_str_ + @4.first_column;
        int32_t str_len = @7.last_column - @4.first_column + 1;
        $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
        check_ptr($$->str_value_);
        $$->str_len_ = str_len;
        $$->str_off_ = @4.first_column;
      }
    }
;

/*****************************************************************************
 *
 *      CREATE PACKAGE BODY grammar
 *
 *****************************************************************************/
create_package_body_stmt:
    CREATE opt_replace opt_editionable package_body_block
    {
      check_ptr($4);
      const char *stmt_str = parse_ctx->orig_stmt_str_ + @4.first_column;
      int32_t str_len = @4.last_column - @4.first_column + 1;
      $4->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($4->str_value_);
      $4->str_len_ = str_len;
      $4->str_off_ = @4.first_column;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_CREATE_BODY, 1, $4);
      $$->int32_values_[0] = $2[0];
      $$->int32_values_[1] = $3[0];
    }
;

package_body_block:
    PACKAGE_P BODY pl_schema_name is_or_as opt_decl_stmt_ext_list opt_execute_section END_KEY opt_tail_name
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $6);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_BODY_BLOCK, 4, $3, $5, proc_stmts, $8);
      if (parse_ctx->is_inner_parse_) {
        const char *stmt_str = parse_ctx->orig_stmt_str_ + @4.first_column;
        int32_t str_len = @8.last_column - @4.first_column + 1;
        $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
        check_ptr($$->str_value_);
        $$->str_len_ = str_len;
        $$->str_off_ = @4.first_column;
      }
    }
;

/*****************************************************************************
 *
 *      ALTER PACKAGE grammar
 *
 *****************************************************************************/
alter_package_stmt:
    ALTER PACKAGE_P pl_schema_name alter_package_clause
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_ALTER, 2, $3, $4);
    }
;

alter_package_clause:
      COMPILE opt_debug opt_compile_unit opt_reuse_settings
      {
        malloc_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_ALTER_OPTIONS);
        $$->int16_values_[0] = PACKAGE_ALTER_COMPILE;
        $$->int16_values_[1] = $2[0];
        $$->int16_values_[2] = $3[0];
        $$->int16_values_[3] = $4[0];
      }
    | EDITIONABLE
      {
        malloc_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_ALTER_OPTIONS);
        $$->int16_values_[0] = PACKAGE_ALTER_EDITIONABLE;
      }
    | NONEDITIONABLE
      {
        malloc_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_ALTER_OPTIONS);
        $$->int16_values_[0] = PACKAGE_ALTER_NONEDITIONABLE;
      }
;

opt_debug:
      /*EMPTY*/      { $$[0] = 0; }
    | DEBUG          { $$[0] = 1; }
;

opt_compile_unit:
      /*EMPTY*/      { $$[0] = PACKAGE_UNIT_PACKAGE; }
    | PACKAGE_P      { $$[0] = PACKAGE_UNIT_PACKAGE; }
    | SPECIFICATION  { $$[0] = PACKAGE_UNIT_SPECIFICATION; }
    | BODY           { $$[0] = PACKAGE_UNIT_BODY; }
;

opt_reuse_settings:
      /*EMPTY*/        { $$[0] = 0; }
    | REUSE SETTINGS   { $$[0] = 1; }
;

/*****************************************************************************
 *
 *      DROP PACKAGE grammar
 *
 *****************************************************************************/
drop_package_stmt:
    DROP PACKAGE_P pl_schema_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_DROP, 1, $3);
      $$->value_ = 0;
    }
    | DROP PACKAGE_P BODY pl_schema_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_PACKAGE_DROP, 1, $4);
      $$->value_ = 2;
    }
;

/*****************************************************************************
 *
 *      common pl grammar
 *
 *****************************************************************************/
pl_schema_name:
    identifier '.' identifier
    {
      if (nodename_equal($1, "INT", 3) ||
          nodename_equal($1, "FLOAT", 5) ||
          nodename_equal($1, "DOUBLE", 6) ||
          nodename_equal($1, "NUMBER", 6) ||
          nodename_equal($1, "VARCHAR", 7) ||
          nodename_equal($1, "VARCHAR2", 8) ||
          nodename_equal($1, "CONSTANT", 8) ||
          nodename_equal($1, "DATE", 4) ||
          nodename_equal($1, "CHAR", 4) ||
          nodename_equal($1, "CHARACTER", 9) ||
          nodename_equal($1, "LOOP", 4) ||
          nodename_equal($1, "REAL", 4) ||
          nodename_equal($1, "RAW", 3) ||
          nodename_equal($1, "INTEGER", 7)) {
        obpl_oracle_yyerror(&@1, parse_ctx, "Syntax Error");
        YYERROR;
      } else if (nodename_equal($3, "INT", 3) ||
                 nodename_equal($3, "FLOAT", 5) ||
                 nodename_equal($3, "DOUBLE", 6) ||
                 nodename_equal($3, "NUMBER", 6) ||
                 nodename_equal($3, "VARCHAR", 7) ||
                 nodename_equal($3, "VARCHAR2", 8) ||
                 nodename_equal($3, "CONSTANT", 8) ||
                 nodename_equal($3, "DATE", 4) ||
                 nodename_equal($3, "CHAR", 4) ||
                 nodename_equal($3, "CHARACTER", 9) ||
                 nodename_equal($3, "LOOP", 4) ||
                 nodename_equal($3, "REAL", 4) ||
                 nodename_equal($3, "RAW", 3) ||
                 nodename_equal($3, "INTEGER", 7)) {
        obpl_oracle_yyerror(&@3, parse_ctx, "Syntax Error");
        YYERROR;
      } else {
        malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_NAME, 2, $1, $3);
      }
    }
  | identifier
    {
      if (nodename_equal($1, "INT", 3) ||
          nodename_equal($1, "FLOAT", 5) ||
          nodename_equal($1, "DOUBLE", 6) ||
          nodename_equal($1, "NUMBER", 6) ||
          nodename_equal($1, "VARCHAR", 7) ||
          nodename_equal($1, "VARCHAR2", 8) ||
          nodename_equal($1, "CONSTANT", 8) ||
          nodename_equal($1, "DATE", 4) ||
          nodename_equal($1, "CHAR", 4) ||
          nodename_equal($1, "CHARACTER", 9) ||
          nodename_equal($1, "LOOP", 4) ||
          nodename_equal($1, "REAL", 4) ||
          nodename_equal($1, "RAW", 3) ||
          nodename_equal($1, "INTEGER", 7)) {
        obpl_oracle_yyerror(&@1, parse_ctx, "Syntax Error");
        YYERROR;
      } else {
        malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_NAME, 2, NULL, $1);
      }
    }
  | sql_keyword_identifier
   {
     malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_NAME, 2, NULL, $1);
   }
;

pl_access_name:
    identifier '.' identifier '.' identifier
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ACCESS_NAME, 3, $1, $3, $5);
    }
  | identifier '.' identifier %prec ';'
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ACCESS_NAME, 3, NULL, $1, $3);
    }
  | identifier %prec ';'
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ACCESS_NAME, 3, NULL, NULL, $1);
    }
  | identifier '.' DELETE %prec ';'
    {
      ParseNode *ident = NULL;
      malloc_terminal_node(ident, parse_ctx->mem_pool_, T_IDENT);
      ident->str_value_ = parse_strndup("delete", strlen("delete"), parse_ctx->mem_pool_);
      ident->str_len_ = strlen("delete");
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ACCESS_NAME, 3, NULL, $1, ident);
    }
  | identifier '.' identifier '.' DELETE
    {
      ParseNode *ident = NULL;
      malloc_terminal_node(ident, parse_ctx->mem_pool_, T_IDENT);
      ident->str_value_ = parse_strndup("delete", strlen("delete"), parse_ctx->mem_pool_);
      ident->str_len_ = strlen("delete");
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ACCESS_NAME, 3, $1, $3, ident);
    }
;

var_common_name:
    IDENT { $$ = $1; }
    | unreserved_keyword
    {
      get_oracle_non_reserved_node($$, parse_ctx->mem_pool_, @1.first_column, @1.last_column);
    }
    | MAP
    {
      get_oracle_non_reserved_node($$, parse_ctx->mem_pool_, @1.first_column, @1.last_column);
    }
;

field_name:
    identifier { $$ = $1; }
;

type_name:
    identifier { $$ = $1; }
;

func_name:
    identifier { $$ = $1; }
  | sql_keyword_identifier { $$ = $1; }
;

proc_name:
    identifier { $$ = $1; }
  | sql_keyword_identifier { $$ = $1; }
;

param_name:
    identifier { $$ = $1; }
;

cursor_name:
    identifier { $$ = $1; }
  | QUESTIONMARK { $$ = $1; }
;

opt_label:
    /*Empty*/ { $$ = NULL; }
  | label_name
    {
      $$ = $1;
    }
;

label_name:
    identifier { $$ = $1; }
;

exception_name:
    identifier { $$ = $1; }
;

opt_tail_name:
      /*EMPTY*/     { $$ = NULL; }
    | identifier         { $$ = $1; }
    | sql_keyword_identifier { $$ = $1; }
;

opt_replace:
      /*EMPTY*/       { $$[0] = 0; }
    | OR REPLACE      { $$[0] = 1; }
;

opt_editionable:
      /*EMPTY*/          { $$[0] = 0; }
    | EDITIONABLE        { $$[0] = 0; }
    | NONEDITIONABLE     { $$[0] = 1; }
;

is_or_as:
      IS { $$ = NULL; }
    | AS { $$ = NULL; }
;

opt_as:
      /*EMPTY*/     { $$ = NULL; }
    | AS            { $$ = NULL; }

/*****************************************************************************
 *
 *      CREATE PROCEDURE grammar
 *
 *****************************************************************************/
create_procedure_stmt:
    CREATE opt_replace opt_editionable plsql_procedure_source
    {
      check_ptr($4);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_CREATE, 1, $4);
      $$->int32_values_[0] = $2[0];
      $$->int32_values_[1] = $3[0];
    }
;

create_function_stmt:
    CREATE opt_replace opt_editionable plsql_function_source
    {
      check_ptr($4);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SF_CREATE, 1, $4);
      $$->int32_values_[0] = $2[0];
      $$->int32_values_[1] = $3[0];
    }
;

create_trigger_stmt:
    CREATE opt_replace opt_editionable plsql_trigger_source
    {
      check_ptr($4);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_CREATE, 1, $4);
      $$->int32_values_[0] = $2[0];
      $$->int32_values_[1] = $3[0];
    }
;

plsql_procedure_source:
    PROCEDURE pl_schema_name opt_sp_param_list opt_proc_clause is_or_as pl_impl_body
    {
      check_ptr($6);
      const char* stmt_str = NULL;
      int32_t str_len = 0;
      int32_t str_off = 0;
      if (parse_ctx->is_inner_parse_) {
        stmt_str = parse_ctx->orig_stmt_str_ + @6.first_column;
        str_len = @6.last_column - @6.first_column + 1;
        str_off = @6.first_column;
      } else {
        stmt_str = parse_ctx->orig_stmt_str_ + @1.first_column;
        str_len = @6.last_column - @1.first_column + 1;
        str_off = @6.first_column;
      }
      $6->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($6->str_value_);
      $6->str_len_ = str_len;
      $6->str_off_ = str_off;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_SOURCE, 4, $2, $3, $4, $6);
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;

plsql_function_source:
    FUNCTION pl_schema_name opt_sp_param_list RETURN pl_outer_data_type opt_sf_clause is_or_as pl_impl_body
    {
      check_ptr($8);
      const char *stmt_str = NULL;
      int32_t str_len = 0;
      int32_t str_off = 0;
      if (parse_ctx->is_inner_parse_) {
        stmt_str = parse_ctx->orig_stmt_str_ + @8.first_column;
        str_len = @8.last_column - @8.first_column + 1;
        str_off = @8.first_column;
      } else {
        stmt_str = parse_ctx->orig_stmt_str_ + @1.first_column;
        str_len = @8.last_column - @1.first_column + 1;
        str_off = @1.first_column;
      }
      $8->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($8->str_value_);
      $8->str_len_ = str_len;
      $8->str_off_ = str_off;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SF_SOURCE, 6, $2, $3, $5, $6, NULL, $8);
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | FUNCTION pl_schema_name opt_sp_param_list RETURN pl_outer_data_type opt_sf_clause AGGREGATE USING pl_schema_name
    {
      ParseNode *body_node = NULL;
      malloc_non_terminal_node(body_node, parse_ctx->mem_pool_, T_SF_AGGREGATE_BODY, 1, $9);
      check_ptr(body_node);
      const char* stmt_str = NULL;
      int32_t str_len = 0;
      if (parse_ctx->is_inner_parse_) {
        stmt_str = parse_ctx->orig_stmt_str_ + @9.first_column;
        str_len = @9.last_column - @9.first_column + 1;
      } else {
        stmt_str = parse_ctx->orig_stmt_str_ + @1.first_column;
        str_len = @9.last_column - @1.first_column + 1;
      }
      body_node->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr(body_node->str_value_);
      body_node->str_len_ = str_len;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SF_AGGREGATE_SOURCE, 6, $2, $3, $5, $6, NULL, body_node);
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;

plsql_trigger_source:
    TRIGGER pl_schema_name opt_default_collation trigger_definition
    {
      check_ptr($4);
      const char *stmt_str = parse_ctx->orig_stmt_str_ + @1.first_column;
      int32_t str_len = @4.last_column - @1.first_column + 1;
      $4->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($4->str_value_);
      $4->str_len_ = str_len;
      $4->str_off_ = @1.first_column;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_SOURCE, 2, $2, $4);

      const char *tg_def = parse_ctx->orig_stmt_str_ + @4.first_column;
      int32_t tg_def_len = @4.last_column - @4.first_column + 1;
      $$->str_value_ = parse_strndup(tg_def, tg_def_len, parse_ctx->mem_pool_);
      check_ptr($$->str_value_);
      $$->str_len_ = tg_def_len;
      $4->str_off_ = @4.first_column;

      $$->value_ = $3[0];
    }
;

opt_default_collation:
    DEFAULT COLLATION USING_NLS_COMP { $$[0] = 1; }
  | /*EMPTY*/ { $$[0] = 0; }

trigger_definition:
    simple_dml_trigger { $$ = $1; }
  | compound_dml_trigger { $$ = $1; }
  | instead_of_dml_trigger { $$ = $1; }
;

simple_dml_trigger:
    before_or_after dml_event_option opt_referencing_list opt_for_each_row opt_enable_or_disable opt_when_condition simple_trigger_body
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_SIMPLE_DML, 4, $2, $3, $6, $7);
      $$->int16_values_[0] = $1[0];
      $$->int16_values_[1] = $4[0];
      $$->int16_values_[2] = $5[0];
    }
;

compound_dml_trigger:
    FOR dml_event_option opt_referencing_list opt_enable_or_disable opt_when_condition compound_trigger_body
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_COMPOUND_DML, 4, $2, $3, $5, $6);
      $$->value_ = $4[0];
    }
;

instead_of_dml_trigger:
    INSTEAD OF dml_event_option opt_referencing_list opt_for_each_row opt_enable_or_disable
    opt_when_condition /* instead of trigger not support when-clause,
                          its only for compatible with error reporting behavior of oracle */
    simple_trigger_body
    {
      // An INSTEAD OF trigger is always a row-level trigger,
      // whether it includes opt_for_each_row clause.
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_INSTEAD_DML, 4, $3, $4, $7, $8);
      $$->int16_values_[0] = $5[0];
      $$->int16_values_[1] = $6[0];
    }
;

before_or_after:
    BEFORE { $$[0] = T_BEFORE; }
  | AFTER  { $$[0] = T_AFTER; }
;

dml_event_option:
    dml_event_list ON pl_schema_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_DML_EVENT_OPTION, 3, $1, NULL, $3);
    }
  | dml_event_list ON nested_table_column pl_schema_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_DML_EVENT_OPTION, 3, $1, $3, $4);
    }
;

dml_event_list:
    dml_event_tree
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_TG_DML_EVENT_LIST, $1);
    }
;

dml_event_tree:
    dml_event_tree OR dml_event
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
  | dml_event
    {
      $$ = $1;
    }
;

dml_event:
    INSERT opt_column_list
    {
      $$ = $2;
      $$->type_ = T_INSERT;
    }
  | DELETE opt_column_list
    {
      $$ = $2;
      $$->type_ = T_DELETE;
    }
  | UPDATE opt_column_list
    {
      $$ = $2;
      $$->type_ = T_UPDATE;
/*
      if ($$->num_child_ > 0) {
        const char *event_str = parse_ctx->stmt_str_ + @1.first_column;
        int32_t str_len = @2.last_column - @1.first_column + 1;
        $$->str_value_ = parse_strndup(event_str, str_len, parse_ctx->mem_pool_);
        $$->str_len_ = str_len;
        check_ptr($$->str_value_);
      }
*/
    }
;

update_column_list:
    UPDATE OF column_list
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_TG_COLUMN_LIST, $3);
    }
;

nested_table_column:
    NESTED TABLE column OF
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_TG_NEST_OF_TABLE_COLUMN, $3);
    }
;

opt_referencing_list:
    REFERENCING referencing_list
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_TG_REF_LIST, $2);
    }
  | /*EMPTY*/
    {
      $$ = NULL;
    }
;

referencing_list:
    referencing_list referencing_node
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
  | referencing_node
    {
      $$ = $1;
    }
;

referencing_node:
    OLD opt_as ref_name
    {
      $$ = $3;
      $$->value_ = T_TG_REF_OLD;
    }
  | NEW opt_as ref_name
    {
      $$ = $3;
      $$->value_ = T_TG_REF_NEW;
    }
  | PARENT opt_as ref_name
    {
      $$ = $3;
      $$->value_ = T_TG_REF_PARENT;
    }
;

ref_name:
    identifier
    {
      if (nodename_equal($1, "INT", 3) ||
          nodename_equal($1, "FLOAT", 5) ||
          nodename_equal($1, "DOUBLE", 6) ||
          nodename_equal($1, "NUMBER", 6) ||
          nodename_equal($1, "VARCHAR", 7) ||
          nodename_equal($1, "VARCHAR2", 8) ||
          nodename_equal($1, "CONSTANT", 8) ||
          nodename_equal($1, "DATE", 4) ||
          nodename_equal($1, "CHAR", 4) ||
          nodename_equal($1, "CHARACTER", 9) ||
          nodename_equal($1, "BODY", 4) ||
          nodename_equal($1, "LOOP", 4) ||
          nodename_equal($1, "REAL", 4) ||
          nodename_equal($1, "INTEGER", 7)) {
        obpl_oracle_yyerror(&@1, parse_ctx, "Syntax Error");
        YYERROR;
      } else {
        $$ = $1;
      }
    }
;

opt_for_each_row:
    FOR EACH ROW { $$[0] = T_TP_EACH_ROW; }
  | /*EMPTY*/ { $$[0] = T_TP_STATEMENT; }
;

opt_enable_or_disable:
    enable_or_disable
  | /*EMPTY*/ { $$[0] = T_ENABLE; }
;

opt_when_condition:
    WHEN '(' bool_expr ')'
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_WHEN_CONDITION, 1, $3);
      const char *expr_str = parse_ctx->stmt_str_ + @3.first_column;
      int32_t expr_str_len = @3.last_column - @3.first_column + 1;
      $$->str_value_ = parse_strndup(expr_str, expr_str_len, parse_ctx->mem_pool_);
      check_ptr($$->str_value_);
      $$->str_len_ = expr_str_len;
      if (NULL != $3)
      copy_node_abs_location($3->stmt_loc_, @3);
    }
  | /*EMPTY*/ { $$ = NULL; }
;

simple_trigger_body:
    pl_block
    {
      $$ = $1;
    }
;

compound_trigger_body:
    COMPOUND TRIGGER opt_decl_stmt_ext_list timing_point_section_list END_KEY opt_tail_name
    {
      ParseNode *sections = NULL;
      merge_nodes(sections, parse_ctx->mem_pool_, T_TG_TIMPING_POINT_SECTION_LIST, $4);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_COMPOUND_BODY, 3, $3, sections, $6);
    }
;

timing_point_section_list:
    timing_point_section_list timing_point_section
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
  | timing_point_section
    {
      $$ = $1;
    }
;

timing_point_section:
    timing_point IS tps_body timing_point ';'
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_TIMPING_POINT_SECTION, 1, $3);
      $$->int16_values_[0] = $1[0];
      $$->int16_values_[1] = $1[1];
      $$->int16_values_[2] = $4[0];
      $$->int16_values_[3] = $4[1];
    }
;

timing_point:
    BEFORE STATEMENT
    {
      $$[0] = T_BEFORE;
      $$[1] = T_TP_STATEMENT;
    }
  | BEFORE EACH ROW
    {
      $$[0] = T_BEFORE;
      $$[1] = T_TP_EACH_ROW;
    }
  | AFTER STATEMENT
    {
      $$[0] = T_AFTER;
      $$[1] = T_TP_STATEMENT;
    }
  | AFTER EACH ROW
    {
      $$[0] = T_AFTER;
      $$[1] = T_TP_EACH_ROW;
    }
  | INSTEAD OF EACH ROW
    {
      $$[0] = T_INSTEAD;
      $$[1] = T_TP_EACH_ROW;
    }
;

tps_body:
    opt_decl_stmt_ext_list execute_section END_KEY
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, 2, $1, $2);
      const char *body_str = parse_ctx->stmt_str_ + ($1 != NULL ? @1.first_column : @2.first_column);
      int32_t body_len = @3.last_column - ($1 != NULL ? @1.first_column : @2.first_column) + 1;
      $$->str_value_ = parse_strndup(body_str, body_len, parse_ctx->mem_pool_);
      check_ptr($$->str_value_);
      $$->str_len_ = body_len;
    }

opt_column_list:
    OF column_list
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_TG_COLUMN_LIST, $2);
      const char *columns_str = parse_ctx->stmt_str_ + @2.first_column;
      int32_t str_len = @2.last_column - @2.first_column + 1;
      $$->str_value_ = parse_strndup(columns_str, str_len, parse_ctx->mem_pool_);
      $$->str_len_ = str_len;
      check_ptr($$->str_value_);
    }
  | /*EMPTY*/
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_TG_COLUMN_LIST);
    }
;

/*stored procedure param list*/
opt_sp_param_list:
    /* empty */ { $$ = NULL; }
  | '(' sp_param_list ')'
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_SP_PARAM_LIST, $2);
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
    param_name pl_outer_data_type opt_default
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PARAM, 3, $1, $2, $3);
      $$->int32_values_[0] = MODE_IN;
      $$->int32_values_[1] = 0;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
    | param_name IN pl_outer_data_type opt_default
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PARAM, 3, $1, $3, $4);
      $$->int32_values_[0] = MODE_IN;
      $$->int32_values_[1] = 0;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
    | param_name OUT pl_outer_data_type opt_default
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PARAM, 3, $1, $3, $4);
      $$->int32_values_[0] = MODE_OUT;
      $$->int32_values_[1] = 0;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
    | param_name OUT NOCOPY pl_outer_data_type opt_default
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PARAM, 3, $1, $4, $5);
      $$->int32_values_[0] = MODE_OUT;
      $$->int32_values_[1] = 1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
    | param_name IN OUT pl_outer_data_type opt_default
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PARAM, 3, $1, $4, $5);
      $$->int32_values_[0] = MODE_INOUT;
      $$->int32_values_[1] = 0;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
    | param_name IN OUT NOCOPY pl_outer_data_type opt_default
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PARAM, 3, $1, $5, $6);
      $$->int32_values_[0] = MODE_INOUT;
      $$->int32_values_[1] = 1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;

opt_sp_inout:
    /* Empty */ { $$[0] = MODE_IN; $$[1] = 0; }
  | IN
    {
      $$[0] = MODE_IN;
      $$[1] = 0;
    }
  | OUT %prec LOWER_PARENS
    {
      $$[0] = MODE_OUT;
      $$[1] = 0;
    }
  | OUT NOCOPY
    {
      $$[0] = MODE_OUT;
      $$[1] = 1;
    }
  | IN OUT %prec LOWER_PARENS
    {
      $$[0] = MODE_INOUT;
      $$[1] = 0;
    }
  | IN OUT NOCOPY
    {
      $$[0] = MODE_INOUT;
      $$[1] = 1;
    }
;

pl_impl_body:
  pl_body
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | call_spec
    {
      $$ = $1;
    }
  | EXTERNAL
    {
      $$ = NULL;
    }
;

call_spec:
    LANGUAGE
    {
      $$ = NULL;
    }
;

pl_lang_stmt:
    pl_lang_stmt_without_semicolon ';' { $$ = $1; }
;

pl_lang_stmt_without_semicolon:
    inline_pragma
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | assign_stmt
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_VARIABLE_SET, $1);
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | sql_stmt
    {
      $$ = $1;
      const char *stmt_str = parse_ctx->stmt_str_ + @1.first_column;
      int32_t str_len = @1.last_column - @1.first_column + 1;
      $1->raw_text_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($1->raw_text_);
      $1->text_len_ = str_len;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | if_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | case_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | basic_loop_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | while_loop_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | for_loop_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | cursor_for_loop_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | forall_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | return_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | continue_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | exit_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | open_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | fetch_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | close_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | execute_immediate_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | raise_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | pl_block
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | goto_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | inner_call_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | null_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | pipe_row_stmt
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;

assign_stmt:
  pl_left_value ASSIGN_OPERATOR pl_right_value
  {
    ParseNode *right_value = NULL;
    malloc_non_terminal_node(right_value, parse_ctx->mem_pool_, T_SP_DECL_DEFAULT, 1, $3);
    // 拷贝EXPR语句
    const char *expr_str = parse_ctx->stmt_str_ + @3.first_column;
    int32_t expr_str_len = @3.last_column - @3.first_column + 1;
    right_value->str_value_ = parse_strndup(expr_str, expr_str_len, parse_ctx->mem_pool_);
    check_ptr(right_value->str_value_);
    right_value->str_len_ = expr_str_len;
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_VAR_VAL, 2, $1, right_value);
      if (NULL != $3)
    copy_node_abs_location($3->stmt_loc_, @3);
  }
;

pl_left_value_list:
    pl_left_value
    {
      $$ = $1;
    }
  | pl_left_value ',' pl_left_value_list
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
;

pl_left_value:
    pl_obj_access_ref
    {
      const char* left_val_str = parse_ctx->stmt_str_ + @1.first_column;
      int32_t left_val_len = @1.last_column - @1.first_column + 1;
      $1->str_value_ = parse_strndup(left_val_str, left_val_len, parse_ctx->mem_pool_);
      check_ptr($1->str_value_);
      $1->str_len_ = left_val_len;
      $$ = $1;
    }
  | QUESTIONMARK { $$ = $1; }
;

pl_obj_access_ref:
    pl_access_name %prec ';'
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ACCESS_REF, 2, $1, NULL);
    }
  | pl_access_name pl_obj_access_ref_suffix_list
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ACCESS_REF, 2, $1, $2);
    }
  | QUESTIONMARK pl_obj_access_ref_suffix_list
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ACCESS_REF, 2, $1, $2);
    }
;

pl_obj_access_ref_suffix_list:
    pl_obj_access_ref_suffix
    {
      $$ = $1;
    }
  | pl_obj_access_ref_suffix pl_obj_access_ref_suffix_list
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ACCESS_REF, 2, $1, $2);
    }
;

pl_obj_access_ref_suffix:
  '.' DELETE
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_IDENT);
      $$->str_value_ = parse_strndup("delete", strlen("delete"), parse_ctx->mem_pool_);
      $$->str_len_ = strlen("delete");
    }
  | '.' identifier
    {
      $$ = $2;
    }
  | '(' opt_sp_cparams ')'
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_SP_CPARAM_LIST, $2);
    }
;

pl_right_value:
    {
      //same as expr in sql rule, and terminate when read ';'
      //in record(a number := 5) or record(a number := 5, b number)
      do_parse_sql_bool_expr_rule_version_2($$, parse_ctx, 3, ';', ')', ',');
      if (NULL == $$) {
        YYERROR;
      }
    }
;

if_stmt:
    IF sp_if END_KEY IF
    {
      $$ = $2;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;

sp_if:
    bool_expr THEN pl_lang_stmt_list opt_sp_elseifs
    {
      if (NULL == $1) YYERROR;
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $3);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_IF, 3, $1, proc_stmts, $4);
      if (NULL != $1)
        copy_node_abs_location($1->stmt_loc_, @1);
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;

case_stmt:
    CASE expr sp_when_list opt_sp_elseifs END_KEY CASE opt_label
    {
      ParseNode *case_stmt = NULL;
      ParseNode *when_list = NULL;
      merge_nodes(when_list, parse_ctx->mem_pool_, T_WHEN_LIST, $3);
      malloc_non_terminal_node(case_stmt, parse_ctx->mem_pool_, T_SP_CASE, 3, $2, when_list, $4);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LABELED_BLOCK, 3, NULL, case_stmt, $7);
      if (NULL != $2)
      copy_node_abs_location($2->stmt_loc_, @2);
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
    WHEN bool_expr THEN pl_lang_stmt_list %prec LOWER_PARENS
    {
      if (NULL == $2) YYERROR;
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $4);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_WHEN, 2, $2, proc_stmts);
      if (NULL != $2)
      copy_node_abs_location($2->stmt_loc_, @2);
    }
;

opt_sp_elseifs:
    /*Empty*/ { $$ = NULL; }
  | ELSIF sp_if
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $2);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ELSE, 1, proc_stmts);
    }
  | ELSE pl_lang_stmt_list
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $2);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ELSE, 1, proc_stmts);
    }
;

pl_block:
    opt_declare_section execute_section END_KEY opt_tail_name
    {
      if ($1 != NULL) {
        if ($4 != NULL) {
          ParseNode *block = NULL;
          malloc_non_terminal_node(block, parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, 2, $1, $2);
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LABELED_BLOCK, 3, NULL, block, $4);
        } else {
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, 2, $1, $2);
        }
      } else {
        if ($4 != NULL) {
          ParseNode *block = NULL;
          malloc_non_terminal_node(block, parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, 1, $2);
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LABELED_BLOCK, 3, NULL, block, $4);
        } else {
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, 1, $2);
        }
      }
      if (parse_ctx->is_for_trigger_) {
        // body string.
        // @1.first_column seems wrong if opt_declare_section is NULL,
        // so we use @2.first_column when opt_declare_section is NULL.
        const char *body_str = parse_ctx->stmt_str_ + ($1 != NULL ? @1.first_column : @2.first_column);
        int32_t body_len = @3.last_column - ($1 != NULL ? @1.first_column : @2.first_column) + 1;
        char *dup_body = parse_strndup(body_str, body_len, parse_ctx->mem_pool_);
        check_ptr(dup_body);
        $$->str_value_ = dup_body;
        $$->str_len_ = body_len;
        $$->str_off_ = $1 != NULL ? @1.first_column : @2.first_column;
        // declare string.
        if ($1 != NULL) {
          const char *declare_str = $1->str_value_;
          int32_t declare_len = parse_ctx->stmt_str_ + @1.last_column - declare_str + 1 + 1;
          char *dup_declare = NULL;
          if (OB_LIKELY(NULL != (dup_declare = (char *)parse_malloc(declare_len + 1, parse_ctx->mem_pool_)))) {
            memmove(dup_declare, declare_str, declare_len - 1);
            dup_declare[declare_len - 1] = '\n';
            dup_declare[declare_len] = '\0';
          }
          check_ptr(dup_declare);
          $1->str_value_ = dup_declare;
          $1->str_len_ = declare_len;
          $1->str_off_ = @1.last_column;
        }
        // execute string.
        const char *execute_str = parse_ctx->stmt_str_ + @2.first_column;
        int32_t execute_len = @3.last_column - @2.first_column + 1 + 2;
        char *dup_execute = NULL;
        if (OB_LIKELY(NULL != (dup_execute = (char *)parse_malloc(execute_len + 1, parse_ctx->mem_pool_)))) {
          memmove(dup_execute, execute_str, execute_len - 2);
          dup_execute[execute_len - 2] = ';';
          dup_execute[execute_len - 1] = '\n';
          dup_execute[execute_len] = '\0';
        }
        check_ptr(dup_execute);
        $2->str_value_ = dup_execute;
        $2->str_len_ = execute_len;
        $2->str_off_ = @2.first_column;
      }
    }
;

opt_declare_section:
    DECLARE declare_section
    {
      $$ = $2;
      if (parse_ctx->is_for_trigger_) {
        $$->str_value_ = parse_ctx->stmt_str_ + @2.first_column;
      }
    }
  | DECLARE /*EMPTY*/ { $$ = NULL; }
  | /*EMPTY*/ { $$ = NULL; }
;

pl_body:
    declare_section execute_section END_KEY opt_tail_name
    {
      ParseNode *block = NULL;
      malloc_non_terminal_node(block, parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, 2, $1, $2);
      if ($4 != NULL) {
        malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LABELED_BLOCK, 3, NULL, block, $4);
      } else {
        $$ = block;
      }
    }
  | execute_section END_KEY opt_tail_name
    {
      ParseNode *block = NULL;
      malloc_non_terminal_node(block, parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, 1, $1);
      if ($3 != NULL) {
        malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LABELED_BLOCK, 3, NULL, block, $3);
      } else {
        $$ = block;
      }
    }
;

declare_section:
   decl_stmt_ext_list
   {
     merge_nodes($$, parse_ctx->mem_pool_, T_SP_DECL_LIST, $1);
   }
;

opt_execute_section:
    /*Empty*/ { $$ = NULL; }
  | execute_section
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;

execute_section:
    BEGIN_KEY pl_lang_stmt_list opt_exception_section
    {
      ParseNode *pl_stmts = NULL;
      if ($3 != NULL) {
        int index = 0;
        int pl_stmt_num = 0;
        int exception_stmt_num = 0;
        int total_stmt_num = 0;
        int tmp_ret = 0;
        if (OB_UNLIKELY(OB_PARSER_SUCCESS != (tmp_ret = count_child(
                                                              $2,
                                                              parse_ctx->mem_pool_,
                                                              &pl_stmt_num)))) {
          (void)fprintf(stderr, "ERROR fail to , count child num code : %d\n", tmp_ret);
        } else if (OB_UNLIKELY(OB_PARSER_SUCCESS != (tmp_ret
                                                     = count_child(
                                                             $3,
                                                             parse_ctx->mem_pool_,
                                                             &exception_stmt_num)))) {
          (void)fprintf(stderr, "ERROR fail to , count child num code : %d\n", tmp_ret);
        } else {
          total_stmt_num = pl_stmt_num + exception_stmt_num;
          malloc_new_non_terminal_node(pl_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, total_stmt_num);
          if (OB_UNLIKELY(OB_PARSER_SUCCESS != (tmp_ret
                                                = merge_child(pl_stmts,
                                                              parse_ctx->mem_pool_,
                                                              $3, &index)))) {
            (void)fprintf(stderr, "ERROR fail to merge_child, error code : %d\n", tmp_ret);
          } else if (OB_UNLIKELY(OB_PARSER_SUCCESS != (tmp_ret
                                                       = merge_child(pl_stmts,
                                                                     parse_ctx->mem_pool_,
                                                                     $2,
                                                                     &index)))) {
            (void)fprintf(stderr, "ERROR fail to merge_child, error code : %d\n", tmp_ret);
          } else if (index != total_stmt_num) {
            (void)fprintf(stderr, "ERROR index:%d is not equal to num:%d\n", index, total_stmt_num);
          } else {
            $$ = pl_stmts;
            // malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, 1, pl_stmts);
          }
        }
        if (tmp_ret != 0) {
          YYERROR;
        }
      } else {
        merge_nodes(pl_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $2);
        $$ = pl_stmts;
        // malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, 1, pl_stmts);
      }
    }
;

opt_exception_section:
    /*Empty*/ { $$ = NULL; }
  | EXCEPTION exception_section
    {
      $$ = $2;
    }
;

exception_section:
    exception_handler { $$ = $1; }
  | exception_handler exception_section
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
;

pl_lang_stmt_list:
    labeled_pl_lang_stmt { $$ = $1; }
  | pl_lang_stmt_list labeled_pl_lang_stmt
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
;

opt_decl_stmt_list:
    /*Empty*/ { $$ = NULL; }
  | decl_stmt_list
    {
      $$ = $1;
    }
;

decl_stmt_list:
    decl_stmt
    {
      $$ = $1;
    }
  | decl_stmt_list decl_stmt
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
;

decl_stmt:
    decl_stmt_without_semicolon ';' { $$ = $1; }
;

decl_stmt_without_semicolon:
    type_def
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL_USER_TYPE, 1, $1);
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | subtype_def
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | cursor_decl
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | cursor_def
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | item_decl
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | func_decl
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
  | proc_decl
    {
      $$ = $1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;

item_decl:
    var_decl
    {
      $$ = $1;
    }
  /*
  | constant_decl
    {
      $$ = $1;
    }
  */
  | exception_decl
    {
      $$ = $1;
    }
  | pragma_stmt
    {
      $$ = $1;
    }
;

/*
constant_decl:
    var_name CONSTANT pl_inner_data_type opt_not_null opt_default
    {
      ParseNode *decl_idents = NULL;
      merge_nodes(decl_idents, parse_ctx->mem_pool_, T_SP_DECL_IDENT_LIST, $1);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL, 3, decl_idents, $3, $5);
      $$->int32_values_[0] = $4[0];
      $$->int32_values_[1] = 1;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;
*/

exception_decl:
    exception_name EXCEPTION
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL_COND, 1, $1);
    }
;

var_decl:
    var_common_name pl_inner_data_type opt_not_null opt_default
    {
      ParseNode *decl_idents = NULL;
      merge_nodes(decl_idents, parse_ctx->mem_pool_, T_SP_DECL_IDENT_LIST, $1);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL, 3, decl_idents, $2, $4);
      $$->int32_values_[0] = $3[0];
      $$->int32_values_[1] = 0;
      if (nodename_equal($2, "SIMPLE_FLOAT", 12) || nodename_equal($2, "SIMPLE_DOUBLE", 13)) {
        $$->int32_values_[0] = 1;
      }
      copy_node_abs_location($$->stmt_loc_, @1);
    }
    | TYPE pl_inner_data_type opt_not_null opt_default
    {
      ParseNode *decl_idents = NULL;
      ParseNode *type_ident = NULL;
      make_name_node(type_ident, parse_ctx->mem_pool_, "TYPE");
      merge_nodes(decl_idents, parse_ctx->mem_pool_, T_SP_DECL_IDENT_LIST, type_ident);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL, 3, decl_idents, $2, $4);
      $$->int32_values_[0] = $3[0];
      $$->int32_values_[1] = 0;
      if (nodename_equal($2, "SIMPLE_FLOAT", 12) || nodename_equal($2, "SIMPLE_DOUBLE", 13)) {
        $$->int32_values_[0] = 1;
      }
      copy_node_abs_location($$->stmt_loc_, @1);
    }
    | var_common_name CONSTANT pl_inner_data_type opt_not_null opt_default
    {
      ParseNode *decl_idents = NULL;
      merge_nodes(decl_idents, parse_ctx->mem_pool_, T_SP_DECL_IDENT_LIST, $1);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL, 3, decl_idents, $3, $5);
      $$->int32_values_[0] = $4[0];
      $$->int32_values_[1] = 1;
      if (nodename_equal($3, "SIMPLE_FLOAT", 12) || nodename_equal($3, "SIMPLE_DOUBLE", 13)) {
        $$->int32_values_[0] = 1;
      }
      copy_node_abs_location($$->stmt_loc_, @1);
    }
    | TYPE CONSTANT pl_inner_data_type opt_not_null opt_default
    {
      ParseNode *decl_idents = NULL;
      ParseNode *type_ident = NULL;
      make_name_node(type_ident, parse_ctx->mem_pool_, "TYPE");
      merge_nodes(decl_idents, parse_ctx->mem_pool_, T_SP_DECL_IDENT_LIST, type_ident);
      merge_nodes(decl_idents, parse_ctx->mem_pool_, T_SP_DECL_IDENT_LIST, type_ident);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL, 3, decl_idents, $3, $5);
      $$->int32_values_[0] = $4[0];
      $$->int32_values_[1] = 1;
      if (nodename_equal($3, "SIMPLE_FLOAT", 12) || nodename_equal($3, "SIMPLE_DOUBLE", 13)) {
        $$->int32_values_[0] = 1;
      }
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;

opt_decl_stmt_ext_list:
    /*Empty*/ %prec DECLARATION { $$ = NULL; }
  | decl_stmt_ext_list %prec DECLARATION
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_PACKAGE_BODY_STMTS, $1);
      if (parse_ctx->is_for_trigger_) {
        const char *decl_str = parse_ctx->stmt_str_ + @1.first_column;
        int32_t decl_len = @1.last_column - @1.first_column + 1;
        $$->str_value_ = parse_strndup(decl_str, decl_len, parse_ctx->mem_pool_);
        $$->str_len_ = decl_len;
        check_ptr($$->str_value_);
      }
    }
;

decl_stmt_ext_list:
    decl_stmt_ext
    {
      $$ = $1;
    }
  | decl_stmt_ext_list decl_stmt_ext
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
;

decl_stmt_ext:
    decl_stmt_ext_without_semicolon ';' { $$ = $1; }
;

decl_stmt_ext_without_semicolon:
    decl_stmt_without_semicolon
    {
      $$ = $1;
    }
  | func_def
    {
      $$ = $1;
    }
  | proc_def
    {
      $$ = $1;
    }
;

func_decl:
    FUNCTION func_name opt_sp_param_list RETURN pl_outer_data_type opt_sf_clause
    {
      const char *stmt_str = parse_ctx->stmt_str_ + @1.first_column;
      int32_t str_len = @6.last_column - @1.first_column + 1;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SUB_FUNC_DECL, 5, $2, $3, $5, $6, NULL);
      check_ptr($$);
      $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($$->str_value_);
      $$->str_len_ = str_len;
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;

func_def:
    func_decl is_or_as pl_impl_body
    {
      check_ptr($3);
      const char* stmt_str = NULL;
      int32_t str_len = 0;
      stmt_str = parse_ctx->stmt_str_ + @1.first_column;
      str_len = @3.last_column - @1.first_column + 1;
      $3->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($3->str_value_);
      $3->str_len_ = str_len;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SUB_FUNC_DEF, 2, $1, $3);
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;

proc_decl:
    PROCEDURE proc_name opt_sp_param_list opt_proc_clause opt_pipelined
    {
      const char *stmt_str = parse_ctx->stmt_str_ + @1.first_column;
      int32_t str_len = @4.last_column - @1.first_column + 1;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SUB_PROC_DECL, 3, $2, $3, $4);
      check_ptr($$);
      $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($$->str_value_);
      $$->str_len_ = str_len;
      if ($5 != NULL) {
        YY_ONLY_FUNC_CAN_PIPELINED("only function can pipelined\n");
      }
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;

proc_def:
    proc_decl is_or_as pl_impl_body
    {
      check_ptr($3);
      const char* stmt_str = NULL;
      int32_t str_len = 0;
      stmt_str = parse_ctx->stmt_str_ + @1.first_column;
      str_len = @3.last_column - @1.first_column + 1;
      $3->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($3->str_value_);
      $3->str_len_ = str_len;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SUB_PROC_DEF, 2, $1, $3);
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;

cursor_decl:
    CURSOR cursor_name opt_sp_param_list opt_return_type
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL_CURSOR, 4, $2, $3, $4, NULL);
    }
;

return_type:
  RETURN pl_outer_data_type
   {
    $$ = $2;
   }
;

opt_return_type:
  /*Empty*/ { $$ = NULL; }
  |  return_type
  {
    $$ = $1;
  }
;

cursor_sql_stmt:
    sql_stmt { $$ = $1; }
  | multi_left_brackets cursor_for_loop_sql multi_right_brackets
    {
      if ($1[0] != $3[0]) {
        YYERROR;
      } else {
        $$ = $2;
      }
    }
;

cursor_def:
    CURSOR cursor_name opt_sp_param_list opt_return_type IS cursor_sql_stmt
    {
      if ($6->children_[0]->type_ != T_SELECT) {
        obpl_oracle_yyerror(&@6, parse_ctx, "Syntax Error");
        YYERROR;
      }
      const char *stmt_str = parse_ctx->stmt_str_ + @6.first_column;
      int32_t str_len = @6.last_column - @6.first_column + 1;
      $6->raw_text_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($6->raw_text_);
      $6->text_len_ = str_len;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL_CURSOR, 4, $2, $3, $4, $6);
      copy_node_abs_location($$->stmt_loc_, @1);
    }
;

pl_inner_data_type:
    pl_inner_scalar_data_type { $$ = $1; }
  | pl_obj_access_ref
    {
      if ($1 != NULL && $1->type_ == T_SP_OBJ_ACCESS_REF && $1->num_child_ == 2 &&
          $1->children_[0] != NULL && $1->children_[1] == NULL &&
          $1->children_[0]->type_ == T_SP_ACCESS_NAME && $1->children_[0]->num_child_ == 3 &&
          $1->children_[0]->children_[0] == NULL && $1->children_[0]->children_[1] == NULL &&
          $1->children_[0]->children_[2] != NULL) {
        ParseNode *obj_node = $1->children_[0]->children_[2];
        if (nodename_equal(obj_node, "PLS_INTEGER", 11)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INTEGER_TYPE);
          $$->value_ = SP_PLS_INTEGER;
        } else if (nodename_equal(obj_node, "BINARY_INTEGER", 14)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INTEGER_TYPE);
          $$->value_ = SP_BINARY_INTEGER;
        } else if (nodename_equal(obj_node, "NATURAL", 7)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INTEGER_TYPE);
          $$->value_ = SP_NATURAL;
        } else if (nodename_equal(obj_node, "NATURALN", 8)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INTEGER_TYPE);
          $$->value_ = SP_NATURALN;
        } else if (nodename_equal(obj_node, "POSITIVE", 8)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INTEGER_TYPE);
          $$->value_ = SP_POSITIVE;
        } else if (nodename_equal(obj_node, "POSITIVEN", 9)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INTEGER_TYPE);
          $$->value_ = SP_POSITIVEN;
        } else if (nodename_equal(obj_node, "SIGNTYPE", 8)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INTEGER_TYPE);
          $$->value_ = SP_SIGNTYPE;
        } else if (nodename_equal(obj_node, "SIMPLE_INTEGER", 14)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INTEGER_TYPE);
          $$->value_ = SP_SIMPLE_INTEGER;
        } else if (nodename_equal(obj_node, "BOOL", 4) ||
                   nodename_equal(obj_node, "BOOLEAN", 7)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_TINYINT);
	      $$->int16_values_[0] = 1;
          $$->int16_values_[2] = 0;  // zerofill always false
        } else if (nodename_equal(obj_node, "TIMESTAMP", 9)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_TIMESTAMP_NANO);
        } else if (nodename_equal(obj_node, "BLOB", 4)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_LONGTEXT);
          $$->int32_values_[0] = 0;  /* need to be reset by accuracy */
          $$->int32_values_[1] = 1; /* is binary */
        } else if (nodename_equal(obj_node, "CLOB", 4)) {
          $$ = 0;
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LONGTEXT, 3, NULL, NULL, $$);
          $$->int32_values_[0] = 0;  /* need to be reset by accuracy */
          $$->int32_values_[1] = 0; /* is text */
        } else if (nodename_equal(obj_node, "CHARACTER", 9) ||
                   nodename_equal(obj_node, "CHAR", 4)) {
          $$ = 0;
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_CHAR, 3, NULL, NULL, $$);
          $$->int32_values_[0] = 1;
          $$->int32_values_[1] = 0; /* is char */
          $$->length_semantics_ = (0 & 0x03); /* length semantics type */
        } else if (nodename_equal(obj_node, "NCHAR", 5)) {
          $$ = 0;
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_NCHAR, 3, NULL, NULL, $$);
          $$->int32_values_[0] = 1;
          $$->int32_values_[1] = 0; /* is char */
          $$->length_semantics_ = 1; /* length semantics type */
        } else if (nodename_equal(obj_node, "NUMERIC", 7) ||
                   nodename_equal(obj_node, "INTEGER", 7) ||
                   nodename_equal(obj_node, "SMALLINT", 8) ||
                   nodename_equal(obj_node, "INT", 3) ||
                   nodename_equal(obj_node, "DECIMAL", 7)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER);
          $$->int16_values_[0] = 38;
          $$->int16_values_[1] = 0;
          $$->int16_values_[2] = NPT_PERC_SCALE;
        } else if (nodename_equal(obj_node, "UROWID", 6)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_UROWID);
          $$->int32_values_[0] = 4000;
        } else if (nodename_equal(obj_node, "SIMPLE_FLOAT", 12) ||
                   nodename_equal(obj_node, "BINARY_FLOAT", 12)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_FLOAT);
          $$->int16_values_[0] = -1;
          $$->int16_values_[1] = -85;  /* SCALE_UNKNOWN_YET */
          $$->int16_values_[2] = 0;
          $$->str_value_ = obj_node->str_value_;
          $$->str_len_ = obj_node->str_len_;
        } else if (nodename_equal(obj_node, "SIMPLE_DOUBLE", 13) ||
                   nodename_equal(obj_node, "BINARY_DOUBLE", 13)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_DOUBLE);
          $$->int16_values_[0] = -1;
          $$->int16_values_[1] = -85;  /* SCALE_UNKNOWN_YET */
          $$->int16_values_[2] = 0;
          $$->str_value_ = obj_node->str_value_;
          $$->str_len_ = obj_node->str_len_;
        } else if (nodename_equal(obj_node, "DATE", 4)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_DATETIME);
          $$->int16_values_[1] = 0;
          $$->int16_values_[2] = 0;
        } else if (nodename_equal(obj_node, "FLOAT", 5)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER_FLOAT);
          $$->int16_values_[0] = 126;
          $$->int16_values_[1] = -1;
        } else if (nodename_equal(obj_node, "NUMBER", 6)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER);
          $$->int16_values_[0] = -1;
          $$->int16_values_[1] = -85;  /* SCALE_UNKNOWN_YET */
          $$->int16_values_[2] = NPT_EMPTY;
        } else if (nodename_equal(obj_node, "REAL", 4)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER_FLOAT);
          $$->int16_values_[0] = 63;
          $$->int16_values_[1] = -1;
        } else if (nodename_equal(obj_node, "ROWID", 5)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_UROWID);
          $$->int32_values_[0] = 4000;
        } else {
          $$ = $1;
        }
      } else {
        $$ = $1;
      }
    }
  | pl_obj_access_ref '%' TYPE
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_TYPE, 1, $1);
      const char* type_str = parse_ctx->stmt_str_ + @1.first_column;
      int32_t type_len = @1.last_column - @1.first_column + 1;
      $$->str_value_ = parse_strndup(type_str, type_len, parse_ctx->mem_pool_);
      $$->str_len_ = type_len;
    }
  | pl_obj_access_ref '%' ROWTYPE
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ROWTYPE, 1, $1);
      const char* type_str = parse_ctx->stmt_str_ + @1.first_column;
      int32_t type_len = @1.last_column - @1.first_column + 1;
      $$->str_value_ = parse_strndup(type_str, type_len, parse_ctx->mem_pool_);
      $$->str_len_ = type_len;
    }
;

pl_outer_data_type:
    pl_outer_scalar_data_type
    {
      if (parse_ctx->is_inner_parse_) {
        const char* type_str = parse_ctx->stmt_str_ + @1.first_column;
        int32_t type_len = @1.last_column - @1.first_column + 1;
        $1->str_value_ = parse_strndup(type_str, type_len, parse_ctx->mem_pool_);
        check_ptr($1->str_value_);
        $1->str_len_ = type_len;
      }
      $$ = $1;
    }
  | pl_obj_access_ref
    {
      if ($1 != NULL && $1->type_ == T_SP_OBJ_ACCESS_REF && $1->num_child_ == 2 &&
          $1->children_[0] != NULL && $1->children_[1] == NULL &&
          $1->children_[0]->type_ == T_SP_ACCESS_NAME && $1->children_[0]->num_child_ == 3 &&
          $1->children_[0]->children_[0] == NULL && $1->children_[0]->children_[1] == NULL &&
          $1->children_[0]->children_[2] != NULL) {
        ParseNode *obj_node = $1->children_[0]->children_[2];
        if (nodename_equal(obj_node, "PLS_INTEGER", 11)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INTEGER_TYPE);
          $$->value_ = SP_PLS_INTEGER;
        } else if (nodename_equal(obj_node, "BINARY_INTEGER", 14)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INTEGER_TYPE);
          $$->value_ = SP_BINARY_INTEGER;
        } else if (nodename_equal(obj_node, "NATURAL", 7)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INTEGER_TYPE);
          $$->value_ = SP_NATURAL;
        } else if (nodename_equal(obj_node, "NATURALN", 8)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INTEGER_TYPE);
          $$->value_ = SP_NATURALN;
        } else if (nodename_equal(obj_node, "POSITIVE", 8)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INTEGER_TYPE);
          $$->value_ = SP_POSITIVE;
        } else if (nodename_equal(obj_node, "POSITIVEN", 9)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INTEGER_TYPE);
          $$->value_ = SP_POSITIVEN;
        } else if (nodename_equal(obj_node, "SIGNTYPE", 8)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INTEGER_TYPE);
          $$->value_ = SP_SIGNTYPE;
        } else if (nodename_equal(obj_node, "SIMPLE_INTEGER", 14)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INTEGER_TYPE);
          $$->value_ = SP_SIMPLE_INTEGER;
        } else if (nodename_equal(obj_node, "RAW", 3)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_RAW);
          $$->int32_values_[0] = 32767;
          $$->int32_values_[1] = 1;
        } else if (nodename_equal(obj_node, "BOOL", 4) ||
                   nodename_equal(obj_node, "BOOLEAN", 7)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_TINYINT);
	      $$->int16_values_[0] = 1;
          $$->int16_values_[2] = 0;  // zerofill always false
        } else if (nodename_equal(obj_node, "TIMESTAMP", 9)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_TIMESTAMP_NANO);
          $$->int16_values_[1] = 6;
          $$->int16_values_[2] = 0;
        } else if (nodename_equal(obj_node, "BLOB", 4)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_LONGTEXT);
          $$->int32_values_[0] = 0;  /* need to be reset by accuracy */
          $$->int32_values_[1] = 1; /* is binary */
        } else if (nodename_equal(obj_node, "CLOB", 4)) {
          $$ = 0;
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LONGTEXT, 3, NULL, NULL, $$);
          $$->int32_values_[0] = 0;  /* need to be reset by accuracy */
          $$->int32_values_[1] = 0; /* is text */
        } else if (nodename_equal(obj_node, "CHARACTER", 9) ||
                   nodename_equal(obj_node, "CHAR", 4)) {
          $$ = 0;
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_CHAR, 3, NULL, NULL, $$);
          $$->int32_values_[0] = DEFAULT_STR_LENGTH;
          $$->int32_values_[1] = 0; /* is char */
          $$->length_semantics_ = (2 & 0x03); /* length semantics type */
        } else if (nodename_equal(obj_node, "NCHAR", 5)) {
          $$ = 0;
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_NCHAR, 3, NULL, NULL, $$);
          $$->int32_values_[0] = DEFAULT_STR_LENGTH;
          $$->int32_values_[1] = 0; /* is char */
          $$->length_semantics_ = 1; /* length semantics type */
        } else if (nodename_equal(obj_node, "NUMERIC", 7) ||
                   nodename_equal(obj_node, "INTEGER", 7) ||
                   nodename_equal(obj_node, "SMALLINT", 8) ||
                   nodename_equal(obj_node, "INT", 3)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER);
          $$->int16_values_[0] = 38;
          $$->int16_values_[1] = 0;
          $$->int16_values_[2] = NPT_PERC_SCALE;
        } else if (nodename_equal(obj_node, "UROWID", 6)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_UROWID);
          $$->int32_values_[0] = DEFAULT_STR_LENGTH;
        } else if (nodename_equal(obj_node, "BINARY_FLOAT", 12) ||
                   nodename_equal(obj_node, "SIMPLE_FLOAT", 12)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_FLOAT);
          $$->int16_values_[0] = -1;
          $$->int16_values_[1] = -85;  /* SCALE_UNKNOWN_YET */
          $$->int16_values_[2] = 0;
        } else if (nodename_equal(obj_node, "BINARY_DOUBLE", 13) ||
                   nodename_equal(obj_node, "SIMPLE_DOUBLE", 13)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_DOUBLE);
          $$->int16_values_[0] = -1;
          $$->int16_values_[1] = -85;  /* SCALE_UNKNOWN_YET */
          $$->int16_values_[2] = 0;
        } else if (nodename_equal(obj_node, "NVARCHAR2", 9)) {
          $$ = 0;
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_NVARCHAR2, 3, NULL, NULL, $$);
          $$->int32_values_[0] = DEFAULT_STR_LENGTH;
          $$->int32_values_[1] = 0; /* is char */
          $$->length_semantics_ = 1; /* length semantics type */
        } else if (nodename_equal(obj_node, "DATE", 4)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_DATETIME);
          $$->int16_values_[1] = 0;
          $$->int16_values_[2] = 0;
        } else if (nodename_equal(obj_node, "FLOAT", 5)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER_FLOAT);
          $$->int16_values_[0] = 126;
          $$->int16_values_[1] = -1;
        } else if (nodename_equal(obj_node, "DECIMAL", 7) ||
                   nodename_equal(obj_node, "NUMBER", 6)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER);
          $$->int16_values_[0] = -1;
          $$->int16_values_[1] = -85;  /* SCALE_UNKNOWN_YET */
          $$->int16_values_[2] = NPT_EMPTY;
        } else if (nodename_equal(obj_node, "REAL", 4)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER_FLOAT);
          $$->int16_values_[0] = 63;
          $$->int16_values_[1] = -1;
        } else if (nodename_equal(obj_node, "ROWID", 5)) {
          malloc_terminal_node($$, parse_ctx->mem_pool_, T_UROWID);
          $$->int32_values_[0] = DEFAULT_STR_LENGTH;
        } else if (nodename_equal(obj_node, "VARCHAR", 7) ||
                   nodename_equal(obj_node, "VARCHAR2", 8)) {
          $$ = 0;
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_VARCHAR, 3, NULL, NULL, $$);
          $$->int32_values_[0] = DEFAULT_STR_LENGTH;
          $$->int32_values_[1] = 0; /* is char */
          $$->length_semantics_ = (2 & 0x03); /* length semantics type */
        } else {
          $$ = $1;
        }
      } else {
        $$ = $1;
      }
      if (parse_ctx->is_inner_parse_) {
        const char* type_str = parse_ctx->stmt_str_ + @1.first_column;
        int32_t type_len = @1.last_column - @1.first_column + 1;
        $$->str_value_ = parse_strndup(type_str, type_len, parse_ctx->mem_pool_);
        check_ptr($$->str_value_);
        $$->str_len_ = type_len;
      }
    }
  | pl_obj_access_ref '%' TYPE
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_TYPE, 1, $1);
      const char* type_str = parse_ctx->stmt_str_ + @1.first_column;
      int32_t type_len = @1.last_column - @1.first_column + 1;
      $$->str_value_ = parse_strndup(type_str, type_len, parse_ctx->mem_pool_);
      $$->str_len_ = type_len;
    }
  | pl_obj_access_ref '%' ROWTYPE
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ROWTYPE, 1, $1);
      const char* type_str = parse_ctx->stmt_str_ + @1.first_column;
      int32_t type_len = @1.last_column - @1.first_column + 1;
      $$->str_value_ = parse_strndup(type_str, type_len, parse_ctx->mem_pool_);
      $$->str_len_ = type_len;
    }
;

opt_default:
    /*Empty*/ { $$ = NULL; }
  | default { $$ = $1; }
;


default:
    DEFAULT default_expr
    {
      if (NULL == $2) {
        YYERROR;
      }
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL_DEFAULT, 1, $2);
      // 拷贝EXPR语句
      const char *expr_str = parse_ctx->stmt_str_ + @2.first_column;
      int32_t expr_str_len = @2.last_column - @2.first_column + 1;
      $$->str_value_ = parse_strndup(expr_str, expr_str_len,parse_ctx->mem_pool_);
      check_ptr($$->str_value_);
      $$->str_len_ = expr_str_len;
      if (NULL != $2)
      copy_node_abs_location($2->stmt_loc_, @2);
    }
  | ASSIGN_OPERATOR default_expr
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
      if (NULL != $2)
      copy_node_abs_location($2->stmt_loc_, @2);
    }
;

expr:
  {
    /*!
     * same as expr in sql rule, and terminate when read ';', INTO, BULK, USING, WHEN etc
     */
    do_parse_sql_bool_expr_rule_version_2($$, parse_ctx, 15,
      INTO, BULK, USING, WHEN, THEN, ';', LOOP, LIMIT, ',', END_KEY, END_P, RANGE_OPERATOR, ')', RETURNING, RETURN);
  }
;

bool_expr:
  {
    do_parse_sql_bool_expr_rule_version_2($$, parse_ctx, 6, WHEN, THEN, LOOP, ',', ')', ';');
  }
;

default_expr:
  {
    do_parse_sql_bool_expr_rule_version_2($$, parse_ctx, 3, ',', ')', ';');
  }
;

return_expr:
  {
    do_parse_sql_bool_expr_rule_version_2($$, parse_ctx, 1, ';');
  }
;

basic_loop_stmt:
	LOOP pl_lang_stmt_list END_KEY LOOP opt_label
	{
	  ParseNode *proc_stmts = NULL;
	  merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $2);
	  if ($5 != NULL) {
	    ParseNode *loop_stmt = NULL;
	    malloc_non_terminal_node(loop_stmt, parse_ctx->mem_pool_, T_SP_LOOP, 1, proc_stmts);
		malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LABELED_CONTROL, 3, NULL, loop_stmt, $5);
	  } else {
	    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LOOP, 1, proc_stmts);
	  }
	}
;

while_loop_stmt:
    WHILE bool_expr LOOP pl_lang_stmt_list END_KEY LOOP opt_label
    {
      if (NULL == $2) YYERROR;
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $4);
      if ($7 != NULL) {
        ParseNode *while_stmt = NULL;
        malloc_non_terminal_node(while_stmt, parse_ctx->mem_pool_, T_SP_WHILE, 2, $2, proc_stmts);
        malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LABELED_CONTROL, 3, NULL, while_stmt, $7);
      } else {
        malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_WHILE, 2, $2, proc_stmts);
      }
      if (NULL != $2)
      copy_node_abs_location($2->stmt_loc_, @2);
    }
;

for_loop_stmt:
  FOR identifier IN opt_reverse lower_bound RANGE_OPERATOR upper_bound LOOP pl_lang_stmt_list END_KEY LOOP opt_label
  {
    if (NULL == $5 || NULL == $7) {
      YYERROR;
    }
    ParseNode *proc_stmts = NULL;
    merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $9);
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_FOR_LOOP, 5, $2, $5, $7, proc_stmts, $12);
    $$->value_ = $4[0];
  }
;

cursor_for_loop_stmt:
    FOR identifier IN opt_reverse for_expr LOOP pl_lang_stmt_list END_KEY LOOP opt_label
    {
      // 这里不加opt_reverse会导致与for_loop_stmt规约冲突
      // for_expr不能替换成IDENT，同样会与for_loop_stmt规约冲突
      if (1 == $4[0]
          || NULL == $5
          || (T_OBJ_ACCESS_REF != $5->type_)
          || $5->num_child_ < 1
          || NULL == $5->children_[0]
          || (T_IDENT != $5->children_[0]->type_ && T_FUN_SYS != $5->children_[0]->type_)) {
        YYERROR;
      }
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $7);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_CURSOR_FOR_LOOP, 4, $2, $5, proc_stmts, $10);
      if (NULL != $5)
      copy_node_abs_location($5->stmt_loc_, @5);
    }
  | FOR identifier IN multi_left_brackets cursor_for_loop_sql multi_right_brackets LOOP pl_lang_stmt_list END_KEY LOOP opt_label
    {
      if ($4[0] != $6[0]) {
        YYERROR;
      } else {
        // 拷贝SQL语句
        const char *stmt_str = parse_ctx->stmt_str_ + @5.first_column;
        int32_t str_len = @5.last_column - @5.first_column + 1;
        $5->raw_text_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
        check_ptr($5->raw_text_);
        $5->text_len_ = str_len;
        ParseNode *proc_stmts = NULL;
        merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $8);
        malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_CURSOR_FOR_LOOP, 4, $2, $5, proc_stmts, $11);
      }
    }
;

multi_left_brackets:
'('
{
  $$[0] = 1;
}
| multi_left_brackets '('
{
  $$[0] = $1[0] + 1;
}
;

multi_right_brackets:
')'
{
  $$[0] = 1;
}
| multi_right_brackets ')'
{
  $$[0] = $1[0] + 1;
}
;

forall_stmt:
  FORALL identifier IN bound_clause opt_save_exception forall_sql_stmt
  {
    ParseNode *proc_stmts = NULL;
    merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $6);
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_FORALL, 3, $2, $4, proc_stmts);
    $$->value_ = $5[0];
  }
;

forall_sql_stmt:
  sql_stmt
  {
    $$ = $1;
    const char *stmt_str = parse_ctx->stmt_str_ + @1.first_column;
    int32_t str_len = @1.last_column - @1.first_column + 1;
    $1->raw_text_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
    check_ptr($1->raw_text_);
    $1->text_len_ = str_len;
    copy_node_abs_location($$->stmt_loc_, @1);
  }
| execute_immediate_stmt { $$ = $1; }
;

opt_reverse:
    /*Empty*/ { $$[0] = 0; }
  | REVERSE { $$[0] = 1; }
;

for_expr:
  {
    do_parse_sql_bool_expr_rule_version_2(
      $$, parse_ctx, 11, ';', RANGE_OPERATOR, LOOP, INSERT, UPDATE, DELETE, SAVE, NOT, EXECUTE, AND, SQL_KEYWORD);
  }
;

lower_bound:
  for_expr {
    $$ = $1;
      if (NULL != $1)
    copy_node_abs_location($1->stmt_loc_, @1);
  }
;

upper_bound:
  for_expr {
    $$ = $1;
      if (NULL != $1)
    copy_node_abs_location($1->stmt_loc_, @1);
  }
;

opt_save_exception:
    /*EMPTY*/ { $$[0] = 0; }
  | SAVE EXCEPTIONS { $$[0] = 1; }
;

/*!
 * lower_bound .. upper_bound ------------------------------------|
 *                       | -> BETWEEN lower_bound AND upper_bound |
 * INDICES OF collection | ---------------------------------------|
 * INDICES OF index_collection -----------------------------------|
 */
opt_between_bound:
    /*EMPTY*/ { $$ = NULL; }
  | BETWEEN lower_bound AND upper_bound
    {
      if (NULL == $2 || NULL == $4) {
        YYERROR;
      }
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_BOUND_CLAUSE, 2, $2, $4);
    }
;

bound_clause:
    lower_bound RANGE_OPERATOR upper_bound
    {
      if (NULL == $1 || NULL == $3) {
        YYERROR;
      }
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_BOUND_CLAUSE, 2, $1, $3);
    }
  | INDICES OF pl_obj_access_ref opt_between_bound
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_INDICES_OF_CALUSE, 2, $3, $4);
    }
  | VALUES OF pl_obj_access_ref
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_VALUES_OF_CALUSE, 1, $3);
    }
;

cursor_for_loop_sql:
  sql_keyword
  {
    ParseNode *sql_stmt = NULL;
    do_parse_sql_stmt(sql_stmt, parse_ctx, @1.first_column, @1.last_column, 1, ')');
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
    if ($$->children_[0]->type_ != T_SELECT) {
      obpl_oracle_yyerror(&@$, parse_ctx, "Syntax Error");
      YYERROR;
    } else {
      $$->str_value_ = sql_stmt->str_value_;
      $$->str_len_ = sql_stmt->str_len_;
    }
  }
;

labeled_pl_lang_stmt:
    pl_lang_stmt
    {
      if (T_SP_LABELED_CONTROL == $1->type_ || T_SP_LABELED_BLOCK == $1->type_) {
        $$ = $1->children_[1];
      } else {
        $$ = $1;
      }
    }
    | label_list pl_lang_stmt
    {
      ParseNode *label_node = NULL;
      merge_nodes(label_node, parse_ctx->mem_pool_, T_LABEL_LIST, $1);
      if (T_SP_LABELED_CONTROL == $2->type_ || T_SP_LABELED_BLOCK == $2->type_) {
        $2->children_[0] = label_node;
        $$ = $2;
      } else {
        if (T_SP_BLOCK_CONTENT == $2->type_ ) {
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LABELED_BLOCK, 3, label_node, $2, NULL);
        } else {
          malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LABELED_CONTROL, 3, label_node, $2, NULL);
        }
      }
    }
;

label_list:
    label_def
    {
      $$ = $1;
    }
  | label_list label_def
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
;

label_def:
  LABEL_LEFT label_name LABEL_RIGHT
  {
    $$ = $2;
  }
;

return_stmt:
    RETURN return_expr
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_RETURN, 1, $2);
      if (NULL != $2) {
        copy_node_abs_location($2->stmt_loc_, @2);
      }
    }
;

goto_stmt:
    GOTO label_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_GOTO, 1, $2);
    }
;

continue_stmt:
    CONTINUE WHEN bool_expr
    {
      if (NULL == $3) YYERROR;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ITERATE, 2, NULL, $3);
    }
  | CONTINUE label_name WHEN bool_expr
    {
      if (NULL == $4) YYERROR;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ITERATE, 2, $2, $4);
      if (NULL != $4)
      copy_node_abs_location($4->stmt_loc_, @4);
    }
  | CONTINUE label_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ITERATE, 2, $2, NULL);
    }
  /*为了解决reduce冲突，在inner_call_stmt中的pl_obj_access_ref中解决,这里会先注释掉，并不表示不支持
  | CONTINUE
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ITERATE, 2, NULL, NULL);
    }
  */
;

exit_stmt:
    EXIT opt_label WHEN bool_expr
    {
      if (NULL == $4) YYERROR;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LEAVE, 2, $2, $4);
      if (NULL != $4)
      copy_node_abs_location($4->stmt_loc_, @4);
    }
  /*
  | EXIT
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LEAVE, 2, NULL, NULL);
    }
  */
  | EXIT label_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_LEAVE, 2, $2, NULL);
    }
;

null_stmt:
    NULLX
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_NULL);
    }
;

pipe_row_stmt:
  PIPE ROW '(' expr ')'
  {
    if (NULL == $4) YYERROR;
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PIPE_ROW, 1, $4);
      if (NULL != $4)
    copy_node_abs_location($4->stmt_loc_, @4);
  }
;

pragma_stmt:
    inline_pragma
    {
      $$ = $1;
    }
  | exception_init_pragma
    {
      $$ = $1;
    }
  | udf_pragma
    {
      $$ = $1;
    }
  | serially_reusable_pragma
    {
      $$ = $1;
    }
  | restrict_references_pragma
    {
      $$ = $1;
    }
  | autonomous_transaction_pragma
    {
      $$ = $1;
    }
  | interface_pragma
    {
      $$ = $1;
    }
;

inline_pragma:
  PRAGMA INLINE '(' identifier ',' STRING ')'
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PRAGMA_INLINE, 2, $4, $6);
  }
;

exception_init_pragma:
  PRAGMA EXCEPTION_INIT '(' exception_init_param_list ')'
  {
     ParseNode *param_list = NULL;
     merge_nodes(param_list, parse_ctx->mem_pool_, T_SP_INIT_PRAGMA_PARAM_LIST, $4);
     malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_INIT_PRAGMA, 1, param_list);
  }
;

exception_init_param_list:
    exception_init_param_list ',' exception_init_param
    {
       malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
  | exception_init_param { $$ = $1;}
;
exception_init_param:
    exception_name { $$ = $1; }
  | error_code { $$ = $1; }
;

udf_pragma:
  PRAGMA UDF
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_PRAGMA_UDF);
  }
;

serially_reusable_pragma:
  PRAGMA SERIALLY_REUSABLE
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_PRAGMA_SERIALLY_REUSABLE);
  }
;

restrict_references_pragma:
  PRAGMA RESTRICT_REFERENCES '(' default_or_string ',' assert_list ')'
  {
    ParseNode *assert_list_node = NULL;
    merge_nodes(assert_list_node, parse_ctx->mem_pool_, T_SP_ASSERT_ITEM_LIST, $6);
    malloc_non_terminal_node(
      $$, parse_ctx->mem_pool_, T_SP_PRAGMA_RESTRICT_REFERENCE, 2, $4, assert_list_node);
  }
;

autonomous_transaction_pragma:
  PRAGMA AUTONOMOUS_TRANSACTION
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_PRAGMA_AUTONOMOUS_TRANSACTION);
  }
;

interface_pragma:
  PRAGMA INTERFACE '(' C ',' identifier ')'
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PRAGMA_INTERFACE, 1, $6);
  }
;

error_code:
    INTNUM
    {
      $$ = $1;
      $$->param_num_ = 1;
    }
  | '-' INTNUM
    {
      $2->value_ = -$2->value_;
      $$ = $2;
      $$->param_num_ = 1;
    }
  | STRING
    {
      $$ = $1;
      $$->param_num_ = 1;
    }
;

exception_handler:
    WHEN exception_list THEN pl_lang_stmt_list
    {
       ParseNode *hcond_list = NULL;
       ParseNode *proc_stmts = NULL;
       merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, $4);
       merge_nodes(hcond_list, parse_ctx->mem_pool_, T_SP_HCOND_LIST, $2);
       malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL_HANDLER, 2, hcond_list, proc_stmts);
       $$->value_ = SP_HANDLER_TYPE_EXIT;
    }
;

exception_item:
  pl_access_name
  {
    if (T_SP_ACCESS_NAME == $1->type_
        && 3 == $1->num_child_
        && NULL == $1->children_[0]
        && NULL == $1->children_[1]
        && $1->children_[2] != NULL) {
      ParseNode *obj_node = $1->children_[2];
      if (nodename_equal(obj_node, "OTHERS", 6)) {
        malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_EXCEPTION_OTHERS);
      } else {
        $$ = $1;
      }
    } else {
      $$ = $1;
    }
  }
;

exception_list:
    exception_item { $$ = $1; }
  | exception_list OR exception_item
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
;

drop_procedure_stmt:
    DROP PROCEDURE opt_if_exists pl_schema_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DROP, 1, $4);
      $$->value_ = $3[0];
    }
;

drop_function_stmt:
    DROP FUNCTION opt_if_exists pl_schema_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SF_DROP, 1, $4);
      $$->value_ = $3[0];
    }
;

opt_if_exists:
    /*Empty*/ { $$[0] = 0; }
  | IF EXISTS { $$[0] = 1; }
;

drop_trigger_stmt:
    DROP TRIGGER pl_schema_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_DROP, 1, $3);
      $$->value_ = 0; // if exists
    }
;

record_member_list:
    record_member { $$ = $1; }
  | record_member_list ',' record_member
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
;

record_member:
    field_name pl_inner_data_type opt_not_null opt_record_member_default
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_RECORD_MEMBER, 3, $1, $2, $4);
      $$->int32_values_[0] = $3[0];
      $$->int32_values_[1] = 0;
    }
;

opt_record_member_default:
  /*empty*/ { $$=NULL; }
  | ASSIGN_OPERATOR pl_right_value
  {
    $$ = $2;
  }
  | DEFAULT pl_right_value
  {
    $$ = $2;
  }
;

type_def:
    ref_cursor_type_def { $$ = $1; }
  | record_type_def { $$ = $1; }
  | collection_type_def { $$ = $1; }
;

subtype_def:
  SUBTYPE type_name IS basetype_of_subtype opt_not_null
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DECL_USER_SUBTYPE, 2, $2, $4);
    $$->value_ = $5[0];
  }
;

basetype_of_subtype:
    pl_inner_data_type opt_subtype_constraint
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_USER_SUBTYPE_BASETYPE, 2, $1, $2);
    }
;

opt_subtype_constraint:
    /*EMPTY*/ { $$ = NULL; }
  | subtype_range { $$ = $1; }
;

subtype_range:
  RANGE lower_bound RANGE_OPERATOR upper_bound
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_USER_SUBTYPE_RANGE, 2, $2, $4);
  }
;

ref_cursor_type_def:
  TYPE type_name IS REF CURSOR opt_return_type
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_REF_CURSOR_TYPE, 2, $2, $6);
  }
;

record_type_def:
    TYPE type_name IS RECORD '(' record_member_list ')'
    {
      ParseNode *record_member_list = NULL;
      merge_nodes(record_member_list, parse_ctx->mem_pool_, T_RECORD_MEMBER_LIST, $6);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_RECORD_TYPE, 2, $2, record_member_list);
    }
;

collection_type_def:
    TYPE type_name IS coll_type_def
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_COLLECTION_TYPE, 2, $2, $4);
    }
;

coll_type_def:
    assoc_array_type_def { $$ = $1; }
  | nested_table_type_def { $$ = $1; }
  | varray_type_def { $$ = $1; }
;

nested_table_type_def:
    TABLE OF pl_inner_data_type opt_not_null
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_NESTED_TABLE_TYPE, 1, $3);
      $$->value_ = $4[0];
    }
;

assoc_array_type_def:
    TABLE OF pl_inner_data_type opt_not_null INDEX BY index_type
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ASSOC_ARRAY_TYPE, 2, $3, $7);
      $$->value_ = $4[0];
    }

;

index_type:
  pl_inner_data_type { $$ = $1; }
;

opt_not_null:
    /*Empty*/ { $$[0] = 0; }
  | NULLX     { $$[0] = -1; }
  | NOT NULLX { $$[0] = 1; }
;

varray_type_def:
  pre_varray '(' expr ')' OF pl_inner_data_type opt_not_null
  {
    if ($3->type_ != T_INT) {
      YY_NON_INT_LITERAL("non-integral numeric literal string is inappropriate in this context\n");
    }
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_VARRAY_TYPE, 2, $6, $3);
    $$->value_ = $7[0];
  }
;

pre_varray:
    VARRAY { $$ = NULL; }
  | opt_varying ARRAY { $$ = NULL; }
;

opt_varying:
    /*EMPTY*/ { $$ = NULL; }
  | VARYING { $$ = NULL; }
;

pl_inner_scalar_data_type:
/*
    int_type_i opt_int_precision
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER);
      $$->int16_values_[0] = $2->int16_values_[0];
      $$->int16_values_[1] = $2->int16_values_[1];
      $$->int16_values_[2] = $2->int16_values_[2];
    }
*/
  NUMERIC number_precision
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER);
      $$->int16_values_[0] = $2->int16_values_[0];
      $$->int16_values_[1] = $2->int16_values_[1];
      $$->int16_values_[2] = $2->int16_values_[2];
      if (NULL != $2->str_value_) {
        $$->str_value_ = $2->str_value_;
        $$->str_len_ = $2->str_len_;
      }
    }
  /*
  | NUMBER
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER);
      $$->int16_values_[0] = -1;
      $$->int16_values_[1] = -85;
      $$->int16_values_[2] = NPT_EMPTY;
    }
  */
  | NUMBER number_precision
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER);
      $$->int16_values_[0] = $2->int16_values_[0];
      $$->int16_values_[1] = $2->int16_values_[1];
      $$->int16_values_[2] = $2->int16_values_[2];
      $$->param_num_ = $2->param_num_;
      if (NULL != $2->str_value_) {
        $$->str_value_ = $2->str_value_;
        $$->str_len_ = $2->str_len_;
      }
    }
  | FLOAT '(' INTNUM ')'
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER_FLOAT);
    $$->int16_values_[0] = $3->value_;
    $$->int16_values_[1] = -1;
  }
  | FLOAT '(' precision_decimal_num ')'
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER_FLOAT);
    $$->int16_values_[0] = $3->int16_values_[0];
    if (0 == $3->int16_values_[1]) {
      $$->int16_values_[1] = -1; /* same as intnum */
    } else {
      $$->int16_values_[1] = $3->int16_values_[1];
    }
    if (NULL != $3->str_value_) {
      $$->str_value_ = $3->str_value_;
      $$->str_len_ = $3->str_len_;
    }
  }
  | FLOAT '(' ')'
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER_FLOAT);
    $$->int16_values_[0] = 0;
    $$->int16_values_[1] = -1;
  }
/*
  | FLOAT
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER_FLOAT);
    $$->int16_values_[0] = 126;
    $$->int16_values_[1] = -1;
  }
  | REAL
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER_FLOAT);
    $$->int16_values_[0] = 63;
    $$->int16_values_[1] = -1;
  }
*/
  | REAL '(' INTNUM ')'
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER_FLOAT);
    $$->int16_values_[0] = $3->value_;
    $$->int16_values_[1] = -1;
  }
  | REAL '(' precision_decimal_num ')'
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER_FLOAT);
    $$->int16_values_[0] = $3->int16_values_[0];
    if (0 == $3->int16_values_[1]) {
      $$->int16_values_[1] = -1; /* same as intnum */
    } else {
      $$->int16_values_[1] = $3->int16_values_[1];
    }
    if (NULL != $3->str_value_) {
      $$->str_value_ = $3->str_value_;
      $$->str_len_ = $3->str_len_;
    }
  }
  /*为了解决reduce冲突，统一pl_inner_data_type/pl_outer_data_type中的pl_obj_access_ref解决，这里会先注释掉，并不表示不支持
  | double_type_i
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, $1[0]);
    $$->int16_values_[0] = -1;
    $$->int16_values_[1] = -85;
    $$->int16_values_[2] = 0;
  }
  | TIMESTAMP
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_TIMESTAMP_NANO);
      $$->int16_values_[1] = $2[0];
      $$->int16_values_[2] = $2[1];
    }
  */
  | TIMESTAMP '(' INTNUM ')'
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_TIMESTAMP_NANO);
      $$->int16_values_[1] = $3->value_;
      $$->int16_values_[2] = 1;
    }
	| TIMESTAMP '(' precision_decimal_num ')'
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_TIMESTAMP_NANO);
      $$->int16_values_[1] = $3->int16_values_[0];
      if (0 == $3->int16_values_[1]) {
        $$->int16_values_[2] = 1; /* same as intnum */
      } else {
        $$->int16_values_[2] = $3->int16_values_[1];
      }
      if (NULL != $3->str_value_) {
        $$->str_value_ = $3->str_value_;
        $$->str_len_ = $3->str_len_;
      }
    }
  | TIMESTAMP opt_datetime WITH TIME ZONE
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_TIMESTAMP_TZ);
      $$->int16_values_[1] = $2->int16_values_[0];
      if (0 == $2->int16_values_[1]) {
        $$->int16_values_[2] = 1; /* same as intnum */
      } else {
        $$->int16_values_[2] = $2->int16_values_[1];
      }
      if (NULL != $2->str_value_) {
        $$->str_value_ = $2->str_value_;
        $$->str_len_ = $2->str_len_;
      }
    }
  | TIMESTAMP opt_datetime WITH LOCAL TIME ZONE
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_TIMESTAMP_LTZ);
      $$->int16_values_[1] = $2->int16_values_[0];
      $$->int16_values_[2] = $2->int16_values_[1];
      if (NULL != $2->str_value_) {
        $$->str_value_ = $2->str_value_;
        $$->str_len_ = $2->str_len_;
      }
    }
  /*
  | datetime_type_i
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_DATETIME);
      $$->int16_values_[1] = 0;
      $$->int16_values_[2] = 0;
    }
  */
/*为了解决reduce冲突，统一pl_inner_data_type/pl_outer_data_type中的pl_obj_access_ref解决，这里会先注释掉，并不表示不支持*/
//  | CHARACTER
//    {
//      $$ = 0;
//      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_CHAR, 3, NULL, NULL, $$);
//      $$->int32_values_[0] = 1;
//      $$->int32_values_[1] = 0; /* is char */
//      $$->length_semantics_ = (0 & 0x03); /* length semantics type */
//    }
  | CHARACTER collation
    {
      $$ = 0;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_CHAR, 3, NULL, $2, $$);
      $$->int32_values_[0] = 1;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = (0 & 0x03); /* length semantics type */
    }
  | CHARACTER charset_key charset_name opt_collation
    {
      $$ = 0;
      (void)($2);
      ParseNode *charset_node = NULL;
      malloc_terminal_node(charset_node, parse_ctx->mem_pool_, T_CHARSET);
      charset_node->str_value_ = $3->str_value_;
      charset_node->str_len_ = $3->str_len_;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_CHAR, 3, charset_node, $4, $$);
      $$->int32_values_[0] = 1;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = (0 & 0x03); /* length semantics type */
    }
  | CHARACTER BINARY opt_charset opt_collation
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_BINARY);
      $$->value_ = 1;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_CHAR, 3, $3, $4, $$);
      $$->int32_values_[0] = 1;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = (0 & 0x03); /* length semantics type */
    }
  | CHARACTER string_length_i opt_binary opt_charset opt_collation
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_CHAR, 3, $4, $5, $3);
      $$->int32_values_[0] = $2[0];
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = ($2[2] & 0x03); /* length semantics type */
    }
  | RAW '(' INTNUM ')'
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_RAW);
      $$->int32_values_[0] = $3->value_;
      $$->int32_values_[1] = 1;
    }
  /*为了解决reduce冲突，统一pl_inner_data_type/pl_outer_data_type中的pl_obj_access_ref解决，这里会先注释掉，并不表示不支持*/
//  | NCHAR
//    {
//      $$ = 0;
//      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_NCHAR, 3, NULL, NULL, $$);
//      $$->int32_values_[0] = 1;
//      $$->int32_values_[1] = 0; /* is char */
//      $$->length_semantics_ = 1; /* length semantics type */
//    }
  | NCHAR collation
    {
      $$ = 0;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_NCHAR, 3, NULL, $2, $$);
      $$->int32_values_[0] = 1;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = 1; /* length semantics type */
    }
  | NCHAR charset_key charset_name opt_collation
    {
      $$ = 0;
      (void)($2);
      ParseNode *charset_node = NULL;
      malloc_terminal_node(charset_node, parse_ctx->mem_pool_, T_CHARSET);
      charset_node->str_value_ = $3->str_value_;
      charset_node->str_len_ = $3->str_len_;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_NCHAR, 3, charset_node, $4, $$);
      $$->int32_values_[0] = 1;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = 1; /* length semantics type */
    }
  | NCHAR BINARY opt_charset opt_collation
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_BINARY);
      $$->value_ = 1;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_NCHAR, 3, $3, $4, $$);
      $$->int32_values_[0] = 1;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = 1; /* length semantics type */
    }
  | NCHAR nstring_length_i opt_binary opt_charset opt_collation
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_NCHAR, 3, $4, $5, $3);
      $$->int32_values_[0] = $2[0];
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = 1; /* length semantics type */
    }
  | VARCHAR string_length_i opt_binary opt_charset opt_collation
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_VARCHAR, 3, $4, $5, $3);
      $$->int32_values_[0] = $2[0];
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = ($2[2] & 0x03); /* length semantics type */
    }
  | VARCHAR2 string_length_i opt_binary opt_charset opt_collation
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_VARCHAR, 3, $4, $5, $3);
      $$->int32_values_[0] = $2[0];
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = ($2[2] & 0x03); /* length semantics type */
    }
  | NVARCHAR2 nstring_length_i opt_binary opt_charset opt_collation
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_NVARCHAR2, 3, $4, $5, $3);
      $$->int32_values_[0] = $2[0];
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = 1; /* length semantics type */
    }
  | INTERVAL YEAR opt_interval_leading TO MONTH
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_INTERVAL_YM);
      $$->int16_values_[0] = $3->int16_values_[0];
      $$->int16_values_[1] = $3->int16_values_[1];
      if (NULL != $3->str_value_) {
        $$->str_value_ = $3->str_value_;
        $$->str_len_ = $3->str_len_;
      }
    }
  | INTERVAL DAY opt_interval_leading TO SECOND opt_datetime
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_INTERVAL_DS);
      $$->int16_values_[0] = $3->int16_values_[0];
      $$->int16_values_[1] = $3->int16_values_[1];
      $$->int16_values_[2] = $6->int16_values_[0];
      $$->int16_values_[3] = $6->int16_values_[1];
      if (NULL != $3->str_value_) {
        $$->str_value_ = $3->str_value_;
        $$->str_len_ = $3->str_len_;
      }
    }
/*为了解决reduce冲突，统一pl_inner_data_type/pl_outer_data_type中的pl_obj_access_ref解决，这里会先注释掉，并不表示不支持*/
//  | BLOB /* oracle 12c only support collate @hanhui TODO */
//    {
//      malloc_terminal_node($$, parse_ctx->mem_pool_, T_LONGTEXT);
//      $$->int32_values_[0] = 0;  /* need to be reset by accuracy */
//      $$->int32_values_[1] = 1; /* is binary */
//    }
//  | CLOB /* oracle 12c only support collate @hanhui TODO */
//    {
//      $$ = 0;
//      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LONGTEXT, 3, NULL, NULL, $$);
//      $$->int32_values_[0] = 0;  /* need to be reset by accuracy */
//      $$->int32_values_[1] = 0; /* is text */
//    }
  | CLOB collation/* oracle 12c only support collate @hanhui TODO */
    {
      $$ = 0;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LONGTEXT, 3, NULL, $2, $$);
      $$->int32_values_[0] = 0;  /* need to be reset by accuracy */
      $$->int32_values_[1] = 0; /* is text */
    }
  | CLOB charset_key charset_name opt_collation/* oracle 12c only support collate @hanhui TODO */
    {
      $$ = 0;
      (void)($2);
      ParseNode *charset_node = NULL;
      malloc_terminal_node(charset_node, parse_ctx->mem_pool_, T_CHARSET);
      charset_node->str_value_ = $3->str_value_;
      charset_node->str_len_ = $3->str_len_;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LONGTEXT, 3, charset_node, $4, $$);
      $$->int32_values_[0] = 0;  /* need to be reset by accuracy */
      $$->int32_values_[1] = 0; /* is text */
    }
  | CLOB BINARY opt_charset opt_collation/* oracle 12c only support collate @hanhui TODO */
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_BINARY);
      $$->value_ = 1;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LONGTEXT, 3, $3, $4, $$);
      $$->int32_values_[0] = 0;  /* need to be reset by accuracy */
      $$->int32_values_[1] = 0; /* is text */
    }
/*为了解决reduce冲突，统一pl_inner_data_type/pl_outer_data_type中的pl_obj_access_ref解决，这里会先注释掉，并不表示不支持
  | UROWID
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_UROWID);
      $$->int32_values_[0] = 4000;
    }
*/
  | UROWID urowid_length_i
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_UROWID);
      $$->int32_values_[0] = $2[0];
    }
  | ROWID urowid_length_i
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_UROWID);
      $$->int32_values_[0] = $2[0];
    }
/*
  | pl_common_scalar_data_type
    {
      $$ = $1;
    }
*/
;

/*为了解决reduce冲突，统一pl_inner_data_type/pl_outer_data_type中的pl_obj_access_ref解决，这里会先注释掉，并不表示不支持
pl_inner_simple_not_null_type:
  simple_type_i
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, $1[0]);
    $$->int16_values_[0] = -1;
    $$->int16_values_[1] = -85;
    $$->int16_values_[2] = 0;
  }
;
*/

precision_decimal_num:
DECIMAL_VAL {
  $$ = $1;
  bool int_flag = false;
  int32_t value = 0;
  CHECK_ASSIGN_ZERO_SUFFIX_INT($$->str_value_, value, int_flag);
  if (value > OB_MAX_PARSER_INT16_VALUE) {
    $$->int16_values_[0] = OB_MAX_PARSER_INT16_VALUE;
  } else {
    $$->int16_values_[0] = value;
  }

  $$->int16_values_[1] = 0;
};

pl_outer_scalar_data_type:
//    int_type_i
//    {
//      /* same as number(38, 0) */
//     malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER);
//      $$->int16_values_[0] = 38;
//      $$->int16_values_[1] = 0;
//      $$->int16_values_[2] = NPT_PERC_SCALE;
//    }
  /*
  | NUMBER
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER);
      $$->int16_values_[0] = -1;
      $$->int16_values_[1] = -85;
      $$->int16_values_[2] = NPT_EMPTY;
    }
  | FLOAT
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER_FLOAT);
      $$->int16_values_[0] = 126;
      $$->int16_values_[1] = -1;
    }
  | REAL
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_NUMBER_FLOAT);
    $$->int16_values_[0] = 63;
    $$->int16_values_[1] = -1;
  }
  */
  /*为了解决reduce冲突，统一pl_inner_data_type/pl_outer_data_type中的pl_obj_access_ref解决，这里会先注释掉，并不表示不支持
  | double_type_i
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, $1[0]);
    $$->int16_values_[0] = -1;
    $$->int16_values_[1] = -85;
    $$->int16_values_[2] = 0;
  }
  | TIMESTAMP
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_TIMESTAMP_NANO);
    }
  */
  TIMESTAMP WITH TIME ZONE
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_TIMESTAMP_TZ);
    }
  | TIMESTAMP WITH LOCAL TIME ZONE
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_TIMESTAMP_LTZ);
    }
  /*为了解决reduce冲突，统一pl_inner_data_type/pl_outer_data_type中的pl_obj_access_ref解决，这里会先注释掉，并不表示不支持
  | RAW
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_RAW);
      $$->int32_values_[0] = 2000;
      $$->int32_values_[1] = 1;
    }
  | datetime_type_i
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_DATETIME);
      $$->int16_values_[1] = 0;
      $$->int16_values_[2] = 0;
    }
  */
/*为了解决reduce冲突，统一pl_inner_data_type/pl_outer_data_type中的pl_obj_access_ref解决，这里会先注释掉，并不表示不支持*/
//  | CHARACTER
//    {
//      $$ = 0;
//      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_CHAR, 3, NULL, NULL, $$);
//      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
//      $$->int32_values_[1] = 0; /* is char */
//      $$->length_semantics_ = (2 & 0x03); /* length semantics type */
//    }
  | CHARACTER collation
    {
      $$ = 0;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_CHAR, 3, NULL, $2, $$);
      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = (2 & 0x03); /* length semantics type */
    }
  | CHARACTER charset_key charset_name opt_collation
    {
      $$ = 0;
      (void)($2);
      ParseNode *charset_node = NULL;
      malloc_terminal_node(charset_node, parse_ctx->mem_pool_, T_CHARSET);
      charset_node->str_value_ = $3->str_value_;
      charset_node->str_len_ = $3->str_len_;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_CHAR, 3, charset_node, $4, $$);
      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = (2 & 0x03); /* length semantics type */
    }
  | CHARACTER BINARY opt_charset opt_collation
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_BINARY);
      $$->value_ = 1;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_CHAR, 3, $3, $4, $$);
      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = (2 & 0x03); /* length semantics type */
    }
  /*为了解决reduce冲突，统一pl_inner_data_type/pl_outer_data_type中的pl_obj_access_ref解决，这里会先注释掉，并不表示不支持*/
//  | NCHAR
//    {
//      $$ = 0;
//      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_NCHAR, 3, NULL, NULL, $$);
//      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
//      $$->int32_values_[1] = 0; /* is char */
//      $$->length_semantics_ = 1; /* length semantics type */
//    }
  | NCHAR collation
    {
      $$ = 0;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_NCHAR, 3, NULL, $2, $$);
      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = 1; /* length semantics type */
    }
  | NCHAR charset_key charset_name opt_collation
    {
      $$ = 0;
      (void)($2);
      ParseNode *charset_node = NULL;
      malloc_terminal_node(charset_node, parse_ctx->mem_pool_, T_CHARSET);
      charset_node->str_value_ = $3->str_value_;
      charset_node->str_len_ = $3->str_len_;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_NCHAR, 3, charset_node, $4, $$);
      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = 1; /* length semantics type */
    }
  | NCHAR BINARY opt_charset opt_collation
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_BINARY);
      $$->value_ = 1;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_NCHAR, 3, $3, $4, $$);
      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = 1; /* length semantics type */
    }
  /*
  | varchar_type_i
    {
      $$ = 0;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_VARCHAR, 3, NULL, NULL, $$);
      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
      $$->int32_values_[1] = 0;
      $$->length_semantics_ = (2 & 0x03);
    }
  */
  | VARCHAR collation
    {
      $$ = 0;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_VARCHAR, 3, NULL, $2, $$);
      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = (2 & 0x03); /* length semantics type */
    }
  | VARCHAR charset_key charset_name opt_collation
    {
      $$ = 0;
      (void)($2);
      ParseNode *charset_node = NULL;
      malloc_terminal_node(charset_node, parse_ctx->mem_pool_, T_CHARSET);
      charset_node->str_value_ = $3->str_value_;
      charset_node->str_len_ = $3->str_len_;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_VARCHAR, 3, charset_node, $4, $$);
      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = (2 & 0x03); /* length semantics type */
    }
  | VARCHAR BINARY opt_charset opt_collation
    {
      ParseNode *binary_node = NULL;
      malloc_terminal_node(binary_node, parse_ctx->mem_pool_, T_BINARY);
      binary_node->value_ = 1;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_VARCHAR, 3, $3, $4, binary_node);
      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = (2 & 0x03); /* length semantics type */
    }
  | VARCHAR2 collation
    {
      $$ = 0;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_VARCHAR, 3, NULL, $2, $$);
      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = (2 & 0x03); /* length semantics type */
    }
  | VARCHAR2 charset_key charset_name opt_collation
    {
      $$ = 0;
      (void)($2);
      ParseNode *charset_node = NULL;
      malloc_terminal_node(charset_node, parse_ctx->mem_pool_, T_CHARSET);
      charset_node->str_value_ = $3->str_value_;
      charset_node->str_len_ = $3->str_len_;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_VARCHAR, 3, charset_node, $4, $$);
      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = (2 & 0x03); /* length semantics type */
    }
  | VARCHAR2 BINARY opt_charset opt_collation
    {
      ParseNode *binary_node = NULL;
      malloc_terminal_node(binary_node, parse_ctx->mem_pool_, T_BINARY);
      binary_node->value_ = 1;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_VARCHAR, 3, $3, $4, binary_node);
      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = (2 & 0x03); /* length semantics type */
    }
/*为了解决reduce冲突，统一pl_inner_data_type/pl_outer_data_type中的pl_obj_access_ref解决，这里会先注释掉，并不表示不支持*/
//  | nvarchar_type_i
//    {
//      $$ = 0;
//      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_NVARCHAR2, 3, NULL, NULL, $$);
//      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
//      $$->int32_values_[1] = 0; /* is char */
//      $$->length_semantics_ = 1; /* length semantics type */
//    }
  | nvarchar_type_i collation
    {
      $$ = 0;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_NVARCHAR2, 3, NULL, $2, $$);
      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = 1; /* length semantics type */
    }
  | nvarchar_type_i charset_key charset_name opt_collation
    {
      $$ = 0;
      (void)($2);
      ParseNode *charset_node = NULL;
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_CHARSET);
      $$->str_value_ = $3->str_value_;
      $$->str_len_ = $3->str_len_;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_NVARCHAR2, 3, charset_node, $4, $$);
      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = 1; /* length semantics type */
    }
  | nvarchar_type_i BINARY opt_charset opt_collation
    {
      ParseNode *binary_node = NULL;
      malloc_terminal_node(binary_node, parse_ctx->mem_pool_, T_BINARY);
      binary_node->value_ = 1;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_NVARCHAR2, 3, $3, $4, binary_node);
      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
      $$->int32_values_[1] = 0; /* is char */
      $$->length_semantics_ = 1; /* length semantics type */
    }
  | INTERVAL YEAR TO MONTH
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_INTERVAL_YM);
      $$->int16_values_[0] = 9;
      $$->int16_values_[1] = 0;
    }
  | INTERVAL DAY TO SECOND
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_INTERVAL_DS);
      $$->int16_values_[0] = 9;
      $$->int16_values_[1] = 0;
      $$->int16_values_[2] = 9;
      $$->int16_values_[3] = 0;
    }
/*为了解决reduce冲突，统一pl_inner_data_type/pl_outer_data_type中的pl_obj_access_ref解决，这里会先注释掉，并不表示不支持*/
//  | BLOB /* oracle 12c only support collate @hanhui TODO */
//    {
//      malloc_terminal_node($$, parse_ctx->mem_pool_, T_LONGTEXT);
//      $$->int32_values_[0] = 0;  /* need to be reset by accuracy */
//      $$->int32_values_[1] = 1; /* is binary */
//    }
//  | CLOB /* oracle 12c only support collate @hanhui TODO */
//    {
//      $$ = 0;
//      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LONGTEXT, 3, NULL, NULL, $$);
//      $$->int32_values_[0] = 0;  /* need to be reset by accuracy */
//      $$->int32_values_[1] = 0; /* is text */
//    }
  | CLOB collation/* oracle 12c only support collate @hanhui TODO */
    {
      $$ = 0;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LONGTEXT, 3, NULL, $2, $$);
      $$->int32_values_[0] = 0;  /* need to be reset by accuracy */
      $$->int32_values_[1] = 0; /* is text */
    }
  | CLOB charset_key charset_name opt_collation/* oracle 12c only support collate @hanhui TODO */
    {
      $$ = 0;
      (void)($2);
      ParseNode *charset_node = NULL;
      malloc_terminal_node(charset_node, parse_ctx->mem_pool_, T_CHARSET);
      charset_node->str_value_ = $3->str_value_;
      charset_node->str_len_ = $3->str_len_;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LONGTEXT, 3, charset_node, $4, $$);
      $$->int32_values_[0] = 0;  /* need to be reset by accuracy */
      $$->int32_values_[1] = 0; /* is text */
    }
  | CLOB BINARY opt_charset opt_collation/* oracle 12c only support collate @hanhui TODO */
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_BINARY);
      $$->value_ = 1;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LONGTEXT, 3, $3, $4, $$);
      $$->int32_values_[0] = 0;  /* need to be reset by accuracy */
      $$->int32_values_[1] = 0; /* is text */
    }
/*为了解决reduce冲突，统一pl_inner_data_type/pl_outer_data_type中的pl_obj_access_ref解决，这里会先注释掉，并不表示不支持
  | UROWID
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_UROWID);
      $$->int32_values_[0] = DEFAULT_STR_LENGTH;
    }
  | pl_common_scalar_data_type
    {
      $$ = $1;
    }
*/
;

/*为了解决reduce冲突，统一pl_inner_data_type/pl_outer_data_type中的pl_obj_access_ref解决，这里会先注释掉，并不表示不支持
pl_common_scalar_data_type:
    BOOL
	{
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_TINYINT);
	  $$->int16_values_[0] = 1;
      $$->int16_values_[2] = 0;  // zerofill always false
    }
  BOOLEAN
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_TINYINT);
      $$->int16_values_[0] = 1;
      $$->int16_values_[2] = 0; // zerofill always false
    }
  pl_int_type_i
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_INTEGER_TYPE);
      $$->value_ = $1[0];
    }
  */
;

/*为了解决reduce冲突，统一pl_inner_data_type/pl_outer_data_type中的pl_obj_access_ref解决，这里会先注释掉，并不表示不支持*/
//pl_int_type_i:
//    PLS_INTEGER     { $$[0] = SP_PLS_INTEGER; }
//  BINARY_INTEGER  { $$[0] = SP_BINARY_INTEGER; }
//  NATURAL         { $$[0] = SP_NATURAL; }
 // | NATURALN        { $$[0] = SP_NATURALN; }
 // | POSITIVE        { $$[0] = SP_POSITIVE; }
 // | POSITIVEN       { $$[0] = SP_POSITIVEN; }
 // | SIGNTYPE        { $$[0] = SP_SIGNTYPE; }
 // | SIMPLE_INTEGER  { $$[0] = SP_SIMPLE_INTEGER; }
;

/*为了解决reduce冲突，统一pl_inner_data_type/pl_outer_data_type中的pl_obj_access_ref解决，这里会先注释掉，并不表示不支持*/
//int_type_i:
  //SMALLINT      { $$[0] = T_SMALLINT; }
  //| INTEGER     { $$[0] = T_INT32; }
  //| NUMERIC     { $$[0] = T_INT32; }
  //| DECIMAL     { $$[0] = T_INT32; }
;

/*
varchar_type_i:
  VARCHAR       { $$[0] = T_VARCHAR; }
  | VARCHAR2    { $$[0] = T_VARCHAR; }
;
*/

nvarchar_type_i:
  NVARCHAR2       { $$[0] = T_NVARCHAR2; }
;

/*
double_type_i:
  BINARY_DOUBLE        { $$[0] = T_DOUBLE; }
  | BINARY_FLOAT       { $$[0] = T_FLOAT;  }
;
*/

/*
simple_type_i:
  SIMPLE_DOUBLE        { $$[0] = T_DOUBLE; }
  | SIMPLE_FLOAT       { $$[0] = T_FLOAT;  }
;
*/

/*
datetime_type_i:
  DATE        { $$[0] = T_DATETIME; }
;
*/

number_precision:
  '(' signed_int_num ',' signed_int_num ')'
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE);
    if($2[0] > OB_MAX_PARSER_INT16_VALUE) {
      $$->int16_values_[0] = OB_MAX_PARSER_INT16_VALUE;
    } else {
      $$->int16_values_[0] = $2[0];
    }
    if($4[0] > OB_MAX_PARSER_INT16_VALUE) {
      $$->int16_values_[1] = OB_MAX_PARSER_INT16_VALUE;
    } else {
      $$->int16_values_[1] = $4[0];
    }
    $$->int16_values_[2] = NPT_PERC_SCALE;
    $$->param_num_ = 2;
  }
  | '(' signed_int_num ')'
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE);
    if($2[0] > OB_MAX_PARSER_INT16_VALUE) {
      $$->int16_values_[0] = OB_MAX_PARSER_INT16_VALUE;
    } else {
      $$->int16_values_[0] = $2[0];
    }
    $$->int16_values_[1] = 0;
    $$->int16_values_[2] = NPT_PERC;
    $$->param_num_ = 1;
  }
  | '(' precision_decimal_num ')'
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE);
    $$->int16_values_[0] = $2->int16_values_[0];
    $$->int16_values_[1] = $2->int16_values_[1];
    $$->int16_values_[2] = NPT_PERC;
    $$->param_num_ = 1;
    const char *stmt_str = parse_ctx->stmt_str_ + @2.first_column;
    int32_t str_len = @2.last_column - @2.first_column + 1;
    $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
    check_ptr($$->str_value_);
    $$->str_len_ = str_len;
  }
;

//opt_int_precision:
//    number_precision { $$ = $1; }
//  | /*EMPTY*/
//    {
//      malloc_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE);
//      $$->int16_values_[0] = 38;
//      $$->int16_values_[1] = 0;
//      $$->int16_values_[2] = NPT_PERC_SCALE;
//    }
//;

signed_int_num:
  INTNUM { $$[0] = $1->value_; }
  | '-' INTNUM
  {
    $$[0] = -$2->value_;
  }
;

opt_datetime:
  '(' INTNUM ')'
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE);
    $$->int16_values_[0] = $2->value_;
    $$->int16_values_[1] = 1;
  }
  | '(' precision_decimal_num ')'
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE);
    $$->int16_values_[0] = $2->int16_values_[0];
    if (0 == $2->int16_values_[1]) {
      $$->int16_values_[1] = 1;
    } else {
      $$->int16_values_[1] = $2->int16_values_[1];
    }
    const char *stmt_str = parse_ctx->stmt_str_ + @2.first_column;
    int32_t str_len = @2.last_column - @2.first_column + 1;
    $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
    check_ptr($$->str_value_);
    $$->str_len_ = str_len;
  }
  | /*EMPTY*/
  {
    malloc_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE);
    $$->int16_values_[0] = 6;
    $$->int16_values_[1] = 0;
  }
;

//interval year/day precision
opt_interval_leading:
'(' INTNUM ')'
{
  malloc_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE);
   $$->int16_values_[0] = $2->value_;
   $$->int16_values_[1] = 1;
}
| '(' precision_decimal_num ')'
{
  malloc_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE);
  $$->int16_values_[0] = $2->int16_values_[0];
  if (0 == $2->int16_values_[1]) {
    $$->int16_values_[1] = 1; /* same as intnum */
  } else {
    $$->int16_values_[1] = $2->int16_values_[1];
  }
  const char *stmt_str = parse_ctx->stmt_str_ + @2.first_column;
  int32_t str_len = @2.last_column - @2.first_column + 1;
  $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
  check_ptr($$->str_value_);
  $$->str_len_ = str_len;
}
| /*EMPTY*/
{
  malloc_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE);
  $$->int16_values_[0] = 2;
  $$->int16_values_[1] = 0;
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

nstring_length_i:
  '(' number_literal ')'
  {
    int64_t val = 0;
    if (T_NUMBER == $2->type_) {
      errno = 0;
      val = strtoll($2->str_value_, NULL, 10);
      if (ERANGE == errno || val > UINT32_MAX)
      {
        $$[0] = OUT_OF_STR_LEN;// out of str_max_len
      }
      else if (val > MAX_VARCHAR_LENGTH)
      {
        $$[0] = MAX_VARCHAR_LENGTH;
      } else
      {
        $$[0] = val;
      }
    } else if ($2->value_ > UINT32_MAX) {
      $$[0] = OUT_OF_STR_LEN;
    } else if ($2->value_ > MAX_VARCHAR_LENGTH) {
      $$[0] = MAX_VARCHAR_LENGTH;
    } else {
      $$[0] = $2->value_;
    }
    $$[1] = $2->param_num_;
  }
;

string_length_i:
  '(' number_literal opt_length_semantics_i ')'
  {
    int64_t val = 0;
    if (T_NUMBER == $2->type_) {
      if (OUT_OF_STR_LEN == $2->value_) {
        errno = 0;
        val = strtoll($2->str_value_, NULL, 10);
        if (ERANGE == errno || val > UINT32_MAX)
        {
          $$[0] = OUT_OF_STR_LEN;// out of str_max_len
        }
        else if (val > MAX_VARCHAR_LENGTH)
        {
          $$[0] = MAX_VARCHAR_LENGTH;
        } else
        {
          $$[0] = val;
        }
      } else {
        /*todo：上报语意错误会触发断连接的问题, 先统一上报语法错误*/
        obpl_oracle_yyerror(&@$, parse_ctx, "Syntax Error");
        YYERROR;
        /*
        bool int_flag = false;
        CHECK_ASSIGN_ZERO_SUFFIX_INT($2->str_value_, val, int_flag);
        if (int_flag) {
          YY_ERR_NUMERIC_OR_VALUE_ERROR("character string buffer too small\n");
        } else {
          YY_ERR_NON_INTEGRAL_NUMERIC_LITERAL("non-integral numeric literal\n");
        }*/
      }
    } else if ($2->value_ > UINT32_MAX) {
      $$[0] = OUT_OF_STR_LEN;
    } else if ($2->value_ > MAX_VARCHAR_LENGTH) {
      $$[0] = MAX_VARCHAR_LENGTH;
    } else {
      $$[0] = $2->value_;
    }
    $$[1] = $2->param_num_;
    $$[2] = $3[0];
  }
;

opt_length_semantics_i:
  /*EMPTY*/         { $$[0] = 0; }
  | CHARACTER       { $$[0] = 1; }
  | BYTE            { $$[0] = 2; }
  ;

//opt_string_length_i:
//  string_length_i
//  {
//    $$[0] = $1[0];
//    $$[1] = $1[1];
//    $$[2] = $1[2];
//  }
//  | /*EMPTY*/
//  {
//    $$[0] = 1;
//    $$[1] = 0;
//    $$[2] = 0;
//  }
//;

opt_binary:
    BINARY
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_BINARY);
      $$->value_ = 1;
    }
  | /*EMPTY*/ {$$ = 0; }
;

urowid_length_i:
'(' INTNUM ')'
{
  $$[0] = $2->value_;
}
;

//opt_urowid_length_i:
//urowid_length_i
//{
//  $$[0] = $1[0];
//  $$[1] = 1;
//}
//| /* EMPTY */
//{
//  $$[0] = 4000; // 默认是4000
//  $$[1] = 0;
//}
//;

collation_name:
    identifier
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
    identifier
    {
      if (nodename_equal($1, "BINARY", 6)) {
        malloc_terminal_node($$, parse_ctx->mem_pool_, T_VARCHAR);
        $$->str_value_ = parse_strdup("binary", parse_ctx->mem_pool_, &($$->str_len_));
        if (OB_UNLIKELY(NULL == $$->str_value_)) {
          YY_FATAL_ERROR("no more memory to malloc 'binary; string\n");
        }
        $$->param_num_ = 0;
        $$->is_hidden_const_ = 1;
      } else {
        $$ = $1;
        $$->type_ = T_VARCHAR;
        $$->param_num_ = 0;
        $$->is_hidden_const_ = 1;
      }
    }
  | STRING
    {
      $$ = $1;
      $$->param_num_ = 1;
      $$->is_hidden_const_ = 0;
    }
  /*
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
    */
;

opt_charset:
    /*EMPTY*/ { $$ = NULL; }
  | charset_key charset_name
    {
      (void)($1);
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_CHARSET);
      $$->str_value_ = $2->str_value_;
      $$->str_len_ = $2->str_len_;
    }
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

open_stmt:
    OPEN pl_access_name opt_sp_cparam_list
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PROC_OPEN, 4, $2, $3, NULL, NULL);
    }
    | OPEN cursor_name for_sql opt_using_clause
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PROC_OPEN, 4, $2, NULL, $3, $4);
    }
;

for_sql:
  FOR sql_stmt
    {
      if ($2->children_[0]->type_ != T_SELECT) {
        obpl_oracle_yyerror(&@2, parse_ctx, "Syntax Error");
        YYERROR;
      }
      $$ = $2;
    }
  | FOR expr
  {
    $$ = $2;
      if (NULL != $2)
    copy_node_abs_location($2->stmt_loc_, @2);
  }
;

fetch_stmt:
    FETCH pl_access_name into_clause opt_limit
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PROC_FETCH, 2, $2, $3, $4);
    }
  | FETCH pl_access_name bulk_collect_into_clause opt_limit
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PROC_FETCH, 3, $2, $3, $4);
    }
;

into_clause:
    INTO pl_left_value_list
    {
      ParseNode *vars_list = NULL;
      merge_nodes(vars_list, parse_ctx->mem_pool_, T_SP_INTO_LIST, $2);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_INTO_VARIABLES, 1, vars_list);
    }
;

bulk_collect_into_clause:
    BULK COLLECT INTO pl_left_value_list
    {
      ParseNode *vars_list = NULL;
      merge_nodes(vars_list, parse_ctx->mem_pool_, T_SP_INTO_LIST, $4);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_INTO_VARIABLES, 1, vars_list);
      $$->value_ = 1;
    }
;

opt_limit:
    /*EMPTY*/ { $$ = NULL; }
  | LIMIT expr
  {
    if ($2 == NULL) YYERROR;
    const char *stmt_str = parse_ctx->stmt_str_ + @2.first_column;
    int32_t str_len = @2.last_column - @2.first_column + 1;
    $2->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
    check_ptr($2->str_value_);
    $2->str_len_ = str_len;
    $$ = $2;
      if (NULL != $2)
    copy_node_abs_location($2->stmt_loc_, @2);
  }
;

close_stmt:
    CLOSE pl_access_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_PROC_CLOSE, 1, $2);
    }
;

execute_immediate_stmt:
    EXECUTE IMMEDIATE expr opt_normal_into_clause opt_using_clause opt_dynamic_returning_clause
    {
      if (NULL == $3) YYERROR;
      if (NULL != $4 && NULL != $6) YYERROR;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_EXECUTE_IMMEDIATE, 4, $3, $4, $5, $6);
      if (NULL != $3)
      copy_node_abs_location($3->stmt_loc_, @3);
    }
;

raise_stmt:
/*为了解决reduce冲突，在inner_call_stmt中的pl_obj_access_ref中解决,这里会先注释掉，并不表示不支持
    RAISE
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_SIGNAL, 1, NULL);
    }
*/
    RAISE pl_access_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_SIGNAL, 1, $2);
    }
;

normal_into_clause:
    into_clause { $$ = $1; }
  | bulk_collect_into_clause { $$ = $1; }
;

opt_normal_into_clause:
    /* Empty */ { $$ = NULL; }
  | normal_into_clause { $$ = $1; }
;

opt_using_clause:
    /* Empty */ { $$ = NULL; }
  | USING using_list { $$ = $2; }
;

using_list:
    using_params
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_SP_USING_LIST, $1);
    }
;

using_params:
    using_params ',' using_param
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
  | using_param { $$ = $1; }
;

using_param:
    opt_sp_inout expr
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_USING_PARAM, 1, $2);
      $$->value_ = $1[0];
    }
;

opt_dynamic_returning_clause:
    /* Empty */ { $$ = NULL; }
  | RETURNING normal_into_clause { $$ = $2; }
  | RETURN normal_into_clause { $$ = $2; }
;

/*****************************************************************************
 *
 *      sql_stmt grammar
 *
 *****************************************************************************/
sql_keyword:
    SQL_KEYWORD { $$ = NULL; }
  | INSERT { $$ = NULL; }
  | UPDATE { $$ = NULL; }
  | DELETE { $$ = NULL; }
  | TABLE { $$ = NULL; }
  | SAVEPOINT { $$ = NULL; }
  | WITH { $$ = NULL; }
  | SET { $$ = NULL; }
  | MERGE { $$ = NULL; }
;

sql_stmt:
    sql_keyword /*sql stmt tail*/
    {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, @1.first_column, @1.last_column, 2, ';', END_P);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
      $$->str_value_ = sql_stmt->str_value_;
      $$->str_len_ = sql_stmt->str_len_;
    }
  | COMMIT /*sql stmt tail*/
    {
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, @1.first_column, @1.last_column, 2, ';', END_P);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
      $$->str_value_ = sql_stmt->str_value_;
      $$->str_len_ = sql_stmt->str_len_;
    }
  | ROLLBACK /*sql stmt tail*/
    {
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, @1.first_column, @1.last_column, 2, ';', END_P);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
      $$->str_value_ = sql_stmt->str_value_;
      $$->str_len_ = sql_stmt->str_len_;
    }
;
/*****************************************************************************
 *
 *      ALTER PROCEDURE grammar
 *
 *****************************************************************************/
 alter_procedure_stmt:
    ALTER PROCEDURE pl_schema_name sp_alter_clause
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_ALTER, 2, $3, $4);
    }
 ;

 alter_function_stmt:
    ALTER FUNCTION pl_schema_name sp_alter_clause
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SF_ALTER, 2, $3, $4);
    }
 ;

 alter_trigger_stmt:
    ALTER TRIGGER pl_schema_name enable_or_disable
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_TG_ALTER, 1, $3);
      $$->value_ = $4[0];
    }
 ;

 enable_or_disable:
    ENABLE { $$[0] = T_ENABLE; }
  | DISABLE { $$[0] = T_DISABLE; }
 ;

 procedure_compile_clause:
    COMPILE opt_reuse_settings
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_COMPILE_CLAUSE, 1, NULL);
      $$->int32_values_[0] = 0;
      $$->int32_values_[1] = $2[0];
    }
    | COMPILE DEBUG opt_reuse_settings
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_COMPILE_CLAUSE, 1, NULL);
      $$->int32_values_[0] = 1;
      $$->int32_values_[1] = $3[0];
    }
    | COMPILE compiler_parameter_list opt_reuse_settings
    {
      ParseNode *params_node = NULL;
      merge_nodes(params_node, parse_ctx->mem_pool_, T_VARIABLE_SET, $2);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_COMPILE_CLAUSE, 1, params_node);
      $$->int32_values_[0] = 0;
      $$->int32_values_[1] = $3[0];
    }
    | COMPILE DEBUG compiler_parameter_list opt_reuse_settings
    {
      ParseNode *params_node = NULL;
      merge_nodes(params_node, parse_ctx->mem_pool_, T_VARIABLE_SET, $3);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_COMPILE_CLAUSE, 1, params_node);
      $$->int32_values_[0] = 1;
      $$->int32_values_[1] = $4[0];
    }
;

compiler_parameter:
    identifier '=' STRING
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_VAR_VAL, 2, $1, $3);
    }
  | identifier '=' INTNUM
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_VAR_VAL, 2, $1, $3);
    }
;

compiler_parameter_list:
    compiler_parameter { $$ = $1; }
  | compiler_parameter_list compiler_parameter
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
;

sp_editionable:
    EDITIONABLE
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_EDITIONABLE_CLAUSE);
      $$->value_ = 1;
    }
  | NONEDITIONABLE
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_EDITIONABLE_CLAUSE);
      $$->value_ = 0;
    }
;

sp_alter_clause:
    procedure_compile_clause { $$ = $1; }
  | sp_editionable { $$ = $1; }
;

/*****************************************************************************
 *
 *      CREATE TYPE grammar
 *
 *****************************************************************************/

create_type_stmt:
  CREATE opt_replace opt_editionable plsql_type_spec_source
  {
    const char *stmt_str = parse_ctx->stmt_str_ + @4.first_column;
    int32_t str_len = @4.last_column - @4.first_column + 1;
    $4->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
    check_ptr($4->str_value_);
    $4->str_len_ = str_len;
    $4->str_off_ = @4.first_column;
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_CREATE_TYPE, 1, $4);
    $$->int32_values_[0] = $2[0];
    $$->int32_values_[1] = $3[0];
  }
;

plsql_type_spec_source:
 TYPE pl_schema_name opt_force opt_oid_clause opt_type_def
 {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_CREATE_TYPE_SRC, 3, $2, $4, $5);
    // set object constructor return type;
    if (NULL == $5) {
      // do nothing
    } else if (T_SP_OBJECT_DEF == $5->type_
              || T_SP_OPAQUE_DEF == $5->type_) {
      ParseNode *elem_list = $5->children_[3];
      if (NULL != elem_list) {
        ParseNode *fun_list = elem_list->children_[1];
        if (NULL != fun_list) {
          for (int i = 0; i < fun_list->num_child_; ++i) {
            ParseNode *fun_item = fun_list->children_[i];
            if (NULL != fun_item) {
              if (T_SP_OBJ_ELEMENT_SPEC == fun_item->type_) {
                ParseNode *con_fun = fun_item->children_[1];
                if (NULL != con_fun && T_SP_OBJ_ELEM_CONSTRUCTOR == con_fun->type_) {
                  // pl_schema_name is T_SP_NAME, we need T_SP_OBJ_ACCESS_REF
                  ParseNode *obj_access = NULL;
                  ParseNode *sn_l = $2->children_[0];
                  ParseNode *sn_r = $2->children_[1];
                  ParseNode *acc_name = NULL;
                  if (NULL == sn_l) {
                    malloc_non_terminal_node(acc_name, parse_ctx->mem_pool_, T_SP_ACCESS_NAME, 3, NULL, NULL, sn_r);
                    check_ptr(acc_name);
                    malloc_non_terminal_node(obj_access, parse_ctx->mem_pool_, T_SP_OBJ_ACCESS_REF, 2, acc_name, NULL);
                    check_ptr(obj_access);
                  } else {
                    malloc_non_terminal_node(acc_name, parse_ctx->mem_pool_, T_SP_ACCESS_NAME, 3, NULL, sn_l, sn_r);
                    check_ptr(acc_name);
                    malloc_non_terminal_node(obj_access, parse_ctx->mem_pool_, T_SP_OBJ_ACCESS_REF, 2, acc_name, NULL);
                    check_ptr(obj_access);
                  }
                  con_fun->children_[2] = obj_access;
                }
              }
            }
          }
        }
      }
    }
    if (parse_ctx->is_inner_parse_) {
      const char *stmt_str = parse_ctx->stmt_str_ + @5.first_column;
      int32_t str_len = @5.last_column - @5.first_column + 1;
      $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($$->str_value_);
      $$->str_len_ = str_len;
    }
 }
;

create_type_body_stmt:
  CREATE opt_replace opt_editionable plsql_type_body_source
  {
    const char *stmt_str = parse_ctx->orig_stmt_str_ + @4.first_column;
    int32_t str_len = @4.last_column - @4.first_column + 1;
    $4->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
    check_ptr($4->str_value_);
    $4->str_len_ = str_len;
    $4->str_off_ = @4.first_column;
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_CREATE_TYPE_BODY, 1, $4);
    $$->int32_values_[0] = $2[0];
    $$->int32_values_[1] = $3[0];
  }
;

plsql_type_body_source:
  TYPE BODY pl_schema_name is_or_as plsql_type_body_decl_list_semicolon END_KEY
  {
    ParseNode *obj_body = NULL;
    merge_nodes(obj_body, parse_ctx->mem_pool_, T_SP_OBJECT_BODY_DEF, $5);
    check_ptr(obj_body);
    for (int i = 0; i < obj_body->num_child_; ++i) {
      ParseNode *fun_wrap = obj_body->children_[i];
      if (NULL != fun_wrap && T_SP_CONTRUCTOR_DEF_IN_TYPE == fun_wrap->type_) {
        ParseNode *func = fun_wrap->children_[1];
        ParseNode *obj_access = NULL;
        ParseNode *sn_l = $3->children_[0];
        ParseNode *sn_r = $3->children_[1];
        ParseNode *acc_name = NULL;
        if (NULL == sn_l) {
          malloc_non_terminal_node(acc_name, parse_ctx->mem_pool_, T_SP_ACCESS_NAME, 3, NULL, NULL, sn_r);
          check_ptr(acc_name);
          malloc_non_terminal_node(obj_access, parse_ctx->mem_pool_, T_SP_OBJ_ACCESS_REF, 2, acc_name, NULL);
          check_ptr(obj_access);
        } else {
          malloc_non_terminal_node(acc_name, parse_ctx->mem_pool_, T_SP_ACCESS_NAME, 3, NULL, sn_l, sn_r);
          check_ptr(acc_name);
          malloc_non_terminal_node(obj_access, parse_ctx->mem_pool_, T_SP_OBJ_ACCESS_REF, 2, acc_name, NULL);
          check_ptr(obj_access);
        }
        if (NULL != func && T_SP_OBJ_CONSTR_IMPL == func->type_) {
          ParseNode *func_spec = func->children_[0];
          // ParseNode *sf_src = func->children_[2];
          // replace the ret node from NULL to a obj_access;
          if (NULL != func_spec && T_SP_OBJ_ELEM_CONSTRUCTOR == func_spec->type_) {
            // pl_schema_name is T_SP_NAME, we need T_SP_OBJ_ACCESS_REF
            func_spec->children_[2] = obj_access;
            // sf_src->children_[2] = obj_access;
          }
        } else if (NULL != func && T_SP_OBJ_ELEM_CONSTRUCTOR == func->type_) {
          func->children_[2] = obj_access;
        }
      }
    }
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_CREATE_TYPE_BODY_SRC, 2, $3, obj_body);
    if (parse_ctx->is_inner_parse_) {
      const char *stmt_str = parse_ctx->orig_stmt_str_ + @5.first_column;
      int32_t str_len = @5.last_column - @5.first_column + 1;
      $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr($$->str_value_);
      $$->str_len_ = str_len;
      $$->str_off_ = @5.first_column;
    }
  }
;

plsql_type_body_decl_list_semicolon:
  plsql_type_body_decl_list ';'
  {
    $$ = $1;
  }

plsql_type_body_decl_list:
  plsql_type_body_decl
  {
    $$ = $1;
  }
| plsql_type_body_decl_list ';' plsql_type_body_decl
{
  malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
}
;

plsql_type_body_decl:
  subprog_decl_in_type { $$ = $1; }
| map_order_function_spec
 {
   $$ = $1;
   check_ptr($$);
   $$->int16_values_[0] = 2; // map order function define
 }
;

subprog_decl_in_type:
  proc_or_func_def_in_type { $$ = $1; }
| constructor_def_in_type { $$ = $1; }
| interface_pragma { $$ = $1; }
;

proc_or_func_def_in_type:
  member_or_static proc_def
  {
    $$ = $2;
    check_ptr($$);
    ParseNode *decl = $$->children_[0];
    check_ptr(decl); // proc_decl
    decl->int16_values_[0] = $1[0] == 1 ? (int16_t)UDT_UDF_MEMBER : (int16_t)UDT_UDF_STATIC;
    decl->int16_values_[1] = 2;
  }
| member_or_static func_def
  {
    $$ = $2;
    check_ptr($$);
    ParseNode *decl = $$->children_[0];
    check_ptr(decl); // func_decl
    decl->int16_values_[0] = $1[0] == 1 ? (int16_t)UDT_UDF_MEMBER : (int16_t)UDT_UDF_STATIC;
    decl->int16_values_[1] = 2;
  }
| member_or_static proc_decl
  {
    $$ = $2;
    check_ptr($$);
    $$->int16_values_[0] = $1[0] == 1 ? (int16_t)UDT_UDF_MEMBER : (int16_t)UDT_UDF_STATIC;
    $$->int16_values_[1] = 2;
  }
| member_or_static func_decl
  {
    $$ = $2;
    check_ptr($$);
    $$->int16_values_[0] = $1[0] == 1 ? (int16_t)UDT_UDF_MEMBER : (int16_t)UDT_UDF_STATIC;
    $$->int16_values_[1] = 2;
  }
;

constructor_def_in_type:
  opt_final_inst_list constructor_def
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_CONTRUCTOR_DEF_IN_TYPE, 2, $1, $2);
  }
| opt_final_inst_list constructor_spec
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_CONTRUCTOR_DEF_IN_TYPE, 2, $1, $2);
  }
;

opt_force:
    /*EMPTY*/ { $$[0] = 0; }
  | FORCE { $$[0] = 1; }
;

opt_oid_clause:
    /*EMPTY*/ { $$ = NULL; }
  | OID STRING { $$ = $2; }
;

opt_type_def:
    /*EMPTY*/ { $$ = NULL; }
  | object_type_def { $$ = $1; }
  | is_or_as varray_type_def { $$ = $2; }
  | is_or_as nested_table_type_def {  $$ = $2; }
  | is_or_as opaque_def { $$ = $2; }
;

opaque_def:
  OPAQUE '(' element_spec ')'
  {
    ParseNode *object_node = NULL;
    malloc_terminal_node(object_node, parse_ctx->mem_pool_, T_SP_IS_OBJECT);

    ParseNode *attr_list_node = NULL;
    malloc_non_terminal_node(attr_list_node, parse_ctx->mem_pool_, T_SP_OBJ_ATTR_AND_ELEMENT_SPEC, 2, NULL, $3);
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OPAQUE_DEF, 5, NULL, object_node, NULL, attr_list_node, NULL);
  }
;

object_type_def:
  object_or_under opt_sqlj_obj_type attr_and_element_spec opt_final_inst_list
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJECT_DEF, 5, NULL, $1, $2, $3, $4);
  }
 | proc_clause_list object_or_under opt_sqlj_obj_type attr_and_element_spec opt_final_inst_list
  {
    ParseNode *proc_list = NULL;
    merge_nodes(proc_list, parse_ctx->mem_pool_, T_SP_CLAUSE_LIST, $1);
    check_ptr(proc_list);
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJECT_DEF, 5, proc_list, $2, $3, $4, $5);
  }
;

object_or_under:
    is_or_as OBJECT
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_IS_OBJECT);
    }
  | UNDER pl_schema_name
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_IS_UNDER_OBJECT, 1, $2);
    }
;

opt_sqlj_obj_type:
    /*EMPTY*/ { $$ = NULL; }
  | EXTERNAL NAME pl_schema_name LANGUAGE JAVA USING sqlj_using
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_SQLJ_OBJ_TYPE, 1, $3);
      $$->value_ = $7[0];
    }
;

sqlj_using:
    SQLDATA { $$[0] = 1; }
  | CUSTOMDATUM { $$[0] = 2; }
  | ORADATA { $$[0] = 3; }
;

final_or_inst:
    FINAL
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_FINAL);
      $$->value_ = 0;
    }
  | INSTANTIABLE
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INISTANTIABLE);
      $$->value_ = 0;
    }
  | NOT FINAL
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_FINAL);
      $$->value_ = 1;
    }
  | NOT INSTANTIABLE
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INISTANTIABLE);
      $$->value_ = 1;
    }
;

opt_final_inst_list:
    /*EMPTY*/ { $$ = NULL; }
  | final_inst_list
    {
      merge_nodes($$, parse_ctx->mem_pool_, T_SP_OBJ_FINAL_INST_LIST, $1);
    }
;

final_inst_list:
    final_or_inst { $$ = $1; }
  | final_inst_list final_or_inst
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $2);
    }
;

attr_and_element_spec:
  '(' attr_list ')'
  {
    ParseNode *attr_list_node = NULL;
    merge_nodes(attr_list_node, parse_ctx->mem_pool_, T_SP_OBJ_ATTR_LIST, $2);
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ATTR_AND_ELEMENT_SPEC, 2, attr_list_node, NULL);
  }
  |'(' attr_list ',' element_spec ')'
  {
    ParseNode *attr_list_node = NULL;
    merge_nodes(attr_list_node, parse_ctx->mem_pool_, T_SP_OBJ_ATTR_LIST, $2);
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ATTR_AND_ELEMENT_SPEC, 2, attr_list_node, $4);
  }
  | '(' element_spec ')'
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ATTR_AND_ELEMENT_SPEC, 2, NULL, $2);
    YY_NO_ATTRIBUTE_FOUND("no attribute found\n");
  }
;

attr_list:
    attr_spec { $$ = $1; }
  | attr_list ',' attr_spec
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
;

attr_spec:
  common_identifier pl_inner_data_type opt_not_null opt_record_member_default opt_sqlj_obj_type_attr
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ATTR_SPEC, 4, $1, $2, $4, $5);
    $$->value_ = $3[0];
  }
  | MAP pl_inner_data_type opt_not_null opt_record_member_default opt_sqlj_obj_type_attr
  {
    get_oracle_non_reserved_node($$, parse_ctx->mem_pool_, @1.first_column, @1.last_column);
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ATTR_SPEC, 4, $$, $2, $4, $5);
    $$->value_ = $3[0];
  }
  | TYPE pl_inner_data_type opt_not_null opt_record_member_default opt_sqlj_obj_type_attr
  {
    get_oracle_non_reserved_node($$, parse_ctx->mem_pool_, @1.first_column, @1.last_column);
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ATTR_SPEC, 4, $$, $2, $4, $5);
    $$->value_ = $3[0];
  }
  | SUBTYPE pl_inner_data_type opt_not_null opt_record_member_default opt_sqlj_obj_type_attr
  {
    get_oracle_non_reserved_node($$, parse_ctx->mem_pool_, @1.first_column, @1.last_column);
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ATTR_SPEC, 4, $$, $2, $4, $5);
    $$->value_ = $3[0];
  }
;

opt_sqlj_obj_type_attr:
    /*EMPTY*/ { $$ = NULL; }
  | EXTERNAL NAME STRING
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_SQLJ_TYPE_ATTR, 1, $3);
    }
;

element_spec:
    el_element_spec_list_cc
    {
      ParseNode *elem_list = NULL;
      merge_nodes(elem_list, parse_ctx->mem_pool_, T_SP_OBJ_ELEMENT_SPEC_LIST, $1);
      $$ = elem_list;
    }
;

element_spec_long:
    el_element_spec
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ELEMENT_SPEC, 3, NULL, $1, NULL);
    }
    | OVERRIDING el_element_spec
    {
      ParseNode *overri_node = NULL;
      malloc_terminal_node(overri_node, parse_ctx->mem_pool_, T_SP_IS_OBJECT);
      overri_node->value_ = 0;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, overri_node, NULL, NULL);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ELEMENT_SPEC, 3, $$, $2, NULL);
    }
    | NOT OVERRIDING el_element_spec
    {
      ParseNode *overri_node = NULL;
      malloc_terminal_node(overri_node, parse_ctx->mem_pool_, T_SP_IS_OBJECT);
      overri_node->value_ = 1;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, overri_node, NULL, NULL);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ELEMENT_SPEC, 3, $$, $3, NULL);
    }
    | OVERRIDING inheritance_final_instantiable_clause el_element_spec
    {
      ParseNode *overri_node = NULL;
      malloc_terminal_node(overri_node, parse_ctx->mem_pool_, T_SP_IS_OBJECT);
      overri_node->value_ = 0;
      $2->children_[0] = overri_node;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ELEMENT_SPEC, 3, $2, $3, NULL);
    }
    | NOT OVERRIDING inheritance_final_instantiable_clause el_element_spec
    {
      ParseNode *overri_node = NULL;
      malloc_terminal_node(overri_node, parse_ctx->mem_pool_, T_SP_IS_OBJECT);
      overri_node->value_ = 1;
      $3->children_[0] = overri_node;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ELEMENT_SPEC, 3, $3, $4, NULL);
    }
    | FINAL el_element_spec
    {
      ParseNode *final_node = NULL;
      malloc_terminal_node(final_node, parse_ctx->mem_pool_, T_SP_OBJ_FINAL);
      final_node->value_ = 0;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, NULL, final_node, NULL);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ELEMENT_SPEC, 3, $$, $2, NULL);
    }
    | NOT FINAL el_element_spec
    {
      ParseNode *final_node = NULL;
      malloc_terminal_node(final_node, parse_ctx->mem_pool_, T_SP_OBJ_FINAL);
      final_node->value_ = 1;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, NULL, final_node, NULL);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ELEMENT_SPEC, 3, $$, $3, NULL);
    }
    | FINAL inheritance_overriding_instantiable_clause el_element_spec
    {
      ParseNode *final_node = NULL;
      malloc_terminal_node(final_node, parse_ctx->mem_pool_, T_SP_OBJ_FINAL);
      final_node->value_ = 0;
      $2->children_[1] = final_node;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ELEMENT_SPEC, 3, $2, $3, NULL);
    }
    | NOT FINAL inheritance_overriding_instantiable_clause el_element_spec
    {
      ParseNode *final_node = NULL;
      malloc_terminal_node(final_node, parse_ctx->mem_pool_, T_SP_OBJ_FINAL);
      final_node->value_ = 1;
      $3->children_[1] = final_node;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ELEMENT_SPEC, 3, $3, $4, NULL);
    }
    | INSTANTIABLE el_element_spec
    {
      ParseNode *inst_node = NULL;
      malloc_terminal_node(inst_node, parse_ctx->mem_pool_, T_SP_OBJ_INISTANTIABLE);
      inst_node->value_ = 0;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, NULL, NULL, inst_node);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ELEMENT_SPEC, 3, $$, $2, NULL);
    }
    | NOT INSTANTIABLE el_element_spec
    {
      ParseNode *inst_node = NULL;
      malloc_terminal_node(inst_node, parse_ctx->mem_pool_, T_SP_OBJ_INISTANTIABLE);
      inst_node->value_ = 1;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, NULL, NULL, inst_node);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ELEMENT_SPEC, 3, $$, $3, NULL);
    }
    | INSTANTIABLE inheritance_overriding_final_clause el_element_spec
    {
      ParseNode *inst_node = NULL;
      malloc_terminal_node(inst_node, parse_ctx->mem_pool_, T_SP_OBJ_INISTANTIABLE);
      inst_node->value_ = 0;
      $2->children_[2] = inst_node;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ELEMENT_SPEC, 3, $2, $3, NULL);
    }
    | NOT INSTANTIABLE inheritance_overriding_final_clause el_element_spec
    {
      ParseNode *inst_node = NULL;
      malloc_terminal_node(inst_node, parse_ctx->mem_pool_, T_SP_OBJ_INISTANTIABLE);
      inst_node->value_ = 1;
      $3->children_[2] = inst_node;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ELEMENT_SPEC, 3, $3, $4, NULL);
    }
    | restrict_references_pragma
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ELEMENT_SPEC, 3, NULL, NULL, $1);
    }
;

inheritance_final_instantiable_clause:
  final_clause
  {
    ParseNode *final_node = NULL;
    malloc_terminal_node(final_node, parse_ctx->mem_pool_, T_SP_OBJ_FINAL);
    final_node->value_ = $1[0];
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, NULL, final_node, NULL);
  }
  | instantiable_clause
  {
    ParseNode *inst_node = NULL;
    malloc_terminal_node(inst_node, parse_ctx->mem_pool_, T_SP_OBJ_INISTANTIABLE);
    inst_node->value_ = $1[0];
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, NULL, NULL, inst_node);
  }
  | final_clause instantiable_clause
  {
    ParseNode *final_node = NULL;
    ParseNode *inst_node = NULL;
    malloc_terminal_node(final_node, parse_ctx->mem_pool_, T_SP_OBJ_FINAL);
    malloc_terminal_node(inst_node, parse_ctx->mem_pool_, T_SP_OBJ_INISTANTIABLE);
    final_node->value_ = $1[0];
    inst_node->value_ = $2[0];
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, NULL, final_node, inst_node);
  }
  | instantiable_clause final_clause
  {
    ParseNode *final_node = NULL;
    ParseNode *inst_node = NULL;
    malloc_terminal_node(final_node, parse_ctx->mem_pool_, T_SP_OBJ_FINAL);
    malloc_terminal_node(inst_node, parse_ctx->mem_pool_, T_SP_OBJ_INISTANTIABLE);
    final_node->value_ = $2[0];
    inst_node->value_ = $1[0];
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, NULL, final_node, inst_node);
  }
;

inheritance_overriding_instantiable_clause:
  overriding_clause
  {
    ParseNode *overri_node = NULL;
    malloc_terminal_node(overri_node, parse_ctx->mem_pool_, T_SP_IS_OBJECT);
    overri_node->value_ = $1[0];
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, overri_node, NULL, NULL);
  }
  | instantiable_clause
  {
    ParseNode *inst_node = NULL;
    malloc_terminal_node(inst_node, parse_ctx->mem_pool_, T_SP_OBJ_INISTANTIABLE);
    inst_node->value_ = $1[0];
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, NULL, NULL, inst_node);
  }
  | overriding_clause instantiable_clause
  {
    ParseNode *overri_node = NULL;
    ParseNode *inst_node = NULL;
    malloc_terminal_node(overri_node, parse_ctx->mem_pool_, T_SP_IS_OBJECT);
    malloc_terminal_node(inst_node, parse_ctx->mem_pool_, T_SP_OBJ_INISTANTIABLE);
    overri_node->value_ = $1[0];
    inst_node->value_ = $2[0];
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, overri_node, NULL, inst_node);
  }
  | instantiable_clause overriding_clause
  {
    ParseNode *overri_node = NULL;
    ParseNode *inst_node = NULL;
    malloc_terminal_node(overri_node, parse_ctx->mem_pool_, T_SP_IS_OBJECT);
    malloc_terminal_node(inst_node, parse_ctx->mem_pool_, T_SP_OBJ_INISTANTIABLE);
    overri_node->value_ = $2[0];
    inst_node->value_ = $1[0];
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, overri_node, NULL, inst_node);
  }
;

inheritance_overriding_final_clause:
  overriding_clause
  {
    ParseNode *overri_node = NULL;
    malloc_terminal_node(overri_node, parse_ctx->mem_pool_, T_SP_IS_OBJECT);
    overri_node->value_ = $1[0];
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, overri_node, NULL, NULL);
  }
  | final_clause
  {
    ParseNode *final_node = NULL;
    malloc_terminal_node(final_node, parse_ctx->mem_pool_, T_SP_OBJ_FINAL);
    final_node->value_ = $1[0];
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, NULL, final_node, NULL);
  }
  | overriding_clause final_clause
  {
    ParseNode *overri_node = NULL;
    ParseNode *final_node = NULL;
    malloc_terminal_node(overri_node, parse_ctx->mem_pool_, T_SP_IS_OBJECT);
    malloc_terminal_node(final_node, parse_ctx->mem_pool_, T_SP_OBJ_FINAL);
    overri_node->value_ = $1[0];
    final_node->value_ = $2[0];
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, overri_node, final_node, NULL);
  }
  | final_clause overriding_clause
  {
    ParseNode *overri_node = NULL;
    ParseNode *final_node = NULL;
    malloc_terminal_node(overri_node, parse_ctx->mem_pool_, T_SP_IS_OBJECT);
    malloc_terminal_node(final_node, parse_ctx->mem_pool_, T_SP_OBJ_FINAL);
    overri_node->value_ = $2[0];
    final_node->value_ = $1[0];
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE, 3, overri_node, final_node, NULL);
  }
;

overriding_clause:
OVERRIDING { $$[0] = 0; }
| NOT OVERRIDING { $$[0] = 1; }
;

final_clause:
FINAL { $$[0] = 0; }
  | NOT FINAL {$$[0] = 1; }
;

instantiable_clause:
INSTANTIABLE { $$[0] = 0; }
  | NOT INSTANTIABLE { $$[0] = 1; }
;

el_element_spec_list_cc:
    element_spec_long { $$ = $1; }
  | el_element_spec_list_cc ',' element_spec_long
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, $3);
    }
;

el_element_spec:
    subprogram_spec { $$ = $1; }
  | constructor_spec { $$ = $1; }
  | map_order_function_spec
    {
      $$ = $1; check_ptr($$);
      $$->int16_values_[0] = 1;  // map order decl
    }
;

/*
inheritance_clauses_list:
  inheritance_clauses
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE);
      $$->value_ = $1[0];
    }
  | inheritance_clauses_list inheritance_clauses
    {
      ParseNode *inher_node = NULL;
      malloc_terminal_node(inher_node, parse_ctx->mem_pool_, T_SP_OBJ_INHERITANCE);
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, inher_node);
    }
;

inheritance_clauses:
    OVERRIDING { $$[0] = 1; }
  | NOT OVERRIDING { $$[0] = 2; }
  | FINAL { $$[0] = 3; }
  | NOT FINAL {$$[0] = 4; }
  | INSTANTIABLE { $$[0] = 5; }
  | NOT INSTANTIABLE { $$[0] = 6; }
*/
;

default_or_string:
    DEFAULT
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_NAME);
    }
  | identifier
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_NAME, 1, $1);
    }
;

assert_list:
    assert_item
    {
      const char* stmt_str = parse_ctx->orig_stmt_str_ + @1.first_column;
      int32_t str_len = @1.last_column - @1.first_column + 1;
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_ASSERT_ITEM);
      $$->value_ = $1[0];
      $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      $$->str_len_ = str_len;
    }
  | assert_list ',' assert_item
    {
      ParseNode *assert_node = NULL;
      const char* stmt_str = parse_ctx->orig_stmt_str_ + @3.first_column;
      int32_t str_len = @3.last_column - @3.first_column + 1;
      malloc_terminal_node(assert_node, parse_ctx->mem_pool_, T_SP_ASSERT_ITEM);
      assert_node->value_ = $3[0];
      assert_node->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      assert_node->str_len_ = str_len;
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_LINK_NODE, 2, $1, assert_node);
    }
;

assert_item: /*! same as ObPLCompileFlag */
    TRUST { $$[0] = 0; }
  | RNDS  { $$[0] = 1; }
  | WNDS  { $$[0] = 2; }
  | RNPS  { $$[0] = 3; }
  | WNPS  { $$[0] = 4; }
  | IDENT { $$[0] = -1;}
  | STRING{ $$[0] = -1;}
;

subprogram_spec:
  member_or_static proc_or_func_spec
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ELEM_SUBPROG_SPEC, 1, $2);
    $2->int16_values_[0] = $1[0] == 1 ? (int16_t)UDT_UDF_MEMBER : (int16_t)UDT_UDF_STATIC;
  }
;

member_or_static:
    MEMBER { $$[0] = 1; }
  | STATIC { $$[0] = 2; }
;

proc_or_func_spec:
    proc_decl { $$ = $1; }
  | proc_def { $$ = $1; }
  | func_decl { $$ = $1; }
  | func_def { $$ = $1; }
  | sqlj_func_decl { $$ = $1; }
;

sqlj_func_decl:
  FUNCTION func_name opt_sp_param_list sqlj_obj_type_sig
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_SQLJ_FUNC_DECL, 3, $2, $3, $4);
  }
;

sqlj_obj_type_sig:
  RETURN type_or_self EXTERNAL varname_or_name
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_SQLJ_SIG, 2, $2, $4);
  }
;

type_or_self:
    pl_outer_data_type { $$ = $1; }
  | SELF AS RESULT
    {
      malloc_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_SELF_RESULT);
    }
;

varname_or_name:
    VARIABLE NAME STRING
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_VARIABLE_NAME, 1, $3);
    }
  | NAME STRING
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_NAME, 1, $2);
    }
;

constructor_spec:
  CONSTRUCTOR FUNCTION func_name opt_sp_param_list
  RETURN SELF AS RESULT
  {
    const char *stmt_str = parse_ctx->stmt_str_ + @1.first_column;
    int32_t str_len = @8.last_column - @1.first_column + 1;
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ELEM_CONSTRUCTOR, 5, $3, $4, NULL /* ret node*/, NULL, NULL);
    check_ptr($$);
    $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
    check_ptr($$->str_value_);
    $$->str_len_ = str_len;
    $$->int16_values_[0] = (int16_t)(UDT_UDF_CONS);; // 1 : cons, 2 : member, 3 : static, 4: map, 5 : order
    $$->int16_values_[1] = 2; // function, not procedure
    copy_node_abs_location($$->stmt_loc_, @3);
  }
  | CONSTRUCTOR FUNCTION func_name opt_sp_param_list RETURN pl_outer_data_type
  {
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_ELEM_CONSTRUCTOR, 5, $3, $4, NULL /* ret node*/, NULL, NULL);
    YY_MUST_RETURN_SELF("construnct function must return self as result\n");
  }
;

constructor_def:
  constructor_spec opt_constructor_impl
  {
    const char *stmt_str = parse_ctx->stmt_str_ + @1.first_column;
    int32_t str_len = @2.last_column - @1.first_column + 1;
    ParseNode *sf_node = NULL;
    ParseNode *func_name = NULL;
    malloc_non_terminal_node(func_name, parse_ctx->mem_pool_, T_SP_NAME, 2, NULL, $1->children_[0]);
    $2->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
    check_ptr($2->str_value_);
    $2->str_len_ = str_len;
    malloc_non_terminal_node(sf_node, parse_ctx->mem_pool_, T_SF_SOURCE, 6, func_name, $1->children_[1], $1->children_[2], $1->children_[3], $1->children_[4], $2);
    check_ptr(sf_node);
    sf_node->int16_values_[0] = (int16_t)(UDT_UDF_CONS); // 1 : cons, 2 : member, 3 : static, 4: map, 5 : order
    sf_node->int16_values_[1] = 2; // function, not procedure
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_CONSTR_IMPL, 2, $1, $2);
    check_ptr($$);
    $$->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
    check_ptr($$->str_value_);
    $$->str_len_ = str_len;
  }
;

opt_constructor_impl:
  is_or_as pl_impl_body
  {
    $$ = $2;
  }
;

map_order_function_spec:
    MAP MEMBER proc_or_func_spec
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_MAP_ORDER, 1, $3);
      check_ptr($3);
      if (T_SUB_FUNC_DECL == $3->type_ || T_SUB_PROC_DECL == $3->type_) {
        $3->int16_values_[0] = (int16_t)UDT_UDF_MAP;;
      } else if (T_SUB_PROC_DEF == $3->type_ || T_SUB_FUNC_DEF == $3->type_) {
        check_ptr($3->children_[0]);
        $3->children_[0]->int16_values_[0] = (int16_t)UDT_UDF_MAP;
      }
    }
  | ORDER MEMBER proc_or_func_spec
    {
      malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_OBJ_MAP_ORDER, 1, $3);
      check_ptr($3);
      if (T_SUB_FUNC_DECL == $3->type_ || T_SUB_PROC_DECL == $3->type_) {
        $3->int16_values_[0] = (int16_t)UDT_UDF_ORDER;
      } else if (T_SUB_PROC_DEF == $3->type_ || T_SUB_FUNC_DEF == $3->type_) {
        check_ptr($3->children_[0]);
        $3->children_[0]->int16_values_[0] = (int16_t)UDT_UDF_ORDER;
      }
    }
;

drop_type_stmt:
/*
这条语法包含了一个drop  type body xxx这样的语法。那为何这个不写两条呢？
例如这样： DROP TYPE BODY pl_schema_name
这是因为这两条语法规则会有一个conflict, 例如：drop type body validate，这样这两条语法规则都能匹配，
就会有一个reduce/shift conflict. 这是因为pl_schema_name包含了body，同时也包含了force和validate
*/
  DROP TYPE pl_schema_name opt_force_or_validate
  {
    ParseNode *schema_name = $3;
    int32_t is_body = 0;
    int32_t is_force_validate = 0;
    if (NULL != $4) {
      is_force_validate = $4->int32_values_[0]; // force or validate
    }
    if (NULL != $3) {
     if (nodename_equal($3->children_[1], "BODY", 4)) { // drop type body
      is_body = 1;
      if (NULL == $4) {
        YYERROR;
      } else {
        schema_name = $4;
      }
    } else {
      if (NULL != $4 && ( 1 != is_force_validate && 2 != is_force_validate)) {
        YYERROR;
      }
    }
    }
    malloc_non_terminal_node($$, parse_ctx->mem_pool_, T_SP_DROP_TYPE, 1, schema_name);
    $$->int32_values_[0] = is_force_validate;
    $$->int32_values_[1] = is_body;
  }
;

opt_force_or_validate:
    /*EMPTY*/ { $$ = NULL; }
  | pl_schema_name
  {
    $$ = $1;
    if (nodename_equal($1->children_[1], "FORCE", 5)) {
      $$->int32_values_[0] = 1;
    } else if (nodename_equal($1->children_[1], "VALIDATE", 8)) {
      $$->int32_values_[0] = 2;
    } else {
      $$->int32_values_[0] = 0;
    }
  }
;

unreserved_keyword:
ACCESSIBLE
|    AUTHID
|    AFTER
|    AGGREGATE
|    AUTONOMOUS_TRANSACTION
|    BLOB
|    BINARY
|    BINARY_DOUBLE
|    BINARY_FLOAT
|    BINARY_INTEGER
|    BOOL
|    BULK
|    BYTE
|    BEFORE
|    BODY %prec LOWER_PARENS
|    C
|    CALL
|    CHARACTER
|    CHARSET
|    CLOB
|    COLLATE
|    COLLECT
|    COLLATION
|    COMPILE
|    CONSTRUCTOR
|    CONTINUE %prec LOWER_PARENS
|    CURRENT_USER
|    CUSTOMDATUM
|    COMPOUND
|    DEBUG
|    DEFINER
|    DETERMINISTIC
|    DISABLE
|    EACH
|    ENABLE
|    ERROR
|    EXCEPTIONS
|    EXTERNAL
|    EDITIONABLE
|    EXECUTE
|    FINAL
|    FORALL
|    FORCE
|    HASH
|    IMMEDIATE
|    INDICES
|    INSTEAD
|    INTERFACE
|    INTERVAL
|    JAVA
|    LANGUAGE
|    LIMIT
|    LOCAL
|    LONG
|    INSTANTIABLE
|    MEMBER
|    NAME
|    NATURALN
|    NCHAR %prec LOWER_PARENS
|    NESTED
|    NVARCHAR2
|    NO
|    NONEDITIONABLE
|    NUMERIC
|    OBJECT
|    OID
|    OVERRIDING
|    OPAQUE
|    ORADATA
|    PARALLEL_ENABLE
|    PIPE
|    PIPELINED
|    PLS_INTEGER
|    POSITIVEN
|    RAW
|    REF
|    RELIES_ON
|    REPLACE
|    RESTRICT_REFERENCES
|    RESULT
|    RESULT_CACHE
|    RETURNING
|    REUSE
|    RNDS
|    RNPS
|    SAVE
|    SELF %prec LOWER_PARENS
|    SERIALLY_REUSABLE
|    SETTINGS
|    SIGNTYPE
|    SIMPLE_INTEGER
|    SIMPLE_DOUBLE
|    SIMPLE_FLOAT
|    SPECIFICATION
|    SQLDATA
|    STATIC
|    TIME
|    TIMESTAMP
|    TRUST
|    UDF
|    UNDER
|    USING
|    USING_NLS_COMP
|    VALIDATE
|    VALUE
|    VARIABLE
|    VARRAY
|    VARYING
|    WNDS
|    WNPS
|    YEAR
|    ZONE
|    MONTH
|    DAY
|    HOUR
|    MINUTE
|    SECOND
|    UROWID
|    LEVEL
|    INLINE
|    NEW
|    OLD
|    PARENT
|    REFERENCING
|    ROW
|    TRIGGER
|    YES
|    NOCOPY
|    ARRAY
|    CLOSE %prec LOWER_PARENS
|    OPEN %prec LOWER_PARENS
|    POSITIVE
|    PRAGMA %prec LOWER_PARENS
|    RAISE %prec LOWER_PARENS
|    RECORD
|    REVERSE
|    ROWTYPE
|    STATEMENT
|    CONSTANT
|    DATE
|    EXCEPTION_INIT
|    EXIT
|    FLOAT %prec LOWER_PARENS
|    LOOP
|    NATURAL
|    NUMBER %prec LOWER_PARENS
|    OTHERS
|    OUT
|    PACKAGE_P
|    PARTITION
|    RANGE
|    REAL %prec LOWER_PARENS
|    ROWID %prec LOWER_PARENS
|    VARCHAR %prec LOWER_PARENS
|    VARCHAR2 %prec LOWER_PARENS
;
;

unreserved_special_keyword:
MAP
|    TYPE
;

%%
/**
 * parser function
 * @param [out] pl_yychar, set the pl parser look-ahead token
 */
ParseNode *obpl_oracle_read_sql_construct(ObParseCtx *parse_ctx,
                                          bool is_for_expr,
                                          const char *prefix,
                                          const char *postfix,
                                          YYLookaheadToken *la_token,
                                          int end_token_cnt, ...)
{
  int errcode = OB_PARSER_SUCCESS;
  va_list va;
  bool is_break = false;
  int sql_str_len = -1;
  int parenlevel = 0;
  const char *sql_str = NULL;
  ParseResult parse_result;
  ParseNode *sql_node = NULL;
  int plsql_line = la_token->la_yylloc->first_line;
  if (*(la_token->la_yychar) != -2) { //#define YYEMPTY    (-2)
    parse_ctx->scanner_ctx_.sql_end_loc = la_token->la_yylloc->last_column;
    if (parse_ctx->scanner_ctx_.sql_start_loc < 0) {
      parse_ctx->scanner_ctx_.sql_start_loc = la_token->la_yylloc->first_column;
    }
  }
  while (!is_break) {
    *(la_token->la_yychar) = obpl_oracle_yylex(la_token->la_yylval, la_token->la_yylloc, parse_ctx->scanner_ctx_.yyscan_info_);
    if (parse_ctx->scanner_ctx_.sql_start_loc < 0) {
      //get sql rule start location
      parse_ctx->scanner_ctx_.sql_start_loc = la_token->la_yylloc->first_column;
    }
    if (QUESTIONMARK == *(la_token->la_yychar)
        && parse_ctx->question_mark_ctx_.by_ordinal_) {
      parse_ctx->question_mark_ctx_.count_--;
    }
    va_start(va, end_token_cnt);
    int i = 0;
    for (; !is_break && i < end_token_cnt; ++i) {
      int end_token = va_arg(va, int);
      if (end_token == *(la_token->la_yychar) && parenlevel == 0) {
        is_break = true;
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
    }
    if (!is_break) {
      //get sql rule end location
      parse_ctx->scanner_ctx_.sql_end_loc = la_token->la_yylloc->last_column;
    }
  }
  if (OB_PARSER_SUCCESS == errcode) {
    sql_str_len = parse_ctx->scanner_ctx_.sql_end_loc - parse_ctx->scanner_ctx_.sql_start_loc + 1;
  }
  if (OB_PARSER_SUCCESS == errcode && sql_str_len > 0) {
    sql_str = strndup_with_prefix_and_postfix(prefix, postfix,
                                  parse_ctx->stmt_str_ + parse_ctx->scanner_ctx_.sql_start_loc,
                                  sql_str_len, parse_ctx->mem_pool_);
    if (NULL == sql_str) {
      YY_FATAL_ERROR("no memory to strdup sql string\n");
    }
    //parse sql_str...
    memset(&parse_result, 0, sizeof(ParseResult));
    parse_result.input_sql_ = sql_str;
    parse_result.input_sql_len_ = sql_str_len + strlen(prefix) + strlen(postfix);
    parse_result.malloc_pool_ = parse_ctx->mem_pool_;
    parse_result.pl_parse_info_.is_pl_parse_ = true;
    parse_result.pl_parse_info_.is_pl_parse_expr_ = is_for_expr;
    parse_result.pl_parse_info_.plsql_line_ = plsql_line;
    //将pl_parser的question_mark_size赋值给sql_parser，使得parser sql的question mark能够接着pl_parser的index
    parse_result.question_mark_ctx_ = parse_ctx->question_mark_ctx_;
    parse_result.sql_mode_ = SMO_ORACLE;
    parse_result.is_for_trigger_ = (1 == parse_ctx->is_for_trigger_);
    parse_result.is_dynamic_sql_ = (1 == parse_ctx->is_dynamic_);
    parse_result.charset_info_ = parse_ctx->charset_info_;
    parse_result.is_not_utf8_connection_ = parse_ctx->is_not_utf8_connection_;
    parse_result.connection_collation_ = parse_ctx->connection_collation_;
    char *buf = (char *)parse_malloc(parse_result.input_sql_len_ * 2, parse_result.malloc_pool_); //因为要把pl变量替换成:num的形式，需要扩大内存
    if (OB_UNLIKELY(NULL == buf)) {
      YY_FATAL_ERROR("no memory to alloc\n");
    } else {
      parse_result.param_nodes_ = NULL;
      parse_result.tail_param_node_ = NULL;
      parse_result.no_param_sql_ = buf;
      parse_result.no_param_sql_buf_len_ = parse_result.input_sql_len_ * 2;
    }
  }
  if (sql_str_len <= 0) {
    //do nothing
  } else if (0 != parse_sql_stmt(&parse_result)) {
    if (parse_result.extra_errno_ != OB_PARSER_SUCCESS) {
      obpl_oracle_parse_fatal_error(parse_result.extra_errno_, YYLEX_PARAM, parse_result.error_msg_);
    } else {
      YYLTYPE sql_yylloc;
      memset(&sql_yylloc, 0, sizeof(YYLTYPE));
      sql_yylloc.first_column = parse_ctx->scanner_ctx_.sql_start_loc;
      sql_yylloc.last_column = parse_ctx->scanner_ctx_.sql_end_loc;
      sql_yylloc.first_line = la_token->la_yylloc->first_line;
      sql_yylloc.last_line = la_token->la_yylloc->last_line;
      obpl_oracle_yyerror(&sql_yylloc, parse_ctx, "Syntax Error\n");
    }
  } else {
    sql_node = parse_result.result_tree_->children_[0];
    if (parse_result.no_param_sql_len_
        + (parse_result.input_sql_len_ - parse_result.pl_parse_info_.last_pl_symbol_pos_) + 1
        > parse_result.no_param_sql_buf_len_) {
      char *buf = (char *)parse_malloc(parse_result.no_param_sql_buf_len_ * 2, parse_result.malloc_pool_);
      if (OB_UNLIKELY(NULL == buf)) {
        YY_FATAL_ERROR("no memory to alloc\n");
      } else {
        memmove(buf, parse_result.no_param_sql_, parse_result.no_param_sql_len_);
        parse_result.no_param_sql_ = buf;
        parse_result.no_param_sql_buf_len_ = parse_result.no_param_sql_buf_len_ * 2;
      }
    }
    memmove(parse_result.no_param_sql_ + parse_result.no_param_sql_len_,
                  parse_result.input_sql_ + parse_result.pl_parse_info_.last_pl_symbol_pos_,
                  parse_result.input_sql_len_ - parse_result.pl_parse_info_.last_pl_symbol_pos_);
    parse_result.no_param_sql_len_ += parse_result.input_sql_len_ - parse_result.pl_parse_info_.last_pl_symbol_pos_;
    sql_node->str_value_ =  parse_strndup(parse_result.no_param_sql_, parse_result.no_param_sql_len_, parse_ctx->mem_pool_);
    sql_node->str_len_ = parse_result.no_param_sql_len_;
    //通过sql_parser的question_mark_size来更新pl_parser
    parse_ctx->question_mark_ctx_ = parse_result.question_mark_ctx_;
  }
  return sql_node;
}

void obpl_oracle_yyerror(YYLTYPE *yylloc, ObParseCtx *parse_ctx, char *s, ...)
{
  if (OB_LIKELY(NULL != parse_ctx)) {
    va_list ap;
    va_start(ap, s);
    vsnprintf(parse_ctx->global_errmsg_, MAX_ERROR_MSG, s, ap);
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
    va_end(ap);
  }
}
