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

#ifndef OCEANBASE_SQL_PARSER_PARSE_NODE_H_
#define OCEANBASE_SQL_PARSER_PARSE_NODE_H_

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>
#include <setjmp.h>
#ifdef SQL_PARSER_COMPILATION
#include "ob_sql_mode.h"
#include "ob_item_type.h"
#else
#include "common/sql_mode/ob_sql_mode.h"
#include "sql/parser/ob_item_type.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_ERROR_MSG 1024

struct ObCharsetInfo;

enum SelectParserOffset {
  PARSE_SELECT_WITH,
  PARSE_SELECT_DISTINCT,
  PARSE_SELECT_SELECT,
  PARSE_SELECT_INTO,  // into before from
  PARSE_SELECT_FROM,
  PARSE_SELECT_WHERE,
  PARSE_SELECT_DYNAMIC_SW_CBY,  // connect by node or start with node
  PARSE_SELECT_DYNAMIC_CBY_SW,  // connect by node or start with node
  PARSE_SELECT_GROUP,
  PARSE_SELECT_HAVING,
  PARSE_SELECT_SET,
  PARSE_SELECT_ALL,
  PARSE_SELECT_FORMER,
  PARSE_SELECT_LATER,
  PARSE_SELECT_ORDER,
  PARSE_SELECT_LIMIT,
  PARSE_SELECT_FOR_UPD,
  PARSE_SELECT_HINTS,
  PARSE_SELECT_WHEN,
  PARSE_SELECT_NAMED_WINDOWS,
  PARSE_SELECT_FETCH,
  PARSE_SELECT_FETCH_TEMP,  // use to temporary store fetch clause in parser
  PARSE_SELECT_INTO_EXTRA,  // ATTENTION!! SELECT_INTO_EXTRA must be the last one
  PARSE_SELECT_MAX_IDX
};

enum GrantParseOffset {
  PARSE_GRANT_ROLE_LIST,
  PARSE_GRANT_ROLE_GRANTEE,
  PARSE_GRANT_ROLE_OPT_WITH,
  PARSE_GRANT_ROLE_MAX_IDX
};

enum GrantParseSysOffset {
  PARSE_GRANT_SYS_PRIV_ORACLE_LIST,
  PARSE_GRANT_SYS_PRIV_ORACLE_GRANTEE,
  PARSE_GRANT_SYS_PRIV_ORACLE_OPT_WITH,
  PARSE_GRANT_SYS_PRIV_ORACLE_MAX_IDX
};

enum ParseMode {
  STD_MODE = 0,
  FP_MODE,                               /* fast parse, keep hint, and do parameterization*/
  MULTI_MODE,                            /* multi query ultra-fast parse */
  FP_PARAMERIZE_AND_FILTER_HINT_MODE,    /*Filter out the hint and do parameterization*/
  FP_NO_PARAMERIZE_AND_FILTER_HINT_MODE, /*Filter out hints and do not parameterize*/
  TRIGGER_MODE,                          /* treat ':xxx' as identifier */
  DYNAMIC_SQL_MODE, /*In the process of parsing dynamic sql: iidx and:identifier should be determined
                      according to the statement type whether to check the name of the placeholder*/
  DBMS_SQL_MODE,
};

typedef struct {
  int err_code_;
  char err_msg_[MAX_ERROR_MSG];
} ErrStat;

struct _ParseNode;

typedef struct _ObStmtLoc {
  int first_column_;
  int last_column_;
  int first_line_;
  int last_line_;
} ObStmtLoc;

typedef struct _ParseNode {
  ObItemType type_;
  int32_t num_child_;
  int16_t param_num_;
  struct {
    uint16_t is_neg_ : 1;
    uint16_t is_hidden_const_ : 1;
    uint16_t is_tree_not_param_ : 1;
    uint16_t length_semantics_ : 2;
    uint16_t is_change_to_char_ : 1;
    uint16_t is_val_paramed_item_idx_ : 1;
    uint16_t is_copy_raw_text_ : 1;
    uint16_t is_column_varchar_ : 1;
    uint16_t is_trans_from_minus_ : 1;
    uint16_t is_assigned_from_child_ : 1;
    uint16_t is_num_must_be_pos_ : 1;
    uint16_t is_date_unit_ : 1;
    uint16_t is_literal_bool_ : 1; // indicate node is a literal TRUE/FALSE
    uint16_t reserved_ : 2;
  };
  union {
    int64_t value_;
    int32_t int32_values_[2];
    int16_t int16_values_[4];
  };
  const char* str_value_;
  int64_t str_len_;
  const char* raw_text_;
  int64_t text_len_;
  int64_t pos_;

  struct _ParseNode** children_; /* attributes for non-terninal node, which has children */
  ObStmtLoc stmt_loc_;
  union {
    int64_t raw_param_idx_;
    int64_t raw_sql_offset_;
  };

#ifdef SQL_PARSER_COMPILATION
  int token_off_;
  int token_len_;
#endif
} ParseNode;

struct _ParamList;

typedef struct _ParamList {
  ParseNode* node_;
  struct _ParamList* next_;
} ParamList;

enum RefType {
  REF_REL = 0,
  REF_PROC,
  REF_FUNC,
};

typedef struct _RefObjList {
  enum RefType type_;
  ParseNode* node_;
  struct _RefObjList* next_;
} RefObjList;

typedef struct _PLParseInfo {
  bool is_pl_parse_;
  bool is_pl_parse_expr_;
  /*for mysql pl*/
  void* pl_ns_;  // ObPLBlockNS
  int last_pl_symbol_pos_;
  RefObjList* ref_object_nodes_;
  RefObjList* tail_ref_object_node_;
} PLParseInfo;

#define MAX_QUESTION_MARK 128

typedef struct _ObQuestionMarkCtx
{
  char **name_;
  int count_;
  int capacity_;
  bool by_ordinal_;
  bool by_name_;
} ObQuestionMarkCtx;

// record the minus status while parsing the sql
// for example, 'select - -1 from dual'
// when parser sees the first '-', pos_ = 7, raw_sql_offset = 7, has_minus_ = true, is_cur_numeric_ = false
// after seeing the second '-', members are reseted, pos_ = 9, raw_sql_offset_ = 9, has_minus_ = true,  is_cur_numberic
// = false after seeing '1', is_cur_numeric = true, then param node '-1' is returned
typedef struct _ObMinusStatuCtx {
  int pos_;
  int raw_sql_offset_;
  bool has_minus_;
  bool is_cur_numeric_;
} ObMinusStatusCtx;

#ifdef SQL_PARSER_COMPILATION
// for comment_list_ in ParseResult
typedef struct TokenPosInfo {
  int token_off_;
  int token_len_;
} TokenPosInfo;
#endif

typedef struct {
  void* yyscan_info_;
  ParseNode* result_tree_;
  jmp_buf jmp_buf_;  // handle fatal error
  const char* input_sql_;
  int input_sql_len_;
  void* malloc_pool_;  // ObIAllocator
  int extra_errno_;
  char error_msg_[MAX_ERROR_MSG];
  int start_col_;
  int end_col_;
  int line_;
  int yycolumn_;
  int yylineno_;
  char* tmp_literal_;
  ObQuestionMarkCtx question_mark_ctx_;
  ObSQLMode sql_mode_;
  bool has_encount_comment_;
  /*for faster parser*/
  bool is_fp_;
  /* for multi query fast parse (split queries) */
  bool is_multi_query_;
  char* no_param_sql_;
  int no_param_sql_len_;
  int no_param_sql_buf_len_;
  ParamList* param_nodes_;
  ParamList* tail_param_node_;
  int param_node_num_;
  int token_num_;
  bool is_ignore_hint_;     // used for outline
  bool is_ignore_token_;    // used for outline
  bool need_parameterize_;  // used for outline, to support signature of outline can contain hint
  /*for pl*/
  PLParseInfo pl_parse_info_;
  /*for  q-quote*/
  bool in_q_quote_;
  ObMinusStatusCtx minus_ctx_;  // for fast parser to parse negative value
  bool is_for_trigger_;
  bool is_dynamic_sql_;
  bool is_dbms_sql_;
  bool is_batched_multi_enabled_split_;
  bool is_not_utf8_connection_;
  const struct ObCharsetInfo* charset_info_;
  int last_well_formed_len_;
  bool may_bool_value_;  // used for true/false in sql parser
  int connection_collation_;       // connection collation
  bool mysql_compatible_comment_;  // whether the parser is parsing "/*! xxxx */"

#ifdef SQL_PARSER_COMPILATION
  TokenPosInfo* comment_list_;
  int comment_cnt_;
  int comment_cap_;
  int realloc_cnt_;
  bool stop_add_comment_;
#endif
} ParseResult;

typedef struct _ObFastParseCtx {
  bool is_fp_;
} ObFastParseCtx;

typedef enum ObSizeUnitType {
  SIZE_UNIT_TYPE_INVALID = -1,
  SIZE_UNIT_TYPE_K,
  SIZE_UNIT_TYPE_M,
  SIZE_UNIT_TYPE_G,
  SIZE_UNIT_TYPE_T,
  SIZE_UNIT_TYPE_P,
  SIZE_UNIT_TYPE_E,
  SIZE_UNIT_TYPE_MAX
} ObSizeUnitType;

extern int parse_init(ParseResult* p);
extern int parse_reset(ParseResult* p);
extern int parse_terminate(ParseResult* p);
extern int parse_sql(ParseResult* p, const char* pszSql, size_t iLen);
extern void destroy_tree(ParseNode* pRoot);
extern unsigned char escaped_char(unsigned char c, int* with_back_slash);
extern char* str_tolower(char* buff, int64_t len);
extern char* str_toupper(char* buff, int64_t len);
extern int64_t str_remove_space(char* buff, int64_t len);
// extern int64_t ob_parse_string(const char *src, char *dest, int64_t len, int quote_type);

extern ParseNode* new_node(void* malloc_pool, ObItemType type, int num);
extern ParseNode* new_non_terminal_node(void* malloc_pool, ObItemType node_tag, int num, ...);
extern ParseNode* new_terminal_node(void* malloc_pool, ObItemType type);

/// convert x'42ab' to binary string
void ob_parse_binary(const char* src, int64_t len, char* dest);
int64_t ob_parse_binary_len(int64_t len);

// convert b'10010110' to binary string
// @pre dest buffer is enough
void ob_parse_bit_string(const char* src, int64_t len, char* dest);
int64_t ob_parse_bit_string_len(int64_t len);

// calculate hash value of syntax tree recursively
// @param [in] node         syntax tree root
// @return                  hash value of syntax tree
extern uint64_t parsenode_hash(const ParseNode* node);
// compare syntax tree recursively
// @param [in] node1        first syntax tree
// @param [in] node2        second syntax tree
extern bool parsenode_equal(const ParseNode* node1, const ParseNode* node2);

extern int64_t get_question_mark(ObQuestionMarkCtx* ctx, void* malloc_pool, const char* name);

// compare ParseNode str_value_ to pattern
// @param [in] node        ParseNode
// @param [in] pattern     pattern_str
// @param [in] pat_len     length of pattern
extern bool nodename_equal(const ParseNode* node, const char* pattern, int64_t pat_len);

#define OB_NODE_CAST_TYPE_IDX 0
#define OB_NODE_CAST_COLL_IDX 1
#define OB_NODE_CAST_N_PREC_IDX 2
#define OB_NODE_CAST_N_SCALE_IDX 3
#define OB_NODE_CAST_NUMBER_TYPE_IDX 1
#define OB_NODE_CAST_C_LEN_IDX 1

typedef enum ObNumberParseType {
  NPT_PERC_SCALE = 0,
  NPT_STAR_SCALE,
  NPT_STAR,
  NPT_PERC,
  NPT_EMPTY,
} ObNumberParseType;

#ifndef SQL_PARSER_COMPILATION
bool check_stack_overflow_c();
#endif

typedef struct _ParserLinkNode {
  struct _ParserLinkNode* next_;
  struct _ParserLinkNode* prev_;
  void* val_;
} ParserLinkNode;

ParserLinkNode* new_link_node(void* malloc);

typedef enum ObTranslateCharset {
  TRANSLATE_CHAR_CS = 0,
  TRANSLATE_NCHAR_CS = 1,
} ObTranslateCharset;

#ifdef __cplusplus
}
#endif

#endif  // OCEANBASE_SQL_PARSER_PARSE_NODE_H_
