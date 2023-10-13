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
#include "objit/common/ob_item_type.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_ERROR_MSG 1024

struct ObCharsetInfo;

enum SelectParserOffset
{
  PARSE_SELECT_WITH,
  PARSE_SELECT_DISTINCT,
  PARSE_SELECT_SELECT,
  PARSE_SELECT_INTO, //into before from
  PARSE_SELECT_FROM,
  PARSE_SELECT_WHERE,
  PARSE_SELECT_DYNAMIC_SW_CBY, // connect by node or start with node
  PARSE_SELECT_DYNAMIC_CBY_SW, // connect by node or start with node
  PARSE_SELECT_DYNAMIC_GROUP,
  PARSE_SELECT_DYNAMIC_HAVING,
  PARSE_SELECT_NAMED_WINDOWS,
  PARSE_SELECT_SET,
  PARSE_SELECT_FORMER,
  PARSE_SELECT_LATER,
  PARSE_SELECT_ORDER,
  PARSE_SELECT_LIMIT,
  PARSE_SELECT_FOR_UPD,
  PARSE_SELECT_HINTS,
  PARSE_SELECT_WHEN,
  PARSE_SELECT_FETCH,
  PARSE_SELECT_FETCH_TEMP, //use to temporary store fetch clause in parser
  PARSE_SELECT_WITH_CHECK_OPTION,
  PARSE_SELECT_INTO_EXTRA,// ATTENTION!! SELECT_INTO_EXTRA must be the last one
  PARSE_SELECT_MAX_IDX
};

enum GrantParseOffset
{
  PARSE_GRANT_ROLE_LIST,
  PARSE_GRANT_ROLE_GRANTEE,
  PARSE_GRANT_ROLE_OPT_WITH,
  PARSE_GRANT_ROLE_MAX_IDX
};

enum GrantParseSysOffset
{
  PARSE_GRANT_SYS_PRIV_ORACLE_LIST,
  PARSE_GRANT_SYS_PRIV_ORACLE_GRANTEE,
  PARSE_GRANT_SYS_PRIV_ORACLE_OPT_WITH,
  PARSE_GRANT_SYS_PRIV_ORACLE_MAX_IDX
};

enum ParseMode
{
  STD_MODE = 0,
  FP_MODE, /* fast parse,保留hint，且做参数化*/
  MULTI_MODE ,/* multi query ultra-fast parse */
  FP_PARAMERIZE_AND_FILTER_HINT_MODE,/*过滤掉hint，并且做参数化*/
  FP_NO_PARAMERIZE_AND_FILTER_HINT_MODE,/*过滤掉hint，并且不做参数化*/
  TRIGGER_MODE, /* treat ':xxx' as identifier */
  DYNAMIC_SQL_MODE, /*解析动态sql过程中，:idx和:identifier要根据语句类型确定是否检查placeholder的名字*/
  DBMS_SQL_MODE,
  UDR_SQL_MODE,
  INS_MULTI_VALUES,
};

typedef struct
{
  int err_code_;
  char err_msg_[MAX_ERROR_MSG];
} ErrStat;

struct _ParseNode;

typedef struct _ObStmtLoc
{
  int first_column_;
  int last_column_;
  int first_line_;
  int last_line_;
} ObStmtLoc;

enum UdtUdfType
{
  UDT_UDF_UNKNOWN,
  UDT_UDF_CONS = 1,
  UDT_UDF_MEMBER = 2,
  UDT_UDF_STATIC = 4,
  UDT_UDF_MAP = 8,
  UDT_UDF_ORDER = 16,
};

typedef struct _ParseNode
{
  ObItemType type_;
  int32_t num_child_;   /* attributes for non-terninal node, which has children */
  int16_t param_num_; //记录该node对应的原始text中常量的个数, 暂时仅T_CAST_ARGUMENT使用
  union {
    uint32_t flag_;
    struct {
      uint32_t is_neg_ : 1;// 记录常量节点的父节点是否为T_OP_NEG节点, 1表示是, 0 表示不是
      uint32_t is_hidden_const_ : 1; //1 表示某常量正常parse能识别但fast parse不能识别, 0 表示都能识别。
      uint32_t is_tree_not_param_ :1; //1 表示该节点及其子节点常量均不能参数化, 0表示没该限制
      uint32_t length_semantics_  :2; //2 for oracle [char|varbinary] (n b [bytes|char])
      uint32_t is_val_paramed_item_idx_ :1; // T_PROJECT_STRING的values是否是select_item_param_infos数组的下标
      uint32_t is_copy_raw_text_ : 1; // 是否回填常量节点的raw_text_，用于select item常量参数化
      uint32_t is_column_varchar_ : 1; // 投影列是否是一个常量字符串，用于select item常量参数化
      uint32_t is_trans_from_minus_: 1; // 负数常量节点是否是从减号操作转换而来，比如1 - 2，词法阶段会生成一个-2
      uint32_t is_assigned_from_child_: 1; // 常量节点是否由子节点赋值得到，用于处理int64_min
      uint32_t is_num_must_be_pos_: 1; //
      uint32_t is_date_unit_ : 1; //1 表示是date unit常量，在反拼的时候需要反拼为字符串
      uint32_t is_literal_bool_ : 1; // indicate node is a literal TRUE/FALSE
      uint32_t is_empty_ : 1; // 表示是否缺省该节点，1表示缺省，0表示没有缺省, opt_asc_desc节点中使用到
      uint32_t is_multiset_ : 1; // for cast(multiset(...) as ...)
      uint32_t is_forbid_anony_parameter_ : 1; // 1 表示禁止匿名块参数化
      uint32_t is_input_quoted_ : 1; // indicate name_ob input whether with double quote
      uint32_t is_forbid_parameter_ : 1; //1 indicate forbid parameter
      uint32_t reserved_;
    };
  };
  /* attributes for terminal node, it is real value */
  /* 数值类型的node将用到value_来存放其值，但是对于字符串和decimal类型，用str_value来存字符串指针，
   * str_len_表示字符串的长度，不要用strlen(str_value_)来获取str_value_的值，因为str_value_不保证以'\0'结尾.
   * 此外，为什么不将value_和str_len_作为union呢，这是因为在parse
   * 一个数值类型的时候,不仅需要存储其value，还需要存储其原始字符串，举例：select
   * 1111;这种语句，我们不仅要存int value的值，还得存'1111' 字符串*/
  union {
    int64_t value_;
    int32_t int32_values_[2];
    int16_t int16_values_[4];
  };
  const char *str_value_;
  int64_t str_len_;
  union {
    int64_t pl_str_off_; // pl层, 记录str在原始字符串中的起始偏移
    int64_t sql_str_off_; // sql层, 记录str在原始字符串中的起始偏移
  };

  /* 用于存放在词法阶段被特殊处理后丢失的文本串 eg: NULL, Date '2010-10-11',
   * 该文本串在fast parse参数化后，如果该参数作为plan cache中stmtkey的一部分，
   * 则需要使用原始的文本串，而不是丢失文本串后的值，否则会导致plan cache误匹配
   * */
  const char *raw_text_;
  int64_t text_len_;
  int64_t pos_; //记录?在带?的sql中的偏移

  struct _ParseNode **children_; /* attributes for non-terminal node, which has children */
  ObStmtLoc stmt_loc_; //临时放在这里，后面要移到parse_stmt_node.h中去
  union {
    int64_t raw_param_idx_; // 常量节点在fp_result.raw_params_中的下标
    int64_t raw_sql_offset_; // 常量节点在sql中的字符偏移
  };

#ifdef SQL_PARSER_COMPILATION
  int token_off_;
  int token_len_;
#endif
} ParseNode;

struct _ParamList;

typedef struct _ParamList
{
  ParseNode *node_;
  struct _ParamList *next_;
} ParamList;

//供parser使用的外部依赖对象类型
enum RefType
{
  REF_REL = 0,
  REF_PROC,
  REF_FUNC,
};

//外部依赖对象链表
typedef struct _RefObjList
{
  enum RefType type_;
  ParseNode *node_;
  struct _RefObjList *next_;
} RefObjList;

//解析PL中sql语句时需要使用的属性集合
typedef struct _PLParseInfo
{
  bool is_pl_parse_;//用于标识当前parser逻辑是否为PLParse调用
  bool is_pl_parse_expr_; //用于标识当前parser逻辑是否在解析PLParser的expr
  bool is_forbid_pl_fp_;
  bool is_inner_parse_;
  int last_pl_symbol_pos_; //上一个pl变量的结束位置
  int plsql_line_;
  /*for mysql pl*/
  void *pl_ns_; //ObPLBlockNS
  RefObjList *ref_object_nodes_; //依赖对象链表头
  RefObjList *tail_ref_object_node_; //依赖对象链表尾
} PLParseInfo;

//跟@如巅讨论，此处的定义后续会改成动态的，此处先定义128
#define MAX_QUESTION_MARK 128

typedef struct _ObQuestionMarkCtx
{
  char **name_;
  int count_;
  int capacity_;
  bool by_ordinal_;
  bool by_name_;
  bool by_defined_name_;
} ObQuestionMarkCtx;


// record the minus status while parsing the sql
// for example, 'select - -1 from dual'
// when parser sees the first '-', pos_ = 7, raw_sql_offset = 7, has_minus_ = true, is_cur_numeric_ = false
// after seeing the second '-', members are reseted, pos_ = 9, raw_sql_offset_ = 9, has_minus_ = true,  is_cur_numeric = false
// after seeing '1', is_cur_numeric = true, then param node '-1' is returned
typedef struct _ObMinusStatuCtx
{
  int pos_; // 负数在参数化后的sql中出现的位置
  int raw_sql_offset_; // 负号在原始sql中出现的位置
  bool has_minus_; // 保留一下负号的状态，在遇到数值类型的时候，词法返回一个负数节点
  bool is_cur_numeric_; // 当前常量节点是否是数值节点
} ObMinusStatusCtx;

#ifdef SQL_PARSER_COMPILATION
// for comment_list_ in ParseResult
typedef struct TokenPosInfo
{
  int token_off_;
  int token_len_;
} TokenPosInfo;
#endif

//外部依赖对象链表
typedef struct _ParenthesesOffset
{
  int left_parentheses_;
  int right_parentheses_;
  struct _ParenthesesOffset *next_;
} ParenthesesOffset;

//dml base runtime context definition
typedef struct _InsMultiValuesResult
{
  ParenthesesOffset *ref_parentheses_;
  ParenthesesOffset *tail_parentheses_;
  int values_col_;
  int values_count_;
  int on_duplicate_pos_; // the start position of on duplicate key in insert ... on duplicate key update statement
  int ret_code_;
} InsMultiValuesResult;


typedef struct
{
  void *yyscan_info_;
  const char *input_sql_;
  int input_sql_len_;
  int param_node_num_;
  int token_num_;
  void *malloc_pool_; // ObIAllocator
  ObQuestionMarkCtx question_mark_ctx_;
  ObSQLMode sql_mode_;
  const struct ObCharsetInfo *charset_info_; //client charset
  const struct ObCharsetInfo *charset_info_oracle_db_; //oracle DB charset
  ParamList *param_nodes_;
  ParamList *tail_param_node_;
  struct {
    uint32_t has_encount_comment_              : 1;
    uint32_t is_fp_                            : 1;
    uint32_t is_multi_query_                   : 1;
    uint32_t is_ignore_hint_                   : 1;//used for outline
    uint32_t is_ignore_token_                  : 1;//used for outline
    uint32_t need_parameterize_                : 1;//used for outline, to support signature of outline can contain hint
    uint32_t in_q_quote_                       : 1;
    uint32_t is_for_trigger_                   : 1;
    uint32_t is_dynamic_sql_                   : 1;
    uint32_t is_dbms_sql_                      : 1;
    uint32_t is_batched_multi_enabled_split_   : 1;
    uint32_t is_not_utf8_connection_           : 1;
    uint32_t may_bool_value_                   : 1; // used for true/false in sql parser
    uint32_t is_include_old_new_in_trigger_    : 1;
    uint32_t is_normal_ps_prepare_             : 1;
    uint32_t is_multi_values_parser_           : 1;
    uint32_t is_for_udr_                       : 1;
    uint32_t is_for_remap_                     : 1;
    uint32_t contain_sensitive_data_           : 1;
    uint32_t may_contain_sensitive_data_       : 1;
  };

  ParseNode *result_tree_;
  jmp_buf *jmp_buf_;//handle fatal error
  int extra_errno_;
  char *error_msg_;
  int start_col_;
  int end_col_;
  int line_;
  int yycolumn_;
  int yylineno_;
  char *tmp_literal_;
  /* for multi query fast parse (split queries) */
  char *no_param_sql_;
  int no_param_sql_len_;
  int no_param_sql_buf_len_;
  /*for pl*/
  PLParseInfo pl_parse_info_;
  /*for  q-quote*/
  ObMinusStatusCtx minus_ctx_; // for fast parser to parse negative value
  int64_t last_escape_check_pos_;  //解析quoted string%parse-param时的一个临时变量，处理连接gbk字符集时遇到的转义字符问题
  int connection_collation_;//connection collation
  bool mysql_compatible_comment_; //whether the parser is parsing "/*! xxxx */"
  bool enable_compatible_comment_;

  InsMultiValuesResult *ins_multi_value_res_;


#ifdef SQL_PARSER_COMPILATION
  TokenPosInfo *comment_list_;
  int comment_cnt_;
  int comment_cap_;
  int realloc_cnt_;
  bool stop_add_comment_;
#endif
} ParseResult;

typedef struct _ObFastParseCtx
{
  bool is_fp_;
} ObFastParseCtx;

typedef enum ObSizeUnitType
{
  SIZE_UNIT_TYPE_INVALID = -1,
  SIZE_UNIT_TYPE_K,
  SIZE_UNIT_TYPE_M,
  SIZE_UNIT_TYPE_G,
  SIZE_UNIT_TYPE_T,
  SIZE_UNIT_TYPE_P,
  SIZE_UNIT_TYPE_E,
  SIZE_UNIT_TYPE_MAX
} ObSizeUnitType;

extern int parse_init(ParseResult *p);
extern int parse_terminate(ParseResult *p);
extern int parse_sql(ParseResult *p, const char *pszSql, size_t iLen);
extern void destroy_tree(ParseNode *pRoot);
extern unsigned char escaped_char(unsigned char c, int *with_back_slash);
extern char *str_tolower(char *buff, int64_t len);
extern char *str_toupper(char *buff, int64_t len);
extern int64_t str_remove_space(char *buff, int64_t len);
//extern int64_t ob_parse_string(const char *src, char *dest, int64_t len, int quote_type);

extern ParseNode *new_node(void *malloc_pool, ObItemType type, int num);
extern ParseNode *new_non_terminal_node(void *malloc_pool, ObItemType node_tag, int num, ...);
extern ParseNode *new_terminal_node(void *malloc_pool, ObItemType type);

extern int obpl_parser_check_stack_overflow();

int get_deep_copy_size(const ParseNode *node, int64_t *size);
int deep_copy_parse_node(void *malloc_pool, const ParseNode *src, ParseNode *dst);

/// convert x'42ab' to binary string
void ob_parse_binary(const char *src, int64_t len, char* dest);
int64_t ob_parse_binary_len(int64_t len);

// convert b'10010110' to binary string
// @pre dest buffer is enough
void ob_parse_bit_string(const char* src, int64_t len, char* dest);
int64_t ob_parse_bit_string_len(int64_t len);

// calculate hash value of syntax tree recursively
// @param [in] node         syntax tree root
// @return                  hash value of syntax tree
extern uint64_t parsenode_hash(const ParseNode *node, int *ret);
// compare syntax tree recursively
// @param [in] node1        first syntax tree
// @param [in] node2        second syntax tree
extern bool parsenode_equal(const ParseNode *node1, const ParseNode *node2, int *ret);

extern int64_t get_question_mark(ObQuestionMarkCtx *ctx, void *malloc_pool, const char *name);
extern int64_t get_question_mark_by_defined_name(ObQuestionMarkCtx *ctx, const char *name);

// compare ParseNode str_value_ to pattern
// @param [in] node        ParseNode
// @param [in] pattern     pattern_str
// @param [in] pat_len     length of pattern
extern bool nodename_equal(const ParseNode *node, const char *pattern, int64_t pat_len);

#define OB_NODE_CAST_TYPE_IDX 0
#define OB_NODE_CAST_COLL_IDX 1
#define OB_NODE_CAST_N_PREC_IDX 2
#define OB_NODE_CAST_N_SCALE_IDX 3
#define OB_NODE_CAST_NUMBER_TYPE_IDX 1
#define OB_NODE_CAST_C_LEN_IDX 1
#define OB_NODE_CAST_GEO_TYPE_IDX 1

typedef enum ObNumberParseType
{
  NPT_PERC_SCALE = 0,
  NPT_STAR_SCALE,
  NPT_STAR,
  NPT_PERC,
  NPT_EMPTY,
} ObNumberParseType;

#ifndef SQL_PARSER_COMPILATION
bool check_stack_overflow_c();
//查找外部pl变量的接口，获取变量在外部符号表中的下标，定义在ob_pl_stmt.cpp中
int lookup_pl_symbol(const void *pl_ns, const char *symbol, size_t len, int64_t *find_idx);
#endif

typedef struct _ParserLinkNode
{
  struct _ParserLinkNode *next_;
  struct _ParserLinkNode *prev_;
  void *val_;
} ParserLinkNode;

ParserLinkNode *new_link_node(void *malloc);

typedef enum ObTranslateCharset
{
  TRANSLATE_CHAR_CS = 0,
  TRANSLATE_NCHAR_CS = 1,
} ObTranslateCharset;

#ifdef __cplusplus
}
#endif

#endif //OCEANBASE_SQL_PARSER_PARSE_NODE_H_
