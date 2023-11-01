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

#ifndef _OB_PARSER_H
#define _OB_PARSER_H 1
#include "sql/parser/parse_node.h"
#include "sql/parser/parse_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "lib/charset/ob_charset.h"
#include "sql/parser/ob_parser_utils.h"
#include "sql/udr/ob_udr_struct.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
class ObMPParseStat
{
public:
  ObMPParseStat() : parse_fail_(false), fail_ret_(common::OB_SUCCESS), fail_query_idx_(-1) {}
  void reset()
  {
    parse_fail_ = false;
    fail_ret_ = common::OB_SUCCESS;
    fail_query_idx_ = -1;
  }
  bool parse_fail_;
  int fail_ret_;
  int64_t fail_query_idx_;
};

struct PreParseResult
{
  common::ObString trace_id_;
};

class ObParser
{
public:
  explicit ObParser(common::ObIAllocator &allocator, ObSQLMode mode,
                    ObCharsets4Parser charsets4parser = ObCharsets4Parser(),
                    QuestionMarkDefNameCtx *ctx = nullptr);
  virtual ~ObParser();
  /// @param queries Note that all three members of ObString is valid, size() is the length
  ///                of the single statement, length() is the length of remainer statements
  bool is_single_stmt(const common::ObString &stmt);
  int split_start_with_pl(const common::ObString &stmt,
                          common::ObIArray<common::ObString> &queries,
                          ObMPParseStat &parse_fail);
  int split_multiple_stmt(const common::ObString &stmt,
                          common::ObIArray<common::ObString> &queries,
                          ObMPParseStat &parse_fail,
                          bool is_ret_first_stmt=false,
                          bool is_prepare = false);
  void get_single_sql(const common::ObString &stmt, int64_t offset, int64_t remain, int64_t &str_len);

  int check_is_insert(common::ObIArray<common::ObString> &queries, bool &is_ins);
  int reconstruct_insert_sql(const common::ObString &stmt,
                             common::ObIArray<common::ObString> &queries,
                             common::ObIArray<common::ObString> &ins_queries,
                             bool &can_batch_exec);

  //@param:
  //  no_throw_parser_error is used to mark not throw parser error. in the split multi stmt
  //  situation we will try find ';' delimiter to parser part of string in case of save memory,
  //  but this maybe parser error and throw error info. However, we will still try parser remain
  //  string when parse part of string failed, if we throw parse part error info, maybe will let
  //  someone misunderstand have bug, So, we introduce this mark to decide to throw parser error.
  //  eg: select '123;' from dual; select '123' from dual;
  int parse_sql(const common::ObString &stmt,
                ParseResult &parse_result,
                const bool no_throw_parser_error);

  virtual int parse(const common::ObString &stmt,
                    ParseResult &parse_result,
                    ParseMode mode=STD_MODE,
                    const bool is_batched_multi_stmt_split_on = false,
                    const bool no_throw_parser_error = false,
                    const bool is_pl_inner_parse = false,
                    const bool is_dbms_sql = false);

  virtual void free_result(ParseResult &parse_result);
  /**
   * @brief  供prepare使用的parse接口
   * @param [in] query      - 待parse语句
   * @param [in] ns      - 外部名称空间
   * @param [out] parse_result  - parse结果
   * @retval OB_SUCCESS execute success
   * @retval OB_SOME_ERROR special errno need to handle
   *
   * 和通用prepare相比，传入了外部名称空间，并简化了此路径不会走到的冗余流程。
   * 其主要思想是在parser的过程中，每当遇到一个sql中的变量，就尝试去pl名字空间
   * 查找，如果找得到，把这个变量之前的sql语句拷贝出来，并把此变量改写为question mark，
   * 同时记录下这个变量。
   * 同时，把遇到的所有对象（表、视图、函数）也都记录下来。
   *
   */
  int prepare_parse(const common::ObString &query, void *ns, ParseResult &parse_result);
  static int pre_parse(const common::ObString &stmt,
                       PreParseResult &res);

enum State {
  S_START = 0,
  S_COMMENT,
  S_C_COMMENT,
  S_NORMAL,
  S_INVALID,

  S_CREATE,
  S_DO,
  S_DD,
  S_DECLARE,
  S_BEGIN,
  S_DROP,
  S_CALL,
  S_ALTER,
  S_UPDATE,
  S_FUNCTION,
  S_PROCEDURE,
  S_PACKAGE,
  S_TRIGGER,
  S_TYPE,
  S_OR,
  S_REPLACE,
  S_DEFINER,
  S_OF,
  S_EDITIONABLE,
  S_SIGNAL,
  S_RESIGNAL,
  S_FORCE,

  S_EXPLAIN,
  S_EXPLAIN_FORMAT,
  S_EXPLAIN_BASIC,
  S_EXPLAIN_EXTENDED,
  S_EXPLAIN_EXTENDED_NOADDR,
  S_EXPLAIN_PARTITIONS,
  S_SELECT,
  S_INSERT,
  S_DELETE,
  S_VALUES,
  S_TABLE,
  S_INTO,
  // add new states above me
  S_MAX
};

  static State transform_normal(common::ObString &normal);
  static State transform_normal(
    State state, common::ObString &normal, bool &is_pl, bool &is_not_pl,
    bool *is_create_func, bool *is_call_procedure);
  
  static bool is_pl_stmt(const common::ObString &stmt,
                         bool *is_create_func = NULL,
                         bool *is_call_procedure = NULL);
  static bool is_explain_stmt(const common::ObString &stmt,
                              const char *&p_normal_start);
  static bool is_comment(const char *&p,
                         const char *&p_end,
                         State &save_state,
                         State &state,
                         State error_state);
private:
  static int scan_trace_id(const char *ptr,
                           int64_t len,
                           int32_t &pos,
                           common::ObString &trace_id);
  static bool is_trace_id_end(char ch);
  static bool is_space(char ch);
  static int32_t get_well_formed_errlen(const struct ObCharsetInfo *charset_info,
                                        const char *err_begin,
                                        int32_t err_len);
  static void match_state(const char*&p,
                          bool &is_explain,
                          bool &has_error,
                          const char *lower[S_MAX],
                          const char *upper[S_MAX],
                          int &i,
                          State &save_state,
                          State &state,
                          State next_state);
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObParser);
  // function members
private:
  // data members
  common::ObIAllocator *allocator_;
  // when sql_mode = "ANSI_QUOTES", Treat " as an identifier quote character
  // we don't use it in parser now
  ObSQLMode sql_mode_;

  ObCharsets4Parser charsets4parser_;
  QuestionMarkDefNameCtx *def_name_ctx_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_PARSER_H */
