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

namespace oceanbase {
namespace sql {

struct ObMPParseStat {
public:
  ObMPParseStat() : parse_fail_(false), fail_ret_(common::OB_SUCCESS), fail_query_idx_(-1)
  {}
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

struct PreParseResult {
  common::ObString trace_id_;
};

class ObParser {
public:
  explicit ObParser(common::ObIAllocator& allocator, ObSQLMode mode,
      common::ObCollationType conn_collation = common::CS_TYPE_UTF8MB4_GENERAL_CI);
  virtual ~ObParser();
  /// @param queries Note that all three members of ObString is valid, size() is the length
  ///                of the single statement, length() is the length of remainer statements
  static bool is_pl_stmt(const common::ObString& stmt, bool* is_create_func = NULL);
  bool is_single_stmt(const common::ObString& stmt);
  int split_multiple_stmt(const common::ObString& stmt, common::ObIArray<common::ObString>& queries,
      ObMPParseStat& parse_fail, bool is_ret_first_stmt = false);
  int parse_sql(const common::ObString& stmt, ParseResult& parse_result);

  virtual int parse(const common::ObString& stmt, ParseResult& parse_result, ParseMode mode = STD_MODE,
      const bool is_batched_multi_stmt_split_on = false);

  virtual void free_result(ParseResult& parse_result);
  /**
   * @brief  The parse interface for prepare
   * @param [in] query      - To be parsed statement
   * @param [in] ns      - External namespace
   * @param [out] parse_result  - parse result
   * @retval OB_SUCCESS execute success
   * @retval OB_SOME_ERROR special errno need to handle
   *
   * Compared with general prepare, the external name space is passed in and the redundant process
   * that this path will not go to is simplified. The main idea is that in the process of parser,
   * whenever you encounter a variable in sql, try to find it in the pl namespace. If you can find
   * it, copy the SQL statement before this variable and rewrite this variable as question mark,Also
   * record this variable.At the same time, all objects (tables, views, functions) encountered are
   * also recorded.
   *
   */
  int prepare_parse(const common::ObString& query, void* ns, ParseResult& parse_result);
  static int pre_parse(const common::ObString& stmt, PreParseResult& res);

private:
  static int scan_trace_id(const char* ptr, int64_t len, int32_t& pos, common::ObString& trace_id);
  static bool is_trace_id_end(char ch);
  static bool is_space(char ch);
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObParser);
  // function members
private:
  // data members
  common::ObIAllocator* allocator_;
  // when sql_mode = "ANSI_QUOTES", Treat " as an identifier quote character
  // we don't use it in parser now
  ObSQLMode sql_mode_;
  common::ObCollationType connection_collation_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_PARSER_H */
