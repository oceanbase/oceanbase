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

#define USING_LOG_PREFIX SQL_PARSER
#include "ob_parser.h"
#include "lib/oblog/ob_log.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "parse_malloc.h"
#include "parse_node.h"
#include "ob_sql_parser.h"
#include "share/ob_define.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObParser::ObParser(common::ObIAllocator& allocator, ObSQLMode mode, ObCollationType conn_collation)
    : allocator_(&allocator), sql_mode_(mode), connection_collation_(conn_collation)
{}

ObParser::~ObParser()
{}

#define ISSPACE(c) ((c) == ' ' || (c) == '\n' || (c) == '\r' || (c) == '\t' || (c) == '\f' || (c) == '\v')

bool ObParser::is_pl_stmt(const ObString& stmt, bool* is_create_func)
{
  UNUSED(stmt);
  UNUSED(is_create_func);
  return false;
}

bool ObParser::is_single_stmt(const ObString& stmt)
{
  int64_t count = 0;
  int64_t end_trim_offset = stmt.length();
  while (end_trim_offset > 0 && ISSPACE(stmt[end_trim_offset - 1])) {
    end_trim_offset--;
  }
  for (int64_t i = 0; i < end_trim_offset; i++) {
    if (';' == stmt[i]) {
      count++;
    }
  }
  return (0 == count || (1 == count && stmt[end_trim_offset - 1] == ';'));
}

// In multi-stmt mode(delimiter #) eg: create t1; create t2; create t3
// in the case of is_ret_first_stmt(false), queries will return the following stmts:
//      queries[0]: create t1; create t2; create t3;
//      queries[1]: create t2; create t3;
//      queries[2]: create t3;
// in the case of is_ret_first_stmt(true) and it will return one element
//      queries[0]: create t1;
// Actually, sql executor only executes the first stmt(create t1) and ignore the others
// even though try to execute stmts like 'create t1; create t2; create t3;'
int ObParser::split_multiple_stmt(
    const ObString& stmt, ObIArray<ObString>& queries, ObMPParseStat& parse_stat, bool is_ret_first_stmt)
{
  int ret = OB_SUCCESS;

  if (is_single_stmt(stmt) || is_pl_stmt(stmt)) {
    ObString query(stmt.length(), stmt.ptr());
    ret = queries.push_back(query);
  } else {
    ParseResult parse_result;
    ParseMode parse_mode = MULTI_MODE;
    int64_t offset = 0;
    int64_t remain = stmt.length();

    parse_stat.reset();
    while (remain > 0 && ISSPACE(stmt[remain - 1])) {
      --remain;
    }
    if (remain > 0 && '\0' == stmt[remain - 1]) {
      --remain;
    }
    while (remain > 0 && ISSPACE(stmt[remain - 1])) {
      --remain;
    }

    if (OB_UNLIKELY(0 >= remain)) {
      ObString part;
      ret = queries.push_back(part);
    }

    int tmp_ret = OB_SUCCESS;
    while (remain > 0 && OB_SUCC(ret) && !parse_stat.parse_fail_) {
      ObArenaAllocator allocator(CURRENT_CONTEXT.get_malloc_allocator());
      allocator.set_label("SplitMultiStmt");
      ObIAllocator* bak_allocator = allocator_;
      allocator_ = &allocator;
      ObString part(remain, stmt.ptr() + offset);
      if (OB_SUCC(tmp_ret = parse(part, parse_result, parse_mode))) {
        // length: length of the remainer statements
        // size: length of the single statement
        int32_t single_stmt_length = parse_result.end_col_;
        if ((!is_ret_first_stmt) && (';' == *(stmt.ptr() + offset + parse_result.end_col_ - 1))) {
          --single_stmt_length;
        }

        if (is_ret_first_stmt) {
          ObString first_query(single_stmt_length, stmt.ptr());
          ret = queries.push_back(first_query);
          break;  // only return the first stmt, so ignore the remaining stmts
        } else {
          ObString query(single_stmt_length, static_cast<int32_t>(remain), stmt.ptr() + offset);
          ret = queries.push_back(query);
        }
        remain -= parse_result.end_col_;
        offset += parse_result.end_col_;
        if (remain < 0 || offset > stmt.length()) {
          LOG_ERROR("split_multiple_stmt data error", K(remain), K(offset), K(stmt.length()), K(ret));
        }
      } else {
        int32_t single_stmt_length = parse_result.end_col_;
        if (';' == *(stmt.ptr() + offset + parse_result.end_col_ - 1)) {
          --single_stmt_length;
        }
        ObString query(single_stmt_length, static_cast<int32_t>(remain), stmt.ptr() + offset);
        ret = queries.push_back(query);
        if (OB_SUCCESS == ret) {
          parse_stat.parse_fail_ = true;
          parse_stat.fail_query_idx_ = queries.count() - 1;
          parse_stat.fail_ret_ = tmp_ret;
        }
        LOG_WARN("fail parse multi part", K(part), K(stmt), K(ret));
      }
      allocator_ = bak_allocator;
    }
  }

  return ret;
}

int ObParser::parse_sql(const ObString& stmt, ParseResult& parse_result)
{
  int ret = OB_SUCCESS;
  ObSQLParser sql_parser(*(ObIAllocator*)(parse_result.malloc_pool_), sql_mode_);
  if (OB_FAIL(sql_parser.parse(stmt.ptr(), stmt.length(), parse_result))) {
    LOG_INFO("failed to parse stmt as sql", K(stmt), K(ret));
  } else if (parse_result.is_dynamic_sql_) {
    memmove(parse_result.no_param_sql_ + parse_result.no_param_sql_len_,
        parse_result.input_sql_ + parse_result.pl_parse_info_.last_pl_symbol_pos_,
        parse_result.input_sql_len_ - parse_result.pl_parse_info_.last_pl_symbol_pos_);
    parse_result.no_param_sql_len_ += parse_result.input_sql_len_ - parse_result.pl_parse_info_.last_pl_symbol_pos_;
  } else { /*do nothing*/
  }
  if (parse_result.is_fp_ || parse_result.is_multi_query_) {
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fast parameterize", K(stmt), K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    auto err_charge_sql_mode = lib::is_oracle_mode();
    LOG_WARN("failed to parse the statement",
        K(stmt),
        K(parse_result.is_fp_),
        K(parse_result.is_multi_query_),
        K(parse_result.yyscan_info_),
        K(parse_result.result_tree_),
        K(parse_result.malloc_pool_),
        "message",
        parse_result.error_msg_,
        "start_col",
        parse_result.start_col_,
        "end_col",
        parse_result.end_col_,
        K(parse_result.line_),
        K(parse_result.yycolumn_),
        K(parse_result.yylineno_),
        K(parse_result.extra_errno_),
        K(err_charge_sql_mode),
        K(sql_mode_),
        K(parse_result.sql_mode_),
        K(parse_result.may_bool_value_),
        K(ret));

    static const int32_t max_error_length = 80;
    int32_t error_length = std::min(stmt.length() - (parse_result.start_col_ - 1), max_error_length);

    if (OB_ERR_PARSE_SQL == ret) {
      LOG_USER_ERROR(OB_ERR_PARSE_SQL,
          ob_errpkt_strerror(OB_ERR_PARSER_SYNTAX, false),
          error_length,
          stmt.ptr() + parse_result.start_col_ - 1,
          parse_result.line_ + 1);
    } else {
      // other errors handle outer side.
    }
  }
  return ret;
}

int ObParser::parse(
    const ObString& query, ParseResult& parse_result, ParseMode parse_mode, const bool is_batched_multi_stmt_split_on)
{
  int ret = OB_SUCCESS;

  int64_t len = query.length();
  while (len > 0 && ISSPACE(query[len - 1])) {
    --len;
  }
  if (MULTI_MODE != parse_mode) {
    if (len > 0 && '\0' == query[len - 1]) {
      --len;
    }
    while (len > 0 && ISSPACE(query[len - 1])) {
      --len;
    }
  }

  const ObString stmt(len, query.ptr());
  parse_reset(&parse_result);
  parse_result.is_fp_ = (FP_MODE == parse_mode || FP_PARAMERIZE_AND_FILTER_HINT_MODE == parse_mode ||
                         FP_NO_PARAMERIZE_AND_FILTER_HINT_MODE == parse_mode);
  parse_result.is_multi_query_ = (MULTI_MODE == parse_mode);
  parse_result.malloc_pool_ = allocator_;
  parse_result.sql_mode_ = sql_mode_;
  parse_result.is_ignore_hint_ =
      (FP_PARAMERIZE_AND_FILTER_HINT_MODE == parse_mode || FP_NO_PARAMERIZE_AND_FILTER_HINT_MODE == parse_mode);
  parse_result.is_ignore_token_ = false;
  parse_result.need_parameterize_ = (FP_MODE == parse_mode || FP_PARAMERIZE_AND_FILTER_HINT_MODE == parse_mode);
  parse_result.pl_parse_info_.is_pl_parse_ = false;
  parse_result.minus_ctx_.has_minus_ = false;
  parse_result.minus_ctx_.pos_ = -1;
  parse_result.minus_ctx_.raw_sql_offset_ = -1;
  parse_result.is_for_trigger_ = (TRIGGER_MODE == parse_mode);
  parse_result.is_dynamic_sql_ = (DYNAMIC_SQL_MODE == parse_mode);
  parse_result.is_dbms_sql_ = (DBMS_SQL_MODE == parse_mode);
  parse_result.is_batched_multi_enabled_split_ = is_batched_multi_stmt_split_on;
  parse_result.charset_info_ = ObCharset::get_charset(connection_collation_);
  parse_result.is_not_utf8_connection_ =
      ObCharset::is_valid_collation(connection_collation_)
          ? (ObCharset::charset_type_by_coll(connection_collation_) != CHARSET_UTF8MB4)
          : false;

  if (parse_result.is_fp_ || parse_result.is_dynamic_sql_) {
    int64_t new_length = parse_result.is_fp_ ? stmt.length() + 1 : stmt.length() * 2;
    char* buf = (char*)parse_malloc(new_length, parse_result.malloc_pool_);
    if (OB_UNLIKELY(NULL == buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory for parser");
    } else {
      parse_result.param_nodes_ = NULL;
      parse_result.tail_param_node_ = NULL;
      parse_result.no_param_sql_ = buf;
      parse_result.no_param_sql_buf_len_ = stmt.length() + 1;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(parse_result.charset_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("charset info is null", K(ret), "connection charset", ObCharset::charset_name(connection_collation_));
  } else {
    LOG_DEBUG("check parse_result param", "connection charset", ObCharset::charset_name(connection_collation_));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_sql(stmt, parse_result))) {
      LOG_WARN("failed to parse stmt as sql", K(stmt), K(parse_mode), K(ret));
    }
  }
  return ret;
}

int ObParser::prepare_parse(const ObString& query, void* ns, ParseResult& parse_result)
{
  int ret = OB_SUCCESS;

  int64_t len = query.length();
  while (len > 0 && ISSPACE(query[len - 1])) {
    --len;
  }
  if (len > 0 && '\0' == query[len - 1]) {
    --len;
  }
  while (len > 0 && ISSPACE(query[len - 1])) {
    --len;
  }

  const ObString stmt(len, query.ptr());

  memset(&parse_result, 0, sizeof(ParseResult));
  parse_result.pl_parse_info_.pl_ns_ = ns;
  parse_result.malloc_pool_ = allocator_;
  parse_result.sql_mode_ = sql_mode_;
  parse_result.pl_parse_info_.is_pl_parse_ = true;
  parse_result.pl_parse_info_.is_pl_parse_expr_ = false;
  char* buf = (char*)parse_malloc(stmt.length() * 2, parse_result.malloc_pool_);
  if (OB_UNLIKELY(NULL == buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory for parser");
  } else {
    parse_result.param_nodes_ = NULL;
    parse_result.tail_param_node_ = NULL;
    parse_result.no_param_sql_ = buf;
    parse_result.no_param_sql_buf_len_ = stmt.length() * 2;
    parse_result.may_bool_value_ = false;
  }

  if (OB_SUCC(ret)) {
    ObSQLParser sql_parser(*(ObIAllocator*)(parse_result.malloc_pool_), sql_mode_);
    if (OB_FAIL(sql_parser.parse(stmt.ptr(), stmt.length(), parse_result))) {
      LOG_WARN("failed to parse the statement",
          K(parse_result.yyscan_info_),
          K(parse_result.result_tree_),
          K(parse_result.malloc_pool_),
          "message",
          parse_result.error_msg_,
          "start_col",
          parse_result.start_col_,
          "end_col",
          parse_result.end_col_,
          K(parse_result.line_),
          K(parse_result.yycolumn_),
          K(parse_result.yylineno_),
          K(parse_result.extra_errno_));

      static const int32_t max_error_length = 80;
      int32_t error_length = std::min(stmt.length() - (parse_result.start_col_ - 1), max_error_length);
      if (OB_ERR_PARSE_SQL == ret) {
        LOG_USER_ERROR(OB_ERR_PARSE_SQL,
            ob_errpkt_strerror(OB_ERR_PARSER_SYNTAX, false),
            error_length,
            stmt.ptr() + parse_result.start_col_ - 1,
            parse_result.line_ + 1);
      } else {
        // other errors handle outer side.
      }
    } else {
      memmove(parse_result.no_param_sql_ + parse_result.no_param_sql_len_,
          parse_result.input_sql_ + parse_result.pl_parse_info_.last_pl_symbol_pos_,
          parse_result.input_sql_len_ - parse_result.pl_parse_info_.last_pl_symbol_pos_);
      parse_result.no_param_sql_len_ += parse_result.input_sql_len_ - parse_result.pl_parse_info_.last_pl_symbol_pos_;
    }
  }
  return ret;
}

int ObParser::pre_parse(const common::ObString& stmt, PreParseResult& res)
{
  int ret = OB_SUCCESS;
  // /*tracd_id=xxx*/
  int64_t len = stmt.length();
  if (len <= 13) {
    // do_nothing
  } else {
    int32_t pos = 0;
    const char* ptr = stmt.ptr();
    while (pos < len && is_space(ptr[pos])) {
      pos++;
    };
    if (pos + 2 < len && ptr[pos++] == '/' && ptr[pos++] == '*') {  // start with '/*'
      char expect_char = 't';
      bool find_trace = false;
      while (!find_trace && pos < len) {
        if (pos < len && ptr[pos] == '*') {
          expect_char = 't';
          if (++pos < len && ptr[pos] == '/') {
            break;
          }
        }
        if (pos >= len) {
          break;
        } else if (ptr[pos] == expect_char ||
                   (expect_char != '_' && expect_char != '=' && ptr[pos] == expect_char - 32)) {
          switch (expect_char) {
            case 't': {
              expect_char = 'r';
              break;
            }
            case 'r': {
              expect_char = 'a';
              break;
            }
            case 'a': {
              expect_char = 'c';
              break;
            }
            case 'c': {
              expect_char = 'e';
              break;
            }
            case 'e': {
              expect_char = '_';
              break;
            }
            case '_': {
              expect_char = 'i';
              break;
            }
            case 'i': {
              expect_char = 'd';
              break;
            }
            case 'd': {
              expect_char = '=';
              break;
            }
            case '=': {
              find_trace = true;
              break;
            }
            default: {
              // do nothing
            }
          }
        } else {
          expect_char = 't';
        }
        pos++;
      }
      if (find_trace) {
        scan_trace_id(ptr, len, pos, res.trace_id_);
      }
    } else {  // not start with '/*'
      // do nothing
    }
  }

  return ret;
}

int ObParser::scan_trace_id(const char* ptr, int64_t len, int32_t& pos, ObString& trace_id)
{
  int ret = OB_SUCCESS;
  trace_id.reset();
  int32_t start = pos;
  int32_t end = pos;
  while (pos < len && !is_trace_id_end(ptr[pos])) {
    pos++;
  }
  end = pos;
  if (start < end) {
    while (pos < len) {
      if (pos < len && ptr[pos] == '*') {
        if (++pos < len && ptr[pos] == '/') {
          int32_t trace_id_len = std::min(static_cast<int32_t>(OB_MAX_TRACE_ID_BUFFER_SIZE), end - start);
          trace_id.assign_ptr(ptr + start, trace_id_len);
          break;
        }
      } else {
        pos++;
      }
    }
  }

  return ret;
}

bool ObParser::is_space(char ch)
{
  return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f';
}

bool ObParser::is_trace_id_end(char ch)
{
  return is_space(ch) || ch == ',' || ch == '*';
}

void ObParser::free_result(ParseResult& parse_result)
{
  UNUSED(parse_result);
  //  destroy_tree(parse_result.result_tree_);
  //  parse_terminate(&parse_result);
  //  parse_result.yyscan_info_ = NULL;
}
