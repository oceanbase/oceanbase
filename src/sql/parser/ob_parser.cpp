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
#include "pl/parser/ob_pl_parser.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/json/ob_json_print_utils.h"
using namespace oceanbase::pl;
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObParser::ObParser(common::ObIAllocator &allocator,
                   ObSQLMode mode,
                   ObCharsets4Parser charsets4parser,
                   QuestionMarkDefNameCtx *ctx)
    :allocator_(&allocator),
     sql_mode_(mode),
     charsets4parser_(charsets4parser),
     def_name_ctx_(ctx)
{}

ObParser::~ObParser()
{}

#define ISSPACE(c) ((c) == ' ' || (c) == '\n' || (c) == '\r' || (c) == '\t' || (c) == '\f' || (c) == '\v')

bool ObParser::is_pl_stmt(const ObString &stmt, bool *is_create_func, bool *is_call_procedure)
{
  int ret = OB_SUCCESS;

  State state = S_START;
  State save_state = state;

  const char *p = stmt.ptr();
  const char *p_start = p;
  const char *p_end = p + stmt.length();
  bool is_pl = false;
  bool is_not_pl = false;
  const char *p_normal_start = nullptr;

  while (p < p_end && !is_pl && !is_not_pl) {
    switch (state) {
      case S_START:
      case S_CREATE:
      case S_DD:
      case S_BEGIN:
      case S_DROP:
      case S_ALTER:
      case S_UPDATE: {
        if (ISSPACE(*p)) {
          p++;
        } else {
          if ('-' == *p) {
            if ((p + 1) < p_end && '-' == *(p + 1)) {
              save_state = state;
              state = S_COMMENT;
              p += 2;
            }
          } else if ('/' == *p) {
            if ((p + 1) < p_end && '*' == *(p + 1)) {
              save_state = state;
              state = S_C_COMMENT;
              p += 2;
            }
          } else if ('#' == *p && lib::is_mysql_mode()) {
            save_state = state;
            state = S_COMMENT;
            p += 1;
          }
          if (state != S_COMMENT && state != S_C_COMMENT) {
            p_normal_start = p;
            save_state = state;
            state = S_NORMAL;
            p++;
          }
        }
      } break;
      case S_COMMENT: {
        if (*p == '\n') {
          state = save_state;
        }
        p++;
      } break;
      case S_C_COMMENT: {
        if (*p == '*') {
          if ((p + 1 < p_end) && '/' == *(p + 1)) {
            state = save_state;
            p++;
          }
        }
        p++;
      } break;
      case S_NORMAL: {
        if (ISSPACE(*p) || (p == (p_end - 1))) {
          int64_t length = (p - p_normal_start) + (ISSPACE(*p) ? 0 : 1);
          ObString normal(length, p_normal_start);
          state = transform_normal(
            save_state, normal, is_pl, is_not_pl, is_create_func, is_call_procedure);
        }
        p++;
      } break;
      default: {
        is_not_pl = true;
      } break;
    }
  }
  return is_pl;
}

// Check if stmt is explain statement.
// On success, set p_normal_start to first char after explain syntax; on error, set p_normal_start to null.
//
// For example, call is_explain_stmt() with SQL query below would return true,
// and p_normal_start would be set as shown below.
//
//   explain update t1 set b=0 where a=1;update t1 set b=0 where a=2;
//           ^
//           p_normal_start
//
// Note that, when using is_explain_stmt(), we assume given SQL query has no syntax errors.
bool ObParser::is_explain_stmt(const ObString &stmt, const char *&p_normal_start)
{
  State state = S_START;
  State save_state = state;

  const char *p = stmt.ptr();
  const char *p_start = p;
  const char *p_end = p + stmt.length();
  bool is_explain = false;
  bool has_error = false;
  p_normal_start = nullptr;
  int i = 0;

  static const char *lower[S_MAX] = { nullptr };
  static const char *upper[S_MAX] = { nullptr };
  static bool inited = false;

  if (!inited) {
    lower[S_EXPLAIN] = "explain";
    lower[S_EXPLAIN_BASIC] = "basic";
    lower[S_EXPLAIN_EXTENDED] = "extended";
    lower[S_EXPLAIN_EXTENDED_NOADDR] = "extended_noaddr";
    lower[S_EXPLAIN_PARTITIONS] = "partitions";

    upper[S_EXPLAIN] = "EXPLAIN";
    upper[S_EXPLAIN_BASIC] = "BASIC";
    upper[S_EXPLAIN_EXTENDED] = "EXTENDED";
    upper[S_EXPLAIN_EXTENDED_NOADDR] = "EXTENDED_NOADDR";
    upper[S_EXPLAIN_PARTITIONS] = "PARTITIONS";
    inited = true;
  }

  while (p < p_end && !has_error) {
    switch (state) {
      case S_START:
      case S_EXPLAIN_FORMAT: {
        if (ISSPACE(*p)) {
          p++;
        } else {
          if (!is_comment(p, p_end, save_state, state, S_INVALID) && state != S_INVALID) {
            if (S_START == state) {
              // p is at first key token (e.g. not comments or white spaces),
              // move to S_EXPLAIN to check if there is 'explain' keyword
              save_state = state;
              state = S_EXPLAIN;
            } else if (S_EXPLAIN_FORMAT == state) {
              if (lower[S_EXPLAIN_BASIC][0] == *p || upper[S_EXPLAIN_BASIC][0] == *p) {
                save_state = state;
                state = S_EXPLAIN_BASIC;
              } else if (lower[S_EXPLAIN_EXTENDED][0] == *p || upper[S_EXPLAIN_EXTENDED][0] == *p) {
                save_state = state;
                state = S_EXPLAIN_EXTENDED;
              } else if (lower[S_EXPLAIN_PARTITIONS][0] == *p || upper[S_EXPLAIN_PARTITIONS][0] == *p) {
                save_state = state;
                state = S_EXPLAIN_PARTITIONS;
              } else {
                save_state = state;
                state = S_NORMAL;
              }
            }
          }
        }
      } break;
      // since we have assumed SQL has no syntax errors,
      // we will always find end tokens (e.g. '\n', '*/') for comments.
      case S_COMMENT: {
        if (*p == '\n') {
          // end of '--' comments
          state = save_state;
        }
        p++;
      } break;
      case S_C_COMMENT: {
        if (*p == '*') {
          if ((p + 1 < p_end) && '/' == *(p + 1)) {
            // end of '/**/' comments
            state = save_state;
            p++;
          }
        }
        p++;
      } break;
      case S_EXPLAIN:
      case S_EXPLAIN_BASIC:
      case S_EXPLAIN_EXTENDED:
      case S_EXPLAIN_PARTITIONS: {
        State next_state = S_NORMAL;
        if (S_EXPLAIN == state) {
          // after checking 'explain' keyword,
          // we should move to S_EXPLAIN_FORMAT to check extra keywords such as 'basic'
          next_state = S_EXPLAIN_FORMAT;
        } else if (S_EXPLAIN_EXTENDED == state) {
          // after checking 'extended' keyword,
          // we should move to S_EXPLAIN_EXTENDED_NOADDR to check if 'extended_noaddr' matches
          next_state = S_EXPLAIN_EXTENDED_NOADDR;
        }
        match_state(p,
                    is_explain,
                    has_error,
                    lower,
                    upper,
                    i,
                    save_state,
                    state,
                    next_state);
      } break;
      case S_EXPLAIN_EXTENDED_NOADDR: {
        if (i == 0) {
          // we have just finished checking 'extended' keyword,
          // continue checking only if next letter is '_'
          if ('_' == *p) {
            i = strlen(lower[S_EXPLAIN_EXTENDED]);
          } else {
            state = S_NORMAL;
          }
        } else {
          match_state(p,
                      is_explain,
                      has_error,
                      lower,
                      upper,
                      i,
                      save_state,
                      state,
                      S_NORMAL);
        }
      } break;
      case S_NORMAL: {
        // we have finished checking keywords,
        // find start of first non-space and non-comment char
        if (ISSPACE(*p)) {
          p++;
        } else if (!is_comment(p, p_end, save_state, state, S_NORMAL)) {
          p_normal_start = p;
          p = p_end;
        }
      } break;
      case S_INVALID:
      default: {
        is_explain = false;
        has_error = true;
      } break;
    }
  }
  return is_explain && !has_error;
}

// Check if p matches keyword at index i.
// When all letters have matched, set is_explain=true if there hasn't been any errors
// On success, move state to next_state; on error, move state to S_INVALID.
//
// Note that, when using match_state(), we assume index i is valid.
void ObParser::match_state(const char*&p,
                           bool &is_explain,
                           bool &has_error,
                           const char *lower[S_MAX],
                           const char *upper[S_MAX],
                           int &i,
                           State &save_state,
                           State &state,
                           State next_state)
{
  if (lower[state][i] == *p || upper[state][i] == *p) {
    i++;
    p++;
    if (i == strlen(lower[state])) {
      is_explain = !has_error;
      save_state = state;
      state = next_state;
      i = 0;
    }
  } else {
    save_state = state;
    state = S_INVALID;
  }
}

// Check if comments start at p.
bool ObParser::is_comment(const char *&p,
                          const char *&p_end,
                          State &save_state,
                          State &state,
                          State error_state)
{
  bool found_comment = false;
  if ('-' == *p) {
    if ((p + 1) < p_end && '-' == *(p + 1)) {
      save_state = state;
      state = S_COMMENT;
      p += 2;
      found_comment = true;
    } else {
      state = error_state;
    }
  } else if ('/' == *p) {
    if ((p + 1) < p_end && '*' == *(p + 1)) {
      save_state = state;
      state = S_C_COMMENT;
      p += 2;
      found_comment = true;
    } else {
      state = error_state;
    }
  }
  return found_comment;
}

ObParser::State ObParser::transform_normal(ObString &normal)
{
  State state = S_INVALID;

#define IF(len, s, str) \
  if (len == normal.length() && 0 == STRNCASECMP(normal.ptr(), str, len)) { \
    state = s; \
  }
#define ELSIF(len, s, str) \
  else if (len == normal.length() && 0 == STRNCASECMP(normal.ptr(), str, len)) { \
    state = s; \
  }
#define ELSE() \
  else { \
    LOG_DEBUG("transform_normal", K(state), K(normal)); \
  }

  IF(6, S_CREATE, "create")
  ELSIF(8, S_FUNCTION, "function")
  ELSIF(9, S_PROCEDURE, "procedure")
  ELSIF(7, S_PACKAGE, "package")
  ELSIF(7, S_TRIGGER, "trigger")
  ELSIF(4, S_TYPE, "type")
  ELSIF(2, S_OR, "or")
  ELSIF(7, S_REPLACE, "replace")
  ELSIF(7, S_DEFINER, "definer")
  ELSIF(2, S_DO, "do")
  ELSIF(2, S_DD, "dd")
  ELSIF(5, S_BEGIN, "begin")
  ELSIF(7, S_DECLARE, "declare")
  ELSIF(2, S_DECLARE, "<<")
  ELSIF(4, S_DROP, "drop")
  ELSIF(4, S_CALL, "call")
  ELSIF(5, S_ALTER, "alter")
  ELSIF(6, S_UPDATE, "update")
  ELSIF(2, S_OF, "of")
  ELSIF(11, S_EDITIONABLE, "editionable")
  ELSIF(14, S_EDITIONABLE, "noneditionable")
  ELSIF(6, S_SIGNAL, "signal")
  ELSIF(8, S_RESIGNAL, "resignal")
  ELSIF(5, S_FORCE, "force")
  ELSE()

  if (S_INVALID == state
      && normal.length() >= 2 && 0 == STRNCASECMP(normal.ptr(), "<<", 2)) {
    state = S_DECLARE;
  }
  if (S_INVALID == state
      && normal.length() >= 7 && 0 == STRNCASECMP(normal.ptr(), "definer", 7)) {
    state = S_DEFINER;
  }

#undef IF
#undef ELSIF
#undef ELSE
  return state;
}

ObParser::State ObParser::transform_normal(
  ObParser::State state, ObString &normal, bool &is_pl, bool &is_not_pl,
  bool *is_create_func, bool *is_call_procedure)
{
  switch (state) {
    case S_START: {
      State token = transform_normal(normal);
      switch (token) {
        case S_DO:
        case S_DECLARE:
        case S_PROCEDURE:
        case S_FUNCTION:
        case S_PACKAGE:
        case S_TRIGGER:
        case S_TYPE:
        case S_SIGNAL:
        case S_RESIGNAL: {
          is_pl = true;
        } break;
        case S_CALL: {
          is_not_pl = lib::is_oracle_mode();
          is_pl = lib::is_mysql_mode();
          if (is_call_procedure != NULL) {
            *is_call_procedure = true;
          }
        } break;
        case S_CREATE:
        case S_DD:
        case S_BEGIN:
        case S_DROP:
        case S_ALTER:
        case S_UPDATE: {
          state = token;
        } break;
        case S_INVALID:
        default: {
          is_not_pl = true;
        } break;
      }
    } break;
    case S_CREATE: {
      State token = transform_normal(normal);
      switch (token) {
        case S_PROCEDURE:
        case S_PACKAGE:
        case S_TRIGGER:
        case S_TYPE:
        case S_DEFINER: {
          is_pl = true;
        } break;
        case S_OR:
        case S_REPLACE:
        case S_EDITIONABLE:
        case S_FORCE: {
          // do nothing ...
        } break;
        case S_FUNCTION: {
          is_pl = true;
          if (is_create_func != NULL) {
            *is_create_func = true;
          }
        } break;
        default: {
          is_not_pl = true;
        } break;
      }
    } break;
    case S_DD: {
      if (S_BEGIN == transform_normal(normal)) {
        is_pl = true;
      } else {
        is_not_pl = true;
      }
    } break;
    case S_BEGIN: {
      if ((normal.length() > 0 && ';' == (normal.ptr()[0]))
          || (normal.length() >= 4 && 0 == STRNCASECMP(normal.ptr(), "work", 4))) {
        is_not_pl = true;
      } else {
        is_pl = true;
      }
    } break;
    case S_DROP:
    case S_ALTER: {
      State token = transform_normal(normal);
      if (S_PROCEDURE == token || S_FUNCTION == token
          || S_PACKAGE == token || S_TRIGGER == token || S_TYPE == token) {
        is_pl = true;
      } else {
        is_not_pl = true;
      }
    } break;
    case S_UPDATE: {
      if (S_OF == transform_normal(normal)) {
        is_pl = true;
      } else {
        is_not_pl = true;
      }
    } break;
    default: {
      is_not_pl = true;
      LOG_WARN_RET(common::OB_ERR_UNEXPECTED, "unexpected state", K(state));
    } break;
  }
  return state;
}

bool ObParser::is_single_stmt(const ObString &stmt)
{
  int64_t count = 0;
  // 去除尾部多余空格，否则 ‘select 1 from dual;   ’ 这种会出错
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

int ObParser::split_start_with_pl(const ObString &stmt,
                                  ObIArray<ObString> &queries,
                                  ObMPParseStat &parse_stat)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t remain = stmt.length();
  ParseResult parse_result;
  ParseMode parse_mode = MULTI_MODE;
  parse_stat.reset();
  // 绕过parser对空查询处理不友好的方法：自己把末尾空格去掉
  while (remain > 0 && ISSPACE(stmt[remain - 1])) {
    --remain;
  }
  //去除末尾一个‘\0’, 为与mysql兼容
  if (remain > 0 && '\0' == stmt[remain - 1]) {
    --remain;
  }
  //再删除末尾空格
  while (remain > 0 && (ISSPACE(stmt[remain - 1]))) {
    --remain;
  }

  // 对于空语句的特殊处理
  if (OB_UNLIKELY(0 >= remain)) {
    ObString part; // 空串
    ret = queries.push_back(part);
  }

  if (remain > 0 && OB_SUCC(ret) && !parse_stat.parse_fail_) {
    ObArenaAllocator allocator(CURRENT_CONTEXT->get_malloc_allocator());
    allocator.set_label("PLSplitStmt");
    ObIAllocator *bak_allocator = allocator_;
    allocator_ = &allocator;
    ObString part(remain, stmt.ptr());

    if (OB_FAIL(tmp_ret = parse(part, parse_result, parse_mode, false, true))) {
      ret = queries.push_back(part);
      if (OB_SUCCESS == ret) {
        parse_stat.parse_fail_ = true;
        parse_stat.fail_query_idx_ = queries.count() - 1;
        parse_stat.fail_ret_ = tmp_ret;
      }
      LOG_WARN("fail parse multi part", K(part), K(stmt), K(ret));
    } else {
      CK(remain == parse_result.end_col_);
      CK(nullptr != bak_allocator);
      CK(nullptr != parse_result.result_tree_);
      for (int64_t i = 0; OB_SUCC(ret) && i < parse_result.result_tree_->num_child_; ++i) {
        int64_t str_len = parse_result.result_tree_->children_[i]->str_len_;
        int64_t offset = parse_result.result_tree_->children_[i]->pos_;
        ObString query(str_len, stmt.ptr() + offset);
        OZ(queries.push_back(query));
      }
    }
    allocator_ = bak_allocator;
  }
  return ret;
}

// In multi-stmt mode(delimiter #) eg: create t1; create t2; create t3
// in the case of is_ret_first_stmt(false), queries will return the following stmts:
//      queries[0]: create t1;
//      queries[1]: create t2;
//      queries[2]: create t3;
// in the case of is_ret_first_stmt(true) and it will return one element
//      queries[0]: create t1;
// Actually, sql executor only executes the first stmt(create t1) and ignore the others
// even though try to execute stmts like 'create t1; create t2; create t3;'
int ObParser::split_multiple_stmt(const ObString &stmt,
                                  ObIArray<ObString> &queries,
                                  ObMPParseStat &parse_stat,
                                  bool is_ret_first_stmt,
                                  bool is_prepare)
{
  int ret = OB_SUCCESS;
  //对于单条sql的场景，先判断is_single_stmt，然后再判断is_pl_stmt，避免is_pl_stmt字符串比较的开销
  if (is_single_stmt(stmt)) {
    ObString query(stmt.length(), stmt.ptr());
    ret = queries.push_back(query);
  } else if (is_pl_stmt(stmt)) {
    if (lib::is_mysql_mode()) {
      ret = split_start_with_pl(stmt, queries, parse_stat);
    } else {
      ObString query(stmt.length(), stmt.ptr());
      ret = queries.push_back(query);
    }
  } else {
    ParseResult parse_result;
    ParseMode parse_mode = MULTI_MODE;
    int64_t offset = 0;
    int64_t remain = stmt.length();

    parse_stat.reset();
    // 绕过parser对空查询处理不友好的方法：自己把末尾空格去掉
    while (remain > 0 && ISSPACE(stmt[remain - 1])) {
      --remain;
    }
    //去除末尾一个‘\0’, 为与mysql兼容
    if (remain > 0 && '\0' == stmt[remain - 1]) {
      --remain;
    }
    //再删除末尾空格
    while (remain > 0 && ((ISSPACE(stmt[remain - 1])) ||
           (lib::is_mysql_mode() && is_prepare && stmt[remain - 1] == ';'))) {
      --remain;
    }

    // 对于空语句的特殊处理
    if (OB_UNLIKELY(0 >= remain)) {
      ObString part; // 空串
      ret = queries.push_back(part);
    }

    int tmp_ret = OB_SUCCESS;
    bool need_continue = true;
    while (remain > 0 && OB_SUCC(ret) && !parse_stat.parse_fail_ && need_continue) {
      ObArenaAllocator allocator(CURRENT_CONTEXT->get_malloc_allocator());
      allocator.set_label("SplitMultiStmt");
      ObIAllocator *bak_allocator = allocator_;
      allocator_ = &allocator;
      int64_t str_len = 0;

      //for save memory allocate in parser, we need try find the single stmt length in advance
      
      //calc the end position of a single sql.
      get_single_sql(stmt, offset, remain, str_len);

      str_len = str_len == remain ? str_len : str_len + 1;
      ObString part(str_len, stmt.ptr() + offset);
      ObString remain_part(remain, stmt.ptr() + offset);
      //first try parse part str, because it's have less length and need less memory
      if (OB_FAIL(tmp_ret = parse(part, parse_result, parse_mode, false, true))) {
        //if parser part str failed, then try parse all remain part, avoid parse many times
        //bug:
        tmp_ret = OB_SUCCESS;
        tmp_ret = parse(remain_part, parse_result, parse_mode);
      }
      if (OB_SUCC(tmp_ret)) {
        int32_t single_stmt_length = parse_result.end_col_;
        if (is_ret_first_stmt) {
          ObString first_query(single_stmt_length, stmt.ptr());
          ret = queries.push_back(first_query);
          need_continue = false; // only return the first stmt, so ignore the remaining stmts
        } else {
          if (is_pl_stmt(part) && lib::is_mysql_mode()) {
            ObString query(remain, stmt.ptr() + offset);
            allocator_ = bak_allocator;
            ret = split_start_with_pl(query, queries, parse_stat);
            need_continue = false;
          } else {
            ObString query(single_stmt_length,stmt.ptr() + offset);
            ret = queries.push_back(query);
          }
        }
        remain -= parse_result.end_col_;
        offset += parse_result.end_col_;
        if (remain < 0 || offset > stmt.length()) {
          LOG_ERROR("split_multiple_stmt data error",
                    K(remain), K(offset), K(stmt.length()), K(ret));
        }
      } else {
        ObString query(static_cast<int32_t>(remain), stmt.ptr() + offset);
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

int ObParser::check_is_insert(common::ObIArray<common::ObString> &queries, bool &is_ins)
{
  int ret = OB_SUCCESS;
  is_ins = false;
  bool is_replace = false;
  if (queries.count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("queries is unexpected", K(ret));
  } else if (queries.count() > 1) {
    // query个数大于1，multi query 不做优化
  } else {
    ObString &sql_str = queries.at(0);
    is_ins = (sql_str.length() > 6 && 0 == STRNCASECMP(sql_str.ptr(), "insert", 6));
    is_replace = (sql_str.length() > 7 && 0 == STRNCASECMP(sql_str.ptr(), "replace", 7));
    is_ins = is_ins | is_replace;
  }
  return ret;
}

int ObParser::reconstruct_insert_sql(const common::ObString &stmt,
                                     common::ObIArray<common::ObString> &queries,
                                     common::ObIArray<common::ObString> &ins_queries,
                                     bool &can_batch_exec)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ParseResult parse_result;
  ParseMode parse_mode = INS_MULTI_VALUES;
  int64_t strlen = stmt.length();
  bool is_insert = false;
  can_batch_exec = false;
  if (OB_FAIL(check_is_insert(queries, is_insert))) {
    LOG_WARN("fail to check is insert", K(ret));
  }
  if (OB_SUCC(ret) && is_insert) {
    ObArenaAllocator allocator(CURRENT_CONTEXT->get_malloc_allocator());
    allocator.set_label("InsMultiValOpt");
    ObIAllocator *bak_allocator = allocator_;
    allocator_ = &allocator;
    if (OB_FAIL(parse(stmt, parse_result, parse_mode, false, true))) {
      // if parser SQL failed，then we won't rewrite it and keep it as it is
      LOG_WARN("failed to parser insert sql", K(ret), K(stmt));
    } else if (parse_result.ins_multi_value_res_->values_count_ == 1) {
      // only one set of values，not need rewrite
    } else if (OB_ISNULL(bak_allocator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bak_allocator is null", K(ret));
    } else {
      // restore the allocator
      allocator_ = bak_allocator;
      LOG_DEBUG("after do reconstruct sql, result is",
          K(parse_result.ins_multi_value_res_->on_duplicate_pos_),
          K(parse_result.ins_multi_value_res_->values_col_),
          K(parse_result.ins_multi_value_res_->values_count_),
          K(stmt));
      bool is_on_duplicate = (parse_result.ins_multi_value_res_->on_duplicate_pos_ > 0);
      int64_t on_duplicate_length = stmt.length() - parse_result.ins_multi_value_res_->on_duplicate_pos_;
      for (ParenthesesOffset *current_obj = parse_result.ins_multi_value_res_->ref_parentheses_;
             OB_SUCC(ret) && NULL != current_obj;
             current_obj = current_obj->next_) {
        char *new_sql_buf = NULL;
        int64_t pos = 0;
        const int64_t shared_length = parse_result.ins_multi_value_res_->values_col_;
        // The reason for +1 here is
        // insert into t1 values(1,1),(2,2);
        // left_parentheses_ is the offset of the first left parenthesis，
        // right_parentheses_ Is the offset after the end of the last matched closing bracket
        // right_parentheses_ - left_parentheses_，not contain the length of the left parenthesis, so the length + 1
        const int64_t values_length = current_obj->right_parentheses_ - current_obj->left_parentheses_ + 1;
        // The reason for here + 1 is to add a delimiter;
        int64_t final_length = shared_length + values_length + 1; // 这个+1的原因是 为了后边的';'
        if (is_on_duplicate) {
          // for the on_duplicate_key clause of insert_up
          final_length = final_length + on_duplicate_length;
        }
        if (OB_ISNULL(new_sql_buf = static_cast<char*>(allocator_->alloc(final_length)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret), K(final_length));
        } else if (OB_FAIL(databuff_memcpy(new_sql_buf, final_length, pos, shared_length, stmt.ptr()))) {
          LOG_WARN("failed to deep copy new sql", K(ret), K(final_length), K(shared_length), K(pos), K(stmt));
        } else if (OB_FAIL(databuff_memcpy(new_sql_buf, final_length, pos, values_length, (stmt.ptr() + (current_obj->left_parentheses_ - 1))))) {
          LOG_WARN("failed to deep copy member list buf", K(ret), K(final_length), K(pos), K(stmt), K(values_length));
        } else if (is_on_duplicate && OB_FAIL(databuff_memcpy(new_sql_buf,
                                                              final_length,
                                                              pos,
                                                              on_duplicate_length,
                                                              stmt.ptr() + parse_result.ins_multi_value_res_->on_duplicate_pos_))) {

        } else {
          new_sql_buf[final_length - 1] = ';';
          ObString part(final_length, new_sql_buf);
          if (OB_FAIL(ins_queries.push_back(part))) {
            LOG_WARN("fail to push back query str", K(ret), K(part));
          } else {
            LOG_DEBUG("after rebuild multi_query sql is", K(part));
          }
        }
      }
    }

    if (OB_SUCC(ret) && ins_queries.count() > 1) {
      can_batch_exec = true;
    }
    // No matter whether the previous operation is successful or not,
    // the allocator should be restored here
    allocator_ = bak_allocator;
  }
  LOG_TRACE("after reconstruct ins sql", K(ret), K(can_batch_exec), K(ins_queries), K(stmt));
  return OB_SUCCESS;
}

int32_t ObParser::get_well_formed_errlen(const ObCharsetInfo *charset_info,
                                         const char *err_ptr,
                                         int32_t err_len)
{
  static const int32_t max_error_length = 80;
  err_len = std::min(err_len, max_error_length);

  int32_t res_len = err_len;
  //issue/29914471
  if (OB_NOT_NULL(charset_info)) {
    int err = 0;
    int32_t well_formed_errlen =
        static_cast<int32_t>(charset_info->cset->well_formed_len(charset_info,
                                                                 err_ptr,
                                                                 err_ptr + err_len,
                                                                 UINT64_MAX,
                                                                 &err));
    LOG_DEBUG("check well_formed_errlen", K(well_formed_errlen), K(err_len),
              K(err), K(charset_info->name));
    if (err != 0
        && well_formed_errlen < err_len
        && err_len - well_formed_errlen < charset_info->mbmaxlen) {
      res_len = well_formed_errlen;
    }
  }
  return res_len;
}
// avoid separating sql by semicolons in quotes or comment.
void ObParser::get_single_sql(const common::ObString &stmt, int64_t offset, int64_t remain, int64_t &str_len) {
  /* following two flags are used to mark wether we are in comment, if in comment, ';' can't be used to split sql*/
  // in -- comment
  bool comment_flag = false;
  // in /*! comment */ or /* comment */
  bool c_comment_flag = false;
  /* following three flags are used to mark wether we are in quotes.*/
  // in '', single quotes
  bool sq_flag = false;
  // in "", double quotes
  bool dq_flag = false;
  // in ``, backticks.
  bool bt_flag = false;
  bool is_escape = false;

  bool in_comment = false;
  bool in_string  = false;
  while (str_len < remain && (in_comment || (stmt[str_len + offset] != ';'))) {
    if (!in_comment && !in_string) {
      if (str_len + 1 >= remain) {
      } else if ((stmt[str_len + offset] == '-' && stmt[str_len + offset + 1] == '-') || stmt[str_len + offset + 1] == '#') {
        comment_flag = true;
      } else if (stmt[str_len + offset] == '/' && stmt[str_len + offset + 1] == '*') {
        c_comment_flag = true;
      } else if (stmt[str_len + offset] == '\'') {
        sq_flag = true;
      } else if (stmt[str_len + offset] == '"') {
        dq_flag = true;
      } else if (stmt[str_len + offset] == '`') {
        bt_flag = true;
      }
    } else if (in_comment) {
      if (comment_flag) {
        if (stmt[str_len + offset] == '\r' || stmt[str_len + offset] == '\n') {
          comment_flag = false;
        }
      } else if (c_comment_flag) {
        if (str_len + 1 >= remain) {

        } else if (stmt[str_len + offset] == '*' && (str_len + 1 < remain) && stmt[str_len + offset + 1] == '/') {
          c_comment_flag = false;
        }
      }
    } else if (in_string) {
      if (str_len + 1 >= remain) {
      } else if (lib::is_mysql_mode() && !bt_flag && stmt[str_len + offset] == '\\') {
        // in mysql mode, handle the escape char in '' and ""
        ++ str_len;
      } else if (sq_flag) {
        if (stmt[str_len + offset] == '\'') {
          sq_flag = false;
        }
      } else if (dq_flag) {
        if (stmt[str_len + offset] == '"') {
          dq_flag = false;
        }
      } else if (bt_flag) {
        if (stmt[str_len + offset] == '`') {
          bt_flag = false;
        }
      }
    }
    ++ str_len;
    
    // update states.
    in_comment = comment_flag || c_comment_flag;
    in_string = sq_flag || bt_flag || dq_flag;
  }
}
int ObParser::parse_sql(const ObString &stmt,
                        ParseResult &parse_result,
                        const bool no_throw_parser_error)
{
  int ret = OB_SUCCESS;
  ObSQLParser sql_parser(*(ObIAllocator*)(parse_result.malloc_pool_), sql_mode_);
  ObString stmt_str = stmt;
  if (OB_FAIL(sql_parser.parse(stmt.ptr(), stmt.length(), parse_result))) {
    // if is multi_values_parser opt not need retry
    if (lib::is_mysql_mode() && !parse_result.is_multi_values_parser_) {
      parse_result.enable_compatible_comment_ = false;
      parse_result.mysql_compatible_comment_ = false;
      if (OB_FAIL(sql_parser.parse(stmt.ptr(), stmt.length(), parse_result))) {
        //do nothing.
      }
    }
#ifdef NDEBUG
    if (parse_result.may_contain_sensitive_data_) {
      parse_result.contain_sensitive_data_ = true;
      stmt_str = ObString(OB_MASKED_STR);
    }
#endif
    if (!no_throw_parser_error) {
      LOG_INFO("failed to parse stmt as sql", K(stmt_str), K(ret));
    }
  } else if (parse_result.is_dynamic_sql_) {
    memmove(parse_result.no_param_sql_ + parse_result.no_param_sql_len_,
            parse_result.input_sql_ + parse_result.pl_parse_info_.last_pl_symbol_pos_,
            parse_result.input_sql_len_ - parse_result.pl_parse_info_.last_pl_symbol_pos_);
    parse_result.no_param_sql_len_
      += parse_result.input_sql_len_ - parse_result.pl_parse_info_.last_pl_symbol_pos_;
  } else { /*do nothing*/ }
#ifndef NDEBUG
  parse_result.contain_sensitive_data_ = false;
#endif
  if (parse_result.is_fp_ || parse_result.is_multi_query_) {
    if (OB_FAIL(ret) && !no_throw_parser_error) {
      LOG_WARN("failed to fast parameterize", K(stmt_str), K(ret));
    }
  }
  if (OB_SUCC(ret) &&
      parse_result.enable_compatible_comment_ &&
      parse_result.mysql_compatible_comment_) {
    ret = OB_ERR_PARSE_SQL;
    LOG_WARN("the sql is invalid", K(ret), K(stmt_str));
  }
  if (OB_FAIL(ret) && !no_throw_parser_error) {
    auto err_charge_sql_mode = lib::is_oracle_mode();
    LOG_WARN("failed to parse the statement",
             K(stmt_str),
             K(parse_result.is_fp_),
             K(parse_result.is_multi_query_),
             K(parse_result.yyscan_info_),
             K(parse_result.result_tree_),
             K(parse_result.malloc_pool_),
             "message", parse_result.error_msg_,
             "start_col", parse_result.start_col_,
             "end_col", parse_result.end_col_,
             K(parse_result.line_),
             K(parse_result.yycolumn_),
             K(parse_result.yylineno_),
             K(parse_result.extra_errno_),
             K(parse_result.is_for_remap_),
             K(err_charge_sql_mode),
             K(sql_mode_),
             K(parse_result.sql_mode_),
             K(parse_result.may_bool_value_),
             K(ret));

    int32_t error_offset = parse_result.start_col_ > 0 ? (parse_result.start_col_ - 1) : 0;
    int32_t error_length = stmt.empty() ? 0 : get_well_formed_errlen(parse_result.charset_info_,
                                                  stmt.ptr() + error_offset,
                                                  stmt.length() - error_offset);



    if (OB_ERR_PARSE_SQL == ret) {
      LOG_USER_ERROR(OB_ERR_PARSE_SQL, ob_errpkt_strerror(OB_ERR_PARSER_SYNTAX, false),
                     error_length,
                     stmt.empty() ? NULL : stmt.ptr() + error_offset,
                     parse_result.line_ + 1);
    } else {
      // other errors handle outer side.
    }
  }
  return ret;
}

int ObParser::parse(const ObString &query,
                    ParseResult &parse_result,
                    ParseMode parse_mode,
                    const bool is_batched_multi_stmt_split_on,
                    const bool no_throw_parser_error,
                    const bool is_pl_inner_parse)
{
  int ret = OB_SUCCESS;

  // 删除SQL语句末尾的空格
  int64_t len = query.length();
  while (len > 0 && ISSPACE(query[len - 1])) {
    --len;
  }
  //为与mysql兼容, 在去除query sql结尾空字符后, 最后一个字符为'\0'的可以被去除，且仅去除一个'\0'。
  //如果是MULTI_MODE, 则已经做过一次去结尾符'\0'操作
  if (MULTI_MODE != parse_mode) {
    //去除末尾一个‘\0’
    if (len > 0 && '\0' == query[len - 1]) {
      --len;
    }
    //再删除末尾空格
    while (len > 0 && ISSPACE(query[len - 1])) {
      --len;
    }
  }

  ObString stmt(len, query.ptr());
  memset(&parse_result, 0, sizeof(ParseResult));
  parse_result.is_multi_values_parser_ = (INS_MULTI_VALUES == parse_mode);
  parse_result.is_fp_ = (FP_MODE == parse_mode
                         || FP_PARAMERIZE_AND_FILTER_HINT_MODE == parse_mode
                         || FP_NO_PARAMERIZE_AND_FILTER_HINT_MODE== parse_mode);
  parse_result.is_multi_query_ = (MULTI_MODE == parse_mode);
  parse_result.is_ignore_hint_ = (FP_PARAMERIZE_AND_FILTER_HINT_MODE == parse_mode
                                  || FP_NO_PARAMERIZE_AND_FILTER_HINT_MODE == parse_mode);
  parse_result.is_for_trigger_ = (TRIGGER_MODE == parse_mode);
  parse_result.is_dynamic_sql_ = (DYNAMIC_SQL_MODE == parse_mode);
  parse_result.is_dbms_sql_ = (DBMS_SQL_MODE == parse_mode);
  parse_result.is_for_udr_ = (UDR_SQL_MODE == parse_mode);
  parse_result.is_batched_multi_enabled_split_ = is_batched_multi_stmt_split_on;
  parse_result.is_not_utf8_connection_ = ObCharset::is_valid_collation(charsets4parser_.string_collation_) ?
        (ObCharset::charset_type_by_coll(charsets4parser_.string_collation_) != CHARSET_UTF8MB4) : false;
  parse_result.malloc_pool_ = allocator_;
  if (lib::is_oracle_mode()) {
    parse_result.sql_mode_ = sql_mode_ | SMO_ORACLE;
  } else {
    parse_result.sql_mode_ = sql_mode_ & (~SMO_ORACLE);
  }
  parse_result.need_parameterize_ = (FP_MODE == parse_mode
                         || FP_PARAMERIZE_AND_FILTER_HINT_MODE == parse_mode);
  parse_result.minus_ctx_.pos_ = -1;
  parse_result.minus_ctx_.raw_sql_offset_ = -1;
  parse_result.charset_info_ = ObCharset::get_charset(charsets4parser_.string_collation_);
  parse_result.charset_info_oracle_db_ = ObCharset::is_valid_collation(charsets4parser_.nls_collation_) ?
        ObCharset::get_charset(charsets4parser_.nls_collation_) : NULL;
  parse_result.connection_collation_ = charsets4parser_.string_collation_;
  parse_result.mysql_compatible_comment_ = false;
  parse_result.enable_compatible_comment_ = true;
  if (nullptr != def_name_ctx_) {
    parse_result.question_mark_ctx_.by_defined_name_ = true;
    parse_result.question_mark_ctx_.name_ = def_name_ctx_->name_;
    parse_result.question_mark_ctx_.count_ = def_name_ctx_->count_;
  }

  parse_result.pl_parse_info_.is_inner_parse_ = is_pl_inner_parse;

  if (INS_MULTI_VALUES == parse_mode) {
    void *buffer = nullptr;
    if (OB_ISNULL(buffer = allocator_->alloc(sizeof(InsMultiValuesResult)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate InsMultiValuesResult failed", K(ret), K(sizeof(InsMultiValuesResult)));
    } else {
      parse_result.ins_multi_value_res_ = new(buffer)InsMultiValuesResult();
    }
  }

  if (OB_SUCC(ret) && stmt.empty()) {
    ret = OB_ERR_EMPTY_QUERY;
    LOG_WARN("query is empty", K(ret));
  }

  if (OB_SUCC(ret) && (parse_result.is_fp_ || parse_result.is_dynamic_sql_)) {
    int64_t new_length = parse_result.is_fp_ ? stmt.length() + 1 : stmt.length() * 2;
    char *buf = (char *)parse_malloc(new_length, parse_result.malloc_pool_);
    if (OB_UNLIKELY(NULL == buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory for parser");
    } else {
      parse_result.no_param_sql_ = buf;
      parse_result.no_param_sql_buf_len_ = new_length;
    }
  }
  //compatible mysql, mysql allow use the "--"
  if (OB_SUCC(ret) && lib::is_mysql_mode() && stmt.case_compare("--") == 0) {
    const char *line_str = "-- ";
    char *buf = (char *)parse_malloc(strlen(line_str), parse_result.malloc_pool_);
    if (OB_UNLIKELY(NULL == buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory for parser");
    } else {
      MEMCPY(buf, line_str, strlen(line_str));
      stmt.assign_ptr(buf, strlen(line_str));
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(parse_result.charset_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("charset info is null", K(ret),
             "connection charset", ObCharset::charset_name(charsets4parser_.string_collation_));
  } else {
    LOG_DEBUG("check parse_result param",
              "connection charset", ObCharset::charset_name(charsets4parser_.string_collation_));
  }
  if (OB_SUCC(ret)) {
    bool is_create_func = false;
    bool is_call_procedure = false;
    // 判断语句的前6个字符是否为select，如果是，直接走parse_sql的逻辑，尽可能消除is_pl_stmt字符解析的开销，
    //  即使这里parser失败，后面还有pl parser的兜底逻辑
    bool is_contain_select = (stmt.length() > 6 && 0 == STRNCASECMP(stmt.ptr(), "select", 6));

    // check wether the stmt start with "/*!", if so, stmt should first go through pl parser.  
    bool is_mysql_comment = false;
    if (lib::is_mysql_mode()) {
      uint64_t pos = 0;
      while (pos < len &&
             (stmt[pos] == ' ' || stmt[pos] == '\t' || stmt[pos] == '\r' ||
              stmt[pos] == '\f' || stmt[pos] == '\n')) {
        pos++;
      }
      if ((pos + 2 < len) && stmt[pos] == '/' && stmt[pos + 1] == '*' && stmt[pos + 2] == '!') { 
        is_mysql_comment = true;
      }
    }


    if ((is_contain_select || !is_pl_stmt(stmt, &is_create_func, &is_call_procedure)
        || (is_call_procedure && parse_result.is_fp_)) && !is_mysql_comment) {
      if (OB_FAIL(parse_sql(stmt, parse_result, no_throw_parser_error))) {
        // if fail, regard /*! */ as comment, and retry;
        if (!no_throw_parser_error) {
          LOG_WARN("failed to parse stmt as sql",
                   "stmt", parse_result.contain_sensitive_data_ ? ObString(OB_MASKED_STR) : stmt,
                   K(parse_mode), K(ret));
        }
      }
    } else {
      ObPLParser pl_parser(*(ObIAllocator*)(parse_result.malloc_pool_), charsets4parser_, sql_mode_);
      if (OB_FAIL(pl_parser.parse(stmt, stmt, parse_result, is_pl_inner_parse))) {
        LOG_WARN("failed to parse stmt as pl", K(stmt), K(ret));
        // may create ddl func, try it.
        if ((OB_ERR_PARSE_SQL == ret
            && is_create_func
            && is_single_stmt(stmt)
            && lib::is_mysql_mode()) ||
            (OB_ERR_PARSE_SQL == ret && is_mysql_comment)) {
          if (OB_FAIL(parse_sql(stmt, parse_result, no_throw_parser_error))) {
            if (!no_throw_parser_error) {
              LOG_WARN("failed to parse stmt as sql",
                  "stmt", parse_result.contain_sensitive_data_ ? ObString(OB_MASKED_STR) : stmt,
                  K(parse_mode), K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

//通过parser进行prepare的接口，供mysql模式下spi_prepare调用
int ObParser::prepare_parse(const ObString &query, void *ns, ParseResult &parse_result)
{
  int ret = OB_SUCCESS;

  // 删除SQL语句末尾的空格
  int64_t len = query.length();
  while (len > 0 && ISSPACE(query[len - 1])) {
    --len;
  }
  if (len > 0 && '\0' == query[len - 1]) {
    --len;
  }
  //再删除末尾空格
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
  parse_result.enable_compatible_comment_ = true;
  char *buf = (char *)parse_malloc(stmt.length() * 2, parse_result.malloc_pool_); //因为要把pl变量替换成:num的形式，需要扩大内存
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
               "message", parse_result.error_msg_,
               "start_col", parse_result.start_col_,
               "end_col", parse_result.end_col_,
               K(parse_result.line_),
               K(parse_result.yycolumn_),
               K(parse_result.yylineno_),
               K(parse_result.extra_errno_));

      int32_t error_offset = parse_result.start_col_ > 0 ? (parse_result.start_col_ - 1) : 0;
      int32_t error_length = stmt.empty() ? 0 : get_well_formed_errlen(parse_result.charset_info_,
                                                    stmt.ptr() + error_offset,
                                                    stmt.length() - error_offset);

      if (OB_ERR_PARSE_SQL == ret) {
        LOG_USER_ERROR(OB_ERR_PARSE_SQL, ob_errpkt_strerror(OB_ERR_PARSER_SYNTAX, false),
                       error_length,
                       stmt.empty() ? NULL : stmt.ptr() + error_offset,
                       parse_result.line_ + 1);
      } else {
        // other errors handle outer side.
      }
    } else {
      int req_size = parse_result.no_param_sql_len_ + parse_result.input_sql_len_ - parse_result.pl_parse_info_.last_pl_symbol_pos_;
      if (parse_result.no_param_sql_buf_len_ < req_size) {
        int64_t new_buf_length = req_size + 1;
        char *new_buf = (char *)parse_malloc(new_buf_length, parse_result.malloc_pool_);
        if (OB_UNLIKELY(NULL == new_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("no memory for parser");
        } else {
          MEMCPY(new_buf, parse_result.no_param_sql_, parse_result.no_param_sql_len_);
          parse_result.no_param_sql_ = new_buf;
          parse_result.no_param_sql_buf_len_ = new_buf_length;
        }
      }
      if (OB_SUCC(ret)) {
        memmove(parse_result.no_param_sql_ + parse_result.no_param_sql_len_,
              parse_result.input_sql_ + parse_result.pl_parse_info_.last_pl_symbol_pos_,
              parse_result.input_sql_len_ - parse_result.pl_parse_info_.last_pl_symbol_pos_);
        parse_result.no_param_sql_len_ += parse_result.input_sql_len_ - parse_result.pl_parse_info_.last_pl_symbol_pos_;
      }
    }
  }
  return ret;
}

// 解析行首注释中trace_id
int ObParser::pre_parse(const common::ObString &stmt,
                        PreParseResult &res)
{
  int ret = OB_SUCCESS;
  // /*trace_id=xxx*/
  int64_t len = stmt.length();
  if (len <= 13) {
    //do_nothing
  } else {
    int32_t pos = 0;
    const char *ptr = stmt.ptr();
    //去掉空格
    while (pos < len && is_space(ptr[pos])) { pos++; };
    if (pos+2 < len
        && ptr[pos++] == '/'
        && ptr[pos++] == '*') { // start with '/*'
      char expect_char = 't';
      bool find_trace = false;
      while (!find_trace && pos < len) {
        //找到注释结束点
        if (pos < len && ptr[pos] == '*') {
          expect_char = 't';
          if (++pos < len && ptr[pos] == '/') {
            break;
          }
        }
        if (pos >= len) {
          break;
        } else if (ptr[pos] == expect_char
                   || (expect_char != '_'
                       && expect_char != '='
                       && ptr[pos] == expect_char - 32)) {
          switch(expect_char) {
            case 't': { expect_char = 'r'; break;}
            case 'r': { expect_char = 'a'; break;}
            case 'a': { expect_char = 'c'; break;}
            case 'c': { expect_char = 'e'; break;}
            case 'e': { expect_char = '_'; break;}
            case '_': { expect_char = 'i'; break;}
            case 'i': { expect_char = 'd'; break;}
            case 'd': { expect_char = '='; break;}
            case '=': { find_trace = true; break;}
            default: {
              //do nothing
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
    } else { //not start with '/*'
      //do nothing
    }
  }

  return ret;
}

int ObParser::scan_trace_id(const char *ptr,
                            int64_t len,
                            int32_t &pos,
                            ObString &trace_id)
{
  int ret = OB_SUCCESS;
  trace_id.reset();
  int32_t start = pos;
  int32_t end = pos;
  //找到trace_id结束点
  while (pos < len && !is_trace_id_end(ptr[pos])) { pos++; }
  end = pos;
  //找到注释结束点
  if (start < end) {
    while (pos < len) {
      if (pos < len && ptr[pos] == '*') {
        if (++pos < len && ptr[pos] == '/') {
          int32_t trace_id_len = std::min(static_cast<int32_t>(OB_MAX_TRACE_ID_BUFFER_SIZE),
                                          end - start);
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

bool ObParser::is_space(char ch) {
  return ch == ' '
         || ch == '\t'
         || ch == '\n'
         || ch == '\r'
         || ch == '\f';
}

bool ObParser::is_trace_id_end(char ch)
{
  return is_space(ch)
         || ch == ','
         || ch == '*';
}

void ObParser::free_result(ParseResult &parse_result)
{
  UNUSED(parse_result);
//  destroy_tree(parse_result.result_tree_);
//  parse_terminate(&parse_result);
//  parse_result.yyscan_info_ = NULL;
}
