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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_substring_index.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "lib/string/ob_string.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprSubstringIndex::ObExprSubstringIndex(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_SUBSTRING_INDEX, N_SUBSTRING_INDEX, 3)
{
  need_charset_convert_ = false;
}

ObExprSubstringIndex::~ObExprSubstringIndex()
{}

inline int ObExprSubstringIndex::calc_result_type3(
    ObExprResType& type, ObExprResType& str, ObExprResType& delim, ObExprResType& count, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  str.set_calc_type(ObVarcharType);
  delim.set_calc_type(ObVarcharType);

  // there can't set int type for count, because it is dynamically
  // count.set_calc_type(ObIntType);

  // substring_index is mysql only expr.
  CK(true == lib::is_mysql_mode());
  CK(NULL != type_ctx.get_session());

  if (OB_SUCC(ret) && type_ctx.get_session()->use_static_typing_engine()) {
    // cast count to int64, see comment in eval_substring_index
    count.set_calc_type(ObIntType);
    // Set cast mode for %count:
    //   truncate string to integer.
    //   no range check for uint to int
    type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
    type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NO_RANGE_CHECK);
  }

  if (OB_SUCC(ret)) {
    type.set_varchar();
    type.set_length(str.get_length());
    common::ObArenaAllocator alloc;
    ObExprResType types[2] = {alloc, alloc};
    types[0] = str;
    types[1] = delim;
    if (OB_FAIL(aggregate_charsets_for_string_result_with_comparison(type, types, 2, type_ctx.get_coll_type()))) {
      LOG_WARN("aggregate_charsets_for_string_result_with_comparison failed", K(ret));

    } else {
      str.set_calc_collation_type(type.get_collation_type());
      str.set_calc_collation_level(type.get_collation_level());
      delim.set_calc_collation_type(type.get_collation_type());
      delim.set_calc_collation_level(type.get_collation_level());
    }
  }

  return ret;
}

/**
 * steps:
 *    1. check null: if any param is null, return null
 *    2. type promotion: convert str and delim to string
 , convert count to int
 *    3. check empty: if any param is empty string, return empty string
 *    4. call string_search: if everything is OK, find nth apperance of delim
 */
int ObExprSubstringIndex::calc_result3(
    ObObj& result, const ObObj& str, const ObObj& delim, const ObObj& count, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  // check null
  if (OB_UNLIKELY(str.is_null() || delim.is_null() || count.is_null())) {
    result.set_null();
  } else {
    /*
     * int64:
     * <0 val = left_point - val (abs(left_point - val) <= len)
     * >0 val = val - right_point(abs(val - right_point) <= len)
     * uint64:
     * val = val - right_point(abs(val - right_point)<=len)
     */

    ObString str_val = str.get_string();
    ObString delim_val = delim.get_string();
    int64_t count_val = 0;
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    EXPR_GET_INT64_V2(count, count_val);

    /* deal with overflow */
    int32_t str_len = str.get_string().length();
    if (OB_SUCCESS == cast_ctx.warning_) {
      if (count_val < 0) {
        count_val = count_val - INT64_MIN <= str_len ? count_val - INT64_MIN : count_val;
      } else {
        count_val = INT64_MAX - count_val < str_len ? count_val - INT64_MAX - 1 : count_val;
      }
    } else {
      uint64_t count_uval = 0;
      EXPR_GET_UINT64_V2(count, count_uval);
      if (OB_SUCCESS == cast_ctx.warning_) {
#define RIGHT_POINT 9223372036854775808UL
        count_val = static_cast<int64_t>(count_uval - RIGHT_POINT) <= str_len
                        ? static_cast<int64_t>(count_uval - RIGHT_POINT)
                        : 0;
#undef RIGHT_POINT
      } else {
        count_val = 0;
      }
    }

    if (OB_SUCC(ret)) {
      // check empty string
      ObString empty;
      if (str_val == empty || delim_val == empty) {
        result.set_varchar(empty);
      } else {
        // everything is OK, find nth apperance of delim, set result
        ObString res_str;
        if (OB_FAIL(string_search(res_str, str_val, delim_val, count_val))) {
          LOG_WARN("string search failed", K(ret));
        } else {
          result.set_varchar(res_str);
        }
      }
      if (OB_LIKELY(OB_SUCC(ret) && !result.is_null())) {
        result.set_collation(result_type_);
      }
    }
  }
  return ret;
}

/**
 * steps:
 *    1. if m_count = 0, return empty string
 *    2. if m_count != 0
 *        A. if m_count > 0, find position of delim from front to back,
 *            return string from 0 to position in m_str
 *        B. if m_count < 0, find position of delim form back to front,
 *            return string form position to the end
 *        C. if position = -1 in A and B, nth m_delim is not found, return m_str
 */
int ObExprSubstringIndex::string_search(
    ObString& varchar, const ObString& m_str, const ObString& m_delim, const int64_t m_count)
{
  int ret = OB_SUCCESS;

  if (0 != m_count) {
    int64_t pos = -1;
    if (0 < m_count) {
      ret = ObExprUtil::kmp(const_cast<char*>(m_delim.ptr()),
          m_delim.length(),
          const_cast<char*>(m_str.ptr()),
          m_str.length(),
          m_count,
          pos);
      if (-1 < pos) {
        // nth delim found from front to back
        varchar.assign(const_cast<char*>(m_str.ptr()), static_cast<int32_t>(pos));
      }
    } else if (0 > m_count) {
      ret = ObExprUtil::kmp_reverse(const_cast<char*>(m_delim.ptr()),
          m_delim.length(),
          const_cast<char*>(m_str.ptr()),
          m_str.length(),
          m_count,
          pos);
      if (-1 < pos) {
        // nth delim found from back to front
        varchar.assign(const_cast<char*>(m_str.ptr() + static_cast<int32_t>(pos + m_delim.length())),
            static_cast<int32_t>(m_str.length() - m_delim.length() - pos));
      }
    }

    if (-1 == pos) {
      // substring not found, return m_str
      varchar.assign(const_cast<char*>(m_str.ptr()), static_cast<int32_t>(m_str.length()));
    }
  }

  return ret;
}

int ObExprSubstringIndex::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(3 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = eval_substring_index;
  return ret;
}

int ObExprSubstringIndex::eval_substring_index(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum& str = expr.locate_param_datum(ctx, 0);
  ObDatum& delim = expr.locate_param_datum(ctx, 1);
  ObDatum& count = expr.locate_param_datum(ctx, 2);
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (OB_UNLIKELY(str.is_null() || delim.is_null() || count.is_null())) {
    expr_datum.set_null();
  } else if (0 == str.len_ || 0 == delim.len_) {
    // return empty string if %str or %delim is empty.
    expr_datum.null_ = 0;
    expr_datum.len_ = 0;
  } else {
    // Static cast count to int32, compatible with mysql 5.6,
    // actually this is a bug and fixed in mysql 8.0.
    int32_t count_val = static_cast<int32_t>(count.get_int());
    ObString res_str;
    if (OB_FAIL(string_search(res_str, str.get_string(), delim.get_string(), count_val))) {
      LOG_WARN("string search failed", K(ret));
    } else {
      expr_datum.set_string(res_str);
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
