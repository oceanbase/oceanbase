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
#include "sql/engine/expr/ob_expr_left.h"
#include "lib/charset/ob_charset.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase {
namespace sql {

ObExprLeft::ObExprLeft(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_LEFT, "left", 2)
{}

ObExprLeft::~ObExprLeft()
{}

int ObExprLeft::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  type2.set_calc_type(ObIntType);
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
  type.set_varchar();
  type.set_length(type1.get_length());
  OZ(aggregate_charsets_for_string_result(type, &type1, 1, type_ctx.get_coll_type()));
  OX(type1.set_calc_type(ObVarcharType));
  OX(type1.set_calc_collation_type(type.get_collation_type()));
  return ret;
}

int calc_left(ObString& res_str, const ObString& text, const ObCollationType type, const int64_t required_num_char)
{
  int ret = OB_SUCCESS;
  const char* str_ptr = text.ptr();
  int64_t str_length = text.length();
  if (OB_ISNULL(str_ptr) && 0 != str_length) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    int64_t input_num_char = ObCharset::strlen_char(type, text.ptr(), text.length());
    int64_t expected_num_char = min(required_num_char, input_num_char);
    int64_t start_pos = 0;
    int64_t char_length = 0;
    while ((start_pos < str_length) && OB_SUCC(ret) && (0 < expected_num_char)) {
      if (OB_FAIL(ObCharset::first_valid_char(type, str_ptr + start_pos, str_length - start_pos, char_length))) {
        LOG_WARN("get char failed", K(ret));
      } else {
        start_pos += char_length;
        --expected_num_char;
      }
    }
    if (OB_SUCC(ret)) {
      res_str.assign_ptr(str_ptr, static_cast<int32_t>(start_pos));
    }
  }
  return ret;
}

int ObExprLeft::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (obj1.is_null() || obj2.is_null()) {
    result.set_null();
  } else {
    TYPE_CHECK(obj1, ObVarcharType);
    TYPE_CHECK(obj2, ObIntType);
    ObString res_str;
    if (OB_FAIL(calc_left(res_str, obj1.get_string(), obj1.get_collation_type(), obj2.get_int()))) {
      LOG_WARN("failed to calculate left expression", K(ret));
    } else {
      result.set_varchar(res_str);
      result.set_collation(result_type_);
    }
  }
  return ret;
}

int calc_left_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  // left(s, n)
  ObDatum* s_datum = NULL;
  ObDatum* n_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, s_datum)) || OB_FAIL(expr.args_[1]->eval(ctx, n_datum))) {
    LOG_WARN("eval arg failed", K(ret), KP(s_datum), KP(n_datum));
  } else if (s_datum->is_null() || n_datum->is_null()) {
    res_datum.set_null();
  } else {
    // res_str will point to the memory space of s_datum, so the string pointed to
    // by res_str cannot be changed below
    ObString res_str;
    const ObCollationType arg_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    if (OB_FAIL(calc_left(res_str, s_datum->get_string(), arg_cs_type, n_datum->get_int()))) {
      LOG_WARN("failed to calculate left expression", K(ret));
    } else {
      if (res_str.empty() && is_oracle_mode()) {
        res_datum.set_null();
      } else {
        res_datum.set_string(res_str);
      }
    }
  }
  return ret;
}

int ObExprLeft::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_left_expr;
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
