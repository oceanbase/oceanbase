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
#include "ob_expr_right.h"
#include <string.h>
#include "lib/charset/ob_charset.h"
#include "share/object/ob_obj_cast.h"
#include "sql/parser/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase {
namespace sql {

ObExprRight::ObExprRight(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_RIGHT, "right", 2)
{}

ObExprRight::~ObExprRight()
{}

int ObExprRight::calc_result_type2(
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

int do_right(const ObString& input_value, const ObCollationType& cs_type, const int64_t length, ObString& res_str)
{
  int ret = OB_SUCCESS;
  const char* input_start = input_value.ptr();
  int64_t input_length = input_value.length();
  if (OB_ISNULL(input_start) && 0 != input_length) {
    LOG_WARN("String invalid", K(input_start), K(input_length));
  } else {
    int64_t char_beyond_end = input_length;
    int64_t wanted = min(length, input_length);
    int64_t got = 0;
    int64_t char_length = 0;
    while (OB_SUCC(ret) && (got < wanted) && (-1 < char_beyond_end)) {
      if (OB_FAIL(ObCharset::last_valid_char(cs_type, input_start, char_beyond_end, char_length))) {
        LOG_WARN("Get last valid char failed ", K(ret));
      } else {
        char_beyond_end -= char_length;
        ++got;
      }
    }
    if (OB_SUCC(ret)) {
      res_str.assign_ptr(input_start + char_beyond_end, static_cast<int32_t>(input_length - char_beyond_end));
    }
  }
  return ret;
}

int ObExprRight::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_ctx);
  if (obj1.is_null() || obj2.is_null()) {
    result.set_null();
  } else {
    TYPE_CHECK(obj2, ObIntType);
    int64_t length = obj2.get_int();
    ObString res_str;
    if (OB_FAIL(do_right(obj1.get_string(), obj1.get_collation_type(), length, res_str))) {
      LOG_WARN("Failed to calc", K(ret));
    } else {
      result.set_varchar(res_str);
      result.set_collation(result_type_);
    }
  }
  return ret;
}

int calc_right_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  // right(s, n)
  ObDatum* s_datum = NULL;
  ObDatum* n_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, s_datum)) || OB_FAIL(expr.args_[1]->eval(ctx, n_datum))) {
    LOG_WARN("eval arg failed", K(ret), KP(s_datum), KP(n_datum));
  } else if (s_datum->is_null() || n_datum->is_null()) {
    res_datum.set_null();
  } else {
    ObString res_str;
    const ObCollationType arg_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    if (OB_FAIL(do_right(s_datum->get_string(), arg_cs_type, n_datum->get_int(), res_str))) {
      LOG_WARN("failed to calculate right expression", K(ret));
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

int ObExprRight::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_right_expr;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
