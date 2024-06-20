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
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprRight::ObExprRight(ObIAllocator &alloc) :
    ObStringExprOperator(alloc, T_FUN_SYS_RIGHT, "right", 2, VALID_FOR_GENERATED_COL)
{
}

ObExprRight::~ObExprRight()
{
}

int ObExprRight::calc_result_type2(ObExprResType &type, ObExprResType &type1,
                                   ObExprResType &type2,ObExprTypeCtx &type_ctx) const
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

int do_right(const ObString &input_value, const ObCollationType &cs_type,
             const int64_t length, ObString &res_str)
{
  int ret = OB_SUCCESS;
  int64_t char_len = (int64_t)(ObCharset::strlen_char(cs_type,
                                                      input_value.ptr(),
                                                      input_value.length()));
  int64_t pos = char_len - length;
  if (length < 0) {
    res_str.reset();
    //return empty string
  } else if (pos <= 0) {
    res_str = input_value;
  } else {
    int32_t offset = (int32_t)
        ObCharset::charpos(cs_type, input_value.ptr(), input_value.length(), pos);
    if (offset <= input_value.length()) {
      res_str.assign_ptr(input_value.ptr() + offset, input_value.length() - offset);
    }
  }
  return ret;
}

int calc_right_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  // right(s, n)
  ObDatum *s_datum = NULL;
  ObDatum *n_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, s_datum)) ||
      OB_FAIL(expr.args_[1]->eval(ctx, n_datum))) {
    LOG_WARN("eval arg failed", K(ret), KP(s_datum), KP(n_datum));
  } else if (s_datum->is_null() || n_datum->is_null()) {
    res_datum.set_null();
  } else {
    ObString res_str;
    const ObCollationType arg_cs_type = expr.args_[0]->datum_meta_.cs_type_;
		if(OB_FAIL(do_right(s_datum->get_string(), arg_cs_type, n_datum->get_int(), res_str))) {
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

int ObExprRight::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_right_expr;
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprRight, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

}// namespace sql
}// namespace oceanbase
