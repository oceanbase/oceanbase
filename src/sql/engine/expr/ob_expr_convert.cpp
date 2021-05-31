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

#include "ob_expr_convert.h"

#include "lib/charset/ob_charset.h"
#include "sql/engine/expr/ob_expr_cast.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase {
namespace sql {

ObExprConvert::ObExprConvert(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_CONVERT, N_CONVERT, 2, NOT_ROW_DIMENSION)
{}

ObExprConvert::~ObExprConvert()
{}

int ObExprConvert::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);

  int ret = OB_SUCCESS;
  type.set_type(type1.get_type());
  type.set_scale(type1.get_scale());
  type.set_precision(type1.get_precision());
  if (ob_is_string_type(type.get_type())) {
    type.set_length(type1.get_length());
  }
  const ObObj& dest_collation = type2.get_param();
  TYPE_CHECK(dest_collation, ObVarcharType);
  if (OB_SUCC(ret)) {
    ObString cs_name = dest_collation.get_string();
    ObCharsetType charset_type = CHARSET_INVALID;
    if (CHARSET_INVALID == (charset_type = ObCharset::charset_type(cs_name.trim()))) {
      ret = OB_ERR_UNKNOWN_CHARSET;
      LOG_WARN("unknown charset", K(ret), K(cs_name));
    } else {
      type.set_collation_level(CS_LEVEL_EXPLICIT);
      type.set_collation_type(ObCharset::get_default_collation(charset_type));
      // set calc type
      // only set type2 here.
      type2.set_calc_type(ObVarcharType);
      type1.set_calc_meta(type.get_obj_meta());
      type1.set_calc_collation_type(type.get_collation_type());
      type1.set_calc_collation_level(type.get_collation_level());
      LOG_DEBUG("in calc result type", K(ret), K(type1), K(type2), K(type));
    }
  }

  return ret;
}

int ObExprConvert::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(obj2);
  UNUSED(expr_ctx);
  result = obj1;
  return ret;
}

int calc_convert_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* child_res = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {
    LOG_WARN("eval arg 0 failed", K(ret));
  } else {
    res_datum.set_datum(*child_res);
  }
  return ret;
}

int ObExprConvert::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_convert_expr;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
