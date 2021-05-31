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

#include "lib/oblog/ob_log.h"
#include "lib/number/ob_number_v2.h"
#include "sql/engine/expr/ob_expr_tan.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/parser/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include <math.h>

namespace oceanbase {
using namespace common;
using namespace common::number;
namespace sql {

ObExprTan::ObExprTan(ObIAllocator& alloc) : ObFuncExprOperator(alloc, T_FUN_SYS_TAN, N_TAN, 1, NOT_ROW_DIMENSION)
{}

ObExprTan::~ObExprTan()
{}

int ObExprTan::calc_result_type1(ObExprResType& type, ObExprResType& radian, ObExprTypeCtx& type_ctx) const
{
  return calc_trig_function_result_type1(type, radian, type_ctx);
}

int ObExprTan::calc_result1(ObObj& result, const ObObj& radian_obj, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (radian_obj.is_null_oracle()) {
    result.set_null();
  } else if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
  } else if (radian_obj.is_number()) {
    ObNumber ret_number;
    ObNumber radian_number;
    if (OB_FAIL(radian_obj.get_number(radian_number))) {
      LOG_WARN("get_number failed", K(radian_obj));
    } else if (OB_FAIL(radian_number.tan(ret_number, *(expr_ctx.calc_buf_)))) {
      LOG_WARN("taylor_series_tan failed", K(radian_number), K(ret));
    } else {
      result.set_number(ret_number);
    }
  } else if (radian_obj.is_double()) {
    double arg = radian_obj.get_double();
    double res = tan(arg);
    if (isinf(res) || isnan(res)) {
      ret = OB_OPERATE_OVERFLOW;
    } else {
      result.set_double(res);
    }
  }
  return ret;
}

DEF_CALC_TRIGONOMETRIC_EXPR(tan, (isinf(arg) || isnan(arg)), OB_OPERATE_OVERFLOW);

int ObExprTan::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_tan_expr;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
