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
#include <math.h>
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_exp.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/number/ob_number_v2.h"

namespace oceanbase {
using namespace oceanbase::common;

namespace sql {
ObExprExp::ObExprExp(ObIAllocator& alloc) : ObFuncExprOperator(alloc, T_FUN_SYS_EXP, N_EXP, 1, NOT_ROW_DIMENSION)
{}

ObExprExp::~ObExprExp()
{}

int ObExprExp::calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const
{
  return calc_trig_function_result_type1(type, type1, type_ctx);
}

int ObExprExp::calc_result1(ObObj& result, const ObObj& obj, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (obj.is_null()) {
    result.set_null();
  } else if (obj.is_number()) {
    const number::ObNumber arg_number = obj.get_number();
    number::ObNumber result_number;
    common::ObIAllocator& allocator = *(expr_ctx.calc_buf_);
    if (OB_FAIL(arg_number.e_power(result_number, allocator))) {
      LOG_WARN("e_power failed", K(ret), K(arg_number), K(result_number));
    } else {
      result.set_number(result_number);
    }
  } else if (obj.is_double()) {
    double value = obj.get_double();
    value = exp(value);
    if (isinf(value) || isnan(value)) {
      ret = OB_OPERATE_OVERFLOW;
    } else {
      result.set_double(value);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj type should be number or double", K(obj), K(ret));
  }
  return ret;
}

int calc_exp_expr_double(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    double value = arg->get_double();
    value = std::exp(value);
    if (isinf(value) || isnan(value)) {
      ret = OB_OPERATE_OVERFLOW;
    } else {
      res_datum.set_double(value);
    }
  }
  return ret;
}

int calc_exp_expr_number(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    number::ObNumber arg_nmb(arg->get_number());
    number::ObNumber res_nmb;
    if (OB_FAIL(arg_nmb.e_power(res_nmb, ctx.get_reset_tmp_alloc()))) {
      LOG_WARN("e_power failed", K(ret));
    } else {
      res_datum.set_number(res_nmb);
    }
  }
  return ret;
}

int ObExprExp::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg_cnt_ of expr", K(ret), K(rt_expr));
  } else {
    ObObjType arg_res_type = rt_expr.args_[0]->datum_meta_.type_;
    if (ObDoubleType == arg_res_type) {
      rt_expr.eval_func_ = calc_exp_expr_double;
    } else if (ObNumberType == arg_res_type) {
      rt_expr.eval_func_ = calc_exp_expr_number;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("arg type must be double or number", K(ret), K(arg_res_type));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
