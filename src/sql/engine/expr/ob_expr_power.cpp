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
#include "sql/engine/expr/ob_expr_power.h"
#include "sql/engine/expr/ob_expr_pow.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/number/ob_number_v2.h"

namespace oceanbase {
using namespace oceanbase::common;

namespace sql {
ObExprPower::ObExprPower(ObIAllocator& alloc) : ObExprOperator(alloc, T_FUN_SYS_POWER, N_POWER, 2, NOT_ROW_DIMENSION)
{}

int ObExprPower::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  return calc_trig_function_result_type2(type, type1, type2, type_ctx);
}

int ObExprPower::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  return calc(result, obj1, obj2, expr_ctx);
}

int ObExprPower::calc(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  if (obj1.is_null() || obj2.is_null()) {
    result.set_null();
  } else if (obj1.is_number() && obj2.is_int()) {
    // TODO this path is never used ?
    // TODO copied code
    LOG_DEBUG("ObExprPower, power_int path start");
    const number::ObNumber base_number = obj1.get_number();
    const int exponent_int = obj2.get_int();
    number::ObNumber result_number;
    common::ObIAllocator& allocator = *(expr_ctx.calc_buf_);
    if (OB_FAIL(base_number.power(exponent_int, result_number, allocator))) {
      LOG_WARN("power (int exponent) failed", K(ret), K(base_number), K(exponent_int), K(result_number));
    } else {
      result.set_number(result_number);
      LOG_DEBUG("ObExprPower, power_int path end successfully");
    }
  } else if (obj1.is_number() && obj2.is_number()) {
    LOG_DEBUG("ObExprPower, both-ObNumber path start");
    const number::ObNumber base_number = obj1.get_number();
    const number::ObNumber exponent_number = obj2.get_number();
    number::ObNumber result_number;
    common::ObIAllocator& allocator = *(expr_ctx.calc_buf_);
    if (OB_FAIL(base_number.power(exponent_number, result_number, allocator))) {
      LOG_WARN("power failed", K(ret), K(base_number), K(exponent_number), K(result_number));
    } else {
      result.set_number(result_number);
      LOG_DEBUG("ObExprPower, ObNumber path end successfully");
    }
  } else if (obj1.is_double() && obj2.is_double()) {
    // same as oracle behavior: if at least one of the arguments is
    // binary_double, result type is double
    double double_base = obj1.get_double();
    double double_exponent = obj2.get_double();
    double result_double = std::pow(double_base, double_exponent);
    if (isinf(result_double) || isnan(result_double)) {
      ret = OB_OPERATE_OVERFLOW;
    } else {
      result.set_double(result_double);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj type should be number or double", K(obj1), K(obj2), K(ret));
  }
  return ret;
}
int calc_power_expr_oracle(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  // power(base, exp)
  ObDatum* base = NULL;
  ObDatum* exp = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, base)) || OB_FAIL(expr.args_[1]->eval(ctx, exp))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (base->is_null() || exp->is_null()) {
    res_datum.set_null();
  } else {
    if (ObDoubleType == expr.datum_meta_.type_) {
      ret = ObExprPow::safe_set_double(res_datum, std::pow(base->get_double(), exp->get_double()));
    } else {
      const number::ObNumber base_nmb(base->get_number());
      const number::ObNumber exp_nmb(exp->get_number());
      number::ObNumber res_nmb;
      if (OB_FAIL(base_nmb.power(exp_nmb, res_nmb, ctx.get_reset_tmp_alloc()))) {
        LOG_WARN("calc power failed", K(ret), K(base_nmb), K(exp_nmb));
      } else {
        res_datum.set_number(res_nmb);
      }
    }
  }
  return ret;
}

int ObExprPower::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  CK(2 == raw_expr.get_param_count());
  if (OB_SUCC(ret)) {
    if (share::is_oracle_mode()) {
      rt_expr.eval_func_ = calc_power_expr_oracle;
    } else {
      rt_expr.eval_func_ = ObExprPow::calc_pow_expr;
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
