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
#include "sql/engine/expr/ob_expr_remainder.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprRemainder::ObExprRemainder(ObIAllocator &alloc)
  : ObArithExprOperator(alloc, T_FUN_SYS_REMAINDER,
                        N_REMAINDER,
                        2,
                        NOT_ROW_DIMENSION,
                        ObExprResultTypeUtil::get_remainder_result_type,
                        ObExprResultTypeUtil::get_remainder_calc_type,
                        remainder_funcs_)
{
  param_lazy_eval_ = true;
}

int ObExprRemainder::calc_result_type2(ObExprResType &type,
                                 ObExprResType &type1,
                                 ObExprResType &type2,
                                 ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObArithExprOperator::calc_result_type2(type, type1, type2, type_ctx))) {
  } else if (type.is_oracle_decimal()) {
    type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
    type.set_precision(PRECISION_UNKNOWN_YET);
  } else {
    if (OB_UNLIKELY(SCALE_UNKNOWN_YET == type1.get_scale()) ||
        OB_UNLIKELY(SCALE_UNKNOWN_YET == type2.get_scale())) {
      type.set_scale(SCALE_UNKNOWN_YET);
    } else {
      ObScale scale1 = static_cast<ObScale>(MAX(type1.get_scale(), 0));
      ObScale scale2 = static_cast<ObScale>(MAX(type2.get_scale(), 0));
      type.set_scale(MAX(scale1, scale2));
      type.set_precision(MAX(type1.get_precision(), type2.get_precision()));
    }
  }
  if (OB_SUCC(ret)) {
    ObObjType calc_type = ObMaxType;
    ObObjType calc_type1 = ObMaxType;
    ObObjType calc_type2 = ObMaxType;
    if (OB_FAIL(ObExprResultTypeUtil::get_remainder_calc_type(calc_type, calc_type1,
            calc_type2, type1.get_type(), type2.get_type()))) {
      LOG_WARN("get calc type failed", K(ret), K(type1), K(type2));
    } else {
      type1.set_calc_type(calc_type1);
      type2.set_calc_type(calc_type2);
    }
  }
  return ret;
}

int ObExprRemainder::remainder_int64(const int64_t dividend, const int64_t divisor, int64_t& value)
{
  int ret = OB_SUCCESS;
  int64_t remainder = 0;
  if (0 == divisor) {
    ret = OB_ERR_DIVISOR_IS_ZERO;
    LOG_WARN("divisor is equal to zero on oracle mode", K(ret), K(divisor));
  } else if (0 == dividend || -1 == divisor) {
    remainder = 0;
  } else {
    bool same_sign = ((dividend > 0) == (divisor > 0));
    remainder = dividend % divisor;
    int64_t right_minus_2rem = (divisor - (same_sign ? remainder : -remainder)) - (same_sign ? remainder : -remainder);
    if ((divisor > 0 && right_minus_2rem < 0) || (divisor < 0 && right_minus_2rem > 0)) {
      remainder -= same_sign ? divisor : -divisor;
    } else if (0 == right_minus_2rem){
      int64_t tmp = (dividend / divisor) % 2;
      if (1 == tmp || -1 == tmp) {
        remainder -= same_sign ? divisor : -divisor;
      }
    }
  }
  if (OB_SUCC(ret)) {
    value = remainder;
  }
  return ret;
}

ObArithFunc ObExprRemainder::remainder_funcs_[ObMaxTC] =
{
  NULL,
  NULL,
  NULL,//uint,
  ObExprRemainder::remainder_float,
  ObExprRemainder::remainder_double,
  ObExprRemainder::remainder_number,
  NULL,//datetime
  NULL,//date
  NULL,//time
  NULL,//year
  NULL,//varchar
  NULL,//extend
  NULL,//unknown
  NULL,//text
  NULL,//bit
  NULL,//enumset
  NULL,//enumsetInner
};

int ObExprRemainder::remainder_float(ObObj &res,
                          const ObObj &left,
                          const ObObj &right,
                          ObIAllocator *allocator,
                          ObScale scale)
{
  UNUSED(allocator);
  UNUSED(scale);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!left.is_float() || !right.is_float())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    float left_f = left.get_float();
    float right_f = right.get_float();
    float res_float = remainder(left_f, right_f);
    res.set_float(res_float);
  }

  return ret;
}

int ObExprRemainder::remainder_double(ObObj &res,
                          const ObObj &left,
                          const ObObj &right,
                          ObIAllocator *allocator,
                          ObScale scale)
{
  UNUSED(allocator);
  UNUSED(scale);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!left.is_double() || !right.is_double())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    double left_d = left.get_double();
    double right_d = right.get_double();
    double res_double = remainder(left_d, right_d);
    res.set_double(res_double);
  }

  return ret;
}

int ObExprRemainder::remainder_number(ObObj &res,
                          const ObObj &left,
                          const ObObj &right,
                          ObIAllocator *allocator,
                          ObScale scale)
{
  int ret = OB_SUCCESS;
  number::ObNumber res_nmb;
  if (OB_UNLIKELY(!left.is_number() || !right.is_number())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else if (OB_UNLIKELY(NULL == allocator)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocator is null", K(ret));
  } else {
    number::ObNumber left_number = left.get_number();
    number::ObNumber right_number = right.get_number();
    int64_t left_int = 0, right_int = 0;
    if (OB_UNLIKELY(right_number.is_zero())) {
      ret = OB_ERR_DIVISOR_IS_ZERO;
      LOG_WARN("divisor is equal to zero on oracle mode", K(ret), K(right));
    } else if (OB_SUCCESS == left_number.cast_to_int64(left_int)
                && OB_SUCCESS == right_number.cast_to_int64(right_int)) {
      int64_t remainder = 0;
      if (OB_FAIL(remainder_int64(left_int, right_int, remainder))) {
          LOG_WARN("failed to call remainder_int64", K(ret), K(left_int), K(right_int));
      } else if (OB_FAIL(res_nmb.from(remainder, *allocator))) {
        LOG_WARN("res_nmb failed to from remaind", K(ret), K(res_nmb), K(remainder));
      } else {
        res.set_number(res_nmb);
      }
    } else if (OB_FAIL(left_number.round_remainder(right_number, res_nmb, *allocator))) {
      LOG_WARN("failed to add numbers", K(ret), K(left), K(right));
    } else {
      res.set_number(res_nmb);
    }
  }
  UNUSED(scale);
  return ret;
}

int ObExprRemainder::calc_remainder_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                         ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *left = NULL;
  ObDatum *right = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, left))) {
    LOG_WARN("failed to calculate parameter 0", K(ret));
  } else if (left->is_null()) {
    res.set_null();
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, right))) {
    LOG_WARN("failed to calculate parameter 0", K(ret));
  } else if (right->is_null()) {
    res.set_null();
  } else {
    const ObObjTypeClass tc = ob_obj_type_class(expr.args_[0]->datum_meta_.type_);
    if (ObNumberTC == tc) {
      number::ObNumber res_nmb;
      const number::ObNumber left_nmb(left->get_number());
      const number::ObNumber right_nmb(right->get_number());
      if (OB_UNLIKELY(right_nmb.is_zero())) {
        ret = OB_ERR_DIVISOR_IS_ZERO;
        LOG_WARN("divisor is equal to zero on oracle mode", K(ret), K(right_nmb));
      } else {
        int64_t left_int = 0, right_int = 0;
        if (OB_SUCCESS == left_nmb.cast_to_int64(left_int) &&
            OB_SUCCESS == right_nmb.cast_to_int64(right_int)) {
          int64_t remainder;
          ObNumStackOnceAlloc tmp_alloc;
          if (OB_FAIL(remainder_int64(left_int, right_int, remainder))) {
            LOG_WARN("failed to call remainder_int64", K(ret), K(left_int), K(right_int));
          } else if (OB_FAIL(res_nmb.from(remainder, tmp_alloc))) {
            LOG_WARN("res_nmb failed to from remaind", K(ret), K(res_nmb), K(remainder));
          } else {
            res.set_number(res_nmb);
          }
        } else {
          ObEvalCtx::TempAllocGuard alloc_guard(ctx);
          ObIAllocator &calc_alloc = alloc_guard.get_allocator();
          if (OB_FAIL(left_nmb.round_remainder(right_nmb, res_nmb, calc_alloc))) {
            LOG_WARN("failed to add numbers", K(ret), K(left), K(right));
          } else {
            res.set_number(res_nmb);
          }
        }
      }
    } else if (ObFloatTC == tc) {
      res.set_float(remainder(left->get_float(), right->get_float()));
    } else if (ObDoubleTC == tc) {
      res.set_double(remainder(left->get_double(), right->get_double()));
    }
  }
  return ret;
}

int ObExprRemainder::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  CK(2 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = calc_remainder_expr;
  return ret;
}

}
}
