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
#include "sql/engine/expr/ob_expr_div.h"
#include "lib/oblog/ob_log.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_batch_eval_util.h"
#include "share/object/ob_obj_cast_util.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace share;
using namespace common::number;

ObExprDiv::ObExprDiv(ObIAllocator &alloc, ObExprOperatorType type)
  : ObArithExprOperator(alloc,
                        type,
                        N_DIV,
                        2,
                        NOT_ROW_DIMENSION,
                        ObExprResultTypeUtil::get_div_result_type,
                        ObExprResultTypeUtil::get_div_calc_type,
                        div_funcs_)
{
  param_lazy_eval_ = lib::is_oracle_mode();
}

#define ROUND_UP(scale) static_cast<ObScale>(((scale) + DIV_CALC_SCALE - 1) / \
                                             DIV_CALC_SCALE * DIV_CALC_SCALE)

int ObExprDiv::calc_result_type2(ObExprResType &type,
                                 ObExprResType &type1,
                                 ObExprResType &type2,
                                 ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t div_precision_increment = 0;

  CK(!OB_UNLIKELY(type_ctx.get_div_precision_increment() < 0));
  div_precision_increment = type_ctx.get_div_precision_increment();

  OC( (ObArithExprOperator::calc_result_type2)(type, type1, type2, type_ctx));
  if (OB_SUCC(ret)) {
    const ObObjTypeClass result_tc = type.get_type_class();
    if (ObNumberTC == result_tc) {
      if (is_oracle_mode()) {
        type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
        type.set_precision(PRECISION_UNKNOWN_YET);

      } else {
        ObScale scale1 = static_cast<ObScale>(MAX(type1.get_scale(), 0));
        ObScale scale2 = static_cast<ObScale>(MAX(type2.get_scale(), 0));
        // res scale.
        ObScale res_scale = static_cast<ObScale>(scale1 + div_precision_increment);
        res_scale = static_cast<ObScale>(MIN(res_scale, OB_MAX_DECIMAL_SCALE));
        type.set_scale(res_scale);

        ObPrecision precision1 = static_cast<ObPrecision>(MAX(type1.get_precision(), 0));
        if (OB_UNLIKELY(PRECISION_UNKNOWN_YET == type1.get_precision()) ||
            OB_UNLIKELY(PRECISION_UNKNOWN_YET == type2.get_precision())) {
          type.set_precision(PRECISION_UNKNOWN_YET);
        } else {
          type.set_precision(static_cast<ObPrecision>(
                               precision1 + MAX(scale2, div_precision_increment)));
        }

        // calc scale.
        if (OB_UNLIKELY(SCALE_UNKNOWN_YET == type1.get_scale()) ||
            OB_UNLIKELY(SCALE_UNKNOWN_YET == type2.get_scale())) {
          type.set_scale(SCALE_UNKNOWN_YET);
        } else {
          ObScale calc_scale = static_cast<ObScale>(
              MAX(ROUND_UP(scale1) + ROUND_UP(scale2),
                  ROUND_UP(scale1 + scale2 + div_precision_increment)));
          type.set_calc_scale(calc_scale);
        }
        LOG_DEBUG("div calc_result_type2", K(type.get_calc_scale()), K(scale1), K(scale2),
                  "new_scale1", ROUND_UP(scale1), "new_scale2", ROUND_UP(scale2),
                  K(div_precision_increment));
      }
    } else if (ObDoubleTC == result_tc || ObFloatTC == result_tc) {
      if (is_oracle_mode()) {
        type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
        type.set_precision(PRECISION_UNKNOWN_YET);
      } else {
        ObScale scale = SCALE_UNKNOWN_YET;
        ObPrecision precision = PRECISION_UNKNOWN_YET;
        if (SCALE_UNKNOWN_YET != type1.get_scale() && SCALE_UNKNOWN_YET != type2.get_scale()) {
          ObScale scale1 = static_cast<ObScale>(MAX(type1.get_scale(), 0));
          ObScale scale2 = static_cast<ObScale>(MAX(type2.get_scale(), 0));
          scale = MAX(scale1, scale2) + div_precision_increment;
          if (scale > OB_MAX_DOUBLE_FLOAT_SCALE) {
            scale = SCALE_UNKNOWN_YET;
          }
        }
        if (PRECISION_UNKNOWN_YET != type1.get_precision() &&
            PRECISION_UNKNOWN_YET != type2.get_precision() &&
            SCALE_UNKNOWN_YET != scale) {
          ObPrecision p1 = ObMySQLUtil::float_length(scale);
          ObPrecision p2 = type1.get_precision() - type1.get_scale() + scale;
          if (ObNumberTC == type1.get_type_class()) {
            p2 += decimal_to_double_precision_inc(type1.get_type(), type1.get_scale());
          }
          precision = MIN(p1, p2);
        }
        type.set_scale(scale);
        type.set_precision(precision);
      }
    } else if (ObIntervalTC == type.get_type_class()) {
      type.set_scale(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][type.get_type()].get_scale());
      type.set_precision(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][type.get_type()].get_precision());
    }
    type.unset_result_flag(NOT_NULL_FLAG); // divided by zero
  }
  return ret;
}

int ObExprDiv::calc(ObObj &res,
                    const ObObj &left,
                    const ObObj &right,
                    ObIAllocator *allocator,
                    ObScale calc_scale)
{
  ObCalcTypeFunc calc_type_func = ObExprResultTypeUtil::get_div_result_type;
  return ObArithExprOperator::calc(res, left, right, allocator, calc_scale, calc_type_func,
                                   div_funcs_);
}

int ObExprDiv::calc_for_avg(ObObj &res,
                    const ObObj &left,
                    const ObObj &right,
                    ObExprCtx &expr_ctx,
                    ObScale res_scale)
{
  int ret = OB_SUCCESS;
  ObCalcTypeFunc calc_type_func = ObExprResultTypeUtil::get_div_result_type;
  ObScale calc_scale = static_cast<ObScale>(res_scale < 0 ?
                                            DIV_MAX_CALC_SCALE :
                                            (res_scale + 8) / 9 * 9);
  if (OB_FAIL(ObArithExprOperator::calc(res, left, right, expr_ctx, calc_scale, calc_type_func,
                                        avg_div_funcs_))) {
    LOG_WARN("calc failed", K(ret), K(left), K(right), K(calc_scale));
  } else if (ob_is_number_tc(res.get_type()) && res_scale >= 0) {
    number::ObNumber res_nmb = res.get_number();
    if (OB_FAIL(res_nmb.round(res_scale))) {
      LOG_WARN("round failed", K(ret), K(res_nmb), K(res_scale));
    } else {
      res.set_number(res_nmb);
    }
  }
  return ret;
}

int ObExprDiv::calc_for_avg(ObDatum &result, const ObDatum &sum, const int64_t count,
    ObEvalCtx &expr_ctx, const common::ObObjType type)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  UNUSED(sum);
  UNUSED(count);
  UNUSED(expr_ctx);
  UNUSED(type);
//  typedef int (*EvalFunc)(ObDatum &result, const ObDatum &left, const ObDatum &right,
//                          ObEvalCtx &ctx);
//  EvalFunc eval_func = NULL;
//  ObDatum new_count;
//  char local_buff[ObNumber::MAX_BYTE_LEN];
//  ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);
//
//  if (ObNumberType == type) {
//    eval_func = div_number;
//    number::ObNumber tmp_num;
//    if (OB_FAIL(tmp_num.from(count, local_alloc))) {
//      LOG_WARN("failed to get number from int", K(ret), K(count));
//    } else {
//      new_count.set_number(tmp_num);
//    }
//  } else if (ObDoubleType == type) {
//    eval_func = div_double;
//    new_count.set_double(count);
//  } else if (ObFloatType == type) {
//    eval_func = div_float;
//    new_count.set_float(count);
//  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("it should not arrive here", K(ret), K(type), K(lbt()));
//  }

//  if (OB_SUCC(ret)) {
//    ret = eval_func(result, sum, new_count, expr_ctx);
//  }
  return ret;
}

ObArithFunc ObExprDiv::div_funcs_[ObMaxTC] =
{
    NULL,
    NULL,
    NULL,
    ObExprDiv::div_float,
    ObExprDiv::div_double,
    ObExprDiv::div_number,
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
    NULL,//otimestamp
    NULL,//raw
    ObExprDiv::div_interval, //interval
};

ObArithFunc ObExprDiv::avg_div_funcs_[ObMaxTC] =
{
    NULL,
    NULL,
    NULL,
    ObExprDiv::div_float,
    ObExprDiv::div_double_no_overflow,
    ObExprDiv::div_number,
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
    NULL,//otimestamp
    NULL,//raw
    ObExprDiv::div_interval, //interval
};

int ObExprDiv::div_float(ObObj &res,
                         const ObObj &left,
                         const ObObj &right,
                         ObIAllocator *allocator,
                         ObScale scale)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lib::is_oracle_mode())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only oracle mode arrive here", K(ret), K(left), K(right));
  } else if (OB_UNLIKELY(left.get_type_class() != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    res.set_float(left.get_float() / right.get_float());
    if (OB_UNLIKELY(is_float_out_of_range(res.get_float()))
        && !lib::is_oracle_mode()) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%e / %e)'",
                      left.get_float(),
                      right.get_float());
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BINARY_FLOAT", expr_str);
      LOG_WARN("double out of range", K(res), K(left), K(right), K(res));
    }
    LOG_DEBUG("succ to div float", K(res.get_float()), K(left.get_float()),
        K(right.get_float()));
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprDiv::div_double(ObObj &res,
                          const ObObj &left,
                          const ObObj &right,
                          ObIAllocator *allocator,
                          ObScale scale)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left.get_type_class() != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else if (lib::is_mysql_mode() && (fabs(right.get_double()) == 0.0)) {
      res.set_null();
  } else {
    res.set_double(left.get_double() / right.get_double());
    if (OB_UNLIKELY(is_double_out_of_range(res.get_double()))
        && !lib::is_oracle_mode()) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%e / %e)'",
                      left.get_double(),
                      right.get_double());
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DOUBLE", expr_str);
      LOG_WARN("double out of range", K(res), K(left), K(right), K(res));
      res.set_null();
    }
    LOG_DEBUG("succ to div double", K(res.get_double()), K(left.get_double()),
        K(right.get_double()));
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprDiv::div_double_no_overflow(ObObj &res,
                                      const ObObj &left,
                                      const ObObj &right,
                                      ObIAllocator *,
                                      ObScale)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left.get_type_class() != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else if (lib::is_mysql_mode() && (fabs(right.get_double()) == 0.0)) {
      res.set_null();
  } else {
    res.set_double(left.get_double() / right.get_double());
    LOG_DEBUG("succ to div double", K(res.get_double()), K(left.get_double()),
              K(right.get_double()));
  }
  return ret;
}

int ObExprDiv::div_number(ObObj &res,
                          const ObObj &left,
                          const ObObj &right,
                          ObIAllocator *allocator,
                          ObScale calc_scale)
{
  int ret = OB_SUCCESS;
  number::ObNumber res_nmb;
  if (OB_UNLIKELY(NULL == allocator)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocator is null", K(ret));
  } else if (OB_UNLIKELY(right.get_number().is_zero())) {
    if (is_oracle_mode()) {
      ret = OB_ERR_DIVISOR_IS_ZERO;
      LOG_WARN("divisor is equal to zero on oracle mode", K(ret), K(right));
    } else {
      res.set_null();
    }
  } else if (OB_FAIL(left.get_number().div_v3(right.get_number(), res_nmb, *allocator))) {
    LOG_WARN("failed to div numbers", K(ret), K(left), K(right));
  } else {

    if (calc_scale >= 0) {
      //calc_scale is calc_scale ,not res_scale.
      //trunc with calc_scale and round with res_scale
      if (OB_FAIL(res_nmb.trunc(calc_scale))) {
        LOG_WARN("failed to trunc result number", K(ret), K(res_nmb), K(calc_scale));
      }
    }
    if (OB_SUCC(ret)) {
      if (ObUNumberType == res.get_type()) {
        res.set_unumber(res_nmb);
      } else {
        res.set_number(res_nmb);
      }
    }
  }
  return ret;
}

int ObExprDiv::div_interval(ObObj &res,
                            const ObObj &left,
                            const ObObj &right,
                            ObIAllocator *allocator,
                            ObScale calc_scale)
{
  int ret = OB_SUCCESS;
  number::ObNumber res_number;
  number::ObNumber left_number;

  if (OB_UNLIKELY(left.get_type_class() != ObIntervalTC)
      || OB_UNLIKELY(right.get_type_class() != ObNumberTC)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else if (OB_UNLIKELY(NULL == allocator)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocator is null", K(ret));
  } else if (OB_UNLIKELY(right.get_number().is_zero())) {
    ret = OB_ERR_DIVISOR_IS_ZERO;
    LOG_WARN("divisor is equal to zero on oracle mode", K(ret), K(right));
  } else if (left.is_interval_ym()) {
    int64_t result_nmonth = 0;
    if (OB_FAIL(left_number.from(left.get_interval_ym().get_nmonth(), *allocator))) {
      LOG_WARN("failed to convert to number", K(ret), K(left));
    } else if (OB_FAIL(left_number.div_v3(right.get_number(), res_number, *allocator))) {
      LOG_WARN("failed to do mul", K(ret), K(left), K(right));
    } else if (OB_FAIL(res_number.trunc(0))) {
      LOG_WARN("failed to do trunc", K(ret), K(res_number));
    } else if (OB_UNLIKELY(!res_number.is_valid_int64(result_nmonth))) {
      ret = OB_INVALID_NUMERIC;
      LOG_WARN("failed to get int64_t from number", K(ret), K(res_number));
    } else if (OB_FAIL(ObIntervalYMValue(result_nmonth).validate())) {
      LOG_WARN("invalid interval ym result", K(ret), K(result_nmonth));
    } else {
      res.set_interval_ym(ObIntervalYMValue(result_nmonth));
    }
  } else {
    int64_t result_nsecond = 0;
    int64_t result_fs = 0;
    number::ObNumber nsecond;
    number::ObNumber fsecond;
    number::ObNumber power10;
    number::ObNumber midresult;
    number::ObNumber nvalue;
    ObIntervalDSValue interval_res;
    static_assert(number::ObNumber::BASE == ObIntervalDSValue::MAX_FS_VALUE,
                  "the div caculation between interval day to second and number is base on this constrain");

    if (OB_FAIL(nsecond.from(left.get_interval_ds().get_nsecond(), *allocator))) {
      LOG_WARN("failed to convert interval to number", K(ret));
    } else if (OB_FAIL(fsecond.from(static_cast<int64_t>(left.get_interval_ds().get_fs()), *allocator))) {
      LOG_WARN("failed to convert interval to number", K(ret));
    } else if (OB_FAIL(power10.from(static_cast<int64_t>(ObIntervalDSValue::MAX_FS_VALUE), *allocator))) {
      LOG_WARN("failed to round number", K(ret));
    } else if (OB_FAIL(nsecond.mul_v3(power10, nvalue, *allocator))) {
      LOG_WARN("fail to div fs", K(ret));
    } else if (OB_FAIL(nvalue.add_v3(fsecond, left_number, *allocator))) {
      LOG_WARN("failed to add number", K(ret));
    } else if (OB_FAIL(left_number.div_v3(right.get_number(), midresult, *allocator))) {
      LOG_WARN("failed to do mul", K(ret));
    } else if (OB_FAIL(midresult.div_v3(power10, res_number, *allocator))) {
      LOG_WARN("failed to do mul", K(ret));
    } else if (OB_FAIL(res_number.round(MAX_SCALE_FOR_ORACLE_TEMPORAL))) {
      LOG_WARN("failed to round number", K(res_number));
    } else if (OB_UNLIKELY(!res_number.is_int_parts_valid_int64(result_nsecond, result_fs))) {
      ret = OB_INVALID_NUMERIC;
      LOG_WARN("invalid date format", K(ret));
    } else {
      if (result_nsecond < 0) {
        result_fs = -result_fs;
      }
      interval_res = ObIntervalDSValue(result_nsecond, static_cast<int32_t>(result_fs));
      if (OB_FAIL(interval_res.validate())) {
        LOG_WARN("invalid interval result", K(ret), K(interval_res));
      } else {
        res.set_interval_ds(interval_res);
      }
    }
  }

  res.set_scale(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][res.get_type()].get_scale());
  UNUSED(calc_scale);
  return ret;
}

const ObScale ObExprDiv::DIV_CALC_SCALE = 9;
const ObScale ObExprDiv::DIV_MAX_CALC_SCALE = 100;

struct ObFloatDivFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, const bool &is_oracle) const
  {
    int ret = OB_SUCCESS;
    const float left_f = l.get_float();
    const float right_f = r.get_float();
    const float result_f = left_f / right_f;
    if (OB_UNLIKELY(ObExprDiv::is_float_out_of_range(result_f))
        && !is_oracle) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%e / %e)'",
                      left_f,
                      right_f);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BINARY_FLOAT", expr_str);
      LOG_WARN("float out of range", K(ret), K(left_f), K(right_f));
    } else {
      res.set_float(result_f);
      LOG_DEBUG("succ to div float", K(left_f), K(right_f), K(result_f));
    }
    return ret;
  }
};


int ObExprDiv::div_float(EVAL_FUNC_ARG_DECL)
{
  const bool is_oracle = lib::is_oracle_mode();
  return def_arith_eval_func<ObFloatDivFunc>(EVAL_FUNC_ARG_LIST, is_oracle);
}

int ObExprDiv::div_float_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  const bool is_oracle = lib::is_oracle_mode();
  return def_batch_arith_op_by_datum_func<ObFloatDivFunc>(BATCH_EVAL_FUNC_ARG_LIST, is_oracle);
}

struct ObDoubleDivFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r,
                 const ObExpr &expr, const bool &is_oracle) const
  {
    int ret = OB_SUCCESS;
    const double left_d = l.get_double();
    const double right_d = r.get_double();
    if (!is_oracle && (fabs(right_d) == 0.0)) {
      if (expr.is_error_div_by_zero_) {
        ret = OB_DIVISION_BY_ZERO;
      } else {
        res.set_null();
      }
    } else {
      const double result_d = left_d / right_d;
      if (OB_UNLIKELY(ObExprDiv::is_double_out_of_range(result_d))
          && T_OP_AGG_DIV != expr.type_
          && !is_oracle) {
        ret = OB_OPERATE_OVERFLOW;
        char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
        int64_t pos = 0;
        databuff_printf(expr_str,
                        OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                        pos,
                        "'(%e / %e)'",
                        left_d,
                        right_d);
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, is_oracle_mode() ? "BINARY_DOUBLE" : "DOUBLE", expr_str);
        LOG_WARN("double out of range", K(ret), "left", left_d, "right", right_d);
      } else {
        res.set_double(result_d);
      }
      LOG_DEBUG("succ to div double", K(left_d), K(right_d), K(result_d));
    }
    return ret;
  }
};

int ObExprDiv::div_double(EVAL_FUNC_ARG_DECL)
{
  const bool is_oracle = lib::is_oracle_mode();
  return def_arith_eval_func<ObDoubleDivFunc>(EVAL_FUNC_ARG_LIST, expr, is_oracle);
}

int ObExprDiv::div_double_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  const bool is_oracle = lib::is_oracle_mode();
  return def_batch_arith_op_by_datum_func<ObDoubleDivFunc>(BATCH_EVAL_FUNC_ARG_LIST, expr, is_oracle);
}

struct ObNumberDivFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, const ObExpr &expr,
                 ObEvalCtx &ctx, const bool &is_oracle) const
  {
    int ret = OB_SUCCESS;
    if (r.get_number().is_zero()) {
      if (is_oracle) {
        ret = OB_ERR_DIVISOR_IS_ZERO;
        LOG_WARN("divisor is equal to zero on oracle mode", K(ret));
      } else if (expr.is_error_div_by_zero_) {
        ret = OB_DIVISION_BY_ZERO;
      } else {
        res.set_null();
        LOG_DEBUG("divisor is equal to zero", K(l), K(ret));
      }
    } else {
      number::ObNumber lnum(l.get_number());
      number::ObNumber rnum(r.get_number());
      char local_buff[ObNumber::MAX_BYTE_LEN];
      ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);
      ObNumber result_num;

      if (OB_FAIL(lnum.div_v3(rnum, result_num, local_alloc))) {
        LOG_WARN("add number failed", K(ret));
      } else {
        if (is_oracle) {
          res.set_number(result_num);
        } else {
          int64_t div_pi = 0;
          if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_div_precision_increment(div_pi))) {
            LOG_WARN("get_div_precision_increment failed", K(ret));
          } else {
            //          const int64_t scale1 = lnum.get_scale();
            //          const int64_t scale2 = rnum.get_scale();
            //          const int64_t new_scale1 = ROUND_UP(scale1);
            //          const int64_t new_scale2 = ROUND_UP(scale2);
            //          const int64_t calc_scale = ROUND_UP(new_scale1 + new_scale2 + div_pi);
            const int64_t calc_scale = expr.div_calc_scale_;
            if (calc_scale >= 0 && OB_FAIL(result_num.trunc(calc_scale))) {
              //calc_scale is calc_scale ,not res_scale.
              //trunc with calc_scale and round with res_scale
              LOG_WARN("failed to trunc result number", K(ret), K(result_num), K(calc_scale));
            } else {
              res.set_number(result_num);
            }
            LOG_DEBUG("finish div", K(ret), K(calc_scale),
                      /*K(scale1), K(scale2), K(new_scale1), K(new_scale2),*/
                      K(div_pi), K(result_num), K(lnum), K(rnum));
          }
        }
      }
    }
    return ret;
  }
};

int ObExprDiv::div_number(EVAL_FUNC_ARG_DECL)
{
  const bool is_oracle = lib::is_oracle_mode();
  return def_arith_eval_func<ObNumberDivFunc>(EVAL_FUNC_ARG_LIST, expr, ctx, is_oracle);
}

int ObExprDiv::div_number_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  const bool is_oracle = lib::is_oracle_mode();
  return def_batch_arith_op_by_datum_func<ObNumberDivFunc>(BATCH_EVAL_FUNC_ARG_LIST, expr, ctx, is_oracle);
}

struct ObIntervalYMNumberDivFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    if (r.get_number().is_zero()) {
      ret = OB_ERR_DIVISOR_IS_ZERO;
      LOG_WARN("divisor is equal to zero on oracle mode", K(ret));
    } else {
      number::ObNumber lnum;
      number::ObNumber rnum(r.get_number());
      char local_buff[ObNumber::MAX_BYTE_LEN * 2];
      ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN * 2);
      ObNumber result_number;
      int64_t result_nmonth = 0;
      if (OB_FAIL(lnum.from(l.get_interval_nmonth(), local_alloc))) {
        LOG_WARN("failed to convert to number", K(ret), K(l));
      } else if (OB_FAIL(lnum.div_v3(rnum, result_number, local_alloc))) {
        LOG_WARN("failed to do mul", K(ret), K(l), K(r));
      } else if (OB_FAIL(result_number.trunc(0))) {
        LOG_WARN("failed to do trunc", K(ret), K(result_number));
      } else if (OB_UNLIKELY(!result_number.is_valid_int64(result_nmonth))) {
        ret = OB_INVALID_NUMERIC;
        LOG_WARN("failed to get int64_t from number", K(ret), K(result_number));
      } else {
        ObIntervalYMValue value = ObIntervalYMValue(result_nmonth);
        if (OB_FAIL(value.validate())) {
          LOG_WARN("invalid interval ym result", K(ret), K(value));
        } else {
          res.set_interval_nmonth(result_nmonth);
        }
      }
    }
    return ret;
  };
};

int ObExprDiv::div_intervalym_number(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObIntervalYMNumberDivFunc>(EVAL_FUNC_ARG_LIST);
}

int ObExprDiv::div_intervalym_number_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalYMNumberDivFunc>(BATCH_EVAL_FUNC_ARG_LIST);
}

struct ObIntervalDSNumberDivFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    if (r.get_number().is_zero()) {
      ret = OB_ERR_DIVISOR_IS_ZERO;
      LOG_WARN("divisor is equal to zero on oracle mode", K(ret));
    } else {
      char local_buff[ObNumber::MAX_BYTE_LEN * 7];
      ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN * 7);
      number::ObNumber lnum;
      ObNumber result_number;
      int64_t result_nsecond = 0;
      int64_t result_fs = 0;
      number::ObNumber nsecond;
      number::ObNumber fsecond;
      number::ObNumber power10;
      number::ObNumber midresult;
      number::ObNumber nvalue;
      if (OB_FAIL(nsecond.from(l.get_interval_ds().get_nsecond(), local_alloc))) {
        LOG_WARN("failed to convert interval to number", K(ret));
      } else if (OB_FAIL(fsecond.from(static_cast<int64_t>(l.get_interval_ds().get_fs()),
                                      local_alloc))) {
        LOG_WARN("failed to convert interval to number", K(ret));
      } else if (OB_FAIL(power10.from(static_cast<int64_t>(ObIntervalDSValue::MAX_FS_VALUE),
                                      local_alloc))) {
        LOG_WARN("failed to round number", K(ret));
      } else if (OB_FAIL(nsecond.mul_v3(power10, nvalue, local_alloc))) {
        LOG_WARN("fail to div fs", K(ret));
      } else if (OB_FAIL(nvalue.add_v3(fsecond, lnum, local_alloc))) {
        LOG_WARN("failed to add number", K(ret));
      } else if (OB_FAIL(lnum.div_v3(r.get_number(), midresult, local_alloc))) {
        LOG_WARN("failed to do mul", K(ret));
      } else if (OB_FAIL(midresult.div_v3(power10, result_number, local_alloc))) {
        LOG_WARN("failed to do mul", K(ret));
      } else if (OB_FAIL(result_number.round(MAX_SCALE_FOR_ORACLE_TEMPORAL))) {
        LOG_WARN("failed to round number", K(result_number));
      } else if (OB_UNLIKELY(!result_number.is_int_parts_valid_int64(result_nsecond, result_fs))) {
        ret = OB_INVALID_NUMERIC;
        LOG_WARN("invalid date format", K(ret));
      } else {
        if (result_nsecond < 0) {
          result_fs = -result_fs;
        }
        ObIntervalDSValue value = ObIntervalDSValue(result_nsecond,static_cast<int32_t>(result_fs));
        if (OB_FAIL(value.validate())) {
          LOG_WARN("invalid interval result", K(ret), K(value));
        } else {
          res.set_interval_ds(value);
        }
      }
    }
    return ret;
  }
};

int ObExprDiv::div_intervalds_number(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObIntervalDSNumberDivFunc>(EVAL_FUNC_ARG_LIST);
}

int ObExprDiv::div_intervalds_number_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalDSNumberDivFunc>(BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprDiv::cg_expr(ObExprCGCtx &op_cg_ctx,
    const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
#define SET_DIV_FUNC_PTR(v) \
  rt_expr.eval_func_ = ObExprDiv::v; \
  rt_expr.eval_batch_func_ = ObExprDiv::v##_batch;
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  OB_ASSERT(2 == rt_expr.arg_cnt_);
  OB_ASSERT(NULL != rt_expr.args_);
  OB_ASSERT(NULL != rt_expr.args_[0]);
  OB_ASSERT(NULL != rt_expr.args_[1]);
  const common::ObObjType left = rt_expr.args_[0]->datum_meta_.type_;
  const common::ObObjType right = rt_expr.args_[1]->datum_meta_.type_;
  OB_ASSERT(left == input_types_[0].get_calc_type());
  OB_ASSERT(right == input_types_[1].get_calc_type());

  rt_expr.inner_functions_ = NULL;
  LOG_DEBUG("arrive here cg_expr", K(ret), K(raw_expr), K(rt_expr));
  rt_expr.div_calc_scale_ = raw_expr.get_result_type().get_calc_scale();
  switch (rt_expr.datum_meta_.type_) {
    case ObFloatType: {
      SET_DIV_FUNC_PTR(div_float);
      break;
    }
    case ObDoubleType: {
      SET_DIV_FUNC_PTR(div_double);
      break;
    }
    case ObUNumberType:
    case ObNumberType: {
      SET_DIV_FUNC_PTR(div_number);
      break;
    }
    case ObIntervalYMType: {
      SET_DIV_FUNC_PTR(div_intervalym_number);
      break;
    }
    case ObIntervalDSType: {
      SET_DIV_FUNC_PTR(div_intervalds_number);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected result type", K(ret), K(rt_expr.datum_meta_.type_));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(op_cg_ctx.session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session is null", K(ret));
  } else {
    stmt::StmtType stmt_type = op_cg_ctx.session_->get_stmt_type();
    if (lib::is_mysql_mode()
        && is_error_for_division_by_zero(op_cg_ctx.session_->get_sql_mode())
        && is_strict_mode(op_cg_ctx.session_->get_sql_mode())
        && !op_cg_ctx.session_->is_ignore_stmt()
        && (stmt::T_INSERT == stmt_type
            || stmt::T_REPLACE == stmt_type
            || stmt::T_UPDATE == stmt_type)) {
      rt_expr.is_error_div_by_zero_ = true;
    } else {
      rt_expr.is_error_div_by_zero_ = false;
    }
  }

  return ret;
#undef SET_DIV_FUNC_PTR
}

}
}


