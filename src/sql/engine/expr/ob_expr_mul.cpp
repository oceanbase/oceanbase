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

#include "sql/engine/expr/ob_expr_mul.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
using namespace common;
using namespace common::number;

ObExprMul::ObExprMul(ObIAllocator& alloc, ObExprOperatorType type)
    : ObArithExprOperator(alloc, type, N_MUL, 2, NOT_ROW_DIMENSION, ObExprResultTypeUtil::get_mul_result_type,
          ObExprResultTypeUtil::get_mul_calc_type, mul_funcs_)
{
  param_lazy_eval_ = lib::is_oracle_mode();
}

int ObExprMul::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObArithExprOperator::calc_result_type2(type, type1, type2, type_ctx))) {
  } else if (lib::is_oracle_mode()) {
    if (type.is_oracle_decimal()) {
      type.set_scale(NUMBER_SCALE_UNKNOWN_YET);
      type.set_precision(PRECISION_UNKNOWN_YET);

    } else if (type.get_type_class() == ObIntervalTC) {
      type.set_scale(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][type.get_type()].get_scale());
      type.set_precision(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][type.get_type()].get_precision());
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("mul expr only support number or char-type for now", K(ret), K(type1), K(type2));
      ObObjType err_type = !type1.is_number() && !type1.is_varchar_or_char() ? type1.get_type() : type2.get_type();
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(ObNumberType), ob_obj_type_str(err_type));
    }
  } else {
    ObScale scale1 = static_cast<ObScale>(MAX(type1.get_scale(), 0));
    ObScale scale2 = static_cast<ObScale>(MAX(type2.get_scale(), 0));
    if (SCALE_UNKNOWN_YET == type1.get_scale() || SCALE_UNKNOWN_YET == type2.get_scale()) {
      type.set_scale(SCALE_UNKNOWN_YET);
    } else {
      if (lib::is_oracle_mode()) {
        type.set_scale(MIN(static_cast<ObScale>(scale1 + scale2), OB_MAX_NUMBER_SCALE));
      } else {
        type.set_scale(MIN(static_cast<ObScale>(scale1 + scale2), OB_MAX_DECIMAL_SCALE));
      }
    }
    ObPrecision precision1 = static_cast<ObPrecision>(MAX(type1.get_precision(), 0));
    ObPrecision precision2 = static_cast<ObPrecision>(MAX(type2.get_precision(), 0));
    if (OB_UNLIKELY(PRECISION_UNKNOWN_YET == type1.get_precision()) ||
        OB_UNLIKELY(PRECISION_UNKNOWN_YET == type2.get_precision())) {
      type.set_precision(PRECISION_UNKNOWN_YET);
    } else {
      // estimated precision
      type.set_precision(static_cast<ObPrecision>((precision1 - scale1) + (precision2 - scale2) + type.get_scale()));
    }
  }
  return ret;
}

int ObExprMul::calc_result2(ObObj& result, const ObObj& left, const ObObj& right, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    if (OB_FAIL(param_eval(expr_ctx, left, 0))) {
      LOG_WARN("parameter evaluate failed", K(ret));
    } else if (left.is_null_oracle()) {
      result.set_null();
    } else if (OB_FAIL(param_eval(expr_ctx, right, 1))) {
      LOG_WARN("parameter evaluate failed", K(ret));
    } else {
      ObObjTypeClass tc1 = left.get_type_class();
      ObObjTypeClass tc2 = right.get_type_class();
      ObScale calc_scale = result_type_.get_calc_scale();
      if (ObIntervalTC == tc1 && ObNumberTC == tc2) {
        ret = mul_interval(result, left, right, expr_ctx.calc_buf_, calc_scale);
      } else if (ObNumberTC == tc1 && ObIntervalTC == tc2) {
        ret = mul_interval(result, right, left, expr_ctx.calc_buf_, calc_scale);
      } else {
        // When the mul_xxx method is called later,
        // these methods can get the compile-time result through result.get_type()
        // Type in order to reasonably set the actual calculation result
        result.set_type(get_result_type().get_type());
        ret = ObArithExprOperator::calc_(
            result, left, right, expr_ctx, result_type_.get_calc_scale(), result_type_.get_calc_type(), mul_funcs_);
      }
    }
  } else if (OB_UNLIKELY(ObNullType == left.get_type() || ObNullType == right.get_type())) {
    result.set_null();
  } else {
    ObObjTypeClass tc1 = left.get_type_class();
    ObObjTypeClass tc2 = right.get_type_class();
    ObScale calc_scale = result_type_.get_calc_scale();
    ObObjType calc_type = result_type_.get_calc_type();
    if (ObIntTC == tc1) {
      if (ObIntTC == tc2 || ObUIntTC == tc2) {
        ret = mul_int(result, left, right, expr_ctx.calc_buf_, calc_scale);
      } else {
        ret = ObArithExprOperator::calc_(result, left, right, expr_ctx, calc_scale, calc_type, mul_funcs_);
      }
    } else if (ObUIntTC == tc1) {
      if (ObIntTC == tc2 || ObUIntTC == tc2) {
        ret = mul_uint(result, left, right, expr_ctx.calc_buf_, calc_scale);
      } else {
        ret = ObArithExprOperator::calc_(result, left, right, expr_ctx, calc_scale, calc_type, mul_funcs_);
      }
    } else {
      result.set_type(get_result_type().get_type());
      ret = ObArithExprOperator::calc_(
          result, left, right, expr_ctx, calc_scale, calc_type, T_OP_AGG_MUL == type_ ? agg_mul_funcs_ : mul_funcs_);
    }
  }
  return ret;
}

int ObExprMul::calc(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  ObCalcTypeFunc calc_type_func = ObExprResultTypeUtil::get_arith_result_type;
  return ObArithExprOperator::calc(res, left, right, allocator, scale, calc_type_func, mul_funcs_);
}

int ObExprMul::calc(ObObj& res, const ObObj& left, const ObObj& right, ObExprCtx& expr_ctx, ObScale scale)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass tc1 = left.get_type_class();
  const ObObjTypeClass tc2 = right.get_type_class();
  if (lib::is_oracle_mode() && ObIntervalTC == tc1 && ObNumberTC == tc2) {
    ret = mul_interval(res, left, right, expr_ctx.calc_buf_, scale);
  } else if (lib::is_oracle_mode() && ObNumberTC == tc1 && ObIntervalTC == tc2) {
    ret = mul_interval(res, right, left, expr_ctx.calc_buf_, scale);
  } else {
    ret = ObArithExprOperator::calc(
        res, left, right, expr_ctx, scale, ObExprResultTypeUtil::get_mul_result_type, mul_funcs_);
  }
  return ret;
}

ObArithFunc ObExprMul::mul_funcs_[ObMaxTC] = {
    NULL,
    ObExprMul::mul_int,
    ObExprMul::mul_uint,
    ObExprMul::mul_float,
    ObExprMul::mul_double,
    ObExprMul::mul_number,
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    NULL,  // extend
    NULL,  // unknown
    NULL,  // text
    NULL,  // bit
    NULL,  // enumset
    NULL,  // enumsetInner
    NULL,  // otimestamp
    NULL,  // raw
    NULL,  // interval
};

ObArithFunc ObExprMul::agg_mul_funcs_[ObMaxTC] = {
    NULL,
    ObExprMul::mul_int,
    ObExprMul::mul_uint,
    ObExprMul::mul_float,
    ObExprMul::mul_double_no_overflow,
    ObExprMul::mul_number,
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    NULL,  // extend
    NULL,  // unknown
    NULL,  // text
    NULL,  // bit
    NULL,  // enumset
    NULL,  // enumsetInner
    NULL,  // otimestamp
    NULL,  // raw
    NULL,  // interval
};

int ObExprMul::mul_int(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  int64_t left_i = left.get_int();
  int64_t right_i = right.get_int();
  char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
  int64_t pos = 0;
  if (left.get_type_class() == right.get_type_class()) {
    if (OB_UNLIKELY(is_multi_overflow64(left_i, right_i))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%ld * %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT", expr_str);
    }
    res.set_int(left_i * right_i);
  } else if (OB_UNLIKELY(ObUIntTC != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    if (OB_UNLIKELY(is_int_uint_mul_out_of_range(left_i, right_i))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%ld * %lu)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
    res.set_uint64(left_i * right_i);
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMul::mul_uint(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  uint64_t left_i = left.get_uint64();
  uint64_t right_i = right.get_uint64();
  char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
  int64_t pos = 0;
  if (left.get_type_class() == right.get_type_class()) {
    if (OB_UNLIKELY(is_uint_uint_mul_out_of_range(left_i, right_i))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%lu * %lu)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
    res.set_uint64(left_i * right_i);
  } else if (OB_UNLIKELY(ObIntTC != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    if (OB_UNLIKELY(is_int_uint_mul_out_of_range(right_i, left_i))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%lu * %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
    res.set_uint64(left_i * right_i);
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMul::mul_float(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lib::is_oracle_mode())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only oracle mode arrive here", K(ret), K(left), K(right));
  } else if (OB_UNLIKELY(left.get_type_class() != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    float left_f = left.get_float();
    float right_f = right.get_float();
    res.set_float(left_f * right_f);
    if (OB_UNLIKELY(is_float_out_of_range(res.get_float()))) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%e * %e)'", left_f, right_f);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BINARY_FLOAT", expr_str);
      LOG_WARN("float out of range", K(ret), K(left), K(right), K(res));
    }
    LOG_DEBUG("succ to mul float", K(left), K(right));
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMul::mul_double(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left.get_type_class() != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    double left_d = left.get_double();
    double right_d = right.get_double();
    res.set_double(left_d * right_d);
    if (OB_UNLIKELY(is_double_out_of_range(res.get_double()))) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%e * %e)'", left_d, right_d);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, lib::is_oracle_mode() ? "BINARY_DOUBLE" : "DOUBLE", expr_str);
      LOG_WARN("double out of range", K(ret), K(left), K(right), K(res));
      res.set_null();
    }
    LOG_DEBUG("succ to mul double", K(left), K(right));
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMul::mul_double_no_overflow(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator*, ObScale)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left.get_type_class() != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    double left_d = left.get_double();
    double right_d = right.get_double();
    res.set_double(left_d * right_d);
    LOG_DEBUG("succ to mul double", K(left), K(right));
  }
  return ret;
}

int ObExprMul::mul_number(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  number::ObNumber res_nmb;
  if (OB_UNLIKELY(NULL == allocator)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocator is null", K(ret));
  } else if (OB_FAIL(left.get_number().mul_v3(right.get_number(), res_nmb, *allocator))) {
    LOG_WARN("failed to mul numbers", K(ret), K(left), K(right));
  } else {
    ObObjType res_type = res.get_type();
    if (ObUNumberType == res_type) {
      res.set_unumber(res_nmb);
    } else {
      res.set_number(res_nmb);
    }
  }
  UNUSED(scale);
  return ret;
}

int ObExprMul::mul_interval(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  number::ObNumber res_number;
  number::ObNumber left_number;

  if (OB_UNLIKELY(left.get_type_class() != ObIntervalTC) || OB_UNLIKELY(right.get_type_class() != ObNumberTC)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocator is null", K(ret));
  } else if (left.is_interval_ym()) {
    int64_t result_nmonth = 0;
    ObIntervalYMValue interval_res;

    if (OB_FAIL(left_number.from(left.get_interval_ym().get_nmonth(), *allocator))) {
      LOG_WARN("failed to convert to number", K(ret));
    } else if (OB_FAIL(left_number.mul_v3(right.get_number(), res_number, *allocator))) {
      LOG_WARN("failed to do mul", K(ret));
    } else if (OB_FAIL(res_number.trunc(0))) {
      LOG_WARN("failed to do trunc", K(ret));
    } else if (OB_UNLIKELY(!res_number.is_valid_int64(result_nmonth))) {
      ret = OB_INVALID_NUMERIC;
      LOG_WARN("failed to get int64_t from number", K(ret));
    } else {
      interval_res = ObIntervalYMValue(result_nmonth);
      if (OB_FAIL(interval_res.validate())) {
        LOG_WARN("invalid interval ym result", K(ret), K(interval_res));
      } else {
        res.set_interval_ym(interval_res);
      }
    }
  } else {
    int64_t result_nsecond = 0;
    int64_t result_fs = 0;
    number::ObNumber nvalue;
    number::ObNumber fsecond;
    number::ObNumber power10;
    number::ObNumber fvalue;
    ObIntervalDSValue interval_res;
    static_assert(number::ObNumber::BASE == ObIntervalDSValue::MAX_FS_VALUE,
        "the mul caculation between interval day to second and number is base on this constrain");

    if (OB_FAIL(nvalue.from(left.get_interval_ds().get_nsecond(), *allocator))) {
      LOG_WARN("failed to convert interval to number", K(ret));
    } else if (OB_FAIL(fsecond.from(static_cast<int64_t>(left.get_interval_ds().get_fs()), *allocator))) {
      LOG_WARN("failed to convert interval to number", K(ret));
    } else if (OB_FAIL(power10.from(static_cast<int64_t>(ObIntervalDSValue::MAX_FS_VALUE), *allocator))) {
      LOG_WARN("failed to round number", K(ret));
    } else if (OB_FAIL(fsecond.div_v3(power10, fvalue, *allocator))) {
      LOG_WARN("fail to div fs", K(ret));
    } else if (OB_FAIL(nvalue.add_v3(fvalue, left_number, *allocator))) {
      LOG_WARN("failed to add number", K(ret));
    } else if (OB_FAIL(left_number.mul_v3(right.get_number(), res_number, *allocator))) {
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
        LOG_WARN("invalid interval result", K(ret), K(nvalue), K(fvalue), K(left_number), K(interval_res));
      } else {
        res.set_interval_ds(interval_res);
      }
    }
  }
  res.set_scale(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][res.get_type()].get_scale());
  UNUSED(scale);
  return ret;
}

int ObExprMul::mul_int_int(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_finish = false;
  if (OB_FAIL(get_arith_operand(expr, ctx, left, right, datum, is_finish))) {
    LOG_WARN("get_arith_operand failed", K(ret));
  } else if (is_finish) {
    // do nothing
  } else {
    int64_t left_i = left->get_int();
    int64_t right_i = right->get_int();
    if (OB_UNLIKELY(is_multi_overflow64(left_i, right_i))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%ld * %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT", expr_str);
    }
    datum.set_int(left_i * right_i);
  }
  return ret;
}

int ObExprMul::mul_int_uint(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_finish = false;
  if (OB_FAIL(get_arith_operand(expr, ctx, left, right, datum, is_finish))) {
    LOG_WARN("get_arith_operand failed", K(ret));
  } else if (is_finish) {
    // do nothing
  } else {
    int64_t left_i = left->get_int();
    uint64_t right_ui = right->get_uint();
    if (OB_UNLIKELY(is_int_uint_mul_out_of_range(left_i, right_ui))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%ld * %lu)'", left_i, right_ui);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
    datum.set_uint(left_i * right_ui);
  }
  return ret;
}

int ObExprMul::mul_uint_int(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_finish = false;
  if (OB_FAIL(get_arith_operand(expr, ctx, left, right, datum, is_finish))) {
    LOG_WARN("get_arith_operand failed", K(ret));
  } else if (is_finish) {
    // do nothing
  } else {
    uint64_t left_ui = left->get_uint();
    int64_t right_i = right->get_int();
    if (OB_UNLIKELY(is_int_uint_mul_out_of_range(right_i, left_ui))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%lu * %ld)'", left_ui, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
    datum.set_uint(left_ui * right_i);
  }
  return ret;
}

int ObExprMul::mul_uint_uint(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_finish = false;
  if (OB_FAIL(get_arith_operand(expr, ctx, left, right, datum, is_finish))) {
    LOG_WARN("get_arith_operand failed", K(ret));
  } else if (is_finish) {
    // do nothing
  } else {
    uint64_t left_ui = left->get_uint();
    uint64_t right_ui = right->get_uint();
    if (OB_UNLIKELY(is_uint_uint_mul_out_of_range(left_ui, right_ui))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%lu * %lu)'", left_ui, right_ui);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
    datum.set_uint(left_ui * right_ui);
  }
  return ret;
}

int ObExprMul::mul_float(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_finish = false;
  if (OB_UNLIKELY(!lib::is_oracle_mode())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only oracle mode arrive here", K(ret), K(left), K(right));
  } else if (OB_FAIL(get_arith_operand(expr, ctx, left, right, datum, is_finish))) {
    LOG_WARN("get_arith_operand failed", K(ret));
  } else if (is_finish) {
    // do nothing
  } else {
    const float left_f = left->get_float();
    const float right_f = right->get_float();
    datum.set_float(left_f * right_f);
    if (OB_UNLIKELY(is_float_out_of_range(datum.get_float()))) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%e * %e)'", left_f, right_f);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BINARY_FLOAT", expr_str);
      LOG_WARN("float out of range", K(left_f), K(right_f));
    }
  }
  return ret;
}

int ObExprMul::mul_double(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_finish = false;
  if (OB_FAIL(get_arith_operand(expr, ctx, left, right, datum, is_finish))) {
    LOG_WARN("get_arith_operand failed", K(ret));
  } else if (is_finish) {
    // do nothing
  } else {
    const double left_d = left->get_double();
    const double right_d = right->get_double();
    datum.set_double(left_d * right_d);
    if (OB_UNLIKELY(is_double_out_of_range(datum.get_double())) && T_OP_AGG_MUL != expr.type_) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%e * %e)'", left_d, right_d);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, lib::is_oracle_mode() ? "BINARY_DOUBLE" : "DOUBLE", expr_str);
      LOG_WARN("double out of range", K(left_d), K(left_d));
    }
    LOG_DEBUG("succ to mul double", K(left_d), K(left_d));
  }
  return ret;
}

int ObExprMul::mul_number(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_finish = false;
  if (OB_FAIL(get_arith_operand(expr, ctx, left, right, datum, is_finish))) {
    LOG_WARN("get_arith_operand failed", K(ret));
  } else if (is_finish) {
    // do nothing
  } else {
    ObNumber lnum(left->get_number());
    ObNumber rnum(right->get_number());
    char local_buff[ObNumber::MAX_BYTE_LEN];
    ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);
    ObNumber result;
    if (OB_FAIL(lnum.mul_v3(rnum, result, local_alloc))) {
      LOG_WARN("add number failed", K(ret));
    } else {
      datum.set_number(result);
    }
  }
  return ret;
}

int ObExprMul::mul_intervalym_number_common(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& datum, const bool number_left)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_finish = false;
  if (OB_FAIL(get_arith_operand(expr, ctx, left, right, datum, is_finish))) {
    LOG_WARN("get_arith_operand failed", K(ret));
  } else if (is_finish) {
    // do nothing
  } else {
    ObIntervalYMValue value_ym;
    ObNumber value_number;
    if (number_left) {
      value_number.assign(left->get_number().desc_.desc_, const_cast<uint32_t*>(&(left->get_number().digits_[0])));
      value_ym = ObIntervalYMValue(right->get_interval_nmonth());
    } else {
      value_ym = ObIntervalYMValue(left->get_interval_nmonth());
      value_number.assign(right->get_number().desc_.desc_, const_cast<uint32_t*>(&(right->get_number().digits_[0])));
    }

    char local_buff[ObNumber::MAX_BYTE_LEN * 2];
    ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN * 2);
    ObNumber result;
    ObNumber ym_number;
    int64_t result_nmonth = 0;
    if (OB_FAIL(ym_number.from(value_ym.get_nmonth(), local_alloc))) {
      LOG_WARN("failed to convert to number", K(ret));
    } else if (OB_FAIL(ym_number.mul_v3(value_number, result, local_alloc))) {
      LOG_WARN("failed to do mul", K(ret));
    } else if (OB_FAIL(result.trunc(0))) {
      LOG_WARN("failed to do trunc", K(ret));
    } else if (OB_UNLIKELY(!result.is_valid_int64(result_nmonth))) {
      ret = OB_INVALID_NUMERIC;
      LOG_WARN("failed to get int64_t from number", K(ret));
    } else {
      ObIntervalYMValue value = ObIntervalYMValue(result_nmonth);
      if (OB_FAIL(value.validate())) {
        LOG_WARN("invalid interval ym result", K(ret), K(value));
      } else {
        datum.set_interval_nmonth(result_nmonth);
      }
    }
  }
  return ret;
}

int ObExprMul::mul_intervalds_number_common(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& datum, const bool number_left)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_finish = false;
  if (OB_FAIL(get_arith_operand(expr, ctx, left, right, datum, is_finish))) {
    LOG_WARN("get_arith_operand failed", K(ret));
  } else if (is_finish) {
    // do nothing
  } else {
    ObIntervalDSValue value_ds;
    ObNumber value_number;
    if (number_left) {
      value_number.assign(left->get_number().desc_.desc_, const_cast<uint32_t*>(&(left->get_number().digits_[0])));
      value_ds = right->get_interval_ds();
    } else {
      value_ds = left->get_interval_ds();
      value_number.assign(right->get_number().desc_.desc_, const_cast<uint32_t*>(&(right->get_number().digits_[0])));
    }

    char local_buff[ObNumber::MAX_BYTE_LEN * 6];
    ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN * 6);
    int64_t result_nsecond = 0;
    int64_t result_fs = 0;
    number::ObNumber nvalue;
    number::ObNumber fsecond;
    number::ObNumber power10;
    number::ObNumber fvalue;
    number::ObNumber left_number;
    number::ObNumber res_number;

    if (OB_FAIL(nvalue.from(value_ds.get_nsecond(), local_alloc))) {
      LOG_WARN("failed to convert interval to number", K(ret));
    } else if (OB_FAIL(fsecond.from(static_cast<int64_t>(value_ds.get_fs()), local_alloc))) {
      LOG_WARN("failed to convert interval to number", K(ret));
    } else if (OB_FAIL(power10.from(static_cast<int64_t>(ObIntervalDSValue::MAX_FS_VALUE), local_alloc))) {
      LOG_WARN("failed to round number", K(ret));
    } else if (OB_FAIL(fsecond.div_v3(power10, fvalue, local_alloc))) {
      LOG_WARN("fail to div fs", K(ret));
    } else if (OB_FAIL(nvalue.add_v3(fvalue, left_number, local_alloc))) {
      LOG_WARN("failed to add number", K(ret));
    } else if (OB_FAIL(left_number.mul_v3(value_number, res_number, local_alloc))) {
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
      ObIntervalDSValue value = ObIntervalDSValue(result_nsecond, static_cast<int32_t>(result_fs));
      if (OB_FAIL(value.validate())) {
        LOG_WARN("invalid interval result", K(ret), K(nvalue), K(fvalue), K(left_number), K(value));
      } else {
        datum.set_interval_ds(value);
      }
    }
  }
  return ret;
}

int ObExprMul::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  OB_ASSERT(2 == rt_expr.arg_cnt_);
  OB_ASSERT(NULL != rt_expr.args_);
  OB_ASSERT(NULL != rt_expr.args_[0]);
  OB_ASSERT(NULL != rt_expr.args_[1]);
  const common::ObObjType left = rt_expr.args_[0]->datum_meta_.type_;
  const common::ObObjType right = rt_expr.args_[1]->datum_meta_.type_;
  const ObObjTypeClass left_tc = ob_obj_type_class(left);
  const ObObjTypeClass right_tc = ob_obj_type_class(right);
  OB_ASSERT(left == input_types_[0].get_calc_type());
  OB_ASSERT(right == input_types_[1].get_calc_type());

  rt_expr.inner_functions_ = NULL;
  LOG_DEBUG("arrive here cg_expr", K(ret), K(rt_expr));
  switch (rt_expr.datum_meta_.type_) {
    case ObIntType: {
      rt_expr.eval_func_ = ObExprMul::mul_int_int;
      break;
    }
    case ObUInt64Type: {
      if (ObIntTC == left_tc) {
        if (ObUIntTC == right_tc) {
          rt_expr.eval_func_ = ObExprMul::mul_int_uint;
        }
      } else if (ObUIntTC == left_tc) {
        if (ObIntTC == right_tc) {
          rt_expr.eval_func_ = ObExprMul::mul_uint_int;
        } else if (ObUIntTC == right_tc) {
          rt_expr.eval_func_ = ObExprMul::mul_uint_uint;
        }
      }
      break;
    }
    case ObFloatType: {
      rt_expr.eval_func_ = ObExprMul::mul_float;
      break;
    }
    case ObDoubleType: {
      rt_expr.eval_func_ = ObExprMul::mul_double;
      break;
    }
    case ObUNumberType:
    case ObNumberType: {
      rt_expr.eval_func_ = ObExprMul::mul_number;
      break;
    }
    case ObIntervalYMType: {
      if (ObIntervalYMType == left) {
        rt_expr.eval_func_ = ObExprMul::mul_intervalym_number;
      } else {
        rt_expr.eval_func_ = ObExprMul::mul_number_intervalym;
      }
      break;
    }
    case ObIntervalDSType: {
      if (ObIntervalDSType == left) {
        rt_expr.eval_func_ = ObExprMul::mul_intervalds_number;
      } else {
        rt_expr.eval_func_ = ObExprMul::mul_number_intervalds;
      }
      break;
    }
    default: {
      break;
    }
  }

  if (OB_ISNULL(rt_expr.eval_func_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected result type", K(ret), K(rt_expr.datum_meta_.type_), K(left), K(right));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
