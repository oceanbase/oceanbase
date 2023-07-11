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
#include "sql/engine/expr/ob_batch_eval_util.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace common::number;

ObExprMul::ObExprMul(ObIAllocator &alloc, ObExprOperatorType type)
  : ObArithExprOperator(alloc,
                        type,
                        N_MUL,
                        2,
                        NOT_ROW_DIMENSION,
                        ObExprResultTypeUtil::get_mul_result_type,
                        ObExprResultTypeUtil::get_mul_calc_type,
                        mul_funcs_)
{
  param_lazy_eval_ = lib::is_oracle_mode();
}

int ObExprMul::calc_result_type2(ObExprResType &type,
                                 ObExprResType &type1,
                                 ObExprResType &type2,
                                 ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObArithExprOperator::calc_result_type2(type, type1, type2, type_ctx))) {
  } else if (lib::is_oracle_mode()){
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
      if (lib::is_mysql_mode() && type.is_double()) {
        type.set_scale(MAX(scale1, scale2));
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
      if (lib::is_mysql_mode() && type.is_double()) {
        type.set_precision(ObMySQLUtil::float_length(type.get_scale()));
      } else {
        type.set_precision(static_cast<ObPrecision>((precision1 - scale1)
            +  (precision2 - scale2) + type.get_scale()));
      }
    }
  }
  return ret;
}

int ObExprMul::calc(ObObj &res,
                    const ObObj &left,
                    const ObObj &right,
                    ObIAllocator *allocator,
                    ObScale scale)
{
  ObCalcTypeFunc calc_type_func = ObExprResultTypeUtil::get_arith_result_type;
  return ObArithExprOperator::calc(res, left, right, allocator, scale, calc_type_func, mul_funcs_);
}

int ObExprMul::calc(ObObj &res,
                    const ObObj &left,
                    const ObObj &right,
                    ObExprCtx &expr_ctx,
                    ObScale scale)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass tc1 = left.get_type_class();
  const ObObjTypeClass tc2 = right.get_type_class();
  if (lib::is_oracle_mode()
      && ObIntervalTC == tc1
      && ObNumberTC == tc2) {
    ret = mul_interval(res, left, right, expr_ctx.calc_buf_, scale);
  } else if (lib::is_oracle_mode()
             && ObNumberTC == tc1
             && ObIntervalTC == tc2) {
    ret = mul_interval(res, right, left, expr_ctx.calc_buf_, scale);
  } else {
    ret = ObArithExprOperator::calc(res, left, right,
                                    expr_ctx, scale,
                                    ObExprResultTypeUtil::get_mul_result_type,
                                    mul_funcs_);
  }
  return ret;
}

ObArithFunc ObExprMul::mul_funcs_[ObMaxTC] =
{
  NULL,
  ObExprMul::mul_int,
  ObExprMul::mul_uint,
  ObExprMul::mul_float,
  ObExprMul::mul_double,
  ObExprMul::mul_number,
  NULL,//datetime
  NULL,//date
  NULL,//time
  NULL,//year
  NULL,//string
  NULL,//extend
  NULL,//unknown
  NULL,//text
  NULL,//bit
  NULL,//enumset
  NULL,//enumsetInner
  NULL,//otimestamp
  NULL,//raw
  NULL,//interval
};

ObArithFunc ObExprMul::agg_mul_funcs_[ObMaxTC] =
{
  NULL,
  ObExprMul::mul_int,
  ObExprMul::mul_uint,
  ObExprMul::mul_float,
  ObExprMul::mul_double_no_overflow,
  ObExprMul::mul_number,
  NULL,//datetime
  NULL,//date
  NULL,//time
  NULL,//year
  NULL,//string
  NULL,//extend
  NULL,//unknown
  NULL,//text
  NULL,//bit
  NULL,//enumset
  NULL,//enumsetInner
  NULL,//otimestamp
  NULL,//raw
  NULL,//interval
};

int ObExprMul::mul_int(ObObj &res,
                       const ObObj &left,
                       const ObObj &right,
                       ObIAllocator *allocator,
                       ObScale scale)
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
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%ld * %ld)'",
                      left_i,
                      right_i);
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
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%ld * %lu)'",
                      left_i,
                      right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
    res.set_uint64(left_i * right_i);
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMul::mul_uint(ObObj &res,
                        const ObObj &left,
                        const ObObj &right,
                        ObIAllocator *allocator,
                        ObScale scale)
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
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%lu * %lu)'",
                      left_i,
                      right_i);
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
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%lu * %ld)'",
                      left_i,
                      right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
    res.set_uint64(left_i * right_i);
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMul::mul_float(ObObj &res,
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
    float left_f = left.get_float();
    float right_f = right.get_float();
    res.set_float(left_f * right_f);
    if (OB_UNLIKELY(is_float_out_of_range(res.get_float()))
        && !lib::is_oracle_mode()) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%e * %e)'",
                      left_f,
                      right_f);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BINARY_FLOAT", expr_str);
      LOG_WARN("float out of range", K(ret), K(left), K(right), K(res));
    }
    LOG_DEBUG("succ to mul float", K(left), K(right));
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMul::mul_double(ObObj &res,
                          const ObObj &left,
                          const ObObj &right,
                          ObIAllocator *allocator,
                          ObScale scale)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left.get_type_class() != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    double left_d = left.get_double();
    double right_d = right.get_double();
    res.set_double(left_d * right_d);
    if (OB_UNLIKELY(is_double_out_of_range(res.get_double()))
        && !lib::is_oracle_mode()) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%e * %e)'",
                      left_d,
                      right_d);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, lib::is_oracle_mode()
                                          ? "BINARY_DOUBLE" : "DOUBLE", expr_str);
      LOG_WARN("double out of range", K(ret), K(left), K(right), K(res));
      res.set_null();
    }
    LOG_DEBUG("succ to mul double", K(left), K(right));
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMul::mul_double_no_overflow(ObObj &res,
                                      const ObObj &left,
                                      const ObObj &right,
                                      ObIAllocator *,
                                      ObScale)
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

int ObExprMul::mul_number(ObObj &res,
                          const ObObj &left,
                          const ObObj &right,
                          ObIAllocator *allocator,
                          ObScale scale)
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

int ObExprMul::mul_interval(ObObj &res,
                            const ObObj &left,
                            const ObObj &right,
                            ObIAllocator *allocator,
                            ObScale scale)
{
  int ret = OB_SUCCESS;
  number::ObNumber res_number;
  number::ObNumber left_number;

  if (OB_UNLIKELY(left.get_type_class() != ObIntervalTC)
      || OB_UNLIKELY(right.get_type_class() != ObNumberTC)) {
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

struct ObIntIntBatchMulRaw : public ObArithOpRawType<int64_t, int64_t, int64_t>
{
  static void raw_op(int64_t &res, const int64_t l, const int64_t r)
  {
    res = l * r;
  }

  static int raw_check(const int64_t, const int64_t l, const int64_t r)
  {
    int ret = OB_SUCCESS;
    long long res;
    if (OB_UNLIKELY(ObExprMul::is_mul_out_of_range(l, r, res))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%ld * %ld)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT", expr_str);
    }
    return ret;
  }
};

int ObExprMul::mul_int_int(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObArithOpWrap<ObIntIntBatchMulRaw>>(EVAL_FUNC_ARG_LIST);
}

int ObExprMul::mul_int_int_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op<ObArithOpWrap<ObIntIntBatchMulRaw>>(BATCH_EVAL_FUNC_ARG_LIST);
}

struct ObIntUIntBatchMulRaw : public ObArithOpRawType<uint64_t, int64_t, uint64_t>
{
  static void raw_op(uint64_t &res, const int64_t l, const uint64_t r)
  {
    res = l * r;
  }

  static int raw_check(const uint64_t, const int64_t l, const uint64_t r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObExprMul::is_int_uint_mul_out_of_range(l, r))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%ld * %lu)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
    return ret;
  }
};

int ObExprMul::mul_int_uint(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObArithOpWrap<ObIntUIntBatchMulRaw>>(EVAL_FUNC_ARG_LIST);
}

int ObExprMul::mul_int_uint_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op<ObArithOpWrap<ObIntUIntBatchMulRaw>>(BATCH_EVAL_FUNC_ARG_LIST);
}

struct ObUIntIntBatchMulRaw : public ObArithOpRawType<uint64_t, uint64_t, int64_t>
{
  static void raw_op(uint64_t &res, const uint64_t l, const int64_t r)
  {
    res = l * r;
  }

  static int raw_check(const uint64_t, const uint64_t l, const int64_t r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObExprMul::is_int_uint_mul_out_of_range(r, l))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%lu * %ld)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
    return ret;
  }
};

int ObExprMul::mul_uint_int(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObArithOpWrap<ObUIntIntBatchMulRaw>>(EVAL_FUNC_ARG_LIST);
}

int ObExprMul::mul_uint_int_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op<ObArithOpWrap<ObUIntIntBatchMulRaw>>(BATCH_EVAL_FUNC_ARG_LIST);
}

struct ObUIntUIntBatchMulRaw : public ObArithOpRawType<uint64_t, uint64_t, uint64_t>
{
  static void raw_op(uint64_t &res, const uint64_t l, const uint64_t r)
  {
    res = l * r;
  }

  static int raw_check(const uint64_t, const uint64_t l, const uint64_t r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObExprMul::is_uint_uint_mul_out_of_range(l, r))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%lu * %lu)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
    return ret;
  }
};

int ObExprMul::mul_uint_uint(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObArithOpWrap<ObUIntUIntBatchMulRaw>>(EVAL_FUNC_ARG_LIST);
}

int ObExprMul::mul_uint_uint_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op<ObArithOpWrap<ObUIntUIntBatchMulRaw>>(BATCH_EVAL_FUNC_ARG_LIST);
}

struct ObFloatBatchMulRawNoCheck : public ObArithOpRawType<float, float, float>
{
  static void raw_op(float &res, const float l, const float r)
  {
    res = l * r;
  }

  static int raw_check(const float, const float, const float)
  {
    return OB_SUCCESS;
  }
};

struct ObFloatBatchMulRawWithCheck: public ObFloatBatchMulRawNoCheck
{
  static int raw_check(const float res, const float l, const float r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObExprMul::is_float_out_of_range(res))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%e * %e)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "FLOAT", expr_str);
      LOG_WARN("float out of range", K(l), K(r), K(res));
    }
    return ret;
  }
};

int ObExprMul::mul_float(EVAL_FUNC_ARG_DECL)
{
  return lib::is_oracle_mode()
      ? def_arith_eval_func<ObArithOpWrap<ObFloatBatchMulRawNoCheck>>(EVAL_FUNC_ARG_LIST)
      : def_arith_eval_func<ObArithOpWrap<ObFloatBatchMulRawWithCheck>>(EVAL_FUNC_ARG_LIST);
}

int ObExprMul::mul_float_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return lib::is_oracle_mode()
      ? def_batch_arith_op<ObArithOpWrap<ObFloatBatchMulRawNoCheck>>(BATCH_EVAL_FUNC_ARG_LIST)
      : def_batch_arith_op<ObArithOpWrap<ObFloatBatchMulRawWithCheck>>(BATCH_EVAL_FUNC_ARG_LIST);
}

struct ObDoubleBatchMulRawNoCheck : public ObArithOpRawType<double, double, double>
{
  static void raw_op(double &res, const double l, const double r)
  {
    res = l * r;
  }

  static int raw_check(const double , const double , const double)
  {
    return OB_SUCCESS;
  }
};

struct ObDoubleBatchMulRawWithCheck: public ObDoubleBatchMulRawNoCheck
{
  static int raw_check(const double res, const double l, const double r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObExprMul::is_double_out_of_range(res))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%e * %e)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DOUBLE", expr_str);
      LOG_WARN("double out of range", K(l), K(r), K(res));
    }
    return ret;
  }
};

int ObExprMul::mul_double(EVAL_FUNC_ARG_DECL)
{
  return lib::is_oracle_mode() || T_OP_AGG_MUL == expr.type_
      ? def_arith_eval_func<ObArithOpWrap<ObDoubleBatchMulRawNoCheck>>(EVAL_FUNC_ARG_LIST)
      : def_arith_eval_func<ObArithOpWrap<ObDoubleBatchMulRawWithCheck>>(EVAL_FUNC_ARG_LIST);
}

int ObExprMul::mul_double_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return lib::is_oracle_mode() || T_OP_AGG_MUL == expr.type_
      ? def_batch_arith_op<ObArithOpWrap<ObDoubleBatchMulRawNoCheck>>(BATCH_EVAL_FUNC_ARG_LIST)
      : def_batch_arith_op<ObArithOpWrap<ObDoubleBatchMulRawWithCheck>>(BATCH_EVAL_FUNC_ARG_LIST);
}

struct ObNumberMulFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    char local_buff[ObNumber::MAX_BYTE_LEN];
    ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);
    number::ObNumber l_num(l.get_number());
    number::ObNumber r_num(r.get_number());
    number::ObNumber res_num;
    if (OB_FAIL(l_num.mul_v3(r_num, res_num, local_alloc))) {
      LOG_WARN("mul num failed", K(ret), K(l_num), K(r_num));
    } else {
      res.set_number(res_num);
    }
    return ret;
  }
};

int ObExprMul::mul_number(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObNumberMulFunc>(EVAL_FUNC_ARG_LIST);
}

int ObExprMul::mul_number_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  LOG_DEBUG("mul_number_batch begin");
  int ret = OB_SUCCESS;
  ObDatumVector l_datums;
  ObDatumVector r_datums;
  const ObExpr &left = *expr.args_[0];
  const ObExpr &right = *expr.args_[1];

  if (OB_FAIL(binary_operand_batch_eval(expr, ctx, skip, size,
                                        lib::is_oracle_mode()))) {
    LOG_WARN("number multiply batch evaluation failure", K(ret));
  } else {
    l_datums = left.locate_expr_datumvector(ctx);
    r_datums = right.locate_expr_datumvector(ctx);
  }

  if (OB_SUCC(ret)) {
    char local_buff[ObNumber::MAX_BYTE_LEN];
    ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);
    ObDatumVector results = expr.locate_expr_datumvector(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

    for (auto i = 0; OB_SUCC(ret) && i < size; i++) {
      if (eval_flags.at(i) || skip.at(i)) {
        continue;
      }
      if (l_datums.at(i)->is_null() || r_datums.at(i)->is_null()) {
        results.at(i)->set_null();
        eval_flags.set(i);
        continue;
      }
      ObNumber res_num;
      ObNumber l_num(l_datums.at(i)->get_number());
      ObNumber r_num(r_datums.at(i)->get_number());
      uint32_t *res_digits = const_cast<uint32_t *> (results.at(i)->get_number_digits());
      ObNumber::Desc &desc_buf = const_cast<ObNumber::Desc &> (results.at(i)->get_number_desc());
      // Notice that, space of desc_buf is allocated in frame but without memset operation, which causes random memory content.
      // And the reserved in storage layer should be 0, thus you must replacement new here to avoid checksum error, etc.
      ObNumber::Desc *res_desc = new (&desc_buf) ObNumber::Desc();
      // speedup detection
      if (ObNumber::try_fast_mul(l_num, r_num, res_digits, *res_desc)) {
        results.at(i)->set_pack(sizeof(number::ObCompactNumber) +
                                res_desc->len_ * sizeof(*res_digits));
        eval_flags.set(i);
        // LOG_DEBUG("mul speedup", K(l_num.format()),
        // K(r_num.format()), K(res_num.format()));
      } else {
        // normal path: no speedup
        if (OB_FAIL(l_num.mul_v3(r_num, res_num, local_alloc))) {
          LOG_WARN("mul num failed", K(ret), K(l_num), K(r_num));
        } else {
          results.at(i)->set_number(res_num);
          eval_flags.set(i);
        }
        local_alloc.free();
      }
    }
  }
  LOG_DEBUG("mul_number_batch done");
  return ret;

}

struct ObIntervalYMNumberMulFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, const bool &swap_l_r) const
  {
    int ret = OB_SUCCESS;
    const ObDatum *interval_ym_datum = !swap_l_r ? &l : &r;
    const ObDatum *num_datum = !swap_l_r ? &r : &l;
    ObIntervalYMValue value_ym = ObIntervalYMValue(interval_ym_datum->get_interval_nmonth());
    ObNumber value_number = num_datum->get_number();

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
        res.set_interval_nmonth(result_nmonth);
      }
    }
    return ret;
  }
};

int ObExprMul::mul_intervalym_number_common(EVAL_FUNC_ARG_DECL,const bool number_left)
{
  return def_arith_eval_func<ObIntervalYMNumberMulFunc>(EVAL_FUNC_ARG_LIST, number_left);
}

int ObExprMul::mul_intervalym_number_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  const bool swap_l_r = false;
  return def_batch_arith_op_by_datum_func<ObIntervalYMNumberMulFunc>(
      BATCH_EVAL_FUNC_ARG_LIST, swap_l_r);
}

int ObExprMul::mul_number_intervalym_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  const bool swap_l_r = true;
  return def_batch_arith_op_by_datum_func<ObIntervalYMNumberMulFunc>(
      BATCH_EVAL_FUNC_ARG_LIST, swap_l_r);
}

struct ObIntervalDSNumberMulFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, const bool &swap_l_r) const
  {
    int ret = OB_SUCCESS;
    const ObDatum *interval_ds_datum = !swap_l_r ? &l : &r;
    const ObDatum *num_datum = !swap_l_r ? &r : &l;
    ObIntervalDSValue value_ds = interval_ds_datum->get_interval_ds();
    ObNumber value_number = num_datum->get_number();

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
    } else if (OB_FAIL(power10.from(static_cast<int64_t>(ObIntervalDSValue::MAX_FS_VALUE),
                                    local_alloc))) {
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
        res.set_interval_ds(value);
      }
    }
    return ret;
  }
};

int ObExprMul::mul_intervalds_number_common(EVAL_FUNC_ARG_DECL, const bool number_left)
{
  return def_arith_eval_func<ObIntervalDSNumberMulFunc>(EVAL_FUNC_ARG_LIST, number_left);
}

int ObExprMul::mul_intervalds_number_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  const bool swap_l_r = false;
  return def_batch_arith_op_by_datum_func<ObIntervalDSNumberMulFunc>(
      BATCH_EVAL_FUNC_ARG_LIST, swap_l_r);
}

int ObExprMul::mul_number_intervalds_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  const bool swap_l_r = true;
  return def_batch_arith_op_by_datum_func<ObIntervalDSNumberMulFunc>(
      BATCH_EVAL_FUNC_ARG_LIST, swap_l_r);
}

int ObExprMul::cg_expr(ObExprCGCtx &op_cg_ctx,
                       const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
#define SET_MUL_FUNC_PTR(v) \
  rt_expr.eval_func_ = ObExprMul::v; \
  rt_expr.eval_batch_func_ = ObExprMul::v##_batch;
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
      SET_MUL_FUNC_PTR(mul_int_int);
      break;
    }
    case ObUInt64Type: {
      if (ObIntTC == left_tc) {
        if (ObUIntTC == right_tc) {
          SET_MUL_FUNC_PTR(mul_int_uint);
        }
      } else if (ObUIntTC == left_tc) {
        if (ObIntTC == right_tc) {
          SET_MUL_FUNC_PTR(mul_uint_int);
        } else if (ObUIntTC == right_tc) {
          SET_MUL_FUNC_PTR(mul_uint_uint);
        }
      }
      break;
    }
    case ObFloatType: {
      SET_MUL_FUNC_PTR(mul_float);
      break;
    }
    case ObDoubleType: {
      SET_MUL_FUNC_PTR(mul_double);
      break;
    }
    case ObUNumberType:
    case ObNumberType: {
      SET_MUL_FUNC_PTR(mul_number);
      break;
    }
    case ObIntervalYMType: {
      if (ObIntervalYMType == left) {
        SET_MUL_FUNC_PTR(mul_intervalym_number);
      } else {
        SET_MUL_FUNC_PTR(mul_number_intervalym);
      }
      break;
    }
    case ObIntervalDSType: {
      if (ObIntervalDSType == left) {
        SET_MUL_FUNC_PTR(mul_intervalds_number);
      } else {
        SET_MUL_FUNC_PTR(mul_number_intervalds);
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

#undef SET_MUL_FUNC_PTR
}


}
}
