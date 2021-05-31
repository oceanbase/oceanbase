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
#include "sql/engine/expr/ob_expr_add.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
namespace oceanbase {
using namespace common;
using namespace common::number;
namespace sql {

ObExprAdd::ObExprAdd(ObIAllocator& alloc, ObExprOperatorType type)
    : ObArithExprOperator(alloc, type, N_ADD, 2, NOT_ROW_DIMENSION, ObExprResultTypeUtil::get_add_result_type,
          ObExprResultTypeUtil::get_add_calc_type, add_funcs_)
{
  param_lazy_eval_ = lib::is_oracle_mode();
}

int ObExprAdd::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  static const int64_t CARRY_OFFSET = 1;
  ObScale scale = SCALE_UNKNOWN_YET;
  ObPrecision precision = PRECISION_UNKNOWN_YET;
  bool is_oracle = share::is_oracle_mode();
  if (OB_FAIL(ObArithExprOperator::calc_result_type2(type, type1, type2, type_ctx))) {
    LOG_WARN("fail to calc result type", K(ret), K(type), K(type1), K(type2));
  } else if (is_oracle && type.is_oracle_decimal()) {
    type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
    type.set_precision(PRECISION_UNKNOWN_YET);
  } else if (OB_UNLIKELY(SCALE_UNKNOWN_YET == type1.get_scale()) ||
             OB_UNLIKELY(SCALE_UNKNOWN_YET == type2.get_scale())) {
    type.set_scale(SCALE_UNKNOWN_YET);
    type.set_precision(PRECISION_UNKNOWN_YET);
  } else if (type1.get_type_class() == ObIntervalTC || type2.get_type_class() == ObIntervalTC) {
    type.set_scale(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][type.get_type()].get_scale());
    type.set_precision(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][type.get_type()].get_precision());
  } else {
    if (OB_UNLIKELY(is_oracle && type.is_datetime())) {
      scale = OB_MAX_DATE_PRECISION;
    } else {
      ObScale scale1 = static_cast<ObScale>(MAX(type1.get_scale(), 0));
      ObScale scale2 = static_cast<ObScale>(MAX(type2.get_scale(), 0));
      int64_t inter_part_length1 = type1.get_precision() - type1.get_scale();
      int64_t inter_part_length2 = type2.get_precision() - type2.get_scale();
      scale = MAX(scale1, scale2);
      precision = static_cast<ObPrecision>(MAX(inter_part_length1, inter_part_length2) + CARRY_OFFSET + scale);
    }

    type.set_scale(scale);

    if (OB_UNLIKELY(PRECISION_UNKNOWN_YET == type1.get_precision()) ||
        OB_UNLIKELY(PRECISION_UNKNOWN_YET == type2.get_precision())) {
      type.set_precision(PRECISION_UNKNOWN_YET);
    } else {
      type.set_precision(precision);
    }
  }
  LOG_DEBUG("calc_result_type2", K(scale), K(type1), K(type2), K(type), K(precision));
  return ret;
}

int ObExprAdd::calc_result2(ObObj& result, const ObObj& left, const ObObj& right, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObScale calc_scale = result_type_.get_calc_scale();
  ObObjType calc_type = result_type_.get_calc_type();
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
      if (ObIntervalTC == tc1 || ObIntervalTC == tc2) {
        // interval can calc with different types such as date, timestamp, interval. the params do not need to do cast
        ret = (ObIntervalTC == tc2) ? interval_add_minus(result, left, right, expr_ctx, calc_scale)
                                    : interval_add_minus(result, right, left, expr_ctx, calc_scale);
      } else {
        // When execute add_xxx later, can set result type with result.get_type().
        result.set_type(get_result_type().get_type());
        ret = ObArithExprOperator::calc_(result, left, right, expr_ctx, calc_scale, calc_type, add_funcs_);
      }
    }
  } else if (OB_UNLIKELY(ObNullType == left.get_type() || ObNullType == right.get_type())) {
    result.set_null();
  } else {
    ObObjTypeClass tc1 = left.get_type_class();
    ObObjTypeClass tc2 = right.get_type_class();
    if (ObIntTC == tc1) {
      if (ObIntTC == tc2 || ObUIntTC == tc2) {
        ret = add_int(result, left, right, expr_ctx.calc_buf_, calc_scale);
      } else {
        ret = ObArithExprOperator::calc_(result, left, right, expr_ctx, calc_scale, calc_type, add_funcs_);
      }
    } else if (ObUIntTC == tc1) {
      if (ObIntTC == tc2 || ObUIntTC == tc2) {
        ret = add_uint(result, left, right, expr_ctx.calc_buf_, calc_scale);
      } else {
        ret = ObArithExprOperator::calc_(result, left, right, expr_ctx, calc_scale, calc_type, add_funcs_);
      }
    } else {
      result.set_type(get_result_type().get_type());
      ret = ObArithExprOperator::calc_(
          result, left, right, expr_ctx, calc_scale, calc_type, type_ == T_OP_AGG_ADD ? agg_add_funcs_ : add_funcs_);
    }
  }
  return ret;
}

int ObExprAdd::calc(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  return ObArithExprOperator::calc(
      res, left, right, allocator, scale, ObExprResultTypeUtil::get_add_result_type, add_funcs_);
}

int ObExprAdd::calc(ObObj& res, const ObObj& left, const ObObj& right, ObExprCtx& expr_ctx, ObScale scale)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass tc1 = left.get_type_class();
  const ObObjTypeClass tc2 = right.get_type_class();
  if (lib::is_oracle_mode() && (ObIntervalTC == tc1 || ObIntervalTC == tc2)) {
    // interval can calc with different types such as date, timestamp, interval.
    // the params do not need to do cast
    ret = (ObIntervalTC == tc2) ? interval_add_minus(res, left, right, expr_ctx, scale)
                                : interval_add_minus(res, right, left, expr_ctx, scale);
  } else {
    ret = ObArithExprOperator::calc(
        res, left, right, expr_ctx, scale, ObExprResultTypeUtil::get_add_result_type, add_funcs_);
  }
  return ret;
}

int ObExprAdd::calc_for_agg(ObObj& res, const ObObj& left, const ObObj& right, ObExprCtx& expr_ctx, ObScale scale)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass tc1 = left.get_type_class();
  const ObObjTypeClass tc2 = right.get_type_class();
  if (lib::is_oracle_mode() && (ObIntervalTC == tc1 || ObIntervalTC == tc2)) {
    // interval can calc with different types such as date, timestamp, interval.
    // the params do not need to do cast
    ret = (ObIntervalTC == tc2) ? interval_add_minus(res, left, right, expr_ctx, scale)
                                : interval_add_minus(res, right, left, expr_ctx, scale);
  } else {
    ret = ObArithExprOperator::calc(
        res, left, right, expr_ctx, scale, ObExprResultTypeUtil::get_add_result_type, agg_add_funcs_);
  }
  return ret;
}

ObArithFunc ObExprAdd::add_funcs_[ObMaxTC] = {
    NULL,
    ObExprAdd::add_int,
    ObExprAdd::add_uint,
    ObExprAdd::add_float,
    ObExprAdd::add_double,
    ObExprAdd::add_number,
    ObExprAdd::add_datetime,  // datetime
    NULL,                     // date
    NULL,                     // time
    NULL,                     // year
    NULL,                     // varchar
    NULL,                     // extend
    NULL,                     // unknown
    NULL,                     // text
    NULL,                     // bit
    NULL,                     // enumset
    NULL,                     // enumsetInner
    NULL,                     // otimestamp
    NULL,                     // raw
    NULL,                     // interval, hard code using interval_add_minus
};

ObArithFunc ObExprAdd::agg_add_funcs_[ObMaxTC] = {
    NULL,
    ObExprAdd::add_int,
    ObExprAdd::add_uint,
    ObExprAdd::add_float,
    ObExprAdd::add_double_no_overflow,
    ObExprAdd::add_number,
    ObExprAdd::add_datetime,  // datetime
    NULL,                     // date
    NULL,                     // time
    NULL,                     // year
    NULL,                     // varchar
    NULL,                     // extend
    NULL,                     // unknown
    NULL,                     // text
    NULL,                     // bit
    NULL,                     // enumset
    NULL,                     // enumsetInner
    NULL,                     // otimestamp
    NULL,                     // raw
    NULL,                     // interval, hard code using interval_add_minus
};

int ObExprAdd::add_int(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  int64_t left_i = left.get_int();
  int64_t right_i = right.get_int();
  char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
  int64_t pos = 0;
  if (OB_LIKELY(left.get_type_class() == right.get_type_class())) {
    res.set_int(left_i + right_i);
    if (OB_UNLIKELY(is_int_int_out_of_range(left_i, right_i, res.get_int()))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%ld + %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT", expr_str);
    }
  } else if (ObUIntTC != right.get_type_class()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    res.set_uint64(left_i + right_i);
    if (OB_UNLIKELY(is_int_uint_out_of_range(left_i, right_i, res.get_uint64()))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%ld + %lu)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprAdd::add_uint(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  uint64_t left_i = left.get_uint64();
  uint64_t right_i = right.get_uint64();
  res.set_uint64(left_i + right_i);
  char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
  int64_t pos = 0;
  if (OB_LIKELY(left.get_type_class() == right.get_type_class())) {
    if (OB_UNLIKELY(is_uint_uint_out_of_range(left_i, right_i, res.get_uint64()))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%lu + %lu)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  } else if (ObIntTC != right.get_type_class()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    if (OB_UNLIKELY(is_int_uint_out_of_range(right_i, left_i, res.get_uint64()))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%lu + %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprAdd::add_float(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lib::is_oracle_mode())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only oracle mode arrive here", K(ret), K(left), K(right));
  } else if (OB_UNLIKELY(left.get_type() != right.get_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    float left_f = left.get_float();
    float right_f = right.get_float();
    res.set_float(left_f + right_f);
    if (OB_UNLIKELY(is_float_out_of_range(res.get_float()))) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%e + %e)'", left_f, right_f);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BINARY_FLOAT", expr_str);
      LOG_WARN("float out of range", K(res), K(left), K(right), K(res));
    }
    LOG_DEBUG("succ to add float", K(res), K(left), K(right));
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprAdd::add_double(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left.get_type_class() != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    double left_d = left.get_double();
    double right_d = right.get_double();
    res.set_double(left_d + right_d);
    if (OB_UNLIKELY(is_double_out_of_range(res.get_double()))) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%e + %e)'", left_d, right_d);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DOUBLE", expr_str);
      LOG_WARN("double out of range", K(res), K(left), K(right), K(res));
      res.set_null();
    }
    LOG_DEBUG("succ to add double", K(res), K(left), K(right));
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprAdd::add_double_no_overflow(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator*, ObScale)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left.get_type_class() != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    double left_d = left.get_double();
    double right_d = right.get_double();
    res.set_double(left_d + right_d);
    LOG_DEBUG("succ to add double", K(res), K(left), K(right));
  }
  return ret;
}

int ObExprAdd::add_number(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  number::ObNumber res_nmb;
  if (OB_UNLIKELY(NULL == allocator)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocator is null", K(ret));
  } else if (OB_FAIL(left.get_number().add_v3(right.get_number(), res_nmb, *allocator))) {
    LOG_WARN("failed to add numbers", K(ret), K(left), K(right));
  } else {
    if (ObUNumberType == res.get_type()) {
      res.set_unumber(res_nmb);
    } else {
      res.set_number(res_nmb);
    }
  }
  UNUSED(scale);
  return ret;
}

int ObExprAdd::add_datetime(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  const int64_t left_i = left.get_datetime();
  const int64_t right_i = right.get_datetime();
  if (OB_LIKELY(left.get_type_class() == right.get_type_class())) {
    int64_t round_value = left_i + right_i;
    ObTimeConverter::round_datetime(OB_MAX_DATE_PRECISION, round_value);
    res.set_datetime(round_value);
    res.set_scale(OB_MAX_DATE_PRECISION);
    ObTime ob_time;
    if (OB_UNLIKELY(res.get_datetime() > DATETIME_MAX_VAL || res.get_datetime() < DATETIME_MIN_VAL) ||
        (OB_FAIL(ObTimeConverter::datetime_to_ob_time(res.get_datetime(), NULL, ob_time))) ||
        (OB_FAIL(ObTimeConverter::validate_oracle_date(ob_time)))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%ld + %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DATE", expr_str);
    }
  }
  LOG_DEBUG("add datetime", K(left), K(right), K(scale), K(res));
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprAdd::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  if (rt_expr.arg_cnt_ != 2 || OB_ISNULL(rt_expr.args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of children is not 2 or children is null", K(ret), K(rt_expr.arg_cnt_), K(rt_expr.args_));
  } else if (OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(rt_expr.args_[0]), K(rt_expr.args_[1]));
  } else {
    rt_expr.eval_func_ = NULL;
    const ObObjType left_type = rt_expr.args_[0]->datum_meta_.type_;
    const ObObjType right_type = rt_expr.args_[1]->datum_meta_.type_;
    const ObObjType result_type = rt_expr.datum_meta_.type_;
    const ObObjTypeClass left_tc = ob_obj_type_class(left_type);
    const ObObjTypeClass right_tc = ob_obj_type_class(right_type);
    switch (result_type) {
      case ObIntType:
        rt_expr.eval_func_ = ObExprAdd::add_int_int;
        break;
      case ObUInt64Type:
        if (ObIntTC == left_tc && ObUIntTC == right_tc) {
          rt_expr.eval_func_ = ObExprAdd::add_int_uint;
        } else if (ObUIntTC == left_tc && ObIntTC == right_tc) {
          rt_expr.eval_func_ = ObExprAdd::add_uint_int;
        } else if (ObUIntTC == left_tc && ObUIntTC == right_tc) {
          rt_expr.eval_func_ = ObExprAdd::add_uint_uint;
        }
        break;
      case ObIntervalYMType:
        rt_expr.eval_func_ = ObExprAdd::add_intervalym_intervalym;
        break;
      case ObIntervalDSType:
        rt_expr.eval_func_ = ObExprAdd::add_intervalds_intervalds;
        break;
      case ObDateTimeType:
        switch (left_type) {
          case ObIntervalYMType:
            rt_expr.eval_func_ = ObExprAdd::add_intervalym_datetime;
            break;
          case ObIntervalDSType:
            rt_expr.eval_func_ = ObExprAdd::add_intervalds_datetime;
            break;
          case ObNumberType:
            rt_expr.eval_func_ = ObExprAdd::add_number_datetime;
            break;
          case ObDateTimeType:
            switch (right_type) {
              case ObIntervalYMType:
                rt_expr.eval_func_ = ObExprAdd::add_datetime_intervalym;
                break;
              case ObNumberType:
                rt_expr.eval_func_ = ObExprAdd::add_datetime_number;
                break;
              case ObIntervalDSType:
                rt_expr.eval_func_ = ObExprAdd::add_datetime_intervalds;
                break;
              default:
                rt_expr.eval_func_ = ObExprAdd::add_datetime_datetime;
                break;
            }
            break;
          default:
            rt_expr.eval_func_ = ObExprAdd::add_datetime_datetime;
            break;
        }
        break;
      case ObTimestampTZType:
        switch (left_type) {
          case ObIntervalYMType:
            rt_expr.eval_func_ = ObExprAdd::add_intervalym_timestamptz;
            break;
          case ObIntervalDSType:
            rt_expr.eval_func_ = ObExprAdd::add_intervalds_timestamptz;
            break;
          case ObTimestampTZType:
            if (ObIntervalYMType == right_type) {
              rt_expr.eval_func_ = ObExprAdd::add_timestamptz_intervalym;
            } else if (ObIntervalDSType == right_type) {
              rt_expr.eval_func_ = ObExprAdd::add_timestamptz_intervalds;
            }
            break;
          default:
            break;
        }
        break;
      case ObTimestampLTZType:
        switch (left_type) {
          case ObIntervalYMType:
            rt_expr.eval_func_ = ObExprAdd::add_intervalym_timestampltz;
            break;
          case ObIntervalDSType:
            rt_expr.eval_func_ = ObExprAdd::add_intervalds_timestamp_tiny;
            break;
          case ObTimestampLTZType:
            if (ObIntervalYMType == right_type) {
              rt_expr.eval_func_ = ObExprAdd::add_timestampltz_intervalym;
            } else if (ObIntervalDSType == right_type) {
              rt_expr.eval_func_ = ObExprAdd::add_timestamp_tiny_intervalds;
            }
            break;
          default:
            break;
        }
        break;
      case ObTimestampNanoType:
        switch (left_type) {
          case ObIntervalYMType:
            rt_expr.eval_func_ = ObExprAdd::add_intervalym_timestampnano;
            break;
          case ObIntervalDSType:
            rt_expr.eval_func_ = ObExprAdd::add_intervalds_timestamp_tiny;
            break;
          case ObTimestampNanoType:
            if (ObIntervalYMType == right_type) {
              rt_expr.eval_func_ = ObExprAdd::add_timestampnano_intervalym;
            } else if (ObIntervalDSType == right_type) {
              rt_expr.eval_func_ = ObExprAdd::add_timestamp_tiny_intervalds;
            }
            break;
          default:
            break;
        }
        break;
      case ObFloatType:
        rt_expr.eval_func_ = ObExprAdd::add_float_float;
        break;
      case ObDoubleType:
        rt_expr.eval_func_ = ObExprAdd::add_double_double;
        break;
      case ObUNumberType:
      case ObNumberType:
        rt_expr.eval_func_ = ObExprAdd::add_number_number;
        break;
      default:
        break;
    }
    if (OB_ISNULL(rt_expr.eval_func_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected params type", K(ret), K(left_type), K(right_type), K(result_type));
    }
  }
  return ret;
}

// calc_type is IntTC  left and right has same TC
int ObExprAdd::add_int_int(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    int64_t left_i = left->get_int();
    int64_t right_i = right->get_int();
    expr_datum.set_int(left_i + right_i);
    if (OB_UNLIKELY(is_int_int_out_of_range(left_i, right_i, expr_datum.get_int()))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%ld + %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT", expr_str);
    }
  }
  return ret;
}

// calc_type/left_type is IntTC, right is ObUIntTC, only mysql mode
int ObExprAdd::add_int_uint(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    int64_t left_i = left->get_int();
    uint64_t right_ui = right->get_uint();
    expr_datum.set_uint(left_i + right_ui);
    if (OB_UNLIKELY(is_int_uint_out_of_range(left_i, right_ui, expr_datum.get_uint()))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%ld + %lu)'", left_i, right_ui);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  }
  return ret;
}

// calc_type is UIntTC  left and right has same TC
int ObExprAdd::add_uint_uint(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    uint64_t left_ui = left->get_uint();
    uint64_t right_ui = right->get_uint();
    expr_datum.set_uint(left_ui + right_ui);
    if (OB_UNLIKELY(is_uint_uint_out_of_range(left_ui, right_ui, expr_datum.get_uint()))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%lu + %lu)'", left_ui, right_ui);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  }
  return ret;
}

// calc_type/left_tpee is UIntTC , right is intTC. only mysql mode
int ObExprAdd::add_uint_int(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    uint64_t left_ui = left->get_uint();
    int64_t right_i = right->get_int();
    expr_datum.set_uint(left_ui + right_i);
    if (OB_UNLIKELY(is_int_uint_out_of_range(right_i, left_ui, expr_datum.get_uint()))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%lu + %ld)'", left_ui, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  }
  return ret;
}

// calc type is floatTC, left and right has same TC
int ObExprAdd::add_float_float(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    float left_f = left->get_float();
    float right_f = right->get_float();
    expr_datum.set_float(left_f + right_f);
    if (OB_UNLIKELY(is_float_out_of_range(expr_datum.get_float()))) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%e + %e)'", left_f, right_f);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, lib::is_oracle_mode() ? "BINARY_FLOAT" : "FLOAT", expr_str);
      LOG_WARN("float out of range", K(*left), K(*right), K(expr_datum));
    }
  }
  return ret;
}

// calc type is doubleTC, left and right has same TC
int ObExprAdd::add_double_double(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    double left_d = left->get_double();
    double right_d = right->get_double();
    expr_datum.set_double(left_d + right_d);
    if (OB_UNLIKELY(is_double_out_of_range(expr_datum.get_double())) && T_OP_AGG_ADD != expr.type_) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%e + %e)'", left_d, right_d);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, lib::is_oracle_mode() ? "BINARY_DOUBLE" : "DOUBLE", expr_str);
      LOG_WARN("double out of range", K(*left), K(*right), K(expr_datum));
      expr_datum.set_null();
    } else {
      LOG_DEBUG("finish add_double_double", K(expr), K(left_d), K(right_d), "result", expr_datum.get_double());
    }
  }
  return ret;
}

// calc type TC is ObNumberTC
int ObExprAdd::add_number_number(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  number::ObNumber res_nmb;
  char local_buff[ObNumber::MAX_BYTE_LEN];
  ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    number::ObNumber left_nmb(left->get_number());
    number::ObNumber right_nmb(right->get_number());
    if (OB_FAIL(left_nmb.add_v3(right_nmb, res_nmb, local_alloc))) {
      LOG_WARN("failed to add numbers", K(ret), K(*left), K(*right));
    } else {
      expr_datum.set_number(res_nmb);
    }
  }
  return ret;
}

// 1.interval can calc with different types such as date, timestamp, interval.
// the params do not need to do cast
// 2.left and right must have the same type. both IntervalYM or both IntervalDS
int ObExprAdd::add_intervalym_intervalym(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    ObIntervalYMValue value = left->get_interval_nmonth() + right->get_interval_nmonth();
    if (OB_FAIL(value.validate())) {
      LOG_WARN("value validate failed", K(ret), K(value));
    } else {
      expr_datum.set_interval_nmonth(value.get_nmonth());
    }
  }
  return ret;
}

// left and right must have the same type. both IntervalDS
int ObExprAdd::add_intervalds_intervalds(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    ObIntervalDSValue value = left->get_interval_ds() + right->get_interval_ds();
    if (OB_FAIL(value.validate())) {
      LOG_WARN("value validate failed", K(ret), K(value));
    } else {
      expr_datum.set_interval_ds(value);
    }
  }
  return ret;
}

// Left is intervalYM type. Right is datetime TC
int ObExprAdd::add_intervalym_datetime_common(
    const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool interval_left)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (interval_left && OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (!interval_left &&
             OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, right, left, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    int64_t result_v = 0;
    if (OB_FAIL(ObTimeConverter::date_add_nmonth(right->get_datetime(), left->get_interval_nmonth(), result_v))) {
      LOG_WARN("add value failed", K(ret), K(*left), K(*right));
    } else {
      expr_datum.set_datetime(result_v);
    }
  }
  return ret;
}

// Left is intervalDS type. Right is datetime TC
int ObExprAdd::add_intervalds_datetime_common(
    const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool interval_left)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (interval_left && OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (!interval_left &&
             OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, right, left, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    int64_t result_v = 0;
    if (OB_FAIL(ObTimeConverter::date_add_nsecond(right->get_datetime(),
            left->get_interval_ds().get_nsecond(),
            left->get_interval_ds().get_fs(),
            result_v))) {
      LOG_WARN("add value failed", K(ret), K(*left), K(*right));
    } else {
      expr_datum.set_datetime(result_v);
    }
  }
  return ret;
}

// Left is intervalYM type. Right is timestampTZ type.
int ObExprAdd::add_intervalym_timestamptz_common(
    const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool interval_left)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (interval_left && OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (!interval_left &&
             OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, right, left, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    ObOTimestampData result_v;
    if (OB_FAIL(ObTimeConverter::otimestamp_add_nmonth(ObTimestampTZType,
            right->get_otimestamp_tz(),
            get_timezone_info(ctx.exec_ctx_.get_my_session()),
            left->get_interval_nmonth(),
            result_v))) {
      LOG_WARN("calc with timestamp value failed", K(ret), K(*left), K(*right));
    } else {
      expr_datum.set_otimestamp_tz(result_v);
    }
  }
  return ret;
}

// Left is intervalYM type. Right is timestampLTZ type.
int ObExprAdd::add_intervalym_timestampltz_common(
    const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool interval_left)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (interval_left && OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (!interval_left &&
             OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, right, left, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    ObOTimestampData result_v;
    if (OB_FAIL(ObTimeConverter::otimestamp_add_nmonth(ObTimestampLTZType,
            right->get_otimestamp_tiny(),
            get_timezone_info(ctx.exec_ctx_.get_my_session()),
            left->get_interval_nmonth(),
            result_v))) {
      LOG_WARN("calc with timestamp value failed", K(ret), K(*left), K(*right));
    } else {
      expr_datum.set_otimestamp_tiny(result_v);
    }
  }
  return ret;
}

// Left is intervalYM type. Right is ObTimestampNano type.
int ObExprAdd::add_intervalym_timestampnano_common(
    const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool interval_left)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (interval_left && OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (!interval_left &&
             OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, right, left, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    ObOTimestampData result_v;
    if (OB_FAIL(ObTimeConverter::otimestamp_add_nmonth(ObTimestampNanoType,
            right->get_otimestamp_tiny(),
            get_timezone_info(ctx.exec_ctx_.get_my_session()),
            left->get_interval_nmonth(),
            result_v))) {
      LOG_WARN("calc with timestamp value failed", K(ret), K(*left), K(*right));
    } else {
      expr_datum.set_otimestamp_tiny(result_v);
    }
  }
  return ret;
}

// Left is intervalDS type. Right is timestampTZ
int ObExprAdd::add_intervalds_timestamptz_common(
    const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool interval_left)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (interval_left && OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (!interval_left &&
             OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, right, left, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    ObOTimestampData result_v;
    if (OB_FAIL(ObTimeConverter::otimestamp_add_nsecond(right->get_otimestamp_tz(),
            left->get_interval_ds().get_nsecond(),
            left->get_interval_ds().get_fs(),
            result_v))) {
      LOG_WARN("calc with timestamp value failed", K(ret), K(*left), K(*right));
    } else {
      expr_datum.set_otimestamp_tz(result_v);
    }
  }
  return ret;
}

// Left is intervalDS type. Right is timestamp LTZ or Nano
int ObExprAdd::add_intervalds_timestamp_tiny_common(
    const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool interval_left)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (interval_left && OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (!interval_left &&
             OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, right, left, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    ObOTimestampData result_v;
    if (OB_FAIL(ObTimeConverter::otimestamp_add_nsecond(right->get_otimestamp_tiny(),
            left->get_interval_ds().get_nsecond(),
            left->get_interval_ds().get_fs(),
            result_v))) {
      LOG_WARN("calc with timestamp value failed", K(ret), K(*left), K(*right));
    } else {
      expr_datum.set_otimestamp_tiny(result_v);
    }
  }
  return ret;
}

// left is NumberType, right is DateTimeType.
int ObExprAdd::add_number_datetime_common(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool number_left)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (number_left && OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (!number_left &&
             OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, right, left, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    number::ObNumber left_nmb(left->get_number());
    const int64_t right_i = right->get_datetime();
    int64_t int_part = 0;
    int64_t dec_part = 0;
    if (!left_nmb.is_int_parts_valid_int64(int_part, dec_part)) {
      ret = OB_INVALID_DATE_FORMAT;
      LOG_WARN("invalid date format", K(ret), K(left_nmb));
    } else {
      const int64_t left_i =
          static_cast<int64_t>(int_part * USECS_PER_DAY) +
          (left_nmb.is_negative() ? -1 : 1) *
              static_cast<int64_t>(static_cast<double>(dec_part) / NSECS_PER_SEC * static_cast<double>(USECS_PER_DAY));
      int64_t round_value = left_i + right_i;
      ObTimeConverter::round_datetime(OB_MAX_DATE_PRECISION, round_value);
      expr_datum.set_datetime(round_value);
      ObTime ob_time;
      if (OB_UNLIKELY(expr_datum.get_datetime() > DATETIME_MAX_VAL || expr_datum.get_datetime() < DATETIME_MIN_VAL) ||
          (OB_FAIL(ObTimeConverter::datetime_to_ob_time(expr_datum.get_datetime(), NULL, ob_time))) ||
          (OB_FAIL(ObTimeConverter::validate_oracle_date(ob_time)))) {
        char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
        int64_t pos = 0;
        ret = OB_OPERATE_OVERFLOW;
        pos = 0;
        databuff_printf(expr_str,
            OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
            pos,
            "'(%ld + %ld)'",
            number_left ? left_i : right_i,
            number_left ? right_i : left_i);
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DATE", expr_str);
      }
    }
  }
  return ret;
}

// both left and right are datetimeType
int ObExprAdd::add_datetime_datetime(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    const int64_t left_i = left->get_datetime();
    const int64_t right_i = right->get_datetime();
    int64_t round_value = left_i + right_i;
    ObTimeConverter::round_datetime(OB_MAX_DATE_PRECISION, round_value);
    expr_datum.set_datetime(round_value);
    ObTime ob_time;
    if (OB_UNLIKELY(expr_datum.get_datetime() > DATETIME_MAX_VAL || expr_datum.get_datetime() < DATETIME_MIN_VAL) ||
        (OB_FAIL(ObTimeConverter::datetime_to_ob_time(expr_datum.get_datetime(), NULL, ob_time))) ||
        (OB_FAIL(ObTimeConverter::validate_oracle_date(ob_time)))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%ld + %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DATE", expr_str);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
