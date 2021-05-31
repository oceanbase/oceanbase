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

#include "sql/engine/expr/ob_expr_minus.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"

namespace oceanbase {
using namespace common;
using namespace common::number;
using namespace oceanbase::lib;

namespace sql {

ObExprMinus::ObExprMinus(ObIAllocator& alloc, ObExprOperatorType type)
    : ObArithExprOperator(alloc, type, N_MINUS, 2, NOT_ROW_DIMENSION, ObExprResultTypeUtil::get_minus_result_type,
          ObExprResultTypeUtil::get_minus_calc_type, minus_funcs_)
{
  param_lazy_eval_ = lib::is_oracle_mode();
}

int ObExprMinus::calc_result_type2(
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
  } else if (OB_UNLIKELY(SCALE_UNKNOWN_YET == type1.get_scale() || SCALE_UNKNOWN_YET == type2.get_scale())) {
    type.set_scale(SCALE_UNKNOWN_YET);
    type.set_precision(PRECISION_UNKNOWN_YET);
  } else if (type1.get_type_class() == ObIntervalTC || type2.get_type_class() == ObIntervalTC) {
    type.set_scale(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][type.get_type()].get_scale());
    type.set_precision(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][type.get_type()].get_precision());
  } else if (ob_is_oracle_datetime_tc(type1.get_type()) && ob_is_oracle_datetime_tc(type2.get_type()) &&
             type.get_type_class() == ObIntervalTC) {
    type.set_scale(ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(
        MAX_SCALE_FOR_ORACLE_TEMPORAL, std::max(type1.get_scale(), type2.get_scale())));
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
    LOG_DEBUG("calc_result_type2", K(scale), K(type1), K(type2), K(type), K(precision));
  }
  return ret;
}

// Parameters are lazy evaluated, %left and %right can not be accessed before param_eval() called.
int ObExprMinus::calc_result2(ObObj& result, const ObObj& left, const ObObj& right, ObExprCtx& expr_ctx) const
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
    } else if (ObNumberType == calc_type && ObDateTimeTC == left.get_type_class() &&
               ObDateTimeTC == right.get_type_class()) {
      // datetime - datetime --> number
      ret = calc_datetime_minus(result, left, right, expr_ctx, calc_scale);
    } else if (ObIntervalTC == right.get_type_class()) {
      // interval can calc with different types such as date, timestamp, interval. the params do not need to do cast
      ret = interval_add_minus(result, left, right, expr_ctx, calc_scale, true);
    } else if (ob_is_oracle_datetime_tc(left.get_type()) && ob_is_oracle_datetime_tc(right.get_type())) {
      ret = calc_timestamp_minus(result, left, right, TZ_INFO(expr_ctx.my_session_));
    } else {
      result.set_type(get_result_type().get_type());
      ret = ObArithExprOperator::calc_(result, left, right, expr_ctx, calc_scale, calc_type, minus_funcs_);
    }
  } else if (OB_UNLIKELY(ObNullType == left.get_type() || ObNullType == right.get_type())) {
    result.set_null();
  } else {
    ObObjTypeClass tc1 = left.get_type_class();
    ObObjTypeClass tc2 = right.get_type_class();
    if (ObIntTC == tc1) {
      if (ObIntTC == tc2 || ObUIntTC == tc2) {
        ret = minus_int(result, left, right, expr_ctx.calc_buf_, calc_scale);
      } else {
        ret = ObArithExprOperator::calc_(result, left, right, expr_ctx, calc_scale, calc_type, minus_funcs_);
      }
    } else if (ObUIntTC == tc1) {
      if (ObIntTC == tc2 || ObUIntTC == tc2) {
        ret = minus_uint(result, left, right, expr_ctx.calc_buf_, calc_scale);
      } else {
        ret = ObArithExprOperator::calc_(result, left, right, expr_ctx, calc_scale, calc_type, minus_funcs_);
      }
    } else {
      result.set_type(get_result_type().get_type());
      ret = ObArithExprOperator::calc_(
          result, left, right, expr_ctx, calc_scale, calc_type, T_OP_AGG_MINUS ? agg_minus_funcs_ : minus_funcs_);
    }
  }
  return ret;
}

int ObExprMinus::calc_datetime_minus(common::ObObj& result, const common::ObObj& left, const common::ObObj& right,
    common::ObExprCtx& expr_ctx, common::ObScale calc_scale)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("calc_buf_ should not be null", K(ret), K(expr_ctx.calc_buf_));
  } else {
    const ObObj* res_left = &left;
    const ObObj* res_right = &right;
    ObObj tmp_left_obj;
    ObObj tmp_right_obj;
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NO_CAST_INT_UINT);
    if (OB_FAIL(ObObjCaster::to_type(ObDateTimeType, cast_ctx, left, tmp_left_obj, res_left))) {
      LOG_WARN("cast failed.", K(ret), K(left));
    } else if (OB_FAIL(ObObjCaster::to_type(ObDateTimeType, cast_ctx, right, tmp_right_obj, res_right))) {
      LOG_WARN("cast failed.", K(ret), K(right));
    } else {
      const int64_t LOCAL_BUF_SIZE = ObNumber::MAX_CALC_BYTE_LEN * 5;
      char local_buf[LOCAL_BUF_SIZE];
      ObDataBuffer local_alloc(local_buf, LOCAL_BUF_SIZE);
      ObNumber left_datetime;
      ObNumber right_datetime;
      ObNumber usecs_per_day;
      ObNumber left_date;
      ObNumber right_date;
      if (OB_FAIL(left_datetime.from(res_left->get_datetime(), local_alloc))) {
        LOG_WARN("convert int64 to number failed", K(ret), K(res_left));
      } else if (OB_FAIL(right_datetime.from(res_right->get_datetime(), local_alloc))) {
        LOG_WARN("convert int64 to number failed", K(ret), K(res_right));
      } else if (OB_FAIL(usecs_per_day.from(USECS_PER_DAY, local_alloc))) {
        LOG_WARN("convert int64 to number failed", K(ret));
      } else if (OB_FAIL(left_datetime.div_v3(
                     usecs_per_day, left_date, local_alloc, ObNumber::OB_MAX_DECIMAL_DIGIT, false))) {
        LOG_WARN("calc left date number failed", K(ret));
      } else if (OB_FAIL(right_datetime.div_v3(
                     usecs_per_day, right_date, local_alloc, ObNumber::OB_MAX_DECIMAL_DIGIT, false))) {
        LOG_WARN("calc left date number failed", K(ret));
      } else if (FALSE_IT(tmp_left_obj.set_number(left_date))) {
      } else if (FALSE_IT(tmp_right_obj.set_number(right_date))) {
      } else if (OB_FAIL(minus_number(result, tmp_left_obj, tmp_right_obj, expr_ctx.calc_buf_, calc_scale))) {
        LOG_WARN("minus_number failed.", K(ret), K(tmp_left_obj), K(tmp_right_obj));
      } else {
        LOG_DEBUG("succ to calc_datetime_minus",
            K(ret),
            K(result),
            K(left),
            K(right),
            K(left_datetime),
            K(right_datetime),
            "left_double",
            res_left->get_datetime(),
            K(left_date),
            K(right_date),
            K(tmp_left_obj),
            K(tmp_right_obj));
      }
    }
  }
  return ret;
}

int ObExprMinus::calc_timestamp_minus(
    common::ObObj& result, const common::ObObj& left, const common::ObObj& right, const ObTimeZoneInfo* tz_info)
{
  int ret = OB_SUCCESS;
  ObIntervalDSValue res_interval;
  ObOTimestampData left_v;
  ObOTimestampData right_v;

  if (OB_UNLIKELY(!ob_is_oracle_datetime_tc(left.get_type()) || !ob_is_oracle_datetime_tc(right.get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (left.is_datetime()) {
      ret = ObTimeConverter::odate_to_otimestamp(left.get_datetime(), tz_info, ObTimestampTZType, left_v);
    } else if (left.is_timestamp_nano()) {
      ret = ObTimeConverter::odate_to_otimestamp(
          left.get_otimestamp_value().time_us_, tz_info, ObTimestampTZType, left_v);
      left_v.time_ctx_.tail_nsec_ = left.get_otimestamp_value().time_ctx_.tail_nsec_;
    } else {
      ret = ObTimeConverter::otimestamp_to_otimestamp(
          left.get_type(), left.get_otimestamp_value(), tz_info, ObTimestampTZType, left_v);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to convert left to timestamp tz", K(ret), K(left));
    }
  }

  if (OB_SUCC(ret)) {
    if (right.is_datetime()) {
      ret = ObTimeConverter::odate_to_otimestamp(right.get_datetime(), tz_info, ObTimestampTZType, right_v);
    } else if (right.is_timestamp_nano()) {
      ret = ObTimeConverter::odate_to_otimestamp(
          right.get_otimestamp_value().time_us_, tz_info, ObTimestampTZType, right_v);
      right_v.time_ctx_.tail_nsec_ = right.get_otimestamp_value().time_ctx_.tail_nsec_;
    } else {
      ret = ObTimeConverter::otimestamp_to_otimestamp(
          right.get_type(), right.get_otimestamp_value(), tz_info, ObTimestampTZType, right_v);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to convert right to timestamp tz", K(ret), K(right));
    }
  }

  if (OB_SUCC(ret)) {
    ObTimeConverter::calc_oracle_temporal_minus(left_v, right_v, res_interval);
    ObScale res_scale = ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(
        MAX_SCALE_FOR_ORACLE_TEMPORAL, std::max(left.get_scale(), right.get_scale()));
    result.set_interval_ds(res_interval);
    result.set_scale(res_scale);
  }

  LOG_DEBUG(
      "succ to calc_timestamp_minus", K(ret), K(result), K(left_v), K(right_v), K(left), K(right), K(res_interval));

  return ret;
}

int ObExprMinus::calc(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  return ObArithExprOperator::calc(
      res, left, right, allocator, scale, ObExprResultTypeUtil::get_minus_result_type, minus_funcs_);
}

int ObExprMinus::calc(ObObj& res, const ObObj& left, const ObObj& right, ObExprCtx& expr_ctx, ObScale scale)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode() && ObIntervalTC == right.get_type_class()) {
    ret = interval_add_minus(res, left, right, expr_ctx, scale, true);
  } else if (lib::is_oracle_mode() && ob_is_oracle_datetime_tc(left.get_type()) &&
             ob_is_oracle_datetime_tc(right.get_type())) {
    ret = calc_timestamp_minus(res, left, right, TZ_INFO(expr_ctx.my_session_));
  } else {
    ret = ObArithExprOperator::calc(
        res, left, right, expr_ctx, scale, ObExprResultTypeUtil::get_minus_result_type, minus_funcs_);
  }
  return ret;
}

ObArithFunc ObExprMinus::minus_funcs_[ObMaxTC] = {
    NULL,
    ObExprMinus::minus_int,
    ObExprMinus::minus_uint,
    ObExprMinus::minus_float,
    ObExprMinus::minus_double,
    ObExprMinus::minus_number,
    ObExprMinus::minus_datetime,  // datetime
    NULL,                         // date
    NULL,                         // time
    NULL,                         // year
    NULL,                         // string
    NULL,                         // extend
    NULL,                         // unknown
    NULL,                         // text
    NULL,                         // bit
    NULL,                         // enumset
    NULL,                         // enumsetInner
};

ObArithFunc ObExprMinus::agg_minus_funcs_[ObMaxTC] = {
    NULL,
    ObExprMinus::minus_int,
    ObExprMinus::minus_uint,
    ObExprMinus::minus_float,
    ObExprMinus::minus_double_no_overflow,
    ObExprMinus::minus_number,
    ObExprMinus::minus_datetime,  // datetime
    NULL,                         // date
    NULL,                         // time
    NULL,                         // year
    NULL,                         // string
    NULL,                         // extend
    NULL,                         // unknown
    NULL,                         // text
    NULL,                         // bit
    NULL,                         // enumset
    NULL,                         // enumsetInner
};

int ObExprMinus::minus_int(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  int64_t left_i = left.get_int();
  int64_t right_i = right.get_int();
  char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
  int64_t pos = 0;
  if (left.get_type_class() == right.get_type_class()) {
    res.set_int(left_i - right_i);
    if (OB_UNLIKELY(is_int_int_out_of_range(left_i, right_i, res.get_int()))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%ld - %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT", expr_str);
    }
  } else if (OB_UNLIKELY(ObUIntTC != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    res.set_uint64(left_i - right_i);
    if (OB_UNLIKELY(is_int_uint_out_of_range(left_i, right_i, res.get_uint64()))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%ld - %lu)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMinus::minus_uint(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  uint64_t left_i = left.get_uint64();
  uint64_t right_i = right.get_uint64();
  res.set_uint64(left_i - right_i);
  char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
  int64_t pos = 0;
  if (left.get_type_class() == right.get_type_class()) {
    if (OB_UNLIKELY(is_uint_uint_out_of_range(left_i, right_i, res.get_uint64()))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%lu - %lu)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  } else if (OB_UNLIKELY(ObIntTC != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    if (OB_UNLIKELY(is_uint_int_out_of_range(right_i, left_i, res.get_uint64()))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%lu - %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMinus::minus_float(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
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
    res.set_float(left_f - right_f);
    if (OB_UNLIKELY(is_float_out_of_range(res.get_float()))) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%e - %e)'", left_f, right_f);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BINARY_FLOAT", expr_str);
      LOG_WARN("float out of range", K(res), K(left), K(right), K(res));
    }
  }
  LOG_DEBUG("succ to minus float", K(res), K(left), K(right));
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMinus::minus_double(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left.get_type_class() != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    double left_d = left.get_double();
    double right_d = right.get_double();
    res.set_double(left_d - right_d);
    if (OB_UNLIKELY(is_double_out_of_range(res.get_double()))) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%e - %e)'", left_d, right_d);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DOUBLE", expr_str);
      LOG_WARN("double out of range", K(res), K(left), K(right), K(res));
      res.set_null();
    }
    LOG_DEBUG("succ to minus double", K(res), K(left), K(right));
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMinus::minus_double_no_overflow(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator*, ObScale)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left.get_type_class() != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    double left_d = left.get_double();
    double right_d = right.get_double();
    res.set_double(left_d - right_d);
  }
  return ret;
}

int ObExprMinus::minus_number(ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  number::ObNumber res_nmb;
  if (OB_UNLIKELY(NULL == allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_FAIL(left.get_number().sub_v3(right.get_number(), res_nmb, *allocator))) {
    LOG_WARN("failed to sub numbers", K(ret), K(left), K(right));
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

int ObExprMinus::minus_datetime(
    ObObj& res, const ObObj& left, const ObObj& right, ObIAllocator* allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  const int64_t left_i = left.get_datetime();
  const int64_t right_i = right.get_datetime();
  ObTime ob_time;
  if (OB_LIKELY(left.get_type_class() == right.get_type_class())) {
    int64_t round_value = left_i - right_i;
    ObTimeConverter::round_datetime(OB_MAX_DATE_PRECISION, round_value);
    res.set_datetime(round_value);
    res.set_scale(OB_MAX_DATE_PRECISION);
    if (OB_UNLIKELY(res.get_datetime() > DATETIME_MAX_VAL || res.get_datetime() < DATETIME_MIN_VAL) ||
        (OB_FAIL(ObTimeConverter::datetime_to_ob_time(res.get_datetime(), NULL, ob_time))) ||
        (OB_FAIL(ObTimeConverter::validate_oracle_date(ob_time)))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%ld - %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DATE", expr_str);
    }
  }
  LOG_DEBUG("minus datetime", K(left), K(right), K(ob_time), K(res));
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMinus::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
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
        rt_expr.eval_func_ = ObExprMinus::minus_int_int;
        break;
      case ObUInt64Type:
        if (ObIntTC == left_tc && ObUIntTC == right_tc) {
          rt_expr.eval_func_ = ObExprMinus::minus_int_uint;
        } else if (ObUIntTC == left_tc && ObIntTC == right_tc) {
          rt_expr.eval_func_ = ObExprMinus::minus_uint_int;
        } else if (ObUIntTC == left_tc && ObUIntTC == right_tc) {
          rt_expr.eval_func_ = ObExprMinus::minus_uint_uint;
        }
        break;
      case ObIntervalYMType:
        rt_expr.eval_func_ = ObExprMinus::minus_intervalym_intervalym;
        break;
      case ObIntervalDSType:
        if (ObIntervalDSType == left_type) {
          rt_expr.eval_func_ = ObExprMinus::minus_intervalds_intervalds;
        } else {
          rt_expr.eval_func_ = ObExprMinus::minus_timestamp_timestamp;
        }
        break;
      case ObDateTimeType:
        if (ObIntervalYMType == right_type) {
          rt_expr.eval_func_ = ObExprMinus::minus_datetime_intervalym;
        } else if (ObIntervalDSType == right_type) {
          rt_expr.eval_func_ = ObExprMinus::minus_datetime_intervalds;
        } else if (ObDateTimeType == left_type && ObNumberType == right_type) {
          rt_expr.eval_func_ = ObExprMinus::minus_datetime_number;
        } else {
          rt_expr.eval_func_ = ObExprMinus::minus_datetime_datetime;
        }
        break;
      case ObTimestampTZType:
        if (ObIntervalYMType == right_type) {
          rt_expr.eval_func_ = ObExprMinus::minus_timestamptz_intervalym;
        } else if (ObIntervalDSType == right_type) {
          rt_expr.eval_func_ = ObExprMinus::minus_timestamptz_intervalds;
        }
        break;
      case ObTimestampLTZType:
        if (ObIntervalYMType == right_type) {
          rt_expr.eval_func_ = ObExprMinus::minus_timestampltz_intervalym;
        } else if (ObIntervalDSType == right_type) {
          rt_expr.eval_func_ = ObExprMinus::minus_timestamp_tiny_intervalds;
        }
        break;
      case ObTimestampNanoType:
        if (ObIntervalYMType == right_type) {
          rt_expr.eval_func_ = ObExprMinus::minus_timestampnano_intervalym;
        } else if (ObIntervalDSType == right_type) {
          rt_expr.eval_func_ = ObExprMinus::minus_timestamp_tiny_intervalds;
        }
        break;
      case ObFloatType:
        rt_expr.eval_func_ = ObExprMinus::minus_float_float;
        break;
      case ObDoubleType:
        rt_expr.eval_func_ = ObExprMinus::minus_double_double;
        break;
      case ObUNumberType:
      case ObNumberType:
        if (ObDateTimeType == left_type) {
          rt_expr.eval_func_ = ObExprMinus::minus_datetime_datetime_oracle;
        } else {
          rt_expr.eval_func_ = ObExprMinus::minus_number_number;
        }
        break;
      default:
        break;
    }
    if (OB_ISNULL(rt_expr.eval_func_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected params type.", K(ret), K(left_type), K(right_type), K(result_type));
    }
  }
  return ret;
}

// calc_type is IntTC  left and right has same TC
int ObExprMinus::minus_int_int(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
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
    expr_datum.set_int(left_i - right_i);
    if (OB_UNLIKELY(is_int_int_out_of_range(left_i, right_i, expr_datum.get_int()))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%ld - %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT", expr_str);
    }
  }
  return ret;
}

// calc_type/left_type is IntTC, right is ObUIntTC, only mysql mode
int ObExprMinus::minus_int_uint(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
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
    expr_datum.set_uint(left_i - right_i);
    if (OB_UNLIKELY(is_int_uint_out_of_range(left_i, right_i, expr_datum.get_uint()))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%ld - %lu)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  }
  return ret;
}

// calc_type is UIntTC  left and right has same TC
int ObExprMinus::minus_uint_uint(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
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
    expr_datum.set_uint(left_ui - right_ui);
    if (OB_UNLIKELY(is_uint_uint_out_of_range(left_ui, right_ui, expr_datum.get_uint()))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%lu - %lu)'", left_ui, right_ui);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  }
  return ret;
}

// calc_type/left_tpee is UIntTC , right is intTC. only mysql mode
int ObExprMinus::minus_uint_int(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
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
    expr_datum.set_uint(left_ui - right_ui);
    if (OB_UNLIKELY(is_uint_int_out_of_range(right_ui, left_ui, expr_datum.get_uint()))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%lu - %ld)'", left_ui, right_ui);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  }
  return ret;
}

// calc type is floatTC, left and right has same TC, only oracle mode
int ObExprMinus::minus_float_float(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
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
    expr_datum.set_float(left_f - right_f);
    if (OB_UNLIKELY(is_float_out_of_range(expr_datum.get_float()))) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%e - %e)'", left_f, right_f);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, lib::is_oracle_mode() ? "BINARY_FLOAT" : "FLOAT", expr_str);
      LOG_WARN("float out of range", K(*left), K(*right), K(expr_datum));
    }
  }
  return ret;
}

// calc type is doubleTC, left and right has same TC
int ObExprMinus::minus_double_double(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
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
    expr_datum.set_double(left_d - right_d);
    if (OB_UNLIKELY(is_double_out_of_range(expr_datum.get_double())) && T_OP_AGG_MINUS != expr.type_) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%e - %e)'", left_d, right_d);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, lib::is_oracle_mode() ? "BINARY_DOUBLE" : "DOUBLE", expr_str);
      LOG_WARN("double out of range", K(*left), K(*right), K(expr_datum));
      expr_datum.set_null();
    }
  }
  return ret;
}

// calc type TC is ObNumberTC
int ObExprMinus::minus_number_number(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
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
    if (OB_FAIL(left_nmb.sub_v3(right_nmb, res_nmb, local_alloc))) {
      LOG_WARN("failed to add numbers", K(ret), K(*left), K(*right));
    } else {
      expr_datum.set_number(res_nmb);
    }
  }
  return ret;
}

// interval can calc with different types such as date, timestamp, interval.
// the params do not need to do cast

// left and right have the same type. both IntervalYM.
int ObExprMinus::minus_intervalym_intervalym(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    ObIntervalYMValue value = left->get_interval_nmonth() - right->get_interval_nmonth();
    if (OB_FAIL(value.validate())) {
      LOG_WARN("value validate failed", K(ret), K(value));
    } else {
      expr_datum.set_interval_nmonth(value.get_nmonth());
    }
  }
  return ret;
}

// left and right must have the same type. both IntervalDS
int ObExprMinus::minus_intervalds_intervalds(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    ObIntervalDSValue value = left->get_interval_ds() - right->get_interval_ds();
    if (OB_FAIL(value.validate())) {
      LOG_WARN("value validate failed", K(ret), K(value));
    } else {
      expr_datum.set_interval_ds(value);
    }
  }
  return ret;
}

// Left is datetime TC. Right is intervalYM type.
int ObExprMinus::minus_datetime_intervalym(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    int64_t date_v = left->get_datetime();
    int64_t result_v = 0;

    if (OB_FAIL(ObTimeConverter::date_add_nmonth(date_v, right->get_interval_nmonth() * (-1), result_v))) {
      LOG_WARN("add value failed", K(ret), K(*left), K(*right));
    } else {
      expr_datum.set_datetime(result_v);
    }
  }
  return ret;
}

// Left is datetime TC. Right is intervalDS type.
int ObExprMinus::minus_datetime_intervalds(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    int64_t date_v = left->get_datetime();
    int64_t result_v = 0;

    if (OB_FAIL(ObTimeConverter::date_add_nsecond(date_v,
            right->get_interval_ds().get_nsecond() * (-1),
            right->get_interval_ds().get_fs() * (-1),
            result_v))) {
      LOG_WARN("add value failed", K(ret), K(*left), K(*right));
    } else {
      expr_datum.set_datetime(result_v);
    }
  }
  return ret;
}

// Left is timestampTZ type. Right is intervalYM type.
int ObExprMinus::minus_timestamptz_intervalym(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    ObOTimestampData result_v;

    if (OB_FAIL(ObTimeConverter::otimestamp_add_nmonth(ObTimestampTZType,
            left->get_otimestamp_tz(),
            get_timezone_info(ctx.exec_ctx_.get_my_session()),
            right->get_interval_nmonth() * (-1),
            result_v))) {
      LOG_WARN("calc with timestamp value failed", K(ret), K(*left), K(*right));
    } else {
      expr_datum.set_otimestamp_tz(result_v);
    }
  }
  return ret;
}

// Left is timestampLTZ type. Right is intervalYM type.
int ObExprMinus::minus_timestampltz_intervalym(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    ObOTimestampData result_v;

    if (OB_FAIL(ObTimeConverter::otimestamp_add_nmonth(ObTimestampLTZType,
            left->get_otimestamp_tiny(),
            get_timezone_info(ctx.exec_ctx_.get_my_session()),
            right->get_interval_nmonth() * (-1),
            result_v))) {
      LOG_WARN("calc with timestamp value failed", K(ret), K(*left), K(*right));
    } else {
      expr_datum.set_otimestamp_tiny(result_v);
    }
  }
  return ret;
}

// Left is timestampNano type. Right is intervalYM type.
int ObExprMinus::minus_timestampnano_intervalym(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    ObOTimestampData result_v;

    if (OB_FAIL(ObTimeConverter::otimestamp_add_nmonth(ObTimestampNanoType,
            left->get_otimestamp_tiny(),
            get_timezone_info(ctx.exec_ctx_.get_my_session()),
            right->get_interval_nmonth() * (-1),
            result_v))) {
      LOG_WARN("calc with timestamp value failed", K(ret), K(*left), K(*right));
    } else {
      expr_datum.set_otimestamp_tiny(result_v);
    }
  }
  return ret;
}

// Left is timestamp TZ. Right is intervalDS type.
int ObExprMinus::minus_timestamptz_intervalds(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    ObOTimestampData result_v;

    if (OB_FAIL(ObTimeConverter::otimestamp_add_nsecond(left->get_otimestamp_tz(),
            right->get_interval_ds().get_nsecond() * (-1),
            right->get_interval_ds().get_fs() * (-1),
            result_v))) {
      LOG_WARN("calc with timestamp value failed", K(ret), K(*left), K(*right));
    } else {
      expr_datum.set_otimestamp_tz(result_v);
    }
  }
  return ret;
}

// Left is timestamp LTZ or Nano. Right is intervalDS type.
int ObExprMinus::minus_timestamp_tiny_intervalds(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    ObOTimestampData result_v;

    if (OB_FAIL(ObTimeConverter::otimestamp_add_nsecond(left->get_otimestamp_tiny(),
            right->get_interval_ds().get_nsecond() * (-1),
            right->get_interval_ds().get_fs() * (-1),
            result_v))) {
      LOG_WARN("calc with timestamp value failed", K(ret), K(*left), K(*right));
    } else {
      expr_datum.set_otimestamp_tiny(result_v);
    }
  }
  return ret;
}

// only oracle mode.
// both left and right are datetimeTC or otimestampTC.
// cast left and right to ObTimestampTZType first.
int ObExprMinus::minus_timestamp_timestamp(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    ObOTimestampData left_v, right_v;
    ObObjType left_type = expr.args_[0]->datum_meta_.type_;
    ObObjType right_type = expr.args_[1]->datum_meta_.type_;
    ObIntervalDSValue result_intervalds;
    const ObTimeZoneInfo* tz_info = get_timezone_info(ctx.exec_ctx_.get_my_session());
    if (ObDateTimeType == left_type) {
      ret = ObTimeConverter::odate_to_otimestamp(left->get_datetime(), tz_info, ObTimestampTZType, left_v);
    } else if (ObTimestampNanoType == left_type) {
      ret = ObTimeConverter::odate_to_otimestamp(
          left->get_otimestamp_tiny().time_us_, tz_info, ObTimestampTZType, left_v);
      left_v.time_ctx_.tail_nsec_ = left->get_otimestamp_tiny().time_ctx_.tail_nsec_;
    } else if (ObTimestampTZType == left_type) {
      ret = ObTimeConverter::otimestamp_to_otimestamp(
          ObTimestampTZType, left->get_otimestamp_tz(), tz_info, ObTimestampTZType, left_v);
    } else {
      ret = ObTimeConverter::otimestamp_to_otimestamp(
          ObTimestampLTZType, left->get_otimestamp_tiny(), tz_info, ObTimestampTZType, left_v);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to convert left to timestamp tz", K(ret), K(*left));
    }

    if (OB_SUCC(ret)) {
      if (ObDateTimeType == right_type) {
        ret = ObTimeConverter::odate_to_otimestamp(right->get_datetime(), tz_info, ObTimestampTZType, right_v);
      } else if (ObTimestampNanoType == right_type) {
        ret = ObTimeConverter::odate_to_otimestamp(
            right->get_otimestamp_tiny().time_us_, tz_info, ObTimestampTZType, right_v);
        right_v.time_ctx_.tail_nsec_ = right->get_otimestamp_tiny().time_ctx_.tail_nsec_;
      } else if (ObTimestampTZType == right_type) {
        ret = ObTimeConverter::otimestamp_to_otimestamp(
            ObTimestampTZType, right->get_otimestamp_tz(), tz_info, ObTimestampTZType, right_v);
      } else {
        ret = ObTimeConverter::otimestamp_to_otimestamp(
            ObTimestampLTZType, right->get_otimestamp_tiny(), tz_info, ObTimestampTZType, right_v);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to convert right to timestamp tz", K(ret), K(*right));
      }
    }

    if (OB_SUCC(ret)) {
      ObTimeConverter::calc_oracle_temporal_minus(left_v, right_v, result_intervalds);
      expr_datum.set_interval_ds(result_intervalds);
    }
  }
  return ret;
}

int ObExprMinus::minus_datetime_number(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    number::ObNumber right_nmb(right->get_number());
    const int64_t left_i = left->get_datetime();
    int64_t int_part = 0;
    int64_t dec_part = 0;
    if (!right_nmb.is_int_parts_valid_int64(int_part, dec_part)) {
      ret = OB_INVALID_DATE_FORMAT;
      LOG_WARN("invalid date format", K(ret), K(right_nmb));
    } else {
      const int64_t right_i =
          static_cast<int64_t>(int_part * USECS_PER_DAY) +
          (right_nmb.is_negative() ? -1 : 1) *
              static_cast<int64_t>(static_cast<double>(dec_part) / NSECS_PER_SEC * static_cast<double>(USECS_PER_DAY));
      int64_t round_value = left_i - right_i;
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
        databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%ld - %ld)'", left_i, right_i);
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DATE", expr_str);
      }
    }
  }
  return ret;
}

// left and right are both ObDateTimeTC. calc_type is ObNumberType. only oracle mode.
int ObExprMinus::minus_datetime_datetime_oracle(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  bool is_null = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, expr_datum, is_null))) {
    LOG_WARN("evaluate params failed", K(ret));
  } else if (false == is_null) {
    const int64_t LOCAL_BUF_SIZE = ObNumber::MAX_CALC_BYTE_LEN * 5;
    char local_buf[LOCAL_BUF_SIZE];
    ObDataBuffer local_alloc(local_buf, LOCAL_BUF_SIZE);
    ObNumber left_datetime;
    ObNumber right_datetime;
    ObNumber usecs_per_day;
    ObNumber sub_datetime;
    ObNumber sub_date;
    if (OB_FAIL(left_datetime.from(left->get_datetime(), local_alloc))) {
      LOG_WARN("convert int64 to number failed", K(ret), K(left->get_datetime()));
    } else if (OB_FAIL(right_datetime.from(right->get_datetime(), local_alloc))) {
      LOG_WARN("convert int64 to number failed", K(ret), K(right->get_datetime()));
    } else if (OB_FAIL(usecs_per_day.from(USECS_PER_DAY, local_alloc))) {
      LOG_WARN("convert int64 to number failed", K(ret));
    } else if (OB_FAIL(left_datetime.sub_v3(right_datetime, sub_datetime, local_alloc))) {
      LOG_WARN("sub failed", K(ret), K(left_datetime), K(right_datetime));
    } else if (OB_FAIL(sub_datetime.div_v3(usecs_per_day, sub_date, local_alloc))) {
      LOG_WARN("calc left date number failed", K(ret));
    } else {
      expr_datum.set_number(sub_date);
    }
  }
  return ret;
}

// calc type is datetimeTC. cast left and right to calc_type.
int ObExprMinus::minus_datetime_datetime(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
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
    ObTime ob_time;
    int64_t round_value = left_i - right_i;
    ObTimeConverter::round_datetime(OB_MAX_DATE_PRECISION, round_value);
    expr_datum.set_datetime(round_value);
    if (OB_UNLIKELY(expr_datum.get_datetime() > DATETIME_MAX_VAL || expr_datum.get_datetime() < DATETIME_MIN_VAL) ||
        (OB_FAIL(ObTimeConverter::datetime_to_ob_time(expr_datum.get_datetime(), NULL, ob_time))) ||
        (OB_FAIL(ObTimeConverter::validate_oracle_date(ob_time)))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "'(%ld - %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DATE", expr_str);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
