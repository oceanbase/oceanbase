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
#include "sql/engine/expr/ob_batch_eval_util.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/engine/expr/ob_expr_util.h"


namespace oceanbase
{
using namespace common;
using namespace common::number;
namespace sql
{

ObExprAdd::ObExprAdd(ObIAllocator &alloc, ObExprOperatorType type)
  : ObArithExprOperator(alloc,
                        type,
                        N_ADD,
                        2,
                        NOT_ROW_DIMENSION,
                        ObExprResultTypeUtil::get_add_result_type,
                        ObExprResultTypeUtil::get_add_calc_type,
                        add_funcs_)
{
  param_lazy_eval_ = lib::is_oracle_mode();
}

/* Note: precision, scale计算需要考虑： 字符串，整数，浮点数，十进制数等混合相加的情况 */
int ObExprAdd::calc_result_type2(ObExprResType &type,
                                 ObExprResType &type1,
                                 ObExprResType &type2,
                                 ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  static const int64_t CARRY_OFFSET = 1;
  ObScale scale = SCALE_UNKNOWN_YET;
  ObPrecision precision = PRECISION_UNKNOWN_YET;
  bool is_oracle = lib::is_oracle_mode();
  const bool is_all_decint_args =
    ob_is_decimal_int(type1.get_type()) && ob_is_decimal_int(type2.get_type());
  if (OB_FAIL(ObArithExprOperator::calc_result_type2(type, type1, type2, type_ctx))) {
    LOG_WARN("fail to calc result type", K(ret), K(type), K(type1), K(type2));
  } else if (type.is_decimal_int() && (type1.is_null() || type2.is_null())) {
    type.set_scale(MAX(type1.get_scale(), type2.get_scale()));
    type.set_precision(MAX(type1.get_precision(), type2.get_precision()));
  } else if (OB_UNLIKELY(SCALE_UNKNOWN_YET == type1.get_scale()) ||
             OB_UNLIKELY(SCALE_UNKNOWN_YET == type2.get_scale())) {
    type.set_scale(NUMBER_SCALE_UNKNOWN_YET);
    type.set_precision(PRECISION_UNKNOWN_YET);
  } else if (type1.get_type_class() == ObIntervalTC
            || type2.get_type_class() == ObIntervalTC) {
    type.set_scale(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][type.get_type()].get_scale());
    type.set_precision(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][type.get_type()].get_precision());
  } else {
    if (OB_UNLIKELY(is_oracle && type.is_datetime())) {
      scale = OB_MAX_DATE_PRECISION;
    } else {
      ObScale scale1 = static_cast<ObScale>(MAX(type1.get_scale(), 0));
      ObScale scale2 = static_cast<ObScale>(MAX(type2.get_scale(), 0));
      scale = MAX(scale1, scale2);
      if (lib::is_mysql_mode() && type.is_double()) {
        precision = ObMySQLUtil::float_length(scale);
      } else if (type.has_result_flag(DECIMAL_INT_ADJUST_FLAG)) {
        precision = MAX(type1.get_precision(), type2.get_precision());
      } else {
        int64_t inter_part_length1 = type1.get_precision() - type1.get_scale();
        int64_t inter_part_length2 = type2.get_precision() - type2.get_scale();
        precision = MIN(static_cast<ObPrecision>(MAX(inter_part_length1, inter_part_length2)
                                                 + CARRY_OFFSET + scale),
                        OB_MAX_DECIMAL_POSSIBLE_PRECISION);
      }
    }

    type.set_scale(scale);

    if (OB_UNLIKELY(PRECISION_UNKNOWN_YET == type1.get_precision()) ||
        OB_UNLIKELY(PRECISION_UNKNOWN_YET == type2.get_precision())) {
      type.set_precision(PRECISION_UNKNOWN_YET);
    } else {
      type.set_precision(precision);
    }
    if (is_all_decint_args || type.is_decimal_int()) {
      if (OB_UNLIKELY(PRECISION_UNKNOWN_YET == type.get_precision() ||
                      SCALE_UNKNOWN_YET == type.get_scale())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected decimal int precision and scale", K(ret), K(type));
      } else if (is_oracle && type.get_precision() > OB_MAX_NUMBER_PRECISION) {
        type1.set_calc_type(ObNumberType);
        type2.set_calc_type(ObNumberType);
        type.set_number();
      } else {
        if (ObRawExprUtils::decimal_int_need_cast(type1.get_accuracy(), type.get_accuracy()) ||
              ObRawExprUtils::decimal_int_need_cast(type2.get_accuracy(), type.get_accuracy())) {
          // 如果参数的scale和结果类型不一致，或者是参数精度和结果类型跨了一个decimal int类型（int32->int64)
          // 需要将参数进行cast，此外还需设置DECIMAL_INT_ADJUST_FLAG，标记参数(P, S)已经调整避免下次类型
          // 推导重复调整
          type.set_result_flag(DECIMAL_INT_ADJUST_FLAG);
        }
        type1.set_calc_accuracy(type.get_accuracy());
        type2.set_calc_accuracy(type.get_accuracy());
      }
      LOG_DEBUG("calc_result_type2", K(type.get_accuracy()), K(type1.get_accuracy()),
                                     K(type2.get_accuracy()));
    }
    // reset PS to unknown for oracle number type
    if (OB_SUCC(ret) && is_oracle && type.is_oracle_decimal() && !type.is_decimal_int()) {
      type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
      type.set_precision(PRECISION_UNKNOWN_YET);
    }
    if ((ob_is_double_tc(type.get_type()) || ob_is_float_tc(type.get_type())) && type.get_scale() > 0) {
      // if result is fixed double/float, calc type's of params should also be fixed double/float
      if (ob_is_double_tc(type1.get_calc_type()) || ob_is_float_tc(type1.get_calc_type())) {
        type1.set_calc_scale(type.get_scale());
      }
      if (ob_is_double_tc(type2.get_calc_type()) || ob_is_float_tc(type2.get_calc_type())) {
        type2.set_calc_scale(type.get_scale());
      }
    }
  }
  LOG_DEBUG("calc_result_type2", K(scale), K(type1), K(type2), K(type), K(precision));
  return ret;
}

int ObExprAdd::calc(ObObj &res,
                    const ObObj &left,
                    const ObObj &right,
                    ObIAllocator *allocator,
                    ObScale scale)
{
  return ObArithExprOperator::calc(res, left, right,
                                   allocator, scale,
                                   ObExprResultTypeUtil::get_add_result_type,
                                   add_funcs_);
}

int ObExprAdd::calc(ObObj &res,
                    const ObObj &left,
                    const ObObj &right,
                    ObExprCtx &expr_ctx,
                    ObScale scale)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass tc1 = left.get_type_class();
  const ObObjTypeClass tc2 = right.get_type_class();
  if (lib::is_oracle_mode() && (ObIntervalTC == tc1 || ObIntervalTC == tc2)) {
    //interval can calc with different types such as date, timestamp, interval.
    //the params do not need to do cast
    ret = (ObIntervalTC == tc2) ? interval_add_minus(res, left, right, expr_ctx, scale)
                                : interval_add_minus(res, right, left, expr_ctx, scale);
  } else {
    ret = ObArithExprOperator::calc(res, left, right,
                                    expr_ctx, scale,
                                    ObExprResultTypeUtil::get_add_result_type,
                                    add_funcs_);
  }
  return ret;
}

int ObExprAdd::calc_for_agg(ObObj &res,
                            const ObObj &left,
                            const ObObj &right,
                            ObExprCtx &expr_ctx,
                            ObScale scale)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass tc1 = left.get_type_class();
  const ObObjTypeClass tc2 = right.get_type_class();
  if (lib::is_oracle_mode() && (ObIntervalTC == tc1 || ObIntervalTC == tc2)) {
    //interval can calc with different types such as date, timestamp, interval.
    //the params do not need to do cast
    ret = (ObIntervalTC == tc2) ? interval_add_minus(res, left, right, expr_ctx, scale)
                                : interval_add_minus(res, right, left, expr_ctx, scale);
  } else {
    ret = ObArithExprOperator::calc(res, left, right,
                                    expr_ctx, scale,
                                    ObExprResultTypeUtil::get_add_result_type,
                                    agg_add_funcs_);
  }
  return ret;
}

ObArithFunc ObExprAdd::add_funcs_[ObMaxTC] =
{
  NULL,
  ObExprAdd::add_int,
  ObExprAdd::add_uint,
  ObExprAdd::add_float,
  ObExprAdd::add_double,
  ObExprAdd::add_number,
  ObExprAdd::add_datetime, //datetime
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
  NULL,//interval, hard code using interval_add_minus
};

ObArithFunc ObExprAdd::agg_add_funcs_[ObMaxTC] =
{
  NULL,
  ObExprAdd::add_int,
  ObExprAdd::add_uint,
  ObExprAdd::add_float,
  ObExprAdd::add_double_no_overflow,
  ObExprAdd::add_number,
  ObExprAdd::add_datetime, //datetime
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
  NULL,//interval, hard code using interval_add_minus
};

int ObExprAdd::add_int(ObObj &res,
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
  if (OB_LIKELY(left.get_type_class() == right.get_type_class())) {
    res.set_int(left_i + right_i);
    if (OB_UNLIKELY(is_int_int_out_of_range(left_i, right_i, res.get_int()))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%ld + %ld)'", left_i, right_i);
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
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%ld + %lu)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprAdd::add_uint(ObObj &res,
                        const ObObj &left,
                        const ObObj &right,
                        ObIAllocator *allocator,
                        ObScale scale)
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
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%lu + %lu)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  } else if (ObIntTC != right.get_type_class()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    if (OB_UNLIKELY(is_int_uint_out_of_range(right_i, left_i, res.get_uint64()))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%lu + %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprAdd::add_float(ObObj &res,
                         const ObObj &left,
                         const ObObj &right,
                         ObIAllocator *allocator,
                         ObScale scale)
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
    if (OB_UNLIKELY(is_float_out_of_range(res.get_float()))
        && !lib::is_oracle_mode()) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%e + %e)'", left_f, right_f);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BINARY_FLOAT", expr_str);
      LOG_WARN("float out of range", K(res), K(left), K(right), K(res));
    }
    LOG_DEBUG("succ to add float", K(res), K(left), K(right));
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprAdd::add_double(ObObj &res,
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
    res.set_double(left_d + right_d);
    if (OB_UNLIKELY(is_double_out_of_range(res.get_double()))
        && !lib::is_oracle_mode()) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%e + %e)'", left_d, right_d);
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

int ObExprAdd::add_double_no_overflow(ObObj &res,
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
    res.set_double(left_d + right_d);
    LOG_DEBUG("succ to add double", K(res), K(left), K(right));
  }
  return ret;
}

int ObExprAdd::add_number(ObObj &res,
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

int ObExprAdd::add_datetime(ObObj &res, const ObObj &left, const ObObj &right,
    ObIAllocator *allocator, ObScale scale)
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
    if (OB_UNLIKELY(res.get_datetime() > DATETIME_MAX_VAL || res.get_datetime() < DATETIME_MIN_VAL)
        || (OB_FAIL(ObTimeConverter::datetime_to_ob_time(res.get_datetime(), NULL, ob_time)))
        || (OB_FAIL(ObTimeConverter::validate_oracle_date(ob_time)))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%ld + %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DATE", expr_str);
    }
  }
  LOG_DEBUG("add datetime", K(left), K(right), K(scale), K(res));
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprAdd::cg_expr(ObExprCGCtx &op_cg_ctx,
                       const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
#define SET_ADD_FUNC_PTR(v) \
  rt_expr.eval_func_ = ObExprAdd::v; \
  rt_expr.eval_batch_func_ = ObExprAdd::v##_batch;

  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  if (rt_expr.arg_cnt_ != 2 || OB_ISNULL(rt_expr.args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of children is not 2 or children is null", K(ret), K(rt_expr.arg_cnt_),
                                                              K(rt_expr.args_));
  } else if (OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(rt_expr.args_[0]), K(rt_expr.args_[1]));
  } else {
    rt_expr.eval_func_ = NULL;
    rt_expr.may_not_need_raw_check_ = false;
    const ObObjType left_type = rt_expr.args_[0]->datum_meta_.type_;
    const ObObjType right_type = rt_expr.args_[1]->datum_meta_.type_;
    const ObObjType result_type = rt_expr.datum_meta_.type_;
    const ObObjTypeClass left_tc = ob_obj_type_class(left_type);
    const ObObjTypeClass right_tc = ob_obj_type_class(right_type);
    switch (result_type) {
      case ObIntType:
        rt_expr.may_not_need_raw_check_ = true;
        SET_ADD_FUNC_PTR(add_int_int);
        rt_expr.eval_vector_func_ = add_int_int_vector;
        break;
      case ObUInt64Type:
        rt_expr.may_not_need_raw_check_ = false;
        if (ObIntTC == left_tc && ObUIntTC == right_tc) {
          SET_ADD_FUNC_PTR(add_int_uint);
          rt_expr.eval_vector_func_ = add_int_uint_vector;
        } else if (ObUIntTC == left_tc && ObIntTC == right_tc) {
          SET_ADD_FUNC_PTR(add_uint_int);
          rt_expr.eval_vector_func_ = add_uint_int_vector;
        } else if (ObUIntTC == left_tc && ObUIntTC == right_tc) {
          SET_ADD_FUNC_PTR(add_uint_uint);
          rt_expr.eval_vector_func_ = add_uint_uint_vector;
        }
        break;
      case ObIntervalYMType:
        SET_ADD_FUNC_PTR(add_intervalym_intervalym);
        break;
      case ObIntervalDSType:
        SET_ADD_FUNC_PTR(add_intervalds_intervalds);
        break;
      case ObDateTimeType:
        switch (left_type) {
          case ObIntervalYMType:
            SET_ADD_FUNC_PTR(add_intervalym_datetime);
            break;
          case ObIntervalDSType:
            SET_ADD_FUNC_PTR(add_intervalds_datetime);
            break;
          case ObNumberType:
            SET_ADD_FUNC_PTR(add_number_datetime);
            break;
          case ObDateTimeType:
            switch(right_type) {
              case ObIntervalYMType:
                SET_ADD_FUNC_PTR(add_datetime_intervalym);
                break;
              case ObNumberType:
                SET_ADD_FUNC_PTR(add_datetime_number);
                break;
              case ObIntervalDSType:
                SET_ADD_FUNC_PTR(add_datetime_intervalds);
                break;
             default:
               SET_ADD_FUNC_PTR(add_datetime_datetime);
               break;
            }
            break;
          default:
            SET_ADD_FUNC_PTR(add_datetime_datetime);
            break;
        }
        break;
      case ObTimestampTZType:
        switch (left_type) {
          case ObIntervalYMType:
            SET_ADD_FUNC_PTR(add_intervalym_timestamptz);
            break;
          case ObIntervalDSType:
            SET_ADD_FUNC_PTR(add_intervalds_timestamptz);
            break;
          case ObTimestampTZType:
            if (ObIntervalYMType == right_type) {
              SET_ADD_FUNC_PTR(add_timestamptz_intervalym);
            } else if (ObIntervalDSType == right_type) {
              SET_ADD_FUNC_PTR(add_timestamptz_intervalds);
            }
            break;
          default:
            break;
        }
        break;
      case ObTimestampLTZType:
        switch (left_type) {
          case ObIntervalYMType:
            SET_ADD_FUNC_PTR(add_intervalym_timestampltz);
            break;
          case ObIntervalDSType:
            SET_ADD_FUNC_PTR(add_intervalds_timestamp_tiny);
            break;
          case ObTimestampLTZType:
            if (ObIntervalYMType == right_type) {
              SET_ADD_FUNC_PTR(add_timestampltz_intervalym);
            } else if (ObIntervalDSType == right_type) {
              SET_ADD_FUNC_PTR(add_timestamp_tiny_intervalds);
            }
            break;
          default:
            break;
        }
        break;
      case ObTimestampNanoType:
        switch (left_type) {
          case ObIntervalYMType:
            SET_ADD_FUNC_PTR(add_intervalym_timestampnano);
            break;
          case ObIntervalDSType:
            SET_ADD_FUNC_PTR(add_intervalds_timestamp_tiny);
            break;
          case ObTimestampNanoType:
            if (ObIntervalYMType == right_type) {
              SET_ADD_FUNC_PTR(add_timestampnano_intervalym);
            } else if (ObIntervalDSType == right_type) {
              SET_ADD_FUNC_PTR(add_timestamp_tiny_intervalds);
            }
            break;
          default:
            break;
        }
        break;
      case ObFloatType:
        SET_ADD_FUNC_PTR(add_float_float);
        rt_expr.eval_vector_func_ = add_float_float_vector;
        break;
      case ObDoubleType:
        SET_ADD_FUNC_PTR(add_double_double);
        rt_expr.eval_vector_func_ = add_double_double_vector;
        break;
      case ObUNumberType:
      case ObNumberType:
        if (ob_is_decimal_int(left_type) && ob_is_decimal_int(right_type)) {
          switch (get_decimalint_type(rt_expr.args_[0]->datum_meta_.precision_)) {
            case DECIMAL_INT_32:
              SET_ADD_FUNC_PTR(add_decimalint32_oracle);
              rt_expr.eval_vector_func_ = add_decimalint32_oracle_vector;
              break;
            case DECIMAL_INT_64:
              SET_ADD_FUNC_PTR(add_decimalint64_oracle);
              rt_expr.eval_vector_func_ = add_decimalint64_oracle_vector;
              break;
            case DECIMAL_INT_128:
              SET_ADD_FUNC_PTR(add_decimalint128_oracle);
              rt_expr.eval_vector_func_ = add_decimalint128_oracle_vector;
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected precision", K(ret), K(rt_expr.datum_meta_));
              break;
          }
        } else {
          SET_ADD_FUNC_PTR(add_number_number);
          rt_expr.eval_vector_func_ = add_number_number_vector;
        }
        break;
      case ObDecimalIntType:
        switch (get_decimalint_type(rt_expr.datum_meta_.precision_)) {
          case DECIMAL_INT_32:
            SET_ADD_FUNC_PTR(add_decimalint32);
            rt_expr.eval_vector_func_ = add_decimalint32_vector;
            break;
          case DECIMAL_INT_64:
            SET_ADD_FUNC_PTR(add_decimalint64);
            rt_expr.eval_vector_func_ = add_decimalint64_vector;
            break;
          case DECIMAL_INT_128:
            SET_ADD_FUNC_PTR(add_decimalint128);
            rt_expr.eval_vector_func_ = add_decimalint128_vector;
            break;
          case DECIMAL_INT_256:
            SET_ADD_FUNC_PTR(add_decimalint256);
            rt_expr.eval_vector_func_ = add_decimalint256_vector;
            break;
          case DECIMAL_INT_512:
            if (rt_expr.datum_meta_.precision_ < OB_MAX_DECIMAL_POSSIBLE_PRECISION) {
              SET_ADD_FUNC_PTR(add_decimalint512);
              rt_expr.eval_vector_func_ = add_decimalint512_vector;
            } else {
              SET_ADD_FUNC_PTR(add_decimalint512_with_check);
              rt_expr.eval_vector_func_ = add_decimalint512_with_check_vector;
            }
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected precision", K(ret), K(rt_expr.datum_meta_));
            break;
        }
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
#undef SET_ADD_FUNC_PTR
}

struct ObIntIntBatchAddRaw : public ObArithOpRawType<int64_t, int64_t, int64_t>
{
  static void raw_op(int64_t &res, const int64_t l, const int64_t r)
  {
    res = l + r;
  }

  static int raw_check(const int64_t res, const int64_t l, const int64_t r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObExprAdd::is_int_int_out_of_range(l, r, res))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%ld + %ld)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT", expr_str);
    }
    return ret;
  }
};

//calc_type is IntTC  left and right has same TC
int ObExprAdd::add_int_int(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObArithOpWrap<ObIntIntBatchAddRaw>>(EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_int_int_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op<ObArithOpWrap<ObIntIntBatchAddRaw>>(BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_int_int_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObIntIntBatchAddRaw>>(VECTOR_EVAL_FUNC_ARG_LIST);
}

struct ObIntUIntBatchAddRaw : public ObArithOpRawType<uint64_t, int64_t, uint64_t>
{
  static void raw_op(uint64_t &res, const int64_t l, const uint64_t r)
  {
    res = l + r;
  }

  static int raw_check(const uint64_t res, const int64_t l, const uint64_t r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObExprAdd::is_int_uint_out_of_range(l, r, res))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%ld + %lu)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
    return ret;
  }
};

// calc_type/left_type is IntTC, right is ObUIntTC, only mysql mode
int ObExprAdd::add_int_uint(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObArithOpWrap<ObIntUIntBatchAddRaw>>(EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_int_uint_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op<ObArithOpWrap<ObIntUIntBatchAddRaw>>(BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_int_uint_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObIntUIntBatchAddRaw>>(VECTOR_EVAL_FUNC_ARG_LIST);
}

struct ObUIntUIntBatchAddRaw : public ObArithOpRawType<uint64_t, uint64_t, uint64_t>
{
  static void raw_op(uint64_t &res, const uint64_t l, const uint64_t r)
  {
    res = l + r;
  }

  static int raw_check(const uint64_t res, const uint64_t l, const uint64_t r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObExprAdd::is_uint_uint_out_of_range(l, r, res))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%lu + %lu)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
    return ret;
  }
};

//calc_type is UIntTC  left and right has same TC
int ObExprAdd::add_uint_uint(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObArithOpWrap<ObUIntUIntBatchAddRaw>>(EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_uint_uint_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op<ObArithOpWrap<ObUIntUIntBatchAddRaw>>(BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_uint_uint_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObUIntUIntBatchAddRaw>>(VECTOR_EVAL_FUNC_ARG_LIST);
}

struct ObUIntIntBatchAddRaw : public ObArithOpRawType<uint64_t, uint64_t, int64_t>
{
  static void raw_op(uint64_t &res, const uint64_t l, const int64_t r)
  {
    res = l + r;
  }

  static int raw_check(const uint64_t res, const uint64_t l, const int64_t r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObExprAdd::is_int_uint_out_of_range(r, l, res))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%lu + %ld)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
    return ret;
  }
};

// calc_type/left_tpee is UIntTC , right is intTC. only mysql mode
int ObExprAdd::add_uint_int(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObArithOpWrap<ObUIntIntBatchAddRaw>>(EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_uint_int_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op<ObArithOpWrap<ObUIntIntBatchAddRaw>>(BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_uint_int_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObUIntIntBatchAddRaw>>(VECTOR_EVAL_FUNC_ARG_LIST);
}

struct ObFloatBatchAddRawNoCheck : public ObArithOpRawType<float, float, float>
{
  static void raw_op(float &res, const float l, const float r)
  {
    res = l + r;
  }

  static int raw_check(const float, const float, const float)
  {
    return OB_SUCCESS;
  }
};

struct ObFloatBatchAddRawWithCheck: public ObFloatBatchAddRawNoCheck
{
  static int raw_check(const float res, const float l, const float r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObExprAdd::is_float_out_of_range(res))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%e + %e)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "FLOAT", expr_str);
      LOG_WARN("float out of range", K(l), K(r), K(res));
    }
    return ret;
  }
};

//calc type is floatTC, left and right has same TC
int ObExprAdd::add_float_float(EVAL_FUNC_ARG_DECL)
{
  return lib::is_oracle_mode()
      ? def_arith_eval_func<ObArithOpWrap<ObFloatBatchAddRawNoCheck>>(EVAL_FUNC_ARG_LIST)
      : def_arith_eval_func<ObArithOpWrap<ObFloatBatchAddRawWithCheck>>(EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_float_float_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return lib::is_oracle_mode()
      ? def_batch_arith_op<ObArithOpWrap<ObFloatBatchAddRawNoCheck>>(BATCH_EVAL_FUNC_ARG_LIST)
      : def_batch_arith_op<ObArithOpWrap<ObFloatBatchAddRawWithCheck>>(BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_float_float_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return lib::is_oracle_mode() ?
           def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObFloatBatchAddRawNoCheck>>(
             VECTOR_EVAL_FUNC_ARG_LIST) :
           def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObFloatBatchAddRawWithCheck>>(
             VECTOR_EVAL_FUNC_ARG_LIST);
}

struct ObDoubleBatchAddRawNoCheck : public ObArithOpRawType<double, double, double>
{
  static void raw_op(double &res, const double l, const double r)
  {
    res = l + r;
  }

  static int raw_check(const double , const double , const double)
  {
    return OB_SUCCESS;
  }
};

struct ObDoubleBatchAddRawWithCheck: public ObDoubleBatchAddRawNoCheck
{
  static int raw_check(const double res, const double l, const double r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObExprAdd::is_double_out_of_range(res))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%e + %e)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DOUBLE", expr_str);
      LOG_WARN("double out of range", K(l), K(r), K(res));
    }
    return ret;
  }
};

//calc type is doubleTC, left and right has same TC
int ObExprAdd::add_double_double(EVAL_FUNC_ARG_DECL)
{
  return lib::is_oracle_mode() || T_OP_AGG_ADD == expr.type_
      ? def_arith_eval_func<ObArithOpWrap<ObDoubleBatchAddRawNoCheck>>(EVAL_FUNC_ARG_LIST)
      : def_arith_eval_func<ObArithOpWrap<ObDoubleBatchAddRawWithCheck>>(EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_double_double_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return lib::is_oracle_mode() || T_OP_AGG_ADD == expr.type_
      ? def_batch_arith_op<ObArithOpWrap<ObDoubleBatchAddRawNoCheck>>(BATCH_EVAL_FUNC_ARG_LIST)
      : def_batch_arith_op<ObArithOpWrap<ObDoubleBatchAddRawWithCheck>>(BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_double_double_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return lib::is_oracle_mode() || T_OP_AGG_ADD == expr.type_ ?
           def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObDoubleBatchAddRawNoCheck>>(
             VECTOR_EVAL_FUNC_ARG_LIST) :
           def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObDoubleBatchAddRawWithCheck>>(
             VECTOR_EVAL_FUNC_ARG_LIST);
}

struct ObNumberAddFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    char local_buff[ObNumber::MAX_BYTE_LEN];
    ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);
    number::ObNumber l_num(l.get_number());
    number::ObNumber r_num(r.get_number());
    number::ObNumber res_num;
    if (OB_FAIL(l_num.add_v3(r_num, res_num, local_alloc))) {
      LOG_WARN("add num failed", K(ret), K(l_num), K(r_num));
    } else {
      res.set_number(res_num);
    }
    return ret;
  }
};

//calc type TC is ObNumberTC
int ObExprAdd::add_number_number(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObNumberAddFunc>(EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_number_number_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  LOG_DEBUG("add_number_number_batch begin");
  int ret = OB_SUCCESS;
  ObDatumVector l_datums;
  ObDatumVector r_datums;
  const ObExpr &left = *expr.args_[0];
  const ObExpr &right = *expr.args_[1];

  if (OB_FAIL(binary_operand_batch_eval(expr, ctx, skip, size,
                                        lib::is_oracle_mode()))) {
    LOG_WARN("number add batch evaluation failure", K(ret));
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
      // Notice that, space of tmp is allocated in frame but without memset operation, which causes random memory content.
      // And the reserved in storage layer should be 0, thus you must replacement new here to avoid checksum error, etc.
      ObNumber::Desc *res_desc = new (&desc_buf) ObNumber::Desc();
      // speedup detection
      if (ObNumber::try_fast_add(l_num, r_num, res_digits, *res_desc)) {
        results.at(i)->set_pack(sizeof(number::ObCompactNumber) +
                                res_desc->len_ * sizeof(*res_digits));
        eval_flags.set(i);
        // LOG_DEBUG("mul speedup", K(l_num.format()),
        // K(r_num.format()), K(res_num.format()));
      } else {
        // normal path: no speedup
        if (OB_FAIL(l_num.add_v3(r_num, res_num, local_alloc))) {
          LOG_WARN("mul num failed", K(ret), K(l_num), K(r_num));
        } else {
          results.at(i)->set_number(res_num);
          eval_flags.set(i);
        }
        local_alloc.free();
      }
    }
  }
  LOG_DEBUG("add_number_number_batch done");
  return ret;
}

struct NmbTryFastAdditionOp
{
  OB_INLINE bool operator()(ObNumber &l_num, ObNumber &r_num, uint32_t *res_digit,
                                   ObNumberDesc &res_desc)
  {
    return ObNumber::try_fast_add(l_num, r_num, res_digit, res_desc);
  }
   OB_INLINE int operator()(const ObNumber &left, const ObNumber &right, ObNumber &value,
                           ObIAllocator &allocator)
  {
    return ObNumber::add_v3(left, right, value, allocator);
  }
};

int ObExprAdd::add_number_number_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  NmbTryFastAdditionOp op;
  return def_number_vector_arith_op(VECTOR_EVAL_FUNC_ARG_LIST, op);
}

struct ObIntervalYMIntervalYMAddFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    ObIntervalYMValue value = l.get_interval_nmonth() + r.get_interval_nmonth();
    if (OB_FAIL(value.validate())) {
      LOG_WARN("value validate failed", K(ret), K(value));
    } else {
      res.set_interval_nmonth(value.get_nmonth());
    }
    return ret;
  }
};

//1.interval can calc with different types such as date, timestamp, interval.
//the params do not need to do cast
//2.left and right must have the same type. both IntervalYM or both IntervalDS
int ObExprAdd::add_intervalym_intervalym(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObIntervalYMIntervalYMAddFunc>(EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_intervalym_intervalym_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalYMIntervalYMAddFunc>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

struct ObIntervalDSIntervalDSAddFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    ObIntervalDSValue value = l.get_interval_ds() + r.get_interval_ds();
    if (OB_FAIL(value.validate())) {
      LOG_WARN("value validate failed", K(ret), K(value));
    } else {
      res.set_interval_ds(value);
    }
    return ret;
  }
};

//left and right must have the same type. both IntervalDS
int ObExprAdd::add_intervalds_intervalds(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObIntervalDSIntervalDSAddFunc>(EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_intervalds_intervalds_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalDSIntervalDSAddFunc>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

template <bool SWAP_L_R>
struct ObIntervalYMDatetimeAddFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    int64_t result_v = 0;
    ret = SWAP_L_R
        ? ObTimeConverter::date_add_nmonth(l.get_datetime(), r.get_interval_nmonth(), result_v)
        : ObTimeConverter::date_add_nmonth(r.get_datetime(), l.get_interval_nmonth(), result_v);
    if (OB_FAIL(ret)) {
      LOG_WARN("add value failed", K(ret), K(l), K(r));
    } else {
      res.set_datetime(result_v);
    }
    return ret;
  }
};

//Left is intervalYM type. Right is datetime TC
int ObExprAdd::add_intervalym_datetime_common(EVAL_FUNC_ARG_DECL, bool interval_left)
{
  return interval_left
      ? def_arith_eval_func<ObIntervalYMDatetimeAddFunc<false>>(EVAL_FUNC_ARG_LIST)
      : def_arith_eval_func<ObIntervalYMDatetimeAddFunc<true>>(EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_intervalym_datetime_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalYMDatetimeAddFunc<false>>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_datetime_intervalym_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalYMDatetimeAddFunc<true>>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

template <bool SWAP_L_R>
struct ObIntervalDSDatetimeAddFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    int64_t result_v = 0;
    ret = !SWAP_L_R
        ? ObTimeConverter::date_add_nsecond(r.get_datetime(),
                                            l.get_interval_ds().get_nsecond(),
                                            l.get_interval_ds().get_fs(), result_v)
        : ObTimeConverter::date_add_nsecond(l.get_datetime(),
                                            r.get_interval_ds().get_nsecond(),
                                            r.get_interval_ds().get_fs(), result_v);
    if (OB_FAIL(ret)) {
      LOG_WARN("add value failed", K(ret), K(l), K(r));
    } else {
      res.set_datetime(result_v);
    }
    return ret;
  }
};

//Left is intervalDS type. Right is datetime TC
int ObExprAdd::add_intervalds_datetime_common(EVAL_FUNC_ARG_DECL, bool interval_left)
{
  return interval_left
      ? def_arith_eval_func<ObIntervalDSDatetimeAddFunc<false>>(EVAL_FUNC_ARG_LIST)
      : def_arith_eval_func<ObIntervalDSDatetimeAddFunc<true>>(EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_intervalds_datetime_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalDSDatetimeAddFunc<false>>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_datetime_intervalds_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalDSDatetimeAddFunc<true>>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

template <bool SWAP_L_R>
struct ObIntervalYMTimestampTZAddFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, ObEvalCtx &ctx) const
  {
    int ret = OB_SUCCESS;
    ObOTimestampData result_v;
    ret = SWAP_L_R
        ? ObTimeConverter::otimestamp_add_nmonth(
            ObTimestampTZType,
            l.get_otimestamp_tz(),
            get_timezone_info(ctx.exec_ctx_.get_my_session()),
            r.get_interval_nmonth(),
            result_v)
        : ObTimeConverter::otimestamp_add_nmonth(
            ObTimestampTZType,
            r.get_otimestamp_tz(),
            get_timezone_info(ctx.exec_ctx_.get_my_session()),
            l.get_interval_nmonth(),
            result_v);
    if (OB_FAIL(ret)) {
      LOG_WARN("add value failed", K(ret), K(l), K(r));
    } else {
      res.set_otimestamp_tz(result_v);
    }
    return ret;
  }
};

//Left is intervalYM type. Right is timestampTZ type.
int ObExprAdd::add_intervalym_timestamptz_common(EVAL_FUNC_ARG_DECL, bool interval_left)
{
  return interval_left
      ? def_arith_eval_func<ObIntervalYMTimestampTZAddFunc<false>>(EVAL_FUNC_ARG_LIST, ctx)
      : def_arith_eval_func<ObIntervalYMTimestampTZAddFunc<true>>(EVAL_FUNC_ARG_LIST, ctx);
}

int ObExprAdd::add_intervalym_timestamptz_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalYMTimestampTZAddFunc<false>>(
      BATCH_EVAL_FUNC_ARG_LIST, ctx);
}

int ObExprAdd::add_timestamptz_intervalym_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalYMTimestampTZAddFunc<true>>(
      BATCH_EVAL_FUNC_ARG_LIST, ctx);
}

template <bool SWAP_L_R>
struct ObIntervalYMTimestampLTZAddFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, ObEvalCtx &ctx) const
  {
    int ret = OB_SUCCESS;
    ObOTimestampData result_v;
    ret = !SWAP_L_R
        ? ObTimeConverter::otimestamp_add_nmonth(
            ObTimestampLTZType, r.get_otimestamp_tiny(),
            get_timezone_info(ctx.exec_ctx_.get_my_session()),
            l.get_interval_nmonth(), result_v)
        : ObTimeConverter::otimestamp_add_nmonth(
            ObTimestampLTZType, l.get_otimestamp_tiny(),
            get_timezone_info(ctx.exec_ctx_.get_my_session()),
            r.get_interval_nmonth(), result_v);

    if (OB_FAIL(ret)) {
      LOG_WARN("calc with timestamp value failed", K(ret), K(l), K(r));
    } else {
      res.set_otimestamp_tiny(result_v);
    }
    return ret;
  }
};


//Left is intervalYM type. Right is timestampLTZ type.
int ObExprAdd::add_intervalym_timestampltz_common(EVAL_FUNC_ARG_DECL, bool interval_left)
{
  return interval_left
      ? def_arith_eval_func<ObIntervalYMTimestampLTZAddFunc<false>>(EVAL_FUNC_ARG_LIST, ctx)
      : def_arith_eval_func<ObIntervalYMTimestampLTZAddFunc<true>>(EVAL_FUNC_ARG_LIST, ctx);
}

int ObExprAdd::add_intervalym_timestampltz_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalYMTimestampLTZAddFunc<false>>(
      BATCH_EVAL_FUNC_ARG_LIST, ctx);
}

int ObExprAdd::add_timestampltz_intervalym_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalYMTimestampLTZAddFunc<true>>(
      BATCH_EVAL_FUNC_ARG_LIST, ctx);
}

template <bool SWAP_L_R>
struct ObIntervalYMTimestampNanoAddFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, ObEvalCtx &ctx) const
  {
    int ret = OB_SUCCESS;
    ObOTimestampData result_v;
    ret = !SWAP_L_R
        ? ObTimeConverter::otimestamp_add_nmonth(
            ObTimestampNanoType, r.get_otimestamp_tiny(),
            get_timezone_info(ctx.exec_ctx_.get_my_session()),
            l.get_interval_nmonth(), result_v)
        : ObTimeConverter::otimestamp_add_nmonth(
            ObTimestampNanoType, l.get_otimestamp_tiny(),
            get_timezone_info(ctx.exec_ctx_.get_my_session()),
            r.get_interval_nmonth(), result_v);

    if (OB_FAIL(ret)) {
      LOG_WARN("calc with timestamp value failed", K(ret), K(l), K(r));
    } else {
      res.set_otimestamp_tiny(result_v);
    }
    return ret;
  }
};

//Left is intervalYM type. Right is ObTimestampNano type.
int ObExprAdd::add_intervalym_timestampnano_common(EVAL_FUNC_ARG_DECL, bool interval_left)
{
  return interval_left
      ? def_arith_eval_func<ObIntervalYMTimestampNanoAddFunc<false>>(EVAL_FUNC_ARG_LIST, ctx)
      : def_arith_eval_func<ObIntervalYMTimestampNanoAddFunc<true>>(EVAL_FUNC_ARG_LIST, ctx);
}

int ObExprAdd::add_intervalym_timestampnano_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalYMTimestampNanoAddFunc<false>>(
      BATCH_EVAL_FUNC_ARG_LIST, ctx);
}

int ObExprAdd::add_timestampnano_intervalym_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalYMTimestampNanoAddFunc<true>>(
      BATCH_EVAL_FUNC_ARG_LIST, ctx);
}

template <bool SWAP_L_R>
struct ObIntervalDSTimestampTZAddFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    ObOTimestampData result_v;
    ret = !SWAP_L_R
        ? ObTimeConverter::otimestamp_add_nsecond(r.get_otimestamp_tz(),
                                                  l.get_interval_ds().get_nsecond(),
                                                  l.get_interval_ds().get_fs(),
                                                  result_v)
        : ObTimeConverter::otimestamp_add_nsecond(l.get_otimestamp_tz(),
                                                  r.get_interval_ds().get_nsecond(),
                                                  r.get_interval_ds().get_fs(),
                                                  result_v);
    if (OB_FAIL(ret)) {
      LOG_WARN("calc with timestamp value failed", K(ret), K(l), K(r));
    } else {
      res.set_otimestamp_tz(result_v);
    }
    return ret;
  }
};

//Left is intervalDS type. Right is timestampTZ
int ObExprAdd::add_intervalds_timestamptz_common(EVAL_FUNC_ARG_DECL, bool interval_left)
{
  return interval_left
      ? def_arith_eval_func<ObIntervalDSTimestampTZAddFunc<false>>(EVAL_FUNC_ARG_LIST)
      : def_arith_eval_func<ObIntervalDSTimestampTZAddFunc<true>>(EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_intervalds_timestamptz_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalDSTimestampTZAddFunc<false>>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_timestamptz_intervalds_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalDSTimestampTZAddFunc<true>>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

template <bool SWAP_L_R>
struct ObIntervalDSTimestampLTZAdd
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    ObOTimestampData result_v;
    ret = !SWAP_L_R
        ? ObTimeConverter::otimestamp_add_nsecond(r.get_otimestamp_tiny(),
                                                  l.get_interval_ds().get_nsecond(),
                                                  l.get_interval_ds().get_fs(),
                                                  result_v)
        : ObTimeConverter::otimestamp_add_nsecond(l.get_otimestamp_tiny(),
                                                  r.get_interval_ds().get_nsecond(),
                                                  r.get_interval_ds().get_fs(),
                                                  result_v);
    if (OB_FAIL(ret)) {
      LOG_WARN("calc with timestamp value failed", K(ret), K(l), K(r));
    } else {
      res.set_otimestamp_tiny(result_v);
    }
    return ret;
  }
};

//Left is intervalDS type. Right is timestamp LTZ or Nano
int ObExprAdd::add_intervalds_timestamp_tiny_common(EVAL_FUNC_ARG_DECL, bool interval_left)
{
  return interval_left
      ? def_arith_eval_func<ObIntervalDSTimestampLTZAdd<false>>(EVAL_FUNC_ARG_LIST)
      : def_arith_eval_func<ObIntervalDSTimestampLTZAdd<true>>(EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_intervalds_timestamp_tiny_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalDSTimestampLTZAdd<false>>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_timestamp_tiny_intervalds_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalDSTimestampLTZAdd<true>>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

template <bool SWAP_L_R>
struct ObNumberDatetimeAddFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    number::ObNumber left_nmb((!SWAP_L_R ? l : r).get_number());
    const int64_t right_i = (!SWAP_L_R ? r : l).get_datetime();
    int64_t int_part = 0;
    int64_t dec_part = 0;
    if (!left_nmb.is_int_parts_valid_int64(int_part,dec_part)) {
      ret = OB_INVALID_DATE_FORMAT;
      LOG_WARN("invalid date format", K(ret), K(left_nmb));
    } else {
      const int64_t left_i = static_cast<int64_t>(int_part * USECS_PER_DAY)
                            + (left_nmb.is_negative() ? -1  : 1 )
                                * static_cast<int64_t>(static_cast<double>(dec_part)
                                / NSECS_PER_SEC * static_cast<double>(USECS_PER_DAY));
      int64_t round_value = left_i + right_i;
      ObTimeConverter::round_datetime(OB_MAX_DATE_PRECISION, round_value);
      res.set_datetime(round_value);
      ObTime ob_time;
      if (OB_UNLIKELY(res.get_datetime() > DATETIME_MAX_VAL
        || res.get_datetime() < DATETIME_MIN_VAL)
        || (OB_FAIL(ObTimeConverter::datetime_to_ob_time(res.get_datetime(), NULL, ob_time)))
        || (OB_FAIL(ObTimeConverter::validate_oracle_date(ob_time)))) {
        char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
        int64_t pos = 0;
        ret = OB_OPERATE_OVERFLOW;
        pos = 0;
        databuff_printf(expr_str,
                        OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                        pos,
                        "'(%ld + %ld)'",
                        !SWAP_L_R ? left_i : right_i,
                        !SWAP_L_R ? right_i : left_i);
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DATE", expr_str);
      }
    }
    return ret;
  }
};

//left is NumberType, right is DateTimeType.
int ObExprAdd::add_number_datetime_common(EVAL_FUNC_ARG_DECL, bool number_left)
{
  return number_left
      ? def_arith_eval_func<ObNumberDatetimeAddFunc<false>>(EVAL_FUNC_ARG_LIST)
      : def_arith_eval_func<ObNumberDatetimeAddFunc<true>>(EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_number_datetime_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObNumberDatetimeAddFunc<false>>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_datetime_number_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObNumberDatetimeAddFunc<true>>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

struct ObDatetimeDatetimeAddFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    const int64_t left_i = l.get_datetime();
    const int64_t right_i = r.get_datetime();
    int64_t round_value = left_i + right_i;
    ObTimeConverter::round_datetime(OB_MAX_DATE_PRECISION, round_value);
    res.set_datetime(round_value);
    ObTime ob_time;
    if (OB_UNLIKELY(res.get_datetime() > DATETIME_MAX_VAL
        || res.get_datetime() < DATETIME_MIN_VAL)
        || (OB_FAIL(ObTimeConverter::datetime_to_ob_time(res.get_datetime(), NULL, ob_time)))
        || (OB_FAIL(ObTimeConverter::validate_oracle_date(ob_time)))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%ld + %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DATE", expr_str);
    }
    return ret;
  }
};

//both left and right are datetimeType
int ObExprAdd::add_datetime_datetime(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObDatetimeDatetimeAddFunc>(EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_datetime_datetime_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObDatetimeDatetimeAddFunc>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

template<typename T>
struct ObDecimalIntBatchAddRaw : public ObArithOpRawType<T, T, T>
{
  static void raw_op(T &res, const T &l, const T &r)
  {
    res = l + r;
  }

  static int raw_check(const T &res, const T &l, const T &r)
  {
    return OB_SUCCESS;
  }
};

struct ObDecimalIntBatchAddRawWithCheck : public ObDecimalIntBatchAddRaw<int512_t>
{
  static int raw_check(const int512_t &res, const int512_t &l, const int512_t &r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(res <= wide::ObDecimalIntConstValue::MYSQL_DEC_INT_MIN
                    || res >= wide::ObDecimalIntConstValue::MYSQL_DEC_INT_MAX)) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "");
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DECIMAL", expr_str);
      LOG_WARN("decimal int out of range", K(ret));
    }
    return ret;
  }
};

#define DECINC_ADD_EVAL_FUNC_DECL(TYPE) \
int ObExprAdd::add_decimal##TYPE(EVAL_FUNC_ARG_DECL)      \
{                                            \
  return def_arith_eval_func<ObArithOpWrap<ObDecimalIntBatchAddRaw<TYPE##_t>>>(EVAL_FUNC_ARG_LIST); \
}                                            \
int ObExprAdd::add_decimal##TYPE##_batch(BATCH_EVAL_FUNC_ARG_DECL)      \
{                                            \
  return def_batch_arith_op<ObArithOpWrap<ObDecimalIntBatchAddRaw<TYPE##_t>>>(BATCH_EVAL_FUNC_ARG_LIST); \
}                                             \
int ObExprAdd::add_decimal##TYPE##_vector(VECTOR_EVAL_FUNC_ARG_DECL)      \
{                                            \
  return def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObDecimalIntBatchAddRaw<TYPE##_t>>>(VECTOR_EVAL_FUNC_ARG_LIST); \
}


DECINC_ADD_EVAL_FUNC_DECL(int32)
DECINC_ADD_EVAL_FUNC_DECL(int64)
DECINC_ADD_EVAL_FUNC_DECL(int128)
DECINC_ADD_EVAL_FUNC_DECL(int256)
DECINC_ADD_EVAL_FUNC_DECL(int512)

int ObExprAdd::add_decimalint512_with_check(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObArithOpWrap<ObDecimalIntBatchAddRawWithCheck>>(EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_decimalint512_with_check_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op<ObArithOpWrap<ObDecimalIntBatchAddRawWithCheck>>(
    BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprAdd::add_decimalint512_with_check_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObDecimalIntBatchAddRawWithCheck>>(
    VECTOR_EVAL_FUNC_ARG_LIST);
}

#undef DECINC_ADD_EVAL_FUNC_DECL

template<typename T>
struct ObDecimalOracleAddFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, const int64_t scale,
                 ObNumStackOnceAlloc &alloc) const
  {
    int ret = OB_SUCCESS;
    const T res_int = *reinterpret_cast<const T *>(l.ptr_) + *reinterpret_cast<const T *>(r.ptr_);
    number::ObNumber res_num;
    if (OB_FAIL(wide::to_number(res_int, scale, alloc, res_num))) {
      LOG_WARN("fail to cast decima int to number", K(ret), K(scale));
    } else {
      res.set_number(res_num);
      alloc.free();  // for batch function reuse alloc
    }
    return ret;
  }
};

template<typename T>
struct ObDecimalOracleVectorAddFunc
{
  template <typename ResVector, typename LeftVector, typename RightVector>
  int operator()(ResVector &res_vec, const LeftVector &l_vec, const RightVector &r_vec,
                 const int64_t idx, const int64_t scale, ObNumStackOnceAlloc &alloc) const
  {
    int ret = OB_SUCCESS;
    const T res_int = *reinterpret_cast<const T *>(l_vec.get_payload(idx))
                      + *reinterpret_cast<const T *>(r_vec.get_payload(idx));
    number::ObNumber res_num;
    if (OB_FAIL(wide::to_number(res_int, scale, alloc, res_num))) {
      LOG_WARN("fail to cast decima int to number", K(ret), K(scale));
    } else {
      res_vec.set_number(idx, res_num);
      alloc.free();  // for batch function reuse alloc
    }
    return ret;
  }
};


#define DECINC_ADD_EVAL_FUNC_ORA_DECL(TYPE) \
int ObExprAdd::add_decimal##TYPE##_oracle(EVAL_FUNC_ARG_DECL)      \
{                                            \
  ObNumStackOnceAlloc tmp_alloc;                                \
  const int64_t scale = expr.args_[0]->datum_meta_.scale_;      \
  return def_arith_eval_func<ObDecimalOracleAddFunc<TYPE##_t>>(EVAL_FUNC_ARG_LIST, scale, tmp_alloc); \
}                                            \
int ObExprAdd::add_decimal##TYPE##_oracle_batch(BATCH_EVAL_FUNC_ARG_DECL)      \
{                                            \
  ObNumStackOnceAlloc tmp_alloc;                                \
  const int64_t scale = expr.args_[0]->datum_meta_.scale_;      \
  return def_batch_arith_op_by_datum_func<ObDecimalOracleAddFunc<TYPE##_t>>(BATCH_EVAL_FUNC_ARG_LIST, scale, tmp_alloc); \
}                                            \
int ObExprAdd::add_decimal##TYPE##_oracle_vector(VECTOR_EVAL_FUNC_ARG_DECL)      \
{                                            \
  ObNumStackOnceAlloc tmp_alloc;                                \
  const int64_t scale = expr.args_[0]->datum_meta_.scale_;      \
  return def_fixed_len_vector_arith_op_func<ObDecimalOracleVectorAddFunc<TYPE##_t>,\
                                            ObArithTypedBase<TYPE##_t, TYPE##_t, TYPE##_t>>(VECTOR_EVAL_FUNC_ARG_LIST, scale, tmp_alloc); \
}

DECINC_ADD_EVAL_FUNC_ORA_DECL(int32)
DECINC_ADD_EVAL_FUNC_ORA_DECL(int64)
DECINC_ADD_EVAL_FUNC_ORA_DECL(int128)

#undef DECINC_ADD_EVAL_FUNC_ORA_DECL


}
}
