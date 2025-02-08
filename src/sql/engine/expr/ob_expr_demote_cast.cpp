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

#include "sql/engine/expr/ob_expr_demote_cast.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_type_demotion.h"
#include "lib/oblog/ob_log.h"
#include "lib/number/ob_number_v2.h"
#include "lib/wide_integer/ob_wide_integer.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprDemoteCastBase::ObExprDemoteCastBase(common::ObIAllocator &alloc, ObExprOperatorType type,
                                           const char *name)
  : ObFuncExprOperator(alloc, type, name, 2, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  // args[0] constant value
  // args[1] column type info (include type, accuracy, length and etc.)
  // all args are const expr
}

// Since the second arguments of expression `demote_cast` are special constants representing
// column type information (dest type of cast), we need to decode the correct type and accuracy
// for the constants expression before type deduce. This way, during the actual type deduce process,
// we don't need to pay extra attention to this information of `ObExprResType`.
int ObExprDemoteCastBase::get_column_res_type(const ObExprResType &param_type,
                                              ObExprResType &column_res_type)
{
  int ret = OB_SUCCESS;
  ObDatumMeta column_datum_meta;
  if (!param_type.is_int() && !param_type.get_param().is_int()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast param type is unexpected", K(ret), K(param_type));
  } else if (OB_FAIL(get_column_datum_meta(param_type.get_param().get_int(), column_datum_meta))) {
    LOG_WARN("fail to get column datum meta", K(ret));
  } else {
    column_res_type.set_type(static_cast<ObObjType>(column_datum_meta.type_));
    column_res_type.set_cs_type(static_cast<ObCollationType>(column_datum_meta.cs_type_));
    if (ob_is_string_type(column_res_type.get_type())) {
      column_res_type.set_length(column_datum_meta.length_semantics_);
    } else {
      column_res_type.set_precision(column_datum_meta.precision_);
      column_res_type.set_scale(column_datum_meta.scale_);
    }
  }
  return ret;
}

int ObExprDemoteCastBase::get_column_datum_meta(const int64_t val, ObDatumMeta &column_datum_meta)
{
  int ret = OB_SUCCESS;
  ParseNode parse_node;
  parse_node.value_ = val;
  column_datum_meta.type_ = static_cast<ObObjType>(parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX]);
  column_datum_meta.cs_type_ =
    static_cast<ObCollationType>(parse_node.int16_values_[OB_NODE_CAST_COLL_IDX]);
  if (ob_is_string_type(column_datum_meta.type_)) {
    column_datum_meta.length_semantics_ = parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX];
  } else {
    column_datum_meta.precision_ = parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX];
    column_datum_meta.scale_ = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX];
  }
  return ret;
}

int ObExprDemoteCastBase::set_calc_type_for_const_param(const ObExprResType &column_type,
                                                        ObExprResType &constant_type,
                                                        common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObCastMode cast_mode = type_ctx.get_cast_mode() | CM_DEMOTE_CAST;
  // Cast constant expression to a computable intermediate type result.
  if (ObRelationalExprOperator::can_cmp_without_cast(constant_type, column_type, CO_EQ)) {
    // Constant expr has already been cast, there is no need to set calc_type again
  } else if (lib::is_mysql_mode()) {
    if (ob_is_int_uint_tc(column_type.get_type())
        || ob_is_number_or_decimal_int_tc(column_type.get_type())) {
      constant_type.set_calc_type(ObNumberType);
      // keep precision and scale of number unfixed
      constant_type.set_calc_precision(PRECISION_UNKNOWN_YET);
      constant_type.set_calc_scale(SCALE_UNKNOWN_YET);
    } else if (ob_is_year_tc(column_type.get_type())) {
      if (ob_is_mysql_compact_dates_type(constant_type.get_type())) {
        constant_type.set_calc_type(ObMySQLDateTimeType);
      } else if (ob_is_temporal_type(constant_type.get_type())) {
        constant_type.set_calc_type(ObDateTimeType);
      } else if (ob_is_string_tc(constant_type.get_type())) {
        // string '0000' convert to year is 0, but '0' convert to year is 2000
        constant_type.set_calc_type(ObYearType);
      } else if (ob_is_real_type(constant_type.get_type())
          || ob_is_number_or_decimal_int_tc(constant_type.get_type())) {
        // real and decimal type need to distinguish between -0.1 and 0
        constant_type.set_calc_type(ObDoubleType);
      } else {
        constant_type.set_calc_type(ObIntType);
      }
    } else if (ob_is_date_tc(column_type.get_type())) {
      constant_type.set_calc_type(ObDateTimeType);
    } else if (ob_is_mysql_date_tc(column_type.get_type())) {
      constant_type.set_calc_type(ObMySQLDateTimeType);
    } else if (ob_is_datetime_tc(column_type.get_type())) {
      constant_type.set_calc_type(column_type.get_type());
    } else if (ob_is_time_tc(column_type.get_type())) {
      constant_type.set_calc_type(ObTimeType);
    } else if (ob_is_string_tc(column_type.get_type())) {
      constant_type.set_calc_type(column_type.get_type());
      constant_type.set_calc_collation_type(column_type.get_collation_type());
    }
    if (ob_is_datetime_tc(constant_type.get_calc_type())) {
      // because date/datetime(timestamp) does not support invalid dates and zero in date,
      // the cast mode cannot be set according to the sql mode.
      cast_mode = ((cast_mode & (~CM_ALLOW_INVALID_DATES)) | CM_NO_ZERO_IN_DATE);
    }
  }
  // All intermediate type conversions must be performed using strict mode casts. If an error
  // occurs during the conversion process, the error code should be caught outside the function
  // to ensure that the conversion is lossless.
  type_ctx.set_cast_mode(cast_mode & (~CM_WARN_ON_FAIL));
  return ret;
}

int ObExprDemoteCastBase::demote_cast(const ObExpr &expr, ObEvalCtx &ctx, TypeDemotionRes &res)
{
  int ret = OB_SUCCESS;
  ObDatum *constant = NULL;
  ObDatum *column_info = NULL;
  ObDatumMeta column_datum_meta;
  ObDatumMeta constant_datum_meta = expr.args_[0]->datum_meta_;
  // demote constant value type and get its range placement in the field.
  if (OB_FAIL(expr.args_[0]->eval(ctx, constant))) {
    // ignore strict cast error
    if (OB_DATA_OUT_OF_RANGE == ret || OB_INVALID_DATE_VALUE == ret
        || OB_ERR_DATA_TRUNCATED == ret) {
      ret = OB_SUCCESS;
      res.set_outside();
    } else {
      LOG_WARN("fail to eval const expr", K(ret));
    }
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, column_info))) {
    LOG_WARN("fail to eval column info", K(ret));
  } else if (OB_UNLIKELY(column_info->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the column type is null", K(ret));
  } else if (OB_FAIL(get_column_datum_meta(column_info->get_int(), // restore column meta from args
                                           column_datum_meta))) {
    LOG_WARN("fail to get column datum meta", K(ret));
  } else if (OB_UNLIKELY(constant->is_null())) {
    if (OB_FAIL(demote_null_constant(expr.args_[0], column_datum_meta, res))) {
      LOG_WARN("fail to demote null constant", K(ret));
    }
  } else {
    if (OB_FAIL(demote_field_constant(*constant, constant_datum_meta, column_datum_meta, res))) {
      LOG_WARN("fail to demote field constant", K(ret));
    }
  }
  return ret;
}

int ObExprDemoteCastBase::demote_field_constant(const ObDatum &constant,
                                                const ObDatumMeta &src_meta, /*constant meta*/
                                                const ObDatumMeta &dst_meta, /*field meta*/
                                                TypeDemotionRes &res)
{
  int ret = OB_SUCCESS;
  // Must be consistent with `ObRawExprTypeDemotion::type_can_demote. An error will be thrown
  // during execution stage if an unsupported type is encountered.
  switch (ob_obj_type_class(dst_meta.type_)) {
    case ObIntTC:
    case ObUIntTC: {
      ret = demote_int_field_constant(constant, src_meta, dst_meta, res);
      break;
    }
    case ObNumberTC:
    case ObDecimalIntTC: {
      ret = demote_decimal_field_constant(constant, src_meta, dst_meta, res);
      break;
    }
    case ObYearTC: {
      ret = demote_year_field_constant(constant, src_meta, dst_meta, res);
      break;
    }
    case ObDateTC:
    case ObMySQLDateTC: {
      ret = demote_date_field_constant(constant, src_meta, dst_meta, res);
      break;
    }
    case ObDateTimeTC: {
      ret = demote_datetime_timestamp_field_constant(constant, src_meta, dst_meta, res);
      break;
    }
    case ObTimeTC: {
      ret = demote_time_field_constant(constant, src_meta, dst_meta, res);
      break;
    }
    case ObStringTC: {
      res.val_.set_datum(constant);
      res.set_inside();
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr type", K(ret), K(dst_meta));
    }
  }
  return ret;
}

int ObExprDemoteCastBase::demote_int_field_constant(const ObDatum &constant,
                                                    const ObDatumMeta &src_meta,
                                                    const ObDatumMeta &dst_meta,
                                                    TypeDemotionRes &res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObNumberType != src_meta.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected input type", K(ret), K(src_meta));
  } else {
    number::ObNumber num(constant.get_number());
    if (ob_is_int_tc(dst_meta.type_)) {
      int64_t int_val = 0;
      if (num.is_valid_int64(int_val)) {
        res.val_.set_int(int_val);
        res.set_inside();
      } else {
        res.set_outside();
      }
    } else { // uint type class
      uint64_t uint_val = 0;
      if (num.is_valid_uint64(uint_val)) {
        res.val_.set_uint(uint_val);
        res.set_inside();
      } else {
        res.set_outside();
      }
    }
  }
  return ret;
}

int ObExprDemoteCastBase::demote_decimal_field_constant(const ObDatum &constant,
                                                        const ObDatumMeta &src_meta,
                                                        const ObDatumMeta &dst_meta,
                                                        TypeDemotionRes &res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObNumberType != src_meta.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected input type", K(ret), K(src_meta));
  } else {
    number::ObNumber num(constant.get_number());
    if (ob_is_number_tc(dst_meta.type_)) {
      res.val_.set_number(num);
      res.set_inside();
    } else { // decimal int type
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber round_num;
      if (OB_FAIL(round_num.from(num, tmp_alloc))) {
        LOG_WARN("fail to copy number", K(ret), K(num));
      } else if (OB_FAIL(round_num.round(dst_meta.scale_))) {
        LOG_WARN("fail to round number", K(ret));
      } else if (0 != round_num.compare(num)) {
        // Digits of number after round is lost (truncated)
        res.set_outside();
      } else {
        // convert number to decimal int.
        const ObScale in_scale = round_num.get_scale();
        const int16_t out_scale = dst_meta.scale_;
        const ObPrecision out_prec = dst_meta.precision_;
        ObDecimalIntBuilder tmp_alloc;
        ObDecimalInt *decint = nullptr;
        int32_t int_bytes = 0;
        int32_t out_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
        if (OB_FAIL(wide::from_number(round_num, tmp_alloc, in_scale, decint, int_bytes))) {
          LOG_WARN("cast number to decimal int failed", K(ret), K(round_num));
        } else if (OB_ISNULL(decint)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("decimal int result is null", K(ret));
        } else if (ObDatumCast::need_scale_decimalint(in_scale, int_bytes, out_scale, out_bytes)) {
          ObDecimalIntBuilder res_val;
          // For check range bound.
          ObDecimalIntBuilder scaled_val;
          ObDecimalIntBuilder max_v;
          ObDecimalIntBuilder min_v;
          max_v.from(wide::ObDecimalIntConstValue::get_max_upper(out_prec), out_bytes);
          min_v.from(wide::ObDecimalIntConstValue::get_min_lower(out_prec), out_bytes);
          int cmp_min = 0;
          int cmp_max = 0;
          if (OB_FAIL(wide::common_scale_decimalint(decint, int_bytes, in_scale, out_scale,
                                                    scaled_val))) {
            LOG_WARN("fail to scale decimal int value", K(ret), K(in_scale), K(out_scale));
          } else if (OB_FAIL(wide::compare(scaled_val, min_v, cmp_min))) {
            LOG_WARN("compare failed", K(ret));
          } else if (OB_FAIL(wide::compare(scaled_val, max_v, cmp_max))) {
            LOG_WARN("compare failed", K(ret));
          } else if (cmp_min <= 0 || cmp_max >= 0 || in_scale > out_scale) {
            // out of range bound
            res.set_outside();
          } else if (OB_FAIL(ObDatumCast::align_decint_precision_unsafe(
              scaled_val.get_decimal_int(), scaled_val.get_int_bytes(), out_bytes, res_val))) {
            LOG_WARN("fail to align decimal int precision", K(ret));
          } else {
            res.val_.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
            res.set_inside();
          }
        } else {
          res.val_.set_decimal_int(decint, int_bytes);
          res.set_inside();
        }
      }
    }
  }
  return ret;
}

int ObExprDemoteCastBase::demote_year_field_constant(const ObDatum &constant,
                                                     const ObDatumMeta &src_meta,
                                                     const ObDatumMeta &dst_meta,
                                                     TypeDemotionRes &res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObIntType != src_meta.type_
      && ObDateTimeType != src_meta.type_
      && ObYearType != src_meta.type_
      && ObDoubleType != src_meta.type_
      && ObMySQLDateTimeType != src_meta.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected input type", K(ret), K(src_meta));
  } else {
    uint8_t year_val = 0;
    if (ObIntType == src_meta.type_) {
      ret = ObTimeConverter::int_to_year(constant.get_int(), year_val);
    } else if (ObDateTimeType == src_meta.type_) {
      ret = ObTimeConverter::datetime_to_year(constant.get_datetime(), NULL /*tz_info*/, year_val);
    } else if (ObYearType == src_meta.type_) {
      year_val = constant.get_year();
    } else if (ObDoubleType == src_meta.type_) {
      const double real_val = constant.get_double();
      if (real_val < 0.0 || real_val >= static_cast<double>(LLONG_MAX)) {
        ret = OB_DATA_OUT_OF_RANGE;
      } else {
        ret = ObTimeConverter::int_to_year(static_cast<int64_t>(rint(real_val)), year_val);
      }
    } else if (ObMySQLDateTimeType == src_meta.type_) {
      ret = ObTimeConverter::mdatetime_to_year(constant.get_mysql_datetime(), year_val);
    }
    if (OB_FAIL(ret)) {
      ret = OB_SUCCESS;
      res.set_outside();
    } else {
      res.val_.set_year(year_val);
      res.set_inside();
    }
  }
  return ret;
}

int ObExprDemoteCastBase::demote_date_field_constant(const ObDatum &constant,
                                                     const ObDatumMeta &src_meta,
                                                     const ObDatumMeta &dst_meta,
                                                     TypeDemotionRes &res)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  if (OB_UNLIKELY(ObDateTimeType != src_meta.type_ && ObMySQLDateTimeType != src_meta.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected input type", K(ret), K(src_meta));
  } else if (ObDateTimeType == src_meta.type_ &&
      OB_FAIL(ObTimeConverter::datetime_to_ob_time(constant.get_datetime(), NULL /*tz_info*/,
                                                   ob_time))) {
    res.set_outside();
  } else if (ObMySQLDateTimeType == src_meta.type_ &&
      OB_FAIL(ObTimeConverter::mdatetime_to_ob_time<false>(constant.get_datetime(), ob_time))) {
    res.set_outside();
  } else if (0 != (ob_time.parts_[DT_HOUR] | ob_time.parts_[DT_MIN] |
                    ob_time.parts_[DT_SEC] | ob_time.parts_[DT_USEC])) {
    // Non-zero field exists that cannot be represented by date, set range placement to outside
    res.set_outside();
  } else if (ObDateType == dst_meta.type_) {
    res.val_.set_date(ob_time.parts_[DT_DATE]);
    res.set_inside();
  } else if (ObMySQLDateType == dst_meta.type_) {
    ObMySQLDate mdate;
    if (OB_FAIL(ObTimeConverter::mdatetime_to_mdate(constant.get_datetime(), mdate))) {
      res.set_outside();
    } else {
      res.val_.set_mysql_date(mdate);
      res.set_inside();
    }
  }
  return ret;
}

int ObExprDemoteCastBase::demote_datetime_timestamp_field_constant(const ObDatum &constant,
                                                                   const ObDatumMeta &src_meta,
                                                                   const ObDatumMeta &dst_meta,
                                                                   TypeDemotionRes &res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ob_is_datetime_tc(src_meta.type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected input type", K(ret), K(src_meta));
  } else {
    res.val_.set_datetime(constant.get_datetime());
    res.set_inside();
  }
  if (ObTimestampType == src_meta.type_) {
    const int64_t ts_value = constant.get_timestamp();
    if (OB_UNLIKELY(ObTimeConverter::ZERO_DATETIME != ts_value &&
        (ts_value < MYSQL_TIMESTAMP_MIN_VAL || ts_value > MYSQL_TIMESTAMP_MAX_VAL))) {
      res.set_outside();
    }
  }
  return ret;
}

int ObExprDemoteCastBase::demote_time_field_constant(const ObDatum &constant,
                                                     const ObDatumMeta &src_meta,
                                                     const ObDatumMeta &dst_meta,
                                                     TypeDemotionRes &res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObTimeType != src_meta.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected input type", K(ret), K(src_meta));
  } else {
    res.val_.set_time(constant.get_time());
    res.set_inside();
  }
  return ret;
}

int ObExprDemoteCastBase::demote_null_constant(const ObExpr *constant_expr,
                                               const ObDatumMeta &column_meta,
                                               TypeDemotionRes &res)
{
  int ret = OB_SUCCESS;
  // Some temporary type conversions will convert non-null datum to null. In this case, we fall back
  // to non-demoted comparison for scenarios that require constraints, and directly return the null
  // result for scenarios that do not require constraints and mark it's range placement as inside.
  const ObExpr *real_expr = NULL;
  if (OB_FAIL(ObExprUtil::get_real_expr_without_cast(constant_expr, real_expr))) {
    LOG_WARN("fail to get real expr without case", K(ret));
  } else if (ObRawExprTypeDemotion::need_constraint(real_expr->datum_meta_.type_,
                                                    column_meta.type_)) {
    res.set_outside();
  } else {
    res.val_.set_null();
    res.set_inside();
  }
  return ret;
}

ObExprDemoteCast::ObExprDemoteCast(common::ObIAllocator &alloc)
  : ObExprDemoteCastBase(alloc, T_FUN_SYS_DEMOTE_CAST, N_DEMOTE_CAST)
{
}

int ObExprDemoteCast::calc_result_type2(ObExprResType &type,
                                        ObExprResType &type1,
                                        ObExprResType &type2,
                                        common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  // The result type of the demote_cast expression must be directly comparable to the column type.
  // It essentially acts as a cast expression. After applying the demote_cast expression,
  // no additional casts should be required for the comparison.
  ObExprResType column_res_type;
  if (OB_FAIL(get_column_res_type(type2, column_res_type))) {
    LOG_WARN("fail to get column res type", K(ret), K(type2));
  } else if (ob_is_int_uint_tc(column_res_type.get_type())) {
    if (ob_is_int_tc(column_res_type.get_type())) {
      type.set_int();
    } else {
      type.set_uint64();
    }
  } else {
    type.set_type(column_res_type.get_type());
  }
  if (OB_SUCC(ret)) {
    // keep accuracy same as the column
    if (ob_is_string_tc(column_res_type.get_type())) {
      type.set_length(column_res_type.get_length());
      type.set_collation_type(column_res_type.get_collation_type());
      type.set_collation_level(CS_LEVEL_IMPLICIT);
    } else {
      type.set_accuracy(column_res_type.get_accuracy());
    }
    if (OB_FAIL(set_calc_type_for_const_param(column_res_type, type1, type_ctx))) {
      LOG_WARN("fail to set calc type for const param", K(ret));
    }
  }
  return ret;
}

int ObExprDemoteCast::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  for (int64_t i = 0; i < rt_expr.arg_cnt_ && OB_SUCC(ret); ++i) {
    if (OB_ISNULL(rt_expr.args_[i])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("the args is null.", K(ret), K(rt_expr), K(i));
    } else if (1 == i && ObIntType != rt_expr.args_[i]->datum_meta_.type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the args type is unexpected", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = eval_demoted_val;
  }
  return ret;
}

int ObExprDemoteCast::eval_demoted_val(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &demoted_val)
{
  int ret = OB_SUCCESS;
  TypeDemotionRes res(demoted_val);
  if (OB_FAIL(demote_cast(expr, ctx, res))) {
    LOG_WARN("fail to demote cast", K(ret), K(res));
  } else if (OB_UNLIKELY(res.is_outside())) {
    // Expected error, in order to prevent the calculable expr from getting a result
    ret = OB_DATA_OUT_OF_RANGE;
  }
  return ret;
}

ObExprRangePlacement::ObExprRangePlacement(common::ObIAllocator &alloc)
  : ObExprDemoteCastBase(alloc, T_FUN_SYS_RANGE_PLACEMENT, N_RANGE_PLACEMENT)
{
}

int ObExprRangePlacement::calc_result_type2(ObExprResType &type,
                                            ObExprResType &type1,
                                            ObExprResType &type2,
                                            common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type.set_int32();
  type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObInt32Type]);
  ObExprResType column_res_type;
  if (OB_FAIL(get_column_res_type(type2, column_res_type))) {
    LOG_WARN("fail to get column res type", K(ret), K(type2));
  } else if (OB_FAIL(set_calc_type_for_const_param(column_res_type, type1, type_ctx))) {
    LOG_WARN("fail to set calc type for const param", K(ret));
  }
  return ret;
}

int ObExprRangePlacement::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  for (int64_t i = 0; i < rt_expr.arg_cnt_ && OB_SUCC(ret); ++i) {
    if (OB_ISNULL(rt_expr.args_[i])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("the args is null.", K(ret), K(rt_expr), K(i));
    } else if (1 == i && ObIntType != rt_expr.args_[i]->datum_meta_.type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the args type is unexpected", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = eval_range_placement;
  }
  return ret;
}

int ObExprRangePlacement::eval_range_placement(const ObExpr &expr, ObEvalCtx &ctx,
                                               ObDatum &range_placement)
{
  int ret = OB_SUCCESS;
  ObDatum demoted_val;
  // Use temporary memory to store the demoted value. Ensure that this buffer can accommodate
  // the maximum value representable by the largest data type, currently it is decimal_int_512.
  char value_buf[MAX_TYPE_BUF_SIZE];
  demoted_val.ptr_ = value_buf;
  demoted_val.pack_ = 0;
  TypeDemotionRes res(demoted_val);
  if (OB_FAIL(demote_cast(expr, ctx, res))) {
    LOG_WARN("fail to demote cast", K(ret), K(res));
  } else {
    // set range placement value as datum result
    range_placement.set_int32(static_cast<int32_t>(res.rp_));
  }
  return ret;
}

}
}