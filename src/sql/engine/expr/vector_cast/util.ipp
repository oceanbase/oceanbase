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

#define DEF_RANGE_CHECKER(vec_tc)                                                                  \
  template <typename Vector>                                                                       \
  struct ValueRangeChecker<vec_tc, Vector>                                                         \
  {                                                                                                \
    static const bool defined_ = true;                                                             \
    inline static int check(const ObExpr &expr, ObEvalCtx &ctx, const uint64_t cast_mode,          \
                            Vector *res_vec, const int64_t batch_idx, int &warning);               \
  };                                                                                               \
  template <typename Vector>                                                                       \
  inline int ValueRangeChecker<vec_tc, Vector>::check(const ObExpr &expr, ObEvalCtx &ctx,          \
                                                      const uint64_t cast_mode, Vector *res_vec,   \
                                                      const int64_t batch_idx, int &warning)

namespace oceanbase
{
using namespace common;
namespace sql
{
template<VecValueTypeClass vec_tc, typename Vector>
struct DecintRangeChecker
{
  using ResType = RTCType<vec_tc>;
  static int check(const ObExpr &expr, ObEvalCtx &ctx, const uint64_t cast_mode, Vector *res_vec,
                   const int64_t batch_idx, int &warning)
  {
    ObPrecision precision = expr.datum_meta_.precision_;
    ObScale scale = expr.datum_meta_.scale_;
    bool is_finish = false;
    int ret = OB_SUCCESS;
    int &cast_ret = CM_IS_ERROR_ON_FAIL(cast_mode) ? ret : warning;
    if (lib::is_oracle_mode()) {
      if (OB_UNLIKELY(precision > OB_MAX_NUMBER_PRECISION)
          || OB_UNLIKELY(scale < number::ObNumber::MIN_SCALE
                         || scale > number::ObNumber::MAX_SCALE)) {
        ret = OB_INVALID_ARGUMENT;
        SQL_LOG(WARN, "invalid precision and scale", K(ret), K(precision), K(scale));
      } else if (precision == PRECISION_UNKNOWN_YET) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected precision", K(ret), K(scale));
      }
    } else if (OB_UNLIKELY(precision < OB_MIN_DECIMAL_PRECISION
                           || precision > number::ObNumber::MAX_PRECISION)
               || OB_UNLIKELY(scale < 0 || scale > number::ObNumber::MAX_SCALE)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_LOG(WARN, "invalid precision and scale", K(ret), K(precision), K(scale));
    } else if (OB_UNLIKELY(precision < scale)) {
      ret = OB_ERR_M_BIGGER_THAN_D;
      SQL_LOG(WARN, "invalid precision and scale", K(ret), K(precision), K(scale));
    }
    if (OB_SUCC(ret) && !is_finish) {
      // int16_t delta_scale = scale;
      // if (lib::is_oracle_mode()) {
      //   delta_scale += wide::ObDecimalIntConstValue::MAX_ORACLE_SCALE_DELTA;
      // }
      const ObDecimalInt *min_decint = nullptr, *max_decint = nullptr;
      int32_t int_bytes2 = 0;
      if (lib::is_mysql_mode()) {
        min_decint = wide::ObDecimalIntConstValue::get_min_value(precision);
        max_decint = wide::ObDecimalIntConstValue::get_max_value(precision);
        // int_bytes2 = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
      } else {
        min_decint = wide::ObDecimalIntConstValue::get_min_value(precision);
        max_decint = wide::ObDecimalIntConstValue::get_max_value(precision);
        // int_bytes2 = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
      }
      const char *dec_val = res_vec->get_payload(batch_idx);
      int cmp_min = *reinterpret_cast<const ResType *>(dec_val) >= *reinterpret_cast<const ResType *>(min_decint) ? 1 : -1;
      int cmp_max = *reinterpret_cast<const ResType *>(dec_val) >= *reinterpret_cast<const ResType *>(max_decint) ? 1 : -1;
      if (cmp_min >= 0 && cmp_max <= 0) {
        // do nothing
      } else if (lib::is_oracle_mode()) {
        cast_ret = OB_ERR_VALUE_LARGER_THAN_ALLOWED;
      } else if (cmp_min < 0) { // res < min(p, s)
        cast_ret = OB_DATA_OUT_OF_RANGE;
        res_vec->set_payload(batch_idx, min_decint, sizeof(ResType));
      } else if (cmp_max > 0) { // res > min(p, s)
        cast_ret = OB_DATA_OUT_OF_RANGE;
        res_vec->set_payload(batch_idx, max_decint, sizeof(ResType));
      }
    }
    return ret;
  }
};

DEF_RANGE_CHECKER(VEC_TC_DEC_INT32)
{
  return DecintRangeChecker<VEC_TC_DEC_INT32, Vector>::check(expr, ctx, cast_mode, res_vec,
                                                             batch_idx, warning);
}

DEF_RANGE_CHECKER(VEC_TC_DEC_INT64)
{
  return DecintRangeChecker<VEC_TC_DEC_INT64, Vector>::check(expr, ctx, cast_mode, res_vec,
                                                             batch_idx, warning);
}

DEF_RANGE_CHECKER(VEC_TC_DEC_INT128)
{
  return DecintRangeChecker<VEC_TC_DEC_INT128, Vector>::check(expr, ctx, cast_mode, res_vec,
                                                             batch_idx, warning);
}

DEF_RANGE_CHECKER(VEC_TC_DEC_INT256)
{
  return DecintRangeChecker<VEC_TC_DEC_INT256, Vector>::check(expr, ctx, cast_mode, res_vec,
                                                             batch_idx, warning);
}

DEF_RANGE_CHECKER(VEC_TC_DEC_INT512)
{
  return DecintRangeChecker<VEC_TC_DEC_INT512, Vector>::check(expr, ctx, cast_mode, res_vec,
                                                              batch_idx, warning);
}

DEF_RANGE_CHECKER(VEC_TC_NUMBER)
{
  int ret = OB_SUCCESS;
  int &cast_ret = CM_IS_ERROR_ON_FAIL(cast_mode) ? ret : warning;
  ObAccuracy accuracy;
  ObObjType out_type = ObMaxType;
  ObCollationType cs_type = expr.datum_meta_.cs_type_;
  const char *payload = res_vec->get_payload(batch_idx);
  ObPrecision precision = expr.datum_meta_.precision_;
  ObScale scale = expr.datum_meta_.scale_;
  const number::ObNumber *min_check_num = NULL;
  const number::ObNumber *max_check_num = NULL;
  const number::ObNumber *min_num_mysql = NULL;
  const number::ObNumber *max_num_mysql = NULL;
  const number::ObNumber in_val(*reinterpret_cast<const number::ObCompactNumber *>(payload));

  const static int64_t num_alloc_used_times = 2; // out val alloc will be used twice
  ObNumStackAllocator<num_alloc_used_times> out_val_alloc;
  number::ObNumber out_val;
  bool is_finish = false;
  if (expr.datum_meta_.type_ == ObNumberFloatType) {
    if (OB_MIN_NUMBER_FLOAT_PRECISION <= precision && precision <= OB_MAX_NUMBER_FLOAT_PRECISION) {
      const int64_t number_precision =
        static_cast<int64_t>(floor(precision * OB_PRECISION_BINARY_TO_DECIMAL_FACTOR));
      if (OB_FAIL(out_val.from(in_val, out_val_alloc))) {
      } else if (OB_FAIL(out_val.round_precision(number_precision))) {
      } else if (CM_IS_ERROR_ON_SCALE_OVER(cast_mode) && in_val.compare(out_val) != 0) {
        ret = OB_OPERATE_OVERFLOW;
        SQL_LOG(WARN, "input value is out of range.", K(scale), K(in_val));
      } else {
        res_vec->set_number(batch_idx, out_val);
        is_finish = true;
      }
      SQL_LOG(DEBUG, "finish round_precision", K(in_val), K(number_precision), K(precision));
    } else if (PRECISION_UNKNOWN_YET == precision) {
      res_vec->set_number(batch_idx, in_val);
      is_finish = true;
    } else {
      ret = OB_INVALID_ARGUMENT;
      SQL_LOG(WARN, "invalid arguments", K(ret), K(precision), K(scale));
    }
  } else if (lib::is_oracle_mode()) {
    if (OB_MAX_NUMBER_PRECISION >= precision && precision >= OB_MIN_NUMBER_PRECISION
        && number::ObNumber::MAX_SCALE >= scale && scale >= number::ObNumber::MIN_SCALE) {
      min_check_num =
        &(ObNumberConstValue::ORACLE_CHECK_MIN[precision]
                                              [scale + ObNumberConstValue::MAX_ORACLE_SCALE_DELTA]);
      max_check_num =
        &(ObNumberConstValue::ORACLE_CHECK_MAX[precision]
                                              [scale + ObNumberConstValue::MAX_ORACLE_SCALE_DELTA]);
    } else if (ORA_NUMBER_SCALE_UNKNOWN_YET == scale && PRECISION_UNKNOWN_YET == precision) {
      res_vec->set_number(batch_idx, in_val);
      is_finish = true;
    } else if (precision == PRECISION_UNKNOWN_YET && number::ObNumber::MAX_SCALE >= scale
               && scale >= number::ObNumber::MIN_SCALE) {
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber num;
      if (OB_FAIL(num.from(in_val, tmp_alloc))) {
      } else if (OB_FAIL(num.round(scale))) {
      } else if (CM_IS_ERROR_ON_SCALE_OVER(cast_mode) && in_val.compare(num) != 0) {
        ret = OB_OPERATE_OVERFLOW;
        SQL_LOG(WARN, "input value is out of range.", K(scale), K(in_val));
      } else {
        res_vec->set_number(batch_idx, num);
        is_finish = true;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      SQL_LOG(WARN, "invalid arguments", K(ret), K(precision), K(scale));
    }
  } else {
    if (OB_UNLIKELY(precision < scale)) {
      ret = OB_ERR_M_BIGGER_THAN_D;
    } else if (number::ObNumber::MAX_PRECISION >= precision && precision >= OB_MIN_DECIMAL_PRECISION
               && number::ObNumber::MAX_SCALE >= scale && scale >= 0) {
      min_check_num = &(ObNumberConstValue::MYSQL_CHECK_MIN[precision][scale]);
      max_check_num = &(ObNumberConstValue::MYSQL_CHECK_MAX[precision][scale]);
      min_num_mysql = &(ObNumberConstValue::MYSQL_MIN[precision][scale]);
      max_num_mysql = &(ObNumberConstValue::MYSQL_MAX[precision][scale]);
    } else {
      ret = OB_INVALID_ARGUMENT;
      SQL_LOG(WARN, "invalid arguments", K(ret), K(precision), K(scale));
    }
  }
  if (OB_SUCC(ret) && !is_finish) {
    if (OB_ISNULL(min_check_num) || OB_ISNULL(max_check_num)
        || (!lib::is_oracle_mode() && (OB_ISNULL(min_num_mysql) || OB_ISNULL(max_num_mysql)))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "min_num or max_num is null", K(ret), KPC(min_check_num), KPC(max_check_num));
    } else if (in_val <= *min_check_num) {
      if (lib::is_oracle_mode()) {
        cast_ret = OB_ERR_VALUE_LARGER_THAN_ALLOWED;
      } else {
        cast_ret = OB_DATA_OUT_OF_RANGE;
        res_vec->set_number(batch_idx, *min_num_mysql);
      }
      is_finish = true;
    } else if (in_val >= *max_check_num) {
      if (lib::is_oracle_mode()) {
        cast_ret = OB_ERR_VALUE_LARGER_THAN_ALLOWED;
      } else {
        cast_ret = OB_DATA_OUT_OF_RANGE;
        res_vec->set_number(batch_idx, *max_num_mysql);
      }
      is_finish = true;
    } else {
      if (OB_FAIL(out_val.from(in_val, out_val_alloc))) {
        SQL_LOG(WARN, "out_val.from failed", K(ret), K(in_val));
      } else if (OB_FAIL(out_val.round(scale))) {
        SQL_LOG(WARN, "round failed", K(ret));
      } else if (CM_IS_ERROR_ON_SCALE_OVER(cast_mode) && in_val.compare(out_val) != 0) {
        ret = OB_OPERATE_OVERFLOW;
        SQL_LOG(WARN, "input value is out of range", K(scale), K(in_val));
      } else {
        res_vec->set_number(batch_idx, out_val);
        is_finish = true;
      }
    }
  }
  if (OB_SUCC(ret) && !is_finish) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "unexpected situation", K(ret));
  }
  SQL_LOG(DEBUG, "number_range_check done", K(ret), K(is_finish), K(accuracy), K(in_val),
          K(out_val), KPC(min_check_num), KPC(max_check_num));
  return ret;
}

// TODO: add other range checkers

} // end sql
} // end oceanbase

#undef DEF_RANGE_CHECKER
