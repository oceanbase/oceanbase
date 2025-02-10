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
#include "sql/engine/expr/ob_datum_cast.h"

#include "sql/engine/ob_exec_context.h"
#include "share/object/ob_obj_cast_util.h"


#define DEF_BATCH_RANGE_CHECKER(vec_tc)                                                    \
  template <typename Vector>                                                               \
  struct BatchValueRangeChecker<vec_tc, Vector>                                            \
  {                                                                                        \
    static const bool defined_ = true;                                                     \
    inline static int check(CAST_CHECKER_ARG_DECL);                                 \
  };                                                                                       \
  template <typename Vector>                                                               \
  inline int BatchValueRangeChecker<vec_tc, Vector>::check(CAST_CHECKER_ARG_DECL)

namespace oceanbase
{
using namespace common;
namespace sql
{
template<VecValueTypeClass vec_tc, typename Vector> struct FloatRangeChecker;
template<VecValueTypeClass vec_tc, typename Vector> struct DecintRangeChecker;
template<VecValueTypeClass vec_tc, typename Vector> struct IntegerRangeChecker;
template<VecValueTypeClass vec_tc, typename Vector> struct NumberRangeChecker;
template<VecValueTypeClass vec_tc, typename Vector> struct DateRangeChecker;
template<VecValueTypeClass vec_tc, typename Vector> struct DateTimeRangeChecker;
template<VecValueTypeClass vec_tc, typename Vector> struct StringRangeChecker;


DEF_BATCH_RANGE_CHECKER(VEC_TC_DEC_INT32) {
  return DecintRangeChecker<VEC_TC_DEC_INT32, Vector>::check(CAST_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_DEC_INT64) {
  return DecintRangeChecker<VEC_TC_DEC_INT64, Vector>::check(CAST_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_DEC_INT128) {
  return DecintRangeChecker<VEC_TC_DEC_INT128, Vector>::check(CAST_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_DEC_INT256) {
  return DecintRangeChecker<VEC_TC_DEC_INT256, Vector>::check(CAST_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_DEC_INT512) {
  return DecintRangeChecker<VEC_TC_DEC_INT512, Vector>::check(CAST_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_FLOAT) {
  return FloatRangeChecker<VEC_TC_FLOAT, Vector>::check(CAST_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_DOUBLE) {
  return FloatRangeChecker<VEC_TC_DOUBLE, Vector>::check(CAST_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_INTEGER) {
  return IntegerRangeChecker<VEC_TC_INTEGER, Vector>::check(CAST_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_UINTEGER) {
  return IntegerRangeChecker<VEC_TC_UINTEGER, Vector>::check(CAST_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_NUMBER) {
  return NumberRangeChecker<VEC_TC_NUMBER, Vector>::check(CAST_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_DATE) {
  return DateRangeChecker<VEC_TC_DATE, Vector>::check(CAST_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_DATETIME) {
  return DateTimeRangeChecker<VEC_TC_DATETIME, Vector>::check(CAST_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_STRING) {
  return StringRangeChecker<VEC_TC_STRING, Vector>::check(CAST_CHECKER_ARG);
}

template<typename Calc, typename Vector>
OB_INLINE static int batch_cast_check(Calc calc,
                                      const ObExpr &expr,
                                      Vector *res_vec,
                                      const ObBitVector &skip,
                                      const EvalBound &bound) {
  int ret = OB_SUCCESS;
  if (bound.get_all_rows_active() && !res_vec->has_null()) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      ret = calc(expr, idx);
    }
  } else {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || res_vec->is_null(idx)) { continue; }
      ret = calc(expr, idx);
    }
  }
  return ret;
}

// TODO: add other range checkers
// 与CAST_FAIL类似，但是上面的宏会将expr.extra_作为cast_mode,这样使用时少写一个参数
#define CAST_FAIL_CM(stmt, cast_mode) \
  (OB_UNLIKELY((OB_SUCCESS != (ret = get_cast_ret_wrap(cast_mode, (stmt), warning)))))

template<VecValueTypeClass vec_tc, typename Vector>
struct DecintRangeChecker
{
  using ResType = RTCType<vec_tc>;
  static int check(CAST_CHECKER_ARG_DECL)
  {
    int ret = OB_SUCCESS;
    warning = ret;
    const uint64_t cast_mode = expr.extra_;
    ObScale scale = expr.datum_meta_.scale_;
    ObPrecision precision = expr.datum_meta_.precision_;
    const ObDecimalInt *min_decint = nullptr, *max_decint = nullptr;
    min_decint = wide::ObDecimalIntConstValue::get_min_value(precision);
    max_decint = wide::ObDecimalIntConstValue::get_max_value(precision);
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
    if (OB_SUCC(ret)) {
      Vector *res_vec = static_cast<Vector *>(expr.get_vector(ctx));
      const ResType min_v = *reinterpret_cast<const ResType *>(min_decint);
      const ResType max_v = *reinterpret_cast<const ResType *>(max_decint);

      class DecimalintCheck {
      public:
        DecimalintCheck(Vector* res_vec, const ResType min_v, const ResType max_v,
                           const ObDecimalInt *min_decint, const ObDecimalInt *max_decint)
            : res_vec_(res_vec), min_v_(min_v), max_v_(max_v), min_decint_(min_decint), max_decint_(max_decint) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS, warning = OB_SUCCESS;
          int &cast_ret = CM_IS_ERROR_ON_FAIL(expr.extra_) ? ret : warning;
          const ResType val = *reinterpret_cast<const ResType *>(res_vec_->get_payload(idx));
          int cmp_min = val >= min_v_ ? 1 : -1;
          int cmp_max = val >= max_v_ ? 1 : -1;
          if (cmp_min >= 0 && cmp_max <= 0) {
            // do nothing
          } else if (lib::is_oracle_mode()) {
            cast_ret = OB_ERR_VALUE_LARGER_THAN_ALLOWED;
          } else if (cmp_min < 0) { // res < min(p, s)
            cast_ret = OB_DATA_OUT_OF_RANGE;
            res_vec_->set_payload(idx, min_decint_, sizeof(ResType));
          } else if (cmp_max > 0) { // res > min(p, s)
            cast_ret = OB_DATA_OUT_OF_RANGE;
            res_vec_->set_payload(idx, max_decint_, sizeof(ResType));
          }
          return ret;
        }
      private:
        Vector *res_vec_;
        const ResType min_v_;
        const ResType max_v_;
        const ObDecimalInt *min_decint_;
        const ObDecimalInt *max_decint_;
      };

      DecimalintCheck cast_fn(res_vec, min_v, max_v, min_decint, max_decint);
      if (OB_FAIL(batch_cast_check(cast_fn, expr, res_vec, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret));
      }
    }
    return ret;
  }
};

template<VecValueTypeClass vec_tc, typename Vector>
struct FloatRangeChecker
{
  using ResType = RTCType<vec_tc>;
  static int check(CAST_CHECKER_ARG_DECL)
  {
    int ret = OB_SUCCESS;
    ObObjType out_type = ObMaxType;
    ObObjType type = expr.datum_meta_.type_;
    ObObjTypeClass type_class = ob_obj_type_class(type);
    ObAccuracy accuracy;
    if (OB_FAIL(get_accuracy_from_parse_node(expr, ctx, accuracy, out_type))) {
      SQL_LOG(WARN, "get accuracy failed", K(ret));
    } else if (OB_UNLIKELY(ObFloatTC != type_class && ObDoubleTC != type_class)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_LOG(WARN, "obj type is invalid, must be float/double tc", K(ret), K(type), K(type_class));
    } else {
      const ObPrecision precision = accuracy.get_precision();
      const ObScale scale = accuracy.get_scale();
      bool need_real_range_check = OB_LIKELY(precision > 0) && OB_LIKELY(scale >= 0)
                                   && OB_LIKELY(precision >= scale);
      ResType integer_part = static_cast<ResType>(pow(10.0, static_cast<double>(precision - scale)));
      ResType decimal_part = static_cast<ResType>(pow(10.0, static_cast<double>(scale)));
      ResType max_value = static_cast<ResType>(integer_part - 1 / decimal_part);
      ResType min_value = static_cast<ResType>(-max_value);
      Vector *res_vec = static_cast<Vector *>(expr.get_vector(ctx));

      FloatDoubleCheck cast_fn(res_vec, integer_part, decimal_part,
                                   max_value, min_value, need_real_range_check);
      if (OB_FAIL(batch_cast_check(cast_fn, expr, res_vec, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret));
      }
    }
    return ret;
  }
  class FloatDoubleCheck {
  public:
    FloatDoubleCheck(Vector* res_vec, ResType integer_part, ResType decimal_part,
                         ResType max_value, ResType min_value, bool need_real_range_check)
        : res_vec_(res_vec), integer_part_(integer_part), decimal_part_(decimal_part),
          max_value_(max_value), min_value_(min_value), need_real_range_check_(need_real_range_check) {}

    OB_INLINE int operator() (const ObExpr &expr, int idx)
    {
      int ret = OB_SUCCESS, warning = OB_SUCCESS;
      const char *payload = res_vec_->get_payload(idx);
      ResType in_val = *(reinterpret_cast<const ResType*>(payload));
      ResType out_val = in_val;
      ObObjType type = expr.datum_meta_.type_;
      if (ob_is_unsigned_type(type) && in_val < 0.0) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unsiged type with negative value", K(ret), K(type), K(in_val));
      } else if (lib::is_oracle_mode() && 0.0 == in_val) {
        res_vec_->set_payload(idx, &in_val, sizeof(ResType));
      } else if (lib::is_oracle_mode() && isnan(in_val)) {
        // overwrite -NAN to NAN, OB only store NAN
        float nan_val = NAN;
        res_vec_->set_payload(idx, &nan_val, sizeof(ResType));
      } else {
        if (need_real_range_check_) {
          if (OB_FAIL(numeric_range_check(out_val, min_value_, max_value_, out_val))) {
          } else {
            out_val = static_cast<ResType>(rint((out_val -
                                            floor(static_cast<double>(out_val)))* decimal_part_) /
                                            decimal_part_ + floor(static_cast<double>(out_val)));
          }
        }
        if (CAST_FAIL_CM(ret, expr.extra_)) {
        // if (OB_UNLIKELY((OB_SUCCESS != (ret = get_cast_ret(cast_mode, real_range_check(accuracy, out_val), warning))))) {
          SQL_LOG(WARN, "real_range_check failed", K(ret));
        } else if (in_val != out_val) {
          res_vec_->set_payload(idx, &out_val, sizeof(ResType));
        } else {
          res_vec_->set_payload(idx, &in_val, sizeof(ResType));
        }
      }
      return ret;
    }
  private:
    Vector *res_vec_;
    ResType integer_part_;
    ResType decimal_part_;
    ResType max_value_;
    ResType min_value_;
    bool need_real_range_check_;
  };
};

template<VecValueTypeClass vec_tc, typename Vector>
struct NumberRangeChecker
{
  using ResType = RTCType<vec_tc>;
  static int check(CAST_CHECKER_ARG_DECL)
  {
    int ret = OB_SUCCESS;
    const uint64_t cast_mode = expr.extra_;
    int &cast_ret = CM_IS_ERROR_ON_FAIL(cast_mode) ? ret : warning;
    ObAccuracy accuracy;
    ObObjType out_type = ObMaxType;
    ObCollationType cs_type = expr.datum_meta_.cs_type_;
    ObPrecision precision = expr.datum_meta_.precision_;
    ObScale scale = expr.datum_meta_.scale_;
    Vector *res_vec = static_cast<Vector *>(expr.get_vector(ctx));
    const number::ObNumber *min_check_num = NULL;
    const number::ObNumber *max_check_num = NULL;
    const number::ObNumber *min_num_mysql = NULL;
    const number::ObNumber *max_num_mysql = NULL;
    const int64_t number_precision =
          static_cast<int64_t>(floor(precision * OB_PRECISION_BINARY_TO_DECIMAL_FACTOR));
    bool is_finish = false;
    if (expr.datum_meta_.type_ == ObNumberFloatType) {
      if (OB_MIN_NUMBER_FLOAT_PRECISION <= precision && precision <= OB_MAX_NUMBER_FLOAT_PRECISION) {
        NumberFloatTypeCheck cast_fn(res_vec, number_precision);
        if (OB_FAIL(batch_cast_check(cast_fn, expr, res_vec, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret));
        } else {
          is_finish = true;
        }
      } else if (PRECISION_UNKNOWN_YET == precision) {
        is_finish = true;
      } else {
        ret = OB_INVALID_ARGUMENT;
        SQL_LOG(WARN, "invalid arguments", K(ret), K(precision), K(scale));
      }
    } else if (lib::is_oracle_mode()) {
      if (OB_MAX_NUMBER_PRECISION >= precision && precision >= OB_MIN_NUMBER_PRECISION
          && number::ObNumber::MAX_SCALE >= scale && scale >= number::ObNumber::MIN_SCALE) {
        min_check_num = &(ObNumberConstValue::ORACLE_CHECK_MIN
                              [precision][scale + ObNumberConstValue::MAX_ORACLE_SCALE_DELTA]);
        max_check_num = &(ObNumberConstValue::ORACLE_CHECK_MAX
                              [precision][scale + ObNumberConstValue::MAX_ORACLE_SCALE_DELTA]);
      } else if (ORA_NUMBER_SCALE_UNKNOWN_YET == scale && PRECISION_UNKNOWN_YET == precision) {
        // res_vec->set_number(idx, in_val);
        is_finish = true;
      } else if (precision == PRECISION_UNKNOWN_YET && number::ObNumber::MAX_SCALE >= scale
                 && scale >= number::ObNumber::MIN_SCALE) {
        NumberOracleCheck cast_fn(res_vec, scale);
        if (OB_FAIL(batch_cast_check(cast_fn, expr, res_vec, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret));
        } else {
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
      } else {
        NumberNotFinishCheck cast_fn(res_vec, scale, min_check_num, max_check_num,
                                          min_num_mysql, max_num_mysql);
        if (OB_FAIL(batch_cast_check(cast_fn, expr, res_vec, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret));
        }
      }
    }
    SQL_LOG(DEBUG, "number_range_check done", K(ret), K(accuracy), KPC(min_check_num), KPC(max_check_num));
    return ret;
  }
  class NumberFloatTypeCheck {
  public:
    NumberFloatTypeCheck(Vector* res_vec, const int64_t number_precision)
        : res_vec_(res_vec), number_precision_(number_precision) {}

    OB_INLINE int operator() (const ObExpr &expr, int idx)
    {
      int ret = OB_SUCCESS, warning = OB_SUCCESS;
      number::ObNumber out_val;
      const char *payload = res_vec_->get_payload(idx);
      const static int64_t num_alloc_used_times = 2; // out val alloc will be used twice
      ObNumStackAllocator<num_alloc_used_times> out_val_alloc;
      const number::ObNumber in_val(*reinterpret_cast<const number::ObCompactNumber *>(payload));
      if (OB_FAIL(out_val.from(in_val, out_val_alloc))) {
      } else if (OB_FAIL(out_val.round_precision(number_precision_))) {
      } else if (CM_IS_ERROR_ON_SCALE_OVER(expr.extra_) && in_val.compare(out_val) != 0) {
        ret = OB_OPERATE_OVERFLOW;
        SQL_LOG(WARN, "input value is out of range.", K(in_val));
      } else {
        res_vec_->set_number(idx, out_val);
      }
      SQL_LOG(DEBUG, "finish round_precision", K(in_val), K(number_precision_));
      return ret;
    }
  private:
    Vector *res_vec_;
    const int64_t number_precision_;
  };
  class NumberOracleCheck {
  public:
    NumberOracleCheck(Vector* res_vec, ObScale scale)
        : res_vec_(res_vec), scale_(scale) {}

    OB_INLINE int operator() (const ObExpr &expr, int idx)
    {
      int ret = OB_SUCCESS, warning = OB_SUCCESS;
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber num;
      const char *payload = res_vec_->get_payload(idx);
      const number::ObNumber in_val(*reinterpret_cast<const number::ObCompactNumber *>(payload));
      if (OB_FAIL(num.from(in_val, tmp_alloc))) {
      } else if (OB_FAIL(num.round(scale_))) {
      } else if (CM_IS_ERROR_ON_SCALE_OVER(expr.extra_) && in_val.compare(num) != 0) {
        ret = OB_OPERATE_OVERFLOW;
        SQL_LOG(WARN, "input value is out of range.", K(scale_), K(in_val));
      } else {
        res_vec_->set_number(idx, num);
      }
      return ret;
    }
  private:
    Vector *res_vec_;
    ObScale scale_;
  };
  class NumberNotFinishCheck {
  public:
    NumberNotFinishCheck(Vector* res_vec, ObScale scale,
            const number::ObNumber *min_check_num, const number::ObNumber *max_check_num,
            const number::ObNumber *min_num_mysql, const number::ObNumber *max_num_mysql)
        : res_vec_(res_vec), scale_(scale),
          min_check_num_(min_check_num), max_check_num_(max_check_num),
          min_num_mysql_(min_num_mysql), max_num_mysql_(max_num_mysql) {}

    OB_INLINE int operator() (const ObExpr &expr, int idx)
    {
      int ret = OB_SUCCESS, warning = OB_SUCCESS;
      int &cast_ret = CM_IS_ERROR_ON_FAIL(expr.extra_) ? ret : warning;
      bool is_finish = false;
      number::ObNumber out_val;
      const char *payload = res_vec_->get_payload(idx);
      const number::ObNumber in_val(*reinterpret_cast<const number::ObCompactNumber *>(payload));
      if (in_val <= *min_check_num_) {
        if (lib::is_oracle_mode()) {
          cast_ret = OB_ERR_VALUE_LARGER_THAN_ALLOWED;
        } else {
          cast_ret = OB_DATA_OUT_OF_RANGE;
          res_vec_->set_number(idx, *min_num_mysql_);
        }
        is_finish = true;
      } else if (in_val >= *max_check_num_) {
        if (lib::is_oracle_mode()) {
          cast_ret = OB_ERR_VALUE_LARGER_THAN_ALLOWED;
        } else {
          cast_ret = OB_DATA_OUT_OF_RANGE;
          res_vec_->set_number(idx, *max_num_mysql_);
        }
        is_finish = true;
      } else {
        const static int64_t num_alloc_used_times = 2; // out val alloc will be used twice
        ObNumStackAllocator<num_alloc_used_times> out_val_alloc;
        if (OB_FAIL(out_val.from(in_val, out_val_alloc))) {
          SQL_LOG(WARN, "out_val.from failed", K(ret), K(in_val));
        } else if (OB_FAIL(out_val.round(scale_))) {
          SQL_LOG(WARN, "round failed", K(ret));
        } else if (CM_IS_ERROR_ON_SCALE_OVER(expr.extra_) && in_val.compare(out_val) != 0) {
          ret = OB_OPERATE_OVERFLOW;
          SQL_LOG(WARN, "input value is out of range", K(scale_), K(in_val));
        } else {
          res_vec_->set_number(idx, out_val);
          is_finish = true;
        }
      }
      if (OB_SUCC(ret) && !is_finish) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected situation", K(ret));
      }
      return ret;
    }
  private:
    Vector *res_vec_;
    ObScale scale_;
    const number::ObNumber *min_check_num_ = NULL;
    const number::ObNumber *max_check_num_ = NULL;
    const number::ObNumber *min_num_mysql_ = NULL;
    const number::ObNumber *max_num_mysql_ = NULL;
  };
};

template<VecValueTypeClass vec_tc, typename Vector>
struct IntegerRangeChecker
{
  using ResType = RTCType<vec_tc>;
  static int check(CAST_CHECKER_ARG_DECL)
  {
    int ret = OB_SUCCESS;
    // no check for integer/uinteger in datum_accuracy_check
    // does it mean no need to check?
    return ret;
  }
};

template<VecValueTypeClass vec_tc, typename Vector>
struct DateRangeChecker
{
  using ResType = RTCType<vec_tc>;
  static int check(CAST_CHECKER_ARG_DECL)
  {
    int ret = OB_SUCCESS;
    // no check for date in datum_accuracy_check
    // does it mean no need to check?
    return ret;
  }
};

template<VecValueTypeClass vec_tc, typename Vector>
struct DateTimeRangeChecker
{
  using ResType = RTCType<vec_tc>;
  static int check(CAST_CHECKER_ARG_DECL)
  {
    int ret = OB_SUCCESS;
    ObObjType out_type = ObMaxType;
    ObObjType type = expr.datum_meta_.type_;
    ObObjTypeClass type_class = ob_obj_type_class(type);
    ObAccuracy accuracy;
    if (OB_FAIL(get_accuracy_from_parse_node(expr, ctx, accuracy, out_type))) {
      SQL_LOG(WARN, "get accuracy failed", K(ret));
    } else if (OB_UNLIKELY(accuracy.get_scale() > MAX_SCALE_FOR_TEMPORAL)) {
      ret = OB_ERR_TOO_BIG_PRECISION;
      LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, accuracy.get_scale(), "CAST",
          static_cast<int64_t>(MAX_SCALE_FOR_TEMPORAL));
    } else {
      class DatetimeValidCheck {
      public:
        DatetimeValidCheck(Vector* res_vec, ObAccuracy accuracy, int64_t trunc_div, int64_t trunc_mul)
            : res_vec_(res_vec), accuracy_(accuracy), trunc_div_(trunc_div), trunc_mul_(trunc_mul) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          ObScale scale = accuracy_.get_scale();
          int64_t value = res_vec_->get_int(idx);
          const uint64_t cast_mode = expr.extra_;
          if (OB_FAIL(time_usec_scale_check(cast_mode, accuracy_, value))) {
            SQL_LOG(WARN, "check zero scale fail.", K(ret), K(value), K(scale));
          } else if (OB_UNLIKELY(0 <= scale && scale < MAX_SCALE_FOR_TEMPORAL)) {
            if(CM_IS_COLUMN_CONVERT(cast_mode) ? CM_IS_TIME_TRUNCATE_FRACTIONAL(cast_mode) : false) {
              value /= trunc_div_;
              value *= trunc_mul_;
            } else {
              ObTimeConverter::round_datetime(scale, value);
            }
            if (ObTimeConverter::is_valid_datetime(value)) {
              res_vec_->set_datetime(idx, value);
            } else {
              res_vec_->set_null(idx);
            }
          }
          return ret;
        }
      private:
        Vector *res_vec_;
        ObAccuracy accuracy_;
        int64_t trunc_div_;
        int64_t trunc_mul_;
      };

      Vector *res_vec = static_cast<Vector *>(expr.get_vector(ctx));
      int64_t trunc_div = power_of_10[MAX_SCALE_FOR_TEMPORAL - accuracy.get_scale()];
      int64_t trunc_mul = power_of_10[MAX_SCALE_FOR_TEMPORAL - accuracy.get_scale()];
      DatetimeValidCheck cast_fn(res_vec, accuracy, trunc_div, trunc_mul);
      if (OB_FAIL(batch_cast_check(cast_fn, expr, res_vec, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret));
      }
    }
    return ret;
  }

public:
  constexpr static const int64_t power_of_10[] =
  {
      1LL,
      10LL,
      100LL,
      1000LL,
      10000LL,
      100000LL,
      1000000LL,
      10000000LL,
      100000000LL,
      1000000000LL
    //2147483647
  };
};


template<VecValueTypeClass vec_tc, typename Vector>
struct StringRangeChecker
{
  using ResType = RTCType<vec_tc>;
  static int check(CAST_CHECKER_ARG_DECL)
  {
    int ret = OB_SUCCESS;
    ObObjType out_type = ObMaxType;
    const uint64_t cast_mode = expr.extra_;
    ObAccuracy accuracy;
    ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session();
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "session is NULL", K(ret));
    } else if (OB_FAIL(get_accuracy_from_parse_node(expr, ctx, accuracy, out_type))) {
      SQL_LOG(WARN, "get accuracy failed", K(ret));
    } else {
      ObObjType in_type = expr.args_[0]->datum_meta_.type_;
      ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
      Vector *res_vec = static_cast<Vector *>(expr.get_vector(ctx));
      bool skip_string_length_check = skip_str_length_check(accuracy, cast_mode, in_type);

      if (ObNVarchar2Type == out_type || ObNCharType == out_type  // ObNVarchar2Type, ObNCharType
         || ObVarcharType == out_type || ObCharType  == out_type) // ObVarcharType, ObCharType
      {
        bool no_padding = mysql_nopadding_opt(in_type, out_type, out_cs_type);
        int64_t max_allowed_packet = 0;
        int packet_ret = session->get_max_allowed_packet(max_allowed_packet);
        for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
          if (skip.at(idx) || res_vec->is_null(idx)) { continue; }
          if(OB_FAIL(varchar_char_check_vec(CAST_CHECKER_ARG, accuracy, idx, out_type, res_vec,
                                            skip_string_length_check,
                                            max_allowed_packet, packet_ret,
                                            no_padding))) {
            SQL_LOG(WARN, "varchar char check failed", K(ret), K(res_vec->get_string(idx)));
          }
        }
      } else {
        // ObTinyTextType is not large_text, so go string_length_check simply
        if (ObHexStringType == out_type || ObTinyTextType == out_type) {
          if (skip_string_length_check) {
            SQL_LOG(DEBUG, "skip string length check", K(in_type), K(accuracy.get_length()));
          } else {
            ObDatum in_datum;
            ObDatum res_datum;
            for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
              if (skip.at(idx) || res_vec->is_null(idx)) { continue; }
              in_datum.set_string(res_vec->get_string(idx));
              if (OB_FAIL(string_length_check(expr, cast_mode, accuracy, out_type, out_cs_type,
                                        ctx, in_datum, res_datum, warning))) {
                SQL_LOG(WARN, "string length check failed", K(ret), K(in_datum));
              } else {
                if (res_datum.is_null()) {
                  res_vec->set_null(idx);
                } else {
                  res_vec->set_string(idx, res_datum.get_string());
                }
              }
            }
          }
        }
      }
    }
    return ret;
  }

  ///@brief refer to ob_datum_cast.cpp:anytype_to_varchar_char_explicit
  /// just to remove get_accuracy_from_parse_node and add some opt switch.
  /// maybe need a better solution ?
  static int varchar_char_check_vec(CAST_CHECKER_ARG_DECL,
                                    ObAccuracy &accuracy,
                                    int64_t idx,
                                    ObObjType &out_type,
                                    Vector *res_vec,
                                    bool skip_string_length_check,
                                    int64_t max_allowed_packet,
                                    int packet_ret,
                                    bool mysql_nopadding_opt) {
    int ret = OB_SUCCESS;
    ObDatum in_datum;
    ObDatum res_datum;
    bool is_from_pl = !expr.is_called_in_sql_;
    ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
    const ObLength accuracy_length = accuracy.get_length();
    if (!skip_string_length_check) {
      in_datum.set_string(res_vec->get_string(idx));
      if (OB_FAIL(string_length_check(expr, expr.extra_, accuracy, out_type, out_cs_type,
                                      ctx, in_datum, res_datum, warning))) {
        if (ob_is_string_type(expr.datum_meta_.type_) && OB_ERR_DATA_TOO_LONG == ret) {
          ObDatumMeta src_meta;
          if (ObDatumCast::is_implicit_cast(*expr.args_[0])) {
            const ObExpr &grand_child = *(expr.args_[0]->args_[0]);
            if (OB_UNLIKELY(ObDatumCast::is_implicit_cast(grand_child) &&
                            !grand_child.obj_meta_.is_xml_sql_type())) {
              ret = OB_ERR_UNEXPECTED;
              SQL_LOG(WARN, "too many cast expr, max is 2", K(ret), K(expr));
            } else {
              src_meta = grand_child.datum_meta_;
            }
          } else {
            src_meta = expr.args_[0]->datum_meta_;
          }
          if (OB_LIKELY(OB_ERR_DATA_TOO_LONG == ret)) {
            if (lib::is_oracle_mode() &&
                (ob_is_character_type(src_meta.type_, src_meta.cs_type_) ||
                ob_is_raw(src_meta.type_))) {
              ret = OB_SUCCESS;
            } else if ((ob_is_clob(src_meta.type_, src_meta.cs_type_)
                        || ob_is_clob_locator(src_meta.type_, src_meta.cs_type_)
                        || expr.args_[0]->obj_meta_.is_xml_sql_type()
                        || (expr.args_[0]->type_ == T_FUN_SYS_CAST &&
                            expr.args_[0]->args_[0]->obj_meta_.is_xml_sql_type()))
                      && lib::is_oracle_mode()) {
              if (ob_is_nchar(expr.datum_meta_.type_)
                  || ob_is_char(expr.datum_meta_.type_, expr.datum_meta_.cs_type_)) {
                ret = OB_OPERATE_OVERFLOW;
              } else {
                ret = OB_SUCCESS;
              }
            } else {
              ret = OB_ERR_TRUNCATED_WRONG_VALUE;
            }
          }
        }
      }
    } else {
      res_datum.set_string(res_vec->get_string(idx));
    }
    if (OB_SUCC(ret)) {
      if (res_datum.is_null()) {
        res_vec->set_null(idx);
      } else if (-1 == accuracy_length) {
      } else if (0 > accuracy_length) {
        ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
        SQL_LOG(WARN, "accuracy too long", K(ret), K(accuracy_length));
      } else {
        res_vec->set_string(idx, res_datum.get_string());
        bool has_result = false;
        if (!lib::is_oracle_mode()) {
          // int64_t max_allowed_packet = 0;
          if (OB_FAIL(packet_ret)) {
            if (OB_ENTRY_NOT_EXIST == ret) { // for compatibility with server before 1470
              ret = OB_SUCCESS;
              max_allowed_packet = OB_MAX_VARCHAR_LENGTH;
            } else {
              SQL_LOG(WARN, "Failed to get max allow packet size", K(ret));
            }
          } else if (accuracy_length > max_allowed_packet &&
                     accuracy_length <= INT32_MAX) {
            res_vec->set_null(idx);
            has_result = true;
            LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "cast",
                          static_cast<int>(max_allowed_packet));
          } else if (accuracy_length == 0) {
            res_vec->set_string(idx, NULL, 0);
            has_result = true;
          }
        }

        if (OB_SUCC(ret) && !has_result) {
          ObString text(res_vec->get_length(idx), res_vec->get_payload(idx));
          bool oracle_char_byte_exceed = false;
          ObLengthSemantics ls = lib::is_oracle_mode() ?
                                 expr.datum_meta_.length_semantics_ : LS_CHAR;
          int32_t text_length = INT32_MAX;
          bool is_varbinary_or_binary =
            (out_type == ObVarcharType && CS_TYPE_BINARY == out_cs_type) ||
            (ObCharType == out_type && CS_TYPE_BINARY == out_cs_type);

          // if satisfy mysql_nopadding_opt, can eliminate high cost of ObCharset::strlen_char
          if (mysql_nopadding_opt && (accuracy_length >= res_vec->get_length(idx))) {
            SQL_LOG(DEBUG, "no need to padding", K(ret), K(accuracy_length), K(mysql_nopadding_opt));
          } else {
            if (is_varbinary_or_binary) {
              text_length = text.length();
            } else {
              if (is_oracle_byte_length(lib::is_oracle_mode(), ls)) {
                text_length = text.length();
              } else {
                text_length = static_cast<int32_t>(ObCharset::strlen_char(
                      expr.datum_meta_.cs_type_, text.ptr(), text.length()));
              }
            }
            if (lib::is_oracle_mode() && ls == LS_CHAR) {
              if ((ObCharType == out_type || ObNCharType == out_type) &&
                  text.length() > (is_from_pl ? OB_MAX_ORACLE_PL_CHAR_LENGTH_BYTE
                                              : OB_MAX_ORACLE_CHAR_LENGTH_BYTE)) {
                oracle_char_byte_exceed = true;
              } else if ((ObVarcharType == out_type || ObNVarchar2Type == out_type) &&
                        text.length() > OB_MAX_ORACLE_VARCHAR_LENGTH) {
                oracle_char_byte_exceed = true;
              }
            }

            if (accuracy_length < text_length || oracle_char_byte_exceed) {
              int64_t acc_len = !oracle_char_byte_exceed? accuracy_length :
                ((ObVarcharType == out_type || ObNVarchar2Type == out_type) ?
                OB_MAX_ORACLE_VARCHAR_LENGTH: (is_from_pl ? OB_MAX_ORACLE_PL_CHAR_LENGTH_BYTE
                                                          : OB_MAX_ORACLE_CHAR_LENGTH_BYTE));
              int64_t char_len = 0; // UNUSED
              int64_t size = (ls == LS_BYTE || oracle_char_byte_exceed ?
                  ObCharset::max_bytes_charpos(out_cs_type, text.ptr(), text.length(),
                                              acc_len, char_len):
                  ObCharset::charpos(out_cs_type, text.ptr(), text.length(), acc_len));
              if (0 == size) {
                if (lib::is_oracle_mode()) {
                  res_vec->set_null(idx);
                } else {
                  res_vec->set_string(idx, NULL, 0);
                }
              } else {
                res_vec->set_length(idx, size);
              }
            } else if (accuracy_length == text_length
                      || (ObCharType != out_type && ObNCharType != out_type)
                      || (lib::is_mysql_mode()
                          && ob_is_char(out_type, expr.datum_meta_.cs_type_))) {
              // do not padding
              SQL_LOG(DEBUG, "no need to padding", K(ret), K(accuracy_length),
                                              K(text_length), K(text));
            } else if (accuracy_length > text_length) {
              int64_t padding_cnt = accuracy_length - text_length;
              ObString padding_res;
              ObEvalCtx::TempAllocGuard alloc_guard(ctx);
              ObIAllocator &calc_alloc = alloc_guard.get_allocator();
              if (OB_FAIL(padding_char_for_cast(padding_cnt, out_cs_type, calc_alloc,
                                                padding_res))) {
                SQL_LOG(WARN, "padding char failed", K(ret), K(padding_cnt), K(out_cs_type));
              } else {
                int64_t padding_size = padding_res.length() + text.length();
                char *res_ptr = expr.get_str_res_mem(ctx, padding_size, idx);
                if (OB_ISNULL(res_ptr)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  SQL_LOG(WARN, "allocate memory failed", K(ret));
                } else {
                  MEMMOVE(res_ptr, text.ptr(), text.length());
                  MEMMOVE(res_ptr + text.length(), padding_res.ptr(), padding_res.length());
                  res_vec->set_string(idx, res_ptr, text.length() + padding_res.length());
                }
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              SQL_LOG(WARN, "can never reach", K(ret));
            }
          }
        }
      }
    }
    return ret;
  }

  ///@brief varchar_char_check_vec中ObCharset::strlen_char的开销很大，
  /// 对满足以下条件的，可以跳过ObCharset::strlen_char
  /// 1. 保证对应字符集的ObCharset::strlen_char()执行后返回长度值不会大于输入的字符串长度
  /// 2. mysql模式，这样strlen_char返回的值，在后续也不会被增大
  /// 3. 不需要padding，保证后续不会实际用到strlen_char返回的具体值大小
  /// 这样就可以不需要执行ObCharset::strlen_char()来获得对应返回值，可以显著降低开销。
  ///@brief 对于条件1，具体哪些字符集的ObCharset::strlen_char()满足要求，目前是通过看函数实现确定的,
  ///  但如果具体实现被改变可能就会引入正确性问题，需要进一步调研确定？
  static bool mysql_nopadding_opt(ObObjType in_type, ObObjType out_type, ObCollationType out_cs_type)
  {
    bool no_padding = (ObCharType != out_type && ObNCharType != out_type)
                      || (ob_is_char(out_type, out_cs_type));
    // todo: maybe other cs_type 's cs->cset->numchars not expand length of string also
    bool numchars_no_expanding = (CS_TYPE_UTF8MB4_BIN == out_cs_type ||
                                  CS_TYPE_BINARY == out_cs_type ||
                                  CS_TYPE_UTF8MB4_GENERAL_CI == out_cs_type);
    return lib::is_mysql_mode() &&
           no_padding && numchars_no_expanding;
  }

  ///@brief string_length_check只针对于
  ///  (max_accuracy_len <= 0 || str_len_byte > max_accuracy_len)的情况，如果:
  ///  1. max_accuracy_len > 0 &&
  ///  2. 输入类型对应的最大返回长度都<=max_accuracy_len
  /// 可以直接跳过长度检查，不需要进入batch计算
  ///@brief 当前仅完成对int类型的判断，其他默认都不跳过
  static bool skip_str_length_check(ObAccuracy &accuracy,
                                    const uint64_t cast_mode,
                                    ObObjType type) {
    const int64_t TYPE_MAX_LENGTH[] = {
        0,   // ObNullType
        4,   // ObTinyIntType
        6,   // ObSmallIntType
        9,   // ObMediumIntType
        11,  // ObInt32Type
        21,  // ObIntType
        4,   // ObUTinyIntType
        6,   // ObUSmallIntType
        9,   // ObUMediumIntType
        11,  // ObUInt32Type
        21,  // ObUInt64Type
    };
    bool res = false;
    if (!CM_IS_ZERO_FILL(cast_mode) && accuracy.get_length() > 0) {
      if (ObNullType < type && type <= ObUInt64Type) {
        res = (TYPE_MAX_LENGTH[type] <= accuracy.get_length());
      } else {
        // todo
      }
    }
    return res;
  }
};

} // end sql
} // end oceanbase

#undef CAST_FAIL_CM
#undef DEF_BATCH_RANGE_CHECKER