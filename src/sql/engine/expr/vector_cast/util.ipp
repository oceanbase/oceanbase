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
    inline static int check(DEF_BATCH_RANGE_CHECKER_DECL);                                 \
  };                                                                                       \
  template <typename Vector>                                                               \
  inline int BatchValueRangeChecker<vec_tc, Vector>::check(DEF_BATCH_RANGE_CHECKER_DECL)

namespace oceanbase
{
using namespace common;
namespace sql
{
template<VecValueTypeClass vec_tc, typename Vector> struct FloatRangeChecker;
template<VecValueTypeClass vec_tc, typename Vector> struct DecintRangeChecker;
template<VecValueTypeClass vec_tc, typename Vector> struct IntegerRangeChecker;
template<VecValueTypeClass vec_tc, typename Vector> struct NumberRangeChecker;


DEF_BATCH_RANGE_CHECKER(VEC_TC_DEC_INT32) {
  return DecintRangeChecker<VEC_TC_DEC_INT32, Vector>::check(DEF_BATCH_RANGE_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_DEC_INT64) {
  return DecintRangeChecker<VEC_TC_DEC_INT64, Vector>::check(DEF_BATCH_RANGE_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_DEC_INT128) {
  return DecintRangeChecker<VEC_TC_DEC_INT128, Vector>::check(DEF_BATCH_RANGE_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_DEC_INT256) {
  return DecintRangeChecker<VEC_TC_DEC_INT256, Vector>::check(DEF_BATCH_RANGE_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_DEC_INT512) {
  return DecintRangeChecker<VEC_TC_DEC_INT512, Vector>::check(DEF_BATCH_RANGE_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_FLOAT) {
  return FloatRangeChecker<VEC_TC_FLOAT, Vector>::check(DEF_BATCH_RANGE_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_DOUBLE) {
  return FloatRangeChecker<VEC_TC_DOUBLE, Vector>::check(DEF_BATCH_RANGE_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_INTEGER) {
  return IntegerRangeChecker<VEC_TC_INTEGER, Vector>::check(DEF_BATCH_RANGE_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_UINTEGER) {
  return IntegerRangeChecker<VEC_TC_UINTEGER, Vector>::check(DEF_BATCH_RANGE_CHECKER_ARG);
}
DEF_BATCH_RANGE_CHECKER(VEC_TC_NUMBER) {
  return NumberRangeChecker<VEC_TC_NUMBER, Vector>::check(DEF_BATCH_RANGE_CHECKER_ARG);
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
  static int check(DEF_BATCH_RANGE_CHECKER_DECL)
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
  static int check(DEF_BATCH_RANGE_CHECKER_DECL)
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
  static int check(DEF_BATCH_RANGE_CHECKER_DECL)
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
  static int check(DEF_BATCH_RANGE_CHECKER_DECL)
  {
    int ret = OB_SUCCESS;
    // no check for integer/uinteger in datum_accuracy_check
    // does it mean no need to check?
    return ret;
  }
};

} // end sql
} // end oceanbase

#undef CAST_FAIL_CM
#undef DEF_BATCH_RANGE_CHECKER