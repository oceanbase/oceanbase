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
#include "sql/engine/expr/vector_cast/vector_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "share/object/ob_obj_cast_util.h"

namespace oceanbase
{
namespace sql
{

// Cast func processing logic is a reference to ob_datum_cast.cpp::CAST_FUNC_NAME(IN_TYPE, OUT_TYPE)
template<typename ArgVec, typename ResVec>
struct ToDateCastImpl
{
  template<typename IN_TYPE>
  static int float_date(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class FloatDoubleToDateFn : public CastFnBase {
      public:
        FloatDoubleToDateFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                                  ObDateSqlMode date_sql_mode)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
              date_sql_mode_(date_sql_mode) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          int64_t int_val = 0;
          int32_t out_val = 0;
          IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec_->get_payload(idx));
          if (OB_FAIL(common_floating_int(in_val, int_val))) {
            SQL_LOG(WARN, "common_double_int failed", K(ret));
          } else if (CAST_FAIL(ObTimeConverter::int_to_date(int_val, out_val, date_sql_mode_))) {
            SQL_LOG(WARN, "int_to_date failed", K(ret), K(int_val), K(out_val));
          } else {
            SET_RES_DATE(idx, out_val);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
        ObDateSqlMode date_sql_mode_;
      };

      ObDateSqlMode date_sql_mode;
      date_sql_mode.allow_invalid_dates_ = CM_IS_EXPLICIT_CAST(expr.extra_)
                                           ? false : CM_IS_ALLOW_INVALID_DATES(expr.extra_);
      date_sql_mode.no_zero_date_ = CM_IS_EXPLICIT_CAST(expr.extra_)
                                    ? false : CM_IS_NO_ZERO_DATE(expr.extra_);
      FloatDoubleToDateFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, date_sql_mode);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  template<typename IN_TYPE>
  static int int_date(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class IntUIntToDateFn : public CastFnBase {
      public:
        IntUIntToDateFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                             ObDateSqlMode date_sql_mode)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
              date_sql_mode_(date_sql_mode) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          int32_t out_val = 0;
          IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec_->get_payload(idx));
          int64_t int64 = in_val;
          if (std::is_same<IN_TYPE, uint64_t>::value &&
              OB_FAIL(ObDataTypeCastUtil::common_uint_int_wrap(expr, ObIntType, in_val, ctx_, int64))) {
            SQL_LOG(WARN, "common_uint_int failed", K(ret));
          } else if (CAST_FAIL(ObTimeConverter::int_to_date(int64, out_val, date_sql_mode_))) {
            SQL_LOG(WARN, "int_to_date failed", K(ret), K(in_val), K(out_val));
          } else {
            SET_RES_DATE(idx, out_val);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
        ObDateSqlMode date_sql_mode_;
      };

      ObDateSqlMode date_sql_mode;
      date_sql_mode.allow_invalid_dates_ = CM_IS_EXPLICIT_CAST(expr.extra_)
                                           ? false : CM_IS_ALLOW_INVALID_DATES(expr.extra_);
      date_sql_mode.no_zero_date_ = CM_IS_EXPLICIT_CAST(expr.extra_)
                                    ? false : CM_IS_NO_ZERO_DATE(expr.extra_);
      IntUIntToDateFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, date_sql_mode);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  static int datetime_date(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      int64_t tz_offset = 0;
      ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session();
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "session is NULL", K(ret));
      } else if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        SQL_LOG(WARN, "get time zone info failed", K(ret));
      } else {
        class DatetimeToDateFn : public CastFnBase {
        public:
          DatetimeToDateFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                           const ObTimeZoneInfo *tz_info)
              : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
                tz_info_(tz_info) {}

          OB_INLINE int operator() (const ObExpr &expr, int idx)
          {
            int ret = OB_SUCCESS;
            int64_t in_val = arg_vec_->get_datetime(idx);
            int32_t out_val = 0;
            if (OB_FAIL(ObTimeConverter::datetime_to_date(in_val, tz_info_, out_val))) {
              SQL_LOG(WARN, "datetime_to_date failed", K(ret), K(in_val));
            } else {
              res_vec_->set_date(idx, out_val);
            }
            return ret;
          }
        private:
          ArgVec *arg_vec_;
          ResVec *res_vec_;
          const ObTimeZoneInfo *tz_info_;
        };

        const ObTimeZoneInfo *tz_info = (ObTimestampType == in_type)
                                         ? tz_info_local : NULL;
        DatetimeToDateFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, tz_info);
        if (OB_FAIL(CastHelperImpl::batch_cast(
                        cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }

  // To speed up the use of division to convert a decimal int to a uint instead of to a number.
  template<typename IN_TYPE>
  static int decimalint_date(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class DecimalintToDateFn : public CastFnBase {
      public:
        DecimalintToDateFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                         ObDateSqlMode date_sql_mode, const IN_TYPE &sf)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
              date_sql_mode_(date_sql_mode), sf_(sf) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          IN_TYPE lhs = *reinterpret_cast<const IN_TYPE *>(arg_vec_->get_payload(idx));
          int32_t out_val = 0;
          if (lhs < 0) {
            ret = OB_INVALID_DATE_VALUE;
            SQL_LOG(WARN, "invalid date value", K(ret), K(lhs));
          } else {
            uint64_t uint64 = CastHelperImpl::scale_to_unsigned_integer(lhs, sf_, in_scale_, false);
            int64_t int64 = uint64;
            if (OB_UNLIKELY(uint64 > INT64_MAX)) {
              int64 = INT64_MAX;
            }
            ret = ObTimeConverter::int_to_date(int64, out_val, date_sql_mode_);
          }
          int warning = OB_SUCCESS;
          if (CAST_FAIL(ret)) {
            SQL_LOG(WARN, "cast decimal to date failed", K(ret));
          } else {
            SET_RES_DATE(idx, out_val);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
        ObDateSqlMode date_sql_mode_;
        const IN_TYPE &sf_;
      };

      ObDateSqlMode date_sql_mode;
      date_sql_mode.allow_invalid_dates_ = CM_IS_EXPLICIT_CAST(expr.extra_)
                                           ? false : CM_IS_ALLOW_INVALID_DATES(expr.extra_);
      date_sql_mode.no_zero_date_ = CM_IS_EXPLICIT_CAST(expr.extra_)
                                    ? false : CM_IS_NO_ZERO_DATE(expr.extra_);
      if (OB_UNLIKELY(in_scale < 0)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid input scale", K(ret), K(in_scale));
      } else {
        IN_TYPE sf = get_scale_factor<IN_TYPE>(in_scale);
        DecimalintToDateFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, date_sql_mode, sf);
        if (OB_FAIL(CastHelperImpl::batch_cast(
                        cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }

  static int number_date(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class NumberToDateFn : public CastFnBase {
      public:
        NumberToDateFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          int32_t out_val = 0;
          const number::ObNumber nmb(arg_vec_->get_number(idx));
          ret = ObDataTypeCastUtil::common_number_date_wrap(nmb, expr.extra_,
                      out_val, expr.datum_meta_.type_ == ObMySQLDateTimeType);
          if (CAST_FAIL(ret)) {
          } else {
            SET_RES_DATE(idx, out_val);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      NumberToDateFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  static int string_date(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {

      class StringToDateFn : public CastFnBase {
      public:
        StringToDateFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                            ObDateSqlMode date_sql_mode)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
              date_sql_mode_(date_sql_mode) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          ObLength in_length = arg_vec_->get_length(idx);
          if (lib::is_oracle_mode() && ObLongTextType != in_type_ && 0 == in_length) {
            res_vec_->set_null(idx);
          } else {
            int32_t out_val = 0;
            ObString in_str = arg_vec_->get_string(idx);
            if (CAST_FAIL(ObTimeConverter::str_to_date(in_str, out_val, date_sql_mode_))) {
              SQL_LOG(WARN, "str_to_date failed", K(ret), K(in_str));
            } else if (CM_IS_ERROR_ON_SCALE_OVER(expr.extra_) && out_val == ObTimeConverter::ZERO_DATE) {
              // check zero date for scale over mode
              ret = OB_INVALID_DATE_VALUE;
              LOG_USER_ERROR(OB_INVALID_DATE_VALUE, in_str.length(), in_str.ptr(), "");
            } else {
              SET_RES_DATE(idx, out_val);
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
        ObDateSqlMode date_sql_mode_;
      };

      ObDateSqlMode date_sql_mode;
      date_sql_mode.allow_invalid_dates_ = CM_IS_ALLOW_INVALID_DATES(expr.extra_);
      date_sql_mode.no_zero_date_ = CM_IS_NO_ZERO_DATE(expr.extra_);
      StringToDateFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, date_sql_mode);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }
};

// ================
// define implicit cast functions
#define DEF_INTEGER_TO_DATE_IMPLICIT_CASTS(in_tc)                          \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DATE)                      \
  {                                                                      \
    return ToDateCastImpl<IN_VECTOR, OUT_VECTOR>::template               \
              int_date<RTCType<in_tc>>(expr, ctx, skip, bound);        \
  }

LST_DO_CODE(DEF_INTEGER_TO_DATE_IMPLICIT_CASTS,
            VEC_TC_INTEGER,
            VEC_TC_UINTEGER
            );

#define DEF_FLOAT_TO_DATE_IMPLICIT_CASTS(in_tc)                          \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DATE)                      \
  {                                                                      \
    return ToDateCastImpl<IN_VECTOR, OUT_VECTOR>::template               \
              float_date<RTCType<in_tc>>(expr, ctx, skip, bound);        \
  }

LST_DO_CODE(DEF_FLOAT_TO_DATE_IMPLICIT_CASTS,
            VEC_TC_FLOAT,
            VEC_TC_DOUBLE
            );

#define DEF_DECIMALINT_TO_DATE_IMPLICIT_CASTS(in_tc)                     \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DATE)                      \
  {                                                                      \
    return ToDateCastImpl<IN_VECTOR, OUT_VECTOR>::template               \
              decimalint_date<RTCType<in_tc>>(expr, ctx, skip, bound);   \
  }

LST_DO_CODE(DEF_DECIMALINT_TO_DATE_IMPLICIT_CASTS,
            VEC_TC_DEC_INT32,
            VEC_TC_DEC_INT64,
            VEC_TC_DEC_INT128,
            VEC_TC_DEC_INT256,
            VEC_TC_DEC_INT512
            );

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_STRING, VEC_TC_DATE)
{                                                                        \
  return ToDateCastImpl<IN_VECTOR, OUT_VECTOR>::                         \
            string_date(expr, ctx, skip, bound);                         \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_NUMBER, VEC_TC_DATE)
{                                                                        \
  return ToDateCastImpl<IN_VECTOR, OUT_VECTOR>::                         \
            number_date(expr, ctx, skip, bound);                         \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATETIME, VEC_TC_DATE)
{                                                                        \
  return ToDateCastImpl<IN_VECTOR, OUT_VECTOR>::                         \
            datetime_date(expr, ctx, skip, bound);                       \
}

} // end sql
} // namespace oceanbase

#undef DEF_INTEGER_TO_DATE_IMPLICIT_CASTS
#undef DEF_FLOAT_TO_DATE_IMPLICIT_CASTS
#undef DEF_DECIMALINT_TO_DATE_IMPLICIT_CASTS
