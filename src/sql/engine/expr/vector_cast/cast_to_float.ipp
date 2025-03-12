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
struct ToFloatCastImpl
{
  template<typename IN_TYPE, typename OUT_TYPE>
  static int float_float(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class FloatDoubleToFloatDoubleFn : public CastFnBase {
      public:
        FloatDoubleToFloatDoubleFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec_->get_payload(idx));
          OUT_TYPE res_val = in_val;
          if (std::is_same<IN_TYPE, double>::value && std::is_same<OUT_TYPE, float>::value) {
            float out_val = 0.0;
            if (OB_FAIL(ObDataTypeCastUtil::common_double_float_wrap(expr, in_val, out_val))) {
              SQL_LOG(WARN, "common_double_float_fastfloat failed", K(ret));
            }
            res_val = out_val;
          } else if (ob_is_unsigned_type(out_type_)) {
            if (CAST_FAIL(numeric_negative_check(res_val))) {
              SQL_LOG(WARN, "numeric_negative_check failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            res_vec_->set_payload(idx, &res_val, sizeof(OUT_TYPE));
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      FloatDoubleToFloatDoubleFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  template<typename IN_TYPE, typename OUT_TYPE>
  static int int_float(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class IntUIntToFloatDoubleFn : public CastFnBase {
      public:
        IntUIntToFloatDoubleFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS, warning = OB_SUCCESS;
          if (std::is_same<IN_TYPE, int64_t>::value && ob_is_unsigned_type(out_type_)) {
            OUT_TYPE res_val = static_cast<OUT_TYPE>(static_cast<double>(arg_vec_->get_int(idx)));
            if (CAST_FAIL(numeric_negative_check(res_val))) {
              SQL_LOG(WARN, "numeric_negative_check failed", K(ret));
            } else {
              res_vec_->set_payload(idx, &res_val, sizeof(OUT_TYPE));
            }
          } else {
            IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec_->get_payload(idx));
            OUT_TYPE res_val = static_cast<OUT_TYPE>(static_cast<double>(in_val));
            res_vec_->set_payload(idx, &res_val, sizeof(OUT_TYPE));
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      IntUIntToFloatDoubleFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  template<typename OUT_TYPE>
  static int date_float(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class DateToFloatDoubleFn : public CastFnBase {
      public:
        DateToFloatDoubleFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int32_t date = arg_vec_->get_date(idx);
          int64_t out_val = 0;
          if (OB_FAIL(ObTimeConverter::date_to_int(date, out_val))) {
            SQL_LOG(WARN, "convert date to int failed", K(ret));
          } else {
            OUT_TYPE res_val = static_cast<OUT_TYPE>(out_val);
            res_vec_->set_payload(idx, &res_val, sizeof(OUT_TYPE));
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      DateToFloatDoubleFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  template<typename OUT_TYPE>
  static int datetime_float(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
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
        class DatetimeToFloatDoubleFn : public CastFnBase {
        public:
          DatetimeToFloatDoubleFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                                  const ObTimeZoneInfo *tz_info)
              : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
                tz_info_(tz_info) {}

          OB_INLINE int operator() (const ObExpr &expr, int idx)
          {
            int ret = OB_SUCCESS;
            double dbl = 0;
            if (OB_FAIL(ObTimeConverter::datetime_to_double(arg_vec_->get_datetime(idx), tz_info_, dbl))) {
              SQL_LOG(WARN, "datetime_to_double failed", K(ret), K(dbl));
            } else {
              OUT_TYPE out_val = dbl;
              res_vec_->set_payload(idx, &out_val, sizeof(OUT_TYPE));
            }
            return ret;
          }
        private:
          ArgVec *arg_vec_;
          ResVec *res_vec_;
          const ObTimeZoneInfo *tz_info_;
        };

        const ObTimeZoneInfo *tz_info = (ObTimestampType == in_type) ?
                                          tz_info_local : NULL;
        DatetimeToFloatDoubleFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, tz_info);
        if (OB_FAIL(CastHelperImpl::batch_cast(
                        cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }

  template<typename IN_TYPE, typename OUT_TYPE>
  static int decimalint_float(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class DecimalintToFloatDoubleFn : public CastFnBase {
      public:
        DecimalintToFloatDoubleFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                                  ObDatum double_datum)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
              double_datum_(double_datum) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int64_t pos = 0;
          if (OB_FAIL(wide::to_string(arg_vec_->get_decimal_int(idx), sizeof(IN_TYPE), in_scale_,
                                      buf_, sizeof(buf_), pos))) {
            SQL_LOG(WARN, "to_string failed", K(ret));
          } else {
            ObString num_str(pos, buf_);
            if (std::is_same<OUT_TYPE, float>::value) {
              float out_val = 0.0;
              if (OB_FAIL(ObDataTypeCastUtil::common_string_float_wrap(expr, num_str, out_val))) {
                SQL_LOG(WARN, "common_string_float failed", K(ret), K(num_str));
              } else {
                res_vec_->set_float(idx, out_val);
              }
            } else {  // double
              if (OB_FAIL(common_string_double(expr, in_type_, in_cs_type_, out_type_,
                                               num_str, double_datum_))) {
                SQL_LOG(WARN, "common_string_double failed", K(ret));
              } else {
                double out_val = double_datum_.get_double();
                res_vec_->set_double(idx, out_val);
              }
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
        ObDatum double_datum_;
        char buf_[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
      };

      ObDatum tmp_datum;
      double tmp_double = 0.0;
      tmp_datum.ptr_ = reinterpret_cast<const char *>(&tmp_double);
      tmp_datum.pack_ = sizeof(double);
      DecimalintToFloatDoubleFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, tmp_datum);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  template<typename OUT_TYPE>
  static int number_float(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class NumberToFloatDoubleFn : public CastFnBase {
      public:
        NumberToFloatDoubleFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                              ObDatum double_datum)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
              double_datum_(double_datum) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          const number::ObNumber nmb(arg_vec_->get_number(idx));
          const char *nmb_buf = nmb.format();
          if (OB_ISNULL(nmb_buf)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_LOG(WARN, "nmb_buf is NULL", K(ret));
          } else {
            ObString num_str(strlen(nmb_buf), nmb_buf);
            if (std::is_same<OUT_TYPE, float>::value) {
              float out_val = 0.0;
              if (OB_FAIL(ObDataTypeCastUtil::common_string_float_wrap(expr, num_str, out_val))) {
                SQL_LOG(WARN, "common_string_float failed", K(ret), K(num_str));
              } else {
                res_vec_->set_float(idx, out_val);
              }
            } else {  // double
              double out_val = 0.0;
              if (OB_FAIL(common_string_double(expr, in_type_, in_cs_type_, out_type_,
                                               num_str, double_datum_))) {
                SQL_LOG(WARN, "common_string_double failed", K(ret), K(num_str));
              } else {
                double out_val = double_datum_.get_double();
                res_vec_->set_double(idx, out_val);
              }
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
        ObDatum double_datum_;
      };

      ObDatum tmp_datum;
      double tmp_double = 0.0;
      tmp_datum.ptr_ = reinterpret_cast<const char *>(&tmp_double);
      tmp_datum.pack_ = sizeof(double);
      NumberToFloatDoubleFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, tmp_datum);
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
#define DEF_INTEGER_TO_FLOAT_IMPLICIT_CASTS(in_tc)                            \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_FLOAT)                          \
  {                                                                           \
    return ToFloatCastImpl<IN_VECTOR, OUT_VECTOR>::template                   \
              int_float<RTCType<in_tc>, float>(expr, ctx, skip, bound);       \
  }                                                                           \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DOUBLE)                         \
  {                                                                           \
    return ToFloatCastImpl<IN_VECTOR, OUT_VECTOR>::template                   \
              int_float<RTCType<in_tc>, double>(expr, ctx, skip, bound);      \
  }

LST_DO_CODE(DEF_INTEGER_TO_FLOAT_IMPLICIT_CASTS,
            VEC_TC_INTEGER,
            VEC_TC_UINTEGER
            );

#define DEF_FLOAT_TO_FLOAT_IMPLICIT_CASTS(in_tc)                              \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_FLOAT)                          \
  {                                                                           \
    return ToFloatCastImpl<IN_VECTOR, OUT_VECTOR>::template                   \
              float_float<RTCType<in_tc>, float>(expr, ctx, skip, bound);     \
  }                                                                           \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DOUBLE)                         \
  {                                                                           \
    return ToFloatCastImpl<IN_VECTOR, OUT_VECTOR>::template                   \
              float_float<RTCType<in_tc>, double>(expr, ctx, skip, bound);    \
  }

LST_DO_CODE(DEF_FLOAT_TO_FLOAT_IMPLICIT_CASTS,
            VEC_TC_FLOAT,
            VEC_TC_DOUBLE
            );

#define DEF_DECIMALINT_TO_FLOAT_IMPLICIT_CASTS(in_tc)                          \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_FLOAT)                           \
  {                                                                            \
    return ToFloatCastImpl<IN_VECTOR, OUT_VECTOR>::template                    \
              decimalint_float<RTCType<in_tc>, float>(expr, ctx, skip, bound); \
  }                                                                            \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DOUBLE)                          \
  {                                                                            \
    return ToFloatCastImpl<IN_VECTOR, OUT_VECTOR>::template                    \
              decimalint_float<RTCType<in_tc>, double>(expr, ctx, skip, bound);\
  }

LST_DO_CODE(DEF_DECIMALINT_TO_FLOAT_IMPLICIT_CASTS,
            VEC_TC_DEC_INT32,
            VEC_TC_DEC_INT64,
            VEC_TC_DEC_INT128,
            VEC_TC_DEC_INT256,
            VEC_TC_DEC_INT512
            );

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATE, VEC_TC_FLOAT)
{                                                                      \
  return ToFloatCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            date_float<float>(expr, ctx, skip, bound);                 \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATE, VEC_TC_DOUBLE)
{                                                                      \
  return ToFloatCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            date_float<double>(expr, ctx, skip, bound);                \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATETIME, VEC_TC_FLOAT)
{                                                                      \
  return ToFloatCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            datetime_float<float>(expr, ctx, skip, bound);             \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATETIME, VEC_TC_DOUBLE)
{                                                                      \
  return ToFloatCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            datetime_float<double>(expr, ctx, skip, bound);            \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_NUMBER, VEC_TC_FLOAT)
{                                                                      \
  return ToFloatCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            number_float<float>(expr, ctx, skip, bound);               \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_NUMBER, VEC_TC_DOUBLE)
{                                                                      \
  return ToFloatCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            number_float<double>(expr, ctx, skip, bound);              \
}
} // end sql
} // namespace oceanbase

#undef DEF_INTEGER_TO_FLOAT_IMPLICIT_CASTS
#undef DEF_DECIMALINT_TO_FLOAT_IMPLICIT_CASTS
#undef DEF_FLOAT_TO_FLOAT_IMPLICIT_CASTS