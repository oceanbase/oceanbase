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
struct ToDecimalintCastImpl
{
  template<typename OUT_TYPE>
  static int string_decimalint(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class StringToDecimalintFn : public CastFnBase {
      public:
        StringToDecimalintFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec, const OUT_TYPE &sf)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec), sf_(sf) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          ObDecimalIntBuilder res_val;
          if (lib::is_oracle_mode() && ObLongTextType != in_type_ && 0 == arg_vec_->get_length(idx)) {
            res_vec_->set_null(idx);
          } else {
            bool can_fast_strtoll = true;
            ObString in_str(arg_vec_->get_length(idx), arg_vec_->get_payload(idx));
            OUT_TYPE out_val = CastHelperImpl::fast_strtoll(in_str, expr.datum_meta_.precision_, can_fast_strtoll);
            if (can_fast_strtoll && !wide::mul_overflow(out_val, sf_, out_val)) {
              switch (get_decimalint_type(expr.datum_meta_.precision_)) {
                case common::DECIMAL_INT_32: {
                  int32_t out_val2 = out_val;
                  res_vec_->set_decimal_int(idx, reinterpret_cast<ObDecimalInt *> (&out_val2), sizeof(out_val2));
                  break;
                }
                case common::DECIMAL_INT_64: {
                  int64_t out_val2 = out_val;
                  res_vec_->set_decimal_int(idx, reinterpret_cast<ObDecimalInt *> (&out_val2), sizeof(out_val2));
                  break;
                }
                case common::DECIMAL_INT_128: {
                  int128_t out_val2 = out_val;
                  res_vec_->set_decimal_int(idx, reinterpret_cast<ObDecimalInt *> (&out_val2), sizeof(out_val2));
                  break;
                }
                case common::DECIMAL_INT_256: {
                  int256_t out_val2 = out_val;
                  res_vec_->set_decimal_int(idx, reinterpret_cast<ObDecimalInt *> (&out_val2), sizeof(out_val2));
                  break;
                }
                case common::DECIMAL_INT_512: {
                  int512_t out_val2 = out_val;
                  res_vec_->set_decimal_int(idx, reinterpret_cast<ObDecimalInt *> (&out_val2), sizeof(out_val2));
                  break;
                }
                default:
                ret = OB_ERR_UNEXPECTED;
                SQL_LOG(WARN,"unexpected precision", K(ret), K(expr.datum_meta_.precision_));
              }
            } else if (OB_FAIL(ObDataTypeCastUtil::common_string_decimalint_wrap(expr, in_str, ctx_.exec_ctx_.get_user_logging_ctx(),
                                                                              res_val))) {
              SQL_LOG(WARN, "cast string to decimal int failed", K(ret));
            } else {
              res_vec_->set_decimal_int(idx, res_val.get_decimal_int(), res_val.get_int_bytes());
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
        const OUT_TYPE &sf_;
      };

      OUT_TYPE sf = get_scale_factor<OUT_TYPE>(out_scale);
      StringToDecimalintFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, sf);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }
  template<typename IN_TYPE, typename OUT_TYPE>
  static int float_decimalint(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class FloatDoubleToDecimalintFn : public CastFnBase {
      public:
        FloatDoubleToDecimalintFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          OUT_TYPE out_val = 0;
          IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec_->get_payload(idx));
          bool nan_or_inf_is_invalid = std::is_same<IN_TYPE, float>::value || lib::is_oracle_mode();
          if (OB_UNLIKELY(isnan(in_val) && nan_or_inf_is_invalid)) {
            ret = OB_INVALID_NUMERIC;
            SQL_LOG(WARN, "float number invalid", K(ret), K(in_val));
          } else if (OB_UNLIKELY(isinf(in_val) && nan_or_inf_is_invalid)) {
            ret = OB_NUMERIC_OVERFLOW;
            SQL_LOG(WARN, "float number overflow", K(ret), K(in_val));
          } else {
            ObDecimalIntBuilder tmp_alloc;
            ObDecimalInt *decint = nullptr;
            int32_t val_len = 0;
            in_scale_ = in_prec_ = 0;
            ob_gcvt_arg_type arg_type = std::is_same<IN_TYPE, float>::value
                                        ? OB_GCVT_ARG_FLOAT : OB_GCVT_ARG_DOUBLE;
            if (OB_FAIL(ObDataTypeCastUtil::common_floating_decimalint_wrap(
                          in_val, arg_type, tmp_alloc, decint, val_len, in_scale_, in_prec_))) {
              SQL_LOG(WARN, "common_floating_decimalint failed", K(ret), K(in_val));
            } else if (ObDatumCast::need_scale_decimalint(in_scale_, in_prec_, out_scale_, out_prec_)) {
              ObDecimalIntBuilder res_val;
              if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, val_len, in_scale_, out_scale_,
                                                               out_prec_, expr.extra_, res_val,
                                                               ctx_.exec_ctx_.get_user_logging_ctx()))) {
                SQL_LOG(WARN, "scale decimalint failed", K(ret));
              } else {
                res_vec_->set_payload(idx, res_val.get_decimal_int(), res_val.get_int_bytes());
              }
            } else {
              res_vec_->set_payload(idx, decint, val_len);
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      FloatDoubleToDecimalintFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  template<typename OUT_TYPE>
  static int number_decimalint(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class NumberToDecimalintFn : public CastFnBase {
      public:
        NumberToDecimalintFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          ObDecimalIntBuilder tmp_alloc;
          ObDecimalInt *decint = nullptr;
          int32_t int_bytes = 0;
          number::ObNumber nmb(arg_vec_->get_number(idx));
          in_scale_ = nmb.get_scale();
          int32_t out_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec_);
          if (OB_FAIL(wide::from_number(nmb, tmp_alloc, in_scale_, decint, int_bytes))) {
            SQL_LOG(WARN, "from_number failed", K(ret), K(out_scale_));
          } else if (ObDatumCast::need_scale_decimalint(in_scale_, int_bytes, out_scale_, out_bytes)) {
            ObDecimalIntBuilder res_val;
            if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, int_bytes, in_scale_, out_scale_, out_prec_,
                                                expr.extra_, res_val, ctx_.exec_ctx_.get_user_logging_ctx()))) {
              SQL_LOG(WARN, "scale decimal int failed", K(ret), K(in_scale_), K(out_scale_));
            } else {
              res_vec_->set_payload(idx, res_val.get_decimal_int(), res_val.get_int_bytes());
            }
          } else {
            res_vec_->set_payload(idx, decint, int_bytes);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      NumberToDecimalintFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  // 1. data to int
  // 2. use mul cast int to decimalint, and check overflow.
  template<typename OUT_TYPE>
  static int date_decimalint(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class DateToDecimalintFn : public CastFnBase {
      public:
        DateToDecimalintFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          int64_t out_val = 0;
          in_scale_ = 0;
          if (OB_FAIL(ObTimeConverter::date_to_int(arg_vec_->get_date(idx), out_val))) {
            SQL_LOG(WARN, "convert date to int failed", K(ret));
          } else if (CAST_FAIL(numeric_negative_check(out_val))) {
            SQL_LOG(WARN, "numeric_negative_check failed", K(ret), K(out_val));
          } else {
            ObDecimalIntBuilder res_val;
            if (OB_FAIL(ObDatumCast::common_scale_decimalint(reinterpret_cast<const ObDecimalInt *>(&out_val),
                                                             8, in_scale_, out_scale_, out_prec_,
                                                             expr.extra_, res_val,
                                                             ctx_.exec_ctx_.get_user_logging_ctx()))) {
              SQL_LOG(WARN, "scale decimal int failed", K(ret), K(in_scale_), K(out_scale_));
            } else {
              res_vec_->set_payload(idx, res_val.get_decimal_int(), res_val.get_int_bytes());
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      if (out_scale < 0) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid output scale", K(ret), K(out_scale));
      } else {
        DateToDecimalintFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
        if (OB_FAIL(CastHelperImpl::batch_cast(
                        cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }

  // 1. datetime to ob_time
  // 2. ob_time to int
  // 3. use mul cast int to decimal, and check overflow.
  template<typename INT_TYPE>
  class DatetimeToDecimalintFn : public CastFnBase {
  public:
    DatetimeToDecimalintFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                            const ObTimeZoneInfo *tz_info)
        : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
          tz_info_(tz_info) {}

    OB_INLINE int operator() (const ObExpr &expr, int idx)
    {
      int ret = OB_SUCCESS;
      int64_t in_val = arg_vec_->get_datetime(idx);
      INT_TYPE out_val = 0;
      ObTime ob_time;
      const int32_t *parts = ob_time.parts_;
      ObTimeConverter::round_datetime(in_scale_, in_val);
      if (OB_UNLIKELY(in_scale_ > 6)) {
        ret = OB_INVALID_ARGUMENT;
        SQL_LOG(WARN, "Invalid argument", K(ret), K(in_scale_));
      } else if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(in_val, tz_info_, ob_time))) {
        SQL_LOG(WARN, "failed to convert seconds to int", K(ret));
      } else {
        out_val = parts[DT_YEAR] * CastHelperImpl::pow10(4)
                + parts[DT_MON] * CastHelperImpl::pow10(2)
                + parts[DT_MDAY];
        out_val = out_val * CastHelperImpl::pow10(6);
        out_val += (parts[DT_HOUR] * CastHelperImpl::pow10(4)
                  + parts[DT_MIN] * CastHelperImpl::pow10(2)
                  + parts[DT_SEC]);
        if (!std::is_same<INT_TYPE, int64_t>::value) {
          out_val = out_val * CastHelperImpl::pow10(in_scale_);
          out_val += parts[DT_USEC] / CastHelperImpl::pow10(6 - in_scale_);
        }
        ObDecimalIntBuilder res_val;
        if (OB_FAIL(ObDatumCast::common_scale_decimalint(reinterpret_cast<const ObDecimalInt *>(&out_val),
                                                          sizeof(INT_TYPE), in_scale_, out_scale_, out_prec_,
                                                          expr.extra_, res_val,
                                                          ctx_.exec_ctx_.get_user_logging_ctx()))) {
          SQL_LOG(WARN, "scale decimal int failed", K(ret), K(in_scale_), K(out_scale_));
        } else {
          res_vec_->set_payload(idx, res_val.get_decimal_int(), res_val.get_int_bytes());
        }
      }
      return ret;
    }
  private:
    ArgVec *arg_vec_;
    ResVec *res_vec_;
    const ObTimeZoneInfo *tz_info_;
  };

  template<typename OUT_TYPE>
  static int datetime_decimalint(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
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
        const ObTimeZoneInfo *tz_info = (ObTimestampType == in_type)
                                         ? tz_info_local : NULL;
        if (out_scale < 0) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "invalid output scale", K(ret), K(out_scale));
        } else {
          if (0 == in_scale) {
            DatetimeToDecimalintFn<int64_t> cast_fn(CAST_ARG_DECL, arg_vec, res_vec, tz_info);
            if (OB_FAIL(CastHelperImpl::batch_cast(
                            cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
              SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
            }
          } else {
            DatetimeToDecimalintFn<int128_t> cast_fn(CAST_ARG_DECL, arg_vec, res_vec, tz_info);
            if (OB_FAIL(CastHelperImpl::batch_cast(
                            cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
              SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
            }
          }
        }
      }
    }
    return ret;
  }
};


// ================
#define DEF_FLOAT_TO_DECIMALINT_IMPLICIT_CASTS(in_tc)                              \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DEC_INT32)                           \
  {                                                                                \
    return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template                   \
              float_decimalint<RTCType<in_tc>, int32_t>(expr, ctx, skip, bound);   \
  }                                                                                \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DEC_INT64)                           \
  {                                                                                \
    return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template                   \
              float_decimalint<RTCType<in_tc>, int64_t>(expr, ctx, skip, bound);   \
  }                                                                                \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DEC_INT128)                          \
  {                                                                                \
    return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template                   \
              float_decimalint<RTCType<in_tc>, int128_t>(expr, ctx, skip, bound);  \
  }                                                                                \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DEC_INT256)                          \
  {                                                                                \
    return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template                   \
              float_decimalint<RTCType<in_tc>, int256_t>(expr, ctx, skip, bound);  \
  }                                                                                \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DEC_INT512)                          \
  {                                                                                \
    return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template                   \
              float_decimalint<RTCType<in_tc>, int512_t>(expr, ctx, skip, bound);  \
  }

LST_DO_CODE(DEF_FLOAT_TO_DECIMALINT_IMPLICIT_CASTS,
            VEC_TC_FLOAT,
            VEC_TC_DOUBLE
            );

// date -> decimal_int
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATE, VEC_TC_DEC_INT32)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            date_decimalint<int32_t>(expr, ctx, skip, bound);               \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATE, VEC_TC_DEC_INT64)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            date_decimalint<int64_t>(expr, ctx, skip, bound);               \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATE, VEC_TC_DEC_INT128)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            date_decimalint<int128_t>(expr, ctx, skip, bound);              \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATE, VEC_TC_DEC_INT256)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            date_decimalint<int256_t>(expr, ctx, skip, bound);              \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATE, VEC_TC_DEC_INT512)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            date_decimalint<int512_t>(expr, ctx, skip, bound);              \
}

// datetime -> decimal_int
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATETIME, VEC_TC_DEC_INT32)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            datetime_decimalint<int32_t>(expr, ctx, skip, bound);           \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATETIME, VEC_TC_DEC_INT64)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            datetime_decimalint<int64_t>(expr, ctx, skip, bound);           \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATETIME, VEC_TC_DEC_INT128)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            datetime_decimalint<int128_t>(expr, ctx, skip, bound);          \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATETIME, VEC_TC_DEC_INT256)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            datetime_decimalint<int256_t>(expr, ctx, skip, bound);          \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATETIME, VEC_TC_DEC_INT512)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            datetime_decimalint<int512_t>(expr, ctx, skip, bound);          \
}

// number -> decimal_int
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_NUMBER, VEC_TC_DEC_INT32)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            number_decimalint<int32_t>(expr, ctx, skip, bound);             \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_NUMBER, VEC_TC_DEC_INT64)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            number_decimalint<int64_t>(expr, ctx, skip, bound);             \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_NUMBER, VEC_TC_DEC_INT128)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            number_decimalint<int128_t>(expr, ctx, skip, bound);            \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_NUMBER, VEC_TC_DEC_INT256)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            number_decimalint<int256_t>(expr, ctx, skip, bound);            \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_NUMBER, VEC_TC_DEC_INT512)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            number_decimalint<int512_t>(expr, ctx, skip, bound);            \
}

// string -> decimal_int
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_STRING, VEC_TC_DEC_INT32)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            string_decimalint<int32_t>(expr, ctx, skip, bound);             \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_STRING, VEC_TC_DEC_INT64)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            string_decimalint<int64_t>(expr, ctx, skip, bound);             \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_STRING, VEC_TC_DEC_INT128)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            string_decimalint<int128_t>(expr, ctx, skip, bound);            \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_STRING, VEC_TC_DEC_INT256)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            string_decimalint<int256_t>(expr, ctx, skip, bound);            \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_STRING, VEC_TC_DEC_INT512)
{                                                                           \
  return ToDecimalintCastImpl<IN_VECTOR, OUT_VECTOR>::template              \
            string_decimalint<int512_t>(expr, ctx, skip, bound);            \
}

} // end sql
} // namespace oceanbase
