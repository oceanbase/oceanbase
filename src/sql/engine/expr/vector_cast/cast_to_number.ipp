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
struct ToNumberCastImpl
{
  static int number_number(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class NumberToNumberFn : public CastFnBase {
      public:
        NumberToNumberFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          number::ObNumber nmb(arg_vec_->get_number(idx));
          if (ObUNumberType == out_type_) {
            if (CAST_FAIL(numeric_negative_check(nmb))) {
              SQL_LOG(WARN, "numeric_negative_check failed", K(ret));
            } else {
              res_vec_->set_number(idx, nmb);
            }
          } else {  // ObNumberType
            res_vec_->set_number(idx, nmb);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      NumberToNumberFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  static int string_number(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class StringToNumberFn : public CastFnBase {
      public:
        StringToNumberFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          ObLength in_len = arg_vec_->get_length(idx);
          if (lib::is_oracle_mode() && 0 == in_len) {
            res_vec_->set_null(idx);
          } else {
            number::ObNumber nmb;
            ObNumStackOnceAlloc tmp_alloc;
            ObString in_str(in_len, arg_vec_->get_payload(idx));
            if (OB_FAIL(ObDataTypeCastUtil::common_string_number_wrap(expr, in_str, tmp_alloc, nmb))) {
              SQL_LOG(WARN, "common_string_number_wrap failed", K(ret), K(in_str), K(tmp_alloc), K(nmb));
            } else {
              res_vec_->set_number(idx, nmb);
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      StringToNumberFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  template<typename IN_TYPE>
  static int float_number(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class FloatDoubleToNumberFn : public CastFnBase {
      public:
        FloatDoubleToNumberFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec_->get_payload(idx));
          bool nan_or_inf_is_invalid = std::is_same<IN_TYPE, float>::value || lib::is_oracle_mode();
          if (isnan(in_val) && nan_or_inf_is_invalid) {
            ret = OB_INVALID_NUMERIC;
            SQL_LOG(WARN, "float_number failed ", K(ret), K(in_val));
          } else if (isinf(in_val) && nan_or_inf_is_invalid) {
            ret = OB_NUMERIC_OVERFLOW;
            SQL_LOG(WARN, "float_number failed", K(ret), K(in_val));
          } else {
            int warning = OB_SUCCESS;
            ObNumStackOnceAlloc tmp_alloc;
            number::ObNumber number;
            ob_gcvt_arg_type arg_type = std::is_same<IN_TYPE, float>::value
                                        ? OB_GCVT_ARG_FLOAT : OB_GCVT_ARG_DOUBLE;
            if (ObUNumberType == out_type_ && CAST_FAIL(numeric_negative_check(in_val))) {
              SQL_LOG(WARN, "numeric_negative_check failed", K(ret), K(out_type_), K(in_val));
            } else if (OB_FAIL(ObDataTypeCastUtil::common_floating_number_wrap(in_val,
                                                                               arg_type,
                                                                               tmp_alloc,
                                                                               number,
                                                                               ctx_,
                                                                               expr.extra_))) {
              SQL_LOG(WARN, "common_float_number failed", K(ret), K(in_val));
            } else {
              res_vec_->set_number(idx, number);
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      FloatDoubleToNumberFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  template<typename IN_TYPE>
  static int int_number(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class IntUIntToNumberFn : public CastFnBase {
      public:
        IntUIntToNumberFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          number::ObNumber nmb;
          ObNumStackOnceAlloc tmp_alloc;
          IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec_->get_payload(idx));
          if (std::is_same<IN_TYPE, int64_t>::value && (ObUNumberType == out_type_)
              && CAST_FAIL(numeric_negative_check(in_val))) {
            SQL_LOG(WARN, "numeric_negative_check faield", K(ret), K(in_val));
          } else if (OB_FAIL(nmb.from(in_val, tmp_alloc))) {
            SQL_LOG(WARN, "nmb.from failed", K(ret), K(in_val));
          } else {
            res_vec_->set_number(idx, nmb);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      IntUIntToNumberFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  static int date_number(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class DateToNumberFn : public CastFnBase {
      public:
        DateToNumberFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          number::ObNumber nmb;
          ObNumStackOnceAlloc tmp_alloc;
          int64_t out_val = 0;
          if (OB_FAIL(ObTimeConverter::date_to_int(arg_vec_->get_date(idx), out_val))) {
            SQL_LOG(WARN, "convert date to int failed", K(ret));
          } else if (CAST_FAIL(numeric_negative_check(out_val))) {
            SQL_LOG(WARN, "numeric_negative_check failed", K(ret), K(out_val));
          } else if (OB_FAIL(nmb.from(out_val, tmp_alloc))) {
            SQL_LOG(WARN, "number.from failed", K(ret), K(out_val));
          } else {
            res_vec_->set_number(idx, nmb);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      DateToNumberFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  static int datetime_number(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
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
        class DatetimeToNumberFn : public CastFnBase {
        public:
          DatetimeToNumberFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                                  const ObTimeZoneInfo *tz_info)
              : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
                tz_info_(tz_info) {}

          OB_INLINE int operator() (const ObExpr &expr, int idx)
          {
            int ret = OB_SUCCESS;
            int warning = OB_SUCCESS;
            int64_t len = 0;
            ObString nls_format;
            ObNumStackOnceAlloc tmp_alloc;
            number::ObNumber number;
            if (OB_FAIL(ObTimeConverter::datetime_to_str(arg_vec_->get_int(idx), tz_info_, nls_format,
                          in_scale_, buf_, sizeof(buf_), len, false))) {
              SQL_LOG(WARN, "failed to convert datetime to string", K(ret));
            } else if (CAST_FAIL(number.from(buf_, len, tmp_alloc, &res_precision_, &res_scale_))) {
              SQL_LOG(WARN, "failed to convert string to number", K(ret));
            } else {
              res_vec_->set_number(idx, number);
            }
            return ret;
          }
        private:
          ArgVec *arg_vec_;
          ResVec *res_vec_;
          const ObTimeZoneInfo *tz_info_;
          ObPrecision res_precision_; // useless
          ObScale res_scale_; // useless
          char buf_[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
        };

        const ObTimeZoneInfo *tz_info = (ObTimestampType == in_type) ?
                                          tz_info_local : NULL;
        DatetimeToNumberFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, tz_info);
        if (OB_FAIL(CastHelperImpl::batch_cast(
                        cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }
};

// ================
// define implicit cast functions
#define DEF_INTEGER_TO_NUMBER_IMPLICIT_CASTS(in_tc)                 \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_NUMBER)               \
  {                                                                 \
    return ToNumberCastImpl<IN_VECTOR, OUT_VECTOR>::template   \
              int_number<RTCType<in_tc>>(expr, ctx, skip, bound);   \
  }

LST_DO_CODE(DEF_INTEGER_TO_NUMBER_IMPLICIT_CASTS,
            VEC_TC_INTEGER,
            VEC_TC_UINTEGER
            );

#define DEF_FLOAT_TO_NUMBER_IMPLICIT_CASTS(in_tc)                   \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_NUMBER)               \
  {                                                                 \
    return ToNumberCastImpl<IN_VECTOR, OUT_VECTOR>::template        \
              float_number<RTCType<in_tc>>(expr, ctx, skip, bound); \
  }

LST_DO_CODE(DEF_FLOAT_TO_NUMBER_IMPLICIT_CASTS,
            VEC_TC_FLOAT,
            VEC_TC_DOUBLE
            );

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATE, VEC_TC_NUMBER)
{                                                                   \
  return ToNumberCastImpl<IN_VECTOR, OUT_VECTOR>::                  \
            date_number(expr, ctx, skip, bound);                    \
}

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATETIME, VEC_TC_NUMBER)
{                                                                   \
  return ToNumberCastImpl<IN_VECTOR, OUT_VECTOR>::                  \
            datetime_number(expr, ctx, skip, bound);                \
}

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_STRING, VEC_TC_NUMBER)
{                                                                   \
  return ToNumberCastImpl<IN_VECTOR, OUT_VECTOR>::                  \
            string_number(expr, ctx, skip, bound);                  \
}

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_NUMBER, VEC_TC_NUMBER)
{                                                                   \
  return ToNumberCastImpl<IN_VECTOR, OUT_VECTOR>::                  \
          number_number(expr, ctx, skip, bound);                    \
}

} // end sql
} // namespace oceanbase

#undef DEF_INTEGER_TO_NUMBER_IMPLICIT_CASTS
#undef DEF_FLOAT_TO_NUMBER_IMPLICIT_CASTS
