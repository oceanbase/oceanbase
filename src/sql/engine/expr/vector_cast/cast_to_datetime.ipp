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
struct ToDatetimeCastImpl
{
  template<typename IN_TYPE>
  static int float_datetime(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session();
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "session is NULL", K(ret));
      } else if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        SQL_LOG(WARN, "get time zone info failed", K(ret));
      } else {
        class FloatDoubleToDatetimeFn : public CastFnBase {
        public:
          FloatDoubleToDatetimeFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                                        ObTimeConvertCtx cvrt_ctx)
              : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
                cvrt_ctx_(cvrt_ctx) {}

          OB_INLINE int operator() (const ObExpr &expr, int idx)
          {
            int ret = OB_SUCCESS;
            int warning = OB_SUCCESS;
            ObNumStackOnceAlloc tmp_alloc;
            number::ObNumber number;
            int64_t res_val = 0;
            IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec_->get_payload(idx));
            double dbl = static_cast<double>(in_val);
            if (OB_FAIL(ObDataTypeCastUtil::common_floating_number_wrap(dbl,
                                                                        OB_GCVT_ARG_DOUBLE,
                                                                        tmp_alloc,
                                                                        number,
                                                                        ctx_,
                                                                        expr.extra_))) {
              SQL_LOG(WARN, "cast float to number failed", K(ret), K(expr.extra_));
              if (CM_IS_WARN_ON_FAIL(expr.extra_)) {
                ret = OB_SUCCESS;
                if (CM_IS_ZERO_ON_WARN(expr.extra_)) {
                  res_vec_->set_datetime(idx, ObTimeConverter::ZERO_DATETIME);
                } else {
                  res_vec_->set_null(idx);
                }
              } else {
                ret = OB_INVALID_DATE_VALUE;
              }
            } else {
              ret = ObDataTypeCastUtil::common_number_datetime_wrap(number, cvrt_ctx_,
                  res_val, expr.extra_, expr.datum_meta_.type_ == ObMySQLDateTimeType);
              if (CAST_FAIL(ret)) {
                SQL_LOG(WARN, "str_to_datetime failed", K(ret));
              } else {
                SET_RES_DATETIME(idx, res_val);
              }
            }
            return ret;
          }
        private:
          ArgVec *arg_vec_;
          ResVec *res_vec_;
          ObTimeConvertCtx cvrt_ctx_;
        };

        ObTimeConvertCtx cvrt_ctx(tz_info_local, ObTimestampType == out_type);
        FloatDoubleToDatetimeFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, cvrt_ctx);
        if (OB_FAIL(CastHelperImpl::batch_cast(
                        cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }

  static int int_datetime(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
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
        class IntToDatetimeFn : public CastFnBase {
        public:
          IntToDatetimeFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                               ObTimeConvertCtx cvrt_ctx, ObDateSqlMode date_sql_mode)
              : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
                cvrt_ctx_(cvrt_ctx), date_sql_mode_(date_sql_mode) {}

          OB_INLINE int operator() (const ObExpr &expr, int idx)
          {
            int ret = OB_SUCCESS, warning = OB_SUCCESS;
            int64_t res_val = 0;
            int64_t in_val = arg_vec_->get_int(idx);
            if (0 > in_val) {
              ret = OB_INVALID_DATE_FORMAT;
              SQL_LOG(WARN, "should cast positive int to datetime", K(ret));
            } else if (OB_FAIL(ObTimeConverter::int_to_datetime(in_val, 0, cvrt_ctx_, res_val, date_sql_mode_))) {
              SQL_LOG(WARN, "int_datetime failed", K(ret), K(in_val));
            }
            if (CAST_FAIL(ret)) {
              SQL_LOG(WARN, "int_datetime failed", K(ret));
            } else {
              SET_RES_DATETIME(idx, res_val);
            }
            return ret;
          }
        private:
          ArgVec *arg_vec_;
          ResVec *res_vec_;
          ObTimeConvertCtx cvrt_ctx_;
          ObDateSqlMode date_sql_mode_;
        };

        ObTimeConvertCtx cvrt_ctx(tz_info_local, ObTimestampType == out_type);
        ObDateSqlMode date_sql_mode;
        date_sql_mode.allow_invalid_dates_ = CM_IS_EXPLICIT_CAST(expr.extra_)
                                             ? false : CM_IS_ALLOW_INVALID_DATES(expr.extra_);
        date_sql_mode.no_zero_date_ = CM_IS_EXPLICIT_CAST(expr.extra_)
                                      ? false : CM_IS_NO_ZERO_DATE(expr.extra_);
        IntToDatetimeFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, cvrt_ctx, date_sql_mode);
        if (OB_FAIL(CastHelperImpl::batch_cast(
                        cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }

  static int uint_datetime(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
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
        class UIntToDatetimeFn : public CastFnBase {
        public:
          UIntToDatetimeFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                            ObTimeConvertCtx cvrt_ctx, ObDateSqlMode date_sql_mode)
              : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
                cvrt_ctx_(cvrt_ctx), date_sql_mode_(date_sql_mode) {}

          OB_INLINE int operator() (const ObExpr &expr, int idx)
          {
            int ret = OB_SUCCESS, warning = OB_SUCCESS;
            uint64_t in_val = arg_vec_->get_uint(idx);
            int64_t res_val = 0;
            if (OB_FAIL(ObDataTypeCastUtil::common_uint_int_wrap(expr, ObIntType, in_val, ctx_, res_val))) {
              SQL_LOG(WARN, "common_uint_int failed", K(ret));
            } else if (0 > res_val) {
              ret = OB_INVALID_DATE_FORMAT;
              SQL_LOG(WARN, "should cast positive int to datetime", K(ret));
            } else if (OB_FAIL(ObTimeConverter::int_to_datetime(in_val, 0, cvrt_ctx_, res_val, date_sql_mode_))) {
              SQL_LOG(WARN, "int_datetime failed", K(ret), K(res_val));
            }
            if (CAST_FAIL(ret)) {
              SQL_LOG(WARN, "int_datetime failed", K(ret));
            } else {
              SET_RES_DATETIME(idx, res_val);
            }
            return ret;
          }
        private:
          ArgVec *arg_vec_;
          ResVec *res_vec_;
          ObTimeConvertCtx cvrt_ctx_;
          ObDateSqlMode date_sql_mode_;
        };

        ObTimeConvertCtx cvrt_ctx(tz_info_local, ObTimestampType == out_type);
        ObDateSqlMode date_sql_mode;
        date_sql_mode.allow_invalid_dates_ = CM_IS_EXPLICIT_CAST(expr.extra_)
                                             ? false : CM_IS_ALLOW_INVALID_DATES(expr.extra_);
        date_sql_mode.no_zero_date_ = CM_IS_EXPLICIT_CAST(expr.extra_)
                                      ? false : CM_IS_NO_ZERO_DATE(expr.extra_);
        UIntToDatetimeFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, cvrt_ctx, date_sql_mode);
        if (OB_FAIL(CastHelperImpl::batch_cast(
                        cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }


  // 1. date * USECS_PER_DAY
  // 2. calc timezone offset
  static int date_datetime(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
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
      } else if (OB_FAIL(get_tz_offset(tz_info_local, tz_offset))) {
        SQL_LOG(WARN, "failed to get offset between utc and local", K(ret));
      } else {
          class DateToDatetimeFn : public CastFnBase {
          public:
            DateToDatetimeFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                             int64_t tz_offset)
                : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
                  tz_offset_(tz_offset) {}

            OB_INLINE int operator() (const ObExpr &expr, int idx)
            {
              int ret = OB_SUCCESS;
              int64_t dt_value = 0;
              int32_t d_value = arg_vec_->get_date(idx);
              if (ObTimeConverter::ZERO_DATE == d_value) {
                dt_value = ObTimeConverter::ZERO_DATETIME;
              } else {
                dt_value = d_value * USECS_PER_DAY;
                if (dt_value > DATETIME_MAX_VAL || dt_value < DATETIME_MIN_VAL) {
                  ret = OB_DATETIME_FUNCTION_OVERFLOW;
                  SQL_LOG(WARN, "datetime filed overflow", K(ret), K(dt_value));
                } else if (ObTimeConverter::ZERO_DATETIME != dt_value
                           || lib::is_oracle_mode()) {
                  ///1. ob_time.is_tz_name_valid_ == false
                  dt_value = dt_value - tz_offset_;
                }
              }
              if (OB_SUCC(ret)) {
                res_vec_->set_datetime(idx, dt_value);
              }
              return ret;
            }
          private:
            ArgVec *arg_vec_;
            ResVec *res_vec_;
            int64_t tz_offset_;
          };

          tz_offset = (ObTimestampType != out_type) ? 0 : tz_offset;
          DateToDatetimeFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, tz_offset);
          if (OB_FAIL(CastHelperImpl::batch_cast(
                          cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
            SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
          }
      }
    }
    return ret;
  }

  template <ObObjType IN_TYPE, ObObjType OUT_TYPE>
  class DatetimeToDatetimeFn : public CastFnBase {
  public:
    DatetimeToDatetimeFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                          const common::ObTimeZoneInfo *tz_info)
        : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
          tz_info_(tz_info) {}

    OB_INLINE int operator() (const ObExpr &expr, int idx)
    {
      int ret = OB_SUCCESS;
      int64_t in_val = arg_vec_->get_datetime(idx);
      int64_t out_val = in_val;
      if (ObDateTimeType == IN_TYPE && ObTimestampType == OUT_TYPE) {
        ret = ObTimeConverter::datetime_to_timestamp(in_val,
                                                      tz_info_,
                                                      out_val);
        ret = OB_ERR_UNEXPECTED_TZ_TRANSITION == ret
              ? OB_INVALID_DATE_VALUE : ret;
      } else if (ObTimestampType == IN_TYPE && ObDateTimeType == OUT_TYPE) {
        ret = ObTimeConverter::timestamp_to_datetime(out_val,
                                                      tz_info_,
                                                      out_val);
      }
      if (OB_FAIL(ret)) {
      } else {
        res_vec_->set_datetime(idx, out_val);
      }
      return ret;
    }
  private:
    ArgVec *arg_vec_;
    ResVec *res_vec_;
    const common::ObTimeZoneInfo *tz_info_;
  };

  #define DEF_DATETIME_CAST_FUNC(in_type, out_type)                               \
    DatetimeToDatetimeFn<in_type, out_type> cast_fn(CAST_ARG_DECL,                \
                                                    arg_vec,                      \
                                                    res_vec,                      \
                                                    tz_info_local);               \
    if (OB_FAIL(CastHelperImpl::batch_cast(                                       \
                    cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) { \
      SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));              \
    }


  static int datetime_datetime(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
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
        if (ObDateTimeType == in_type && ObDateTimeType == out_type) {
          DEF_DATETIME_CAST_FUNC(ObDateTimeType, ObDateTimeType)
        } else if (ObDateTimeType == in_type && ObTimestampType == out_type) {
          DEF_DATETIME_CAST_FUNC(ObDateTimeType, ObTimestampType)
        } else if (ObTimestampType == in_type && ObDateTimeType == out_type) {
          DEF_DATETIME_CAST_FUNC(ObTimestampType, ObDateTimeType)
        } else if (ObTimestampType == in_type && ObTimestampType == out_type) {
          DEF_DATETIME_CAST_FUNC(ObTimestampType, ObTimestampType)
        } else {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "type is error", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }

  // 1. use parse_decimalint_to_datetime() to convert decimal to int_part and dec_part, eg: 20010528 and 143031.
  // 2. 20010528 and 143031 record year-month-day and hour-minute-second to cast.
  template<typename IN_TYPE>
  static int decimalint_datetime(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session();
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "session is NULL", K(ret));
      } else if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        SQL_LOG(WARN, "get time zone info failed", K(ret));
      } else {
        class DecimalintToDatetimeFn : public CastFnBase {
        public:
          DecimalintToDatetimeFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                        ObTimeConvertCtx cvrt_ctx, ObDateSqlMode date_sql_mode, IN_TYPE sf)
              : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
                cvrt_ctx_(cvrt_ctx), date_sql_mode_(date_sql_mode), sf_(sf) {}

          OB_INLINE int operator() (const ObExpr &expr, int idx)
          {
            int ret = OB_SUCCESS;
            IN_TYPE lhs = *reinterpret_cast<const IN_TYPE *>(arg_vec_->get_payload(idx));
            int64_t out_val = 0;
            if (OB_UNLIKELY(lhs < 0)) {
              ret = OB_INVALID_DATE_VALUE;
              SQL_LOG(WARN, "invalid datetime value", K(ret), K(lhs));
            } else {
              int64_t int_part = 0;
              int64_t dec_part = 0;
              if (OB_FAIL(CastHelperImpl::parse_decimalint_to_datetime(lhs, sf_, in_scale_,
                                                                       expr.extra_, cvrt_ctx_,
                                                                       int_part, dec_part))) {
                SQL_LOG(WARN, "failed to get int and dec part", K(ret));
              } else if (OB_FAIL(ObTimeConverter::int_to_datetime(int_part, dec_part,
                                                        cvrt_ctx_, out_val, date_sql_mode_))) {
                SQL_LOG(WARN, "int to datetime failed", K(ret), K(int_part), K(dec_part));
              }
            }
            int warning = OB_SUCCESS;
            if (CAST_FAIL(ret)) {
            } else {
              SET_RES_DATETIME(idx, out_val);
              if (warning != OB_SUCCESS) { SQL_LOG(DEBUG, "cast decimalint to datetime", K(warning)); }
            }
            return ret;
          }
        private:
          ArgVec *arg_vec_;
          ResVec *res_vec_;
          ObTimeConvertCtx cvrt_ctx_;
          ObDateSqlMode date_sql_mode_;
          IN_TYPE sf_;
        };

        ObTimeConvertCtx cvrt_ctx(tz_info_local, ObTimestampType == out_type);
        ObDateSqlMode date_sql_mode;
        date_sql_mode.allow_invalid_dates_ = CM_IS_EXPLICIT_CAST(expr.extra_)
                                             ? false : CM_IS_ALLOW_INVALID_DATES(expr.extra_);
        date_sql_mode.no_zero_date_ = CM_IS_EXPLICIT_CAST(expr.extra_)
                                      ? false : CM_IS_NO_ZERO_DATE(expr.extra_);
        if (in_scale < 0) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "invalid input scale", K(ret), K(in_scale));
        } else {
          IN_TYPE sf = get_scale_factor<IN_TYPE>(in_scale);
          DecimalintToDatetimeFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, cvrt_ctx, date_sql_mode, sf);
          if (OB_FAIL(CastHelperImpl::batch_cast(
                          cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
            SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
          }
        }
      }
    }
    return ret;
  }

  static int number_datetime(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session();
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "session is NULL", K(ret));
      } else if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        SQL_LOG(WARN, "get time zone info failed", K(ret));
      } else {
        class NumberToDatetimeFn : public CastFnBase {
        public:
          NumberToDatetimeFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                                  ObTimeConvertCtx cvrt_ctx)
              : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
                cvrt_ctx_(cvrt_ctx) {}

          OB_INLINE int operator() (const ObExpr &expr, int idx)
          {
            int ret = OB_SUCCESS;
            int warning = OB_SUCCESS;
            int64_t out_val = 0;
            const number::ObNumber nmb(arg_vec_->get_number(idx));
            ret = ObDataTypeCastUtil::common_number_datetime_wrap(nmb, cvrt_ctx_,
                    out_val, expr.extra_, expr.datum_meta_.type_ == ObMySQLDateTimeType);
            if (CAST_FAIL(ret)) {
            } else {
              SET_RES_DATETIME(idx, out_val);
            }
            return ret;
          }
        private:
          ArgVec *arg_vec_;
          ResVec *res_vec_;
          ObTimeConvertCtx cvrt_ctx_;
        };

        ObTimeConvertCtx cvrt_ctx(tz_info_local, ObTimestampType == out_type);
        NumberToDatetimeFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, cvrt_ctx);
        if (OB_FAIL(CastHelperImpl::batch_cast(
                        cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }

  static int string_datetime(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session();
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "session is NULL", K(ret));
      } else if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        SQL_LOG(WARN, "get time zone info failed", K(ret));
      } else {
        class StringToDatetimeFn : public CastFnBase {
        public:
          StringToDatetimeFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                          ObDateSqlMode date_sql_mode, ObTimeConvertCtx cvrt_ctx)
              : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
                date_sql_mode_(date_sql_mode), cvrt_ctx_(cvrt_ctx) {}

          OB_INLINE int operator() (const ObExpr &expr, int idx)
          {
            int ret = OB_SUCCESS;
            int warning = OB_SUCCESS;
            int64_t out_val = 0;
            ObString in_str(arg_vec_->get_string(idx));
            if (lib::is_oracle_mode()) {
              if (CAST_FAIL(ObTimeConverter::str_to_date_oracle(in_str, cvrt_ctx_, out_val))) {
                SQL_LOG(WARN, "str_to_date_oracle failed", K(ret));
              }
            } else { // mysql
              if (CAST_FAIL(ObTimeConverter::str_to_datetime(
                                in_str, cvrt_ctx_, out_val, NULL, date_sql_mode_))) {
                SQL_LOG(WARN, "str_to_datetime failed", K(ret), K(in_str));
              } else if (CM_IS_ERROR_ON_SCALE_OVER(expr.extra_)
                        &&  (out_val == ObTimeConverter::ZERO_DATE
                             || out_val == ObTimeConverter::ZERO_DATETIME)) {
                ret = OB_INVALID_DATE_VALUE;
                LOG_USER_ERROR(OB_INVALID_DATE_VALUE, in_str.length(), in_str.ptr(), "");
              }
            }
            if (OB_SUCC(ret)) {
              SET_RES_DATETIME(idx, out_val);
            }
            return ret;
          }
        private:
          ArgVec *arg_vec_;
          ResVec *res_vec_;
          ObDateSqlMode date_sql_mode_;
          ObTimeConvertCtx cvrt_ctx_;
        };

        ObDateSqlMode date_sql_mode;
        const ObCastMode cast_mode = expr.extra_;
        bool need_truncate = CM_IS_COLUMN_CONVERT(cast_mode)
                            ? CM_IS_TIME_TRUNCATE_FRACTIONAL(cast_mode) : false;
        ObTimeConvertCtx cvrt_ctx(tz_info_local, ObTimestampType == out_type, need_truncate);
        if (lib::is_oracle_mode()) {
          if (OB_FAIL(common_get_nls_format(session, ctx, &expr, out_type,
                                            CM_IS_FORCE_USE_STANDARD_NLS_FORMAT(cast_mode),
                                            cvrt_ctx.oracle_nls_format_))) {
            SQL_LOG(WARN, "common_get_nls_format failed", K(ret));
          }
        } else {
          date_sql_mode.allow_invalid_dates_ = CM_IS_ALLOW_INVALID_DATES(cast_mode);
          date_sql_mode.no_zero_date_ = CM_IS_NO_ZERO_DATE(cast_mode);
        }
        if (OB_SUCC(ret)) {
          StringToDatetimeFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, date_sql_mode, cvrt_ctx);
          if (OB_FAIL(CastHelperImpl::batch_cast(
                          cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
            SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
          }
        }
      }
    }
    return ret;
  }
};

// ================
// define implicit cast functions
#define DEF_FLOAT_TO_DATETIME_IMPLICIT_CASTS(in_tc)                                  \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DATETIME)                              \
  {                                                                                  \
    return ToDatetimeCastImpl<IN_VECTOR, OUT_VECTOR>::template                  \
              float_datetime<RTCType<in_tc>>(expr, ctx, skip, bound);       \
  }

LST_DO_CODE(DEF_FLOAT_TO_DATETIME_IMPLICIT_CASTS,
            VEC_TC_FLOAT,
            VEC_TC_DOUBLE
            );

#define DEF_DECIMALINT_TO_DATETIME_IMPLICIT_CASTS(in_tc)                             \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DATETIME)                              \
  {                                                                                  \
    return ToDatetimeCastImpl<IN_VECTOR, OUT_VECTOR>::template                  \
              decimalint_datetime<RTCType<in_tc>>(expr, ctx, skip, bound);  \
  }

LST_DO_CODE(DEF_DECIMALINT_TO_DATETIME_IMPLICIT_CASTS,
            VEC_TC_DEC_INT32,
            VEC_TC_DEC_INT64,
            VEC_TC_DEC_INT128,
            VEC_TC_DEC_INT256,
            VEC_TC_DEC_INT512
            );

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_INTEGER, VEC_TC_DATETIME)
{                                                                                 \
  return ToDatetimeCastImpl<IN_VECTOR, OUT_VECTOR>::                         \
            int_datetime(expr, ctx, skip, bound);                                 \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_UINTEGER, VEC_TC_DATETIME)
{                                                                                 \
  return ToDatetimeCastImpl<IN_VECTOR, OUT_VECTOR>::                         \
            uint_datetime(expr, ctx, skip, bound);                                \
}

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATETIME, VEC_TC_DATETIME)
{                                                                                  \
  return ToDatetimeCastImpl<IN_VECTOR, OUT_VECTOR>::                          \
            datetime_datetime(expr, ctx, skip, bound);                             \
}

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_STRING, VEC_TC_DATETIME)
{                                                                                  \
  return ToDatetimeCastImpl<IN_VECTOR, OUT_VECTOR>::                          \
            string_datetime(expr, ctx, skip, bound);\
}

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATE, VEC_TC_DATETIME)
{                                                                                  \
  return ToDatetimeCastImpl<IN_VECTOR, OUT_VECTOR>::\
            date_datetime(expr, ctx, skip, bound);\
}

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_NUMBER, VEC_TC_DATETIME)
{                                                                                  \
  return ToDatetimeCastImpl<IN_VECTOR, OUT_VECTOR>::\
            number_datetime(expr, ctx, skip, bound);\
}

} // end sql
} // namespace oceanbase

#undef DEF_INTEGER_TO_DATETIME_IMPLICIT_CASTS
#undef DEF_FLOAT_TO_DATETIME_IMPLICIT_CASTS
#undef DEF_DECIMALINT_TO_DATETIME_IMPLICIT_CASTS
