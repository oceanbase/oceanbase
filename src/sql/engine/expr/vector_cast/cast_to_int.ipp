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
#include "share/object/ob_obj_cast.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
template<typename ArgVec, typename ResVec>
struct ToIntegerCastImpl
{
  static int string_int(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class StringToIntFn : public CastFnBase {
      public:
        StringToIntFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                      bool support_avx512)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
              support_avx512_(support_avx512) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          int64_t out_val = 0;
          const ObCastMode cast_mode = expr.extra_;
          ObString in_str(arg_vec_->get_string(idx));
          ObLength str_length = in_str.length();
          if (lib::is_oracle_mode() && 0 == str_length) {
            res_vec_->set_null(idx);
          } else {
            bool can_fast_strtoll = false;
            if (ObHexStringType != in_type_) {
              if ((str_length < 5 || str_length > 16) && str_length <= 20) {
                out_val = CastHelperImpl::fast_strtoll(in_str, CastHelperImpl::INT_TC_PRECISION, can_fast_strtoll);
              } else {
#if OB_USE_MULTITARGET_CODE
                if (support_avx512_) {
                  if (str_length >= 5 && str_length <= 16) {
                    can_fast_strtoll = specific::avx512::parse_16_chars(in_str.ptr(), in_str.length(), out_val);
                  }
                } else {
                  // do nothing
                }
#endif
              }
            }
            if (!can_fast_strtoll) {  // 退化回原路径
              ret = common_string_integer(cast_mode, in_type_, in_cs_type_, in_str, true, out_val);
            }
            if (CAST_FAIL(ret)) {
              SQL_LOG(WARN, "string_int failed", K(ret), K(in_str));
            } else if (out_type_ < ObIntType && CAST_FAIL(int_range_check(out_type_, out_val, out_val))) {
              SQL_LOG(WARN, "int_range_check failed", K(ret));
            } else {
              SET_RES_INT(idx, out_val);
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
        bool support_avx512_;
      };

      bool support_avx512 = common::is_arch_supported(ObTargetArch::AVX512);
      StringToIntFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, support_avx512);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  static int string_uint(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class StringToUIntFn : public CastFnBase {
      public:
        StringToUIntFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                      bool support_avx512)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
              support_avx512_(support_avx512) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          uint64_t out_val = 0;
          const ObCastMode cast_mode = expr.extra_;
          ObString in_str(arg_vec_->get_string(idx));
          ObLength str_length = in_str.length();
          if (lib::is_oracle_mode() && 0 == str_length) {
            res_vec_->set_null(idx);
          } else {
            bool can_fast_strtoull = false;
            if (ObHexStringType != in_type_) {
              if (str_length >= 5 && str_length <= 16) {
#if OB_USE_MULTITARGET_CODE
                if (support_avx512_) {
                  int64_t int64 = 0;
                  can_fast_strtoull = specific::avx512::parse_16_chars(in_str.ptr(), in_str.length(), int64);
                  out_val = int64;  // if can_fast_strtoull for length <= 16, will not out_of_range for both int64 & uint64
                } else {
                  // do nothing
                }
#endif
              }
            }
            if (!can_fast_strtoull) {  // 退化回原路径
              ret = common_string_unsigned_integer(cast_mode, in_type_, in_cs_type_, in_str, true, out_val);
            }
            if (CAST_FAIL(ret)) {
              SQL_LOG(WARN, "string_int failed", K(ret));
            } else if (out_type_ < ObUInt64Type && CM_NEED_RANGE_CHECK(expr.extra_) &&
                       CAST_FAIL(uint_upper_check(out_type_, out_val))) {
              SQL_LOG(WARN, "uint_upper_check failed", K(ret));
            } else {
              SET_RES_UINT(idx, out_val);
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
        bool support_avx512_;
      };

      bool support_avx512 = common::is_arch_supported(ObTargetArch::AVX512);
      StringToUIntFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, support_avx512);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }
  template<typename IN_TYPE>
  static int float_int(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class FloatDoubleToIntFn : public CastFnBase {
      public:
        FloatDoubleToIntFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec_->get_payload(idx));
          int64_t out_val = 0;
          const int64_t trunc_max_value = (std::is_same<IN_TYPE, double>::value || CM_IS_COLUMN_CONVERT(expr.extra_))
                                           ? LLONG_MAX : LLONG_MIN;
          if (CAST_FAIL(ObDataTypeCastUtil::common_double_int_wrap(in_val, out_val, LLONG_MIN, trunc_max_value))) {
            SQL_LOG(WARN, "common_double_int_wrap failed", K(ret));
          } else if ((CM_NEED_RANGE_CHECK(expr.extra_) || std::is_same<IN_TYPE, float>::value)
                     && CAST_FAIL(int_range_check(out_type_, out_val, out_val))) {
            SQL_LOG(WARN, "int_range_check failed", K(ret), K(out_val));
          } else {
            res_vec_->set_int(idx, out_val);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      FloatDoubleToIntFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  template<typename IN_TYPE>
  static int float_uint(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class FloatDoubleToUIntFn : public CastFnBase {
      public:
        FloatDoubleToUIntFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec_->get_payload(idx));
          uint64_t out_val = 0;
          if (in_val <= static_cast<double>(LLONG_MIN)) {
            out_val = static_cast<uint64_t>(LLONG_MIN);
            ret = OB_DATA_OUT_OF_RANGE;
          } else if (in_val >= static_cast<double>(ULLONG_MAX)) {
            out_val = std::is_same<IN_TYPE, float>::value
                      ? static_cast<uint64_t>(LLONG_MIN) : static_cast<uint64_t>(LLONG_MAX);
            ret = OB_DATA_OUT_OF_RANGE;
          } else {
            if (CM_IS_COLUMN_CONVERT(expr.extra_)) {
              out_val = static_cast<uint64_t>(rint(in_val));
            } else if (std::is_same<IN_TYPE, double>::value && in_val >= static_cast<double>(LLONG_MAX)) {
              out_val = static_cast<uint64_t>(LLONG_MAX);
            } else {
              out_val = static_cast<uint64_t>(static_cast<int64_t>(rint(in_val)));
            }
            if (in_val < 0 && out_val != 0) {
              // 这里处理[LLONG_MIN, 0)范围内的in，转换为unsigned应该报OB_DATA_OUT_OF_RANGE。
              // out不等于0避免[-0.5, 0)内的值被误判，因为它们round后的值是0，处于合法范围内。
              ret = OB_DATA_OUT_OF_RANGE;
            }
          }
          if (CAST_FAIL(ret)) {
            SQL_LOG(WARN, "cast float to uint failed", K(ret), K(in_val), K(out_val));
          } else if (CAST_FAIL(uint_range_check(out_type_, out_val, out_val))) {
            SQL_LOG(WARN, "int_range_check failed", K(ret));
          } else {
            res_vec_->set_uint(idx, out_val);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      FloatDoubleToUIntFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  static int int_int(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class IntToIntFn : public CastFnBase {
      public:
        IntToIntFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          int64_t in_val = *reinterpret_cast<const int64_t*>(arg_vec_->get_payload(idx));
          int64_t out_val = in_val;
          if (in_type_ > out_type_ && CAST_FAIL(int_range_check(out_type_, in_val, out_val))) {
            SQL_LOG(WARN, "int_range_check failed", K(ret), K(out_type_), K(out_val));
          } else {
            res_vec_->set_int(idx, out_val);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      IntToIntFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  static int int_uint(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class IntToUIntFn : public CastFnBase {
      public:
        IntToUIntFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          int64_t in_val = *reinterpret_cast<const int64_t*>(arg_vec_->get_payload(idx));
          uint64_t out_val = in_val;
          if (CM_NEED_RANGE_CHECK(expr.extra_) && CAST_FAIL(uint_range_check(out_type_, in_val, out_val))) {
            SQL_LOG(WARN, "int_range_check failed", K(ret), K(out_type_), K(out_val));
          } else {
            res_vec_->set_uint(idx, out_val);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      if (CM_SKIP_CAST_INT_UINT(expr.extra_)) {
        SQL_LOG(DEBUG, "skip cast int uint", K(ret));
      } else {
        IntToUIntFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
        if (OB_FAIL(CastHelperImpl::batch_cast(
                        cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }

  static int uint_int(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class UIntToIntFn : public CastFnBase {
      public:
        UIntToIntFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS, warning = OB_SUCCESS;
          uint64_t in_val = *reinterpret_cast<const uint64_t*>(arg_vec_->get_payload(idx));
          int64_t out_val = in_val;
          if (CM_NEED_RANGE_CHECK(expr.extra_) && CAST_FAIL(int_upper_check(out_type_, in_val, out_val))) {
            SQL_LOG(WARN, "int_range_check failed", K(ret), K(out_type_), K(out_val));
          } else {
            res_vec_->set_int(idx, out_val);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      UIntToIntFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  static int uint_uint(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class UIntToUIntFn : public CastFnBase {
      public:
        UIntToUIntFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS, warning = OB_SUCCESS;
          uint64_t in_val = *reinterpret_cast<const uint64_t*>(arg_vec_->get_payload(idx));
          uint64_t out_val = in_val;
          if (in_type_ > out_type_ && CAST_FAIL(uint_upper_check(out_type_, out_val))) {
            SQL_LOG(WARN, "int_range_check failed", K(ret), K(out_type_), K(out_val));
          } else {
            res_vec_->set_uint(idx, out_val);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      UIntToUIntFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  template<typename OUT_TYPE>
  static int date_int(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class DateToIntUIntFn : public CastFnBase {
      public:
        DateToIntUIntFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                              OUT_TYPE type_min_val, OUT_TYPE type_max_val)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
              type_min_val_(type_min_val), type_max_val_(type_max_val) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          int64_t int_val = 0;
          if (OB_FAIL(ObTimeConverter::date_to_int(arg_vec_->get_date(idx), int_val))) {
            SQL_LOG(WARN, "convert date to int failed", K(ret));
          } else {
            OUT_TYPE out_val = int_val;
            bool need_range_check = std::is_same<OUT_TYPE, int64_t>::value
                                    ? out_type_ < ObInt32Type : out_type_ < ObUInt32Type;
            if (need_range_check && CAST_FAIL(numeric_range_check(out_val, type_min_val_,
                                                                  type_max_val_, out_val))) {
              SQL_LOG(WARN, "int_range_check failed", K(ret));
            } else {
              res_vec_->set_payload(idx, &out_val, sizeof(OUT_TYPE));
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
        OUT_TYPE type_min_val_;
        OUT_TYPE type_max_val_;
      };

      bool is_signed = std::is_same<OUT_TYPE, int64_t>::value;
      const OUT_TYPE type_max_val = is_signed ? INT_MAX_VAL[out_type] : UINT_MAX_VAL[out_type];
      const OUT_TYPE type_min_val = is_signed ? INT_MIN_VAL[out_type] : static_cast<uint64_t>(0);
      DateToIntUIntFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, type_min_val, type_max_val);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  template<typename OUT_TYPE>
  static int datetime_int(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
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
        class DatetimeToIntUIntFn : public CastFnBase {
        public:
          DatetimeToIntUIntFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                              OUT_TYPE type_min_val, OUT_TYPE type_max_val, const ObTimeZoneInfo *tz_info)
              : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
                type_min_val_(type_min_val), type_max_val_(type_max_val), tz_info_(tz_info) {}

          OB_INLINE int operator() (const ObExpr &expr, int idx)
          {
            int ret = OB_SUCCESS;
            int warning = OB_SUCCESS;
            int64_t int_val = 0;
            if (OB_FAIL(ObTimeConverter::datetime_to_int(arg_vec_->get_datetime(idx),
                                                         tz_info_, int_val))) {
              SQL_LOG(WARN, "datetime_to_int failed", K(ret), K(int_val));
            } else {
              OUT_TYPE out_val = int_val;
              bool need_range_check = std::is_same<OUT_TYPE, int64_t>::value
                                      ? out_type_ < ObInt32Type : out_type_ < ObUInt32Type;
              if (need_range_check && CAST_FAIL(numeric_range_check(out_val, type_min_val_,
                                                                    type_max_val_, out_val))) {
                SQL_LOG(WARN, "int_range_check failed", K(ret));
              } else {
                res_vec_->set_payload(idx, &out_val, sizeof(OUT_TYPE));
              }
            }
            return ret;
          }
        private:
          ArgVec *arg_vec_;
          ResVec *res_vec_;
          OUT_TYPE type_min_val_;
          OUT_TYPE type_max_val_;
          const ObTimeZoneInfo *tz_info_;
        };

        const ObTimeZoneInfo *tz_info = (ObTimestampType == in_type) ?
                                          tz_info_local : NULL;
        bool is_signed = std::is_same<OUT_TYPE, int64_t>::value;
        const OUT_TYPE type_max_val = is_signed ? INT_MAX_VAL[out_type] : UINT_MAX_VAL[out_type];
        const OUT_TYPE type_min_val = is_signed ? INT_MIN_VAL[out_type] : static_cast<uint64_t>(0);
        DatetimeToIntUIntFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, type_min_val, type_max_val, tz_info);
        if (OB_FAIL(CastHelperImpl::batch_cast(
                        cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }

  // use divide cast decimalint to int, avoid cast to str.
  template<typename IN_TYPE>
  static int decimalint_int(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class DecimalintToIntFn : public CastFnBase {
      public:
        DecimalintToIntFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec, IN_TYPE sf)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec), sf_(sf) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          IN_TYPE lhs = *reinterpret_cast<const IN_TYPE *>(arg_vec_->get_payload(idx));
          int64_t out_val = CastHelperImpl::scale_to_integer(lhs, sf_, in_scale_);
          int warning = OB_SUCCESS;
          if (out_type_ < ObIntType && CAST_FAIL(int_range_check(out_type_, out_val, out_val))) {
            SQL_LOG(WARN, "int_range_check failed", K(ret), K(expr.extra_));
          } else {
            res_vec_->set_int(idx, out_val);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
        IN_TYPE sf_;
      };

      if (OB_UNLIKELY(in_scale < 0)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid input scale", K(ret), K(in_scale));
      } else {
        IN_TYPE sf = get_scale_factor<IN_TYPE>(in_scale);
        DecimalintToIntFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, sf);
        if (OB_FAIL(CastHelperImpl::batch_cast(
                        cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }

  template<typename IN_TYPE>
  static int decimalint_uint(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class DecimalintToUIntFn : public CastFnBase {
      public:
        DecimalintToUIntFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec, IN_TYPE sf)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec), sf_(sf) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          IN_TYPE lhs = *reinterpret_cast<const IN_TYPE *>(arg_vec_->get_payload(idx));
          uint64_t out_val = (lhs < 0) ? 0 :
                              CastHelperImpl::scale_to_unsigned_integer(lhs, sf_, in_scale_);
          int warning = OB_SUCCESS;
          if ((out_type_ < ObUInt64Type && CM_NEED_RANGE_CHECK(expr.extra_))
              && CAST_FAIL(uint_upper_check(out_type_, out_val))) {
            SQL_LOG(WARN, "int_range_check failed", K(ret), K(expr.extra_));
          } else {
            res_vec_->set_uint(idx, out_val);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
        IN_TYPE sf_;
      };

      if (OB_UNLIKELY(in_scale < 0)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid input scale", K(ret), K(in_scale));
      } else {
        IN_TYPE sf = get_scale_factor<IN_TYPE>(in_scale);
        DecimalintToUIntFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, sf);
        if (OB_FAIL(CastHelperImpl::batch_cast(
                        cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }

  static int number_int(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class NumberToIntFn : public CastFnBase {
      public:
        NumberToIntFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          const number::ObNumber nmb(arg_vec_->get_number(idx));
          const char *nmb_buf = nmb.format();
          if (OB_ISNULL(nmb_buf)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_LOG(WARN, "nmb_buf is NULL", K(ret));
          } else {
            int64_t out_val = 0;
            ObString num_str(strlen(nmb_buf), nmb_buf);
            ret = common_string_integer(expr.extra_, in_type_, in_cs_type_,
                                        num_str, false, out_val);
            if (CAST_FAIL(ret)) {
              SQL_LOG(WARN, "string_int failed", K(ret), K(num_str));
            } else if (out_type_ < ObIntType &&
                       CAST_FAIL(int_range_check(out_type_, out_val, out_val))) {
              SQL_LOG(WARN, "int_range_check failed", K(ret));
            } else {
              SET_RES_INT(idx, out_val);
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      NumberToIntFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  static int number_uint(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class NumberToUIntFn : public CastFnBase {
      public:
        NumberToUIntFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int warning = OB_SUCCESS;
          const number::ObNumber nmb(arg_vec_->get_number(idx));
          const char *nmb_buf = nmb.format();
          if (OB_ISNULL(nmb_buf)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_LOG(WARN, "nmb_buf is NULL", K(ret));
          } else {
            uint64_t out_val = 0;
            ObString num_str(strlen(nmb_buf), nmb_buf);
            ret = common_string_unsigned_integer(expr.extra_, in_type_, in_cs_type_,
                                                 num_str, false, out_val);
            if (CAST_FAIL(ret)) {
              SQL_LOG(WARN, "string_int failed", K(ret));
            } else if (out_type_ < ObUInt64Type && CM_NEED_RANGE_CHECK(expr.extra_) &&
              CAST_FAIL(uint_upper_check(out_type_, out_val))) {
              SQL_LOG(WARN, "uint_upper_check failed", K(ret));
            } else {
              SET_RES_UINT(idx, out_val);
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      NumberToUIntFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
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
#define DEF_FLOAT_TO_INTEGER_IMPLICIT_CASTS(in_tc)                            \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_INTEGER)                        \
  {                                                                           \
    return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::template                 \
              float_int<RTCType<in_tc>>(expr, ctx, skip, bound);              \
  }                                                                           \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_UINTEGER)                       \
  {                                                                           \
    return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::template                 \
              float_uint<RTCType<in_tc>>(expr, ctx, skip, bound);             \
  }

LST_DO_CODE(DEF_FLOAT_TO_INTEGER_IMPLICIT_CASTS,
            VEC_TC_FLOAT,
            VEC_TC_DOUBLE
            );

#define DEF_DECIMALINT_TO_INTEGER_IMPLICIT_CASTS(in_tc)                       \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_INTEGER)                        \
  {                                                                           \
    return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::template                 \
              decimalint_int<RTCType<in_tc>>(expr, ctx, skip, bound);         \
  }                                                                           \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_UINTEGER)                       \
  {                                                                           \
    return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::template                 \
              decimalint_uint<RTCType<in_tc>>(expr, ctx, skip, bound);        \
  }

LST_DO_CODE(DEF_DECIMALINT_TO_INTEGER_IMPLICIT_CASTS,
            VEC_TC_DEC_INT32,
            VEC_TC_DEC_INT64,
            VEC_TC_DEC_INT128,
            VEC_TC_DEC_INT256,
            VEC_TC_DEC_INT512
            );

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_STRING, VEC_TC_INTEGER)
{                                                                            \
  return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::                          \
            string_int(expr, ctx, skip, bound);                              \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_STRING, VEC_TC_UINTEGER)
{                                                                            \
  return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::                          \
            string_uint(expr, ctx, skip, bound);                             \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_INTEGER, VEC_TC_INTEGER)
{                                                                            \
  return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::                          \
            int_int(expr, ctx, skip, bound);                                 \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_INTEGER, VEC_TC_UINTEGER)
{                                                                            \
  return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::                          \
            int_uint(expr, ctx, skip, bound);                                \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_UINTEGER, VEC_TC_INTEGER)
{                                                                            \
  return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::                          \
            uint_int(expr, ctx, skip, bound);                                \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_UINTEGER, VEC_TC_UINTEGER)
{                                                                            \
  return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::                          \
            uint_uint(expr, ctx, skip, bound);                               \
}

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATE, VEC_TC_INTEGER)
{                                                                            \
  return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::template                  \
            date_int<int64_t>(expr, ctx, skip, bound);                       \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATE, VEC_TC_UINTEGER)
{                                                                            \
  return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::template                  \
            date_int<uint64_t>(expr, ctx, skip, bound);                      \
}

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATETIME, VEC_TC_INTEGER)
{                                                                            \
  return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::template                  \
            datetime_int<int64_t>(expr, ctx, skip, bound);                   \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATETIME, VEC_TC_UINTEGER)
{                                                                            \
  return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::template                  \
            datetime_int<uint64_t>(expr, ctx, skip, bound);                  \
}

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_NUMBER, VEC_TC_INTEGER)
{                                                                            \
  return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::                          \
            number_int(expr, ctx, skip, bound);                              \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_NUMBER, VEC_TC_UINTEGER)
{                                                                            \
  return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::                          \
            number_uint(expr, ctx, skip, bound);                             \
}


} // end sql
} // namespace oceanbase
