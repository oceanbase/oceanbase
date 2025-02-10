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
template<typename ArgVec, typename ResVec>
struct ToStringCastImpl
{
  template<typename IN_TYPE>
  static int float_string(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class FloatDoubleToStringFn : public CastFnBase {
      public:
        FloatDoubleToStringFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int64_t length = 0;
          IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec_->get_payload(idx));
          if (is_ieee754_nan_inf(in_val, buf_, length)) {
            SQL_LOG(DEBUG, "Infinity or NaN value is", K(in_val));
          } else { // float/double -> string
            int buf_len = sizeof(buf_);
            ob_gcvt_arg_type arg_type = std::is_same<IN_TYPE, float>::value
                                       ? OB_GCVT_ARG_FLOAT : OB_GCVT_ARG_DOUBLE;
            if (0 <= in_scale_) {
              length = ob_fcvt(in_val, in_scale_, buf_len - 1, buf_, NULL);
            } else {
              length = ob_gcvt_opt(
                  in_val, arg_type, static_cast<int32_t>(buf_len - 1), buf_, NULL, lib::is_oracle_mode(), TRUE);
            }
          }
          const ObCharsetInfo *cs = NULL;
          int64_t align_offset = 0;
          if (CS_TYPE_BINARY == in_cs_type_ && lib::is_mysql_mode()
              && (NULL != (cs = ObCharset::get_charset(out_cs_type_)))) {
            if (cs->mbminlen > 0 && length % cs->mbminlen != 0) {
              align_offset = cs->mbminlen - length % cs->mbminlen;
            }
          }
          char *out_ptr = NULL;
          int64_t out_len = 0;
          if (OB_FAIL(CastHelperImpl::vector_copy_string_zf(
                          expr, ctx_, idx, length, buf_, out_scale_, out_len, out_ptr, align_offset))) {
            SQL_LOG(WARN, "vector_copy_string_zf failed", K(ret), K(buf_));
          } else {
            res_vec_->set_string(idx, out_ptr, out_len);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
        char buf_[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
      };

      if (lib::is_oracle_mode() &&
          ob_is_blob(out_type, out_cs_type) && !ob_is_blob(in_type, in_cs_type)) {
        // !blob -> blob  // in_type != ObCharType && in_type != ObVarcharType
        ret = OB_NOT_SUPPORTED;
        SQL_LOG(ERROR, "invalid use of blob type", K(ret), K(out_type));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "cast to blob type");
      } else if (static_cast<uint8_t>(out_scale) <= 0) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected zf length", K(ret), K(out_scale), K(expr.datum_meta_.scale_));
      } else {
        FloatDoubleToStringFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
        if (OB_FAIL(CastHelperImpl::batch_cast(
                        cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }

  template<typename IN_TYPE>
  static int int_string(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class IntUIntToStringFn : public CastFnBase {
      public:
        IntUIntToStringFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          char *out_ptr = NULL;
          int64_t out_len = 0;
          IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec_->get_payload(idx));
          ObFastFormatInt ffi(in_val);
          if(OB_FAIL(CastHelperImpl::vector_copy_string_zf(
                        expr, ctx_, idx, ffi.length(), ffi.ptr(), out_scale_, out_len, out_ptr))) {
            SQL_LOG(WARN, "vector_ffi_copy_string_zf failed", K(ret));
          } else {
            res_vec_->set_string(idx, out_ptr, out_len);
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      if (static_cast<uint8_t>(out_scale) <= 0) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected zf length", K(ret), K(out_scale), K(expr.datum_meta_.scale_));
      } else {
        IntUIntToStringFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
        if (OB_FAIL(CastHelperImpl::batch_cast(
                        cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }

  static int date_string(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class DateToStringFn : public CastFnBase {
      public:
        DateToStringFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int32_t in_val = arg_vec_->get_date(idx);
          int64_t len = 0;
          if (OB_FAIL(ObTimeConverter::date_to_str(in_val, buf_, sizeof(buf_), len))) {
            SQL_LOG(WARN, "date_to_str failed", K(ret));
          } else {
            char *out_ptr = NULL;
            int64_t out_len = 0;
            if(OB_FAIL(CastHelperImpl::vector_copy_string(
                          expr, ctx_, idx, len, buf_, out_len, out_ptr))) {
              SQL_LOG(WARN, "vector_ffi_copy_string_zf failed", K(ret));
            } else {
              res_vec_->set_string(idx, out_ptr, out_len);
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
        char buf_[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
      };

      DateToStringFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  static int datetime_string(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class DatetimeToStringFn : public CastFnBase {
      public:
        DatetimeToStringFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int64_t len = 0;
          int64_t in_val = arg_vec_->get_int(idx);
          if (OB_FAIL(common_datetime_string(expr, in_type_, out_type_, in_scale_,
                                            CM_IS_FORCE_USE_STANDARD_NLS_FORMAT(expr.extra_),
                                            in_val, ctx_, buf_, sizeof(buf_), len))) {
            SQL_LOG(WARN, "common_datetime_string failed", K(ret));
          } else {
            char *out_ptr = NULL;
            int64_t out_len = 0;
            if(OB_FAIL(CastHelperImpl::vector_copy_string(
                          expr, ctx_, idx, len, buf_, out_len, out_ptr))) {
              SQL_LOG(WARN, "vector_ffi_copy_string_zf failed", K(ret));
            } else {
              res_vec_->set_string(idx, out_ptr, out_len);
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
        char buf_[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
      };

      DatetimeToStringFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }

  template<typename IN_TYPE>
  static int decimalint_string(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class DecimalintToStringFn : public CastFnBase {
      public:
        DecimalintToStringFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          int64_t length = 0;
          int buf_len = sizeof(buf_);
          const bool need_to_sci = lib::is_oracle_mode() && CM_IS_FORMAT_NUMBER_WITH_LIMIT(expr.extra_);
          if (OB_FAIL(wide::to_string(arg_vec_->get_decimal_int(idx), sizeof(IN_TYPE), in_scale_,
                                        buf_, buf_len, length, need_to_sci))) {
            SQL_LOG(WARN, "to_string failed", K(ret));
          } else {
            const ObCharsetInfo *cs = NULL;
            int64_t align_offset = 0;
            if (CS_TYPE_BINARY == in_cs_type_ && lib::is_mysql_mode()
                && (NULL != (cs = ObCharset::get_charset(out_cs_type_)))) {
              if (cs->mbminlen > 0 && length % cs->mbminlen != 0) {
                align_offset = cs->mbminlen - length % cs->mbminlen;
              }
            }
            char *out_ptr = NULL;
            int64_t out_len = 0;
            if (OB_FAIL(CastHelperImpl::vector_copy_string_zf(
                            expr, ctx_, idx, length, buf_, out_scale_, out_len, out_ptr, align_offset))) {
              SQL_LOG(WARN, "vector_copy_string_zf failed", K(ret), K(buf_));
            } else {
              res_vec_->set_string(idx, out_ptr, out_len);
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
        char buf_[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
      };

      if (lib::is_oracle_mode() &&
          (ob_is_blob(out_type, out_cs_type) || ob_is_blob_locator(out_type, out_cs_type))) {
        // !blob -> blob  // in_type != ObCharType && in_type != ObVarcharType
        ret = OB_NOT_SUPPORTED;
        SQL_LOG(ERROR, "invalid use of blob type", K(ret), K(out_type));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "cast to blob type");
      } else if (static_cast<uint8_t>(out_scale) <= 0) {  // out_scale == 0
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected zf length", K(ret), K(in_scale), K(out_scale));
      } else {
        DecimalintToStringFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
        if (OB_FAIL(CastHelperImpl::batch_cast(
                        cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }

  static int number_string(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class NumberToStringFn : public CastFnBase {
      public:
        NumberToStringFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          const number::ObNumber nmb(arg_vec_->get_number(idx));
          int64_t length = 0;
          if (lib::is_oracle_mode() && CM_IS_FORMAT_NUMBER_WITH_LIMIT(expr.extra_)) {
            if (OB_FAIL(nmb.format_with_oracle_limit(buf_, sizeof(buf_), length, in_scale_))) {
              SQL_LOG(WARN, "fail to format", K(ret), K(nmb));
            }
          } else {
            if (OB_FAIL(nmb.format(buf_, sizeof(buf_), length, in_scale_))) {
              SQL_LOG(WARN, "fail to format", K(ret), K(nmb));
            }
          }
          if (OB_SUCC(ret)) {
            const ObCharsetInfo *cs = NULL;
            int64_t align_offset = 0;
            if (CS_TYPE_BINARY == in_cs_type_ && lib::is_mysql_mode()
                && (NULL != (cs = ObCharset::get_charset(out_cs_type_)))) {
              if (cs->mbminlen > 0 && length % cs->mbminlen != 0) {
                align_offset = cs->mbminlen - length % cs->mbminlen;
              }
            }
            char *out_ptr = NULL;
            int64_t out_len = 0;
            if (OB_FAIL(CastHelperImpl::vector_copy_string_zf(
                            expr, ctx_, idx, length, buf_, out_scale_, out_len, out_ptr, align_offset))) {
              SQL_LOG(WARN, "vector_copy_string_zf failed", K(ret), K(buf_));
            } else {
              res_vec_->set_string(idx, out_ptr, out_len);
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
        char buf_[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
      };

      if (lib::is_oracle_mode() &&
          ob_is_blob(out_type, out_cs_type) && !ob_is_blob(in_type, in_cs_type)) {
        // !blob -> blob
        ret = OB_NOT_SUPPORTED;
        SQL_LOG(ERROR, "invalid use of blob type", K(ret), K(out_type));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "cast to blob type");
      } else if (static_cast<uint8_t>(out_scale) <= 0) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected zf length", K(ret), K(expr.datum_meta_.scale_));
      } else {
        NumberToStringFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
        if (OB_FAIL(CastHelperImpl::batch_cast(
                        cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
          SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
        }
      }
    }
    return ret;
  }

  static int string_string(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  { // todo: tiny_text_type not completed yet
    EVAL_COMMON_ARG()
    {
      class StringToStringFn : public CastFnBase {
      public:
        StringToStringFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          ObString in_str(arg_vec_->get_length(idx), arg_vec_->get_payload(idx));
          if (lib::is_oracle_mode() && 0 == in_str.length()) {
            res_vec_->set_null(idx);
          } else if (CS_TYPE_BINARY != in_cs_type_
                     && CS_TYPE_BINARY != out_cs_type_
                     && (ObCharset::charset_type_by_coll(in_cs_type_)
                           != ObCharset::charset_type_by_coll(out_cs_type_))) {
            char *buf = NULL;
            //latin1 1bytes,utf8mb4 4bytes,the factor should be 4
            int64_t buf_len = in_str.length() * ObCharset::CharConvertFactorNum;
            uint32_t result_len = 0;
            if (OB_ISNULL(buf = expr.get_str_res_mem(ctx_, buf_len, idx))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              SQL_LOG(WARN, "alloc memory failed", K(ret));
            } else if (OB_FAIL(ObCharset::charset_convert(in_cs_type_, in_str.ptr(), in_str.length(),
                                                          out_cs_type_, buf, buf_len, result_len, lib::is_mysql_mode(),
                                                          !CM_IS_IGNORE_CHARSET_CONVERT_ERR(expr.extra_) && CM_IS_IMPLICIT_CAST(expr.extra_),
                                                          ObCharset::is_cs_unicode(out_cs_type_) ? 0xFFFD : '?'))) {
              SQL_LOG(WARN, "charset convert failed", K(ret));
            } else {
              res_vec_->set_string(idx, buf, result_len);
            }
          } else {  // CS_TYPE_BINARY == in_cs_type || CS_TYPE_BINARY == out_cs_type
            if (lib::is_oracle_mode() &&
                !ob_is_blob(in_type_, in_cs_type_) &&
                ob_is_blob(out_type_, out_cs_type_)) {// !blob->blob  //ObCharType == in_type || ObVarcharType == in_type
              bool has_set_res = false;
              ObDatum res_datum;
              if (OB_FAIL(ObDatumHexUtils::hextoraw_string(expr, in_str, ctx_, res_datum, has_set_res))) {
                SQL_LOG(WARN, "fail to hextoraw_string for blob", K(ret), K(in_str));
              } else {
                if (res_datum.is_null()) { res_vec_->set_null(idx); }
                else {
                  ObString res_val = res_datum.get_string();
                  res_vec_->set_string(idx, res_val);
                }
              }
            } else {
              const ObCharsetInfo *cs = NULL;
              int64_t align_offset = 0;
              if (CS_TYPE_BINARY == in_cs_type_ && lib::is_mysql_mode()
                  && (NULL != (cs = ObCharset::get_charset(out_cs_type_)))) {
                if (cs->mbminlen > 0 && in_str.length() % cs->mbminlen != 0) {
                  align_offset = cs->mbminlen - in_str.length() % cs->mbminlen;
                }
              }
              char *out_ptr = NULL;
              int64_t out_len = 0;
              if (OB_FAIL(CastHelperImpl::vector_copy_string_zf(
                      expr, ctx_, idx, in_str.length(), in_str.ptr(), out_scale_, out_len, out_ptr, align_offset))) {
                SQL_LOG(WARN, "vector_copy_string_zf failed", K(ret), K(out_ptr));
              } else {
                res_vec_->set_string(idx, out_ptr, out_len);
              }
            }
          }
          return ret;
        }
      private:
        ArgVec *arg_vec_;
        ResVec *res_vec_;
      };

      if (!(CS_TYPE_BINARY != in_cs_type
            && CS_TYPE_BINARY != out_cs_type
            && (ObCharset::charset_type_by_coll(in_cs_type)
                  != ObCharset::charset_type_by_coll(out_cs_type))) &&
          !(CS_TYPE_BINARY == in_cs_type || CS_TYPE_BINARY == out_cs_type)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "same charset should not be here, just use cast_eval_arg", K(ret), K(in_type), K(out_type), K(in_cs_type), K(out_cs_type));
      } else if ((lib::is_oracle_mode() &&
                  !ob_is_blob(in_type, in_cs_type) &&
                  ob_is_blob(out_type, out_cs_type)) &&
                !(ObCharType == in_type || ObVarcharType == in_type)) {
        ret = OB_NOT_SUPPORTED;
        SQL_LOG(ERROR, "invalid use of blob type", K(ret), K(out_type));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "cast to blob type");
      } else {
        StringToStringFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
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
#define DEF_INTEGER_TO_STRING_IMPLICIT_CASTS(in_tc)                                 \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_STRING)                               \
  {                                                                                 \
    return ToStringCastImpl<IN_VECTOR, OUT_VECTOR>::template                   \
              int_string<RTCType<in_tc>>(expr, ctx, skip, bound);                   \
  }
LST_DO_CODE(DEF_INTEGER_TO_STRING_IMPLICIT_CASTS,
            VEC_TC_INTEGER,
            VEC_TC_UINTEGER
            );

#define DEF_FLOAT_TO_STRING_IMPLICIT_CASTS(in_tc)                                   \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_STRING)                               \
  {                                                                                 \
    return ToStringCastImpl<IN_VECTOR, OUT_VECTOR>::template                   \
              float_string<RTCType<in_tc>>(expr, ctx, skip, bound);                 \
  }
LST_DO_CODE(DEF_FLOAT_TO_STRING_IMPLICIT_CASTS,
            VEC_TC_FLOAT,
            VEC_TC_DOUBLE
            );

#define DEF_DECIMALINT_TO_STRING_IMPLICIT_CASTS(in_tc)                              \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_STRING)                               \
  {                                                                                 \
    return ToStringCastImpl<IN_VECTOR, OUT_VECTOR>::template                   \
              decimalint_string<RTCType<in_tc>>(expr, ctx, skip, bound);            \
  }
LST_DO_CODE(DEF_DECIMALINT_TO_STRING_IMPLICIT_CASTS,
            VEC_TC_DEC_INT32,
            VEC_TC_DEC_INT64,
            VEC_TC_DEC_INT128,
            VEC_TC_DEC_INT256,
            VEC_TC_DEC_INT512
            );

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_STRING, VEC_TC_STRING)
{                                                                 \
  return ToStringCastImpl<IN_VECTOR, OUT_VECTOR>::           \
            string_string(expr, ctx, skip, bound);                \
}

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_NUMBER, VEC_TC_STRING)
{                                                                 \
  return ToStringCastImpl<IN_VECTOR, OUT_VECTOR>::           \
            number_string(expr, ctx, skip, bound);                \
}

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATE, VEC_TC_STRING)
{                                                                 \
  return ToStringCastImpl<IN_VECTOR, OUT_VECTOR>::           \
            date_string(expr, ctx, skip, bound);                  \
}

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_DATETIME, VEC_TC_STRING)
{                                                                 \
  return ToStringCastImpl<IN_VECTOR, OUT_VECTOR>::           \
            datetime_string(expr, ctx, skip, bound);              \
}

} // end sql
} // namespace oceanbase

#undef DEF_INTEGER_TO_STRING_IMPLICIT_CASTS
#undef DEF_FLOAT_TO_STRING_IMPLICIT_CASTS
#undef DEF_DECIMALINT_TO_STRING_IMPLICIT_CASTS