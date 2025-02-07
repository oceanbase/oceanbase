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
};

// ================
// define implicit cast functions
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_STRING, VEC_TC_INTEGER)
{                                                                  \
  return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::                \
            string_int(expr, ctx, skip, bound);                    \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_STRING, VEC_TC_UINTEGER)
{                                                                  \
  return ToIntegerCastImpl<IN_VECTOR, OUT_VECTOR>::                \
            string_uint(expr, ctx, skip, bound);                   \
}


} // end sql
} // namespace oceanbase
