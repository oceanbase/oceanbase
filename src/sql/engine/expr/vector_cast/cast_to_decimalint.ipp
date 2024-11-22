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
struct ToDecimalintCastImpl
{
  template<typename OUT_TYPE>
  static int string_decimalint(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      class StringToDecimalintFn : public CastFnBase {
      public:
        StringToDecimalintFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec)
            : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec) {}

        OB_INLINE int operator() (const ObExpr &expr, int idx)
        {
          int ret = OB_SUCCESS;
          ObDecimalIntBuilder res_val;
          if (lib::is_oracle_mode() && ObLongTextType != in_type_ && 0 == arg_vec_->get_length(idx)) {
            res_vec_->set_null(idx);
          } else {
            ObString in_str(arg_vec_->get_length(idx), arg_vec_->get_payload(idx));
            bool can_fast_strtoll = (expr.datum_meta_.scale_ == 0);
            int64_t out_val = can_fast_strtoll ? CastHelperImpl::fast_strtoll(in_str, expr.datum_meta_.precision_, can_fast_strtoll) : 0;
            if (can_fast_strtoll) {
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
            } else if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_decimalint_wrap(expr, in_str, ctx_.exec_ctx_.get_user_logging_ctx(),
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
      };

      StringToDecimalintFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec);
      if (OB_FAIL(CastHelperImpl::batch_cast(
                      cast_fn, expr, arg_vec, res_vec, eval_flags, skip, bound))) {
        SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
      }
    }
    return ret;
  }
};


// ================
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
